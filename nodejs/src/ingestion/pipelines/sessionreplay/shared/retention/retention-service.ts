import { SessionBatchMetrics } from '~/ingestion/pipelines/sessionreplay/sessions/metrics'
import { RetentionPeriod, ValidRetentionPeriods } from '~/ingestion/pipelines/sessionreplay/shared/constants'
import { TeamService } from '~/ingestion/pipelines/sessionreplay/shared/teams/team-service'
import { RedisPool, TeamId } from '~/types'

import { RetentionServiceMetrics } from './metrics'

/**
 * Outcome of resolving one session's retention. `resolved: false` is the expected, permanent
 * "can't determine retention" case (deleted/unknown team, invalid stored value) — the caller drops
 * that session. A transient failure (e.g. Redis unavailable) is thrown, not returned, so a retry
 * wrapper can re-run the lookup.
 */
export type RetentionResolution = { resolved: true; retentionPeriod: RetentionPeriod } | { resolved: false }

function isValidRetentionPeriod(retentionPeriod: string): retentionPeriod is RetentionPeriod {
    return ValidRetentionPeriods.includes(retentionPeriod as RetentionPeriod)
}

export class RetentionService {
    constructor(
        private redisPool: RedisPool,
        private teamService: TeamService,
        private keyPrefix = '@posthog/replay/'
    ) {}

    private generateRedisKey(sessionId: string): string {
        return `${this.keyPrefix}session-retention-${sessionId}`
    }

    /**
     * Resolves retention for a whole batch of sessions in one Redis round trip (MGET), falling back
     * to Postgres for cache misses — deduped to one lookup per distinct team — and writing the
     * resolved values back to Redis in a single pipeline. Results are returned aligned with the
     * input order. Permanent failures come back as `{ resolved: false }`; a transient Redis or
     * Postgres failure throws so the caller's retry wrapper can re-run the whole lookup.
     */
    public async resolveSessionRetentions(
        sessions: { teamId: TeamId; sessionId: string }[]
    ): Promise<RetentionResolution[]> {
        if (sessions.length === 0) {
            return []
        }

        const startTime = performance.now()
        const client = await this.redisPool.acquire()
        try {
            const redisKeys = sessions.map(({ sessionId }) => this.generateRedisKey(sessionId))
            const cached = await client.mget(redisKeys)

            const resolutions = new Array<RetentionResolution>(sessions.length)
            const missIndexes: number[] = []

            for (let i = 0; i < sessions.length; i++) {
                const value = cached[i]
                if (value === null) {
                    missIndexes.push(i)
                } else if (isValidRetentionPeriod(value)) {
                    resolutions[i] = { resolved: true, retentionPeriod: value }
                } else {
                    RetentionServiceMetrics.incrementLookupErrors()
                    resolutions[i] = { resolved: false }
                }
            }

            if (missIndexes.length > 0) {
                // One Postgres lookup per distinct team, resolved concurrently, not per session.
                const teamRetentions = new Map<TeamId, RetentionPeriod | null>()
                await Promise.all(
                    [...new Set(missIndexes.map((i) => sessions[i].teamId))].map(async (teamId) => {
                        teamRetentions.set(teamId, await this.teamService.getRetentionPeriodByTeamId(teamId))
                    })
                )

                const writeBack = client.pipeline()
                let hasWriteBack = false
                for (const i of missIndexes) {
                    const retentionPeriod = teamRetentions.get(sessions[i].teamId) ?? null
                    if (retentionPeriod === null) {
                        RetentionServiceMetrics.incrementLookupErrors()
                        resolutions[i] = { resolved: false }
                    } else {
                        resolutions[i] = { resolved: true, retentionPeriod }
                        // Cache for future batches, with a TTL of 24 hours.
                        writeBack.set(redisKeys[i], retentionPeriod, 'EX', 24 * 60 * 60)
                        hasWriteBack = true
                    }
                }
                if (hasWriteBack) {
                    await writeBack.exec()
                }
            }

            return resolutions
        } finally {
            await this.redisPool.release(client)
            SessionBatchMetrics.observeRetentionRedisLatency((performance.now() - startTime) / 1000)
        }
    }
}
