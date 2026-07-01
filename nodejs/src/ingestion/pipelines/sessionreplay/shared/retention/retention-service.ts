import { SessionBatchMetrics } from '~/ingestion/pipelines/sessionreplay/sessions/metrics'
import {
    RetentionPeriod,
    RetentionPeriodToDaysMap,
    ValidRetentionPeriods,
} from '~/ingestion/pipelines/sessionreplay/shared/constants'
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

/** As {@link RetentionResolution}, but resolved to a concrete number of retention days. */
export type RetentionDaysResolution = { resolved: true; retentionPeriodDays: number } | { resolved: false }

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

    /** Returns the team's retention period, or null if the team is unknown/has none set. */
    public async getRetentionByTeamId(teamId: TeamId): Promise<RetentionPeriod | null> {
        const retentionPeriod = await this.teamService.getRetentionPeriodByTeamId(teamId)

        if (retentionPeriod === null) {
            RetentionServiceMetrics.incrementLookupErrors()
        }

        return retentionPeriod
    }

    public async getSessionRetention(teamId: TeamId, sessionId: string): Promise<RetentionResolution> {
        let retentionPeriod: string | null = null

        const startTime = performance.now()
        const client = await this.redisPool.acquire()
        const redisKey = this.generateRedisKey(sessionId)

        try {
            // Attempt to look up the retention period for the session in Redis
            retentionPeriod = await client.get(redisKey)

            // ...if no retention period exists for the session
            if (retentionPeriod === null) {
                // ...get the value from Postgres
                retentionPeriod = await this.getRetentionByTeamId(teamId)

                // ...and then set it in Redis for future batches, with a TTL of 24 hours
                if (retentionPeriod !== null) {
                    await client.set(redisKey, retentionPeriod, 'EX', 24 * 60 * 60)
                }
            }
        } finally {
            await this.redisPool.release(client)
            SessionBatchMetrics.observeRetentionRedisLatency((performance.now() - startTime) / 1000)
        }

        if (retentionPeriod !== null && isValidRetentionPeriod(retentionPeriod)) {
            return { resolved: true, retentionPeriod }
        }
        // A non-null but invalid stored value is an error; an unknown team was already counted in
        // getRetentionByTeamId.
        if (retentionPeriod !== null) {
            RetentionServiceMetrics.incrementLookupErrors()
        }
        return { resolved: false }
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

    public async getSessionRetentionDays(teamId: TeamId, sessionId: string): Promise<RetentionDaysResolution> {
        const resolution = await this.getSessionRetention(teamId, sessionId)
        if (!resolution.resolved) {
            return { resolved: false }
        }

        const retentionPeriodDays = RetentionPeriodToDaysMap[resolution.retentionPeriod]
        if (retentionPeriodDays !== null) {
            return { resolved: true, retentionPeriodDays }
        }
        RetentionServiceMetrics.incrementLookupErrors()
        return { resolved: false }
    }
}
