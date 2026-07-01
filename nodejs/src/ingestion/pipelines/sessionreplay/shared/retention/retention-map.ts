import { RetentionPeriod } from '~/ingestion/pipelines/sessionreplay/shared/constants'
import { TeamId } from '~/types'

/**
 * Resolved per-session retention, addressed by team + session. Owns the composite key format so
 * callers (the resolve-retention step that fills it, the recorder that reads it) never construct
 * the key themselves. A missing entry means the session was dropped during retention resolution.
 */
export class RetentionMap {
    private readonly byKey = new Map<string, RetentionPeriod>()

    private static key(teamId: TeamId, sessionId: string): string {
        return `${teamId}$${sessionId}`
    }

    public set(teamId: TeamId, sessionId: string, retentionPeriod: RetentionPeriod): void {
        this.byKey.set(RetentionMap.key(teamId, sessionId), retentionPeriod)
    }

    public get(teamId: TeamId, sessionId: string): RetentionPeriod | undefined {
        return this.byKey.get(RetentionMap.key(teamId, sessionId))
    }

    public get size(): number {
        return this.byKey.size
    }
}
