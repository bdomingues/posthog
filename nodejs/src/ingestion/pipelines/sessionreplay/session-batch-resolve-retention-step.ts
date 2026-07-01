import { logger } from '~/common/utils/logger'
import { ok } from '~/ingestion/framework/results'
import { ProcessingStep } from '~/ingestion/framework/steps'
import { RetentionMap } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-map'
import { RetentionService } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'

import { SessionBatchMetrics } from './sessions/metrics'
import { SessionBatchContext } from './sessions/session-batch-recorder'

/**
 * Flush step: resolve per-session retention off the S3 write path.
 *
 * Retention for the whole batch is resolved in one call (batched Redis MGET + deduped Postgres
 * fallback). An unresolvable session (deleted/unknown team, invalid value) is expected, not an
 * error: it's left out of the map, the write step skips it, and its offset still commits, so one
 * poison session can't wedge the whole batch. A transient failure (e.g. Redis) is thrown by the
 * service so the pipeline's retry wrapper re-runs the whole step.
 *
 * Additive: preserves the input batch context and adds `retentionMap`.
 */
export function createResolveRetentionStep<T extends SessionBatchContext>(
    retentionService: RetentionService
): ProcessingStep<T, T & { retentionMap: RetentionMap }> {
    return async function resolveRetentionStep(batchContext) {
        const sessions = batchContext.sessionBatchRecorder.getPendingSessions()
        const resolutions = await retentionService.resolveSessionRetentions(sessions)

        const retentionMap = new RetentionMap()
        for (let i = 0; i < sessions.length; i++) {
            const { teamId, sessionId } = sessions[i]
            const resolution = resolutions[i]
            if (resolution.resolved) {
                retentionMap.set(teamId, sessionId, resolution.retentionPeriod)
            } else {
                // Permanent (deleted/unknown team) — drop this session from the batch.
                SessionBatchMetrics.incrementSessionsDroppedDuringFlush()
                logger.warn('🔁', 'session_replay_retention_unresolved_dropping_session', { sessionId, teamId })
            }
        }
        return ok({ ...batchContext, retentionMap })
    }
}
