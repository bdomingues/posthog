import { logger } from '~/common/utils/logger'
import { captureException } from '~/common/utils/posthog'
import { ok } from '~/ingestion/framework/results'
import { ProcessingStep } from '~/ingestion/framework/steps'
import { RetentionPeriod } from '~/ingestion/pipelines/sessionreplay/shared/constants'
import {
    RetentionLookupError,
    RetentionService,
} from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'

import { SessionBatchMetrics } from './sessions/metrics'
import { SessionBatchContext } from './sessions/session-batch-factory'

export type RetentionByKey = Map<string, RetentionPeriod>

/**
 * Flush step: resolve per-session retention off the S3 write path.
 *
 * A permanent failure (deleted/unknown team, invalid value) drops that session — it's left out of
 * the map, the write step skips it, and its offset still commits, so one poison session can't wedge
 * the whole batch. A transient failure (e.g. Redis) is rethrown so the pipeline's retry wrapper
 * re-runs the whole step.
 *
 * Additive: preserves the input batch context and adds `retentionByKey`.
 */
export function createResolveRetentionStep<T extends SessionBatchContext>(
    retentionService: RetentionService
): ProcessingStep<T, T & { retentionByKey: RetentionByKey }> {
    return async function resolveRetentionStep(batchContext) {
        const retentionByKey: RetentionByKey = new Map()
        for (const { teamId, sessionId } of batchContext.sessionBatchRecorder.getPendingSessions()) {
            try {
                retentionByKey.set(
                    `${teamId}$${sessionId}`,
                    await retentionService.getSessionRetention(teamId, sessionId)
                )
            } catch (error) {
                if (!(error instanceof RetentionLookupError)) {
                    // Transient (e.g. Redis) — let the retry wrapper retry the whole step.
                    throw error
                }
                // Permanent (deleted/unknown team) — drop this session.
                SessionBatchMetrics.incrementSessionsDroppedDuringFlush()
                logger.warn('🔁', 'session_replay_retention_unresolved_dropping_session', {
                    error: String(error),
                    sessionId,
                    teamId,
                })
                captureException(error, { tags: { sessionId, teamId: String(teamId) } })
            }
        }
        return ok({ ...batchContext, retentionByKey })
    }
}
