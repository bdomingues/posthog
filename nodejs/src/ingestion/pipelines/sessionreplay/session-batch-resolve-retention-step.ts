import { logger } from '~/common/utils/logger'
import { BatchProcessingStep } from '~/ingestion/framework/base-batch-pipeline'
import { drop, ok } from '~/ingestion/framework/results'
import { ParsedMessageData } from '~/ingestion/pipelines/sessionreplay/kafka/types'
import { RetentionPeriod } from '~/ingestion/pipelines/sessionreplay/shared/constants'
import { RetentionService } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'
import { TeamForReplay } from '~/ingestion/pipelines/sessionreplay/teams/types'

import { SessionBatchMetrics } from './sessions/metrics'

/**
 * Record-phase batch step: resolve per-session retention for the whole batch in one call (batched
 * Redis MGET + deduped Postgres fallback) and attach it to each element, before the record step
 * generates keys and folds events in — so retention is resolved off the S3 write path.
 *
 * A session whose retention can't be resolved (deleted/unknown team, invalid value) is dropped here
 * — before any key generation or recording — so a poison session never reaches storage. A transient
 * failure (e.g. Redis) is thrown by the service so the pipeline's retry wrapper can re-run the step.
 */
export function createResolveRetentionStep<T extends { team: TeamForReplay; parsedMessage: ParsedMessageData }>(
    retentionService: RetentionService
): BatchProcessingStep<T, T & { retentionPeriod: RetentionPeriod }> {
    return async function resolveRetentionStep(values) {
        const resolutions = await retentionService.resolveSessionRetentions(
            values.map((value) => ({ teamId: value.team.teamId, sessionId: value.parsedMessage.session_id }))
        )
        return values.map((value, index) => {
            const resolution = resolutions[index]
            if (resolution.resolved) {
                return ok({ ...value, retentionPeriod: resolution.retentionPeriod })
            }
            SessionBatchMetrics.incrementSessionsDroppedMissingRetention()
            logger.warn('🔁', 'session_replay_retention_unresolved_dropping_session', {
                sessionId: value.parsedMessage.session_id,
                teamId: value.team.teamId,
            })
            return drop('retention_unresolved')
        })
    }
}
