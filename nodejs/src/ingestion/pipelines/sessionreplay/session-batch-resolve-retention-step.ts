import { logger } from '~/common/utils/logger'
import { BatchProcessingStep } from '~/ingestion/framework/base-batch-pipeline'
import { drop, ok } from '~/ingestion/framework/results'
import { ParsedMessageData } from '~/ingestion/pipelines/sessionreplay/kafka/types'
import { RetentionPeriod } from '~/ingestion/pipelines/sessionreplay/shared/constants'
import { RetentionService } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'
import { TeamForReplay } from '~/ingestion/pipelines/sessionreplay/teams/types'

import { SessionBatchContext } from './session-batch-context'
import { SessionBatchMetrics } from './sessions/metrics'

type ResolveRetentionInput = SessionBatchContext & { team: TeamForReplay; parsedMessage: ParsedMessageData }

/**
 * Record-phase batch step: resolve per-session retention for the whole batch and attach it to each
 * element, before the record step generates keys and folds events in.
 *
 * Sessions the batch already holds carry their retention already, so only the rest are resolved —
 * in one call (batched Redis MGET + deduped Postgres fallback). A session whose retention can't be
 * resolved (deleted/unknown team, invalid value) is dropped here — before any key generation or
 * recording — so a poison session never reaches storage. A transient failure (e.g. Redis) is thrown
 * by the service so the pipeline's retry wrapper can re-run the step.
 */
export function createResolveRetentionStep<T extends ResolveRetentionInput>(
    retentionService: RetentionService
): BatchProcessingStep<T, T & { retentionPeriod: RetentionPeriod }> {
    return async function resolveRetentionStep(values) {
        // Retention already known for sessions accumulated in earlier polls of this batch.
        const known = values.map((value) =>
            value.sessionBatchRecorder.getRetention(value.team.teamId, value.parsedMessage.session_id)
        )

        // Resolve only the sessions we don't already hold, in one batched call.
        const unknownIndexes = values.map((_, index) => index).filter((index) => known[index] === undefined)
        const resolutions = unknownIndexes.length
            ? await retentionService.resolveSessionRetentions(
                  unknownIndexes.map((index) => ({
                      teamId: values[index].team.teamId,
                      sessionId: values[index].parsedMessage.session_id,
                  }))
              )
            : []
        const resolutionByIndex = new Map(unknownIndexes.map((index, i) => [index, resolutions[i]]))

        return values.map((value, index) => {
            const cached = known[index]
            if (cached !== undefined) {
                return ok({ ...value, retentionPeriod: cached })
            }
            const resolution = resolutionByIndex.get(index)!
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
