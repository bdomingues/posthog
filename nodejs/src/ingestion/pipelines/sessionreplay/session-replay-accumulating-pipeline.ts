import { Message } from 'node-rdkafka'

import { OverflowOutput } from '~/common/outputs'
import { logger } from '~/common/utils/logger'
import { captureException } from '~/common/utils/posthog'
import {
    AccumulatingPipeline,
    AccumulationContext,
    BeforeAccumulationInput,
    BeforeAccumulationOutput,
} from '~/ingestion/framework/accumulating-pipeline'
import { BatchPipeline } from '~/ingestion/framework/batch-pipeline.interface'
import { newAccumulatingPipeline } from '~/ingestion/framework/builders'
import { ok } from '~/ingestion/framework/results'
import { ProcessingStep } from '~/ingestion/framework/steps'
import { RetentionPeriod } from '~/ingestion/pipelines/sessionreplay/shared/constants'
import { SessionBlockMetadata } from '~/ingestion/pipelines/sessionreplay/shared/metadata/session-block-metadata'
import { RetentionService } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'

import { SessionReplayPipelineInput, SessionReplayPipelineOutput } from './session-replay-pipeline'
import { SessionBatchMetrics } from './sessions/metrics'
import { SessionBatchContext, SessionBatchFactory } from './sessions/session-batch-factory'

/**
 * The per-message record pipeline wrapped by the accumulating pipeline. Its input carries the
 * batch context (the recorder) tagged on by the accumulating pipeline, which the record step
 * folds events into.
 */
export type SessionReplayRecordPipeline = BatchPipeline<
    SessionReplayPipelineInput & SessionBatchContext & AccumulationContext,
    SessionReplayPipelineOutput,
    { message: Message },
    { message: Message },
    OverflowOutput
>

export type SessionReplayAccumulatingPipeline = AccumulatingPipeline<
    SessionReplayPipelineInput,
    SessionReplayPipelineOutput,
    { message: Message },
    { message: Message },
    SessionBatchContext,
    SessionBlockMetadata[],
    Record<string, never>,
    OverflowOutput
>

export interface SessionReplayAccumulatingPipelineConfig {
    recordPipeline: SessionReplayRecordPipeline
    sessionBatchFactory: SessionBatchFactory
    /** Resolves per-session retention off the S3 write path in the resolve-retention flush step */
    retentionService: RetentionService
    /** Maximum raw size (before compression) of a batch in bytes before it is flushed */
    maxBatchSizeBytes: number
    /** Maximum age of a batch in milliseconds before it is flushed */
    maxBatchAgeMs: number
}

/**
 * Assembles the session replay accumulating pipeline: the record pipeline folds events into a
 * recorder minted per cycle by the factory; the flush pipeline writes that recorder to storage on a
 * size or age trigger. Offset commit stays with the consumer (it commits on each flushed result).
 */
export function createSessionReplayAccumulatingPipeline(
    config: SessionReplayAccumulatingPipelineConfig
): SessionReplayAccumulatingPipeline {
    const { recordPipeline, sessionBatchFactory, retentionService, maxBatchSizeBytes, maxBatchAgeMs } = config

    // beforeBatch: mint a fresh recorder for the next accumulation cycle.
    const beforeBatchStep: ProcessingStep<BeforeAccumulationInput, BeforeAccumulationOutput<SessionBatchContext>> = (
        input
    ) =>
        Promise.resolve(
            ok({ batchContext: { sessionBatchRecorder: sessionBatchFactory.createBatch(), batchId: input.batchId } })
        )

    // Flush step 1: resolve per-session retention off the S3 write path. A session whose retention
    // can't be resolved (e.g. a deleted team) is left out of the map, so the write step drops it —
    // its offset still commits, so a poison session doesn't wedge the batch.
    type RetentionResolved = SessionBatchContext &
        AccumulationContext & { retentionByKey: Map<string, RetentionPeriod> }
    const resolveRetentionStep: ProcessingStep<SessionBatchContext & AccumulationContext, RetentionResolved> = async (
        batchContext
    ) => {
        const retentionByKey = new Map<string, RetentionPeriod>()
        for (const { teamId, sessionId } of batchContext.sessionBatchRecorder.getPendingSessions()) {
            try {
                retentionByKey.set(
                    `${teamId}$${sessionId}`,
                    await retentionService.getSessionRetention(teamId, sessionId)
                )
            } catch (error) {
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

    // Flush step 2: write the accumulated batch to storage (retention already resolved; no offsets).
    const writeStep: ProcessingStep<RetentionResolved, SessionBlockMetadata[]> = async (batchContext) =>
        ok(await batchContext.sessionBatchRecorder.flushToStorage(batchContext.retentionByKey))

    return newAccumulatingPipeline<
        SessionReplayPipelineInput,
        SessionReplayPipelineOutput,
        { message: Message },
        { message: Message },
        SessionBatchContext,
        SessionBlockMetadata[],
        Record<string, never>,
        OverflowOutput
    >({
        pipeline: recordPipeline,
        beforeBatch: (builder) => builder.pipe(beforeBatchStep),
        flush: (builder) => builder.sequentially((b) => b.pipe(resolveRetentionStep).pipe(writeStep)),
        shouldFlush: (batchContext) => batchContext.sessionBatchRecorder.size >= maxBatchSizeBytes,
        maxBatchAgeMs,
    })
}
