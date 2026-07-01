import { Message } from 'node-rdkafka'

import { OverflowOutput } from '~/common/outputs'
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
import { SessionBlockMetadata } from '~/ingestion/pipelines/sessionreplay/shared/metadata/session-block-metadata'

import { SessionReplayPipelineInput, SessionReplayPipelineOutput } from './session-replay-pipeline'
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
    const { recordPipeline, sessionBatchFactory, maxBatchSizeBytes, maxBatchAgeMs } = config

    // beforeBatch: mint a fresh recorder for the next accumulation cycle.
    const beforeBatchStep: ProcessingStep<BeforeAccumulationInput, BeforeAccumulationOutput<SessionBatchContext>> = (
        input
    ) =>
        Promise.resolve(
            ok({ batchContext: { sessionBatchRecorder: sessionBatchFactory.createBatch(), batchId: input.batchId } })
        )

    // flush step: write the accumulated batch to storage (without committing offsets).
    const flushStep: ProcessingStep<SessionBatchContext & AccumulationContext, SessionBlockMetadata[]> = async (
        batchContext
    ) => ok(await batchContext.sessionBatchRecorder.flushToStorage())

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
        flush: (builder) => builder.sequentially((b) => b.pipe(flushStep)),
        shouldFlush: (batchContext) => batchContext.sessionBatchRecorder.size >= maxBatchSizeBytes,
        maxBatchAgeMs,
    })
}
