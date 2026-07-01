import {
    AccumulatingPipeline,
    AccumulationContext,
    BeforeAccumulationInput,
    BeforeAccumulationOutput,
} from '~/ingestion/framework/accumulating-pipeline'
import { BatchPipeline } from '~/ingestion/framework/batch-pipeline.interface'
import {
    AfterBatchInput,
    AfterBatchOutput,
    BatchingContext,
    BatchingPipeline,
    BatchingPipelineOptions,
    BeforeBatchInput,
    BeforeBatchOutput,
} from '~/ingestion/framework/batching-pipeline'
import { BufferingBatchPipeline } from '~/ingestion/framework/buffering-batch-pipeline'

import { BatchPipelineBuilder } from './batch-pipeline-builders'
import { PipelineBuilder, StartPipelineBuilder } from './pipeline-builders'

export function newBatchPipelineBuilder<T, C = Record<string, never>>(): BatchPipelineBuilder<T, T, C> {
    return new BatchPipelineBuilder(new BufferingBatchPipeline<T, C>())
}

export function newPipelineBuilder<T, C = Record<string, never>>(): StartPipelineBuilder<T, C> {
    return new StartPipelineBuilder<T, C>()
}

export function newBatchingPipeline<
    TInput,
    TOutput,
    CInput,
    CBatch = NonNullable<unknown>,
    COutput = CInput,
    R extends string = never,
>(
    beforeBatch: (
        builder: StartPipelineBuilder<BeforeBatchInput<TInput, CInput>, Record<string, never>>
    ) => PipelineBuilder<
        BeforeBatchInput<TInput, CInput>,
        BeforeBatchOutput<TInput, CInput, CBatch>,
        Record<string, never>
    >,
    callback: (
        builder: BatchPipelineBuilder<
            TInput & CBatch,
            TInput & CBatch,
            CInput & BatchingContext,
            CInput & BatchingContext
        >
    ) => BatchPipelineBuilder<TInput & CBatch, TOutput, CInput & BatchingContext, COutput & BatchingContext, R>,
    afterBatch: (
        builder: StartPipelineBuilder<
            AfterBatchInput<TOutput, COutput & BatchingContext, CBatch, R>,
            Record<string, never>
        >
    ) => PipelineBuilder<
        AfterBatchInput<TOutput, COutput & BatchingContext, CBatch, R>,
        AfterBatchOutput<TOutput, COutput & BatchingContext, CBatch, R>,
        Record<string, never>
    >,
    options?: Partial<BatchingPipelineOptions>
): BatchingPipeline<TInput, TOutput, CInput, CBatch, COutput & BatchingContext, R> {
    const startBuilder = new BatchPipelineBuilder(
        new BufferingBatchPipeline<TInput & CBatch, CInput & BatchingContext>()
    )
    const subPipeline = callback(startBuilder).build()

    const beforePipeline = beforeBatch(
        new StartPipelineBuilder<BeforeBatchInput<TInput, CInput>, Record<string, never>>()
    ).build()

    const afterPipeline = afterBatch(
        new StartPipelineBuilder<
            AfterBatchInput<TOutput, COutput & BatchingContext, CBatch, R>,
            Record<string, never>
        >()
    ).build()

    return new BatchingPipeline(subPipeline, beforePipeline, afterPipeline, options)
}

/**
 * Builder-style constructor for AccumulatingPipeline, mirroring newBatchingPipeline: `beforeBatch`
 * and `flush` are builder callbacks that get `.build()`-ed for you. The record `pipeline` is passed
 * pre-built, since deployments choose it (e.g. the default vs ML-mirror session replay pipeline).
 */
export function newAccumulatingPipeline<
    TRecordIn extends object,
    TRecordOut,
    CRecordIn,
    CRecordOut,
    CBatch,
    TFlushOut,
    CFlushOut = Record<string, never>,
    R extends string = never,
>(config: {
    pipeline: BatchPipeline<TRecordIn & CBatch & AccumulationContext, TRecordOut, CRecordIn, CRecordOut, R>
    beforeBatch: (
        builder: StartPipelineBuilder<BeforeAccumulationInput, Record<string, never>>
    ) => PipelineBuilder<BeforeAccumulationInput, BeforeAccumulationOutput<CBatch>, Record<string, never>>
    flush: (
        builder: BatchPipelineBuilder<CBatch & AccumulationContext, CBatch & AccumulationContext, Record<string, never>>
    ) => BatchPipelineBuilder<CBatch & AccumulationContext, TFlushOut, Record<string, never>, CFlushOut, R>
    shouldFlush: (batchContext: CBatch & AccumulationContext) => boolean
    maxBatchAgeMs: number
}): AccumulatingPipeline<TRecordIn, TRecordOut, CRecordIn, CRecordOut, CBatch, TFlushOut, CFlushOut, R> {
    const beforeBatch = config
        .beforeBatch(new StartPipelineBuilder<BeforeAccumulationInput, Record<string, never>>())
        .build()
    const flushPipeline = config
        .flush(
            new BatchPipelineBuilder(new BufferingBatchPipeline<CBatch & AccumulationContext, Record<string, never>>())
        )
        .build()
    return new AccumulatingPipeline<TRecordIn, TRecordOut, CRecordIn, CRecordOut, CBatch, TFlushOut, CFlushOut, R>({
        beforeBatch,
        pipeline: config.pipeline,
        shouldFlush: config.shouldFlush,
        maxBatchAgeMs: config.maxBatchAgeMs,
        flushPipeline,
    })
}
