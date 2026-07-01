import { Message } from 'node-rdkafka'

import { DlqOutput, IngestionWarningsOutput, OverflowOutput } from '~/common/outputs'
import { IngestionOutputs } from '~/common/outputs/ingestion-outputs'
import { EventIngestionRestrictionManager } from '~/common/utils/event-ingestion-restrictions'
import { PromiseScheduler } from '~/common/utils/promise-scheduler'
import { createApplyEventRestrictionsStep, createParseHeadersStep } from '~/ingestion/common/steps/event-preprocessing'
import { AccumulatingPipeline, AccumulationContext } from '~/ingestion/framework/accumulating-pipeline'
import { BatchPipeline } from '~/ingestion/framework/batch-pipeline.interface'
import { newAccumulatingPipeline, newBatchPipelineBuilder } from '~/ingestion/framework/builders'
import { TopHogRegistry, createTopHogWrapper, sum, timer } from '~/ingestion/framework/extensions/tophog'
import { PipelineConfig } from '~/ingestion/framework/result-handling-pipeline'
import { KafkaOffsetManager } from '~/ingestion/pipelines/sessionreplay/kafka/offset-manager'
import { ParsedMessageData } from '~/ingestion/pipelines/sessionreplay/kafka/types'
import { SessionBatchFactory } from '~/ingestion/pipelines/sessionreplay/sessions/session-batch-factory'
import { SessionBatchContext } from '~/ingestion/pipelines/sessionreplay/sessions/session-batch-recorder'
import { SessionBlockMetadata } from '~/ingestion/pipelines/sessionreplay/shared/metadata/session-block-metadata'
import { RetentionService } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'
import { TeamService } from '~/ingestion/pipelines/sessionreplay/shared/teams/team-service'
import { TeamForReplay } from '~/ingestion/pipelines/sessionreplay/teams/types'
import { ValueMatcher } from '~/types'

import { createLibVersionMonitorStep } from './lib-version-monitor-step'
import { createParseMessageStep } from './parse-message-step'
import { createRecordSessionEventStep } from './record-session-event-step'
import { createCommitOffsetsStep } from './session-batch-commit-offsets-step'
import { createRecordMetricsStep } from './session-batch-record-metrics-step'
import { createResolveRetentionStep } from './session-batch-resolve-retention-step'
import { createCreateSessionBatchStep } from './session-batch-step'
import { createWriteStep } from './session-batch-write-step'
import { createTeamFilterStep } from './team-filter-step'

export interface SessionReplayPipelineInput {
    message: Message
}

export interface SessionReplayPipelineOutput {
    team: TeamForReplay
    parsedMessage: ParsedMessageData
}

/**
 * The per-message record pipeline wrapped by the accumulating pipeline. Its input carries the
 * batch context (the recorder) tagged on by the accumulating pipeline, which the record step
 * folds events into.
 */
export type SessionReplayRecordPipeline = BatchPipeline<
    SessionReplayPipelineInput & SessionBatchContext & AccumulationContext, // TInput: element in (raw input + batch recorder + batch id)
    SessionReplayPipelineOutput, // TOutput: element out of the record pipeline
    { message: Message }, // CInput: per-element context in (the Kafka message)
    { message: Message }, // COutput: per-element context out (the Kafka message)
    OverflowOutput // R: redirect output names this pipeline can emit
>

export type SessionReplayAccumulatingPipeline = AccumulatingPipeline<
    SessionReplayPipelineInput, // TRecordIn: element fed in per message (batch context is added internally)
    SessionReplayPipelineOutput, // TRecordOut: element out of the record pipeline
    { message: Message }, // CRecordIn: record-pipeline context in (the Kafka message)
    { message: Message }, // CRecordOut: record-pipeline context out (the Kafka message)
    SessionBatchContext, // CBatch: batch context minted per cycle (the recorder), tagged on every element and the flush unit
    SessionBlockMetadata[], // TFlushOut: element out of the flush pipeline (written block metadata)
    Record<string, never>, // CFlushOut: flush-pipeline context out (empty — the flush unit carries no context)
    OverflowOutput // R: redirect output names this pipeline can emit
>

export interface SessionReplayAccumulatingPipelineConfig {
    recordPipeline: SessionReplayRecordPipeline
    sessionBatchFactory: SessionBatchFactory
    /** Resolves per-session retention off the S3 write path in the resolve-retention flush step */
    retentionService: RetentionService
    /** Committed by the commit-offsets flush step after the write step persists the batch */
    offsetManager: KafkaOffsetManager
    /** Maximum raw size (before compression) of a batch in bytes before it is flushed */
    maxBatchSizeBytes: number
    /** Maximum age of a batch in milliseconds before it is flushed */
    maxBatchAgeMs: number
}

export interface SessionReplayPipelineConfig {
    outputs: IngestionOutputs<IngestionWarningsOutput | DlqOutput | OverflowOutput>
    eventIngestionRestrictionManager: EventIngestionRestrictionManager
    overflowEnabled: boolean
    promiseScheduler: PromiseScheduler
    teamService: TeamService
    /** TopHog registry for tracking metrics. */
    topHog: TopHogRegistry
    /** Debug logging matcher for partition-based debugging. */
    isDebugLoggingEnabled: ValueMatcher<number>
}

/**
 * Creates the session replay pipeline.
 *
 * The pipeline processes messages through these phases:
 * 1. Restrictions - Parse headers and apply event ingestion restrictions (drop/overflow)
 * 2. Team Filter - Validate team ownership and enrich with team context
 * 3. Parse - Parse Kafka messages into structured session recording data (inside teamAware for warning handling)
 * 4. Version Monitor - Check library version and emit warnings for old versions
 * 5. Record - Record parsed messages to session batches
 */
export function createSessionReplayPipeline(config: SessionReplayPipelineConfig): SessionReplayRecordPipeline {
    const {
        outputs,
        eventIngestionRestrictionManager,
        overflowEnabled,
        promiseScheduler,
        teamService,
        topHog,
        isDebugLoggingEnabled,
    } = config

    const pipelineConfig: PipelineConfig<OverflowOutput> = {
        outputs,
        promiseScheduler,
    }

    const topHogWrapper = createTopHogWrapper(topHog)

    const pipeline = newBatchPipelineBuilder<
        SessionReplayPipelineInput & SessionBatchContext & AccumulationContext,
        { message: Message }
    >()
        .messageAware((b) =>
            b
                .sequentially((b) =>
                    b
                        // Parse headers and apply restrictions (drop/overflow)
                        .pipe(createParseHeadersStep())
                        .pipe(
                            createApplyEventRestrictionsStep(eventIngestionRestrictionManager, {
                                overflowEnabled,
                                preservePartitionLocality: true, // Sessions must stay on the same partition
                            })
                        )
                        // Validate team ownership and enrich with team context
                        .pipe(createTeamFilterStep(teamService))
                )
                // Map TeamForReplay.teamId to context.team.id for handleIngestionWarnings
                .filterMap(
                    (element) => ({
                        result: element.result,
                        context: {
                            ...element.context,
                            team: { id: element.result.value.team.teamId },
                        },
                    }),
                    (b) =>
                        b
                            .teamAware((b) =>
                                b
                                    .sequentially((b) =>
                                        b
                                            // Parse message content
                                            .pipe(
                                                topHogWrapper(createParseMessageStep(), [
                                                    timer('parse_time_ms_by_session_id', (input) => ({
                                                        token: input.headers.token ?? 'unknown',
                                                        session_id: input.headers.session_id ?? 'unknown',
                                                    })),
                                                ])
                                            )
                                            // Monitor library version and emit warnings for old versions
                                            .pipe(createLibVersionMonitorStep())
                                            // Record to session batch
                                            .pipe(
                                                topHogWrapper(
                                                    createRecordSessionEventStep({
                                                        isDebugLoggingEnabled,
                                                    }),
                                                    [
                                                        sum(
                                                            'message_size_by_session_id',
                                                            (input) => ({
                                                                token: input.parsedMessage.token ?? 'unknown',
                                                                session_id: input.parsedMessage.session_id,
                                                            }),
                                                            (input) => input.parsedMessage.metadata.rawSize
                                                        ),
                                                        timer('consume_time_ms_by_session_id', (input) => ({
                                                            token: input.parsedMessage.token ?? 'unknown',
                                                            session_id: input.parsedMessage.session_id,
                                                        })),
                                                    ]
                                                )
                                            )
                                    )
                                    .gather()
                            )
                            .handleIngestionWarnings(outputs)
                )
        )
        .handleResults(pipelineConfig)
        .handleSideEffects(promiseScheduler, { await: false })
        .gather()
        .build()

    return pipeline
}

/**
 * Wraps the record pipeline in an accumulating pipeline: the record pipeline folds events into a
 * recorder minted per cycle by the factory; the flush pipeline resolves retention off the S3 write
 * path (retrying transient failures) and then writes the recorder to storage on a size or age
 * trigger. Offset commit stays with the consumer — it commits on each flushed result.
 */
export function createSessionReplayAccumulatingPipeline(
    config: SessionReplayAccumulatingPipelineConfig
): SessionReplayAccumulatingPipeline {
    const { recordPipeline, sessionBatchFactory, retentionService, offsetManager, maxBatchSizeBytes, maxBatchAgeMs } =
        config

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
        beforeBatch: (builder) => builder.pipe(createCreateSessionBatchStep(sessionBatchFactory)),
        pipeline: recordPipeline,
        // The flush lifecycle: resolve retention (off the S3 path), write to storage, commit the
        // offsets it covers, then record the flush metrics from the write step's block metadata.
        flush: (builder) =>
            builder.sequentially((b) =>
                b
                    // Retry transient retention failures (e.g. Redis). Permanent failures (deleted
                    // team, invalid value) are non-retriable — the step drops those sessions instead.
                    .retry((rb) => rb.pipe(createResolveRetentionStep(retentionService)), {
                        tries: 3,
                        sleepMs: 100,
                        name: 'session_replay_retention',
                    })
                    .pipe(createWriteStep())
                    .pipe(createCommitOffsetsStep(offsetManager))
                    .pipe(createRecordMetricsStep())
            ),
        shouldFlush: (batchContext) => batchContext.sessionBatchRecorder.size >= maxBatchSizeBytes,
        maxBatchAgeMs,
    })
}
