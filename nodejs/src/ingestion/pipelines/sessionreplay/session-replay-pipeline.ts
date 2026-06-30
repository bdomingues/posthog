import { Message } from 'node-rdkafka'

import { DlqOutput, IngestionWarningsOutput, OverflowOutput } from '~/common/outputs'
import { IngestionOutputs } from '~/common/outputs/ingestion-outputs'
import { EventIngestionRestrictionManager } from '~/common/utils/event-ingestion-restrictions'
import { PromiseScheduler } from '~/common/utils/promise-scheduler'
import { createApplyEventRestrictionsStep, createParseHeadersStep } from '~/ingestion/common/steps/event-preprocessing'
import { AccumulationContext } from '~/ingestion/framework/accumulating-pipeline'
import { BatchPipeline } from '~/ingestion/framework/batch-pipeline.interface'
import { newBatchPipelineBuilder } from '~/ingestion/framework/builders'
import { TopHogRegistry, createTopHogWrapper, sum, timer } from '~/ingestion/framework/extensions/tophog'
import { PipelineConfig } from '~/ingestion/framework/result-handling-pipeline'
import { ParsedMessageData } from '~/ingestion/pipelines/sessionreplay/kafka/types'
import { SessionBatchContext } from '~/ingestion/pipelines/sessionreplay/sessions/session-batch-factory'
import { TeamService } from '~/ingestion/pipelines/sessionreplay/shared/teams/team-service'
import { TeamForReplay } from '~/ingestion/pipelines/sessionreplay/teams/types'
import { ValueMatcher } from '~/types'

import { createLibVersionMonitorStep } from './lib-version-monitor-step'
import { createParseMessageStep } from './parse-message-step'
import { createRecordSessionEventStep } from './record-session-event-step'
import { createTeamFilterStep } from './team-filter-step'

export interface SessionReplayPipelineInput {
    message: Message
}

export interface SessionReplayPipelineOutput {
    team: TeamForReplay
    parsedMessage: ParsedMessageData
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
export function createSessionReplayPipeline(
    config: SessionReplayPipelineConfig
): BatchPipeline<
    SessionReplayPipelineInput & SessionBatchContext & AccumulationContext,
    SessionReplayPipelineOutput,
    { message: Message },
    { message: Message },
    OverflowOutput
> {
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
