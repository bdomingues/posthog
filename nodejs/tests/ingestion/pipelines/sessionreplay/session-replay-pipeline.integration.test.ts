/**
 * Integration test for the session replay pipeline.
 *
 * Drives createSessionReplayPipeline end-to-end with every external dependency (Kafka, Redis, S3)
 * mocked: the real inner pipeline (which resolves retention in a batch step before recording, over a
 * mock Redis client), the real flush pipeline (write → commit offsets → record metrics), and a mock
 * recorder standing in for the S3/metadata writes. It locks in two guarantees: retention is resolved
 * before recording (unresolvable sessions are dropped there), and flush metrics are recorded only
 * after the batch is written and its offsets committed.
 */
import { Message } from 'node-rdkafka'

import { DLQ_OUTPUT, INGESTION_WARNINGS_OUTPUT, OVERFLOW_OUTPUT } from '~/common/outputs'
import { EventIngestionRestrictionManager } from '~/common/utils/event-ingestion-restrictions'
import { PromiseScheduler } from '~/common/utils/promise-scheduler'
import { createApplyEventRestrictionsStep, createParseHeadersStep } from '~/ingestion/common/steps/event-preprocessing'
import { TopHogRegistry } from '~/ingestion/framework/extensions/tophog'
import { createOkContext } from '~/ingestion/framework/helpers'
import { ok } from '~/ingestion/framework/results'
import { KafkaOffsetManager } from '~/ingestion/pipelines/sessionreplay/kafka/offset-manager'
import {
    SessionReplayPipeline,
    createSessionReplayInnerPipeline,
    createSessionReplayPipeline,
} from '~/ingestion/pipelines/sessionreplay/session-replay-pipeline'
import { SessionBatchMetrics } from '~/ingestion/pipelines/sessionreplay/sessions/metrics'
import { SessionBatchFactory } from '~/ingestion/pipelines/sessionreplay/sessions/session-batch-factory'
import { SessionBatchRecorder } from '~/ingestion/pipelines/sessionreplay/sessions/session-batch-recorder'
import { SessionBlockMetadata } from '~/ingestion/pipelines/sessionreplay/shared/metadata/session-block-metadata'
import { RetentionService } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'
import { TeamService } from '~/ingestion/pipelines/sessionreplay/shared/teams/team-service'
import { TeamForReplay } from '~/ingestion/pipelines/sessionreplay/teams/types'
import { createMockIngestionOutputs } from '~/tests/helpers/mock-ingestion-outputs'
import { RedisPool } from '~/types'

jest.mock('~/ingestion/common/steps/event-preprocessing', () => ({
    createParseHeadersStep: jest.fn(),
    createApplyEventRestrictionsStep: jest.fn(),
}))

const mockCreateParseHeadersStep = createParseHeadersStep as jest.Mock
const mockCreateApplyEventRestrictionsStep = createApplyEventRestrictionsStep as jest.Mock

const defaultTeam: TeamForReplay = {
    teamId: 1,
    consoleLogIngestionEnabled: false,
    aiTrainingOptedIn: true,
}

function createMockTopHog(): TopHogRegistry {
    const recorder = { record: jest.fn() }
    return {
        registerSum: jest.fn().mockReturnValue(recorder),
        registerMax: jest.fn().mockReturnValue(recorder),
        registerAverage: jest.fn().mockReturnValue(recorder),
    } as unknown as TopHogRegistry
}

function createSnapshotMessage(sessionId: string, offset: number): Message {
    const event = {
        event: '$snapshot_items',
        properties: {
            $session_id: sessionId,
            $window_id: 'window-1',
            $snapshot_items: [{ type: 2, timestamp: Date.now(), data: {} }],
        },
    }
    const payload = JSON.stringify({ distinct_id: 'user-123', data: JSON.stringify(event) })
    return {
        partition: 0,
        offset,
        topic: 'test-topic',
        value: Buffer.from(payload),
        key: Buffer.from('test-key'),
        timestamp: Date.now(),
        headers: [{ token: Buffer.from('test-token') }, { session_id: Buffer.from(sessionId) }],
        size: payload.length,
    }
}

function blockMetadata(sessionId: string): SessionBlockMetadata {
    return {
        sessionId,
        teamId: 1,
        distinctId: 'user-123',
        eventCount: 1,
        blockLength: 100,
    } as unknown as SessionBlockMetadata
}

describe('session replay pipeline integration', () => {
    let pipeline: SessionReplayPipeline
    let mockRecorder: jest.Mocked<SessionBatchRecorder>
    let mockOffsetManager: jest.Mocked<KafkaOffsetManager>
    let mockRedisClient: { mget: jest.Mock; pipeline: jest.Mock }
    let mockTeamServiceForRetention: jest.Mocked<TeamService>
    let recordFlushedBatchSpy: jest.SpyInstance
    let events: string[]

    beforeEach(() => {
        jest.clearAllMocks()
        events = []

        mockCreateParseHeadersStep.mockReturnValue((input: { message: Message; headers?: Record<string, string> }) => {
            const headers: Record<string, string> = {}
            for (const header of input.message.headers || []) {
                for (const [key, value] of Object.entries(header)) {
                    headers[key] = Buffer.isBuffer(value) ? value.toString() : value
                }
            }
            return Promise.resolve(ok({ ...input, headers }))
        })
        mockCreateApplyEventRestrictionsStep.mockReturnValue((input: unknown) => Promise.resolve(ok(input)))

        // Mock recorder: the record step folds into it, the flush step drains it. size >= 1 makes the
        // batch flush after any recorded event.
        mockRecorder = {
            record: jest.fn().mockResolvedValue(100),
            size: 100,
            flushToStorage: jest.fn().mockResolvedValue([blockMetadata('session-1')]),
            discardPartition: jest.fn(),
        } as unknown as jest.Mocked<SessionBatchRecorder>

        // First create() mints the batch we record into and flush; any batch re-minted after the
        // flush is empty (size 0), so the size trigger fires exactly once.
        const emptyRecorder = () =>
            ({
                record: jest.fn().mockResolvedValue(0),
                size: 0,
                flushToStorage: jest.fn().mockResolvedValue([]),
                discardPartition: jest.fn(),
            }) as unknown as SessionBatchRecorder
        let created = 0
        const fakeFactory = { create: () => (created++ === 0 ? mockRecorder : emptyRecorder()) } as SessionBatchFactory

        mockOffsetManager = {
            trackOffset: jest.fn(),
            discardPartition: jest.fn(),
            commit: jest.fn().mockImplementation(() => {
                events.push('commit')
                return Promise.resolve()
            }),
        } as unknown as jest.Mocked<KafkaOffsetManager>

        // Cache hit by default: retention resolves to 30d in one MGET, no Postgres lookup.
        mockRedisClient = {
            mget: jest.fn().mockResolvedValue(['30d']),
            pipeline: jest
                .fn()
                .mockReturnValue({ set: jest.fn().mockReturnThis(), exec: jest.fn().mockResolvedValue([]) }),
        }
        const mockRedisPool = {
            acquire: jest.fn().mockResolvedValue(mockRedisClient),
            release: jest.fn().mockResolvedValue(undefined),
        } as unknown as RedisPool
        mockTeamServiceForRetention = {
            getRetentionPeriodByTeamId: jest.fn().mockResolvedValue('30d'),
        } as unknown as jest.Mocked<TeamService>
        const retentionService = new RetentionService(mockRedisPool, mockTeamServiceForRetention)

        recordFlushedBatchSpy = jest
            .spyOn(SessionBatchMetrics, 'recordFlushedBatch')
            .mockImplementation(() => events.push('metrics'))
        jest.spyOn(SessionBatchMetrics, 'incrementSessionsDroppedMissingRetention').mockImplementation(() => {})

        const recordPipeline = createSessionReplayInnerPipeline({
            outputs: createMockIngestionOutputs<
                typeof DLQ_OUTPUT | typeof OVERFLOW_OUTPUT | typeof INGESTION_WARNINGS_OUTPUT
            >(),
            eventIngestionRestrictionManager: {} as unknown as EventIngestionRestrictionManager,
            overflowEnabled: false,
            promiseScheduler: new PromiseScheduler(),
            teamService: {
                getTeamByToken: jest.fn().mockResolvedValue(defaultTeam),
                getRetentionPeriodByTeamId: jest.fn().mockResolvedValue('30d'),
            } as unknown as TeamService,
            retentionService,
            topHog: createMockTopHog(),
            isDebugLoggingEnabled: () => false,
        })

        pipeline = createSessionReplayPipeline({
            recordPipeline,
            sessionBatchFactory: fakeFactory,
            offsetManager: mockOffsetManager,
            maxBatchSizeBytes: 1,
            maxBatchAgeMs: 60_000,
        })
    })

    async function feed(messages: Message[]): Promise<void> {
        await pipeline.feed(messages.map((message) => createOkContext({ message }, { message })))
    }

    // Drains next() to null, returning whether a flush happened.
    async function drainToFlush(): Promise<boolean> {
        let flushed = false
        let result = await pipeline.next()
        while (result !== null) {
            flushed = flushed || result.flushed
            result = await pipeline.next()
        }
        return flushed
    }

    it('resolves retention and records, then on flush writes, commits offsets, and records metrics in that order', async () => {
        await feed([createSnapshotMessage('session-1', 1)])

        // Drain just the record phase: retention resolves, the message is recorded, nothing flushed yet.
        const recordResult = await pipeline.next()
        expect(recordResult?.flushed).toBe(false)
        expect(mockRecorder.record).toHaveBeenCalledTimes(1)
        expect(mockRecorder.record).toHaveBeenCalledWith(expect.anything(), '30d')
        expect(mockOffsetManager.commit).not.toHaveBeenCalled()
        expect(recordFlushedBatchSpy).not.toHaveBeenCalled()

        // Drain the rest: the size trigger flushes the batch.
        const flushed = await drainToFlush()
        expect(flushed).toBe(true)
        expect(mockRecorder.flushToStorage).toHaveBeenCalledTimes(1)
        expect(mockOffsetManager.commit).toHaveBeenCalledTimes(1)
        expect(recordFlushedBatchSpy).toHaveBeenCalledWith([blockMetadata('session-1')])
        // Metrics are recorded only after the write and the offset commit.
        expect(events).toEqual(['commit', 'metrics'])
    })

    it('does not record flush metrics when the offset commit fails', async () => {
        mockOffsetManager.commit.mockRejectedValue(new Error('kafka commit failed'))

        await feed([createSnapshotMessage('session-1', 1)])

        await expect(drainToFlush()).rejects.toThrow('kafka commit failed')
        expect(mockRecorder.flushToStorage).toHaveBeenCalledTimes(1)
        expect(recordFlushedBatchSpy).not.toHaveBeenCalled()
    })

    it('drops a session with unresolvable retention before it is recorded', async () => {
        // Cache miss and the team has no retention → the session is unresolvable.
        mockRedisClient.mget.mockResolvedValue([null])
        mockTeamServiceForRetention.getRetentionPeriodByTeamId.mockResolvedValue(null)

        await feed([createSnapshotMessage('session-1', 1)])
        await pipeline.next() // record phase

        expect(mockRecorder.record).not.toHaveBeenCalled()
        expect(SessionBatchMetrics.incrementSessionsDroppedMissingRetention).toHaveBeenCalledTimes(1)
    })

    it('retries a transient retention failure and then records', async () => {
        jest.useFakeTimers()
        try {
            // First retention lookup fails transiently (Redis), the retry succeeds.
            mockRedisClient.mget.mockRejectedValueOnce(new Error('redis unavailable')).mockResolvedValue(['30d'])

            await feed([createSnapshotMessage('session-1', 1)])

            const recordPromise = pipeline.next()
            // Let the retry's backoff sleep elapse.
            await jest.advanceTimersByTimeAsync(200)
            await recordPromise

            expect(mockRedisClient.mget).toHaveBeenCalledTimes(2)
            expect(mockRecorder.record).toHaveBeenCalledTimes(1)
        } finally {
            jest.useRealTimers()
        }
    })
})
