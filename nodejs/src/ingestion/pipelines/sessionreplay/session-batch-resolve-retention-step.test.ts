import { AccumulationContext } from '~/ingestion/framework/accumulating-pipeline'
import { isOkResult } from '~/ingestion/framework/results'
import {
    RetentionLookupError,
    RetentionService,
} from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'

import { createResolveRetentionStep } from './session-batch-resolve-retention-step'
import { SessionBatchMetrics } from './sessions/metrics'
import { SessionBatchContext } from './sessions/session-batch-factory'
import { SessionBatchRecorder } from './sessions/session-batch-recorder'

jest.mock('~/common/utils/logger', () => ({ logger: { warn: jest.fn() } }))
jest.mock('~/common/utils/posthog', () => ({ captureException: jest.fn() }))
jest.mock('./sessions/metrics', () => ({
    SessionBatchMetrics: { incrementSessionsDroppedDuringFlush: jest.fn() },
}))

describe('createResolveRetentionStep', () => {
    let mockRetentionService: jest.Mocked<RetentionService>

    function batchContextWith(
        sessions: { teamId: number; sessionId: string }[]
    ): SessionBatchContext & AccumulationContext {
        const sessionBatchRecorder = {
            getPendingSessions: jest.fn().mockReturnValue(sessions),
        } as unknown as SessionBatchRecorder
        return { sessionBatchRecorder, batchId: 0 }
    }

    beforeEach(() => {
        jest.clearAllMocks()
        mockRetentionService = { getSessionRetention: jest.fn() } as unknown as jest.Mocked<RetentionService>
    })

    it('resolves retention for every pending session into the map', async () => {
        mockRetentionService.getSessionRetention.mockResolvedValueOnce('30d').mockResolvedValueOnce('1y')
        const step = createResolveRetentionStep(mockRetentionService)

        const result = await step(
            batchContextWith([
                { teamId: 1, sessionId: 'a' },
                { teamId: 2, sessionId: 'b' },
            ])
        )

        expect(isOkResult(result)).toBe(true)
        if (isOkResult(result)) {
            expect(result.value.retentionByKey).toEqual(
                new Map([
                    ['1$a', '30d'],
                    ['2$b', '1y'],
                ])
            )
        }
    })

    it('drops a session on a non-retriable RetentionLookupError and keeps the rest', async () => {
        mockRetentionService.getSessionRetention
            .mockRejectedValueOnce(new RetentionLookupError('Unknown team id 999'))
            .mockResolvedValueOnce('90d')
        const step = createResolveRetentionStep(mockRetentionService)

        const result = await step(
            batchContextWith([
                { teamId: 999, sessionId: 'gone' },
                { teamId: 2, sessionId: 'ok' },
            ])
        )

        expect(isOkResult(result)).toBe(true)
        if (isOkResult(result)) {
            // the deleted team's session is left out of the map (the write step will skip it)
            expect(result.value.retentionByKey).toEqual(new Map([['2$ok', '90d']]))
        }
        expect(SessionBatchMetrics.incrementSessionsDroppedDuringFlush).toHaveBeenCalledTimes(1)
    })

    it('rethrows a transient (retriable) error so the retry wrapper can retry the whole step', async () => {
        mockRetentionService.getSessionRetention.mockRejectedValue(new Error('Redis connection lost'))
        const step = createResolveRetentionStep(mockRetentionService)

        await expect(step(batchContextWith([{ teamId: 1, sessionId: 'a' }]))).rejects.toThrow('Redis connection lost')
        expect(SessionBatchMetrics.incrementSessionsDroppedDuringFlush).not.toHaveBeenCalled()
    })
})
