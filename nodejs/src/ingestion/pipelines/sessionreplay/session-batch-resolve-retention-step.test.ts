import { AccumulationContext } from '~/ingestion/framework/accumulating-pipeline'
import { isOkResult } from '~/ingestion/framework/results'
import { RetentionService } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'

import { createResolveRetentionStep } from './session-batch-resolve-retention-step'
import { SessionBatchMetrics } from './sessions/metrics'
import { SessionBatchContext, SessionBatchRecorder } from './sessions/session-batch-recorder'

jest.mock('~/common/utils/logger', () => ({ logger: { warn: jest.fn() } }))
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
        mockRetentionService = { resolveSessionRetentions: jest.fn() } as unknown as jest.Mocked<RetentionService>
    })

    it('resolves retention for every pending session into the map', async () => {
        mockRetentionService.resolveSessionRetentions.mockResolvedValue([
            { resolved: true, retentionPeriod: '30d' },
            { resolved: true, retentionPeriod: '1y' },
        ])
        const step = createResolveRetentionStep(mockRetentionService)

        const result = await step(
            batchContextWith([
                { teamId: 1, sessionId: 'a' },
                { teamId: 2, sessionId: 'b' },
            ])
        )

        expect(isOkResult(result)).toBe(true)
        if (isOkResult(result)) {
            expect(result.value.retentionMap.get(1, 'a')).toBe('30d')
            expect(result.value.retentionMap.get(2, 'b')).toBe('1y')
            expect(result.value.retentionMap.size).toBe(2)
        }
        expect(SessionBatchMetrics.incrementSessionsDroppedDuringFlush).not.toHaveBeenCalled()
    })

    it('drops an unresolvable session and keeps the rest', async () => {
        mockRetentionService.resolveSessionRetentions.mockResolvedValue([
            { resolved: false },
            { resolved: true, retentionPeriod: '90d' },
        ])
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
            expect(result.value.retentionMap.get(999, 'gone')).toBeUndefined()
            expect(result.value.retentionMap.get(2, 'ok')).toBe('90d')
            expect(result.value.retentionMap.size).toBe(1)
        }
        expect(SessionBatchMetrics.incrementSessionsDroppedDuringFlush).toHaveBeenCalledTimes(1)
    })

    it('propagates a transient failure so the retry wrapper can retry the whole step', async () => {
        mockRetentionService.resolveSessionRetentions.mockRejectedValue(new Error('Redis connection lost'))
        const step = createResolveRetentionStep(mockRetentionService)

        await expect(step(batchContextWith([{ teamId: 1, sessionId: 'a' }]))).rejects.toThrow('Redis connection lost')
        expect(SessionBatchMetrics.incrementSessionsDroppedDuringFlush).not.toHaveBeenCalled()
    })
})
