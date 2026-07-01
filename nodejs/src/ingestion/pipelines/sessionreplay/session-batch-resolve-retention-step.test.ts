import { PipelineResultType, isOkResult } from '~/ingestion/framework/results'
import { ParsedMessageData } from '~/ingestion/pipelines/sessionreplay/kafka/types'
import { RetentionPeriod } from '~/ingestion/pipelines/sessionreplay/shared/constants'
import { RetentionService } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'
import { TeamForReplay } from '~/ingestion/pipelines/sessionreplay/teams/types'

import { SessionBatchContext } from './session-batch-context'
import { createResolveRetentionStep } from './session-batch-resolve-retention-step'
import { SessionBatchMetrics } from './sessions/metrics'

jest.mock('~/common/utils/logger', () => ({ logger: { warn: jest.fn() } }))
jest.mock('./sessions/metrics', () => ({
    SessionBatchMetrics: { incrementSessionsDroppedMissingRetention: jest.fn() },
}))

type Element = SessionBatchContext & { team: TeamForReplay; parsedMessage: ParsedMessageData }

describe('createResolveRetentionStep', () => {
    let mockRetentionService: jest.Mocked<RetentionService>

    // Minimal element carrying what the step reads: team id, session id, and the batch recorder
    // (whose getRetention reports what the batch already holds — undefined unless `heldRetention` set).
    const element = (teamId: number, sessionId: string, heldRetention?: RetentionPeriod): Element =>
        ({
            team: { teamId, consoleLogIngestionEnabled: false, aiTrainingOptedIn: true },
            parsedMessage: { session_id: sessionId },
            sessionBatchRecorder: { getRetention: jest.fn().mockReturnValue(heldRetention) },
        }) as unknown as Element

    beforeEach(() => {
        jest.clearAllMocks()
        mockRetentionService = { resolveSessionRetentions: jest.fn() } as unknown as jest.Mocked<RetentionService>
    })

    it('attaches the resolved retention to every session', async () => {
        mockRetentionService.resolveSessionRetentions.mockResolvedValue([
            { resolved: true, retentionPeriod: '30d' },
            { resolved: true, retentionPeriod: '1y' },
        ])
        const step = createResolveRetentionStep(mockRetentionService)

        const results = await step([element(1, 'a'), element(2, 'b')])

        expect(mockRetentionService.resolveSessionRetentions).toHaveBeenCalledWith([
            { teamId: 1, sessionId: 'a' },
            { teamId: 2, sessionId: 'b' },
        ])
        expect(results.map((r) => (isOkResult(r) ? r.value.retentionPeriod : null))).toEqual(['30d', '1y'])
        expect(SessionBatchMetrics.incrementSessionsDroppedMissingRetention).not.toHaveBeenCalled()
    })

    it('drops an unresolvable session and keeps the rest', async () => {
        mockRetentionService.resolveSessionRetentions.mockResolvedValue([
            { resolved: false },
            { resolved: true, retentionPeriod: '90d' },
        ])
        const step = createResolveRetentionStep(mockRetentionService)

        const results = await step([element(999, 'gone'), element(2, 'ok')])

        expect(results[0].type).toBe(PipelineResultType.DROP)
        expect(isOkResult(results[1]) ? results[1].value.retentionPeriod : null).toBe('90d')
        expect(SessionBatchMetrics.incrementSessionsDroppedMissingRetention).toHaveBeenCalledTimes(1)
    })

    it('reuses retention the batch already holds instead of re-resolving it', async () => {
        mockRetentionService.resolveSessionRetentions.mockResolvedValue([{ resolved: true, retentionPeriod: '30d' }])
        const step = createResolveRetentionStep(mockRetentionService)

        // 'held' is already in the batch (90d); only 'fresh' needs resolving.
        const results = await step([element(1, 'held', '90d'), element(2, 'fresh')])

        expect(mockRetentionService.resolveSessionRetentions).toHaveBeenCalledWith([{ teamId: 2, sessionId: 'fresh' }])
        expect(results.map((r) => (isOkResult(r) ? r.value.retentionPeriod : null))).toEqual(['90d', '30d'])
    })

    it('propagates a transient failure so the retry wrapper can retry the whole step', async () => {
        mockRetentionService.resolveSessionRetentions.mockRejectedValue(new Error('Redis connection lost'))
        const step = createResolveRetentionStep(mockRetentionService)

        await expect(step([element(1, 'a')])).rejects.toThrow('Redis connection lost')
        expect(SessionBatchMetrics.incrementSessionsDroppedMissingRetention).not.toHaveBeenCalled()
    })
})
