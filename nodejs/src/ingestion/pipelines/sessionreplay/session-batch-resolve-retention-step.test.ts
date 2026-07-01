import { PipelineResultType, isOkResult } from '~/ingestion/framework/results'
import { RetentionService } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'
import { TeamForReplay } from '~/ingestion/pipelines/sessionreplay/teams/types'

import { createResolveRetentionStep } from './session-batch-resolve-retention-step'
import { SessionBatchMetrics } from './sessions/metrics'
import { SessionBatchManager } from './sessions/session-batch-manager'
import { SessionBatchRecorder } from './sessions/session-batch-recorder'
import { SessionReplayHeaders } from './validate-headers-step'

jest.mock('~/common/utils/logger', () => ({ logger: { warn: jest.fn() } }))
jest.mock('./sessions/metrics', () => ({
    SessionBatchMetrics: { incrementSessionsDroppedMissingRetention: jest.fn() },
}))

describe('createResolveRetentionStep', () => {
    let mockRetentionService: jest.Mocked<RetentionService>
    let mockBatch: jest.Mocked<Pick<SessionBatchRecorder, 'getRetention'>>
    let mockSessionBatchManager: jest.Mocked<Pick<SessionBatchManager, 'getCurrentBatch'>>

    // Minimal element carrying just what the step reads (team id + session_id header).
    const element = (teamId: number, sessionId: string): { team: TeamForReplay; headers: SessionReplayHeaders } =>
        ({
            team: { teamId, consoleLogIngestionEnabled: false, aiTrainingOptedIn: true },
            headers: { token: 'token', session_id: sessionId, distinct_id: 'distinct-1' },
        }) as unknown as { team: TeamForReplay; headers: SessionReplayHeaders }

    const createStep = () =>
        createResolveRetentionStep(mockRetentionService, mockSessionBatchManager as unknown as SessionBatchManager)

    beforeEach(() => {
        jest.clearAllMocks()
        mockRetentionService = {
            resolveSessionRetentions: jest.fn().mockResolvedValue([]),
        } as unknown as jest.Mocked<RetentionService>
        // Default: no session is already in the batch, so everything is resolved via the service.
        mockBatch = { getRetention: jest.fn().mockReturnValue(undefined) } as unknown as jest.Mocked<
            Pick<SessionBatchRecorder, 'getRetention'>
        >
        mockSessionBatchManager = { getCurrentBatch: jest.fn().mockReturnValue(mockBatch) } as unknown as jest.Mocked<
            Pick<SessionBatchManager, 'getCurrentBatch'>
        >
    })

    it('resolves the batch in one call (keyed on the session_id header) and attaches retention', async () => {
        mockRetentionService.resolveSessionRetentions.mockResolvedValue([
            { resolved: true, retentionPeriod: '30d' },
            { resolved: true, retentionPeriod: '1y' },
        ])
        const step = createStep()

        const results = await step([element(1, 'a'), element(2, 'b')])

        expect(mockRetentionService.resolveSessionRetentions).toHaveBeenCalledWith([
            { teamId: 1, sessionId: 'a' },
            { teamId: 2, sessionId: 'b' },
        ])
        expect(results.map((r) => (isOkResult(r) ? r.value.retentionPeriod : null))).toEqual(['30d', '1y'])
        expect(SessionBatchMetrics.incrementSessionsDroppedMissingRetention).not.toHaveBeenCalled()
    })

    it('reuses retention already held in the batch and only resolves the unseen sessions', async () => {
        // Session 'a' (team 1) is already in the batch; 'b' (team 2) is not.
        mockBatch.getRetention.mockImplementation((teamId: number) => (teamId === 1 ? '90d' : undefined))
        mockRetentionService.resolveSessionRetentions.mockResolvedValue([{ resolved: true, retentionPeriod: '1y' }])
        const step = createStep()

        const results = await step([element(1, 'a'), element(2, 'b')])

        // Only the unseen session is sent to the service.
        expect(mockRetentionService.resolveSessionRetentions).toHaveBeenCalledWith([{ teamId: 2, sessionId: 'b' }])
        expect(results.map((r) => (isOkResult(r) ? r.value.retentionPeriod : null))).toEqual(['90d', '1y'])
    })

    it('skips the service entirely when every session is already in the batch', async () => {
        mockBatch.getRetention.mockReturnValue('30d')
        const step = createStep()

        const results = await step([element(1, 'a'), element(2, 'b')])

        expect(mockRetentionService.resolveSessionRetentions).toHaveBeenCalledWith([])
        expect(results.map((r) => (isOkResult(r) ? r.value.retentionPeriod : null))).toEqual(['30d', '30d'])
    })

    it('drops an unresolvable session and keeps the rest', async () => {
        mockRetentionService.resolveSessionRetentions.mockResolvedValue([
            { resolved: false },
            { resolved: true, retentionPeriod: '90d' },
        ])
        const step = createStep()

        const results = await step([element(999, 'gone'), element(2, 'ok')])

        expect(results[0].type).toBe(PipelineResultType.DROP)
        expect(isOkResult(results[1]) ? results[1].value.retentionPeriod : null).toBe('90d')
        expect(SessionBatchMetrics.incrementSessionsDroppedMissingRetention).toHaveBeenCalledTimes(1)
    })

    it('propagates a transient failure so the retry wrapper can retry the whole step', async () => {
        mockRetentionService.resolveSessionRetentions.mockRejectedValue(new Error('Redis connection lost'))
        const step = createStep()

        await expect(step([element(1, 'a')])).rejects.toThrow('Redis connection lost')
        expect(SessionBatchMetrics.incrementSessionsDroppedMissingRetention).not.toHaveBeenCalled()
    })
})
