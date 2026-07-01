import { PipelineResultType, isOkResult } from '~/ingestion/framework/results'
import { ParsedMessageData } from '~/ingestion/pipelines/sessionreplay/kafka/types'
import { RetentionService } from '~/ingestion/pipelines/sessionreplay/shared/retention/retention-service'
import { TeamForReplay } from '~/ingestion/pipelines/sessionreplay/teams/types'

import { createResolveRetentionStep } from './session-batch-resolve-retention-step'
import { SessionBatchMetrics } from './sessions/metrics'

jest.mock('~/common/utils/logger', () => ({ logger: { warn: jest.fn() } }))
jest.mock('./sessions/metrics', () => ({
    SessionBatchMetrics: { incrementSessionsDroppedMissingRetention: jest.fn() },
}))

describe('createResolveRetentionStep', () => {
    let mockRetentionService: jest.Mocked<RetentionService>

    // Minimal element carrying just what the step reads (team id + session id).
    const element = (teamId: number, sessionId: string): { team: TeamForReplay; parsedMessage: ParsedMessageData } =>
        ({
            team: { teamId, consoleLogIngestionEnabled: false, aiTrainingOptedIn: true },
            parsedMessage: { session_id: sessionId },
        }) as unknown as { team: TeamForReplay; parsedMessage: ParsedMessageData }

    beforeEach(() => {
        jest.clearAllMocks()
        mockRetentionService = { resolveSessionRetentions: jest.fn() } as unknown as jest.Mocked<RetentionService>
    })

    it('resolves the batch in one call and attaches retention to every session', async () => {
        mockRetentionService.resolveSessionRetentions.mockResolvedValue([
            { resolved: true, retentionPeriod: '30d' },
            { resolved: true, retentionPeriod: '1y' },
        ])
        const step = createResolveRetentionStep(mockRetentionService)

        const results = await step([element(1, 'a'), element(2, 'b')])

        expect(mockRetentionService.resolveSessionRetentions).toHaveBeenCalledTimes(1)
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

    it('propagates a transient failure so the retry wrapper can retry the whole step', async () => {
        mockRetentionService.resolveSessionRetentions.mockRejectedValue(new Error('Redis connection lost'))
        const step = createResolveRetentionStep(mockRetentionService)

        await expect(step([element(1, 'a')])).rejects.toThrow('Redis connection lost')
        expect(SessionBatchMetrics.incrementSessionsDroppedMissingRetention).not.toHaveBeenCalled()
    })
})
