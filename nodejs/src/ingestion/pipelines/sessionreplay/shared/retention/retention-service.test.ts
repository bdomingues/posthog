import { Redis } from 'ioredis'

import { TeamService } from '~/ingestion/pipelines/sessionreplay/shared/teams/team-service'
import { RedisPool, TeamId } from '~/types'

import { RetentionServiceMetrics } from './metrics'
import { RetentionService } from './retention-service'

jest.mock('./metrics', () => ({
    RetentionServiceMetrics: {
        incrementLookupErrors: jest.fn(),
    },
}))

jest.mock('~/ingestion/pipelines/sessionreplay/sessions/metrics', () => ({
    SessionBatchMetrics: {
        observeRetentionRedisLatency: jest.fn(),
    },
}))

describe('RetentionService', () => {
    let retentionService: RetentionService
    let mockRedisClient: jest.Mocked<Redis>
    let mockPipeline: { set: jest.Mock; exec: jest.Mock }
    let mockTeamService: jest.Mocked<TeamService>

    beforeEach(() => {
        jest.useFakeTimers()

        mockPipeline = { set: jest.fn().mockReturnThis(), exec: jest.fn().mockResolvedValue([]) }
        mockRedisClient = {
            get: jest.fn().mockResolvedValue(null),
            set: jest.fn(),
            mget: jest.fn().mockResolvedValue([]),
            pipeline: jest.fn().mockReturnValue(mockPipeline),
        } as unknown as jest.Mocked<Redis>

        const mockRedisPool = {
            acquire: jest.fn().mockReturnValue(mockRedisClient),
            release: jest.fn(),
        } as unknown as jest.Mocked<RedisPool>

        mockTeamService = {
            getRetentionPeriodByTeamId: jest.fn().mockImplementation((teamId: TeamId) => {
                return {
                    1: '30d', // Valid
                    2: '1y', // Valid
                    3: null, // Missing
                    4: 'foobar', //Invalid
                }[teamId]
            }),
        } as unknown as jest.Mocked<TeamService>

        retentionService = new RetentionService(mockRedisPool, mockTeamService)
    })

    afterEach(() => {
        jest.useRealTimers()
    })

    describe('resolveSessionRetentions', () => {
        it('returns [] without touching Redis for an empty batch', async () => {
            const results = await retentionService.resolveSessionRetentions([])
            expect(results).toEqual([])
            expect(mockRedisClient.mget).not.toHaveBeenCalled()
        })

        it('resolves cached hits in one MGET without hitting Postgres', async () => {
            mockRedisClient.mget = jest.fn().mockResolvedValue(['30d', '1y'])

            const results = await retentionService.resolveSessionRetentions([
                { teamId: 1, sessionId: 'a' },
                { teamId: 2, sessionId: 'b' },
            ])

            expect(results).toEqual([
                { resolved: true, retentionPeriod: '30d' },
                { resolved: true, retentionPeriod: '1y' },
            ])
            expect(mockRedisClient.mget).toHaveBeenCalledTimes(1)
            expect(mockRedisClient.mget).toHaveBeenCalledWith([
                '@posthog/replay/session-retention-a',
                '@posthog/replay/session-retention-b',
            ])
            expect(mockTeamService.getRetentionPeriodByTeamId).not.toHaveBeenCalled()
            expect(mockPipeline.set).not.toHaveBeenCalled()
        })

        it('falls back to Postgres for misses across teams, deduped per team, and caches each result', async () => {
            // sessions a and b share team 1 (→ 30d); session c is team 2 (→ 1y).
            mockRedisClient.mget = jest.fn().mockResolvedValue([null, null, null])

            const results = await retentionService.resolveSessionRetentions([
                { teamId: 1, sessionId: 'a' },
                { teamId: 1, sessionId: 'b' },
                { teamId: 2, sessionId: 'c' },
            ])

            // Each session gets its own team's value.
            expect(results).toEqual([
                { resolved: true, retentionPeriod: '30d' },
                { resolved: true, retentionPeriod: '30d' },
                { resolved: true, retentionPeriod: '1y' },
            ])
            // Three misses across two distinct teams → one Postgres lookup per team.
            expect(mockTeamService.getRetentionPeriodByTeamId).toHaveBeenCalledTimes(2)
            expect(mockTeamService.getRetentionPeriodByTeamId).toHaveBeenCalledWith(1)
            expect(mockTeamService.getRetentionPeriodByTeamId).toHaveBeenCalledWith(2)
            // Each resolved value is written back to its own key with a TTL.
            expect(mockPipeline.set).toHaveBeenCalledTimes(3)
            expect(mockPipeline.set).toHaveBeenCalledWith(
                '@posthog/replay/session-retention-a',
                '30d',
                'EX',
                24 * 60 * 60
            )
            expect(mockPipeline.set).toHaveBeenCalledWith(
                '@posthog/replay/session-retention-b',
                '30d',
                'EX',
                24 * 60 * 60
            )
            expect(mockPipeline.set).toHaveBeenCalledWith(
                '@posthog/replay/session-retention-c',
                '1y',
                'EX',
                24 * 60 * 60
            )
            expect(mockPipeline.exec).toHaveBeenCalledTimes(1)
        })

        it('marks a session unresolvable (not thrown) when its team has no retention', async () => {
            mockRedisClient.mget = jest.fn().mockResolvedValue([null])

            const results = await retentionService.resolveSessionRetentions([{ teamId: 3, sessionId: 'gone' }])

            expect(results).toEqual([{ resolved: false }])
            expect(mockPipeline.set).not.toHaveBeenCalled()
            expect(RetentionServiceMetrics.incrementLookupErrors).toHaveBeenCalledTimes(1)
        })

        it('marks a session unresolvable when the cached value is invalid', async () => {
            mockRedisClient.mget = jest.fn().mockResolvedValue(['foobar'])

            const results = await retentionService.resolveSessionRetentions([{ teamId: 1, sessionId: 'a' }])

            expect(results).toEqual([{ resolved: false }])
            expect(mockTeamService.getRetentionPeriodByTeamId).not.toHaveBeenCalled()
            expect(RetentionServiceMetrics.incrementLookupErrors).toHaveBeenCalledTimes(1)
        })

        it('keeps results aligned with input order for a mix of hits and misses', async () => {
            mockRedisClient.mget = jest.fn().mockResolvedValue(['1y', null])

            const results = await retentionService.resolveSessionRetentions([
                { teamId: 2, sessionId: 'cached' },
                { teamId: 1, sessionId: 'miss' },
            ])

            expect(results).toEqual([
                { resolved: true, retentionPeriod: '1y' },
                { resolved: true, retentionPeriod: '30d' },
            ])
        })
    })
})
