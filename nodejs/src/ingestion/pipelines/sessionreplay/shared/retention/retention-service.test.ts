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

    describe('getRetentionByTeamId', () => {
        it('should return retention period for valid team id 1', async () => {
            const retentionPeriod = await retentionService.getRetentionByTeamId(1)
            expect(retentionPeriod).toEqual('30d')
        })

        it('should return retention period for valid team id 2', async () => {
            const retentionPeriod = await retentionService.getRetentionByTeamId(2)
            expect(retentionPeriod).toEqual('1y')
        })

        it('should return null for unknown team id', async () => {
            const retentionPeriod = await retentionService.getRetentionByTeamId(3)
            expect(retentionPeriod).toBeNull()
        })
    })

    describe('getSessionRetention', () => {
        it('resolves retention for valid team id 1', async () => {
            const resolution = await retentionService.getSessionRetention(1, '123')
            expect(resolution).toEqual({ resolved: true, retentionPeriod: '30d' })
        })

        it('resolves retention for valid team id 2', async () => {
            const resolution = await retentionService.getSessionRetention(2, '321')
            expect(resolution).toEqual({ resolved: true, retentionPeriod: '1y' })
        })

        it('is unresolved (not thrown) for unknown team id', async () => {
            const resolution = await retentionService.getSessionRetention(3, '456')
            expect(resolution).toEqual({ resolved: false })
        })

        it('is unresolved (not thrown) for an invalid retention period', async () => {
            const resolution = await retentionService.getSessionRetention(4, '654')
            expect(resolution).toEqual({ resolved: false })
        })

        it('does not cache an unresolvable lookup back to Redis', async () => {
            await retentionService.getSessionRetention(3, '456')
            expect(mockRedisClient.set).not.toHaveBeenCalled()
        })

        it('should load retention from Redis if key exists', async () => {
            mockRedisClient.get = jest.fn().mockReturnValue('30d')

            const resolution = await retentionService.getSessionRetention(1, '123')
            expect(resolution).toEqual({ resolved: true, retentionPeriod: '30d' })

            expect(mockRedisClient.get).toHaveBeenCalledTimes(1)
            expect(mockRedisClient.get).toHaveBeenCalledWith('@posthog/replay/session-retention-123')
        })

        it('should store retention in Redis if key does not exist', async () => {
            mockRedisClient.get = jest.fn().mockReturnValue(null)

            const resolution = await retentionService.getSessionRetention(1, '123')
            expect(resolution).toEqual({ resolved: true, retentionPeriod: '30d' })

            expect(mockRedisClient.set).toHaveBeenCalledTimes(1)
            expect(mockRedisClient.set).toHaveBeenCalledWith(
                '@posthog/replay/session-retention-123',
                '30d',
                'EX',
                24 * 60 * 60
            )
        })
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

    describe('metrics', () => {
        it('should increment lookup errors once for unknown team id', async () => {
            await retentionService.getSessionRetention(3, '456')
            expect(RetentionServiceMetrics.incrementLookupErrors).toHaveBeenCalledTimes(1)
        })

        it('should increment lookup errors once for invalid retention period', async () => {
            await retentionService.getSessionRetention(4, '654')
            expect(RetentionServiceMetrics.incrementLookupErrors).toHaveBeenCalledTimes(1)
        })
    })
})
