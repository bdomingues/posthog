import { defaultConfig } from '~/common/config/config'
import { createIngestionRedisConnectionConfig } from '~/common/config/redis-pools'
import { createRedisPoolFromConfig } from '~/common/utils/db/redis'
import { SessionSet } from '~/ingestion/pipelines/sessionreplay/shared/session-map'
import { TeamService } from '~/ingestion/pipelines/sessionreplay/shared/teams/team-service'
import { RedisPool } from '~/types'

import { RetentionService } from './retention-service'

describe('RetentionService (integration)', () => {
    let redisPool: RedisPool

    beforeEach(() => {
        redisPool = createRedisPoolFromConfig({
            connection: createIngestionRedisConnectionConfig(defaultConfig),
            poolMinSize: 1,
            poolMaxSize: 3,
        })
    })

    afterEach(async () => {
        await redisPool.drain()
        await redisPool.clear()
    })

    it('round-trips a resolved retention through real Redis (write, then read back via mget)', async () => {
        const teamId = 1
        const sessionId = `it-retention-${Date.now()}` // unique so the first lookup is a real cache miss
        const mockTeamService = {
            getRetentionPeriodByTeamId: jest.fn().mockResolvedValue('1y'),
        } as unknown as jest.Mocked<TeamService>
        const service = new RetentionService(redisPool, mockTeamService)

        // Miss → resolved from (mocked) Postgres → written back to real Redis.
        const first = await service.resolveSessionRetentions(new SessionSet().add(teamId, sessionId))
        expect(first.get(teamId, sessionId)).toEqual({ resolved: true, retentionPeriod: '1y' })

        // Hit → the value must come back from real Redis via mget and deserialize; Postgres not consulted again.
        const second = await service.resolveSessionRetentions(new SessionSet().add(teamId, sessionId))
        expect(second.get(teamId, sessionId)).toEqual({ resolved: true, retentionPeriod: '1y' })
        expect(mockTeamService.getRetentionPeriodByTeamId).toHaveBeenCalledTimes(1)
    })
})
