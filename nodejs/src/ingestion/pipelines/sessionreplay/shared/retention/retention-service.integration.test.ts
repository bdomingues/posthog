import { defaultConfig } from '~/common/config/config'
import { createIngestionRedisConnectionConfig } from '~/common/config/redis-pools'
import { PostgresRouter } from '~/common/utils/db/postgres'
import { createRedisPoolFromConfig } from '~/common/utils/db/redis'
import { SessionSet } from '~/ingestion/pipelines/sessionreplay/shared/session-map'
import { TeamService } from '~/ingestion/pipelines/sessionreplay/shared/teams/team-service'
import { getFirstTeam, resetTestDatabase } from '~/tests/helpers/sql'
import { RedisPool } from '~/types'

import { RetentionService } from './retention-service'

describe('RetentionService (integration)', () => {
    let redisPool: RedisPool
    let postgres: PostgresRouter
    let teamService: TeamService
    let teamId: number
    let getRetentionSpy: jest.SpyInstance

    beforeEach(async () => {
        await resetTestDatabase()
        postgres = new PostgresRouter(defaultConfig)
        teamId = (await getFirstTeam(postgres)).id // seeded with retention '30d'

        teamService = new TeamService(postgres)
        getRetentionSpy = jest.spyOn(teamService, 'getRetentionPeriodByTeamId')

        redisPool = createRedisPoolFromConfig({
            connection: createIngestionRedisConnectionConfig(defaultConfig),
            poolMinSize: 1,
            poolMaxSize: 3,
        })
    })

    afterEach(async () => {
        await redisPool.drain()
        await redisPool.clear()
        await postgres.end()
    })

    it('resolves from Postgres on a miss, then serves the second lookup from Redis', async () => {
        const sessionId = `it-hit-${Date.now()}` // unique so the first lookup is a real cache miss
        const service = new RetentionService(redisPool, teamService)

        const first = await service.resolveSessionRetentions(new SessionSet().add(teamId, sessionId))
        expect(first.get(teamId, sessionId)).toEqual({ resolved: true, retentionPeriod: '30d' })

        const second = await service.resolveSessionRetentions(new SessionSet().add(teamId, sessionId))
        expect(second.get(teamId, sessionId)).toEqual({ resolved: true, retentionPeriod: '30d' })

        // Postgres (via the team service) is consulted once; the second lookup is served from Redis.
        expect(getRetentionSpy).toHaveBeenCalledTimes(1)
    })

    it('marks a session unresolvable when the team has no retention, and does not cache the miss', async () => {
        const unknownTeamId = 9_999_999
        const sessionId = `it-null-${Date.now()}`
        const service = new RetentionService(redisPool, teamService)

        const first = await service.resolveSessionRetentions(new SessionSet().add(unknownTeamId, sessionId))
        expect(first.get(unknownTeamId, sessionId)).toEqual({ resolved: false })

        // A null retention is not written to Redis, so the second lookup consults Postgres again
        // rather than serving a stale miss from the cache.
        const second = await service.resolveSessionRetentions(new SessionSet().add(unknownTeamId, sessionId))
        expect(second.get(unknownTeamId, sessionId)).toEqual({ resolved: false })
        expect(getRetentionSpy).toHaveBeenCalledTimes(2)
    })
})
