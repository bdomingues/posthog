import { defaultConfig } from '~/common/config/config'
import { PostgresRouter, PostgresUse } from '~/common/utils/db/postgres'
import { getFirstTeam, resetTestDatabase } from '~/tests/helpers/sql'

import { TeamService } from './team-service'

describe('TeamService (integration)', () => {
    let postgres: PostgresRouter
    let teamId: number
    let apiToken: string

    beforeEach(async () => {
        await resetTestDatabase()
        postgres = new PostgresRouter(defaultConfig)
        const team = await getFirstTeam(postgres)
        teamId = team.id
        apiToken = team.api_token
    })

    afterEach(async () => {
        await postgres.end()
    })

    it('deserializes the retention period and token from a real Postgres row', async () => {
        // A value distinct from the seeded default, so we know it's read from the actual column.
        await postgres.query(
            PostgresUse.COMMON_WRITE,
            `UPDATE posthog_team SET session_recording_retention_period = '90d' WHERE id = $1`,
            [teamId],
            'test-set-retention'
        )
        const teamService = new TeamService(postgres)

        expect(await teamService.getRetentionPeriodByTeamId(teamId)).toBe('90d')
        expect(await teamService.getTeamByToken(apiToken)).toMatchObject({ teamId })
    })
})
