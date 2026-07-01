import { PipelineResultType, isOkResult } from '~/ingestion/framework/results'
import { createTestEventHeaders } from '~/tests/helpers/event-headers'
import { EventHeaders } from '~/types'

import { createValidateReplayHeadersStep } from './validate-headers-step'

describe('createValidateReplayHeadersStep', () => {
    const step = createValidateReplayHeadersStep()

    it('passes through and narrows headers when token and session_id are present', async () => {
        const step = createValidateReplayHeadersStep<{ marker: string; headers: EventHeaders }>()
        const input = {
            marker: 'preserved',
            headers: createTestEventHeaders({ token: 'tok', session_id: 'sess-1' }),
        }

        const result = await step(input)

        expect(isOkResult(result)).toBe(true)
        if (isOkResult(result)) {
            // Additive: every input property survives, headers still the same values.
            expect(result.value.marker).toBe('preserved')
            expect(result.value.headers.token).toBe('tok')
            expect(result.value.headers.session_id).toBe('sess-1')
        }
    })

    it('DLQs when the token header is missing (capture always sets it, so absence is a bug)', async () => {
        const result = await step({ headers: createTestEventHeaders({ session_id: 'sess-1' }) })

        expect(result.type).toBe(PipelineResultType.DLQ)
        if (result.type === PipelineResultType.DLQ) {
            expect(result.reason).toBe('no_token_in_header')
        }
    })

    it('DLQs when the session_id header is missing', async () => {
        const result = await step({ headers: createTestEventHeaders({ token: 'tok' }) })

        expect(result.type).toBe(PipelineResultType.DLQ)
        if (result.type === PipelineResultType.DLQ) {
            expect(result.reason).toBe('no_session_id_in_header')
        }
    })
})
