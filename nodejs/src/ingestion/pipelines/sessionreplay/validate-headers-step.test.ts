import { PipelineResultType, isOkResult } from '~/ingestion/framework/results'
import { createTestEventHeaders } from '~/tests/helpers/event-headers'
import { EventHeaders } from '~/types'

import { createValidateSessionReplayHeadersStep } from './validate-headers-step'

describe('createValidateSessionReplayHeadersStep', () => {
    const step = createValidateSessionReplayHeadersStep()

    it('narrows headers to the guaranteed fields and drops the rest, preserving other input', async () => {
        const step = createValidateSessionReplayHeadersStep<{ marker: string; headers: EventHeaders }>()
        const input = {
            marker: 'preserved',
            headers: createTestEventHeaders({ token: 'tok', session_id: 'sess-1', distinct_id: 'user-1' }),
        }

        const result = await step(input)

        expect(isOkResult(result)).toBe(true)
        if (isOkResult(result)) {
            expect(result.value.marker).toBe('preserved')
            // Only the guaranteed replay headers survive — the wide EventHeaders fields are dropped.
            expect(result.value.headers).toEqual({ token: 'tok', session_id: 'sess-1', distinct_id: 'user-1' })
        }
    })

    it('DLQs when the token header is missing (capture always sets it, so absence is a bug)', async () => {
        const result = await step({ headers: createTestEventHeaders({ session_id: 'sess-1', distinct_id: 'user-1' }) })

        expect(result.type).toBe(PipelineResultType.DLQ)
        if (result.type === PipelineResultType.DLQ) {
            expect(result.reason).toBe('no_token_in_header')
        }
    })

    it('DLQs when the session_id header is missing', async () => {
        const result = await step({ headers: createTestEventHeaders({ token: 'tok', distinct_id: 'user-1' }) })

        expect(result.type).toBe(PipelineResultType.DLQ)
        if (result.type === PipelineResultType.DLQ) {
            expect(result.reason).toBe('no_session_id_in_header')
        }
    })

    it('DLQs when the distinct_id header is missing', async () => {
        const result = await step({ headers: createTestEventHeaders({ token: 'tok', session_id: 'sess-1' }) })

        expect(result.type).toBe(PipelineResultType.DLQ)
        if (result.type === PipelineResultType.DLQ) {
            expect(result.reason).toBe('no_distinct_id_in_header')
        }
    })
})
