import { dlq, ok } from '~/ingestion/framework/results'
import { ProcessingStep } from '~/ingestion/framework/steps'
import { EventHeaders } from '~/types'

/**
 * Session replay message headers, narrowed to the fields capture guarantees for the replay path and
 * that the pipeline relies on. Downstream replay steps take this instead of {@link EventHeaders} so
 * they can read `token`/`session_id` without re-checking for their presence.
 */
export type SessionReplayHeaders = EventHeaders & {
    token: string
    session_id: string
}

export interface ValidateReplayHeadersStepInput {
    headers: EventHeaders
}

/**
 * Validates that a session replay message carries the headers capture guarantees, and narrows the
 * header type so downstream steps can trust them.
 *
 * Capture's recordings handler rejects a snapshot before it reaches Kafka unless it has a token and a
 * valid `session_id` (see `rust/capture/src/events/recordings.rs`), so their absence here indicates a
 * bug upstream rather than bad user input — such messages are sent to the DLQ. Only the headers the
 * replay pipeline actually consumes are enforced; the rest of the guaranteed set is read from the
 * parsed payload, not the headers.
 */
export function createValidateReplayHeadersStep<T extends ValidateReplayHeadersStepInput>(): ProcessingStep<
    T,
    T & { headers: SessionReplayHeaders }
> {
    return async function validateReplayHeadersStep(input) {
        const { headers } = input

        if (!headers.token) {
            return dlq('no_token_in_header')
        }
        if (!headers.session_id) {
            return dlq('no_session_id_in_header')
        }

        return Promise.resolve(
            ok({ ...input, headers: { ...headers, token: headers.token, session_id: headers.session_id } })
        )
    }
}
