import { ok } from '~/ingestion/framework/results'
import { ProcessingStep } from '~/ingestion/framework/steps'
import { SessionBlockMetadata } from '~/ingestion/pipelines/sessionreplay/shared/metadata/session-block-metadata'

import { SessionBatchContext } from './sessions/session-batch-recorder'
import { RetentionMap } from './shared/retention/retention-map'

/**
 * Flush step: write the accumulated batch to storage. Retention is already resolved (by the
 * resolve-retention step) and passed in the batch context; a later flush step commits offsets on
 * the written result, so nothing here touches Kafka offsets.
 *
 * Terminal transform (produces block metadata, not an extended context), but its input is generic
 * so it only requires the fields it reads — the recorder and the resolved retention map.
 */
export function createWriteStep<T extends SessionBatchContext & { retentionMap: RetentionMap }>(): ProcessingStep<
    T,
    SessionBlockMetadata[]
> {
    return async function writeStep(batchContext) {
        return ok(await batchContext.sessionBatchRecorder.flushToStorage(batchContext.retentionMap))
    }
}
