import { ok } from '~/ingestion/framework/results'
import { ProcessingStep } from '~/ingestion/framework/steps'
import { SessionBlockMetadata } from '~/ingestion/pipelines/sessionreplay/shared/metadata/session-block-metadata'

import { RetentionByKey } from './session-batch-resolve-retention-step'
import { SessionBatchContext } from './sessions/session-batch-factory'

/**
 * Flush step: write the accumulated batch to storage. Retention is already resolved (by the
 * resolve-retention step) and passed in the batch context; the consumer commits offsets on the
 * flushed result, so nothing here touches Kafka offsets.
 *
 * Terminal transform (produces block metadata, not an extended context), but its input is generic
 * so it only requires the fields it reads — the recorder and the resolved retention map.
 */
export function createWriteStep<T extends SessionBatchContext & { retentionByKey: RetentionByKey }>(): ProcessingStep<
    T,
    SessionBlockMetadata[]
> {
    return async function writeStep(batchContext) {
        return ok(await batchContext.sessionBatchRecorder.flushToStorage(batchContext.retentionByKey))
    }
}
