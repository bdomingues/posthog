import { AccumulationContext } from '~/ingestion/framework/accumulating-pipeline'
import { ok } from '~/ingestion/framework/results'
import { ProcessingStep } from '~/ingestion/framework/steps'
import { SessionBlockMetadata } from '~/ingestion/pipelines/sessionreplay/shared/metadata/session-block-metadata'

import { RetentionByKey } from './session-batch-resolve-retention-step'
import { SessionBatchContext } from './sessions/session-batch-factory'

/**
 * Flush step: write the accumulated batch to storage. Retention is already resolved (by the
 * resolve-retention step) and passed in the batch context; the consumer commits offsets on the
 * flushed result, so nothing here touches Kafka offsets.
 */
export function createWriteStep(): ProcessingStep<
    SessionBatchContext & AccumulationContext & { retentionByKey: RetentionByKey },
    SessionBlockMetadata[]
> {
    return async function writeStep(batchContext) {
        return ok(await batchContext.sessionBatchRecorder.flushToStorage(batchContext.retentionByKey))
    }
}
