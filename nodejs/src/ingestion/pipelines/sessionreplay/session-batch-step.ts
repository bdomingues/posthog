import { BeforeAccumulationInput, BeforeAccumulationOutput } from '~/ingestion/framework/accumulating-pipeline'
import { ok } from '~/ingestion/framework/results'
import { ProcessingStep } from '~/ingestion/framework/steps'

import { SessionBatchContext, SessionBatchFactory } from './sessions/session-batch-factory'

/**
 * The accumulating pipeline's beforeBatch step: mints a fresh recorder from the factory for the next
 * accumulation cycle and hands it to the pipeline as the batch context.
 */
export function createSessionBatchStep(
    sessionBatchFactory: SessionBatchFactory
): ProcessingStep<BeforeAccumulationInput, BeforeAccumulationOutput<SessionBatchContext>> {
    return function sessionBatchStep(input) {
        return Promise.resolve(
            ok({ batchContext: { sessionBatchRecorder: sessionBatchFactory.createBatch(), batchId: input.batchId } })
        )
    }
}
