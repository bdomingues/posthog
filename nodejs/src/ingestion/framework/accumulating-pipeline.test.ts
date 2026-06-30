import { AccumulatingPipeline, BeforeAccumulationInput, BeforeAccumulationOutput } from './accumulating-pipeline'
import { BatchPipeline, BatchPipelineResultWithContext, OkResultWithContext } from './batch-pipeline.interface'
import { createOkContext } from './helpers'
import { Pipeline } from './pipeline.interface'
import { isOkResult, ok } from './results'

type RecordIn = { id: number }
// The batch context carries its own accumulator array, like SessionBatchRecorder.
type Batch = { records: number[] }

function feedBatch(ids: number[]): OkResultWithContext<RecordIn, Record<string, never>>[] {
    return ids.map((id) => createOkContext({ id }, {}))
}

// Folds each fed element's id into the accumulator carried on its (tagged) value, then re-emits
// the element as a record-phase ack. Mirrors the session-replay record step folding into the manager.
class FoldingRecordPipeline
    implements BatchPipeline<RecordIn & Batch, RecordIn, Record<string, never>, Record<string, never>>
{
    private buffer: OkResultWithContext<RecordIn & Batch, Record<string, never>>[] = []

    feed(elements: OkResultWithContext<RecordIn & Batch, Record<string, never>>[]): void {
        this.buffer.push(...elements)
    }

    next(): Promise<BatchPipelineResultWithContext<RecordIn, Record<string, never>> | null> {
        if (this.buffer.length === 0) {
            return Promise.resolve(null)
        }
        const out = this.buffer
        this.buffer = []
        for (const element of out) {
            if (isOkResult(element.result)) {
                element.result.value.records.push(element.result.value.id)
            }
        }
        return Promise.resolve(
            out.map((element) => ({ result: ok({ id: element.result.value.id }), context: element.context }))
        )
    }
}

// Passthrough flush pipeline: emits one result per drained unit.
class PassthroughFlushPipeline
    implements BatchPipeline<{ id: number }, { id: number }, Record<string, never>, Record<string, never>>
{
    private buffer: OkResultWithContext<{ id: number }, Record<string, never>>[] = []

    feed(elements: OkResultWithContext<{ id: number }, Record<string, never>>[]): void {
        this.buffer.push(...elements)
    }

    next(): Promise<BatchPipelineResultWithContext<{ id: number }, Record<string, never>> | null> {
        if (this.buffer.length === 0) {
            return Promise.resolve(null)
        }
        const out = this.buffer
        this.buffer = []
        return Promise.resolve(out)
    }
}

describe('AccumulatingPipeline', () => {
    let beforeBatch: jest.Mock
    let flushPipeline: PassthroughFlushPipeline

    function createPipeline(options: { flushAt: number; maxBatchAgeMs?: number }) {
        beforeBatch = jest.fn(
            (
                input: OkResultWithContext<BeforeAccumulationInput, Record<string, never>>
            ): Promise<{ result: any; context: any }> =>
                Promise.resolve(
                    createOkContext<BeforeAccumulationOutput<Batch>>(
                        { batchContext: { records: [], batchId: input.result.value.batchId } },
                        {}
                    )
                )
        )
        const beforePipeline = { process: beforeBatch } as unknown as Pipeline<
            BeforeAccumulationInput,
            BeforeAccumulationOutput<Batch>,
            Record<string, never>
        >
        flushPipeline = new PassthroughFlushPipeline()

        return new AccumulatingPipeline<
            RecordIn,
            RecordIn,
            Record<string, never>,
            Record<string, never>,
            Batch,
            { id: number },
            Record<string, never>,
            { id: number },
            Record<string, never>
        >(
            new FoldingRecordPipeline(),
            beforePipeline,
            (batchContext) => {
                const units = batchContext.records.map((id) => createOkContext({ id }, {}))
                batchContext.records.length = 0
                return units
            },
            flushPipeline,
            {
                shouldFlush: (batchContext) => batchContext.records.length >= options.flushAt,
                maxBatchAgeMs: options.maxBatchAgeMs ?? 60_000,
            }
        )
    }

    async function drainNext(pipeline: ReturnType<typeof createPipeline>) {
        return await pipeline.next()
    }

    it('accumulates across feeds and does not flush under the size threshold', async () => {
        const pipeline = createPipeline({ flushAt: 5 })

        await pipeline.feed(feedBatch([1, 2]))
        const recorded = await drainNext(pipeline)
        await pipeline.feed(feedBatch([3]))
        await drainNext(pipeline)

        expect(recorded).toEqual({ flushed: false, elements: expect.any(Array) })
        // record drained, accumulator under threshold → next() finds nothing to flush
        expect(await drainNext(pipeline)).toBeNull()
        // beforeBatch ran exactly once (no flush → no re-mint)
        expect(beforeBatch).toHaveBeenCalledTimes(1)
    })

    it('flushes on the size trigger and re-mints the accumulator', async () => {
        const pipeline = createPipeline({ flushAt: 3 })

        await pipeline.feed(feedBatch([1, 2, 3]))
        const recorded = await drainNext(pipeline)
        expect(recorded).toMatchObject({ flushed: false })

        const flushed = await drainNext(pipeline)
        expect(flushed).toMatchObject({ flushed: true })
        expect(flushed!.elements.map((e) => isOkResult(e.result) && e.result.value)).toEqual([
            { id: 1 },
            { id: 2 },
            { id: 3 },
        ])
        // re-mint: beforeBatch ran for the initial cycle and again after the flush
        expect(beforeBatch).toHaveBeenCalledTimes(2)
    })

    it('emits record results first, then the flush on the next call', async () => {
        const pipeline = createPipeline({ flushAt: 2 })

        await pipeline.feed(feedBatch([1, 2]))

        const first = await drainNext(pipeline)
        expect(first).toMatchObject({ flushed: false })

        const second = await drainNext(pipeline)
        expect(second).toMatchObject({ flushed: true })

        expect(await drainNext(pipeline)).toBeNull()
    })

    it('returns null when there is nothing to record and nothing to flush', async () => {
        const pipeline = createPipeline({ flushAt: 5 })
        expect(await drainNext(pipeline)).toBeNull()
        expect(beforeBatch).not.toHaveBeenCalled()
    })

    it('flush() drains buffered records and flushes immediately, below the size threshold', async () => {
        const pipeline = createPipeline({ flushAt: 100 })

        await pipeline.feed(feedBatch([1, 2]))
        // No next() drain: flush() must fold the buffered records in itself before flushing.
        const flushed = await pipeline.flush()

        expect(flushed).toMatchObject({ flushed: true })
        expect(flushed!.elements).toHaveLength(2)
        // re-minted, so a subsequent flush with nothing accumulated is a no-op
        expect(await pipeline.flush()).toBeNull()
    })

    describe('age timer', () => {
        beforeEach(() => jest.useFakeTimers())
        afterEach(() => jest.useRealTimers())

        it('flushes a buffered batch once the age elapses with no further feeds (idle topic)', async () => {
            const pipeline = createPipeline({ flushAt: 100, maxBatchAgeMs: 1000 })
            pipeline.start()

            await pipeline.feed(feedBatch([1]))
            await drainNext(pipeline) // drain the record ack; under size threshold, no flush yet
            expect(await drainNext(pipeline)).toBeNull()

            jest.advanceTimersByTime(1000)

            const flushed = await drainNext(pipeline)
            expect(flushed).toMatchObject({ flushed: true })
            expect(flushed!.elements).toHaveLength(1)

            await pipeline.stop()
        })

        it('re-arms the timer on flush so age is measured from the last flush', async () => {
            const pipeline = createPipeline({ flushAt: 1, maxBatchAgeMs: 1000 })
            pipeline.start()

            // size flush at the first element
            await pipeline.feed(feedBatch([1]))
            await drainNext(pipeline)
            const sizeFlush = await drainNext(pipeline)
            expect(sizeFlush).toMatchObject({ flushed: true })

            // less than maxBatchAgeMs since that flush, empty accumulator → no age flush
            jest.advanceTimersByTime(999)
            expect(await drainNext(pipeline)).toBeNull()

            await pipeline.stop()
        })

        it('waitForActivity resolves when the timer fires', async () => {
            const pipeline = createPipeline({ flushAt: 100, maxBatchAgeMs: 1000 })
            pipeline.start()

            let woke = false
            const activity = pipeline.waitForActivity().then(() => {
                woke = true
            })

            jest.advanceTimersByTime(1000)
            await activity
            expect(woke).toBe(true)

            await pipeline.stop()
        })
    })

    describe('stop', () => {
        it('performs a final flush of the accumulated batch', async () => {
            const pipeline = createPipeline({ flushAt: 100 })

            await pipeline.feed(feedBatch([1, 2]))
            await drainNext(pipeline) // fold records into accumulator; under threshold

            const flushed = await pipeline.stop()
            expect(flushed).toMatchObject({ flushed: true })
            expect(flushed!.elements).toHaveLength(2)
        })

        it('returns null when there is nothing accumulated', async () => {
            const pipeline = createPipeline({ flushAt: 100 })
            expect(await pipeline.stop()).toBeNull()
        })
    })

    it('throws when beforeBatch returns a non-ok result', async () => {
        beforeBatch = jest.fn(() => Promise.resolve({ result: { type: 99 }, context: {} }))
        const beforePipeline = { process: beforeBatch } as unknown as Pipeline<
            BeforeAccumulationInput,
            BeforeAccumulationOutput<Batch>,
            Record<string, never>
        >
        const pipeline = new AccumulatingPipeline<
            RecordIn,
            RecordIn,
            Record<string, never>,
            Record<string, never>,
            Batch,
            { id: number },
            Record<string, never>,
            { id: number },
            Record<string, never>
        >(
            new FoldingRecordPipeline(),
            beforePipeline,
            (batchContext) => batchContext.records.map((id) => createOkContext({ id }, {})),
            new PassthroughFlushPipeline(),
            { shouldFlush: () => false, maxBatchAgeMs: 60_000 }
        )

        await expect(pipeline.feed(feedBatch([1]))).rejects.toThrow('beforeBatch returned non-ok result')
    })
})
