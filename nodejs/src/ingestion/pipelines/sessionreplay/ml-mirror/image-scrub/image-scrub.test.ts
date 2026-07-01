import { hashImageBytes, imageRef, isImageRef } from './content-ref'
import { type DedupStore, type ImageInput, type TopicMessage, type TopicProducer, emitImagesForScrub } from './producer'
import { routeImage } from './routing'

/** In-memory dedup that counts round-trips, so we can assert one call per batch. Mirrors the ordered
 *  semantics of a Redis SET NX pipeline: within one batch the first occurrence of a key is fresh. */
class FakeDedup implements DedupStore {
    keys = new Map<string, number>()
    reserveCalls = 0
    releaseCalls = 0
    reserveBatch(keys: string[], ttl: number): Promise<boolean[]> {
        this.reserveCalls++
        return Promise.resolve(
            keys.map((k) => {
                if (this.keys.has(k)) {
                    return false
                }
                this.keys.set(k, ttl)
                return true
            })
        )
    }
    releaseBatch(keys: string[]): Promise<void> {
        this.releaseCalls++
        keys.forEach((k) => this.keys.delete(k))
        return Promise.resolve()
    }
}

class FakeProducer implements TopicProducer {
    sent: TopicMessage[] = []
    produceCalls = 0
    fail = false
    produceBatch(messages: TopicMessage[]): Promise<void> {
        this.produceCalls++
        if (this.fail) {
            return Promise.reject(new Error('broker down'))
        }
        this.sent.push(...messages)
        return Promise.resolve()
    }
}

const img = (teamId: number, s: string): ImageInput => ({ teamId, bytes: Buffer.from(s) })

describe('ml-mirror/image-scrub', () => {
    // CONTRACT: these golden vectors MUST match the consumer's content-ref (its dev/content-ref.test.ts
    // asserts the same input -> hash/ref/s3-key), or references won't resolve. Change both together.
    describe('content-ref contract (golden vectors)', () => {
        const INPUT = 'posthog-image-scrub-contract-v1'
        const HASH = 'q1YIODUgcFH6CgV1DOI4SU'

        it('hashes to the golden 22-char base64url content hash', () => {
            expect(hashImageBytes(Buffer.from(INPUT))).toBe(HASH)
        })

        it('builds the golden team-scoped reference', () => {
            expect(imageRef(42, HASH)).toBe(`image:42:${HASH}`)
        })

        it('is team-scoped: same bytes in different teams get different refs (tenant isolation)', () => {
            const bytes = Buffer.from('logo-png-bytes')
            expect(imageRef(42, hashImageBytes(bytes))).not.toBe(imageRef(99, hashImageBytes(bytes)))
        })

        it('accepts a reference and rejects a raw data URI', () => {
            expect(isImageRef(imageRef(7, hashImageBytes(Buffer.from('x'))))).toBe(true)
            expect(isImageRef('data:image/png;base64,iVBORw0KG')).toBe(false)
        })
    })

    describe('routeImage', () => {
        it('passes tiny images through, for any source', () => {
            expect(routeImage({ source: 'img', width: 16, height: 8, byteLength: 200 })).toBe('passthrough')
            expect(routeImage({ source: 'canvas', width: 10, height: 10, byteLength: 200 })).toBe('passthrough')
        })

        it('routes canvas to the cheap in-process blur (dynamic, no dedup)', () => {
            expect(routeImage({ source: 'canvas', width: 800, height: 600, byteLength: 5000 })).toBe('cheap')
        })

        it('falls back to cheap when too big for the topic', () => {
            expect(routeImage({ source: 'img', width: 4000, height: 4000, byteLength: 2_000_000 })).toBe('cheap')
        })

        it('routes static <img>/media raster to the advanced topic path', () => {
            expect(routeImage({ source: 'img', width: 300, height: 300, byteLength: 5000 })).toBe('advanced')
            expect(routeImage({ source: 'media', width: 300, height: 300, byteLength: 5000 })).toBe('advanced')
        })

        it('scrubs unknown-size images rather than passing them through', () => {
            expect(routeImage({ source: 'img', byteLength: 5000 })).toBe('advanced')
        })
    })

    describe('emitImagesForScrub', () => {
        it('dedups + posts in exactly ONE redis round-trip and ONE produce', async () => {
            const dedup = new FakeDedup()
            const producer = new FakeProducer()
            const images = Array.from({ length: 8 }, (_, i) => img(42, `image-${i}`))

            const results = await emitImagesForScrub(images, { dedup, producer })

            expect(dedup.reserveCalls).toBe(1) // one call per batch, regardless of image count
            expect(producer.produceCalls).toBe(1)
            expect(results).toHaveLength(8)
            expect(results.every((r) => r.posted)).toBe(true)
            expect(producer.sent).toHaveLength(8)
        })

        it('posts duplicates within a batch and across batches only once', async () => {
            const dedup = new FakeDedup()
            const producer = new FakeProducer()
            const dup = img(42, 'same-image')

            const first = await emitImagesForScrub([dup, dup, img(42, 'other')], { dedup, producer })
            expect(first.map((r) => r.posted)).toEqual([true, false, true]) // 2nd is the in-batch duplicate
            expect(producer.sent).toHaveLength(2)

            const second = await emitImagesForScrub([dup], { dedup, producer })
            expect(second[0].posted).toBe(false) // already posted in the previous batch
            expect(producer.sent).toHaveLength(2)
        })

        it('posts the same image in two teams twice (separate dedup keys)', async () => {
            const dedup = new FakeDedup()
            const producer = new FakeProducer()
            const results = await emitImagesForScrub([img(42, 'shared'), img(99, 'shared')], { dedup, producer })
            expect(results.map((r) => r.posted)).toEqual([true, true])
            expect(producer.sent).toHaveLength(2)
        })

        it('releases the batch reservations on a produce failure so later sightings can retry', async () => {
            const dedup = new FakeDedup()
            const producer = new FakeProducer()
            producer.fail = true
            const images = [img(42, 'a'), img(42, 'b')]

            await expect(emitImagesForScrub(images, { dedup, producer })).rejects.toThrow()
            expect(dedup.releaseCalls).toBe(1)
            expect(dedup.keys.size).toBe(0) // both reservations rolled back

            producer.fail = false
            const retry = await emitImagesForScrub(images, { dedup, producer })
            expect(retry.every((r) => r.posted)).toBe(true)
            expect(producer.sent).toHaveLength(2)
        })

        it('does no work for an empty batch', async () => {
            const dedup = new FakeDedup()
            const producer = new FakeProducer()
            expect(await emitImagesForScrub([], { dedup, producer })).toEqual([])
            expect(dedup.reserveCalls).toBe(0)
            expect(producer.produceCalls).toBe(0)
        })
    })
})
