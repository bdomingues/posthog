import assert from 'node:assert/strict'
import { test } from 'node:test'

import { hashImageBytes, imageRef, isImageRef, parseImageRef, s3KeyForRef } from '../src/content-ref.ts'
import {
    type DedupStore,
    type ImageInput,
    type TopicMessage,
    type TopicProducer,
    emitImagesForScrub,
} from '../src/producer.ts'

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

test('reference is team-scoped: same bytes in different teams get different refs (tenant isolation)', () => {
    const bytes = Buffer.from('logo-png-bytes')
    const a = imageRef(42, hashImageBytes(bytes))
    const b = imageRef(99, hashImageBytes(bytes))
    assert.notEqual(a, b)
    assert.equal(parseImageRef(a)?.teamId, 42)
    assert.equal(parseImageRef(a)?.hash, parseImageRef(b)?.hash)
})

test('isImageRef accepts a reference and rejects a raw data URI', () => {
    assert.ok(isImageRef(imageRef(7, hashImageBytes(Buffer.from('x')))))
    assert.equal(isImageRef('data:image/png;base64,iVBORw0KG'), false)
})

test('s3 key shards by team then hash', () => {
    assert.match(
        s3KeyForRef(imageRef(42, hashImageBytes(Buffer.from('x')))),
        /^scrubbed-images\/team_id=42\/[A-Za-z0-9_-]{22}\.png$/
    )
})

test('a batch dedups + posts in exactly ONE redis round-trip and ONE produce', async () => {
    const dedup = new FakeDedup()
    const producer = new FakeProducer()
    const images = Array.from({ length: 8 }, (_, i) => img(42, `image-${i}`))

    const results = await emitImagesForScrub(images, { dedup, producer })

    assert.equal(dedup.reserveCalls, 1) // <- one call per batch, regardless of image count
    assert.equal(producer.produceCalls, 1)
    assert.equal(results.length, 8)
    assert.ok(results.every((r) => r.posted))
    assert.equal(producer.sent.length, 8)
})

test('duplicates within a batch and across batches are posted once', async () => {
    const dedup = new FakeDedup()
    const producer = new FakeProducer()
    const dup = img(42, 'same-image')

    const first = await emitImagesForScrub([dup, dup, img(42, 'other')], { dedup, producer })
    assert.deepEqual(
        first.map((r) => r.posted),
        [true, false, true]
    ) // 2nd is the in-batch duplicate
    assert.equal(producer.sent.length, 2)

    const second = await emitImagesForScrub([dup], { dedup, producer })
    assert.equal(second[0].posted, false) // already posted in the previous batch
    assert.equal(producer.sent.length, 2)
})

test('the same image in two teams is posted twice (separate dedup keys)', async () => {
    const dedup = new FakeDedup()
    const producer = new FakeProducer()
    const results = await emitImagesForScrub([img(42, 'shared'), img(99, 'shared')], { dedup, producer })
    assert.deepEqual(
        results.map((r) => r.posted),
        [true, true]
    )
    assert.equal(producer.sent.length, 2)
})

test('a produce failure releases the batch reservations so later sightings can retry', async () => {
    const dedup = new FakeDedup()
    const producer = new FakeProducer()
    producer.fail = true
    const images = [img(42, 'a'), img(42, 'b')]

    await assert.rejects(emitImagesForScrub(images, { dedup, producer }))
    assert.equal(dedup.releaseCalls, 1)
    assert.equal(dedup.keys.size, 0) // both reservations rolled back

    producer.fail = false
    const retry = await emitImagesForScrub(images, { dedup, producer })
    assert.ok(retry.every((r) => r.posted))
    assert.equal(producer.sent.length, 2)
})

test('an empty batch does no work', async () => {
    const dedup = new FakeDedup()
    const producer = new FakeProducer()
    assert.deepEqual(await emitImagesForScrub([], { dedup, producer }), [])
    assert.equal(dedup.reserveCalls, 0)
    assert.equal(producer.produceCalls, 0)
})
