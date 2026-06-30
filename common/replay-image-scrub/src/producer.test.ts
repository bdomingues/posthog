import assert from 'node:assert/strict'
import { test } from 'node:test'

import { hashImageBytes, imageRef, isImageRef, parseImageRef, s3KeyForRef } from './content-ref.ts'
import { type DedupStore, type TopicProducer, emitImageForScrub } from './producer.ts'

class FakeDedup implements DedupStore {
    keys = new Map<string, number>()
    reserve(key: string, ttl: number): Promise<boolean> {
        if (this.keys.has(key)) {
            return Promise.resolve(false)
        }
        this.keys.set(key, ttl)
        return Promise.resolve(true)
    }
    release(key: string): Promise<void> {
        this.keys.delete(key)
        return Promise.resolve()
    }
}

class FakeProducer implements TopicProducer {
    sent: { key: string; bytes: number }[] = []
    fail = false
    produce(key: string, value: Buffer): Promise<void> {
        if (this.fail) {
            return Promise.reject(new Error('broker down'))
        }
        this.sent.push({ key, bytes: value.length })
        return Promise.resolve()
    }
}

const img = (s: string): Buffer => Buffer.from(s)

test('reference is team-scoped: same bytes in different teams get different refs (tenant isolation)', () => {
    const bytes = img('logo-png-bytes')
    const a = imageRef(42, hashImageBytes(bytes))
    const b = imageRef(99, hashImageBytes(bytes))
    assert.notEqual(a, b)
    assert.equal(parseImageRef(a)?.teamId, 42)
    assert.equal(parseImageRef(a)?.hash, parseImageRef(b)?.hash) // same content hash, different team
})

test('isImageRef accepts a reference and rejects a raw data URI', () => {
    assert.ok(isImageRef(imageRef(7, hashImageBytes(img('x')))))
    assert.equal(isImageRef('data:image/png;base64,iVBORw0KG'), false)
})

test('s3 key shards by team then hash', () => {
    const ref = imageRef(42, hashImageBytes(img('x')))
    assert.match(s3KeyForRef(ref), /^scrubbed-images\/team_id=42\/[A-Za-z0-9_-]{22}\.png$/)
})

test('first sighting posts the raw image; the same image is then deduped (not re-posted)', async () => {
    const dedup = new FakeDedup()
    const producer = new FakeProducer()
    const bytes = img('the-same-image')

    const first = await emitImageForScrub(42, bytes, { dedup, producer })
    assert.equal(first.posted, true)
    assert.equal(producer.sent.length, 1)
    assert.equal(producer.sent[0].key, first.ref)

    const second = await emitImageForScrub(42, bytes, { dedup, producer })
    assert.equal(second.posted, false)
    assert.equal(second.ref, first.ref)
    assert.equal(producer.sent.length, 1) // still 1 — dedup suppressed the re-post
})

test('the same image in two teams is posted twice (separate dedup keys)', async () => {
    const dedup = new FakeDedup()
    const producer = new FakeProducer()
    const bytes = img('shared-asset')
    const a = await emitImageForScrub(42, bytes, { dedup, producer })
    const b = await emitImageForScrub(99, bytes, { dedup, producer })
    assert.equal(a.posted, true)
    assert.equal(b.posted, true)
    assert.equal(producer.sent.length, 2)
})

test('a produce failure releases the reservation so a later sighting can retry', async () => {
    const dedup = new FakeDedup()
    const producer = new FakeProducer()
    producer.fail = true
    const bytes = img('flaky')

    await assert.rejects(emitImageForScrub(42, bytes, { dedup, producer }))
    const ref = imageRef(42, hashImageBytes(bytes))
    assert.equal(dedup.keys.has(ref), false) // reservation rolled back

    producer.fail = false
    const retry = await emitImageForScrub(42, bytes, { dedup, producer })
    assert.equal(retry.posted, true) // not stuck deduped after the earlier failure
    assert.equal(producer.sent.length, 1)
})
