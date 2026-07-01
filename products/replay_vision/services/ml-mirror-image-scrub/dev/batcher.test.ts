import assert from 'node:assert/strict'
import { test } from 'node:test'

import { ImageBatcher } from '../src/batcher.ts'
import type { ImageShardStore, ScrubbedImage } from '../src/shard-store.ts'

/** Records writeTeam calls and can be told to fail, standing in for the real S3-backed store. */
class FakeStore {
    writes: Array<{ teamId: number; hashes: string[] }> = []
    fail = false
    writeTeam(teamId: number, images: ScrubbedImage[]): Promise<{ shard: string; bytes: number }> {
        if (this.fail) {
            return Promise.reject(new Error('s3 down'))
        }
        this.writes.push({ teamId, hashes: images.map((i) => i.hash) })
        return Promise.resolve({ shard: `shard-${teamId}`, bytes: images.reduce((n, i) => n + i.bytes.length, 0) })
    }
}

const img = (teamId: number, hash: string, bytes = 10): ScrubbedImage => ({
    teamId,
    hash,
    bytes: Buffer.alloc(bytes),
})

function batcher(
    store: FakeStore,
    overrides: Partial<{ maxImages: number; maxBytes: number; intervalMs: number }> = {}
): ImageBatcher {
    return new ImageBatcher(
        store as unknown as ImageShardStore,
        {
            maxImages: overrides.maxImages ?? 1000,
            maxBytes: overrides.maxBytes ?? 1e9,
            flushIntervalMs: overrides.intervalMs ?? 30_000,
        },
        0
    )
}

test('flushes on the image-count threshold and groups by team into one shard each', async () => {
    const store = new FakeStore()
    const b = batcher(store, { maxImages: 3 })

    b.add(img(42, 'a'))
    b.add(img(99, 'b'))
    assert.equal(b.shouldFlush(0), false) // 2 < 3
    b.add(img(42, 'c'))
    assert.equal(b.shouldFlush(0), true) // 3 >= 3

    await b.flush(0)
    assert.equal(store.writes.length, 2) // one shard per team
    assert.deepEqual(store.writes.find((w) => w.teamId === 42)?.hashes, ['a', 'c'])
    assert.deepEqual(store.writes.find((w) => w.teamId === 99)?.hashes, ['b'])
    assert.equal(b.size, 0)
})

test('flushes on the byte threshold', () => {
    const store = new FakeStore()
    const b = batcher(store, { maxBytes: 100 })
    b.add(img(1, 'a', 60))
    assert.equal(b.shouldFlush(0), false)
    b.add(img(1, 'b', 60))
    assert.equal(b.shouldFlush(0), true) // 120 >= 100
})

test('flushes on the interval only when there is something buffered', () => {
    const store = new FakeStore()
    const b = batcher(store, { intervalMs: 1000 })
    assert.equal(b.shouldFlush(5000), false) // empty, no flush
    b.add(img(1, 'a'))
    assert.equal(b.shouldFlush(500), false) // not old enough
    assert.equal(b.shouldFlush(1000), true) // 1000 - 0 >= 1000
})

test('a failed write throws and keeps offsets un-committed; the retried images flush next time', async () => {
    const store = new FakeStore()
    const b = batcher(store)
    b.add(img(1, 'a'))
    b.add(img(1, 'b'))
    store.fail = true
    await assert.rejects(b.flush(0))
    // The snapshot was cleared on flush; those images redeliver from Kafka (offsets weren't committed).
    assert.equal(b.size, 0)
    assert.equal(store.writes.length, 0)

    store.fail = false
    b.add(img(1, 'a')) // redelivered
    b.add(img(1, 'b'))
    await b.flush(0)
    assert.deepEqual(
        store.writes.map((w) => w.hashes),
        [['a', 'b']]
    )
})
