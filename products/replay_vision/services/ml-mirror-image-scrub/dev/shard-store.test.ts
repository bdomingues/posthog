import { ParquetReader } from '@dsnp/parquetjs'
import assert from 'node:assert/strict'
import { test } from 'node:test'

import { ImageShardStore } from '../src/shard-store.ts'

/** Fake S3Client capturing PutObjectCommand bodies by key. */
function fakeS3(): { s3: any; objects: Map<string, Buffer> } {
    const objects = new Map<string, Buffer>()
    const s3 = {
        send: (cmd: any) => {
            const { Key, Body } = cmd.input
            objects.set(Key, Buffer.isBuffer(Body) ? Body : Buffer.from(Body))
            return Promise.resolve({})
        },
    }
    return { s3, objects }
}

test('writes a concat shard + parquet index that round-trips to the exact image bytes', async () => {
    const { s3, objects } = fakeS3()
    const store = new ImageShardStore(s3, 'bucket', 'node1')
    const images = [
        { teamId: 42, hash: 'a'.repeat(22), bytes: Buffer.from('first-image') },
        { teamId: 42, hash: 'b'.repeat(22), bytes: Buffer.from('second-image-longer') },
    ]

    const { shard, bytes } = await store.writeTeam(42, images)

    // The shard is the raw concatenation of scrubbed bytes.
    const shardBody = objects.get(shard)!
    assert.equal(shardBody.toString(), 'first-imagesecond-image-longer')
    assert.equal(bytes, shardBody.length)
    assert.match(shard, /^scrubbed-images\/team_id=42\/shards\/node1-\d+-\d+\.bin$/)

    // The index parquet maps each hash -> (shard, offset, length); the range reproduces the bytes.
    const indexKey = [...objects.keys()].find((k) => k.endsWith('.parquet'))!
    assert.match(indexKey, /^scrubbed-images\/team_id=42\/index\/node1-\d+-\d+\.parquet$/)
    const reader = await ParquetReader.openBuffer(objects.get(indexKey)!)
    const cursor = reader.getCursor()
    const rows: any[] = []
    let row
    while ((row = await cursor.next())) {
        rows.push(row)
    }
    await reader.close()

    assert.equal(rows.length, 2)
    for (const img of images) {
        const r = rows.find((x) => x.hash === img.hash)!
        assert.equal(r.shard, shard)
        const slice = shardBody.subarray(Number(r.offset), Number(r.offset) + Number(r.length))
        assert.equal(slice.toString(), img.bytes.toString())
    }
})
