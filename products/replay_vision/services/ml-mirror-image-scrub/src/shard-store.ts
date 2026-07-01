/**
 * Writes scrubbed images as batched shard objects + a parquet index, so lookups are by content hash
 * (not by replay id). One shard + one index parquet per team per flush. Mirrors the ml-mirror
 * block-metadata parquet store.
 *
 * Layout (team_id-partitioned):
 *   scrubbed-images/team_id={team}/shards/{node}-{ts}-{seq}.bin      raw concat of scrubbed image bytes
 *   scrubbed-images/team_id={team}/index/{node}-{ts}-{seq}.parquet   rows: hash, shard, offset, length
 *
 * Read contract: resolve `image:{team}:{hash}` by querying the team's index for `hash`, then
 * range-GET `shard` bytes [offset, offset+length). See README.
 */
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { ParquetSchema, ParquetWriter } from '@dsnp/parquetjs'
import { randomUUID } from 'node:crypto'
import { Writable } from 'node:stream'

/** A scrubbed image awaiting shard write. `hash` is the ORIGINAL content hash (the index key, from the
 *  reference); `bytes` are the SCRUBBED image bytes stored in the shard. */
export interface ScrubbedImage {
    teamId: number
    hash: string
    bytes: Buffer
}

interface IndexRow {
    hash: string
    shard: string
    offset: number
    length: number
}

// Snappy per column (compression is a per-field option in parquetjs, not a writer option).
const INDEX_SCHEMA = new ParquetSchema({
    hash: { type: 'UTF8', compression: 'SNAPPY' },
    shard: { type: 'UTF8', compression: 'SNAPPY' },
    offset: { type: 'INT64', compression: 'SNAPPY' },
    length: { type: 'INT64', compression: 'SNAPPY' },
})

async function rowsToIndexParquet(rows: IndexRow[]): Promise<Buffer> {
    const chunks: Buffer[] = []
    const sink = new Writable({
        write(chunk: Buffer, _encoding, callback) {
            chunks.push(chunk)
            callback()
        },
    })
    // parquetjs types want an fs.WriteStream, but at runtime it only uses write/end/on, which Writable provides.
    const writer = await ParquetWriter.openStream(
        INDEX_SCHEMA,
        sink as unknown as Parameters<typeof ParquetWriter.openStream>[1]
    )
    for (const r of rows) {
        await writer.appendRow({ hash: r.hash, shard: r.shard, offset: BigInt(r.offset), length: BigInt(r.length) })
    }
    await writer.close()
    return Buffer.concat(chunks)
}

export class ImageShardStore {
    private seq = 0
    private readonly nodeId: string

    constructor(
        private readonly s3: S3Client,
        private readonly bucket: string,
        nodeId?: string
    ) {
        // Stable per-pod id so concurrent writers can't collide on a key.
        this.nodeId = nodeId || process.env.HOSTNAME || randomUUID().slice(0, 8)
    }

    /** Write one team's images as ONE shard blob + ONE parquet index. Throws on failure so the caller
     *  replays from Kafka (at-least-once); a redelivery just writes a fresh shard (the reader dedups by
     *  hash), and an orphaned shard from a mid-write failure is invisible (no index points at it). */
    public async writeTeam(teamId: number, images: ScrubbedImage[]): Promise<{ shard: string; bytes: number }> {
        this.seq += 1
        const stamp = `${this.nodeId}-${Date.now()}-${this.seq}`
        const shardKey = `scrubbed-images/team_id=${teamId}/shards/${stamp}.bin`

        const rows: IndexRow[] = []
        const parts: Buffer[] = []
        let offset = 0
        for (const img of images) {
            rows.push({ hash: img.hash, shard: shardKey, offset, length: img.bytes.length })
            parts.push(img.bytes)
            offset += img.bytes.length
        }
        const shardBody = Buffer.concat(parts, offset)
        const indexBody = await rowsToIndexParquet(rows)

        // Shard first, then index: a dangling shard (index write failed) just wastes storage, whereas an
        // index pointing at a missing shard would break reads.
        await this.s3.send(
            new PutObjectCommand({
                Bucket: this.bucket,
                Key: shardKey,
                Body: shardBody,
                ContentType: 'application/octet-stream',
            })
        )
        await this.s3.send(
            new PutObjectCommand({
                Bucket: this.bucket,
                Key: `scrubbed-images/team_id=${teamId}/index/${stamp}.parquet`,
                Body: indexBody,
                ContentType: 'application/vnd.apache.parquet',
            })
        )
        return { shard: shardKey, bytes: offset }
    }
}
