/* eslint-disable no-console -- worker logs to stdout */
/**
 * Image-scrub consumer worker: reads raw images off the scrub topic (key = `image:{team}:{hash}`),
 * scrubs them, and batches the bytes into shard objects + a content-hash-keyed parquet index (see
 * shard-store.ts + README). Stage 1 scrub is the sharp-only blur (blur.ts); Stage 2 swaps in advancedScrub.
 *
 *   npm run consume   (needs the dev stack: Kafka + SeaweedFS; env overrides in config.ts)
 */
import { Kafka } from 'kafkajs'

import { ImageBatcher } from './batcher.ts'
import { blurOnly } from './blur.ts'
import { ensureBucket, ensureTopic, makeS3 } from './clients.ts'
import { loadConfig } from './config.ts'
import { hashImageBytes, isImageRef, parseImageRef } from './content-ref.ts'
import { ScrubMetrics, startMetricsServer } from './metrics.ts'
import { ImageShardStore, ScrubbedImage } from './shard-store.ts'

/** Parse + verify + scrub one message into a ScrubbedImage, or null to skip it. */
async function scrubImage(ref: string, raw: Buffer): Promise<ScrubbedImage | null> {
    // The S3 location derives from the key, so reject bytes whose hash doesn't match it: a forged key must not write under another team's reference.
    const parsed = parseImageRef(ref)
    if (!parsed || hashImageBytes(raw) !== parsed.hash) {
        ScrubMetrics.incMismatch()
        return null
    }
    const bytes = await blurOnly(raw)
    ScrubMetrics.incScrubbed()
    return { teamId: parsed.teamId, hash: parsed.hash, bytes }
}

async function main(): Promise<void> {
    const cfg = loadConfig()
    const s3 = makeS3(cfg)
    await ensureBucket(s3, cfg.s3.bucket)
    const kafka = new Kafka({ clientId: 'ml-mirror-image-scrub', brokers: cfg.kafkaBrokers })
    await ensureTopic(kafka, cfg.topic)
    const stopMetrics = startMetricsServer()

    const store = new ImageShardStore(s3, cfg.s3.bucket)
    const batcher = new ImageBatcher(store, cfg.flush, Date.now())

    const consumer = kafka.consumer({ groupId: cfg.consumerGroup })
    await consumer.connect()
    await consumer.subscribe({ topic: cfg.topic, fromBeginning: false })
    console.log(`consuming ${cfg.topic} (group ${cfg.consumerGroup}) -> s3://${cfg.s3.bucket} @ ${cfg.s3.endpoint}`)

    // Un-committed offsets accumulated across batches, committed only after a flush lands; autoCommit/autoResolve off so nothing commits ahead of a write.
    const pending = new Map<number, { topic: string; partition: number; offset: string }>()
    await consumer.run({
        autoCommit: false,
        eachBatchAutoResolve: false,
        eachBatch: async ({ batch, heartbeat, isRunning, isStale }) => {
            for (const m of batch.messages) {
                if (!isRunning() || isStale()) {
                    break
                }
                const ref = m.key?.toString('utf8')
                if (ref && isImageRef(ref) && m.value) {
                    try {
                        const scrubbed = await scrubImage(ref, m.value)
                        if (scrubbed) {
                            batcher.add(scrubbed)
                        }
                    } catch (e) {
                        // One bad image is skipped (its reference resolves to nothing), acceptable for the mirror; metered, and the offset still advances.
                        ScrubMetrics.incFailed()
                        console.error(`${ref}: scrub failed: ${String(e)}`)
                    }
                }
                pending.set(batch.partition, {
                    topic: batch.topic,
                    partition: batch.partition,
                    offset: (Number(m.offset) + 1).toString(),
                })
                await heartbeat()
            }
            if (batcher.shouldFlush(Date.now())) {
                // Write shards first, commit only after: a failed write throws here, leaving the window un-committed for Kafka to replay.
                await batcher.flush(Date.now())
                if (pending.size > 0) {
                    await consumer.commitOffsets([...pending.values()])
                    pending.clear()
                }
                await heartbeat()
            }
        },
    })

    // Graceful shutdown: disconnect finishes the in-flight batch; buffered-but-unflushed images have un-committed offsets, so they replay on restart.
    let shuttingDown = false
    for (const sig of ['SIGINT', 'SIGTERM'] as const) {
        process.on(sig, () => {
            if (shuttingDown) {
                return
            }
            shuttingDown = true
            console.log(`${sig} received, draining...`)
            const force = setTimeout(() => process.exit(1), 30_000)
            consumer
                .disconnect()
                .catch((e) => console.error(`disconnect error: ${String(e)}`))
                .finally(() => {
                    clearTimeout(force)
                    stopMetrics()
                    process.exit(0)
                })
        })
    }
}

main().catch((e) => {
    console.error(e)
    process.exit(1)
})
