/* eslint-disable no-console -- worker logs to stdout */
/**
 * Image-scrub consumer worker. Reads raw images off the scrub topic (key = `image:{team}:{hash}`,
 * value = raw image bytes), scrubs them, and writes the result to S3 under the reference. Idempotent:
 * skips images already present in S3 (HEAD-then-skip), so a redelivery or a duplicate that slipped
 * past producer dedup just no-ops.
 *
 * Stage 1 scrub is the sharp-only downsample+blur (blur.ts) — no ML deps, so this worker's image stays
 * lean. Stage 2 swaps in advancedScrub (NSFW gate + face mosaic + text solid-fill) from scrub.ts.
 *
 *   npm run consume
 *
 * Run the dev stack first (Kafka/SeaweedFS). Env overrides in src/config.ts.
 */
import { Kafka } from 'kafkajs'

import { blurOnly } from './blur.ts'
import { ensureBucket, ensureTopic, makeS3, s3Exists, s3Put } from './clients.ts'
import { loadConfig } from './config.ts'
import { hashImageBytes, isImageRef, parseImageRef, s3KeyForRef } from './content-ref.ts'
import { ScrubMetrics, startMetricsServer } from './metrics.ts'

/** Run fn over items with at most `concurrency` in flight — bounds CPU (scrub) and S3 connections. */
async function mapPool<T>(items: T[], concurrency: number, fn: (item: T) => Promise<void>): Promise<void> {
    let next = 0
    const worker = async (): Promise<void> => {
        while (next < items.length) {
            await fn(items[next++])
        }
    }
    await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()))
}

async function handle(s3: ReturnType<typeof makeS3>, bucket: string, ref: string, bytes: Buffer): Promise<string> {
    // Bind the key to the content: the S3 path is derived from the message key, so refuse to write bytes
    // whose hash doesn't match the key's hash — a forged/corrupt key must not land under another team's
    // reference (defense in depth against cross-tenant writes and in-transit corruption).
    const parsed = parseImageRef(ref)
    if (!parsed || hashImageBytes(bytes) !== parsed.hash) {
        ScrubMetrics.incMismatch()
        return 'skip (key/content mismatch)'
    }
    const key = s3KeyForRef(ref)
    if (await s3Exists(s3, bucket, key)) {
        ScrubMetrics.incSkipExists()
        return 'skip (exists)'
    }
    const out = await blurOnly(bytes)
    await s3Put(s3, bucket, key, out)
    ScrubMetrics.incScrubbed()
    return `blurred -> ${key}`
}

async function main(): Promise<void> {
    const cfg = loadConfig()
    const s3 = makeS3(cfg)
    await ensureBucket(s3, cfg.s3.bucket)
    const kafka = new Kafka({ clientId: 'ml-mirror-image-scrub', brokers: cfg.kafkaBrokers })
    await ensureTopic(kafka, cfg.topic)
    const stopMetrics = startMetricsServer()

    const consumer = kafka.consumer({ groupId: cfg.consumerGroup })
    await consumer.connect()
    await consumer.subscribe({ topic: cfg.topic, fromBeginning: false })
    console.log(`consuming ${cfg.topic} (group ${cfg.consumerGroup}) -> s3://${cfg.s3.bucket} @ ${cfg.s3.endpoint}`)

    // eachBatchAutoResolve: false so kafkajs commits ONLY the offsets we resolve. A batch's images are
    // scrubbed in parallel (S3 round-trips would never keep up sequentially), but we resolve offsets in
    // strict order: only the longest contiguous run of completed messages, so an interrupted batch
    // (rebalance/shutdown) never commits past an un-scrubbed offset and silently drops it.
    const concurrency = Number(process.env.SCRUB_CONCURRENCY ?? 8)
    await consumer.run({
        eachBatchAutoResolve: false,
        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
            const messages = batch.messages
            const done = new Array<boolean>(messages.length).fill(false)
            await mapPool(
                messages.map((m, i) => ({ m, i })),
                concurrency,
                async ({ m, i }) => {
                    if (!isRunning() || isStale()) {
                        return // leave unresolved so it's redelivered, never silently skipped
                    }
                    const ref = m.key?.toString('utf8')
                    if (ref && isImageRef(ref) && m.value) {
                        try {
                            console.log(`${ref}: ${await handle(s3, cfg.s3.bucket, ref, m.value)}`)
                        } catch (e) {
                            // Don't wedge the partition on one bad image; it stays unscrubbed (reference
                            // resolves to nothing), which is acceptable for the training mirror. Metered.
                            ScrubMetrics.incFailed()
                            console.error(`${ref}: scrub failed: ${String(e)}`)
                        }
                    }
                    done[i] = true
                    await heartbeat()
                }
            )
            let watermark = -1
            for (let i = 0; i < messages.length && done[i]; i++) {
                watermark = i
            }
            if (watermark >= 0) {
                resolveOffset(messages[watermark].offset)
            }
            await heartbeat()
        },
    })

    // Graceful shutdown: stop consuming and await disconnect (finishes the in-flight batch and commits
    // resolved offsets) before exiting, so a rolling deploy doesn't abandon in-flight scrub/S3 work. A
    // hard timeout guards against a wedged batch hanging the drain.
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
