/* eslint-disable no-console -- worker logs to stdout */
/**
 * Image-scrub consumer worker. Reads raw images off the scrub topic (key = `image:{team}:{hash}`,
 * value = raw image bytes), scrubs them, and writes the result to S3 under the reference. Idempotent:
 * skips images already present in S3 (HEAD-then-skip), so a redelivery or a duplicate that slipped
 * past producer dedup just no-ops.
 *
 * Scrub is the native ML pipeline (NSFW gate + face mosaic + text solid-fill) in scrub.ts. Models are
 * loaded once at startup. polyfill.ts is imported first so tfjs-node loads on Node 23+.
 *
 *   npm run consume
 *
 * Run the dev stack first (Kafka/Redis/SeaweedFS) and `npm run setup` for the models. Env overrides
 * in src/config.ts.
 */
import './polyfill.ts'

import { Kafka } from 'kafkajs'

import { ensureBucket, ensureTopic, makeS3, s3Exists, s3Put } from './clients.ts'
import { loadConfig } from './config.ts'
import { isImageRef, s3KeyForRef } from './content-ref.ts'
import { type Models, advancedScrub, loadModels } from './scrub.ts'

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

async function handle(
    models: Models,
    s3: ReturnType<typeof makeS3>,
    bucket: string,
    ref: string,
    bytes: Buffer
): Promise<string> {
    const key = s3KeyForRef(ref)
    if (await s3Exists(s3, bucket, key)) {
        return 'skip (exists)'
    }
    const { out, t } = await advancedScrub(bytes, models)
    await s3Put(s3, bucket, key, out)
    return `scrubbed ${t.totalMs.toFixed(0)}ms -> ${key}`
}

async function main(): Promise<void> {
    const cfg = loadConfig()
    const models = await loadModels()
    const s3 = makeS3(cfg)
    await ensureBucket(s3, cfg.s3.bucket)
    const kafka = new Kafka({ clientId: 'ml-mirror-image-scrub', brokers: cfg.kafkaBrokers })
    await ensureTopic(kafka, cfg.topic)

    const consumer = kafka.consumer({ groupId: cfg.consumerGroup })
    await consumer.connect()
    await consumer.subscribe({ topic: cfg.topic, fromBeginning: false })
    console.log(`consuming ${cfg.topic} (group ${cfg.consumerGroup}) -> s3://${cfg.s3.bucket} @ ${cfg.s3.endpoint}`)

    // eachBatch so we can scrub + write S3 with bounded concurrency: sequential S3 writes (one RTT
    // each) would never keep up with the ingest rate, so a batch's images are processed in parallel.
    const concurrency = Number(process.env.SCRUB_CONCURRENCY ?? 8)
    await consumer.run({
        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
            const jobs = batch.messages.map((m) => ({ ref: m.key?.toString('utf8'), bytes: m.value, offset: m.offset }))
            await mapPool(jobs, concurrency, async (job) => {
                if (!isRunning() || isStale()) {
                    return
                }
                if (job.ref && isImageRef(job.ref) && job.bytes) {
                    try {
                        console.log(`${job.ref}: ${await handle(models, s3, cfg.s3.bucket, job.ref, job.bytes)}`)
                    } catch (e) {
                        // Don't wedge the partition on one bad image; it stays unscrubbed (reference
                        // resolves to nothing), which is acceptable for the training mirror.
                        console.error(`${job.ref}: scrub failed: ${String(e)}`)
                    }
                }
                resolveOffset(job.offset)
                await heartbeat()
            })
        },
    })

    for (const sig of ['SIGINT', 'SIGTERM'] as const) {
        process.on(sig, () => {
            consumer.disconnect().finally(() => process.exit(0))
        })
    }
}

main().catch((e) => {
    console.error(e)
    process.exit(1)
})
