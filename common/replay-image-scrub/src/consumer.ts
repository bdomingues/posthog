/* eslint-disable no-console -- worker logs to stdout */
/**
 * Image-scrub consumer worker. Reads raw images off the scrub topic (key = `image:{team}:{hash}`,
 * value = raw image bytes), scrubs them (NSFW gate + face mosaic + text solid-fill), and writes the
 * result to S3 under the reference. Idempotent: skips images already present in S3 (HEAD-then-skip),
 * so a redelivery or a duplicate that slipped past producer dedup just no-ops.
 *
 *   npm run consume
 *
 * Run the dev stack first (Kafka/Redis/SeaweedFS). Env overrides in src/config.ts.
 */
import './polyfill.ts'

import { Kafka } from 'kafkajs'

import { ensureBucket, ensureTopic, makeS3, s3Exists, s3Put } from './clients.ts'
import { loadConfig } from './config.ts'
import { isImageRef, s3KeyForRef } from './content-ref.ts'
import { type Models, advancedScrub, loadModels } from './scrub.ts'

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
    const kafka = new Kafka({ clientId: 'replay-image-scrub', brokers: cfg.kafkaBrokers })
    await ensureTopic(kafka, cfg.topic)

    const consumer = kafka.consumer({ groupId: cfg.consumerGroup })
    await consumer.connect()
    await consumer.subscribe({ topic: cfg.topic, fromBeginning: false })
    console.log(`consuming ${cfg.topic} (group ${cfg.consumerGroup}) -> s3://${cfg.s3.bucket} @ ${cfg.s3.endpoint}`)

    await consumer.run({
        eachMessage: async ({ message }) => {
            const ref = message.key?.toString('utf8')
            const bytes = message.value
            if (!ref || !isImageRef(ref) || !bytes) {
                console.warn('skip malformed message', { ref })
                return
            }
            try {
                console.log(`${ref}: ${await handle(models, s3, cfg.s3.bucket, ref, bytes)}`)
            } catch (e) {
                // Don't wedge the partition on one bad image; it stays unscrubbed (reference resolves
                // to nothing), which is acceptable for the training mirror.
                console.error(`${ref}: scrub failed: ${String(e)}`)
            }
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
