/* eslint-disable no-console -- CLI logs to stdout */
/**
 * Producer CLI for local end-to-end testing: take an image off disk, run the real routing + dedup +
 * topic-produce path (the logic that will live in the ml-mirror worker), and print the reference
 * that would replace the inline image in the block. The consumer (npm run consume) then scrubs it.
 *
 *   npm run produce -- <image-path> <team-id>
 *
 * Demonstrates the full local flow: produce -> topic -> consumer -> S3. Run it twice to see dedup.
 */
import '../src/polyfill.ts'

import Redis from 'ioredis'
import { Kafka } from 'kafkajs'
import { readFile } from 'node:fs/promises'

import { KafkaTopicProducer, RedisDedupStore, ensureTopic } from '../src/clients.ts'
import { loadConfig } from '../src/config.ts'
import { emitImagesForScrub } from '../src/producer.ts'
import { routeImage } from '../src/routing.ts'
import { decodeSrc } from '../src/src-image.ts'

async function main(): Promise<void> {
    const [file, teamStr] = process.argv.slice(2)
    if (!file || !teamStr) {
        console.error('usage: npm run produce -- <image-path> <team-id>')
        process.exit(2)
    }
    const teamId = Number(teamStr)
    const bytes = await readFile(file)
    const { W, H } = await decodeSrc(bytes)

    const route = routeImage({ source: 'img', width: W, height: H, byteLength: bytes.length })
    console.log(`route=${route} (${W}x${H}, ${bytes.length} bytes)`)
    if (route !== 'advanced') {
        console.log('not an advanced-path image; nothing posted (handled in-process by the worker)')
        return
    }

    const cfg = loadConfig()
    const redis = new Redis(cfg.redisUrl)
    const kafka = new Kafka({ clientId: 'replay-image-scrub-producer', brokers: cfg.kafkaBrokers })
    await ensureTopic(kafka, cfg.topic)
    const producer = kafka.producer()
    await producer.connect()

    try {
        const [result] = await emitImagesForScrub([{ teamId, bytes }], {
            dedup: new RedisDedupStore(redis),
            producer: new KafkaTopicProducer(producer, cfg.topic),
        })
        console.log(result.posted ? `posted ${result.ref}` : `deduped ${result.ref} (already posted recently)`)
    } finally {
        await producer.disconnect()
        redis.disconnect()
    }
}

main().catch((e) => {
    console.error(e)
    process.exit(1)
})
