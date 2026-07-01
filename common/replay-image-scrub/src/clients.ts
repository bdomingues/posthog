/** Real implementations of the injected ports, plus S3 helpers, for the local-dev producer/consumer.
 *  These are the pieces that get swapped for the plugin-server's own Redis/Kafka when the producer
 *  is wired into the real ml-mirror pipeline. */
import { CreateBucketCommand, HeadObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import type Redis from 'ioredis'
import type { Kafka, Producer } from 'kafkajs'

import type { Config } from './config.ts'
import type { DedupStore, TopicProducer } from './producer.ts'

/** Redis-backed dedup. Batches a block's keys into ONE round-trip with an ioredis pipeline (the same
 *  pattern as the cookieless redis-helpers): SET key 1 EX ttl NX reserves on first sighting. */
export class RedisDedupStore implements DedupStore {
    constructor(private redis: Redis) {}
    async reserveBatch(keys: string[], ttlSeconds: number): Promise<boolean[]> {
        if (keys.length === 0) {
            return []
        }
        const pipeline = this.redis.pipeline()
        for (const key of keys) {
            pipeline.set(key, '1', 'EX', ttlSeconds, 'NX')
        }
        const raw = await pipeline.exec() // one round-trip; ordered results
        return (raw ?? []).map(([err, res]) => !err && res === 'OK') // 'OK' = we set it (fresh)
    }
    async releaseBatch(keys: string[]): Promise<void> {
        if (keys.length === 0) {
            return
        }
        const pipeline = this.redis.pipeline()
        for (const key of keys) {
            pipeline.del(key)
        }
        await pipeline.exec()
    }
}

/** Kafka-backed producer; one send per batch, resolves only after the broker acks (acks: -1). */
export class KafkaTopicProducer implements TopicProducer {
    constructor(
        private producer: Producer,
        private topic: string
    ) {}
    async produceBatch(messages: { key: string; value: Buffer }[]): Promise<void> {
        if (messages.length === 0) {
            return
        }
        await this.producer.send({ topic: this.topic, acks: -1, messages })
    }
}

/** Idempotently create the topic so a fresh local stack works without editing the dev compose.
 *  Lists first so a pre-existing topic doesn't log a broker "already exists" error. */
export async function ensureTopic(kafka: Kafka, topic: string): Promise<void> {
    const admin = kafka.admin()
    await admin.connect()
    try {
        const existing = await admin.listTopics()
        if (!existing.includes(topic)) {
            await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] })
        }
    } finally {
        await admin.disconnect()
    }
}

export function makeS3(cfg: Config): S3Client {
    return new S3Client({
        endpoint: cfg.s3.endpoint,
        region: cfg.s3.region,
        forcePathStyle: true, // required for SeaweedFS / MinIO
        credentials: { accessKeyId: cfg.s3.accessKeyId, secretAccessKey: cfg.s3.secretAccessKey },
    })
}

/** Create the bucket if missing (MinIO errors on PUT to a missing bucket; SeaweedFS auto-creates). */
export async function ensureBucket(s3: S3Client, bucket: string): Promise<void> {
    try {
        await s3.send(new CreateBucketCommand({ Bucket: bucket }))
    } catch (e) {
        const code = (e as { name?: string }).name ?? ''
        if (!/BucketAlreadyOwnedByYou|BucketAlreadyExists/.test(code)) {
            // already-exists is fine; anything else is a real problem
            const status = (e as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode
            if (status !== 409) {
                throw e
            }
        }
    }
}

export async function s3Exists(s3: S3Client, bucket: string, key: string): Promise<boolean> {
    try {
        await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }))
        return true
    } catch (e) {
        const status = (e as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode
        if (status === 404 || status === 403) {
            return false
        }
        throw e
    }
}

export async function s3Put(s3: S3Client, bucket: string, key: string, body: Buffer): Promise<void> {
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body, ContentType: 'image/png' }))
}
