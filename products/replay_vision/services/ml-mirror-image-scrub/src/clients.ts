/** S3 helpers plus a topic-ensure for the consumer worker (and the local produce CLI). Producer-side
 *  Redis dedup + Kafka producing live in the ml-mirror pipeline (nodejs), not here — the consumer only
 *  reads already-routed images off the topic and writes the scrubbed result to S3. */
import { CreateBucketCommand, HeadObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import type { Kafka } from 'kafkajs'

import type { Config } from './config.ts'

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
    const { accessKeyId, secretAccessKey } = cfg.s3
    return new S3Client({
        endpoint: cfg.s3.endpoint,
        region: cfg.s3.region,
        forcePathStyle: true, // required for SeaweedFS / MinIO
        // Use static keys only when both are provided (local dev); otherwise omit them so the SDK's
        // default credential chain resolves the IRSA role in-cluster — never fall back to dev creds.
        ...(accessKeyId && secretAccessKey ? { credentials: { accessKeyId, secretAccessKey } } : {}),
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
        if (status === 404) {
            return false
        }
        // Only 404 means "absent". A 403 is a permissions problem (missing s3:HeadObject, bad policy):
        // surfacing it fails loudly instead of masking the misconfig as a cache miss and re-scrubbing.
        throw e
    }
}

export async function s3Put(s3: S3Client, bucket: string, key: string, body: Buffer): Promise<void> {
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body, ContentType: 'image/png' }))
}
