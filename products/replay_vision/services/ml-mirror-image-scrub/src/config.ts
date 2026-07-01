/** Runtime config for the consumer worker (and the local produce CLI). Defaults point at the standard
 *  PostHog dev stack (Kafka :9092, SeaweedFS S3 :8333). Override via env in other envs. */

/** The scrub topic. Keep in sync with KAFKA_SESSION_REPLAY_IMAGE_SCRUB in kafka-topics.ts + terraform.
 *  The nodejs producer prefixes this with KAFKA_PREFIX, so in any env where that is non-empty the
 *  deployment MUST set IMAGE_SCRUB_TOPIC to the same fully-resolved (prefixed) name, or the consumer
 *  subscribes to a different topic than the producer writes to. (Empty prefix in prod today.) */
export const IMAGE_SCRUB_TOPIC = 'session_replay_image_scrub'

export interface Config {
    kafkaBrokers: string[]
    topic: string
    consumerGroup: string
    // accessKeyId/secretAccessKey are optional: when unset (production against real S3), makeS3 omits
    // them so the AWS SDK default chain resolves the IRSA role. Set them locally for SeaweedFS/MinIO.
    s3: { endpoint: string; region: string; bucket: string; accessKeyId?: string; secretAccessKey?: string }
    // Shard-flush thresholds: buffer scrubbed images until any of these trips, then write one shard +
    // index per team. Bigger shards = fewer, cheaper S3 writes; the interval bounds worst-case latency.
    flush: { maxImages: number; maxBytes: number; intervalMs: number }
}

export function loadConfig(): Config {
    return {
        kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
        topic: process.env.IMAGE_SCRUB_TOPIC ?? IMAGE_SCRUB_TOPIC,
        consumerGroup: process.env.IMAGE_SCRUB_GROUP ?? 'ml-mirror-image-scrub-consumer',
        flush: {
            maxImages: Number(process.env.IMAGE_SCRUB_FLUSH_MAX_IMAGES ?? 1000),
            maxBytes: Number(process.env.IMAGE_SCRUB_FLUSH_MAX_BYTES ?? 128 * 1024 * 1024),
            intervalMs: Number(process.env.IMAGE_SCRUB_FLUSH_INTERVAL_MS ?? 30_000),
        },
        // Read the standard PostHog object-storage env (so prod points at SESSION_RECORDING_V2_S3 /
        // OBJECT_STORAGE_* config, not a hardcoded endpoint). Default to SeaweedFS, the direction of
        // travel. If your local stack runs the MinIO-style `objectstorage` on :19000 instead, set
        // S3_ENDPOINT=http://localhost:19000.
        s3: {
            endpoint: process.env.OBJECT_STORAGE_ENDPOINT ?? process.env.S3_ENDPOINT ?? 'http://localhost:8333',
            region: process.env.S3_REGION ?? 'us-east-1',
            bucket: process.env.OBJECT_STORAGE_BUCKET ?? process.env.S3_BUCKET ?? 'posthog',
            accessKeyId: process.env.OBJECT_STORAGE_ACCESS_KEY_ID ?? process.env.S3_ACCESS_KEY_ID,
            secretAccessKey: process.env.OBJECT_STORAGE_SECRET_ACCESS_KEY ?? process.env.S3_SECRET_ACCESS_KEY,
        },
    }
}
