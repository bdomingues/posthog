/** Runtime config for the local-dev producer CLI and consumer worker. Defaults point at the standard
 *  PostHog dev stack (Kafka :9092, Redis :6379, SeaweedFS S3 :8333). Override via env in other envs. */
import { IMAGE_SCRUB_TOPIC } from './producer.ts'

export interface Config {
    kafkaBrokers: string[]
    topic: string
    consumerGroup: string
    redisUrl: string
    s3: { endpoint: string; region: string; bucket: string; accessKeyId: string; secretAccessKey: string }
}

export function loadConfig(): Config {
    return {
        kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
        topic: process.env.IMAGE_SCRUB_TOPIC ?? IMAGE_SCRUB_TOPIC,
        consumerGroup: process.env.IMAGE_SCRUB_GROUP ?? 'replay-image-scrub-consumer',
        redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
        // Read the standard PostHog object-storage env (so prod points at SESSION_RECORDING_V2_S3 /
        // OBJECT_STORAGE_* config, not a hardcoded endpoint). Default to SeaweedFS, the direction of
        // travel. If your local stack runs the MinIO-style `objectstorage` on :19000 instead, set
        // S3_ENDPOINT=http://localhost:19000.
        s3: {
            endpoint: process.env.OBJECT_STORAGE_ENDPOINT ?? process.env.S3_ENDPOINT ?? 'http://localhost:8333',
            region: process.env.S3_REGION ?? 'us-east-1',
            bucket: process.env.OBJECT_STORAGE_BUCKET ?? process.env.S3_BUCKET ?? 'posthog',
            accessKeyId:
                process.env.OBJECT_STORAGE_ACCESS_KEY_ID ?? process.env.S3_ACCESS_KEY_ID ?? 'object_storage_root_user',
            secretAccessKey:
                process.env.OBJECT_STORAGE_SECRET_ACCESS_KEY ??
                process.env.S3_SECRET_ACCESS_KEY ??
                'object_storage_root_password',
        },
    }
}
