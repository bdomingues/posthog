/**
 * Producer-side logic for the image-scrub topic, run inline by the ml-mirror anonymize pipeline.
 *
 * A recorded block can carry MANY inlined images, and we cannot afford a Redis round-trip per image
 * (the RTT dominates). So the API is BATCHED: collect every advanced-route image in the message, then
 * do ONE Redis round-trip to dedup them all and ONE Kafka send to post the fresh ones.
 *
 * For each image: compute its team-scoped reference; the caller substitutes it for the inline
 * `rr_dataURL`. We post the RAW image to the topic only the first time it's seen within the TTL. The
 * scrub consumer later writes the scrubbed image to S3 under the reference. Redis presence means only
 * "posted to the topic recently" (dedup), NOT "scrubbed image exists in S3".
 *
 * The reserve/release/produce operations are injected as plain functions so this stays pure and
 * unit-testable; the ml-mirror server wires them to the Redis pool (redis-dedup.ts) and the Kafka
 * producer. Kept in sync with the consumer package's content-ref (see content-ref.ts CONTRACT).
 */
import { hashImageBytes, imageRef } from './content-ref'
import { ImageScrubMetrics } from './metrics'

export const DEDUP_TTL_SECONDS = 24 * 60 * 60 // 24h window; ~2.6GB raw keys at 20M/day, fits a 10GB Redis

export interface TopicMessage {
    key: string
    value: Buffer
}

export interface ImageInput {
    teamId: number
    bytes: Buffer
}

/** Injected side-effects: batched Redis reserve/release + a batched topic produce. */
export interface ImageScrubEmitDeps {
    /** SET NX EX a batch of content-hash keys in Redis, in one round-trip. Returns, per key in order,
     *  whether it was newly set (true = first sighting within the TTL, so post it; false = a recent or
     *  in-batch duplicate). */
    setBatchContentKeysRedis: (keys: string[], ttlSeconds: number) => Promise<boolean[]>
    /** DEL a batch of content-hash keys in Redis — rolls the reservations back after a failed produce. */
    deleteBatchContentKeysRedis: (keys: string[]) => Promise<void>
    /** Produce a batch of raw-image messages to the scrub topic; resolves once the broker acks them. */
    produceBatchImagesKafka: (messages: TopicMessage[]) => Promise<void>
    ttlSeconds?: number
}

export interface EmitResult {
    /** Reference to substitute for the inline image, e.g. `image:42:a1B2...`. */
    ref: string
    /** True if this call posted the image; false if a recent (or in-batch) duplicate suppressed it. */
    posted: boolean
}

/**
 * Dedup a message's images in one Redis round-trip and post the fresh ones in one Kafka send. Rolls
 * the reservations back if the produce fails, so images aren't lost (a stuck reservation would
 * otherwise dedup every later sighting until the TTL expired). Returns one result per input image,
 * in order; the caller substitutes each `ref` for its inline image. Throws if the produce fails after
 * rollback — the fail-closed ml-mirror then drops the message rather than record references whose
 * images never made it onto the topic.
 */
export async function emitImagesForScrub(images: ImageInput[], deps: ImageScrubEmitDeps): Promise<EmitResult[]> {
    if (images.length === 0) {
        return []
    }
    const refs = images.map((img) => imageRef(img.teamId, hashImageBytes(img.bytes)))
    const ttl = deps.ttlSeconds ?? DEDUP_TTL_SECONDS

    const fresh = await deps.setBatchContentKeysRedis(refs, ttl) // one round-trip

    const toPost: TopicMessage[] = []
    for (let i = 0; i < images.length; i++) {
        if (fresh[i]) {
            toPost.push({ key: refs[i], value: images[i].bytes })
        }
    }
    if (toPost.length > 0) {
        try {
            await deps.produceBatchImagesKafka(toPost) // one send
        } catch (err) {
            // Roll back the reservations so later sightings retry; if the rollback itself fails (Redis
            // down) the keys sit until the TTL and dedup those images away — meter it so it's visible.
            await deps
                .deleteBatchContentKeysRedis(toPost.map((m) => m.key))
                .catch(() => ImageScrubMetrics.incrementReservationRollbackFailure())
            throw err
        }
    }
    return refs.map((ref, i) => ({ ref, posted: fresh[i] }))
}
