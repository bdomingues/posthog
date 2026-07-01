/**
 * Producer-side logic for the image-scrub topic, run by the ORIGINAL replay worker.
 *
 * A recorded block can carry MANY inlined images, and we cannot afford a Redis round-trip per image
 * (the RTT dominates). So the API is BATCHED: collect every advanced-route image in the block, then
 * do ONE Redis round-trip to dedup them all and ONE Kafka send to post the fresh ones. This mirrors
 * the cookieless redis-helpers pattern (ioredis pipeline() + exec()).
 *
 * For each image: compute its team-scoped reference; the caller substitutes it for the inline
 * `rr_dataURL`. We post the RAW image to the topic only the first time it's seen within the TTL. The
 * blur consumer later writes the scrubbed image to S3 under the reference.
 *
 * Redis presence means only "posted to the topic recently" (dedup), NOT "scrubbed image exists in
 * S3" — a reference can resolve to nothing until the consumer catches up, which is fine.
 *
 * Ports are injected so this is unit-testable without live Redis/Kafka and portable into the real
 * ml-mirror pipeline (where the ports wrap the plugin-server's Redis and the Kafka output).
 */
import { hashImageBytes, imageRef } from './content-ref.ts'

export const IMAGE_SCRUB_TOPIC = 'session_replay_image_scrub' // declared for real in kafka-topics.ts + terraform
export const DEDUP_TTL_SECONDS = 24 * 60 * 60 // 24h window; ~2.6GB raw keys at 20M/day, fits a 10GB Redis

export interface DedupStore {
    /** Reserve every key that's absent, in ONE round-trip (pipeline of SET NX EX). Returns, per key
     *  in order, whether we reserved it (true = first sighting). Duplicate keys within the batch
     *  resolve correctly: the first is fresh, the rest see it already set. */
    reserveBatch(keys: string[], ttlSeconds: number): Promise<boolean[]>
    /** Release reservations in ONE round-trip (pipeline of DEL) — used to roll back a failed produce. */
    releaseBatch(keys: string[]): Promise<void>
}

export interface TopicMessage {
    key: string
    value: Buffer
}

export interface TopicProducer {
    /** Post all messages in ONE send; resolves only once the broker has acked them. */
    produceBatch(messages: TopicMessage[]): Promise<void>
}

export interface ImageInput {
    teamId: number
    bytes: Buffer
}

export interface ProducerDeps {
    dedup: DedupStore
    producer: TopicProducer
    ttlSeconds?: number
}

export interface EmitResult {
    /** Reference to substitute for the inline image, e.g. `image:42:a1B2...`. */
    ref: string
    /** True if this call posted the image; false if a recent (or in-batch) duplicate suppressed it. */
    posted: boolean
}

/**
 * Dedup a block's images in one Redis round-trip and post the fresh ones in one Kafka send. Rolls
 * the reservations back if the produce fails, so images aren't lost (a stuck reservation would
 * otherwise dedup every later sighting until the TTL expired). Returns one result per input image,
 * in order; the caller substitutes each `ref` for its inline image. Throws if the produce fails
 * after rollback — the fail-closed ml-mirror then drops the message rather than record references
 * whose images never made it onto the topic.
 */
export async function emitImagesForScrub(images: ImageInput[], deps: ProducerDeps): Promise<EmitResult[]> {
    if (images.length === 0) {
        return []
    }
    const refs = images.map((img) => imageRef(img.teamId, hashImageBytes(img.bytes)))
    const ttl = deps.ttlSeconds ?? DEDUP_TTL_SECONDS

    const fresh = await deps.dedup.reserveBatch(refs, ttl) // one round-trip

    const toPost: TopicMessage[] = []
    for (let i = 0; i < images.length; i++) {
        if (fresh[i]) {
            toPost.push({ key: refs[i], value: images[i].bytes })
        }
    }
    if (toPost.length > 0) {
        try {
            await deps.producer.produceBatch(toPost) // one send
        } catch (err) {
            await deps.dedup.releaseBatch(toPost.map((m) => m.key)).catch(() => {})
            throw err
        }
    }
    return refs.map((ref, i) => ({ ref, posted: fresh[i] }))
}
