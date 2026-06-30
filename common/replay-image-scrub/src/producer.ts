/**
 * Producer-side logic for the image-scrub topic, run by the ORIGINAL replay worker.
 *
 * For each inlined image in a recorded block: compute its team-scoped reference, dedup against Redis,
 * and post the RAW image to the scrub topic only the first time it's seen within the TTL. The caller
 * substitutes the returned reference for the inline `rr_dataURL` in the block. The blur consumer
 * later writes the scrubbed image to S3 under the same reference.
 *
 * Redis presence means only "posted to the topic recently", NOT "scrubbed image exists in S3" — the
 * reference can resolve to nothing in S3 until the consumer catches up, which is fine.
 *
 * Ports are injected so this is unit-testable without live Redis/Kafka and portable into the real
 * ml-mirror pipeline (where `reserve`/`release` wrap Redis and `produce` wraps the Kafka output).
 */
import { hashImageBytes, imageRef } from './content-ref.ts'

export const IMAGE_SCRUB_TOPIC = 'session_replay_image_scrub' // declared for real in kafka-topics.ts + terraform
export const DEDUP_TTL_SECONDS = 24 * 60 * 60 // 24h window; ~2.6GB raw keys at 20M/day, fits a 10GB Redis

export interface DedupStore {
    /** Atomically reserve the key if absent (SET key NX EX ttl). True if we reserved it (first sighting). */
    reserve(key: string, ttlSeconds: number): Promise<boolean>
    /** Undo a reservation so a later sighting can retry (used when the produce after reserve fails). */
    release(key: string): Promise<void>
}

export interface TopicProducer {
    /** Post one message; should resolve only once the broker has acked (so offsets commit safely). */
    produce(key: string, value: Buffer): Promise<void>
}

export interface ProducerDeps {
    dedup: DedupStore
    producer: TopicProducer
    ttlSeconds?: number
}

export interface EmitResult {
    /** Reference to substitute for the inline image, e.g. `image:42:a1B2...`. */
    ref: string
    /** True if we posted the image to the topic; false if a recent duplicate suppressed it. */
    posted: boolean
}

/**
 * Reserve-then-produce, rolling the reservation back if the produce fails so the image isn't lost
 * (a failed reservation would otherwise dedup every later sighting until the TTL expired). Keyed by
 * the reference so all sightings of one image land on the same partition. Throws if the produce
 * fails after rollback — the caller (fail-closed ml-mirror) drops the message rather than record a
 * reference whose image never made it onto the topic.
 */
export async function emitImageForScrub(teamId: number, bytes: Buffer, deps: ProducerDeps): Promise<EmitResult> {
    const ref = imageRef(teamId, hashImageBytes(bytes))
    const ttl = deps.ttlSeconds ?? DEDUP_TTL_SECONDS
    const fresh = await deps.dedup.reserve(ref, ttl)
    if (!fresh) {
        return { ref, posted: false }
    }
    try {
        await deps.producer.produce(ref, bytes)
    } catch (err) {
        await deps.dedup.release(ref).catch(() => {}) // best-effort; TTL also reclaims it
        throw err
    }
    return { ref, posted: true }
}
