import { DedupStore, TopicProducer } from '~/ingestion/pipelines/sessionreplay/ml-mirror/image-scrub/producer'

import { AllowLists } from './allow-lists'

/** Replacement char for redacted (non-allow-listed) word characters. */
export const REDACT_CHAR = '*'
/** Replacement char for numeric tokens. */
export const NUMBER_CHAR = '#'

/** A deferred image-blur job: an async closure that blurs its image and writes the result back in place. */
export type BlurJob = () => Promise<void>

/** An advanced-route image to hand off to the scrub topic: raw bytes to emit, plus a callback that
 *  writes the resolved `image:{team}:{hash}` reference back over the inline image. */
export interface ImageScrubJob {
    bytes: Buffer
    apply: (ref: string) => void
}

/** Injected Redis/Kafka ports for the image-scrub topic. Present only in the ml-mirror pipeline —
 *  when absent, advanced-route images fall back to the in-process blur, so other pipelines are
 *  unaffected. */
export interface ImageScrubPorts {
    dedup: DedupStore
    producer: TopicProducer
}

/** Per-scrub context: the active allow lists plus tunables read by the scrubbers. */
export interface ScrubContext {
    allow: AllowLists
    /** Optional collector for deferred image-blur jobs (see {@link BlurJob}). */
    blurJobs?: BlurJob[]
    /** Team id of the message being scrubbed — needed to build team-scoped image references. */
    teamId?: number
    /** Image-scrub topic ports; when set (ml-mirror pipeline), advanced-route images are hashed,
     *  referenced, and emitted to the topic instead of blurred in-process. */
    imageScrub?: ImageScrubPorts
    /** Collector for advanced-route images awaiting a batched emit (see {@link ImageScrubJob}). */
    imageScrubJobs?: ImageScrubJob[]
}

/** Shared non-null-object type guard used across the scrubbers. */
export function isObject(v: unknown): v is Record<string, any> {
    return typeof v === 'object' && v !== null
}
