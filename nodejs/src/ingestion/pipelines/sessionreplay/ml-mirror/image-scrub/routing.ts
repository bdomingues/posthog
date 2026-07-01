/**
 * Routing policy: given an inlined replay image, decide how the ml-mirror producer handles it.
 *
 *  - 'passthrough': leave the image untouched. Only for images below the detectors' floor (<=16px),
 *    where no face/text could be found anyway, so scrubbing would lose zero protection while
 *    destroying high-signal icons/logos/glyphs that are valuable for model training.
 *  - 'cheap': the existing in-process downsample+blur. For canvas (dynamic, ~no dedup, so the
 *    advanced path would flood the topic + S3 with non-deduping frames) and for images too big to
 *    fit on the topic.
 *  - 'advanced': hash -> Redis dedup -> post to the scrub topic -> consumer blurs and writes to S3.
 *    For static <img>/media raster, which dedups well (logos/avatars/photos) and is PII-bearing.
 *
 * Kept in sync with the consumer package's routing.ts (see content-ref.ts CONTRACT note).
 */
export type ImageSource = 'canvas' | 'img' | 'media'
export type ScrubRoute = 'passthrough' | 'cheap' | 'advanced'

export const TINY_MAX_SIDE = 16 // <= this on the long side => below the face/text detector floor
export const TOPIC_MAX_BYTES = 900_000 // under Kafka's ~1MB message cap, leaving room for the envelope

export interface RouteInput {
    source: ImageSource
    /** Pixel dimensions if known (from rrweb attrs or a header decode); omit if unknown. */
    width?: number
    height?: number
    /** Size of the raw (decoded-from-base64) image bytes. */
    byteLength: number
}

export function routeImage(i: RouteInput): ScrubRoute {
    // 1. Tiny + high-signal + below the detector floor -> keep as-is. Only when we actually know the
    //    dimensions; an unknown-size image is scrubbed rather than risk passing something through.
    if (i.width != null && i.height != null && Math.max(i.width, i.height) <= TINY_MAX_SIDE) {
        return 'passthrough'
    }
    // 2. Canvas is dynamic and dedups ~never -> cheap in-process blur instead of flooding the topic.
    if (i.source === 'canvas') {
        return 'cheap'
    }
    // 3. Can't fit on the topic -> cheap in-process blur fallback (rare; capture drops >~1MB already).
    if (i.byteLength > TOPIC_MAX_BYTES) {
        return 'cheap'
    }
    // 4. Static <img>/media raster -> advanced topic path (dedups, fidelity, PII-bearing).
    return 'advanced'
}
