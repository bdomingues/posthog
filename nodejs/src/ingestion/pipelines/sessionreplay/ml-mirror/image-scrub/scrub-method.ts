/**
 * Scrub-method policy: given an inlined replay image, decide how the ml-mirror producer handles it.
 *
 *  - 'passthrough': leave the image untouched. Only for images below the detectors' floor (<=16px),
 *    where no face/text could be found anyway, so scrubbing would lose zero protection while
 *    destroying high-signal icons/logos/glyphs that are valuable for model training.
 *  - 'cheapBlur': the existing in-process downsample+blur. For canvas (dynamic, ~no dedup, so the
 *    advanced path would flood the topic + S3 with non-deduping frames) and for images too big to
 *    fit on the topic.
 *  - 'advancedScrub': hash -> Redis dedup -> post to the scrub topic -> consumer scrubs and writes to
 *    S3. For static <img>/media raster, which dedups well (logos/avatars/photos) and is PII-bearing.
 */
export type ImageSource = 'canvas' | 'img' | 'media'
export type ScrubMethod = 'passthrough' | 'cheapBlur' | 'advancedScrub'

export const TINY_MAX_SIDE = 16 // <= this on the long side => below the face/text detector floor
// A genuine <=16px image is a few hundred bytes. The byte floor is a secondary guard: the caller reads
// width/height from the image header (not spoofable rrweb attrs), so this only backstops a malformed
// image whose header a parser might misread as tiny.
export const TINY_MAX_BYTES = 4096
export const TOPIC_MAX_BYTES = 900_000 // under Kafka's ~1MB message cap, leaving room for the envelope

export interface ImageMetadata {
    source: ImageSource
    /** Pixel dimensions if known (from rrweb attrs or a header decode); omit if unknown. */
    width?: number
    height?: number
    /** Size of the raw (decoded-from-base64) image bytes. */
    byteLength: number
}

export function getScrubMethodForImage(i: ImageMetadata): ScrubMethod {
    // 1. Tiny + high-signal + below the detector floor -> keep as-is. Requires BOTH the intrinsic
    //    header dimensions and the byte size to be tiny (the byte floor backstops a misread header on a
    //    malformed image). Unknown dimensions (unreadable header) => scrubbed, never passed through.
    if (
        i.width != null &&
        i.height != null &&
        Math.max(i.width, i.height) <= TINY_MAX_SIDE &&
        i.byteLength <= TINY_MAX_BYTES
    ) {
        return 'passthrough'
    }
    // 2. Canvas is dynamic and dedups ~never -> cheap in-process blur instead of flooding the topic.
    if (i.source === 'canvas') {
        return 'cheapBlur'
    }
    // 3. Can't fit on the topic -> cheap in-process blur fallback (rare; capture drops >~1MB already).
    if (i.byteLength > TOPIC_MAX_BYTES) {
        return 'cheapBlur'
    }
    // 4. Static <img>/media raster -> advanced topic path (dedups, fidelity, PII-bearing).
    return 'advancedScrub'
}
