/** Media detection + placeholder/blur dispatch. */
import { ImageSource, routeImage } from '~/ingestion/pipelines/sessionreplay/ml-mirror/image-scrub/routing'

import { BLANK_IMAGE_DATA_URI, blurImageBytes, isImageDataUri } from './blur'
import { ScrubContext } from './config'
import { scrubUrl } from './url'

// Bound how much a single message can hand off to the scrub topic, so an outlier session with many
// large inlined images can't pin unbounded memory across the emit. Overflow falls back to cheap blur.
const MAX_ADVANCED_IMAGES_PER_MESSAGE = 64
const MAX_ADVANCED_BYTES_PER_MESSAGE = 32 * 1024 * 1024 // 32 MB

// rrweb inlines rendered pixels (a `toDataURL()` snapshot) into this attribute — for `<canvas>`
// in a FullSnapshot/adds, and for `<img>` when image inlining is on. It holds raw drawn content.
export const INLINE_IMAGE_ATTR = 'rr_dataURL'

export const PLACEHOLDER_SRC =
    "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 80 80'><rect width='80' height='80' fill='%23f3f4f6'/><rect x='6' y='6' width='68' height='68' fill='none' stroke='%23d1d5db' stroke-width='2' rx='6'/><circle cx='26' cy='26' r='6' fill='%239ca3af'/><path d='M14 60 L34 40 L48 50 L66 32 L66 66 L14 66 Z' fill='%239ca3af'/></svg>"

export const MEDIA_SRC_ATTRS = ['src', 'srcset', 'href', 'xlink:href', 'poster']

export function isMediaTag(tag: string): boolean {
    switch (tag.toLowerCase()) {
        case 'img':
        case 'image':
        case 'video':
        case 'audio':
        case 'source':
        case 'track':
        case 'picture':
            return true
        default:
            return false
    }
}

export function isMediaSrcAttr(name: string): boolean {
    return MEDIA_SRC_ATTRS.includes(name)
}

/** True if an attribute map contains any media-source attribute. */
export function hasMediaSrcAttr(attrs: Record<string, unknown>): boolean {
    return MEDIA_SRC_ATTRS.some((name) => Object.prototype.hasOwnProperty.call(attrs, name))
}

/** True if the bytes start with a known raster-image magic (PNG/JPEG/GIF/WEBP/BMP). The `data:image/…`
 *  header is attacker-controlled, so this rejects a bogus base64 payload that would otherwise be posted
 *  to the topic only to fail decode downstream. rrweb inlines PNG/JPEG/WEBP, all covered here. */
function looksLikeRasterImage(b: Buffer): boolean {
    if (b.length < 12) {
        return false
    }
    const png = b[0] === 0x89 && b[1] === 0x50 && b[2] === 0x4e && b[3] === 0x47
    const jpeg = b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff
    const gif = b[0] === 0x47 && b[1] === 0x49 && b[2] === 0x46 && b[3] === 0x38
    const webp =
        b[0] === 0x52 &&
        b[1] === 0x49 &&
        b[2] === 0x46 &&
        b[3] === 0x46 &&
        b[8] === 0x57 &&
        b[9] === 0x45 &&
        b[10] === 0x42 &&
        b[11] === 0x50
    const bmp = b[0] === 0x42 && b[1] === 0x4d
    return png || jpeg || gif || webp || bmp
}

/** Raw bytes of an image data URI's base64 payload, or null if it isn't a base64 raster image. */
function imageDataUriBytes(dataUri: string): Buffer | null {
    const comma = dataUri.indexOf(',')
    if (comma < 0) {
        return null
    }
    const meta = dataUri.slice('data:'.length, comma)
    if (!meta.includes('base64') || !meta.startsWith('image/')) {
        return null
    }
    const bytes = Buffer.from(dataUri.slice(comma + 1), 'base64')
    return looksLikeRasterImage(bytes) ? bytes : null
}

/** Coerce an rrweb width/height attribute (number or numeric string) to a positive number, else undefined. */
function toDim(v: unknown): number | undefined {
    const n = typeof v === 'number' ? v : typeof v === 'string' ? Number(v) : NaN
    return Number.isFinite(n) && n > 0 ? n : undefined
}

/**
 * Scrub one inlined image in `attrs[name]` by the routing policy:
 *  - advanced (static <img>/media raster, ml-mirror ports present): fail-safe placeholder now, then
 *    collect the raw bytes for the batched emit to the scrub topic; the reference is written in place
 *    once the emit resolves (consumer scrubs -> S3).
 *  - cheap (canvas, oversize, or ports absent): the existing in-process downsample+blur.
 *  - passthrough (tiny): leave untouched.
 * Returns whether it acted on the attribute.
 */
function scrubInlineImage(
    ctx: ScrubContext,
    attrs: Record<string, unknown>,
    name: string,
    source: ImageSource,
    placeholder: string
): boolean {
    const value = attrs[name]
    if (typeof value !== 'string' || !isImageDataUri(value)) {
        return false
    }
    const bytes = imageDataUriBytes(value)
    if (bytes === null) {
        return false
    }
    const route = routeImage({
        source,
        width: toDim(attrs.width),
        height: toDim(attrs.height),
        byteLength: bytes.length,
    })
    if (route === 'passthrough') {
        return false
    }
    const jobs = ctx.imageScrubJobs
    const underCap =
        jobs != null &&
        jobs.length < MAX_ADVANCED_IMAGES_PER_MESSAGE &&
        jobs.reduce((n, j) => n + j.bytes.length, bytes.length) <= MAX_ADVANCED_BYTES_PER_MESSAGE
    if (route === 'advanced' && ctx.imageScrub && ctx.teamId != null && jobs != null && underCap) {
        attrs[name] = placeholder // fail-safe until the reference is written in place after the emit
        jobs.push({
            bytes,
            apply: (ref) => {
                attrs[name] = ref
            },
        })
        return true
    }
    // cheap: in-process blur — for canvas/oversize, when ports/team are absent, or over the per-message
    // cap. Reuses the already-decoded bytes (no second base64 decode).
    attrs[name] = placeholder
    ctx.blurJobs?.push(async () => {
        const blurred = await blurImageBytes(bytes)
        if (blurred !== null) {
            attrs[name] = blurred
        }
    })
    return true
}

/**
 * Scrub an inlined-image data URI held in an attribute (a `<canvas>`/`<img>` `rr_dataURL`). Canvas is
 * dynamic (routed cheap); a static <img>'s inline pixels take the advanced topic path when wired.
 * Returns whether it acted.
 */
export function blurInlineImageAttr(
    ctx: ScrubContext,
    attrs: Record<string, unknown>,
    name: string,
    source: ImageSource = 'canvas'
): boolean {
    return scrubInlineImage(ctx, attrs, name, source, BLANK_IMAGE_DATA_URI)
}

/** Replace a media element's source attrs with the placeholder (routing inline data-images to the
 *  scrub topic or the in-process blur; remote srcs are host+path scrubbed and stashed). */
export function applyBlur(ctx: ScrubContext, attrs: Record<string, unknown>): void {
    for (const key of MEDIA_SRC_ATTRS) {
        const existing = attrs[key]
        if (typeof existing !== 'string') {
            continue
        }
        if (isImageDataUri(existing)) {
            scrubInlineImage(ctx, attrs, key, 'media', PLACEHOLDER_SRC)
        } else {
            // Stash the scrubbed original under a namespaced attr (won't collide with app
            // `data-original-*`), host-scrubbed too so the CDN host can't leak.
            const scrubbed = scrubUrl(ctx, existing, { scrubAuthority: true })
            attrs[key] = PLACEHOLDER_SRC
            attrs[`data-anon-original-${key}`] = scrubbed.changed ? scrubbed.value : existing
        }
    }
}
