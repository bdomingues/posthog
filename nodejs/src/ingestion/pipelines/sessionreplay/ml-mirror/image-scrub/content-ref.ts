/**
 * Content reference for an inlined replay image. The ml-mirror producer (this side) replaces the raw
 * `rr_dataURL` in the recorded block with one of these strings; the image-scrub consumer writes the
 * scrubbed image to S3 under the same reference, and the training side resolves block -> image by it.
 *
 * Format: `image:{team_id}:{hash}` where hash is a 132-bit content hash (sha256, base64url, 22 chars).
 *
 * The team_id prefix is deliberate and load-bearing: it scopes dedup and S3 storage PER TEAM, so the
 * same image bytes seen in two teams produce two references and never share a scrubbed object.
 *
 * CONTRACT: this MUST stay byte-identical to the consumer's copy at
 * products/replay_vision/services/ml-mirror-image-scrub/src/content-ref.ts, or references won't
 * resolve. Both sides assert the same hardcoded golden vector (input -> hash/ref) — image-scrub.test.ts
 * here and dev/content-ref.test.ts there — so a unilateral change to either copy fails its own test;
 * change them together. Stage 2 should extract a single shared package and delete this duplicate.
 */
import { createHash } from 'node:crypto'

const PREFIX = 'image'
const REF_RE = /^image:(\d+):([A-Za-z0-9_-]{22})$/

/** 132-bit content hash of the raw image bytes, base64url. Content-only (not team-scoped) so the
 *  team prefix in the reference is what provides per-tenant separation. */
export function hashImageBytes(bytes: Buffer): string {
    return createHash('sha256').update(bytes).digest('base64url').slice(0, 22)
}

export function imageRef(teamId: number, hash: string): string {
    return `${PREFIX}:${teamId}:${hash}`
}

export function isImageRef(s: string): boolean {
    return REF_RE.test(s)
}
