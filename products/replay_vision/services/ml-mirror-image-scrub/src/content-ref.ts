/**
 * Content reference for an inlined replay image. The original worker replaces the raw `rr_dataURL`
 * in the recorded block with one of these strings; the consumer scrubs the image and writes it into a
 * team-scoped shard, and the training side resolves block -> image by looking the hash up in the team's
 * parquet index (shard, offset, length) and range-fetching the bytes. See shard-store.ts.
 *
 * Format: `image:{team_id}:{hash}` where hash is a 132-bit content hash (sha256, base64url, 22 chars).
 *
 * The team_id prefix is deliberate and load-bearing: it scopes dedup and storage PER TEAM, so the same
 * image bytes seen in two teams produce two references and never share a shard/index entry — keeping
 * tenant isolation intact (no cross-team data sharing via a shared content hash).
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

export function parseImageRef(ref: string): { teamId: number; hash: string } | null {
    const m = REF_RE.exec(ref)
    return m ? { teamId: Number(m[1]), hash: m[2] } : null
}
