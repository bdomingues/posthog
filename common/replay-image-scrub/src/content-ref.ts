/**
 * Content reference for an inlined replay image. The original worker replaces the raw `rr_dataURL`
 * in the recorded block with one of these strings; the blur consumer writes the scrubbed image to
 * S3 under the same reference, and the training side resolves block -> image by it.
 *
 * Format: `image:{team_id}:{hash}` where hash is a 132-bit content hash (sha256, base64url, 22 chars).
 *
 * The team_id prefix is deliberate and load-bearing: it scopes dedup and S3 storage PER TEAM, so the
 * same image bytes seen in two teams produce two references and never share a scrubbed object. That
 * keeps tenant isolation intact (no cross-team data sharing via a shared content hash).
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

/** S3 object key for a reference. Sharded by team then hash so a team's images live under one prefix
 *  and listings/lifecycle can target a team. */
export function s3KeyForRef(ref: string): string {
    const p = parseImageRef(ref)
    if (!p) {
        throw new Error(`not an image reference: ${ref}`)
    }
    return `scrubbed-images/team_id=${p.teamId}/${p.hash}.png`
}
