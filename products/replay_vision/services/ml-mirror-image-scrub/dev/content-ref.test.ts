import assert from 'node:assert/strict'
import { test } from 'node:test'

import { hashImageBytes, imageRef, parseImageRef, s3KeyForRef } from '../src/content-ref.ts'

// CONTRACT: these golden vectors MUST match the producer's content-ref in nodejs (its
// image-scrub.test.ts asserts the same input -> hash/ref), or references written by the producer
// won't resolve to the S3 keys this consumer writes. Change both together.
const INPUT = 'posthog-image-scrub-contract-v1'
const HASH = 'q1YIODUgcFH6CgV1DOI4SU'

test('hashes to the golden 22-char base64url content hash', () => {
    assert.equal(hashImageBytes(Buffer.from(INPUT)), HASH)
})

test('builds the golden team-scoped reference', () => {
    assert.equal(imageRef(42, HASH), `image:42:${HASH}`)
})

test('derives the golden S3 key from the reference', () => {
    assert.equal(s3KeyForRef(imageRef(42, HASH)), `scrubbed-images/team_id=42/${HASH}.png`)
})

test('round-trips ref -> parse', () => {
    const p = parseImageRef(imageRef(42, HASH))
    assert.equal(p?.teamId, 42)
    assert.equal(p?.hash, HASH)
})
