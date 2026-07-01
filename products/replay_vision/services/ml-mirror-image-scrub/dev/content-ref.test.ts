import assert from 'node:assert/strict'
import { test } from 'node:test'

import { hashImageBytes, imageRef, isImageRef, parseImageRef } from '../src/content-ref.ts'

// CONTRACT: these golden vectors MUST match the producer's content-ref in nodejs (its
// image-scrub.test.ts asserts the same input -> hash/ref), or references written by the producer
// won't resolve to what this consumer indexes. Change both together.
const INPUT = 'posthog-image-scrub-contract-v1'
const HASH = 'q1YIODUgcFH6CgV1DOI4SU'

test('hashes to the golden 22-char base64url content hash', () => {
    assert.equal(hashImageBytes(Buffer.from(INPUT)), HASH)
})

test('builds the golden team-scoped reference', () => {
    assert.equal(imageRef(42, HASH), `image:42:${HASH}`)
})

test('round-trips ref -> parse', () => {
    const p = parseImageRef(imageRef(42, HASH))
    assert.equal(p?.teamId, 42)
    assert.equal(p?.hash, HASH)
})

test('is team-scoped: same bytes in different teams get different refs, same hash (tenant isolation)', () => {
    const bytes = Buffer.from('logo-png-bytes')
    const a = imageRef(42, hashImageBytes(bytes))
    const b = imageRef(99, hashImageBytes(bytes))
    assert.notEqual(a, b)
    assert.equal(parseImageRef(a)?.hash, parseImageRef(b)?.hash)
})

test('isImageRef accepts a reference and rejects a raw data URI', () => {
    assert.ok(isImageRef(imageRef(7, hashImageBytes(Buffer.from('x')))))
    assert.equal(isImageRef('data:image/png;base64,iVBORw0KG'), false)
})
