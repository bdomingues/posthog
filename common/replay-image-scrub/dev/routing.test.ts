import assert from 'node:assert/strict'
import { test } from 'node:test'

import { type RouteInput, TINY_MAX_SIDE, TOPIC_MAX_BYTES, routeImage } from '../src/routing.ts'

const base: RouteInput = { source: 'img', width: 800, height: 600, byteLength: 50_000 }

test('static <img> raster goes to the advanced topic path', () => {
    assert.equal(routeImage(base), 'advanced')
})

test('media raster also goes advanced', () => {
    assert.equal(routeImage({ ...base, source: 'media' }), 'advanced')
})

test('canvas goes to the cheap in-process blur (dynamic, no dedup)', () => {
    assert.equal(routeImage({ ...base, source: 'canvas' }), 'cheap')
})

test('tiny images (<=16px) pass through untouched, for any source', () => {
    assert.equal(
        routeImage({ source: 'img', width: TINY_MAX_SIDE, height: TINY_MAX_SIDE, byteLength: 400 }),
        'passthrough'
    )
    assert.equal(routeImage({ source: 'canvas', width: 8, height: 8, byteLength: 100 }), 'passthrough')
    // 1px on one side but long side over the floor ⇒ not tiny
    assert.equal(routeImage({ source: 'img', width: 1, height: 17, byteLength: 400 }), 'advanced')
})

test('just over the tiny threshold is scrubbed, not passed through', () => {
    assert.equal(routeImage({ source: 'img', width: 17, height: 17, byteLength: 400 }), 'advanced')
})

test('images too big for the topic fall back to cheap in-process blur', () => {
    assert.equal(routeImage({ ...base, byteLength: TOPIC_MAX_BYTES + 1 }), 'cheap')
})

test('unknown dimensions are scrubbed, never passed through', () => {
    assert.equal(routeImage({ source: 'img', byteLength: 50_000 }), 'advanced')
    assert.equal(routeImage({ source: 'img', width: 10, byteLength: 50_000 }), 'advanced') // only one dim known
})
