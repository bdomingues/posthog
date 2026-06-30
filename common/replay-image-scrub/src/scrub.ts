/**
 * Image scrubbing PoC. Pipelines over the same input so we can compare throughput:
 *  - blurOnly:  the current production baseline (downsample + gaussian blur), ported from
 *               nodejs/.../anonymize/blur.ts so the comparison is apples-to-apples.
 *  - advancedScrub: NSFW/gore gate -> faces blurred -> text regions blurred.
 *
 * NSFW runs on tfjs (native libtensorflow via tfjs-node, else wasm). Face (YuNet) and text (DBNet)
 * run on native onnxruntime-node, async, so they overlap the synchronous NSFW classify. The source
 * is decoded once to raw RGB and shared across stages.
 * import './polyfill.ts' BEFORE this module so tfjs-node loads on Node 23+.
 */
import * as tf from '@tensorflow/tfjs'
import * as nsfw from 'nsfwjs'
import sharp from 'sharp'

import { type DbnetModel, detectTextDbnet, loadDbnet } from './dbnet.ts'
import { type Src, decodeSrc, srcSharp } from './src-image.ts'
import { type YunetModel, detectFacesYunet, loadYunet } from './yunet.ts'

export type TextMode = 'heuristic' | 'dbnet'

// --- baseline blur (ported from anonymize/blur.ts) ----------------------------------------------
const DOWNSAMPLE_RATIO = 0.12
const BLUR_SIGMA = 2.34
const MAX_LONG_SIDE = 96

export const BLANK_PNG = Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
    'base64'
)

function targetDims(w: number, h: number): [number, number] {
    const scale = Math.min(DOWNSAMPLE_RATIO, MAX_LONG_SIDE / Math.max(w, h))
    return [Math.max(1, Math.round(w * scale)), Math.max(1, Math.round(h * scale))]
}

/** The current worker's whole job for one image: decode -> downsample -> blur -> re-encode. */
export async function blurOnly(input: Buffer): Promise<Buffer> {
    const meta = await sharp(input).metadata()
    const [tw, th] = targetDims(meta.width ?? 1, meta.height ?? 1)
    return sharp(input).resize(tw, th, { fit: 'fill' }).blur(BLUR_SIGMA).png().toBuffer()
}

// --- models -------------------------------------------------------------------------------------
export interface Models {
    nsfw: nsfw.NSFWJS
    dbnet: DbnetModel
    yunet: YunetModel
}

export async function loadModels(
    dbnetPath = 'models/dbnet_det.onnx',
    yunetPath = 'models/yunet.onnx'
): Promise<Models> {
    // Prefer native libtensorflow (tfjs-node) for NSFW; fall back to wasm if it can't load.
    try {
        await import('@tensorflow/tfjs-node') // side effect: registers the 'tensorflow' backend
        await tf.setBackend('tensorflow')
        await tf.ready()
    } catch (e) {
        console.warn('tfjs-node (native) failed, falling back to wasm:', String(e))
        const { setWasmPaths } = await import('@tensorflow/tfjs-backend-wasm')
        setWasmPaths('node_modules/@tensorflow/tfjs-backend-wasm/dist/')
        await tf.setBackend('wasm')
        await tf.ready()
    }
    console.error(`  tfjs backend: ${tf.getBackend()}`)

    const [nsfwModel, dbnet, yunet] = await Promise.all([
        nsfw.load(), // default MobileNetV2 224 model, fetched + cached
        loadDbnet(dbnetPath),
        loadYunet(yunetPath),
    ])
    return { nsfw: nsfwModel, dbnet, yunet }
}

export async function disposeModels(_m: Models): Promise<void> {
    // nothing to tear down
}

// --- advanced pipeline --------------------------------------------------------------------------
export interface StageTimings {
    decodeMs: number
    nsfwMs: number
    faceMs: number
    textMs: number
    composeMs: number
    encodeMs: number
    totalMs: number
    blanked: boolean
    faces: number
    textBoxes: number
}

const NSFW_THRESHOLD = 0.6 // Porn/Hentai/Sexy combined; deliberately loose, this is a safety net
const PNG_LEVEL = Number(process.env.PNG_LEVEL ?? 3) // sharp png compressionLevel; lower = faster, bigger
const PIXELATE_ENV = process.env.PIXELATE ? Number(process.env.PIXELATE) : null

/** Mosaic block size (~px). Scales with resolution so retina text is destroyed, not just softened,
 *  while small images aren't over-blocked. Re-detection by the verifier confirms it's strong enough. */
function pixelateBlock(W: number, H: number): number {
    return PIXELATE_ENV ?? Math.max(10, Math.min(24, Math.round(Math.max(W, H) / 170)))
}

interface Box {
    left: number
    top: number
    width: number
    height: number
}

function clampBox(b: Box, W: number, H: number): Box | null {
    const left = Math.max(0, Math.min(W - 1, Math.round(b.left)))
    const top = Math.max(0, Math.min(H - 1, Math.round(b.top)))
    const width = Math.max(1, Math.min(W - left, Math.round(b.width)))
    const height = Math.max(1, Math.min(H - top, Math.round(b.height)))
    if (width < 2 || height < 2) {
        return null
    }
    return { left, top, width, height }
}

// --- input preparation --------------------------------------------------------------------------
const NSFW_SIZE = 224 // nsfwjs resizes to this internally; feed it pre-shrunk so the resize is cheap

/** Adaptive DBNet input resolution: big enough to resolve small text on retina shots, capped for cost. */
// Detection input resolution as a fraction of the image's long side. 0.75 clears all crisp rendered
// UI (session replay's actual domain) cheaply; raise toward 1.0 for faint/small scanned-document
// print (more CPU), lower for more throughput. Faint low-contrast text is contrast- not size-limited,
// so resolution alone won't catch every faded fax line.
const DET_FACTOR = Number(process.env.DET_FACTOR ?? 0.75)
const DET_CAP = Number(process.env.DET_CAP ?? 1600) // cap so retina screenshots don't explode
function adaptiveDetLimit(W: number, H: number): number {
    const target = Math.round((Math.max(W, H) * DET_FACTOR) / 32) * 32
    return Math.max(736, Math.min(DET_CAP, target))
}

async function nsfwTensor(src: Src): Promise<tf.Tensor3D> {
    const { data } = await srcSharp(src)
        .resize(NSFW_SIZE, NSFW_SIZE, { fit: 'fill' })
        .raw()
        .toBuffer({ resolveWithObject: true })
    return tf.tensor3d(new Uint8Array(data), [NSFW_SIZE, NSFW_SIZE, 3], 'int32')
}

/** Whole worker job for one image, advanced path. Detection is parallelized: DBNet runs on
 *  onnxruntime's background thread (async) while NSFW + face run on tfjs (synchronous), so the
 *  text-detection latency is hidden behind the tfjs compute. */
export async function advancedScrub(
    input: Buffer,
    m: Models,
    textMode: TextMode = 'dbnet'
): Promise<{ out: Buffer; t: StageTimings }> {
    const timings: StageTimings = {
        decodeMs: 0,
        nsfwMs: 0,
        faceMs: 0,
        textMs: 0,
        composeMs: 0,
        encodeMs: 0,
        totalMs: 0,
        blanked: false,
        faces: 0,
        textBoxes: 0,
    }
    const t0 = performance.now()
    const tDec = performance.now()
    const src = await decodeSrc(input) // decode the PNG ONCE; every stage re-wraps these raw pixels
    const { W, H } = src
    timings.decodeMs = performance.now() - tDec

    // 1. NSFW / gore gate FIRST: if it trips we skip all detection. Running it first (rather than
    //    overlapping detection) keeps each worker ~1 core, which packs better under multi-process
    //    scaling — the throughput-bound case. Set PARALLEL_DETECT=1 to overlap instead (lower latency
    //    per image, but each worker uses more cores).
    const tN = performance.now()
    const nt = await nsfwTensor(src)
    let bad = 0
    try {
        const preds = await m.nsfw.classify(nt as unknown as tf.Tensor3D)
        bad = preds
            .filter((p) => p.className === 'Porn' || p.className === 'Hentai' || p.className === 'Sexy')
            .reduce((s, p) => s + p.probability, 0)
    } finally {
        nt.dispose()
    }
    timings.nsfwMs = performance.now() - tN
    if (bad >= NSFW_THRESHOLD) {
        timings.blanked = true
        timings.totalMs = performance.now() - t0
        return { out: BLANK_PNG, t: timings }
    }

    // 2. Face (YuNet) + text (DBNet), both native ORT. Serial by default (1 core/worker); parallel opt-in.
    const det = adaptiveDetLimit(W, H)
    const runText = (): Promise<Box[]> =>
        textMode === 'dbnet' ? detectTextDbnet(m.dbnet, src, W, H, { detLimit: det }) : detectTextRegions(input, W, H)
    let faceBoxes: Box[]
    let textBoxes: Box[]
    if (process.env.PARALLEL_DETECT === '1') {
        const tD = performance.now()
        ;[faceBoxes, textBoxes] = await Promise.all([detectFacesYunet(m.yunet, src, W, H), runText()])
        timings.faceMs = timings.textMs = performance.now() - tD
    } else {
        const tF = performance.now()
        faceBoxes = await detectFacesYunet(m.yunet, src, W, H)
        timings.faceMs = performance.now() - tF
        const tT = performance.now()
        textBoxes = await runText()
        timings.textMs = performance.now() - tT
    }
    timings.faces = faceBoxes.length
    timings.textBoxes = textBoxes.length

    const out = await compose(src, W, H, faceBoxes, textBoxes, timings)
    timings.totalMs = performance.now() - t0
    return { out, t: timings }
}

/**
 * Model-free text detector. Text has high local edge density, so: downscale to grayscale, compute
 * a gradient map, tile it, and mark tiles whose mean gradient is high (but not saturated, which
 * filters out hard image/photo edges). Returns the texty tiles as boxes in full-res coords. Rough,
 * but we only need "blur where text is", not character-accurate boxes.
 */
const TEXT_DS_WIDTH = 480 // downscale width for the gradient pass
const TEXT_TILE = 10 // tile size in downscaled px
const TEXT_EDGE_T = 22 // mean gradient threshold for a tile to count as text

async function detectTextRegions(input: Buffer, W: number, H: number): Promise<Box[]> {
    const dsW = Math.min(W, TEXT_DS_WIDTH)
    const sx = W / dsW
    const dsH = Math.max(1, Math.round(H / sx))
    const { data, info } = await sharp(input)
        .grayscale()
        .resize(dsW, dsH, { fit: 'fill' })
        .raw()
        .toBuffer({ resolveWithObject: true })
    const w = info.width
    const h = info.height
    const cols = Math.ceil(w / TEXT_TILE)
    const rows = Math.ceil(h / TEXT_TILE)
    const sum = new Float64Array(cols * rows)
    const cnt = new Int32Array(cols * rows)
    const sat = new Int32Array(cols * rows) // count of very-strong edges (likely photo/icon, not text)

    for (let y = 1; y < h - 1; y++) {
        const row = y * w
        for (let x = 1; x < w - 1; x++) {
            const i = row + x
            const g = Math.abs(data[i + 1] - data[i - 1]) + Math.abs(data[i + w] - data[i - w])
            const ci = Math.floor(y / TEXT_TILE) * cols + Math.floor(x / TEXT_TILE)
            sum[ci] += g
            cnt[ci]++
            if (g > 200) {
                sat[ci]++
            }
        }
    }

    const boxes: Box[] = []
    for (let r = 0; r < rows; r++) {
        for (let c = 0; c < cols; c++) {
            const ci = r * cols + c
            const n = cnt[ci]
            if (n === 0) {
                continue
            }
            const mean = sum[ci] / n
            const satFrac = sat[ci] / n
            if (mean > TEXT_EDGE_T && satFrac < 0.12) {
                const b = clampBox(
                    {
                        left: c * TEXT_TILE * sx,
                        top: r * TEXT_TILE * sx,
                        width: TEXT_TILE * sx,
                        height: TEXT_TILE * sx,
                    },
                    W,
                    H
                )
                if (b) {
                    boxes.push(b)
                }
            }
        }
    }
    return boxes
}

/** Blur the union of face + text regions back onto the base image. Box-count-independent: blur the
 *  whole frame once, keep only the masked regions (dest-in), composite back over the original. */
async function compose(
    src: Src,
    W: number,
    H: number,
    faceBoxes: Box[],
    textBoxes: Box[],
    timings: StageTimings
): Promise<Buffer> {
    const tC = performance.now()
    const allBoxes = [...faceBoxes, ...textBoxes]
    if (allBoxes.length === 0) {
        timings.composeMs = performance.now() - tC
        const tE0 = performance.now()
        const out0 = await srcSharp(src).png({ compressionLevel: PNG_LEVEL }).toBuffer()
        timings.encodeMs = performance.now() - tE0
        return out0
    }

    // Pixelate (downscale -> nearest-neighbour upscale) instead of a full-frame gaussian blur:
    // de-identifies text/faces just as well (mosaic) and is cheaper than blur(sigma).
    // NOTE: sharp applies only ONE resize per pipeline, so the down- and up-scale MUST be two
    // separate sharp() calls — chaining them in one pipeline silently drops the downscale.
    const block = pixelateBlock(W, H)
    const pw = Math.max(1, Math.round(W / block))
    const ph = Math.max(1, Math.round(H / block))
    // Keep intermediates as raw RGB (no PNG round-trips). NOTE: sharp does ONE resize per pipeline,
    // so down- and up-scale are two separate pipelines.
    const small = await srcSharp(src).resize(pw, ph, { fit: 'fill' }).raw().toBuffer()
    const pixelatedFull = await sharp(small, { raw: { width: pw, height: ph, channels: 3 } })
        .resize(W, H, { fit: 'fill', kernel: 'nearest' })
        .raw()
        .toBuffer()

    // Build the keep-mask as a raw single-channel alpha buffer (255 inside boxes). Much cheaper than
    // rasterizing an SVG with ~100 <rect>s over a multi-megapixel canvas.
    const alpha = new Uint8Array(W * H)
    for (const b of allBoxes) {
        for (let y = b.top; y < b.top + b.height; y++) {
            alpha.fill(255, y * W + b.left, y * W + b.left + b.width)
        }
    }
    const pixelatedMasked = await sharp(pixelatedFull, { raw: { width: W, height: H, channels: 3 } })
        .joinChannel(Buffer.from(alpha.buffer), { raw: { width: W, height: H, channels: 1 } })
        .png()
        .toBuffer()

    timings.composeMs = performance.now() - tC
    const tE = performance.now()
    const out = await srcSharp(src)
        .composite([{ input: pixelatedMasked, left: 0, top: 0 }])
        .png({ compressionLevel: PNG_LEVEL })
        .toBuffer()
    timings.encodeMs = performance.now() - tE
    return out
}
