# @posthog/replay-image-scrub (experimental)

Image scrubber for the session-replay ML-training mirror. Given an inlined replay image it:

1. **NSFW/gore gate**: if the image is explicit, it collapses to a 1x1 blank.
2. **Face mosaic**: every detected face is mosaicked (the rest of the frame, e.g. clothing, is kept).
3. **Text redaction**: every detected text region is filled with its **mean colour** (a solid,
   irreversible fill), with a margin scaled to the box height (= font size) and softened edges. We
   detect _where_ text is and never read it.

The goal is to protect data labellers and reduce PII exposure. It does not need to be perfect; the
self-verifying test (below) keeps it honest.

**Why solid fill, not blur/mosaic, for text.** Blur and pixelation are low-pass filters: they remove
fine detail but keep coarse structure, so large text (titles, headings) stays legible to a capable
reader. We confirmed an LLM could still read blurred titles and the opening sentence of a test page.
A solid mean-colour fill removes the information entirely. Faces stay mosaicked (identity is in the
fine detail a mosaic destroys, and the mosaic keeps useful "a person is here" context); text needs
the stronger treatment. Note the edge blur is applied to the text fill's _colour_ only, never the
mask, and never the face mosaic (blurring a mosaic re-smooths it into a detectable face).

## This is native code, not ML-in-JS

All model inference and image processing run in optimized native libraries. The TypeScript is
orchestration plus lightweight output decoding (over small downscaled maps, not full images):

| Stage                                         | Library                              | Native engine       |
| --------------------------------------------- | ------------------------------------ | ------------------- |
| NSFW classify                                 | `nsfwjs` via `@tensorflow/tfjs-node` | libtensorflow (C++) |
| Face detection (YuNet)                        | `onnxruntime-node`                   | ONNX Runtime (C++)  |
| Text detection (DBNet / PP-OCRv3)             | `onnxruntime-node`                   | ONNX Runtime (C++)  |
| resize / blur / pixelate / composite / encode | `sharp`                              | libvips (C++)       |

We do not train anything and run no neural nets in JS. The only hand-written JS is model-output
decoding (DBNet threshold + dilation + connected components, YuNet anchor decode + NMS, tensor
packing, mask fill), which runs over the small detection maps and is not the bottleneck.

## Layout

```text
src/
  scrub.ts        pipeline: decode-once, NSFW gate, face (YuNet) mosaic + text (DBNet) solid-fill
  yunet.ts        YuNet face detector (ONNX): single multi-scale pass, no tiling
  dbnet.ts        DBNet text-region detector (ONNX): threshold + dilation + connected components
  src-image.ts    decode the source PNG once to raw RGB, shared across stages
  verify.ts       quick OCR-readability check on the bundled corpus + sample
  test.ts         the suite: OCR text check + face-redaction check over downloaded images
  bench.ts        per-image latency + per-stage breakdown + in-process concurrency sweep
  scale.ts        multi-process throughput (blur vs advanced), the realistic machine number
  worker-proc.ts  one worker process used by scale.ts
  make-corpus.ts  synthetic screenshot corpus (offline)
models/           downloaded by `npm run setup` (gitignored)
test-data/        downloaded by `npm run setup` (gitignored)
```

## Run

```bash
npm install          # standalone; see "Packaging" below
npm run setup        # download ONNX models + sample test images, generate the corpus
npm run test         # the suite (text + face), non-zero exit on a gated failure
npm run verify       # quick OCR-readability check
npm run bench        # latency + per-stage breakdown
npm run scale        # multi-process throughput (blur vs advanced)
```

Some hosts serve an incomplete TLS chain; the scripts set `NODE_TLS_REJECT_UNAUTHORIZED=0` for the
model/data downloads (PoC only).

## The self-verifying test

The production path _detects_ text with DBNet (fast). The test _reads_ the scrubbed output with OCR
(tesseract, a different model doing recognition not detection) and counts confident multi-character
words. OCR generally reads degraded text better than people, so "OCR can't read it" is a conservative
proxy for "a labeller can't". The face check re-runs YuNet at high sensitivity on the scrubbed output
and asserts no face still sits (by IoU) where one was; a successfully mosaicked face is no longer
detectable.

The suite **gates** on session replay's representative domain (crisp rendered-UI text + faces) and
**reports** on a harder scanned-document set:

```text
UI TEXT (gated):        12/12 clean, 0.0% leak   [PASS]   # rendered screenshots
DOCUMENT TEXT (report): 18/20 clean, 5.2% worst  [report] # faint fax/scan print, out of domain
FACE:                   88/88 faces redacted (100%)
```

Faint, low-contrast scanned-fax lines occasionally survive. That is contrast-limited not size-limited,
so resolution alone won't catch every faded line, and it is outside the rendered-UI domain and within
the "best-effort, not catastrophic if a little gets through" bar. Raise `DET_FACTOR` (env, default
0.75 of the long side) toward 1.0 to spend more CPU on text recall.

## Test data

`npm run setup` pulls a bounded sample via the HuggingFace datasets-server REST API (no Python). It
auto-discovers a config/split and the image column, so swapping datasets is just editing `DATASETS`
in `scripts/setup.ts`. Current defaults (verified that their image assets actually fetch; some
datasets' presigned URLs 403):

- Faces: `tonyassi/celebrity-1000` and `logasja/lfw` (Labeled Faces in the Wild).
- Text: `nielsr/funsd` (forms with dense text and PII-like fields).

Other good sources to drop in: RICO / WebUI for mobile/web UI screenshots, COCO-Text / ICDAR for
scene text.

## Packaging / deployment

This is a **standalone package, deliberately not in the root pnpm workspace**, so its heavy native ML
deps stay out of the main plugin-server image. The plugin-server is one shared image across most
deployments, so anything in `nodejs/package.json` ships to every pod.

Productionization (mirrors `Dockerfile.recording-rasterizer`, the existing per-workflow replay image):

1. Promote to a workspace package (add to `pnpm-workspace.yaml`) so deps are `pnpm --filter`-installable.
2. Add a dedicated Dockerfile + entrypoint that consumes the image-scrub Kafka topic.
3. Bake the ONNX models into the image (don't download at runtime); keep NSFW/face/text inference native.
