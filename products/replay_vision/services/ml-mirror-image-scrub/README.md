# @posthog/ml-mirror-image-scrub

Consumer worker that scrubs inlined images for the session-replay ML-training mirror and writes them
to S3. It ships in two stages:

- **Stage 1 (this deployable):** a lean sharp-only downsample+blur (`src/blur.ts`), matching what the
  inline anonymizer already produces. No ML deps, so the worker image stays small. This proves the
  plumbing — its own image, the Kafka topic, batched Redis dedup, and batched S3 writes.
- **Stage 2:** swap the consumer's `blurOnly` for `advancedScrub` (`src/scrub.ts`) — the full native
  ML scrub below — and promote the ML deps from `devDependencies` to `dependencies`.

The Stage-2 scrub, given an inlined replay image:

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

## Two sides: producer and consumer

The expensive scrub (this package's `scrub.ts`) runs in a **separate consumer worker**, not inline.
The original replay worker (**producer**, `routing.ts` + `content-ref.ts` + `producer.ts`) decides
per image what to do, replaces the inline `rr_dataURL` with a reference, and hands the heavy work off
via a Kafka topic:

| image                                        | route         | handling                                                                                                                             |
| -------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| tiny (≤16px long side)                       | `passthrough` | left untouched — below the detector floor, so scrubbing finds nothing, and these icons/logos are high-signal training data           |
| canvas (`<canvas>` pixels, canvas mutations) | `cheap`       | existing in-process downsample+blur — canvas is dynamic and dedups ~never, so the topic path would flood S3/compute                  |
| `<img>` / media raster                       | `advanced`    | replace with `image:{team}:{hash}`, dedup in Redis (`SET NX`, 24h), post raw bytes to the topic; the consumer blurs and writes to S3 |
| oversize (> ~1MB, won't fit the topic)       | `cheap`       | in-process blur fallback                                                                                                             |

Redis presence means "posted to the topic recently" (dedup), **not** "scrubbed image is in S3" — a
reference can resolve to nothing until the consumer catches up, which is fine. The `image:{team}:{hash}`
reference is team-scoped so dedup and S3 storage stay per-tenant (identical bytes in two teams never
share a scrubbed object). Ports (`DedupStore`/`TopicProducer`) are injected so the producer logic
drops into the real `ml-mirror` pipeline during integration; `*.test.ts` cover it without live infra.

## Layout

`src/` is production (ships in the consumer worker); `dev/` is everything non-production (tests,
benchmarks, the eval harness, local CLIs, data setup). Production never imports from `dev/`.

```text
src/  (production — ships)
  consumer.ts     consumer worker: read topic -> scrub -> write S3 (batched)
  blur.ts         Stage-1 scrub: sharp-only downsample+blur (no ML deps)
  clients.ts      real ports: Redis (pipeline), Kafka, S3
  config.ts       env-driven runtime config
  routing.ts      producer: decide passthrough / cheap-blur / advanced per image
  content-ref.ts  producer: team-scoped content reference image:{team}:{hash}
  producer.ts     producer: batched Redis dedup + post raw images to the scrub topic
  scrub.ts        Stage-2 scrub pipeline: decode-once, NSFW gate, face mosaic + text solid-fill
  yunet.ts        Stage-2 YuNet face detector (ONNX)
  dbnet.ts        Stage-2 DBNet text-region detector (ONNX)
  src-image.ts    Stage-2 decode the source once to raw RGB, shared across stages
  polyfill.ts     Stage-2 Node 23+ util shims so tfjs-node loads

  scrub.ts/yunet.ts/dbnet.ts/src-image.ts/polyfill.ts are Stage-2: their ML deps are devDependencies,
  so they are excluded from the Stage-1 --prod image and are exercised only by the dev/ eval harness.

dev/  (non-production — tests + utilities)
  producer.test.ts routing.test.ts   unit tests (npm run test:unit)
  scrub-eval.ts   OCR + face-redaction eval over downloaded images (npm run eval)
  verify.ts       quick OCR-readability check
  bench.ts scale.ts worker-proc.ts   latency + throughput benchmarks
  make-corpus.ts  synthetic screenshot corpus
  produce.ts      local-dev producer CLI (npm run produce)
  setup.ts        download ONNX models + sample test images (npm run setup)

models/  test-data/  corpus/  out/   downloaded/generated by setup (gitignored)
```

## Run

```bash
npm install          # standalone; see "Packaging" below
npm run setup        # download ONNX models + sample test images, generate the corpus
npm run test:unit    # fast unit tests (no models/network)
npm run eval         # scrub-quality suite (text + face) over real images
npm run bench        # latency + per-stage breakdown
npm run consume      # the consumer worker; `npm run produce -- <img> <team>` to feed it
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

This worker is owned by the `replay_vision` product, so it lives under
`products/replay_vision/services/` (a service the product deploys — see `docs/internal/monorepo-layout.md`).
It is a **standalone package, deliberately not registered in `pnpm-workspace.yaml`**: it has no
`workspace:*` deps, so keeping it out of the workspace keeps its deps (especially the Stage-2 ML
libraries) out of the root lockfile and out of the shared plugin-server image (`nodejs/package.json`
ships to every pod). It has its own `pnpm-lock.yaml`, and `Dockerfile.ml-mirror-image-scrub` (at the
repo root) installs `--prod --frozen-lockfile` against it — so only the Stage-1 `dependencies`
(sharp, kafkajs, ioredis, aws-sdk, tsx) land in the image, not the Stage-2 ML `devDependencies`.

The image builds and deploys via `.github/workflows/ci-ml-mirror-image-scrub-container.yml`, mirroring
`recording-rasterizer` (Depot build -> ECR/ghcr push -> `repository_dispatch` to the charts repo).

Stage 2 (later): swap `blurOnly` -> `advancedScrub` in `consumer.ts`, move the ML libs to
`dependencies`, and bake the ONNX models into the image (don't download at runtime).
