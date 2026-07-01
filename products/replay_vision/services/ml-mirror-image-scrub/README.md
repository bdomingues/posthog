# @posthog/ml-mirror-image-scrub

Consumer worker that scrubs inlined images for the session-replay ML-training mirror and writes them
to S3. It ships in two stages:

- **Stage 1 (this package):** a lean sharp-only downsample+blur (`src/blur.ts`), matching what the
  inline anonymizer already produces. No ML deps, so the worker image stays small. This proves the
  plumbing — its own image, the Kafka topic, the producer's batched Redis dedup, and batched S3 writes.
- **Stage 2 (follow-up PR):** swap the consumer's `blurOnly` for the native ML scrub (NSFW gate +
  face mosaic + text solid-fill), and add those ML libraries as `dependencies`.

The goal is to protect data labellers and reduce PII exposure in the training mirror.

## Two sides: producer (nodejs) and consumer (this package)

The **producer** lives in the ml-mirror anonymize pipeline in nodejs
(`nodejs/src/ingestion/pipelines/sessionreplay/ml-mirror/image-scrub/`). Per inlined image it decides:

| image                                        | route         | handling                                                                                                                              |
| -------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| tiny (≤16px long side)                       | `passthrough` | left untouched — below the detector floor, so scrubbing finds nothing, and these icons/logos are high-signal training data            |
| canvas (`<canvas>` pixels, canvas mutations) | `cheap`       | existing in-process downsample+blur — canvas is dynamic and dedups ~never, so it is **never produced** to the topic                   |
| `<img>` / media raster                       | `advanced`    | replace with `image:{team}:{hash}`, dedup in Redis (`SET NX`, 24h), post raw bytes to the topic; the consumer scrubs and writes to S3 |
| oversize (> ~1MB, won't fit the topic)       | `cheap`       | in-process blur fallback                                                                                                              |

So only advanced-route `<img>`/media raster reaches the topic — this **consumer** package reads those
raw images, scrubs them, and writes the result to S3 under the reference. It never routes and never
sees canvas. The `image:{team}:{hash}` reference is team-scoped so dedup and S3 storage stay per-tenant
(identical bytes in two teams never share a scrubbed object). The consumer shares only the `content-ref`
contract with the producer; a golden-vector test pins it on both sides (see `dev/content-ref.test.ts`).

## Layout

`src/` is production (ships in the consumer worker); `dev/` is everything non-production. Production
never imports from `dev/`.

```text
src/  (production — ships)
  consumer.ts     consumer worker: read topic -> scrub -> write S3 (batched)
  blur.ts         Stage-1 scrub: sharp-only downsample+blur (no ML deps)
  clients.ts      S3 helpers + idempotent topic-ensure (no Redis; dedup is producer-side)
  config.ts       env-driven runtime config
  content-ref.ts  the shared contract: parse image:{team}:{hash} -> S3 key (matches the producer)
  metrics.ts      prom-client counters + a /metrics server (scrubbed/failed/mismatch/skip)

dev/  (non-production)
  content-ref.test.ts   pins the image:{team}:{hash} contract with the producer (npm run test:unit)
  produce.ts            thin CLI that posts one image to the topic to exercise the consumer (npm run produce)
```

## Run

```bash
npm install
npm run test:unit    # fast unit tests (no network)
npm run consume      # the consumer worker; `npm run produce -- <img> <team>` to feed it
```

## Packaging / deployment

This worker is owned by the `replay_vision` product, so it lives under
`products/replay_vision/services/` (a service the product deploys — see `docs/internal/monorepo-layout.md`).
It is a **standalone package, deliberately not registered in `pnpm-workspace.yaml`**: it has no
`workspace:*` deps, so keeping it out of the workspace keeps its deps out of the root lockfile and out
of the shared plugin-server image (`nodejs/package.json` ships to every pod). It has its own
`pnpm-lock.yaml`, and `Dockerfile.ml-mirror-image-scrub` (at the repo root) installs
`--prod --frozen-lockfile` against it — so only `dependencies` (sharp, kafkajs, aws-sdk, tsx) land in
the image.

The image builds and deploys via `.github/workflows/ci-ml-mirror-image-scrub-container.yml`, mirroring
`recording-rasterizer` (Depot build -> ECR/ghcr push -> `repository_dispatch` to the charts repo).
