import { ScrubMetrics } from './metrics.ts'
/** Accumulates scrubbed images across Kafka batches and flushes them as shard + index objects once the
 *  buffer is large enough (images or bytes) or old enough. Mirrors the ml-mirror block-metadata batcher.
 *  The caller commits Kafka offsets only after a flush succeeds, so a failed write replays (at-least-once). */
import { ImageShardStore, ScrubbedImage } from './shard-store.ts'

export interface BatcherOptions {
    maxImages: number
    maxBytes: number
    flushIntervalMs: number
}

export class ImageBatcher {
    private buffer: ScrubbedImage[] = []
    private bufferBytes = 0
    private lastFlushMs: number

    constructor(
        private readonly store: ImageShardStore,
        private readonly options: BatcherOptions,
        nowMs: number
    ) {
        this.lastFlushMs = nowMs
    }

    public add(image: ScrubbedImage): void {
        this.buffer.push(image)
        this.bufferBytes += image.bytes.length
    }

    public get size(): number {
        return this.buffer.length
    }

    public shouldFlush(nowMs: number): boolean {
        if (this.buffer.length >= this.options.maxImages || this.bufferBytes >= this.options.maxBytes) {
            return true
        }
        return this.buffer.length > 0 && nowMs - this.lastFlushMs >= this.options.flushIntervalMs
    }

    /**
     * Group the buffered images by team and write one shard + index per team. Snapshots and clears the
     * buffer up front so images added concurrently aren't dropped; throws if a write fails (the caller
     * then doesn't commit offsets, so Kafka redelivers those images — the buffered snapshot being lost
     * is safe because its offsets were never committed).
     */
    public async flush(nowMs: number): Promise<void> {
        this.lastFlushMs = nowMs
        if (this.buffer.length === 0) {
            return
        }
        const batch = this.buffer
        this.buffer = []
        this.bufferBytes = 0

        const byTeam = new Map<number, ScrubbedImage[]>()
        for (const image of batch) {
            const images = byTeam.get(image.teamId)
            if (images) {
                images.push(image)
            } else {
                byTeam.set(image.teamId, [image])
            }
        }
        for (const [teamId, images] of byTeam) {
            const { bytes } = await this.store.writeTeam(teamId, images)
            ScrubMetrics.observeShard(images.length, bytes)
        }
    }
}
