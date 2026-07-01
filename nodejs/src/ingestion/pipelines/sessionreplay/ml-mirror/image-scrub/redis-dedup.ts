/** Redis dedup for the image-scrub topic, over the shared session-replay Redis pool. Follows the same
 *  inline acquire/release pattern as the other session-replay services (retention-service, redis-cache):
 *  one pipelined round-trip per call, client always returned to the pool. */
import { RedisPool } from '~/types'

/** Reserve every absent key in ONE round-trip (`SET key 1 EX ttl NX`). Returns, per key in order,
 *  whether we reserved it (true = first sighting; a nil reply means a recent duplicate). */
export async function reserveImageKeys(pool: RedisPool, keys: string[], ttlSeconds: number): Promise<boolean[]> {
    if (keys.length === 0) {
        return []
    }
    const client = await pool.acquire()
    try {
        const pipeline = client.pipeline()
        for (const key of keys) {
            pipeline.set(key, '1', 'EX', ttlSeconds, 'NX')
        }
        const raw = await pipeline.exec() // one round-trip; ordered results
        return (raw ?? []).map(([err, res]) => !err && res === 'OK') // 'OK' = we set it (fresh)
    } finally {
        await pool.release(client)
    }
}

/** Delete reservations in ONE round-trip (pipeline of DEL) — rolls back a failed produce. */
export async function releaseImageKeys(pool: RedisPool, keys: string[]): Promise<void> {
    if (keys.length === 0) {
        return
    }
    const client = await pool.acquire()
    try {
        const pipeline = client.pipeline()
        for (const key of keys) {
            pipeline.del(key)
        }
        await pipeline.exec()
    } finally {
        await pool.release(client)
    }
}
