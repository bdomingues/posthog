/** Concrete image-scrub ports over the plugin-server's Redis pool and Kafka producer, injected into
 *  the anonymize ScrubContext at the ml-mirror server. The pure emit logic lives in producer.ts. */
import { KafkaProducerWrapper } from '~/common/kafka/producer'
import { RedisPool } from '~/types'

import { DedupStore, TopicMessage, TopicProducer } from './producer'

/** Redis-backed dedup over the shared pool. Batches a message's keys into ONE round-trip with an
 *  ioredis pipeline: `SET key 1 EX ttl NX` reserves on first sighting ('OK'); a nil reply means a
 *  recent duplicate. Acquires and releases a pooled client per call. */
export class RedisPoolDedupStore implements DedupStore {
    constructor(private pool: RedisPool) {}

    async reserveBatch(keys: string[], ttlSeconds: number): Promise<boolean[]> {
        if (keys.length === 0) {
            return []
        }
        const client = await this.pool.acquire()
        try {
            const pipeline = client.pipeline()
            for (const key of keys) {
                pipeline.set(key, '1', 'EX', ttlSeconds, 'NX')
            }
            const raw = await pipeline.exec() // one round-trip; ordered results
            return (raw ?? []).map(([err, res]) => !err && res === 'OK') // 'OK' = we set it (fresh)
        } finally {
            await this.pool.release(client)
        }
    }

    async releaseBatch(keys: string[]): Promise<void> {
        if (keys.length === 0) {
            return
        }
        const client = await this.pool.acquire()
        try {
            const pipeline = client.pipeline()
            for (const key of keys) {
                pipeline.del(key)
            }
            await pipeline.exec()
        } finally {
            await this.pool.release(client)
        }
    }
}

/** Kafka-backed producer over a shared KafkaProducerWrapper. Produces all of a batch's messages and
 *  resolves once the broker has acked them (rdkafka batches the sends internally). */
export class KafkaWrapperTopicProducer implements TopicProducer {
    constructor(
        private producer: KafkaProducerWrapper,
        private topic: string
    ) {}

    async produceBatch(messages: TopicMessage[]): Promise<void> {
        if (messages.length === 0) {
            return
        }
        await Promise.all(
            messages.map((m) => this.producer.produce({ topic: this.topic, key: Buffer.from(m.key), value: m.value }))
        )
    }
}
