import { KafkaProducerWrapper } from '~/common/kafka/producer'
import { RedisPool } from '~/types'

import { KafkaWrapperTopicProducer, RedisPoolDedupStore } from './ports'

/** Fake ioredis pipeline that records SET/DEL commands and returns a scripted exec result. */
class FakePipeline {
    sets: Array<[string, string, string, number, string]> = []
    dels: string[] = []
    constructor(private execResult: [Error | null, unknown][]) {}
    set(...args: [string, string, string, number, string]): this {
        this.sets.push(args)
        return this
    }
    del(key: string): this {
        this.dels.push(key)
        return this
    }
    exec(): Promise<[Error | null, unknown][]> {
        return Promise.resolve(this.execResult)
    }
}

describe('ml-mirror/image-scrub ports', () => {
    describe('RedisPoolDedupStore', () => {
        it('reserves in one pipelined round-trip and maps OK -> fresh, nil -> duplicate', async () => {
            const pipeline = new FakePipeline([
                [null, 'OK'],
                [null, null],
                [null, 'OK'],
            ])
            const state = { acquired: 0, released: 0 }
            const client = { pipeline: () => pipeline }
            const pool = {
                acquire: () => {
                    state.acquired++
                    return Promise.resolve(client)
                },
                release: () => {
                    state.released++
                    return Promise.resolve()
                },
            } as unknown as RedisPool

            const fresh = await new RedisPoolDedupStore(pool).reserveBatch(['a', 'b', 'c'], 3600)

            expect(fresh).toEqual([true, false, true])
            expect(pipeline.sets).toEqual([
                ['a', '1', 'EX', 3600, 'NX'],
                ['b', '1', 'EX', 3600, 'NX'],
                ['c', '1', 'EX', 3600, 'NX'],
            ])
            expect(state.acquired).toBe(1) // one round-trip
            expect(state.released).toBe(1) // client always returned to the pool
        })

        it('short-circuits an empty batch without touching the pool', async () => {
            const pool = {
                acquire: () => Promise.reject(new Error('should not acquire')),
                release: () => Promise.resolve(),
            } as unknown as RedisPool
            expect(await new RedisPoolDedupStore(pool).reserveBatch([], 3600)).toEqual([])
        })

        it('releases the pooled client even if the pipeline throws', async () => {
            let released = false
            const client = {
                pipeline: () => ({
                    del: () => client.pipeline(),
                    exec: () => Promise.reject(new Error('redis down')),
                }),
            }
            const pool = {
                acquire: () => Promise.resolve(client),
                release: () => {
                    released = true
                    return Promise.resolve()
                },
            } as unknown as RedisPool
            await expect(new RedisPoolDedupStore(pool).releaseBatch(['a'])).rejects.toThrow('redis down')
            expect(released).toBe(true)
        })
    })

    describe('KafkaWrapperTopicProducer', () => {
        it('produces every message to the topic with a Buffer key, resolving after all acks', async () => {
            const produced: Array<{ topic: string; key: Buffer; value: Buffer }> = []
            const wrapper = {
                produce: (m: { topic: string; key: Buffer; value: Buffer }) => {
                    produced.push(m)
                    return Promise.resolve()
                },
            } as unknown as KafkaProducerWrapper

            await new KafkaWrapperTopicProducer(wrapper, 'the_topic').produceBatch([
                { key: 'image:42:aaa', value: Buffer.from('one') },
                { key: 'image:42:bbb', value: Buffer.from('two') },
            ])

            expect(produced).toHaveLength(2)
            expect(produced[0].topic).toBe('the_topic')
            expect(Buffer.isBuffer(produced[0].key)).toBe(true)
            expect(produced[0].key.toString()).toBe('image:42:aaa')
            expect(produced[1].value.toString()).toBe('two')
        })

        it('does nothing for an empty batch', async () => {
            let called = false
            const wrapper = {
                produce: () => {
                    called = true
                    return Promise.resolve()
                },
            } as unknown as KafkaProducerWrapper
            await new KafkaWrapperTopicProducer(wrapper, 't').produceBatch([])
            expect(called).toBe(false)
        })
    })
})
