import { RedisPool } from '~/types'

import { releaseImageKeys, reserveImageKeys } from './redis-dedup'

/** Fake ioredis pipeline recording SET/DEL and returning a scripted exec result. */
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

function fakePool(pipeline: { exec: () => Promise<unknown> }): {
    pool: RedisPool
    state: { acquired: number; released: number }
} {
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
    return { pool, state }
}

describe('ml-mirror/image-scrub redis-dedup', () => {
    it('reserves in one pipelined round-trip and maps OK -> fresh, nil -> duplicate', async () => {
        const pipeline = new FakePipeline([
            [null, 'OK'],
            [null, null],
            [null, 'OK'],
        ])
        const { pool, state } = fakePool(pipeline)

        const fresh = await reserveImageKeys(pool, ['a', 'b', 'c'], 3600)

        expect(fresh).toEqual([true, false, true])
        expect(pipeline.sets).toEqual([
            ['a', '1', 'EX', 3600, 'NX'],
            ['b', '1', 'EX', 3600, 'NX'],
            ['c', '1', 'EX', 3600, 'NX'],
        ])
        expect(state.acquired).toBe(1)
        expect(state.released).toBe(1)
    })

    it('short-circuits an empty batch without touching the pool', async () => {
        const { pool, state } = fakePool(new FakePipeline([]))
        expect(await reserveImageKeys(pool, [], 3600)).toEqual([])
        expect(state.acquired).toBe(0)
    })

    it('releases the pooled client even if the pipeline throws', async () => {
        const pipeline = { del: (): unknown => pipeline, exec: () => Promise.reject(new Error('redis down')) }
        const { pool, state } = fakePool(pipeline)
        await expect(releaseImageKeys(pool, ['a'])).rejects.toThrow('redis down')
        expect(state.released).toBe(1)
    })
})
