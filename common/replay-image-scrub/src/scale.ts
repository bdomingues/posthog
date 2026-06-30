/**
 * Multi-process scaling test for BOTH pipelines under the same one-core-per-process pin, so the
 * blur baseline and the advanced path are measured identically and the ratio holds at machine scale.
 * Spawns W workers (each its own process), each grinds WORK_N images; aggregate img/s =
 * sum(imgs) / max(processing-window).
 *
 * Usage: tsx src/scale.ts [w1 w2 ...]   (default: 1 4 8 12)
 */
import { spawn } from 'node:child_process'
import { availableParallelism } from 'node:os'

const WORK_N = process.env.WORK_N ?? '40'
const widths = process.argv
    .slice(2)
    .map(Number)
    .filter((n) => n > 0)
const levels = widths.length ? widths : [1, 4, 8, 12]
const cores = availableParallelism()

function runWorker(mode: string): Promise<{ imgs: number; ms: number }> {
    return new Promise((resolve, reject) => {
        const p = spawn('npx', ['tsx', 'src/worker-proc.ts'], {
            env: {
                ...process.env,
                MODE: mode,
                WORK_N,
                NODE_TLS_REJECT_UNAUTHORIZED: '0',
                // pin every native thread pool to 1 so each worker process ≈ one core
                TF_NUM_INTRAOP_THREADS: '1',
                TF_NUM_INTEROP_THREADS: '1',
                OMP_NUM_THREADS: '1',
                OPENBLAS_NUM_THREADS: '1',
                ORT_THREADS: '1',
                UV_THREADPOOL_SIZE: '2',
            },
        })
        let out = ''
        let err = ''
        p.stdout.on('data', (d) => (out += d))
        p.stderr.on('data', (d) => (err += d))
        p.on('close', (code) => {
            const m = out.indexOf('@@R@@')
            if (code !== 0 || m < 0) {
                return reject(new Error(`worker exited ${code}: ${err.slice(-200)}`))
            }
            resolve(JSON.parse(out.slice(m + 5).trim()))
        })
    })
}

async function sweep(mode: string): Promise<number> {
    let single = 0
    let best = 0
    for (const w of levels) {
        const results = await Promise.all(Array.from({ length: w }, () => runWorker(mode)))
        const imgs = results.reduce((s, r) => s + r.imgs, 0)
        const maxMs = Math.max(...results.map((r) => r.ms))
        const agg = imgs / (maxMs / 1000)
        if (w === 1) {
            single = agg
        }
        best = Math.max(best, agg)
    }
    return best
}

async function main(): Promise<void> {
    const blur = await sweep('blur')
    const adv = await sweep('advanced')
}

main().catch((e) => {
    console.error(e)
    process.exit(1)
})
