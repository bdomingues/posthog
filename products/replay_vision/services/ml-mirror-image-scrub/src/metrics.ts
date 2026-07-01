/** Prometheus counters + a minimal /metrics server for the consumer worker, so scrub failures and
 *  key/content mismatches are alertable rather than only visible as stdout log lines. */
import http from 'node:http'
import { Counter, register } from 'prom-client'

export class ScrubMetrics {
    private static readonly scrubbed = new Counter({
        name: 'ml_mirror_image_scrub_scrubbed_total',
        help: 'Images scrubbed and written to S3',
    })
    private static readonly failed = new Counter({
        name: 'ml_mirror_image_scrub_failed_total',
        help: 'Images whose scrub/S3 write failed (image stays unscrubbed; reference resolves to nothing)',
    })
    private static readonly skipExists = new Counter({
        name: 'ml_mirror_image_scrub_skip_exists_total',
        help: 'Images skipped because the scrubbed object already exists in S3 (idempotent redelivery)',
    })
    private static readonly mismatch = new Counter({
        name: 'ml_mirror_image_scrub_key_content_mismatch_total',
        help: 'Messages dropped because the key hash did not match the value bytes (forged/corrupt key)',
    })

    public static incScrubbed(): void {
        this.scrubbed.inc()
    }
    public static incFailed(): void {
        this.failed.inc()
    }
    public static incSkipExists(): void {
        this.skipExists.inc()
    }
    public static incMismatch(): void {
        this.mismatch.inc()
    }
}

/** Serve /metrics (Prometheus) and /_health, /_ready. Returns a stop function. */
export function startMetricsServer(port = Number(process.env.METRICS_PORT ?? 9090)): () => void {
    const server = http.createServer((req, res) => {
        if (req.url === '/metrics') {
            register
                .metrics()
                .then((body) => {
                    res.setHeader('Content-Type', register.contentType)
                    res.end(body)
                })
                .catch(() => {
                    res.statusCode = 500
                    res.end()
                })
        } else if (req.url === '/_health' || req.url === '/_ready') {
            res.statusCode = 200
            res.end('ok')
        } else {
            res.statusCode = 404
            res.end()
        }
    })
    server.listen(port)
    return () => server.close()
}
