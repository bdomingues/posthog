import { KafkaOffsetManager } from '~/ingestion/pipelines/sessionreplay/kafka/offset-manager'
import { SessionFeatureStore } from '~/ingestion/pipelines/sessionreplay/shared/features/session-feature-store'
import { SessionMetadataSink } from '~/ingestion/pipelines/sessionreplay/shared/metadata/session-metadata-store'
import { KeyStore, RecordingEncryptor } from '~/ingestion/pipelines/sessionreplay/shared/types'

import { SessionBatchFileStorage } from './session-batch-file-storage'
import { SessionBatchRecorder } from './session-batch-recorder'
import { SessionConsoleLogStore } from './session-console-log-store'
import { SessionFilter } from './session-filter'
import { SessionTracker } from './session-tracker'

/**
 * Batch context attached to every element of an accumulation cycle and to the flush units.
 * Carries the recorder that the record step folds into and that the flush step drains.
 */
export interface SessionBatchContext {
    sessionBatchRecorder: SessionBatchRecorder
}

export interface SessionBatchFactoryConfig {
    /** Maximum number of events per session per batch before rate limiting */
    maxEventsPerSessionPerBatch: number
    /** Rollout percentage (0-100) for the per-session ML feature recorder */
    featuresRolloutPercentage?: number
    /** Manages Kafka offset tracking and commits */
    offsetManager: KafkaOffsetManager
    /** Handles writing session batch files to storage */
    fileStorage: SessionBatchFileStorage
    /** Manages storing session metadata */
    metadataStore: SessionMetadataSink
    /** Manages storing console logs */
    consoleLogStore: SessionConsoleLogStore
    /** Manages storing session features for ML scoring */
    featureStore: SessionFeatureStore
    /** Session tracker for new session detection */
    sessionTracker: SessionTracker
    /** Session filter for blocking and rate-limiting sessions */
    sessionFilter: SessionFilter
    /** Key store for session encryption keys */
    keyStore: KeyStore
    /** Encryptor for session recording data */
    encryptor: RecordingEncryptor
}

/**
 * Stateless factory for session batch recorders.
 *
 * Each accumulation cycle of the session replay pipeline gets a fresh recorder from here. The
 * factory holds no current-batch state — the live recorder lives in the pipeline's batch context —
 * which keeps batch lifecycle entirely inside the accumulating pipeline and leaves room for it to
 * run concurrent batches later.
 *
 * How the pieces fit (see `createSessionReplayAccumulatingPipeline` in `session-replay-pipeline.ts`):
 *
 * ```
 * AccumulatingPipeline
 * ├── beforeBatch  → SessionBatchFactory.createBatch()  ── mints the recorder for this cycle
 * ├── pipeline     → recorder.record(message)           ── record step folds events into the recorder
 * └── flush (on size/age trigger)
 *     ├── resolveRetention → retentionService            ── per-session retention, off the S3 path
 *     └── write            → recorder.flushToStorage()   ── S3 write + metadata
 * ```
 *
 * One recorder writes one batch file per retention period, each a sequence of independently-readable,
 * per-session compressed blocks (see {@link SessionBatchRecorder} for the on-disk block layout):
 *
 * ```
 * Session batch (one recorder / one flush)
 * ├── Batch file (30d retention)
 * │   ├── Compressed session block  →  JSONL: [windowId, event], [windowId, event], ...
 * │   └── ...
 * ├── Batch file (1y retention)
 * │   └── ...
 * └── ...
 * ```
 */
export class SessionBatchFactory {
    private readonly maxEventsPerSessionPerBatch: number
    private readonly featuresRolloutPercentage: number
    private readonly offsetManager: KafkaOffsetManager
    private readonly fileStorage: SessionBatchFileStorage
    private readonly metadataStore: SessionMetadataSink
    private readonly consoleLogStore: SessionConsoleLogStore
    private readonly featureStore: SessionFeatureStore
    private readonly sessionTracker: SessionTracker
    private readonly sessionFilter: SessionFilter
    private readonly keyStore: KeyStore
    private readonly encryptor: RecordingEncryptor

    constructor(config: SessionBatchFactoryConfig) {
        this.maxEventsPerSessionPerBatch = config.maxEventsPerSessionPerBatch
        this.featuresRolloutPercentage = config.featuresRolloutPercentage ?? 100
        this.offsetManager = config.offsetManager
        this.fileStorage = config.fileStorage
        this.metadataStore = config.metadataStore
        this.consoleLogStore = config.consoleLogStore
        this.featureStore = config.featureStore
        this.sessionTracker = config.sessionTracker
        this.sessionFilter = config.sessionFilter
        this.keyStore = config.keyStore
        this.encryptor = config.encryptor
    }

    public createBatch(): SessionBatchRecorder {
        return new SessionBatchRecorder(
            this.offsetManager,
            this.fileStorage,
            this.metadataStore,
            this.consoleLogStore,
            this.featureStore,
            this.sessionTracker,
            this.sessionFilter,
            this.keyStore,
            this.encryptor,
            this.maxEventsPerSessionPerBatch,
            this.featuresRolloutPercentage
        )
    }
}
