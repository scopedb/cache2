//! Inclusive hybrid cache coordinator for mixed small and large objects.
//!
//! The default mode is a performance-first write-back cache. Mutations publish
//! to L1 after allocating an in-memory version. Dirty victims normally reach
//! SSD on eviction or flush; under executor pressure they may instead invalidate
//! lower visibility in memory and become misses. One session-level dirty fence
//! makes an unclean restart, or the next clean boundary after such loss, start
//! from an empty cache without per-key journal writes or durability syncs.

use std::cmp::Ordering as CmpOrdering;
use std::fmt;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::async_cache::{AsyncFailure, AsyncQueueStats, TaskContext};
use crate::async_hybrid::{AsyncHybridCloseStats, HybridCloseCompletion};
use crate::bucket_engine::{
    BucketCache, BucketCacheConfig, BucketCacheStats, BucketConfigDiagnostics, BucketEntryUsage,
};
use crate::cache::{
    CacheConfig, CacheError, CacheStats, CacheStatus, DiskCache, ManagedPutCommit, PutOptions,
    PutOutcome, RecoveryMode, RejectReason, RemoveOutcome, Result,
};
use crate::diagnostics::ConfigDiagnostics;
#[cfg(test)]
use crate::format::RecordHeader;
#[cfg(test)]
use crate::hybrid_crash::{HybridCrashPoint, hit as crash_hit};
use crate::hybrid_journal::{
    JournalGroupCommit, JournalGroupCommitConfig, JournalGroupCommitSnapshot,
    MAX_DURABILITY_SYNC_GROUPS,
};
use crate::hybrid_manifest::{
    DEFAULT_JOURNAL_CAPACITY, HybridManifest, HybridVersion, JournalScan,
    MAX_MANIFEST_NAMESPACE_USAGES, ManifestSnapshot, journal_recovery_memory_bytes,
    validate_journal_capacity,
};
#[cfg(test)]
use crate::hybrid_manifest::{JournalIntentInput, JournalIntentKind};
use crate::index::MAX_RECORD_LEN;
use crate::io_backend::DIRECT_IO_ALIGNMENT;
use crate::memory_engine::{
    MEMORY_ENTRY_OVERHEAD_BYTES, MemoryEngine, MemoryEntry, MemoryError, MemoryLookup,
    MemoryPutResult, MemoryRejectReason,
};
use crate::metrics::{
    CacheErrorClass, CacheOperation, OperationMetricsSnapshot, RequestResultClass,
    RequestTelemetry, StateChangeReason, StateTransition,
};
use crate::pending_write::{
    PENDING_WRITE_OWNED_OVERHEAD_BYTES, PendingRegisterError, PendingWaitOutcome,
    PendingWriteDirectory, PendingWriteSlot, allocation_bytes as pending_write_allocation_bytes,
};
use crate::policy::{
    AdmissionDecision, AdmissionMode, AdmissionPolicy, AdmissionSnapshot, DailyWriteReservation,
    DeviceHealthPolicy, HostWriteSnapshot, NamespaceCapacityReservation, NamespaceConfig,
    NamespaceController, NamespaceId, NamespaceRejectReason, NamespaceSnapshot, NamespaceUsage,
    NamespaceWriteReservation, NvmeHealthSample, NvmeHealthStats, PolicyController,
};
use crate::resources::{
    BackpressurePolicy, MAX_BACKPRESSURE_TIMEOUT, MAX_QUEUE_DEPTH, OverloadReason,
};
use crate::write_back::{
    LowerCandidateAdmission, WriteBackExecutor, WriteBackRunError, WriteBackSnapshot,
};

const DEFAULT_MEMORY_SHARDS: usize = 256;
const MAX_MEMORY_SHARDS: usize = 4_096;
const DEFAULT_SMALL_OBJECT_MAX_BYTES: usize = 1024;
const DEFAULT_REQUEST_SLOTS: usize = 256;
const DEFAULT_REQUEST_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_ASYNC_READ_QUEUE_DEPTH: usize = 256;
const DEFAULT_ASYNC_WRITE_QUEUE_DEPTH: usize = 256;
const DEFAULT_ASYNC_IO_CONCURRENCY: usize = 64;
const DEFAULT_ASYNC_MUTATION_WORKERS: usize = 4;
const DEFAULT_WRITE_BACK_QUEUE_DEPTH: usize = 64;
const DEFAULT_WRITE_BACK_WORKERS: usize = 4;
const DEFAULT_WRITE_BACK_MEMORY_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_JOURNAL_COMMIT_QUEUE_DEPTH: usize = 1024;
const DEFAULT_JOURNAL_COMMIT_MEMORY_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_JOURNAL_COMMIT_BATCH_BYTES: usize = 256 * 1024;
const DEFAULT_JOURNAL_COMMIT_BATCH_RECORDS: usize = 256;
const MAX_ASYNC_WORKERS: usize = 128;
const ASYNC_QUEUE_SLOT_OVERHEAD_BYTES: usize = 256;
const WRITE_BACK_SLOT_OVERHEAD_BYTES: usize = 512;
const JOURNAL_BATCH_RECORD_OVERHEAD_BYTES: usize = 64;
const POLICY_NAMESPACE_OVERHEAD_BYTES: usize = 512;
const BUCKET_ENTRY_HEADER_BYTES: usize = 32;
const BUCKET_ENTRY_ALIGNMENT: usize = 8;
const HYBRID_VALUE_HEADER_SIZE: usize = 56;
const HYBRID_VALUE_MAGIC: [u8; 8] = *b"CRHYB001";
const HYBRID_VALUE_VERSION: u16 = 2;
const HYBRID_FIXED_OVERHEAD_BYTES: usize = 256 * 1024;

/// Top-level configuration for one RAM + small-object SSD + region-log SSD
/// cache. The two disk paths must be distinct dedicated files.
#[derive(Clone, Debug)]
pub struct HybridCacheConfig {
    memory_capacity_bytes: usize,
    memory_shards: usize,
    small_object_max_bytes: usize,
    memory_budget_bytes: Option<usize>,
    request_slots: usize,
    request_memory_bytes: usize,
    backpressure: BackpressurePolicy,
    async_read_queue_depth: usize,
    async_write_queue_depth: usize,
    async_io_concurrency: usize,
    async_mutation_workers: usize,
    write_mode: HybridWriteMode,
    write_back_queue_depth: usize,
    write_back_workers: usize,
    write_back_memory_bytes: usize,
    admission_mode: AdmissionMode,
    namespace_configs: Vec<NamespaceConfig>,
    daily_host_write_budget_bytes: Option<u64>,
    daily_host_write_baseline: Option<(u64, u64)>,
    device_health_policy: DeviceHealthPolicy,
    manifest_path: PathBuf,
    journal_capacity_bytes: u64,
    journal_commit_queue_depth: usize,
    journal_commit_memory_bytes: usize,
    journal_commit_batch_bytes: usize,
    journal_commit_batch_records: usize,
    bucket: BucketCacheConfig,
    region: CacheConfig,
}

impl HybridCacheConfig {
    pub fn new(
        memory_capacity_bytes: usize,
        bucket: BucketCacheConfig,
        region: CacheConfig,
    ) -> Self {
        let manifest_path = default_manifest_path(bucket.path());
        // Hybrid is a disposable performance cache: a periodic Region index
        // checkpoint would stop all traffic and rewrite metadata proportional
        // to the live entry count. Explicit flush/close remain available.
        let region = region.with_checkpoint_interval_bytes(0);
        Self {
            memory_capacity_bytes,
            memory_shards: DEFAULT_MEMORY_SHARDS,
            small_object_max_bytes: DEFAULT_SMALL_OBJECT_MAX_BYTES,
            memory_budget_bytes: None,
            request_slots: DEFAULT_REQUEST_SLOTS,
            request_memory_bytes: DEFAULT_REQUEST_MEMORY_BYTES,
            backpressure: BackpressurePolicy::Reject,
            async_read_queue_depth: DEFAULT_ASYNC_READ_QUEUE_DEPTH,
            async_write_queue_depth: DEFAULT_ASYNC_WRITE_QUEUE_DEPTH,
            async_io_concurrency: DEFAULT_ASYNC_IO_CONCURRENCY,
            async_mutation_workers: DEFAULT_ASYNC_MUTATION_WORKERS,
            write_mode: HybridWriteMode::WriteBack,
            write_back_queue_depth: DEFAULT_WRITE_BACK_QUEUE_DEPTH,
            write_back_workers: DEFAULT_WRITE_BACK_WORKERS,
            write_back_memory_bytes: DEFAULT_WRITE_BACK_MEMORY_BYTES,
            admission_mode: AdmissionMode::Always,
            namespace_configs: Vec::new(),
            daily_host_write_budget_bytes: None,
            daily_host_write_baseline: None,
            device_health_policy: DeviceHealthPolicy::ObserveOnly,
            manifest_path,
            journal_capacity_bytes: DEFAULT_JOURNAL_CAPACITY,
            journal_commit_queue_depth: DEFAULT_JOURNAL_COMMIT_QUEUE_DEPTH,
            journal_commit_memory_bytes: DEFAULT_JOURNAL_COMMIT_MEMORY_BYTES,
            journal_commit_batch_bytes: DEFAULT_JOURNAL_COMMIT_BATCH_BYTES,
            journal_commit_batch_records: DEFAULT_JOURNAL_COMMIT_BATCH_RECORDS,
            bucket,
            region,
        }
    }

    pub fn with_memory_shards(mut self, shards: usize) -> Self {
        self.memory_shards = shards;
        self
    }

    /// Route entries whose complete user key+value size is at most this value
    /// to the fixed-bucket disk engine, provided the encoded entry also fits.
    pub fn with_small_object_max(mut self, bytes: usize) -> Self {
        self.small_object_max_bytes = bytes;
        self
    }

    /// Set a hard aggregate budget over L1, both disk engines' configured
    /// logical budgets, and Hybrid ordering metadata.
    pub fn with_memory_budget(mut self, bytes: usize) -> Self {
        self.memory_budget_bytes = Some(bytes);
        self
    }

    /// Bound synchronous operations admitted into the Hybrid coordinator.
    /// Waiting callers retain only caller-owned inputs and thread stacks.
    pub fn with_request_slots(mut self, slots: usize) -> Self {
        self.request_slots = slots;
        self
    }

    /// Bound temporary Hybrid-owned request bytes, including encoded values,
    /// L1 return clones, and simultaneous cross-size disk candidates. Reads
    /// charge current candidate sizes; a maximum-size read requires at most
    /// [`HybridConfigDiagnostics::maximum_read_temporary_bytes`]. A smaller
    /// legal budget rejects only requests that cannot fit it.
    pub fn with_request_memory(mut self, bytes: usize) -> Self {
        self.request_memory_bytes = bytes;
        self
    }

    /// Select rejection, blocking, or bounded waiting at the Hybrid request
    /// gate. Lower engines keep their own independent device backpressure.
    pub fn with_backpressure(mut self, policy: BackpressurePolicy) -> Self {
        self.backpressure = policy;
        self
    }

    /// Configure the bounded asynchronous facade queues. Control operations
    /// retain a small independent reserve inside the write queue.
    pub fn with_async_queue_depths(mut self, read: usize, write: usize) -> Self {
        self.async_read_queue_depth = read;
        self.async_write_queue_depth = write;
        self
    }

    /// Configure asynchronous read concurrency and mutation worker count.
    pub fn with_async_workers(mut self, read_io: usize, mutations: usize) -> Self {
        self.async_io_concurrency = read_io;
        self.async_mutation_workers = mutations;
        self
    }

    /// Choose disk-first write-through or memory-first L1 write-back with
    /// bounded background persistence. An exact-key pending directory masks
    /// older lower values while detached writes run. When a full-value task
    /// would push projected executor occupancy above 75%, a lower-candidate
    /// eviction synchronously hides its Region candidate and complete Bucket
    /// page in memory, then loses the dirty value to a miss without queueing
    /// device I/O. A following flush/close publishes a safe-empty cache
    /// boundary. The eviction path never falls back to foreground device I/O.
    /// Write-back values are disposable cache data: an unflushed process crash
    /// may lose them, and the session dirty fence makes an unclean reopen
    /// discard the lower tiers instead of reviving an older disk value.
    pub fn with_write_mode(mut self, mode: HybridWriteMode) -> Self {
        self.write_mode = mode;
        self
    }

    /// Configure the reserved dirty-entry demotion executor. Its memory limit
    /// covers the detached entry copy plus the pending owner's duplicate key
    /// and fixed allocation charge. Pending-directory shard overhead is
    /// accounted separately by the aggregate Hybrid memory plan. With a
    /// one-slot queue, the 75% proactive budget rounds to zero: disposable
    /// lower-absent evictions are dropped and lower-candidate updates use the
    /// slot-free volatile invalidation path.
    pub fn with_write_back_resources(
        mut self,
        queue_depth: usize,
        workers: usize,
        memory_bytes: usize,
    ) -> Self {
        self.write_back_queue_depth = queue_depth;
        self.write_back_workers = workers;
        self.write_back_memory_bytes = memory_bytes;
        self
    }

    /// Select one admission policy for both SSD routes.
    pub fn with_admission_mode(mut self, mode: AdmissionMode) -> Self {
        self.admission_mode = mode;
        self
    }

    /// Configure one namespace's SSD/future-demotion capacity and write
    /// budgets across both disk routes. Inclusive clean DRAM copies are not a
    /// second capacity charge; dirty write-back values reserve their eventual
    /// SSD charge before L1 publication. Repeating an id replaces its settings.
    pub fn with_namespace(mut self, namespace: NamespaceConfig) -> Self {
        if let Some(existing) = self
            .namespace_configs
            .iter_mut()
            .find(|existing| existing.namespace() == namespace.namespace())
        {
            *existing = namespace;
        } else {
            self.namespace_configs.push(namespace);
        }
        self
    }

    /// Limit combined Bucket and Region host writes in each UTC day.
    pub fn with_daily_host_write_budget(mut self, bytes: u64) -> Self {
        self.daily_host_write_budget_bytes = Some(bytes);
        self
    }

    pub fn without_daily_host_write_budget(mut self) -> Self {
        self.daily_host_write_budget_bytes = None;
        self
    }

    /// Seed the current UTC day's device-level host-write counter.
    pub fn with_daily_host_write_baseline(mut self, utc_day: u64, bytes: u64) -> Self {
        self.daily_host_write_baseline = Some((utc_day, bytes));
        self
    }

    /// Keep health advisory-only or reject new Hybrid puts after a critical
    /// externally supplied NVMe health observation.
    pub fn with_device_health_policy(mut self, policy: DeviceHealthPolicy) -> Self {
        self.device_health_policy = policy;
        self
    }

    /// Use a dedicated file for the Hybrid identity, version fence, and
    /// transition journal. The default appends `.hybrid-manifest` to the Bucket
    /// file name.
    pub fn with_manifest_path(mut self, path: impl AsRef<Path>) -> Self {
        self.manifest_path = path.as_ref().to_path_buf();
        self
    }

    /// Bound the on-disk transition journal and its worst-case recovery
    /// workspace. When full, the coordinator checkpoints both disk tiers once
    /// and retries the mutation.
    pub fn with_journal_capacity(mut self, bytes: u64) -> Self {
        self.journal_capacity_bytes = bytes;
        self
    }

    /// Configure the bounded transition-journal group commit. Every accepted
    /// mutation returns only after its batch is durable; batching reduces
    /// metadata sync amplification without weakening the intent fence.
    pub fn with_journal_group_commit(
        mut self,
        queue_depth: usize,
        memory_bytes: usize,
        max_batch_bytes: usize,
        max_batch_records: usize,
    ) -> Self {
        self.journal_commit_queue_depth = queue_depth;
        self.journal_commit_memory_bytes = memory_bytes;
        self.journal_commit_batch_bytes = max_batch_bytes;
        self.journal_commit_batch_records = max_batch_records;
        self
    }

    pub fn diagnostics(&self) -> Result<HybridConfigDiagnostics> {
        let (diagnostics, _) = self.validate(false)?;
        Ok(diagnostics)
    }

    pub fn open(self) -> Result<HybridCache> {
        let open_started = Instant::now();
        let (diagnostics, lock_tables) = self.validate(true)?;
        let policy = Arc::new(
            PolicyController::try_new_with_health(
                self.admission_mode,
                &self.namespace_configs,
                self.daily_host_write_budget_bytes,
                self.daily_host_write_baseline,
                self.device_health_policy,
            )
            .map_err(|error| CacheError::InvalidConfig(format!("hybrid policy: {error}")))?,
        );
        let layout_fingerprint = hybrid_layout_fingerprint(&diagnostics);
        let initial_namespace_usage = policy_usage_snapshot(policy.as_ref())?;
        let bucket_preexisted = path_has_content(self.bucket.path());
        let region_preexisted = path_has_content(self.region.path());
        let lower_files_preexisted = bucket_preexisted || region_preexisted;
        let manifest = HybridManifest::open_managed_with_journal_capacity(
            &self.manifest_path,
            layout_fingerprint,
            self.journal_capacity_bytes,
            Arc::clone(policy.host_writes()),
            &initial_namespace_usage,
        )?;
        let (manifest, manifest_open) = manifest;
        let manifest_needed_recovery = manifest_open.needs_recovery;
        let manifest = Arc::new(manifest);
        let memory = MemoryEngine::new(self.memory_capacity_bytes, self.memory_shards)
            .map_err(map_memory_open_error);
        let memory = match memory {
            Ok(memory) => memory,
            Err(error) => {
                let _ = manifest.close();
                return Err(error);
            }
        };
        // Fence the owner checkpoint before either managed lower tier can
        // recover, reformat, or publish a clean superblock. If this process
        // dies anywhere between lower open and the final usage publication,
        // the next process sees the manifest dirty and cannot trust a stale
        // namespace checkpoint.
        if let Err(error) = manifest.mark_dirty_for_lower_checkpoint() {
            let _ = manifest.close();
            return Err(error);
        }
        let region_maximum_key_bytes = self.region.maximum_key_size();
        let region_maximum_value_bytes = self.region.maximum_value_size();
        let owner_manifest = Arc::clone(&manifest);
        let owner_dirty: Arc<dyn Fn() -> Result<()> + Send + Sync> =
            Arc::new(move || owner_manifest.mark_dirty_for_lower_checkpoint());
        let bucket = match BucketCache::open_managed_with_owner_dirty(
            self.bucket.clone(),
            Arc::clone(policy.host_writes()),
            Arc::clone(&owner_dirty),
        ) {
            Ok(bucket) => bucket,
            Err(error) => {
                let _ = manifest.close();
                return Err(error);
            }
        };
        let namespace_accounting = Arc::clone(policy.namespaces());
        let retire_sink = Arc::new(move |usage: NamespaceUsage| {
            !namespace_accounting.contains(usage.namespace)
                || namespace_accounting.record_removal_exact(usage)
        });
        let region = match DiskCache::open_managed_with_owner_hooks(
            self.region.clone(),
            Arc::clone(policy.host_writes()),
            Arc::clone(policy.namespaces()),
            retire_sink,
            owner_dirty,
        ) {
            Ok(region) => region,
            Err(error) => {
                let _ = bucket.close_without_checkpoint();
                let _ = manifest.close();
                return Err(error);
            }
        };
        let disk = DiskPair {
            bucket,
            region,
            small_object_max_bytes: self.small_object_max_bytes,
            bucket_size_bytes: diagnostics.bucket.bucket_size_bytes,
            bucket_maximum_item_bytes: diagnostics.bucket.maximum_item_bytes,
            region_maximum_key_bytes,
            region_maximum_value_bytes,
        };
        let policy_restore_started = Instant::now();
        let mut policy_restored_from_checkpoint = false;
        let mut bucket_usage_scanned = false;
        let mut region_usage_scanned = false;
        let usage_recovery = (|| -> Result<()> {
            if manifest_open.created && lower_files_preexisted {
                manifest.begin_clear()?;
                disk.clear().map_err(|error| error.error)?;
                disk.flush().map_err(|error| error.error)?;
                policy.namespaces().reset_live_bytes();
                let usage = policy_usage_snapshot(policy.as_ref())?;
                manifest.publish_clean_with_usage(&usage)?;
                return Ok(());
            }
            if manifest_open.needs_recovery {
                reconcile_dirty_journal(&disk, manifest_open.journal)?;
                disk.flush().map_err(|error| error.error)?;
                // A dirty slot's attached usage describes the previous clean
                // checkpoint. Rebuild from the reconciled lower engines before
                // publishing a new clean boundary so quotas are never restored
                // from stale, potentially lower counters.
                restore_policy_usage(&disk, policy.as_ref(), bucket_preexisted, region_preexisted)?;
                bucket_usage_scanned = bucket_preexisted;
                region_usage_scanned = region_preexisted;
                let usage = policy_usage_snapshot(policy.as_ref())?;
                manifest.finish_dirty_recovery_with_usage(&usage)?;
                return Ok(());
            }

            let configured = policy_usage_snapshot(policy.as_ref())?;
            match manifest.namespace_usage_checkpoint()? {
                Some(checkpoint)
                    if same_namespace_set(&configured, &checkpoint)
                        && disk.bucket.opened_clean()
                        && disk.region.opened_clean()
                        && (manifest_open.created || (bucket_preexisted && region_preexisted)) =>
                {
                    policy_restored_from_checkpoint = true;
                    restore_policy_usage_checkpoint(policy.as_ref(), &checkpoint)?;
                    // Opening is itself fenced dirty before lower recovery.
                    // Republish the unchanged exact usage only after both
                    // lower tiers proved that they opened clean.
                    manifest.publish_clean_with_usage(&checkpoint).map(|_| ())
                }
                // Legacy V1 slots and a changed namespace configuration scan
                // once, then upgrade both slots with the bounded extension.
                _ => {
                    bucket_usage_scanned = bucket_preexisted;
                    region_usage_scanned = region_preexisted;
                    restore_policy_usage(
                        &disk,
                        policy.as_ref(),
                        bucket_preexisted,
                        region_preexisted,
                    )?;
                    let usage = policy_usage_snapshot(policy.as_ref())?;
                    manifest.publish_clean_with_usage(&usage).map(|_| ())
                }
            }
        })();
        if let Err(error) = usage_recovery {
            let _ = disk.close_without_checkpoint();
            let _ = manifest.close();
            return Err(error);
        }
        // The recovery/usage publication above may have made the global slot
        // clean. Fence this runtime dirty exactly once before serving traffic;
        // all steady-state mutation versions are then allocated in memory.
        if let Err(error) = manifest.mark_dirty_for_lower_checkpoint() {
            let _ = disk.close_without_checkpoint();
            let _ = manifest.close();
            return Err(error);
        }
        let policy_restore_elapsed_us = duration_us(policy_restore_started.elapsed());
        let journal = match JournalGroupCommit::try_new(
            Arc::clone(&manifest),
            JournalGroupCommitConfig {
                queue_depth: self.journal_commit_queue_depth,
                memory_budget_bytes: self.journal_commit_memory_bytes,
                max_batch_bytes: self.journal_commit_batch_bytes,
                max_batch_records: self.journal_commit_batch_records,
            },
        ) {
            Ok(journal) => journal,
            Err(error) => {
                let _ = disk.close();
                let _ = manifest.close();
                return Err(error);
            }
        };
        let write_back = if self.write_mode == HybridWriteMode::WriteBack {
            match WriteBackExecutor::try_new(
                self.write_back_queue_depth,
                self.write_back_workers,
                self.write_back_memory_bytes,
                self.backpressure,
            ) {
                Ok(executor) => Some(executor),
                Err(error) => {
                    let _ = journal.shutdown();
                    let _ = disk.close();
                    let _ = manifest.close();
                    return Err(CacheError::Io(error));
                }
            }
        } else {
            None
        };
        let telemetry = RequestTelemetry::new(CacheStatus::Healthy);
        if manifest_needed_recovery {
            telemetry.record_transition(
                CacheStatus::Healthy,
                CacheStatus::Healthy,
                StateChangeReason::RecoveryCompleted,
            );
        }
        let open_stats = HybridOpenStats {
            open_elapsed_us: duration_us(open_started.elapsed()),
            policy_restore_elapsed_us,
            policy_restored_from_checkpoint,
            bucket_usage_scanned,
            region_usage_scanned,
            dirty_manifest_recovered: manifest_needed_recovery,
        };
        Ok(HybridCache {
            inner: Arc::new(HybridInner {
                memory,
                manifest,
                journal,
                disk,
                operation: RwLock::new(()),
                ordering: lock_tables.ordering,
                pending_writes: lock_tables.pending_writes.ok_or_else(|| {
                    CacheError::InvalidConfig(
                        "hybrid pending-write directory was not allocated".into(),
                    )
                })?,
                state: Mutex::new(HybridState {
                    status: CacheStatus::Healthy,
                }),
                close_changed: Condvar::new(),
                requests: Arc::new(HybridRequestGate::new(
                    self.request_slots,
                    self.request_memory_bytes,
                    self.backpressure,
                )),
                async_config: HybridAsyncConfig {
                    read_queue_depth: self.async_read_queue_depth,
                    write_queue_depth: self.async_write_queue_depth,
                    io_concurrency: self.async_io_concurrency,
                    mutation_workers: self.async_mutation_workers,
                },
                async_handle: Mutex::new(None),
                async_close_completion: Arc::new(HybridCloseCompletion::new()),
                accepting: AtomicBool::new(true),
                volatile_lower_loss: AtomicBool::new(false),
                write_mode: self.write_mode,
                write_back,
                policy,
                telemetry,
                open_stats,
                counters: HybridCounters::default(),
                #[cfg(test)]
                dirty_expiry_observer: Mutex::new(None),
            }),
        })
    }

    fn validate(
        &self,
        allocate_locks: bool,
    ) -> Result<(HybridConfigDiagnostics, HybridLockTables)> {
        if self.memory_shards == 0
            || !self.memory_shards.is_power_of_two()
            || self.memory_shards > MAX_MEMORY_SHARDS
        {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid memory_shards must be a power of two in 1..={MAX_MEMORY_SHARDS}"
            )));
        }
        if self.small_object_max_bytes == 0 {
            return Err(CacheError::InvalidConfig(
                "small_object_max_bytes must be positive".into(),
            ));
        }
        if self.request_slots == 0 || self.request_slots > MAX_QUEUE_DEPTH {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid request_slots must be in 1..={MAX_QUEUE_DEPTH}"
            )));
        }
        if self.request_memory_bytes == 0 {
            return Err(CacheError::InvalidConfig(
                "hybrid request_memory_bytes must be positive".into(),
            ));
        }
        if let BackpressurePolicy::Timeout(timeout) = self.backpressure {
            if timeout > MAX_BACKPRESSURE_TIMEOUT {
                return Err(CacheError::InvalidConfig(
                    "hybrid backpressure timeout must not exceed 24 hours".into(),
                ));
            }
        }
        if self.async_read_queue_depth == 0
            || self.async_write_queue_depth == 0
            || self.async_read_queue_depth > MAX_QUEUE_DEPTH
            || self.async_write_queue_depth > MAX_QUEUE_DEPTH
        {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid async queue depths must be in 1..={MAX_QUEUE_DEPTH}"
            )));
        }
        if self.async_io_concurrency == 0
            || self.async_mutation_workers == 0
            || self.async_io_concurrency > MAX_ASYNC_WORKERS
            || self.async_mutation_workers > MAX_ASYNC_WORKERS
        {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid async worker counts must be in 1..={MAX_ASYNC_WORKERS}"
            )));
        }
        if self.write_back_queue_depth == 0
            || self.write_back_queue_depth > MAX_QUEUE_DEPTH
            || self.write_back_workers == 0
            || self.write_back_workers > MAX_ASYNC_WORKERS
            || self.write_back_workers > self.write_back_queue_depth
        {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid write-back requires queue depth in 1..={MAX_QUEUE_DEPTH} and workers in 1..=min(queue_depth, {MAX_ASYNC_WORKERS})"
            )));
        }
        if self.write_back_memory_bytes == 0 {
            return Err(CacheError::InvalidConfig(
                "hybrid write-back memory must be positive".into(),
            ));
        }
        if self.region.has_driver_policy_settings() {
            return Err(CacheError::InvalidConfig(
                "configure admission, namespace, write, daily-write, and device-health policy on HybridCacheConfig, not on its Region CacheConfig"
                    .into(),
            ));
        }
        let namespace_count = self.namespace_configs.len()
            + usize::from(
                !self
                    .namespace_configs
                    .iter()
                    .any(|item| item.namespace() == 0),
            );
        if namespace_count > MAX_MANIFEST_NAMESPACE_USAGES {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid namespace count exceeds manifest checkpoint limit {MAX_MANIFEST_NAMESPACE_USAGES}"
            )));
        }
        let _policy = PolicyController::try_new_with_health(
            self.admission_mode,
            &self.namespace_configs,
            self.daily_host_write_budget_bytes,
            self.daily_host_write_baseline,
            self.device_health_policy,
        )
        .map_err(|error| CacheError::InvalidConfig(format!("hybrid policy: {error}")))?;
        validate_journal_capacity(self.journal_capacity_bytes)?;
        JournalGroupCommitConfig {
            queue_depth: self.journal_commit_queue_depth,
            memory_budget_bytes: self.journal_commit_memory_bytes,
            max_batch_bytes: self.journal_commit_batch_bytes,
            max_batch_records: self.journal_commit_batch_records,
        }
        .validate()?;
        let journal_commit_overhead_bytes = self
            .journal_commit_memory_bytes
            .checked_add(self.journal_commit_batch_bytes)
            .and_then(|bytes| {
                self.journal_commit_batch_records
                    .checked_mul(MAX_DURABILITY_SYNC_GROUPS)
                    .and_then(|records| records.checked_mul(JOURNAL_BATCH_RECORD_OVERHEAD_BYTES))
                    .and_then(|records| bytes.checked_add(records))
            })
            .ok_or_else(|| {
                CacheError::InvalidConfig("hybrid journal group-commit memory overflow".into())
            })?;
        let journal_recovery_bytes = journal_recovery_memory_bytes(self.journal_capacity_bytes)?;
        if self.region.configured_recovery_mode() != RecoveryMode::Blocking {
            return Err(CacheError::InvalidConfig(
                "HybridCache currently requires blocking Region recovery so the global manifest can be reconciled before traffic starts"
                    .into(),
            ));
        }
        if paths_refer_to_same_file(self.bucket.path(), self.region.path()) {
            return Err(CacheError::InvalidConfig(
                "bucket and region engines require distinct dedicated files".into(),
            ));
        }
        if paths_refer_to_same_file(&self.manifest_path, self.bucket.path())
            || paths_refer_to_same_file(&self.manifest_path, self.region.path())
        {
            return Err(CacheError::InvalidConfig(
                "hybrid manifest, bucket, and region require three distinct dedicated files".into(),
            ));
        }
        let bucket = self.bucket.diagnostics()?;
        let region = self.region.diagnostics()?;
        if self.region.maximum_value_size() < HYBRID_VALUE_HEADER_SIZE {
            return Err(CacheError::InvalidConfig(format!(
                "region max_value_size must reserve {HYBRID_VALUE_HEADER_SIZE} bytes for the hybrid envelope"
            )));
        }
        let maximum_bucket_user_bytes = bucket
            .maximum_item_bytes
            .checked_sub(HYBRID_VALUE_HEADER_SIZE)
            .ok_or_else(|| CacheError::InvalidConfig("bucket cannot hold a hybrid value".into()))?;
        if self.small_object_max_bytes > maximum_bucket_user_bytes {
            return Err(CacheError::InvalidConfig(format!(
                "small_object_max_bytes {} exceeds bucket hybrid payload limit {maximum_bucket_user_bytes}",
                self.small_object_max_bytes
            )));
        }
        let maximum_region_record_bytes = usize::try_from(region.maximum_record_bytes)
            .ok()
            .and_then(maximum_persisted_region_record_bytes)
            .ok_or_else(|| {
                CacheError::InvalidConfig(
                    "hybrid maximum persisted Region record is not addressable".into(),
                )
            })?;
        let maximum_read_temporary_bytes =
            read_temporary_bytes(maximum_region_record_bytes, bucket.maximum_item_bytes)
                .and_then(|bytes| bytes.checked_add(self.region.maximum_key_size()))
                .ok_or_else(|| {
                    CacheError::InvalidConfig("hybrid maximum read memory overflow".into())
                })?;
        let ordering_bytes = self
            .memory_shards
            .checked_mul(size_of::<Mutex<()>>())
            .ok_or_else(|| CacheError::InvalidConfig("hybrid ordering memory overflow".into()))?;
        let pending_write_overhead_bytes =
            pending_write_allocation_bytes(self.memory_shards, self.write_back_queue_depth)
                .ok_or_else(|| {
                    CacheError::InvalidConfig(
                        "hybrid pending-write directory memory overflow".into(),
                    )
                })?;
        let async_queue_bytes = self
            .async_read_queue_depth
            .checked_add(self.async_write_queue_depth)
            .and_then(|slots| slots.checked_add(2))
            .and_then(|slots| slots.checked_mul(ASYNC_QUEUE_SLOT_OVERHEAD_BYTES))
            .ok_or_else(|| {
                CacheError::InvalidConfig("hybrid async queue memory overflow".into())
            })?;
        let write_back_overhead_bytes = if self.write_mode == HybridWriteMode::WriteBack {
            self.write_back_queue_depth
                .checked_mul(WRITE_BACK_SLOT_OVERHEAD_BYTES)
                .and_then(|bytes| bytes.checked_add(self.write_back_memory_bytes))
                .ok_or_else(|| {
                    CacheError::InvalidConfig("hybrid write-back memory overflow".into())
                })?
        } else {
            0
        };
        let policy_memory_bytes = self
            .namespace_configs
            .len()
            .checked_add(1)
            .and_then(|namespaces| namespaces.checked_mul(POLICY_NAMESPACE_OVERHEAD_BYTES))
            .and_then(|bytes| bytes.checked_add(AdmissionPolicy::allocation_bytes()))
            .ok_or_else(|| CacheError::InvalidConfig("hybrid policy memory overflow".into()))?;
        let configured_component_budget = self
            .memory_capacity_bytes
            .checked_add(bucket.memory_budget_bytes)
            .and_then(|bytes| bytes.checked_add(region.memory_budget_bytes as usize))
            .and_then(|bytes| bytes.checked_add(ordering_bytes))
            .and_then(|bytes| bytes.checked_add(pending_write_overhead_bytes))
            .and_then(|bytes| bytes.checked_add(journal_recovery_bytes))
            .and_then(|bytes| bytes.checked_add(journal_commit_overhead_bytes))
            .and_then(|bytes| bytes.checked_add(self.request_memory_bytes))
            .and_then(|bytes| bytes.checked_add(async_queue_bytes))
            .and_then(|bytes| bytes.checked_add(write_back_overhead_bytes))
            .and_then(|bytes| bytes.checked_add(policy_memory_bytes))
            .and_then(|bytes| bytes.checked_add(HYBRID_FIXED_OVERHEAD_BYTES))
            .ok_or_else(|| CacheError::InvalidConfig("hybrid memory budget overflow".into()))?;
        let planned_memory_bytes = self
            .memory_capacity_bytes
            .checked_add(bucket.planned_memory_bytes)
            .and_then(|bytes| bytes.checked_add(region.planned_memory_bytes as usize))
            .and_then(|bytes| bytes.checked_add(ordering_bytes))
            .and_then(|bytes| bytes.checked_add(pending_write_overhead_bytes))
            .and_then(|bytes| bytes.checked_add(journal_recovery_bytes))
            .and_then(|bytes| bytes.checked_add(journal_commit_overhead_bytes))
            .and_then(|bytes| bytes.checked_add(self.request_memory_bytes))
            .and_then(|bytes| bytes.checked_add(async_queue_bytes))
            .and_then(|bytes| bytes.checked_add(write_back_overhead_bytes))
            .and_then(|bytes| bytes.checked_add(policy_memory_bytes))
            .and_then(|bytes| bytes.checked_add(HYBRID_FIXED_OVERHEAD_BYTES))
            .ok_or_else(|| CacheError::InvalidConfig("hybrid memory plan overflow".into()))?;
        let memory_budget_bytes = self
            .memory_budget_bytes
            .unwrap_or(configured_component_budget);
        if configured_component_budget > memory_budget_bytes {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid component budgets need {configured_component_budget} bytes, exceeding aggregate budget {memory_budget_bytes}"
            )));
        }

        // Exercise MemoryEngine's exact shard/capacity validation without
        // retaining an allocation in diagnostics-only mode.
        let _memory = MemoryEngine::new(self.memory_capacity_bytes, self.memory_shards)
            .map_err(map_memory_open_error)?;
        let mut ordering = Vec::new();
        let mut pending_writes = None;
        if allocate_locks {
            ordering
                .try_reserve_exact(self.memory_shards)
                .map_err(|_| {
                    CacheError::InvalidConfig("hybrid ordering table cannot be allocated".into())
                })?;
            ordering.resize_with(self.memory_shards, || Mutex::new(()));
            pending_writes = Some(PendingWriteDirectory::try_new(self.memory_shards).map_err(
                |_| {
                    CacheError::InvalidConfig(
                        "hybrid pending-write directory cannot be allocated".into(),
                    )
                },
            )?);
        }
        Ok((
            HybridConfigDiagnostics {
                memory_capacity_bytes: self.memory_capacity_bytes,
                memory_shards: self.memory_shards,
                small_object_max_bytes: self.small_object_max_bytes,
                memory_budget_bytes,
                planned_memory_bytes,
                configured_component_budget_bytes: configured_component_budget,
                manifest_path: self.manifest_path.clone(),
                journal_capacity_bytes: self.journal_capacity_bytes,
                journal_recovery_memory_bytes: journal_recovery_bytes,
                journal_commit_queue_depth: self.journal_commit_queue_depth,
                journal_commit_memory_bytes: self.journal_commit_memory_bytes,
                journal_commit_batch_bytes: self.journal_commit_batch_bytes,
                journal_commit_batch_records: self.journal_commit_batch_records,
                journal_commit_overhead_bytes,
                request_slots: self.request_slots,
                request_memory_bytes: self.request_memory_bytes,
                maximum_read_temporary_bytes,
                backpressure: self.backpressure,
                async_read_queue_depth: self.async_read_queue_depth,
                async_write_queue_depth: self.async_write_queue_depth,
                async_io_concurrency: self.async_io_concurrency,
                async_mutation_workers: self.async_mutation_workers,
                async_queue_overhead_bytes: async_queue_bytes,
                write_mode: self.write_mode,
                write_back_queue_depth: self.write_back_queue_depth,
                write_back_workers: self.write_back_workers,
                write_back_memory_bytes: self.write_back_memory_bytes,
                write_back_overhead_bytes,
                pending_write_overhead_bytes,
                policy_memory_bytes,
                admission_mode: self.admission_mode,
                namespace_count,
                daily_host_write_budget_bytes: self.daily_host_write_budget_bytes,
                device_health_policy: self.device_health_policy,
                bucket,
                region,
            },
            HybridLockTables {
                ordering,
                pending_writes,
            },
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HybridConfigDiagnostics {
    pub memory_capacity_bytes: usize,
    pub memory_shards: usize,
    pub small_object_max_bytes: usize,
    pub memory_budget_bytes: usize,
    pub planned_memory_bytes: usize,
    pub configured_component_budget_bytes: usize,
    pub manifest_path: PathBuf,
    pub journal_capacity_bytes: u64,
    /// Conservative peak retained memory for dirty-journal recovery: one
    /// encoded prefix plus one 32-bit offset per minimum-size record.
    pub journal_recovery_memory_bytes: usize,
    pub journal_commit_queue_depth: usize,
    pub journal_commit_memory_bytes: usize,
    pub journal_commit_batch_bytes: usize,
    pub journal_commit_batch_records: usize,
    pub journal_commit_overhead_bytes: usize,
    pub request_slots: usize,
    pub request_memory_bytes: usize,
    /// Worst-case bytes charged to one maximum-size read, including an async
    /// key copy. Smaller reads reserve their current L1 value or current disk
    /// candidate sizes instead.
    pub maximum_read_temporary_bytes: usize,
    pub backpressure: BackpressurePolicy,
    pub async_read_queue_depth: usize,
    pub async_write_queue_depth: usize,
    pub async_io_concurrency: usize,
    pub async_mutation_workers: usize,
    pub async_queue_overhead_bytes: usize,
    pub write_mode: HybridWriteMode,
    pub write_back_queue_depth: usize,
    pub write_back_workers: usize,
    pub write_back_memory_bytes: usize,
    pub write_back_overhead_bytes: usize,
    /// Fixed exact-key pending-directory allocation. Owned detached values and
    /// their duplicate fence keys are charged to `write_back_memory_bytes`.
    pub pending_write_overhead_bytes: usize,
    pub policy_memory_bytes: usize,
    pub admission_mode: AdmissionMode,
    pub namespace_count: usize,
    pub daily_host_write_budget_bytes: Option<u64>,
    pub device_health_policy: DeviceHealthPolicy,
    pub bucket: BucketConfigDiagnostics,
    pub region: ConfigDiagnostics,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HybridWriteMode {
    WriteThrough,
    #[default]
    WriteBack,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheTier {
    Memory,
    SmallObjectDisk,
    RegionLogDisk,
}

/// Shared immutable cache value.
///
/// `get_handle` returns this type so an L1 hit does not copy the payload. The
/// allocation stays alive until the final handle is dropped, independently of
/// later replacement or eviction. `get` remains the compatibility API that
/// returns an owned `Vec<u8>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HybridValueHandle {
    value: Arc<Vec<u8>>,
}

impl HybridValueHandle {
    pub fn as_slice(&self) -> &[u8] {
        self.value.as_slice()
    }

    pub fn len(&self) -> usize {
        self.value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    pub fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.value, &other.value)
    }
}

impl AsRef<[u8]> for HybridValueHandle {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::ops::Deref for HybridValueHandle {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HybridMissKind {
    NotResident,
    Recovering,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HybridLookupOutcome {
    Hit { value: Vec<u8>, tier: CacheTier },
    Miss(HybridMissKind),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HybridCacheStats {
    pub open: HybridOpenStats,
    pub memory_capacity_bytes: u64,
    pub memory_charged_bytes: u64,
    pub memory_entries: u64,
    pub memory_dirty_entries: u64,
    pub memory_dirty_bytes: u64,
    pub memory_hits: u64,
    pub small_disk_hits: u64,
    pub region_disk_hits: u64,
    pub misses: u64,
    pub promotions: u64,
    pub promotion_skips: u64,
    pub puts: u64,
    pub removes: u64,
    pub memory_evictions: u64,
    pub journal_capacity_bytes: u64,
    pub journal_used_bytes: u64,
    pub journal_rollovers: u64,
    pub journal_rollover_wait_ns: u64,
    pub journal_rollover_max_ns: u64,
    pub requests_in_flight: u64,
    pub requests_in_flight_peak: u64,
    pub request_bytes_in_use: u64,
    pub request_bytes_peak: u64,
    pub request_rejections: u64,
    pub request_wait_ns: u64,
    pub journal_group_commit: HybridJournalGroupCommitStats,
    pub write_back: HybridWriteBackStats,
    pub admission: AdmissionSnapshot,
    pub host_writes: HostWriteSnapshot,
    pub nvme_health: Option<NvmeHealthStats>,
    pub bucket: BucketCacheStats,
    pub region: CacheStats,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HybridOpenStats {
    pub open_elapsed_us: u64,
    pub policy_restore_elapsed_us: u64,
    pub policy_restored_from_checkpoint: bool,
    pub bucket_usage_scanned: bool,
    pub region_usage_scanned: bool,
    pub dirty_manifest_recovered: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HybridMetricsSnapshot {
    pub health: HybridHealthSnapshot,
    pub stats: HybridCacheStats,
    pub operations: [OperationMetricsSnapshot; CacheOperation::ALL.len()],
    pub async_queue: Option<AsyncQueueStats>,
    pub async_close: Option<AsyncHybridCloseStats>,
    /// Oldest-to-newest bounded lifecycle history.
    pub state_transitions: Vec<StateTransition>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HybridJournalGroupCommitStats {
    pub queue_capacity: u64,
    pub memory_capacity_bytes: u64,
    pub fixed_memory_bytes: u64,
    pub in_flight: u64,
    pub in_flight_peak: u64,
    pub memory_in_use_bytes: u64,
    pub memory_peak_bytes: u64,
    pub committed_batches: u64,
    pub committed_records: u64,
    pub durability_syncs: u64,
    pub sync_elapsed_ns_total: u64,
    pub sync_elapsed_ns_max: u64,
    pub rejected: u64,
    pub worker_panics: u64,
    pub accepting: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HybridWriteBackStats {
    pub enabled: bool,
    pub memory_only_puts: u64,
    pub write_through_fallbacks: u64,
    pub demotion_attempts: u64,
    pub demotion_failures: u64,
    pub demoted_entries: u64,
    pub demoted_bytes: u64,
    pub dirty_expiry_fences: u64,
    pub lower_absent_evictions: u64,
    pub lower_candidate_evictions: u64,
    pub synchronous_demotions: u64,
    pub dropped_evictions: u64,
    pub proactive_scheduled: u64,
    pub proactive_skipped: u64,
    pub proactive_persisted: u64,
    pub proactive_rejected: u64,
    pub proactive_fatal: u64,
    pub proactive_invalidated: u64,
    /// True after a volatile pressure invalidation and until flush/close
    /// publishes a safe-empty lower boundary.
    pub volatile_loss_pending: bool,
    pub pending_entries: u64,
    pub pending_entries_peak: u64,
    pub pending_bytes: u64,
    pub pending_bytes_peak: u64,
    pub pending_lookup_misses: u64,
    pub pending_same_key_waits: u64,
    pub pending_same_key_wait_ns: u64,
    pub queue_capacity: u64,
    pub queue_in_flight: u64,
    pub queue_in_flight_peak: u64,
    pub memory_capacity_bytes: u64,
    pub memory_in_use_bytes: u64,
    pub memory_peak_bytes: u64,
    pub queue_submitted: u64,
    pub queue_completed: u64,
    pub queue_rejections: u64,
    pub worker_panics: u64,
    pub queue_wait_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HybridHealthSnapshot {
    pub overall: CacheStatus,
    pub manifest: CacheStatus,
    pub bucket: CacheStatus,
    pub region: CacheStatus,
    pub memory_reads_available: bool,
    pub mutations_available: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HybridPolicySnapshot {
    pub admission: AdmissionSnapshot,
    pub namespaces: Vec<NamespaceSnapshot>,
    pub host_writes: HostWriteSnapshot,
    pub nvme_health: Option<NvmeHealthStats>,
}

#[derive(Default)]
struct HybridCounters {
    memory_hits: AtomicU64,
    small_disk_hits: AtomicU64,
    region_disk_hits: AtomicU64,
    misses: AtomicU64,
    promotions: AtomicU64,
    promotion_skips: AtomicU64,
    puts: AtomicU64,
    removes: AtomicU64,
    write_back_puts: AtomicU64,
    write_through_fallbacks: AtomicU64,
    demotion_attempts: AtomicU64,
    demotion_failures: AtomicU64,
    demoted_entries: AtomicU64,
    demoted_bytes: AtomicU64,
    dirty_expiry_fences: AtomicU64,
    lower_absent_evictions: AtomicU64,
    lower_candidate_evictions: AtomicU64,
    synchronous_demotions: AtomicU64,
    dropped_evictions: AtomicU64,
    proactive_scheduled: AtomicU64,
    proactive_skipped: AtomicU64,
    proactive_persisted: AtomicU64,
    proactive_rejected: AtomicU64,
    proactive_fatal: AtomicU64,
    proactive_invalidated: AtomicU64,
    pending_lookup_misses: AtomicU64,
    pending_same_key_waits: AtomicU64,
    pending_same_key_wait_ns: AtomicU64,
    journal_rollovers: AtomicU64,
    journal_rollover_wait_ns: AtomicU64,
    journal_rollover_max_ns: AtomicU64,
}

struct HybridState {
    status: CacheStatus,
}

struct HybridLockTables {
    ordering: Vec<Mutex<()>>,
    pending_writes: Option<PendingWriteDirectory>,
}

#[derive(Clone, Copy)]
pub(crate) struct HybridAsyncConfig {
    pub(crate) read_queue_depth: usize,
    pub(crate) write_queue_depth: usize,
    pub(crate) io_concurrency: usize,
    pub(crate) mutation_workers: usize,
}

pub(crate) struct HybridInner {
    memory: MemoryEngine,
    manifest: Arc<HybridManifest>,
    journal: JournalGroupCommit,
    disk: DiskPair,
    operation: RwLock<()>,
    ordering: Vec<Mutex<()>>,
    pending_writes: PendingWriteDirectory,
    state: Mutex<HybridState>,
    close_changed: Condvar,
    requests: Arc<HybridRequestGate>,
    async_config: HybridAsyncConfig,
    async_handle: Mutex<Option<std::sync::Weak<crate::async_hybrid::AsyncHybridInner>>>,
    pub(crate) async_close_completion: Arc<HybridCloseCompletion>,
    accepting: AtomicBool,
    volatile_lower_loss: AtomicBool,
    write_mode: HybridWriteMode,
    write_back: Option<WriteBackExecutor>,
    policy: Arc<PolicyController>,
    telemetry: RequestTelemetry,
    open_stats: HybridOpenStats,
    counters: HybridCounters,
    #[cfg(test)]
    dirty_expiry_observer: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

#[derive(Clone, Copy)]
enum HybridRequestKind {
    Read,
    Write,
}

struct HybridRequestState {
    in_flight: usize,
    bytes_in_use: usize,
    closed: bool,
}

struct HybridRequestGate {
    max_in_flight: usize,
    max_bytes: usize,
    backpressure: BackpressurePolicy,
    state: Mutex<HybridRequestState>,
    available: Condvar,
    in_flight_peak: AtomicU64,
    bytes_peak: AtomicU64,
    rejections: AtomicU64,
    wait_ns: AtomicU64,
}

#[derive(Clone, Copy, Default)]
struct HybridRequestSnapshot {
    in_flight: u64,
    in_flight_peak: u64,
    bytes_in_use: u64,
    bytes_peak: u64,
    rejections: u64,
    wait_ns: u64,
}

pub(crate) struct HybridRequestPermit {
    gate: Arc<HybridRequestGate>,
    bytes: usize,
}

impl HybridRequestGate {
    fn new(max_in_flight: usize, max_bytes: usize, backpressure: BackpressurePolicy) -> Self {
        Self {
            max_in_flight,
            max_bytes,
            backpressure,
            state: Mutex::new(HybridRequestState {
                in_flight: 0,
                bytes_in_use: 0,
                closed: false,
            }),
            available: Condvar::new(),
            in_flight_peak: AtomicU64::new(0),
            bytes_peak: AtomicU64::new(0),
            rejections: AtomicU64::new(0),
            wait_ns: AtomicU64::new(0),
        }
    }

    fn acquire(
        self: &Arc<Self>,
        kind: HybridRequestKind,
        bytes: usize,
    ) -> std::result::Result<HybridRequestPermit, OverloadReason> {
        self.acquire_with(kind, bytes, self.backpressure)
    }

    fn try_acquire(
        self: &Arc<Self>,
        kind: HybridRequestKind,
        bytes: usize,
    ) -> std::result::Result<HybridRequestPermit, OverloadReason> {
        self.acquire_with(kind, bytes, BackpressurePolicy::Reject)
    }

    fn acquire_with(
        self: &Arc<Self>,
        kind: HybridRequestKind,
        bytes: usize,
        backpressure: BackpressurePolicy,
    ) -> std::result::Result<HybridRequestPermit, OverloadReason> {
        let started = Instant::now();
        if bytes > self.max_bytes {
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return Err(buffer_unavailable(kind));
        }
        let deadline = match backpressure {
            BackpressurePolicy::Timeout(timeout) => Some(
                Instant::now()
                    .checked_add(timeout)
                    .unwrap_or_else(Instant::now),
            ),
            BackpressurePolicy::Reject | BackpressurePolicy::Block => None,
        };
        let mut state = lock_mutex(&self.state);
        loop {
            if state.closed {
                self.rejections.fetch_add(1, Ordering::Relaxed);
                add_request_wait(&self.wait_ns, started.elapsed());
                return Err(queue_full(kind));
            }
            let slots_full = state.in_flight >= self.max_in_flight;
            let bytes_full = bytes > self.max_bytes.saturating_sub(state.bytes_in_use);
            if !slots_full && !bytes_full {
                break;
            }
            state = match backpressure {
                BackpressurePolicy::Reject => {
                    self.rejections.fetch_add(1, Ordering::Relaxed);
                    add_request_wait(&self.wait_ns, started.elapsed());
                    return Err(if bytes_full {
                        buffer_unavailable(kind)
                    } else {
                        queue_full(kind)
                    });
                }
                BackpressurePolicy::Block => self
                    .available
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                BackpressurePolicy::Timeout(_) => {
                    let Some(remaining) = deadline
                        .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
                    else {
                        self.rejections.fetch_add(1, Ordering::Relaxed);
                        add_request_wait(&self.wait_ns, started.elapsed());
                        return Err(request_timeout(kind));
                    };
                    let (next, timed_out) = self
                        .available
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if timed_out.timed_out() {
                        let still_full = next.in_flight >= self.max_in_flight
                            || bytes > self.max_bytes.saturating_sub(next.bytes_in_use);
                        if still_full {
                            self.rejections.fetch_add(1, Ordering::Relaxed);
                            add_request_wait(&self.wait_ns, started.elapsed());
                            return Err(request_timeout(kind));
                        }
                    }
                    next
                }
            };
        }
        state.in_flight += 1;
        state.bytes_in_use += bytes;
        update_request_peak(&self.in_flight_peak, state.in_flight as u64);
        update_request_peak(&self.bytes_peak, state.bytes_in_use as u64);
        add_request_wait(&self.wait_ns, started.elapsed());
        Ok(HybridRequestPermit {
            gate: Arc::clone(self),
            bytes,
        })
    }

    fn snapshot(&self) -> HybridRequestSnapshot {
        let state = lock_mutex(&self.state);
        HybridRequestSnapshot {
            in_flight: state.in_flight as u64,
            in_flight_peak: self
                .in_flight_peak
                .load(Ordering::Relaxed)
                .max(state.in_flight as u64),
            bytes_in_use: state.bytes_in_use as u64,
            bytes_peak: self
                .bytes_peak
                .load(Ordering::Relaxed)
                .max(state.bytes_in_use as u64),
            rejections: self.rejections.load(Ordering::Relaxed),
            wait_ns: self.wait_ns.load(Ordering::Relaxed),
        }
    }

    fn try_grow(
        &self,
        kind: HybridRequestKind,
        additional_bytes: usize,
    ) -> std::result::Result<(), OverloadReason> {
        if additional_bytes == 0 {
            return Ok(());
        }
        let mut state = lock_mutex(&self.state);
        if additional_bytes > self.max_bytes.saturating_sub(state.bytes_in_use) {
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return Err(buffer_unavailable(kind));
        }
        state.bytes_in_use += additional_bytes;
        update_request_peak(&self.bytes_peak, state.bytes_in_use as u64);
        Ok(())
    }

    fn shrink(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut state = lock_mutex(&self.state);
        debug_assert!(state.bytes_in_use >= bytes);
        state.bytes_in_use = state.bytes_in_use.saturating_sub(bytes);
        self.available.notify_all();
    }

    fn release(&self, bytes: usize) {
        let mut state = lock_mutex(&self.state);
        debug_assert!(state.in_flight != 0 && state.bytes_in_use >= bytes);
        state.in_flight = state.in_flight.saturating_sub(1);
        state.bytes_in_use = state.bytes_in_use.saturating_sub(bytes);
        self.available.notify_all();
    }

    fn stop_admission(&self) {
        let mut state = lock_mutex(&self.state);
        state.closed = true;
        self.available.notify_all();
    }

    fn wait_idle(&self) {
        let mut state = lock_mutex(&self.state);
        while state.in_flight != 0 {
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

impl Drop for HybridRequestPermit {
    fn drop(&mut self) {
        self.gate.release(self.bytes);
    }
}

impl HybridRequestPermit {
    fn try_grow_read(&mut self, additional_bytes: usize) -> Result<()> {
        let bytes = self
            .bytes
            .checked_add(additional_bytes)
            .ok_or(CacheError::Overloaded(
                OverloadReason::ReadBufferUnavailable,
            ))?;
        self.gate
            .try_grow(HybridRequestKind::Read, additional_bytes)
            .map_err(CacheError::Overloaded)?;
        self.bytes = bytes;
        Ok(())
    }

    fn shrink(&mut self, bytes: usize) {
        debug_assert!(self.bytes >= bytes);
        self.bytes = self.bytes.saturating_sub(bytes);
        self.gate.shrink(bytes);
    }
}

fn queue_full(kind: HybridRequestKind) -> OverloadReason {
    match kind {
        HybridRequestKind::Read => OverloadReason::ReadQueueFull,
        HybridRequestKind::Write => OverloadReason::WriteQueueFull,
    }
}

fn buffer_unavailable(kind: HybridRequestKind) -> OverloadReason {
    match kind {
        HybridRequestKind::Read => OverloadReason::ReadBufferUnavailable,
        HybridRequestKind::Write => OverloadReason::WriteBufferUnavailable,
    }
}

fn request_timeout(kind: HybridRequestKind) -> OverloadReason {
    match kind {
        HybridRequestKind::Read => OverloadReason::ReadTimeout,
        HybridRequestKind::Write => OverloadReason::WriteTimeout,
    }
}

fn add_request_wait(counter: &AtomicU64, duration: Duration) {
    let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(nanos))
    });
}

fn update_request_peak(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        (value > current).then_some(value)
    });
}

/// A bounded inclusive hybrid cache with a DRAM LRU and size-routed SSD pair.
#[derive(Clone)]
pub struct HybridCache {
    pub(crate) inner: Arc<HybridInner>,
}

impl fmt::Debug for HybridCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HybridCache")
            .field("status", &self.status())
            .field("memory", &self.inner.memory.stats())
            .finish_non_exhaustive()
    }
}

impl HybridCache {
    /// Return the single bounded asynchronous facade shared by every clone of
    /// this Hybrid cache instance.
    pub fn async_handle(&self) -> Result<crate::async_hybrid::AsyncHybridCache> {
        let mut shared = lock_mutex(&self.inner.async_handle);
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(CacheError::Closed);
        }
        match self.status() {
            CacheStatus::Healthy | CacheStatus::MissOnly => {}
            CacheStatus::Poisoned => return Err(CacheError::Poisoned),
            CacheStatus::Closed => return Err(CacheError::Closed),
        }
        if let Some(inner) = shared.as_ref().and_then(std::sync::Weak::upgrade) {
            return Ok(crate::async_hybrid::AsyncHybridCache::from_inner(inner));
        }
        let handle = crate::async_hybrid::AsyncHybridCache::try_new(
            Arc::clone(&self.inner),
            self.inner.async_config,
        )?;
        *shared = Some(Arc::downgrade(handle.shared_inner()));
        Ok(handle)
    }

    pub(crate) fn from_inner(inner: Arc<HybridInner>) -> Self {
        Self { inner }
    }

    pub(crate) fn stop_admission_for_close(&self) {
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.requests.stop_admission();
    }

    pub(crate) fn try_reserve_async_read(&self, key_bytes: usize) -> Result<HybridRequestPermit> {
        self.inner
            .requests
            .try_acquire(HybridRequestKind::Read, key_bytes)
            .map_err(|reason| self.request_gate_error(reason))
    }

    pub(crate) fn try_reserve_async_put(
        &self,
        key_bytes: usize,
        value_bytes: usize,
    ) -> Result<HybridRequestPermit> {
        let bytes = key_bytes
            .checked_add(value_bytes)
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(HYBRID_VALUE_HEADER_SIZE))
            .ok_or(CacheError::Overloaded(
                OverloadReason::WriteBufferUnavailable,
            ))?;
        self.inner
            .requests
            .try_acquire(HybridRequestKind::Write, bytes)
            .map_err(|reason| self.request_gate_error(reason))
    }

    pub(crate) fn try_reserve_async_remove(&self, key_bytes: usize) -> Result<HybridRequestPermit> {
        let bytes = key_bytes.checked_mul(2).ok_or(CacheError::Overloaded(
            OverloadReason::WriteBufferUnavailable,
        ))?;
        self.inner
            .requests
            .try_acquire(HybridRequestKind::Write, bytes)
            .map_err(|reason| self.request_gate_error(reason))
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_in(0, key)
    }

    pub fn get_in(&self, namespace: NamespaceId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(match self.lookup_in(namespace, key)? {
            HybridLookupOutcome::Hit { value, .. } => Some(value),
            HybridLookupOutcome::Miss(_) => None,
        })
    }

    /// Read a value through a shared handle. L1 hits clone only an `Arc` while
    /// holding the memory shard lock; an L2 hit moves its newly-read buffer
    /// into the handle. Callers that can consume a borrowed slice should use
    /// this API on throughput-sensitive paths.
    pub fn get_handle(&self, key: &[u8]) -> Result<Option<HybridValueHandle>> {
        self.get_handle_in(0, key)
    }

    pub fn get_handle_in(
        &self,
        namespace: NamespaceId,
        key: &[u8],
    ) -> Result<Option<HybridValueHandle>> {
        self.ensure_accepting()?;
        match self.status() {
            CacheStatus::Healthy | CacheStatus::MissOnly => {}
            CacheStatus::Poisoned => return Err(CacheError::Poisoned),
            CacheStatus::Closed => return Err(CacheError::Closed),
        }
        let mut request = self
            .inner
            .requests
            .acquire(HybridRequestKind::Read, key.len())
            .map_err(|reason| self.request_gate_error(reason))?;

        let started = Instant::now();
        {
            let _operation = self.read_operation_admitted()?;
            if self.inner.policy.namespaces().contains(namespace) {
                if let Some(hit) = self
                    .inner
                    .memory
                    .get_live_with_reservation(namespace, key, |_| true)
                    .map_err(map_memory_error)?
                {
                    self.inner
                        .policy
                        .admission()
                        .observe(hybrid_hash(namespace, key));
                    self.inner
                        .counters
                        .memory_hits
                        .fetch_add(1, Ordering::Relaxed);
                    self.inner.telemetry.observe(
                        CacheOperation::Get,
                        RequestResultClass::Hit,
                        None,
                        started.elapsed(),
                    );
                    return Ok(Some(HybridValueHandle { value: hit.value }));
                }
            }
        }

        Ok(
            match self.lookup_in_admitted(namespace, key, &mut request)? {
                HybridLookupOutcome::Hit { value, .. } => Some(HybridValueHandle {
                    value: Arc::new(value),
                }),
                HybridLookupOutcome::Miss(_) => None,
            },
        )
    }

    pub fn lookup(&self, key: &[u8]) -> Result<HybridLookupOutcome> {
        self.lookup_in(0, key)
    }

    pub fn lookup_in(&self, namespace: NamespaceId, key: &[u8]) -> Result<HybridLookupOutcome> {
        self.ensure_accepting()?;
        match self.status() {
            CacheStatus::Healthy | CacheStatus::MissOnly => {}
            CacheStatus::Poisoned => return Err(CacheError::Poisoned),
            CacheStatus::Closed => return Err(CacheError::Closed),
        }
        let mut request = self
            .inner
            .requests
            .acquire(HybridRequestKind::Read, key.len())
            .map_err(|reason| self.request_gate_error(reason))?;
        self.lookup_in_admitted(namespace, key, &mut request)
    }

    pub(crate) fn lookup_in_admitted(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        request: &mut HybridRequestPermit,
    ) -> Result<HybridLookupOutcome> {
        self.lookup_in_admitted_with_context(namespace, key, request, None)
    }

    pub(crate) fn lookup_in_admitted_with_task_context(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        request: &mut HybridRequestPermit,
        context: &TaskContext,
    ) -> Result<HybridLookupOutcome> {
        self.lookup_in_admitted_with_context(namespace, key, request, Some(context))
    }

    fn lookup_in_admitted_with_context(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        request: &mut HybridRequestPermit,
        context: Option<&TaskContext>,
    ) -> Result<HybridLookupOutcome> {
        let started = Instant::now();
        let result = (|| -> Result<HybridLookupOutcome> {
            if context.is_some_and(TaskContext::is_stopped) {
                return Err(hybrid_context_stop_error(context));
            }
            let _operation = self.read_operation_admitted()?;
            if context.is_some_and(TaskContext::is_stopped) {
                return Err(hybrid_context_stop_error(context));
            }
            match self.status() {
                CacheStatus::Healthy | CacheStatus::MissOnly => {}
                CacheStatus::Poisoned => return Err(CacheError::Poisoned),
                CacheStatus::Closed => return Err(CacheError::Closed),
            }
            if !self.inner.policy.namespaces().contains(namespace) {
                self.inner.counters.misses.fetch_add(1, Ordering::Relaxed);
                return Ok(HybridLookupOutcome::Miss(HybridMissKind::NotResident));
            }
            let hash = hybrid_hash(namespace, key);
            self.inner.policy.admission().observe(hash);
            let mut l1_temporary_bytes = 0;
            if let Some(hit) = self
                .inner
                .memory
                .get_live_with_reservation(namespace, key, |bytes| {
                    if request.try_grow_read(bytes).is_ok() {
                        l1_temporary_bytes = bytes;
                        true
                    } else {
                        false
                    }
                })
                .map_err(map_memory_error)?
            {
                self.inner
                    .counters
                    .memory_hits
                    .fetch_add(1, Ordering::Relaxed);
                let value =
                    try_clone_bytes(hit.value.as_slice(), OverloadReason::ReadBufferUnavailable)?;
                return Ok(HybridLookupOutcome::Hit {
                    value,
                    tier: CacheTier::Memory,
                });
            }

            // Misses and expiry transfers still serialize with lower-tier
            // publication. Recheck L1 after acquiring the stripe so a racing
            // put cannot be shadowed by an older disk record.
            let _ordering = self.lock_key(hash);
            if context.is_some_and(TaskContext::is_stopped) {
                return Err(hybrid_context_stop_error(context));
            }
            match self
                .inner
                .memory
                .get_with_reservation(
                    namespace,
                    key,
                    |bytes| {
                        if request.try_grow_read(bytes).is_ok() {
                            l1_temporary_bytes = bytes;
                            true
                        } else {
                            false
                        }
                    },
                    || {
                        let committed = context.is_none_or(TaskContext::try_commit);
                        if committed {
                            #[cfg(test)]
                            {
                                let observer =
                                    { lock_mutex(&self.inner.dirty_expiry_observer).take() };
                                if let Some(observer) = observer {
                                    observer();
                                }
                            }
                        }
                        committed
                    },
                )
                .map_err(map_memory_error)?
            {
                MemoryLookup::Hit(hit) => {
                    self.inner
                        .counters
                        .memory_hits
                        .fetch_add(1, Ordering::Relaxed);
                    let value = try_clone_bytes(
                        hit.value.as_slice(),
                        OverloadReason::ReadBufferUnavailable,
                    )?;
                    return Ok(HybridLookupOutcome::Hit {
                        value,
                        tier: CacheTier::Memory,
                    });
                }
                MemoryLookup::Expired(entry) if !entry.disk_clean => {
                    // The phase CAS ran under the Memory shard lock before the
                    // destructive transfer. From here the disk fence and exact
                    // pending refund are uncancellable; an older L2 version
                    // can therefore never revive after cancellation returns.
                    if let Err(error) =
                        self.inner
                            .disk
                            .remove(namespace, key, Some(self.inner.policy.as_ref()))
                    {
                        self.poison();
                        return Err(error.error);
                    }
                    self.retire_dirty_memory_usage(&entry)?;
                    self.inner
                        .counters
                        .dirty_expiry_fences
                        .fetch_add(1, Ordering::Relaxed);
                    self.inner.counters.misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(HybridLookupOutcome::Miss(HybridMissKind::NotResident));
                }
                MemoryLookup::Expired(entry) => {
                    drop(entry);
                    request.shrink(l1_temporary_bytes);
                }
                MemoryLookup::ExpiryCommitRejected => {
                    return Err(hybrid_context_stop_error(context));
                }
                MemoryLookup::Miss => {}
            }

            if let Some(pending) = self.inner.pending_writes.find(namespace, key, hash) {
                if pending.failed() {
                    return Err(CacheError::Poisoned);
                }
                self.inner
                    .counters
                    .pending_lookup_misses
                    .fetch_add(1, Ordering::Relaxed);
                self.inner.counters.misses.fetch_add(1, Ordering::Relaxed);
                return Ok(HybridLookupOutcome::Miss(HybridMissKind::NotResident));
            }

            if context.is_some_and(TaskContext::is_stopped) {
                return Err(hybrid_context_stop_error(context));
            }
            request.try_grow_read(self.inner.disk.read_temporary_bytes(namespace, key)?)?;
            let manifest = self.inner.manifest.snapshot()?;
            let candidate = match self.inner.disk.get(
                namespace,
                key,
                manifest,
                context,
                self.inner.policy.as_ref(),
            )? {
                DiskLookup::Hit { entry, tier } => {
                    match tier {
                        CacheTier::SmallObjectDisk => self
                            .inner
                            .counters
                            .small_disk_hits
                            .fetch_add(1, Ordering::Relaxed),
                        CacheTier::RegionLogDisk => self
                            .inner
                            .counters
                            .region_disk_hits
                            .fetch_add(1, Ordering::Relaxed),
                        CacheTier::Memory => unreachable!("disk pair cannot return a memory hit"),
                    };
                    (entry, tier)
                }
                DiskLookup::Miss(kind) => {
                    self.inner.counters.misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(HybridLookupOutcome::Miss(kind));
                }
            };
            // A cancelled read may finish its device operation, but it must not
            // mutate L1 after the caller has stopped waiting for the result.
            if context.is_some_and(TaskContext::is_stopped) {
                return Err(hybrid_context_stop_error(context));
            }
            let value = try_clone_bytes(&candidate.0.value, OverloadReason::ReadBufferUnavailable)?;
            if context.is_some_and(TaskContext::is_stopped) {
                return Err(hybrid_context_stop_error(context));
            }
            let promotion = if let Some(context) = context {
                context
                    .run_if_active(|| self.inner.memory.put(candidate.0))
                    .ok_or_else(|| hybrid_context_stop_error(Some(context)))?
            } else {
                self.inner.memory.put(candidate.0)
            };
            match promotion {
                MemoryPutResult::Stored { .. } => {
                    self.inner
                        .counters
                        .promotions
                        .fetch_add(1, Ordering::Relaxed);
                }
                MemoryPutResult::NotStored { .. } => {
                    self.inner
                        .counters
                        .promotion_skips
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(HybridLookupOutcome::Hit {
                value,
                tier: candidate.1,
            })
        })();
        self.record_operation(
            CacheOperation::Get,
            &result,
            |outcome| match outcome {
                HybridLookupOutcome::Hit { .. } => RequestResultClass::Hit,
                HybridLookupOutcome::Miss(_) => RequestResultClass::Miss,
            },
            started.elapsed(),
        );
        result
    }

    pub fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        options: PutOptions,
    ) -> Result<PutOutcome> {
        self.put_in(0, key, value, options)
    }

    pub fn put_in(
        &self,
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        options: PutOptions,
    ) -> Result<PutOutcome> {
        let key = key.as_ref();
        let value = value.as_ref();
        self.ensure_accepting()?;
        self.ensure_mutation_healthy()?;
        if options
            .expires_at_unix_ms
            .is_some_and(|expires_at| expires_at <= now_unix_ms())
        {
            return Ok(PutOutcome::Rejected(RejectReason::AlreadyExpired));
        }
        let route = match self.inner.disk.route_put(key.len(), value.len()) {
            Ok(route) => route,
            Err(outcome) => return Ok(outcome),
        };
        let request_bytes = key
            .len()
            .checked_add(value.len())
            .and_then(|bytes| bytes.checked_add(HYBRID_VALUE_HEADER_SIZE));
        let Some(request_bytes) = request_bytes else {
            return Ok(PutOutcome::Rejected(RejectReason::BufferUnavailable));
        };
        let _request = match self
            .inner
            .requests
            .acquire(HybridRequestKind::Write, request_bytes)
        {
            Ok(request) => request,
            Err(reason) => {
                if !self.inner.accepting.load(Ordering::Acquire) {
                    return Err(CacheError::Closed);
                }
                return Ok(PutOutcome::Rejected(hybrid_put_reject_reason(reason)));
            }
        };
        self.put_in_routed(namespace, key, value, options, route)
    }

    pub(crate) fn put_in_admitted(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
    ) -> Result<PutOutcome> {
        self.ensure_mutation_healthy()?;
        if options
            .expires_at_unix_ms
            .is_some_and(|expires_at| expires_at <= now_unix_ms())
        {
            return Ok(PutOutcome::Rejected(RejectReason::AlreadyExpired));
        }
        let route = match self.inner.disk.route_put(key.len(), value.len()) {
            Ok(route) => route,
            Err(outcome) => return Ok(outcome),
        };
        self.put_in_routed(namespace, key, value, options, route)
    }

    fn put_in_routed(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
        route: DiskRoute,
    ) -> Result<PutOutcome> {
        let started = Instant::now();
        let result = match self.inner.write_mode {
            HybridWriteMode::WriteThrough => {
                self.put_in_routed_write_through(namespace, key, value, options, route)
            }
            HybridWriteMode::WriteBack => {
                self.put_in_routed_write_back(namespace, key, value, options, route)
            }
        };
        self.record_operation(
            CacheOperation::Put,
            &result,
            |outcome| match outcome {
                PutOutcome::Stored => RequestResultClass::Stored,
                PutOutcome::Rejected(_) => RequestResultClass::Rejected,
            },
            started.elapsed(),
        );
        result
    }

    fn put_in_routed_write_back(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
        route: DiskRoute,
    ) -> Result<PutOutcome> {
        let write_back_memory_bytes = self
            .inner
            .write_back
            .as_ref()
            .map_or(0, WriteBackExecutor::memory_capacity_bytes);
        // A dirty entry must always fit one write-back reservation. Otherwise
        // it could be accepted into L1 but become impossible to flush or
        // demote. Route such entries through the synchronous disk-first path
        // before publishing a journal intent or dirty value.
        let minimum_dirty_bytes = MEMORY_ENTRY_OVERHEAD_BYTES
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .and_then(|bytes| bytes.checked_add(key.len()))
            .and_then(|bytes| bytes.checked_add(PENDING_WRITE_OWNED_OVERHEAD_BYTES));
        if minimum_dirty_bytes.is_none_or(|bytes| bytes > write_back_memory_bytes) {
            self.inner
                .counters
                .write_through_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return self.put_in_routed_write_through(namespace, key, value, options, route);
        }
        // Complete every fallible L1 allocation before publishing the volatile
        // version. Allocation pressure alone never invalidates the previous
        // value.
        let memory_key = match try_clone_bytes(key, OverloadReason::WriteBufferUnavailable) {
            Ok(key) => key,
            Err(_) => return Ok(PutOutcome::Rejected(RejectReason::BufferUnavailable)),
        };
        let memory_value = match try_clone_bytes(value, OverloadReason::WriteBufferUnavailable) {
            Ok(value) => value,
            Err(_) => return Ok(PutOutcome::Rejected(RejectReason::BufferUnavailable)),
        };
        let owned_dirty_bytes = MEMORY_ENTRY_OVERHEAD_BYTES
            .checked_add(memory_key.capacity())
            .and_then(|bytes| bytes.checked_add(memory_value.capacity()))
            .and_then(|bytes| bytes.checked_add(memory_key.capacity()))
            .and_then(|bytes| bytes.checked_add(PENDING_WRITE_OWNED_OVERHEAD_BYTES));
        if owned_dirty_bytes.is_none_or(|bytes| bytes > write_back_memory_bytes) {
            self.inner
                .counters
                .write_through_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return self.put_in_routed_write_through(namespace, key, value, options, route);
        }
        let hash = hybrid_hash(namespace, key);
        let _operation = self.read_operation_admitted()?;
        self.ensure_mutation_healthy()?;
        let _ordering = self.lock_key_after_pending(namespace, key, hash)?;
        let memory_usage = self.inner.memory.entry_usage(namespace, key);
        let previous_live_bytes = memory_usage
            .filter(|usage| !usage.expired && !usage.disk_clean)
            .map_or(0, |usage| usage.pending_disk_bytes);
        let replaced_dirty_usage =
            memory_usage
                .filter(|usage| !usage.disk_clean)
                .map(|usage| NamespaceUsage {
                    namespace,
                    live_bytes: usage.pending_disk_bytes,
                });
        let is_update = memory_usage.is_some_and(|usage| !usage.expired)
            || self.inner.disk.may_contain_for_admission(namespace, key)?;
        let mut policy_reservation = match self.reserve_put_policy(PolicyPutRequest {
            namespace,
            hash,
            key_len: key.len(),
            value_len: value.len(),
            route,
            previous_live_bytes,
            is_update,
            admission_preapproved: false,
        }) {
            Ok(reservation) => reservation,
            Err(reason) => return Ok(PutOutcome::Rejected(reason)),
        };
        let (manifest, version) = self.inner.manifest.allocate_volatile_version()?;

        let pending_disk_bytes = policy_reservation.pending_live_bytes();
        let memory_entry = MemoryEntry::new_versioned_with_pending_disk_bytes(
            namespace,
            memory_key,
            memory_value,
            options.expires_at_unix_ms,
            version,
            false,
            pending_disk_bytes,
        );
        let mut demotion_failure = None;
        let memory_result = self.inner.memory.put_with_demote(memory_entry, |victim| {
            match self.prepare_dirty_eviction(victim) {
                Ok(disk_live_bytes) => Some(disk_live_bytes),
                Err(failure) => {
                    demotion_failure = Some(failure);
                    None
                }
            }
        });
        if let Some(failure) = demotion_failure {
            return self.handle_demotion_failure(failure);
        }
        match memory_result {
            MemoryPutResult::Stored { .. } => {
                policy_reservation.commit_pending(self.inner.policy.as_ref(), replaced_dirty_usage);
                self.inner
                    .counters
                    .write_back_puts
                    .fetch_add(1, Ordering::Relaxed);
                self.inner.counters.puts.fetch_add(1, Ordering::Relaxed);
                // A dirty value stays only in L1 until it becomes an eviction
                // victim. This coalesces repeated updates to hot keys instead of
                // turning write-back into asynchronous write-through.
                Ok(PutOutcome::Stored)
            }
            MemoryPutResult::NotStored { entry, reason } => {
                if reason == MemoryRejectReason::DemotionFailed {
                    self.poison();
                    return Err(CacheError::Poisoned);
                }
                self.inner
                    .counters
                    .write_through_fallbacks
                    .fetch_add(1, Ordering::Relaxed);
                let usage_commit = policy_reservation.take_lower_commit()?;
                let receipt = match self.inner.disk.put(DiskPutRequest {
                    namespace,
                    key: &entry.key,
                    value: &entry.value,
                    options,
                    route,
                    cache_id: manifest.cache_id,
                    version,
                    policy: self.inner.policy.as_ref(),
                    usage_commit,
                }) {
                    Ok(receipt) => receipt,
                    Err(error) => {
                        self.poison();
                        return Err(error.error);
                    }
                };
                if receipt.outcome == PutOutcome::Stored {
                    if receipt.new_usage.is_none() {
                        self.poison();
                        return Err(CacheError::CorruptMetadata(
                            "stored managed Region put omitted its live usage",
                        ));
                    }
                    if !policy_reservation.commit_lower_published(self.inner.policy.as_ref()) {
                        self.poison();
                        return Err(CacheError::CorruptMetadata(
                            "managed lower put omitted its namespace commit",
                        ));
                    }
                    self.inner.memory.remove(namespace, key);
                    if let Some(previous) = replaced_dirty_usage {
                        self.retire_pending_namespace_usage(previous)?;
                    }
                    self.inner.counters.puts.fetch_add(1, Ordering::Relaxed);
                }
                Ok(receipt.outcome)
            }
        }
    }

    fn put_in_routed_write_through(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
        route: DiskRoute,
    ) -> Result<PutOutcome> {
        let hash = hybrid_hash(namespace, key);
        let _operation = self.read_operation_admitted()?;
        self.ensure_mutation_healthy()?;
        let _ordering = self.lock_key_after_pending(namespace, key, hash)?;
        let memory_usage = self.inner.memory.entry_usage(namespace, key);
        let is_update = memory_usage.is_some_and(|usage| !usage.expired)
            || self.inner.disk.may_contain_for_admission(namespace, key)?;
        let mut policy_reservation = match self.reserve_put_policy(PolicyPutRequest {
            namespace,
            hash,
            key_len: key.len(),
            value_len: value.len(),
            route,
            // A clean L1 copy does not prove its inclusive L2 copy still
            // exists: unrelated Bucket eviction or Region reclaim may already
            // have retired it. Full-size reservation is therefore required.
            previous_live_bytes: 0,
            is_update,
            admission_preapproved: false,
        }) {
            Ok(reservation) => reservation,
            Err(reason) => return Ok(PutOutcome::Rejected(reason)),
        };
        let (manifest, version) = self.inner.manifest.allocate_volatile_version()?;
        let usage_commit = policy_reservation.take_lower_commit()?;
        let receipt = match self.inner.disk.put(DiskPutRequest {
            namespace,
            key,
            value,
            options,
            route,
            cache_id: manifest.cache_id,
            version,
            policy: self.inner.policy.as_ref(),
            usage_commit,
        }) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.poison();
                return Err(error.error);
            }
        };
        if receipt.outcome != PutOutcome::Stored {
            // Lower engines reject only before publishing a mutation. The
            // session remains dirty, so an unclean restart discards both tiers.
            return Ok(receipt.outcome);
        }
        let Some(disk_live_bytes) = receipt.new_usage.map(|usage| usage.live_bytes) else {
            self.poison();
            return Err(CacheError::CorruptMetadata(
                "stored managed Region put omitted its live usage",
            ));
        };
        if !policy_reservation.commit_lower_published(self.inner.policy.as_ref()) {
            self.poison();
            return Err(CacheError::CorruptMetadata(
                "managed lower put omitted its namespace commit",
            ));
        }
        // The replacement may be too large for its fixed L1 shard. Remove the
        // previous clean copy before attempting admission so a skipped L1
        // insert cannot leave an older value shadowing the committed L2 value.
        let replaced_memory = self.inner.memory.remove(namespace, key);
        if let Some(entry) = replaced_memory.as_ref().filter(|entry| !entry.disk_clean) {
            // Write-back may route an oversized replacement through this
            // synchronous path while the old version is still dirty in L1.
            // The new physical receipt is already committed above, so retiring
            // the exact pending charge here cannot expose a capacity undercount.
            self.retire_dirty_memory_usage(entry)?;
        }
        let (memory_key, memory_value) = match (
            try_clone_bytes(key, OverloadReason::WriteBufferUnavailable),
            try_clone_bytes(value, OverloadReason::WriteBufferUnavailable),
        ) {
            (Ok(key), Ok(value)) => (key, value),
            _ => {
                // L2 is already committed. L1 is an optional clean copy, so
                // local allocation pressure must not turn a stored write into
                // an ambiguous error.
                self.inner
                    .counters
                    .promotion_skips
                    .fetch_add(1, Ordering::Relaxed);
                self.inner.counters.puts.fetch_add(1, Ordering::Relaxed);
                return Ok(PutOutcome::Stored);
            }
        };
        let memory_entry = MemoryEntry::new_versioned_with_pending_disk_bytes(
            namespace,
            memory_key,
            memory_value,
            options.expires_at_unix_ms,
            version,
            true,
            disk_live_bytes,
        );
        match self.inner.memory.put(memory_entry) {
            MemoryPutResult::Stored { .. } => {}
            MemoryPutResult::NotStored { .. } => {}
        }
        self.inner.counters.puts.fetch_add(1, Ordering::Relaxed);
        Ok(PutOutcome::Stored)
    }

    pub fn remove(&self, key: &[u8]) -> Result<RemoveOutcome> {
        self.remove_in(0, key)
    }

    pub fn remove_in(&self, namespace: NamespaceId, key: &[u8]) -> Result<RemoveOutcome> {
        self.ensure_accepting()?;
        self.ensure_mutation_healthy()?;
        let _request = self
            .inner
            .requests
            .acquire(HybridRequestKind::Write, key.len())
            .map_err(|reason| self.request_gate_error(reason))?;
        self.remove_in_admitted(namespace, key)
    }

    pub(crate) fn remove_in_admitted(
        &self,
        namespace: NamespaceId,
        key: &[u8],
    ) -> Result<RemoveOutcome> {
        let started = Instant::now();
        let result = (|| -> Result<RemoveOutcome> {
            self.ensure_mutation_healthy()?;
            if !self.inner.policy.namespaces().contains(namespace) {
                return Ok(RemoveOutcome::NotFound);
            }
            let hash = hybrid_hash(namespace, key);
            let _operation = self.read_operation_admitted()?;
            self.ensure_mutation_healthy()?;
            let _ordering = self.lock_key_after_pending(namespace, key, hash)?;
            let _ = self.inner.manifest.allocate_volatile_version()?;
            let disk =
                match self
                    .inner
                    .disk
                    .remove(namespace, key, Some(self.inner.policy.as_ref()))
                {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        if error.phase == DiskMutationPhase::CommittedOrUncertain {
                            self.poison();
                        }
                        return Err(error.error);
                    }
                };
            let memory = self.inner.memory.remove(namespace, key);
            if let Some(entry) = memory.as_ref().filter(|entry| !entry.disk_clean) {
                self.retire_dirty_memory_usage(entry)?;
            }
            let outcome = if disk == RemoveOutcome::Removed || memory.is_some() {
                RemoveOutcome::Removed
            } else {
                RemoveOutcome::NotFound
            };
            if outcome == RemoveOutcome::Removed {
                self.inner.counters.removes.fetch_add(1, Ordering::Relaxed);
            }
            Ok(outcome)
        })();
        self.record_operation(
            CacheOperation::Remove,
            &result,
            |outcome| match outcome {
                RemoveOutcome::Removed => RequestResultClass::Removed,
                RemoveOutcome::NotFound => RequestResultClass::NotFound,
            },
            started.elapsed(),
        );
        result
    }

    pub fn clear(&self) -> Result<()> {
        let started = Instant::now();
        let _background_pause = self
            .inner
            .write_back
            .as_ref()
            .map(WriteBackExecutor::pause_background);
        let result = (|| {
            let _operation = self.write_operation()?;
            self.clear_locked_with_checkpoint_boundary(|| Ok(()))
        })();
        self.record_operation(
            CacheOperation::Clear,
            &result,
            |_| RequestResultClass::Success,
            started.elapsed(),
        );
        result
    }

    /// Run clear while the caller owns the exclusive operation lock. The
    /// boundary callback lets crash tests stop after a full checkpoint has
    /// made the existing journal reusable but before the replacement Clear
    /// intent is appended.
    fn clear_locked_with_checkpoint_boundary<F>(&self, after_checkpoint: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        self.ensure_mutation_healthy()?;
        if let Err(error) = self.inner.manifest.begin_volatile_clear() {
            self.poison();
            return Err(error);
        }
        after_checkpoint()?;
        self.discard_volatile_lower_state()
    }

    /// Publish empty lower generations and forget every L1/policy charge. The
    /// caller owns the exclusive Hybrid operation lock. No manifest version is
    /// allocated here so close can use it after the compatibility journal has
    /// stopped; the session dirty fence already makes any interrupted clear a
    /// safe-empty restart.
    fn discard_volatile_lower_state(&self) -> Result<()> {
        if let Err(error) = self.inner.disk.clear() {
            self.poison();
            return Err(error.error);
        }
        self.inner.memory.clear();
        self.inner.policy.namespaces().reset_live_bytes();
        self.inner
            .volatile_lower_loss
            .store(false, Ordering::Release);
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let started = Instant::now();
        let _background_pause = self
            .inner
            .write_back
            .as_ref()
            .map(WriteBackExecutor::pause_background);
        let result = (|| {
            let _operation = self.write_operation()?;
            self.checkpoint_locked()
        })();
        self.record_operation(
            CacheOperation::Flush,
            &result,
            |_| RequestResultClass::Success,
            started.elapsed(),
        );
        result
    }

    /// Publish one complete global checkpoint while the exclusive operation
    /// lock is held. Normally dirty L1 values drain first; pending volatile
    /// loss instead clears L1/L2. Both lower boundaries precede the clean
    /// Hybrid manifest generation.
    fn checkpoint_locked(&self) -> Result<()> {
        self.ensure_mutation_healthy()?;
        if self.inner.volatile_lower_loss.load(Ordering::Acquire) {
            // Bucket invalidation is intentionally process-local. Once any
            // pressure drop occurs, a clean boundary discards the complete
            // cache instead of scanning hidden buckets or risking old-value
            // revival. This is constant metadata work, not per-key cleanup.
            self.discard_volatile_lower_state()?;
        } else {
            self.flush_dirty_entries()?;
        }
        // Never allow a crash to observe a newer clean lower checkpoint with
        // an older clean Hybrid usage extension. An empty dirty journal is a
        // deliberate safe-miss fence during recovery.
        if let Err(error) = self.inner.manifest.mark_dirty_for_lower_checkpoint() {
            self.poison();
            return Err(error);
        }
        match self
            .inner
            .disk
            .with_mutations_frozen_after_flush(|| self.publish_manifest_clean_with_policy_usage())
        {
            // The cache remains open after either a dirty drain or safe-empty
            // boundary. Re-arm the one session-level dirty fence here so the
            // next put is still metadata-I/O free. A normal `close` publishes
            // its final clean manifest after request admission has stopped.
            Ok(()) => self
                .inner
                .manifest
                .mark_dirty_for_lower_checkpoint()
                .inspect_err(|_| self.poison()),
            Err(error) => {
                self.poison();
                Err(error.error)
            }
        }
    }

    fn publish_manifest_clean_with_policy_usage(&self) -> Result<()> {
        policy_usage_snapshot(self.inner.policy.as_ref())
            .and_then(|usage| {
                self.inner
                    .manifest
                    .publish_clean_with_usage(&usage)
                    .map(|_| ())
            })
            .inspect_err(|_| {
                self.poison();
            })
    }

    pub fn close(&self) -> Result<()> {
        self.stop_admission_for_close();
        let async_handle = lock_mutex(&self.inner.async_handle)
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        if let Some(inner) = async_handle {
            return crate::async_hybrid::AsyncHybridCache::from_inner(inner)
                .close()
                .wait();
        }
        let started = Instant::now();
        self.inner.async_close_completion.start();
        let result = self.close_after_async_drain();
        self.inner
            .async_close_completion
            .complete(result.is_ok(), started.elapsed());
        result
    }

    /// Begin an uncancellable asynchronous drain and wait for at most
    /// `timeout`. `TimedOut` means the same close owner is still draining; it
    /// does not publish `Closed`, release any file lock, or cancel submitted
    /// I/O. Calling `close` or `close_with_timeout` again joins that owner.
    pub fn close_with_timeout(&self, timeout: Duration) -> Result<()> {
        let started = Instant::now();
        let asynchronous = {
            let mut shared = lock_mutex(&self.inner.async_handle);
            if let Some(inner) = shared.as_ref().and_then(std::sync::Weak::upgrade) {
                Some(crate::async_hybrid::AsyncHybridCache::from_inner(inner))
            } else if !self.inner.accepting.load(Ordering::Acquire) {
                None
            } else {
                match self.status() {
                    CacheStatus::Healthy | CacheStatus::MissOnly | CacheStatus::Poisoned => {}
                    CacheStatus::Closed => return Ok(()),
                }
                let handle = crate::async_hybrid::AsyncHybridCache::try_new(
                    Arc::clone(&self.inner),
                    self.inner.async_config,
                )?;
                *shared = Some(Arc::downgrade(handle.shared_inner()));
                Some(handle)
            }
        };
        let remaining = timeout.saturating_sub(started.elapsed());
        if let Some(asynchronous) = asynchronous {
            return asynchronous.close().wait_timeout(remaining);
        }
        self.wait_for_concurrent_close(remaining)
    }

    fn wait_for_concurrent_close(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = lock_mutex(&self.inner.state);
        loop {
            if state.status == CacheStatus::Closed {
                return Ok(());
            }
            let Some(remaining) =
                deadline.and_then(|deadline| deadline.checked_duration_since(Instant::now()))
            else {
                return Err(CacheError::TimedOut);
            };
            let (next, timed_out) = self
                .inner
                .close_changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if timed_out.timed_out() && state.status != CacheStatus::Closed {
                return Err(CacheError::TimedOut);
            }
        }
    }

    pub(crate) fn close_after_async_drain(&self) -> Result<()> {
        let started = Instant::now();
        let result = self.close_storage_after_async_drain();
        self.record_operation(
            CacheOperation::Close,
            &result,
            |_| RequestResultClass::Success,
            started.elapsed(),
        );
        result
    }

    fn close_storage_after_async_drain(&self) -> Result<()> {
        self.inner.requests.wait_idle();
        let _background_pause = self
            .inner
            .write_back
            .as_ref()
            .map(WriteBackExecutor::pause_background);
        let _operation = self.write_operation_allow_closed()?;
        {
            let state = lock_mutex(&self.inner.state);
            if state.status == CacheStatus::Closed {
                return Ok(());
            }
        }
        let journal = self.inner.journal.shutdown();
        let status = self.status();
        let dirty = if status == CacheStatus::Healthy && journal.is_ok() {
            if self.inner.volatile_lower_loss.load(Ordering::Acquire) {
                self.discard_volatile_lower_state()
            } else {
                self.flush_dirty_entries()
            }
        } else {
            Err(CacheError::Poisoned)
        };
        let global_dirty = if dirty.is_ok() {
            self.inner.manifest.mark_dirty_for_lower_checkpoint()
        } else {
            Err(CacheError::Poisoned)
        };
        // Region close stops every autonomous worker and publishes its final
        // lower checkpoint before releasing the file lock. Namespace usage is
        // stable only after that drain, so publish the Hybrid clean snapshot
        // afterwards rather than racing a late reinsertion or reclaim.
        let disk = if global_dirty.is_ok() {
            self.inner.disk.close()
        } else {
            // A failed global fence may have written no dirty manifest slot.
            // Lower tiers must therefore drain and unlock without publishing
            // clean checkpoints that an older clean usage snapshot could trust.
            self.inner.disk.close_without_checkpoint()
        };
        let publish = if global_dirty.is_ok() && disk.is_ok() {
            self.publish_manifest_clean_with_policy_usage()
        } else {
            Err(CacheError::Poisoned)
        };
        let manifest = self.inner.manifest.close();
        let write_back = if self
            .inner
            .write_back
            .as_ref()
            .is_none_or(WriteBackExecutor::shutdown)
        {
            Ok(())
        } else {
            Err(CacheError::Poisoned)
        };
        self.inner.memory.clear();
        let previous = {
            let mut state = lock_mutex(&self.inner.state);
            let previous = state.status;
            state.status = CacheStatus::Closed;
            previous
        };
        if previous != CacheStatus::Closed {
            self.inner.telemetry.record_transition(
                previous,
                CacheStatus::Closed,
                StateChangeReason::Closing,
            );
        }
        self.inner.close_changed.notify_all();
        journal
            .and(dirty)
            .and(global_dirty)
            .and(disk)
            .and(publish)
            .and(manifest)
            .and(write_back)
    }

    pub fn status(&self) -> CacheStatus {
        match lock_mutex(&self.inner.state).status {
            CacheStatus::Healthy => {}
            status => return status,
        }
        match self.inner.manifest.status() {
            CacheStatus::Healthy => {}
            CacheStatus::Closed => return CacheStatus::Closed,
            CacheStatus::MissOnly => return CacheStatus::MissOnly,
            CacheStatus::Poisoned => return CacheStatus::Poisoned,
        }
        match (
            self.inner.disk.region.status(),
            self.inner.disk.bucket.status(),
        ) {
            (CacheStatus::Healthy, CacheStatus::Healthy) => CacheStatus::Healthy,
            _ => CacheStatus::MissOnly,
        }
    }

    pub fn health_snapshot(&self) -> HybridHealthSnapshot {
        let manifest = self.inner.manifest.status();
        let bucket = self.inner.disk.bucket.status();
        let region = self.inner.disk.region.status();
        let coordinator = lock_mutex(&self.inner.state).status;
        let journal = self.inner.journal.snapshot();
        HybridHealthSnapshot {
            overall: self.status(),
            manifest,
            bucket,
            region,
            memory_reads_available: self.inner.accepting.load(Ordering::Acquire)
                && coordinator == CacheStatus::Healthy
                && manifest == CacheStatus::Healthy,
            mutations_available: self.inner.accepting.load(Ordering::Acquire)
                && coordinator == CacheStatus::Healthy
                && manifest == CacheStatus::Healthy
                && bucket == CacheStatus::Healthy
                && region == CacheStatus::Healthy
                && journal.accepting
                && journal.worker_panics == 0,
        }
    }

    /// Supply an externally collected SMART/NVMe health sample to the unified
    /// driver policy.
    pub fn observe_nvme_health(&self, sample: NvmeHealthSample) -> NvmeHealthStats {
        self.inner.policy.observe_nvme_health(sample)
    }

    pub fn policy_snapshot(&self) -> Result<HybridPolicySnapshot> {
        let namespaces = self
            .inner
            .policy
            .namespaces()
            .try_snapshots()
            .map_err(|_| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
        Ok(HybridPolicySnapshot {
            admission: self.inner.policy.admission().snapshot(),
            namespaces,
            host_writes: self.inner.policy.host_writes().snapshot(),
            nvme_health: self.inner.policy.nvme_health(),
        })
    }

    pub fn stats(&self) -> HybridCacheStats {
        let memory = self.inner.memory.stats();
        let requests = self.inner.requests.snapshot();
        let write_back = self
            .inner
            .write_back
            .as_ref()
            .map(WriteBackExecutor::snapshot)
            .unwrap_or_default();
        let manifest = self.inner.manifest.snapshot().ok();
        let journal_capacity_bytes = manifest.map_or(0, |snapshot| snapshot.journal_capacity);
        let journal_remaining_bytes = self.inner.manifest.remaining_journal_bytes().unwrap_or(0);
        HybridCacheStats {
            open: self.inner.open_stats,
            memory_capacity_bytes: memory.capacity_bytes as u64,
            memory_charged_bytes: memory.charged_bytes as u64,
            memory_entries: memory.entries as u64,
            memory_dirty_entries: memory.dirty_entries as u64,
            memory_dirty_bytes: memory.dirty_bytes as u64,
            memory_hits: self.inner.counters.memory_hits.load(Ordering::Relaxed),
            small_disk_hits: self.inner.counters.small_disk_hits.load(Ordering::Relaxed),
            region_disk_hits: self.inner.counters.region_disk_hits.load(Ordering::Relaxed),
            misses: self.inner.counters.misses.load(Ordering::Relaxed),
            promotions: self.inner.counters.promotions.load(Ordering::Relaxed),
            promotion_skips: self.inner.counters.promotion_skips.load(Ordering::Relaxed),
            puts: self.inner.counters.puts.load(Ordering::Relaxed),
            removes: self.inner.counters.removes.load(Ordering::Relaxed),
            memory_evictions: memory.evictions,
            journal_capacity_bytes,
            journal_used_bytes: journal_capacity_bytes.saturating_sub(journal_remaining_bytes),
            journal_rollovers: self
                .inner
                .counters
                .journal_rollovers
                .load(Ordering::Relaxed),
            journal_rollover_wait_ns: self
                .inner
                .counters
                .journal_rollover_wait_ns
                .load(Ordering::Relaxed),
            journal_rollover_max_ns: self
                .inner
                .counters
                .journal_rollover_max_ns
                .load(Ordering::Relaxed),
            requests_in_flight: requests.in_flight,
            requests_in_flight_peak: requests.in_flight_peak,
            request_bytes_in_use: requests.bytes_in_use,
            request_bytes_peak: requests.bytes_peak,
            request_rejections: requests.rejections,
            request_wait_ns: requests.wait_ns,
            journal_group_commit: journal_group_commit_stats(self.inner.journal.snapshot()),
            write_back: self.write_back_stats(write_back),
            admission: self.inner.policy.admission().snapshot(),
            host_writes: self.inner.policy.host_writes().snapshot(),
            nvme_health: self.inner.policy.nvme_health(),
            bucket: self.inner.disk.bucket.stats(),
            region: self.inner.disk.region.stats(),
        }
    }

    /// Fixed-cardinality end-to-end request telemetry plus bounded lifecycle
    /// history for the complete Hybrid coordinator.
    pub fn metrics_snapshot(&self) -> HybridMetricsSnapshot {
        let asynchronous = lock_mutex(&self.inner.async_handle)
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .map(crate::async_hybrid::AsyncHybridCache::from_inner);
        HybridMetricsSnapshot {
            health: self.health_snapshot(),
            stats: self.stats(),
            operations: self.inner.telemetry.operation_snapshots(),
            async_queue: asynchronous
                .as_ref()
                .map(crate::async_hybrid::AsyncHybridCache::queue_stats),
            async_close: Some(self.inner.async_close_completion.snapshot()),
            state_transitions: self.inner.telemetry.state_transitions(),
        }
    }

    fn write_back_stats(&self, queue: WriteBackSnapshot) -> HybridWriteBackStats {
        let pending = self.inner.pending_writes.snapshot();
        HybridWriteBackStats {
            enabled: self.inner.write_mode == HybridWriteMode::WriteBack,
            memory_only_puts: self.inner.counters.write_back_puts.load(Ordering::Relaxed),
            write_through_fallbacks: self
                .inner
                .counters
                .write_through_fallbacks
                .load(Ordering::Relaxed),
            demotion_attempts: self
                .inner
                .counters
                .demotion_attempts
                .load(Ordering::Relaxed),
            demotion_failures: self
                .inner
                .counters
                .demotion_failures
                .load(Ordering::Relaxed),
            demoted_entries: self.inner.counters.demoted_entries.load(Ordering::Relaxed),
            demoted_bytes: self.inner.counters.demoted_bytes.load(Ordering::Relaxed),
            dirty_expiry_fences: self
                .inner
                .counters
                .dirty_expiry_fences
                .load(Ordering::Relaxed),
            lower_absent_evictions: self
                .inner
                .counters
                .lower_absent_evictions
                .load(Ordering::Relaxed),
            lower_candidate_evictions: self
                .inner
                .counters
                .lower_candidate_evictions
                .load(Ordering::Relaxed),
            synchronous_demotions: self
                .inner
                .counters
                .synchronous_demotions
                .load(Ordering::Relaxed),
            dropped_evictions: self
                .inner
                .counters
                .dropped_evictions
                .load(Ordering::Relaxed),
            proactive_scheduled: self
                .inner
                .counters
                .proactive_scheduled
                .load(Ordering::Relaxed),
            proactive_skipped: self
                .inner
                .counters
                .proactive_skipped
                .load(Ordering::Relaxed),
            proactive_persisted: self
                .inner
                .counters
                .proactive_persisted
                .load(Ordering::Relaxed),
            proactive_rejected: self
                .inner
                .counters
                .proactive_rejected
                .load(Ordering::Relaxed),
            proactive_fatal: self.inner.counters.proactive_fatal.load(Ordering::Relaxed),
            proactive_invalidated: self
                .inner
                .counters
                .proactive_invalidated
                .load(Ordering::Relaxed),
            volatile_loss_pending: self.inner.volatile_lower_loss.load(Ordering::Acquire),
            pending_entries: pending.entries,
            pending_entries_peak: pending.entries_peak,
            pending_bytes: pending.bytes,
            pending_bytes_peak: pending.bytes_peak,
            pending_lookup_misses: self
                .inner
                .counters
                .pending_lookup_misses
                .load(Ordering::Relaxed),
            pending_same_key_waits: self
                .inner
                .counters
                .pending_same_key_waits
                .load(Ordering::Relaxed),
            pending_same_key_wait_ns: self
                .inner
                .counters
                .pending_same_key_wait_ns
                .load(Ordering::Relaxed),
            queue_capacity: queue.queue_capacity,
            queue_in_flight: queue.in_flight,
            queue_in_flight_peak: queue.in_flight_peak,
            memory_capacity_bytes: queue.memory_capacity_bytes,
            memory_in_use_bytes: queue.bytes_in_use,
            memory_peak_bytes: queue.bytes_peak,
            queue_submitted: queue.submitted,
            queue_completed: queue.completed,
            queue_rejections: queue.rejected,
            worker_panics: queue.worker_panics,
            queue_wait_ns: queue.wait_ns,
        }
    }

    fn read_operation_admitted(&self) -> Result<RwLockReadGuard<'_, ()>> {
        let guard = self
            .inner
            .operation
            .read()
            .map_err(|_| CacheError::Poisoned)?;
        if lock_mutex(&self.inner.state).status == CacheStatus::Closed {
            return Err(CacheError::Closed);
        }
        Ok(guard)
    }

    fn ensure_accepting(&self) -> Result<()> {
        if self.inner.accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(CacheError::Closed)
        }
    }

    fn request_gate_error(&self, reason: OverloadReason) -> CacheError {
        if self.inner.accepting.load(Ordering::Acquire) {
            CacheError::Overloaded(reason)
        } else {
            CacheError::Closed
        }
    }

    fn write_operation(&self) -> Result<RwLockWriteGuard<'_, ()>> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(CacheError::Closed);
        }
        let guard = self.write_operation_allow_closed()?;
        if lock_mutex(&self.inner.state).status == CacheStatus::Closed {
            return Err(CacheError::Closed);
        }
        Ok(guard)
    }

    fn write_operation_allow_closed(&self) -> Result<RwLockWriteGuard<'_, ()>> {
        self.inner
            .operation
            .write()
            .map_err(|_| CacheError::Poisoned)
    }

    fn lock_key(&self, hash: u64) -> MutexGuard<'_, ()> {
        lock_mutex(&self.inner.ordering[hash as usize & (self.inner.ordering.len() - 1)])
    }

    /// Serialize a foreground mutation behind only an exact same-key detached
    /// write. Waiting never retains the coarse ordering stripe, so unrelated
    /// keys sharing a shard continue to make progress.
    fn lock_key_after_pending<'a>(
        &'a self,
        namespace: NamespaceId,
        key: &[u8],
        hash: u64,
    ) -> Result<MutexGuard<'a, ()>> {
        loop {
            let ordering = self.lock_key(hash);
            let Some(pending) = self.inner.pending_writes.find(namespace, key, hash) else {
                return Ok(ordering);
            };
            drop(ordering);
            self.inner
                .counters
                .pending_same_key_waits
                .fetch_add(1, Ordering::Relaxed);
            let (outcome, elapsed) = pending.wait_finished();
            add_request_wait(&self.inner.counters.pending_same_key_wait_ns, elapsed);
            if outcome == PendingWaitOutcome::Failed {
                return Err(CacheError::Poisoned);
            }
            self.ensure_mutation_healthy()?;
        }
    }

    fn ensure_mutation_healthy(&self) -> Result<()> {
        match lock_mutex(&self.inner.state).status {
            CacheStatus::Closed => return Err(CacheError::Closed),
            CacheStatus::Poisoned | CacheStatus::MissOnly => return Err(CacheError::Poisoned),
            CacheStatus::Healthy => {}
        }
        if self.inner.manifest.status() != CacheStatus::Healthy
            || self.inner.disk.region.status() != CacheStatus::Healthy
            || self.inner.disk.bucket.status() != CacheStatus::Healthy
            || {
                let journal = self.inner.journal.snapshot();
                !journal.accepting || journal.worker_panics != 0
            }
        {
            return Err(CacheError::Poisoned);
        }
        Ok(())
    }

    fn demote_dirty_entry_with_usage(
        &self,
        entry: &MemoryEntry,
    ) -> std::result::Result<u64, DirtyPersistFailure> {
        // MemoryEngine uses the same namespace+key hash and shard mask as the
        // Hybrid ordering stripes. A put can therefore evict only a dirty
        // victim protected by the ordering lock already held by that put.
        // Waiting for this worker before eviction preserves same-key ordering
        // without acquiring a second (and potentially deadlocking) key lock.
        self.inner
            .counters
            .demotion_attempts
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .counters
            .synchronous_demotions
            .fetch_add(1, Ordering::Relaxed);
        let persisted_bytes = entry.charged_bytes().unwrap_or(0);
        let outcome = (|| {
            let executor = self
                .inner
                .write_back
                .as_ref()
                .ok_or(DirtyPersistFailure::Fatal(CacheError::Poisoned))?;
            let bytes = entry.charged_bytes().ok_or(DirtyPersistFailure::Rejected(
                RejectReason::BufferUnavailable,
            ))?;
            let reservation = executor.reserve(bytes).map_err(write_back_run_failure)?;
            let owned = try_clone_memory_entry(entry).map_err(|error| match error {
                CacheError::Overloaded(_) => {
                    DirtyPersistFailure::Rejected(RejectReason::BufferUnavailable)
                }
                error => DirtyPersistFailure::Fatal(error),
            })?;
            let cache = self.clone();
            executor
                .run(reservation, move || cache.persist_dirty_entry(&owned))
                .map_err(write_back_run_failure)?
        })();
        if outcome.is_err() {
            self.inner
                .counters
                .demotion_failures
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner
                .counters
                .demoted_entries
                .fetch_add(1, Ordering::Relaxed);
            self.inner
                .counters
                .demoted_bytes
                .fetch_add(persisted_bytes as u64, Ordering::Relaxed);
        }
        outcome
    }

    /// Prepare a dirty victim before L1 releases it. Asynchronous persistence
    /// transfers ownership through the bounded exact-key pending directory.
    /// Under pressure, a proven lower absence may be dropped immediately; a
    /// lower candidate is synchronously hidden in memory, so it needs neither a
    /// pending owner nor an SSD mutation.
    fn prepare_dirty_eviction(
        &self,
        entry: &MemoryEntry,
    ) -> std::result::Result<u64, DirtyPersistFailure> {
        let lower_candidate = self
            .inner
            .disk
            .may_contain_for_admission(entry.namespace, &entry.key)
            .map_err(DirtyPersistFailure::Fatal)?;
        if lower_candidate {
            self.inner
                .counters
                .lower_candidate_evictions
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner
                .counters
                .lower_absent_evictions
                .fetch_add(1, Ordering::Relaxed);
        }

        self.inner
            .counters
            .demotion_attempts
            .fetch_add(1, Ordering::Relaxed);
        if self.schedule_async_eviction(entry, lower_candidate)? {
            return Ok(0);
        }

        self.inner
            .counters
            .proactive_skipped
            .fetch_add(1, Ordering::Relaxed);
        if lower_candidate {
            self.inner
                .counters
                .demotion_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(DirtyPersistFailure::Rejected(
                RejectReason::BufferUnavailable,
            ));
        }

        // Absence was proven while the L1 shard still protected the victim, so
        // dropping it exposes only a miss. This is the cache-loss overload path.
        self.retire_dirty_memory_usage(entry)
            .map_err(DirtyPersistFailure::Fatal)?;
        self.inner
            .counters
            .dropped_evictions
            .fetch_add(1, Ordering::Relaxed);
        Ok(0)
    }

    fn schedule_async_eviction(
        &self,
        entry: &MemoryEntry,
        lower_candidate: bool,
    ) -> std::result::Result<bool, DirtyPersistFailure> {
        let Some(executor) = self.inner.write_back.as_ref() else {
            return Ok(false);
        };
        let Some(owned_bytes) = entry.charged_bytes() else {
            return Ok(false);
        };
        let Some(reservation_bytes) = owned_bytes
            .checked_add(entry.key.len())
            .and_then(|bytes| bytes.checked_add(PENDING_WRITE_OWNED_OVERHEAD_BYTES))
        else {
            return Ok(false);
        };
        let reservation = if lower_candidate {
            match executor.try_reserve_lower_candidate(reservation_bytes) {
                Some(LowerCandidateAdmission::Persist(reservation)) => reservation,
                Some(LowerCandidateAdmission::Invalidate) => {
                    self.invalidate_dirty_in_memory(
                        entry.namespace,
                        &entry.key,
                        entry.pending_disk_bytes,
                    )?;
                    return Ok(true);
                }
                None => return Ok(false),
            }
        } else {
            let Some(reservation) = executor.try_reserve_background(reservation_bytes) else {
                return Ok(false);
            };
            reservation
        };
        let owned = match try_clone_memory_entry(entry) {
            Ok(entry) => entry,
            Err(CacheError::Overloaded(_)) => {
                drop(reservation);
                return Ok(false);
            }
            Err(error) => {
                drop(reservation);
                return Err(DirtyPersistFailure::Fatal(error));
            }
        };
        let hash = hybrid_hash(entry.namespace, &entry.key);
        let pending = match self.inner.pending_writes.try_register(
            entry.namespace,
            &entry.key,
            hash,
            entry.version,
            reservation_bytes,
        ) {
            Ok(pending) => pending,
            Err(PendingRegisterError::AlreadyPending | PendingRegisterError::AllocationFailed) => {
                drop(reservation);
                return Ok(false);
            }
        };
        let worker = self.clone();
        let panic_owner = self.clone();
        let worker_pending = Arc::clone(&pending);
        let panic_pending = Arc::clone(&pending);
        match executor.submit_background(
            reservation,
            move || worker.run_async_eviction(owned, worker_pending),
            move || {
                panic_owner
                    .inner
                    .counters
                    .proactive_fatal
                    .fetch_add(1, Ordering::Relaxed);
                panic_owner.poison();
                panic_owner.inner.pending_writes.fail(&panic_pending);
            },
        ) {
            Ok(()) => {
                self.inner
                    .counters
                    .proactive_scheduled
                    .fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            Err(WriteBackRunError::Overloaded(_) | WriteBackRunError::Closed) => {
                self.inner.pending_writes.finish(&pending);
                Ok(false)
            }
            Err(WriteBackRunError::WorkerPanicked) => {
                self.poison();
                self.inner.pending_writes.fail(&pending);
                Err(DirtyPersistFailure::Fatal(CacheError::Poisoned))
            }
        }
    }

    fn run_async_eviction(&self, entry: MemoryEntry, pending: Arc<PendingWriteSlot>) {
        let result = (|| -> std::result::Result<(), DirtyPersistFailure> {
            let _operation = self
                .inner
                .operation
                .read()
                .map_err(|_| DirtyPersistFailure::Fatal(CacheError::Poisoned))?;
            debug_assert_eq!(pending.version(), entry.version);
            match self.persist_dirty_entry(&entry) {
                Ok(_) => {
                    self.inner
                        .counters
                        .demoted_entries
                        .fetch_add(1, Ordering::Relaxed);
                    self.inner
                        .counters
                        .demoted_bytes
                        .fetch_add(entry.charged_bytes().unwrap_or(0) as u64, Ordering::Relaxed);
                    self.inner
                        .counters
                        .proactive_persisted
                        .fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
                Err(DirtyPersistFailure::Rejected(_)) => {
                    self.inner
                        .counters
                        .proactive_rejected
                        .fetch_add(1, Ordering::Relaxed);
                    self.invalidate_dirty_in_memory(
                        entry.namespace,
                        &entry.key,
                        entry.pending_disk_bytes,
                    )
                }
                Err(failure) => Err(failure),
            }
        })();
        match result {
            Ok(()) => self.inner.pending_writes.finish(&pending),
            Err(_) => {
                self.inner
                    .counters
                    .proactive_fatal
                    .fetch_add(1, Ordering::Relaxed);
                self.poison();
                self.inner.pending_writes.fail(&pending);
            }
        }
    }

    fn invalidate_dirty_in_memory(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        pending_disk_bytes: u64,
    ) -> std::result::Result<(), DirtyPersistFailure> {
        // A crash already sees the session dirty fence and reopens empty. Keep
        // this flag before changing volatile lower visibility so an orderly
        // flush/close also publishes only a safe empty checkpoint.
        self.inner
            .volatile_lower_loss
            .store(true, Ordering::Release);
        self.inner
            .disk
            .invalidate_in_memory(namespace, key)
            .map_err(DirtyPersistFailure::Fatal)?;
        self.retire_pending_namespace_usage(NamespaceUsage {
            namespace,
            live_bytes: pending_disk_bytes,
        })
        .map_err(DirtyPersistFailure::Fatal)?;
        self.inner
            .counters
            .proactive_invalidated
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .counters
            .dropped_evictions
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn persist_dirty_entry(
        &self,
        entry: &MemoryEntry,
    ) -> std::result::Result<u64, DirtyPersistFailure> {
        let cache_id = self
            .inner
            .manifest
            .snapshot()
            .map_err(DirtyPersistFailure::Fatal)?
            .cache_id;
        self.persist_dirty_entry_with_cache_id(entry, cache_id)
    }

    fn persist_dirty_entry_with_cache_id(
        &self,
        entry: &MemoryEntry,
        cache_id: [u8; 16],
    ) -> std::result::Result<u64, DirtyPersistFailure> {
        if entry
            .expires_at_unix_ms
            .is_some_and(|expires_at| expires_at <= now_unix_ms())
        {
            self.inner
                .disk
                .remove(
                    entry.namespace,
                    &entry.key,
                    Some(self.inner.policy.as_ref()),
                )
                .map_err(dirty_persist_failure)?;
            self.retire_dirty_memory_usage(entry)
                .map_err(DirtyPersistFailure::Fatal)?;
            self.inner
                .counters
                .dirty_expiry_fences
                .fetch_add(1, Ordering::Relaxed);
            return Ok(0);
        }
        let route = self
            .inner
            .disk
            .route_put(entry.key.len(), entry.value.len())
            .map_err(|outcome| match outcome {
                PutOutcome::Rejected(reason) => DirtyPersistFailure::Rejected(reason),
                PutOutcome::Stored => DirtyPersistFailure::Fatal(CacheError::Poisoned),
            })?;
        let pending_usage = NamespaceUsage {
            namespace: entry.namespace,
            live_bytes: entry.pending_disk_bytes,
        };
        let receipt = self
            .inner
            .disk
            .put(DiskPutRequest {
                namespace: entry.namespace,
                key: &entry.key,
                value: &entry.value,
                options: PutOptions {
                    expires_at_unix_ms: entry.expires_at_unix_ms,
                },
                route,
                cache_id,
                version: entry.version,
                policy: self.inner.policy.as_ref(),
                usage_commit: LowerUsageCommit::Pending {
                    namespaces: Arc::clone(self.inner.policy.namespaces()),
                    maximum_live_bytes: entry.pending_disk_bytes,
                },
            })
            .map_err(dirty_persist_failure)?;
        match receipt.outcome {
            PutOutcome::Stored => {
                let disk_live_bytes = receipt
                    .new_usage
                    .ok_or(DirtyPersistFailure::Fatal(CacheError::CorruptMetadata(
                        "stored dirty demotion omitted its live usage",
                    )))?
                    .live_bytes;
                // Keep the conservative pending charge live until the exact
                // physical receipt has been added and its prior lower value
                // retired. Removing it first exposes a transient undercount in
                // which another key can reserve past the hard capacity.
                self.retire_pending_namespace_usage(pending_usage)
                    .map_err(DirtyPersistFailure::Fatal)?;
                Ok(disk_live_bytes)
            }
            PutOutcome::Rejected(reason) => Err(DirtyPersistFailure::Rejected(reason)),
        }
    }

    fn handle_demotion_failure(&self, failure: DirtyPersistFailure) -> Result<PutOutcome> {
        match failure {
            DirtyPersistFailure::Rejected(reason) => Ok(PutOutcome::Rejected(reason)),
            DirtyPersistFailure::Fatal(error) => {
                if !matches!(error, CacheError::Closed) {
                    self.poison();
                }
                Err(error)
            }
        }
    }

    fn flush_dirty_entries(&self) -> Result<()> {
        let parallelism = self
            .inner
            .write_back
            .as_ref()
            .map_or(1, WriteBackExecutor::parallelism);
        match self.inner.memory.persist_all_dirty(parallelism, |entry| {
            self.demote_dirty_entry_with_usage(entry)
        }) {
            Ok(_) => Ok(()),
            Err(DirtyPersistFailure::Rejected(reason)) => Err(demotion_reject_error(reason)),
            Err(DirtyPersistFailure::Fatal(error)) => {
                self.poison();
                Err(error)
            }
        }
    }

    fn retire_dirty_memory_usage(&self, entry: &MemoryEntry) -> Result<()> {
        self.retire_pending_namespace_usage(NamespaceUsage {
            namespace: entry.namespace,
            live_bytes: entry.pending_disk_bytes,
        })
    }

    fn retire_pending_namespace_usage(&self, usage: NamespaceUsage) -> Result<()> {
        if self.inner.policy.namespaces().record_removal_exact(usage) {
            return Ok(());
        }
        self.poison();
        Err(CacheError::CorruptMetadata(
            "dirty pending usage exceeded namespace live usage",
        ))
    }

    fn reserve_put_policy(
        &self,
        request: PolicyPutRequest,
    ) -> std::result::Result<HybridPolicyReservation, RejectReason> {
        let PolicyPutRequest {
            namespace,
            hash,
            key_len,
            value_len,
            route,
            previous_live_bytes,
            is_update,
            admission_preapproved,
        } = request;
        self.admit_put_policy(namespace, hash, value_len, is_update, admission_preapproved)?;
        self.reserve_disk_policy(namespace, key_len, value_len, route, previous_live_bytes)
    }

    fn admit_put_policy(
        &self,
        namespace: NamespaceId,
        hash: u64,
        value_len: usize,
        is_update: bool,
        admission_preapproved: bool,
    ) -> std::result::Result<(), RejectReason> {
        let policy = self.inner.policy.as_ref();
        if !policy.namespaces().contains(namespace) {
            return Err(RejectReason::NamespaceNotConfigured);
        }
        if policy.should_reject_put() {
            return Err(RejectReason::DeviceHealth);
        }
        if !admission_preapproved
            && policy.admission().consider(hash, value_len, is_update) == AdmissionDecision::Reject
        {
            return Err(if value_len > crate::policy::LARGE_OBJECT_THRESHOLD_BYTES {
                RejectReason::LargeObjectCold
            } else {
                RejectReason::AdmissionFiltered
            });
        }
        Ok(())
    }

    fn reserve_disk_policy(
        &self,
        namespace: NamespaceId,
        key_len: usize,
        value_len: usize,
        route: DiskRoute,
        previous_live_bytes: u64,
    ) -> std::result::Result<HybridPolicyReservation, RejectReason> {
        let policy = self.inner.policy.as_ref();
        if policy.should_reject_put() {
            return Err(RejectReason::DeviceHealth);
        }
        let (live_bytes, write_bytes) = self
            .inner
            .disk
            .policy_charge(namespace, key_len, value_len, route)
            .ok_or(RejectReason::RecordTooLarge)?;
        let capacity = policy
            .namespaces()
            .try_reserve_capacity_replacing(namespace, live_bytes, previous_live_bytes)
            .map_err(namespace_reject_reason)?;
        let namespace_write = policy
            .namespaces()
            .try_reserve_write(namespace, write_bytes)
            .map_err(namespace_reject_reason)?;
        let daily_write = policy
            .host_writes()
            .try_reserve_daily(write_bytes)
            .map_err(|_| RejectReason::DailyWriteBudgetExceeded)?;
        Ok(HybridPolicyReservation {
            capacity: Some(capacity),
            namespace_write,
            daily_write,
            admitted_value_bytes: value_len as u64,
        })
    }

    fn record_operation<T>(
        &self,
        operation: CacheOperation,
        result: &Result<T>,
        success: impl FnOnce(&T) -> RequestResultClass,
        elapsed: Duration,
    ) {
        let (class, error) = match result {
            Ok(value) => (success(value), None),
            Err(error) => (
                hybrid_request_result_for_error(error),
                Some(hybrid_cache_error_class(error)),
            ),
        };
        self.inner
            .telemetry
            .observe(operation, class, error, elapsed);
    }

    fn poison(&self) {
        let mut state = lock_mutex(&self.inner.state);
        if state.status != CacheStatus::Closed {
            let previous = state.status;
            state.status = CacheStatus::Poisoned;
            if previous != CacheStatus::Poisoned {
                self.inner.telemetry.record_transition(
                    previous,
                    CacheStatus::Poisoned,
                    StateChangeReason::MetadataFailure,
                );
            }
        }
    }
}

struct DiskPair {
    bucket: BucketCache,
    region: DiskCache,
    small_object_max_bytes: usize,
    bucket_size_bytes: usize,
    bucket_maximum_item_bytes: usize,
    region_maximum_key_bytes: usize,
    region_maximum_value_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiskRoute {
    Bucket,
    Region,
}

struct PolicyPutRequest {
    namespace: NamespaceId,
    hash: u64,
    key_len: usize,
    value_len: usize,
    route: DiskRoute,
    previous_live_bytes: u64,
    is_update: bool,
    admission_preapproved: bool,
}

struct DiskPutRequest<'a> {
    namespace: NamespaceId,
    key: &'a [u8],
    value: &'a [u8],
    options: PutOptions,
    route: DiskRoute,
    cache_id: [u8; 16],
    version: HybridVersion,
    policy: &'a PolicyController,
    usage_commit: LowerUsageCommit,
}

struct DiskPutReceipt {
    outcome: PutOutcome,
    new_usage: Option<NamespaceUsage>,
}

struct HybridPolicyReservation {
    capacity: Option<NamespaceCapacityReservation>,
    namespace_write: NamespaceWriteReservation,
    daily_write: DailyWriteReservation,
    admitted_value_bytes: u64,
}

enum LowerUsageCommit {
    Reserved(NamespaceCapacityReservation),
    Pending {
        namespaces: Arc<NamespaceController>,
        maximum_live_bytes: u64,
    },
}

enum DirtyPersistFailure {
    Rejected(RejectReason),
    Fatal(CacheError),
}

impl HybridPolicyReservation {
    fn pending_live_bytes(&self) -> u64 {
        self.capacity
            .as_ref()
            .expect("uncommitted Hybrid reservation owns capacity")
            .live_bytes()
    }

    fn take_lower_commit(&mut self) -> Result<LowerUsageCommit> {
        self.capacity
            .take()
            .map(LowerUsageCommit::Reserved)
            .ok_or(CacheError::CorruptMetadata(
                "Hybrid capacity reservation was already consumed",
            ))
    }

    fn commit_pending(mut self, policy: &PolicyController, previous: Option<NamespaceUsage>) {
        self.capacity
            .take()
            .expect("pending Hybrid commit owns capacity")
            .commit(previous);
        self.namespace_write.commit();
        self.daily_write.commit();
        policy
            .host_writes()
            .record_admitted_value(self.admitted_value_bytes);
    }

    fn commit_lower_published(self, policy: &PolicyController) -> bool {
        let Self {
            capacity,
            namespace_write,
            daily_write,
            admitted_value_bytes,
        } = self;
        if capacity.is_some() {
            return false;
        }
        namespace_write.commit();
        daily_write.commit();
        policy
            .host_writes()
            .record_admitted_value(admitted_value_bytes);
        true
    }
}

impl LowerUsageCommit {
    fn commit(self, current: NamespaceUsage, previous: Option<NamespaceUsage>) -> Result<()> {
        let committed = match self {
            Self::Reserved(reservation) => {
                reservation.commit_actual_exact(current.live_bytes, previous)
            }
            Self::Pending {
                namespaces,
                maximum_live_bytes,
            } => {
                current.live_bytes <= maximum_live_bytes
                    && namespaces.record_replacement_exact(current, previous)
            }
        };
        if committed {
            Ok(())
        } else {
            Err(CacheError::CorruptMetadata(
                "managed lower usage exceeded or lost its reserved charge",
            ))
        }
    }
}

enum DiskLookup {
    Hit { entry: MemoryEntry, tier: CacheTier },
    Miss(HybridMissKind),
}

struct DiskPairError {
    error: CacheError,
    phase: DiskMutationPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiskMutationPhase {
    Uncommitted,
    CommittedOrUncertain,
}

impl DiskPairError {
    fn before_commit(error: CacheError) -> Self {
        Self {
            error,
            phase: DiskMutationPhase::Uncommitted,
        }
    }

    fn after_commit(error: CacheError) -> Self {
        Self {
            error,
            phase: DiskMutationPhase::CommittedOrUncertain,
        }
    }
}

type DiskMutationResult<T> = std::result::Result<T, DiskPairError>;

impl DiskPair {
    fn read_temporary_bytes(&self, namespace: NamespaceId, key: &[u8]) -> Result<usize> {
        let region_bytes = if self.region.status() == CacheStatus::Healthy {
            self.region
                .candidate_record_bytes_in(namespace, key)?
                .unwrap_or(0)
        } else {
            0
        };
        let bucket_bytes = if self.bucket.status() == CacheStatus::Healthy
            && self.bucket.may_contain_in(namespace, key)?
        {
            self.bucket_maximum_item_bytes
        } else {
            0
        };
        read_temporary_bytes(region_bytes, bucket_bytes).ok_or(CacheError::Overloaded(
            OverloadReason::ReadBufferUnavailable,
        ))
    }

    fn policy_charge(
        &self,
        namespace: NamespaceId,
        key_len: usize,
        value_len: usize,
        route: DiskRoute,
    ) -> Option<(u64, u64)> {
        let encoded_value_len = HYBRID_VALUE_HEADER_SIZE.checked_add(value_len)?;
        match route {
            DiskRoute::Bucket => {
                let entry_len = BUCKET_ENTRY_HEADER_BYTES
                    .checked_add(key_len)?
                    .checked_add(encoded_value_len)?;
                let live_bytes = align_up(entry_len, BUCKET_ENTRY_ALIGNMENT)? as u64;
                Some((live_bytes, self.bucket_size_bytes as u64))
            }
            DiskRoute::Region => {
                let bytes =
                    self.region
                        .maximum_record_bytes_in(namespace, key_len, encoded_value_len)?;
                Some((bytes, bytes))
            }
        }
    }

    fn may_contain_for_admission(&self, namespace: NamespaceId, key: &[u8]) -> Result<bool> {
        Ok(self.region.may_contain_in(namespace, key)?
            || self.bucket.may_contain_in(namespace, key)?)
    }

    /// Forget every lower candidate reachable by this key without device I/O.
    /// Region retirement is exact; Bucket invalidation deliberately hides the
    /// complete fixed bucket until a later put rebuilds it. The Hybrid owner
    /// keeps its dirty-session fence and publishes an empty checkpoint before
    /// any orderly restart can trust these volatile mutations.
    fn invalidate_in_memory(&self, namespace: NamespaceId, key: &[u8]) -> Result<()> {
        self.region.invalidate_in_memory(namespace, key)?;
        self.bucket.invalidate_bucket_in_memory(namespace, key)
    }

    fn route_put(
        &self,
        key_len: usize,
        value_len: usize,
    ) -> std::result::Result<DiskRoute, PutOutcome> {
        let Some(encoded_len) = HYBRID_VALUE_HEADER_SIZE.checked_add(value_len) else {
            return Err(PutOutcome::Rejected(RejectReason::ValueTooLarge));
        };
        if value_len > u32::MAX as usize {
            return Err(PutOutcome::Rejected(RejectReason::ValueTooLarge));
        }
        let user_size = key_len.saturating_add(value_len);
        if user_size <= self.small_object_max_bytes && self.bucket.fits(key_len, encoded_len) {
            return Ok(DiskRoute::Bucket);
        }
        if key_len > self.region_maximum_key_bytes {
            return Err(PutOutcome::Rejected(RejectReason::KeyTooLarge));
        }
        if encoded_len > self.region_maximum_value_bytes {
            return Err(PutOutcome::Rejected(RejectReason::ValueTooLarge));
        }
        Ok(DiskRoute::Region)
    }

    fn get(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        manifest: ManifestSnapshot,
        context: Option<&TaskContext>,
        policy: &PolicyController,
    ) -> Result<DiskLookup> {
        let mut degraded = false;
        let region = if self.region.status() == CacheStatus::Healthy {
            match self.region.may_contain_in(namespace, key) {
                Ok(true) => {
                    let disk_live_bytes = self
                        .region
                        .candidate_record_bytes_in(namespace, key)?
                        .unwrap_or(0) as u64;
                    match context.map_or_else(
                        || self.region.get_in(namespace, key),
                        |context| {
                            self.region
                                .get_in_with_task_context(namespace, key, context)
                        },
                    ) {
                        Ok(value) => value
                            .map(|encoded| decode_hybrid_value(namespace, key, encoded, manifest))
                            .transpose()?
                            .flatten()
                            .map(|mut entry| {
                                entry.pending_disk_bytes = disk_live_bytes;
                                (entry, CacheTier::RegionLogDisk)
                            }),
                        Err(error) if self.region.status() != CacheStatus::Healthy => {
                            let _ = error;
                            degraded = true;
                            None
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok(false) => None,
                Err(error) if self.region.status() != CacheStatus::Healthy => {
                    let _ = error;
                    degraded = true;
                    None
                }
                Err(error) => return Err(error),
            }
        } else {
            degraded = true;
            None
        };
        let bucket = if self.bucket.status() == CacheStatus::Healthy {
            match context.map_or_else(
                || self.bucket.get_in_managed(namespace, key),
                |context| {
                    self.bucket
                        .get_in_managed_with_task_context(namespace, key, context)
                },
            ) {
                Ok(receipt) => {
                    // Expiry compaction is already physically committed and
                    // uncancellable. Retire its exact charges before decoding
                    // the surviving candidate, whose corruption may still
                    // turn this lookup into an error.
                    self.record_bucket_retirements(policy, receipt.removed)?;
                    if self.bucket.status() != CacheStatus::Healthy {
                        degraded = true;
                    }
                    receipt
                        .value
                        .map(|encoded| decode_hybrid_value(namespace, key, encoded, manifest))
                        .transpose()?
                        .flatten()
                        .map(|mut entry| {
                            entry.pending_disk_bytes = self
                                .policy_charge(
                                    namespace,
                                    key.len(),
                                    entry.value.len(),
                                    DiskRoute::Bucket,
                                )
                                .map_or(0, |(live_bytes, _)| live_bytes);
                            (entry, CacheTier::SmallObjectDisk)
                        })
                }
                Err(error) if self.bucket.status() != CacheStatus::Healthy => {
                    let _ = error;
                    degraded = true;
                    None
                }
                Err(error) => return Err(error),
            }
        } else {
            degraded = true;
            None
        };
        let selected = match (region, bucket) {
            (Some(region), Some(bucket)) => match region.0.version.cmp(&bucket.0.version) {
                CmpOrdering::Equal => {
                    if region.0.value != bucket.0.value
                        || region.0.expires_at_unix_ms != bucket.0.expires_at_unix_ms
                    {
                        return Err(CacheError::CorruptMetadata(
                            "hybrid tiers disagree for the same global version",
                        ));
                    }
                    Some(region)
                }
                CmpOrdering::Greater => Some(region),
                CmpOrdering::Less => Some(bucket),
            },
            (Some(candidate), None) | (None, Some(candidate)) => Some(candidate),
            (None, None) => None,
        };
        Ok(match selected {
            Some((entry, tier)) => DiskLookup::Hit { entry, tier },
            None if degraded => DiskLookup::Miss(HybridMissKind::Recovering),
            None => DiskLookup::Miss(HybridMissKind::NotResident),
        })
    }

    fn put(&self, request: DiskPutRequest<'_>) -> DiskMutationResult<DiskPutReceipt> {
        let DiskPutRequest {
            namespace,
            key,
            value,
            options,
            route,
            cache_id,
            version,
            policy,
            usage_commit,
        } = request;
        let encoded = encode_hybrid_value(value, options.expires_at_unix_ms, cache_id, version)
            .map_err(DiskPairError::before_commit)?;
        if route == DiskRoute::Bucket {
            let source_present = self
                .region
                .may_contain_in(namespace, key)
                .map_err(DiskPairError::before_commit)?;
            let mut usage_commit = Some(usage_commit);
            let receipt = self
                .bucket
                .put_in_managed_with_commit(
                    namespace,
                    key,
                    &encoded,
                    options,
                    |new_usage, removed| {
                        let current = bucket_namespace_usage(new_usage);
                        usage_commit
                            .take()
                            .ok_or(CacheError::CorruptMetadata(
                                "managed Bucket commit callback ran more than once",
                            ))?
                            .commit(current, None)?;
                        record_bucket_retirements_slice(policy, removed)
                    },
                )
                .map_err(DiskPairError::before_commit)?;
            #[cfg(test)]
            crash_hit(HybridCrashPoint::TargetWritten);
            if receipt.outcome == PutOutcome::Stored && source_present {
                #[cfg(test)]
                crash_hit(HybridCrashPoint::BeforeSourceRemove);
                self.region
                    .invalidate_in_memory(namespace, key)
                    .map_err(DiskPairError::after_commit)?;
                #[cfg(test)]
                crash_hit(HybridCrashPoint::AfterSourceRemove);
            }
            let new_usage = if receipt.outcome == PutOutcome::Stored {
                Some(
                    bucket_namespace_usage_for_lengths(namespace, key.len(), encoded.len())
                        .map_err(DiskPairError::after_commit)?,
                )
            } else {
                None
            };
            Ok(DiskPutReceipt {
                outcome: receipt.outcome,
                new_usage,
            })
        } else {
            let commit: ManagedPutCommit =
                Box::new(move |current, previous| usage_commit.commit(current, previous));
            let receipt = self
                .region
                .put_in_managed_with_commit(namespace, key, &encoded, options, commit)
                .map_err(DiskPairError::before_commit)?;
            #[cfg(test)]
            crash_hit(HybridCrashPoint::TargetWritten);
            if receipt.outcome == PutOutcome::Stored {
                #[cfg(test)]
                crash_hit(HybridCrashPoint::BeforeSourceRemove);
                let receipt = self
                    .bucket
                    .remove_in_managed(namespace, key)
                    .map_err(DiskPairError::after_commit)?;
                self.record_bucket_retirements(policy, receipt.removed)
                    .map_err(DiskPairError::after_commit)?;
                #[cfg(test)]
                crash_hit(HybridCrashPoint::AfterSourceRemove);
            }
            Ok(DiskPutReceipt {
                outcome: receipt.outcome,
                new_usage: receipt.new_usage,
            })
        }
    }

    fn remove(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        policy: Option<&PolicyController>,
    ) -> DiskMutationResult<RemoveOutcome> {
        let region = if policy.is_some() {
            // The Hybrid dirty-session fence makes a Region tombstone
            // redundant. Verify the exact compact-index candidate, retire it
            // in memory, and let the next clean checkpoint publish the miss.
            match self.region.remove_in_memory_managed(namespace, key) {
                Ok(outcome) => outcome,
                Err(error @ CacheError::Overloaded(_)) => {
                    return Err(DiskPairError::before_commit(error));
                }
                Err(error) => return Err(DiskPairError::after_commit(error)),
            }
        } else {
            // Dirty-journal recovery runs before normal traffic and must leave
            // a physical fence that survives its own recovery process.
            self.region
                .remove_in_managed(namespace, key)
                .map_err(DiskPairError::before_commit)?
                .outcome
        };
        #[cfg(test)]
        crash_hit(HybridCrashPoint::AfterFirstRemove);
        let bucket = self
            .bucket
            .remove_in_managed(namespace, key)
            .map_err(DiskPairError::after_commit)?;
        if let Some(policy) = policy {
            self.record_bucket_retirements(policy, bucket.removed)
                .map_err(DiskPairError::after_commit)?;
        }
        #[cfg(test)]
        crash_hit(HybridCrashPoint::AfterAllRemoves);
        Ok(
            if region == RemoveOutcome::Removed || bucket.outcome == RemoveOutcome::Removed {
                RemoveOutcome::Removed
            } else {
                RemoveOutcome::NotFound
            },
        )
    }

    fn clear(&self) -> DiskMutationResult<()> {
        self.region.clear().map_err(DiskPairError::before_commit)?;
        self.bucket.clear().map_err(DiskPairError::after_commit)
    }

    fn record_bucket_retirements(
        &self,
        policy: &PolicyController,
        removed: Vec<BucketEntryUsage>,
    ) -> Result<()> {
        if let Err(error) = record_bucket_retirements(policy, removed) {
            // The page mutation is already durable. Never allow a lower tier
            // with unmatched owner accounting to publish a clean checkpoint.
            self.bucket.poison_managed_accounting();
            return Err(error);
        }
        Ok(())
    }

    fn flush(&self) -> DiskMutationResult<()> {
        self.region.flush().map_err(DiskPairError::before_commit)?;
        self.bucket.flush().map_err(DiskPairError::after_commit)
    }

    fn with_mutations_frozen_after_flush<T>(
        &self,
        after_flush: impl FnOnce() -> Result<T>,
    ) -> DiskMutationResult<T> {
        self.region
            .with_mutations_frozen_after_flush(|| {
                self.bucket.flush()?;
                after_flush()
            })
            .map_err(DiskPairError::before_commit)
    }

    fn close(&self) -> Result<()> {
        let region = self.region.close();
        let bucket = self.bucket.close();
        region.and(bucket)
    }

    fn close_without_checkpoint(&self) -> Result<()> {
        let region = self.region.close_without_checkpoint();
        let bucket = self.bucket.close_without_checkpoint();
        region.and(bucket)
    }
}

fn restore_policy_usage(
    disk: &DiskPair,
    policy: &PolicyController,
    scan_bucket: bool,
    scan_region: bool,
) -> Result<()> {
    policy.namespaces().reset_live_bytes();
    if scan_region {
        let mut region_error = None;
        disk.region.scan_live_usage(|usage| {
            if region_error.is_none()
                && policy.namespaces().contains(usage.namespace)
                && policy
                    .namespaces()
                    .restore_live_bytes(usage.namespace, usage.live_bytes)
                    .is_err()
            {
                region_error = Some(CacheError::CorruptMetadata(
                    "hybrid namespace usage overflows during Region recovery",
                ));
            }
        })?;
        if let Some(error) = region_error {
            return Err(error);
        }
    }
    if scan_bucket {
        disk.bucket.scan_live_entries(|usage| {
            let usage = bucket_namespace_usage(usage);
            if policy.namespaces().contains(usage.namespace) {
                policy
                    .namespaces()
                    .restore_live_bytes(usage.namespace, usage.live_bytes)
                    .map_err(|_| {
                        CacheError::CorruptMetadata(
                            "hybrid namespace usage overflows during Bucket recovery",
                        )
                    })?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

fn policy_usage_snapshot(policy: &PolicyController) -> Result<Vec<NamespaceUsage>> {
    let snapshots = policy
        .namespaces()
        .try_snapshots()
        .map_err(|_| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
    let mut usage = Vec::new();
    usage
        .try_reserve_exact(snapshots.len())
        .map_err(|_| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
    usage.extend(snapshots.into_iter().map(|snapshot| NamespaceUsage {
        namespace: snapshot.namespace,
        live_bytes: snapshot.live_bytes,
    }));
    Ok(usage)
}

fn same_namespace_set(expected: &[NamespaceUsage], checkpoint: &[NamespaceUsage]) -> bool {
    expected.len() == checkpoint.len()
        && expected
            .iter()
            .zip(checkpoint)
            .all(|(expected, checkpoint)| expected.namespace == checkpoint.namespace)
}

fn restore_policy_usage_checkpoint(
    policy: &PolicyController,
    checkpoint: &[NamespaceUsage],
) -> Result<()> {
    policy.namespaces().reset_live_bytes();
    for usage in checkpoint {
        policy
            .namespaces()
            .restore_live_bytes(usage.namespace, usage.live_bytes)
            .map_err(|_| {
                CacheError::CorruptMetadata("hybrid namespace usage checkpoint cannot be restored")
            })?;
    }
    Ok(())
}

fn reconcile_dirty_journal(disk: &DiskPair, mut journal: JournalScan) -> Result<()> {
    // Open fencing and autonomous lower mutations can leave a dirty marker
    // without a durable key intent. Their complete mutation set cannot be
    // reconstructed, so use the safe disposable-cache fallback instead of
    // guessing or pairing newer lower state with an older usage snapshot.
    if journal.requires_full_clear || journal.intent_count == 0 || journal.contains_clear {
        return disk.clear().map_err(|error| error.error);
    }

    // Validate redundant routing fields before using the journal as a recovery
    // authority. A mismatch means the bounded log is self-inconsistent; a full
    // tier clear is safer than applying a fence to the wrong key or bucket.
    if journal.intents.iter().any(|intent| {
        intent.key_hash != hybrid_hash(intent.namespace, intent.key)
            || intent.bucket_id.is_some_and(|bucket_id| {
                bucket_id != disk.bucket.bucket_id_for(intent.namespace, intent.key)
            })
    }) {
        return disk.clear().map_err(|error| error.error);
    }

    journal.intents.sort_and_dedup_keys();
    for intent in journal.intents.iter() {
        disk.remove(intent.namespace, intent.key, None)
            .map_err(|error| error.error)?;
    }
    Ok(())
}

fn encode_hybrid_value(
    value: &[u8],
    expires_at_unix_ms: Option<u64>,
    cache_id: [u8; 16],
    version: HybridVersion,
) -> Result<Vec<u8>> {
    let length = HYBRID_VALUE_HEADER_SIZE
        .checked_add(value.len())
        .ok_or_else(|| CacheError::InvalidConfig("hybrid value length overflow".into()))?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(length)
        .map_err(|_| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
    encoded.resize(length, 0);
    encoded[..8].copy_from_slice(&HYBRID_VALUE_MAGIC);
    encoded[8..10].copy_from_slice(&HYBRID_VALUE_VERSION.to_le_bytes());
    encoded[10..12].copy_from_slice(&(HYBRID_VALUE_HEADER_SIZE as u16).to_le_bytes());
    encoded[12..16].copy_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| CacheError::InvalidConfig("hybrid value is too large".into()))?
            .to_le_bytes(),
    );
    encoded[16..24].copy_from_slice(&expires_at_unix_ms.unwrap_or(0).to_le_bytes());
    encoded[24..32].copy_from_slice(&version.epoch.to_le_bytes());
    encoded[32..40].copy_from_slice(&version.seqno.to_le_bytes());
    encoded[40..56].copy_from_slice(&cache_id);
    encoded[HYBRID_VALUE_HEADER_SIZE..].copy_from_slice(value);
    Ok(encoded)
}

fn decode_hybrid_value(
    namespace: NamespaceId,
    key: &[u8],
    encoded: Vec<u8>,
    manifest: ManifestSnapshot,
) -> Result<Option<MemoryEntry>> {
    if encoded.len() < HYBRID_VALUE_HEADER_SIZE
        || encoded.get(..8) != Some(HYBRID_VALUE_MAGIC.as_slice())
        || get_u16(&encoded, 8) != Some(HYBRID_VALUE_VERSION)
        || get_u16(&encoded, 10) != Some(HYBRID_VALUE_HEADER_SIZE as u16)
    {
        return Ok(None);
    }
    let value_len =
        get_u32(&encoded, 12)
            .map(|length| length as usize)
            .ok_or(CacheError::CorruptMetadata(
                "hybrid value length is truncated",
            ))?;
    if HYBRID_VALUE_HEADER_SIZE.checked_add(value_len) != Some(encoded.len()) {
        return Err(CacheError::CorruptMetadata(
            "hybrid value length does not match its record",
        ));
    }
    let expires_at = get_u64(&encoded, 16).ok_or(CacheError::CorruptMetadata(
        "hybrid expiration is truncated",
    ))?;
    let version = HybridVersion {
        epoch: get_u64(&encoded, 24).ok_or(CacheError::CorruptMetadata(
            "hybrid version epoch is truncated",
        ))?,
        seqno: get_u64(&encoded, 32).ok_or(CacheError::CorruptMetadata(
            "hybrid version sequence is truncated",
        ))?,
    };
    let mut cache_id = [0_u8; 16];
    cache_id.copy_from_slice(encoded.get(40..56).ok_or(CacheError::CorruptMetadata(
        "hybrid cache identity is truncated",
    ))?);
    if cache_id != manifest.cache_id
        || (manifest.clear_floor != HybridVersion::ZERO && version <= manifest.clear_floor)
        || version.epoch == 0
        || version.seqno == 0
        || version.epoch > manifest.version_epoch
        || (version.epoch == manifest.version_epoch && version.seqno >= manifest.next_seqno)
    {
        return Ok(None);
    }
    if expires_at != 0 && expires_at <= now_unix_ms() {
        return Ok(None);
    }
    let key = try_clone_bytes(key, OverloadReason::ReadBufferUnavailable)?;
    let mut value = encoded;
    value.drain(..HYBRID_VALUE_HEADER_SIZE);
    Ok(Some(MemoryEntry::new_versioned(
        namespace,
        key,
        value,
        (expires_at != 0).then_some(expires_at),
        version,
        true,
    )))
}

fn try_clone_bytes(bytes: &[u8], reason: OverloadReason) -> Result<Vec<u8>> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(bytes.len())
        .map_err(|_| CacheError::Overloaded(reason))?;
    cloned.extend_from_slice(bytes);
    Ok(cloned)
}

fn try_clone_memory_entry(entry: &MemoryEntry) -> Result<MemoryEntry> {
    Ok(MemoryEntry {
        namespace: entry.namespace,
        key: try_clone_bytes(&entry.key, OverloadReason::WriteBufferUnavailable)?,
        value: Arc::clone(&entry.value),
        expires_at_unix_ms: entry.expires_at_unix_ms,
        version: entry.version,
        disk_clean: entry.disk_clean,
        pending_disk_bytes: entry.pending_disk_bytes,
    })
}

fn write_back_run_failure(error: WriteBackRunError) -> DirtyPersistFailure {
    match error {
        WriteBackRunError::Overloaded(reason) => {
            DirtyPersistFailure::Rejected(hybrid_put_reject_reason(reason))
        }
        WriteBackRunError::Closed => DirtyPersistFailure::Fatal(CacheError::Closed),
        WriteBackRunError::WorkerPanicked => DirtyPersistFailure::Fatal(CacheError::Poisoned),
    }
}

fn dirty_persist_failure(error: DiskPairError) -> DirtyPersistFailure {
    match (error.phase, error.error) {
        (DiskMutationPhase::Uncommitted, CacheError::Overloaded(reason)) => {
            DirtyPersistFailure::Rejected(hybrid_put_reject_reason(reason))
        }
        (DiskMutationPhase::Uncommitted, CacheError::ReclaimBacklog) => {
            DirtyPersistFailure::Rejected(RejectReason::ReclaimBacklog)
        }
        (_, error) => DirtyPersistFailure::Fatal(error),
    }
}

fn demotion_reject_error(reason: RejectReason) -> CacheError {
    match reason {
        RejectReason::SubmissionFull => CacheError::Overloaded(OverloadReason::WriteQueueFull),
        RejectReason::SubmissionTimeout => CacheError::Overloaded(OverloadReason::WriteTimeout),
        RejectReason::BufferUnavailable => {
            CacheError::Overloaded(OverloadReason::WriteBufferUnavailable)
        }
        _ => CacheError::ReclaimBacklog,
    }
}

fn hybrid_put_reject_reason(reason: OverloadReason) -> RejectReason {
    match reason {
        OverloadReason::ReadQueueFull
        | OverloadReason::WriteQueueFull
        | OverloadReason::JournalCapacityFull
        | OverloadReason::CloseWaitersFull => RejectReason::SubmissionFull,
        OverloadReason::ReadBufferUnavailable | OverloadReason::WriteBufferUnavailable => {
            RejectReason::BufferUnavailable
        }
        OverloadReason::ReadTimeout | OverloadReason::WriteTimeout => {
            RejectReason::SubmissionTimeout
        }
    }
}

fn hybrid_request_result_for_error(error: &CacheError) -> RequestResultClass {
    match error {
        CacheError::Io(_) => RequestResultClass::IoError,
        CacheError::CorruptMetadata(_) => RequestResultClass::Corrupt,
        CacheError::Overloaded(_) | CacheError::ReclaimBacklog => RequestResultClass::Overloaded,
        CacheError::Cancelled | CacheError::TimedOut => RequestResultClass::Cancelled,
        CacheError::InvalidConfig(_)
        | CacheError::Locked
        | CacheError::Closed
        | CacheError::Poisoned => RequestResultClass::Unavailable,
    }
}

fn hybrid_cache_error_class(error: &CacheError) -> CacheErrorClass {
    match error {
        CacheError::Io(error) if error.raw_os_error() == Some(28) => CacheErrorClass::NoSpace,
        CacheError::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            CacheErrorClass::Permission
        }
        CacheError::Io(_) => CacheErrorClass::DeviceIo,
        CacheError::InvalidConfig(_) => CacheErrorClass::InvalidConfig,
        CacheError::CorruptMetadata(_) => CacheErrorClass::CorruptMetadata,
        CacheError::Locked => CacheErrorClass::Locked,
        CacheError::Closed => CacheErrorClass::Closed,
        CacheError::Poisoned => CacheErrorClass::Poisoned,
        CacheError::Cancelled => CacheErrorClass::Cancelled,
        CacheError::TimedOut => CacheErrorClass::TimedOut,
        CacheError::Overloaded(_) => CacheErrorClass::Overloaded,
        CacheError::ReclaimBacklog => CacheErrorClass::ReclaimBacklog,
    }
}

fn align_up(value: usize, alignment: usize) -> Option<usize> {
    debug_assert!(alignment.is_power_of_two());
    value
        .checked_add(alignment.checked_sub(1)?)
        .map(|rounded| rounded & !(alignment - 1))
}

/// A compatible file may contain a direct-I/O tail-padded record even when
/// this reopen uses buffered I/O, so static diagnostics include the complete
/// Format V1 padding range.
fn maximum_persisted_region_record_bytes(minimum: usize) -> Option<usize> {
    minimum
        .checked_add(DIRECT_IO_ALIGNMENT.checked_sub(1)?)
        .map(|bytes| bytes.min(MAX_RECORD_LEN as usize))
}

/// Bound the two decoded lower-tier candidates that can coexist while a key
/// transitions between size classes, plus the caller-owned clone of whichever
/// candidate wins the global-version comparison.
fn read_temporary_bytes(region_candidate: usize, bucket_candidate: usize) -> Option<usize> {
    region_candidate
        .checked_add(bucket_candidate)?
        .checked_add(region_candidate.max(bucket_candidate))
}

fn bucket_namespace_usage_for_lengths(
    namespace: NamespaceId,
    key_len: usize,
    encoded_value_len: usize,
) -> Result<NamespaceUsage> {
    let bytes = BUCKET_ENTRY_HEADER_BYTES
        .checked_add(key_len)
        .and_then(|bytes| bytes.checked_add(encoded_value_len))
        .and_then(|bytes| align_up(bytes, BUCKET_ENTRY_ALIGNMENT))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(CacheError::CorruptMetadata(
            "managed bucket usage exceeds addressable policy accounting",
        ))?;
    Ok(NamespaceUsage {
        namespace,
        live_bytes: bytes,
    })
}

fn bucket_namespace_usage(usage: BucketEntryUsage) -> NamespaceUsage {
    NamespaceUsage {
        namespace: usage.namespace,
        live_bytes: usage.live_bytes,
    }
}

fn record_bucket_retirements(
    policy: &PolicyController,
    removed: Vec<BucketEntryUsage>,
) -> Result<()> {
    record_bucket_retirements_slice(policy, &removed)
}

fn record_bucket_retirements_slice(
    policy: &PolicyController,
    removed: &[BucketEntryUsage],
) -> Result<()> {
    for &usage in removed {
        let usage = bucket_namespace_usage(usage);
        // Namespace configuration may legitimately drop ids across reopen.
        // Recovery ignores their physical entries, so later compaction must
        // ignore the matching receipt rather than treating it as corruption.
        if policy.namespaces().contains(usage.namespace)
            && !policy.namespaces().record_removal_exact(usage)
        {
            return Err(CacheError::CorruptMetadata(
                "managed Bucket retirement exceeded namespace live usage",
            ));
        }
    }
    Ok(())
}

fn namespace_reject_reason(reason: NamespaceRejectReason) -> RejectReason {
    match reason {
        NamespaceRejectReason::UnknownNamespace => RejectReason::NamespaceNotConfigured,
        NamespaceRejectReason::CapacityExceeded => RejectReason::NamespaceCapacityExceeded,
        NamespaceRejectReason::WriteBudgetExceeded => RejectReason::NamespaceWriteBudgetExceeded,
    }
}

fn map_memory_open_error(error: MemoryError) -> CacheError {
    CacheError::InvalidConfig(error.to_string())
}

fn map_memory_error(error: MemoryError) -> CacheError {
    match error {
        MemoryError::InvalidConfig(message) => CacheError::InvalidConfig(message.into()),
        MemoryError::AllocationFailed => {
            CacheError::Overloaded(OverloadReason::ReadBufferUnavailable)
        }
    }
}

fn paths_refer_to_same_file(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn path_has_content(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|metadata| metadata.len() != 0)
}

fn default_manifest_path(bucket_path: &Path) -> PathBuf {
    let mut path = bucket_path.as_os_str().to_os_string();
    path.push(".hybrid-manifest");
    PathBuf::from(path)
}

fn hybrid_layout_fingerprint(diagnostics: &HybridConfigDiagnostics) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for value in [
        diagnostics.small_object_max_bytes as u64,
        diagnostics.journal_capacity_bytes,
        diagnostics.bucket.capacity_bytes,
        diagnostics.bucket.file_len_bytes,
        diagnostics.bucket.bucket_size_bytes as u64,
        diagnostics.bucket.bucket_count,
        diagnostics.region.data_file_len_bytes,
        diagnostics.region.region_size_bytes,
        u64::from(diagnostics.region.region_count),
        diagnostics.region.append_lanes as u64,
    ] {
        for byte in value.to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
    }
    hash
}

fn hybrid_hash(namespace: NamespaceId, key: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in namespace.to_le_bytes().iter().chain(key) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

fn journal_group_commit_stats(
    snapshot: JournalGroupCommitSnapshot,
) -> HybridJournalGroupCommitStats {
    HybridJournalGroupCommitStats {
        queue_capacity: snapshot.queue_capacity,
        memory_capacity_bytes: snapshot.memory_capacity_bytes,
        fixed_memory_bytes: snapshot.fixed_memory_bytes,
        in_flight: snapshot.in_flight,
        in_flight_peak: snapshot.in_flight_peak,
        memory_in_use_bytes: snapshot.bytes_in_use,
        memory_peak_bytes: snapshot.bytes_peak,
        committed_batches: snapshot.committed_batches,
        committed_records: snapshot.committed_records,
        durability_syncs: snapshot.durability_syncs,
        sync_elapsed_ns_total: snapshot.sync_elapsed_ns_total,
        sync_elapsed_ns_max: snapshot.sync_elapsed_ns_max,
        rejected: snapshot.rejected,
        worker_panics: snapshot.worker_panics,
        accepting: snapshot.accepting,
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn hybrid_context_stop_error(context: Option<&TaskContext>) -> CacheError {
    match context.and_then(TaskContext::stop_reason) {
        Some(AsyncFailure::TimedOut) => CacheError::TimedOut,
        _ => CacheError::Cancelled,
    }
}

fn get_u16(input: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        input.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn get_u32(input: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        input.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn get_u64(input: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        input.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_backend::{
        FileBackend, IoBackend, SyncMode, SyncPoint, WritePoint, write_all_at,
    };
    use crate::policy::HostWriteTracker;
    use std::fs;
    use std::io;
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    use std::path::PathBuf;
    #[cfg(unix)]
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Barrier, Condvar, Mutex, mpsc};
    use std::thread;
    use std::time::{Duration, Instant};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    #[derive(Default)]
    struct BlockingReadState {
        armed: bool,
        entered: usize,
        released: bool,
    }

    struct BlockingReadHandle {
        state: Arc<(Mutex<BlockingReadState>, Condvar)>,
    }

    impl BlockingReadHandle {
        fn arm(&self) {
            let (state, _) = self.state.as_ref();
            lock_mutex(state).armed = true;
        }

        fn wait_for_entered(&self, expected: usize) -> bool {
            let (state, changed) = self.state.as_ref();
            let state = lock_mutex(state);
            let (state, _) = changed
                .wait_timeout_while(state, Duration::from_secs(1), |state| {
                    state.entered < expected
                })
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.entered >= expected
        }

        fn release(&self) {
            let (state, changed) = self.state.as_ref();
            let mut state = lock_mutex(state);
            state.released = true;
            changed.notify_all();
        }
    }

    impl Drop for BlockingReadHandle {
        fn drop(&mut self) {
            self.release();
        }
    }

    struct BlockingFileBackend {
        file: FileBackend,
        state: Arc<(Mutex<BlockingReadState>, Condvar)>,
    }

    impl BlockingFileBackend {
        fn open(path: &Path) -> io::Result<(Self, BlockingReadHandle)> {
            let state = Arc::new((Mutex::new(BlockingReadState::default()), Condvar::new()));
            Ok((
                Self {
                    file: FileBackend::open(path)?,
                    state: Arc::clone(&state),
                },
                BlockingReadHandle { state },
            ))
        }
    }

    impl IoBackend for BlockingFileBackend {
        fn len(&self) -> io::Result<u64> {
            self.file.len()
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            self.file.set_len(len)
        }

        fn preallocate(&self, len: u64) -> io::Result<()> {
            self.file.preallocate(len)
        }

        fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
            let (state, changed) = self.state.as_ref();
            let mut state = lock_mutex(state);
            if state.armed {
                state.entered += 1;
                changed.notify_all();
                while !state.released {
                    state = changed
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
            drop(state);
            self.file.read_at(buffer, offset)
        }

        fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
            self.file.write_at(point, buffer, offset)
        }

        fn sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()> {
            self.file.sync(point, mode)
        }

        fn try_lock_exclusive(&self) -> io::Result<()> {
            self.file.try_lock_exclusive()
        }

        fn unlock(&self) -> io::Result<()> {
            self.file.unlock()
        }
    }

    fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if condition() {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        condition()
    }

    struct TestFiles {
        bucket: PathBuf,
        region: PathBuf,
        manifest: PathBuf,
    }

    impl TestFiles {
        fn new(name: &str) -> Self {
            let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            let base = std::env::temp_dir().join(format!(
                "cache-rs-hybrid-{name}-{}-{id}",
                std::process::id()
            ));
            Self {
                bucket: base.with_extension("bucket"),
                region: base.with_extension("region"),
                manifest: base.with_extension("hybrid-manifest"),
            }
        }
    }

    impl Drop for TestFiles {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.bucket);
            let _ = fs::remove_file(&self.region);
            let _ = fs::remove_file(&self.manifest);
        }
    }

    fn config(files: &TestFiles, memory_bytes: usize) -> HybridCacheConfig {
        config_for_paths(&files.bucket, &files.region, &files.manifest, memory_bytes)
    }

    fn config_for_paths(
        bucket_path: &Path,
        region_path: &Path,
        manifest_path: &Path,
        memory_bytes: usize,
    ) -> HybridCacheConfig {
        let bucket =
            BucketCacheConfig::new(bucket_path, 16 * 4096).with_memory_budget(4 * 1024 * 1024);
        let region = CacheConfig::new(region_path, 8 * 64 * 1024)
            .with_region_size(64 * 1024)
            .with_index_slots(1024)
            .with_max_key_size(1024)
            .with_max_value_size(32 * 1024)
            .with_memory_budget(16 * 1024 * 1024);
        HybridCacheConfig::new(memory_bytes, bucket, region)
            .with_memory_shards(4)
            .with_small_object_max(512)
            .with_manifest_path(manifest_path)
            .with_write_mode(HybridWriteMode::WriteThrough)
    }

    #[test]
    fn mixed_sizes_route_to_distinct_disk_engines_and_promote() {
        let files = TestFiles::new("mixed");
        let cache = config(&files, 4 * 1024).open().unwrap();
        let large = vec![9_u8; 2048];
        cache
            .put(b"small", b"value", PutOptions::default())
            .unwrap();
        cache.put(b"large", &large, PutOptions::default()).unwrap();
        cache.flush().unwrap();
        cache.close().unwrap();

        let reopened = config(&files, 4 * 1024).open().unwrap();
        assert_eq!(
            reopened.lookup(b"small").unwrap(),
            HybridLookupOutcome::Hit {
                value: b"value".to_vec(),
                tier: CacheTier::SmallObjectDisk,
            }
        );
        assert_eq!(
            reopened.lookup(b"large").unwrap(),
            HybridLookupOutcome::Hit {
                value: large,
                tier: CacheTier::RegionLogDisk,
            }
        );
        assert!(reopened.stats().promotions >= 1);
        assert!(matches!(
            reopened.lookup(b"small").unwrap(),
            HybridLookupOutcome::Hit {
                tier: CacheTier::Memory,
                ..
            }
        ));
        reopened.close().unwrap();
    }

    #[test]
    fn memory_hit_does_not_wait_for_key_disk_ordering_stripe() {
        let files = TestFiles::new("memory-hit-ordering");
        let cache = config(&files, 8 * 1024).open().unwrap();
        cache.put(b"key", b"value", PutOptions::default()).unwrap();
        let hash = hybrid_hash(0, b"key");

        thread::scope(|scope| {
            let ordering = cache.lock_key(hash);
            let (done_tx, done_rx) = mpsc::channel();
            let cache_ref = &cache;
            scope.spawn(move || {
                done_tx.send(cache_ref.get(b"key")).unwrap();
            });
            let completed_while_disk_ordering_was_held =
                done_rx.recv_timeout(Duration::from_millis(250));
            drop(ordering);
            assert_eq!(
                completed_while_disk_ordering_was_held.unwrap().unwrap(),
                Some(b"value".to_vec())
            );
        });
        cache.close().unwrap();
    }

    #[test]
    fn memory_handles_share_payload_and_survive_replacement() {
        let files = TestFiles::new("memory-handle");
        let cache = config(&files, 8 * 1024).open().unwrap();
        cache.put(b"key", b"value", PutOptions::default()).unwrap();

        let first = cache.get_handle(b"key").unwrap().unwrap();
        let second = cache.get_handle(b"key").unwrap().unwrap();
        assert!(first.shares_storage_with(&second));
        assert_eq!(first.as_slice(), b"value");

        cache
            .put(b"key", b"replacement", PutOptions::default())
            .unwrap();
        assert_eq!(first.as_slice(), b"value");
        assert_eq!(cache.get(b"key").unwrap(), Some(b"replacement".to_vec()));
        cache.close().unwrap();
    }

    #[test]
    fn dirty_persist_rejects_only_definitely_uncommitted_pressure() {
        assert!(matches!(
            dirty_persist_failure(DiskPairError::before_commit(CacheError::Overloaded(
                OverloadReason::WriteBufferUnavailable,
            ))),
            DirtyPersistFailure::Rejected(RejectReason::BufferUnavailable)
        ));
        assert!(matches!(
            dirty_persist_failure(DiskPairError::before_commit(CacheError::ReclaimBacklog)),
            DirtyPersistFailure::Rejected(RejectReason::ReclaimBacklog)
        ));
        assert!(matches!(
            dirty_persist_failure(DiskPairError::after_commit(CacheError::Overloaded(
                OverloadReason::WriteBufferUnavailable,
            ))),
            DirtyPersistFailure::Fatal(CacheError::Overloaded(
                OverloadReason::WriteBufferUnavailable
            ))
        ));
    }

    #[test]
    fn crossing_size_threshold_and_remove_do_not_expose_old_copy() {
        let files = TestFiles::new("migration");
        let cache = config(&files, 8 * 1024).open().unwrap();
        cache.put(b"key", b"small", PutOptions::default()).unwrap();
        let large = vec![3_u8; 2048];
        cache.put(b"key", &large, PutOptions::default()).unwrap();
        assert_eq!(cache.get(b"key").unwrap(), Some(large));
        let region_batches = cache.stats().region.write_batches;
        cache
            .put(b"key", b"small-again", PutOptions::default())
            .unwrap();
        assert_eq!(
            cache.stats().region.write_batches,
            region_batches,
            "moving into Bucket must invalidate the old Region index without a tombstone"
        );
        assert_eq!(cache.get(b"key").unwrap(), Some(b"small-again".to_vec()));
        assert_eq!(cache.remove(b"key").unwrap(), RemoveOutcome::Removed);
        assert_eq!(cache.get(b"key").unwrap(), None);
        cache.flush().unwrap();
        cache.close().unwrap();

        let reopened = config(&files, 8 * 1024).open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn managed_region_remove_avoids_a_tombstone_and_survives_checkpoint() {
        let files = TestFiles::new("volatile-region-remove");
        let cache_config = config(&files, 8 * 1024);
        let cache = cache_config.clone().open().unwrap();
        let value = vec![7_u8; 2048];
        assert_eq!(
            cache.put(b"large", &value, PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        let before = cache.stats().region;

        assert_eq!(cache.remove(b"large").unwrap(), RemoveOutcome::Removed);
        let after = cache.stats().region;
        assert_eq!(after.write_batches, before.write_batches);
        assert_eq!(after.bytes_written, before.bytes_written);
        assert_eq!(after.removes, before.removes + 1);
        assert_eq!(cache.get(b"large").unwrap(), None);

        cache.flush().unwrap();
        cache.close().unwrap();
        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.get(b"large").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn cross_tier_migration_retires_old_usage_and_reopen_restores_exact_usage() {
        let files = TestFiles::new("migration-accounting");
        let key = b"key";
        let small = b"small";
        let large = vec![3_u8; 2048];
        let small_usage = bucket_namespace_usage_for_lengths(
            7,
            key.len(),
            HYBRID_VALUE_HEADER_SIZE + small.len(),
        )
        .unwrap()
        .live_bytes;
        let large_usage = u64::from(
            RecordHeader::aligned_len(key.len() + 4, HYBRID_VALUE_HEADER_SIZE + large.len())
                .unwrap(),
        );
        let capacity = small_usage + large_usage;
        let cache_config = config(&files, 8 * 1024)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(capacity));

        let cache = cache_config.clone().open().unwrap();
        assert_eq!(
            cache.put_in(7, key, small, PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            small_usage
        );

        assert_eq!(
            cache.put_in(7, key, &large, PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            large_usage
        );

        assert_eq!(
            cache.put_in(7, key, small, PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        let snapshot = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(snapshot.live_bytes, small_usage);
        assert_eq!(snapshot.reserved_bytes, 0);
        cache.close().unwrap();

        let reopened = cache_config.clone().open().unwrap();
        assert!(reopened.stats().open.policy_restored_from_checkpoint);
        assert!(!reopened.stats().open.bucket_usage_scanned);
        assert!(!reopened.stats().open.region_usage_scanned);
        let snapshot = reopened
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(snapshot.live_bytes, small_usage);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(reopened.get_in(7, key).unwrap(), Some(small.to_vec()));
        reopened.close().unwrap();
    }

    #[test]
    fn direct_padded_region_receipts_preserve_other_namespace_usage_across_reopen() {
        let files = TestFiles::new("direct-region-usage");
        let cache_config = config(&files, 8 * 1024)
            .with_small_object_max(1)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(64 * 1024));
        let first_key = b"region-a";
        let second_key = b"region-b";

        let cache = cache_config.clone().open().unwrap();
        // Portable test-only planner injection: data still uses the buffered
        // backend, but Format V1 records receive the exact tail padding used
        // by the production direct-I/O append path.
        cache
            .inner
            .disk
            .region
            .force_direct_append_padding_for_test();
        assert_eq!(
            cache
                .put_in(7, first_key, b"first", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache
                .put_in(7, second_key, b"second", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );

        let mut physical = Vec::new();
        cache
            .inner
            .disk
            .region
            .scan_live_usage(|usage| physical.push(usage))
            .unwrap();
        assert_eq!(physical.len(), 2);
        assert!(physical.iter().all(|usage| usage.live_bytes == 4096));
        assert_eq!(
            cache
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            8192
        );

        assert_eq!(
            cache
                .put_in(7, first_key, b"replacement", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            8192,
            "replacing one padded Region record must retain the other charge"
        );
        assert_eq!(
            cache.remove_in(7, first_key).unwrap(),
            RemoveOutcome::Removed
        );
        let snapshot = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(snapshot.live_bytes, 4096);
        assert_eq!(snapshot.reserved_bytes, 0);

        cache.flush().unwrap();
        cache.close().unwrap();
        let reopened = cache_config.open().unwrap();
        assert!(reopened.stats().open.policy_restored_from_checkpoint);
        assert!(!reopened.stats().open.region_usage_scanned);
        let snapshot = reopened
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(snapshot.live_bytes, 4096);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(reopened.get_in(7, first_key).unwrap(), None);
        assert_eq!(
            reopened.get_in(7, second_key).unwrap(),
            Some(b"second".to_vec())
        );
        reopened.close().unwrap();
    }

    #[test]
    fn padded_reinsertion_reserves_hard_cap_and_dirty_lower_rebuilds_usage() {
        let files = TestFiles::new("direct-reinsert-usage");
        let mut cache_config = config(&files, 8 * 1024)
            .with_small_object_max(1)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(6 * 1024));
        cache_config.region = cache_config
            .region
            .clone()
            .with_reclaim_mode(crate::cache::ReclaimMode::SecondChance);
        let first_key = b"region-a";
        let second_key = b"region-b";
        let cache = cache_config.clone().open().unwrap();
        assert_eq!(
            cache
                .put_in(7, first_key, b"first", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache
                .put_in(7, second_key, b"second", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        cache.flush().unwrap();
        let first_before = cache
            .inner
            .disk
            .region
            .candidate_record_bytes_in(7, first_key)
            .unwrap()
            .unwrap() as u64;
        let second_before = cache
            .inner
            .disk
            .region
            .candidate_record_bytes_in(7, second_key)
            .unwrap()
            .unwrap() as u64;

        cache
            .inner
            .disk
            .region
            .force_direct_append_padding_for_test();
        assert!(
            cache
                .inner
                .disk
                .region
                .reinsert_current_for_test(7, first_key)
                .unwrap()
        );
        let first_after = cache
            .inner
            .disk
            .region
            .candidate_record_bytes_in(7, first_key)
            .unwrap()
            .unwrap() as u64;
        assert!(first_after > first_before);
        assert!(first_after <= 4096);
        let expected = first_after + second_before;
        let snapshot = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(snapshot.live_bytes, expected);
        assert_eq!(snapshot.reserved_bytes, 0);

        // Reserving the complete prospective physical record before writing
        // keeps the hard cap strict. The other live object is neither retired
        // nor silently credited when this reinsertion is rejected.
        assert!(
            !cache
                .inner
                .disk
                .region
                .reinsert_current_for_test(7, second_key)
                .unwrap()
        );
        assert_eq!(
            cache
                .inner
                .disk
                .region
                .candidate_record_bytes_in(7, second_key)
                .unwrap(),
            Some(second_before as usize)
        );
        assert_eq!(
            cache
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            expected
        );

        // Simulate a crash after an autonomous Region mutation but before a
        // new Hybrid checkpoint. Its owner-dirty fence has no key journal, so
        // recovery takes the documented disposable-cache fallback rather than
        // trusting stale usage or guessing which autonomous changes landed.
        drop(cache);
        let reopened = cache_config.clone().open().unwrap();
        assert!(!reopened.stats().open.policy_restored_from_checkpoint);
        assert!(reopened.stats().open.region_usage_scanned);
        assert_eq!(
            reopened
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            0
        );
        assert_eq!(reopened.get_in(7, first_key).unwrap(), None);
        assert_eq!(reopened.get_in(7, second_key).unwrap(), None);
        reopened.close().unwrap();

        let clean = cache_config.open().unwrap();
        assert!(clean.stats().open.policy_restored_from_checkpoint);
        assert_eq!(
            clean
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            0
        );
        clean.close().unwrap();
    }

    #[test]
    fn lower_clean_before_manifest_publish_reopens_as_safe_miss() {
        let files = TestFiles::new("lower-clean-manifest-window");
        let mut cache_config = config(&files, 8 * 1024)
            .with_small_object_max(1)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(64 * 1024));
        cache_config.region = cache_config
            .region
            .clone()
            .with_reclaim_mode(crate::cache::ReclaimMode::SecondChance);
        let cache = cache_config.clone().open().unwrap();
        cache
            .put_in(7, b"key", b"value", PutOptions::default())
            .unwrap();
        cache.flush().unwrap();
        let checkpointed = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap()
            .live_bytes;

        cache
            .inner
            .disk
            .region
            .force_direct_append_padding_for_test();
        assert!(
            cache
                .inner
                .disk
                .region
                .reinsert_current_for_test(7, b"key")
                .unwrap()
        );
        let current = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap()
            .live_bytes;
        assert!(current > checkpointed);

        // Emulate a process death after both lower tiers became clean but
        // before the global usage publication. The durable Hybrid dirty fence
        // makes an empty journal a safe-clear boundary, so the old lower usage
        // can never be restored as a smaller clean quota checkpoint.
        cache
            .inner
            .manifest
            .mark_dirty_for_lower_checkpoint()
            .unwrap();
        cache
            .inner
            .disk
            .region
            .with_mutations_frozen_after_flush(|| cache.inner.disk.bucket.flush())
            .unwrap();
        drop(cache);

        let reopened = cache_config.open().unwrap();
        assert!(!reopened.stats().open.policy_restored_from_checkpoint);
        let namespace = reopened
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(namespace.live_bytes, 0);
        assert_eq!(namespace.reserved_bytes, 0);
        assert_eq!(reopened.get_in(7, b"key").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn failed_global_dirty_fence_closes_lower_tiers_without_clean_checkpoint() {
        let files = TestFiles::new("failed-global-dirty-close");
        let mut cache_config = config(&files, 8 * 1024)
            .with_small_object_max(1)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(64 * 1024));
        cache_config.region = cache_config
            .region
            .clone()
            .with_reclaim_mode(crate::cache::ReclaimMode::SecondChance);
        let cache = cache_config.clone().open().unwrap();
        cache
            .put_in(7, b"key", b"value", PutOptions::default())
            .unwrap();
        cache.flush().unwrap();
        let checkpointed = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap()
            .live_bytes;

        cache
            .inner
            .disk
            .region
            .force_direct_append_padding_for_test();
        assert!(
            cache
                .inner
                .disk
                .region
                .reinsert_current_for_test(7, b"key")
                .unwrap()
        );
        let current = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap()
            .live_bytes;
        assert!(current > checkpointed);

        // The injected failure occurs before either dirty slot is written.
        // close must still drain and unlock both lower tiers, but must leave
        // their durable state dirty so the old clean usage extension cannot
        // be trusted by the next opener.
        cache
            .inner
            .manifest
            .fail_lower_checkpoint_dirty_once_for_test();
        assert!(cache.close().is_err());

        // Reopen while the closed handle is still alive to prove that every
        // file lock was released despite the failed close result.
        let reopened = cache_config.open().unwrap();
        assert!(!reopened.stats().open.policy_restored_from_checkpoint);
        assert!(reopened.stats().open.region_usage_scanned);
        let namespace = reopened
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(namespace.live_bytes, 0);
        assert_eq!(namespace.reserved_bytes, 0);
        assert_eq!(reopened.get_in(7, b"key").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn clean_reopen_restores_usage_from_manifest_without_lower_scan() {
        let files = TestFiles::new("usage-checkpoint-reopen");
        let cache_config = config(&files, 8 * 1024)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(16 * 1024));
        let layout = hybrid_layout_fingerprint(&cache_config.diagnostics().unwrap());
        let cache = cache_config.clone().open().unwrap();
        cache
            .put_in(7, b"key", b"value", PutOptions::default())
            .unwrap();
        cache.close().unwrap();

        // A deliberately conservative clean counter is enough to distinguish
        // metadata restoration from a lower-tier scan: scanning the one real
        // entry would calculate its smaller encoded size instead.
        let (manifest, opened) = HybridManifest::open_with_journal_capacity(
            &files.manifest,
            layout,
            DEFAULT_JOURNAL_CAPACITY,
        )
        .unwrap();
        assert!(!opened.needs_recovery);
        manifest
            .publish_clean_with_usage(&[
                NamespaceUsage {
                    namespace: 0,
                    live_bytes: 0,
                },
                NamespaceUsage {
                    namespace: 7,
                    live_bytes: 777,
                },
            ])
            .unwrap();
        manifest.close().unwrap();

        let reopened = cache_config.clone().open().unwrap();
        assert!(reopened.stats().open.policy_restored_from_checkpoint);
        assert!(!reopened.stats().open.bucket_usage_scanned);
        assert!(!reopened.stats().open.region_usage_scanned);
        let usage = reopened
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(usage.live_bytes, 777);
        assert_eq!(reopened.get_in(7, b"key").unwrap(), Some(b"value".to_vec()));
        reopened.close().unwrap();

        let (manifest, _) = HybridManifest::open_with_journal_capacity(
            &files.manifest,
            layout,
            DEFAULT_JOURNAL_CAPACITY,
        )
        .unwrap();
        manifest
            .publish_clean_with_usage(&[NamespaceUsage {
                namespace: 0,
                live_bytes: 0,
            }])
            .unwrap();
        manifest.close().unwrap();

        let rescanned = cache_config.open().unwrap();
        assert!(!rescanned.stats().open.policy_restored_from_checkpoint);
        assert!(rescanned.stats().open.bucket_usage_scanned);
        assert!(rescanned.stats().open.region_usage_scanned);
        let expected = bucket_namespace_usage_for_lengths(
            7,
            b"key".len(),
            HYBRID_VALUE_HEADER_SIZE + b"value".len(),
        )
        .unwrap()
        .live_bytes;
        let usage = rescanned
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(usage.live_bytes, expected);
        rescanned.close().unwrap();
    }

    #[test]
    fn dirty_recovery_rebuilds_usage_instead_of_trusting_clean_extension() {
        let files = TestFiles::new("dirty-usage-recovery");
        let cache_config = config(&files, 8 * 1024)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(16 * 1024));
        let layout = hybrid_layout_fingerprint(&cache_config.diagnostics().unwrap());
        let cache = cache_config.clone().open().unwrap();
        cache
            .put_in(7, b"key", b"value", PutOptions::default())
            .unwrap();
        cache.close().unwrap();

        let (manifest, _) = HybridManifest::open_with_journal_capacity(
            &files.manifest,
            layout,
            DEFAULT_JOURNAL_CAPACITY,
        )
        .unwrap();
        manifest
            .publish_clean_with_usage(&[
                NamespaceUsage {
                    namespace: 0,
                    live_bytes: 0,
                },
                NamespaceUsage {
                    namespace: 7,
                    live_bytes: 0,
                },
            ])
            .unwrap();
        let pending_key = b"never-published";
        manifest
            .append_intent(JournalIntentInput {
                kind: JournalIntentKind::PutRegion,
                namespace: 7,
                key_hash: hybrid_hash(7, pending_key),
                bucket_id: None,
                key: pending_key,
            })
            .unwrap();
        manifest.close().unwrap();

        let recovered = cache_config.open().unwrap();
        let expected = bucket_namespace_usage_for_lengths(
            7,
            b"key".len(),
            HYBRID_VALUE_HEADER_SIZE + b"value".len(),
        )
        .unwrap()
        .live_bytes;
        let usage = recovered
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(usage.live_bytes, expected);
        assert_eq!(
            recovered.get_in(7, b"key").unwrap(),
            Some(b"value".to_vec())
        );
        recovered.close().unwrap();
    }

    #[test]
    fn reformatted_existing_lower_tiers_reject_stale_clean_usage_checkpoint() {
        let files = TestFiles::new("reformatted-lower-usage");
        let cache_config = config(&files, 8 * 1024)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(64 * 1024));
        let cache = cache_config.clone().open().unwrap();
        let large = vec![0x6d; 2048];
        cache
            .put_in(7, b"small", b"value", PutOptions::default())
            .unwrap();
        cache
            .put_in(7, b"large", &large, PutOptions::default())
            .unwrap();
        assert!(
            cache
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes
                > 0
        );
        cache.close().unwrap();

        // Preserve each non-empty lower file while destroying both redundant
        // superblocks. Their safe open behavior is to format an empty cache;
        // that fallback must invalidate the older Hybrid usage extension.
        let zeros = [0_u8; crate::format::SUPERBLOCK_AREA_SIZE as usize];
        let bucket = FileBackend::open(&files.bucket).unwrap();
        write_all_at(&bucket, WritePoint::Superblock, &zeros, 0).unwrap();
        bucket
            .sync(SyncPoint::CheckpointClean, SyncMode::Data)
            .unwrap();
        let region = FileBackend::open(&files.region).unwrap();
        region.set_len(crate::format::SUPERBLOCK_AREA_SIZE).unwrap();
        write_all_at(&region, WritePoint::Superblock, &zeros, 0).unwrap();
        region
            .sync(SyncPoint::CheckpointClean, SyncMode::Data)
            .unwrap();

        let reopened = cache_config.open().unwrap();
        assert!(!reopened.stats().open.policy_restored_from_checkpoint);
        assert!(reopened.stats().open.bucket_usage_scanned);
        assert!(reopened.stats().open.region_usage_scanned);
        let namespace = reopened
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(namespace.live_bytes, 0);
        assert_eq!(namespace.reserved_bytes, 0);
        assert_eq!(reopened.get_in(7, b"small").unwrap(), None);
        assert_eq!(reopened.get_in(7, b"large").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn aggregate_budget_and_distinct_paths_are_validated_before_open() {
        let files = TestFiles::new("validation");
        let diagnostics = config(&files, 4096)
            .with_journal_capacity(64 * 1024)
            .diagnostics()
            .unwrap();
        assert_eq!(
            diagnostics.journal_recovery_memory_bytes,
            journal_recovery_memory_bytes(64 * 1024).unwrap()
        );
        assert!(
            diagnostics.configured_component_budget_bytes
                >= diagnostics.journal_recovery_memory_bytes
        );
        let too_small = config(&files, 4096).with_memory_budget(4096);
        assert!(matches!(
            too_small.diagnostics(),
            Err(CacheError::InvalidConfig(_))
        ));
        assert!(!files.bucket.exists());
        assert!(!files.region.exists());

        let same = HybridCacheConfig::new(
            4096,
            BucketCacheConfig::new(&files.bucket, 8 * 4096).with_memory_budget(4 * 1024 * 1024),
            CacheConfig::new(&files.bucket, 8 * 64 * 1024)
                .with_region_size(64 * 1024)
                .with_index_slots(1024)
                .with_max_value_size(32 * 1024)
                .with_memory_budget(16 * 1024 * 1024),
        )
        .with_memory_shards(4);
        assert!(matches!(
            same.diagnostics(),
            Err(CacheError::InvalidConfig(_))
        ));
        assert!(!files.bucket.exists());
    }

    #[test]
    fn rejected_transition_is_non_destructive_and_closed_wins_validation() {
        let files = TestFiles::new("reject-transition");
        let cache = config(&files, 8 * 1024).open().unwrap();
        cache.put(b"key", b"old", PutOptions::default()).unwrap();
        assert_eq!(
            cache
                .put(b"key", vec![0_u8; 40 * 1024], PutOptions::default())
                .unwrap(),
            PutOutcome::Rejected(RejectReason::ValueTooLarge)
        );
        assert_eq!(cache.get(b"key").unwrap(), Some(b"old".to_vec()));
        cache.close().unwrap();
        assert!(matches!(
            cache.put(
                b"key",
                b"value",
                PutOptions {
                    expires_at_unix_ms: Some(0),
                }
            ),
            Err(CacheError::Closed)
        ));
    }

    #[test]
    fn dirty_cross_tier_transition_never_revives_the_old_route() {
        let files = TestFiles::new("dirty-transition");
        let old = vec![1_u8; 2048];
        let cache = config(&files, 8 * 1024).open().unwrap();
        cache.put(b"key", &old, PutOptions::default()).unwrap();
        cache.flush().unwrap();
        cache
            .put(b"key", b"new-small", PutOptions::default())
            .unwrap();
        drop(cache);

        let reopened = config(&files, 8 * 1024).open().unwrap();
        assert_ne!(reopened.get(b"key").unwrap(), Some(old));
        reopened.close().unwrap();
    }

    #[test]
    fn repeated_small_updates_do_not_append_region_tombstones() {
        let files = TestFiles::new("small-update-write-amplification");
        let cache = config(&files, 8 * 1024).open().unwrap();
        for value in [b"one".as_slice(), b"two", b"three"] {
            assert_eq!(
                cache.put(b"key", value, PutOptions::default()).unwrap(),
                PutOutcome::Stored
            );
        }
        let stats = cache.stats();
        assert_eq!(stats.region.removes, 0);
        assert_eq!(cache.get(b"key").unwrap(), Some(b"three".to_vec()));
        cache.close().unwrap();
    }

    #[test]
    fn recreating_a_missing_manifest_invalidates_unbound_lower_files() {
        let files = TestFiles::new("missing-manifest");
        let cache_config = config(&files, 8 * 1024);
        let cache = cache_config.clone().open().unwrap();
        cache
            .put(b"small", b"value", PutOptions::default())
            .unwrap();
        cache.flush().unwrap();
        cache.close().unwrap();
        fs::remove_file(&files.manifest).unwrap();

        // Simulate a crash after the replacement manifest is formatted but
        // before Hybrid reaches its normal pre-lower open fence.
        let layout = hybrid_layout_fingerprint(&cache_config.diagnostics().unwrap());
        let host_writes = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
        let (manifest, opened) = HybridManifest::open_managed_with_journal_capacity(
            &files.manifest,
            layout,
            DEFAULT_JOURNAL_CAPACITY,
            host_writes,
            &[NamespaceUsage {
                namespace: 0,
                live_bytes: 0,
            }],
        )
        .unwrap();
        assert!(opened.created);
        assert!(opened.needs_recovery);
        manifest.close().unwrap();

        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.get(b"small").unwrap(), None);
        assert_eq!(
            reopened
                .put(b"small", b"replacement", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(reopened.status(), CacheStatus::Healthy);
        assert_eq!(
            reopened.get(b"small").unwrap(),
            Some(b"replacement".to_vec())
        );
        reopened.close().unwrap();
    }

    #[test]
    fn dirty_session_recovery_discards_the_complete_cache() {
        let files = TestFiles::new("dirty-session-recovery");
        let untouched_large = vec![4_u8; 2048];
        let touched_large = vec![5_u8; 2048];
        let cache = config(&files, 8 * 1024).open().unwrap();
        cache
            .put(b"small", b"untouched", PutOptions::default())
            .unwrap();
        cache
            .put(b"large", &untouched_large, PutOptions::default())
            .unwrap();
        cache.flush().unwrap();
        cache
            .put(b"touched", &touched_large, PutOptions::default())
            .unwrap();
        drop(cache);

        let reopened = config(&files, 8 * 1024).open().unwrap();
        assert_eq!(reopened.get(b"touched").unwrap(), None);
        assert_eq!(reopened.get(b"small").unwrap(), None);
        assert_eq!(reopened.get(b"large").unwrap(), None);
        reopened.close().unwrap();
    }

    #[cfg(unix)]
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CombinedCrashWorkload {
        SmallToLarge,
        LargeToSmall,
        Remove,
        TtlFlush,
        WriteBackEviction,
    }

    #[cfg(unix)]
    impl CombinedCrashWorkload {
        const fn name(self) -> &'static str {
            match self {
                Self::SmallToLarge => "small-to-large",
                Self::LargeToSmall => "large-to-small",
                Self::Remove => "remove",
                Self::TtlFlush => "ttl-flush",
                Self::WriteBackEviction => "write-back-eviction",
            }
        }

        fn parse(name: &str) -> Self {
            match name {
                "small-to-large" => Self::SmallToLarge,
                "large-to-small" => Self::LargeToSmall,
                "remove" => Self::Remove,
                "ttl-flush" => Self::TtlFlush,
                "write-back-eviction" => Self::WriteBackEviction,
                name => panic!("unknown combined Hybrid crash workload {name}"),
            }
        }

        const fn baseline(self) -> &'static [u8] {
            match self {
                Self::LargeToSmall => &[1_u8; 2048],
                _ => b"old-small",
            }
        }
    }

    #[cfg(unix)]
    fn combined_crash_config(
        bucket: &Path,
        region: &Path,
        manifest: &Path,
        workload: CombinedCrashWorkload,
    ) -> HybridCacheConfig {
        let memory_bytes = if workload == CombinedCrashWorkload::WriteBackEviction {
            300
        } else {
            8 * 1024
        };
        let mut config = config_for_paths(bucket, region, manifest, memory_bytes)
            .with_journal_capacity(64 * 1024);
        if workload == CombinedCrashWorkload::WriteBackEviction {
            config = config
                .with_memory_shards(1)
                .with_write_mode(HybridWriteMode::WriteBack)
                .with_write_back_resources(2, 2, 1024)
                .with_backpressure(BackpressurePolicy::Block);
        }
        config
    }

    #[cfg(unix)]
    fn run_combined_crash_worker(
        files: &TestFiles,
        point: HybridCrashPoint,
        workload: CombinedCrashWorkload,
    ) {
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("hybrid::tests::combined_crash_worker")
            .arg("--ignored")
            .arg("--test-threads=1")
            .env("CACHE_RS_HYBRID_COMBINED_BUCKET", &files.bucket)
            .env("CACHE_RS_HYBRID_COMBINED_REGION", &files.region)
            .env("CACHE_RS_HYBRID_COMBINED_MANIFEST", &files.manifest)
            .env("CACHE_RS_HYBRID_COMBINED_WORKLOAD", workload.name())
            .env("CACHE_RS_HYBRID_COMBINED_CRASH_POINT", point.name())
            // Every successful managed reopen now republishes the manifest
            // after its pre-lower dirty fence. For the clean-publication case,
            // occurrence one belongs to open and occurrence two to the
            // workload's explicit flush boundary under test.
            .env(
                "CACHE_RS_HYBRID_COMBINED_CRASH_OCCURRENCE",
                if point == HybridCrashPoint::GlobalCleanPublished {
                    "2"
                } else {
                    "1"
                },
            )
            .output()
            .unwrap();
        assert_eq!(
            output.status.signal(),
            Some(9),
            "combined crash point {point:?}/{workload:?} was not reached: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(unix)]
    #[test]
    fn combined_hybrid_crash_matrix_never_revives_a_superseded_value() {
        let cases = [
            (
                HybridCrashPoint::TargetWritten,
                CombinedCrashWorkload::SmallToLarge,
            ),
            (
                HybridCrashPoint::BeforeSourceRemove,
                CombinedCrashWorkload::LargeToSmall,
            ),
            (
                HybridCrashPoint::AfterSourceRemove,
                CombinedCrashWorkload::SmallToLarge,
            ),
            (
                HybridCrashPoint::AfterFirstRemove,
                CombinedCrashWorkload::Remove,
            ),
            (
                HybridCrashPoint::AfterAllRemoves,
                CombinedCrashWorkload::Remove,
            ),
            (
                HybridCrashPoint::GlobalCleanPublished,
                CombinedCrashWorkload::TtlFlush,
            ),
            (
                HybridCrashPoint::TargetWritten,
                CombinedCrashWorkload::WriteBackEviction,
            ),
        ];

        for (point, workload) in cases {
            let files = TestFiles::new(&format!("combined-{}-{}", point.name(), workload.name()));
            let cache_config =
                combined_crash_config(&files.bucket, &files.region, &files.manifest, workload);
            let cache = cache_config.clone().open().unwrap();
            cache
                .put(b"key", workload.baseline(), PutOptions::default())
                .unwrap();
            cache.flush().unwrap();
            cache.close().unwrap();

            run_combined_crash_worker(&files, point, workload);
            if workload == CombinedCrashWorkload::TtlFlush {
                thread::sleep(Duration::from_millis(350));
            }
            let reopened = cache_config.open().unwrap();
            let recovered = reopened.get(b"key").unwrap();
            match workload {
                CombinedCrashWorkload::WriteBackEviction => {
                    assert_ne!(recovered.as_deref(), Some(workload.baseline()));
                    assert!(
                        recovered.is_none() || recovered.as_deref() == Some(b"write-back-latest"),
                        "{point:?}/{workload:?} recovered {recovered:?}"
                    );
                }
                CombinedCrashWorkload::TtlFlush
                | CombinedCrashWorkload::SmallToLarge
                | CombinedCrashWorkload::LargeToSmall
                | CombinedCrashWorkload::Remove => {
                    assert_eq!(recovered, None, "{point:?}/{workload:?}");
                }
            }
            reopened.close().unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "spawned by combined_hybrid_crash_matrix_never_revives_a_superseded_value"]
    fn combined_crash_worker() {
        let Ok(bucket) = std::env::var("CACHE_RS_HYBRID_COMBINED_BUCKET") else {
            return;
        };
        let region = std::env::var("CACHE_RS_HYBRID_COMBINED_REGION").unwrap();
        let manifest = std::env::var("CACHE_RS_HYBRID_COMBINED_MANIFEST").unwrap();
        let workload = CombinedCrashWorkload::parse(
            &std::env::var("CACHE_RS_HYBRID_COMBINED_WORKLOAD").unwrap(),
        );
        let cache = combined_crash_config(
            Path::new(&bucket),
            Path::new(&region),
            Path::new(&manifest),
            workload,
        )
        .open()
        .unwrap();

        match workload {
            CombinedCrashWorkload::SmallToLarge => {
                cache
                    .put(b"key", vec![2_u8; 2048], PutOptions::default())
                    .unwrap();
            }
            CombinedCrashWorkload::LargeToSmall => {
                cache
                    .put(b"key", b"new-small", PutOptions::default())
                    .unwrap();
            }
            CombinedCrashWorkload::Remove => {
                cache.remove(b"key").unwrap();
            }
            CombinedCrashWorkload::TtlFlush => {
                cache
                    .put(
                        b"key",
                        b"ttl-latest",
                        PutOptions {
                            expires_at_unix_ms: Some(now_unix_ms().saturating_add(250)),
                        },
                    )
                    .unwrap();
                cache.flush().unwrap();
            }
            CombinedCrashWorkload::WriteBackEviction => {
                cache
                    .put(b"key", b"write-back-latest", PutOptions::default())
                    .unwrap();
                cache
                    .put(b"evictor", b"force-eviction", PutOptions::default())
                    .unwrap();
                // Dirty eviction is detached. Drain it so this subprocess
                // reaches the requested persistence failpoint deterministically.
                cache.flush().unwrap();
            }
        }
        panic!("combined Hybrid crash point was not reached");
    }

    #[test]
    fn steady_state_mutations_do_not_write_the_durable_journal() {
        let files = TestFiles::new("volatile-mutations");
        let cache = config(&files, 8 * 1024)
            .with_journal_capacity(64 * 1024)
            .open()
            .unwrap();
        let journal_before = cache.stats().journal_used_bytes;
        for index in 0..80_u64 {
            let mut key = vec![b'k'; 900];
            key[..8].copy_from_slice(&index.to_le_bytes());
            assert_eq!(
                cache.put(&key, b"value", PutOptions::default()).unwrap(),
                PutOutcome::Stored
            );
        }
        let stats = cache.stats();
        assert_eq!(stats.journal_used_bytes, journal_before);
        assert_eq!(stats.journal_rollovers, 0);
        assert_eq!(stats.journal_group_commit.committed_records, 0);
        assert_eq!(stats.journal_group_commit.durability_syncs, 0);
        cache.close().unwrap();
    }

    #[test]
    fn clear_discards_dirty_memory_without_persisting_it() {
        let files = TestFiles::new("clear-dirty-write-back");
        let cache_config = config(&files, 8 * 1024)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack);
        let cache = cache_config.clone().open().unwrap();

        cache
            .put(b"key", b"dirty-value", PutOptions::default())
            .unwrap();
        assert_eq!(cache.stats().memory_dirty_entries, 1);
        assert_eq!(cache.stats().bucket.puts, 0);
        cache.clear().unwrap();
        assert_eq!(cache.get(b"key").unwrap(), None);
        assert_eq!(cache.stats().bucket.puts, 0);
        cache.close().unwrap();

        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn request_memory_rejects_before_the_durable_intent() {
        let files = TestFiles::new("request-memory");
        let cache = config(&files, 8 * 1024)
            .with_request_memory(64)
            .open()
            .unwrap();
        let journal_before = cache.stats().journal_used_bytes;
        assert_eq!(
            cache
                .put(b"key", vec![7_u8; 32], PutOptions::default())
                .unwrap(),
            PutOutcome::Rejected(RejectReason::BufferUnavailable)
        );
        let stats = cache.stats();
        assert_eq!(stats.journal_used_bytes, journal_before);
        assert_eq!(stats.request_rejections, 1);
        assert_eq!(cache.get(b"key").unwrap(), None);
        cache.close().unwrap();
    }

    #[test]
    fn request_memory_bounds_l1_clones_and_current_disk_candidates() {
        let files = TestFiles::new("read-request-memory");
        let value = vec![9_u8; 2048];
        let cache = config(&files, 64 * 1024)
            .with_request_memory(4096)
            .open()
            .unwrap();
        assert_eq!(
            cache.put(b"large", &value, PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.get(b"large").unwrap(), Some(value.clone()));
        assert!(cache.stats().request_bytes_peak <= 4096);
        cache.close().unwrap();

        // The Region candidate and returned clone need slightly more than
        // 4 KiB. Admission rejects before issuing the read and never exceeds
        // the configured request budget.
        let cache = config(&files, 64 * 1024)
            .with_request_memory(4096)
            .open()
            .unwrap();
        assert!(matches!(
            cache.get(b"large"),
            Err(CacheError::Overloaded(
                OverloadReason::ReadBufferUnavailable
            ))
        ));
        let stats = cache.stats();
        assert_eq!(stats.request_rejections, 1);
        assert!(stats.request_bytes_peak <= 4096);
        cache.close().unwrap();

        // Reservation is based on the current record, not the configured
        // 32-KiB maximum, so a modest increase admits this 2-KiB value.
        let cache = config(&files, 64 * 1024)
            .with_request_memory(12 * 1024)
            .open()
            .unwrap();
        assert_eq!(cache.get(b"large").unwrap(), Some(value));
        assert!(cache.stats().request_bytes_peak <= 12 * 1024);
        cache.close().unwrap();
    }

    #[test]
    fn async_handle_is_shared_and_close_drains_accepted_mutations() {
        let files = TestFiles::new("async");
        let cache = config(&files, 8 * 1024)
            .with_async_queue_depths(4, 4)
            .with_async_workers(2, 2)
            .open()
            .unwrap();
        let first = cache.async_handle().unwrap();
        let second = cache.async_handle().unwrap();
        assert_eq!(first.queue_stats().read_queue_capacity, 4);
        let pending = first.put(b"key", b"value", PutOptions::default());
        cache.close().unwrap();
        assert_eq!(pending.wait().unwrap(), PutOutcome::Stored);
        assert!(matches!(second.get(b"key").wait(), Err(CacheError::Closed)));

        let reopened = config(&files, 8 * 1024).open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
        reopened.close().unwrap();
    }

    #[test]
    fn cancelled_and_timed_out_bucket_reads_release_hybrid_capacity_before_close_drain() {
        let files = TestFiles::new("async-read-stop");
        let mut cache_config = config(&files, 8 * 1024)
            .with_request_slots(3)
            .with_async_queue_depths(4, 2)
            .with_async_workers(2, 1);
        cache_config.bucket = cache_config
            .bucket
            .clone()
            .with_buffer_slots(1)
            .with_io_queue_depth(1);
        let bucket_config = cache_config.bucket.clone();
        let mut cache = cache_config.clone().open().unwrap();

        let first_key = b"blocked-a".to_vec();
        let first_bucket = cache.inner.disk.bucket.bucket_id_for(0, &first_key);
        let first_ordering = hybrid_hash(0, &first_key) % cache.inner.ordering.len() as u64;
        let second_key = (0_u64..)
            .map(|index| format!("waiting-{index}").into_bytes())
            .find(|key| {
                cache.inner.disk.bucket.bucket_id_for(0, key) != first_bucket
                    && hybrid_hash(0, key) % cache.inner.ordering.len() as u64 != first_ordering
            })
            .unwrap();
        cache
            .put(&first_key, b"first", PutOptions::default())
            .unwrap();
        cache
            .put(&second_key, b"second", PutOptions::default())
            .unwrap();
        cache.flush().unwrap();
        cache.inner.memory.clear();

        cache.inner.disk.bucket.close().unwrap();
        let (backend, blocking) = BlockingFileBackend::open(&files.bucket).unwrap();
        let controlled_bucket = BucketCache::open_with_backend_and_host_writes(
            bucket_config,
            Box::new(backend),
            Some(Arc::clone(cache.inner.policy.host_writes())),
        )
        .unwrap();
        Arc::get_mut(&mut cache.inner).unwrap().disk.bucket = controlled_bucket;
        blocking.arm();

        let asynchronous = cache.async_handle().unwrap();
        let blocked = asynchronous.get(&first_key);
        assert!(blocking.wait_for_entered(1));

        let cancelled = asynchronous.get(&second_key);
        assert!(wait_until(Duration::from_secs(1), || {
            asynchronous.queue_stats().read_in_flight == 2 && cache.stats().requests_in_flight == 2
        }));
        assert_eq!(cancelled.cancel(), crate::CancelOutcome::Requested);
        assert!(matches!(cancelled.wait(), Err(CacheError::Cancelled)));
        assert!(wait_until(Duration::from_secs(1), || {
            asynchronous.queue_stats().read_in_flight == 1 && cache.stats().requests_in_flight == 1
        }));
        assert_eq!(cache.stats().promotions, 0);

        let timed_out = asynchronous.get_with_options(
            &second_key,
            crate::AsyncRequestOptions::with_timeout(Duration::from_millis(30)),
        );
        assert!(wait_until(Duration::from_secs(1), || {
            asynchronous.queue_stats().read_in_flight == 2 && cache.stats().requests_in_flight == 2
        }));
        assert!(matches!(timed_out.wait(), Err(CacheError::TimedOut)));
        assert!(wait_until(Duration::from_secs(1), || {
            asynchronous.queue_stats().read_in_flight == 1 && cache.stats().requests_in_flight == 1
        }));
        assert_eq!(cache.stats().promotions, 0);
        assert_eq!(cache.health_snapshot().bucket, CacheStatus::Healthy);

        assert!(matches!(
            cache.close_with_timeout(Duration::from_millis(30)),
            Err(CacheError::TimedOut)
        ));
        drop(asynchronous);
        let close_stats = cache.metrics_snapshot().async_close.unwrap();
        assert!(close_stats.draining);
        assert_eq!(close_stats.timed_out_waits, 1);
        assert_ne!(cache.status(), CacheStatus::Closed);
        assert!(matches!(
            cache_config.clone().open(),
            Err(CacheError::Locked)
        ));

        blocking.release();
        assert_eq!(blocked.wait().unwrap(), Some(b"first".to_vec()));
        cache.close_with_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(cache.status(), CacheStatus::Closed);
        let close_stats = cache.metrics_snapshot().async_close.unwrap();
        assert!(close_stats.completed);
        assert!(close_stats.succeeded);
        assert_eq!(close_stats.timed_out_waits, 1);
        assert!(close_stats.drain_duration_ns != 0);

        let reopened = cache_config.open().unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn page_buffer_wait_honors_async_stop_and_does_not_block_close_drain() {
        let files = TestFiles::new("async-page-wait-stop");
        let mut cache_config = config(&files, 8 * 1024)
            .with_request_slots(2)
            .with_async_queue_depths(2, 2)
            .with_async_workers(1, 1);
        cache_config.bucket = cache_config.bucket.clone().with_buffer_slots(1);
        let cache = cache_config.open().unwrap();
        cache.put(b"key", b"value", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        cache.inner.memory.clear();
        let held_page = cache.inner.disk.bucket.hold_page_for_test().unwrap();

        let asynchronous = cache.async_handle().unwrap();
        let cancelled = asynchronous.get(b"key");
        assert!(wait_until(Duration::from_secs(1), || {
            cache.inner.disk.bucket.page_waiters_for_test() == 1
                && cache.stats().requests_in_flight == 1
        }));
        assert_eq!(cancelled.cancel(), crate::CancelOutcome::Requested);
        assert!(matches!(cancelled.wait(), Err(CacheError::Cancelled)));
        assert!(wait_until(Duration::from_secs(1), || {
            cache.inner.disk.bucket.page_waiters_for_test() == 0
                && cache.stats().requests_in_flight == 0
                && asynchronous.queue_stats().read_in_flight == 0
        }));

        let timed_out = asynchronous.get_with_options(
            b"key",
            crate::AsyncRequestOptions::with_timeout(Duration::from_millis(30)),
        );
        assert!(wait_until(Duration::from_secs(1), || {
            cache.inner.disk.bucket.page_waiters_for_test() == 1
                && cache.stats().requests_in_flight == 1
        }));
        assert!(matches!(timed_out.wait(), Err(CacheError::TimedOut)));
        assert!(wait_until(Duration::from_secs(1), || {
            cache.inner.disk.bucket.page_waiters_for_test() == 0
                && cache.stats().requests_in_flight == 0
                && asynchronous.queue_stats().read_in_flight == 0
        }));
        assert_eq!(cache.stats().promotions, 0);
        assert_eq!(cache.health_snapshot().bucket, CacheStatus::Healthy);

        let close = asynchronous.close();
        let (closed_tx, closed_rx) = mpsc::channel();
        let close_waiter = thread::spawn(move || closed_tx.send(close.wait()).unwrap());
        assert!(
            closed_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .is_ok()
        );
        close_waiter.join().unwrap();
        drop(held_page);
        assert_eq!(cache.status(), CacheStatus::Closed);
    }

    #[test]
    fn write_back_evicts_to_background_and_flushes_remaining_dirty_entries() {
        let files = TestFiles::new("write-back");
        let cache_config = config(&files, 300)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(2, 2, 1024)
            .with_backpressure(BackpressurePolicy::Block);
        let cache = cache_config.clone().open().unwrap();

        assert_eq!(
            cache.put(b"a", b"value-a", PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.stats().bucket.puts, 0);
        assert_eq!(cache.stats().memory_dirty_entries, 1);

        assert_eq!(
            cache.put(b"b", b"value-b", PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert!(wait_until(Duration::from_secs(2), || {
            cache.stats().write_back.proactive_persisted == 1
        }));
        assert_eq!(cache.stats().bucket.puts, 1);
        assert!(matches!(
            cache.lookup(b"a").unwrap(),
            HybridLookupOutcome::Hit {
                value,
                tier: CacheTier::SmallObjectDisk,
            } if value == b"value-a"
        ));
        assert_eq!(cache.get(b"b").unwrap(), Some(b"value-b".to_vec()));

        cache.flush().unwrap();
        let stats = cache.stats();
        assert_eq!(stats.memory_dirty_entries, 0);
        assert_eq!(stats.write_back.memory_only_puts, 2);
        assert_eq!(stats.write_back.demoted_entries, 2);
        assert_eq!(stats.write_back.proactive_scheduled, 1);
        assert_eq!(stats.write_back.proactive_persisted, 1);
        assert_eq!(stats.write_back.queue_submitted, 2);
        cache.close().unwrap();

        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.get(b"a").unwrap(), Some(b"value-a".to_vec()));
        assert_eq!(reopened.get(b"b").unwrap(), Some(b"value-b".to_vec()));
        reopened.close().unwrap();
    }

    #[test]
    fn dirty_eviction_does_not_wait_for_a_blocked_bucket_page() {
        let files = TestFiles::new("write-back-nonblocking-eviction");
        let mut cache_config = config(&files, 300)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(2, 1, 1024)
            .with_backpressure(BackpressurePolicy::Block);
        cache_config.bucket = cache_config
            .bucket
            .clone()
            .with_buffer_slots(1)
            .with_io_queue_depth(1);
        let cache = cache_config.open().unwrap();
        cache.put(b"a", b"value-a", PutOptions::default()).unwrap();

        let held_page = cache.inner.disk.bucket.hold_page_for_test().unwrap();
        let started = Instant::now();
        assert_eq!(
            cache.put(b"b", b"value-b", PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(
            wait_until(Duration::from_secs(1), || {
                cache.inner.disk.bucket.page_waiters_for_test() == 1
            }),
            "background eviction did not reach the page pool: {:?}",
            cache.stats().write_back
        );

        drop(held_page);
        cache.flush().unwrap();
        assert_eq!(cache.get(b"a").unwrap(), Some(b"value-a".to_vec()));
        assert_eq!(cache.get(b"b").unwrap(), Some(b"value-b".to_vec()));
        let write_back = cache.stats().write_back;
        assert_eq!(write_back.proactive_persisted, 1);
        assert_eq!(write_back.proactive_skipped, 0);
        cache.close().unwrap();
    }

    #[test]
    fn existing_lower_dirty_eviction_is_async_and_masks_stale_l2() {
        let files = TestFiles::new("write-back-pending-mask");
        let mut cache_config = config(&files, 300)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(2, 1, 1024)
            .with_backpressure(BackpressurePolicy::Block);
        cache_config.bucket = cache_config
            .bucket
            .clone()
            .with_buffer_slots(1)
            .with_io_queue_depth(1);
        let cache = cache_config.open().unwrap();

        cache.put(b"key", b"old", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        let before = cache.stats().write_back;
        cache.put(b"key", b"new", PutOptions::default()).unwrap();

        let held_page = cache.inner.disk.bucket.hold_page_for_test().unwrap();
        let started = Instant::now();
        assert_eq!(
            cache
                .put(b"evictor", b"force-eviction", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(wait_until(Duration::from_secs(1), || {
            cache.stats().write_back.pending_entries == 1
                && cache.inner.disk.bucket.page_waiters_for_test() == 1
        }));

        let lookup_started = Instant::now();
        assert_eq!(
            cache.lookup(b"key").unwrap(),
            HybridLookupOutcome::Miss(HybridMissKind::NotResident)
        );
        assert!(lookup_started.elapsed() < Duration::from_millis(250));
        assert_eq!(cache.inner.disk.bucket.page_waiters_for_test(), 1);

        drop(held_page);
        assert!(wait_until(Duration::from_secs(2), || {
            cache.stats().write_back.pending_entries == 0
                && cache.stats().write_back.proactive_persisted == 1
        }));
        assert_eq!(cache.get(b"key").unwrap(), Some(b"new".to_vec()));
        let after = cache.stats().write_back;
        assert_eq!(
            after.lower_candidate_evictions - before.lower_candidate_evictions,
            1
        );
        assert_eq!(
            after.synchronous_demotions - before.synchronous_demotions,
            0
        );
        assert_eq!(after.pending_lookup_misses, 1);
        cache.close().unwrap();
    }

    #[test]
    fn saturated_lower_update_invalidates_without_io_and_clean_reopen_is_empty() {
        let files = TestFiles::new("write-back-overload-invalidation");
        let cache_config = config(&files, 300)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(4, 1, 1024)
            .with_backpressure(BackpressurePolicy::Block);
        let cache = cache_config.clone().open().unwrap();

        cache.put(b"key", b"old", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        cache.put(b"key", b"new", PutOptions::default()).unwrap();

        let executor = cache.inner.write_back.as_ref().unwrap();
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::channel();
        let blocker = executor.try_reserve_background(1).unwrap();
        executor
            .submit_background(
                blocker,
                move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                },
                || {},
            )
            .unwrap();
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        for _ in 0..2 {
            let queued = executor.try_reserve_background(1).unwrap();
            executor.submit_background(queued, || {}, || {}).unwrap();
        }

        let before = cache.stats();
        assert_eq!(
            cache
                .put(b"evictor", b"force-eviction", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        let after_put = cache.stats();
        assert_eq!(after_put.write_back.pending_entries, 0);
        assert_eq!(after_put.write_back.queue_in_flight, 3);
        assert_eq!(after_put.bucket.io_submitted, before.bucket.io_submitted);
        assert_eq!(after_put.region.io_submitted, before.region.io_submitted);
        assert_eq!(
            after_put.write_back.proactive_invalidated - before.write_back.proactive_invalidated,
            1
        );
        assert_eq!(
            after_put.write_back.dropped_evictions - before.write_back.dropped_evictions,
            1
        );
        assert!(after_put.write_back.volatile_loss_pending);
        assert_eq!(
            cache.lookup(b"key").unwrap(),
            HybridLookupOutcome::Miss(HybridMissKind::NotResident)
        );

        release_tx.send(()).unwrap();
        assert!(wait_until(Duration::from_secs(2), || {
            cache.stats().write_back.queue_in_flight == 0
        }));
        assert_eq!(cache.get(b"key").unwrap(), None);
        let after = cache.stats();
        assert_eq!(
            after.write_back.demotion_failures - before.write_back.demotion_failures,
            0
        );
        assert_eq!(after.bucket.io_submitted, before.bucket.io_submitted);
        assert_eq!(after.region.io_submitted, before.region.io_submitted);

        cache.close().unwrap();
        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
        assert_eq!(reopened.get(b"evictor").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn flush_after_volatile_pressure_loss_publishes_an_empty_boundary() {
        let files = TestFiles::new("write-back-volatile-loss-flush");
        let cache_config = config(&files, 300)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(4, 1, 1024)
            .with_backpressure(BackpressurePolicy::Block);
        let cache = cache_config.clone().open().unwrap();

        cache.put(b"key", b"old", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        cache.put(b"key", b"new", PutOptions::default()).unwrap();
        let executor = cache.inner.write_back.as_ref().unwrap();
        let first = executor.try_reserve_background(1).unwrap();
        let second = executor.try_reserve_background(1).unwrap();
        let third = executor.try_reserve_background(1).unwrap();
        assert_eq!(
            cache
                .put(b"evictor", b"force-eviction", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert!(cache.stats().write_back.volatile_loss_pending);
        drop((first, second, third));

        cache.flush().unwrap();
        let flushed = cache.stats();
        assert!(!flushed.write_back.volatile_loss_pending);
        assert_eq!(flushed.memory_entries, 0);
        assert_eq!(cache.get(b"key").unwrap(), None);
        assert_eq!(cache.get(b"evictor").unwrap(), None);
        let namespace = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|namespace| namespace.namespace == 0)
            .unwrap();
        assert_eq!(namespace.live_bytes, 0);
        assert_eq!(namespace.reserved_bytes, 0);

        cache
            .put(b"survivor", b"after-flush", PutOptions::default())
            .unwrap();
        cache.close().unwrap();
        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
        assert_eq!(
            reopened.get(b"survivor").unwrap().as_deref(),
            Some(b"after-flush".as_slice())
        );
        reopened.close().unwrap();
    }

    #[test]
    fn remove_waits_for_exact_pending_write_and_cannot_be_resurrected() {
        let files = TestFiles::new("write-back-pending-remove");
        let mut cache_config = config(&files, 300)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(2, 1, 1024)
            .with_backpressure(BackpressurePolicy::Block);
        cache_config.bucket = cache_config
            .bucket
            .clone()
            .with_buffer_slots(1)
            .with_io_queue_depth(1);
        let cache = cache_config.clone().open().unwrap();

        cache.put(b"key", b"old", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        cache.put(b"key", b"new", PutOptions::default()).unwrap();
        let held_page = cache.inner.disk.bucket.hold_page_for_test().unwrap();
        cache
            .put(b"evictor", b"force-eviction", PutOptions::default())
            .unwrap();
        assert!(wait_until(Duration::from_secs(1), || {
            cache.stats().write_back.pending_entries == 1
                && cache.inner.disk.bucket.page_waiters_for_test() == 1
        }));

        let remover = cache.clone();
        let (removed_tx, removed_rx) = mpsc::channel();
        let thread = thread::spawn(move || removed_tx.send(remover.remove(b"key")).unwrap());
        assert!(wait_until(Duration::from_secs(1), || {
            cache.stats().write_back.pending_same_key_waits == 1
        }));
        assert!(matches!(
            removed_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        drop(held_page);
        assert_eq!(
            removed_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap(),
            RemoveOutcome::Removed
        );
        thread.join().unwrap();
        assert_eq!(cache.get(b"key").unwrap(), None);
        cache.flush().unwrap();
        cache.close().unwrap();

        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn background_eviction_cannot_starve_a_synchronous_demotion_worker() {
        let files = TestFiles::new("write-back-ordering-starvation");
        let cache = config(&files, 8 * 1024)
            .with_memory_shards(2)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(2, 1, 64 * 1024)
            .with_backpressure(BackpressurePolicy::Block)
            .open()
            .unwrap();

        let first_key = b"blocked-background".to_vec();
        let first_stripe = hybrid_hash(0, &first_key) as usize & (cache.inner.ordering.len() - 1);
        let second_key = (0_u64..)
            .map(|index| format!("foreground-{index}").into_bytes())
            .find(|key| {
                hybrid_hash(0, key) as usize & (cache.inner.ordering.len() - 1) != first_stripe
            })
            .unwrap();
        cache
            .put(&first_key, b"first", PutOptions::default())
            .unwrap();
        cache
            .put(&second_key, b"second", PutOptions::default())
            .unwrap();

        // Detach two real dirty entries while retaining their pending
        // namespace charges. This models MemoryEngine handing victims to the
        // asynchronous and synchronous demotion paths.
        let first_hash = hybrid_hash(0, &first_key);
        let first_ordering = cache.lock_key(first_hash);
        let first = cache.inner.memory.remove(0, &first_key).unwrap();
        let second = cache.inner.memory.remove(0, &second_key).unwrap();
        assert!(!first.disk_clean && !second.disk_clean);

        let executor = cache.inner.write_back.as_ref().unwrap();
        let background_bytes = first
            .charged_bytes()
            .unwrap()
            .checked_add(first.key.len())
            .and_then(|bytes| bytes.checked_add(PENDING_WRITE_OWNED_OVERHEAD_BYTES))
            .unwrap();
        let background = executor.try_reserve_background(background_bytes).unwrap();
        let pending = cache
            .inner
            .pending_writes
            .try_register(
                first.namespace,
                &first.key,
                first_hash,
                first.version,
                background_bytes,
            )
            .unwrap();
        let background_cache = cache.clone();
        let background_pending = Arc::clone(&pending);
        let panic_cache = cache.clone();
        let panic_pending = Arc::clone(&pending);
        executor
            .submit_background(
                background,
                move || background_cache.run_async_eviction(first, background_pending),
                move || {
                    panic_cache.poison();
                    panic_cache.inner.pending_writes.fail(&panic_pending);
                },
            )
            .unwrap();

        // With the old blocking ordering lock, the only worker stopped on the
        // first stripe and this mandatory demotion could never run. Keep the
        // first stripe locked until after the bounded observation, then always
        // release it so a regressed implementation fails without hanging the
        // test process.
        let foreground_cache = cache.clone();
        let second_hash = hybrid_hash(0, &second_key);
        let (completed_tx, completed_rx) = mpsc::channel();
        let foreground = thread::spawn(move || {
            let _second_ordering = foreground_cache.lock_key(second_hash);
            let result = foreground_cache.demote_dirty_entry_with_usage(&second);
            completed_tx.send(result).unwrap();
        });
        let timely = completed_rx.recv_timeout(Duration::from_secs(2));
        let completed_before_unlock = timely.is_ok();
        drop(first_ordering);
        let result = match timely {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => completed_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("synchronous demotion did not recover after releasing ordering stripe"),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("synchronous demotion worker disconnected")
            }
        };
        foreground.join().unwrap();

        assert!(
            completed_before_unlock,
            "background eviction occupied the only write-back worker while waiting for ordering"
        );
        let persisted = match result {
            Ok(bytes) => bytes,
            Err(_) => panic!("synchronous demotion failed"),
        };
        assert!(persisted > 0);
        assert!(wait_until(Duration::from_secs(1), || {
            cache.stats().write_back.queue_in_flight == 0
        }));
        assert_eq!(cache.stats().write_back.proactive_persisted, 1);
        assert_eq!(cache.stats().write_back.proactive_skipped, 0);
        assert_eq!(cache.get(&first_key).unwrap(), Some(b"first".to_vec()));
        assert_eq!(cache.get(&second_key).unwrap(), Some(b"second".to_vec()));
        cache.close().unwrap();
    }

    #[test]
    fn repeated_write_back_updates_stay_memory_only_until_flush() {
        let files = TestFiles::new("write-back-hot-update");
        let cache_config = config(&files, 8 * 1024)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(2, 1, 1024)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(64 * 1024));
        let cache = cache_config.clone().open().unwrap();
        let journal_before = cache.stats().journal_used_bytes;

        assert_eq!(
            cache
                .put_in(7, b"key", b"revision-a", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache
                .put_in(7, b"key", b"revision-b", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        let before_flush = cache.stats();
        assert_eq!(before_flush.bucket.puts, 0);
        assert_eq!(before_flush.journal_used_bytes, journal_before);
        assert_eq!(before_flush.journal_group_commit.durability_syncs, 0);
        assert_eq!(before_flush.memory_dirty_entries, 1);
        assert_eq!(
            cache.get_in(7, b"key").unwrap(),
            Some(b"revision-b".to_vec())
        );
        cache.flush().unwrap();

        let stats = cache.stats().write_back;
        assert_eq!(stats.proactive_scheduled, 0);
        assert_eq!(stats.proactive_persisted, 0);
        assert_eq!(stats.demoted_entries, 1);
        assert_eq!(stats.queue_rejections, 0);
        cache.close().unwrap();

        let reopened = cache_config.open().unwrap();
        assert_eq!(
            reopened.get_in(7, b"key").unwrap(),
            Some(b"revision-b".to_vec())
        );
        reopened.close().unwrap();
    }

    #[test]
    fn flush_rearms_the_session_dirty_fence_before_returning() {
        let files = TestFiles::new("flush-session-fence");
        let cache = config(&files, 8 * 1024)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(2, 1, 1024)
            .open()
            .unwrap();
        let journal_before = cache.stats().journal_used_bytes;
        cache.put(b"a", b"first", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        assert!(!cache.inner.manifest.snapshot().unwrap().clean);
        cache.put(b"b", b"second", PutOptions::default()).unwrap();
        let stats = cache.stats();
        assert_eq!(stats.journal_used_bytes, journal_before);
        assert_eq!(stats.journal_group_commit.durability_syncs, 0);
        assert_eq!(cache.get(b"b").unwrap(), Some(b"second".to_vec()));
        cache.close().unwrap();
    }

    #[test]
    fn write_back_entry_larger_than_demotion_budget_falls_back_before_dirty_publish() {
        let files = TestFiles::new("write-back-single-entry-budget");
        let value = vec![7_u8; 2048];
        let cache_config = config(&files, 8 * 1024)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(2, 1, 128);
        let cache = cache_config.clone().open().unwrap();

        assert_eq!(
            cache.put(b"large", &value, PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        let stats = cache.stats();
        assert_eq!(stats.memory_dirty_entries, 0);
        assert_eq!(stats.write_back.write_through_fallbacks, 1);
        assert_eq!(stats.region.puts, 1);
        cache.flush().unwrap();
        cache.close().unwrap();

        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.get(b"large").unwrap(), Some(value));
        reopened.close().unwrap();
    }

    #[test]
    fn write_back_fallback_replacement_retires_exact_pending_charge() {
        let files = TestFiles::new("write-back-fallback-replacement");
        let replacement = vec![0x5a; 512];
        let cache_config = config(&files, 8 * 1024)
            .with_memory_shards(1)
            .with_small_object_max(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(2, 1, 768)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(16 * 1024));
        let cache = cache_config.clone().open().unwrap();
        let region = &cache.inner.disk.region;
        region.force_direct_append_padding_for_test();

        assert_eq!(
            cache
                .put_in(7, b"key", b"old", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        let pending = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap()
            .live_bytes;
        assert_eq!(cache.stats().memory_dirty_entries, 1);

        // This value cannot fit the write-back demotion reservation, so the
        // same-key replacement commits directly to Region. The physical
        // receipt must be installed before the prior dirty pending charge is
        // refunded; afterwards only the new record remains live.
        assert_eq!(
            cache
                .put_in(7, b"key", &replacement, PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.stats().memory_dirty_entries, 0);
        let actual = region
            .candidate_record_bytes_in(7, b"key")
            .unwrap()
            .unwrap() as u64;
        assert!(pending >= actual);
        let namespace = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(namespace.live_bytes, actual);
        assert_eq!(namespace.reserved_bytes, 0);
        assert_eq!(cache.get_in(7, b"key").unwrap(), Some(replacement.clone()));
        cache.flush().unwrap();
        cache.close().unwrap();

        let reopened = cache_config.open().unwrap();
        assert!(reopened.stats().open.policy_restored_from_checkpoint);
        assert_eq!(
            reopened
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            actual
        );
        assert_eq!(reopened.get_in(7, b"key").unwrap(), Some(replacement));
        reopened.close().unwrap();
    }

    #[test]
    fn write_back_l1_oversize_fallback_retires_dirty_credit_exactly() {
        let files = TestFiles::new("write-back-l1-oversize-replacement");
        let replacement = vec![0x6b; 512];
        let cache_config = config(&files, 300)
            .with_memory_shards(1)
            .with_small_object_max(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_write_back_resources(4, 1, 4096)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(16 * 1024));
        let cache = cache_config.clone().open().unwrap();

        assert_eq!(
            cache
                .put_in(7, b"key", b"old", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.stats().memory_dirty_entries, 1);

        // The replacement fits the executor budget but not this single L1
        // shard, exercising the MemoryPutResult::NotStored disk fallback.
        assert_eq!(
            cache
                .put_in(7, b"key", &replacement, PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.stats().memory_dirty_entries, 0);
        let actual = cache
            .inner
            .disk
            .region
            .candidate_record_bytes_in(7, b"key")
            .unwrap()
            .unwrap() as u64;
        let namespace = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(namespace.live_bytes, actual);
        assert_eq!(namespace.reserved_bytes, 0);
        assert_eq!(cache.get_in(7, b"key").unwrap(), Some(replacement.clone()));
        cache.close().unwrap();

        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.get_in(7, b"key").unwrap(), Some(replacement));
        reopened.close().unwrap();
    }

    #[test]
    fn write_back_dirty_replacement_uses_credit_but_clean_l2_does_not() {
        let files = TestFiles::new("write-back-policy");
        let value = b"value";
        let live_bytes =
            bucket_namespace_usage_for_lengths(7, 1, HYBRID_VALUE_HEADER_SIZE + value.len())
                .unwrap()
                .live_bytes;
        let cache = config(&files, 8 * 1024)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(live_bytes))
            .open()
            .unwrap();
        assert_eq!(
            cache.put_in(7, b"a", value, PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.stats().bucket.puts, 0);
        assert_eq!(
            cache.put_in(7, b"a", value, PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        cache.flush().unwrap();
        assert_eq!(
            cache.put_in(7, b"a", value, PutOptions::default()).unwrap(),
            PutOutcome::Rejected(RejectReason::NamespaceCapacityExceeded)
        );
        assert_eq!(
            cache.put_in(7, b"b", value, PutOptions::default()).unwrap(),
            PutOutcome::Rejected(RejectReason::NamespaceCapacityExceeded)
        );
        assert_eq!(cache.get_in(7, b"b").unwrap(), None);
        assert_eq!(cache.remove_in(7, b"a").unwrap(), RemoveOutcome::Removed);
        let namespace = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(namespace.live_bytes, 0);
        assert_eq!(namespace.reserved_bytes, 0);
        cache.close().unwrap();
    }

    #[test]
    fn dirty_pending_refund_underflow_poisoned_instead_of_saturating() {
        let files = TestFiles::new("write-back-pending-underflow");
        let cache = config(&files, 8 * 1024)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(64 * 1024))
            .open()
            .unwrap();

        assert_eq!(
            cache
                .put_in(7, b"key", b"dirty", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.stats().memory_dirty_entries, 1);

        // Fault-inject a lost owner charge. The subsequent exact retirement
        // must expose the invariant violation and forbid a clean checkpoint;
        // silently saturating to zero could publish unrelated usage as valid.
        cache.inner.policy.namespaces().reset_live_bytes();
        assert!(matches!(
            cache.remove_in(7, b"key"),
            Err(CacheError::CorruptMetadata(
                "dirty pending usage exceeded namespace live usage"
            ))
        ));
        assert_eq!(cache.status(), CacheStatus::Poisoned);
        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
    }

    #[test]
    fn write_back_keeps_exact_pending_charge_across_direct_fallback() {
        let files = TestFiles::new("write-back-direct-fallback");
        let value = vec![0x5a; 2048];
        let cache = config(&files, 8 * 1024)
            .with_memory_shards(1)
            .with_small_object_max(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(64 * 1024))
            .open()
            .unwrap();
        let region = &cache.inner.disk.region;

        region.force_direct_append_padding_for_test();
        assert_eq!(
            cache
                .put_in(7, b"key", &value, PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        let pending = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap()
            .live_bytes;
        region.set_direct_append_padding_for_test(false);
        assert_eq!(cache.remove_in(7, b"key").unwrap(), RemoveOutcome::Removed);
        assert_eq!(
            cache
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            0,
            "dirty removal must refund the originally committed direct upper bound"
        );

        region.force_direct_append_padding_for_test();
        cache
            .put_in(7, b"key", &value, PutOptions::default())
            .unwrap();
        region.set_direct_append_padding_for_test(false);
        cache.flush().unwrap();
        let actual = region
            .candidate_record_bytes_in(7, b"key")
            .unwrap()
            .unwrap() as u64;
        assert!(actual < pending);
        assert_eq!(
            cache
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            actual,
            "demotion clone must reconcile the stored pending charge to the actual receipt"
        );

        region.force_direct_append_padding_for_test();
        cache
            .put_in(7, b"key", b"replacement", PutOptions::default())
            .unwrap();
        region.set_direct_append_padding_for_test(false);
        assert_eq!(cache.remove_in(7, b"key").unwrap(), RemoveOutcome::Removed);
        let namespace = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(namespace.live_bytes, 0);
        assert_eq!(namespace.reserved_bytes, 0);
        cache.close().unwrap();
    }

    #[test]
    fn cancelled_async_dirty_expiry_finishes_fence_and_refunds_usage() {
        let files = TestFiles::new("write-back-expiry-cancel");
        let cache_config = config(&files, 8 * 1024)
            .with_memory_shards(1)
            .with_write_mode(HybridWriteMode::WriteBack)
            .with_async_queue_depths(1, 1)
            .with_async_workers(1, 1)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(64 * 1024));
        let cache = cache_config.clone().open().unwrap();
        cache
            .put_in(7, b"key", b"old-on-disk", PutOptions::default())
            .unwrap();
        cache.flush().unwrap();

        let expires_at = now_unix_ms().saturating_add(1_000);
        assert_eq!(
            cache
                .put_in(
                    7,
                    b"key",
                    b"new-dirty",
                    PutOptions {
                        expires_at_unix_ms: Some(expires_at),
                    },
                )
                .unwrap(),
            PutOutcome::Stored
        );
        while now_unix_ms() < expires_at {
            thread::yield_now();
        }

        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let observer_entered = Arc::clone(&entered);
        let observer_release = Arc::clone(&release);
        *lock_mutex(&cache.inner.dirty_expiry_observer) = Some(Arc::new(move || {
            observer_entered.wait();
            observer_release.wait();
        }));

        let asynchronous = cache.async_handle().unwrap();
        let lookup = asynchronous.get_in(7, b"key");
        entered.wait();
        assert_eq!(lookup.cancel(), crate::CancelOutcome::TooLate);
        release.wait();
        assert_eq!(lookup.wait().unwrap(), None);

        // The expiry CAS committed before the destructive L1 transfer, so
        // cancellation is too late and the task must finish its lower-tier
        // tombstone plus both exact usage refunds.
        assert_eq!(cache.get_in(7, b"key").unwrap(), None);
        let namespace = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(namespace.live_bytes, 0);
        assert_eq!(namespace.reserved_bytes, 0);
        cache.flush().unwrap();
        drop(asynchronous);
        cache.close().unwrap();

        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.get_in(7, b"key").unwrap(), None);
        assert_eq!(
            reopened
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
                .live_bytes,
            0
        );
        reopened.close().unwrap();
    }

    #[test]
    fn bucket_expiry_scan_cancel_and_reopen_keep_exact_namespace_usage() {
        let files = TestFiles::new("bucket-expiry-accounting");
        let entry_bytes = bucket_namespace_usage_for_lengths(7, 8, HYBRID_VALUE_HEADER_SIZE + 1)
            .unwrap()
            .live_bytes;
        let cache_config = config(&files, 8 * 1024)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(entry_bytes * 2));
        let layout = hybrid_layout_fingerprint(&cache_config.diagnostics().unwrap());
        let cache = cache_config.clone().open().unwrap();

        let mut keys = Vec::new();
        let mut target_bucket = None;
        for index in 0..10_000_u64 {
            let key = format!("{index:08}").into_bytes();
            let bucket = cache.inner.disk.bucket.bucket_id_for(7, &key);
            if target_bucket.is_none_or(|target| target == bucket) {
                target_bucket = Some(bucket);
                keys.push(key);
                if keys.len() == 3 {
                    break;
                }
            }
        }
        assert_eq!(keys.len(), 3);
        let expires_at = now_unix_ms().saturating_add(60_000);
        assert_eq!(
            cache
                .put_in(7, &keys[0], b"l", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache
                .put_in(
                    7,
                    &keys[1],
                    b"e",
                    PutOptions {
                        expires_at_unix_ms: Some(expires_at),
                    },
                )
                .unwrap(),
            PutOutcome::Stored
        );
        cache.flush().unwrap();
        cache.close().unwrap();

        // Force the bounded startup scan with a clean but incompatible usage
        // namespace set. It must count the expired entry because that identity
        // is still present in the physical page.
        let (manifest, _) = HybridManifest::open_with_journal_capacity(
            &files.manifest,
            layout,
            DEFAULT_JOURNAL_CAPACITY,
        )
        .unwrap();
        manifest
            .publish_clean_with_usage(&[NamespaceUsage {
                namespace: 0,
                live_bytes: 0,
            }])
            .unwrap();
        manifest.close().unwrap();

        let cache = cache_config.clone().open().unwrap();
        cache
            .inner
            .disk
            .bucket
            .set_now_unix_ms_for_test(expires_at.saturating_add(1));
        let namespace = || {
            cache
                .policy_snapshot()
                .unwrap()
                .namespaces
                .into_iter()
                .find(|snapshot| snapshot.namespace == 7)
                .unwrap()
        };
        assert!(cache.stats().open.bucket_usage_scanned);
        assert_eq!(namespace().live_bytes, entry_bytes * 2);

        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let observer_entered = Arc::clone(&entered);
        let observer_release = Arc::clone(&release);
        cache
            .inner
            .disk
            .bucket
            .set_expiry_cleanup_observer_for_test(Arc::new(move || {
                observer_entered.wait();
                observer_release.wait();
            }));
        let asynchronous = cache.async_handle().unwrap();
        let lookup = asynchronous.get_in(7, &keys[1]);
        entered.wait();
        assert_eq!(lookup.cancel(), crate::CancelOutcome::TooLate);
        release.wait();
        assert_eq!(lookup.wait().unwrap(), None);
        assert_eq!(namespace().live_bytes, entry_bytes);
        assert_eq!(namespace().reserved_bytes, 0);

        // The exact retirement frees enough quota for another object in the
        // same page; the live neighbour must remain accounted and readable.
        assert_eq!(
            cache
                .put_in(7, &keys[2], b"n", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(namespace().live_bytes, entry_bytes * 2);
        drop(asynchronous);
        cache.flush().unwrap();
        cache.close().unwrap();

        let reopened = cache_config.open().unwrap();
        let usage = reopened
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(usage.live_bytes, entry_bytes * 2);
        assert_eq!(reopened.get_in(7, &keys[0]).unwrap(), Some(b"l".to_vec()));
        assert_eq!(reopened.get_in(7, &keys[1]).unwrap(), None);
        assert_eq!(reopened.get_in(7, &keys[2]).unwrap(), Some(b"n".to_vec()));
        reopened.close().unwrap();
    }

    #[test]
    fn bucket_publish_and_namespace_commit_share_the_page_linearization_point() {
        let files = TestFiles::new("bucket-usage-linearization");
        let value = vec![0x5a; 2_500];
        let entry_bytes =
            bucket_namespace_usage_for_lengths(7, 8, HYBRID_VALUE_HEADER_SIZE + value.len())
                .unwrap()
                .live_bytes;
        let cache = config(&files, 8 * 1024)
            .with_small_object_max(3_500)
            .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(entry_bytes * 4))
            .open()
            .unwrap();

        let first = b"00000000".to_vec();
        let bucket = cache.inner.disk.bucket.bucket_id_for(7, &first);
        let first_ordering = hybrid_hash(7, &first) as usize & (cache.inner.ordering.len() - 1);
        let second = (1..100_000_u64)
            .map(|index| format!("{index:08}").into_bytes())
            .find(|key| {
                cache.inner.disk.bucket.bucket_id_for(7, key) == bucket
                    && (hybrid_hash(7, key) as usize & (cache.inner.ordering.len() - 1))
                        != first_ordering
            })
            .expect("test needs two same-page keys on distinct ordering stripes");

        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let observer_entered = Arc::clone(&entered);
        let observer_release = Arc::clone(&release);
        cache
            .inner
            .disk
            .bucket
            .set_managed_put_commit_observer_for_test(Arc::new(move || {
                observer_entered.wait();
                observer_release.wait();
            }));

        let first_cache = cache.clone();
        let first_value = value.clone();
        let first_key = first.clone();
        let first_put = thread::spawn(move || {
            first_cache.put_in(7, first_key, first_value, PutOptions::default())
        });
        entered.wait();

        let second_cache = cache.clone();
        let second_value = value.clone();
        let second_key = second.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let second_put = thread::spawn(move || {
            started_tx.send(()).unwrap();
            done_tx
                .send(second_cache.put_in(7, second_key, second_value, PutOptions::default()))
                .unwrap();
        });
        started_rx.recv().unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(50)).is_err());
        release.wait();
        assert_eq!(first_put.join().unwrap().unwrap(), PutOutcome::Stored);
        assert_eq!(done_rx.recv().unwrap().unwrap(), PutOutcome::Stored);
        second_put.join().unwrap();

        let mut physical = 0_u64;
        cache
            .inner
            .disk
            .bucket
            .scan_live_entries(|usage| {
                if usage.namespace == 7 {
                    physical = physical.checked_add(usage.live_bytes).unwrap();
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(physical, entry_bytes);
        let accounted = cache
            .policy_snapshot()
            .unwrap()
            .namespaces
            .into_iter()
            .find(|snapshot| snapshot.namespace == 7)
            .unwrap();
        assert_eq!(accounted.live_bytes, physical);
        assert_eq!(accounted.reserved_bytes, 0);
        cache.close().unwrap();
    }

    #[test]
    fn unflushed_write_back_update_cannot_revive_the_previous_disk_value() {
        let files = TestFiles::new("write-back-crash-fence");
        let large_old = vec![1_u8; 2048];
        let large_new = vec![2_u8; 2048];
        let write_through = config(&files, 8 * 1024);
        let cache = write_through.clone().open().unwrap();
        cache
            .put(b"key", &large_old, PutOptions::default())
            .unwrap();
        cache.flush().unwrap();
        cache.close().unwrap();

        let write_back = write_through.with_write_mode(HybridWriteMode::WriteBack);
        let cache = write_back.clone().open().unwrap();
        cache
            .put(b"key", &large_new, PutOptions::default())
            .unwrap();
        assert_eq!(cache.get(b"key").unwrap(), Some(large_new));
        drop(cache);

        let reopened = write_back.open().unwrap();
        assert_ne!(reopened.get(b"key").unwrap(), Some(large_old));
        reopened.close().unwrap();
    }

    #[test]
    fn unified_policy_rejects_before_journal_and_second_hit_admits() {
        let files = TestFiles::new("policy-admission");
        let cache = config(&files, 8 * 1024)
            .with_admission_mode(AdmissionMode::SecondHit)
            .with_namespace(NamespaceConfig::new(7))
            .open()
            .unwrap();
        let journal_before = cache.stats().journal_used_bytes;
        assert_eq!(
            cache
                .put_in(8, b"key", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Rejected(RejectReason::NamespaceNotConfigured)
        );
        assert_eq!(cache.stats().journal_used_bytes, journal_before);
        assert_eq!(
            cache
                .put_in(7, b"key", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Rejected(RejectReason::AdmissionFiltered)
        );
        assert_eq!(cache.stats().journal_used_bytes, journal_before);
        assert_eq!(
            cache
                .put_in(7, b"key", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        cache.clear().unwrap();
        let after_clear = cache.stats().journal_used_bytes;
        assert_eq!(
            cache
                .put_in(7, b"after-clear", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Rejected(RejectReason::AdmissionFiltered)
        );
        assert_eq!(cache.stats().journal_used_bytes, after_clear);
        assert_eq!(
            cache
                .put_in(7, b"after-clear", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.status(), CacheStatus::Healthy);
        cache.close().unwrap();
    }

    #[test]
    fn daily_write_and_device_health_policy_cover_both_routes() {
        let daily_files = TestFiles::new("policy-daily");
        let daily = config(&daily_files, 8 * 1024)
            .with_daily_host_write_budget(1)
            .open()
            .unwrap();
        assert_eq!(
            daily.put(b"small", b"v", PutOptions::default()).unwrap(),
            PutOutcome::Rejected(RejectReason::DailyWriteBudgetExceeded)
        );
        assert_eq!(daily.status(), CacheStatus::Healthy);
        daily.close().unwrap();

        let health_files = TestFiles::new("policy-health");
        let health = config(&health_files, 8 * 1024)
            .with_device_health_policy(DeviceHealthPolicy::RejectPutsOnCritical)
            .open()
            .unwrap();
        health.observe_nvme_health(NvmeHealthSample {
            critical_warning: 1,
            available_spare_percent: 100,
            available_spare_threshold_percent: 10,
            ..NvmeHealthSample::default()
        });
        assert_eq!(
            health
                .put(b"large", vec![3_u8; 2048], PutOptions::default())
                .unwrap(),
            PutOutcome::Rejected(RejectReason::DeviceHealth)
        );
        assert_eq!(health.status(), CacheStatus::Healthy);
        health.close().unwrap();
    }

    #[test]
    fn one_degraded_disk_tier_preserves_memory_and_other_tier_reads() {
        let files = TestFiles::new("tier-local-degradation");
        let cache = config(&files, 300).with_memory_shards(1).open().unwrap();
        let large = vec![8_u8; 2048];
        cache.put(b"large", &large, PutOptions::default()).unwrap();
        cache
            .put(b"small", b"memory", PutOptions::default())
            .unwrap();

        cache.inner.disk.bucket.force_miss_only_for_test();
        let health = cache.health_snapshot();
        assert_eq!(health.overall, CacheStatus::MissOnly);
        assert_eq!(health.bucket, CacheStatus::MissOnly);
        assert_eq!(health.region, CacheStatus::Healthy);
        assert!(!health.mutations_available);
        assert_eq!(cache.get(b"large").unwrap(), Some(large));
        assert_eq!(cache.get(b"small").unwrap(), Some(b"memory".to_vec()));
        assert_eq!(
            cache.lookup(b"unknown").unwrap(),
            HybridLookupOutcome::Miss(HybridMissKind::Recovering)
        );
        assert!(matches!(
            cache.put(b"new", b"value", PutOptions::default()),
            Err(CacheError::Poisoned)
        ));
        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
    }

    #[test]
    fn hybrid_metrics_cover_end_to_end_results_and_fixed_cardinality_export() {
        let files = TestFiles::new("hybrid-telemetry");
        let cache = config(&files, 8 * 1024).open().unwrap();
        assert_eq!(cache.get(b"key").unwrap(), None);
        assert_eq!(
            cache.put(b"key", b"value", PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(cache.remove(b"key").unwrap(), RemoveOutcome::Removed);
        cache.flush().unwrap();

        let metrics = cache.metrics_snapshot();
        let get = &metrics.operations[CacheOperation::Get as usize];
        assert_eq!(get.result_count(RequestResultClass::Hit), 1);
        assert_eq!(get.result_count(RequestResultClass::Miss), 1);
        assert_eq!(get.latency.count, 2);
        assert_eq!(
            metrics.operations[CacheOperation::Put as usize]
                .result_count(RequestResultClass::Stored),
            1
        );
        let openmetrics = metrics.to_openmetrics();
        assert!(
            openmetrics
                .contains("cache_rs_hybrid_requests_total{operation=\"get\",result=\"hit\"} 1")
        );
        assert!(openmetrics.contains(
            "cache_rs_hybrid_request_errors_total{operation=\"close\",class=\"closed\"} 0"
        ));
        assert!(openmetrics.contains("cache_rs_hybrid_async_facade_active 0"));
        assert!(openmetrics.ends_with("# EOF\n"));
        cache.close().unwrap();
    }

    #[test]
    fn nested_region_policy_is_rejected_during_diagnostics() {
        let files = TestFiles::new("nested-policy");
        let bucket =
            BucketCacheConfig::new(&files.bucket, 16 * 4096).with_memory_budget(4 * 1024 * 1024);
        let region = CacheConfig::new(&files.region, 8 * 64 * 1024)
            .with_region_size(64 * 1024)
            .with_index_slots(1024)
            .with_memory_budget(16 * 1024 * 1024)
            .with_admission_mode(AdmissionMode::SecondHit);
        let invalid = HybridCacheConfig::new(8 * 1024, bucket, region)
            .with_memory_shards(4)
            .with_manifest_path(&files.manifest);
        assert!(matches!(
            invalid.diagnostics(),
            Err(CacheError::InvalidConfig(_))
        ));
        assert!(!files.manifest.exists());
    }

    #[test]
    fn namespace_count_is_bounded_by_the_manifest_checkpoint() {
        let files = TestFiles::new("namespace-checkpoint-limit");
        let mut invalid = config(&files, 8 * 1024);
        for namespace in 1..=MAX_MANIFEST_NAMESPACE_USAGES as u32 {
            invalid = invalid.with_namespace(NamespaceConfig::new(namespace));
        }
        assert!(matches!(
            invalid.diagnostics(),
            Err(CacheError::InvalidConfig(message))
                if message.contains("manifest checkpoint limit")
        ));
        assert!(!files.manifest.exists());
    }
}
