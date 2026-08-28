//! Public performance-first RAM + Region SSD hybrid cache.
//!
//! Static configuration defines the on-disk identity and clean recovery image.
//! Runtime configuration is selected on every open; changing the append-shard
//! topology discards an incompatible clean image. Ordinary mutations are never
//! made durable; only `close_warm` publishes state that a later process may
//! recover.

use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::index::{MAX_INDEX_SLOTS, MAX_PACKED_REGION_COUNT, MAX_PACKED_REGION_SIZE};
use crate::index_storage::canonical_index_partition_ranges;
use crate::recovery::{
    DataGeometry, DataSuperblock, KEY_HASH_ALGORITHM_XXH3_64, PersistentId,
    RECOVERY_IMAGE_INDEX_OFFSET, STATE_FILE_SIZE, recovery_image_index_len,
};
use crate::region::{FileRegionBackend, RegionFiles, SystemRegionFileSystem};
use crate::region_metadata::{
    REGION_METADATA_PAGE_SIZE, REGION_METADATA_PARTITIONS_PER_PAGE,
    REGION_METADATA_REGIONS_PER_PAGE,
};
use crate::region_runtime::HybridValueRead;
use crate::region_store::RegionStore;
#[cfg(test)]
use crate::runtime_config::DEFAULT_L1_SHARDS;
use crate::runtime_config::RuntimeConfig;
use crate::snapshot::{CacheSnapshot, DetailedCacheSnapshot, StartupMode};

const DEFAULT_REGION_SIZE: u64 = 32 * 1024 * 1024;
const DEFAULT_EXPECTED_ENTRY_BYTES: u64 = 16 * 1024;
const DEFAULT_HASH_SEED: u64 = 0x6a09_e667_f3bc_c909;
const MIN_INDEX_SLOTS: usize = 8;
const MAX_DATA_FILES: usize = 64;
const STATIC_FINGERPRINT_SCHEMA: u64 = 4;

pub type Result<T> = std::io::Result<T>;

/// Persistent L2 geometry and fixed-index sizing.
///
/// These values define the static disk identity. A clean image created with a
/// different static configuration is discarded and the cache starts empty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticConfig {
    capacity_bytes: u64,
    region_size_bytes: u64,
    index_slots: usize,
    hash_seed: u64,
}

impl StaticConfig {
    /// Creates static L2 geometry and sizes its index assuming an average
    /// 16 KiB live entry.
    ///
    /// `capacity_bytes` is the total Region extent, excluding the data
    /// superblock, state, and clean-image files. The default Region size is
    /// 32 MiB. A default plan above 4 TiB exceeds the supported index size and
    /// fails validation; use [`Self::with_expected_entries`] when a larger
    /// average entry permits a smaller explicit index. Use
    /// [`Self::peak_disk_bytes`] for the cache-owned logical disk bound.
    pub fn new(capacity_bytes: u64) -> Self {
        let expected_entries = capacity_bytes / DEFAULT_EXPECTED_ENTRY_BYTES;
        let index_slots = usize::try_from(
            expected_entries
                .saturating_mul(2)
                .max(MIN_INDEX_SLOTS as u64),
        )
        .unwrap_or(usize::MAX);
        Self {
            capacity_bytes,
            region_size_bytes: DEFAULT_REGION_SIZE,
            index_slots,
            hash_seed: DEFAULT_HASH_SEED,
        }
    }

    /// Sets the Region size in bytes.
    ///
    /// It must be a 4 KiB multiple no larger than 32 MiB, and the L2 capacity
    /// must be an exact multiple containing at least two Regions. A complete
    /// encoded record must fit in one Region. Every append shard eagerly owns
    /// two Region-sized staging buffers.
    pub fn with_region_size_bytes(mut self, bytes: u64) -> Self {
        self.region_size_bytes = bytes;
        self
    }

    /// Sizes the fixed L2 index for the expected number of simultaneously live
    /// keys.
    ///
    /// This is not a lifetime-write count. The resulting index uses roughly
    /// two physical buckets per expected live key and is part of the static disk
    /// identity.
    pub fn with_expected_entries(mut self, entries: usize) -> Self {
        self.index_slots = entries.saturating_mul(2).max(MIN_INDEX_SLOTS);
        self
    }

    /// Returns the total Region extent in bytes.
    pub const fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Returns the Region size in bytes.
    pub const fn region_size_bytes(&self) -> u64 {
        self.region_size_bytes
    }

    /// Returns the fixed number of physical L2 index slots.
    pub const fn index_slots(&self) -> usize {
        self.index_slots
    }

    /// Validates the static physical geometry without opening cache files.
    pub fn validate(&self) -> Result<()> {
        self.geometry().map(|_| ())
    }

    /// Maximum cache-owned logical disk bytes for one data file after open.
    ///
    /// The bound includes the fixed data and state files plus both the current
    /// clean image and the temporary image used for atomic warm publication.
    /// Filesystem metadata and block-allocation granularity are outside it.
    pub fn peak_disk_bytes(&self) -> Result<u64> {
        self.peak_disk_bytes_for_data_files(1)
    }

    /// Maximum cache-owned logical disk bytes for a striped data-file count.
    ///
    /// Each data file owns one 4 KiB superblock. State and recovery images
    /// remain beside the primary data file and are counted once.
    pub fn peak_disk_bytes_for_data_files(&self, data_files: usize) -> Result<u64> {
        let geometry = self.geometry()?;
        if data_files == 0
            || data_files > MAX_DATA_FILES
            || data_files > geometry.region_count as usize
        {
            return Err(invalid_config(
                "data file count must be in 1..=min(64, Region count)",
            ));
        }
        let index_slots = u64::try_from(self.index_slots)
            .map_err(|_| invalid_config("index capacity does not fit u64"))?;
        let index_len = recovery_image_index_len(index_slots)
            .ok_or_else(|| invalid_config("index image length overflow"))?;
        let partition_count = u64::try_from(
            canonical_index_partition_ranges(self.index_slots)
                .map_err(|_| invalid_config("index partition layout is invalid"))?
                .len(),
        )
        .map_err(|_| invalid_config("index partition count does not fit u64"))?;
        let region_pages =
            u64::from(geometry.region_count).div_ceil(REGION_METADATA_REGIONS_PER_PAGE as u64);
        let partition_pages = partition_count.div_ceil(REGION_METADATA_PARTITIONS_PER_PAGE as u64);
        let metadata_len = 1_u64
            .checked_add(region_pages)
            .and_then(|pages| pages.checked_add(partition_pages))
            .and_then(|pages| pages.checked_mul(REGION_METADATA_PAGE_SIZE as u64))
            .ok_or_else(|| invalid_config("Region metadata length overflow"))?;
        let image_len = RECOVERY_IMAGE_INDEX_OFFSET
            .checked_add(index_len)
            .and_then(|bytes| bytes.checked_add(metadata_len))
            .ok_or_else(|| invalid_config("recovery image length overflow"))?;
        let data_file_headers = u64::try_from(data_files)
            .ok()
            .and_then(|count| count.checked_mul(crate::recovery::RECOVERY_PAGE_SIZE as u64))
            .ok_or_else(|| invalid_config("data-file header size overflow"))?;
        self.capacity_bytes
            .checked_add(data_file_headers)
            .and_then(|bytes| bytes.checked_add(STATE_FILE_SIZE as u64))
            .and_then(|bytes| bytes.checked_add(image_len.checked_mul(2)?))
            .ok_or_else(|| invalid_config("peak disk usage overflow"))
    }

    fn geometry(&self) -> Result<DataGeometry> {
        if self.region_size_bytes == 0
            || self.region_size_bytes > MAX_PACKED_REGION_SIZE
            || !self.region_size_bytes.is_multiple_of(4096)
            || self.capacity_bytes == 0
            || !self.capacity_bytes.is_multiple_of(self.region_size_bytes)
        {
            return Err(invalid_config(
                "capacity must be a non-zero multiple of an aligned representable Region size",
            ));
        }
        let region_count = u32::try_from(self.capacity_bytes / self.region_size_bytes)
            .ok()
            .filter(|count| *count <= MAX_PACKED_REGION_COUNT)
            .ok_or_else(|| invalid_config("cache Region count is not representable"))?;
        if region_count < 2 {
            return Err(invalid_config("cache requires at least two Regions"));
        }
        if !(MIN_INDEX_SLOTS..=MAX_INDEX_SLOTS).contains(&self.index_slots) {
            return Err(invalid_config("index slots must be in 8..=536870912"));
        }
        let data_file_len = DataGeometry::expected_file_len(self.region_size_bytes, region_count)
            .ok_or_else(|| invalid_config("cache data length overflow"))?;
        let geometry = DataGeometry {
            data_file_len,
            region_size: self.region_size_bytes,
            region_count,
        };
        if !geometry.is_valid() {
            return Err(invalid_config("cache data geometry is not representable"));
        }
        Ok(geometry)
    }

    fn fingerprint(&self, geometry: DataGeometry, data_files: u32) -> u64 {
        self.fingerprint_with_hash_algorithm(
            geometry,
            data_files,
            u64::from(KEY_HASH_ALGORITHM_XXH3_64),
        )
    }

    fn fingerprint_with_hash_algorithm(
        &self,
        geometry: DataGeometry,
        data_files: u32,
        hash_algorithm_id: u64,
    ) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for value in [
            STATIC_FINGERPRINT_SCHEMA,
            geometry.data_file_len,
            geometry.region_size,
            u64::from(geometry.region_count),
            self.index_slots as u64,
            self.hash_seed,
            u64::from(data_files),
            hash_algorithm_id,
        ] {
            for byte in value.to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100_0000_01b3);
            }
        }
        hash.max(1)
    }
}

/// Builder that combines a cache path, static geometry, runtime tuning, and a
/// Tokio runtime binding before opening a [`Cache`].
#[derive(Clone, Debug)]
pub struct CacheBuilder {
    path: PathBuf,
    additional_data_paths: Vec<PathBuf>,
    static_config: StaticConfig,
    runtime_config: RuntimeConfig,
    tokio_handle: Option<tokio::runtime::Handle>,
}

impl CacheBuilder {
    /// Creates a builder using default static and runtime tuning.
    ///
    /// The L2 index is sized for 16 KiB live entries. Use
    /// [`Self::from_static`] when Region or index geometry must be explicit.
    pub fn new(path: impl AsRef<Path>, capacity_bytes: u64) -> Self {
        Self::from_static(path, StaticConfig::new(capacity_bytes))
    }

    /// Creates a cache builder from a complete static configuration.
    pub fn from_static(path: impl AsRef<Path>, static_config: StaticConfig) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            additional_data_paths: Vec::new(),
            static_config,
            runtime_config: RuntimeConfig::default(),
            tokio_handle: None,
        }
    }

    /// Stripes global Regions across these additional data-file paths.
    ///
    /// The path passed to [`Self::new`] or [`Self::from_static`] remains device
    /// zero and owns the state and recovery-image sidecars. Capacity is total
    /// across at most 64 data files. Path order is part of the static disk
    /// identity. Multi-device operation currently requires the POSIX I/O
    /// engine.
    pub fn with_additional_data_paths<I, P>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        self.additional_data_paths
            .extend(paths.into_iter().map(|path| path.as_ref().to_path_buf()));
        self
    }

    /// Replaces the process-local runtime tuning used by [`Self::open`].
    pub fn with_runtime_config(mut self, config: RuntimeConfig) -> Self {
        self.runtime_config = config;
        self
    }

    /// Uses a specific Tokio runtime for blocking lifecycle work and L2 read
    /// deadlines. The runtime must have time enabled and outlive the cache.
    pub fn with_tokio_handle(mut self, handle: tokio::runtime::Handle) -> Self {
        self.tokio_handle = Some(handle);
        self
    }

    /// Opens the cache on Tokio's blocking pool because recovery and file setup
    /// use blocking filesystem operations. Without an explicit handle, this
    /// captures the current Tokio runtime when the future is first polled.
    pub async fn open(self) -> Result<Cache> {
        let tokio_handle = match self.tokio_handle.clone() {
            Some(handle) => handle,
            None => tokio::runtime::Handle::try_current().map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
            })?,
        };
        let cache_handle = tokio_handle.clone();
        let started = Instant::now();
        tokio_handle
            .spawn_blocking(move || self.open_blocking(cache_handle, started))
            .await
            .map_err(|error| blocking_task_error("cache open", error))?
    }

    fn open_blocking(
        self,
        tokio_handle: tokio::runtime::Handle,
        started: Instant,
    ) -> Result<Cache> {
        let path = self.path.clone();
        let capacity_bytes = self.static_config.capacity_bytes;
        let index_slots = self.static_config.index_slots;
        let data_files = self.additional_data_paths.len().saturating_add(1);
        let result = self.open_blocking_inner(tokio_handle);
        match &result {
            Ok(cache) => {
                let startup = cache.startup_mode();
                log::info!(
                    target: "cache2::lifecycle",
                    event = "cache_opened",
                    path:% = path.display(),
                    startup = startup_name(startup),
                    index_backing = index_backing_name(startup),
                    capacity_bytes,
                    index_slots,
                    data_files,
                    elapsed_us = elapsed_micros(started.elapsed());
                    "cache opened"
                );
            }
            Err(error) => log::error!(
                target: "cache2::lifecycle",
                event = "cache_open_failed",
                path:% = path.display(),
                capacity_bytes,
                index_slots,
                data_files,
                elapsed_us = elapsed_micros(started.elapsed()),
                error:% = error;
                "cache open failed"
            ),
        }
        result
    }

    fn open_blocking_inner(self, tokio_handle: tokio::runtime::Handle) -> Result<Cache> {
        let geometry = self.static_config.geometry()?;
        let data_file_count = self
            .additional_data_paths
            .len()
            .checked_add(1)
            .ok_or_else(|| invalid_config("data file count overflow"))?;
        let device_count = u32::try_from(data_file_count)
            .ok()
            .filter(|count| data_file_count <= MAX_DATA_FILES && *count <= geometry.region_count)
            .ok_or_else(|| {
                invalid_config("data file count must be in 1..=min(64, Region count)")
            })?;
        let logical_disk_peak_bytes = self
            .static_config
            .peak_disk_bytes_for_data_files(data_file_count)?;
        let runtime_config = self.runtime_config;
        runtime_config.validate()?;
        if geometry.region_count <= runtime_config.append_shards {
            return Err(invalid_config(
                "append shards require one Active Region each plus one spare Region",
            ));
        }
        runtime_config.validate_memory_plan(
            geometry,
            self.static_config.index_slots,
            runtime_config.append_shards as usize,
        )?;
        let format_base = DataSuperblock {
            generation: 1,
            cache_uuid: next_persistent_id(),
            data_identity: next_persistent_id(),
            geometry,
            hash_seed: self.static_config.hash_seed,
            config_fingerprint: self.static_config.fingerprint(geometry, device_count),
            device_count,
            device_id: 0,
            device_file_len: geometry
                .expected_device_file_len(device_count, 0)
                .expect("validated device zero has an extent"),
        };
        let mut format_data = Vec::new();
        format_data
            .try_reserve_exact(data_file_count)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "cannot allocate data-device superblocks",
                )
            })?;
        for device_id in 0..device_count {
            format_data.push(
                format_base
                    .for_device(device_id, device_count)
                    .ok_or_else(|| {
                        invalid_config("data device is not representable by the Region geometry")
                    })?,
            );
        }
        let files = RegionFiles::new(
            &self.path,
            sidecar_path(&self.path, ".state"),
            sidecar_path(&self.path, ".image"),
        )
        .with_additional_data(self.additional_data_paths);
        let backend = FileRegionBackend::new_with_data_set_configs(
            files,
            format_data,
            runtime_config.append_shards,
            runtime_config,
        );
        let store = RegionStore::open(self.static_config.index_slots, backend)?;
        Ok(Cache {
            store,
            path: self.path,
            logical_disk_peak_bytes,
            tokio_handle,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheTier {
    L1,
    L2,
}

pub struct Value {
    inner: HybridValueRead,
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.value()
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Value {
    /// Returns the tier that served this lookup.
    ///
    /// A Region hit may already be backed by its promoted L1 value.
    pub const fn tier(&self) -> CacheTier {
        if self.inner.is_l1() {
            CacheTier::L1
        } else {
            CacheTier::L2
        }
    }
}

/// An open cache bound to one Tokio runtime.
///
/// Prefer [`Self::close_fast`] or [`Self::close_warm`] for async shutdown.
/// Dropping an open cache is a synchronous fast close and may block the thread
/// performing the drop while accepted work is fenced.
pub struct Cache {
    store: RegionStore<FileRegionBackend<SystemRegionFileSystem>>,
    path: PathBuf,
    logical_disk_peak_bytes: u64,
    tokio_handle: tokio::runtime::Handle,
}

impl fmt::Debug for Cache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Cache")
            .field("startup", &self.startup_mode())
            .finish_non_exhaustive()
    }
}

impl Cache {
    pub fn startup_mode(&self) -> StartupMode {
        self.store.startup()
    }

    /// Stores a value and returns its monotonic mutation sequence.
    ///
    /// Keys are limited to 4 KiB. The complete encoded record must fit in one
    /// Region; values have no smaller independent limit.
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<u64> {
        self.store.put_value(key.as_ref(), value.as_ref())
    }

    /// Deletes a key and returns its monotonic mutation sequence.
    ///
    /// Delete performs a bounded in-memory index removal and best-effort L1
    /// cleanup. It does not append a physical Region record.
    /// Keys are limited to 4 KiB.
    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<u64> {
        self.store.delete_value(key.as_ref())
    }

    /// Looks up a value in L1 and then L2.
    ///
    /// An L2 index miss returns directly. An L2 candidate reserves one
    /// immediately available engine slot, allocates one alignment-rounded
    /// buffer, performs one size-class-bounded record read, and validates the
    /// exact envelope locally. Internal allocation or I/O pressure fails open
    /// as a cache miss.
    /// A key longer than 4 KiB is also a miss.
    pub async fn get(&self, key: impl AsRef<[u8]> + Send) -> Result<Option<Value>> {
        self.store
            .get_value_async(key.as_ref(), &self.tokio_handle)
            .await
            .map(|value| value.map(|inner| Value { inner }))
    }

    pub async fn drain(&self) -> Result<()> {
        self.store.drain_async().await
    }

    /// Returns a lock-free operational snapshot. Activity and I/O counters are
    /// cumulative for this open and are populated only when statistics are
    /// enabled; health and resource gauges are always available.
    pub fn snapshot(&self) -> Result<CacheSnapshot> {
        let mut snapshot = self.store.snapshot()?;
        snapshot.logical_disk_peak_bytes = self.logical_disk_peak_bytes;
        Ok(snapshot)
    }

    /// Samples L1, index, queue, I/O, and Region state in addition to the
    /// regular cache summary. This is intended for periodic diagnostics rather
    /// than a request hot path because it briefly reads every configured L1
    /// shard and index partition and scans Region metadata. It never scans
    /// index slots or Region data.
    pub fn detailed_snapshot(&self) -> Result<DetailedCacheSnapshot> {
        let mut snapshot = self.store.detailed_snapshot()?;
        snapshot.summary.logical_disk_peak_bytes = self.logical_disk_peak_bytes;
        Ok(snapshot)
    }

    /// Stops without publishing a recovery image on Tokio's blocking pool.
    /// Once called, the close continues even if the returned future is dropped.
    pub fn close_fast(mut self) -> impl Future<Output = Result<()>> + Send + 'static {
        let tokio_handle = self.tokio_handle.clone();
        let started = Instant::now();
        let close = tokio_handle.spawn_blocking(move || {
            let result = self.store.close_fast();
            log_cache_close(&self.path, "fast", started.elapsed(), &result);
            result
        });
        async move {
            close
                .await
                .map_err(|error| blocking_task_error("fast close", error))?
        }
    }

    /// Publishes a clean recovery image on Tokio's blocking pool. Once called,
    /// the close continues even if the returned future is dropped.
    pub fn close_warm(mut self) -> impl Future<Output = Result<()>> + Send + 'static {
        let tokio_handle = self.tokio_handle.clone();
        let started = Instant::now();
        let close = tokio_handle.spawn_blocking(move || {
            let result = self.store.close_warm();
            log_cache_close(&self.path, "warm", started.elapsed(), &result);
            result
        });
        async move {
            close
                .await
                .map_err(|error| blocking_task_error("warm close", error))?
        }
    }
}

fn startup_name(startup: StartupMode) -> &'static str {
    match startup {
        StartupMode::Cold => "cold",
        StartupMode::Warm => "warm",
    }
}

fn index_backing_name(startup: StartupMode) -> &'static str {
    match startup {
        StartupMode::Cold => "anonymous",
        StartupMode::Warm => "file_private_mmap",
    }
}

fn elapsed_micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

fn log_cache_close(path: &Path, mode: &'static str, elapsed: Duration, result: &Result<()>) {
    match result {
        Ok(()) => log::info!(
            target: "cache2::lifecycle",
            event = "cache_closed",
            path:% = path.display(),
            mode,
            elapsed_us = elapsed_micros(elapsed);
            "cache closed"
        ),
        Err(error) => log::error!(
            target: "cache2::lifecycle",
            event = "cache_close_failed",
            path:% = path.display(),
            mode,
            elapsed_us = elapsed_micros(elapsed),
            error:% = error;
            "cache close failed"
        ),
    }
}

fn blocking_task_error(operation: &'static str, error: tokio::task::JoinError) -> std::io::Error {
    std::io::Error::other(format!("{operation} task failed: {error}"))
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn invalid_config(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

fn next_persistent_id() -> PersistentId {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let counter = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut bytes = now.to_le_bytes();
    let mix = counter ^ u64::from(std::process::id()).rotate_left(32);
    for (target, source) in bytes[8..].iter_mut().zip(mix.to_le_bytes()) {
        *target ^= source;
    }
    PersistentId::from_bytes(bytes).unwrap_or_else(|| {
        PersistentId::from_bytes(counter.max(1).to_le_bytes().repeat(2).try_into().unwrap())
            .expect("non-zero counter produces a cache identity")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_cache_and_values_are_send_and_sync() {
        assert_send_sync::<Cache>();
        assert_send_sync::<Value>();
    }

    #[test]
    fn l1_shards_are_runtime_tunable() {
        let default = RuntimeConfig::default();
        assert_eq!(default.l1_shards(), DEFAULT_L1_SHARDS);
        assert_eq!(default.with_l1_shards(7).l1_shards(), 7);
    }

    #[test]
    fn static_fingerprint_binds_the_hash_algorithm() {
        let config = StaticConfig::new(5 * DEFAULT_REGION_SIZE);
        let geometry = config.geometry().unwrap();
        let algorithm = u64::from(KEY_HASH_ALGORITHM_XXH3_64);
        assert_eq!(
            config.fingerprint(geometry, 1),
            config.fingerprint_with_hash_algorithm(geometry, 1, algorithm)
        );
        assert_ne!(
            config.fingerprint(geometry, 1),
            config.fingerprint_with_hash_algorithm(geometry, 1, algorithm + 1)
        );
        assert_ne!(
            config.fingerprint(geometry, 1),
            config.fingerprint(geometry, 2)
        );
    }

    #[test]
    fn minimum_static_region_geometry_is_encodable() {
        let config = StaticConfig::new(5 * 4096).with_region_size_bytes(4096);
        config.validate().unwrap();
        let geometry = config.geometry().unwrap();
        let data = DataSuperblock {
            generation: 1,
            cache_uuid: PersistentId::from_bytes([1; 16]).unwrap(),
            data_identity: PersistentId::from_bytes([2; 16]).unwrap(),
            geometry,
            hash_seed: config.hash_seed,
            config_fingerprint: config.fingerprint(geometry, 1),
            device_count: 1,
            device_id: 0,
            device_file_len: geometry.data_file_len,
        };
        assert!(data.encode().is_ok());
    }

    #[test]
    fn four_tib_default_index_matches_sixteen_kib_entries() {
        let config = StaticConfig::new(4_u64 << 40);

        assert_eq!(config.index_slots(), 536_870_912);
        config.validate().unwrap();
    }

    #[test]
    fn striped_peak_disk_bound_counts_one_superblock_per_data_file() {
        let config = StaticConfig::new(5 * DEFAULT_REGION_SIZE);
        let single = config.peak_disk_bytes().unwrap();
        let striped = config.peak_disk_bytes_for_data_files(3).unwrap();

        assert_eq!(
            striped - single,
            2 * crate::recovery::RECOVERY_PAGE_SIZE as u64
        );
        assert!(config.peak_disk_bytes_for_data_files(0).is_err());
        assert!(config.peak_disk_bytes_for_data_files(6).is_err());
        assert!(
            config
                .peak_disk_bytes_for_data_files(MAX_DATA_FILES + 1)
                .is_err()
        );
    }

    #[test]
    fn default_index_sizing_does_not_silently_clamp_above_four_tib() {
        let capacity = (4_u64 << 40) + DEFAULT_REGION_SIZE;
        let implicit = StaticConfig::new(capacity);

        assert!(implicit.index_slots() > MAX_INDEX_SLOTS);
        assert!(implicit.validate().is_err());

        StaticConfig::new(capacity)
            .with_expected_entries(MAX_INDEX_SLOTS / 2)
            .validate()
            .unwrap();
    }
}
