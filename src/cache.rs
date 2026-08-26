//! Public performance-first RAM + Region SSD hybrid cache.
//!
//! Static configuration defines the on-disk identity and clean recovery image.
//! Runtime configuration may change on every open without invalidating that
//! image. Ordinary mutations are never made durable; only `close_warm` publishes
//! state that a later process may recover.

use std::fmt;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::expiry::ExpiryClock;
use crate::index::{MAX_INDEX_SLOTS, MAX_PACKED_REGION_COUNT, MAX_PACKED_REGION_SIZE};
use crate::index_storage::canonical_index_partition_ranges;
use crate::recovery::{
    DataGeometry, DataSuperblock, KEY_HASH_ALGORITHM_XXH3_64, PersistentId,
    RECOVERY_IMAGE_INDEX_OFFSET, STATE_FILE_SIZE, recovery_image_index_len,
};
use crate::region::{FileRegionBackend, RegionFiles, SystemRegionFileSystem};
use crate::region_layout::{MAX_REGION_SETS, RegionLayout, RegionSetAllocation, RegionSetConfig};
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
const DEFAULT_HASH_SEED: u64 = 0x6a09_e667_f3bc_c909;
const DEFAULT_SHARDS: u32 = 4;
const MIN_INDEX_SLOTS: usize = 8;
const MAX_SHARDS: u32 = 256;

pub type Result<T> = std::io::Result<T>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticConfig {
    capacity_bytes: u64,
    region_size: u64,
    index_slots: usize,
    write_shards: u32,
    hash_seed: u64,
    region_sets: Vec<RegionSetConfig>,
}

impl StaticConfig {
    pub fn new(capacity_bytes: u64) -> Self {
        let expected_entries = capacity_bytes / (64 * 1024);
        let index_slots = expected_entries
            .saturating_mul(5)
            .saturating_add(3)
            .saturating_div(4)
            .clamp(MIN_INDEX_SLOTS as u64, MAX_INDEX_SLOTS as u64)
            as usize;
        Self {
            capacity_bytes,
            region_size: DEFAULT_REGION_SIZE,
            index_slots,
            write_shards: DEFAULT_SHARDS,
            hash_seed: DEFAULT_HASH_SEED,
            region_sets: Vec::new(),
        }
    }

    pub fn with_region_size(mut self, bytes: u64) -> Self {
        self.region_size = bytes;
        self
    }

    pub fn with_index_slots(mut self, slots: usize) -> Self {
        self.index_slots = slots;
        self
    }

    pub fn with_expected_entries(mut self, entries: usize) -> Self {
        self.index_slots = entries
            .saturating_mul(5)
            .saturating_add(3)
            .saturating_div(4)
            .max(MIN_INDEX_SLOTS);
        self
    }

    /// Sets the number of independent append paths in the on-disk layout.
    pub fn with_write_shards(mut self, shards: u32) -> Self {
        self.write_shards = shards;
        self
    }

    pub fn with_hash_seed(mut self, seed: u64) -> Self {
        self.hash_seed = seed;
        self
    }

    /// Replaces the physical RegionSet layout.
    ///
    /// Capacity weights divide the fixed Region count. Append shards are
    /// distributed evenly and deterministically across the configured sets.
    /// RegionSet zero is required and receives every namespace not explicitly
    /// listed by another set.
    /// Every set needs one active Region per assigned shard plus one spare.
    /// The complete layout is static recovery identity.
    pub fn with_region_sets(mut self, sets: impl IntoIterator<Item = RegionSetConfig>) -> Self {
        self.region_sets = sets.into_iter().take(MAX_REGION_SETS + 1).collect();
        self
    }

    pub const fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub const fn region_size(&self) -> u64 {
        self.region_size
    }

    pub const fn index_slots(&self) -> usize {
        self.index_slots
    }

    pub const fn write_shards(&self) -> u32 {
        self.write_shards
    }

    pub fn region_sets(&self) -> &[RegionSetConfig] {
        &self.region_sets
    }

    /// Validates the complete static geometry without opening cache files.
    pub fn validate(&self) -> Result<()> {
        self.geometry().map(|_| ())
    }

    /// Resolves capacity weights and append-shard assignment without opening
    /// cache files. Results are ordered by RegionSet id.
    pub fn region_set_allocations(&self) -> Result<Vec<RegionSetAllocation>> {
        let geometry = self.geometry()?;
        self.region_layout(geometry)?
            .allocations(geometry.region_size)
    }

    /// Maximum cache-owned logical disk bytes after a successful open.
    ///
    /// The bound includes the fixed data and state files plus both the current
    /// clean image and the temporary image used for atomic warm publication.
    /// Filesystem metadata and block-allocation granularity are outside it.
    pub fn peak_disk_bytes(&self) -> Result<u64> {
        let geometry = self.geometry()?;
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
        geometry
            .data_file_len
            .checked_add(STATE_FILE_SIZE as u64)
            .and_then(|bytes| bytes.checked_add(image_len.checked_mul(2)?))
            .ok_or_else(|| invalid_config("peak disk usage overflow"))
    }

    fn geometry(&self) -> Result<DataGeometry> {
        if self.region_size == 0
            || self.region_size > MAX_PACKED_REGION_SIZE
            || !self.region_size.is_multiple_of(4096)
            || self.capacity_bytes == 0
            || !self.capacity_bytes.is_multiple_of(self.region_size)
        {
            return Err(invalid_config(
                "capacity must be a non-zero multiple of an aligned representable Region size",
            ));
        }
        let region_count = u32::try_from(self.capacity_bytes / self.region_size)
            .ok()
            .filter(|count| *count <= MAX_PACKED_REGION_COUNT)
            .ok_or_else(|| invalid_config("cache Region count is not representable"))?;
        if self.write_shards == 0
            || self.write_shards > MAX_SHARDS
            || region_count <= self.write_shards
        {
            return Err(invalid_config(
                "write shards must be in 1..=256 with one additional Region for rotation",
            ));
        }
        if !(MIN_INDEX_SLOTS..=MAX_INDEX_SLOTS).contains(&self.index_slots) {
            return Err(invalid_config("index slots must be in 8..=268435456"));
        }
        let data_file_len = DataGeometry::expected_file_len(self.region_size, region_count)
            .ok_or_else(|| invalid_config("cache data length overflow"))?;
        let geometry = DataGeometry {
            data_file_len,
            region_size: self.region_size,
            region_count,
        };
        if !geometry.is_valid() {
            return Err(invalid_config("cache data geometry is not representable"));
        }
        self.region_layout(geometry)?;
        Ok(geometry)
    }

    fn region_layout(&self, geometry: DataGeometry) -> Result<RegionLayout> {
        RegionLayout::build(geometry.region_count, self.write_shards, &self.region_sets)
    }

    fn fingerprint(&self, geometry: DataGeometry, layout: &RegionLayout) -> u64 {
        self.fingerprint_with_hash_algorithm(
            geometry,
            layout,
            u64::from(KEY_HASH_ALGORITHM_XXH3_64),
        )
    }

    fn fingerprint_with_hash_algorithm(
        &self,
        geometry: DataGeometry,
        layout: &RegionLayout,
        hash_algorithm_id: u64,
    ) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for value in [
            geometry.data_file_len,
            geometry.region_size,
            u64::from(geometry.region_count),
            self.index_slots as u64,
            u64::from(self.write_shards),
            self.hash_seed,
            hash_algorithm_id,
        ] {
            for byte in value.to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100_0000_01b3);
            }
        }
        if !layout.uses_default_single_set() {
            hash_identity_word(&mut hash, 0x7265_6769_6f6e_7365);
            hash_identity_word(&mut hash, layout.sets().len() as u64);
            hash_identity_word(&mut hash, 0);
            for set in layout.sets() {
                for value in [
                    u64::from(set.id.get()),
                    u64::from(set.first_region),
                    u64::from(set.region_count),
                    u64::from(set.first_shard),
                    u64::from(set.shard_count),
                ] {
                    hash_identity_word(&mut hash, value);
                }
            }
            hash_identity_word(&mut hash, layout.routes().len() as u64);
            for &(namespace_id, set_index) in layout.routes() {
                hash_identity_word(&mut hash, u64::from(namespace_id));
                hash_identity_word(&mut hash, u64::from(layout.sets()[set_index].id.get()));
            }
        }
        hash.max(1)
    }
}

fn hash_identity_word(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(0x100_0000_01b3);
    }
}

#[derive(Clone, Debug)]
pub struct HybridCacheConfig {
    path: PathBuf,
    static_config: StaticConfig,
    runtime_config: RuntimeConfig,
}

impl HybridCacheConfig {
    pub fn new(path: impl AsRef<Path>, capacity_bytes: u64) -> Self {
        Self::from_static(path, StaticConfig::new(capacity_bytes))
    }

    /// Creates a cache builder from a complete static configuration.
    pub fn from_static(path: impl AsRef<Path>, static_config: StaticConfig) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            static_config,
            runtime_config: RuntimeConfig::default(),
        }
    }

    pub fn with_runtime_config(mut self, config: RuntimeConfig) -> Self {
        self.runtime_config = config;
        self
    }

    pub fn open(self) -> Result<HybridCache> {
        let geometry = self.static_config.geometry()?;
        let region_layout = self.static_config.region_layout(geometry)?;
        let logical_disk_peak_bytes = self.static_config.peak_disk_bytes()?;
        let runtime_config = self.runtime_config;
        runtime_config.validate_memory_plan(
            geometry,
            self.static_config.index_slots,
            self.static_config.write_shards as usize,
            region_layout.memory_bytes(),
        )?;
        let format_data = DataSuperblock {
            generation: 1,
            cache_uuid: next_persistent_id(),
            data_identity: next_persistent_id(),
            geometry,
            hash_seed: self.static_config.hash_seed,
            config_fingerprint: self.static_config.fingerprint(geometry, &region_layout),
        };
        let files = RegionFiles::new(
            &self.path,
            sidecar_path(&self.path, ".state"),
            sidecar_path(&self.path, ".image"),
        );
        let backend = FileRegionBackend::new_with_region_layout(
            files,
            format_data,
            region_layout,
            runtime_config,
        );
        let store = RegionStore::open(self.static_config.index_slots, backend)?;
        Ok(HybridCache {
            store,
            logical_disk_peak_bytes,
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

pub struct HybridCache {
    store: RegionStore<FileRegionBackend<SystemRegionFileSystem>>,
    logical_disk_peak_bytes: u64,
}

impl fmt::Debug for HybridCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HybridCache")
            .field("startup", &self.startup_mode())
            .finish_non_exhaustive()
    }
}

impl HybridCache {
    pub fn startup_mode(&self) -> StartupMode {
        self.store.startup()
    }

    /// Stores a value and returns its monotonic mutation sequence.
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<u64> {
        self.put_in(0, key, value)
    }

    /// Stores a value in a logical namespace and returns its mutation sequence.
    pub fn put_in(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<u64> {
        self.put_until(namespace, key, value, 0)
    }

    /// Stores a value until an absolute Unix timestamp and returns its mutation
    /// sequence.
    ///
    /// An expiration of zero means that the value does not expire.
    pub fn put_until(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        expires_at_unix_ms: u64,
    ) -> Result<u64> {
        self.store
            .put_value(namespace, key.as_ref(), value.as_ref(), expires_at_unix_ms)
    }

    /// Looks up a value in L1 and then L2.
    ///
    /// An L2 index miss returns directly. An L2 candidate reserves one
    /// immediately available engine slot, allocates one exact-size aligned
    /// buffer, performs one record read, and revalidates the record identity
    /// before returning. Internal allocation or I/O pressure fails
    /// open as a cache miss.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Value>> {
        self.get_in(0, key)
    }

    /// Looks up a value in a logical namespace.
    ///
    /// The system clock is read lazily only when a matching value carries an
    /// expiration timestamp. This method has the same L2 semantics as
    /// [`Self::get`].
    pub fn get_in(&self, namespace: u32, key: impl AsRef<[u8]>) -> Result<Option<Value>> {
        self.get_with_clock(namespace, key.as_ref(), ExpiryClock::System)
    }

    /// Looks up a value using an explicit Unix timestamp in milliseconds.
    ///
    /// This is useful for deterministic tests and for callers that amortize a
    /// coarse clock sample across a batch of cache reads.
    pub fn get_in_at(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
        now_unix_ms: u64,
    ) -> Result<Option<Value>> {
        self.get_with_clock(namespace, key.as_ref(), ExpiryClock::Fixed(now_unix_ms))
    }

    fn get_with_clock(
        &self,
        namespace: u32,
        key: &[u8],
        clock: ExpiryClock,
    ) -> Result<Option<Value>> {
        self.store
            .get_value(namespace, key, clock)
            .map(|value| value.map(|inner| Value { inner }))
    }

    pub fn drain(&self) -> Result<()> {
        self.store.drain()
    }

    pub fn flush(&self) -> Result<()> {
        self.store.flush()
    }

    pub fn snapshot(&self) -> Result<CacheSnapshot> {
        let mut snapshot = self.store.snapshot()?;
        snapshot.logical_disk_peak_bytes = self.logical_disk_peak_bytes;
        Ok(snapshot)
    }

    /// Samples queue, I/O, and per-RegionSet state in addition to the regular
    /// cache summary. This is intended for periodic diagnostics rather than a
    /// request hot path because it briefly locks and scans Region metadata.
    pub fn detailed_snapshot(&self) -> Result<DetailedCacheSnapshot> {
        let mut snapshot = self.store.detailed_snapshot()?;
        snapshot.summary.logical_disk_peak_bytes = self.logical_disk_peak_bytes;
        Ok(snapshot)
    }

    pub fn close_fast(mut self) -> Result<()> {
        self.store.close_fast()
    }

    pub fn close_warm(mut self) -> Result<()> {
        self.store.close_warm()
    }
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
        let layout = config.region_layout(geometry).unwrap();
        let algorithm = u64::from(KEY_HASH_ALGORITHM_XXH3_64);
        assert_eq!(
            config.fingerprint(geometry, &layout),
            config.fingerprint_with_hash_algorithm(geometry, &layout, algorithm)
        );
        assert_ne!(
            config.fingerprint(geometry, &layout),
            config.fingerprint_with_hash_algorithm(geometry, &layout, algorithm + 1)
        );
    }

    #[test]
    fn minimum_static_region_geometry_is_encodable() {
        let config = StaticConfig::new(5 * 4096)
            .with_region_size(4096)
            .with_write_shards(4);
        config.validate().unwrap();
        let geometry = config.geometry().unwrap();
        let layout = config.region_layout(geometry).unwrap();
        let data = DataSuperblock {
            generation: 1,
            cache_uuid: PersistentId::from_bytes([1; 16]).unwrap(),
            data_identity: PersistentId::from_bytes([2; 16]).unwrap(),
            geometry,
            hash_seed: config.hash_seed,
            config_fingerprint: config.fingerprint(geometry, &layout),
        };
        assert!(data.encode().is_ok());
    }

    #[test]
    fn static_fingerprint_binds_region_assignment() {
        let base = StaticConfig::new(10 * DEFAULT_REGION_SIZE).with_region_sets([
            RegionSetConfig::new(0).with_weight(1),
            RegionSetConfig::new(2).with_weight(1).with_namespaces([7]),
        ]);
        let changed = base.clone().with_region_sets([
            RegionSetConfig::new(0).with_weight(1),
            RegionSetConfig::new(2)
                .with_weight(1)
                .with_namespaces([7, 9]),
        ]);
        let geometry = base.geometry().unwrap();
        let base_layout = base.region_layout(geometry).unwrap();
        let changed_layout = changed.region_layout(geometry).unwrap();

        assert_ne!(
            base.fingerprint(geometry, &base_layout),
            changed.fingerprint(geometry, &changed_layout)
        );
    }

    #[test]
    fn equivalent_default_region_layouts_share_fingerprint() {
        let implicit = StaticConfig::new(5 * DEFAULT_REGION_SIZE);
        let explicit = implicit
            .clone()
            .with_region_sets([RegionSetConfig::new(0).with_weight(99).with_namespaces([7])]);
        let geometry = implicit.geometry().unwrap();
        let implicit_layout = implicit.region_layout(geometry).unwrap();
        let explicit_layout = explicit.region_layout(geometry).unwrap();

        assert!(explicit_layout.routes().is_empty());
        assert_eq!(
            implicit.fingerprint(geometry, &implicit_layout),
            explicit.fingerprint(geometry, &explicit_layout)
        );
    }

    #[test]
    fn resolved_region_set_allocations_expose_rounding_and_shards() {
        let config = StaticConfig::new(10 * DEFAULT_REGION_SIZE)
            .with_write_shards(4)
            .with_region_sets([
                RegionSetConfig::new(0).with_weight(1),
                RegionSetConfig::new(2).with_weight(3),
            ]);

        let allocations = config.region_set_allocations().unwrap();
        assert_eq!(allocations.len(), 2);
        assert_eq!(allocations[0].id.get(), 0);
        assert_eq!(allocations[0].region_count, 3);
        assert_eq!(allocations[0].capacity_bytes, 3 * DEFAULT_REGION_SIZE);
        assert_eq!(allocations[0].append_shard_count, 2);
        assert_eq!(allocations[1].id.get(), 2);
        assert_eq!(allocations[1].region_count, 7);
        assert_eq!(allocations[1].append_shard_count, 2);
    }

    #[test]
    fn region_set_builder_stops_after_the_invalid_sentinel() {
        let config = StaticConfig::new(5 * DEFAULT_REGION_SIZE)
            .with_region_sets((0..MAX_REGION_SETS + 2).map(|id| RegionSetConfig::new(id as u16)));

        assert_eq!(config.region_sets().len(), MAX_REGION_SETS + 1);
        assert_eq!(
            config.validate().unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
}
