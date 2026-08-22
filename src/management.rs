//! Read-only inspection and verification for an offline cache file.
//!
//! These entry points deliberately do not call `DiskCache::open`: opening the
//! runtime cache may create, checkpoint, or safely reset disposable cache
//! state. Management reads instead hold the same non-blocking exclusive file
//! lock as a writer and never issue a write or durability operation.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use crate::cache::MAX_APPEND_LANES;
use crate::checkpoint::{
    CHECKPOINT_DIRECTORY_SIZE, CHECKPOINT_INDEX_ENTRY_SIZE, CHECKPOINT_REGION_SNAPSHOT_SIZE,
    CHECKPOINT_SLOT_COUNT, CHECKPOINT_SLOT_HEADER_SIZE, CheckpointCodecError, CheckpointDirectory,
    CheckpointPayloadDecoder, CheckpointRegionSnapshot, CheckpointSlotHeader,
    decode_checkpoint_index_entry, decode_region_snapshot, padded_payload_len,
};
use crate::checksum::Crc32c;
use crate::format::{
    FORMAT_VERSION, RECORD_HEADER_SIZE, REGION_HEADER_SIZE, RecordCodec, RecordHeader, RecordKind,
    RegionHeader, RegionState, SUPERBLOCK_A_OFFSET, SUPERBLOCK_AREA_SIZE, SUPERBLOCK_B_OFFSET,
    SUPERBLOCK_SIZE, Superblock, SuperblockProbe,
};
use crate::index::{MAX_REGION_ID, MAX_REGION_OFFSET};

const NAMESPACE_KEY_PREFIX_SIZE: usize = size_of::<u32>();
const NAMESPACE_HASH_DOMAIN: &[u8] = b"cache-rs/ns/v1\0";
const VERIFY_BUFFER_SIZE: usize = 64 * 1024;
const CHECKPOINT_LANE_VERSION: u16 = 3;

#[cfg(target_os = "linux")]
const SAFE_READ_OPEN_FLAGS: i32 = 0o400_000 | 0o4_000; // O_NOFOLLOW | O_NONBLOCK
#[cfg(any(target_os = "macos", target_os = "ios"))]
const SAFE_READ_OPEN_FLAGS: i32 = 0x0100 | 0x0004; // O_NOFOLLOW | O_NONBLOCK

/// Maximum number of detailed verification issues retained in a report.
///
/// `issues_total` continues increasing after this bound is reached.
pub const MAX_REPORTED_VERIFY_ISSUES: usize = 32;

/// Failure to access an offline cache file.
#[non_exhaustive]
#[derive(Debug)]
pub enum ManagementError {
    Io(io::Error),
    Locked,
    UnsupportedTarget(&'static str),
    Allocation,
}

impl fmt::Display for ManagementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "cache management I/O error: {error}"),
            Self::Locked => formatter.write_str("cache file is open by another instance"),
            Self::UnsupportedTarget(message) => formatter.write_str(message),
            Self::Allocation => formatter.write_str("verification metadata cannot be allocated"),
        }
    }
}

impl std::error::Error for ManagementError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ManagementError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type ManagementResult<T> = std::result::Result<T, ManagementError>;

/// Classification of the bytes at the cache path.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheFileKind {
    Empty,
    InterruptedV1,
    FormatV1,
    CorruptV1,
    Unsupported(u16),
    Unrecognized,
}

impl CacheFileKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::InterruptedV1 => "interrupted_v1",
            Self::FormatV1 => "format_v1",
            Self::CorruptV1 => "corrupt_v1",
            Self::Unsupported(_) => "unsupported",
            Self::Unrecognized => "unrecognized",
        }
    }
}

/// Result of probing one redundant Superblock page.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SuperblockState {
    Missing,
    Empty,
    InterruptedV1,
    ValidV1,
    CorruptV1,
    Unsupported,
    Unrecognized,
    Truncated,
}

impl SuperblockState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Empty => "empty",
            Self::InterruptedV1 => "interrupted_v1",
            Self::ValidV1 => "valid_v1",
            Self::CorruptV1 => "corrupt_v1",
            Self::Unsupported => "unsupported",
            Self::Unrecognized => "unrecognized",
            Self::Truncated => "truncated",
        }
    }
}

/// Public fields from one Superblock copy. Values are populated only for a
/// checksum-valid Format V1 page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SuperblockSummary {
    pub slot: u8,
    pub state: SuperblockState,
    pub version: Option<u16>,
    pub generation: Option<u64>,
    pub clean: Option<bool>,
    pub region_size: Option<u64>,
    pub region_count: Option<u32>,
    pub epoch: Option<u32>,
    pub epoch_start_seqno: Option<u64>,
    pub next_seqno: Option<u64>,
    pub hash_seed: Option<u64>,
}

impl SuperblockSummary {
    const fn missing(slot: u8) -> Self {
        Self {
            slot,
            state: SuperblockState::Missing,
            version: None,
            generation: None,
            clean: None,
            region_size: None,
            region_count: None,
            epoch: None,
            epoch_start_seqno: None,
            next_seqno: None,
            hash_seed: None,
        }
    }

    fn valid(slot: u8, superblock: Superblock) -> Self {
        Self {
            slot,
            state: SuperblockState::ValidV1,
            version: Some(FORMAT_VERSION),
            generation: Some(superblock.generation),
            clean: Some(superblock.clean),
            region_size: Some(superblock.region_size),
            region_count: Some(superblock.region_count),
            epoch: Some(superblock.epoch),
            epoch_start_seqno: Some(superblock.epoch_start_seqno),
            next_seqno: Some(superblock.next_seqno),
            hash_seed: Some(superblock.hash_seed),
        }
    }
}

/// Aggregate Region Header information. `record_extent_bytes` is the sum of
/// persisted `used - header_size` cursors; it does not imply record CRCs have
/// been checked.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegionSummary {
    pub expected: u32,
    pub valid_headers: u32,
    pub invalid_headers: u32,
    pub truncated_headers: u32,
    pub free: u32,
    pub active: u32,
    pub sealed: u32,
    pub record_extent_bytes: u64,
}

/// State of the optional checkpoint directory page.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointDirectoryState {
    Absent,
    Valid,
    Invalid,
    LayoutMismatch,
    Truncated,
}

impl CheckpointDirectoryState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Valid => "valid",
            Self::Invalid => "invalid",
            Self::LayoutMismatch => "layout_mismatch",
            Self::Truncated => "truncated",
        }
    }
}

/// State of one checkpoint commit-header page. A valid header is not reported
/// as a verified payload until `verify_cache_file` has streamed its payload.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointSlotState {
    Absent,
    HeaderValid,
    Invalid,
    Truncated,
}

impl CheckpointSlotState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::HeaderValid => "header_valid",
            Self::Invalid => "invalid",
            Self::Truncated => "truncated",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointSlotSummary {
    pub slot: u8,
    pub state: CheckpointSlotState,
    pub version: Option<u16>,
    pub generation: Option<u64>,
    pub superblock_generation: Option<u64>,
    pub entry_count: Option<u32>,
    pub payload_len: Option<u64>,
    pub matches_selected_superblock: bool,
}

impl CheckpointSlotSummary {
    const fn absent(slot: u8) -> Self {
        Self {
            slot,
            state: CheckpointSlotState::Absent,
            version: None,
            generation: None,
            superblock_generation: None,
            entry_count: None,
            payload_len: None,
            matches_selected_superblock: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointSummary {
    pub directory_state: CheckpointDirectoryState,
    pub slot_size: Option<u64>,
    pub expected_file_len: Option<u64>,
    /// Newest header whose lineage matches the selected Superblock. Inspect
    /// does not claim that this slot's payload checksum is valid.
    pub selected_header_slot: Option<u8>,
    pub slots: [CheckpointSlotSummary; CHECKPOINT_SLOT_COUNT],
}

impl Default for CheckpointSummary {
    fn default() -> Self {
        Self {
            directory_state: CheckpointDirectoryState::Absent,
            slot_size: None,
            expected_file_len: None,
            selected_header_slot: None,
            slots: [
                CheckpointSlotSummary::absent(0),
                CheckpointSlotSummary::absent(1),
            ],
        }
    }
}

/// Read-only metadata report. No record payload checksum is read by inspect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectReport {
    pub file_len: u64,
    pub kind: CacheFileKind,
    pub selected_superblock: Option<u8>,
    pub data_file_len: Option<u64>,
    pub superblocks: [SuperblockSummary; 2],
    pub regions: RegionSummary,
    pub checkpoint: CheckpointSummary,
}

impl InspectReport {
    pub fn selected(&self) -> Option<&SuperblockSummary> {
        let slot = usize::from(self.selected_superblock?);
        self.superblocks.get(slot)
    }
}

/// Verification subsystem associated with an issue.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerifyComponent {
    Superblock,
    Layout,
    RegionHeader,
    RecordHeader,
    RecordPayload,
    CheckpointDirectory,
    CheckpointHeader,
    CheckpointPayload,
}

impl VerifyComponent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Superblock => "superblock",
            Self::Layout => "layout",
            Self::RegionHeader => "region_header",
            Self::RecordHeader => "record_header",
            Self::RecordPayload => "record_payload",
            Self::CheckpointDirectory => "checkpoint_directory",
            Self::CheckpointHeader => "checkpoint_header",
            Self::CheckpointPayload => "checkpoint_payload",
        }
    }
}

/// One bounded, structured verification diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifyIssue {
    pub component: VerifyComponent,
    pub offset: u64,
    pub region_id: Option<u32>,
    pub checkpoint_slot: Option<u8>,
    pub message: &'static str,
}

/// Expected action of the current runtime open protocol. This is diagnostic;
/// verify itself never performs the action.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReopenDisposition {
    Refused,
    CleanCheckpoint,
    CleanFullScan,
    DirtyIncremental,
    SafeEmpty,
}

impl ReopenDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Refused => "refused",
            Self::CleanCheckpoint => "clean_checkpoint",
            Self::CleanFullScan => "clean_full_scan",
            Self::DirtyIncremental => "dirty_incremental",
            Self::SafeEmpty => "safe_empty",
        }
    }
}

/// Full read-only verification report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifyReport {
    pub inspect: InspectReport,
    /// True only when every present structure and record passed verification.
    pub valid: bool,
    /// True when the current runtime protocol can open the file without ever
    /// returning an unverified value. This may be true with `SafeEmpty`.
    pub safe_to_open: bool,
    pub reopen_disposition: ReopenDisposition,
    pub regions_verified: u64,
    pub records_verified: u64,
    pub values_verified: u64,
    pub tombstones_verified: u64,
    pub record_bytes_verified: u64,
    pub checkpoint_slots_verified: u64,
    pub selected_verified_checkpoint: Option<u8>,
    pub issues_total: u64,
    pub issues: Vec<VerifyIssue>,
}

impl VerifyReport {
    fn new(inspect: InspectReport) -> Self {
        Self {
            inspect,
            valid: true,
            safe_to_open: false,
            reopen_disposition: ReopenDisposition::Refused,
            regions_verified: 0,
            records_verified: 0,
            values_verified: 0,
            tombstones_verified: 0,
            record_bytes_verified: 0,
            checkpoint_slots_verified: 0,
            selected_verified_checkpoint: None,
            issues_total: 0,
            issues: Vec::new(),
        }
    }

    fn issue(&mut self, issue: VerifyIssue) {
        self.valid = false;
        self.issues_total = self.issues_total.saturating_add(1);
        if self.issues.len() < MAX_REPORTED_VERIFY_ISSUES {
            self.issues.push(issue);
        }
    }
}

/// Inspect an offline cache without reading record payloads.
pub fn inspect_cache_file(path: impl AsRef<Path>) -> ManagementResult<InspectReport> {
    let mut file = LockedReadFile::open(path.as_ref())?;
    Ok(inspect_locked(&mut file)?.report)
}

/// Stream all persisted records and checkpoint payloads in an offline cache.
///
/// The function uses a fixed 64 KiB data buffer. Its only input-sized
/// allocation is one `u64` per Region, bounded by the Format V1 Region limit,
/// and is used to detect duplicate creation sequence numbers.
pub fn verify_cache_file(path: impl AsRef<Path>) -> ManagementResult<VerifyReport> {
    let mut file = LockedReadFile::open(path.as_ref())?;
    let inspected = inspect_locked(&mut file)?;
    verify_locked(&mut file, inspected)
}

/// Offline summary of the fixed-bucket small-object file in a Hybrid cache.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BucketFileReport {
    pub file_len: u64,
    pub valid: bool,
    pub selected_superblock: Option<u8>,
    pub generation: Option<u64>,
    pub clean: Option<bool>,
    pub bucket_size_bytes: Option<u32>,
    pub bucket_count: Option<u64>,
    pub epoch: Option<u64>,
    pub expected_file_len: Option<u64>,
    pub buckets_verified: u64,
    pub current_epoch_buckets: u64,
    pub stale_epoch_buckets: u64,
    pub empty_buckets: u64,
    pub entries_verified: u64,
    pub invalid_buckets: u64,
}

/// Offline summary of the global Hybrid identity/version journal.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HybridManifestFileReport {
    pub file_len: u64,
    pub valid: bool,
    pub selected_slot: Option<u8>,
    pub generation: Option<u64>,
    /// Global identity embedded in every managed Hybrid value envelope.
    pub cache_id: Option<[u8; 16]>,
    /// Persisted fingerprint checked against the runtime disk-pair layout.
    pub layout_fingerprint: Option<u64>,
    pub clean: Option<bool>,
    pub version_epoch: Option<u64>,
    pub next_seqno: Option<u64>,
    pub journal_generation: Option<u64>,
    pub journal_capacity_bytes: Option<u64>,
    pub journal_valid_bytes: u64,
    pub journal_records: u64,
    pub journal_torn_tail: bool,
    pub recovery_required: bool,
}

/// One coherent, locked offline view of all three Hybrid persistence files.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HybridInspectReport {
    pub valid: bool,
    pub bucket: BucketFileReport,
    pub region: InspectReport,
    pub manifest: HybridManifestFileReport,
}

/// Full offline verification of Bucket pages, Region records/checkpoints, and
/// the Hybrid transition journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HybridVerifyReport {
    pub valid: bool,
    /// All three files are structurally safe for the runtime to *attempt* to
    /// open. This does not promise that a supplied runtime configuration has
    /// the same layout fingerprint, nor that recovery will avoid a safe clear.
    pub safe_to_open: bool,
    pub bucket: BucketFileReport,
    pub region: VerifyReport,
    pub manifest: HybridManifestFileReport,
}

/// Inspect a closed Hybrid cache without writing recovery or format state.
///
/// Locks are acquired in runtime order (manifest, Bucket, Region) and retained
/// until all three reports have been captured, preventing a mixed-generation
/// sidecar view.
pub fn inspect_hybrid_cache_files(
    bucket_path: impl AsRef<Path>,
    region_path: impl AsRef<Path>,
    manifest_path: impl AsRef<Path>,
) -> ManagementResult<HybridInspectReport> {
    let mut manifest = LockedReadFile::open(manifest_path.as_ref())?;
    let mut bucket = LockedReadFile::open(bucket_path.as_ref())?;
    let mut region = LockedReadFile::open(region_path.as_ref())?;
    let manifest = inspect_hybrid_manifest(&mut manifest, false)?;
    let bucket = inspect_bucket_file(&mut bucket, false)?;
    let region = inspect_locked(&mut region)?.report;
    let valid = bucket.valid && manifest.valid && region_inspect_valid(&region);
    Ok(HybridInspectReport {
        valid,
        bucket,
        region,
        manifest,
    })
}

/// Verify every persisted component of a closed Hybrid cache.
pub fn verify_hybrid_cache_files(
    bucket_path: impl AsRef<Path>,
    region_path: impl AsRef<Path>,
    manifest_path: impl AsRef<Path>,
) -> ManagementResult<HybridVerifyReport> {
    let mut manifest = LockedReadFile::open(manifest_path.as_ref())?;
    let mut bucket = LockedReadFile::open(bucket_path.as_ref())?;
    let mut region = LockedReadFile::open(region_path.as_ref())?;
    let manifest = inspect_hybrid_manifest(&mut manifest, true)?;
    let bucket = inspect_bucket_file(&mut bucket, true)?;
    let inspected = inspect_locked(&mut region)?;
    let region = verify_locked(&mut region, inspected)?;
    let valid = bucket.valid && manifest.valid && region.valid;
    Ok(HybridVerifyReport {
        valid,
        safe_to_open: valid && region.safe_to_open,
        bucket,
        region,
        manifest,
    })
}

const BUCKET_SUPERBLOCK_SIZE: usize = 4 * 1024;
const BUCKET_DATA_OFFSET: u64 = (2 * BUCKET_SUPERBLOCK_SIZE) as u64;
const BUCKET_SUPERBLOCK_MAGIC: [u8; 8] = *b"CRBKT001";
const BUCKET_PAGE_MAGIC: [u8; 8] = *b"CRBUCKT1";
const BUCKET_FORMAT_VERSION: u16 = 1;
const BUCKET_HEADER_BYTES: usize = 64;
const BUCKET_ENTRY_HEADER_BYTES: usize = 32;
const BUCKET_ENTRY_ALIGNMENT: usize = 8;

#[derive(Clone, Copy)]
struct OfflineBucketSuperblock {
    generation: u64,
    bucket_size: u32,
    bucket_count: u64,
    epoch: u64,
    clean: bool,
}

fn inspect_bucket_file(
    file: &mut LockedReadFile,
    verify_pages: bool,
) -> ManagementResult<BucketFileReport> {
    let mut report = BucketFileReport {
        file_len: file.len,
        ..BucketFileReport::default()
    };
    if file.len < BUCKET_DATA_OFFSET {
        return Ok(report);
    }
    let mut pages = [[0_u8; BUCKET_SUPERBLOCK_SIZE]; 2];
    let mut selected = None;
    for (slot, page) in pages.iter_mut().enumerate() {
        file.read_exact_at(page, (slot * BUCKET_SUPERBLOCK_SIZE) as u64)?;
        if let Some(superblock) = decode_bucket_superblock(page) {
            if selected.is_none_or(|(_, current): (usize, OfflineBucketSuperblock)| {
                superblock.generation > current.generation
            }) {
                selected = Some((slot, superblock));
            }
        }
    }
    let Some((slot, superblock)) = selected else {
        return Ok(report);
    };
    report.selected_superblock = Some(slot as u8);
    report.generation = Some(superblock.generation);
    report.clean = Some(superblock.clean);
    report.bucket_size_bytes = Some(superblock.bucket_size);
    report.bucket_count = Some(superblock.bucket_count);
    report.epoch = Some(superblock.epoch);
    let expected = BUCKET_DATA_OFFSET
        .checked_add(u64::from(superblock.bucket_size).saturating_mul(superblock.bucket_count));
    report.expected_file_len = expected;
    let layout_valid = superblock.generation != 0
        && superblock.epoch != 0
        && superblock.bucket_count != 0
        && (4 * 1024..=64 * 1024).contains(&(superblock.bucket_size as usize))
        && superblock.bucket_size.is_power_of_two()
        && expected == Some(file.len);
    if !layout_valid {
        return Ok(report);
    }
    report.valid = true;
    if !verify_pages {
        return Ok(report);
    }
    let page_len = superblock.bucket_size as usize;
    let mut page = Vec::new();
    page.try_reserve_exact(page_len)
        .map_err(|_| ManagementError::Allocation)?;
    page.resize(page_len, 0);
    for bucket_id in 0..superblock.bucket_count {
        let offset = BUCKET_DATA_OFFSET
            .checked_add(bucket_id.saturating_mul(u64::from(superblock.bucket_size)))
            .ok_or(ManagementError::Allocation)?;
        file.read_exact_at(&mut page, offset)?;
        report.buckets_verified = report.buckets_verified.saturating_add(1);
        match verify_bucket_page(&page, superblock.epoch) {
            BucketPageVerification::Empty => {
                report.empty_buckets = report.empty_buckets.saturating_add(1);
            }
            BucketPageVerification::Stale => {
                report.stale_epoch_buckets = report.stale_epoch_buckets.saturating_add(1);
            }
            BucketPageVerification::Current(entries) => {
                report.current_epoch_buckets = report.current_epoch_buckets.saturating_add(1);
                report.entries_verified = report.entries_verified.saturating_add(entries);
            }
            BucketPageVerification::Invalid => {
                report.invalid_buckets = report.invalid_buckets.saturating_add(1);
                report.valid = false;
            }
        }
    }
    Ok(report)
}

fn decode_bucket_superblock(page: &[u8]) -> Option<OfflineBucketSuperblock> {
    if page.len() != BUCKET_SUPERBLOCK_SIZE
        || page.get(..8)? != BUCKET_SUPERBLOCK_MAGIC
        || read_u16(page, 8)? != BUCKET_FORMAT_VERSION
        || !checksum_matches(page, BUCKET_SUPERBLOCK_SIZE - size_of::<u32>())
    {
        return None;
    }
    let clean = match *page.get(56)? {
        0 => false,
        1 => true,
        _ => return None,
    };
    Some(OfflineBucketSuperblock {
        generation: read_u64(page, 16)?,
        bucket_size: read_u32(page, 24)?,
        bucket_count: read_u64(page, 32)?,
        epoch: read_u64(page, 48)?,
        clean,
    })
}

enum BucketPageVerification {
    Empty,
    Stale,
    Current(u64),
    Invalid,
}

fn verify_bucket_page(page: &[u8], current_epoch: u64) -> BucketPageVerification {
    if page.iter().all(|byte| *byte == 0) {
        return BucketPageVerification::Empty;
    }
    if page.len() < BUCKET_HEADER_BYTES + size_of::<u32>()
        || page.get(..8) != Some(BUCKET_PAGE_MAGIC.as_slice())
        || read_u16(page, 8) != Some(BUCKET_FORMAT_VERSION)
        || !checksum_matches(page, page.len() - size_of::<u32>())
    {
        return BucketPageVerification::Invalid;
    }
    let Some(epoch) = read_u64(page, 24) else {
        return BucketPageVerification::Invalid;
    };
    if epoch != current_epoch {
        return BucketPageVerification::Stale;
    }
    let Some(entry_count) = read_u32(page, 32).map(|value| value as usize) else {
        return BucketPageVerification::Invalid;
    };
    let Some(used) = read_u32(page, 36).map(|value| value as usize) else {
        return BucketPageVerification::Invalid;
    };
    if used < BUCKET_HEADER_BYTES || used > page.len() - size_of::<u32>() {
        return BucketPageVerification::Invalid;
    }
    if entry_count > (used - BUCKET_HEADER_BYTES) / BUCKET_ENTRY_HEADER_BYTES {
        return BucketPageVerification::Invalid;
    }
    let mut cursor = BUCKET_HEADER_BYTES;
    for _ in 0..entry_count {
        let Some(key_len) = read_u16(page, cursor + 12).map(usize::from) else {
            return BucketPageVerification::Invalid;
        };
        if read_u16(page, cursor + 14) != Some(0) {
            return BucketPageVerification::Invalid;
        }
        let Some(value_len) = read_u32(page, cursor + 16).map(|value| value as usize) else {
            return BucketPageVerification::Invalid;
        };
        let Some(entry_len) = read_u32(page, cursor + 20).map(|value| value as usize) else {
            return BucketPageVerification::Invalid;
        };
        let expected = BUCKET_ENTRY_HEADER_BYTES
            .checked_add(key_len)
            .and_then(|bytes| bytes.checked_add(value_len))
            .and_then(|bytes| bytes.checked_add(BUCKET_ENTRY_ALIGNMENT - 1))
            .map(|bytes| bytes / BUCKET_ENTRY_ALIGNMENT * BUCKET_ENTRY_ALIGNMENT);
        let Some(end) = cursor.checked_add(entry_len) else {
            return BucketPageVerification::Invalid;
        };
        if entry_len == 0
            || entry_len % BUCKET_ENTRY_ALIGNMENT != 0
            || expected != Some(entry_len)
            || end > used
        {
            return BucketPageVerification::Invalid;
        }
        cursor = end;
    }
    if cursor != used {
        return BucketPageVerification::Invalid;
    }
    BucketPageVerification::Current(entry_count as u64)
}

const HYBRID_MANIFEST_SLOT_SIZE: usize = 4 * 1024;
const HYBRID_JOURNAL_OFFSET: u64 = (2 * HYBRID_MANIFEST_SLOT_SIZE) as u64;
const HYBRID_MANIFEST_MAGIC: [u8; 8] = *b"CRHYBM01";
const HYBRID_JOURNAL_MAGIC: [u8; 8] = *b"CRHYJR01";
const HYBRID_MANIFEST_VERSION: u16 = 1;
const HYBRID_MANIFEST_HEADER_SIZE: u16 = 120;
const HYBRID_USAGE_OFFSET: usize = 128;
const HYBRID_USAGE_MAGIC: [u8; 8] = *b"CRHYUS01";
const HYBRID_USAGE_HEADER_SIZE: u16 = 24;
const HYBRID_USAGE_ENTRIES_OFFSET: usize = HYBRID_USAGE_OFFSET + 24;
const HYBRID_USAGE_ENTRY_SIZE: usize = 16;
const HYBRID_USAGE_MAX_ENTRIES: usize = 240;
const HYBRID_JOURNAL_HEADER_SIZE: usize = 80;
const HYBRID_JOURNAL_ALIGNMENT: usize = 32;

#[derive(Clone, Copy)]
struct OfflineHybridManifest {
    generation: u64,
    cache_id: [u8; 16],
    version_epoch: u64,
    next_seqno: u64,
    layout_fingerprint: u64,
    journal_generation: u64,
    journal_capacity: u64,
    checkpoint_epoch: u64,
    checkpoint_seqno: u64,
    clear_floor_epoch: u64,
    clear_floor_seqno: u64,
    clean: bool,
}

fn inspect_hybrid_manifest(
    file: &mut LockedReadFile,
    verify_journal: bool,
) -> ManagementResult<HybridManifestFileReport> {
    let mut report = HybridManifestFileReport {
        file_len: file.len,
        ..HybridManifestFileReport::default()
    };
    if file.len < HYBRID_JOURNAL_OFFSET {
        return Ok(report);
    }
    let mut pages = [[0_u8; HYBRID_MANIFEST_SLOT_SIZE]; 2];
    let mut selected = None;
    for (slot, page) in pages.iter_mut().enumerate() {
        file.read_exact_at(page, (slot * HYBRID_MANIFEST_SLOT_SIZE) as u64)?;
        if let Some(manifest) = decode_hybrid_manifest(page) {
            if selected.is_none_or(|(_, current): (usize, OfflineHybridManifest)| {
                manifest.generation > current.generation
            }) {
                selected = Some((slot, manifest));
            }
        }
    }
    let Some((slot, manifest)) = selected else {
        return Ok(report);
    };
    report.selected_slot = Some(slot as u8);
    report.generation = Some(manifest.generation);
    report.cache_id = Some(manifest.cache_id);
    report.layout_fingerprint = Some(manifest.layout_fingerprint);
    report.clean = Some(manifest.clean);
    report.version_epoch = Some(manifest.version_epoch);
    report.next_seqno = Some(manifest.next_seqno);
    report.journal_generation = Some(manifest.journal_generation);
    report.journal_capacity_bytes = Some(manifest.journal_capacity);
    report.recovery_required = !manifest.clean;
    let expected_len = HYBRID_JOURNAL_OFFSET.checked_add(manifest.journal_capacity);
    report.valid = manifest.generation != 0
        && manifest.version_epoch != 0
        && manifest.next_seqno != 0
        && manifest.journal_generation != 0
        && version_precedes_manifest_next(
            manifest.checkpoint_epoch,
            manifest.checkpoint_seqno,
            manifest.version_epoch,
            manifest.next_seqno,
        )
        && version_precedes_manifest_next(
            manifest.clear_floor_epoch,
            manifest.clear_floor_seqno,
            manifest.version_epoch,
            manifest.next_seqno,
        )
        && (64 * 1024..=4 * 1024 * 1024 * 1024).contains(&manifest.journal_capacity)
        && manifest.journal_capacity % HYBRID_MANIFEST_SLOT_SIZE as u64 == 0
        && expected_len == Some(file.len);
    if !report.valid || !verify_journal {
        return Ok(report);
    }
    verify_hybrid_journal(file, manifest, &mut report)?;
    Ok(report)
}

fn decode_hybrid_manifest(page: &[u8]) -> Option<OfflineHybridManifest> {
    if page.len() != HYBRID_MANIFEST_SLOT_SIZE
        || page.get(..8)? != HYBRID_MANIFEST_MAGIC
        || read_u16(page, 8)? != HYBRID_MANIFEST_VERSION
        || read_u16(page, 10)? != HYBRID_MANIFEST_HEADER_SIZE
        || !checksum_matches(page, HYBRID_MANIFEST_SLOT_SIZE - size_of::<u32>())
        || !hybrid_usage_extension_valid(page)
    {
        return None;
    }
    let mut cache_id = [0_u8; 16];
    cache_id.copy_from_slice(page.get(24..40)?);
    if cache_id == [0; 16] {
        return None;
    }
    let clean = match *page.get(112)? {
        0 => false,
        1 => true,
        _ => return None,
    };
    Some(OfflineHybridManifest {
        generation: read_u64(page, 16)?,
        cache_id,
        version_epoch: read_u64(page, 40)?,
        next_seqno: read_u64(page, 48)?,
        layout_fingerprint: read_u64(page, 56)?,
        journal_generation: read_u64(page, 64)?,
        journal_capacity: read_u64(page, 72)?,
        checkpoint_epoch: read_u64(page, 80)?,
        checkpoint_seqno: read_u64(page, 88)?,
        clear_floor_epoch: read_u64(page, 96)?,
        clear_floor_seqno: read_u64(page, 104)?,
        clean,
    })
}

fn hybrid_usage_extension_valid(page: &[u8]) -> bool {
    let end = HYBRID_MANIFEST_SLOT_SIZE - size_of::<u32>();
    let Some(extension) = page.get(HYBRID_USAGE_OFFSET..end) else {
        return false;
    };
    if extension.iter().all(|byte| *byte == 0) {
        return true;
    }
    let count = read_u16(page, HYBRID_USAGE_OFFSET + 12).map(usize::from);
    let Some(count) = count else {
        return false;
    };
    let Some(used_end) = count
        .checked_mul(HYBRID_USAGE_ENTRY_SIZE)
        .and_then(|bytes| HYBRID_USAGE_ENTRIES_OFFSET.checked_add(bytes))
    else {
        return false;
    };
    if page.get(HYBRID_USAGE_OFFSET..HYBRID_USAGE_OFFSET + 8) != Some(HYBRID_USAGE_MAGIC.as_slice())
        || read_u16(page, HYBRID_USAGE_OFFSET + 8) != Some(1)
        || read_u16(page, HYBRID_USAGE_OFFSET + 10) != Some(HYBRID_USAGE_HEADER_SIZE)
        || read_u16(page, HYBRID_USAGE_OFFSET + 14) != Some(HYBRID_USAGE_ENTRY_SIZE as u16)
        || count > HYBRID_USAGE_MAX_ENTRIES
        || used_end > end
        || page
            .get(used_end..end)
            .is_none_or(|tail| tail.iter().any(|byte| *byte != 0))
        || !checksum_matches_range(page, HYBRID_USAGE_OFFSET, end, HYBRID_USAGE_OFFSET + 16)
    {
        return false;
    }
    let mut previous = None;
    for index in 0..count {
        let offset = HYBRID_USAGE_ENTRIES_OFFSET + index * HYBRID_USAGE_ENTRY_SIZE;
        let Some(namespace) = read_u32(page, offset) else {
            return false;
        };
        if read_u32(page, offset + 4) != Some(0)
            || previous.is_some_and(|previous| previous >= namespace)
            || read_u64(page, offset + 8).is_none()
        {
            return false;
        }
        previous = Some(namespace);
    }
    true
}

fn version_precedes_manifest_next(
    epoch: u64,
    seqno: u64,
    current_epoch: u64,
    next_seqno: u64,
) -> bool {
    (epoch == 0 && seqno == 0)
        || epoch < current_epoch
        || (epoch == current_epoch && seqno < next_seqno)
}

fn verify_hybrid_journal(
    file: &mut LockedReadFile,
    manifest: OfflineHybridManifest,
    report: &mut HybridManifestFileReport,
) -> ManagementResult<()> {
    let mut relative = 0_u64;
    let mut previous = (0_u64, 0_u64);
    while relative
        .checked_add(HYBRID_JOURNAL_HEADER_SIZE as u64)
        .is_some_and(|end| end <= manifest.journal_capacity)
    {
        let offset = HYBRID_JOURNAL_OFFSET
            .checked_add(relative)
            .ok_or(ManagementError::Allocation)?;
        let mut header = [0_u8; HYBRID_JOURNAL_HEADER_SIZE];
        file.read_exact_at(&mut header, offset)?;
        if header.iter().all(|byte| *byte == 0) {
            break;
        }
        let record_len = read_u32(&header, 12).map(u64::from);
        let generation = read_u64(&header, 24);
        let structural = header.get(..8) == Some(HYBRID_JOURNAL_MAGIC.as_slice())
            && read_u16(&header, 8) == Some(HYBRID_MANIFEST_VERSION)
            && read_u16(&header, 10) == Some(HYBRID_JOURNAL_HEADER_SIZE as u16)
            && generation == Some(manifest.journal_generation)
            && record_len.is_some_and(|length| {
                length >= HYBRID_JOURNAL_HEADER_SIZE as u64
                    && length % HYBRID_JOURNAL_ALIGNMENT as u64 == 0
                    && relative
                        .checked_add(length)
                        .is_some_and(|end| end <= manifest.journal_capacity)
            });
        if !structural {
            report.journal_torn_tail = true;
            report.recovery_required = true;
            break;
        }
        let record_len = record_len.expect("validated record length");
        let key_len = read_u32(&header, 52).map(u64::from).unwrap_or(u64::MAX);
        let expected_len = (HYBRID_JOURNAL_HEADER_SIZE as u64)
            .checked_add(key_len)
            .and_then(|bytes| bytes.checked_add(HYBRID_JOURNAL_ALIGNMENT as u64 - 1))
            .map(|bytes| bytes / HYBRID_JOURNAL_ALIGNMENT as u64 * HYBRID_JOURNAL_ALIGNMENT as u64);
        let kind = header[16];
        let flags = header[17];
        let epoch = read_u64(&header, 32).unwrap_or(0);
        let seqno = read_u64(&header, 40).unwrap_or(0);
        if expected_len != Some(record_len)
            || !(1..=4).contains(&kind)
            || flags & !1 != 0
            || epoch == 0
            || seqno == 0
            || (epoch, seqno) <= previous
            || epoch > manifest.version_epoch
            || (kind == 1 && flags & 1 == 0)
            || (kind == 4 && (key_len != 0 || flags != 0))
            || !journal_checksum_matches(file, offset, record_len, &header)?
        {
            report.valid = false;
            report.journal_torn_tail = true;
            report.recovery_required = true;
            break;
        }
        previous = (epoch, seqno);
        relative = relative.saturating_add(record_len);
        report.journal_records = report.journal_records.saturating_add(1);
    }
    report.journal_valid_bytes = relative;
    report.recovery_required |= report.journal_records != 0 || report.journal_torn_tail;
    Ok(())
}

fn journal_checksum_matches(
    file: &mut LockedReadFile,
    offset: u64,
    record_len: u64,
    header: &[u8; HYBRID_JOURNAL_HEADER_SIZE],
) -> ManagementResult<bool> {
    let Some(stored) = read_u32(header, 72) else {
        return Ok(false);
    };
    let mut checksum = Crc32c::new();
    checksum.update(&header[..72]);
    checksum.update(&[0_u8; size_of::<u32>()]);
    checksum.update(&header[76..]);
    let mut remaining = record_len.saturating_sub(HYBRID_JOURNAL_HEADER_SIZE as u64);
    let mut cursor = offset.saturating_add(HYBRID_JOURNAL_HEADER_SIZE as u64);
    let mut buffer = [0_u8; VERIFY_BUFFER_SIZE];
    while remaining != 0 {
        let length = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| ManagementError::Allocation)?;
        file.read_exact_at(&mut buffer[..length], cursor)?;
        checksum.update(&buffer[..length]);
        remaining -= length as u64;
        cursor = cursor.saturating_add(length as u64);
    }
    Ok(checksum.finish() == stored)
}

fn checksum_matches(input: &[u8], checksum_offset: usize) -> bool {
    let Some(stored) = read_u32(input, checksum_offset) else {
        return false;
    };
    let mut checksum = Crc32c::new();
    checksum.update(&input[..checksum_offset]);
    checksum.update(&[0_u8; size_of::<u32>()]);
    checksum.update(&input[checksum_offset + size_of::<u32>()..]);
    checksum.finish() == stored
}

fn checksum_matches_range(input: &[u8], start: usize, end: usize, checksum_offset: usize) -> bool {
    if start > checksum_offset
        || checksum_offset
            .checked_add(size_of::<u32>())
            .is_none_or(|offset_end| offset_end > end)
        || end > input.len()
    {
        return false;
    }
    let Some(stored) = read_u32(input, checksum_offset) else {
        return false;
    };
    let mut checksum = Crc32c::new();
    checksum.update(&input[start..checksum_offset]);
    checksum.update(&[0_u8; size_of::<u32>()]);
    checksum.update(&input[checksum_offset + size_of::<u32>()..end]);
    checksum.finish() == stored
}

fn read_u16(input: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        input.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_u32(input: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        input.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64(input: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        input.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn region_inspect_valid(report: &InspectReport) -> bool {
    report.kind == CacheFileKind::FormatV1
        && report
            .data_file_len
            .is_some_and(|data_file_len| report.file_len >= data_file_len)
        && report.regions.invalid_headers == 0
        && report.regions.truncated_headers == 0
        && matches!(
            report.checkpoint.directory_state,
            CheckpointDirectoryState::Absent | CheckpointDirectoryState::Valid
        )
        && report.checkpoint.slots.iter().all(|slot| {
            matches!(
                slot.state,
                CheckpointSlotState::Absent | CheckpointSlotState::HeaderValid
            )
        })
}

struct LockedReadFile {
    file: File,
    len: u64,
}

impl LockedReadFile {
    fn open(path: &Path) -> ManagementResult<Self> {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() {
            return Err(ManagementError::UnsupportedTarget(
                "cache management refuses symbolic links",
            ));
        }
        if !metadata.is_file() {
            return Err(ManagementError::UnsupportedTarget(
                "cache management requires a regular file",
            ));
        }
        let file = open_read_file(path)?;
        let opened = file.metadata()?;
        if !opened.is_file() {
            return Err(ManagementError::UnsupportedTarget(
                "cache management requires a regular file",
            ));
        }
        try_lock_exclusive(&file)?;
        Ok(Self {
            len: opened.len(),
            file,
        })
    }

    fn read_exact_at(&mut self, output: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(output)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
fn open_read_file(path: &Path) -> ManagementResult<File> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(SAFE_READ_OPEN_FLAGS)
        .open(path)?)
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "macos", target_os = "ios"))
))]
fn open_read_file(_path: &Path) -> ManagementResult<File> {
    Err(ManagementError::UnsupportedTarget(
        "race-safe offline cache opening is supported only on Linux and Apple Unix",
    ))
}

#[cfg(not(unix))]
fn open_read_file(_path: &Path) -> ManagementResult<File> {
    Err(ManagementError::UnsupportedTarget(
        "offline cache locking is supported only on Unix",
    ))
}

impl Drop for LockedReadFile {
    fn drop(&mut self) {
        unlock(&self.file);
    }
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> ManagementResult<()> {
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    // SAFETY: `file` owns a valid descriptor for the duration of this call.
    let result = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::WouldBlock {
        Err(ManagementError::Locked)
    } else {
        Err(ManagementError::Io(error))
    }
}

#[cfg(not(unix))]
fn try_lock_exclusive(_file: &File) -> ManagementResult<()> {
    Err(ManagementError::UnsupportedTarget(
        "offline cache locking is supported only on Unix",
    ))
}

#[cfg(unix)]
fn unlock(file: &File) {
    const LOCK_UN: i32 = 8;
    // SAFETY: `file` owns a valid descriptor for the duration of this call.
    let _ = unsafe { flock(file.as_raw_fd(), LOCK_UN) };
}

#[cfg(not(unix))]
fn unlock(_file: &File) {}

#[cfg(unix)]
unsafe extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

struct Inspected {
    report: InspectReport,
    selected: Option<Superblock>,
    directory: Option<CheckpointDirectory>,
    checkpoint_headers: [Option<CheckpointSlotHeader>; CHECKPOINT_SLOT_COUNT],
}

fn inspect_locked(file: &mut LockedReadFile) -> ManagementResult<Inspected> {
    let (superblocks, candidates, kind, selected_slot, selected) = inspect_superblocks(file)?;
    let mut report = InspectReport {
        file_len: file.len,
        kind,
        selected_superblock: selected_slot,
        data_file_len: None,
        superblocks,
        regions: RegionSummary::default(),
        checkpoint: CheckpointSummary::default(),
    };
    let mut directory = None;
    let mut checkpoint_headers = [None; CHECKPOINT_SLOT_COUNT];
    if let Some(superblock) = selected {
        let data_file_len = data_file_len(superblock);
        report.data_file_len = data_file_len;
        report.regions.expected = superblock.region_count;
        if !supported_superblock_layout(superblock) {
            report.kind = CacheFileKind::CorruptV1;
            return Ok(Inspected {
                report,
                selected: None,
                directory: None,
                checkpoint_headers,
            });
        }
        if let Some(data_file_len) = data_file_len {
            if file.len >= data_file_len {
                report.regions = inspect_regions(file, superblock)?;
                let (checkpoint, decoded_directory, decoded_headers) =
                    inspect_checkpoint(file, superblock, data_file_len)?;
                report.checkpoint = checkpoint;
                directory = decoded_directory;
                checkpoint_headers = decoded_headers;
            }
        }
    }
    let _ = candidates;
    Ok(Inspected {
        report,
        selected,
        directory,
        checkpoint_headers,
    })
}

type SuperblockInspection = (
    [SuperblockSummary; 2],
    [Option<Superblock>; 2],
    CacheFileKind,
    Option<u8>,
    Option<Superblock>,
);

fn inspect_superblocks(file: &mut LockedReadFile) -> ManagementResult<SuperblockInspection> {
    let offsets = [SUPERBLOCK_A_OFFSET, SUPERBLOCK_B_OFFSET];
    let mut summaries = [SuperblockSummary::missing(0), SuperblockSummary::missing(1)];
    let mut candidates = [None, None];
    let mut unsupported = None;
    let mut saw_corrupt = false;
    let mut saw_unrecognized = false;
    let mut saw_interrupted = false;
    let mut saw_nonempty = false;
    for (slot, offset) in offsets.into_iter().enumerate() {
        if offset >= file.len {
            continue;
        }
        let available = usize::try_from((file.len - offset).min(SUPERBLOCK_SIZE as u64))
            .map_err(|_| ManagementError::UnsupportedTarget("superblock length overflow"))?;
        let mut encoded = [0_u8; SUPERBLOCK_SIZE];
        file.read_exact_at(&mut encoded[..available], offset)?;
        saw_nonempty |= encoded[..available].iter().any(|byte| *byte != 0);
        let probe = Superblock::probe(&encoded);
        let slot_u8 = slot as u8;
        summaries[slot] = match probe {
            SuperblockProbe::Empty if available < SUPERBLOCK_SIZE => {
                saw_interrupted = true;
                SuperblockSummary {
                    state: SuperblockState::Truncated,
                    ..SuperblockSummary::missing(slot_u8)
                }
            }
            SuperblockProbe::Empty => SuperblockSummary {
                state: SuperblockState::Empty,
                ..SuperblockSummary::missing(slot_u8)
            },
            SuperblockProbe::InterruptedV1 => {
                saw_interrupted = true;
                SuperblockSummary {
                    state: SuperblockState::InterruptedV1,
                    version: Some(FORMAT_VERSION),
                    ..SuperblockSummary::missing(slot_u8)
                }
            }
            SuperblockProbe::ValidV1(superblock) => {
                candidates[slot] = Some(superblock);
                SuperblockSummary::valid(slot_u8, superblock)
            }
            SuperblockProbe::CorruptV1 => {
                saw_corrupt = true;
                SuperblockSummary {
                    state: SuperblockState::CorruptV1,
                    version: Some(FORMAT_VERSION),
                    ..SuperblockSummary::missing(slot_u8)
                }
            }
            SuperblockProbe::Unsupported(version) => {
                unsupported = Some(version);
                SuperblockSummary {
                    state: SuperblockState::Unsupported,
                    version: Some(version),
                    ..SuperblockSummary::missing(slot_u8)
                }
            }
            SuperblockProbe::Unrecognized => {
                saw_unrecognized = true;
                SuperblockSummary {
                    state: SuperblockState::Unrecognized,
                    ..SuperblockSummary::missing(slot_u8)
                }
            }
        };
    }

    let selected_pair = candidates
        .iter()
        .enumerate()
        .filter_map(|(slot, candidate)| candidate.map(|candidate| (slot as u8, candidate)))
        .max_by_key(|(_, candidate)| candidate.generation);
    let (kind, selected_slot, selected) = if let Some(version) = unsupported {
        (CacheFileKind::Unsupported(version), None, None)
    } else if let Some((slot, superblock)) = selected_pair {
        (CacheFileKind::FormatV1, Some(slot), Some(superblock))
    } else if saw_unrecognized || (file.len > SUPERBLOCK_AREA_SIZE && !saw_corrupt) {
        (CacheFileKind::Unrecognized, None, None)
    } else if saw_corrupt {
        (CacheFileKind::CorruptV1, None, None)
    } else if saw_interrupted || saw_nonempty || file.len != 0 {
        (CacheFileKind::InterruptedV1, None, None)
    } else {
        (CacheFileKind::Empty, None, None)
    };
    Ok((summaries, candidates, kind, selected_slot, selected))
}

fn data_file_len(superblock: Superblock) -> Option<u64> {
    SUPERBLOCK_AREA_SIZE
        .checked_add(u64::from(superblock.region_count).checked_mul(superblock.region_size)?)
}

fn supported_superblock_layout(superblock: Superblock) -> bool {
    superblock.region_count != 0
        && superblock.region_count <= MAX_REGION_ID
        && superblock.region_size <= u64::from(MAX_REGION_OFFSET) + 8
        && usize::try_from(superblock.region_count).is_ok()
        && data_file_len(superblock).is_some()
}

fn region_offset(superblock: Superblock, region_id: u32) -> Option<u64> {
    SUPERBLOCK_AREA_SIZE.checked_add(u64::from(region_id).checked_mul(superblock.region_size)?)
}

fn inspect_regions(
    file: &mut LockedReadFile,
    superblock: Superblock,
) -> ManagementResult<RegionSummary> {
    let mut summary = RegionSummary {
        expected: superblock.region_count,
        ..RegionSummary::default()
    };
    for region_id in 0..superblock.region_count {
        let Some(offset) = region_offset(superblock, region_id) else {
            summary.invalid_headers = summary.invalid_headers.saturating_add(1);
            continue;
        };
        let Some(end) = offset.checked_add(REGION_HEADER_SIZE as u64) else {
            summary.invalid_headers = summary.invalid_headers.saturating_add(1);
            continue;
        };
        if end > file.len {
            summary.truncated_headers = summary.truncated_headers.saturating_add(1);
            continue;
        }
        let mut encoded = [0_u8; REGION_HEADER_SIZE];
        file.read_exact_at(&mut encoded, offset)?;
        let Some(header) = RegionHeader::decode(&encoded) else {
            summary.invalid_headers = summary.invalid_headers.saturating_add(1);
            continue;
        };
        if !region_header_valid(header, region_id, superblock) {
            summary.invalid_headers = summary.invalid_headers.saturating_add(1);
            continue;
        }
        summary.valid_headers = summary.valid_headers.saturating_add(1);
        summary.record_extent_bytes = summary
            .record_extent_bytes
            .saturating_add(header.used.saturating_sub(REGION_HEADER_SIZE as u64));
        match header.state {
            RegionState::Free => summary.free = summary.free.saturating_add(1),
            RegionState::Active => summary.active = summary.active.saturating_add(1),
            RegionState::Sealed => summary.sealed = summary.sealed.saturating_add(1),
        }
    }
    Ok(summary)
}

fn region_header_valid(header: RegionHeader, expected_id: u32, superblock: Superblock) -> bool {
    if header.region_id != expected_id || header.used > superblock.region_size {
        return false;
    }
    match header.state {
        RegionState::Free => {
            header.incarnation == 0
                && header.created_seqno == 0
                && header.used == REGION_HEADER_SIZE as u64
        }
        RegionState::Active | RegionState::Sealed => {
            header.incarnation != 0
                && header.created_seqno != 0
                && header.created_seqno != u64::MAX
                // The dirty marker is durable before later Region rotations,
                // so a new Region's creation sequence may legitimately be at
                // or beyond the marker's persisted `next_seqno`.
                && (!superblock.clean || header.created_seqno < superblock.next_seqno)
        }
    }
}

type CheckpointInspection = (
    CheckpointSummary,
    Option<CheckpointDirectory>,
    [Option<CheckpointSlotHeader>; CHECKPOINT_SLOT_COUNT],
);

fn inspect_checkpoint(
    file: &mut LockedReadFile,
    superblock: Superblock,
    data_file_len: u64,
) -> ManagementResult<CheckpointInspection> {
    let mut summary = CheckpointSummary::default();
    let directory_end = data_file_len.saturating_add(CHECKPOINT_DIRECTORY_SIZE as u64);
    if file.len <= data_file_len {
        return Ok((summary, None, [None; CHECKPOINT_SLOT_COUNT]));
    }
    if directory_end > file.len {
        summary.directory_state = CheckpointDirectoryState::Truncated;
        return Ok((summary, None, [None; CHECKPOINT_SLOT_COUNT]));
    }
    let mut encoded = [0_u8; CHECKPOINT_DIRECTORY_SIZE];
    file.read_exact_at(&mut encoded, data_file_len)?;
    if encoded.iter().all(|byte| *byte == 0) {
        return Ok((summary, None, [None; CHECKPOINT_SLOT_COUNT]));
    }
    let Ok(directory) = CheckpointDirectory::decode(&encoded) else {
        summary.directory_state = CheckpointDirectoryState::Invalid;
        return Ok((summary, None, [None; CHECKPOINT_SLOT_COUNT]));
    };
    if directory.data_file_len != data_file_len
        || directory.region_size != superblock.region_size
        || directory.region_count != superblock.region_count
    {
        summary.directory_state = CheckpointDirectoryState::LayoutMismatch;
        return Ok((summary, None, [None; CHECKPOINT_SLOT_COUNT]));
    }
    summary.directory_state = CheckpointDirectoryState::Valid;
    summary.slot_size = Some(directory.slot_size);
    summary.expected_file_len = directory.total_file_len().ok();
    let mut headers = [None; CHECKPOINT_SLOT_COUNT];
    for slot in 0..CHECKPOINT_SLOT_COUNT {
        let Ok(offset) = directory.slot_header_offset(slot) else {
            summary.slots[slot].state = CheckpointSlotState::Invalid;
            continue;
        };
        let Some(end) = offset.checked_add(CHECKPOINT_SLOT_HEADER_SIZE as u64) else {
            summary.slots[slot].state = CheckpointSlotState::Invalid;
            continue;
        };
        if offset >= file.len {
            continue;
        }
        if end > file.len {
            summary.slots[slot].state = CheckpointSlotState::Truncated;
            continue;
        }
        let mut header_bytes = [0_u8; CHECKPOINT_SLOT_HEADER_SIZE];
        file.read_exact_at(&mut header_bytes, offset)?;
        if header_bytes.iter().all(|byte| *byte == 0) {
            continue;
        }
        match CheckpointSlotHeader::decode(&header_bytes, directory, slot) {
            Ok(header) => {
                headers[slot] = Some(header);
                let matches = checkpoint_matches_superblock(header, superblock);
                summary.slots[slot] = CheckpointSlotSummary {
                    slot: slot as u8,
                    state: CheckpointSlotState::HeaderValid,
                    version: Some(header.version),
                    generation: Some(header.generation),
                    superblock_generation: Some(header.superblock_generation),
                    entry_count: Some(header.entry_count),
                    payload_len: Some(header.payload_len),
                    matches_selected_superblock: matches,
                };
                if matches
                    && summary.selected_header_slot.is_none_or(|selected| {
                        headers[selected as usize]
                            .is_some_and(|current| header.generation > current.generation)
                    })
                {
                    summary.selected_header_slot = Some(slot as u8);
                }
            }
            Err(_) => summary.slots[slot].state = CheckpointSlotState::Invalid,
        }
    }
    Ok((summary, Some(directory), headers))
}

fn checkpoint_matches_superblock(header: CheckpointSlotHeader, superblock: Superblock) -> bool {
    if header.generation != header.superblock_generation || header.hash_seed != superblock.hash_seed
    {
        return false;
    }
    if superblock.clean {
        return header.superblock_generation == superblock.generation
            && header.epoch == superblock.epoch
            && header.epoch_start_seqno == superblock.epoch_start_seqno
            && superblock
                .next_seqno
                .checked_sub(1)
                .is_some_and(|maximum| maximum == header.max_seqno);
    }
    if header.superblock_generation >= superblock.generation {
        return false;
    }
    let checkpoint_next = header.max_seqno.checked_add(1);
    if header.epoch == superblock.epoch {
        return header.epoch_start_seqno == superblock.epoch_start_seqno
            && checkpoint_next == Some(superblock.next_seqno)
            && header.superblock_generation.checked_add(1) == Some(superblock.generation);
    }
    header.epoch < superblock.epoch
        && header.epoch_start_seqno < superblock.epoch_start_seqno
        && header.max_seqno < superblock.epoch_start_seqno
        && superblock
            .epoch_start_seqno
            .checked_add(1)
            .is_some_and(|next| next == superblock.next_seqno)
        && header
            .superblock_generation
            .checked_add(2)
            .is_some_and(|minimum| minimum <= superblock.generation)
}

fn verify_locked(
    file: &mut LockedReadFile,
    inspected: Inspected,
) -> ManagementResult<VerifyReport> {
    let mut report = VerifyReport::new(inspected.report);
    let Some(superblock) = inspected.selected else {
        report.issue(VerifyIssue {
            component: VerifyComponent::Superblock,
            offset: 0,
            region_id: None,
            checkpoint_slot: None,
            message: "no supported checksum-valid Superblock",
        });
        return Ok(report);
    };
    report.safe_to_open = true;
    verify_superblock_replicas(&mut report);
    // A dirty checkpoint limits what runtime recovery must replay, but it is
    // not the boundary of an offline integrity check. Verify every record in
    // the current Region incarnations as well as the checkpoint and its
    // incremental tails below.
    let data_valid = verify_regions_and_records(file, superblock, &mut report)?;
    let verified_slots = verify_checkpoint_payloads(
        file,
        superblock,
        inspected.directory,
        inspected.checkpoint_headers,
        &mut report,
    )?;
    let mut usable_slots = [false; CHECKPOINT_SLOT_COUNT];
    if let Some(directory) = inspected.directory {
        for slot in 0..CHECKPOINT_SLOT_COUNT {
            let Some(header) = inspected.checkpoint_headers[slot] else {
                continue;
            };
            if !verified_slots[slot] || !checkpoint_matches_superblock(header, superblock) {
                continue;
            }
            match cross_validate_checkpoint_regions(file, superblock, directory, header) {
                Ok(CheckpointCrossValidation::Usable) => usable_slots[slot] = true,
                Ok(CheckpointCrossValidation::ConservativeFallback) => {}
                Err(CheckpointCrossError::Io(error)) => {
                    return Err(ManagementError::Io(error));
                }
                Err(CheckpointCrossError::Allocation) => {
                    return Err(ManagementError::Allocation);
                }
                Err(CheckpointCrossError::Codec(error)) => report.issue(VerifyIssue {
                    component: VerifyComponent::CheckpointPayload,
                    offset: directory
                        .slot_payload_offset(slot)
                        .unwrap_or(directory.data_file_len),
                    region_id: None,
                    checkpoint_slot: Some(slot as u8),
                    message: checkpoint_error_message(error),
                }),
                Err(CheckpointCrossError::Invalid(message)) => report.issue(VerifyIssue {
                    component: VerifyComponent::CheckpointPayload,
                    offset: directory
                        .slot_payload_offset(slot)
                        .unwrap_or(directory.data_file_len),
                    region_id: None,
                    checkpoint_slot: Some(slot as u8),
                    message,
                }),
            }
        }
    }
    let mut selected_verified = usable_slots
        .iter()
        .enumerate()
        .filter_map(|(slot, usable)| {
            (*usable)
                .then_some(inspected.checkpoint_headers[slot])
                .flatten()
                .filter(|header| checkpoint_matches_superblock(*header, superblock))
                .map(|header| (slot as u8, header.generation))
        })
        .max_by_key(|(_, generation)| *generation)
        .map(|(slot, _)| slot);
    let mut disposition = if superblock.clean {
        if selected_verified.is_some() {
            ReopenDisposition::CleanCheckpoint
        } else if data_valid {
            ReopenDisposition::CleanFullScan
        } else {
            ReopenDisposition::SafeEmpty
        }
    } else if selected_verified.is_some() {
        ReopenDisposition::DirtyIncremental
    } else {
        ReopenDisposition::SafeEmpty
    };
    let generation_error = if !superblock.clean && superblock.generation == u64::MAX {
        Some("dirty Superblock generation cannot publish a clean checkpoint")
    } else if disposition == ReopenDisposition::SafeEmpty && superblock.generation > u64::MAX - 2 {
        Some("Superblock generation cannot complete safe-empty formatting")
    } else {
        None
    };
    if let Some(message) = generation_error {
        report.safe_to_open = false;
        report.issue(VerifyIssue {
            component: VerifyComponent::Superblock,
            offset: report
                .inspect
                .selected_superblock
                .map_or(0, |slot| u64::from(slot) * SUPERBLOCK_SIZE as u64),
            region_id: None,
            checkpoint_slot: None,
            message,
        });
        selected_verified = None;
        disposition = ReopenDisposition::Refused;
    }
    report.selected_verified_checkpoint = selected_verified;
    report.reopen_disposition = disposition;
    Ok(report)
}

fn verify_superblock_replicas(report: &mut VerifyReport) {
    for superblock in report.inspect.superblocks {
        match superblock.state {
            SuperblockState::ValidV1 => {}
            SuperblockState::Missing | SuperblockState::Empty if report.inspect.file_len == 0 => {}
            state => report.issue(VerifyIssue {
                component: VerifyComponent::Superblock,
                offset: u64::from(superblock.slot) * SUPERBLOCK_SIZE as u64,
                region_id: None,
                checkpoint_slot: None,
                message: match state {
                    SuperblockState::Missing => "redundant Superblock is missing",
                    SuperblockState::Empty => "redundant Superblock is empty",
                    SuperblockState::InterruptedV1 => "interrupted Superblock marker",
                    SuperblockState::CorruptV1 => "Superblock checksum or fields are invalid",
                    SuperblockState::Unsupported => "unsupported Superblock version",
                    SuperblockState::Unrecognized => "unrecognized Superblock bytes",
                    SuperblockState::Truncated => "truncated Superblock page",
                    SuperblockState::ValidV1 => unreachable!(),
                },
            }),
        }
    }
    let Some(selected) = report.inspect.selected() else {
        return;
    };
    if selected.epoch == Some(0)
        || selected.epoch_start_seqno == Some(0)
        || selected
            .epoch_start_seqno
            .zip(selected.next_seqno)
            .is_none_or(|(start, next)| start >= next)
    {
        report.issue(VerifyIssue {
            component: VerifyComponent::Superblock,
            offset: u64::from(selected.slot) * SUPERBLOCK_SIZE as u64,
            region_id: None,
            checkpoint_slot: None,
            message: "Superblock epoch or sequence bounds are invalid",
        });
    }
}

fn verify_regions_and_records(
    file: &mut LockedReadFile,
    superblock: Superblock,
    report: &mut VerifyReport,
) -> ManagementResult<bool> {
    let Some(expected_data_len) = data_file_len(superblock) else {
        report.issue(VerifyIssue {
            component: VerifyComponent::Layout,
            offset: SUPERBLOCK_AREA_SIZE,
            region_id: None,
            checkpoint_slot: None,
            message: "data extent length overflows",
        });
        return Ok(false);
    };
    if file.len < expected_data_len {
        report.issue(VerifyIssue {
            component: VerifyComponent::Layout,
            offset: file.len,
            region_id: None,
            checkpoint_slot: None,
            message: "cache file is shorter than its declared data extent",
        });
        return Ok(false);
    }

    let region_count = superblock.region_count as usize;
    let mut creation_seqnos = Vec::new();
    creation_seqnos
        .try_reserve_exact(region_count)
        .map_err(|_| ManagementError::Allocation)?;
    let mut allocated = 0_u32;
    let mut active = 0_u32;
    let mut oldest: Option<(u64, u32)> = None;
    let issue_start = report.issues_total;
    for region_id in 0..superblock.region_count {
        let Some(offset) = region_offset(superblock, region_id) else {
            add_region_issue(
                report,
                VerifyComponent::RegionHeader,
                u64::MAX,
                region_id,
                "Region offset overflows",
            );
            continue;
        };
        if offset
            .checked_add(REGION_HEADER_SIZE as u64)
            .is_none_or(|end| end > file.len)
        {
            add_region_issue(
                report,
                VerifyComponent::RegionHeader,
                offset,
                region_id,
                "Region Header is truncated",
            );
            continue;
        }
        let mut encoded = [0_u8; REGION_HEADER_SIZE];
        file.read_exact_at(&mut encoded, offset)?;
        let Some(header) = RegionHeader::decode(&encoded) else {
            add_region_issue(
                report,
                VerifyComponent::RegionHeader,
                offset,
                region_id,
                "Region Header checksum or fields are invalid",
            );
            continue;
        };
        if !region_header_valid(header, region_id, superblock) {
            add_region_issue(
                report,
                VerifyComponent::RegionHeader,
                offset,
                region_id,
                "Region Header metadata is inconsistent",
            );
            continue;
        }
        report.regions_verified = report.regions_verified.saturating_add(1);
        if header.state != RegionState::Free {
            allocated = allocated.saturating_add(1);
            active = active.saturating_add(u32::from(header.state == RegionState::Active));
            creation_seqnos.push(header.created_seqno);
            if oldest.is_none_or(|current| header.created_seqno < current.0) {
                oldest = Some((header.created_seqno, region_id));
            }
        }
    }
    creation_seqnos.sort_unstable();
    for duplicate in creation_seqnos.windows(2).filter(|pair| pair[0] == pair[1]) {
        let _ = duplicate;
        report.issue(VerifyIssue {
            component: VerifyComponent::RegionHeader,
            offset: SUPERBLOCK_AREA_SIZE,
            region_id: None,
            checkpoint_slot: None,
            message: "Region creation sequence is duplicated",
        });
    }
    if active == 0 {
        report.issue(VerifyIssue {
            component: VerifyComponent::RegionHeader,
            offset: SUPERBLOCK_AREA_SIZE,
            region_id: None,
            checkpoint_slot: None,
            message: "cache has no Active Region",
        });
    }
    if allocated < superblock.region_count && oldest.is_some_and(|(_, region_id)| region_id != 0) {
        report.issue(VerifyIssue {
            component: VerifyComponent::RegionHeader,
            offset: SUPERBLOCK_AREA_SIZE,
            region_id: oldest.map(|(_, region_id)| region_id),
            checkpoint_slot: None,
            message: "partially allocated FIFO does not start at Region zero",
        });
    }

    let mut scratch = [0_u8; VERIFY_BUFFER_SIZE];
    for region_id in 0..superblock.region_count {
        let Some(base) = region_offset(superblock, region_id) else {
            continue;
        };
        if base
            .checked_add(REGION_HEADER_SIZE as u64)
            .is_none_or(|end| end > file.len)
        {
            continue;
        }
        let mut encoded = [0_u8; REGION_HEADER_SIZE];
        file.read_exact_at(&mut encoded, base)?;
        let Some(region) = RegionHeader::decode(&encoded)
            .filter(|header| region_header_valid(*header, region_id, superblock))
        else {
            continue;
        };
        if region.state == RegionState::Free {
            continue;
        }
        verify_region_records(file, superblock, region, base, &mut scratch, report)?;
    }
    Ok(report.issues_total == issue_start)
}

fn verify_region_records(
    file: &mut LockedReadFile,
    superblock: Superblock,
    region: RegionHeader,
    base: u64,
    scratch: &mut [u8; VERIFY_BUFFER_SIZE],
    report: &mut VerifyReport,
) -> ManagementResult<()> {
    let mut cursor = REGION_HEADER_SIZE as u64;
    let mut last_seqno = None;
    // Active Region Headers are persisted at checkpoints and rotations, not
    // after every append. On a dirty file their `used` cursor can therefore
    // lag complete records already on disk. Scan until zeroes or bytes from an
    // older incarnation; sealed Regions still end exactly at `used`.
    let scan_active_tail = !superblock.clean && region.state == RegionState::Active;
    loop {
        if !scan_active_tail && cursor >= region.used {
            break;
        }
        let Some(absolute) = base.checked_add(cursor) else {
            add_region_issue(
                report,
                VerifyComponent::RecordHeader,
                base,
                region.region_id,
                "record offset overflows",
            );
            break;
        };
        if cursor >= superblock.region_size {
            if cursor > superblock.region_size {
                add_region_issue(
                    report,
                    VerifyComponent::RecordHeader,
                    absolute,
                    region.region_id,
                    "record cursor exceeds the Region boundary",
                );
            }
            break;
        }
        if superblock.region_size - cursor < RECORD_HEADER_SIZE as u64 {
            if cursor < region.used {
                add_region_issue(
                    report,
                    VerifyComponent::RecordHeader,
                    absolute,
                    region.region_id,
                    "record does not fill the persisted Region extent",
                );
            }
            break;
        }
        let mut header_bytes = [0_u8; RECORD_HEADER_SIZE];
        file.read_exact_at(&mut header_bytes, absolute)?;
        if header_bytes.iter().all(|byte| *byte == 0) {
            if cursor < region.used {
                add_region_issue(
                    report,
                    VerifyComponent::RecordHeader,
                    absolute,
                    region.region_id,
                    "zero record appears inside the persisted Region extent",
                );
            }
            break;
        }
        let Some(header) = RecordHeader::decode(&header_bytes) else {
            add_region_issue(
                report,
                VerifyComponent::RecordHeader,
                absolute,
                region.region_id,
                "record header checksum or fields are invalid",
            );
            break;
        };
        if header.region_incarnation != region.incarnation {
            if cursor < region.used {
                add_region_issue(
                    report,
                    VerifyComponent::RecordHeader,
                    absolute,
                    region.region_id,
                    "persisted record has the wrong Region incarnation",
                );
            }
            break;
        }
        let Some(record_end) = cursor
            .checked_add(u64::from(header.record_len))
            .filter(|end| *end <= superblock.region_size)
        else {
            add_region_issue(
                report,
                VerifyComponent::RecordHeader,
                absolute,
                region.region_id,
                "record crosses the persisted Region extent",
            );
            break;
        };
        if cursor < region.used && record_end > region.used {
            add_region_issue(
                report,
                VerifyComponent::RecordHeader,
                absolute,
                region.region_id,
                "record crosses the persisted Region extent",
            );
            break;
        }
        if absolute
            .checked_add(u64::from(header.record_len))
            .is_none_or(|end| end > file.len)
        {
            add_region_issue(
                report,
                VerifyComponent::RecordPayload,
                absolute,
                region.region_id,
                "record bytes are truncated",
            );
            break;
        }
        if header.seqno == 0
            || header.seqno == u64::MAX
            || (superblock.clean && header.seqno >= superblock.next_seqno)
            || header.seqno < region.created_seqno
            || last_seqno.is_some_and(|previous| header.seqno <= previous)
            || header.epoch == 0
            || header.epoch > superblock.epoch
            || (header.epoch == superblock.epoch && header.seqno <= superblock.epoch_start_seqno)
            || (header.epoch < superblock.epoch && header.seqno >= superblock.epoch_start_seqno)
        {
            add_region_issue(
                report,
                VerifyComponent::RecordHeader,
                absolute,
                region.region_id,
                "record generation or sequence metadata is inconsistent",
            );
            cursor = record_end;
            continue;
        }
        let payload_offset = absolute.checked_add(RECORD_HEADER_SIZE as u64).ok_or(
            ManagementError::UnsupportedTarget("record payload offset overflows"),
        )?;
        match verify_record_payload(file, superblock.hash_seed, header, payload_offset, scratch)? {
            None => {
                report.records_verified = report.records_verified.saturating_add(1);
                report.record_bytes_verified = report
                    .record_bytes_verified
                    .saturating_add(u64::from(header.record_len));
                match header.kind {
                    RecordKind::Value => {
                        report.values_verified = report.values_verified.saturating_add(1)
                    }
                    RecordKind::Tombstone => {
                        report.tombstones_verified = report.tombstones_verified.saturating_add(1)
                    }
                }
            }
            Some(message) => add_region_issue(
                report,
                VerifyComponent::RecordPayload,
                payload_offset,
                region.region_id,
                message,
            ),
        }
        last_seqno = Some(header.seqno);
        cursor = record_end;
    }
    Ok(())
}

fn verify_record_payload(
    file: &mut LockedReadFile,
    hash_seed: u64,
    header: RecordHeader,
    payload_offset: u64,
    scratch: &mut [u8; VERIFY_BUFFER_SIZE],
) -> ManagementResult<Option<&'static str>> {
    let key_len = header.key_len as usize;
    let stored_len = header.stored_len as usize;
    let Some(payload_len) = key_len.checked_add(stored_len) else {
        return Ok(Some("record payload length overflows"));
    };
    let mut checksum = Crc32c::new();
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ hash_seed;
    let mut key_offset = payload_offset;
    let mut remaining_key = key_len;
    if matches!(
        header.codec,
        RecordCodec::NamespacedKey | RecordCodec::SecondChanceNamespacedKey
    ) {
        if remaining_key < NAMESPACE_KEY_PREFIX_SIZE {
            return Ok(Some("namespaced record key is shorter than its prefix"));
        }
        let mut namespace = [0_u8; NAMESPACE_KEY_PREFIX_SIZE];
        file.read_exact_at(&mut namespace, key_offset)?;
        if u32::from_le_bytes(namespace) == 0 {
            return Ok(Some("namespaced record uses namespace zero"));
        }
        checksum.update(&namespace);
        hash_key_update(&mut hash, NAMESPACE_HASH_DOMAIN);
        hash_key_update(&mut hash, &namespace);
        key_offset += NAMESPACE_KEY_PREFIX_SIZE as u64;
        remaining_key -= NAMESPACE_KEY_PREFIX_SIZE;
    }
    stream_payload(
        file,
        key_offset,
        remaining_key,
        scratch,
        &mut checksum,
        Some(&mut hash),
    )?;
    let value_offset =
        payload_offset
            .checked_add(key_len as u64)
            .ok_or(ManagementError::UnsupportedTarget(
                "record value offset overflows",
            ))?;
    stream_payload(file, value_offset, stored_len, scratch, &mut checksum, None)?;
    debug_assert_eq!(payload_len, key_len + stored_len);
    if checksum.finish() != header.payload_crc {
        return Ok(Some("record payload checksum does not match"));
    }
    if hash != header.key_hash {
        return Ok(Some("record key hash does not match"));
    }
    Ok(None)
}

fn stream_payload(
    file: &mut LockedReadFile,
    mut offset: u64,
    mut remaining: usize,
    scratch: &mut [u8; VERIFY_BUFFER_SIZE],
    checksum: &mut Crc32c,
    mut hash: Option<&mut u64>,
) -> ManagementResult<()> {
    while remaining != 0 {
        let length = remaining.min(scratch.len());
        file.read_exact_at(&mut scratch[..length], offset)?;
        checksum.update(&scratch[..length]);
        if let Some(hash) = hash.as_deref_mut() {
            hash_key_update(hash, &scratch[..length]);
        }
        offset = offset
            .checked_add(length as u64)
            .ok_or(ManagementError::UnsupportedTarget(
                "record payload offset overflows",
            ))?;
        remaining -= length;
    }
    Ok(())
}

fn hash_key_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
}

fn add_region_issue(
    report: &mut VerifyReport,
    component: VerifyComponent,
    offset: u64,
    region_id: u32,
    message: &'static str,
) {
    report.issue(VerifyIssue {
        component,
        offset,
        region_id: Some(region_id),
        checkpoint_slot: None,
        message,
    });
}

fn verify_checkpoint_payloads(
    file: &mut LockedReadFile,
    superblock: Superblock,
    directory: Option<CheckpointDirectory>,
    headers: [Option<CheckpointSlotHeader>; CHECKPOINT_SLOT_COUNT],
    report: &mut VerifyReport,
) -> ManagementResult<[bool; CHECKPOINT_SLOT_COUNT]> {
    let mut verified = [false; CHECKPOINT_SLOT_COUNT];
    match report.inspect.checkpoint.directory_state {
        CheckpointDirectoryState::Absent => return Ok(verified),
        CheckpointDirectoryState::Valid => {}
        CheckpointDirectoryState::Invalid => {
            report.issue(VerifyIssue {
                component: VerifyComponent::CheckpointDirectory,
                offset: report.inspect.data_file_len.unwrap_or(SUPERBLOCK_AREA_SIZE),
                region_id: None,
                checkpoint_slot: None,
                message: "checkpoint directory checksum or fields are invalid",
            });
            return Ok(verified);
        }
        CheckpointDirectoryState::LayoutMismatch => {
            report.issue(VerifyIssue {
                component: VerifyComponent::CheckpointDirectory,
                offset: report.inspect.data_file_len.unwrap_or(SUPERBLOCK_AREA_SIZE),
                region_id: None,
                checkpoint_slot: None,
                message: "checkpoint directory does not match the data layout",
            });
            return Ok(verified);
        }
        CheckpointDirectoryState::Truncated => {
            report.issue(VerifyIssue {
                component: VerifyComponent::CheckpointDirectory,
                offset: report.inspect.data_file_len.unwrap_or(SUPERBLOCK_AREA_SIZE),
                region_id: None,
                checkpoint_slot: None,
                message: "checkpoint directory is truncated",
            });
            return Ok(verified);
        }
    }
    let Some(directory) = directory else {
        return Ok(verified);
    };
    for slot in 0..CHECKPOINT_SLOT_COUNT {
        match report.inspect.checkpoint.slots[slot].state {
            CheckpointSlotState::Absent => continue,
            CheckpointSlotState::Invalid => {
                add_checkpoint_issue(
                    report,
                    VerifyComponent::CheckpointHeader,
                    directory
                        .slot_header_offset(slot)
                        .unwrap_or(directory.data_file_len),
                    slot,
                    "checkpoint slot header checksum or fields are invalid",
                );
                continue;
            }
            CheckpointSlotState::Truncated => {
                add_checkpoint_issue(
                    report,
                    VerifyComponent::CheckpointHeader,
                    directory
                        .slot_header_offset(slot)
                        .unwrap_or(directory.data_file_len),
                    slot,
                    "checkpoint slot header is truncated",
                );
                continue;
            }
            CheckpointSlotState::HeaderValid => {}
        }
        let Some(header) = headers[slot] else {
            continue;
        };
        match verify_one_checkpoint_payload(file, directory, header) {
            Ok(()) => {
                verified[slot] = true;
                report.checkpoint_slots_verified =
                    report.checkpoint_slots_verified.saturating_add(1);
            }
            Err(CheckpointVerifyError::Io(error)) => return Err(ManagementError::Io(error)),
            Err(CheckpointVerifyError::Codec(error)) => add_checkpoint_issue(
                report,
                VerifyComponent::CheckpointPayload,
                directory
                    .slot_payload_offset(slot)
                    .unwrap_or(directory.data_file_len),
                slot,
                checkpoint_error_message(error),
            ),
            Err(CheckpointVerifyError::Truncated) => add_checkpoint_issue(
                report,
                VerifyComponent::CheckpointPayload,
                directory
                    .slot_payload_offset(slot)
                    .unwrap_or(directory.data_file_len),
                slot,
                "checkpoint payload is truncated",
            ),
        }
    }
    let _ = superblock;
    Ok(verified)
}

enum CheckpointVerifyError {
    Io(io::Error),
    Codec(CheckpointCodecError),
    Truncated,
}

enum CheckpointCrossValidation {
    Usable,
    ConservativeFallback,
}

enum CheckpointCrossError {
    Io(io::Error),
    Codec(CheckpointCodecError),
    Invalid(&'static str),
    Allocation,
}

fn cross_validate_checkpoint_regions(
    file: &mut LockedReadFile,
    superblock: Superblock,
    directory: CheckpointDirectory,
    header: CheckpointSlotHeader,
) -> std::result::Result<CheckpointCrossValidation, CheckpointCrossError> {
    let payload_offset = directory
        .slot_payload_offset(usize::from(header.slot))
        .map_err(CheckpointCrossError::Codec)?;
    let region_count = usize::try_from(header.region_count)
        .map_err(|_| CheckpointCrossError::Invalid("checkpoint Region count is unsupported"))?;
    let mut creation_seqnos = Vec::new();
    creation_seqnos
        .try_reserve_exact(region_count)
        .map_err(|_| CheckpointCrossError::Allocation)?;

    let mut checkpoint_allocated = 0_u32;
    let mut checkpoint_active = 0_usize;
    let mut checkpoint_oldest: Option<(u64, u32)> = None;
    let mut checkpoint_lane_mask = 0_u8;
    let mut current_allocated = 0_u32;
    let mut current_active = 0_usize;
    let mut current_oldest: Option<(u64, u32)> = None;
    let mut current_lane_mask = 0_u8;
    let mut conservative = header.version < CHECKPOINT_LANE_VERSION;
    let mut scratch = [0_u8; VERIFY_BUFFER_SIZE];

    for region_id in 0..header.region_count {
        let prior = read_checkpoint_region(file, payload_offset, header, region_id)?;
        let current = read_current_region(file, superblock, region_id)?;

        if prior.state != RegionState::Free {
            checkpoint_allocated = checkpoint_allocated.saturating_add(1);
            checkpoint_oldest = older_region(checkpoint_oldest, prior.created_seqno, region_id);
        }
        if prior.state == RegionState::Active {
            checkpoint_active = checkpoint_active.saturating_add(1);
            if header.version >= CHECKPOINT_LANE_VERSION {
                let lane_id = prior.lane_id.ok_or(CheckpointCrossError::Invalid(
                    "checkpoint Active Region has no append lane",
                ))?;
                let lane = usize::from(lane_id);
                if lane >= MAX_APPEND_LANES {
                    return Err(CheckpointCrossError::Invalid(
                        "checkpoint append lane is out of range",
                    ));
                }
                let bit = 1_u8 << lane;
                if checkpoint_lane_mask & bit != 0 {
                    return Err(CheckpointCrossError::Invalid(
                        "checkpoint append lane is duplicated",
                    ));
                }
                checkpoint_lane_mask |= bit;
            }
        }

        if current.state != RegionState::Free {
            current_allocated = current_allocated.saturating_add(1);
            current_oldest = older_region(current_oldest, current.created_seqno, region_id);
            creation_seqnos.push(current.created_seqno);
        }
        if current.state == RegionState::Active {
            current_active = current_active.saturating_add(1);
        }

        if superblock.clean {
            if current.incarnation != prior.incarnation
                || current.state != prior.state
                || current.created_seqno != prior.created_seqno
                || current.used != prior.used
            {
                return Err(CheckpointCrossError::Invalid(
                    "clean checkpoint does not exactly match current Region Headers",
                ));
            }
        } else {
            validate_dirty_region_transition(current, prior, header.max_seqno)?;
            verify_dirty_incremental_tail(file, superblock, header, prior, current, &mut scratch)?;
        }

        if header.version >= CHECKPOINT_LANE_VERSION && current.state == RegionState::Active {
            if current.incarnation == prior.incarnation && prior.state == RegionState::Active {
                let lane = usize::from(prior.lane_id.ok_or(CheckpointCrossError::Invalid(
                    "checkpoint Active Region has no append lane",
                ))?);
                let bit = 1_u8 << lane;
                if current_lane_mask & bit != 0 {
                    return Err(CheckpointCrossError::Invalid(
                        "current Active Regions have duplicate append lanes",
                    ));
                }
                current_lane_mask |= bit;
            } else {
                // The runtime can infer a newly activated lane from its records.
                // The offline verifier deliberately declines to claim that path.
                conservative = true;
            }
        }
    }

    validate_region_topology(
        &mut creation_seqnos,
        current_allocated,
        current_active,
        current_oldest,
        superblock.region_count,
    )?;

    creation_seqnos.clear();
    for region_id in 0..header.region_count {
        let prior = read_checkpoint_region(file, payload_offset, header, region_id)?;
        if prior.state != RegionState::Free {
            creation_seqnos.push(prior.created_seqno);
        }
    }
    validate_region_topology(
        &mut creation_seqnos,
        checkpoint_allocated,
        checkpoint_active,
        checkpoint_oldest,
        superblock.region_count,
    )?;

    if current_active != checkpoint_active {
        return Err(CheckpointCrossError::Invalid(
            "current Active Region count differs from checkpoint",
        ));
    }
    if checkpoint_active == 0 || checkpoint_active > MAX_APPEND_LANES {
        return Err(CheckpointCrossError::Invalid(
            "checkpoint append lane count is unsupported",
        ));
    }
    if header.version >= CHECKPOINT_LANE_VERSION {
        let expected_lane_mask = if checkpoint_active == u8::BITS as usize {
            u8::MAX
        } else {
            (1_u8 << checkpoint_active) - 1
        };
        if checkpoint_lane_mask != expected_lane_mask {
            return Err(CheckpointCrossError::Invalid(
                "checkpoint append lanes are incomplete or out of range",
            ));
        }
        if !conservative && current_lane_mask != expected_lane_mask {
            return Err(CheckpointCrossError::Invalid(
                "current Active Region append lanes are incomplete",
            ));
        }
    }

    Ok(if conservative {
        CheckpointCrossValidation::ConservativeFallback
    } else {
        CheckpointCrossValidation::Usable
    })
}

fn read_checkpoint_region(
    file: &mut LockedReadFile,
    payload_offset: u64,
    header: CheckpointSlotHeader,
    region_id: u32,
) -> std::result::Result<CheckpointRegionSnapshot, CheckpointCrossError> {
    let offset = payload_offset
        .checked_add(u64::from(region_id) * CHECKPOINT_REGION_SNAPSHOT_SIZE as u64)
        .ok_or(CheckpointCrossError::Codec(
            CheckpointCodecError::ArithmeticOverflow,
        ))?;
    let mut encoded = [0_u8; CHECKPOINT_REGION_SNAPSHOT_SIZE];
    file.read_exact_at(&mut encoded, offset)
        .map_err(CheckpointCrossError::Io)?;
    decode_region_snapshot(&encoded, header.version).map_err(CheckpointCrossError::Codec)
}

fn read_current_region(
    file: &mut LockedReadFile,
    superblock: Superblock,
    region_id: u32,
) -> std::result::Result<RegionHeader, CheckpointCrossError> {
    let offset = region_offset(superblock, region_id).ok_or(CheckpointCrossError::Invalid(
        "current Region offset overflows",
    ))?;
    let mut encoded = [0_u8; REGION_HEADER_SIZE];
    file.read_exact_at(&mut encoded, offset)
        .map_err(CheckpointCrossError::Io)?;
    let current = RegionHeader::decode(&encoded).ok_or(CheckpointCrossError::Invalid(
        "current Region Header checksum or fields are invalid",
    ))?;
    if current.region_id != region_id || current.used > superblock.region_size {
        return Err(CheckpointCrossError::Invalid(
            "current Region Header metadata is inconsistent",
        ));
    }
    match current.state {
        RegionState::Free
            if current.incarnation == 0
                && current.created_seqno == 0
                && current.used == REGION_HEADER_SIZE as u64 => {}
        RegionState::Free => {
            return Err(CheckpointCrossError::Invalid(
                "current Free Region has non-empty metadata",
            ));
        }
        RegionState::Active | RegionState::Sealed
            if current.incarnation != 0
                && current.created_seqno != 0
                && current.created_seqno != u64::MAX
                && (!superblock.clean || current.created_seqno < superblock.next_seqno) => {}
        RegionState::Active | RegionState::Sealed => {
            return Err(CheckpointCrossError::Invalid(
                "current allocated Region has invalid generation metadata",
            ));
        }
    }
    Ok(current)
}

fn validate_dirty_region_transition(
    current: RegionHeader,
    prior: CheckpointRegionSnapshot,
    checkpoint_max_seqno: u64,
) -> std::result::Result<(), CheckpointCrossError> {
    if current.incarnation < prior.incarnation {
        return Err(CheckpointCrossError::Invalid(
            "Region incarnation moved backwards from checkpoint",
        ));
    }
    if current.incarnation == prior.incarnation {
        let legal_state = matches!(
            (prior.state, current.state),
            (RegionState::Free, RegionState::Free)
                | (RegionState::Active, RegionState::Active)
                | (RegionState::Active, RegionState::Sealed)
                | (RegionState::Sealed, RegionState::Sealed)
        );
        if !legal_state || current.created_seqno != prior.created_seqno || current.used < prior.used
        {
            return Err(CheckpointCrossError::Invalid(
                "Region changed without a new incarnation",
            ));
        }
    } else if current.state == RegionState::Free || current.created_seqno <= checkpoint_max_seqno {
        return Err(CheckpointCrossError::Invalid(
            "reused Region has invalid generation metadata",
        ));
    }
    Ok(())
}

fn verify_dirty_incremental_tail(
    file: &mut LockedReadFile,
    superblock: Superblock,
    checkpoint: CheckpointSlotHeader,
    prior: CheckpointRegionSnapshot,
    current: RegionHeader,
    scratch: &mut [u8; VERIFY_BUFFER_SIZE],
) -> std::result::Result<(), CheckpointCrossError> {
    if current.state == RegionState::Free {
        return Ok(());
    }
    let same_incarnation = current.incarnation == prior.incarnation;
    let mut cursor = if same_incarnation {
        prior.used
    } else {
        REGION_HEADER_SIZE as u64
    };
    let required_end = current.used;
    let scan_active_tail = current.state == RegionState::Active;
    let mut last_seqno = if same_incarnation && prior.max_seqno != 0 {
        Some(prior.max_seqno)
    } else {
        None
    };
    let base = region_offset(superblock, current.region_id).ok_or(
        CheckpointCrossError::Invalid("incremental record offset overflows"),
    )?;

    loop {
        if !scan_active_tail && cursor >= required_end {
            break;
        }
        if cursor >= superblock.region_size {
            if cursor == superblock.region_size {
                break;
            }
            return Err(CheckpointCrossError::Invalid(
                "incremental recovery cursor exceeds Region",
            ));
        }
        if superblock.region_size - cursor < RECORD_HEADER_SIZE as u64 {
            if cursor < required_end {
                return Err(CheckpointCrossError::Invalid(
                    "persisted Region tail cannot contain a record header",
                ));
            }
            break;
        }
        let absolute = base
            .checked_add(cursor)
            .ok_or(CheckpointCrossError::Invalid(
                "incremental record offset overflows",
            ))?;
        let mut encoded = [0_u8; RECORD_HEADER_SIZE];
        file.read_exact_at(&mut encoded, absolute)
            .map_err(CheckpointCrossError::Io)?;
        if encoded.iter().all(|byte| *byte == 0) {
            if cursor < required_end {
                return Err(CheckpointCrossError::Invalid(
                    "zero record appears inside persisted Region extent",
                ));
            }
            break;
        }
        let record = RecordHeader::decode(&encoded).ok_or(CheckpointCrossError::Invalid(
            "non-zero incremental tail has an invalid record header",
        ))?;
        if record.region_incarnation != current.incarnation {
            if cursor < required_end {
                return Err(CheckpointCrossError::Invalid(
                    "persisted record has the wrong Region incarnation",
                ));
            }
            break;
        }
        let record_end = cursor
            .checked_add(u64::from(record.record_len))
            .filter(|end| *end <= superblock.region_size)
            .ok_or(CheckpointCrossError::Invalid(
                "record crosses the recovered Region boundary",
            ))?;
        if cursor < required_end && record_end > required_end {
            return Err(CheckpointCrossError::Invalid(
                "record crosses the persisted Region extent",
            ));
        }
        if record.seqno == u64::MAX
            || record.seqno <= checkpoint.max_seqno
            || record.seqno < current.created_seqno
            || last_seqno.is_some_and(|previous| record.seqno <= previous)
            || record.epoch == 0
            || record.epoch > superblock.epoch
            || (record.epoch == superblock.epoch && record.seqno <= superblock.epoch_start_seqno)
            || (record.epoch < superblock.epoch && record.seqno >= superblock.epoch_start_seqno)
        {
            return Err(CheckpointCrossError::Invalid(
                "incremental record generation metadata is inconsistent",
            ));
        }
        let payload_offset = absolute.checked_add(RECORD_HEADER_SIZE as u64).ok_or(
            CheckpointCrossError::Invalid("incremental record payload offset overflows"),
        )?;
        match verify_record_payload(file, superblock.hash_seed, record, payload_offset, scratch) {
            Ok(None) => {}
            Ok(Some(_)) => {
                return Err(CheckpointCrossError::Invalid(
                    "incremental record payload checksum or key hash does not match",
                ));
            }
            Err(ManagementError::Io(error)) => return Err(CheckpointCrossError::Io(error)),
            Err(_) => {
                return Err(CheckpointCrossError::Invalid(
                    "incremental record payload cannot be verified",
                ));
            }
        }
        last_seqno = Some(record.seqno);
        cursor = record_end;
    }
    if current.state == RegionState::Sealed && cursor != required_end {
        return Err(CheckpointCrossError::Invalid(
            "sealed Region recovery did not reach its persisted cursor",
        ));
    }
    Ok(())
}

fn older_region(current: Option<(u64, u32)>, seqno: u64, region_id: u32) -> Option<(u64, u32)> {
    if current.is_none_or(|oldest| seqno < oldest.0) {
        Some((seqno, region_id))
    } else {
        current
    }
}

fn validate_region_topology(
    creation_seqnos: &mut [u64],
    allocated: u32,
    active: usize,
    oldest: Option<(u64, u32)>,
    region_count: u32,
) -> std::result::Result<(), CheckpointCrossError> {
    creation_seqnos.sort_unstable();
    if creation_seqnos.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CheckpointCrossError::Invalid(
            "Region creation sequence is duplicated",
        ));
    }
    if active == 0 {
        return Err(CheckpointCrossError::Invalid(
            "Region topology has no Active Region",
        ));
    }
    if allocated < region_count && oldest.is_some_and(|(_, region_id)| region_id != 0) {
        return Err(CheckpointCrossError::Invalid(
            "partially allocated FIFO does not start at Region zero",
        ));
    }
    Ok(())
}

fn verify_one_checkpoint_payload(
    file: &mut LockedReadFile,
    directory: CheckpointDirectory,
    header: CheckpointSlotHeader,
) -> std::result::Result<(), CheckpointVerifyError> {
    let payload_offset = directory
        .slot_payload_offset(usize::from(header.slot))
        .map_err(CheckpointVerifyError::Codec)?;
    let padded = padded_payload_len(header.payload_len).map_err(CheckpointVerifyError::Codec)?;
    if payload_offset
        .checked_add(padded)
        .is_none_or(|end| end > file.len)
    {
        return Err(CheckpointVerifyError::Truncated);
    }
    let mut decoder =
        CheckpointPayloadDecoder::new(directory, header).map_err(CheckpointVerifyError::Codec)?;
    let mut region_bytes = [0_u8; CHECKPOINT_REGION_SNAPSHOT_SIZE];
    for region_id in 0..header.region_count {
        let offset = payload_offset
            .checked_add(u64::from(region_id) * CHECKPOINT_REGION_SNAPSHOT_SIZE as u64)
            .ok_or(CheckpointVerifyError::Codec(
                CheckpointCodecError::ArithmeticOverflow,
            ))?;
        file.read_exact_at(&mut region_bytes, offset)
            .map_err(CheckpointVerifyError::Io)?;
        decoder
            .decode_region(&region_bytes)
            .map_err(CheckpointVerifyError::Codec)?;
    }
    let regions_len = u64::from(header.region_count)
        .checked_mul(CHECKPOINT_REGION_SNAPSHOT_SIZE as u64)
        .ok_or(CheckpointVerifyError::Codec(
            CheckpointCodecError::ArithmeticOverflow,
        ))?;
    let entries_offset =
        payload_offset
            .checked_add(regions_len)
            .ok_or(CheckpointVerifyError::Codec(
                CheckpointCodecError::ArithmeticOverflow,
            ))?;
    let entry_size = decoder.index_entry_size();
    let mut entry_bytes = [0_u8; CHECKPOINT_INDEX_ENTRY_SIZE];
    for entry_index in 0..header.entry_count {
        let offset = entries_offset
            .checked_add(u64::from(entry_index) * entry_size as u64)
            .ok_or(CheckpointVerifyError::Codec(
                CheckpointCodecError::ArithmeticOverflow,
            ))?;
        file.read_exact_at(&mut entry_bytes[..entry_size], offset)
            .map_err(CheckpointVerifyError::Io)?;
        let entry = decode_checkpoint_index_entry(&entry_bytes[..entry_size])
            .map_err(CheckpointVerifyError::Codec)?;
        if entry.location.region_id() >= header.region_count {
            return Err(CheckpointVerifyError::Codec(
                CheckpointCodecError::InvalidField("entry_location"),
            ));
        }
        let owner_offset = payload_offset
            .checked_add(
                u64::from(entry.location.region_id()) * CHECKPOINT_REGION_SNAPSHOT_SIZE as u64,
            )
            .ok_or(CheckpointVerifyError::Codec(
                CheckpointCodecError::ArithmeticOverflow,
            ))?;
        file.read_exact_at(&mut region_bytes, owner_offset)
            .map_err(CheckpointVerifyError::Io)?;
        let owner = decode_region_snapshot(&region_bytes, header.version)
            .map_err(CheckpointVerifyError::Codec)?;
        decoder
            .decode_index_entry(&entry_bytes[..entry_size], owner)
            .map_err(CheckpointVerifyError::Codec)?;
    }
    decoder.finish().map_err(CheckpointVerifyError::Codec)
}

fn checkpoint_error_message(error: CheckpointCodecError) -> &'static str {
    match error {
        CheckpointCodecError::ChecksumMismatch => "checkpoint payload checksum does not match",
        CheckpointCodecError::UnsupportedVersion(_) => "checkpoint payload version is unsupported",
        CheckpointCodecError::PayloadTooLarge => "checkpoint payload exceeds its slot",
        CheckpointCodecError::ArithmeticOverflow => "checkpoint payload offset or size overflows",
        CheckpointCodecError::InvalidLength => "checkpoint payload length is invalid",
        CheckpointCodecError::InvalidMagic | CheckpointCodecError::InvalidField(_) => {
            "checkpoint payload fields are invalid"
        }
    }
}

fn add_checkpoint_issue(
    report: &mut VerifyReport,
    component: VerifyComponent,
    offset: u64,
    slot: usize,
    message: &'static str,
) {
    report.issue(VerifyIssue {
        component,
        offset,
        region_id: None,
        checkpoint_slot: Some(slot as u8),
        message,
    });
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::{
        BucketCacheConfig, CacheConfig, CacheError, HybridCacheConfig, PutOptions, PutOutcome,
    };

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);
    const TEST_REGION_SIZE: u64 = 64 * 1024;
    const TEST_REGION_COUNT: u64 = 4;

    struct TestFile(PathBuf);

    impl TestFile {
        fn new(label: &str) -> Self {
            let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "cache-rs-management-{label}-{}-{nonce}.cache",
                std::process::id()
            )))
        }

        fn config(&self) -> CacheConfig {
            CacheConfig::new(
                &self.0,
                SUPERBLOCK_AREA_SIZE + TEST_REGION_COUNT * TEST_REGION_SIZE,
            )
            .with_region_size(TEST_REGION_SIZE)
            .with_index_slots(128)
            .with_max_key_size(128)
            .with_max_value_size(1024)
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn create_cache(file: &TestFile) {
        let cache = file.config().open().unwrap();
        assert_eq!(
            cache
                .put(b"key", b"verified-value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        cache.close().unwrap();
    }

    fn write_superblock(file: &mut File, slot: usize, superblock: Superblock) {
        let offset = [SUPERBLOCK_A_OFFSET, SUPERBLOCK_B_OFFSET][slot];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&superblock.encode()).unwrap();
    }

    fn selected_superblock(path: &Path) -> (usize, Superblock) {
        let mut file = OpenOptions::new().read(true).open(path).unwrap();
        [SUPERBLOCK_A_OFFSET, SUPERBLOCK_B_OFFSET]
            .into_iter()
            .enumerate()
            .filter_map(|(slot, offset)| {
                let mut encoded = [0_u8; SUPERBLOCK_SIZE];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut encoded).unwrap();
                Superblock::decode(&encoded).map(|superblock| (slot, superblock))
            })
            .max_by_key(|(_, superblock)| superblock.generation)
            .unwrap()
    }

    #[test]
    fn inspect_and_verify_a_closed_cache_without_changing_any_bytes() {
        let file = TestFile::new("valid");
        create_cache(&file);
        let before = std::fs::read(&file.0).unwrap();

        let inspected = inspect_cache_file(&file.0).unwrap();
        assert_eq!(inspected.kind, CacheFileKind::FormatV1);
        assert_eq!(inspected.regions.active, 1);
        assert_eq!(inspected.regions.invalid_headers, 0);
        assert_eq!(
            inspected.checkpoint.directory_state,
            CheckpointDirectoryState::Valid
        );

        let verified = verify_cache_file(&file.0).unwrap();
        assert!(verified.valid, "{:?}", verified.issues);
        assert!(verified.safe_to_open);
        assert_eq!(verified.records_verified, 1);
        assert_eq!(verified.values_verified, 1);
        assert_eq!(verified.tombstones_verified, 0);
        assert!(verified.selected_verified_checkpoint.is_some());
        assert_eq!(std::fs::read(&file.0).unwrap(), before);
    }

    #[test]
    fn verify_accepts_every_runtime_append_lane_in_a_clean_checkpoint() {
        let file = TestFile::new("eight-lane-bound");
        let region_count = MAX_APPEND_LANES as u64 + 1;
        let cache = CacheConfig::new(
            &file.0,
            SUPERBLOCK_AREA_SIZE + region_count * TEST_REGION_SIZE,
        )
        .with_region_size(TEST_REGION_SIZE)
        .with_index_slots(128)
        .with_max_key_size(128)
        .with_max_value_size(1024)
        .with_append_lanes(MAX_APPEND_LANES)
        .open()
        .unwrap();
        assert_eq!(
            cache
                .put(b"key", b"verified-value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        cache.close().unwrap();

        let verified = verify_cache_file(&file.0).unwrap();
        assert!(verified.valid, "{:?}", verified.issues);
        assert!(verified.safe_to_open);
        assert_eq!(
            verified.reopen_disposition,
            ReopenDisposition::CleanCheckpoint
        );
        assert!(verified.selected_verified_checkpoint.is_some());
    }

    #[test]
    fn offline_management_refuses_a_live_writer_lock() {
        let file = TestFile::new("locked");
        let cache = file.config().open().unwrap();

        assert!(matches!(
            inspect_cache_file(&file.0),
            Err(ManagementError::Locked)
        ));
        assert!(matches!(
            verify_cache_file(&file.0),
            Err(ManagementError::Locked)
        ));

        cache.close().unwrap();
        assert_eq!(
            inspect_cache_file(&file.0).unwrap().kind,
            CacheFileKind::FormatV1
        );
    }

    #[test]
    fn full_verify_detects_payload_corruption_and_remains_read_only() {
        let file = TestFile::new("payload-corrupt");
        create_cache(&file);
        let key_offset =
            SUPERBLOCK_AREA_SIZE + REGION_HEADER_SIZE as u64 + RECORD_HEADER_SIZE as u64;
        let mut writable = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file.0)
            .unwrap();
        writable.seek(SeekFrom::Start(key_offset)).unwrap();
        let mut byte = [0_u8; 1];
        writable.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x80;
        writable.seek(SeekFrom::Start(key_offset)).unwrap();
        writable.write_all(&byte).unwrap();
        writable.sync_all().unwrap();
        drop(writable);
        let corrupted = std::fs::read(&file.0).unwrap();

        assert!(inspect_cache_file(&file.0).is_ok());
        let report = verify_cache_file(&file.0).unwrap();
        assert!(!report.valid);
        assert!(report.safe_to_open);
        assert!(report.issues.iter().any(|issue| {
            issue.component == VerifyComponent::RecordPayload && issue.message.contains("checksum")
        }));
        assert_eq!(std::fs::read(&file.0).unwrap(), corrupted);
    }

    #[test]
    fn hostile_region_count_is_rejected_before_region_allocation_or_iteration() {
        let file = TestFile::new("hostile-region-count");
        let superblock = Superblock {
            generation: 1,
            region_size: TEST_REGION_SIZE,
            region_count: MAX_REGION_ID + 1,
            epoch: 1,
            epoch_start_seqno: 1,
            next_seqno: 2,
            hash_seed: 7,
            clean: true,
        };
        let mut writable = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&file.0)
            .unwrap();
        writable.set_len(SUPERBLOCK_AREA_SIZE).unwrap();
        write_superblock(&mut writable, 0, superblock);
        write_superblock(&mut writable, 1, superblock);
        writable.sync_all().unwrap();
        drop(writable);

        let inspected = inspect_cache_file(&file.0).unwrap();
        assert_eq!(inspected.kind, CacheFileKind::CorruptV1);
        assert_eq!(inspected.regions.expected, MAX_REGION_ID + 1);
        assert_eq!(inspected.regions.valid_headers, 0);

        let verified = verify_cache_file(&file.0).unwrap();
        assert!(!verified.valid);
        assert!(!verified.safe_to_open);
        assert_eq!(verified.reopen_disposition, ReopenDisposition::Refused);
        assert_eq!(verified.regions_verified, 0);
    }

    #[test]
    fn short_declared_data_extent_is_never_reported_as_data_valid() {
        let file = TestFile::new("short-data-extent");
        create_cache(&file);
        let data_file_len = inspect_cache_file(&file.0).unwrap().data_file_len.unwrap();
        let writable = OpenOptions::new().write(true).open(&file.0).unwrap();
        writable.set_len(data_file_len - 1).unwrap();
        writable.sync_all().unwrap();
        drop(writable);

        let report = verify_cache_file(&file.0).unwrap();
        assert!(!report.valid);
        assert!(report.safe_to_open);
        assert_eq!(report.reopen_disposition, ReopenDisposition::SafeEmpty);
        assert_eq!(report.regions_verified, 0);
        assert!(report.issues.iter().any(|issue| {
            issue.component == VerifyComponent::Layout
                && issue
                    .message
                    .contains("shorter than its declared data extent")
        }));
    }

    #[test]
    fn epoch_advanced_dirty_lineage_requires_two_superblock_generations() {
        let checkpoint = CheckpointSlotHeader {
            version: CHECKPOINT_LANE_VERSION,
            slot: 0,
            generation: 5,
            payload_len: 0,
            region_count: 1,
            entry_count: 0,
            epoch: 1,
            epoch_start_seqno: 1,
            max_seqno: 9,
            superblock_generation: 5,
            hash_seed: 7,
            payload_crc: 0,
            index_slots: None,
            index_shards: None,
        };
        let mut dirty = Superblock {
            generation: 6,
            region_size: TEST_REGION_SIZE,
            region_count: 1,
            epoch: 2,
            epoch_start_seqno: 10,
            next_seqno: 11,
            hash_seed: 7,
            clean: false,
        };

        assert!(!checkpoint_matches_superblock(checkpoint, dirty));
        dirty.generation = 7;
        assert!(checkpoint_matches_superblock(checkpoint, dirty));
    }

    #[test]
    fn dirty_verify_refuses_superblock_generation_overflow() {
        let file = TestFile::new("dirty-superblock-generation-overflow");
        create_cache(&file);
        let (selected_slot, clean) = selected_superblock(&file.0);
        let mut writable = OpenOptions::new().write(true).open(&file.0).unwrap();
        write_superblock(
            &mut writable,
            1 - selected_slot,
            Superblock {
                generation: u64::MAX,
                clean: false,
                ..clean
            },
        );
        writable.sync_all().unwrap();
        drop(writable);

        let report = verify_cache_file(&file.0).unwrap();
        assert!(!report.valid);
        assert!(!report.safe_to_open);
        assert_eq!(report.selected_verified_checkpoint, None);
        assert_eq!(report.reopen_disposition, ReopenDisposition::Refused);
        assert!(report.issues.iter().any(|issue| {
            issue.component == VerifyComponent::Superblock
                && issue.message.contains("cannot publish a clean checkpoint")
        }));
        let before_open = std::fs::read(&file.0).unwrap();
        assert!(matches!(
            file.config().open(),
            Err(CacheError::CorruptMetadata(_))
        ));
        assert_eq!(std::fs::read(&file.0).unwrap(), before_open);

        let mut writable = OpenOptions::new().write(true).open(&file.0).unwrap();
        write_superblock(
            &mut writable,
            1 - selected_slot,
            Superblock {
                generation: u64::MAX - 1,
                clean: false,
                ..clean
            },
        );
        writable.sync_all().unwrap();
        drop(writable);

        let safe_empty_overflow = verify_cache_file(&file.0).unwrap();
        assert!(!safe_empty_overflow.valid);
        assert!(!safe_empty_overflow.safe_to_open);
        assert_eq!(safe_empty_overflow.selected_verified_checkpoint, None);
        assert_eq!(
            safe_empty_overflow.reopen_disposition,
            ReopenDisposition::Refused
        );
        assert!(safe_empty_overflow.issues.iter().any(|issue| {
            issue.component == VerifyComponent::Superblock
                && issue
                    .message
                    .contains("cannot complete safe-empty formatting")
        }));
        let before_open = std::fs::read(&file.0).unwrap();
        assert!(matches!(
            file.config().open(),
            Err(CacheError::CorruptMetadata(_))
        ));
        assert_eq!(std::fs::read(&file.0).unwrap(), before_open);
    }

    #[test]
    fn clean_checkpoint_must_exactly_match_current_region_headers() {
        let file = TestFile::new("checkpoint-region-mismatch");
        create_cache(&file);
        let mut writable = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file.0)
            .unwrap();
        let mut encoded = [0_u8; REGION_HEADER_SIZE];
        writable
            .seek(SeekFrom::Start(SUPERBLOCK_AREA_SIZE))
            .unwrap();
        writable.read_exact(&mut encoded).unwrap();
        let mut region = RegionHeader::decode(&encoded).unwrap();
        assert_eq!(region.state, RegionState::Active);
        region.used = REGION_HEADER_SIZE as u64;
        writable
            .seek(SeekFrom::Start(SUPERBLOCK_AREA_SIZE))
            .unwrap();
        writable.write_all(&region.encode()).unwrap();
        writable.sync_all().unwrap();
        drop(writable);

        let report = verify_cache_file(&file.0).unwrap();
        assert!(!report.valid);
        assert_eq!(report.selected_verified_checkpoint, None);
        assert_eq!(report.reopen_disposition, ReopenDisposition::CleanFullScan);
        assert!(report.issues.iter().any(|issue| {
            issue.component == VerifyComponent::CheckpointPayload
                && issue.message.contains("does not exactly match")
        }));
    }

    #[test]
    fn dirty_incremental_verifies_active_tail_beyond_persisted_cursor() {
        let file = TestFile::new("dirty-incremental-tail");
        create_cache(&file);
        let (selected_slot, clean) = selected_superblock(&file.0);
        let mut writable = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file.0)
            .unwrap();

        let mut region_bytes = [0_u8; REGION_HEADER_SIZE];
        writable
            .seek(SeekFrom::Start(SUPERBLOCK_AREA_SIZE))
            .unwrap();
        writable.read_exact(&mut region_bytes).unwrap();
        let region = RegionHeader::decode(&region_bytes).unwrap();
        assert_eq!(region.state, RegionState::Active);

        let first_record_offset = SUPERBLOCK_AREA_SIZE + REGION_HEADER_SIZE as u64;
        let mut first_header_bytes = [0_u8; RECORD_HEADER_SIZE];
        writable.seek(SeekFrom::Start(first_record_offset)).unwrap();
        writable.read_exact(&mut first_header_bytes).unwrap();
        let mut tail_header = RecordHeader::decode(&first_header_bytes).unwrap();
        let mut tail_record = vec![0_u8; tail_header.record_len as usize];
        writable.seek(SeekFrom::Start(first_record_offset)).unwrap();
        writable.read_exact(&mut tail_record).unwrap();
        tail_header.seqno = clean.next_seqno;
        tail_header.epoch = clean.epoch;
        tail_record[..RECORD_HEADER_SIZE].copy_from_slice(&tail_header.encode());
        let tail_offset = SUPERBLOCK_AREA_SIZE + region.used;
        writable.seek(SeekFrom::Start(tail_offset)).unwrap();
        writable.write_all(&tail_record).unwrap();

        let dirty = Superblock {
            generation: clean.generation + 1,
            clean: false,
            ..clean
        };
        write_superblock(&mut writable, 1 - selected_slot, dirty);
        writable.sync_all().unwrap();
        drop(writable);

        let report = verify_cache_file(&file.0).unwrap();
        assert!(report.valid, "{:?}", report.issues);
        assert_eq!(
            report.reopen_disposition,
            ReopenDisposition::DirtyIncremental
        );
        assert!(report.selected_verified_checkpoint.is_some());

        // Runtime recovery must derive `next_seqno = max_seqno + 1`. Verify
        // must reject the same overflow instead of promising an incremental
        // reopen that will actually fall back to an empty cache.
        tail_header.seqno = u64::MAX;
        tail_record[..RECORD_HEADER_SIZE].copy_from_slice(&tail_header.encode());
        let mut writable = OpenOptions::new().write(true).open(&file.0).unwrap();
        writable.seek(SeekFrom::Start(tail_offset)).unwrap();
        writable.write_all(&tail_record).unwrap();
        writable.sync_all().unwrap();
        drop(writable);

        let overflow = verify_cache_file(&file.0).unwrap();
        assert!(!overflow.valid);
        assert_eq!(overflow.selected_verified_checkpoint, None);
        assert_eq!(overflow.reopen_disposition, ReopenDisposition::SafeEmpty);
        assert!(overflow.issues.iter().any(|issue| {
            issue.component == VerifyComponent::RecordHeader
                && issue.message.contains("generation or sequence")
        }));

        let mut writable = OpenOptions::new().write(true).open(&file.0).unwrap();
        tail_header.seqno = clean.next_seqno;
        tail_record[..RECORD_HEADER_SIZE].copy_from_slice(&tail_header.encode());
        writable.seek(SeekFrom::Start(tail_offset)).unwrap();
        writable.write_all(&tail_record).unwrap();
        writable.seek(SeekFrom::Start(tail_offset)).unwrap();
        writable.write_all(b"X").unwrap();
        writable.sync_all().unwrap();
        drop(writable);

        let torn = verify_cache_file(&file.0).unwrap();
        assert!(!torn.valid);
        assert_eq!(torn.selected_verified_checkpoint, None);
        assert_eq!(torn.reopen_disposition, ReopenDisposition::SafeEmpty);
        assert!(torn.issues.iter().any(|issue| {
            issue.component == VerifyComponent::CheckpointPayload
                && issue.message.contains("non-zero incremental tail")
        }));
    }

    #[test]
    fn dirty_verify_rejects_region_creation_sequence_overflow() {
        let file = TestFile::new("dirty-region-sequence-overflow");
        create_cache(&file);
        let (selected_slot, clean) = selected_superblock(&file.0);
        let mut writable = OpenOptions::new().write(true).open(&file.0).unwrap();

        // Region one is Free in the clean checkpoint. Make it a structurally
        // plausible new incarnation whose creation sequence leaves no valid
        // `next_seqno`; runtime recovery must fall back to an empty cache.
        let overflow_region = RegionHeader {
            region_id: 1,
            incarnation: 1,
            state: RegionState::Sealed,
            created_seqno: u64::MAX,
            used: REGION_HEADER_SIZE as u64,
        };
        writable
            .seek(SeekFrom::Start(SUPERBLOCK_AREA_SIZE + clean.region_size))
            .unwrap();
        writable.write_all(&overflow_region.encode()).unwrap();
        write_superblock(
            &mut writable,
            1 - selected_slot,
            Superblock {
                generation: clean.generation + 1,
                clean: false,
                ..clean
            },
        );
        writable.sync_all().unwrap();
        drop(writable);

        let report = verify_cache_file(&file.0).unwrap();
        assert!(!report.valid);
        assert_eq!(report.selected_verified_checkpoint, None);
        assert_eq!(report.reopen_disposition, ReopenDisposition::SafeEmpty);
        assert!(report.issues.iter().any(|issue| {
            issue.component == VerifyComponent::RegionHeader && issue.region_id == Some(1)
        }));
    }

    #[test]
    fn dirty_verify_checks_checkpoint_covered_record_payloads() {
        let file = TestFile::new("dirty-checkpoint-covered-payload");
        create_cache(&file);
        let (selected_slot, clean) = selected_superblock(&file.0);
        let mut writable = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file.0)
            .unwrap();

        let mut region_bytes = [0_u8; REGION_HEADER_SIZE];
        writable
            .seek(SeekFrom::Start(SUPERBLOCK_AREA_SIZE))
            .unwrap();
        writable.read_exact(&mut region_bytes).unwrap();
        let region = RegionHeader::decode(&region_bytes).unwrap();
        assert_eq!(region.state, RegionState::Active);

        let first_record_offset = SUPERBLOCK_AREA_SIZE + REGION_HEADER_SIZE as u64;
        let mut first_header_bytes = [0_u8; RECORD_HEADER_SIZE];
        writable.seek(SeekFrom::Start(first_record_offset)).unwrap();
        writable.read_exact(&mut first_header_bytes).unwrap();
        let mut tail_header = RecordHeader::decode(&first_header_bytes).unwrap();
        let mut tail_record = vec![0_u8; tail_header.record_len as usize];
        writable.seek(SeekFrom::Start(first_record_offset)).unwrap();
        writable.read_exact(&mut tail_record).unwrap();
        tail_header.seqno = clean.next_seqno;
        tail_header.epoch = clean.epoch;
        tail_record[..RECORD_HEADER_SIZE].copy_from_slice(&tail_header.encode());
        let tail_offset = SUPERBLOCK_AREA_SIZE + region.used;
        writable.seek(SeekFrom::Start(tail_offset)).unwrap();
        writable.write_all(&tail_record).unwrap();

        let dirty = Superblock {
            generation: clean.generation + 1,
            clean: false,
            ..clean
        };
        write_superblock(&mut writable, 1 - selected_slot, dirty);

        let old_payload_offset = first_record_offset + RECORD_HEADER_SIZE as u64;
        writable.seek(SeekFrom::Start(old_payload_offset)).unwrap();
        let mut corrupted = [0_u8; 1];
        writable.read_exact(&mut corrupted).unwrap();
        corrupted[0] ^= 0xff;
        writable.seek(SeekFrom::Start(old_payload_offset)).unwrap();
        writable.write_all(&corrupted).unwrap();
        writable.sync_all().unwrap();
        drop(writable);

        let report = verify_cache_file(&file.0).unwrap();
        assert!(!report.valid);
        assert!(report.selected_verified_checkpoint.is_some());
        assert_eq!(
            report.reopen_disposition,
            ReopenDisposition::DirtyIncremental
        );
        assert!(report.issues.iter().any(|issue| {
            issue.component == VerifyComponent::RecordPayload && issue.message.contains("checksum")
        }));
    }

    #[test]
    fn checkpoint_entry_with_out_of_range_region_is_structurally_invalid() {
        let file = TestFile::new("checkpoint-entry-region");
        create_cache(&file);
        let inspected = inspect_cache_file(&file.0).unwrap();
        let data_file_len = inspected.data_file_len.unwrap();
        let slot = usize::from(inspected.checkpoint.selected_header_slot.unwrap());
        let mut writable = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file.0)
            .unwrap();

        let mut directory_bytes = [0_u8; CHECKPOINT_DIRECTORY_SIZE];
        writable.seek(SeekFrom::Start(data_file_len)).unwrap();
        writable.read_exact(&mut directory_bytes).unwrap();
        let directory = CheckpointDirectory::decode(&directory_bytes).unwrap();
        let header_offset = directory.slot_header_offset(slot).unwrap();
        let mut header_bytes = [0_u8; CHECKPOINT_SLOT_HEADER_SIZE];
        writable.seek(SeekFrom::Start(header_offset)).unwrap();
        writable.read_exact(&mut header_bytes).unwrap();
        let mut header = CheckpointSlotHeader::decode(&header_bytes, directory, slot).unwrap();
        assert!(header.entry_count != 0);

        let payload_offset = directory.slot_payload_offset(slot).unwrap();
        let mut payload = vec![0_u8; header.payload_len as usize];
        writable.seek(SeekFrom::Start(payload_offset)).unwrap();
        writable.read_exact(&mut payload).unwrap();
        let entry_offset = header.region_count as usize * CHECKPOINT_REGION_SNAPSHOT_SIZE;
        let impossible_owner = crate::index::PackedLocation::new(
            header.region_count,
            REGION_HEADER_SIZE as u32,
            96,
            false,
        )
        .unwrap();
        payload[entry_offset + 8..entry_offset + 16]
            .copy_from_slice(&impossible_owner.raw().to_le_bytes());
        header.payload_crc = crate::checksum::crc32c(&payload);
        writable.seek(SeekFrom::Start(payload_offset)).unwrap();
        writable.write_all(&payload).unwrap();
        writable.seek(SeekFrom::Start(header_offset)).unwrap();
        writable
            .write_all(&header.encode(directory).unwrap())
            .unwrap();
        writable.sync_all().unwrap();
        drop(writable);

        let report = verify_cache_file(&file.0).unwrap();
        assert!(!report.valid);
        assert_eq!(report.selected_verified_checkpoint, None);
        assert!(report.issues.iter().any(|issue| {
            issue.component == VerifyComponent::CheckpointPayload
                && issue.checkpoint_slot == Some(slot as u8)
                && issue.message.contains("fields are invalid")
        }));
    }

    #[test]
    fn hybrid_offline_verify_covers_all_three_files_and_bucket_pages() {
        let bucket = TestFile::new("hybrid-bucket");
        let region = TestFile::new("hybrid-region");
        let manifest = TestFile::new("hybrid-manifest");
        let bucket_config = BucketCacheConfig::new(&bucket.0, 4 * 4096)
            .with_buffer_slots(1)
            .with_memory_budget(1024 * 1024);
        let region_config = CacheConfig::new(
            &region.0,
            SUPERBLOCK_AREA_SIZE + TEST_REGION_COUNT * TEST_REGION_SIZE,
        )
        .with_region_size(TEST_REGION_SIZE)
        .with_index_slots(128)
        .with_max_key_size(128)
        .with_max_value_size(4096)
        .with_memory_budget(16 * 1024 * 1024);
        let cache = HybridCacheConfig::new(16 * 1024, bucket_config, region_config)
            .with_memory_shards(4)
            .with_small_object_max(512)
            .with_manifest_path(&manifest.0)
            .with_journal_capacity(64 * 1024)
            .open()
            .unwrap();
        assert_eq!(
            cache
                .put(b"small", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache
                .put(b"large", vec![7_u8; 1024], PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        cache.close().unwrap();

        let inspected = inspect_hybrid_cache_files(&bucket.0, &region.0, &manifest.0).unwrap();
        assert!(inspected.valid);
        let verified = verify_hybrid_cache_files(&bucket.0, &region.0, &manifest.0).unwrap();
        assert!(verified.valid);
        assert!(verified.safe_to_open);
        assert!(verified.bucket.entries_verified >= 1);
        assert!(verified.region.records_verified >= 1);

        let mut writable = OpenOptions::new().write(true).open(&bucket.0).unwrap();
        writable
            .seek(SeekFrom::Start(BUCKET_DATA_OFFSET + 100))
            .unwrap();
        writable.write_all(b"X").unwrap();
        writable.sync_all().unwrap();
        drop(writable);
        let damaged = verify_hybrid_cache_files(&bucket.0, &region.0, &manifest.0).unwrap();
        assert!(!damaged.valid);
        assert_eq!(damaged.bucket.invalid_buckets, 1);
    }
}
