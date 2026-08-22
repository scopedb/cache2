//! Global identity, version allocation, and bounded intent journal for one
//! Hybrid cache pair.
//!
//! The first two independently checksummed 4 KiB pages are the manifest. The
//! remainder is a fixed-capacity append-only journal generation. A caller must
//! durably append an intent before either disk engine publishes a mutation.
//! Once both lower engines are clean, [`HybridManifest::publish_clean`] moves
//! both manifest slots to a new journal generation; only then may offset zero
//! be reused. This ordering leaves either the old manifest plus its journal or
//! a new clean checkpoint after every interruption.

use std::fmt;
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::cache::{CacheError, CacheStatus, Result};
use crate::checksum::{Crc32c, crc32c};
#[cfg(test)]
use crate::hybrid_crash::{HybridCrashPoint, hit as crash_hit};
use crate::io_backend::{
    DirectIoMode, FileBackend, IoBackend, SyncMode, SyncPoint, WritePoint, read_exact_at,
    write_all_at,
};
use crate::policy::{HostWriteKind, HostWriteTracker, NamespaceId, NamespaceUsage};
use crate::resources::OverloadReason;

const MANIFEST_SLOT_SIZE: usize = 4 * 1024;
const MANIFEST_SLOT_COUNT: usize = 2;
const JOURNAL_OFFSET: u64 = (MANIFEST_SLOT_SIZE * MANIFEST_SLOT_COUNT) as u64;
const MANIFEST_MAGIC: [u8; 8] = *b"CRHYBM01";
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_HEADER_SIZE: u16 = 120;
const MANIFEST_CRC_OFFSET: usize = MANIFEST_SLOT_SIZE - size_of::<u32>();

// Format V1 intentionally leaves the bytes after the 120-byte base header
// reserved.  The namespace usage checkpoint is a backwards-compatible V1
// extension: an all-zero extension is the legacy "not present" value, while a
// populated extension has its own checksum in addition to the enclosing slot
// checksum.  Keeping entries fixed-width makes recovery memory and decode work
// independent of SSD capacity.
const USAGE_EXTENSION_OFFSET: usize = 128;
const USAGE_EXTENSION_MAGIC: [u8; 8] = *b"CRHYUS01";
const USAGE_EXTENSION_VERSION: u16 = 1;
const USAGE_EXTENSION_HEADER_SIZE: u16 = 24;
const USAGE_EXTENSION_VERSION_OFFSET: usize = USAGE_EXTENSION_OFFSET + 8;
const USAGE_EXTENSION_HEADER_SIZE_OFFSET: usize = USAGE_EXTENSION_OFFSET + 10;
const USAGE_EXTENSION_COUNT_OFFSET: usize = USAGE_EXTENSION_OFFSET + 12;
const USAGE_EXTENSION_ENTRY_SIZE_OFFSET: usize = USAGE_EXTENSION_OFFSET + 14;
const USAGE_EXTENSION_CRC_OFFSET: usize = USAGE_EXTENSION_OFFSET + 16;
const USAGE_EXTENSION_ENTRIES_OFFSET: usize =
    USAGE_EXTENSION_OFFSET + USAGE_EXTENSION_HEADER_SIZE as usize;
const USAGE_EXTENSION_ENTRY_SIZE: usize = 16;
pub(crate) const MAX_MANIFEST_NAMESPACE_USAGES: usize = 240;

pub(crate) const DEFAULT_JOURNAL_CAPACITY: u64 = 16 * 1024 * 1024;
const MIN_JOURNAL_CAPACITY: u64 = 64 * 1024;
const MAX_JOURNAL_CAPACITY: u64 = 4 * 1024 * 1024 * 1024;
const JOURNAL_CAPACITY_ALIGNMENT: u64 = MANIFEST_SLOT_SIZE as u64;

const VERSION_OFFSET: usize = 8;
const HEADER_SIZE_OFFSET: usize = 10;
const GENERATION_OFFSET: usize = 16;
const CACHE_ID_OFFSET: usize = 24;
const VERSION_EPOCH_OFFSET: usize = 40;
const NEXT_SEQNO_OFFSET: usize = 48;
const LAYOUT_FINGERPRINT_OFFSET: usize = 56;
const JOURNAL_GENERATION_OFFSET: usize = 64;
const JOURNAL_CAPACITY_OFFSET: usize = 72;
const CHECKPOINT_EPOCH_OFFSET: usize = 80;
const CHECKPOINT_SEQNO_OFFSET: usize = 88;
const CLEAR_FLOOR_EPOCH_OFFSET: usize = 96;
const CLEAR_FLOOR_SEQNO_OFFSET: usize = 104;
const CLEAN_OFFSET: usize = 112;

const JOURNAL_MAGIC: [u8; 8] = *b"CRHYJR01";
const JOURNAL_VERSION: u16 = 1;
const JOURNAL_HEADER_SIZE: usize = 80;
pub(crate) const JOURNAL_COMMIT_SENTINEL_BYTES: usize = JOURNAL_HEADER_SIZE;
const JOURNAL_ALIGNMENT: usize = 32;
const MIN_JOURNAL_RECORD_SIZE: u64 = 96;
const JOURNAL_SCAN_CHUNK_BYTES: usize = 64 * 1024;
const JOURNAL_VERSION_OFFSET: usize = 8;
const JOURNAL_HEADER_SIZE_OFFSET: usize = 10;
const JOURNAL_RECORD_LEN_OFFSET: usize = 12;
const JOURNAL_KIND_OFFSET: usize = 16;
const JOURNAL_FLAGS_OFFSET: usize = 17;
const JOURNAL_GENERATION_FIELD_OFFSET: usize = 24;
const JOURNAL_EPOCH_OFFSET: usize = 32;
const JOURNAL_SEQNO_OFFSET: usize = 40;
const JOURNAL_NAMESPACE_OFFSET: usize = 48;
const JOURNAL_KEY_LEN_OFFSET: usize = 52;
const JOURNAL_KEY_HASH_OFFSET: usize = 56;
const JOURNAL_BUCKET_ID_OFFSET: usize = 64;
const JOURNAL_CRC_OFFSET: usize = 72;
const JOURNAL_FLAG_TOUCHES_BUCKET: u8 = 1;

static CACHE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct HybridVersion {
    pub(crate) epoch: u64,
    pub(crate) seqno: u64,
}

impl HybridVersion {
    pub(crate) const ZERO: Self = Self { epoch: 0, seqno: 0 };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum JournalIntentKind {
    PutBucket = 1,
    PutRegion = 2,
    Delete = 3,
    Clear = 4,
}

impl JournalIntentKind {
    fn decode(encoded: u8) -> Option<Self> {
        match encoded {
            1 => Some(Self::PutBucket),
            2 => Some(Self::PutRegion),
            3 => Some(Self::Delete),
            4 => Some(Self::Clear),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JournalIntentInput<'a> {
    pub(crate) kind: JournalIntentKind,
    pub(crate) namespace: NamespaceId,
    pub(crate) key_hash: u64,
    /// Bucket page that the subsequent transaction may update. This is present
    /// for Bucket targets/removals and Region transitions with a Bucket source.
    pub(crate) bucket_id: Option<u64>,
    pub(crate) key: &'a [u8],
}

pub(crate) struct JournalWaveCommit {
    pub(crate) versions: Vec<HybridVersion>,
    pub(crate) sync_elapsed: Duration,
}

impl JournalIntentInput<'static> {
    fn clear() -> Self {
        Self {
            kind: JournalIntentKind::Clear,
            namespace: 0,
            key_hash: 0,
            bucket_id: None,
            key: &[],
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JournalIntent {
    pub(crate) kind: JournalIntentKind,
    pub(crate) version: HybridVersion,
    pub(crate) namespace: NamespaceId,
    pub(crate) key_hash: u64,
    pub(crate) bucket_id: Option<u64>,
    pub(crate) key: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JournalIntentRef<'a> {
    pub(crate) kind: JournalIntentKind,
    pub(crate) version: HybridVersion,
    pub(crate) namespace: NamespaceId,
    pub(crate) key_hash: u64,
    pub(crate) bucket_id: Option<u64>,
    pub(crate) key: &'a [u8],
}

#[derive(Default, Eq, PartialEq)]
pub(crate) struct JournalIntents {
    // One allocation holds the exact encoded prefix selected by the first
    // streaming pass. Offsets point into this immutable buffer, so recovery
    // never allocates one Vec per key.
    encoded: Vec<u8>,
    offsets: Vec<u32>,
}

impl JournalIntents {
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.offsets.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = JournalIntentRef<'_>> + '_ {
        self.offsets
            .iter()
            .map(|offset| self.intent_at_offset(*offset))
    }

    #[cfg(test)]
    pub(crate) fn get(&self, index: usize) -> Option<JournalIntentRef<'_>> {
        self.offsets
            .get(index)
            .map(|offset| self.intent_at_offset(*offset))
    }

    pub(crate) fn sort_and_dedup_keys(&mut self) {
        let encoded = self.encoded.as_slice();
        self.offsets.sort_unstable_by(|left, right| {
            let left = trusted_intent_at(encoded, *left);
            let right = trusted_intent_at(encoded, *right);
            left.namespace
                .cmp(&right.namespace)
                .then_with(|| left.key.cmp(right.key))
                .then_with(|| left.version.cmp(&right.version))
        });
        self.offsets.dedup_by(|later, earlier| {
            let later = trusted_intent_at(encoded, *later);
            let earlier = trusted_intent_at(encoded, *earlier);
            later.namespace == earlier.namespace && later.key == earlier.key
        });
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.encoded
            .capacity()
            .saturating_add(self.offsets.capacity().saturating_mul(size_of::<u32>()))
    }

    fn intent_at_offset(&self, offset: u32) -> JournalIntentRef<'_> {
        trusted_intent_at(&self.encoded, offset)
    }

    #[cfg(test)]
    fn to_owned_vec(&self) -> Vec<JournalIntent> {
        self.iter()
            .map(|intent| JournalIntent {
                kind: intent.kind,
                version: intent.version,
                namespace: intent.namespace,
                key_hash: intent.key_hash,
                bucket_id: intent.bucket_id,
                key: intent.key.to_vec(),
            })
            .collect()
    }
}

impl fmt::Debug for JournalIntents {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JournalIntents")
            .field("count", &self.offsets.len())
            .field("encoded_bytes", &self.encoded.len())
            .field("allocated_bytes", &self.allocated_bytes())
            .finish()
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct JournalScan {
    pub(crate) intents: JournalIntents,
    pub(crate) intent_count: u64,
    pub(crate) valid_bytes: u64,
    first_version: Option<HybridVersion>,
    highest_version: HybridVersion,
    highest_clear_version: HybridVersion,
    pub(crate) contains_clear: bool,
    /// A non-zero suffix that was not a valid record in the selected journal
    /// generation.
    pub(crate) ignored_torn_tail: bool,
    /// The selected generation did not end in the zero sentinel written by a
    /// successful journal sync. Recovery must conservatively reset both tiers
    /// instead of attempting per-key reconciliation.
    pub(crate) requires_full_clear: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ManifestSnapshot {
    pub(crate) cache_id: [u8; 16],
    pub(crate) version_epoch: u64,
    pub(crate) next_seqno: u64,
    pub(crate) checkpoint_version: HybridVersion,
    pub(crate) clear_floor: HybridVersion,
    pub(crate) journal_generation: u64,
    pub(crate) journal_capacity: u64,
    pub(crate) journal_bytes: u64,
    pub(crate) clean: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ManifestOpenState {
    pub(crate) created: bool,
    pub(crate) needs_recovery: bool,
    pub(crate) journal: JournalScan,
}

#[derive(Clone, Copy)]
struct ManifestSlot {
    generation: u64,
    cache_id: [u8; 16],
    version_epoch: u64,
    next_seqno: u64,
    layout_fingerprint: u64,
    journal_generation: u64,
    journal_capacity: u64,
    checkpoint_version: HybridVersion,
    clear_floor: HybridVersion,
    clean: bool,
    namespace_usage: NamespaceUsageCheckpoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NamespaceUsageCheckpoint {
    present: bool,
    count: u16,
    entries: [NamespaceUsage; MAX_MANIFEST_NAMESPACE_USAGES],
}

impl NamespaceUsageCheckpoint {
    const EMPTY_ENTRY: NamespaceUsage = NamespaceUsage {
        namespace: 0,
        live_bytes: 0,
    };

    const fn absent() -> Self {
        Self {
            present: false,
            count: 0,
            entries: [Self::EMPTY_ENTRY; MAX_MANIFEST_NAMESPACE_USAGES],
        }
    }

    fn try_from_usage(usage: &[NamespaceUsage]) -> Result<Self> {
        if usage.len() > MAX_MANIFEST_NAMESPACE_USAGES {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid namespace count exceeds manifest checkpoint limit {MAX_MANIFEST_NAMESPACE_USAGES}"
            )));
        }
        if usage
            .windows(2)
            .any(|pair| pair[0].namespace >= pair[1].namespace)
        {
            return Err(CacheError::InvalidConfig(
                "hybrid namespace usage checkpoint must be sorted and unique".into(),
            ));
        }
        let mut checkpoint = Self {
            present: true,
            count: u16::try_from(usage.len()).expect("manifest usage limit fits u16"),
            entries: [Self::EMPTY_ENTRY; MAX_MANIFEST_NAMESPACE_USAGES],
        };
        checkpoint.entries[..usage.len()].copy_from_slice(usage);
        Ok(checkpoint)
    }

    fn as_slice(&self) -> &[NamespaceUsage] {
        &self.entries[..usize::from(self.count)]
    }

    fn encode_into(&self, output: &mut [u8; MANIFEST_SLOT_SIZE]) {
        if !self.present {
            return;
        }
        output[USAGE_EXTENSION_OFFSET..USAGE_EXTENSION_OFFSET + 8]
            .copy_from_slice(&USAGE_EXTENSION_MAGIC);
        put_u16(
            output,
            USAGE_EXTENSION_VERSION_OFFSET,
            USAGE_EXTENSION_VERSION,
        );
        put_u16(
            output,
            USAGE_EXTENSION_HEADER_SIZE_OFFSET,
            USAGE_EXTENSION_HEADER_SIZE,
        );
        put_u16(output, USAGE_EXTENSION_COUNT_OFFSET, self.count);
        put_u16(
            output,
            USAGE_EXTENSION_ENTRY_SIZE_OFFSET,
            USAGE_EXTENSION_ENTRY_SIZE as u16,
        );
        for (index, usage) in self.as_slice().iter().enumerate() {
            let offset = USAGE_EXTENSION_ENTRIES_OFFSET + index * USAGE_EXTENSION_ENTRY_SIZE;
            put_u32(output, offset, usage.namespace);
            put_u64(output, offset + 8, usage.live_bytes);
        }
        let checksum = crc32c(&output[USAGE_EXTENSION_OFFSET..MANIFEST_CRC_OFFSET]);
        put_u32(output, USAGE_EXTENSION_CRC_OFFSET, checksum);
    }

    fn decode(input: &[u8]) -> Option<Self> {
        let extension = input.get(USAGE_EXTENSION_OFFSET..MANIFEST_CRC_OFFSET)?;
        if extension.iter().all(|byte| *byte == 0) {
            return Some(Self::absent());
        }
        if input.get(USAGE_EXTENSION_OFFSET..USAGE_EXTENSION_OFFSET + 8)? != USAGE_EXTENSION_MAGIC
            || get_u16(input, USAGE_EXTENSION_VERSION_OFFSET)? != USAGE_EXTENSION_VERSION
            || get_u16(input, USAGE_EXTENSION_HEADER_SIZE_OFFSET)? != USAGE_EXTENSION_HEADER_SIZE
            || get_u16(input, USAGE_EXTENSION_ENTRY_SIZE_OFFSET)?
                != USAGE_EXTENSION_ENTRY_SIZE as u16
            || !fixed_checksum_matches_extension(
                input,
                USAGE_EXTENSION_OFFSET,
                MANIFEST_CRC_OFFSET,
                USAGE_EXTENSION_CRC_OFFSET,
            )
        {
            return None;
        }
        let count = get_u16(input, USAGE_EXTENSION_COUNT_OFFSET)?;
        if usize::from(count) > MAX_MANIFEST_NAMESPACE_USAGES {
            return None;
        }
        let used_end = USAGE_EXTENSION_ENTRIES_OFFSET
            .checked_add(usize::from(count).checked_mul(USAGE_EXTENSION_ENTRY_SIZE)?)?;
        if used_end > MANIFEST_CRC_OFFSET
            || input
                .get(used_end..MANIFEST_CRC_OFFSET)?
                .iter()
                .any(|byte| *byte != 0)
        {
            return None;
        }
        let mut checkpoint = Self {
            present: true,
            count,
            entries: [Self::EMPTY_ENTRY; MAX_MANIFEST_NAMESPACE_USAGES],
        };
        for index in 0..usize::from(count) {
            let offset = USAGE_EXTENSION_ENTRIES_OFFSET + index * USAGE_EXTENSION_ENTRY_SIZE;
            let namespace = get_u32(input, offset)?;
            if get_u32(input, offset + 4)? != 0 {
                return None;
            }
            checkpoint.entries[index] = NamespaceUsage {
                namespace,
                live_bytes: get_u64(input, offset + 8)?,
            };
            if index != 0 && checkpoint.entries[index - 1].namespace >= namespace {
                return None;
            }
        }
        Some(checkpoint)
    }
}

// Keeping the fixed, bounded checkpoint inline avoids a fallible heap
// allocation while probing the two slots during recovery.
#[allow(clippy::large_enum_variant)]
enum ManifestSlotProbe {
    Valid(ManifestSlot),
    Unsupported(u16),
    Unrecognized,
}

impl ManifestSlot {
    fn encode(self) -> [u8; MANIFEST_SLOT_SIZE] {
        let mut output = [0_u8; MANIFEST_SLOT_SIZE];
        output[..8].copy_from_slice(&MANIFEST_MAGIC);
        put_u16(&mut output, VERSION_OFFSET, MANIFEST_VERSION);
        put_u16(&mut output, HEADER_SIZE_OFFSET, MANIFEST_HEADER_SIZE);
        put_u64(&mut output, GENERATION_OFFSET, self.generation);
        output[CACHE_ID_OFFSET..CACHE_ID_OFFSET + 16].copy_from_slice(&self.cache_id);
        put_u64(&mut output, VERSION_EPOCH_OFFSET, self.version_epoch);
        put_u64(&mut output, NEXT_SEQNO_OFFSET, self.next_seqno);
        put_u64(
            &mut output,
            LAYOUT_FINGERPRINT_OFFSET,
            self.layout_fingerprint,
        );
        put_u64(
            &mut output,
            JOURNAL_GENERATION_OFFSET,
            self.journal_generation,
        );
        put_u64(&mut output, JOURNAL_CAPACITY_OFFSET, self.journal_capacity);
        put_u64(
            &mut output,
            CHECKPOINT_EPOCH_OFFSET,
            self.checkpoint_version.epoch,
        );
        put_u64(
            &mut output,
            CHECKPOINT_SEQNO_OFFSET,
            self.checkpoint_version.seqno,
        );
        put_u64(
            &mut output,
            CLEAR_FLOOR_EPOCH_OFFSET,
            self.clear_floor.epoch,
        );
        put_u64(
            &mut output,
            CLEAR_FLOOR_SEQNO_OFFSET,
            self.clear_floor.seqno,
        );
        output[CLEAN_OFFSET] = u8::from(self.clean);
        self.namespace_usage.encode_into(&mut output);
        let checksum = crc32c(&output);
        put_u32(&mut output, MANIFEST_CRC_OFFSET, checksum);
        output
    }

    fn decode(input: &[u8]) -> Option<Self> {
        if input.len() != MANIFEST_SLOT_SIZE
            || input.get(..8)? != MANIFEST_MAGIC
            || get_u16(input, VERSION_OFFSET)? != MANIFEST_VERSION
            || get_u16(input, HEADER_SIZE_OFFSET)? != MANIFEST_HEADER_SIZE
            || !fixed_checksum_matches(input, MANIFEST_CRC_OFFSET)
        {
            return None;
        }
        let clean = match *input.get(CLEAN_OFFSET)? {
            0 => false,
            1 => true,
            _ => return None,
        };
        let mut cache_id = [0_u8; 16];
        cache_id.copy_from_slice(input.get(CACHE_ID_OFFSET..CACHE_ID_OFFSET + 16)?);
        let version_epoch = get_u64(input, VERSION_EPOCH_OFFSET)?;
        let next_seqno = get_u64(input, NEXT_SEQNO_OFFSET)?;
        let journal_generation = get_u64(input, JOURNAL_GENERATION_OFFSET)?;
        let journal_capacity = get_u64(input, JOURNAL_CAPACITY_OFFSET)?;
        let checkpoint_version = HybridVersion {
            epoch: get_u64(input, CHECKPOINT_EPOCH_OFFSET)?,
            seqno: get_u64(input, CHECKPOINT_SEQNO_OFFSET)?,
        };
        let clear_floor = HybridVersion {
            epoch: get_u64(input, CLEAR_FLOOR_EPOCH_OFFSET)?,
            seqno: get_u64(input, CLEAR_FLOOR_SEQNO_OFFSET)?,
        };
        if cache_id == [0_u8; 16]
            || version_epoch == 0
            || next_seqno == 0
            || journal_generation == 0
            || validate_journal_capacity(journal_capacity).is_err()
            || !version_precedes_next(checkpoint_version, version_epoch, next_seqno)
            || !version_precedes_next(clear_floor, version_epoch, next_seqno)
        {
            return None;
        }
        Some(Self {
            generation: get_u64(input, GENERATION_OFFSET)?,
            cache_id,
            version_epoch,
            next_seqno,
            layout_fingerprint: get_u64(input, LAYOUT_FINGERPRINT_OFFSET)?,
            journal_generation,
            journal_capacity,
            checkpoint_version,
            clear_floor,
            clean,
            namespace_usage: NamespaceUsageCheckpoint::decode(input)?,
        })
    }

    fn probe(input: &[u8]) -> ManifestSlotProbe {
        if input.len() != MANIFEST_SLOT_SIZE
            || input.get(..8) != Some(MANIFEST_MAGIC.as_slice())
            || !fixed_checksum_matches(input, MANIFEST_CRC_OFFSET)
        {
            return ManifestSlotProbe::Unrecognized;
        }
        let Some(version) = get_u16(input, VERSION_OFFSET) else {
            return ManifestSlotProbe::Unrecognized;
        };
        if version != MANIFEST_VERSION {
            return ManifestSlotProbe::Unsupported(version);
        }
        Self::decode(input)
            .map(ManifestSlotProbe::Valid)
            .unwrap_or(ManifestSlotProbe::Unrecognized)
    }
}

struct ManifestState {
    status: CacheStatus,
    slot: ManifestSlot,
    active_slot: usize,
    journal_tail: u64,
    highest_version: HybridVersion,
    effective_clear_floor: HybridVersion,
    ignored_torn_tail: bool,
}

pub(crate) struct HybridManifest {
    io: Box<dyn IoBackend>,
    host_writes: Option<Arc<HostWriteTracker>>,
    status: AtomicU8,
    state: Mutex<ManifestState>,
    #[cfg(test)]
    fail_lower_checkpoint_dirty_once: AtomicBool,
}

impl HybridManifest {
    #[cfg(test)]
    pub(crate) fn open_with_journal_capacity(
        path: &Path,
        layout_fingerprint: u64,
        journal_capacity: u64,
    ) -> Result<(Self, ManifestOpenState)> {
        validate_journal_capacity(journal_capacity)?;
        let io = FileBackend::open_with_io_mode(path, DirectIoMode::Buffered)?;
        Self::open_with_backend_and_host_writes(
            Box::new(io),
            layout_fingerprint,
            journal_capacity,
            None,
        )
    }

    pub(crate) fn open_managed_with_journal_capacity(
        path: &Path,
        layout_fingerprint: u64,
        journal_capacity: u64,
        host_writes: Arc<HostWriteTracker>,
        initial_namespace_usage: &[NamespaceUsage],
    ) -> Result<(Self, ManifestOpenState)> {
        validate_journal_capacity(journal_capacity)?;
        let io = FileBackend::open_with_io_mode(path, DirectIoMode::Buffered)?;
        Self::open_with_backend_host_writes_and_usage(
            Box::new(io),
            layout_fingerprint,
            journal_capacity,
            Some(host_writes),
            NamespaceUsageCheckpoint::try_from_usage(initial_namespace_usage)?,
        )
    }

    #[cfg(test)]
    pub(crate) fn open_with_backend(
        io: Box<dyn IoBackend>,
        layout_fingerprint: u64,
        journal_capacity: u64,
    ) -> Result<(Self, ManifestOpenState)> {
        Self::open_with_backend_and_host_writes(io, layout_fingerprint, journal_capacity, None)
    }

    #[cfg(test)]
    fn open_with_backend_and_host_writes(
        io: Box<dyn IoBackend>,
        layout_fingerprint: u64,
        journal_capacity: u64,
        host_writes: Option<Arc<HostWriteTracker>>,
    ) -> Result<(Self, ManifestOpenState)> {
        Self::open_with_backend_host_writes_and_usage(
            io,
            layout_fingerprint,
            journal_capacity,
            host_writes,
            NamespaceUsageCheckpoint::absent(),
        )
    }

    fn open_with_backend_host_writes_and_usage(
        io: Box<dyn IoBackend>,
        layout_fingerprint: u64,
        journal_capacity: u64,
        host_writes: Option<Arc<HostWriteTracker>>,
        initial_namespace_usage: NamespaceUsageCheckpoint,
    ) -> Result<(Self, ManifestOpenState)> {
        validate_journal_capacity(journal_capacity)?;
        io.try_lock_exclusive().map_err(map_lock_error)?;
        let prepared = (|| {
            let (mut slot, active_slot, created) = open_or_format(
                io.as_ref(),
                host_writes.as_deref(),
                layout_fingerprint,
                journal_capacity,
                initial_namespace_usage,
            )?;
            let journal =
                scan_journal(io.as_ref(), slot.journal_generation, slot.journal_capacity)?;
            let mut highest_version = slot.checkpoint_version;
            let mut effective_clear_floor = slot.clear_floor;
            if let Some(first_version) = journal.first_version {
                if first_version <= slot.checkpoint_version
                    || journal.highest_version.epoch > slot.version_epoch
                {
                    return Err(CacheError::CorruptMetadata(
                        "hybrid journal versions are not strictly increasing",
                    ));
                }
                highest_version = journal.highest_version;
                effective_clear_floor = effective_clear_floor.max(journal.highest_clear_version);
            }
            if highest_version.epoch == slot.version_epoch
                && slot.next_seqno <= highest_version.seqno
            {
                slot.next_seqno = highest_version.seqno.saturating_add(1);
            }
            let needs_recovery =
                !slot.clean || journal.intent_count != 0 || journal.requires_full_clear;
            Ok((
                slot,
                active_slot,
                created,
                journal,
                highest_version,
                effective_clear_floor,
                needs_recovery,
            ))
        })();
        let (
            slot,
            active_slot,
            created,
            journal,
            highest_version,
            effective_clear_floor,
            needs_recovery,
        ) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = io.unlock();
                return Err(error);
            }
        };
        let journal_tail = journal.valid_bytes;
        let ignored_torn_tail = journal.ignored_torn_tail;
        Ok((
            Self {
                io,
                host_writes,
                status: AtomicU8::new(CacheStatus::Healthy as u8),
                state: Mutex::new(ManifestState {
                    status: CacheStatus::Healthy,
                    slot,
                    active_slot,
                    journal_tail,
                    highest_version,
                    effective_clear_floor,
                    ignored_torn_tail,
                }),
                #[cfg(test)]
                fail_lower_checkpoint_dirty_once: AtomicBool::new(false),
            },
            ManifestOpenState {
                created,
                needs_recovery,
                journal,
            },
        ))
    }

    pub(crate) fn status(&self) -> CacheStatus {
        decode_cache_status(self.status.load(Ordering::Acquire))
    }

    pub(crate) fn snapshot(&self) -> Result<ManifestSnapshot> {
        let state = lock_mutex(&self.state);
        ensure_healthy(&state)?;
        Ok(snapshot(&state))
    }

    /// Return the bounded usage snapshot attached to the selected clean slot.
    /// `None` is the backwards-compatible representation of an older V1 slot
    /// whose reserved extension is all zero.
    pub(crate) fn namespace_usage_checkpoint(&self) -> Result<Option<Vec<NamespaceUsage>>> {
        let state = lock_mutex(&self.state);
        ensure_healthy(&state)?;
        if !state.slot.namespace_usage.present {
            return Ok(None);
        }
        let usage = state.slot.namespace_usage.as_slice();
        let mut copied = Vec::new();
        copied
            .try_reserve_exact(usage.len())
            .map_err(|_| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
        copied.extend_from_slice(usage);
        Ok(Some(copied))
    }

    /// Append and sync one intent. The returned version is the transaction's
    /// linearization fence; lower-engine mutation must not begin before this
    /// method succeeds.
    pub(crate) fn append_intent(&self, input: JournalIntentInput<'_>) -> Result<HybridVersion> {
        let mut versions = self.append_batch(std::slice::from_ref(&input))?;
        Ok(versions.pop().expect("one input produces one version"))
    }

    /// Append a batch with one data write and one sync. Inputs are assigned
    /// adjacent versions in slice order. The method serializes batches so the
    /// on-disk records and versions are both strictly ordered.
    pub(crate) fn append_batch(
        &self,
        inputs: &[JournalIntentInput<'_>],
    ) -> Result<Vec<HybridVersion>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self.append_wave(inputs, &[inputs.len()])?.versions)
    }

    /// Append several bounded logical groups and fence all of their writes with
    /// one durability sync. Group boundaries control encoder memory and write
    /// size only; versions remain one strictly ordered FIFO sequence.
    pub(crate) fn append_wave(
        &self,
        inputs: &[JournalIntentInput<'_>],
        group_lengths: &[usize],
    ) -> Result<JournalWaveCommit> {
        if inputs.is_empty() {
            if group_lengths.is_empty() {
                return Ok(JournalWaveCommit {
                    versions: Vec::new(),
                    sync_elapsed: Duration::ZERO,
                });
            }
            return Err(CacheError::InvalidConfig(
                "empty hybrid journal wave has logical groups".into(),
            ));
        }
        if group_lengths.is_empty() || group_lengths.contains(&0) {
            return Err(CacheError::InvalidConfig(
                "hybrid journal wave has an empty logical group".into(),
            ));
        }

        let mut group_bytes = Vec::new();
        group_bytes
            .try_reserve_exact(group_lengths.len())
            .map_err(|_| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
        let mut input_cursor = 0_usize;
        let mut total_bytes = 0_usize;
        for &group_len in group_lengths {
            let group_end = input_cursor.checked_add(group_len).ok_or_else(|| {
                CacheError::InvalidConfig("hybrid journal wave record count overflow".into())
            })?;
            let group = inputs.get(input_cursor..group_end).ok_or_else(|| {
                CacheError::InvalidConfig(
                    "hybrid journal wave groups do not cover their inputs".into(),
                )
            })?;
            let mut encoded_bytes = 0_usize;
            for input in group {
                validate_intent(input)?;
                encoded_bytes = encoded_bytes
                    .checked_add(journal_intent_record_len(input)?)
                    .ok_or_else(|| {
                        CacheError::InvalidConfig(
                            "hybrid journal logical group length overflow".into(),
                        )
                    })?;
            }
            total_bytes = total_bytes.checked_add(encoded_bytes).ok_or_else(|| {
                CacheError::InvalidConfig("hybrid journal wave length overflow".into())
            })?;
            group_bytes.push(encoded_bytes);
            input_cursor = group_end;
        }
        if input_cursor != inputs.len() {
            return Err(CacheError::InvalidConfig(
                "hybrid journal wave groups do not cover their inputs".into(),
            ));
        }

        // One encoder is reused for every logical group. Its capacity never
        // exceeds the largest group selected by the bounded queue planner.
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(group_bytes.iter().copied().max().unwrap_or_default())
            .map_err(|_| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
        let mut versions = Vec::new();
        versions
            .try_reserve_exact(inputs.len())
            .map_err(|_| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;

        let mut state = lock_mutex(&self.state);
        ensure_healthy(&state)?;
        let total_bytes_u64 = u64::try_from(total_bytes)
            .map_err(|_| CacheError::InvalidConfig("hybrid journal batch is too large".into()))?;
        if state
            .journal_tail
            .checked_add(total_bytes_u64)
            .and_then(|end| end.checked_add(JOURNAL_HEADER_SIZE as u64))
            .is_none_or(|end| end > state.slot.journal_capacity)
        {
            return Err(CacheError::Overloaded(OverloadReason::JournalCapacityFull));
        }
        self.ensure_dirty_locked(&mut state)?;
        self.ensure_sequence_space_locked(&mut state, inputs.len())?;

        let first_seqno = state.slot.next_seqno;
        let mut absolute_offset =
            JOURNAL_OFFSET
                .checked_add(state.journal_tail)
                .ok_or(CacheError::CorruptMetadata(
                    "hybrid journal offset overflow",
                ))?;
        input_cursor = 0;
        for (&group_len, &encoded_bytes) in group_lengths.iter().zip(&group_bytes) {
            let group_end = input_cursor + group_len;
            encoded.resize(encoded_bytes, 0);
            let mut encoded_cursor = 0_usize;
            for (index, input) in inputs[input_cursor..group_end].iter().enumerate() {
                let length = journal_intent_record_len(input)?;
                let sequence_offset = u64::try_from(input_cursor + index).map_err(|_| {
                    CacheError::InvalidConfig("hybrid journal wave has too many records".into())
                })?;
                let version = HybridVersion {
                    epoch: state.slot.version_epoch,
                    seqno: first_seqno.checked_add(sequence_offset).ok_or(
                        CacheError::CorruptMetadata("hybrid sequence number overflow"),
                    )?,
                };
                encode_journal_record(
                    &mut encoded[encoded_cursor..encoded_cursor + length],
                    state.slot.journal_generation,
                    version,
                    input,
                )?;
                versions.push(version);
                encoded_cursor += length;
            }
            if let Err(error) = write_all_at_tracked(
                self.io.as_ref(),
                self.host_writes.as_deref(),
                WritePoint::HybridJournal,
                &encoded,
                absolute_offset,
            ) {
                self.set_status_locked(&mut state, CacheStatus::Poisoned);
                return Err(CacheError::Io(error));
            }
            absolute_offset = absolute_offset.checked_add(encoded_bytes as u64).ok_or(
                CacheError::CorruptMetadata("hybrid journal offset overflow"),
            )?;
            input_cursor = group_end;
        }
        #[cfg(test)]
        if inputs.len() > 1 {
            crash_hit(HybridCrashPoint::GroupJournalRecordsWritten);
        }
        // A synced zero header distinguishes the committed tail from stale
        // bytes belonging to an older, longer generation. If a restart sees a
        // non-zero invalid suffix, it can request a conservative full reset
        // instead of silently losing a possibly durable intent.
        if let Err(error) = write_all_at_tracked(
            self.io.as_ref(),
            self.host_writes.as_deref(),
            WritePoint::HybridJournal,
            &[0_u8; JOURNAL_HEADER_SIZE],
            absolute_offset,
        ) {
            self.set_status_locked(&mut state, CacheStatus::Poisoned);
            return Err(CacheError::Io(error));
        }
        #[cfg(test)]
        if inputs.len() > 1 {
            crash_hit(HybridCrashPoint::GroupJournalSentinelWritten);
        }
        let sync_started = Instant::now();
        if let Err(error) = sync_tracked(
            self.io.as_ref(),
            self.host_writes.as_deref(),
            SyncPoint::HybridJournal,
            SyncMode::Data,
        ) {
            self.set_status_locked(&mut state, CacheStatus::Poisoned);
            return Err(CacheError::Io(error));
        }
        let sync_elapsed = sync_started.elapsed();
        #[cfg(test)]
        if inputs.len() > 1 {
            crash_hit(HybridCrashPoint::GroupJournalSynced);
        }

        state.journal_tail += total_bytes_u64;
        state.slot.next_seqno = first_seqno
            .checked_add(u64::try_from(inputs.len()).map_err(|_| {
                CacheError::InvalidConfig("hybrid journal batch has too many records".into())
            })?)
            .ok_or(CacheError::CorruptMetadata(
                "hybrid sequence number overflow",
            ))?;
        state.highest_version = *versions
            .last()
            .expect("a non-empty batch has a final version");
        for (input, version) in inputs.iter().zip(&versions) {
            if input.kind == JournalIntentKind::Clear {
                state.effective_clear_floor = state.effective_clear_floor.max(*version);
            }
        }
        state.ignored_torn_tail = false;
        Ok(JournalWaveCommit {
            versions,
            sync_elapsed,
        })
    }

    pub(crate) fn begin_clear(&self) -> Result<(ManifestSnapshot, HybridVersion)> {
        let version = self.append_intent(JournalIntentInput::clear())?;
        Ok((self.snapshot()?, version))
    }

    /// Allocate one process-local mutation version without writing the route
    /// journal. Hybrid's performance-first mode fences the session dirty once,
    /// then uses this in-memory sequence for same-process ordering. A crash may
    /// discard the complete cache, so no per-mutation durability is required.
    pub(crate) fn allocate_volatile_version(&self) -> Result<(ManifestSnapshot, HybridVersion)> {
        let mut state = lock_mutex(&self.state);
        ensure_healthy(&state)?;
        if state.slot.clean {
            return Err(CacheError::CorruptMetadata(
                "hybrid volatile mutation started without a session dirty fence",
            ));
        }
        let version = HybridVersion {
            epoch: state.slot.version_epoch,
            seqno: state.slot.next_seqno,
        };
        state.slot.next_seqno =
            state
                .slot
                .next_seqno
                .checked_add(1)
                .ok_or(CacheError::CorruptMetadata(
                    "hybrid volatile sequence number exhausted",
                ))?;
        state.highest_version = version;
        Ok((snapshot(&state), version))
    }

    /// Advance the volatile clear floor without appending a journal record.
    /// The lower tiers are still cleared before Hybrid publishes the operation;
    /// after a crash the dirty-session contract falls back to an empty cache.
    pub(crate) fn begin_volatile_clear(&self) -> Result<(ManifestSnapshot, HybridVersion)> {
        let (_, version) = self.allocate_volatile_version()?;
        let mut state = lock_mutex(&self.state);
        ensure_healthy(&state)?;
        state.effective_clear_floor = state.effective_clear_floor.max(version);
        Ok((snapshot(&state), version))
    }

    pub(crate) fn remaining_journal_bytes(&self) -> Result<u64> {
        let state = lock_mutex(&self.state);
        ensure_healthy(&state)?;
        Ok((state.slot.journal_capacity - state.journal_tail)
            .saturating_sub(JOURNAL_HEADER_SIZE as u64))
    }

    /// Persist a global dirty fence before a composing driver lets either
    /// lower tier publish a newer clean checkpoint. With no journal intents,
    /// recovery deliberately treats this as a disposable-cache boundary and
    /// clears the lower tiers rather than trusting an older usage extension.
    pub(crate) fn mark_dirty_for_lower_checkpoint(&self) -> Result<()> {
        #[cfg(test)]
        if self
            .fail_lower_checkpoint_dirty_once
            .swap(false, Ordering::AcqRel)
        {
            return Err(CacheError::Io(std::io::Error::other(
                "injected Hybrid lower-checkpoint dirty-fence failure",
            )));
        }
        let mut state = lock_mutex(&self.state);
        ensure_healthy(&state)?;
        self.ensure_dirty_locked(&mut state)
    }

    #[cfg(test)]
    pub(crate) fn fail_lower_checkpoint_dirty_once_for_test(&self) {
        self.fail_lower_checkpoint_dirty_once
            .store(true, Ordering::Release);
    }

    /// Called only after journal recovery reconciles every intent and both
    /// lower engines publish clean checkpoints. A new epoch prevents reuse of
    /// an unpersisted dirty-runtime seqno.
    #[cfg(test)]
    pub(crate) fn finish_dirty_recovery(&self) -> Result<ManifestSnapshot> {
        self.finish_dirty_recovery_inner(None)
    }

    pub(crate) fn finish_dirty_recovery_with_usage(
        &self,
        usage: &[NamespaceUsage],
    ) -> Result<ManifestSnapshot> {
        self.finish_dirty_recovery_inner(Some(usage))
    }

    fn finish_dirty_recovery_inner(
        &self,
        usage: Option<&[NamespaceUsage]>,
    ) -> Result<ManifestSnapshot> {
        let usage = usage
            .map(NamespaceUsageCheckpoint::try_from_usage)
            .transpose()?;
        let mut state = lock_mutex(&self.state);
        ensure_healthy(&state)?;
        if state.slot.clean && state.journal_tail == 0 {
            if usage.is_none_or(|usage| usage == state.slot.namespace_usage) {
                return Ok(snapshot(&state));
            }
            self.publish_checkpoint_locked(&mut state, None, usage)?;
            return Ok(snapshot(&state));
        }
        let next_epoch =
            state
                .slot
                .version_epoch
                .checked_add(1)
                .ok_or(CacheError::CorruptMetadata(
                    "hybrid version epoch exhausted",
                ))?;
        self.publish_checkpoint_locked(&mut state, Some(next_epoch), usage)?;
        Ok(snapshot(&state))
    }

    /// Publish the global clean boundary after both lower tiers are durable.
    /// Journal offset zero may be reused only after both manifest writes and
    /// syncs complete inside this method.
    #[cfg(test)]
    pub(crate) fn publish_clean(&self) -> Result<ManifestSnapshot> {
        self.publish_clean_inner(None)
    }

    pub(crate) fn publish_clean_with_usage(
        &self,
        usage: &[NamespaceUsage],
    ) -> Result<ManifestSnapshot> {
        self.publish_clean_inner(Some(usage))
    }

    fn publish_clean_inner(&self, usage: Option<&[NamespaceUsage]>) -> Result<ManifestSnapshot> {
        let usage = usage
            .map(NamespaceUsageCheckpoint::try_from_usage)
            .transpose()?;
        let mut state = lock_mutex(&self.state);
        ensure_healthy(&state)?;
        if state.slot.clean
            && state.journal_tail == 0
            && usage.is_none_or(|usage| usage == state.slot.namespace_usage)
        {
            return Ok(snapshot(&state));
        }
        self.publish_checkpoint_locked(&mut state, None, usage)?;
        Ok(snapshot(&state))
    }

    pub(crate) fn close(&self) -> Result<()> {
        let mut state = lock_mutex(&self.state);
        if state.status == CacheStatus::Closed {
            return Ok(());
        }
        let status_result = match state.status {
            CacheStatus::Healthy => Ok(()),
            CacheStatus::MissOnly | CacheStatus::Poisoned => Err(CacheError::Poisoned),
            CacheStatus::Closed => Ok(()),
        };
        let unlock_result = self.io.unlock().map_err(CacheError::Io);
        self.set_status_locked(&mut state, CacheStatus::Closed);
        status_result.and(unlock_result)
    }

    fn set_status_locked(&self, state: &mut ManifestState, status: CacheStatus) {
        state.status = status;
        self.status.store(status as u8, Ordering::Release);
    }

    fn ensure_dirty_locked(&self, state: &mut ManifestState) -> Result<()> {
        if !state.slot.clean {
            return Ok(());
        }
        let candidate = ManifestSlot {
            clean: false,
            ..state.slot
        };
        self.publish_pair_locked(state, candidate, SyncPoint::HybridManifestDirty)
    }

    fn ensure_sequence_space_locked(&self, state: &mut ManifestState, count: usize) -> Result<()> {
        let count = u64::try_from(count)
            .map_err(|_| CacheError::InvalidConfig("hybrid batch size overflow".into()))?;
        if state.slot.next_seqno.checked_add(count).is_some() {
            return Ok(());
        }
        let next_epoch =
            state
                .slot
                .version_epoch
                .checked_add(1)
                .ok_or(CacheError::CorruptMetadata(
                    "hybrid version epoch exhausted",
                ))?;
        let candidate = ManifestSlot {
            version_epoch: next_epoch,
            next_seqno: 1,
            clean: false,
            ..state.slot
        };
        self.publish_pair_locked(state, candidate, SyncPoint::HybridManifestDirty)
    }

    fn publish_checkpoint_locked(
        &self,
        state: &mut ManifestState,
        next_epoch: Option<u64>,
        namespace_usage: Option<NamespaceUsageCheckpoint>,
    ) -> Result<()> {
        let journal_generation =
            state
                .slot
                .journal_generation
                .checked_add(1)
                .ok_or(CacheError::CorruptMetadata(
                    "hybrid journal generation exhausted",
                ))?;
        let candidate = ManifestSlot {
            version_epoch: next_epoch.unwrap_or(state.slot.version_epoch),
            next_seqno: next_epoch.map_or(state.slot.next_seqno, |_| 1),
            journal_generation,
            checkpoint_version: state.highest_version,
            clear_floor: state.effective_clear_floor,
            clean: true,
            namespace_usage: namespace_usage.unwrap_or(state.slot.namespace_usage),
            ..state.slot
        };
        self.publish_pair_locked(state, candidate, SyncPoint::HybridManifestClean)?;
        #[cfg(test)]
        crash_hit(HybridCrashPoint::GlobalCleanPublished);
        state.journal_tail = 0;
        state.ignored_torn_tail = false;
        Ok(())
    }

    fn publish_pair_locked(
        &self,
        state: &mut ManifestState,
        candidate: ManifestSlot,
        sync_point: SyncPoint,
    ) -> Result<()> {
        let first_generation =
            state
                .slot
                .generation
                .checked_add(1)
                .ok_or(CacheError::CorruptMetadata(
                    "hybrid manifest generation exhausted",
                ))?;
        let second_generation =
            first_generation
                .checked_add(1)
                .ok_or(CacheError::CorruptMetadata(
                    "hybrid manifest generation exhausted",
                ))?;
        let first_slot = 1 - state.active_slot;
        let second_slot = state.active_slot;
        let first = ManifestSlot {
            generation: first_generation,
            ..candidate
        };
        if let Err(error) = self.write_slot(first_slot, first) {
            self.set_status_locked(state, CacheStatus::Poisoned);
            return Err(error);
        }
        if let Err(error) = sync_tracked(
            self.io.as_ref(),
            self.host_writes.as_deref(),
            sync_point,
            SyncMode::Data,
        ) {
            self.set_status_locked(state, CacheStatus::Poisoned);
            return Err(CacheError::Io(error));
        }
        let second = ManifestSlot {
            generation: second_generation,
            ..candidate
        };
        if let Err(error) = self.write_slot(second_slot, second) {
            self.set_status_locked(state, CacheStatus::Poisoned);
            return Err(error);
        }
        if let Err(error) = sync_tracked(
            self.io.as_ref(),
            self.host_writes.as_deref(),
            sync_point,
            SyncMode::Data,
        ) {
            self.set_status_locked(state, CacheStatus::Poisoned);
            return Err(CacheError::Io(error));
        }
        state.slot = second;
        state.active_slot = second_slot;
        Ok(())
    }

    fn write_slot(&self, slot: usize, manifest: ManifestSlot) -> Result<()> {
        write_all_at_tracked(
            self.io.as_ref(),
            self.host_writes.as_deref(),
            WritePoint::HybridManifest,
            &manifest.encode(),
            (slot * MANIFEST_SLOT_SIZE) as u64,
        )
        .map_err(CacheError::Io)
    }
}

fn write_all_at_tracked(
    io: &dyn IoBackend,
    host_writes: Option<&HostWriteTracker>,
    point: WritePoint,
    bytes: &[u8],
    offset: u64,
) -> std::io::Result<()> {
    if let Some(host_writes) = host_writes {
        host_writes.record_write(HostWriteKind::Metadata, bytes.len() as u64);
    }
    match write_all_at(io, point, bytes, offset) {
        Ok(()) => Ok(()),
        Err(error) => {
            record_write_failure(host_writes);
            Err(error)
        }
    }
}

fn sync_tracked(
    io: &dyn IoBackend,
    host_writes: Option<&HostWriteTracker>,
    point: SyncPoint,
    mode: SyncMode,
) -> std::io::Result<()> {
    match io.sync(point, mode) {
        Ok(()) => Ok(()),
        Err(error) => {
            record_write_failure(host_writes);
            Err(error)
        }
    }
}

fn record_write_failure(host_writes: Option<&HostWriteTracker>) {
    if let Some(host_writes) = host_writes {
        host_writes.record_write_failure();
    }
}

fn open_or_format(
    io: &dyn IoBackend,
    host_writes: Option<&HostWriteTracker>,
    layout_fingerprint: u64,
    journal_capacity: u64,
    initial_namespace_usage: NamespaceUsageCheckpoint,
) -> Result<(ManifestSlot, usize, bool)> {
    let expected_len = manifest_file_len(journal_capacity)?;
    let len = io.len()?;
    if len == 0 {
        return format_manifest(
            io,
            host_writes,
            layout_fingerprint,
            journal_capacity,
            initial_namespace_usage,
        );
    }
    if len < JOURNAL_OFFSET {
        return Err(CacheError::CorruptMetadata(
            "hybrid manifest is shorter than its superblocks",
        ));
    }
    let mut pages = [[0_u8; MANIFEST_SLOT_SIZE]; MANIFEST_SLOT_COUNT];
    for (slot, page) in pages.iter_mut().enumerate() {
        read_exact_at(io, page, (slot * MANIFEST_SLOT_SIZE) as u64)?;
    }
    let mut valid = Vec::with_capacity(MANIFEST_SLOT_COUNT);
    let mut unsupported_version = None;
    for (slot, page) in pages.iter().enumerate() {
        match ManifestSlot::probe(page) {
            ManifestSlotProbe::Valid(manifest) => valid.push((manifest, slot)),
            ManifestSlotProbe::Unsupported(version) => unsupported_version = Some(version),
            ManifestSlotProbe::Unrecognized => {}
        }
    }
    if let Some(version) = unsupported_version {
        return Err(CacheError::InvalidConfig(format!(
            "hybrid manifest format version {version} is not supported"
        )));
    }
    if valid.is_empty() {
        if pages.iter().all(recognizable_interrupted_v1) {
            return format_manifest(
                io,
                host_writes,
                layout_fingerprint,
                journal_capacity,
                initial_namespace_usage,
            );
        }
        return Err(CacheError::CorruptMetadata(
            "hybrid manifest slots are not recognized",
        ));
    }
    valid.sort_unstable_by_key(|(manifest, _)| manifest.generation);
    let (manifest, active_slot) = valid.pop().expect("checked non-empty manifests");
    if manifest.layout_fingerprint != layout_fingerprint {
        return Err(CacheError::InvalidConfig(
            "hybrid manifest does not match the configured disk pair".into(),
        ));
    }
    if manifest.journal_capacity != journal_capacity || len != expected_len {
        return Err(CacheError::InvalidConfig(
            "hybrid journal capacity or file length does not match".into(),
        ));
    }
    Ok((manifest, active_slot, false))
}

fn recognizable_interrupted_v1(page: &[u8; MANIFEST_SLOT_SIZE]) -> bool {
    if page.iter().all(|byte| *byte == 0) {
        return true;
    }
    let nonzero_end = page
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(0, |index| index + 1);
    if nonzero_end <= CACHE_ID_OFFSET {
        return [1_u64, 2_u64].into_iter().any(|generation| {
            let mut prefix = [0_u8; CACHE_ID_OFFSET];
            prefix[..8].copy_from_slice(&MANIFEST_MAGIC);
            put_u16(&mut prefix, VERSION_OFFSET, MANIFEST_VERSION);
            put_u16(&mut prefix, HEADER_SIZE_OFFSET, MANIFEST_HEADER_SIZE);
            put_u64(&mut prefix, GENERATION_OFFSET, generation);
            page[..nonzero_end] == prefix[..nonzero_end]
        });
    }
    page.get(..8) == Some(MANIFEST_MAGIC.as_slice())
        && get_u16(page, VERSION_OFFSET) == Some(MANIFEST_VERSION)
        && get_u16(page, HEADER_SIZE_OFFSET) == Some(MANIFEST_HEADER_SIZE)
}

fn format_manifest(
    io: &dyn IoBackend,
    host_writes: Option<&HostWriteTracker>,
    layout_fingerprint: u64,
    journal_capacity: u64,
    initial_namespace_usage: NamespaceUsageCheckpoint,
) -> Result<(ManifestSlot, usize, bool)> {
    let file_len = manifest_file_len(journal_capacity)?;
    io.set_len(0).inspect_err(|_| {
        record_write_failure(host_writes);
    })?;
    sync_tracked(io, host_writes, SyncPoint::FormatTruncate, SyncMode::All)?;
    io.preallocate(file_len).inspect_err(|_| {
        record_write_failure(host_writes);
    })?;
    let cache_id = generate_cache_id(layout_fingerprint);
    // A managed manifest (identified by its bounded namespace extension) is
    // not yet bound to the lower files at format time. Persist it dirty from
    // the first valid slot so a crash before Hybrid's open-time fence cannot
    // make a later process trust zero usage for pre-existing lower data.
    let initially_clean = !initial_namespace_usage.present;
    let first = ManifestSlot {
        generation: 1,
        cache_id,
        version_epoch: 1,
        next_seqno: 1,
        layout_fingerprint,
        journal_generation: 1,
        journal_capacity,
        checkpoint_version: HybridVersion::ZERO,
        clear_floor: HybridVersion::ZERO,
        clean: initially_clean,
        namespace_usage: initial_namespace_usage,
    };
    let second = ManifestSlot {
        generation: 2,
        ..first
    };
    write_all_at_tracked(
        io,
        host_writes,
        WritePoint::HybridManifest,
        &first.encode(),
        0,
    )?;
    write_all_at_tracked(
        io,
        host_writes,
        WritePoint::HybridManifest,
        &second.encode(),
        MANIFEST_SLOT_SIZE as u64,
    )?;
    sync_tracked(io, host_writes, SyncPoint::FormatClean, SyncMode::All)?;
    Ok((second, 1, true))
}

fn scan_journal(
    io: &dyn IoBackend,
    journal_generation: u64,
    journal_capacity: u64,
) -> Result<JournalScan> {
    let recovery_limit = journal_recovery_memory_bytes(journal_capacity)?;
    let probe = probe_journal(io, journal_generation, journal_capacity, recovery_limit)?;
    if probe.requires_full_clear || probe.contains_clear {
        return Ok(JournalScan {
            intents: JournalIntents::default(),
            intent_count: probe.intent_count,
            valid_bytes: probe.valid_bytes,
            first_version: probe.first_version,
            highest_version: probe.highest_version,
            highest_clear_version: probe.highest_clear_version,
            contains_clear: probe.contains_clear,
            ignored_torn_tail: probe.ignored_torn_tail,
            requires_full_clear: probe.requires_full_clear,
        });
    }
    let valid_bytes = usize::try_from(probe.valid_bytes).map_err(|_| {
        CacheError::InvalidConfig("hybrid journal recovery prefix is not addressable".into())
    })?;
    let intent_count = usize::try_from(probe.intent_count).map_err(|_| {
        CacheError::InvalidConfig("hybrid journal recovery intent count is not addressable".into())
    })?;

    // Allocate each retained container exactly once, after the streaming
    // scratch buffer has been dropped. This avoids both per-key allocations
    // and Vec's geometric growth peak on a dense journal.
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(valid_bytes)
        .map_err(|_| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
    encoded.resize(valid_bytes, 0);
    if !encoded.is_empty() {
        read_exact_at(io, &mut encoded, JOURNAL_OFFSET)?;
    }
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(intent_count)
        .map_err(|_| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;

    let mut relative = 0_usize;
    let mut previous = HybridVersion::ZERO;
    while relative < encoded.len() {
        let record_len = get_u32(&encoded[relative..], JOURNAL_RECORD_LEN_OFFSET)
            .map(|length| length as usize)
            .ok_or(CacheError::CorruptMetadata(
                "hybrid journal changed while loading recovery prefix",
            ))?;
        let end = relative
            .checked_add(record_len)
            .filter(|end| *end <= encoded.len())
            .ok_or(CacheError::CorruptMetadata(
                "hybrid journal changed while loading recovery prefix",
            ))?;
        let Some(intent) = decode_journal_record_ref(&encoded[relative..end], journal_generation)
        else {
            return Err(CacheError::CorruptMetadata(
                "hybrid journal changed while loading recovery prefix",
            ));
        };
        if intent.version <= previous {
            return Err(CacheError::CorruptMetadata(
                "hybrid journal versions are not strictly increasing",
            ));
        }
        previous = intent.version;
        offsets.push(u32::try_from(relative).map_err(|_| {
            CacheError::InvalidConfig("hybrid journal recovery offset exceeds Format V1".into())
        })?);
        relative = end;
    }
    if offsets.len() != intent_count {
        return Err(CacheError::CorruptMetadata(
            "hybrid journal changed while loading recovery prefix",
        ));
    }

    let intents = JournalIntents { encoded, offsets };
    if intents.allocated_bytes() > recovery_limit {
        return Err(CacheError::InvalidConfig(
            "hybrid journal recovery allocation exceeds its configured bound".into(),
        ));
    }
    Ok(JournalScan {
        intents,
        intent_count: probe.intent_count,
        valid_bytes: probe.valid_bytes,
        first_version: probe.first_version,
        highest_version: probe.highest_version,
        highest_clear_version: probe.highest_clear_version,
        contains_clear: probe.contains_clear,
        ignored_torn_tail: probe.ignored_torn_tail,
        requires_full_clear: probe.requires_full_clear,
    })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct JournalProbe {
    intent_count: u64,
    valid_bytes: u64,
    first_version: Option<HybridVersion>,
    highest_version: HybridVersion,
    highest_clear_version: HybridVersion,
    contains_clear: bool,
    ignored_torn_tail: bool,
    requires_full_clear: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct JournalRecordMeta {
    kind: JournalIntentKind,
    version: HybridVersion,
}

fn probe_journal(
    io: &dyn IoBackend,
    journal_generation: u64,
    journal_capacity: u64,
    recovery_limit: usize,
) -> Result<JournalProbe> {
    let scratch_len = usize::try_from(journal_capacity.min(JOURNAL_SCAN_CHUNK_BYTES as u64))
        .map_err(|_| {
            CacheError::InvalidConfig("hybrid journal scan buffer is not addressable".into())
        })?;
    let mut scratch = Vec::new();
    scratch
        .try_reserve_exact(scratch_len)
        .map_err(|_| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
    if scratch.capacity() > recovery_limit {
        return Err(CacheError::InvalidConfig(
            "hybrid journal scan allocation exceeds its configured bound".into(),
        ));
    }
    scratch.resize(scratch_len, 0);

    let mut probe = JournalProbe::default();
    let mut relative = 0_u64;
    let mut previous = HybridVersion::ZERO;
    while relative
        .checked_add(JOURNAL_HEADER_SIZE as u64)
        .is_some_and(|end| end <= journal_capacity)
    {
        let mut header = [0_u8; JOURNAL_HEADER_SIZE];
        read_exact_at(
            io,
            &mut header,
            JOURNAL_OFFSET
                .checked_add(relative)
                .ok_or(CacheError::CorruptMetadata(
                    "hybrid journal offset overflow",
                ))?,
        )?;
        if header.iter().all(|byte| *byte == 0) {
            break;
        }
        if header.get(..8) != Some(JOURNAL_MAGIC.as_slice())
            || get_u16(&header, JOURNAL_VERSION_OFFSET) != Some(JOURNAL_VERSION)
            || get_u16(&header, JOURNAL_HEADER_SIZE_OFFSET) != Some(JOURNAL_HEADER_SIZE as u16)
        {
            probe.ignored_torn_tail = true;
            probe.requires_full_clear = true;
            break;
        }
        let Some(record_generation) = get_u64(&header, JOURNAL_GENERATION_FIELD_OFFSET) else {
            probe.ignored_torn_tail = true;
            probe.requires_full_clear = true;
            break;
        };
        if record_generation != journal_generation {
            if relative != 0 {
                probe.ignored_torn_tail = true;
                probe.requires_full_clear = true;
            }
            break;
        }
        let Some(record_len) = get_u32(&header, JOURNAL_RECORD_LEN_OFFSET).map(|len| len as usize)
        else {
            probe.ignored_torn_tail = true;
            probe.requires_full_clear = true;
            break;
        };
        let record_len_u64 = record_len as u64;
        if record_len < JOURNAL_HEADER_SIZE
            || record_len % JOURNAL_ALIGNMENT != 0
            || relative
                .checked_add(record_len_u64)
                .is_none_or(|end| end > journal_capacity)
        {
            probe.ignored_torn_tail = true;
            probe.requires_full_clear = true;
            break;
        }
        let Some(meta) = decode_journal_header(&header, record_len, journal_generation) else {
            probe.ignored_torn_tail = true;
            probe.requires_full_clear = true;
            break;
        };
        if !streamed_journal_checksum_matches(
            io,
            &header,
            record_len,
            JOURNAL_OFFSET
                .checked_add(relative)
                .ok_or(CacheError::CorruptMetadata(
                    "hybrid journal offset overflow",
                ))?,
            &mut scratch,
        )? {
            probe.ignored_torn_tail = true;
            probe.requires_full_clear = true;
            break;
        }
        if meta.version <= previous {
            return Err(CacheError::CorruptMetadata(
                "hybrid journal versions are not strictly increasing",
            ));
        }
        previous = meta.version;
        probe.first_version.get_or_insert(meta.version);
        probe.highest_version = meta.version;
        if meta.kind == JournalIntentKind::Clear {
            probe.contains_clear = true;
            probe.highest_clear_version = meta.version;
        }
        probe.intent_count = probe.intent_count.checked_add(1).ok_or_else(|| {
            CacheError::InvalidConfig("hybrid journal recovery intent count overflow".into())
        })?;
        relative += record_len_u64;
    }
    probe.valid_bytes = relative;
    Ok(probe)
}

fn decode_journal_header(
    header: &[u8; JOURNAL_HEADER_SIZE],
    record_len: usize,
    journal_generation: u64,
) -> Option<JournalRecordMeta> {
    if header.get(..8) != Some(JOURNAL_MAGIC.as_slice())
        || get_u16(header, JOURNAL_VERSION_OFFSET) != Some(JOURNAL_VERSION)
        || get_u16(header, JOURNAL_HEADER_SIZE_OFFSET) != Some(JOURNAL_HEADER_SIZE as u16)
        || get_u32(header, JOURNAL_RECORD_LEN_OFFSET).map(|length| length as usize)
            != Some(record_len)
        || get_u64(header, JOURNAL_GENERATION_FIELD_OFFSET) != Some(journal_generation)
    {
        return None;
    }
    let kind = header
        .get(JOURNAL_KIND_OFFSET)
        .copied()
        .and_then(JournalIntentKind::decode)?;
    let flags = *header.get(JOURNAL_FLAGS_OFFSET)?;
    if flags & !JOURNAL_FLAG_TOUCHES_BUCKET != 0 {
        return None;
    }
    let key_len = get_u32(header, JOURNAL_KEY_LEN_OFFSET)? as usize;
    if journal_record_len(key_len) != Some(record_len) {
        return None;
    }
    let version = HybridVersion {
        epoch: get_u64(header, JOURNAL_EPOCH_OFFSET)?,
        seqno: get_u64(header, JOURNAL_SEQNO_OFFSET)?,
    };
    if version.epoch == 0 || version.seqno == 0 {
        return None;
    }
    let namespace = get_u32(header, JOURNAL_NAMESPACE_OFFSET)?;
    let key_hash = get_u64(header, JOURNAL_KEY_HASH_OFFSET)?;
    let bucket_id = if flags & JOURNAL_FLAG_TOUCHES_BUCKET != 0 {
        get_u64(header, JOURNAL_BUCKET_ID_OFFSET)
    } else {
        None
    };
    if kind == JournalIntentKind::Clear
        && (namespace != 0 || key_hash != 0 || bucket_id.is_some() || key_len != 0)
    {
        return None;
    }
    if kind == JournalIntentKind::PutBucket && bucket_id.is_none() {
        return None;
    }
    Some(JournalRecordMeta { kind, version })
}

fn streamed_journal_checksum_matches(
    io: &dyn IoBackend,
    header: &[u8; JOURNAL_HEADER_SIZE],
    record_len: usize,
    record_offset: u64,
    scratch: &mut [u8],
) -> Result<bool> {
    let Some(stored) = get_u32(header, JOURNAL_CRC_OFFSET) else {
        return Ok(false);
    };
    let mut checksum = Crc32c::new();
    checksum.update(&header[..JOURNAL_CRC_OFFSET]);
    checksum.update(&[0_u8; size_of::<u32>()]);
    checksum.update(&header[JOURNAL_CRC_OFFSET + size_of::<u32>()..]);

    let mut consumed = JOURNAL_HEADER_SIZE;
    while consumed < record_len {
        let length = (record_len - consumed).min(scratch.len());
        read_exact_at(
            io,
            &mut scratch[..length],
            record_offset
                .checked_add(u64::try_from(consumed).map_err(|_| {
                    CacheError::InvalidConfig("hybrid journal checksum offset overflow".into())
                })?)
                .ok_or(CacheError::CorruptMetadata(
                    "hybrid journal offset overflow",
                ))?,
        )?;
        checksum.update(&scratch[..length]);
        consumed += length;
    }
    Ok(checksum.finish() == stored)
}

fn encode_journal_record(
    output: &mut [u8],
    journal_generation: u64,
    version: HybridVersion,
    input: &JournalIntentInput<'_>,
) -> Result<()> {
    let expected_len = journal_record_len(input.key.len())
        .ok_or_else(|| CacheError::InvalidConfig("hybrid journal record length overflow".into()))?;
    if output.len() != expected_len {
        return Err(CacheError::CorruptMetadata(
            "hybrid journal encoder length mismatch",
        ));
    }
    output.fill(0);
    output[..8].copy_from_slice(&JOURNAL_MAGIC);
    put_u16(output, JOURNAL_VERSION_OFFSET, JOURNAL_VERSION);
    put_u16(
        output,
        JOURNAL_HEADER_SIZE_OFFSET,
        JOURNAL_HEADER_SIZE as u16,
    );
    put_u32(
        output,
        JOURNAL_RECORD_LEN_OFFSET,
        u32::try_from(output.len())
            .map_err(|_| CacheError::InvalidConfig("hybrid journal record is too large".into()))?,
    );
    output[JOURNAL_KIND_OFFSET] = input.kind as u8;
    if input.bucket_id.is_some() {
        output[JOURNAL_FLAGS_OFFSET] = JOURNAL_FLAG_TOUCHES_BUCKET;
    }
    put_u64(output, JOURNAL_GENERATION_FIELD_OFFSET, journal_generation);
    put_u64(output, JOURNAL_EPOCH_OFFSET, version.epoch);
    put_u64(output, JOURNAL_SEQNO_OFFSET, version.seqno);
    put_u32(output, JOURNAL_NAMESPACE_OFFSET, input.namespace);
    put_u32(
        output,
        JOURNAL_KEY_LEN_OFFSET,
        u32::try_from(input.key.len())
            .map_err(|_| CacheError::InvalidConfig("hybrid journal key is too large".into()))?,
    );
    put_u64(output, JOURNAL_KEY_HASH_OFFSET, input.key_hash);
    put_u64(
        output,
        JOURNAL_BUCKET_ID_OFFSET,
        input.bucket_id.unwrap_or(u64::MAX),
    );
    output[JOURNAL_HEADER_SIZE..JOURNAL_HEADER_SIZE + input.key.len()].copy_from_slice(input.key);
    let checksum = crc32c(output);
    put_u32(output, JOURNAL_CRC_OFFSET, checksum);
    Ok(())
}

fn decode_journal_record_ref(
    input: &[u8],
    journal_generation: u64,
) -> Option<JournalIntentRef<'_>> {
    if input.len() < JOURNAL_HEADER_SIZE
        || input.get(..8) != Some(JOURNAL_MAGIC.as_slice())
        || get_u16(input, JOURNAL_VERSION_OFFSET) != Some(JOURNAL_VERSION)
        || get_u16(input, JOURNAL_HEADER_SIZE_OFFSET) != Some(JOURNAL_HEADER_SIZE as u16)
        || get_u32(input, JOURNAL_RECORD_LEN_OFFSET).map(|len| len as usize) != Some(input.len())
        || get_u64(input, JOURNAL_GENERATION_FIELD_OFFSET) != Some(journal_generation)
        || !fixed_checksum_matches(input, JOURNAL_CRC_OFFSET)
    {
        return None;
    }
    let kind = input
        .get(JOURNAL_KIND_OFFSET)
        .copied()
        .and_then(JournalIntentKind::decode)?;
    let flags = *input.get(JOURNAL_FLAGS_OFFSET).unwrap_or(&u8::MAX);
    if flags & !JOURNAL_FLAG_TOUCHES_BUCKET != 0 {
        return None;
    }
    let key_len = get_u32(input, JOURNAL_KEY_LEN_OFFSET)
        .map(|len| len as usize)
        .unwrap_or(usize::MAX);
    if journal_record_len(key_len) != Some(input.len()) {
        return None;
    }
    let version = HybridVersion {
        epoch: get_u64(input, JOURNAL_EPOCH_OFFSET).unwrap_or(0),
        seqno: get_u64(input, JOURNAL_SEQNO_OFFSET).unwrap_or(0),
    };
    if version.epoch == 0 || version.seqno == 0 {
        return None;
    }
    let namespace = get_u32(input, JOURNAL_NAMESPACE_OFFSET).unwrap_or(u32::MAX);
    let key_hash = get_u64(input, JOURNAL_KEY_HASH_OFFSET).unwrap_or(0);
    let bucket_id = if flags & JOURNAL_FLAG_TOUCHES_BUCKET != 0 {
        get_u64(input, JOURNAL_BUCKET_ID_OFFSET)
    } else {
        None
    };
    let key_bytes = &input[JOURNAL_HEADER_SIZE..JOURNAL_HEADER_SIZE + key_len];
    if kind == JournalIntentKind::Clear
        && (namespace != 0 || key_hash != 0 || bucket_id.is_some() || !key_bytes.is_empty())
    {
        return None;
    }
    if kind == JournalIntentKind::PutBucket && bucket_id.is_none() {
        return None;
    }
    Some(JournalIntentRef {
        kind,
        version,
        namespace,
        key_hash,
        bucket_id,
        key: key_bytes,
    })
}

fn trusted_intent_at(encoded: &[u8], offset: u32) -> JournalIntentRef<'_> {
    let start = offset as usize;
    let record_len = get_u32(&encoded[start..], JOURNAL_RECORD_LEN_OFFSET)
        .map(|length| length as usize)
        .expect("validated journal offset retains a record length");
    let input = &encoded[start..start + record_len];
    let kind = JournalIntentKind::decode(input[JOURNAL_KIND_OFFSET])
        .expect("validated journal record retains its kind");
    let flags = input[JOURNAL_FLAGS_OFFSET];
    let key_len = get_u32(input, JOURNAL_KEY_LEN_OFFSET)
        .expect("validated journal record retains its key length") as usize;
    JournalIntentRef {
        kind,
        version: HybridVersion {
            epoch: get_u64(input, JOURNAL_EPOCH_OFFSET)
                .expect("validated journal record retains its epoch"),
            seqno: get_u64(input, JOURNAL_SEQNO_OFFSET)
                .expect("validated journal record retains its sequence"),
        },
        namespace: get_u32(input, JOURNAL_NAMESPACE_OFFSET)
            .expect("validated journal record retains its namespace"),
        key_hash: get_u64(input, JOURNAL_KEY_HASH_OFFSET)
            .expect("validated journal record retains its key hash"),
        bucket_id: if flags & JOURNAL_FLAG_TOUCHES_BUCKET != 0 {
            get_u64(input, JOURNAL_BUCKET_ID_OFFSET)
        } else {
            None
        },
        key: &input[JOURNAL_HEADER_SIZE..JOURNAL_HEADER_SIZE + key_len],
    }
}

#[cfg(test)]
fn decode_journal_record(input: &[u8], journal_generation: u64) -> Result<Option<JournalIntent>> {
    let Some(intent) = decode_journal_record_ref(input, journal_generation) else {
        return Ok(None);
    };
    let mut key = Vec::new();
    key.try_reserve_exact(intent.key.len())
        .map_err(|_| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
    key.extend_from_slice(intent.key);
    Ok(Some(JournalIntent {
        kind: intent.kind,
        version: intent.version,
        namespace: intent.namespace,
        key_hash: intent.key_hash,
        bucket_id: intent.bucket_id,
        key,
    }))
}

fn validate_intent(input: &JournalIntentInput<'_>) -> Result<()> {
    if input.key.len() > u32::MAX as usize {
        return Err(CacheError::InvalidConfig(
            "hybrid journal key is too large".into(),
        ));
    }
    if input.kind == JournalIntentKind::Clear {
        if input.namespace != 0
            || input.key_hash != 0
            || input.bucket_id.is_some()
            || !input.key.is_empty()
        {
            return Err(CacheError::InvalidConfig(
                "clear intent cannot carry a key or bucket".into(),
            ));
        }
    } else if input.kind == JournalIntentKind::PutBucket && input.bucket_id.is_none() {
        return Err(CacheError::InvalidConfig(
            "Bucket put intent requires a bucket id".into(),
        ));
    }
    Ok(())
}

pub(crate) fn journal_intent_record_len(input: &JournalIntentInput<'_>) -> Result<usize> {
    validate_intent(input)?;
    journal_record_len(input.key.len())
        .ok_or_else(|| CacheError::InvalidConfig("hybrid journal record length overflow".into()))
}

fn journal_record_len(key_len: usize) -> Option<usize> {
    let unaligned = JOURNAL_HEADER_SIZE.checked_add(key_len)?;
    unaligned
        .checked_add(JOURNAL_ALIGNMENT - 1)
        .map(|length| length / JOURNAL_ALIGNMENT * JOURNAL_ALIGNMENT)
}

fn manifest_file_len(journal_capacity: u64) -> Result<u64> {
    JOURNAL_OFFSET
        .checked_add(journal_capacity)
        .ok_or_else(|| CacheError::InvalidConfig("hybrid manifest file length overflow".into()))
}

pub(crate) fn validate_journal_capacity(capacity: u64) -> Result<()> {
    if !(MIN_JOURNAL_CAPACITY..=MAX_JOURNAL_CAPACITY).contains(&capacity)
        || capacity % JOURNAL_CAPACITY_ALIGNMENT != 0
    {
        return Err(CacheError::InvalidConfig(format!(
            "hybrid journal capacity must be a 4096-byte multiple in {MIN_JOURNAL_CAPACITY}..={MAX_JOURNAL_CAPACITY}"
        )));
    }
    Ok(())
}

/// Conservative retained-memory bound for opening a journal generation.
///
/// Recovery keeps at most one encoded byte buffer plus one 32-bit offset per
/// minimum-size record. The first-pass 64 KiB scan scratch is released before
/// either retained allocation and is never larger than the minimum journal
/// capacity, so it does not increase the peak.
pub(crate) fn journal_recovery_memory_bytes(capacity: u64) -> Result<usize> {
    validate_journal_capacity(capacity)?;
    let encoded = usize::try_from(capacity).map_err(|_| {
        CacheError::InvalidConfig(
            "hybrid journal capacity exceeds addressable recovery memory".into(),
        )
    })?;
    let maximum_offsets = usize::try_from(capacity / MIN_JOURNAL_RECORD_SIZE).map_err(|_| {
        CacheError::InvalidConfig("hybrid journal recovery offset count is not addressable".into())
    })?;
    maximum_offsets
        .checked_mul(size_of::<u32>())
        .and_then(|offsets| encoded.checked_add(offsets))
        .ok_or_else(|| CacheError::InvalidConfig("hybrid journal recovery memory overflow".into()))
}

fn version_precedes_next(version: HybridVersion, epoch: u64, next_seqno: u64) -> bool {
    version == HybridVersion::ZERO
        || version.epoch < epoch
        || (version.epoch == epoch && version.seqno < next_seqno)
}

fn snapshot(state: &ManifestState) -> ManifestSnapshot {
    ManifestSnapshot {
        cache_id: state.slot.cache_id,
        version_epoch: state.slot.version_epoch,
        next_seqno: state.slot.next_seqno,
        checkpoint_version: state.slot.checkpoint_version,
        clear_floor: state.effective_clear_floor,
        journal_generation: state.slot.journal_generation,
        journal_capacity: state.slot.journal_capacity,
        journal_bytes: state.journal_tail,
        clean: state.slot.clean,
    }
}

fn ensure_healthy(state: &ManifestState) -> Result<()> {
    match state.status {
        CacheStatus::Healthy => Ok(()),
        CacheStatus::Closed => Err(CacheError::Closed),
        CacheStatus::MissOnly | CacheStatus::Poisoned => Err(CacheError::Poisoned),
    }
}

fn decode_cache_status(value: u8) -> CacheStatus {
    match value {
        value if value == CacheStatus::Healthy as u8 => CacheStatus::Healthy,
        value if value == CacheStatus::MissOnly as u8 => CacheStatus::MissOnly,
        value if value == CacheStatus::Poisoned as u8 => CacheStatus::Poisoned,
        value if value == CacheStatus::Closed as u8 => CacheStatus::Closed,
        _ => CacheStatus::Poisoned,
    }
}

fn generate_cache_id(layout_fingerprint: u64) -> [u8; 16] {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = CACHE_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let process = u64::from(std::process::id());
    let low = mix64((now as u64) ^ counter ^ layout_fingerprint);
    let high = mix64((now >> 64) as u64 ^ process.rotate_left(17) ^ !layout_fingerprint);
    let mut id = [0_u8; 16];
    id[..8].copy_from_slice(&low.to_le_bytes());
    id[8..].copy_from_slice(&high.to_le_bytes());
    if id == [0_u8; 16] {
        id[0] = 1;
    }
    id
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn fixed_checksum_matches(input: &[u8], checksum_offset: usize) -> bool {
    let Some(stored) = get_u32(input, checksum_offset) else {
        return false;
    };
    let mut checksum = Crc32c::new();
    checksum.update(&input[..checksum_offset]);
    checksum.update(&[0_u8; size_of::<u32>()]);
    checksum.update(&input[checksum_offset + size_of::<u32>()..]);
    checksum.finish() == stored
}

fn fixed_checksum_matches_extension(
    input: &[u8],
    start: usize,
    end: usize,
    checksum_offset: usize,
) -> bool {
    if start > checksum_offset
        || checksum_offset
            .checked_add(size_of::<u32>())
            .is_none_or(|offset_end| offset_end > end)
        || end > input.len()
    {
        return false;
    }
    let Some(stored) = get_u32(input, checksum_offset) else {
        return false;
    };
    let mut checksum = Crc32c::new();
    checksum.update(&input[start..checksum_offset]);
    checksum.update(&[0_u8; size_of::<u32>()]);
    checksum.update(&input[checksum_offset + size_of::<u32>()..end]);
    checksum.finish() == stored
}

fn map_lock_error(error: std::io::Error) -> CacheError {
    if error.kind() == std::io::ErrorKind::WouldBlock
        || error
            .raw_os_error()
            .is_some_and(|code| code == 11 || code == 35)
    {
        CacheError::Locked
    } else {
        CacheError::Io(error)
    }
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};
    use std::io::{self, Seek, SeekFrom, Write};
    use std::os::unix::process::ExitStatusExt;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::{Condvar, mpsc};
    use std::thread;
    use std::time::Duration;

    use crate::io_backend::testing::{FaultAction, FaultBackend, FaultEvent, FaultHandle};

    const TEST_JOURNAL_CAPACITY: u64 = MIN_JOURNAL_CAPACITY;

    struct TestPath(PathBuf);

    impl TestPath {
        fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(format!(
                "cache-rs-hybrid-manifest-{name}-{}-{}",
                std::process::id(),
                CACHE_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    #[derive(Default)]
    struct JournalSyncGateState {
        entered: bool,
        released: bool,
    }

    #[derive(Clone, Default)]
    struct JournalSyncGate {
        shared: Arc<(Mutex<JournalSyncGateState>, Condvar)>,
    }

    impl JournalSyncGate {
        fn block(&self) {
            let (state, changed) = self.shared.as_ref();
            let mut state = state.lock().unwrap();
            state.entered = true;
            changed.notify_all();
            while !state.released {
                state = changed.wait(state).unwrap();
            }
        }

        fn wait_until_entered(&self, timeout: Duration) -> bool {
            let (state, changed) = self.shared.as_ref();
            let state = state.lock().unwrap();
            let (state, _) = changed
                .wait_timeout_while(state, timeout, |state| !state.entered)
                .unwrap();
            state.entered
        }

        fn release(&self) {
            let (state, changed) = self.shared.as_ref();
            let mut state = state.lock().unwrap();
            state.released = true;
            changed.notify_all();
        }
    }

    struct BlockingJournalSyncBackend {
        inner: FileBackend,
        gate: JournalSyncGate,
    }

    impl BlockingJournalSyncBackend {
        fn open(path: &Path) -> io::Result<(Self, JournalSyncGate)> {
            let gate = JournalSyncGate::default();
            Ok((
                Self {
                    inner: FileBackend::open(path)?,
                    gate: gate.clone(),
                },
                gate,
            ))
        }
    }

    impl IoBackend for BlockingJournalSyncBackend {
        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }

        fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buffer, offset)
        }

        fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
            self.inner.write_at(point, buffer, offset)
        }

        fn sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()> {
            if point == SyncPoint::HybridJournal {
                self.gate.block();
            }
            self.inner.sync(point, mode)
        }

        fn try_lock_exclusive(&self) -> io::Result<()> {
            self.inner.try_lock_exclusive()
        }

        fn unlock(&self) -> io::Result<()> {
            self.inner.unlock()
        }
    }

    fn open(path: &Path, fingerprint: u64) -> Result<(HybridManifest, ManifestOpenState)> {
        HybridManifest::open_with_journal_capacity(path, fingerprint, TEST_JOURNAL_CAPACITY)
    }

    fn open_fault(
        path: &Path,
        fingerprint: u64,
    ) -> Result<(HybridManifest, ManifestOpenState, FaultHandle)> {
        let (backend, handle) = FaultBackend::open(path)?;
        let (manifest, opened) = HybridManifest::open_with_backend(
            Box::new(backend),
            fingerprint,
            TEST_JOURNAL_CAPACITY,
        )?;
        Ok((manifest, opened, handle))
    }

    fn assert_io_error<T>(result: Result<T>, raw_os_error: i32) {
        match result {
            Err(CacheError::Io(error)) => assert_eq!(error.raw_os_error(), Some(raw_os_error)),
            Err(error) => panic!("expected I/O error {raw_os_error}, got {error}"),
            Ok(_) => panic!("expected I/O error {raw_os_error}"),
        }
    }

    fn sparse_golden(input: &str) -> Vec<u8> {
        let mut output: Option<Vec<u8>> = None;
        for raw_line in input.lines() {
            let line = raw_line.split('#').next().unwrap().trim();
            if line.is_empty() {
                continue;
            }
            let mut fields = line.split_whitespace();
            let first = fields.next().unwrap();
            if first == "length" {
                let length = fields.next().unwrap().parse::<usize>().unwrap();
                assert!(output.replace(vec![0_u8; length]).is_none());
                continue;
            }
            let offset = usize::from_str_radix(first, 16).unwrap();
            let encoded = fields.next().unwrap();
            assert_eq!(encoded.len() % 2, 0);
            let bytes = encoded
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect::<Vec<_>>();
            let output = output.as_mut().expect("golden length must come first");
            output[offset..offset + bytes.len()].copy_from_slice(&bytes);
            assert!(fields.next().is_none());
        }
        output.expect("golden fixture must declare its length")
    }

    fn put_bucket(key: &[u8], bucket_id: u64) -> JournalIntentInput<'_> {
        JournalIntentInput {
            kind: JournalIntentKind::PutBucket,
            namespace: 7,
            key_hash: 0x1234,
            bucket_id: Some(bucket_id),
            key,
        }
    }

    #[test]
    fn status_does_not_wait_for_blocked_journal_sync() {
        let path = TestPath::new("nonblocking-status");
        let (backend, gate) = BlockingJournalSyncBackend::open(&path.0).unwrap();
        let (manifest, _) =
            HybridManifest::open_with_backend(Box::new(backend), 69, TEST_JOURNAL_CAPACITY)
                .unwrap();
        let manifest = Arc::new(manifest);

        let append_manifest = Arc::clone(&manifest);
        let append_thread = thread::spawn(move || {
            append_manifest.append_intent(put_bucket(b"blocked-journal-sync", 3))
        });
        let sync_entered = gate.wait_until_entered(Duration::from_secs(2));

        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let (status_tx, status_rx) = mpsc::sync_channel(1);
        let status_manifest = Arc::clone(&manifest);
        let status_thread = thread::spawn(move || {
            started_tx.send(()).unwrap();
            status_tx.send(status_manifest.status()).unwrap();
        });
        let status_started = started_rx.recv_timeout(Duration::from_secs(2));
        let status_before_release = status_rx.recv_timeout(Duration::from_secs(1));

        gate.release();
        let append_result = append_thread.join().unwrap();
        status_thread.join().unwrap();

        assert!(sync_entered, "append did not reach the journal sync gate");
        status_started.expect("status thread did not start");
        assert_eq!(
            status_before_release.expect("status waited for the manifest state mutex"),
            CacheStatus::Healthy
        );
        append_result.unwrap();
        manifest.close().unwrap();
        assert_eq!(manifest.status(), CacheStatus::Closed);
    }

    #[test]
    fn managed_manifest_tracks_format_journal_bytes_and_sync_failure() {
        let path = TestPath::new("managed-host-writes");
        let (backend, faults) = FaultBackend::open(&path.0).unwrap();
        let host_writes = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
        let (manifest, opened) = HybridManifest::open_with_backend_and_host_writes(
            Box::new(backend),
            71,
            TEST_JOURNAL_CAPACITY,
            Some(Arc::clone(&host_writes)),
        )
        .unwrap();
        assert!(opened.created);
        let formatted = host_writes.snapshot();
        assert_eq!(formatted.metadata_bytes, (2 * MANIFEST_SLOT_SIZE) as u64);
        assert_eq!(formatted.host_write_operations, 2);
        assert_eq!(formatted.failed_write_operations, 0);

        faults.arm(
            FaultEvent::Sync(SyncPoint::HybridJournal),
            1,
            FaultAction::Error(28),
        );
        let input = put_bucket(b"tracked-key", 4);
        let record_bytes = journal_intent_record_len(&input).unwrap();
        assert_io_error(manifest.append_intent(input), 28);
        let failed = host_writes.snapshot();
        assert_eq!(
            failed.metadata_bytes,
            (4 * MANIFEST_SLOT_SIZE + record_bytes + JOURNAL_HEADER_SIZE) as u64
        );
        assert_eq!(failed.host_write_operations, 6);
        assert_eq!(failed.failed_write_operations, 1);
        assert_eq!(manifest.status(), CacheStatus::Poisoned);
        let _ = manifest.close();
    }

    #[test]
    fn managed_format_persists_zero_usage_for_the_configured_namespace_set() {
        let path = TestPath::new("managed-zero-usage");
        let host_writes = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
        let initial = [
            NamespaceUsage {
                namespace: 0,
                live_bytes: 0,
            },
            NamespaceUsage {
                namespace: 9,
                live_bytes: 0,
            },
        ];
        let (manifest, opened) = HybridManifest::open_managed_with_journal_capacity(
            &path.0,
            73,
            TEST_JOURNAL_CAPACITY,
            host_writes,
            &initial,
        )
        .unwrap();
        assert!(opened.created);
        assert!(opened.needs_recovery);
        assert_eq!(
            manifest.namespace_usage_checkpoint().unwrap().unwrap(),
            initial
        );
        manifest.close().unwrap();

        // A crash immediately after format but before Hybrid opens either
        // lower tier must not turn the zero usage template into a trusted
        // clean checkpoint on the next process.
        let host_writes = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
        let (reopened, opened) = HybridManifest::open_managed_with_journal_capacity(
            &path.0,
            73,
            TEST_JOURNAL_CAPACITY,
            host_writes,
            &initial,
        )
        .unwrap();
        assert!(!opened.created);
        assert!(opened.needs_recovery);
        reopened.close().unwrap();
    }

    #[test]
    fn format_v1_manifest_and_journal_match_committed_golden_bytes() {
        let manifest_golden = sparse_golden(include_str!(
            "../tests/fixtures/format_v1/hybrid_manifest_slot.golden"
        ));
        let mut cache_id = [0_u8; 16];
        for (byte, value) in cache_id.iter_mut().zip(0_u8..) {
            *byte = value;
        }
        let slot = ManifestSlot {
            generation: 0x0102_0304_0506_0708,
            cache_id,
            version_epoch: 3,
            next_seqno: 35,
            layout_fingerprint: 0x1122_3344_5566_7788,
            journal_generation: 9,
            journal_capacity: TEST_JOURNAL_CAPACITY,
            checkpoint_version: HybridVersion {
                epoch: 3,
                seqno: 34,
            },
            clear_floor: HybridVersion {
                epoch: 2,
                seqno: 99,
            },
            clean: true,
            namespace_usage: NamespaceUsageCheckpoint::absent(),
        };
        assert_eq!(slot.encode().as_slice(), manifest_golden);
        assert!(matches!(
            ManifestSlot::probe(&manifest_golden),
            ManifestSlotProbe::Valid(decoded)
                if decoded.generation == slot.generation
                    && decoded.cache_id == slot.cache_id
                    && decoded.checkpoint_version == slot.checkpoint_version
        ));

        let input = JournalIntentInput {
            kind: JournalIntentKind::PutBucket,
            namespace: 7,
            key_hash: 0x8877_6655_4433_2211,
            bucket_id: Some(4),
            key: b"key",
        };
        let journal_golden = sparse_golden(include_str!(
            "../tests/fixtures/format_v1/hybrid_journal_record.golden"
        ));
        let mut encoded = vec![0_u8; journal_golden.len()];
        let version = HybridVersion {
            epoch: 3,
            seqno: 35,
        };
        encode_journal_record(&mut encoded, 9, version, &input).unwrap();
        assert_eq!(encoded, journal_golden);
        assert_eq!(
            decode_journal_record(&journal_golden, 9).unwrap(),
            Some(JournalIntent {
                kind: JournalIntentKind::PutBucket,
                version,
                namespace: 7,
                key_hash: 0x8877_6655_4433_2211,
                bucket_id: Some(4),
                key: b"key".to_vec(),
            })
        );
    }

    #[test]
    fn format_v1_usage_extension_roundtrips_and_legacy_zero_extension_remains_readable() {
        let legacy = sparse_golden(include_str!(
            "../tests/fixtures/format_v1/hybrid_manifest_slot.golden"
        ));
        let ManifestSlotProbe::Valid(mut slot) = ManifestSlot::probe(&legacy) else {
            panic!("legacy V1 golden must remain readable");
        };
        assert!(!slot.namespace_usage.present);

        let usage = [
            NamespaceUsage {
                namespace: 0,
                live_bytes: 17,
            },
            NamespaceUsage {
                namespace: 7,
                live_bytes: u64::MAX - 1,
            },
        ];
        slot.namespace_usage = NamespaceUsageCheckpoint::try_from_usage(&usage).unwrap();
        let encoded = slot.encode();
        let ManifestSlotProbe::Valid(decoded) = ManifestSlot::probe(&encoded) else {
            panic!("usage-extended V1 slot must decode");
        };
        assert!(decoded.namespace_usage.present);
        assert_eq!(decoded.namespace_usage.as_slice(), usage);
    }

    #[test]
    fn corrupt_usage_extension_invalidates_only_its_manifest_slot() {
        let legacy = sparse_golden(include_str!(
            "../tests/fixtures/format_v1/hybrid_manifest_slot.golden"
        ));
        let ManifestSlotProbe::Valid(mut first) = ManifestSlot::probe(&legacy) else {
            panic!("legacy V1 golden must decode");
        };
        first.generation = 1;
        first.namespace_usage = NamespaceUsageCheckpoint::try_from_usage(&[NamespaceUsage {
            namespace: 0,
            live_bytes: 11,
        }])
        .unwrap();
        let mut second = ManifestSlot {
            generation: 2,
            namespace_usage: NamespaceUsageCheckpoint::try_from_usage(&[NamespaceUsage {
                namespace: 0,
                live_bytes: 22,
            }])
            .unwrap(),
            ..first
        }
        .encode();
        second[USAGE_EXTENSION_ENTRIES_OFFSET + 8] ^= 1;
        // Recompute the enclosing checksum to prove that the extension's
        // independent checksum participates in slot validity.
        put_u32(&mut second, MANIFEST_CRC_OFFSET, 0);
        let outer = crc32c(&second);
        put_u32(&mut second, MANIFEST_CRC_OFFSET, outer);
        assert!(matches!(
            ManifestSlot::probe(&second),
            ManifestSlotProbe::Unrecognized
        ));

        let path = TestPath::new("usage-corrupt-fallback");
        let mut file = vec![0_u8; manifest_file_len(TEST_JOURNAL_CAPACITY).unwrap() as usize];
        file[..MANIFEST_SLOT_SIZE].copy_from_slice(&first.encode());
        file[MANIFEST_SLOT_SIZE..JOURNAL_OFFSET as usize].copy_from_slice(&second);
        fs::write(&path.0, file).unwrap();
        let (manifest, opened) = open(&path.0, first.layout_fingerprint).unwrap();
        assert!(!opened.needs_recovery);
        assert_eq!(
            manifest.namespace_usage_checkpoint().unwrap().unwrap(),
            vec![NamespaceUsage {
                namespace: 0,
                live_bytes: 11,
            }]
        );
        manifest.close().unwrap();
    }

    #[test]
    fn unsupported_manifest_version_is_rejected_without_downgrade_or_rewrite() {
        let path = TestPath::new("unsupported-v2");
        let v1 = sparse_golden(include_str!(
            "../tests/fixtures/format_v1/hybrid_manifest_slot.golden"
        ));
        let v2 = sparse_golden(include_str!(
            "../tests/fixtures/format_v1/hybrid_manifest_v2.golden"
        ));
        assert!(matches!(
            ManifestSlot::probe(&v2),
            ManifestSlotProbe::Unsupported(2)
        ));
        let mut file = vec![0_u8; manifest_file_len(TEST_JOURNAL_CAPACITY).unwrap() as usize];
        file[..MANIFEST_SLOT_SIZE].copy_from_slice(&v1);
        file[MANIFEST_SLOT_SIZE..JOURNAL_OFFSET as usize].copy_from_slice(&v2);
        fs::write(&path.0, &file).unwrap();
        let before = fs::read(&path.0).unwrap();

        assert!(matches!(
            open(&path.0, 0x1122_3344_5566_7788),
            Err(CacheError::InvalidConfig(message))
                if message.contains("format version 2")
        ));
        assert_eq!(fs::read(&path.0).unwrap(), before);
    }

    #[test]
    fn interrupted_format_reopens_as_an_empty_v1_manifest() {
        const EIO: i32 = 5;
        for (name, event, occurrence, action) in [
            (
                "format-first-slot",
                FaultEvent::Write(WritePoint::HybridManifest),
                1,
                FaultAction::Torn {
                    bytes: 13,
                    raw_os_error: EIO,
                },
            ),
            (
                "format-second-slot",
                FaultEvent::Write(WritePoint::HybridManifest),
                2,
                FaultAction::Torn {
                    bytes: 13,
                    raw_os_error: EIO,
                },
            ),
            (
                "format-truncate-sync",
                FaultEvent::Sync(SyncPoint::FormatTruncate),
                1,
                FaultAction::Error(EIO),
            ),
            (
                "format-clean-sync",
                FaultEvent::Sync(SyncPoint::FormatClean),
                1,
                FaultAction::Error(EIO),
            ),
        ] {
            let path = TestPath::new(name);
            let (backend, handle) = FaultBackend::open(&path.0).unwrap();
            handle.arm(event, occurrence, action);
            assert_io_error(
                HybridManifest::open_with_backend(Box::new(backend), 23, TEST_JOURNAL_CAPACITY),
                EIO,
            );

            let (reopened, opened) = open(&path.0, 23).unwrap();
            assert!(opened.journal.intents.is_empty(), "{name}");
            assert!(!opened.needs_recovery, "{name}");
            let snapshot = reopened.snapshot().unwrap();
            assert!(snapshot.clean, "{name}");
            assert_eq!(snapshot.checkpoint_version, HybridVersion::ZERO, "{name}");
            reopened.close().unwrap();
        }
    }

    #[test]
    fn append_failpoints_poison_runtime_and_reopen_only_a_durable_intent_or_miss() {
        const EIO: i32 = 5;
        const ENOSPC: i32 = 28;
        let cases = [
            (
                "dirty-slot-a-torn",
                FaultEvent::Write(WritePoint::HybridManifest),
                1,
                FaultAction::Torn {
                    bytes: 13,
                    raw_os_error: EIO,
                },
                EIO,
            ),
            (
                "dirty-sync-a-eio",
                FaultEvent::Sync(SyncPoint::HybridManifestDirty),
                1,
                FaultAction::Error(EIO),
                EIO,
            ),
            (
                "dirty-slot-b-enospc",
                FaultEvent::Write(WritePoint::HybridManifest),
                2,
                FaultAction::Torn {
                    bytes: 37,
                    raw_os_error: ENOSPC,
                },
                ENOSPC,
            ),
            (
                "dirty-sync-b-enospc",
                FaultEvent::Sync(SyncPoint::HybridManifestDirty),
                2,
                FaultAction::Error(ENOSPC),
                ENOSPC,
            ),
            (
                "journal-record-torn",
                FaultEvent::Write(WritePoint::HybridJournal),
                1,
                FaultAction::Torn {
                    bytes: 13,
                    raw_os_error: EIO,
                },
                EIO,
            ),
            (
                "journal-sentinel-enospc",
                FaultEvent::Write(WritePoint::HybridJournal),
                2,
                FaultAction::Torn {
                    bytes: 13,
                    raw_os_error: ENOSPC,
                },
                ENOSPC,
            ),
            (
                "journal-sync-eio",
                FaultEvent::Sync(SyncPoint::HybridJournal),
                1,
                FaultAction::Error(EIO),
                EIO,
            ),
        ];

        for (name, event, occurrence, action, error_code) in cases {
            let path = TestPath::new(name);
            let (manifest, _, handle) = open_fault(&path.0, 29).unwrap();
            handle.arm(event, occurrence, action);
            assert_io_error(manifest.append_intent(put_bucket(b"key", 4)), error_code);
            assert_eq!(manifest.status(), CacheStatus::Poisoned, "{name}");
            assert!(
                matches!(manifest.close(), Err(CacheError::Poisoned)),
                "{name}"
            );

            let (reopened, opened) = open(&path.0, 29).unwrap();
            let snapshot = reopened.snapshot().unwrap();
            assert_eq!(snapshot.checkpoint_version, HybridVersion::ZERO, "{name}");
            assert!(
                opened.journal.intents.is_empty()
                    || opened.journal.intents.to_owned_vec().as_slice()
                        == [JournalIntent {
                            kind: JournalIntentKind::PutBucket,
                            version: HybridVersion { epoch: 1, seqno: 1 },
                            namespace: 7,
                            key_hash: 0x1234,
                            bucket_id: Some(4),
                            key: b"key".to_vec(),
                        }],
                "{name} reopened an impossible journal: {:?}",
                opened.journal
            );
            if snapshot.clean {
                assert!(opened.journal.intents.is_empty(), "{name}");
                assert!(!opened.needs_recovery, "{name}");
            } else {
                assert!(opened.needs_recovery, "{name}");
            }
            reopened.close().unwrap();
        }
    }

    #[test]
    fn positioned_short_writes_are_completed_before_publication() {
        for (name, event, occurrence) in [
            (
                "short-dirty-slot",
                FaultEvent::Write(WritePoint::HybridManifest),
                1,
            ),
            (
                "short-journal-record",
                FaultEvent::Write(WritePoint::HybridJournal),
                1,
            ),
            (
                "short-journal-sentinel",
                FaultEvent::Write(WritePoint::HybridJournal),
                2,
            ),
        ] {
            let path = TestPath::new(name);
            let (manifest, _, handle) = open_fault(&path.0, 31).unwrap();
            handle.arm(event, occurrence, FaultAction::Short(7));
            let version = manifest.append_intent(put_bucket(b"key", 4)).unwrap();
            manifest.close().unwrap();

            let (reopened, opened) = open(&path.0, 31).unwrap();
            assert_eq!(opened.journal.intents.len(), 1, "{name}");
            assert_eq!(
                opened.journal.intents.get(0).unwrap().version,
                version,
                "{name}"
            );
            assert!(!opened.journal.ignored_torn_tail, "{name}");
            reopened.close().unwrap();
        }
    }

    #[test]
    fn clean_checkpoint_failpoints_reopen_old_dirty_or_exact_new_clean_boundary() {
        const EIO: i32 = 5;
        const ENOSPC: i32 = 28;
        for (name, event, occurrence, action, error_code) in [
            (
                "clean-slot-a-torn",
                FaultEvent::Write(WritePoint::HybridManifest),
                1,
                FaultAction::Torn {
                    bytes: 19,
                    raw_os_error: EIO,
                },
                EIO,
            ),
            (
                "clean-sync-a-eio",
                FaultEvent::Sync(SyncPoint::HybridManifestClean),
                1,
                FaultAction::Error(EIO),
                EIO,
            ),
            (
                "clean-slot-b-enospc",
                FaultEvent::Write(WritePoint::HybridManifest),
                2,
                FaultAction::Torn {
                    bytes: 43,
                    raw_os_error: ENOSPC,
                },
                ENOSPC,
            ),
            (
                "clean-sync-b-enospc",
                FaultEvent::Sync(SyncPoint::HybridManifestClean),
                2,
                FaultAction::Error(ENOSPC),
                ENOSPC,
            ),
        ] {
            let path = TestPath::new(name);
            let (manifest, _, handle) = open_fault(&path.0, 37).unwrap();
            let version = manifest.append_intent(put_bucket(b"key", 4)).unwrap();
            let dirty = manifest.snapshot().unwrap();
            handle.arm(event, occurrence, action);
            assert_io_error(manifest.publish_clean(), error_code);
            assert_eq!(manifest.status(), CacheStatus::Poisoned, "{name}");
            assert!(
                matches!(manifest.close(), Err(CacheError::Poisoned)),
                "{name}"
            );

            let (reopened, opened) = open(&path.0, 37).unwrap();
            let recovered = reopened.snapshot().unwrap();
            if recovered.clean {
                assert_eq!(recovered.checkpoint_version, version, "{name}");
                assert_eq!(
                    recovered.journal_generation,
                    dirty.journal_generation + 1,
                    "{name}"
                );
                assert!(opened.journal.intents.is_empty(), "{name}");
                assert!(!opened.needs_recovery, "{name}");
            } else {
                assert_eq!(recovered.checkpoint_version, HybridVersion::ZERO, "{name}");
                assert_eq!(
                    recovered.journal_generation, dirty.journal_generation,
                    "{name}"
                );
                assert_eq!(opened.journal.intents.len(), 1, "{name}");
                assert_eq!(
                    opened.journal.intents.get(0).unwrap().version,
                    version,
                    "{name}"
                );
                assert!(opened.needs_recovery, "{name}");
            }
            reopened.close().unwrap();
        }
    }

    #[test]
    fn kill_restart_matrix_preserves_only_old_dirty_or_exact_new_clean_state() {
        for (name, event, occurrence) in [
            ("format-truncate", "format-truncate-sync", 1),
            ("format-slot-a", "manifest", 1),
            ("format-slot-b", "manifest", 2),
            ("format-clean", "format-clean-sync", 1),
        ] {
            for timing in ["before", "after"] {
                let path = TestPath::new(&format!("crash-{name}-{timing}"));
                run_crash_worker(&path.0, event, occurrence, timing, "format");
                let (manifest, opened) = open(&path.0, 41).unwrap();
                assert!(!opened.needs_recovery, "{name}/{timing}");
                assert!(opened.journal.intents.is_empty(), "{name}/{timing}");
                assert!(manifest.snapshot().unwrap().clean, "{name}/{timing}");
                manifest.close().unwrap();
            }
        }

        for (name, event, occurrence) in [
            ("dirty-slot-a", "manifest", 1),
            ("dirty-sync-a", "dirty-sync", 1),
            ("dirty-slot-b", "manifest", 2),
            ("dirty-sync-b", "dirty-sync", 2),
            ("journal-record", "journal", 1),
            ("journal-sentinel", "journal", 2),
            ("journal-sync", "journal-sync", 1),
        ] {
            for timing in ["before", "after"] {
                let path = TestPath::new(&format!("crash-append-{name}-{timing}"));
                let (manifest, _) = open(&path.0, 43).unwrap();
                manifest.close().unwrap();
                run_crash_worker(&path.0, event, occurrence, timing, "append");

                let (reopened, opened) = open(&path.0, 43).unwrap();
                let snapshot = reopened.snapshot().unwrap();
                assert_eq!(snapshot.checkpoint_version, HybridVersion::ZERO);
                assert!(
                    opened.journal.intents.is_empty()
                        || opened.journal.intents.to_owned_vec().as_slice()
                            == [JournalIntent {
                                kind: JournalIntentKind::PutBucket,
                                version: HybridVersion { epoch: 1, seqno: 1 },
                                namespace: 7,
                                key_hash: 0x1234,
                                bucket_id: Some(4),
                                key: b"key".to_vec(),
                            }],
                    "{name}/{timing}: {:?}",
                    opened.journal
                );
                if snapshot.clean {
                    assert!(opened.journal.intents.is_empty(), "{name}/{timing}");
                    assert!(!opened.needs_recovery, "{name}/{timing}");
                } else {
                    assert!(opened.needs_recovery, "{name}/{timing}");
                }
                reopened.close().unwrap();
            }
        }

        for (name, event, occurrence) in [
            ("clean-slot-a", "manifest", 1),
            ("clean-sync-a", "clean-sync", 1),
            ("clean-slot-b", "manifest", 2),
            ("clean-sync-b", "clean-sync", 2),
        ] {
            for timing in ["before", "after"] {
                let path = TestPath::new(&format!("crash-clean-{name}-{timing}"));
                let (manifest, _) = open(&path.0, 47).unwrap();
                manifest.close().unwrap();
                run_crash_worker(&path.0, event, occurrence, timing, "clean");

                let (reopened, opened) = open(&path.0, 47).unwrap();
                let snapshot = reopened.snapshot().unwrap();
                let expected = HybridVersion { epoch: 1, seqno: 1 };
                if snapshot.clean {
                    assert_eq!(snapshot.checkpoint_version, expected, "{name}/{timing}");
                    assert_eq!(snapshot.journal_generation, 2, "{name}/{timing}");
                    assert!(opened.journal.intents.is_empty(), "{name}/{timing}");
                    assert!(!opened.needs_recovery, "{name}/{timing}");
                } else {
                    assert_eq!(snapshot.checkpoint_version, HybridVersion::ZERO);
                    assert_eq!(snapshot.journal_generation, 1);
                    assert_eq!(opened.journal.intents.len(), 1, "{name}/{timing}");
                    assert_eq!(opened.journal.intents.get(0).unwrap().version, expected);
                    assert!(opened.needs_recovery, "{name}/{timing}");
                }
                reopened.close().unwrap();
            }
        }
    }

    fn run_crash_worker(
        path: &Path,
        event: &str,
        occurrence: usize,
        timing: &str,
        operation: &str,
    ) {
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("hybrid_manifest::tests::crash_worker")
            .arg("--ignored")
            .arg("--test-threads=1")
            .env("CACHE_RS_HYBRID_CRASH_PATH", path)
            .env("CACHE_RS_HYBRID_CRASH_EVENT", event)
            .env("CACHE_RS_HYBRID_CRASH_OCCURRENCE", occurrence.to_string())
            .env("CACHE_RS_HYBRID_CRASH_TIMING", timing)
            .env("CACHE_RS_HYBRID_CRASH_OPERATION", operation)
            .output()
            .unwrap();
        assert_eq!(
            output.status.signal(),
            Some(9),
            "crash worker missed {operation}/{event}/{occurrence}/{timing}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "spawned by kill_restart_matrix_preserves_only_old_dirty_or_exact_new_clean_state"]
    fn crash_worker() {
        let Ok(path) = std::env::var("CACHE_RS_HYBRID_CRASH_PATH") else {
            return;
        };
        let event = std::env::var("CACHE_RS_HYBRID_CRASH_EVENT").unwrap();
        let occurrence = std::env::var("CACHE_RS_HYBRID_CRASH_OCCURRENCE")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let action = match std::env::var("CACHE_RS_HYBRID_CRASH_TIMING")
            .unwrap()
            .as_str()
        {
            "before" => FaultAction::KillBefore,
            "after" => FaultAction::KillAfter,
            timing => panic!("unknown crash timing {timing}"),
        };
        let fault_event = match event.as_str() {
            "manifest" => FaultEvent::Write(WritePoint::HybridManifest),
            "journal" => FaultEvent::Write(WritePoint::HybridJournal),
            "format-truncate-sync" => FaultEvent::Sync(SyncPoint::FormatTruncate),
            "format-clean-sync" => FaultEvent::Sync(SyncPoint::FormatClean),
            "dirty-sync" => FaultEvent::Sync(SyncPoint::HybridManifestDirty),
            "journal-sync" => FaultEvent::Sync(SyncPoint::HybridJournal),
            "clean-sync" => FaultEvent::Sync(SyncPoint::HybridManifestClean),
            event => panic!("unknown crash event {event}"),
        };
        let operation = std::env::var("CACHE_RS_HYBRID_CRASH_OPERATION").unwrap();
        let path = PathBuf::from(path);
        let (backend, handle) = FaultBackend::open(&path).unwrap();
        if operation == "format" {
            handle.arm(fault_event, occurrence, action);
            let _ = HybridManifest::open_with_backend(Box::new(backend), 41, TEST_JOURNAL_CAPACITY)
                .unwrap();
            panic!("format crash fault was not reached");
        }

        let (manifest, _) = HybridManifest::open_with_backend(
            Box::new(backend),
            if operation == "append" { 43 } else { 47 },
            TEST_JOURNAL_CAPACITY,
        )
        .unwrap();
        if operation == "clean" {
            manifest.append_intent(put_bucket(b"key", 4)).unwrap();
        }
        handle.arm(fault_event, occurrence, action);
        match operation.as_str() {
            "append" => {
                manifest.append_intent(put_bucket(b"key", 4)).unwrap();
            }
            "clean" => {
                manifest.publish_clean().unwrap();
            }
            operation => panic!("unknown crash operation {operation}"),
        }
        panic!("crash fault was not reached");
    }

    #[test]
    fn dirty_reopen_scans_intents_and_clean_checkpoint_reuses_generation() {
        let path = TestPath::new("lifecycle");
        let (manifest, opened) = open(&path.0, 7).unwrap();
        assert!(opened.created);
        assert!(opened.journal.intents.is_empty());
        let initial = manifest.snapshot().unwrap();
        let versions = manifest
            .append_batch(&[
                put_bucket(b"small", 9),
                JournalIntentInput {
                    kind: JournalIntentKind::Delete,
                    namespace: 7,
                    key_hash: 0x5678,
                    bucket_id: Some(10),
                    key: b"removed",
                },
            ])
            .unwrap();
        assert!(versions[0] < versions[1]);
        assert!(!manifest.snapshot().unwrap().clean);
        manifest.close().unwrap();

        let (reopened, opened) = open(&path.0, 7).unwrap();
        assert!(opened.needs_recovery);
        assert_eq!(opened.journal.intents.len(), 2);
        assert_eq!(opened.journal.intents.get(0).unwrap().bucket_id, Some(9));
        let recovered = reopened.finish_dirty_recovery().unwrap();
        assert_eq!(recovered.cache_id, initial.cache_id);
        assert_eq!(recovered.checkpoint_version, versions[1]);
        assert!(recovered.version_epoch > versions[1].epoch);
        assert!(recovered.clean);
        assert_eq!(recovered.journal_bytes, 0);
        reopened.close().unwrap();

        let (clean, opened) = open(&path.0, 7).unwrap();
        assert!(!opened.needs_recovery);
        assert!(opened.journal.intents.is_empty());
        assert_eq!(clean.snapshot().unwrap().cache_id, initial.cache_id);
        clean.close().unwrap();
    }

    #[test]
    fn minimum_record_density_has_a_single_bounded_recovery_representation() {
        assert_eq!(
            journal_record_len(0),
            Some(MIN_JOURNAL_RECORD_SIZE as usize)
        );
        let path = TestPath::new("dense-recovery-memory");
        let (manifest, _) = open(&path.0, 8).unwrap();
        let intent = JournalIntentInput {
            kind: JournalIntentKind::Delete,
            namespace: 1,
            key_hash: 7,
            bucket_id: None,
            key: &[],
        };
        let intent_count = usize::try_from(
            (TEST_JOURNAL_CAPACITY - JOURNAL_COMMIT_SENTINEL_BYTES as u64)
                / MIN_JOURNAL_RECORD_SIZE,
        )
        .unwrap();
        let inputs = vec![intent; intent_count];
        manifest.append_batch(&inputs).unwrap();
        manifest.close().unwrap();

        let (reopened, mut opened) = open(&path.0, 8).unwrap();
        assert_eq!(opened.journal.intents.len(), intent_count);
        let recovery_bound = journal_recovery_memory_bytes(TEST_JOURNAL_CAPACITY).unwrap();
        assert!(opened.journal.intents.allocated_bytes() <= recovery_bound);
        let allocated_before_dedup = opened.journal.intents.allocated_bytes();
        opened.journal.intents.sort_and_dedup_keys();
        assert_eq!(opened.journal.intents.len(), 1);
        assert_eq!(
            opened.journal.intents.allocated_bytes(),
            allocated_before_dedup
        );

        let maximum_bound = journal_recovery_memory_bytes(MAX_JOURNAL_CAPACITY).unwrap();
        assert_eq!(
            maximum_bound,
            usize::try_from(MAX_JOURNAL_CAPACITY).unwrap()
                + usize::try_from(MAX_JOURNAL_CAPACITY / MIN_JOURNAL_RECORD_SIZE).unwrap()
                    * size_of::<u32>()
        );
        reopened.close().unwrap();
    }

    #[test]
    fn clear_intent_advances_full_version_floor_only_after_durable_append() {
        let path = TestPath::new("clear");
        let (manifest, _) = open(&path.0, 11).unwrap();
        let old = manifest.append_intent(put_bucket(b"old", 1)).unwrap();
        let (dirty, clear) = manifest.begin_clear().unwrap();
        assert!(clear > old);
        assert_eq!(dirty.clear_floor, clear);
        assert!(!dirty.clean);
        manifest.close().unwrap();

        let (reopened, opened) = open(&path.0, 11).unwrap();
        assert!(opened.journal.contains_clear);
        assert_eq!(opened.journal.highest_clear_version, clear);
        assert!(opened.journal.intents.is_empty());
        assert_eq!(opened.journal.intents.allocated_bytes(), 0);
        assert_eq!(reopened.snapshot().unwrap().clear_floor, clear);
        let clean = reopened.finish_dirty_recovery().unwrap();
        assert_eq!(clean.clear_floor, clear);
        reopened.close().unwrap();
    }

    #[test]
    fn torn_unsynced_tail_is_ignored_after_the_last_complete_intent() {
        let path = TestPath::new("torn-tail");
        let (manifest, _) = open(&path.0, 13).unwrap();
        manifest.append_intent(put_bucket(b"complete", 3)).unwrap();
        let tail = manifest.snapshot().unwrap().journal_bytes;
        manifest.close().unwrap();

        let mut file = OpenOptions::new().write(true).open(&path.0).unwrap();
        file.seek(SeekFrom::Start(JOURNAL_OFFSET + tail)).unwrap();
        file.write_all(&JOURNAL_MAGIC[..3]).unwrap();
        file.sync_data().unwrap();

        let (reopened, opened) = open(&path.0, 13).unwrap();
        assert_eq!(opened.journal.intent_count, 1);
        assert!(opened.journal.intents.is_empty());
        assert_eq!(opened.journal.intents.allocated_bytes(), 0);
        assert!(opened.journal.ignored_torn_tail);
        assert!(opened.journal.requires_full_clear);
        reopened.close().unwrap();
    }

    #[test]
    fn corrupt_middle_record_requires_full_clear_before_later_valid_intent() {
        let path = TestPath::new("corrupt-middle");
        let (manifest, _) = open(&path.0, 15).unwrap();
        manifest
            .append_batch(&[put_bucket(b"first", 4), put_bucket(b"later-valid", 5)])
            .unwrap();
        manifest.close().unwrap();

        // Damage only the first record payload. The second record and its CRC
        // remain complete in the same journal generation, so recovery must not
        // mistake the first failure for a harmless end-of-log tear.
        let mut file = OpenOptions::new().write(true).open(&path.0).unwrap();
        file.seek(SeekFrom::Start(JOURNAL_OFFSET + JOURNAL_HEADER_SIZE as u64))
            .unwrap();
        file.write_all(b"X").unwrap();
        file.sync_data().unwrap();

        let (reopened, opened) = open(&path.0, 15).unwrap();
        assert!(opened.needs_recovery);
        assert!(opened.journal.requires_full_clear);
        assert!(opened.journal.intents.is_empty());
        reopened.close().unwrap();
    }

    #[test]
    fn layout_and_journal_capacity_mismatch_are_rejected_without_reformatting() {
        let path = TestPath::new("layout");
        let (manifest, _) = open(&path.0, 17).unwrap();
        manifest.close().unwrap();
        assert!(matches!(
            open(&path.0, 18),
            Err(CacheError::InvalidConfig(_))
        ));
        assert!(matches!(
            HybridManifest::open_with_journal_capacity(&path.0, 17, TEST_JOURNAL_CAPACITY * 2,),
            Err(CacheError::InvalidConfig(_))
        ));
    }
}
