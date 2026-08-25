//! Shard-local eviction policies for the volatile RAM tier.
//!
//! Capacity accounting, pending-write pinning, TTL, and key correctness stay
//! in `memory`; policies only maintain access metadata and choose a clean slot.

use std::collections::HashMap;
use std::io;

/// Maximum policy metadata inspected by one foreground victim selection.
/// Exhausting this budget means L1 bypass; it never expands with cache size.
const MAX_POLICY_SCAN_STEPS: usize = 64;
const FREQUENCY_AGE_BATCH: usize = 64;

/// Runtime-selectable RAM-tier eviction policy.
///
/// This setting never participates in the persistent cache fingerprint and may
/// change between warm opens.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EvictionPolicy {
    /// Low-overhead second-chance CLOCK. This is the default.
    #[default]
    Clock,
    /// Exact least-recently-used ordering.
    Lru,
    /// TinyLFU admission backed by an LRU replacement order.
    TinyLfu,
    /// Lazy-promotion SIEVE with one visited bit per resident entry.
    Sieve,
    /// First-in-first-out ordering; hits do not affect eviction order.
    Fifo,
    /// S3-FIFO with small, main, and non-resident ghost queues.
    S3Fifo,
}

#[cfg(test)]
impl EvictionPolicy {
    pub(crate) const ALL: [Self; 6] = [
        Self::Clock,
        Self::Lru,
        Self::TinyLfu,
        Self::Sieve,
        Self::Fifo,
        Self::S3Fifo,
    ];
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AdmissionHint {
    s3_main: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VictimSelection {
    Victim(usize),
    Reject,
    None,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum QueueId {
    #[default]
    None,
    Primary,
    Small,
    Main,
}

const RESIDENT_BIT: u8 = 1 << 0;
const EVICTABLE_BIT: u8 = 1 << 1;
const QUEUE_SHIFT: u8 = 2;
const QUEUE_MASK: u8 = 0b11 << QUEUE_SHIFT;
const VISITED_BIT: u8 = 1 << 4;
const FREQUENCY_SHIFT: u8 = 5;
const FREQUENCY_MASK: u8 = 0b11 << FREQUENCY_SHIFT;
const LINK_INDEX_BITS: u32 = 25;
const LINK_INDEX_MASK: u32 = (1 << LINK_INDEX_BITS) - 1;
const FLAGS_SHIFT: u32 = LINK_INDEX_BITS;
pub(crate) const MAX_POLICY_SLOT_INDEX: usize = (LINK_INDEX_MASK - 1) as usize;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PolicySlot {
    hash: u64,
    previous_and_flags: u32,
    next: u32,
}

impl PolicySlot {
    fn new(hash: u64, evictable: bool) -> Self {
        let mut slot = Self {
            hash,
            ..Self::default()
        };
        slot.set_flags(RESIDENT_BIT);
        slot.set_evictable(evictable);
        slot
    }

    pub(crate) fn hash(&self) -> u64 {
        self.hash
    }

    pub(crate) fn is_evictable(&self) -> bool {
        self.flags() & EVICTABLE_BIT != 0
    }

    fn is_resident(&self) -> bool {
        self.flags() & RESIDENT_BIT != 0
    }

    fn set_evictable(&mut self, evictable: bool) {
        self.set_flags((self.flags() & !EVICTABLE_BIT) | (u8::from(evictable) * EVICTABLE_BIT));
    }

    fn queue(&self) -> QueueId {
        match (self.flags() & QUEUE_MASK) >> QUEUE_SHIFT {
            1 => QueueId::Primary,
            2 => QueueId::Small,
            3 => QueueId::Main,
            _ => QueueId::None,
        }
    }

    fn set_queue(&mut self, queue: QueueId) {
        self.set_flags((self.flags() & !QUEUE_MASK) | ((queue as u8) << QUEUE_SHIFT));
    }

    fn is_visited(&self) -> bool {
        self.flags() & VISITED_BIT != 0
    }

    fn set_visited(&mut self, visited: bool) {
        self.set_flags((self.flags() & !VISITED_BIT) | (u8::from(visited) * VISITED_BIT));
    }

    fn frequency(&self) -> u8 {
        (self.flags() & FREQUENCY_MASK) >> FREQUENCY_SHIFT
    }

    fn set_frequency(&mut self, frequency: u8) {
        debug_assert!(frequency <= 3);
        self.set_flags(
            (self.flags() & !FREQUENCY_MASK) | ((frequency << FREQUENCY_SHIFT) & FREQUENCY_MASK),
        );
    }

    fn flags(&self) -> u8 {
        (self.previous_and_flags >> FLAGS_SHIFT) as u8
    }

    fn set_flags(&mut self, flags: u8) {
        debug_assert!(flags <= u8::MAX >> 1);
        self.previous_and_flags =
            (self.previous_and_flags & LINK_INDEX_MASK) | (u32::from(flags) << FLAGS_SHIFT);
    }

    fn previous(&self) -> Option<usize> {
        unpack_index(self.previous_and_flags)
    }

    fn set_previous(&mut self, index: Option<usize>) {
        self.previous_and_flags =
            (self.previous_and_flags & !LINK_INDEX_MASK) | pack_optional_index(index);
    }

    fn next(&self) -> Option<usize> {
        unpack_index(self.next)
    }

    fn set_next(&mut self, index: Option<usize>) {
        self.next = pack_optional_index(index);
    }
}

fn pack_index(index: usize) -> Option<u32> {
    let packed = u32::try_from(index).ok()?.checked_add(1)?;
    (packed <= LINK_INDEX_MASK).then_some(packed)
}

fn pack_optional_index(index: Option<usize>) -> u32 {
    index.map_or(0, |index| {
        pack_index(index).expect("eviction slot index exceeds packed-link limit")
    })
}

fn unpack_index(index: u32) -> Option<usize> {
    let packed = index & LINK_INDEX_MASK;
    (packed != 0).then(|| (packed - 1) as usize)
}

#[derive(Default)]
struct SlotList {
    head: Option<usize>,
    tail: Option<usize>,
    len: usize,
    weight: usize,
}

impl SlotList {
    fn push_front(
        &mut self,
        slots: &mut [PolicySlot],
        index: usize,
        queue: QueueId,
        weight: usize,
    ) {
        debug_assert!(slots[index].is_resident());
        debug_assert_eq!(slots[index].queue(), QueueId::None);
        let old_head = self.head;
        slots[index].set_previous(None);
        slots[index].set_next(old_head);
        slots[index].set_queue(queue);
        if let Some(old_head) = old_head {
            slots[old_head].set_previous(Some(index));
        } else {
            self.tail = Some(index);
        }
        self.head = Some(index);
        self.len += 1;
        self.weight = self.weight.saturating_add(weight);
    }

    fn remove(&mut self, slots: &mut [PolicySlot], index: usize, queue: QueueId, weight: usize) {
        debug_assert_eq!(slots[index].queue(), queue);
        let previous = slots[index].previous();
        let next = slots[index].next();
        if let Some(previous) = previous {
            slots[previous].set_next(next);
        } else {
            self.head = next;
        }
        if let Some(next) = next {
            slots[next].set_previous(previous);
        } else {
            self.tail = previous;
        }
        slots[index].set_previous(None);
        slots[index].set_next(None);
        slots[index].set_queue(QueueId::None);
        self.len = self.len.saturating_sub(1);
        self.weight = self.weight.saturating_sub(weight);
    }

    fn move_to_front(&mut self, slots: &mut [PolicySlot], index: usize, queue: QueueId) {
        if self.head == Some(index) {
            return;
        }
        self.remove(slots, index, queue, 0);
        self.push_front(slots, index, queue, 0);
    }

    fn oldest_evictable(&self, slots: &[PolicySlot]) -> Option<usize> {
        let mut candidate = self.tail;
        for _ in 0..self.len.min(MAX_POLICY_SCAN_STEPS) {
            let index = candidate?;
            let slot = &slots[index];
            if slot.is_resident() && slot.is_evictable() {
                return Some(index);
            }
            candidate = slot.previous();
        }
        None
    }
}

struct ClockState {
    hand: usize,
}

#[derive(Default)]
struct LruState {
    entries: SlotList,
}

struct TinyLfuState {
    entries: SlotList,
    frequencies: FrequencySketch,
}

#[derive(Default)]
struct SieveState {
    entries: SlotList,
    hand: Option<usize>,
}

#[derive(Default)]
struct FifoState {
    entries: SlotList,
}

struct S3FifoState {
    small: SlotList,
    main: SlotList,
    main_target_bytes: usize,
    small_target_bytes: usize,
    ghost: GhostQueue,
}

enum PolicyState {
    Clock(ClockState),
    Lru(LruState),
    TinyLfu(TinyLfuState),
    Sieve(SieveState),
    Fifo(FifoState),
    S3Fifo(S3FifoState),
}

pub(crate) struct EvictionState {
    state: PolicyState,
}

impl EvictionState {
    pub(crate) fn new(
        policy: EvictionPolicy,
        capacity_bytes: usize,
        maximum_entries: usize,
    ) -> io::Result<Self> {
        let state = match policy {
            EvictionPolicy::Clock => PolicyState::Clock(ClockState { hand: 0 }),
            EvictionPolicy::Lru => PolicyState::Lru(LruState::default()),
            EvictionPolicy::TinyLfu => PolicyState::TinyLfu(TinyLfuState {
                entries: SlotList::default(),
                frequencies: FrequencySketch::new(maximum_entries)?,
            }),
            EvictionPolicy::Sieve => PolicyState::Sieve(SieveState::default()),
            EvictionPolicy::Fifo => PolicyState::Fifo(FifoState::default()),
            EvictionPolicy::S3Fifo => {
                let small_target_bytes = capacity_bytes / 10;
                let main_target_bytes = capacity_bytes.saturating_sub(small_target_bytes);
                let ghost_entries = main_target_bytes
                    .saturating_mul(maximum_entries)
                    .checked_div(capacity_bytes.max(1))
                    .unwrap_or(0);
                PolicyState::S3Fifo(S3FifoState {
                    small: SlotList::default(),
                    main: SlotList::default(),
                    main_target_bytes,
                    small_target_bytes,
                    ghost: GhostQueue::new(ghost_entries),
                })
            }
        };
        Ok(Self { state })
    }

    pub(crate) fn prepare_insert(&mut self, hash: u64) -> AdmissionHint {
        match &mut self.state {
            PolicyState::TinyLfu(state) => {
                state.frequencies.increment(hash);
                AdmissionHint::default()
            }
            PolicyState::S3Fifo(state) => AdmissionHint {
                s3_main: state.ghost.take(hash),
            },
            _ => AdmissionHint::default(),
        }
    }

    pub(crate) fn record_miss(&mut self, hash: u64) -> AdmissionHint {
        self.prepare_insert(hash)
    }

    pub(crate) fn insert(
        &mut self,
        slots: &mut [PolicySlot],
        index: usize,
        hash: u64,
        weight: usize,
        evictable: bool,
        hint: AdmissionHint,
    ) {
        debug_assert!(!slots[index].is_resident());
        slots[index] = PolicySlot::new(hash, evictable);
        match &mut self.state {
            PolicyState::Clock(_) => slots[index].set_visited(true),
            PolicyState::Lru(state) => {
                state
                    .entries
                    .push_front(slots, index, QueueId::Primary, weight);
            }
            PolicyState::TinyLfu(state) => {
                state
                    .entries
                    .push_front(slots, index, QueueId::Primary, weight);
            }
            PolicyState::Sieve(state) => {
                state
                    .entries
                    .push_front(slots, index, QueueId::Primary, weight);
            }
            PolicyState::Fifo(state) => {
                state
                    .entries
                    .push_front(slots, index, QueueId::Primary, weight);
            }
            PolicyState::S3Fifo(state) => {
                let use_main = hint.s3_main
                    || state.small_target_bytes == 0
                    || weight > state.small_target_bytes;
                if use_main {
                    state.main.push_front(slots, index, QueueId::Main, weight);
                } else {
                    state.small.push_front(slots, index, QueueId::Small, weight);
                }
            }
        }
    }

    pub(crate) fn record_hit(&mut self, slots: &mut [PolicySlot], index: usize) {
        let hash = slots[index].hash();
        match &mut self.state {
            PolicyState::Clock(_) => slots[index].set_visited(true),
            PolicyState::Lru(state) => {
                state.entries.move_to_front(slots, index, QueueId::Primary);
            }
            PolicyState::TinyLfu(state) => {
                state.frequencies.increment(hash);
                state.entries.move_to_front(slots, index, QueueId::Primary);
            }
            PolicyState::Sieve(_) => slots[index].set_visited(true),
            PolicyState::Fifo(_) => {}
            PolicyState::S3Fifo(_) => {
                let frequency = slots[index].frequency().saturating_add(1).min(3);
                slots[index].set_frequency(frequency);
            }
        }
    }

    pub(crate) fn set_evictable(&mut self, slots: &mut [PolicySlot], index: usize) {
        if slots.get(index).is_some_and(PolicySlot::is_resident) {
            slots[index].set_evictable(true);
        }
    }

    pub(crate) fn remove(&mut self, slots: &mut [PolicySlot], index: usize, weight: usize) {
        if !slots.get(index).is_some_and(PolicySlot::is_resident) {
            return;
        }
        match &mut self.state {
            PolicyState::Clock(_) => {}
            PolicyState::Lru(state) => {
                state.entries.remove(slots, index, QueueId::Primary, weight);
            }
            PolicyState::TinyLfu(state) => {
                state.entries.remove(slots, index, QueueId::Primary, weight);
            }
            PolicyState::Sieve(state) => {
                if state.hand == Some(index) {
                    state.hand = next_sieve_candidate(&state.entries, slots, index);
                }
                state.entries.remove(slots, index, QueueId::Primary, weight);
                if state.entries.len == 0 {
                    state.hand = None;
                }
            }
            PolicyState::Fifo(state) => {
                state.entries.remove(slots, index, QueueId::Primary, weight);
            }
            PolicyState::S3Fifo(state) => match slots[index].queue() {
                QueueId::Small => state.small.remove(slots, index, QueueId::Small, weight),
                QueueId::Main => state.main.remove(slots, index, QueueId::Main, weight),
                QueueId::None | QueueId::Primary => {
                    debug_assert!(false, "S3-FIFO resident is outside its queues");
                }
            },
        }
        slots[index] = PolicySlot::default();
    }

    pub(crate) fn select_victim<F>(
        &mut self,
        slots: &mut [PolicySlot],
        incoming_hash: u64,
        apply_admission: bool,
        weight_of: F,
    ) -> VictimSelection
    where
        F: Fn(usize) -> usize,
    {
        match &mut self.state {
            PolicyState::Clock(state) => select_clock(state, slots),
            PolicyState::Lru(state) => state
                .entries
                .oldest_evictable(slots)
                .map_or(VictimSelection::None, VictimSelection::Victim),
            PolicyState::TinyLfu(state) => {
                let Some(victim) = state.entries.oldest_evictable(slots) else {
                    return VictimSelection::None;
                };
                if apply_admission
                    && state.frequencies.estimate(incoming_hash)
                        <= state.frequencies.estimate(slots[victim].hash())
                {
                    VictimSelection::Reject
                } else {
                    VictimSelection::Victim(victim)
                }
            }
            PolicyState::Sieve(state) => select_sieve(state, slots),
            PolicyState::Fifo(state) => state
                .entries
                .oldest_evictable(slots)
                .map_or(VictimSelection::None, VictimSelection::Victim),
            PolicyState::S3Fifo(state) => select_s3fifo(state, slots, weight_of),
        }
    }
}

fn select_clock(state: &mut ClockState, slots: &mut [PolicySlot]) -> VictimSelection {
    let maximum_steps = slots
        .len()
        .saturating_mul(2)
        .saturating_add(1)
        .min(MAX_POLICY_SCAN_STEPS);
    for _ in 0..maximum_steps {
        if slots.is_empty() {
            break;
        }
        let index = state.hand % slots.len();
        state.hand = (index + 1) % slots.len();
        let slot = &mut slots[index];
        if !slot.is_resident() || !slot.is_evictable() {
            continue;
        }
        if slot.is_visited() {
            slot.set_visited(false);
            continue;
        }
        return VictimSelection::Victim(index);
    }
    VictimSelection::None
}

fn next_sieve_candidate(entries: &SlotList, slots: &[PolicySlot], index: usize) -> Option<usize> {
    if entries.len <= 1 {
        None
    } else {
        slots[index].previous().or(entries.tail)
    }
}

fn select_sieve(state: &mut SieveState, slots: &mut [PolicySlot]) -> VictimSelection {
    if state.entries.len == 0 {
        return VictimSelection::None;
    }
    let mut candidate = state.hand.or(state.entries.tail);
    let maximum_steps = state
        .entries
        .len
        .saturating_mul(2)
        .saturating_add(1)
        .min(MAX_POLICY_SCAN_STEPS);
    for _ in 0..maximum_steps {
        let Some(index) = candidate else {
            break;
        };
        let next = slots[index].previous().or(state.entries.tail);
        if slots[index].is_evictable() {
            if slots[index].is_visited() {
                slots[index].set_visited(false);
            } else {
                state.hand = next_sieve_candidate(&state.entries, slots, index);
                return VictimSelection::Victim(index);
            }
        }
        candidate = next;
    }
    state.hand = candidate;
    VictimSelection::None
}

fn select_s3fifo<F>(
    state: &mut S3FifoState,
    slots: &mut [PolicySlot],
    weight_of: F,
) -> VictimSelection
where
    F: Fn(usize) -> usize,
{
    let maximum_steps = slots
        .len()
        .saturating_mul(5)
        .saturating_add(1)
        .min(MAX_POLICY_SCAN_STEPS);
    for _ in 0..maximum_steps {
        let prefer_main = state.main.weight > state.main_target_bytes || state.small.len == 0;
        if !prefer_main {
            if let Some(index) = state.small.oldest_evictable(slots) {
                if slots[index].frequency() >= 2 {
                    let weight = weight_of(index);
                    state.small.remove(slots, index, QueueId::Small, weight);
                    slots[index].set_frequency(0);
                    state.main.push_front(slots, index, QueueId::Main, weight);
                    continue;
                }
                state.ghost.insert(slots[index].hash());
                return VictimSelection::Victim(index);
            }
        }

        if let Some(index) = state.main.oldest_evictable(slots) {
            let frequency = slots[index].frequency();
            if frequency > 0 {
                slots[index].set_frequency(frequency - 1);
                state.main.move_to_front(slots, index, QueueId::Main);
                continue;
            }
            return VictimSelection::Victim(index);
        }

        if prefer_main {
            if let Some(index) = state.small.oldest_evictable(slots) {
                if slots[index].frequency() >= 2 {
                    let weight = weight_of(index);
                    state.small.remove(slots, index, QueueId::Small, weight);
                    slots[index].set_frequency(0);
                    state.main.push_front(slots, index, QueueId::Main, weight);
                    continue;
                }
                state.ghost.insert(slots[index].hash());
                return VictimSelection::Victim(index);
            }
        }
        return VictimSelection::None;
    }
    VictimSelection::None
}

struct FrequencySketch {
    counters: Box<[u8]>,
    width: usize,
    events: usize,
    sample_size: usize,
    age_cursor: usize,
    aging: bool,
}

impl FrequencySketch {
    fn new(maximum_entries: usize) -> io::Result<Self> {
        if maximum_entries == 0 {
            return Ok(Self {
                counters: Box::new([]),
                width: 0,
                events: 0,
                sample_size: 0,
                age_cursor: 0,
                aging: false,
            });
        }
        let width = maximum_entries
            .max(64)
            .checked_next_power_of_two()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TinyLFU is too large"))?;
        let counter_count = width.checked_mul(4).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "TinyLFU sketch size overflow")
        })?;
        let counter_bytes = counter_count.div_ceil(2);
        let mut counters = Vec::new();
        counters.try_reserve_exact(counter_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate TinyLFU frequency sketch",
            )
        })?;
        counters.resize(counter_bytes, 0_u8);
        Ok(Self {
            counters: counters.into_boxed_slice(),
            width,
            events: 0,
            sample_size: maximum_entries.saturating_mul(10).max(1),
            age_cursor: 0,
            aging: false,
        })
    }

    fn estimate(&self, hash: u64) -> u8 {
        if self.width == 0 {
            return 0;
        }
        (0..4)
            .map(|row| self.counter(self.position(hash, row)))
            .min()
            .unwrap_or(0)
    }

    fn increment(&mut self, hash: u64) {
        if self.width == 0 {
            return;
        }
        for row in 0..4 {
            let position = self.position(hash, row);
            let frequency = self.counter(position).saturating_add(1).min(15);
            self.set_counter(position, frequency);
        }
        self.events += 1;
        if self.events >= self.sample_size {
            self.aging = true;
        }
        if self.aging {
            let end = self
                .age_cursor
                .saturating_add(FREQUENCY_AGE_BATCH / 2)
                .min(self.counters.len());
            for counters in &mut self.counters[self.age_cursor..end] {
                *counters = (*counters >> 1) & 0x77;
            }
            self.age_cursor = end;
            if self.age_cursor == self.counters.len() {
                self.age_cursor = 0;
                self.aging = false;
                self.events /= 2;
            }
        }
    }

    fn position(&self, hash: u64, row: usize) -> usize {
        const SEEDS: [u64; 4] = [
            0x9e37_79b9_7f4a_7c15,
            0xc2b2_ae3d_27d4_eb4f,
            0x1656_6791_9e37_79f9,
            0x85eb_ca77_c2b2_ae63,
        ];
        let mixed = mix64(hash ^ SEEDS[row]);
        row * self.width + (mixed as usize & (self.width - 1))
    }

    fn counter(&self, position: usize) -> u8 {
        let counters = self.counters[position / 2];
        if position & 1 == 0 {
            counters & 0x0f
        } else {
            counters >> 4
        }
    }

    fn set_counter(&mut self, position: usize, frequency: u8) {
        debug_assert!(frequency <= 15);
        let counters = &mut self.counters[position / 2];
        if position & 1 == 0 {
            *counters = (*counters & 0xf0) | frequency;
        } else {
            *counters = (*counters & 0x0f) | (frequency << 4);
        }
    }
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

struct GhostQueue {
    maximum_entries: usize,
    cursor: usize,
    order: Vec<u64>,
    members: HashMap<u64, usize>,
}

impl GhostQueue {
    fn new(maximum_entries: usize) -> Self {
        Self {
            maximum_entries,
            cursor: 0,
            order: Vec::new(),
            members: HashMap::new(),
        }
    }

    fn take(&mut self, hash: u64) -> bool {
        self.members.remove(&hash).is_some()
    }

    fn insert(&mut self, hash: u64) {
        if self.maximum_entries == 0 {
            return;
        }
        let slot = if self.order.len() < self.maximum_entries {
            let slot = self.order.len();
            self.order.push(hash);
            slot
        } else {
            let slot = self.cursor;
            let old_hash = self.order[slot];
            if self.members.get(&old_hash) == Some(&slot) {
                self.members.remove(&old_hash);
            }
            self.order[slot] = hash;
            self.cursor = (slot + 1) % self.maximum_entries;
            slot
        };
        self.members.insert(hash, slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install(policy: &mut EvictionState, slots: &mut Vec<PolicySlot>, hash: u64) -> usize {
        let index = slots.len();
        slots.push(PolicySlot::default());
        let hint = policy.prepare_insert(hash);
        policy.insert(slots, index, hash, 100, true, hint);
        index
    }

    #[test]
    fn sieve_retains_a_visited_old_entry_without_reordering_it() {
        let mut policy = EvictionState::new(EvictionPolicy::Sieve, 1_000, 10).unwrap();
        let mut slots = Vec::new();
        let old = install(&mut policy, &mut slots, 1);
        let new = install(&mut policy, &mut slots, 2);
        policy.record_hit(&mut slots, old);

        assert_eq!(
            policy.select_victim(&mut slots, 3, true, |_| 100),
            VictimSelection::Victim(new)
        );
    }

    #[test]
    fn s3fifo_ghost_hit_enters_the_main_queue() {
        let mut policy = EvictionState::new(EvictionPolicy::S3Fifo, 10_000, 100).unwrap();
        let mut slots = Vec::new();
        let first = install(&mut policy, &mut slots, 1);
        assert_eq!(
            policy.select_victim(&mut slots, 2, true, |_| 100),
            VictimSelection::Victim(first)
        );
        policy.remove(&mut slots, first, 100);

        let hint = policy.record_miss(1);
        policy.insert(&mut slots, first, 1, 100, true, hint);
        assert_eq!(slots[first].queue(), QueueId::Main);
    }

    #[test]
    fn frequency_sketch_ages_old_accesses() {
        let mut sketch = FrequencySketch::new(2).unwrap();
        assert_eq!(sketch.counters.len(), sketch.width * 2);
        for _ in 0..15 {
            sketch.increment(1);
        }
        let before = sketch.estimate(1);
        for ordinal in 10..40 {
            sketch.increment(ordinal);
        }
        assert!(sketch.estimate(1) < before);
    }

    #[test]
    fn ghost_ring_ignores_stale_duplicate_slots() {
        let mut ghost = GhostQueue::new(2);
        ghost.insert(1);
        ghost.insert(1);
        ghost.insert(2);

        assert!(ghost.take(1));
        assert!(ghost.take(2));
        assert!(!ghost.take(1));
    }
}
