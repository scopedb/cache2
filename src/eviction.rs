//! Bounded shard-local eviction for the volatile RAM tier.
//!
//! Capacity accounting and key correctness stay in `memory`; this module owns
//! the optional CLOCK or S3-FIFO metadata and chooses bounded victims.

use std::io;

use crate::hashing::FixedPrehashedMap;
use crate::runtime_config::L1EvictionPolicy;

/// Maximum policy metadata inspected by one complete foreground admission.
/// Exhausting this budget means L1 bypass; it never expands with cache size or
/// the number of victims required by a mixed-size candidate.
pub(crate) const MAX_POLICY_SCAN_STEPS: usize = 64;

/// Memory slot indices are packed into `u32`; the final value is reserved as
/// the end-of-chain sentinel by `memory` and the policy queues.
pub(crate) const MAX_POLICY_SLOT_INDEX: usize = (u32::MAX - 1) as usize;

const NO_POLICY_SLOT: u32 = u32::MAX;
const RESIDENT_BIT: u8 = 1 << 0;
const CLOCK_VISITED_BIT: u8 = 1 << 1;
const S3FIFO_FREQUENCY_SHIFT: u8 = 1;
const S3FIFO_FREQUENCY_MASK: u8 = 0b11 << S3FIFO_FREQUENCY_SHIFT;
const S3FIFO_MAIN_BIT: u8 = 1 << 3;
const S3FIFO_MAX_FREQUENCY: u8 = 3;
const S3FIFO_MOVE_TO_MAIN_THRESHOLD: u8 = 2;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PolicySlot {
    hash: u64,
    flags: u8,
}

impl PolicySlot {
    fn new_clock(hash: u64, visited: bool) -> Self {
        Self {
            hash,
            flags: RESIDENT_BIT | (u8::from(visited) * CLOCK_VISITED_BIT),
        }
    }

    fn new_s3fifo(hash: u64, queue: QueueKind, frequency: u8) -> Self {
        debug_assert!(frequency <= S3FIFO_MAX_FREQUENCY);
        Self {
            hash,
            flags: RESIDENT_BIT
                | ((frequency << S3FIFO_FREQUENCY_SHIFT) & S3FIFO_FREQUENCY_MASK)
                | (u8::from(queue == QueueKind::Main) * S3FIFO_MAIN_BIT),
        }
    }

    pub(crate) const fn hash(&self) -> u64 {
        self.hash
    }

    const fn is_resident(&self) -> bool {
        self.flags & RESIDENT_BIT != 0
    }

    const fn clock_visited(&self) -> bool {
        self.flags & CLOCK_VISITED_BIT != 0
    }

    fn set_clock_visited(&mut self, visited: bool) {
        self.flags = (self.flags & !CLOCK_VISITED_BIT) | (u8::from(visited) * CLOCK_VISITED_BIT);
    }

    const fn s3fifo_queue(&self) -> QueueKind {
        if self.flags & S3FIFO_MAIN_BIT == 0 {
            QueueKind::Small
        } else {
            QueueKind::Main
        }
    }

    const fn s3fifo_frequency(&self) -> u8 {
        (self.flags & S3FIFO_FREQUENCY_MASK) >> S3FIFO_FREQUENCY_SHIFT
    }

    fn set_s3fifo_frequency(&mut self, frequency: u8) {
        debug_assert!(frequency <= S3FIFO_MAX_FREQUENCY);
        self.flags = (self.flags & !S3FIFO_FREQUENCY_MASK)
            | ((frequency << S3FIFO_FREQUENCY_SHIFT) & S3FIFO_FREQUENCY_MASK);
    }

    fn increment_s3fifo_frequency(&mut self) {
        let frequency = self.flags & S3FIFO_FREQUENCY_MASK;
        self.flags += u8::from(frequency != S3FIFO_FREQUENCY_MASK) << S3FIFO_FREQUENCY_SHIFT;
    }

    fn set_s3fifo_queue(&mut self, queue: QueueKind) {
        self.flags = (self.flags & !S3FIFO_MAIN_BIT)
            | (u8::from(queue == QueueKind::Main) * S3FIFO_MAIN_BIT);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueKind {
    Small,
    Main,
}

#[derive(Clone, Copy, Debug)]
enum DetachedPolicyKind {
    Clock,
    S3Fifo { queue: QueueKind, frequency: u8 },
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DetachedPolicy {
    hash: u64,
    weight: usize,
    kind: DetachedPolicyKind,
}

impl DetachedPolicy {
    pub(crate) const fn hash(self) -> u64 {
        self.hash
    }

    pub(crate) const fn weight(self) -> usize {
        self.weight
    }
}

pub(crate) struct EvictionState {
    policy: EvictionPolicyState,
}

enum EvictionPolicyState {
    Clock(ClockState),
    S3Fifo(S3FifoState),
}

impl EvictionState {
    pub(crate) fn new(
        policy: L1EvictionPolicy,
        capacity_bytes: usize,
        maximum_entries: usize,
    ) -> io::Result<Self> {
        let policy = match policy {
            L1EvictionPolicy::Clock => EvictionPolicyState::Clock(ClockState::new()),
            L1EvictionPolicy::S3Fifo => {
                EvictionPolicyState::S3Fifo(S3FifoState::new(capacity_bytes, maximum_entries)?)
            }
        };
        Ok(Self { policy })
    }

    pub(crate) fn allocation_bytes(
        policy: L1EvictionPolicy,
        maximum_entries: usize,
    ) -> io::Result<usize> {
        match policy {
            L1EvictionPolicy::Clock => Ok(0),
            L1EvictionPolicy::S3Fifo => S3FifoState::allocation_bytes(maximum_entries),
        }
    }

    pub(crate) fn insert(
        &mut self,
        slots: &mut [PolicySlot],
        index: usize,
        hash: u64,
        weight: usize,
    ) {
        debug_assert!(!slots[index].is_resident());
        match &mut self.policy {
            EvictionPolicyState::Clock(_) => {
                // One-shot scans receive no free second chance. A real L1 hit
                // sets the bit before this entry reaches the hand.
                slots[index] = PolicySlot::new_clock(hash, false);
            }
            EvictionPolicyState::S3Fifo(state) => {
                state.insert(slots, index, hash, weight);
            }
        }
    }

    pub(crate) fn record_hit(&mut self, slots: &mut [PolicySlot], index: usize) {
        match &mut self.policy {
            EvictionPolicyState::Clock(_) => slots[index].set_clock_visited(true),
            EvictionPolicyState::S3Fifo(_) => slots[index].increment_s3fifo_frequency(),
        }
    }

    pub(crate) fn remove(&mut self, slots: &mut [PolicySlot], index: usize) {
        if !slots.get(index).is_some_and(PolicySlot::is_resident) {
            return;
        }
        match &mut self.policy {
            EvictionPolicyState::Clock(_) => slots[index] = PolicySlot::default(),
            EvictionPolicyState::S3Fifo(state) => state.remove(slots, index),
        }
    }

    /// Detaches a resident only from policy metadata while an admission plan
    /// is assembled. The memory directory and value remain intact so a failed
    /// plan can restore the entry without losing a cache hit.
    pub(crate) fn detach_for_admission(
        &mut self,
        slots: &mut [PolicySlot],
        index: usize,
        weight: usize,
    ) -> DetachedPolicy {
        match &mut self.policy {
            EvictionPolicyState::Clock(_) => {
                let hash = slots[index].hash();
                slots[index] = PolicySlot::default();
                DetachedPolicy {
                    hash,
                    weight,
                    kind: DetachedPolicyKind::Clock,
                }
            }
            EvictionPolicyState::S3Fifo(state) => state.detach(slots, index, weight),
        }
    }

    pub(crate) fn restore_for_admission(
        &mut self,
        slots: &mut [PolicySlot],
        index: usize,
        detached: DetachedPolicy,
    ) {
        debug_assert!(!slots[index].is_resident());
        match (&mut self.policy, detached.kind) {
            (EvictionPolicyState::Clock(_), DetachedPolicyKind::Clock) => {
                // A failed admission is optional work and must not make its
                // temporarily detached victim immediately vulnerable again.
                slots[index] = PolicySlot::new_clock(detached.hash, true);
            }
            (
                EvictionPolicyState::S3Fifo(state),
                DetachedPolicyKind::S3Fifo { queue, frequency },
            ) => state.restore(
                slots,
                index,
                detached.hash,
                detached.weight,
                queue,
                frequency,
            ),
            _ => unreachable!("detached L1 policy does not match the configured policy"),
        }
    }

    /// Records policy history only after the associated memory victim commits.
    pub(crate) fn commit_eviction(&mut self, detached: DetachedPolicy) {
        match (&mut self.policy, detached.kind) {
            (EvictionPolicyState::Clock(_), DetachedPolicyKind::Clock) => {}
            (EvictionPolicyState::S3Fifo(state), DetachedPolicyKind::S3Fifo { queue, .. }) => {
                if queue == QueueKind::Small {
                    state.ghost.insert(detached.hash, detached.weight);
                }
            }
            _ => unreachable!("detached L1 policy does not match the configured policy"),
        }
    }

    pub(crate) fn select_victim<F>(
        &mut self,
        slots: &mut [PolicySlot],
        remaining_steps: &mut usize,
        can_reclaim: F,
    ) -> Option<usize>
    where
        F: Fn(usize) -> bool,
    {
        match &mut self.policy {
            EvictionPolicyState::Clock(state) => {
                state.select_victim(slots, remaining_steps, can_reclaim)
            }
            EvictionPolicyState::S3Fifo(state) => {
                state.select_victim(slots, remaining_steps, can_reclaim)
            }
        }
    }
}

struct ClockState {
    hand: usize,
}

impl ClockState {
    const fn new() -> Self {
        Self { hand: 0 }
    }

    fn select_victim<F>(
        &mut self,
        slots: &mut [PolicySlot],
        remaining_steps: &mut usize,
        can_reclaim: F,
    ) -> Option<usize>
    where
        F: Fn(usize) -> bool,
    {
        for _ in 0..maximum_scan_steps(slots.len()) {
            if slots.is_empty() || !take_scan_step(remaining_steps) {
                break;
            }
            let index = if self.hand < slots.len() {
                self.hand
            } else {
                0
            };
            self.hand = if index + 1 == slots.len() {
                0
            } else {
                index + 1
            };
            let slot = &mut slots[index];
            if !slot.is_resident() {
                continue;
            }
            if slot.clock_visited() {
                slot.set_clock_visited(false);
                continue;
            }
            if can_reclaim(index) {
                return Some(index);
            }
        }
        None
    }
}

#[derive(Clone, Copy)]
struct S3FifoLink {
    previous: u32,
    next: u32,
    weight: u32,
}

impl Default for S3FifoLink {
    fn default() -> Self {
        Self {
            previous: NO_POLICY_SLOT,
            next: NO_POLICY_SLOT,
            weight: 0,
        }
    }
}

struct PolicyQueue {
    head: u32,
    tail: u32,
    bytes: usize,
}

impl PolicyQueue {
    const fn empty() -> Self {
        Self {
            head: NO_POLICY_SLOT,
            tail: NO_POLICY_SLOT,
            bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.head == NO_POLICY_SLOT
    }

    fn front(&self) -> Option<usize> {
        (self.head != NO_POLICY_SLOT)
            .then(|| usize::try_from(self.head).expect("policy slot index exceeds usize"))
    }

    fn push_back(&mut self, links: &mut [S3FifoLink], index: usize, weight: usize) {
        let packed = u32::try_from(index).expect("policy slot index exceeds u32");
        let packed_weight = u32::try_from(weight).expect("L1 entry weight exceeds u32");
        debug_assert_eq!(links[index].weight, 0);
        links[index] = S3FifoLink {
            previous: self.tail,
            next: NO_POLICY_SLOT,
            weight: packed_weight,
        };
        if self.tail == NO_POLICY_SLOT {
            self.head = packed;
        } else {
            let tail = usize::try_from(self.tail).expect("policy slot index exceeds usize");
            links[tail].next = packed;
        }
        self.tail = packed;
        self.bytes = self.bytes.saturating_add(weight);
    }

    fn remove(&mut self, links: &mut [S3FifoLink], index: usize) -> usize {
        let packed = u32::try_from(index).expect("policy slot index exceeds u32");
        let link = links[index];
        debug_assert_ne!(link.weight, 0);
        if link.previous == NO_POLICY_SLOT {
            debug_assert_eq!(self.head, packed);
            self.head = link.next;
        } else {
            let previous = usize::try_from(link.previous).expect("policy slot index exceeds usize");
            links[previous].next = link.next;
        }
        if link.next == NO_POLICY_SLOT {
            debug_assert_eq!(self.tail, packed);
            self.tail = link.previous;
        } else {
            let next = usize::try_from(link.next).expect("policy slot index exceeds usize");
            links[next].previous = link.previous;
        }
        links[index] = S3FifoLink::default();
        let weight = link.weight as usize;
        self.bytes = self.bytes.saturating_sub(weight);
        weight
    }

    fn move_to_back(&mut self, links: &mut [S3FifoLink], index: usize) {
        let packed = u32::try_from(index).expect("policy slot index exceeds u32");
        if self.tail == packed {
            return;
        }
        let weight = self.remove(links, index);
        self.push_back(links, index, weight);
    }
}

struct S3FifoState {
    links: Box<[S3FifoLink]>,
    small: PolicyQueue,
    main: PolicyQueue,
    main_capacity_bytes: usize,
    ghost: GhostQueue,
}

impl S3FifoState {
    fn new(capacity_bytes: usize, maximum_entries: usize) -> io::Result<Self> {
        let mut links = Vec::new();
        links.try_reserve_exact(maximum_entries).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate S3-FIFO resident links",
            )
        })?;
        links.resize(maximum_entries, S3FifoLink::default());
        let small_capacity_bytes = capacity_bytes / 10;
        let main_capacity_bytes = capacity_bytes.saturating_sub(small_capacity_bytes);
        Ok(Self {
            links: links.into_boxed_slice(),
            small: PolicyQueue::empty(),
            main: PolicyQueue::empty(),
            main_capacity_bytes,
            ghost: GhostQueue::new(maximum_entries, main_capacity_bytes)?,
        })
    }

    fn allocation_bytes(maximum_entries: usize) -> io::Result<usize> {
        let links = maximum_entries
            .checked_mul(std::mem::size_of::<S3FifoLink>())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "S3-FIFO is too large"))?;
        links
            .checked_add(GhostQueue::allocation_bytes(maximum_entries)?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "S3-FIFO is too large"))
    }

    fn insert(&mut self, slots: &mut [PolicySlot], index: usize, hash: u64, weight: usize) {
        let queue = if self.ghost.take(hash) {
            QueueKind::Main
        } else {
            QueueKind::Small
        };
        slots[index] = PolicySlot::new_s3fifo(hash, queue, 0);
        self.push_back(queue, index, weight);
    }

    fn remove(&mut self, slots: &mut [PolicySlot], index: usize) {
        let queue = slots[index].s3fifo_queue();
        self.remove_from(queue, index);
        slots[index] = PolicySlot::default();
    }

    fn detach(
        &mut self,
        slots: &mut [PolicySlot],
        index: usize,
        expected_weight: usize,
    ) -> DetachedPolicy {
        let slot = slots[index];
        let queue = slot.s3fifo_queue();
        let frequency = slot.s3fifo_frequency();
        let weight = self.remove_from(queue, index);
        debug_assert_eq!(weight, expected_weight);
        slots[index] = PolicySlot::default();
        DetachedPolicy {
            hash: slot.hash(),
            weight,
            kind: DetachedPolicyKind::S3Fifo { queue, frequency },
        }
    }

    fn restore(
        &mut self,
        slots: &mut [PolicySlot],
        index: usize,
        hash: u64,
        weight: usize,
        queue: QueueKind,
        frequency: u8,
    ) {
        slots[index] = PolicySlot::new_s3fifo(hash, queue, frequency);
        self.push_back(queue, index, weight);
    }

    fn select_victim<F>(
        &mut self,
        slots: &mut [PolicySlot],
        remaining_steps: &mut usize,
        can_reclaim: F,
    ) -> Option<usize>
    where
        F: Fn(usize) -> bool,
    {
        let mut prefer_small = false;
        for _ in 0..maximum_scan_steps(slots.len()) {
            if !take_scan_step(remaining_steps) {
                break;
            }
            let queue = if prefer_small && !self.small.is_empty() {
                prefer_small = false;
                QueueKind::Small
            } else if self.main.bytes > self.main_capacity_bytes || self.small.is_empty() {
                QueueKind::Main
            } else {
                QueueKind::Small
            };
            let index = self.queue(queue).front()?;
            let frequency = slots[index].s3fifo_frequency();
            match queue {
                QueueKind::Small if frequency >= S3FIFO_MOVE_TO_MAIN_THRESHOLD => {
                    let weight = self.small.remove(&mut self.links, index);
                    slots[index].set_s3fifo_queue(QueueKind::Main);
                    self.main.push_back(&mut self.links, index, weight);
                }
                QueueKind::Main if frequency > 0 => {
                    slots[index].set_s3fifo_frequency(frequency - 1);
                    self.main.move_to_back(&mut self.links, index);
                }
                _ if can_reclaim(index) => return Some(index),
                QueueKind::Small => {
                    // A caller-retained value cannot release its memory charge.
                    // Move it out of the probationary head so one retained
                    // entry cannot consume the complete admission budget.
                    let weight = self.small.remove(&mut self.links, index);
                    slots[index].set_s3fifo_queue(QueueKind::Main);
                    self.main.push_back(&mut self.links, index, weight);
                }
                QueueKind::Main => {
                    self.main.move_to_back(&mut self.links, index);
                    prefer_small = true;
                }
            }
        }
        None
    }

    fn queue(&self, queue: QueueKind) -> &PolicyQueue {
        match queue {
            QueueKind::Small => &self.small,
            QueueKind::Main => &self.main,
        }
    }

    fn push_back(&mut self, queue: QueueKind, index: usize, weight: usize) {
        let links = &mut self.links;
        match queue {
            QueueKind::Small => self.small.push_back(links, index, weight),
            QueueKind::Main => self.main.push_back(links, index, weight),
        }
    }

    fn remove_from(&mut self, queue: QueueKind, index: usize) -> usize {
        let links = &mut self.links;
        match queue {
            QueueKind::Small => self.small.remove(links, index),
            QueueKind::Main => self.main.remove(links, index),
        }
    }
}

#[derive(Clone, Copy)]
struct GhostSlot {
    hash: u64,
    previous: u32,
    next: u32,
    weight: u32,
}

impl Default for GhostSlot {
    fn default() -> Self {
        Self {
            hash: 0,
            previous: NO_POLICY_SLOT,
            next: NO_POLICY_SLOT,
            weight: 0,
        }
    }
}

struct GhostQueue {
    directory: FixedPrehashedMap,
    slots: Box<[GhostSlot]>,
    free_slots: Vec<u32>,
    head: u32,
    tail: u32,
    bytes: usize,
    capacity_bytes: usize,
}

impl GhostQueue {
    fn new(maximum_entries: usize, capacity_bytes: usize) -> io::Result<Self> {
        let directory = FixedPrehashedMap::try_new(maximum_entries)?;
        let mut slots = Vec::new();
        slots.try_reserve_exact(maximum_entries).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate S3-FIFO ghost slots",
            )
        })?;
        slots.resize(maximum_entries, GhostSlot::default());
        let mut free_slots = Vec::new();
        free_slots.try_reserve_exact(maximum_entries).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate S3-FIFO ghost free list",
            )
        })?;
        free_slots.extend(
            (0..maximum_entries).rev().map(|index| {
                u32::try_from(index).expect("validated L1 entry capacity exceeds u32")
            }),
        );
        Ok(Self {
            directory,
            slots: slots.into_boxed_slice(),
            free_slots,
            head: NO_POLICY_SLOT,
            tail: NO_POLICY_SLOT,
            bytes: 0,
            capacity_bytes,
        })
    }

    fn allocation_bytes(maximum_entries: usize) -> io::Result<usize> {
        let slots = maximum_entries
            .checked_mul(std::mem::size_of::<GhostSlot>())
            .and_then(|bytes| {
                maximum_entries
                    .checked_mul(std::mem::size_of::<u32>())
                    .and_then(|free| bytes.checked_add(free))
            })
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "ghost queue is too large")
            })?;
        FixedPrehashedMap::allocation_bytes(maximum_entries)?
            .checked_add(slots)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "ghost queue is too large"))
    }

    fn take(&mut self, hash: u64) -> bool {
        let Some(packed) = self.directory.remove(hash) else {
            return false;
        };
        let index = usize::try_from(packed).expect("ghost slot index exceeds usize");
        debug_assert_eq!(self.slots[index].hash, hash);
        self.unlink(index);
        true
    }

    fn insert(&mut self, hash: u64, weight: usize) {
        let Ok(packed_weight) = u32::try_from(weight) else {
            return;
        };
        if packed_weight == 0
            || weight > self.capacity_bytes
            || self.slots.is_empty()
            || self.capacity_bytes == 0
        {
            return;
        }
        if self.free_slots.is_empty() || self.bytes.saturating_add(weight) > self.capacity_bytes {
            let Some(head) = self.head_index() else {
                return;
            };
            let head_weight = self.slots[head].weight as usize;
            if self
                .bytes
                .saturating_sub(head_weight)
                .saturating_add(weight)
                > self.capacity_bytes
            {
                // Mixed sizes can require many tiny ghost removals for one
                // large record. Preserve the existing history and skip the
                // optional insert instead of adding a cleanup loop.
                return;
            }
            let removed = self.remove_head();
            debug_assert!(removed);
        }
        let packed = self
            .free_slots
            .pop()
            .expect("ghost capacity preflight left one free slot");
        let index = usize::try_from(packed).expect("ghost slot index exceeds usize");
        self.slots[index] = GhostSlot {
            hash,
            previous: self.tail,
            next: NO_POLICY_SLOT,
            weight: packed_weight,
        };
        if self.tail == NO_POLICY_SLOT {
            self.head = packed;
        } else {
            let tail = usize::try_from(self.tail).expect("ghost slot index exceeds usize");
            self.slots[tail].next = packed;
        }
        self.tail = packed;
        self.bytes = self.bytes.saturating_add(weight);
        match self.directory.insert(hash, packed) {
            Some(Some(previous)) => {
                let previous = usize::try_from(previous).expect("ghost slot index exceeds usize");
                self.unlink(previous);
            }
            Some(None) => {}
            None => self.unlink(index),
        }
    }

    fn remove_head(&mut self) -> bool {
        let Some(index) = self.head_index() else {
            return false;
        };
        let removed = self.directory.remove(self.slots[index].hash);
        debug_assert_eq!(removed, Some(self.head));
        self.unlink(index);
        true
    }

    fn head_index(&self) -> Option<usize> {
        (self.head != NO_POLICY_SLOT)
            .then(|| usize::try_from(self.head).expect("ghost slot index exceeds usize"))
    }

    fn unlink(&mut self, index: usize) {
        let packed = u32::try_from(index).expect("ghost slot index exceeds u32");
        let slot = self.slots[index];
        debug_assert_ne!(slot.weight, 0);
        if slot.previous == NO_POLICY_SLOT {
            debug_assert_eq!(self.head, packed);
            self.head = slot.next;
        } else {
            let previous = usize::try_from(slot.previous).expect("ghost slot index exceeds usize");
            self.slots[previous].next = slot.next;
        }
        if slot.next == NO_POLICY_SLOT {
            debug_assert_eq!(self.tail, packed);
            self.tail = slot.previous;
        } else {
            let next = usize::try_from(slot.next).expect("ghost slot index exceeds usize");
            self.slots[next].previous = slot.previous;
        }
        self.bytes = self.bytes.saturating_sub(slot.weight as usize);
        self.slots[index] = GhostSlot::default();
        self.free_slots.push(packed);
    }
}

fn maximum_scan_steps(slot_count: usize) -> usize {
    slot_count
        .saturating_mul(2)
        .saturating_add(1)
        .min(MAX_POLICY_SCAN_STEPS)
}

fn take_scan_step(remaining_steps: &mut usize) -> bool {
    if *remaining_steps == 0 {
        false
    } else {
        *remaining_steps -= 1;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(kind: L1EvictionPolicy, capacity: usize, entries: usize) -> EvictionState {
        EvictionState::new(kind, capacity, entries).unwrap()
    }

    fn install(
        policy: &mut EvictionState,
        slots: &mut Vec<PolicySlot>,
        hash: u64,
        weight: usize,
    ) -> usize {
        let index = slots.len();
        slots.push(PolicySlot::default());
        policy.insert(slots, index, hash, weight);
        index
    }

    #[test]
    fn clock_new_entries_are_immediately_evictable() {
        let mut policy = policy(L1EvictionPolicy::Clock, 1024, 0);
        let mut slots = Vec::new();
        let first = install(&mut policy, &mut slots, 1, 1);
        let second = install(&mut policy, &mut slots, 2, 1);
        let mut remaining_steps = MAX_POLICY_SCAN_STEPS;

        assert_eq!(
            policy.select_victim(&mut slots, &mut remaining_steps, |_| true),
            Some(first)
        );
        assert_eq!(
            policy.select_victim(&mut slots, &mut remaining_steps, |_| true),
            Some(second)
        );
    }

    #[test]
    fn one_clock_admission_has_one_fixed_scan_budget() {
        let mut policy = policy(L1EvictionPolicy::Clock, 1024, 0);
        let mut slots = Vec::new();
        for hash in 0..100 {
            let index = install(&mut policy, &mut slots, hash, 1);
            policy.record_hit(&mut slots, index);
        }
        let mut remaining_steps = MAX_POLICY_SCAN_STEPS;

        assert_eq!(
            policy.select_victim(&mut slots, &mut remaining_steps, |_| true),
            None
        );
        assert_eq!(remaining_steps, 0);
    }

    #[test]
    fn failed_clock_plan_restores_a_detached_entry() {
        let mut policy = policy(L1EvictionPolicy::Clock, 1024, 0);
        let mut slots = Vec::new();
        let index = install(&mut policy, &mut slots, 7, 11);
        let detached = policy.detach_for_admission(&mut slots, index, 11);
        policy.restore_for_admission(&mut slots, index, detached);

        assert_eq!(slots[index].hash(), 7);
        assert!(slots[index].clock_visited());
    }

    #[test]
    fn clock_prefers_a_hit_over_a_cold_one_shot_entry() {
        let mut policy = policy(L1EvictionPolicy::Clock, 1024, 0);
        let mut slots = Vec::new();
        let hot = install(&mut policy, &mut slots, 1, 1);
        let cold = install(&mut policy, &mut slots, 2, 1);
        policy.record_hit(&mut slots, hot);
        let mut remaining_steps = MAX_POLICY_SCAN_STEPS;

        assert_eq!(
            policy.select_victim(&mut slots, &mut remaining_steps, |_| true),
            Some(cold)
        );
    }

    #[test]
    fn s3fifo_promotes_reused_small_entries_and_ghost_hits_enter_main() {
        let mut policy = policy(L1EvictionPolicy::S3Fifo, 1000, 10);
        let mut slots = Vec::new();
        for hash in 1..=10 {
            install(&mut policy, &mut slots, hash, 100);
        }
        policy.record_hit(&mut slots, 0);
        policy.record_hit(&mut slots, 0);
        let mut remaining_steps = MAX_POLICY_SCAN_STEPS;

        let victim = policy
            .select_victim(&mut slots, &mut remaining_steps, |_| true)
            .unwrap();
        assert_eq!(victim, 1);
        assert_eq!(slots[0].s3fifo_queue(), QueueKind::Main);
        let detached = policy.detach_for_admission(&mut slots, victim, 100);
        policy.commit_eviction(detached);

        policy.insert(&mut slots, victim, 2, 100);
        assert_eq!(slots[victim].s3fifo_queue(), QueueKind::Main);
    }

    #[test]
    fn s3fifo_main_frequency_is_consumed_with_a_fixed_budget() {
        let mut policy = policy(L1EvictionPolicy::S3Fifo, 100, 1);
        let mut slots = Vec::new();
        let hot = install(&mut policy, &mut slots, 7, 100);
        policy.record_hit(&mut slots, hot);
        policy.record_hit(&mut slots, hot);
        let mut remaining_steps = MAX_POLICY_SCAN_STEPS;

        assert_eq!(
            policy.select_victim(&mut slots, &mut remaining_steps, |_| true),
            None
        );
        assert_eq!(slots[hot].s3fifo_queue(), QueueKind::Main);
        assert_eq!(slots[hot].s3fifo_frequency(), 0);
        assert_eq!(
            policy.select_victim(&mut slots, &mut remaining_steps, |_| true),
            Some(hot)
        );
    }

    #[test]
    fn failed_s3fifo_plan_restores_queue_and_frequency() {
        let mut policy = policy(L1EvictionPolicy::S3Fifo, 1000, 1);
        let mut slots = Vec::new();
        let index = install(&mut policy, &mut slots, 7, 100);
        policy.record_hit(&mut slots, index);
        let detached = policy.detach_for_admission(&mut slots, index, 100);
        policy.restore_for_admission(&mut slots, index, detached);

        assert_eq!(slots[index].hash(), 7);
        assert_eq!(slots[index].s3fifo_queue(), QueueKind::Small);
        assert_eq!(slots[index].s3fifo_frequency(), 1);
    }

    #[test]
    fn retained_small_entry_does_not_block_a_reclaimable_follower() {
        let mut policy = policy(L1EvictionPolicy::S3Fifo, 1000, 2);
        let mut slots = Vec::new();
        let retained = install(&mut policy, &mut slots, 7, 100);
        let reclaimable = install(&mut policy, &mut slots, 8, 100);
        let mut remaining_steps = MAX_POLICY_SCAN_STEPS;

        assert_eq!(
            policy.select_victim(&mut slots, &mut remaining_steps, |index| index != retained),
            Some(reclaimable)
        );
        assert_eq!(slots[retained].s3fifo_queue(), QueueKind::Main);
    }

    #[test]
    fn mixed_size_ghost_insert_never_runs_a_multi_entry_cleanup() {
        let mut ghost = GhostQueue::new(3, 100).unwrap();
        ghost.insert(1, 10);
        ghost.insert(2, 10);
        ghost.insert(3, 80);

        // Removing the 10-byte head would still not fit this entry. Optional
        // history is skipped without gradually erasing the existing queue.
        ghost.insert(4, 80);

        assert_eq!(ghost.bytes, 100);
        assert!(ghost.free_slots.is_empty());
        assert_eq!(ghost.directory.get(1), Some(0));
        assert_eq!(ghost.directory.get(2), Some(1));
        assert_eq!(ghost.directory.get(3), Some(2));
        assert_eq!(ghost.directory.get(4), None);
    }
}
