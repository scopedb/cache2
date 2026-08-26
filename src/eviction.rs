//! Shard-local CLOCK eviction for the volatile RAM tier.
//!
//! Capacity accounting and key correctness stay in `memory`; this module only
//! keeps one visited bit per resident slot and chooses bounded victims.

/// Maximum policy metadata inspected by one complete foreground admission.
/// Exhausting this budget means L1 bypass; it never expands with cache size or
/// the number of victims required by a mixed-size candidate.
pub(crate) const MAX_POLICY_SCAN_STEPS: usize = 64;

/// Memory slot indices are packed into `u32`; the final value is reserved as
/// the end-of-chain sentinel by `memory`.
pub(crate) const MAX_POLICY_SLOT_INDEX: usize = (u32::MAX - 1) as usize;

const RESIDENT_BIT: u8 = 1 << 0;
const VISITED_BIT: u8 = 1 << 1;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PolicySlot {
    hash: u64,
    flags: u8,
}

impl PolicySlot {
    fn new(hash: u64) -> Self {
        Self {
            hash,
            flags: RESIDENT_BIT | VISITED_BIT,
        }
    }

    pub(crate) const fn hash(&self) -> u64 {
        self.hash
    }

    const fn is_resident(&self) -> bool {
        self.flags & RESIDENT_BIT != 0
    }

    const fn is_visited(&self) -> bool {
        self.flags & VISITED_BIT != 0
    }

    fn set_visited(&mut self, visited: bool) {
        self.flags = (self.flags & !VISITED_BIT) | (u8::from(visited) * VISITED_BIT);
    }
}

pub(crate) struct EvictionState {
    hand: usize,
}

impl EvictionState {
    pub(crate) const fn new() -> Self {
        Self { hand: 0 }
    }

    pub(crate) fn insert(&mut self, slots: &mut [PolicySlot], index: usize, hash: u64) {
        debug_assert!(!slots[index].is_resident());
        slots[index] = PolicySlot::new(hash);
    }

    pub(crate) fn record_hit(&mut self, slots: &mut [PolicySlot], index: usize) {
        slots[index].set_visited(true);
    }

    pub(crate) fn remove(&mut self, slots: &mut [PolicySlot], index: usize) {
        if slots.get(index).is_some_and(PolicySlot::is_resident) {
            slots[index] = PolicySlot::default();
        }
    }

    /// Detaches a resident only from CLOCK metadata while an admission plan is
    /// assembled. The memory directory and value remain intact so a failed
    /// plan can restore the entry without losing a cache hit.
    pub(crate) fn detach_for_admission(&mut self, slots: &mut [PolicySlot], index: usize) {
        self.remove(slots, index);
    }

    pub(crate) fn restore_for_admission(
        &mut self,
        slots: &mut [PolicySlot],
        index: usize,
        hash: u64,
    ) {
        debug_assert!(!slots[index].is_resident());
        slots[index] = PolicySlot::new(hash);
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
        let maximum_steps = slots
            .len()
            .saturating_mul(2)
            .saturating_add(1)
            .min(MAX_POLICY_SCAN_STEPS);
        for _ in 0..maximum_steps {
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
            if slot.is_visited() {
                slot.set_visited(false);
                continue;
            }
            if can_reclaim(index) {
                return Some(index);
            }
        }
        None
    }
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

    fn install(policy: &mut EvictionState, slots: &mut Vec<PolicySlot>, hash: u64) -> usize {
        let index = slots.len();
        slots.push(PolicySlot::default());
        policy.insert(slots, index, hash);
        index
    }

    #[test]
    fn new_entries_receive_one_second_chance() {
        let mut policy = EvictionState::new();
        let mut slots = Vec::new();
        let first = install(&mut policy, &mut slots, 1);
        let second = install(&mut policy, &mut slots, 2);
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
    fn one_admission_has_one_fixed_scan_budget() {
        let mut policy = EvictionState::new();
        let mut slots = Vec::new();
        for hash in 0..100 {
            install(&mut policy, &mut slots, hash);
        }
        let mut remaining_steps = MAX_POLICY_SCAN_STEPS;

        assert_eq!(
            policy.select_victim(&mut slots, &mut remaining_steps, |_| true),
            None
        );
        assert_eq!(remaining_steps, 0);
    }

    #[test]
    fn failed_plan_can_restore_a_detached_entry() {
        let mut policy = EvictionState::new();
        let mut slots = Vec::new();
        let index = install(&mut policy, &mut slots, 7);
        policy.detach_for_admission(&mut slots, index);
        policy.restore_for_admission(&mut slots, index, 7);

        assert_eq!(slots[index].hash(), 7);
        policy.record_hit(&mut slots, index);
    }
}
