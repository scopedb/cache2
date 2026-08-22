//! Bounded admission, namespace quotas, and SSD write accounting.
//!
//! This module deliberately keeps M7 policy independent from the record and
//! index formats. Callers supply stable key hashes and encoded live-byte
//! counts. Capacity and write-budget reservations roll back on drop, so a
//! failed append cannot leak quota. All policy tables have fixed capacity and
//! all proportional allocations use fallible `Vec` reservation.

use std::collections::TryReserveError;
use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Number of one-byte counters in the approximate frequency table.
pub(crate) const ADMISSION_COUNTER_COUNT: usize = 64 * 1024;
/// Values larger than this require one additional observation in `SecondHit` mode.
pub const LARGE_OBJECT_THRESHOLD_BYTES: usize = 1024 * 1024;
/// Hard bound for configured namespaces, including the implicit namespace zero.
pub const MAX_NAMESPACE_CONFIGS: usize = 1024;

const ORDINARY_ADMISSION_OBSERVATIONS: u8 = 2;
const LARGE_ADMISSION_OBSERVATIONS: u8 = 3;
const NANOS_PER_SECOND: u128 = 1_000_000_000;
const SECONDS_PER_UTC_DAY: u64 = 24 * 60 * 60;

pub type NamespaceId = u32;

/// Admission behavior for new cache entries.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AdmissionMode {
    /// Preserve the v0.7 behavior: every otherwise valid put is admitted.
    #[default]
    Always,
    /// Admit ordinary objects after two observations and large objects after three.
    SecondHit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionDecision {
    Admit,
    Reject,
}

impl AdmissionDecision {
    #[cfg(test)]
    pub const fn is_admitted(self) -> bool {
        matches!(self, Self::Admit)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AdmissionSnapshot {
    pub observations: u64,
    pub admitted: u64,
    pub rejected: u64,
    pub large_object_rejected: u64,
}

/// Fixed-memory approximate frequency/admission policy.
///
/// One counter is decayed on every observation, so the complete table ages in
/// a bounded number of accesses without a maintenance scan or background
/// thread. Hash collisions may over-admit, but can never reject an established
/// update because `is_update` bypasses the frequency threshold.
pub(crate) struct AdmissionPolicy {
    mode: AdmissionMode,
    counters: Vec<AtomicU8>,
    decay_cursor: AtomicUsize,
    observations: AtomicU64,
    admitted: AtomicU64,
    rejected: AtomicU64,
    large_object_rejected: AtomicU64,
}

impl AdmissionPolicy {
    pub(crate) const fn allocation_bytes() -> usize {
        ADMISSION_COUNTER_COUNT * std::mem::size_of::<AtomicU8>()
    }

    pub(crate) fn try_new(mode: AdmissionMode) -> Result<Self, PolicyBuildError> {
        let mut counters = Vec::new();
        counters
            .try_reserve_exact(ADMISSION_COUNTER_COUNT)
            .map_err(|_| PolicyBuildError::Allocation)?;
        counters.resize_with(ADMISSION_COUNTER_COUNT, || AtomicU8::new(0));
        Ok(Self {
            mode,
            counters,
            decay_cursor: AtomicUsize::new(0),
            observations: AtomicU64::new(0),
            admitted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            large_object_rejected: AtomicU64::new(0),
        })
    }

    /// Record an access, including a cache miss, and return its approximate count.
    pub(crate) fn observe(&self, hash: u64) -> u8 {
        atomic_saturating_add(&self.observations, 1);

        let decay_index =
            self.decay_cursor.fetch_add(1, Ordering::Relaxed) & (ADMISSION_COUNTER_COUNT - 1);
        decay_counter(&self.counters[decay_index]);

        let index = admission_index(hash);
        increment_counter(&self.counters[index])
    }

    /// Observe a put and decide whether it may enter the append path.
    ///
    /// Existing-key updates are always admitted to protect hot objects and to
    /// preserve update semantics when admission is enabled at runtime.
    pub(crate) fn consider(
        &self,
        hash: u64,
        object_bytes: usize,
        is_update: bool,
    ) -> AdmissionDecision {
        let observed = self.observe(hash);
        let required = if object_bytes > LARGE_OBJECT_THRESHOLD_BYTES {
            LARGE_ADMISSION_OBSERVATIONS
        } else {
            ORDINARY_ADMISSION_OBSERVATIONS
        };
        let admitted = self.mode == AdmissionMode::Always || is_update || observed >= required;
        if admitted {
            atomic_saturating_add(&self.admitted, 1);
            AdmissionDecision::Admit
        } else {
            atomic_saturating_add(&self.rejected, 1);
            if object_bytes > LARGE_OBJECT_THRESHOLD_BYTES {
                atomic_saturating_add(&self.large_object_rejected, 1);
            }
            AdmissionDecision::Reject
        }
    }

    pub(crate) fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            observations: self.observations.load(Ordering::Relaxed),
            admitted: self.admitted.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            large_object_rejected: self.large_object_rejected.load(Ordering::Relaxed),
        }
    }
}

/// Per-namespace runtime limits. Omitted limits are unlimited.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceConfig {
    namespace: NamespaceId,
    capacity_bytes: Option<u64>,
    write_budget_bytes_per_second: Option<u64>,
}

impl NamespaceConfig {
    pub const fn new(namespace: NamespaceId) -> Self {
        Self {
            namespace,
            capacity_bytes: None,
            write_budget_bytes_per_second: None,
        }
    }

    pub const fn with_capacity_bytes(mut self, bytes: u64) -> Self {
        self.capacity_bytes = Some(bytes);
        self
    }

    pub const fn with_write_budget(mut self, bytes_per_second: u64) -> Self {
        self.write_budget_bytes_per_second = Some(bytes_per_second);
        self
    }

    pub const fn namespace(&self) -> NamespaceId {
        self.namespace
    }

    pub const fn capacity_bytes(&self) -> Option<u64> {
        self.capacity_bytes
    }

    pub const fn write_budget_bytes_per_second(&self) -> Option<u64> {
        self.write_budget_bytes_per_second
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceUsage {
    pub namespace: NamespaceId,
    pub live_bytes: u64,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamespaceRejectReason {
    UnknownNamespace,
    CapacityExceeded,
    WriteBudgetExceeded,
}

impl fmt::Display for NamespaceRejectReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownNamespace => "namespace is not configured",
            Self::CapacityExceeded => "namespace capacity is exhausted",
            Self::WriteBudgetExceeded => "namespace write budget is exhausted",
        })
    }
}

impl std::error::Error for NamespaceRejectReason {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NamespaceSnapshot {
    pub namespace: NamespaceId,
    pub capacity_bytes: Option<u64>,
    pub live_bytes: u64,
    pub reserved_bytes: u64,
    pub capacity_rejections: u64,
    pub write_budget_bytes_per_second: Option<u64>,
    pub write_budget_available_bytes: Option<u64>,
    pub write_budget_rejections: u64,
}

struct NamespaceRuntime {
    namespace: NamespaceId,
    capacity_bytes: Option<u64>,
    accounting: Mutex<NamespaceAccounting>,
}

struct NamespaceAccounting {
    live_bytes: u64,
    reserved_bytes: u64,
    capacity_rejections: u64,
    write_budget_rejections: u64,
    write_budget: Option<TokenBucket>,
}

/// Thread-safe namespace accounting with a fixed configuration table.
pub(crate) struct NamespaceController {
    namespaces: Vec<NamespaceRuntime>,
}

impl NamespaceController {
    pub(crate) fn try_new(configs: &[NamespaceConfig]) -> Result<Self, PolicyBuildError> {
        let has_default = configs.iter().any(|config| config.namespace == 0);
        let total = configs.len().saturating_add(usize::from(!has_default));
        if total > MAX_NAMESPACE_CONFIGS {
            return Err(PolicyBuildError::Invalid(
                "namespace count exceeds its hard limit",
            ));
        }

        let mut namespaces = Vec::new();
        namespaces
            .try_reserve_exact(total)
            .map_err(|_| PolicyBuildError::Allocation)?;
        if !has_default {
            namespaces.push(NamespaceRuntime::new(&NamespaceConfig::new(0))?);
        }
        for config in configs {
            namespaces.push(NamespaceRuntime::new(config)?);
        }
        namespaces.sort_unstable_by_key(|runtime| runtime.namespace);
        if namespaces
            .windows(2)
            .any(|pair| pair[0].namespace == pair[1].namespace)
        {
            return Err(PolicyBuildError::Invalid(
                "namespace identifiers must be unique",
            ));
        }
        Ok(Self { namespaces })
    }

    pub(crate) fn contains(&self, namespace: NamespaceId) -> bool {
        self.find(namespace).is_some()
    }

    /// Return the sole configured namespace when aggregate retirement can be
    /// attributed without inspecting individual cache records.
    pub(crate) fn single_namespace(&self) -> Option<NamespaceId> {
        (self.namespaces.len() == 1).then(|| self.namespaces[0].namespace)
    }

    /// Reserve the complete encoded size of a prospective live value.
    ///
    /// Reserving the full new size, rather than relying on replacement credit,
    /// keeps capacity strict even when a compact-index collision replaces an
    /// entry from another namespace. Tombstones reserve zero bytes.
    pub(crate) fn try_reserve_capacity(
        self: &Arc<Self>,
        namespace: NamespaceId,
        new_live_bytes: u64,
    ) -> Result<NamespaceCapacityReservation, NamespaceRejectReason> {
        self.try_reserve_capacity_replacing(namespace, new_live_bytes, 0)
    }

    /// Reserve only the positive live-byte delta for an exact same-key
    /// replacement. The caller must hold that key's ordering lock and must
    /// arrange for the credited old value to be retired before committing.
    pub(crate) fn try_reserve_capacity_replacing(
        self: &Arc<Self>,
        namespace: NamespaceId,
        new_live_bytes: u64,
        previous_live_bytes: u64,
    ) -> Result<NamespaceCapacityReservation, NamespaceRejectReason> {
        let index = self
            .find(namespace)
            .ok_or(NamespaceRejectReason::UnknownNamespace)?;
        let runtime = &self.namespaces[index];
        let mut accounting = lock_unpoisoned(&runtime.accounting);
        let credited_bytes = previous_live_bytes.min(accounting.live_bytes);
        let reserved_bytes = new_live_bytes.saturating_sub(credited_bytes);
        let next = accounting
            .live_bytes
            .checked_add(accounting.reserved_bytes)
            .and_then(|bytes| bytes.checked_add(reserved_bytes));
        let within_capacity = match (runtime.capacity_bytes, next) {
            (_, None) => false,
            (Some(capacity), Some(bytes)) => bytes <= capacity,
            (None, Some(_)) => true,
        };
        if !within_capacity {
            accounting.capacity_rejections = accounting.capacity_rejections.saturating_add(1);
            return Err(NamespaceRejectReason::CapacityExceeded);
        }
        accounting.reserved_bytes += reserved_bytes;
        drop(accounting);
        Ok(NamespaceCapacityReservation {
            controller: Arc::clone(self),
            index,
            new_live_bytes,
            reserved_bytes,
            active: true,
        })
    }

    /// Reserve namespace write tokens. The charge is refunded unless committed.
    pub(crate) fn try_reserve_write(
        self: &Arc<Self>,
        namespace: NamespaceId,
        encoded_bytes: u64,
    ) -> Result<NamespaceWriteReservation, NamespaceRejectReason> {
        self.try_reserve_write_at(namespace, encoded_bytes, Instant::now())
    }

    fn try_reserve_write_at(
        self: &Arc<Self>,
        namespace: NamespaceId,
        encoded_bytes: u64,
        now: Instant,
    ) -> Result<NamespaceWriteReservation, NamespaceRejectReason> {
        let index = self
            .find(namespace)
            .ok_or(NamespaceRejectReason::UnknownNamespace)?;
        let mut accounting = lock_unpoisoned(&self.namespaces[index].accounting);
        if let Some(budget) = accounting.write_budget.as_mut() {
            if !budget.try_charge_at(encoded_bytes, now) {
                accounting.write_budget_rejections =
                    accounting.write_budget_rejections.saturating_add(1);
                return Err(NamespaceRejectReason::WriteBudgetExceeded);
            }
        }
        drop(accounting);
        Ok(NamespaceWriteReservation {
            controller: Arc::clone(self),
            index,
            encoded_bytes,
            active: true,
        })
    }

    /// Restore live usage during recovery without enforcing a newly lowered quota.
    ///
    /// An over-quota recovered namespace remains readable, while subsequent
    /// writes are rejected until eviction brings it below the configured cap.
    pub(crate) fn restore_live_bytes(
        &self,
        namespace: NamespaceId,
        live_bytes: u64,
    ) -> Result<(), NamespaceRejectReason> {
        let index = self
            .find(namespace)
            .ok_or(NamespaceRejectReason::UnknownNamespace)?;
        let mut accounting = lock_unpoisoned(&self.namespaces[index].accounting);
        accounting.live_bytes = accounting
            .live_bytes
            .checked_add(live_bytes)
            .ok_or(NamespaceRejectReason::CapacityExceeded)?;
        Ok(())
    }

    /// Remove one exact physical charge, rejecting stale or duplicate receipts
    /// instead of silently borrowing bytes from unrelated live entries.
    pub(crate) fn record_removal_exact(&self, previous: NamespaceUsage) -> bool {
        let Some(index) = self.find(previous.namespace) else {
            return false;
        };
        let mut accounting = lock_unpoisoned(&self.namespaces[index].accounting);
        let Some(live_bytes) = accounting.live_bytes.checked_sub(previous.live_bytes) else {
            return false;
        };
        accounting.live_bytes = live_bytes;
        true
    }

    pub(crate) fn record_replacement_exact(
        &self,
        current: NamespaceUsage,
        previous: Option<NamespaceUsage>,
    ) -> bool {
        let Some(index) = self.find(current.namespace) else {
            return false;
        };
        self.commit_capacity_exact(index, current.live_bytes, 0, previous)
    }

    pub(crate) fn reset_live_bytes(&self) {
        for runtime in &self.namespaces {
            let mut accounting = lock_unpoisoned(&runtime.accounting);
            accounting.live_bytes = 0;
            accounting.reserved_bytes = 0;
        }
    }

    /// Build the fixed namespace-id template used by streaming recovery
    /// accounting without taking a second, heavier statistics snapshot.
    pub(crate) fn try_zero_usage(&self) -> Result<Vec<NamespaceUsage>, TryReserveError> {
        let mut usage = Vec::new();
        usage.try_reserve_exact(self.namespaces.len())?;
        usage.extend(self.namespaces.iter().map(|runtime| NamespaceUsage {
            namespace: runtime.namespace,
            live_bytes: 0,
        }));
        Ok(usage)
    }

    /// Return a fallibly allocated point-in-time snapshot sorted by namespace id.
    pub(crate) fn try_snapshots(&self) -> Result<Vec<NamespaceSnapshot>, TryReserveError> {
        let mut snapshots = Vec::new();
        snapshots.try_reserve_exact(self.namespaces.len())?;
        let now = Instant::now();
        for runtime in &self.namespaces {
            let mut accounting = lock_unpoisoned(&runtime.accounting);
            let (rate, available) = if let Some(budget) = accounting.write_budget.as_mut() {
                budget.refill(now);
                (Some(budget.rate), Some(budget.available))
            } else {
                (None, None)
            };
            snapshots.push(NamespaceSnapshot {
                namespace: runtime.namespace,
                capacity_bytes: runtime.capacity_bytes,
                live_bytes: accounting.live_bytes,
                reserved_bytes: accounting.reserved_bytes,
                capacity_rejections: accounting.capacity_rejections,
                write_budget_bytes_per_second: rate,
                write_budget_available_bytes: available,
                write_budget_rejections: accounting.write_budget_rejections,
            });
        }
        Ok(snapshots)
    }

    pub(crate) fn snapshot(&self, namespace: NamespaceId) -> Option<NamespaceSnapshot> {
        let index = self.find(namespace)?;
        let runtime = &self.namespaces[index];
        let mut accounting = lock_unpoisoned(&runtime.accounting);
        let (rate, available) = if let Some(budget) = accounting.write_budget.as_mut() {
            budget.refill(Instant::now());
            (Some(budget.rate), Some(budget.available))
        } else {
            (None, None)
        };
        Some(NamespaceSnapshot {
            namespace: runtime.namespace,
            capacity_bytes: runtime.capacity_bytes,
            live_bytes: accounting.live_bytes,
            reserved_bytes: accounting.reserved_bytes,
            capacity_rejections: accounting.capacity_rejections,
            write_budget_bytes_per_second: rate,
            write_budget_available_bytes: available,
            write_budget_rejections: accounting.write_budget_rejections,
        })
    }

    pub(crate) fn rejection_totals(&self) -> (u64, u64) {
        let mut capacity = 0_u64;
        let mut write_budget = 0_u64;
        for runtime in &self.namespaces {
            let accounting = lock_unpoisoned(&runtime.accounting);
            capacity = capacity.saturating_add(accounting.capacity_rejections);
            write_budget = write_budget.saturating_add(accounting.write_budget_rejections);
        }
        (capacity, write_budget)
    }

    fn find(&self, namespace: NamespaceId) -> Option<usize> {
        self.namespaces
            .binary_search_by_key(&namespace, |runtime| runtime.namespace)
            .ok()
    }

    fn commit_capacity(
        &self,
        new_index: usize,
        new_live_bytes: u64,
        reserved_bytes: u64,
        previous: Option<NamespaceUsage>,
    ) {
        let previous_index = previous.and_then(|usage| {
            self.find(usage.namespace)
                .map(|index| (index, usage.live_bytes))
        });
        match previous_index {
            Some((old_index, old_live_bytes)) if old_index == new_index => {
                let mut accounting = lock_unpoisoned(&self.namespaces[new_index].accounting);
                accounting.reserved_bytes =
                    accounting.reserved_bytes.saturating_sub(reserved_bytes);
                accounting.live_bytes = accounting
                    .live_bytes
                    .saturating_sub(old_live_bytes)
                    .saturating_add(new_live_bytes);
            }
            Some((old_index, old_live_bytes)) if old_index < new_index => {
                let mut old = lock_unpoisoned(&self.namespaces[old_index].accounting);
                let mut new = lock_unpoisoned(&self.namespaces[new_index].accounting);
                old.live_bytes = old.live_bytes.saturating_sub(old_live_bytes);
                new.reserved_bytes = new.reserved_bytes.saturating_sub(reserved_bytes);
                new.live_bytes = new.live_bytes.saturating_add(new_live_bytes);
            }
            Some((old_index, old_live_bytes)) => {
                let mut new = lock_unpoisoned(&self.namespaces[new_index].accounting);
                let mut old = lock_unpoisoned(&self.namespaces[old_index].accounting);
                old.live_bytes = old.live_bytes.saturating_sub(old_live_bytes);
                new.reserved_bytes = new.reserved_bytes.saturating_sub(reserved_bytes);
                new.live_bytes = new.live_bytes.saturating_add(new_live_bytes);
            }
            None => {
                let mut accounting = lock_unpoisoned(&self.namespaces[new_index].accounting);
                accounting.reserved_bytes =
                    accounting.reserved_bytes.saturating_sub(reserved_bytes);
                accounting.live_bytes = accounting.live_bytes.saturating_add(new_live_bytes);
            }
        }
    }

    fn commit_capacity_exact(
        &self,
        new_index: usize,
        new_live_bytes: u64,
        reserved_bytes: u64,
        previous: Option<NamespaceUsage>,
    ) -> bool {
        let previous_index = match previous {
            // A namespace can be removed from configuration across reopen.
            // Recovery deliberately omits its physical entries, so replacing
            // one later must treat that previous charge as unaccounted.
            Some(usage) => self
                .find(usage.namespace)
                .map(|index| (index, usage.live_bytes)),
            None => None,
        };
        match previous_index {
            Some((old_index, old_live_bytes)) if old_index == new_index => {
                let mut accounting = lock_unpoisoned(&self.namespaces[new_index].accounting);
                let Some(next_reserved) = accounting.reserved_bytes.checked_sub(reserved_bytes)
                else {
                    return false;
                };
                let Some(next_live) = accounting
                    .live_bytes
                    .checked_sub(old_live_bytes)
                    .and_then(|live| live.checked_add(new_live_bytes))
                else {
                    return false;
                };
                accounting.reserved_bytes = next_reserved;
                accounting.live_bytes = next_live;
                true
            }
            Some((old_index, old_live_bytes)) if old_index < new_index => {
                let mut old = lock_unpoisoned(&self.namespaces[old_index].accounting);
                let mut new = lock_unpoisoned(&self.namespaces[new_index].accounting);
                let Some(old_live) = old.live_bytes.checked_sub(old_live_bytes) else {
                    return false;
                };
                let Some(new_reserved) = new.reserved_bytes.checked_sub(reserved_bytes) else {
                    return false;
                };
                let Some(new_live) = new.live_bytes.checked_add(new_live_bytes) else {
                    return false;
                };
                old.live_bytes = old_live;
                new.reserved_bytes = new_reserved;
                new.live_bytes = new_live;
                true
            }
            Some((old_index, old_live_bytes)) => {
                let mut new = lock_unpoisoned(&self.namespaces[new_index].accounting);
                let mut old = lock_unpoisoned(&self.namespaces[old_index].accounting);
                let Some(old_live) = old.live_bytes.checked_sub(old_live_bytes) else {
                    return false;
                };
                let Some(new_reserved) = new.reserved_bytes.checked_sub(reserved_bytes) else {
                    return false;
                };
                let Some(new_live) = new.live_bytes.checked_add(new_live_bytes) else {
                    return false;
                };
                old.live_bytes = old_live;
                new.reserved_bytes = new_reserved;
                new.live_bytes = new_live;
                true
            }
            None => {
                let mut accounting = lock_unpoisoned(&self.namespaces[new_index].accounting);
                let Some(next_reserved) = accounting.reserved_bytes.checked_sub(reserved_bytes)
                else {
                    return false;
                };
                let Some(next_live) = accounting.live_bytes.checked_add(new_live_bytes) else {
                    return false;
                };
                accounting.reserved_bytes = next_reserved;
                accounting.live_bytes = next_live;
                true
            }
        }
    }

    fn release_capacity(&self, index: usize, live_bytes: u64) {
        let mut accounting = lock_unpoisoned(&self.namespaces[index].accounting);
        accounting.reserved_bytes = accounting.reserved_bytes.saturating_sub(live_bytes);
    }

    fn refund_write(&self, index: usize, encoded_bytes: u64) {
        let mut accounting = lock_unpoisoned(&self.namespaces[index].accounting);
        if let Some(budget) = accounting.write_budget.as_mut() {
            budget.refund_at(encoded_bytes, Instant::now());
        }
    }
}

impl NamespaceRuntime {
    fn new(config: &NamespaceConfig) -> Result<Self, PolicyBuildError> {
        if config.capacity_bytes == Some(0) {
            return Err(PolicyBuildError::Invalid(
                "namespace capacity must be greater than zero when enabled",
            ));
        }
        if config.write_budget_bytes_per_second == Some(0) {
            return Err(PolicyBuildError::Invalid(
                "namespace write budget must be greater than zero when enabled",
            ));
        }
        Ok(Self {
            namespace: config.namespace,
            capacity_bytes: config.capacity_bytes,
            accounting: Mutex::new(NamespaceAccounting {
                live_bytes: 0,
                reserved_bytes: 0,
                capacity_rejections: 0,
                write_budget_rejections: 0,
                write_budget: config.write_budget_bytes_per_second.map(TokenBucket::new),
            }),
        })
    }
}

#[must_use = "dropping a capacity reservation rolls it back"]
pub(crate) struct NamespaceCapacityReservation {
    controller: Arc<NamespaceController>,
    index: usize,
    new_live_bytes: u64,
    reserved_bytes: u64,
    active: bool,
}

impl NamespaceCapacityReservation {
    pub(crate) const fn live_bytes(&self) -> u64 {
        self.new_live_bytes
    }

    #[cfg(test)]
    pub(crate) const fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }

    /// Publish the new usage and remove the replaced live entry, if any.
    pub(crate) fn commit(mut self, previous: Option<NamespaceUsage>) {
        self.controller.commit_capacity(
            self.index,
            self.new_live_bytes,
            self.reserved_bytes,
            previous,
        );
        self.active = false;
    }

    pub(crate) fn commit_actual_exact(
        mut self,
        actual_live_bytes: u64,
        previous: Option<NamespaceUsage>,
    ) -> bool {
        if actual_live_bytes > self.new_live_bytes
            || !self.controller.commit_capacity_exact(
                self.index,
                actual_live_bytes,
                self.reserved_bytes,
                previous,
            )
        {
            return false;
        }
        self.active = false;
        true
    }
}

impl Drop for NamespaceCapacityReservation {
    fn drop(&mut self) {
        if self.active {
            self.controller
                .release_capacity(self.index, self.reserved_bytes);
        }
    }
}

#[must_use = "dropping a namespace write reservation refunds its tokens"]
pub(crate) struct NamespaceWriteReservation {
    controller: Arc<NamespaceController>,
    index: usize,
    encoded_bytes: u64,
    active: bool,
}

impl NamespaceWriteReservation {
    pub(crate) fn commit(mut self) {
        self.active = false;
    }
}

impl Drop for NamespaceWriteReservation {
    fn drop(&mut self) {
        if self.active {
            self.controller.refund_write(self.index, self.encoded_bytes);
        }
    }
}

struct TokenBucket {
    rate: u64,
    available: u64,
    last_refill: Instant,
    fractional_nano_bytes: u128,
}

impl TokenBucket {
    fn new(rate: u64) -> Self {
        Self::new_at(rate, Instant::now())
    }

    fn new_at(rate: u64, now: Instant) -> Self {
        Self {
            rate,
            available: rate,
            last_refill: now,
            fractional_nano_bytes: 0,
        }
    }

    fn try_charge_at(&mut self, bytes: u64, now: Instant) -> bool {
        self.refill(now);
        if bytes > self.available {
            return false;
        }
        self.available -= bytes;
        true
    }

    fn refund_at(&mut self, bytes: u64, now: Instant) {
        self.refill(now);
        self.available = self.available.saturating_add(bytes).min(self.rate);
    }

    fn refill(&mut self, now: Instant) {
        let elapsed_ns = now.duration_since(self.last_refill).as_nanos();
        self.last_refill = now;
        let generated = elapsed_ns
            .saturating_mul(u128::from(self.rate))
            .saturating_add(self.fractional_nano_bytes);
        let whole = generated / NANOS_PER_SECOND;
        self.fractional_nano_bytes = generated % NANOS_PER_SECOND;
        self.available = self
            .available
            .saturating_add(whole.min(u128::from(u64::MAX)) as u64)
            .min(self.rate);
        if self.available == self.rate {
            self.fractional_nano_bytes = 0;
        }
    }
}

/// Source of submitted host-write bytes.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostWriteKind {
    ForegroundRecord,
    Reinsertion,
    /// Background reclaim data copies other than ordinary hit reinsertion.
    Reclaimer,
    /// Correctness-required tombstone copies performed before victim reuse.
    ForcedTombstone,
    Metadata,
    Checkpoint,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HostWriteSnapshot {
    pub host_write_operations: u64,
    pub host_write_bytes: u64,
    pub foreground_record_bytes: u64,
    pub reinsertion_bytes: u64,
    pub reclaimer_bytes: u64,
    pub forced_tombstone_bytes: u64,
    pub metadata_bytes: u64,
    pub checkpoint_bytes: u64,
    pub admitted_value_bytes: u64,
    /// Total submitted host bytes per 1,000 admitted logical value bytes.
    pub write_amplification_milli: u64,
    pub failed_write_operations: u64,
    pub utc_day: u64,
    pub daily_host_write_bytes: u64,
    pub daily_budget_used_bytes: u64,
    pub daily_budget_reserved_bytes: u64,
    pub daily_budget_bytes: Option<u64>,
    pub daily_budget_remaining_bytes: Option<u64>,
    pub daily_budget_rejections: u64,
    pub daily_budget_exceeded: bool,
}

struct DailyWriteWindow {
    utc_day: u64,
    host_write_bytes: u64,
    budget_used_bytes: u64,
    budget_reserved_bytes: u64,
    budget_rejections: u64,
}

/// Host-write and write-amplification accounting since cache open.
///
/// The optional limit is a fixed UTC-day budget. Foreground code reserves its
/// expected record bytes before marking the cache dirty. Metadata needed to
/// complete an already accepted durability transition is always recorded and
/// may set `daily_budget_exceeded`; it is never rejected mid-commit.
pub(crate) struct HostWriteTracker {
    daily_budget_bytes: Option<u64>,
    host_write_operations: AtomicU64,
    host_write_bytes: AtomicU64,
    foreground_record_bytes: AtomicU64,
    reinsertion_bytes: AtomicU64,
    reclaimer_bytes: AtomicU64,
    forced_tombstone_bytes: AtomicU64,
    metadata_bytes: AtomicU64,
    checkpoint_bytes: AtomicU64,
    admitted_value_bytes: AtomicU64,
    failed_write_operations: AtomicU64,
    daily: Mutex<DailyWriteWindow>,
}

impl HostWriteTracker {
    pub(crate) fn try_new(
        daily_budget_bytes: Option<u64>,
        daily_baseline: Option<(u64, u64)>,
    ) -> Result<Self, PolicyBuildError> {
        if daily_budget_bytes == Some(0) {
            return Err(PolicyBuildError::Invalid(
                "daily host-write budget must be greater than zero when enabled",
            ));
        }
        let utc_day = current_utc_day();
        let baseline_bytes = daily_baseline
            .filter(|(baseline_day, _)| *baseline_day == utc_day)
            .map_or(0, |(_, bytes)| bytes);
        Ok(Self::new_at(daily_budget_bytes, utc_day, baseline_bytes))
    }

    fn new_at(daily_budget_bytes: Option<u64>, utc_day: u64, baseline_bytes: u64) -> Self {
        Self {
            daily_budget_bytes,
            host_write_operations: AtomicU64::new(0),
            host_write_bytes: AtomicU64::new(0),
            foreground_record_bytes: AtomicU64::new(0),
            reinsertion_bytes: AtomicU64::new(0),
            reclaimer_bytes: AtomicU64::new(0),
            forced_tombstone_bytes: AtomicU64::new(0),
            metadata_bytes: AtomicU64::new(0),
            checkpoint_bytes: AtomicU64::new(0),
            admitted_value_bytes: AtomicU64::new(0),
            failed_write_operations: AtomicU64::new(0),
            daily: Mutex::new(DailyWriteWindow {
                utc_day,
                host_write_bytes: baseline_bytes,
                budget_used_bytes: baseline_bytes,
                budget_reserved_bytes: 0,
                budget_rejections: 0,
            }),
        }
    }

    /// Reserve expected record bytes against the fixed UTC-day device budget.
    pub(crate) fn try_reserve_daily(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<DailyWriteReservation, DailyWriteBudgetExceeded> {
        self.try_reserve_daily_at(bytes, current_utc_day())
    }

    fn try_reserve_daily_at(
        self: &Arc<Self>,
        bytes: u64,
        utc_day: u64,
    ) -> Result<DailyWriteReservation, DailyWriteBudgetExceeded> {
        let mut daily = lock_unpoisoned(&self.daily);
        reset_daily_window_if_needed(&mut daily, utc_day);
        let already_used = daily.host_write_bytes.max(daily.budget_used_bytes);
        let prospective = already_used
            .checked_add(daily.budget_reserved_bytes)
            .and_then(|used| used.checked_add(bytes));
        if prospective.is_none()
            || self
                .daily_budget_bytes
                .is_some_and(|budget| prospective.is_some_and(|used| used > budget))
        {
            daily.budget_rejections = daily.budget_rejections.saturating_add(1);
            return Err(DailyWriteBudgetExceeded);
        }
        daily.budget_reserved_bytes += bytes;
        drop(daily);
        Ok(DailyWriteReservation {
            tracker: Arc::clone(self),
            bytes,
            utc_day,
            active: true,
        })
    }

    /// Count a submitted host write. Failed submissions may still have reached
    /// the device, so call this at submission rather than completion.
    pub(crate) fn record_write(&self, kind: HostWriteKind, bytes: u64) {
        self.record_write_at(kind, bytes, current_utc_day());
    }

    fn record_write_at(&self, kind: HostWriteKind, bytes: u64, utc_day: u64) {
        atomic_saturating_add(&self.host_write_operations, 1);
        atomic_saturating_add(&self.host_write_bytes, bytes);
        let category = match kind {
            HostWriteKind::ForegroundRecord => &self.foreground_record_bytes,
            HostWriteKind::Reinsertion => &self.reinsertion_bytes,
            HostWriteKind::Reclaimer => &self.reclaimer_bytes,
            HostWriteKind::ForcedTombstone => &self.forced_tombstone_bytes,
            HostWriteKind::Metadata => &self.metadata_bytes,
            HostWriteKind::Checkpoint => &self.checkpoint_bytes,
        };
        atomic_saturating_add(category, bytes);
        let mut daily = lock_unpoisoned(&self.daily);
        reset_daily_window_if_needed(&mut daily, utc_day);
        daily.host_write_bytes = daily.host_write_bytes.saturating_add(bytes);
    }

    pub(crate) fn record_admitted_value(&self, bytes: u64) {
        atomic_saturating_add(&self.admitted_value_bytes, bytes);
    }

    pub(crate) fn record_write_failure(&self) {
        atomic_saturating_add(&self.failed_write_operations, 1);
    }

    pub(crate) fn snapshot(&self) -> HostWriteSnapshot {
        self.snapshot_at(current_utc_day())
    }

    fn snapshot_at(&self, utc_day: u64) -> HostWriteSnapshot {
        let host_write_bytes = self.host_write_bytes.load(Ordering::Relaxed);
        let admitted_value_bytes = self.admitted_value_bytes.load(Ordering::Relaxed);
        let mut daily = lock_unpoisoned(&self.daily);
        reset_daily_window_if_needed(&mut daily, utc_day);
        let daily_used = daily.host_write_bytes.max(daily.budget_used_bytes);
        HostWriteSnapshot {
            host_write_operations: self.host_write_operations.load(Ordering::Relaxed),
            host_write_bytes,
            foreground_record_bytes: self.foreground_record_bytes.load(Ordering::Relaxed),
            reinsertion_bytes: self.reinsertion_bytes.load(Ordering::Relaxed),
            reclaimer_bytes: self.reclaimer_bytes.load(Ordering::Relaxed),
            forced_tombstone_bytes: self.forced_tombstone_bytes.load(Ordering::Relaxed),
            metadata_bytes: self.metadata_bytes.load(Ordering::Relaxed),
            checkpoint_bytes: self.checkpoint_bytes.load(Ordering::Relaxed),
            admitted_value_bytes,
            write_amplification_milli: ratio_milli(host_write_bytes, admitted_value_bytes),
            failed_write_operations: self.failed_write_operations.load(Ordering::Relaxed),
            utc_day: daily.utc_day,
            daily_host_write_bytes: daily.host_write_bytes,
            daily_budget_used_bytes: daily.budget_used_bytes,
            daily_budget_reserved_bytes: daily.budget_reserved_bytes,
            daily_budget_bytes: self.daily_budget_bytes,
            daily_budget_remaining_bytes: self
                .daily_budget_bytes
                .map(|budget| budget.saturating_sub(daily_used)),
            daily_budget_rejections: daily.budget_rejections,
            daily_budget_exceeded: self
                .daily_budget_bytes
                .is_some_and(|budget| daily_used > budget),
        }
    }

    fn commit_daily(&self, bytes: u64, reserved_day: u64, current_day: u64) {
        let mut daily = lock_unpoisoned(&self.daily);
        reset_daily_window_if_needed(&mut daily, current_day);
        if daily.utc_day == reserved_day {
            daily.budget_reserved_bytes = daily.budget_reserved_bytes.saturating_sub(bytes);
        }
        daily.budget_used_bytes = daily.budget_used_bytes.saturating_add(bytes);
    }

    fn release_daily(&self, bytes: u64, reserved_day: u64, current_day: u64) {
        let mut daily = lock_unpoisoned(&self.daily);
        reset_daily_window_if_needed(&mut daily, current_day);
        if daily.utc_day == reserved_day {
            daily.budget_reserved_bytes = daily.budget_reserved_bytes.saturating_sub(bytes);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DailyWriteBudgetExceeded;

impl fmt::Display for DailyWriteBudgetExceeded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("daily host-write budget is exhausted")
    }
}

impl std::error::Error for DailyWriteBudgetExceeded {}

#[must_use = "dropping a daily write reservation rolls it back"]
pub(crate) struct DailyWriteReservation {
    tracker: Arc<HostWriteTracker>,
    bytes: u64,
    utc_day: u64,
    active: bool,
}

impl DailyWriteReservation {
    pub(crate) fn commit(mut self) {
        self.tracker
            .commit_daily(self.bytes, self.utc_day, current_utc_day());
        self.active = false;
    }
}

impl Drop for DailyWriteReservation {
    fn drop(&mut self) {
        if self.active {
            self.tracker
                .release_daily(self.bytes, self.utc_day, current_utc_day());
        }
    }
}

/// Whether an observed critical NVMe health sample affects put admission.
///
/// Health is advisory by default. Reads, removes, flushes, and close are never
/// rejected by this policy.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DeviceHealthPolicy {
    #[default]
    ObserveOnly,
    RejectPutsOnCritical,
}

/// One externally collected NVMe SMART/health sample.
///
/// The cache library does not guess which controller backs an ordinary file;
/// an operator or management tool supplies samples for the intended device.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NvmeHealthSample {
    pub observed_at_unix_ms: u64,
    pub data_units_written: u64,
    pub critical_warning: u8,
    pub available_spare_percent: u8,
    pub available_spare_threshold_percent: u8,
    pub percentage_used: u8,
    pub media_errors: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NvmeHealthStats {
    pub sample: NvmeHealthSample,
    pub observations: u64,
    /// True when the latest counter is greater than the preceding sample.
    pub media_errors_increased: bool,
    /// Latched for this cache instance once any media-error growth is observed.
    pub media_error_growth_observed: bool,
    pub critical: bool,
}

struct DeviceHealthState {
    latest: Option<NvmeHealthStats>,
    media_error_growth_observed: bool,
    observations: u64,
}

struct DeviceHealthTracker {
    policy: DeviceHealthPolicy,
    state: Mutex<DeviceHealthState>,
}

impl DeviceHealthTracker {
    fn new(policy: DeviceHealthPolicy) -> Self {
        Self {
            policy,
            state: Mutex::new(DeviceHealthState {
                latest: None,
                media_error_growth_observed: false,
                observations: 0,
            }),
        }
    }

    fn observe(&self, sample: NvmeHealthSample) -> NvmeHealthStats {
        let mut state = lock_unpoisoned(&self.state);
        let media_errors_increased = state
            .latest
            .is_some_and(|previous| sample.media_errors > previous.sample.media_errors);
        state.media_error_growth_observed |= media_errors_increased;
        state.observations = state.observations.saturating_add(1);
        let critical = sample.critical_warning != 0
            || sample.available_spare_percent < sample.available_spare_threshold_percent
            || state.media_error_growth_observed;
        let stats = NvmeHealthStats {
            sample,
            observations: state.observations,
            media_errors_increased,
            media_error_growth_observed: state.media_error_growth_observed,
            critical,
        };
        state.latest = Some(stats);
        stats
    }

    fn latest(&self) -> Option<NvmeHealthStats> {
        lock_unpoisoned(&self.state).latest
    }

    fn should_reject_put(&self) -> bool {
        self.policy == DeviceHealthPolicy::RejectPutsOnCritical
            && self.latest().is_some_and(|stats| stats.critical)
    }
}

/// Complete M7 policy state owned by one cache instance.
pub(crate) struct PolicyController {
    admission: AdmissionPolicy,
    namespaces: Arc<NamespaceController>,
    host_writes: Arc<HostWriteTracker>,
    device_health: DeviceHealthTracker,
}

impl PolicyController {
    #[cfg(test)]
    pub(crate) fn try_new(
        admission_mode: AdmissionMode,
        namespace_configs: &[NamespaceConfig],
        daily_host_write_budget_bytes: Option<u64>,
    ) -> Result<Self, PolicyBuildError> {
        Self::try_new_with_health(
            admission_mode,
            namespace_configs,
            daily_host_write_budget_bytes,
            None,
            DeviceHealthPolicy::ObserveOnly,
        )
    }

    pub(crate) fn try_new_with_health(
        admission_mode: AdmissionMode,
        namespace_configs: &[NamespaceConfig],
        daily_host_write_budget_bytes: Option<u64>,
        daily_host_write_baseline: Option<(u64, u64)>,
        device_health_policy: DeviceHealthPolicy,
    ) -> Result<Self, PolicyBuildError> {
        Ok(Self {
            admission: AdmissionPolicy::try_new(admission_mode)?,
            namespaces: Arc::new(NamespaceController::try_new(namespace_configs)?),
            host_writes: Arc::new(HostWriteTracker::try_new(
                daily_host_write_budget_bytes,
                daily_host_write_baseline,
            )?),
            device_health: DeviceHealthTracker::new(device_health_policy),
        })
    }

    /// Build policy bookkeeping around a host-write tracker owned by a higher
    /// level driver. The caller decides whether this controller's admission
    /// and namespace policies are active; every physical write recorded through
    /// this controller is accumulated in the supplied shared tracker.
    pub(crate) fn try_new_with_external_host_writes(
        admission_mode: AdmissionMode,
        namespace_configs: &[NamespaceConfig],
        host_writes: Arc<HostWriteTracker>,
        device_health_policy: DeviceHealthPolicy,
    ) -> Result<Self, PolicyBuildError> {
        Ok(Self {
            admission: AdmissionPolicy::try_new(admission_mode)?,
            namespaces: Arc::new(NamespaceController::try_new(namespace_configs)?),
            host_writes,
            device_health: DeviceHealthTracker::new(device_health_policy),
        })
    }

    pub(crate) const fn admission(&self) -> &AdmissionPolicy {
        &self.admission
    }

    pub(crate) const fn namespaces(&self) -> &Arc<NamespaceController> {
        &self.namespaces
    }

    pub(crate) const fn host_writes(&self) -> &Arc<HostWriteTracker> {
        &self.host_writes
    }

    pub(crate) fn observe_nvme_health(&self, sample: NvmeHealthSample) -> NvmeHealthStats {
        self.device_health.observe(sample)
    }

    pub(crate) fn nvme_health(&self) -> Option<NvmeHealthStats> {
        self.device_health.latest()
    }

    pub(crate) fn should_reject_put(&self) -> bool {
        self.device_health.should_reject_put()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PolicyBuildError {
    Invalid(&'static str),
    Allocation,
}

impl fmt::Display for PolicyBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::Allocation => formatter.write_str("policy bookkeeping cannot be allocated"),
        }
    }
}

impl std::error::Error for PolicyBuildError {}

fn admission_index(hash: u64) -> usize {
    let mut mixed = hash;
    mixed ^= mixed >> 33;
    mixed = mixed.wrapping_mul(0xff51_afd7_ed55_8ccd);
    mixed ^= mixed >> 33;
    mixed = mixed.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    mixed ^= mixed >> 33;
    (mixed as usize) & (ADMISSION_COUNTER_COUNT - 1)
}

fn increment_counter(counter: &AtomicU8) -> u8 {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(1);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(observed) => current = observed,
        }
    }
}

fn decay_counter(counter: &AtomicU8) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current / 2;
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn atomic_saturating_add(counter: &AtomicU64, amount: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(amount);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn ratio_milli(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    u64::try_from(
        u128::from(numerator)
            .saturating_mul(1000)
            .checked_div(u128::from(denominator))
            .unwrap_or(0)
            .min(u128::from(u64::MAX)),
    )
    .unwrap_or(u64::MAX)
}

fn current_utc_day() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / SECONDS_PER_UTC_DAY
}

fn reset_daily_window_if_needed(daily: &mut DailyWriteWindow, utc_day: u64) {
    if daily.utc_day != utc_day {
        daily.utc_day = utc_day;
        daily.host_write_bytes = 0;
        daily.budget_used_bytes = 0;
        daily.budget_reserved_bytes = 0;
        daily.budget_rejections = 0;
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn aggregate_retirement_requires_exactly_one_effective_namespace() {
        assert_eq!(
            NamespaceController::try_new(&[])
                .unwrap()
                .single_namespace(),
            Some(0)
        );
        assert_eq!(
            NamespaceController::try_new(&[NamespaceConfig::new(0)])
                .unwrap()
                .single_namespace(),
            Some(0)
        );
        assert_eq!(
            NamespaceController::try_new(&[NamespaceConfig::new(7)])
                .unwrap()
                .single_namespace(),
            None
        );
    }

    #[test]
    fn second_hit_admission_protects_updates_and_large_objects() {
        let policy = AdmissionPolicy::try_new(AdmissionMode::SecondHit).unwrap();
        let ordinary_hash = 17;
        assert_eq!(
            policy.consider(ordinary_hash, 4096, false),
            AdmissionDecision::Reject
        );
        assert_eq!(
            policy.consider(ordinary_hash, 4096, false),
            AdmissionDecision::Admit
        );

        let large_hash = 23;
        assert_eq!(
            policy.consider(
                large_hash,
                LARGE_OBJECT_THRESHOLD_BYTES.saturating_add(1),
                false
            ),
            AdmissionDecision::Reject
        );
        assert_eq!(
            policy.consider(
                large_hash,
                LARGE_OBJECT_THRESHOLD_BYTES.saturating_add(1),
                false
            ),
            AdmissionDecision::Reject
        );
        assert_eq!(
            policy.consider(
                large_hash,
                LARGE_OBJECT_THRESHOLD_BYTES.saturating_add(1),
                false
            ),
            AdmissionDecision::Admit
        );
        assert!(policy.consider(99, usize::MAX, true).is_admitted());
        assert_eq!(policy.snapshot().large_object_rejected, 2);
    }

    #[test]
    fn namespace_reservations_rollback_and_publication_moves_live_usage() {
        let namespaces = Arc::new(
            NamespaceController::try_new(&[
                NamespaceConfig::new(7)
                    .with_capacity_bytes(100)
                    .with_write_budget(100),
                NamespaceConfig::new(8).with_capacity_bytes(100),
            ])
            .unwrap(),
        );
        assert!(namespaces.contains(0));

        drop(namespaces.try_reserve_capacity(7, 80).unwrap());
        let snapshots = namespaces.try_snapshots().unwrap();
        assert_eq!(snapshots[1].reserved_bytes, 0);

        namespaces.try_reserve_capacity(7, 80).unwrap().commit(None);
        assert_eq!(
            namespaces.try_reserve_capacity(7, 21).err(),
            Some(NamespaceRejectReason::CapacityExceeded)
        );
        namespaces
            .try_reserve_capacity(8, 60)
            .unwrap()
            .commit(Some(NamespaceUsage {
                namespace: 7,
                live_bytes: 80,
            }));
        let snapshots = namespaces.try_snapshots().unwrap();
        assert_eq!(snapshots[1].live_bytes, 0);
        assert_eq!(snapshots[2].live_bytes, 60);

        let start = Instant::now();
        drop(namespaces.try_reserve_write_at(7, 80, start).unwrap());
        namespaces
            .try_reserve_write_at(7, 80, start)
            .unwrap()
            .commit();
        assert_eq!(
            namespaces.try_reserve_write_at(7, 21, start).err(),
            Some(NamespaceRejectReason::WriteBudgetExceeded)
        );
        assert!(
            namespaces
                .try_reserve_write_at(7, 21, start + Duration::from_secs(1))
                .is_ok()
        );
    }

    #[test]
    fn replacement_reserves_its_full_size_and_commits_actual_previous() {
        let namespaces = Arc::new(
            NamespaceController::try_new(&[NamespaceConfig::new(7).with_capacity_bytes(170)])
                .unwrap(),
        );
        namespaces.try_reserve_capacity(7, 80).unwrap().commit(None);

        let replacement = namespaces.try_reserve_capacity(7, 90).unwrap();
        assert_eq!(replacement.live_bytes(), 90);
        assert_eq!(replacement.reserved_bytes(), 90);
        let snapshot = namespaces.try_snapshots().unwrap()[1];
        assert_eq!(snapshot.live_bytes, 80);
        assert_eq!(snapshot.reserved_bytes, 90);
        drop(replacement);
        assert_eq!(namespaces.try_snapshots().unwrap()[1].reserved_bytes, 0);

        namespaces
            .try_reserve_capacity(7, 90)
            .unwrap()
            .commit(Some(NamespaceUsage {
                namespace: 7,
                live_bytes: 80,
            }));
        let snapshot = namespaces.try_snapshots().unwrap()[1];
        assert_eq!(snapshot.live_bytes, 90);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(
            namespaces.try_reserve_capacity(7, 81).err(),
            Some(NamespaceRejectReason::CapacityExceeded)
        );
    }

    #[test]
    fn daily_budget_and_write_amplification_include_all_host_writes() {
        let day = current_utc_day();
        let tracker = Arc::new(HostWriteTracker::new_at(Some(100), day, 0));
        drop(tracker.try_reserve_daily_at(80, day).unwrap());
        tracker.try_reserve_daily_at(80, day).unwrap().commit();
        assert!(tracker.try_reserve_daily_at(21, day).is_err());

        tracker.record_admitted_value(50);
        tracker.record_write_at(HostWriteKind::ForegroundRecord, 50, day);
        tracker.record_write_at(HostWriteKind::Metadata, 10, day);
        tracker.record_write_at(HostWriteKind::Reclaimer, 7, day);
        tracker.record_write_at(HostWriteKind::ForcedTombstone, 5, day);
        let snapshot = tracker.snapshot_at(day);
        assert_eq!(snapshot.host_write_bytes, 72);
        assert_eq!(snapshot.write_amplification_milli, 1440);
        assert_eq!(snapshot.reclaimer_bytes, 7);
        assert_eq!(snapshot.forced_tombstone_bytes, 5);
        assert_eq!(snapshot.daily_budget_used_bytes, 80);
        assert_eq!(snapshot.daily_budget_remaining_bytes, Some(20));
        assert_eq!(snapshot.daily_budget_rejections, 1);

        let next_day = tracker.snapshot_at(day.saturating_add(1));
        assert_eq!(next_day.daily_host_write_bytes, 0);
        assert_eq!(next_day.daily_budget_used_bytes, 0);
        assert_eq!(next_day.daily_budget_rejections, 0);

        let resumed = Arc::new(HostWriteTracker::new_at(Some(100), day, 70));
        assert!(resumed.try_reserve_daily_at(31, day).is_err());
        let resumed = resumed.snapshot_at(day);
        assert_eq!(resumed.host_write_bytes, 0);
        assert_eq!(resumed.daily_host_write_bytes, 70);
        assert_eq!(resumed.daily_budget_remaining_bytes, Some(30));
    }

    #[test]
    fn nvme_health_is_advisory_by_default_and_media_error_growth_latches() {
        let observe_only = PolicyController::try_new(AdmissionMode::Always, &[], None).unwrap();
        let warning = observe_only.observe_nvme_health(NvmeHealthSample {
            critical_warning: 1,
            available_spare_percent: 100,
            available_spare_threshold_percent: 10,
            ..NvmeHealthSample::default()
        });
        assert!(warning.critical);
        assert!(!observe_only.should_reject_put());

        let rejecting = PolicyController::try_new_with_health(
            AdmissionMode::Always,
            &[],
            None,
            None,
            DeviceHealthPolicy::RejectPutsOnCritical,
        )
        .unwrap();
        let healthy = NvmeHealthSample {
            available_spare_percent: 90,
            available_spare_threshold_percent: 10,
            media_errors: 4,
            ..NvmeHealthSample::default()
        };
        assert!(!rejecting.observe_nvme_health(healthy).critical);
        let growth = rejecting.observe_nvme_health(NvmeHealthSample {
            media_errors: 5,
            ..healthy
        });
        assert!(growth.media_errors_increased);
        assert!(growth.critical);
        assert!(rejecting.should_reject_put());

        let latched = rejecting.observe_nvme_health(NvmeHealthSample {
            media_errors: 5,
            ..healthy
        });
        assert!(!latched.media_errors_increased);
        assert!(latched.media_error_growth_observed);
        assert!(rejecting.nvme_health().unwrap().critical);
    }
}
