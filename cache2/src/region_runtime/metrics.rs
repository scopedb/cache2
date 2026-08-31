// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::hashing::route_hash;
use crate::memory::MemoryMetricsSnapshot;
use crate::resources::ManagedMemorySnapshot;
use crate::snapshot::{CacheHealth, CacheIoSnapshot, CacheReclaimSnapshot, CacheSnapshot};

use super::{LIFECYCLE_DRAINING, LIFECYCLE_FAILED};

static NEXT_METRICS_EPOCH: AtomicU64 = AtomicU64::new(1);

pub(super) struct RuntimeMetrics {
    metrics_epoch: u64,
    pub(super) lifecycle: std::sync::atomic::AtomicU8,
    activity: Box<[ActivityMetrics]>,
    l2_read_overloads: AtomicU64,
    l2_read_wait_ns: AtomicU64,
    write_rejections: AtomicU64,
    pub(super) write_buffer_rejections: AtomicU64,
    pub(super) io_failures: AtomicU64,
    pub(super) region_rotations: AtomicU64,
    reclaimed_regions: AtomicU64,
    reclaimed_bytes: AtomicU64,
    reclaim_records_scanned: AtomicU64,
    reclaim_index_entries_removed: AtomicU64,
    reclaim_reinsert_records: AtomicU64,
    reclaim_reinsert_bytes: AtomicU64,
    reclaim_reinsert_skipped: AtomicU64,
    reclaim_reinsert_budget_skipped: AtomicU64,
}

#[repr(align(64))]
pub(super) struct ActivityMetrics {
    pub(super) puts: AtomicU64,
    pub(super) deletes: AtomicU64,
    pub(super) written_bytes: AtomicU64,
    pub(super) l1_hits: AtomicU64,
    pub(super) l1_misses: AtomicU64,
    pub(super) l2_hits: AtomicU64,
    pub(super) l2_misses: AtomicU64,
    pub(super) l2_read_memory_misses: AtomicU64,
    pub(super) l2_read_busy_misses: AtomicU64,
    pub(super) served_bytes: AtomicU64,
    pub(super) l1_promotions: AtomicU64,
}

impl ActivityMetrics {
    fn new() -> Self {
        Self {
            puts: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            written_bytes: AtomicU64::new(0),
            l1_hits: AtomicU64::new(0),
            l1_misses: AtomicU64::new(0),
            l2_hits: AtomicU64::new(0),
            l2_misses: AtomicU64::new(0),
            l2_read_memory_misses: AtomicU64::new(0),
            l2_read_busy_misses: AtomicU64::new(0),
            served_bytes: AtomicU64::new(0),
            l1_promotions: AtomicU64::new(0),
        }
    }
}

impl RuntimeMetrics {
    pub(super) fn new(shard_count: usize) -> io::Result<Self> {
        let mut activity = Vec::new();
        activity.try_reserve_exact(shard_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate shard activity counters",
            )
        })?;
        activity.resize_with(shard_count, ActivityMetrics::new);
        Ok(Self {
            metrics_epoch: NEXT_METRICS_EPOCH.fetch_add(1, Ordering::Relaxed),
            lifecycle: std::sync::atomic::AtomicU8::new(super::LIFECYCLE_RUNNING),
            activity: activity.into_boxed_slice(),
            l2_read_overloads: AtomicU64::new(0),
            l2_read_wait_ns: AtomicU64::new(0),
            write_rejections: AtomicU64::new(0),
            write_buffer_rejections: AtomicU64::new(0),
            io_failures: AtomicU64::new(0),
            region_rotations: AtomicU64::new(0),
            reclaimed_regions: AtomicU64::new(0),
            reclaimed_bytes: AtomicU64::new(0),
            reclaim_records_scanned: AtomicU64::new(0),
            reclaim_index_entries_removed: AtomicU64::new(0),
            reclaim_reinsert_records: AtomicU64::new(0),
            reclaim_reinsert_bytes: AtomicU64::new(0),
            reclaim_reinsert_skipped: AtomicU64::new(0),
            reclaim_reinsert_budget_skipped: AtomicU64::new(0),
        })
    }

    pub(super) fn activity(&self, shard_id: usize) -> &ActivityMetrics {
        &self.activity[shard_id]
    }

    pub(super) fn activity_for_hash(&self, hash: u64) -> &ActivityMetrics {
        self.activity(route_hash(hash, self.activity.len()))
    }

    pub(super) fn add(counter: &AtomicU64, value: usize) {
        let value = u64::try_from(value).unwrap_or(u64::MAX);
        counter.fetch_add(value, Ordering::Relaxed);
    }

    pub(super) fn increment(counter: &AtomicU64) {
        Self::add(counter, 1);
    }

    pub(super) fn record_write_rejection(&self) {
        Self::increment(&self.write_rejections);
    }

    pub(super) fn record_read_overload(&self) {
        Self::increment(&self.l2_read_overloads);
    }

    pub(super) fn record_read_wait(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.l2_read_wait_ns.fetch_add(nanos, Ordering::Relaxed);
    }

    pub(super) fn record_reclaim(&self, stats: crate::region::core::RegionReclaimStats) {
        Self::increment(&self.reclaimed_regions);
        self.reclaimed_bytes
            .fetch_add(stats.bytes_read, Ordering::Relaxed);
        self.reclaim_records_scanned
            .fetch_add(stats.records_scanned, Ordering::Relaxed);
        self.reclaim_index_entries_removed
            .fetch_add(stats.records_removed, Ordering::Relaxed);
        self.reclaim_reinsert_records
            .fetch_add(stats.reinsert_records, Ordering::Relaxed);
        self.reclaim_reinsert_bytes
            .fetch_add(stats.reinsert_bytes, Ordering::Relaxed);
        self.reclaim_reinsert_skipped
            .fetch_add(stats.reinsert_skipped, Ordering::Relaxed);
        self.reclaim_reinsert_budget_skipped
            .fetch_add(stats.reinsert_budget_skipped, Ordering::Relaxed);
    }

    pub(super) fn snapshot(
        &self,
        core_healthy: bool,
        statistics_enabled: bool,
        memory: ManagedMemorySnapshot,
        memory_metrics: MemoryMetricsSnapshot,
    ) -> CacheSnapshot {
        let lifecycle = self.lifecycle.load(Ordering::Acquire);
        let health = if lifecycle == LIFECYCLE_FAILED {
            CacheHealth::Failed
        } else if !core_healthy {
            CacheHealth::MissOnly
        } else if lifecycle == LIFECYCLE_DRAINING {
            CacheHealth::Draining
        } else {
            CacheHealth::Running
        };
        let mut puts = 0_u64;
        let mut deletes = 0_u64;
        let mut written_bytes = 0_u64;
        let mut l1_hits = 0_u64;
        let mut l1_misses = 0_u64;
        let mut l2_hits = 0_u64;
        let mut l2_misses = 0_u64;
        let mut l2_read_memory_misses = 0_u64;
        let mut l2_read_busy_misses = 0_u64;
        let mut served_bytes = 0_u64;
        let mut l1_promotions = 0_u64;
        for activity in &self.activity {
            puts = puts.saturating_add(activity.puts.load(Ordering::Relaxed));
            deletes = deletes.saturating_add(activity.deletes.load(Ordering::Relaxed));
            written_bytes =
                written_bytes.saturating_add(activity.written_bytes.load(Ordering::Relaxed));
            l1_hits = l1_hits.saturating_add(activity.l1_hits.load(Ordering::Relaxed));
            l1_misses = l1_misses.saturating_add(activity.l1_misses.load(Ordering::Relaxed));
            l2_hits = l2_hits.saturating_add(activity.l2_hits.load(Ordering::Relaxed));
            l2_misses = l2_misses.saturating_add(activity.l2_misses.load(Ordering::Relaxed));
            l2_read_memory_misses = l2_read_memory_misses
                .saturating_add(activity.l2_read_memory_misses.load(Ordering::Relaxed));
            l2_read_busy_misses = l2_read_busy_misses
                .saturating_add(activity.l2_read_busy_misses.load(Ordering::Relaxed));
            served_bytes =
                served_bytes.saturating_add(activity.served_bytes.load(Ordering::Relaxed));
            l1_promotions =
                l1_promotions.saturating_add(activity.l1_promotions.load(Ordering::Relaxed));
        }
        CacheSnapshot {
            metrics_epoch: self.metrics_epoch,
            health,
            statistics_enabled,
            puts,
            deletes,
            written_bytes,
            l1_hits,
            l1_misses,
            l2_hits,
            l2_misses,
            l2_read_memory_misses,
            l2_read_busy_misses,
            l2_read_overloads: self.l2_read_overloads.load(Ordering::Relaxed),
            l2_read_wait_ns: self.l2_read_wait_ns.load(Ordering::Relaxed),
            served_bytes,
            l1_promotions,
            l1_evictions: memory_metrics.evictions,
            l1_bypasses: memory_metrics.bypasses,
            write_rejections: self.write_rejections.load(Ordering::Relaxed),
            io_failures: self.io_failures.load(Ordering::Relaxed),
            region_rotations: self.region_rotations.load(Ordering::Relaxed),
            reclaim: CacheReclaimSnapshot {
                regions: self.reclaimed_regions.load(Ordering::Relaxed),
                bytes_read: self.reclaimed_bytes.load(Ordering::Relaxed),
                records_scanned: self.reclaim_records_scanned.load(Ordering::Relaxed),
                index_entries_removed: self.reclaim_index_entries_removed.load(Ordering::Relaxed),
                reinsert_records: self.reclaim_reinsert_records.load(Ordering::Relaxed),
                reinsert_bytes: self.reclaim_reinsert_bytes.load(Ordering::Relaxed),
                reinsert_skipped: self.reclaim_reinsert_skipped.load(Ordering::Relaxed),
                reinsert_budget_skipped: self
                    .reclaim_reinsert_budget_skipped
                    .load(Ordering::Relaxed),
            },
            managed_memory_bytes: memory.current_bytes,
            managed_memory_peak_bytes: memory.peak_bytes,
            managed_memory_limit_bytes: memory.limit_bytes,
            logical_disk_peak_bytes: 0,
            io: CacheIoSnapshot::default(),
        }
    }
}
