//! Fixed-cardinality metrics for the complete Hybrid cache coordinator.

use std::fmt;

use crate::{
    CacheErrorClass, CacheStatus, HybridCache, HybridCacheStats, HybridHealthSnapshot,
    HybridMetricsSnapshot, LATENCY_BUCKET_COUNT, LATENCY_BUCKET_UPPER_US, RequestResultClass,
};

impl HybridCache {
    /// Write a bounded OpenMetrics snapshot for Memory, Bucket, Region, and
    /// global Hybrid policy state. The caller owns transport/export lifetime.
    pub fn write_openmetrics(&self, output: &mut impl fmt::Write) -> fmt::Result {
        self.metrics_snapshot().write_openmetrics(output)
    }

    /// Materialize [`HybridCache::write_openmetrics`] into an owned string.
    pub fn openmetrics_snapshot(&self) -> String {
        let mut output = String::with_capacity(8 * 1024);
        self.write_openmetrics(&mut output)
            .expect("writing OpenMetrics into a String cannot fail");
        output
    }
}

impl HybridCacheStats {
    /// Write counters and gauges that remain useful after `close`.
    pub fn write_openmetrics(&self, output: &mut impl fmt::Write) -> fmt::Result {
        self.write_openmetrics_with_health(
            output,
            HybridHealthSnapshot {
                overall: CacheStatus::Closed,
                manifest: CacheStatus::Closed,
                bucket: CacheStatus::Closed,
                region: CacheStatus::Closed,
                memory_reads_available: false,
                mutations_available: false,
            },
        )?;
        output.write_str("# EOF\n")
    }

    /// Materialize the final stats snapshot as OpenMetrics.
    pub fn to_openmetrics(&self) -> String {
        let mut output = String::with_capacity(8 * 1024);
        self.write_openmetrics(&mut output)
            .expect("writing OpenMetrics into a String cannot fail");
        output
    }

    fn write_openmetrics_with_health(
        &self,
        output: &mut impl fmt::Write,
        health: HybridHealthSnapshot,
    ) -> fmt::Result {
        output.write_str("# TYPE cache_rs_hybrid_status gauge\n")?;
        for (component, status) in [
            ("overall", health.overall),
            ("manifest", health.manifest),
            ("bucket", health.bucket),
            ("region", health.region),
        ] {
            for (name, state) in [
                ("healthy", CacheStatus::Healthy),
                ("miss_only", CacheStatus::MissOnly),
                ("poisoned", CacheStatus::Poisoned),
                ("closed", CacheStatus::Closed),
            ] {
                writeln!(
                    output,
                    "cache_rs_hybrid_status{{component=\"{component}\",state=\"{name}\"}} {}",
                    u8::from(status == state)
                )?;
            }
        }
        metric(
            output,
            "memory_reads_available",
            u64::from(health.memory_reads_available),
        )?;
        metric(
            output,
            "mutations_available",
            u64::from(health.mutations_available),
        )?;
        metric(output, "open_elapsed_us", self.open.open_elapsed_us)?;
        metric(
            output,
            "policy_restore_elapsed_us",
            self.open.policy_restore_elapsed_us,
        )?;
        metric(
            output,
            "policy_restored_from_checkpoint",
            u64::from(self.open.policy_restored_from_checkpoint),
        )?;
        metric(
            output,
            "bucket_usage_scanned_on_open",
            u64::from(self.open.bucket_usage_scanned),
        )?;
        metric(
            output,
            "region_usage_scanned_on_open",
            u64::from(self.open.region_usage_scanned),
        )?;
        metric(
            output,
            "dirty_manifest_recovered_on_open",
            u64::from(self.open.dirty_manifest_recovered),
        )?;
        metric(output, "memory_capacity_bytes", self.memory_capacity_bytes)?;
        metric(output, "memory_charged_bytes", self.memory_charged_bytes)?;
        metric(output, "memory_entries", self.memory_entries)?;
        metric(output, "memory_dirty_entries", self.memory_dirty_entries)?;
        metric(output, "memory_dirty_bytes", self.memory_dirty_bytes)?;
        output.write_str("# TYPE cache_rs_hybrid_hits_total counter\n")?;
        writeln!(
            output,
            "cache_rs_hybrid_hits_total{{tier=\"memory\"}} {}",
            self.memory_hits
        )?;
        writeln!(
            output,
            "cache_rs_hybrid_hits_total{{tier=\"bucket\"}} {}",
            self.small_disk_hits
        )?;
        writeln!(
            output,
            "cache_rs_hybrid_hits_total{{tier=\"region\"}} {}",
            self.region_disk_hits
        )?;
        counter(output, "misses", self.misses)?;
        counter(output, "puts", self.puts)?;
        counter(output, "removes", self.removes)?;
        counter(output, "promotions", self.promotions)?;
        counter(output, "promotion_skips", self.promotion_skips)?;
        counter(output, "memory_evictions", self.memory_evictions)?;
        metric(
            output,
            "journal_capacity_bytes",
            self.journal_capacity_bytes,
        )?;
        metric(output, "journal_used_bytes", self.journal_used_bytes)?;
        counter(output, "journal_rollovers", self.journal_rollovers)?;
        counter(
            output,
            "journal_rollover_wait_ns",
            self.journal_rollover_wait_ns,
        )?;
        metric(
            output,
            "journal_rollover_max_ns",
            self.journal_rollover_max_ns,
        )?;
        metric(output, "requests_in_flight", self.requests_in_flight)?;
        metric(
            output,
            "requests_in_flight_peak",
            self.requests_in_flight_peak,
        )?;
        metric(output, "request_bytes_in_use", self.request_bytes_in_use)?;
        metric(output, "request_bytes_peak", self.request_bytes_peak)?;
        counter(output, "request_rejections", self.request_rejections)?;
        counter(output, "request_wait_ns", self.request_wait_ns)?;

        metric(
            output,
            "journal_group_queue_capacity",
            self.journal_group_commit.queue_capacity,
        )?;
        metric(
            output,
            "journal_group_in_flight",
            self.journal_group_commit.in_flight,
        )?;
        metric(
            output,
            "journal_group_in_flight_peak",
            self.journal_group_commit.in_flight_peak,
        )?;
        metric(
            output,
            "journal_group_memory_capacity_bytes",
            self.journal_group_commit.memory_capacity_bytes,
        )?;
        metric(
            output,
            "journal_group_fixed_memory_bytes",
            self.journal_group_commit.fixed_memory_bytes,
        )?;
        metric(
            output,
            "journal_group_memory_in_use_bytes",
            self.journal_group_commit.memory_in_use_bytes,
        )?;
        metric(
            output,
            "journal_group_memory_peak_bytes",
            self.journal_group_commit.memory_peak_bytes,
        )?;
        counter(
            output,
            "journal_group_committed_batches",
            self.journal_group_commit.committed_batches,
        )?;
        counter(
            output,
            "journal_group_committed_records",
            self.journal_group_commit.committed_records,
        )?;
        counter(
            output,
            "journal_group_durability_syncs",
            self.journal_group_commit.durability_syncs,
        )?;
        counter(
            output,
            "journal_group_sync_elapsed_ns_total",
            self.journal_group_commit.sync_elapsed_ns_total,
        )?;
        metric(
            output,
            "journal_group_sync_elapsed_ns_max",
            self.journal_group_commit.sync_elapsed_ns_max,
        )?;
        counter(
            output,
            "journal_group_rejections",
            self.journal_group_commit.rejected,
        )?;
        counter(
            output,
            "journal_group_worker_panics",
            self.journal_group_commit.worker_panics,
        )?;
        metric(
            output,
            "journal_group_accepting",
            u64::from(self.journal_group_commit.accepting),
        )?;

        metric(
            output,
            "write_back_enabled",
            u64::from(self.write_back.enabled),
        )?;
        counter(
            output,
            "write_back_memory_only_puts",
            self.write_back.memory_only_puts,
        )?;
        counter(
            output,
            "write_back_write_through_fallbacks",
            self.write_back.write_through_fallbacks,
        )?;
        counter(
            output,
            "write_back_demotion_attempts",
            self.write_back.demotion_attempts,
        )?;
        counter(
            output,
            "write_back_demotion_failures",
            self.write_back.demotion_failures,
        )?;
        counter(
            output,
            "write_back_demoted_entries",
            self.write_back.demoted_entries,
        )?;
        counter(
            output,
            "write_back_demoted_bytes",
            self.write_back.demoted_bytes,
        )?;
        counter(
            output,
            "write_back_dirty_expiry_fences",
            self.write_back.dirty_expiry_fences,
        )?;
        counter(
            output,
            "write_back_proactive_scheduled",
            self.write_back.proactive_scheduled,
        )?;
        counter(
            output,
            "write_back_proactive_skipped",
            self.write_back.proactive_skipped,
        )?;
        counter(
            output,
            "write_back_proactive_persisted",
            self.write_back.proactive_persisted,
        )?;
        counter(
            output,
            "write_back_proactive_rejected",
            self.write_back.proactive_rejected,
        )?;
        counter(
            output,
            "write_back_proactive_fatal",
            self.write_back.proactive_fatal,
        )?;
        metric(
            output,
            "write_back_queue_capacity",
            self.write_back.queue_capacity,
        )?;
        metric(
            output,
            "write_back_queue_in_flight",
            self.write_back.queue_in_flight,
        )?;
        metric(
            output,
            "write_back_queue_in_flight_peak",
            self.write_back.queue_in_flight_peak,
        )?;
        metric(
            output,
            "write_back_memory_capacity_bytes",
            self.write_back.memory_capacity_bytes,
        )?;
        metric(
            output,
            "write_back_memory_in_use_bytes",
            self.write_back.memory_in_use_bytes,
        )?;
        metric(
            output,
            "write_back_memory_peak_bytes",
            self.write_back.memory_peak_bytes,
        )?;
        counter(
            output,
            "write_back_queue_submitted",
            self.write_back.queue_submitted,
        )?;
        counter(
            output,
            "write_back_queue_completed",
            self.write_back.queue_completed,
        )?;
        counter(
            output,
            "write_back_queue_rejections",
            self.write_back.queue_rejections,
        )?;
        counter(
            output,
            "write_back_worker_panics",
            self.write_back.worker_panics,
        )?;
        counter(
            output,
            "write_back_queue_wait_ns",
            self.write_back.queue_wait_ns,
        )?;

        counter(
            output,
            "admission_observations",
            self.admission.observations,
        )?;
        counter(output, "admission_admitted", self.admission.admitted)?;
        counter(output, "admission_rejected", self.admission.rejected)?;
        counter(
            output,
            "admission_large_object_rejected",
            self.admission.large_object_rejected,
        )?;

        counter(
            output,
            "host_write_operations",
            self.host_writes.host_write_operations,
        )?;
        counter(
            output,
            "host_write_bytes",
            self.host_writes.host_write_bytes,
        )?;
        output.write_str("# TYPE cache_rs_hybrid_host_write_category_bytes_total counter\n")?;
        for (kind, value) in [
            ("foreground", self.host_writes.foreground_record_bytes),
            ("reinsertion", self.host_writes.reinsertion_bytes),
            ("reclaimer", self.host_writes.reclaimer_bytes),
            ("forced_tombstone", self.host_writes.forced_tombstone_bytes),
            ("metadata", self.host_writes.metadata_bytes),
            ("checkpoint", self.host_writes.checkpoint_bytes),
        ] {
            writeln!(
                output,
                "cache_rs_hybrid_host_write_category_bytes_total{{kind=\"{kind}\"}} {value}"
            )?;
        }
        metric(
            output,
            "write_amplification_milli",
            self.host_writes.write_amplification_milli,
        )?;
        metric(
            output,
            "daily_host_write_bytes",
            self.host_writes.daily_host_write_bytes,
        )?;
        counter(
            output,
            "daily_budget_rejections",
            self.host_writes.daily_budget_rejections,
        )?;

        counter(output, "bucket_gets", self.bucket.gets)?;
        counter(output, "bucket_hits", self.bucket.hits)?;
        counter(output, "bucket_misses", self.bucket.misses)?;
        counter(output, "bucket_puts", self.bucket.puts)?;
        counter(output, "bucket_evictions", self.bucket.evictions)?;
        counter(output, "bucket_io_errors", self.bucket.io_errors)?;
        counter(
            output,
            "bucket_io_engine_errors",
            self.bucket.io_engine_errors,
        )?;
        counter(
            output,
            "bucket_corruption_errors",
            self.bucket.corruption_errors,
        )?;
        counter(output, "bucket_bytes_read", self.bucket.bytes_read)?;
        counter(output, "bucket_bytes_written", self.bucket.bytes_written)?;
        metric(
            output,
            "bucket_buffer_slots_in_use",
            self.bucket.page_buffers_in_use,
        )?;
        metric(
            output,
            "bucket_buffer_slots_peak",
            self.bucket.page_buffers_in_use_peak,
        )?;
        metric(output, "bucket_io_in_flight", self.bucket.io_in_flight)?;
        metric(
            output,
            "bucket_io_in_flight_peak",
            self.bucket.io_in_flight_peak,
        )?;
        counter(
            output,
            "bucket_page_buffer_rejections",
            self.bucket.page_buffer_rejections,
        )?;

        counter(output, "region_hits", self.region.hits)?;
        counter(output, "region_misses", self.region.misses)?;
        counter(output, "region_bytes_read", self.region.bytes_read)?;
        counter(output, "region_bytes_written", self.region.bytes_written)?;
        counter(output, "region_io_errors", self.region.io_errors)?;
        metric(output, "region_io_in_flight", self.region.io_in_flight)?;
        metric(
            output,
            "region_io_in_flight_peak",
            self.region.io_in_flight_peak,
        )?;
        metric(
            output,
            "region_recovery_in_progress",
            u64::from(self.region.recovery_in_progress),
        )?;
        counter(
            output,
            "region_checkpoint_errors",
            self.region.checkpoint_errors,
        )?;
        Ok(())
    }
}

impl HybridMetricsSnapshot {
    pub fn write_openmetrics(&self, output: &mut impl fmt::Write) -> fmt::Result {
        self.stats
            .write_openmetrics_with_health(output, self.health)?;
        output.write_str("# TYPE cache_rs_hybrid_requests_total counter\n")?;
        for operation in &self.operations {
            for class in RequestResultClass::ALL {
                writeln!(
                    output,
                    "cache_rs_hybrid_requests_total{{operation=\"{}\",result=\"{}\"}} {}",
                    operation.operation.as_str(),
                    class.as_str(),
                    operation.result_count(class),
                )?;
            }
        }
        output.write_str("# TYPE cache_rs_hybrid_request_errors_total counter\n")?;
        for operation in &self.operations {
            for class in CacheErrorClass::ALL {
                writeln!(
                    output,
                    "cache_rs_hybrid_request_errors_total{{operation=\"{}\",class=\"{}\"}} {}",
                    operation.operation.as_str(),
                    class.as_str(),
                    operation.error_count(class),
                )?;
            }
        }
        output.write_str("# TYPE cache_rs_hybrid_request_duration_seconds histogram\n")?;
        for operation in &self.operations {
            let mut cumulative = 0_u64;
            for (index, upper_us) in LATENCY_BUCKET_UPPER_US.iter().enumerate() {
                cumulative = cumulative.saturating_add(operation.latency.bucket_counts[index]);
                writeln!(
                    output,
                    "cache_rs_hybrid_request_duration_seconds_bucket{{operation=\"{}\",le=\"{}\"}} {cumulative}",
                    operation.operation.as_str(),
                    seconds_label(*upper_us),
                )?;
            }
            cumulative = cumulative
                .saturating_add(operation.latency.bucket_counts[LATENCY_BUCKET_COUNT - 1]);
            writeln!(
                output,
                "cache_rs_hybrid_request_duration_seconds_bucket{{operation=\"{}\",le=\"+Inf\"}} {cumulative}",
                operation.operation.as_str(),
            )?;
            writeln!(
                output,
                "cache_rs_hybrid_request_duration_seconds_sum{{operation=\"{}\"}} {:.6}",
                operation.operation.as_str(),
                operation.latency.sum_us as f64 / 1_000_000.0,
            )?;
            writeln!(
                output,
                "cache_rs_hybrid_request_duration_seconds_count{{operation=\"{}\"}} {}",
                operation.operation.as_str(),
                operation.latency.count,
            )?;
            writeln!(
                output,
                "cache_rs_hybrid_request_duration_seconds_max{{operation=\"{}\"}} {:.6}",
                operation.operation.as_str(),
                operation.latency.max_us as f64 / 1_000_000.0,
            )?;
        }

        let queue = self.async_queue.unwrap_or_default();
        metric(
            output,
            "async_facade_active",
            u64::from(self.async_queue.is_some()),
        )?;
        for (name, value) in [
            ("async_read_queued", queue.read_queued),
            ("async_read_in_flight", queue.read_in_flight),
            ("async_read_reserved", queue.read_reserved),
            ("async_mutation_queued", queue.mutation_queued),
            ("async_mutation_in_flight", queue.mutation_in_flight),
            ("async_mutation_reserved", queue.mutation_reserved),
            ("async_read_queue_capacity", queue.read_queue_capacity),
            ("async_write_queue_capacity", queue.write_queue_capacity),
        ] {
            metric(output, name, value)?;
        }
        counter(output, "async_queue_rejections", queue.queue_rejections)?;
        let close = self.async_close.unwrap_or_default();
        metric(output, "close_draining", u64::from(close.draining))?;
        metric(output, "close_completed", u64::from(close.completed))?;
        metric(output, "close_succeeded", u64::from(close.succeeded))?;
        metric(output, "close_registered_waiters", close.registered_waiters)?;
        metric(
            output,
            "close_registered_waiters_peak",
            close.registered_waiters_peak,
        )?;
        counter(output, "close_waiter_rejections", close.waiter_rejections)?;
        counter(output, "close_timed_out_waits", close.timed_out_waits)?;
        metric(output, "close_drain_duration_ns", close.drain_duration_ns)?;
        metric(
            output,
            "state_transition_sequence",
            self.state_transitions
                .last()
                .map_or(0, |event| event.sequence),
        )?;
        output.write_str("# EOF\n")
    }

    pub fn to_openmetrics(&self) -> String {
        let mut output = String::with_capacity(16 * 1024);
        self.write_openmetrics(&mut output)
            .expect("writing OpenMetrics into a String cannot fail");
        output
    }

    pub fn write_state_log_json(&self, output: &mut impl fmt::Write) -> fmt::Result {
        for event in &self.state_transitions {
            writeln!(
                output,
                concat!(
                    "{{\"schema_version\":1,\"event\":\"hybrid_cache_state_change\",",
                    "\"sequence\":{},\"unix_ms\":{},\"from\":\"{}\",",
                    "\"to\":\"{}\",\"reason\":\"{}\"}}"
                ),
                event.sequence,
                event.unix_ms,
                status_name(event.from),
                status_name(event.to),
                event.reason.as_str(),
            )?;
        }
        Ok(())
    }
}

fn counter(output: &mut impl fmt::Write, name: &str, value: u64) -> fmt::Result {
    writeln!(output, "# TYPE cache_rs_hybrid_{name}_total counter")?;
    writeln!(output, "cache_rs_hybrid_{name}_total {value}")
}

fn metric(output: &mut impl fmt::Write, name: &str, value: u64) -> fmt::Result {
    writeln!(output, "# TYPE cache_rs_hybrid_{name} gauge")?;
    writeln!(output, "cache_rs_hybrid_{name} {value}")
}

fn seconds_label(microseconds: u64) -> String {
    if microseconds < 1_000_000 {
        format!("0.{microseconds:06}")
    } else {
        format!(
            "{}.{:06}",
            microseconds / 1_000_000,
            microseconds % 1_000_000
        )
    }
}

fn status_name(status: CacheStatus) -> &'static str {
    match status {
        CacheStatus::Healthy => "healthy",
        CacheStatus::MissOnly => "miss_only",
        CacheStatus::Poisoned => "poisoned",
        CacheStatus::Closed => "closed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_snapshot_has_fixed_tiers_and_openmetrics_terminator() {
        let stats = HybridCacheStats {
            memory_hits: 3,
            small_disk_hits: 4,
            region_disk_hits: 5,
            ..HybridCacheStats::default()
        };
        let output = stats.to_openmetrics();
        assert!(output.contains("cache_rs_hybrid_hits_total{tier=\"memory\"} 3"));
        assert!(output.contains("cache_rs_hybrid_hits_total{tier=\"bucket\"} 4"));
        assert!(output.contains("cache_rs_hybrid_hits_total{tier=\"region\"} 5"));
        assert!(output.ends_with("# EOF\n"));
    }
}
