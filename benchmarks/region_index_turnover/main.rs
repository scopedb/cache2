// Copyright 2026 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::io;

use cache2::benchmarking::{
    RegionIndexTurnoverConfig, RegionIndexTurnoverPhase, run_region_index_turnover,
};

fn main() -> io::Result<()> {
    let defaults = RegionIndexTurnoverConfig::default();
    let config = RegionIndexTurnoverConfig {
        region_count: env_usize("CACHE_INDEX_TURNOVER_REGIONS", defaults.region_count)?,
        entries_per_region: env_usize(
            "CACHE_INDEX_TURNOVER_ENTRIES_PER_REGION",
            defaults.entries_per_region,
        )?,
        turns: env_usize("CACHE_INDEX_TURNOVER_TURNS", defaults.turns)?,
        sample_operations: env_usize(
            "CACHE_INDEX_TURNOVER_SAMPLE_OPS",
            defaults.sample_operations,
        )?,
        key_space_multiplier: env_usize(
            "CACHE_INDEX_TURNOVER_KEY_MULTIPLIER",
            defaults.key_space_multiplier,
        )?,
    };
    let report = run_region_index_turnover(config)?;

    println!("C² RegionIndex turnover benchmark");
    println!(
        "regions={} entries_per_region={} physical_entries={} key_space={} index_slots={} load={:.1}% partitions={} partition_slots={}..={} turns={} sample_ops={}",
        report.config.region_count,
        report.config.entries_per_region,
        report.physical_entries,
        report.key_space_entries,
        report.index_slots,
        report.physical_entries as f64 * 100.0 / report.index_slots as f64,
        report.partition_count,
        report.minimum_partition_slots,
        report.maximum_partition_slots,
        report.config.turns,
        report.config.sample_operations,
    );
    println!(
        "production_projection capacity=4TiB average_entry=16KiB partitions=4096 physical_entries_per_partition=65536 index_slots_per_partition=131072"
    );

    for checkpoint in report.checkpoints {
        println!(
            "checkpoint turn={} logical_live={} physical_values={} empty={} relocations={} overflow_evictions={} conditional_remove_misses={}",
            checkpoint.turn,
            checkpoint.logical_live_keys,
            checkpoint.index.physical_value_slots,
            checkpoint.index.empty_slots,
            checkpoint.index.relocations,
            checkpoint.index.overflow_evictions,
            checkpoint.index.conditional_remove_misses,
        );
        report_phase(checkpoint.turn, "publish", checkpoint.publish);
        report_phase(checkpoint.turn, "recent_lookup", checkpoint.recent_lookup);
        if let Some(stale) = checkpoint.stale_lookup {
            report_phase(checkpoint.turn, "stale_lookup", stale);
        }
        report_phase(checkpoint.turn, "missing_lookup", checkpoint.missing_lookup);
    }
    Ok(())
}

fn report_phase(turn: usize, phase: &str, measurement: RegionIndexTurnoverPhase) {
    let seconds = measurement.elapsed.as_secs_f64();
    let operations_per_second = measurement.operations as f64 / seconds;
    let probes_per_operation = measurement.probes as f64 / measurement.operations as f64;
    let full_window_percent =
        measurement.full_windows as f64 * 100.0 / measurement.operations as f64;
    println!(
        "{phase:<16} turn={turn:<4} {:>9.3} ms {:>12.0} ops/s probes/op={probes_per_operation:>5.2} max_probe={:>2} full={full_window_percent:>6.2}% hits={} misses={} stale_slots={} checksum={:016x}",
        seconds * 1_000.0,
        operations_per_second,
        measurement.max_probes,
        measurement.hits,
        measurement.misses,
        measurement.stale_slots,
        measurement.checksum,
    );
    println!(
        "result phase=index_{phase} turn={turn} elapsed_ns={} operations={} ops_per_sec={operations_per_second:.3} hits={} misses={} probes={} probes_per_op={probes_per_operation:.3} max_probe={} full_windows={} full_window_percent={full_window_percent:.3} stale_slots={} checksum={:016x}",
        measurement.elapsed.as_nanos(),
        measurement.operations,
        measurement.hits,
        measurement.misses,
        measurement.probes,
        measurement.max_probes,
        measurement.full_windows,
        measurement.stale_slots,
        measurement.checksum,
    );
}

fn env_usize(name: &str, default: usize) -> io::Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| invalid(format!("{name} must be an unsigned integer"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
