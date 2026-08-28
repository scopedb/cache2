// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

//! Internal benchmark entry points. This module is available only with the
//! `benchmarking` feature and is not part of the supported cache API.

use std::hint::black_box;
use std::io;
use std::time::{Duration, Instant};

use crate::index::{
    IndexEntry, MAX_INDEX_PROBES, MAX_INDEX_SLOTS, MAX_PACKED_REGION_COUNT, MAX_REGION_OFFSET,
    PackedLocation,
};
use crate::index_storage::PartitionedIndexStorage;
use crate::record_codec::hash_key;
use crate::region_index::{
    BenchmarkProbeStats, RegionIndex, reset_benchmark_probe_stats, take_benchmark_probe_stats,
};
use crate::snapshot::CacheIndexSnapshot;

const BENCHMARK_HASH_SEED: u64 = 0x6a09_e667_f3bc_c909;
const ENTRY_HASH_DOMAIN: u64 = 0x656e_7472_792d_6b65;
const MISSING_HASH_DOMAIN: u64 = 0x6d69_7373_696e_672d;
const BENCHMARK_RECORD_BYTES: u32 = 16 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct RegionIndexTurnoverConfig {
    pub region_count: usize,
    pub entries_per_region: usize,
    pub turns: usize,
    pub sample_operations: usize,
    pub key_space_multiplier: usize,
}

impl Default for RegionIndexTurnoverConfig {
    fn default() -> Self {
        Self {
            // One 4 TiB / 16 KiB production partition contains 65,536 live
            // entries in 131,072 physical slots at the default 50% load.
            region_count: 128,
            entries_per_region: 512,
            turns: 100,
            sample_operations: 262_144,
            key_space_multiplier: 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RegionIndexTurnoverPhase {
    pub elapsed: Duration,
    pub operations: usize,
    pub hits: usize,
    pub misses: usize,
    pub checksum: u64,
    pub probes: u64,
    pub stale_slots: u64,
    pub full_windows: u64,
    pub max_probes: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct RegionIndexTurnoverCheckpoint {
    pub turn: usize,
    pub logical_live_keys: usize,
    pub index: CacheIndexSnapshot,
    pub publish: RegionIndexTurnoverPhase,
    pub recent_lookup: RegionIndexTurnoverPhase,
    pub stale_lookup: Option<RegionIndexTurnoverPhase>,
    pub missing_lookup: RegionIndexTurnoverPhase,
}

#[derive(Debug)]
pub struct RegionIndexTurnoverReport {
    pub config: RegionIndexTurnoverConfig,
    pub physical_entries: usize,
    pub key_space_entries: usize,
    pub index_slots: usize,
    pub partition_count: usize,
    pub minimum_partition_slots: usize,
    pub maximum_partition_slots: usize,
    pub checkpoints: Vec<RegionIndexTurnoverCheckpoint>,
}

pub fn run_region_index_turnover(
    config: RegionIndexTurnoverConfig,
) -> io::Result<RegionIndexTurnoverReport> {
    let plan = TurnoverPlan::new(config)?;
    let mut workload = TurnoverWorkload::new(plan)?;
    let mut checkpoints = Vec::new();
    checkpoints.try_reserve_exact(6).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "cannot allocate index-turnover checkpoints",
        )
    })?;

    let initial_fill = workload.write_turn()?;
    checkpoints.push(workload.checkpoint(0, initial_fill)?);
    for turn in 1..=config.turns {
        let publish = workload.write_turn()?;
        if is_checkpoint(turn, config.turns) {
            checkpoints.push(workload.checkpoint(turn, publish)?);
        }
    }

    let minimum_partition_slots = workload
        .index
        .storage()
        .partition_ranges()
        .iter()
        .map(|range| range.slot_count)
        .min()
        .ok_or_else(|| io::Error::other("turnover index has no partitions"))?;
    let maximum_partition_slots = workload
        .index
        .storage()
        .partition_ranges()
        .iter()
        .map(|range| range.slot_count)
        .max()
        .ok_or_else(|| io::Error::other("turnover index has no partitions"))?;
    Ok(RegionIndexTurnoverReport {
        config,
        physical_entries: plan.physical_entries,
        key_space_entries: plan.key_space_entries,
        index_slots: plan.index_slots,
        partition_count: workload.index.storage().partition_count(),
        minimum_partition_slots,
        maximum_partition_slots,
        checkpoints,
    })
}

#[derive(Clone, Copy)]
struct TurnoverPlan {
    config: RegionIndexTurnoverConfig,
    physical_entries: usize,
    key_space_entries: usize,
    index_slots: usize,
}

impl TurnoverPlan {
    fn new(config: RegionIndexTurnoverConfig) -> io::Result<Self> {
        if config.region_count == 0
            || config.entries_per_region == 0
            || config.turns == 0
            || config.sample_operations == 0
        {
            return Err(invalid("turnover counts must be positive"));
        }
        if config.key_space_multiplier < 2 {
            return Err(invalid(
                "index turnover requires a key-space multiplier of at least two",
            ));
        }
        if config.region_count > MAX_PACKED_REGION_COUNT as usize {
            return Err(invalid("turnover Region count exceeds packed locations"));
        }
        let final_offset = config
            .entries_per_region
            .saturating_sub(1)
            .checked_mul(BENCHMARK_RECORD_BYTES as usize)
            .ok_or_else(|| invalid("turnover Region offset overflow"))?;
        if final_offset > MAX_REGION_OFFSET as usize {
            return Err(invalid(
                "turnover entries do not fit one 32 MiB Region at 16 KiB each",
            ));
        }
        let physical_entries = config
            .region_count
            .checked_mul(config.entries_per_region)
            .ok_or_else(|| invalid("turnover physical entry count overflow"))?;
        let index_slots = physical_entries
            .checked_mul(2)
            .ok_or_else(|| invalid("turnover index slot count overflow"))?
            .max(8);
        if index_slots > MAX_INDEX_SLOTS {
            return Err(invalid("turnover index exceeds the supported slot limit"));
        }
        let key_space_entries = physical_entries
            .checked_mul(config.key_space_multiplier)
            .ok_or_else(|| invalid("turnover key-space size overflow"))?;
        let total_writes = config
            .turns
            .checked_add(1)
            .and_then(|turns| turns.checked_mul(physical_entries))
            .ok_or_else(|| invalid("turnover write count overflow"))?;
        if u64::try_from(total_writes).is_err() || u64::try_from(key_space_entries).is_err() {
            return Err(invalid("turnover sequence space exceeds u64"));
        }
        Ok(Self {
            config,
            physical_entries,
            key_space_entries,
            index_slots,
        })
    }
}

struct TurnoverWorkload {
    plan: TurnoverPlan,
    index: RegionIndex,
    hashes: Vec<u64>,
    missing_hashes: Vec<u64>,
    locations: Vec<PackedLocation>,
    location_owners: Vec<Option<usize>>,
    expected_entries: Vec<Option<IndexEntry>>,
    total_writes: usize,
}

impl TurnoverWorkload {
    fn new(plan: TurnoverPlan) -> io::Result<Self> {
        let storage = PartitionedIndexStorage::anonymous_single_partition(plan.index_slots)
            .map_err(index_error)?;
        let index = RegionIndex::from_storage(storage).map_err(index_error)?;
        index.set_statistics_enabled(true);

        let mut hashes = Vec::new();
        hashes
            .try_reserve_exact(plan.key_space_entries)
            .map_err(|_| out_of_memory("turnover key hashes"))?;
        for ordinal in 0..plan.key_space_entries {
            hashes.push(benchmark_hash(ENTRY_HASH_DOMAIN, ordinal)?);
        }

        let mut missing_hashes = Vec::new();
        missing_hashes
            .try_reserve_exact(plan.config.sample_operations)
            .map_err(|_| out_of_memory("turnover missing-key hashes"))?;
        for ordinal in 0..plan.config.sample_operations {
            missing_hashes.push(benchmark_hash(MISSING_HASH_DOMAIN, ordinal)?);
        }

        let mut locations = Vec::new();
        locations
            .try_reserve_exact(plan.physical_entries)
            .map_err(|_| out_of_memory("turnover packed locations"))?;
        for region_id in 0..plan.config.region_count {
            for slot in 0..plan.config.entries_per_region {
                let offset = slot
                    .checked_mul(BENCHMARK_RECORD_BYTES as usize)
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or_else(|| invalid("turnover Region offset overflow"))?;
                locations.push(
                    PackedLocation::new(region_id as u32, offset, BENCHMARK_RECORD_BYTES)
                        .map_err(|error| invalid(format!("invalid turnover location: {error}")))?,
                );
            }
        }

        let mut expected_entries = Vec::new();
        expected_entries
            .try_reserve_exact(plan.key_space_entries)
            .map_err(|_| out_of_memory("turnover correctness oracle"))?;
        expected_entries.resize(plan.key_space_entries, None);

        let mut location_owners = Vec::new();
        location_owners
            .try_reserve_exact(plan.physical_entries)
            .map_err(|_| out_of_memory("turnover Region owners"))?;
        location_owners.resize(plan.physical_entries, None);

        Ok(Self {
            plan,
            index,
            hashes,
            missing_hashes,
            locations,
            location_owners,
            expected_entries,
            total_writes: 0,
        })
    }

    fn write_turn(&mut self) -> io::Result<RegionIndexTurnoverPhase> {
        reset_benchmark_probe_stats();
        let started = Instant::now();
        let mut checksum = 0_u64;
        let mut installed = 0_usize;
        for region_id in 0..self.plan.config.region_count {
            let location_start = region_id * self.plan.config.entries_per_region;
            for physical in location_start..location_start + self.plan.config.entries_per_region {
                let Some(key_ordinal) = self.location_owners[physical].take() else {
                    continue;
                };
                let old = IndexEntry {
                    location: self.locations[physical],
                };
                self.index
                    .remove_if_match(self.hashes[key_ordinal], old.location)
                    .map_err(index_error)?;
                if self.expected_entries[key_ordinal] == Some(old) {
                    self.expected_entries[key_ordinal] = None;
                }
            }
            for slot in 0..self.plan.config.entries_per_region {
                let key_ordinal = self.total_writes % self.plan.key_space_entries;
                let entry = IndexEntry {
                    location: self.locations[location_start + slot],
                };
                let accepted = self
                    .index
                    .upsert(black_box(self.hashes[key_ordinal]), black_box(entry))
                    .map_err(index_error)?;
                if !accepted {
                    return Err(io::Error::other("turnover index publication was rejected"));
                }
                self.expected_entries[key_ordinal] = Some(entry);
                self.location_owners[location_start + slot] = Some(key_ordinal);
                self.total_writes = self
                    .total_writes
                    .checked_add(1)
                    .ok_or_else(|| invalid("turnover write ordinal overflow"))?;
                installed = installed.saturating_add(1);
                checksum =
                    checksum.wrapping_add(entry.location.raw().rotate_left((slot % 64) as u32));
            }
        }
        finish_phase(
            started.elapsed(),
            self.plan.physical_entries,
            installed,
            self.plan.physical_entries.saturating_sub(installed),
            checksum,
        )
    }

    fn checkpoint(
        &self,
        turn: usize,
        publish: RegionIndexTurnoverPhase,
    ) -> io::Result<RegionIndexTurnoverCheckpoint> {
        let recent_start = self
            .total_writes
            .checked_sub(self.plan.physical_entries)
            .ok_or_else(|| io::Error::other("turnover recent window underflow"))?;
        let recent_lookup = measure_lookups(
            &self.index,
            self.plan.config.sample_operations,
            |operation| {
                let key_ordinal = (recent_start + operation % self.plan.physical_entries)
                    % self.plan.key_space_entries;
                let expected = self.expected_entries[key_ordinal]
                    .ok_or_else(|| io::Error::other("recent turnover key has no oracle entry"))?;
                Ok((self.hashes[key_ordinal], LookupExpectation::Live(expected)))
            },
        )?;

        let stale_lookup = if self.total_writes >= 2 * self.plan.physical_entries {
            let stale_start = self.total_writes - 2 * self.plan.physical_entries;
            Some(measure_lookups(
                &self.index,
                self.plan.config.sample_operations,
                |operation| {
                    let key_ordinal = (stale_start + operation % self.plan.physical_entries)
                        % self.plan.key_space_entries;
                    Ok(match self.expected_entries[key_ordinal] {
                        Some(expected) => {
                            (self.hashes[key_ordinal], LookupExpectation::Live(expected))
                        }
                        None => (self.hashes[key_ordinal], LookupExpectation::Miss),
                    })
                },
            )?)
        } else {
            None
        };

        let missing_lookup = measure_lookups(
            &self.index,
            self.plan.config.sample_operations,
            |operation| Ok((self.missing_hashes[operation], LookupExpectation::Miss)),
        )?;

        Ok(RegionIndexTurnoverCheckpoint {
            turn,
            logical_live_keys: self.expected_entries.iter().flatten().count(),
            index: self.index.snapshot().map_err(index_error)?,
            publish,
            recent_lookup,
            stale_lookup,
            missing_lookup,
        })
    }
}

#[derive(Clone, Copy)]
enum LookupExpectation {
    Live(IndexEntry),
    Miss,
}

fn measure_lookups(
    index: &RegionIndex,
    operations: usize,
    mut request: impl FnMut(usize) -> io::Result<(u64, LookupExpectation)>,
) -> io::Result<RegionIndexTurnoverPhase> {
    reset_benchmark_probe_stats();
    let started = Instant::now();
    let mut hits = 0_usize;
    let mut misses = 0_usize;
    let mut false_candidates = 0_u64;
    let mut checksum = 0_u64;
    for operation in 0..operations {
        let (hash, expected) = request(operation)?;
        let observed = index.lookup_raw(black_box(hash)).map_err(index_error)?;
        match (expected, observed) {
            (LookupExpectation::Live(expected), Some(observed)) if observed == expected => {
                hits = hits.saturating_add(1);
                checksum = checksum
                    .wrapping_add(observed.location.raw().rotate_left((operation % 64) as u32));
            }
            (LookupExpectation::Live(_), None) | (LookupExpectation::Miss, None) => {
                misses = misses.saturating_add(1);
            }
            (LookupExpectation::Live(_), Some(_)) | (LookupExpectation::Miss, Some(_)) => {
                // The compact index stores a fingerprint rather than the full
                // hash. A false candidate is a production miss after the one
                // record read validates generation, hash, and full key.
                misses = misses.saturating_add(1);
                false_candidates = false_candidates.saturating_add(1);
            }
        }
        black_box(observed);
    }
    let mut phase = finish_phase(started.elapsed(), operations, hits, misses, checksum)?;
    phase.stale_slots = false_candidates;
    Ok(phase)
}

fn finish_phase(
    elapsed: Duration,
    operations: usize,
    hits: usize,
    misses: usize,
    checksum: u64,
) -> io::Result<RegionIndexTurnoverPhase> {
    let probes = take_benchmark_probe_stats();
    if probes.operations != u64::try_from(operations).unwrap_or(u64::MAX) {
        return Err(io::Error::other(
            "turnover operation and probe counts disagree",
        ));
    }
    if probes.max_probes > MAX_INDEX_PROBES {
        return Err(io::Error::other(
            "turnover probe exceeded the production bound",
        ));
    }
    Ok(phase_from_probe_stats(
        elapsed, operations, hits, misses, checksum, probes,
    ))
}

fn phase_from_probe_stats(
    elapsed: Duration,
    operations: usize,
    hits: usize,
    misses: usize,
    checksum: u64,
    probes: BenchmarkProbeStats,
) -> RegionIndexTurnoverPhase {
    RegionIndexTurnoverPhase {
        elapsed,
        operations,
        hits,
        misses,
        checksum,
        probes: probes.probes,
        stale_slots: probes.stale_slots,
        full_windows: probes.full_windows,
        max_probes: probes.max_probes,
    }
}

fn benchmark_hash(domain: u64, ordinal: usize) -> io::Result<u64> {
    let ordinal =
        u64::try_from(ordinal).map_err(|_| invalid("turnover key ordinal exceeds u64"))?;
    let mut key = [0_u8; 16];
    key[..8].copy_from_slice(&domain.to_le_bytes());
    key[8..].copy_from_slice(&ordinal.to_le_bytes());
    Ok(hash_key(BENCHMARK_HASH_SEED, &key))
}

fn is_checkpoint(turn: usize, final_turn: usize) -> bool {
    matches!(turn, 1 | 10 | 100 | 500) || turn == final_turn
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn out_of_memory(target: &'static str) -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        format!("cannot allocate {target}"),
    )
}

fn index_error(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accelerated_turnover_stays_bounded_and_classifies_false_candidates_as_misses() {
        let report = run_region_index_turnover(RegionIndexTurnoverConfig {
            region_count: 8,
            entries_per_region: 8,
            turns: 2,
            sample_operations: 1024,
            key_space_multiplier: 4,
        })
        .unwrap();

        assert_eq!(
            report
                .checkpoints
                .iter()
                .map(|checkpoint| checkpoint.turn)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(report.partition_count, 1);
        assert_eq!(report.minimum_partition_slots, report.index_slots);
        assert_eq!(report.maximum_partition_slots, report.index_slots);
        for checkpoint in &report.checkpoints {
            assert_eq!(checkpoint.logical_live_keys, report.physical_entries);
            assert!(checkpoint.index.physical_value_slots <= report.index_slots as u64);
            for phase in [
                Some(checkpoint.publish),
                Some(checkpoint.recent_lookup),
                checkpoint.stale_lookup,
                Some(checkpoint.missing_lookup),
            ]
            .into_iter()
            .flatten()
            {
                assert!(phase.max_probes <= MAX_INDEX_PROBES);
                assert_eq!(phase.hits + phase.misses, phase.operations);
            }
            if let Some(stale) = checkpoint.stale_lookup {
                assert_eq!(stale.hits, 0);
                assert_eq!(stale.misses, stale.operations);
            }
            assert_eq!(checkpoint.missing_lookup.hits, 0);
            assert_eq!(
                checkpoint.missing_lookup.misses,
                checkpoint.missing_lookup.operations
            );
        }
    }
}
