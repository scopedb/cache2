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

use std::io;

use crate::io_engine::MAX_IO_REQUESTS_PER_ENGINE;
use crate::memory::MemoryStore;
use crate::recovery::DataGeometry;
use crate::region_staging::RegionStaging;
use crate::resources::{CACHE_THREAD_STACK_BYTES, MAX_CONFIG_COUNT};
use crate::runtime_config::{
    MAX_APPEND_SHARDS, MAX_READ_IO_WAIT_TIMEOUT, MAX_WRITE_FLUSH_THRESHOLD_BYTES, RuntimeConfig,
};

use super::metrics::ActivityMetrics;

// Covers the bounded engine registry, command channel, and driver-side
// bookkeeping for one admitted I/O operation. Payload buffers are charged by
// ResourceController separately.
pub(super) const IO_QUEUE_ENTRY_RESERVATION_BYTES: usize = 512;
// Covers worker/shard controls and handles whose size does not scale with the
// payload or engine depth.
pub(super) const RUNTIME_CONTROL_RESERVATION_BYTES: usize = 4096;
// Keep the fixed L1 directory useful when the configured L2 has deliberate
// headroom. Smaller entries may still bypass before the byte budget fills;
// this avoids sizing metadata for the theoretical 64-byte minimum.
const PLANNED_MIN_L1_ENTRY_BYTES: usize = 4 * 1024;

impl RuntimeConfig {
    pub(crate) fn validate(&self) -> io::Result<()> {
        if !self.io_engine.is_available() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is unavailable on this build or platform",
            ));
        }
        if !self.io_mode.is_available() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "direct I/O is unavailable on this platform",
            ));
        }
        if self.append_shards == 0 || self.append_shards > MAX_APPEND_SHARDS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "append shards must be in 1..=256",
            ));
        }
        if self.reclaim_workers == 0 || self.reclaim_workers > self.append_shards as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "reclaim workers must be non-zero and no greater than append shards",
            ));
        }
        if self.read_io_workers == 0 || self.write_io_workers == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read and write I/O worker counts must be non-zero",
            ));
        }
        if self.read_io_wait_timeout > MAX_READ_IO_WAIT_TIMEOUT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read I/O wait timeout must not exceed five seconds",
            ));
        }
        if !(1..=MAX_CONFIG_COUNT).contains(&self.read_io_wait_capacity()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read I/O wait capacity must be in 1..=65536",
            ));
        }
        if self.io_engine == crate::runtime_config::IoEngine::Posix
            && (self.read_io_workers > MAX_IO_REQUESTS_PER_ENGINE
                || self.write_io_workers > MAX_IO_REQUESTS_PER_ENGINE)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "POSIX read and write I/O workers must each be in 1..={MAX_IO_REQUESTS_PER_ENGINE}"
                ),
            ));
        }
        if self.managed_memory_limit_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed memory limit must be non-zero",
            ));
        }
        if self.l1_capacity_bytes > self.managed_memory_limit_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "L1 capacity must not exceed the managed memory limit",
            ));
        }
        if self.l1_shards == 0 || self.l1_shards > MAX_CONFIG_COUNT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "L1 shards must be in 1..=65536",
            ));
        }
        if self.write_flush_threshold_bytes == 0
            || self.write_flush_threshold_bytes > MAX_WRITE_FLUSH_THRESHOLD_BYTES
            || !self
                .write_flush_threshold_bytes
                .is_multiple_of(crate::resources::BUFFER_ALIGNMENT)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write flush threshold must be 4 KiB aligned and within 4 KiB..=4 MiB",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_memory_plan(
        &self,
        geometry: DataGeometry,
        index_slots: usize,
        shard_count: usize,
    ) -> io::Result<()> {
        let l1_entry_capacity = self.l1_entry_capacity(geometry, index_slots)?;
        let l1_metadata_bytes = MemoryStore::allocation_bytes(
            self.l1_capacity_bytes,
            l1_entry_capacity,
            self.l1_shards,
            self.l1_eviction_policy,
        )?;
        let fixed_bytes =
            crate::region::core::runtime_fixed_memory_bytes(index_slots, geometry.region_count)?
                .checked_add(l1_metadata_bytes)
                .ok_or_else(|| invalid_runtime_config("fixed memory plan overflow"))?;
        self.validated_reserved_memory_bytes(geometry, shard_count, fixed_bytes)?;
        Ok(())
    }

    pub(super) fn l1_entry_capacity(
        &self,
        geometry: DataGeometry,
        index_slots: usize,
    ) -> io::Result<usize> {
        if self.l1_capacity_bytes == 0 {
            return Ok(0);
        }
        let l2_capacity = u128::from(geometry.region_size)
            .checked_mul(u128::from(geometry.region_count))
            .filter(|capacity| *capacity != 0)
            .ok_or_else(|| invalid_runtime_config("L2 capacity does not fit the L1 plan"))?;
        let expected_entries = index_slots.div_ceil(2).max(1);
        let proportional = (expected_entries as u128)
            .checked_mul(self.l1_capacity_bytes as u128)
            .and_then(|entries| entries.checked_add(l2_capacity - 1))
            .map(|entries| entries / l2_capacity)
            .and_then(|entries| usize::try_from(entries).ok())
            .ok_or_else(|| invalid_runtime_config("L1 entry capacity does not fit usize"))?;
        let four_kib_density = self.l1_capacity_bytes.div_ceil(PLANNED_MIN_L1_ENTRY_BYTES);
        let maximum = MemoryStore::maximum_entry_capacity(self.l1_capacity_bytes, self.l1_shards);
        let minimum = self.l1_shards.min(maximum);
        Ok(proportional
            .max(four_kib_density)
            .min(expected_entries)
            .max(minimum)
            .min(maximum))
    }

    pub(super) fn validated_reserved_memory_bytes(
        &self,
        geometry: DataGeometry,
        shard_count: usize,
        fixed_bytes: usize,
    ) -> io::Result<usize> {
        let (reserved_memory, minimum) =
            self.memory_plan_bytes(geometry, shard_count, fixed_bytes)?;
        if minimum > self.managed_memory_limit_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "managed memory limit cannot hold the fixed cache memory plan: requires {minimum} bytes, configured {} bytes",
                    self.managed_memory_limit_bytes
                ),
            ));
        }
        Ok(reserved_memory)
    }

    pub(super) fn memory_plan_bytes(
        &self,
        geometry: DataGeometry,
        shard_count: usize,
        fixed_bytes: usize,
    ) -> io::Result<(usize, usize)> {
        self.validate()?;
        let topology_bytes = runtime_topology_memory_bytes(shard_count, self)
            .ok_or_else(|| invalid_runtime_config("runtime topology memory plan overflow"))?;
        let usable_region = usize::try_from(geometry.region_size)
            .map_err(|_| invalid_runtime_config("Region size does not fit the memory plan"))?;
        let chunk_bytes = usable_region;
        let write_buffer_reservation =
            RegionStaging::reservation_bytes(shard_count, chunk_bytes)
                .ok_or_else(|| invalid_runtime_config("write buffer memory plan overflow"))?;
        let reserved_memory = fixed_bytes
            .checked_add(self.l1_capacity_bytes)
            .and_then(|bytes| bytes.checked_add(topology_bytes))
            .ok_or_else(|| invalid_runtime_config("reserved memory plan overflow"))?;
        let reclaim_buffers = usable_region
            .checked_mul(self.reclaim_workers)
            .ok_or_else(|| invalid_runtime_config("reclaim buffer memory plan overflow"))?;
        let minimum = reserved_memory
            .checked_add(write_buffer_reservation)
            // Every reclaimer permanently owns one Region-sized buffer. Keep
            // one additional maximum-size bounded read for the foreground.
            .and_then(|bytes| bytes.checked_add(reclaim_buffers))
            .and_then(|bytes| bytes.checked_add(usable_region))
            .ok_or_else(|| invalid_runtime_config("minimum memory plan overflow"))?;
        Ok((reserved_memory, minimum))
    }
}

pub(super) fn runtime_topology_memory_bytes(
    shard_count: usize,
    config: &RuntimeConfig,
) -> Option<usize> {
    // Reserve one stack per configured I/O worker, one possible shutdown
    // reaper per engine, and every append/reclaim worker.
    let read_engine_count = config.io_engine_count(config.read_io_workers);
    let write_engine_count = config.io_engine_count(config.write_io_workers);
    let reclaim_engine_count = config.io_engine_count(config.reclaim_workers);
    let engine_count = read_engine_count
        .checked_add(write_engine_count)?
        .checked_add(reclaim_engine_count)?;
    let stack_count = config
        .read_io_workers
        .checked_add(config.write_io_workers)?
        .checked_add(config.reclaim_workers)?
        .checked_add(engine_count)?
        .checked_add(shard_count)?
        .checked_add(config.reclaim_workers)?;
    let stacks = stack_count.checked_mul(CACHE_THREAD_STACK_BYTES)?;
    let read_queue =
        read_engine_count.checked_mul(config.io_depth_per_engine(config.read_io_workers))?;
    let read_wait_queue = if config.read_io_wait_timeout.is_zero() {
        0
    } else {
        config.read_io_wait_capacity()
    };
    let reclaim_queue =
        reclaim_engine_count.checked_mul(config.io_depth_per_engine(config.reclaim_workers))?;
    let queue = write_engine_count
        .checked_mul(config.io_depth_per_engine(config.write_io_workers))?
        .checked_add(read_queue)?
        .checked_add(read_wait_queue)?
        .checked_add(reclaim_queue)?
        .checked_mul(IO_QUEUE_ENTRY_RESERVATION_BYTES)?;
    let controls = engine_count
        .checked_add(shard_count)?
        .checked_add(config.l1_shards)?
        .checked_add(config.reclaim_workers)?
        .checked_mul(RUNTIME_CONTROL_RESERVATION_BYTES)?;
    let metrics = shard_count.checked_mul(std::mem::size_of::<ActivityMetrics>())?;
    stacks
        .checked_add(queue)?
        .checked_add(controls)?
        .checked_add(metrics)
}

fn invalid_runtime_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
