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
    IoEngine, IoMode, IoPoolTopology, IoUringPoolConfig, MAX_APPEND_SHARDS,
    MAX_READ_IO_WAIT_TIMEOUT, MAX_WRITE_FLUSH_THRESHOLD_BYTES, RuntimeConfig,
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
        let read_topology = self.read_io_topology();
        let write_topology = self.write_io_topology();
        let reclaim_topology = self.reclaim_io_topology();
        match self.io_engine {
            IoEngine::Posix(_) => {
                validate_posix_pool("read", read_topology)?;
                validate_posix_pool("write", write_topology)?;
                validate_posix_pool("reclaim", reclaim_topology)?;
            }
            IoEngine::IoUring(config) => {
                validate_io_uring_pool("read", config.read())?;
                validate_io_uring_pool("write", config.write())?;
                validate_io_uring_pool("reclaim", config.reclaim())?;
                if self.io_mode != IoMode::Direct
                    && (config.read().io_poll()
                        || config.write().io_poll()
                        || config.reclaim().io_poll())
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "io_uring IOPOLL requires direct I/O mode",
                    ));
                }
            }
        }
        if reclaim_topology.max_in_flight > self.append_shards as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "reclaim I/O concurrency must be no greater than append shards",
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
            .checked_mul(self.reclaim_io_max_in_flight())
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
    // Reserve one stack per physical I/O thread, one possible shutdown reaper
    // per engine, and every append/reclaim worker.
    let read = config.read_io_topology();
    let write = config.write_io_topology();
    let reclaim = config.reclaim_io_topology();
    let engine_count = read
        .engine_count
        .checked_add(write.engine_count)?
        .checked_add(reclaim.engine_count)?;
    let stack_count = read
        .worker_threads
        .checked_add(write.worker_threads)?
        .checked_add(reclaim.worker_threads)?
        .checked_add(engine_count)?
        .checked_add(shard_count)?
        .checked_add(reclaim.max_in_flight)?;
    let stacks = stack_count.checked_mul(CACHE_THREAD_STACK_BYTES)?;
    let read_wait_queue = if config.read_io_wait_timeout.is_zero() {
        0
    } else {
        config.read_io_wait_capacity()
    };
    let queue = write
        .max_in_flight
        .checked_add(read.max_in_flight)?
        .checked_add(read_wait_queue)?
        .checked_add(reclaim.max_in_flight)?
        .checked_mul(IO_QUEUE_ENTRY_RESERVATION_BYTES)?;
    let controls = engine_count
        .checked_add(shard_count)?
        .checked_add(config.l1_shards)?
        .checked_add(reclaim.max_in_flight)?
        .checked_mul(RUNTIME_CONTROL_RESERVATION_BYTES)?;
    let metrics = shard_count.checked_mul(std::mem::size_of::<ActivityMetrics>())?;
    stacks
        .checked_add(queue)?
        .checked_add(controls)?
        .checked_add(metrics)
}

fn validate_posix_pool(name: &str, topology: IoPoolTopology) -> io::Result<()> {
    if !(1..=MAX_IO_REQUESTS_PER_ENGINE).contains(&topology.max_in_flight) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("POSIX {name} worker count must be in 1..={MAX_IO_REQUESTS_PER_ENGINE}"),
        ));
    }
    Ok(())
}

fn validate_io_uring_pool(name: &str, config: IoUringPoolConfig) -> io::Result<()> {
    if !(1..=MAX_CONFIG_COUNT).contains(&config.rings()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} ring count must be in 1..={MAX_CONFIG_COUNT}"),
        ));
    }
    if !(1..=MAX_CONFIG_COUNT).contains(&config.max_in_flight()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} maximum in-flight requests must be in 1..={MAX_CONFIG_COUNT}"),
        ));
    }
    if config.rings() > config.max_in_flight() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} ring count must not exceed its in-flight limit"),
        ));
    }
    if config.max_in_flight().div_ceil(config.rings()) > MAX_IO_REQUESTS_PER_ENGINE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} per-ring depth must not exceed {MAX_IO_REQUESTS_PER_ENGINE}"),
        ));
    }
    Ok(())
}

fn invalid_runtime_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_config::IoUringSqPollConfig;

    #[test]
    fn io_uring_pool_validation_bounds_rings_and_depth() {
        validate_io_uring_pool("read", IoUringPoolConfig::new(3, 8)).unwrap();

        for config in [
            IoUringPoolConfig::new(0, 8),
            IoUringPoolConfig::new(1, 0),
            IoUringPoolConfig::new(3, 2),
            IoUringPoolConfig::new(1, MAX_IO_REQUESTS_PER_ENGINE + 1),
        ] {
            assert_eq!(
                validate_io_uring_pool("read", config).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn sq_poll_cpu_affinity_is_retained_for_each_ring() {
        let config =
            IoUringPoolConfig::new(2, 8).with_sq_poll(IoUringSqPollConfig::new(1_000).with_cpu(4));
        validate_io_uring_pool("read", config).unwrap();
        assert_eq!(config.sq_poll().and_then(|sq_poll| sq_poll.cpu()), Some(4));
    }

    #[cfg(all(
        feature = "io-uring",
        target_os = "linux",
        any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "powerpc64"
        )
    ))]
    #[test]
    fn io_poll_requires_direct_mode() {
        let pool = IoUringPoolConfig::default().with_io_poll(true);
        let config = RuntimeConfig::default().with_io_engine(IoEngine::IoUring(
            crate::runtime_config::IoUringConfig::new(
                pool,
                IoUringPoolConfig::default(),
                IoUringPoolConfig::new(1, 1),
            ),
        ));

        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
