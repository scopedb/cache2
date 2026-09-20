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

//! Environment configuration shared by the cache benchmark harnesses.

use std::env;
use std::io;
use std::str::FromStr;

use cache2::IoEngineOptions;
use cache2::IoUringOptions;
use cache2::IoUringPoolOptions;
use cache2::IoUringSqPollOptions;
use cache2::PosixIoOptions;

/// Reads backend-specific pool settings under a benchmark's environment prefix.
pub fn io_engine_from_env(prefix: &str) -> io::Result<IoEngineOptions> {
    reject_renamed_env(prefix)?;
    // Migration scaffolding tied to `reject_renamed_env`; remove both together.
    for suffix in ["READ_IO_WORKERS", "WRITE_IO_WORKERS", "RECLAIM_WORKERS"] {
        let name = format!("{prefix}_{suffix}");
        if env::var_os(&name).is_some() {
            return Err(invalid(format!(
                "{name} was removed; use {prefix}_POSIX_<ROLE>_WORKERS or {prefix}_IO_URING_<ROLE>_RINGS and _MAX_IN_FLIGHT"
            )));
        }
    }
    let name = format!("{prefix}_IO_ENGINE");
    match setting::<String>(&name)?.as_deref().unwrap_or("posix") {
        "posix" => {
            let mut options = PosixIoOptions::default();
            options.read_workers = setting(&format!("{prefix}_POSIX_READ_WORKERS"))?.unwrap_or(4);
            options.write_workers = setting(&format!("{prefix}_POSIX_WRITE_WORKERS"))?.unwrap_or(4);
            options.reclaim_workers =
                setting(&format!("{prefix}_POSIX_RECLAIM_WORKERS"))?.unwrap_or(1);
            Ok(IoEngineOptions::Posix(options))
        }
        "io-uring" => {
            let mut options = IoUringOptions::default();
            options.read = io_uring_pool(prefix, "READ", 4, 256)?;
            options.write = io_uring_pool(prefix, "WRITE", 4, 256)?;
            options.reclaim = io_uring_pool(prefix, "RECLAIM", 1, 1)?;
            Ok(IoEngineOptions::IoUring(options))
        }
        value => Err(invalid(format!("unsupported {name}: {value}"))),
    }
}

/// Maximum active reads across the selected backend's pool.
pub fn read_max_in_flight(options: IoEngineOptions) -> usize {
    match options {
        IoEngineOptions::Posix(options) => options.read_workers,
        IoEngineOptions::IoUring(options) => options.read.max_in_flight,
        _ => unreachable!("unsupported benchmark I/O engine"),
    }
}

/// Maximum active reclaim requests across the selected backend's pool.
pub fn reclaim_max_in_flight(options: IoEngineOptions) -> usize {
    match options {
        IoEngineOptions::Posix(options) => options.reclaim_workers,
        IoEngineOptions::IoUring(options) => options.reclaim.max_in_flight,
        _ => unreachable!("unsupported benchmark I/O engine"),
    }
}

/// Rejects renamed knobs so old scripts cannot silently select different defaults.
///
/// Migration scaffolding for the pre-rename names; remove once those names have
/// aged out of benchmark runbooks.
pub fn reject_renamed_env(prefix: &str) -> io::Result<()> {
    for (old, new) in [
        ("STATS", "ACTIVITY_COUNTERS"),
        ("MEMORY_MIB", "L1_CAPACITY_MIB"),
        ("L1_MIB", "L1_CAPACITY_MIB"),
        ("L2_MIB", "CAPACITY_MIB"),
        ("REGION_MIB", "REGION_SIZE_MIB"),
    ] {
        let old = format!("{prefix}_{old}");
        if env::var_os(&old).is_some() {
            return Err(invalid(format!("{old} was renamed to {prefix}_{new}")));
        }
    }
    Ok(())
}

fn io_uring_pool(
    prefix: &str,
    role: &str,
    rings: usize,
    max_in_flight: usize,
) -> io::Result<IoUringPoolOptions> {
    let prefix = format!("{prefix}_IO_URING_{role}");
    let mut options = IoUringPoolOptions::default();
    options.rings = setting(&format!("{prefix}_RINGS"))?.unwrap_or(rings);
    options.max_in_flight = setting(&format!("{prefix}_MAX_IN_FLIGHT"))?.unwrap_or(max_in_flight);
    options.io_poll = match setting::<String>(&format!("{prefix}_IOPOLL"))?.as_deref() {
        None | Some("false" | "0") => false,
        Some("true" | "1") => true,
        Some(_) => {
            return Err(invalid(format!(
                "{prefix}_IOPOLL must be true, false, 1, or 0"
            )));
        }
    };
    let idle = setting(&format!("{prefix}_SQPOLL_MS"))?;
    let cpu = setting(&format!("{prefix}_SQPOLL_CPU"))?;
    if let Some(idle) = idle {
        let mut sq_poll = IoUringSqPollOptions::new(idle);
        sq_poll.cpu = cpu;
        options.sq_poll = Some(sq_poll);
    } else if cpu.is_some() {
        return Err(invalid(format!(
            "{prefix}_SQPOLL_CPU requires {prefix}_SQPOLL_MS"
        )));
    }
    Ok(options)
}

fn setting<T: FromStr>(name: &str) -> io::Result<Option<T>> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|_| invalid(format!("invalid {name}: {value}"))),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
