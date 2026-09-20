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
use cache2::IoMode;
use cache2::IoUringOptions;
use cache2::IoUringPoolOptions;
use cache2::IoUringSqPollOptions;
use cache2::L1EvictionPolicy;
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

/// Reads an unsigned 64-bit setting, falling back to `default` when unset.
pub fn env_u64(name: &str, default: u64) -> io::Result<u64> {
    Ok(setting(name)?.unwrap_or(default))
}

/// Reads a `usize` setting, falling back to `default` when unset.
pub fn env_usize(name: &str, default: usize) -> io::Result<usize> {
    Ok(setting(name)?.unwrap_or(default))
}

/// Reads an unsigned 32-bit setting, falling back to `default` when unset.
pub fn env_u32(name: &str, default: u32) -> io::Result<u32> {
    Ok(setting(name)?.unwrap_or(default))
}

/// Reads a boolean setting (`true`/`1` or `false`/`0`), falling back to `default` when unset.
pub fn env_bool(name: &str, default: bool) -> io::Result<bool> {
    match setting::<String>(name)?.as_deref() {
        None => Ok(default),
        Some("true" | "1") => Ok(true),
        Some("false" | "0") => Ok(false),
        Some(_) => Err(invalid(format!("{name} must be true, false, 1, or 0"))),
    }
}

/// Reads an optional `usize` setting.
pub fn env_optional_usize(name: &str) -> io::Result<Option<usize>> {
    setting(name)
}

/// Reads an optional finite, non-negative threshold setting.
pub fn env_optional_f64(name: &str) -> io::Result<Option<f64>> {
    let Some(value) = setting::<f64>(name)? else {
        return Ok(None);
    };
    if !value.is_finite() || value < 0.0 {
        return Err(invalid(format!(
            "{name} must be a finite non-negative number"
        )));
    }
    Ok(Some(value))
}

/// Reads a comma-separated list of `usize` values, falling back to `default` when unset.
pub fn env_usize_list(name: &str, default: &[usize]) -> io::Result<Box<[usize]>> {
    match env::var(name) {
        Ok(value) => value
            .split(',')
            .map(|item| {
                item.parse::<usize>()
                    .map_err(|_| invalid(format!("{name} must be comma-separated integers")))
            })
            .collect::<io::Result<Vec<_>>>()
            .map(Vec::into_boxed_slice),
        Err(env::VarError::NotPresent) => Ok(default.to_vec().into_boxed_slice()),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
}

/// Reads an I/O mode setting (`buffered` or `direct`, default `buffered`).
pub fn parse_io_mode(name: &str) -> io::Result<IoMode> {
    match setting::<String>(name)?.as_deref().unwrap_or("buffered") {
        "buffered" => Ok(IoMode::Buffered),
        "direct" => Ok(IoMode::Direct),
        value => Err(invalid(format!("unsupported I/O mode: {value}"))),
    }
}

/// Reads an L1 eviction policy setting (`clock` or `s3-fifo`, default `clock`).
pub fn parse_l1_eviction_policy(name: &str) -> io::Result<L1EvictionPolicy> {
    match setting::<String>(name)?.as_deref().unwrap_or("clock") {
        "clock" => Ok(L1EvictionPolicy::Clock),
        "s3-fifo" => Ok(L1EvictionPolicy::S3Fifo),
        value => Err(invalid(format!("unsupported L1 eviction policy: {value}"))),
    }
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
    options.io_poll = env_bool(&format!("{prefix}_IOPOLL"), false)?;
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

/// Reads an optional typed setting, reporting the raw value on parse failure.
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

/// Builds an `InvalidInput` error for a rejected benchmark setting.
pub fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(all(test, unix))]
mod tests {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::process::Command;

    use super::*;

    #[test]
    fn non_utf8_enum_settings_are_rejected() {
        const SETTING: &str = "CACHE2_TEST_NON_UTF8_ENUM_SETTING";

        if env::var_os(SETTING).is_some() {
            assert_eq!(
                parse_io_mode(SETTING).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                parse_l1_eviction_policy(SETTING).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            return;
        }

        // Set the child's environment without mutating the parallel test process.
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "config::tests::non_utf8_enum_settings_are_rejected",
            ])
            .env(SETTING, OsStr::from_bytes(b"\xff"))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child test failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
