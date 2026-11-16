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
use std::path::PathBuf;
use std::str::FromStr;

use cache2::FillControlOptions;
use cache2::FillLimits;
use cache2::IoEngineOptions;
use cache2::IoMode;
use cache2::IoUringOptions;
use cache2::IoUringPoolOptions;
use cache2::IoUringSqPollOptions;
use cache2::L1EvictionPolicy;
use cache2::PosixIoOptions;

/// Number of bytes in one mebibyte (MiB).
pub const MIB: usize = 1024 * 1024;

/// Reads backend-specific pool settings under a benchmark's environment prefix.
pub fn io_engine_from_env(prefix: &str) -> io::Result<IoEngineOptions> {
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
            options.read = io_uring_pool(prefix, "READ", options.read)?;
            options.write = io_uring_pool(prefix, "WRITE", options.write)?;
            options.reclaim = io_uring_pool(prefix, "RECLAIM", options.reclaim)?;
            Ok(IoEngineOptions::IoUring(options))
        }
        value => Err(invalid(format!("unsupported {name}: {value}"))),
    }
}

/// Reads optional fill-control mode and its explicit logical rate ceilings.
pub fn fill_control_from_env(prefix: &str) -> io::Result<FillControlOptions> {
    let name = format!("{prefix}_FILL_CONTROL");
    let mode = setting::<String>(&name)?.unwrap_or_else(|| "disabled".into());
    if mode == "disabled" {
        return Ok(FillControlOptions::Disabled);
    }
    let bytes = setting(&format!("{prefix}_FILL_BYTES_PER_SECOND"))?
        .ok_or_else(|| invalid("enabled fill control requires FILL_BYTES_PER_SECOND"))?;
    let operations = setting(&format!("{prefix}_FILL_OPERATIONS_PER_SECOND"))?
        .ok_or_else(|| invalid("enabled fill control requires FILL_OPERATIONS_PER_SECOND"))?;
    let options = FillLimits::new(bytes, operations);
    match mode.as_str() {
        "observe" => Ok(FillControlOptions::Observe(options)),
        "adaptive" => Ok(FillControlOptions::Adaptive(options)),
        _ => Err(invalid(format!("unsupported {name}: {mode}"))),
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

/// Reads `<prefix>_DIR` (defaulting to the temporary directory) and verifies it exists.
pub fn env_dir(prefix: &str) -> io::Result<PathBuf> {
    let name = format!("{prefix}_DIR");
    let directory = env::var_os(&name)
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);
    if !directory.is_dir() {
        return Err(invalid(format!(
            "{name} does not exist as a directory: {}",
            directory.display()
        )));
    }
    Ok(directory)
}

/// Converts a MiB setting into `usize` bytes, rejecting overflow.
pub fn mib_to_usize(name: &str, mib: usize) -> io::Result<usize> {
    mib.checked_mul(MIB)
        .ok_or_else(|| invalid(format!("{name} is too large")))
}

/// Converts a MiB setting into `u64` bytes, rejecting overflow.
pub fn mib_to_u64(name: &str, mib: u64) -> io::Result<u64> {
    mib.checked_mul(MIB as u64)
        .ok_or_else(|| invalid(format!("{name} is too large")))
}

/// Reads a MiB-denominated `usize` setting and converts it to bytes.
pub fn env_mib_usize(name: &str, default: usize) -> io::Result<usize> {
    mib_to_usize(name, env_usize(name, default)?)
}

/// Reads a MiB-denominated `u64` setting and converts it to bytes.
pub fn env_mib_u64(name: &str, default: u64) -> io::Result<u64> {
    mib_to_u64(name, env_u64(name, default)?)
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
    mut options: IoUringPoolOptions,
) -> io::Result<IoUringPoolOptions> {
    let prefix = format!("{prefix}_IO_URING_{role}");
    options.rings = setting(&format!("{prefix}_RINGS"))?.unwrap_or(options.rings);
    options.max_in_flight =
        setting(&format!("{prefix}_MAX_IN_FLIGHT"))?.unwrap_or(options.max_in_flight);
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
