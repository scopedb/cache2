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

//! Runtime support shared by the benchmark harnesses: benchmark file
//! lifecycle, process memory sampling, write-admission retry, and
//! deterministic mixing.

use std::fs;
use std::io;
#[cfg(unix)]
use std::mem::MaybeUninit;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use cache2::Cache;

use crate::config::invalid;

const RETRY_DELAY: Duration = Duration::from_micros(50);
const YIELD_RETRIES: usize = 8;

/// A benchmark's data file and its sidecars, removed together on drop.
///
/// The set covers the data file plus its `.state`, `.image`, and
/// `.image.next` siblings.
pub struct BenchFiles {
    data: PathBuf,
    stem: String,
    cleanup_on_drop: bool,
}

impl BenchFiles {
    /// Creates a timestamped file set that drop always removes.
    pub fn new(directory: &Path, stem: &str) -> Self {
        Self::with_cleanup(directory, stem, true)
    }

    /// Creates a timestamped file set that drop preserves until
    /// [`mark_success`](Self::mark_success), so failed runs keep their
    /// artifacts for inspection.
    pub fn preserved_on_failure(directory: &Path, stem: &str) -> Self {
        Self::with_cleanup(directory, stem, false)
    }

    fn with_cleanup(directory: &Path, stem: &str, cleanup_on_drop: bool) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            data: directory.join(format!(
                "cache2-{stem}-{}-{timestamp}.cache",
                std::process::id()
            )),
            stem: stem.to_owned(),
            cleanup_on_drop,
        }
    }

    /// Arms drop to remove the file set.
    pub fn mark_success(&mut self) {
        self.cleanup_on_drop = true;
    }

    /// Returns the path of the benchmark's data file.
    pub fn data(&self) -> &Path {
        &self.data
    }

    /// Returns the total logical bytes across the existing files in the set.
    pub fn logical_bytes(&self) -> io::Result<u64> {
        self.paths()
            .into_iter()
            .try_fold(0_u64, |total, path| match fs::metadata(path) {
                Ok(metadata) => total
                    .checked_add(metadata.len())
                    .ok_or_else(|| invalid("logical disk byte count overflow")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(total),
                Err(error) => Err(error),
            })
    }

    /// Returns the total allocated on-disk bytes across the existing files in the set.
    #[cfg(unix)]
    pub fn allocated_bytes(&self) -> io::Result<u64> {
        use std::os::unix::fs::MetadataExt;

        self.paths()
            .into_iter()
            .try_fold(0_u64, |total, path| match fs::metadata(path) {
                Ok(metadata) => total
                    .checked_add(metadata.blocks().saturating_mul(512))
                    .ok_or_else(|| invalid("allocated file size overflow")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(total),
                Err(error) => Err(error),
            })
    }

    /// Returns the total allocated bytes, approximated by logical size without block accounting.
    #[cfg(not(unix))]
    pub fn allocated_bytes(&self) -> io::Result<u64> {
        self.logical_bytes()
    }

    fn paths(&self) -> [PathBuf; 4] {
        [
            self.data.clone(),
            sidecar(&self.data, ".state"),
            sidecar(&self.data, ".image"),
            sidecar(&self.data, ".image.next"),
        ]
    }
}

impl Drop for BenchFiles {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            eprintln!(
                "{} artifacts preserved after failure: data={}",
                self.stem,
                self.data.display()
            );
            return;
        }
        for path in self.paths() {
            let _ = fs::remove_file(path);
        }
    }
}

/// Writes one value, retrying overload rejections until `timeout` elapses.
///
/// Returns the write receipt and the number of attempts. The first few
/// retries yield the thread so bounded staging can drain; later retries back
/// off with a short sleep.
pub fn put_eventually(
    cache: &Cache,
    key: &[u8],
    value: &[u8],
    timeout: Duration,
) -> io::Result<(u64, usize)> {
    let deadline = Instant::now() + timeout;
    let mut attempts = 0_usize;
    loop {
        attempts = attempts.saturating_add(1);
        match cache.put(key, value) {
            Ok(receipt) => return Ok((receipt, attempts)),
            Err(error) if error.kind() == cache2::ErrorKind::Overloaded => {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "benchmark write did not enter bounded staging",
                    ));
                }
                if attempts <= YIELD_RETRIES {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(RETRY_DELAY);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// Returns the peak resident set size of this process in bytes, or 0 when unavailable.
#[cfg(unix)]
pub fn peak_rss_bytes() -> u64 {
    let mut usage = MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `usage` points to writable storage for one `rusage` value.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return 0;
    }
    // SAFETY: a successful getrusage initialized the complete value.
    let usage = unsafe { usage.assume_init() };
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        u64::try_from(usage.ru_maxrss).unwrap_or(0)
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        u64::try_from(usage.ru_maxrss)
            .unwrap_or(0)
            .saturating_mul(1024)
    }
}

/// Returns the peak resident set size of this process in bytes, or 0 when unavailable.
#[cfg(not(unix))]
pub fn peak_rss_bytes() -> u64 {
    0
}

/// Returns the current resident set size in bytes, read from `/proc/self/status`.
#[cfg(target_os = "linux")]
pub fn current_rss_bytes() -> io::Result<u64> {
    let status = fs::read_to_string("/proc/self/status")?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_ascii_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| io::Error::other("cannot read current RSS from /proc/self/status"))?;
    kib.checked_mul(1024)
        .ok_or_else(|| io::Error::other("current RSS byte count overflow"))
}

/// Returns the current resident set size in bytes, or 0 on platforms without `/proc`.
#[cfg(not(target_os = "linux"))]
pub fn current_rss_bytes() -> io::Result<u64> {
    Ok(0)
}

/// Deterministic 64-bit bit mixer (the splitmix64 finalizer).
pub fn splitmix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}
