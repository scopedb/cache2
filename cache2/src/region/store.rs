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

//! Recovery and shutdown state machine for the Region-backed cache.
//!
//! The coordinator owns no file-format or data-plane logic. A
//! [`FileRegionBackend`] supplies those operations, while this module enforces the
//! order that makes warm recovery safe:
//!
//! - inspect recovery before constructing an index;
//! - publish `RUNNING` before exposing a runtime;
//! - publish `CLEAN` only after freezing and persisting a complete image;
//! - release exclusive ownership on every terminal path.

use std::io;

use crate::io::fs::FileSystem;
use crate::io::fs::OsFileSystem;
use crate::region::file_backend::FileRegionBackend;
use crate::region::file_backend::FileRegionRuntime;
use crate::region::index::storage::validated_index_partition_ranges;
use crate::region::runtime::RegionDataPlane;
use crate::snapshot::StartupMode;

/// Owns the files and runtime for one Region-backed cache.
pub struct RegionStore<F: FileSystem = OsFileSystem> {
    backend: FileRegionBackend<F>,
    runtime: Option<FileRegionRuntime>,
    startup: StartupMode,
    closed: bool,
}

impl<F: FileSystem> RegionStore<F> {
    pub fn open(index_slots: usize, mut backend: FileRegionBackend<F>) -> io::Result<Self> {
        validate_index_slots(index_slots)?;
        backend.acquire_exclusive()?;

        let opened = (|| {
            let runtime = match backend.inspect_recovery(index_slots)? {
                Some(clean) => backend.map_clean_runtime(clean, index_slots)?,
                None => None,
            };
            let (runtime, startup) = match runtime {
                Some(runtime) => (runtime, StartupMode::Warm),
                None => (backend.anonymous_runtime(index_slots)?, StartupMode::Cold),
            };

            backend.publish_running()?;
            let runtime = backend.start_runtime(runtime)?;
            Ok((runtime, startup))
        })();

        match opened {
            Ok((runtime, startup)) => Ok(Self {
                backend,
                runtime: Some(runtime),
                startup,
                closed: false,
            }),
            Err(error) => {
                let _ = backend.release_exclusive();
                Err(error)
            }
        }
    }

    pub const fn startup(&self) -> StartupMode {
        self.startup
    }

    pub fn data_plane_handle(&self) -> io::Result<RegionDataPlane> {
        Ok(self.runtime()?.data_plane()?.clone())
    }

    pub fn runtime(&self) -> io::Result<&FileRegionRuntime> {
        if self.closed {
            return Err(closed_error());
        }
        self.runtime.as_ref().ok_or_else(closed_error)
    }

    #[cfg(test)]
    pub fn runtime_mut(&mut self) -> io::Result<&mut FileRegionRuntime> {
        if self.closed {
            return Err(closed_error());
        }
        self.runtime.as_mut().ok_or_else(closed_error)
    }

    /// Stop without producing a recovery image. The next open starts empty.
    pub fn close_fast(&mut self) -> io::Result<()> {
        self.close(false)
    }

    /// Freeze and publish one complete warm-restart image.
    pub fn close_warm(&mut self) -> io::Result<()> {
        self.close(true)
    }

    fn close(&mut self, warm: bool) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }

        let result = match self.runtime.take() {
            Some(runtime) if warm => self
                .backend
                .freeze_warm(runtime)
                .and_then(|frozen| self.backend.persist_frozen(&frozen))
                .and_then(|prepared| self.backend.publish_clean(prepared)),
            Some(runtime) => self.backend.stop_fast(runtime),
            None => Err(closed_error()),
        };

        let unlock = self.backend.release_exclusive();
        self.closed = true;
        result.and(unlock)
    }
}

impl<F: FileSystem> Drop for RegionStore<F> {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.close_fast();
        }
    }
}

fn closed_error() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "RegionStore is closed")
}

fn validate_index_slots(index_slots: usize) -> io::Result<()> {
    if index_slots < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RegionStore requires at least 8 index slots",
        ));
    }
    validated_index_partition_ranges(index_slots)
        .map(|_| ())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
}
