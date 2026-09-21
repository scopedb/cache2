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

//! Owns one open cache session and orders restart recovery, execution, and close.
//!
//! RUNNING is durable before workers start. CLEAN is published only after
//! mutation quiescence and a durable recovery image. Every terminal path
//! releases file ownership unless an issued write still requires the lock.

use std::io;
use std::sync::Arc;

use crate::cache::runtime::CacheRuntime;
use crate::config::CacheConfig;
#[cfg(test)]
use crate::config::runtime::RuntimeOptions;
#[cfg(test)]
use crate::config::storage::cache_config;
use crate::io::fs::FileSystem;
use crate::io::fs::OsFileSystem;
#[cfg(test)]
use crate::region::RegionStore;
use crate::region::persistence::RegionPaths;
use crate::region::persistence::RegionPersistence;
use crate::region::recovery::DataSuperblock;
use crate::snapshot::StartupMode;

pub struct CacheSession<F: FileSystem = OsFileSystem> {
    persistence: RegionPersistence<F>,
    runtime: Option<CacheRuntime>,
    startup: StartupMode,
    closed: bool,
}

impl CacheSession {
    pub fn open(
        paths: RegionPaths,
        format_data: DataSuperblock,
        config: CacheConfig,
    ) -> io::Result<Self> {
        Self::open_with_file_system(paths, format_data, config, OsFileSystem)
    }

    #[cfg(test)]
    pub fn for_test(
        paths: RegionPaths,
        data: DataSuperblock,
        index_slots: usize,
    ) -> io::Result<Self> {
        Self::for_test_with_options(paths, data, index_slots, RuntimeOptions::default())
    }

    #[cfg(test)]
    pub fn for_test_with_options(
        paths: RegionPaths,
        data: DataSuperblock,
        index_slots: usize,
        options: RuntimeOptions,
    ) -> io::Result<Self> {
        Self::open(
            paths,
            data,
            cache_config(data.geometry, index_slots, options),
        )
    }
}

impl<F: FileSystem> CacheSession<F> {
    fn open_with_file_system(
        paths: RegionPaths,
        format_data: DataSuperblock,
        config: CacheConfig,
        file_system: F,
    ) -> io::Result<Self> {
        let index_slots = config.storage().index_slots();
        let append_shards = config.runtime().append_shards;
        let mut persistence = RegionPersistence::new(paths, format_data, file_system);
        persistence.acquire_exclusive(config.runtime().io_mode)?;

        let opened = (|| {
            let regions = match persistence.inspect_recovery(index_slots)? {
                Some(image) => persistence.recover_regions(image, index_slots, append_shards)?,
                None => None,
            };
            let (regions, startup) = match regions {
                Some(regions) => (regions, StartupMode::Warm),
                None => (
                    persistence.cold_regions(index_slots, append_shards)?,
                    StartupMode::Cold,
                ),
            };
            persistence.publish_running()?;
            let runtime = CacheRuntime::start(
                Arc::new(regions),
                persistence.data_superblock()?,
                persistence.clone_data_handles()?,
                config,
            )?;
            Ok((runtime, startup))
        })();

        match opened {
            Ok((runtime, startup)) => Ok(Self {
                persistence,
                runtime: Some(runtime),
                startup,
                closed: false,
            }),
            Err(error) => {
                let _ = persistence.release_exclusive();
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub fn for_test_with_file_system(
        paths: RegionPaths,
        data: DataSuperblock,
        index_slots: usize,
        file_system: F,
    ) -> io::Result<Self> {
        Self::open_with_file_system(
            paths,
            data,
            cache_config(data.geometry, index_slots, RuntimeOptions::default()),
            file_system,
        )
    }

    pub const fn startup(&self) -> StartupMode {
        self.startup
    }

    pub fn runtime(&self) -> io::Result<&CacheRuntime> {
        self.runtime.as_ref().ok_or_else(closed_error)
    }

    #[cfg(test)]
    pub fn regions(&self) -> io::Result<&RegionStore> {
        Ok(self.runtime()?.regions())
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
            Some(runtime) => (|| {
                let regions = Arc::clone(runtime.regions());
                let stopped = runtime.shutdown();
                drop(runtime);
                match stopped {
                    Ok(false) => {}
                    Ok(true) => {
                        self.persistence.retain_data_lock();
                        let message = if warm {
                            regions.enter_miss_only();
                            "I/O engine could not fence an issued write; CLEAN rejected"
                        } else {
                            "I/O engine could not fence an issued write; lock retained"
                        };
                        return Err(io::Error::other(message));
                    }
                    Err(error) => {
                        regions.enter_miss_only();
                        return Err(error);
                    }
                }
                if warm {
                    let frozen = regions.freeze()?;
                    let prepared = self.persistence.persist_frozen(&frozen)?;
                    self.persistence.publish_clean(prepared)?;
                }
                Ok(())
            })(),
            None => Err(closed_error()),
        };
        let unlock = self.persistence.release_exclusive();
        self.closed = true;
        result.and(unlock)
    }
}

impl<F: FileSystem> Drop for CacheSession<F> {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.close_fast();
        }
    }
}

fn closed_error() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "cache session is closed")
}
