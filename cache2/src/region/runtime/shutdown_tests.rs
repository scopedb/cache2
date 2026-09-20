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
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;

use super::*;
use crate::IoEngineOptions;
use crate::io::backend::IoBackend;
use crate::io::backend::RuntimeIoStats;
use crate::io::backend::SyncMode;
use crate::io::backend::SyncPoint;
use crate::io::backend::WritePoint;
use crate::io::engine::BackendIoEngine;
use crate::io::engine::IoRequest;
use crate::io::engine::RuntimeInner;

#[derive(Default)]
struct BlockedReadState {
    started: bool,
    released: bool,
}

#[derive(Default)]
struct BlockedRead {
    state: Mutex<BlockedReadState>,
    changed: Condvar,
}

impl BlockedRead {
    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }

    fn wait_started(&self) {
        let state = self.state.lock().unwrap();
        let (state, _) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(1), |state| !state.started)
            .unwrap();
        assert!(state.started);
    }
}

impl IoBackend for BlockedRead {
    fn len(&self) -> io::Result<u64> {
        Ok(4096)
    }

    fn set_len(&self, _: u64) -> io::Result<()> {
        Ok(())
    }

    fn read_at(&self, bytes: &mut [u8], _: u64) -> io::Result<usize> {
        let mut state = self.state.lock().unwrap();
        state.started = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
        bytes.fill(0);
        Ok(bytes.len())
    }

    fn write_at(&self, _: WritePoint, bytes: &[u8], _: u64) -> io::Result<usize> {
        Ok(bytes.len())
    }

    fn sync(&self, _: SyncPoint, _: SyncMode) -> io::Result<()> {
        Ok(())
    }

    fn try_lock_exclusive(&self) -> io::Result<()> {
        Ok(())
    }

    fn unlock(&self) -> io::Result<()> {
        Ok(())
    }
}

struct RacingEngine {
    inner: BackendIoEngine,
    backend: Arc<BlockedRead>,
    managed_memory: Arc<ManagedMemory>,
    inject: AtomicBool,
    pending: Mutex<Option<IoRequest>>,
}

impl IoEngine for RacingEngine {
    fn inner(&self) -> &RuntimeInner {
        self.inner.inner()
    }

    fn runtime_io_stats(&self) -> RuntimeIoStats {
        self.inner.runtime_io_stats()
    }

    fn in_flight(&self) -> usize {
        let observed = self.inner.in_flight();
        if self.inject.swap(false, Ordering::AcqRel) {
            // Schedule a competing read immediately after the idle observation.
            if let Ok(slot) = self.inner.try_reserve_read() {
                let buffer =
                    IoBuffer::for_read(self.managed_memory.try_read_buffer(4096).unwrap(), 4096)
                        .unwrap();
                if let Ok(request) = self
                    .inner
                    .submit_reserved_read(slot, IoOperation::read(buffer, 0))
                {
                    *self.pending.lock().unwrap() = Some(request);
                    self.backend.wait_started();
                }
            }
        }
        observed
    }
}

#[test]
fn late_read_must_not_pin_close() {
    assert_close_does_not_wait_for_read(false);
}

#[test]
fn submitted_read_must_not_pin_close() {
    assert_close_does_not_wait_for_read(true);
}

fn assert_close_does_not_wait_for_read(submit_before_close: bool) {
    use crate::config::runtime::PosixIoOptions;
    use crate::region::file_backend::FileRegionBackend;
    use crate::region::file_backend::RegionFiles;
    use crate::region::recovery::PersistentId;
    use crate::region::store::RegionStore;
    let root = env::temp_dir().join(format!(
        "cache2-close-race-{}-{submit_before_close}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let files = RegionFiles::new(root.join("data"), root.join("state"), root.join("image"));
    let data = DataSuperblock {
        generation: 1,
        cache_uuid: PersistentId::from_bytes([1; 16]).unwrap(),
        data_identity: PersistentId::from_bytes([2; 16]).unwrap(),
        geometry: DataGeometry {
            data_file_len: DataGeometry::expected_file_len(4096, 2).unwrap(),
            region_size: 4096,
            region_count: 2,
        },
        hash_seed: 3,
        storage_fingerprint: 4,
    };
    let config = RuntimeOptions {
        append_shards: 1,
        l1_capacity_bytes: 0,
        io_engine: IoEngineOptions::Posix(PosixIoOptions {
            read_workers: 1,
            write_workers: 1,
            reclaim_workers: 1,
        }),
        ..RuntimeOptions::default()
    };
    let mut store = RegionStore::open(
        8,
        FileRegionBackend::for_test_with_options(files, data, 8, config),
    )
    .unwrap();
    let mut plane = store.data_plane_handle().unwrap();
    store.close_fast().unwrap();
    // Reuse a stopped runtime's fixed resources without unrelated workers.
    let shared = Arc::get_mut(&mut plane.shared).unwrap();
    let backend = Arc::new(BlockedRead::default());
    let engine = Arc::new(RacingEngine {
        inner: BackendIoEngine::new(backend.clone(), 1).unwrap(),
        backend: backend.clone(),
        managed_memory: shared.managed_memory.clone(),
        inject: AtomicBool::new(true),
        pending: Mutex::new(None),
    });
    if submit_before_close {
        assert_eq!(engine.in_flight(), 0);
        assert_eq!(engine.inner.in_flight(), 1);
    }
    shared.read_engines = vec![engine.clone() as Arc<dyn IoEngine>].into_boxed_slice();
    shared.write_engines = Box::new([]);
    shared.reclaim_engines = Box::new([]);
    shared.shards = Box::new([]);
    let shared = Arc::clone(&plane.shared);
    let (tx, rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let result = stop_running(RunningOwner {
            shared,
            shard_workers: vec![],
            reclaim_workers: vec![],
        });
        tx.send(result).unwrap();
    });
    let result = rx.recv_timeout(Duration::from_secs(1));
    backend.release();
    thread.join().unwrap();
    engine.shutdown().unwrap();
    assert!(!engine.inject.load(Ordering::Acquire));
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        matches!(result, Ok(Ok(false))),
        "close synchronously joined a blocked read"
    );
}
