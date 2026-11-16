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

use std::sync::mpsc;

use super::*;
use crate::FillControlOptions;
use crate::FillLimits;
use crate::FillPressure;
use crate::IoEngineOptions;
use crate::cache::session::CacheSession;
use crate::config::runtime::PosixIoOptions;
use crate::fixtures::TestDirectory;
use crate::fixtures::eventually_admitted;
use crate::io::engine::CompletionStatus;
use crate::io::engine::IoEngine;
use crate::io::engine::IoRequest;
use crate::io::file::PositionedIo;
use crate::io::file::WritePoint;
use crate::region::recovery::DataGeometry;
use crate::region::recovery::PersistentId;
use crate::snapshot::CacheHealth;

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

impl PositionedIo for BlockedRead {
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
}

#[test]
fn late_read_must_not_pin_close() {
    assert_close_with_blocked_read(false, None);
}

#[test]
fn submitted_read_must_not_pin_close() {
    assert_close_with_blocked_read(true, None);
}

#[test]
fn drain_failure_propagates_when_read_is_in_flight() {
    assert_close_with_blocked_read(
        true,
        Some(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "shard drain failed",
        )),
    );
}

fn assert_close_with_blocked_read(submit_before_close: bool, drain_failure: Option<io::Error>) {
    let suffix = if drain_failure.is_some() {
        "drain-failure"
    } else {
        "ok"
    };
    let directory = TestDirectory::new(&format!("close-race-{submit_before_close}-{suffix}"));
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
    let mut session =
        CacheSession::for_test_with_options(directory.paths.clone(), data, 8, config).unwrap();
    let mut runtime = session.runtime().unwrap().clone();
    session.close_fast().unwrap();
    // Reuse a stopped runtime's fixed resources without unrelated workers.
    let state = Arc::get_mut(&mut runtime.state).unwrap();
    let io = Arc::new(BlockedRead::default());
    let engine = Arc::new(IoEngine::for_test(io.clone(), 1).unwrap());
    let read_engine = Arc::clone(&engine);
    let read_io = Arc::clone(&io);
    let managed_memory = Arc::clone(&state.managed_memory);
    let submit_read = move || -> io::Result<IoRequest> {
        let slot = read_engine.try_reserve_read()?;
        let buffer =
            IoBuffer::for_read(managed_memory.try_read_buffer(4096).unwrap(), 4096).unwrap();
        let request = read_engine
            .submit_reserved_read(slot, IoOperation::read(buffer, 0))
            .map_err(|error| error.error)?;
        read_io.wait_started();
        Ok(request)
    };
    let (submitted, submission) = mpsc::channel();
    if submit_before_close {
        submitted.send(submit_read()).unwrap();
        assert_eq!(engine.in_flight(), 1);
        assert_eq!(engine.writes_in_flight(), 0);
        assert!(!engine.has_unfenced_writes());
    } else {
        // Exercise the interval between the idle snapshot and synchronous shutdown.
        *state.after_io_snapshot.get_mut().unwrap() = Some(Box::new(move || {
            submitted.send(submit_read()).unwrap();
        }));
    }
    state.read_engines = vec![engine.clone()].into_boxed_slice();
    state.write_engines = Box::new([]);
    state.reclaim_engines = Box::new([]);
    let expected_failure_kind = drain_failure.as_ref().map(io::Error::kind);
    if let Some(error) = drain_failure {
        let control = Arc::new(AppendWorkerControl::new());
        control.fail(&error);
        state.append_controls = vec![control].into_boxed_slice();
    } else {
        state.append_controls = Box::new([]);
    }
    let state = Arc::clone(&runtime.state);
    let (tx, rx) = mpsc::channel();
    let thread = thread::spawn(move || {
        let result = stop_workers(RuntimeWorkers {
            state,
            append_workers: vec![],
            reclaim_workers: vec![],
        });
        tx.send(result).unwrap();
    });
    let result = rx.recv_timeout(Duration::from_secs(1));
    io.release();
    thread.join().unwrap();
    engine.shutdown().unwrap();
    drop(directory);
    let result = result.expect("close synchronously joined a blocked read");
    match expected_failure_kind {
        Some(kind) => {
            let error = result.expect_err("drain failure must propagate despite in-flight read");
            assert_eq!(error.kind(), kind);
        }
        None => assert_eq!(
            result.expect("close with only reads in flight succeeds"),
            ShutdownDisposition::Quiescent
        ),
    }
    let submitted = submission.recv_timeout(Duration::from_secs(1)).unwrap();
    if submit_before_close {
        assert!(matches!(
            submitted.unwrap().wait().status,
            CompletionStatus::Completed
        ));
    } else {
        assert_eq!(submitted.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    }
}

#[test]
fn recovery_rejects_fills_preserves_reads_and_waits_for_all_workers() {
    for fill_control in [
        FillControlOptions::Disabled,
        FillControlOptions::Observe(FillLimits::new(1_048_576, 1000)),
        FillControlOptions::Adaptive(FillLimits::new(1_048_576, 1000)),
    ] {
        let directory = TestDirectory::new("recovery-admission");
        let data = DataSuperblock {
            generation: 1,
            cache_uuid: PersistentId::from_bytes([1; 16]).unwrap(),
            data_identity: PersistentId::from_bytes([2; 16]).unwrap(),
            geometry: DataGeometry {
                data_file_len: DataGeometry::expected_file_len(4096, 4).unwrap(),
                region_size: 4096,
                region_count: 4,
            },
            hash_seed: 3,
            storage_fingerprint: 4,
        };
        let config = RuntimeOptions {
            fill_control,
            append_shards: 1,
            l1_capacity_bytes: 0,
            ..RuntimeOptions::default()
        };
        let mut session =
            CacheSession::for_test_with_options(directory.paths.clone(), data, 8, config).unwrap();
        let runtime = session.runtime().unwrap().clone();
        runtime.put(b"existing", b"value").unwrap();
        runtime.drain().unwrap();
        let mut first = runtime.state.background_io_attempt();
        let mut second = runtime.state.background_io_attempt();
        assert!(first.next_deadline(Instant::now()).is_some());
        assert!(second.next_deadline(Instant::now()).is_some());
        let snapshot = runtime.snapshot().unwrap();
        assert_eq!(snapshot.health, CacheHealth::Recovering);
        if fill_control != FillControlOptions::Disabled {
            assert_eq!(snapshot.fill_control.pressure, FillPressure::Paused);
        }
        assert_eq!(
            runtime.put(b"new", b"value").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            runtime.put_l2(b"new", b"value").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(runtime.get(b"existing").unwrap().unwrap().value(), b"value");
        runtime.delete(b"existing").unwrap();
        assert!(runtime.get(b"existing").unwrap().is_none());
        first.finish();
        let snapshot = runtime.snapshot().unwrap();
        assert_eq!(snapshot.health, CacheHealth::Recovering);
        if fill_control != FillControlOptions::Disabled {
            assert_eq!(snapshot.fill_control.pressure, FillPressure::Paused);
        }
        second.finish();
        assert_eq!(runtime.snapshot().unwrap().health, CacheHealth::Running);
        eventually_admitted(|| runtime.put(b"new", b"value"));
        runtime.drain().unwrap();
        assert_eq!(runtime.get(b"new").unwrap().unwrap().value(), b"value");
        // A completion after close starts must release recovery without reopening fills.
        let mut pending = runtime.state.background_io_attempt();
        assert!(pending.next_deadline(Instant::now()).is_some());
        runtime.start_close();
        pending.finish();
        assert!(
            runtime
                .state
                .background_io_attempt()
                .next_deadline(Instant::now())
                .is_none()
        );
        if let Some(fill) = &runtime.state.fill_control {
            assert_eq!(fill.snapshot().pressure, FillPressure::Paused);
        }
        session.close_fast().unwrap();
    }
}

#[test]
fn adaptive_pressure_preserves_reads_and_deletes_and_resumes_fills() {
    let directory = TestDirectory::new("fill-admission");
    let data = DataSuperblock {
        generation: 1,
        cache_uuid: PersistentId::from_bytes([1; 16]).unwrap(),
        data_identity: PersistentId::from_bytes([2; 16]).unwrap(),
        geometry: DataGeometry {
            data_file_len: DataGeometry::expected_file_len(4096, 4).unwrap(),
            region_size: 4096,
            region_count: 4,
        },
        hash_seed: 3,
        storage_fingerprint: 4,
    };
    let config = RuntimeOptions {
        fill_control: FillControlOptions::Adaptive(FillLimits::new(1_048_576, 1000)),
        append_shards: 1,
        l1_capacity_bytes: 0,
        ..RuntimeOptions::default()
    };
    let mut session =
        CacheSession::for_test_with_options(directory.paths.clone(), data, 8, config).unwrap();
    let runtime = session.runtime().unwrap().clone();
    runtime.put(b"existing", b"value").unwrap();
    runtime.drain().unwrap();
    let fill = runtime.state.fill_control.as_ref().unwrap();
    fill.hold_pause();
    let snapshot = runtime.snapshot().unwrap();
    assert_eq!(snapshot.health, CacheHealth::Running);
    assert_eq!(snapshot.fill_control.pressure, FillPressure::Paused);
    assert_eq!(
        runtime.put(b"new", b"value").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        runtime.put_l2(b"new", b"value").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(runtime.get(b"existing").unwrap().unwrap().value(), b"value");
    runtime.delete(b"existing").unwrap();
    assert!(runtime.get(b"existing").unwrap().is_none());
    assert!(runtime.put(b"new", b"value").is_err());
    fill.release_pause();
    eventually_admitted(|| runtime.put(b"new", b"value"));
    runtime.drain().unwrap();
    assert_eq!(runtime.get(b"new").unwrap().unwrap().value(), b"value");
    session.close_fast().unwrap();
}
