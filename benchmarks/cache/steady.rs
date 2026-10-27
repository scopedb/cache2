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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

use super::*;

// Dropping command senders also stops workers on an early error. Unlike a
// reusable barrier, a disconnected channel cannot strand the other writers.
#[derive(Default)]
struct Writers {
    commands: Vec<mpsc::SyncSender<()>>,
    results: Vec<mpsc::Receiver<io::Result<WriteTotals>>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl Drop for Writers {
    fn drop(&mut self) {
        self.commands.clear();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

pub async fn run(cache: Arc<Cache>, config: &BenchConfig) -> io::Result<()> {
    let mut writers = Writers::default();
    let next = Arc::new(AtomicUsize::new(0));
    for _ in 0..config.write_clients {
        let (command, commands) = mpsc::sync_channel(1);
        let cache = Arc::clone(&cache);
        let (completed, results) = mpsc::sync_channel(1);
        let entries = config.entries;
        let next = Arc::clone(&next);
        let value_bytes = config.value_bytes;
        writers.threads.push(std::thread::spawn(move || {
            let mut value = vec![0xa5; value_bytes];
            let _ = completed.send(Ok(WriteTotals::default()));
            while commands.recv().is_ok() {
                // Fixed small chunks let faster workers keep doing useful work
                // instead of waiting for one slow core's entire static share.
                let ordinals = std::iter::from_fn(|| {
                    let first = next.fetch_add(64, Ordering::Relaxed);
                    (first < entries).then_some(first..first.saturating_add(64).min(entries))
                })
                .flatten();
                let result = write_ordinals(&cache, &mut value, ordinals);
                let failed = result.is_err();
                if completed.send(result).is_err() || failed {
                    break;
                }
            }
        }));
        writers.commands.push(command);
        writers.results.push(results);
    }
    for results in &writers.results {
        results
            .recv()
            .map_err(|_| io::Error::other("steady writer failed to start"))??;
    }
    let mut measurement = Measurement {
        elapsed: Duration::ZERO,
        operations: config.entries * config.write_rounds,
        bytes: (config.entries as u128)
            * (config.write_rounds as u128)
            * (config.value_bytes as u128),
        checksum: 0,
    };
    let mut attempts = 0_usize;
    let mut throttled = 0_usize;
    let mut drain_elapsed = Duration::ZERO;
    for round in 0..config.write_warmup_rounds + config.write_rounds {
        // Timestamp before dispatch: no accepted operation can precede it.
        next.store(0, Ordering::Relaxed);
        let started = Instant::now();
        for command in &writers.commands {
            command
                .send(())
                .map_err(|_| io::Error::other("steady writer stopped"))?;
        }
        let mut operations = 0_usize;
        for results in &writers.results {
            let totals = results
                .recv()
                .map_err(|_| io::Error::other("steady writer panicked"))??;
            operations += totals.operations;
            if round >= config.write_warmup_rounds {
                attempts = attempts.saturating_add(totals.attempts);
                throttled += totals.throttled;
                measurement.checksum = measurement.checksum.wrapping_add(totals.checksum);
            }
        }
        if operations != config.entries {
            return Err(io::Error::other(
                "steady round did not complete its fixed operation count",
            ));
        }
        let drain_started = Instant::now();
        cache.drain().await?;
        if round >= config.write_warmup_rounds {
            drain_elapsed += drain_started.elapsed();
            measurement.elapsed += started.elapsed();
        }
    }
    report(
        "put_steady",
        "steady put + drain",
        "write",
        config.write_clients,
        &measurement,
        throttled as u64,
        None,
        0,
    );
    println!(
        "result phase=put_steady_admission rounds={} warmup_rounds={} attempts={} retries={} throttled={} admission_elapsed_ns={} drain_elapsed_ns={}",
        config.write_rounds,
        config.write_warmup_rounds,
        attempts,
        attempts.saturating_sub(measurement.operations),
        throttled,
        measurement.elapsed.saturating_sub(drain_elapsed).as_nanos(),
        drain_elapsed.as_nanos()
    );
    // Validation is outside the timed interval and remains active with all
    // library statistics disabled. Misses are allowed by the cache contract.
    let mut verified = 0;
    for ordinal in (0..config.entries).step_by((config.entries / 1024).max(1)) {
        if let Some(value) = cache.get(benchmark_key(ordinal)).await? {
            verify_value(ordinal, &value)?;
            if value.len() != config.value_bytes || value[8..].iter().any(|byte| *byte != 0xa5) {
                return Err(io::Error::other("steady write payload mismatch"));
            }
            verified += 1;
        }
    }
    if verified == 0 {
        return Err(io::Error::other(
            "steady write verification found no values",
        ));
    }
    println!("result phase=put_steady_verify hits={verified}");
    Ok(())
}
