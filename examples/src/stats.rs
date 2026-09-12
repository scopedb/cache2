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
use std::num::NonZeroU32;

use cache2::Cache;
use cache2::CacheConfig;
use cache2::LatencyMode;
use cache2::RuntimeOptions;
use cache2::StatsOptions;
use cache2::StorageOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args_os()
        .nth(1)
        .ok_or("usage: stats <cache-data-path>")?;
    let storage = StorageOptions {
        region_size_bytes: 1024 * 1024,
        ..StorageOptions::new(8 * 1024 * 1024)
    }
    .build()?;
    let runtime = RuntimeOptions {
        statistics: true,
        stats: StatsOptions {
            request_counters: true,
            l1_latency: LatencyMode::Sampled {
                interval: NonZeroU32::new(64).unwrap(),
            },
            l2_latency: LatencyMode::Full,
            io_latency: true,
            ..StatsOptions::default()
        },
        ..RuntimeOptions::default()
    };
    let cache = Cache::open(path, CacheConfig::new(storage, runtime)?).await?;
    cache.put("example", "value")?;
    let _value = cache.get("example").await?;
    cache.drain().await?;
    // Pass this owned, cumulative snapshot to the application's metrics adapter.
    // It carries collection modes/scopes; bucket bounds and sums use nanoseconds.
    let stats = cache.stats_snapshot()?;
    println!("{stats:#?}");
    cache.close_fast().await?;
    Ok(())
}
