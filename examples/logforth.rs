// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

use std::env;
use std::io;

use cache2::{CacheBuilder, StaticConfig};
use logforth::append::Stderr;
use logforth::bridge::log::LogBridge;
use logforth::filter::rustlog::RustLogFilterBuilder;
use logforth::layout::JsonLayout;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    init_logforth();

    let path = env::args_os().nth(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: logforth <cache-data-path>",
        )
    })?;
    let static_config = StaticConfig::new(5 * 4096).with_region_size_bytes(4096);
    let cache = CacheBuilder::from_static(path, static_config)
        .open()
        .await?;
    cache.close_warm().await
}

fn init_logforth() {
    let logger = logforth::core::builder()
        .dispatch(|dispatch| {
            dispatch
                .filter(RustLogFilterBuilder::from_default_env().build())
                .append(Stderr::default().with_layout(JsonLayout::default()))
        })
        .build();
    log::set_boxed_logger(Box::new(LogBridge::new(logger)))
        .expect("logforth must be installed before another global logger");
    log::set_max_level(log::LevelFilter::Trace);
}
