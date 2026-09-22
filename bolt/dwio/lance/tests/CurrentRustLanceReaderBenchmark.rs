// Copyright (c) ByteDance Ltd. and/or its affiliates.
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
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use lance_core::cache::LanceCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_io::ReadBatchParams;
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;

fn env_u64(name: &str, default: u64) -> anyhow::Result<u64> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

async fn scan() -> anyhow::Result<(u64, usize)> {
    let file = env::var("BOLT_LANCE_BENCHMARK_FILE")?;
    let batch_size = u32::try_from(env_u64("BOLT_LANCE_BENCHMARK_BATCH_SIZE", 1024)?)?;
    let batch_readahead = u32::try_from(env_u64("BOLT_LANCE_BENCHMARK_READAHEAD", 16)?)?;
    let expected_rows = env::var("BOLT_LANCE_BENCHMARK_EXPECTED_ROWS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?;

    let started = Instant::now();
    let (object_store, path) = ObjectStore::from_uri(&file).await?;
    let scan_scheduler = ScanScheduler::new(
        object_store,
        SchedulerConfig::new(env_u64(
            "BOLT_LANCE_BENCHMARK_IO_BUFFER_BYTES",
            256 * 1024 * 1024,
        )?),
    );
    let file_scheduler = scan_scheduler
        .open_file(&path, &CachedFileSize::unknown())
        .await?;
    let cache = LanceCache::with_capacity(256 * 1024 * 1024);
    let reader = FileReader::try_open(
        file_scheduler,
        None,
        Arc::<DecoderPlugins>::default(),
        &cache,
        FileReaderOptions::default(),
    )
    .await?;
    let opened = Instant::now();
    let mut stream = reader
        .read_stream(
            ReadBatchParams::RangeFull,
            batch_size,
            batch_readahead,
            FilterExpression::no_filter(),
        )
        .await?;
    let stream_created = Instant::now();

    let mut rows = 0_u64;
    let mut retained_bytes = 0_usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        rows += batch.num_rows() as u64;
        for column in batch.columns() {
            retained_bytes = retained_bytes.wrapping_add(column.get_array_memory_size());
            black_box(column);
        }
    }
    if let Some(expected_rows) = expected_rows {
        anyhow::ensure!(
            rows == expected_rows,
            "expected {expected_rows} rows but read {rows}"
        );
    }
    if env::var_os("BOLT_LANCE_BENCHMARK_PRINT_STATS").is_some() {
        eprintln!(
            "BOLT_LANCE_RUST_MAIN_STATS open_ns={} stream_ns={} consume_ns={} rows={} retained_bytes={}",
            opened.duration_since(started).as_nanos(),
            stream_created.duration_since(opened).as_nanos(),
            Instant::now().duration_since(stream_created).as_nanos(),
            rows,
            retained_bytes
        );
    }
    Ok((rows, retained_bytes))
}

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let start = Instant::now();
    let result = runtime.block_on(scan())?;
    let elapsed_ns = start.elapsed().as_nanos();
    anyhow::ensure!(result.0 > 0, "benchmark scan produced no rows");
    // Folly's BENCHMARK_MULTI reports picoseconds per unit of work returned by
    // the benchmark function. NativeLanceReaderBenchmark returns its row count,
    // so emit the same ps/row metric instead of whole-scan nanoseconds.
    let picoseconds_per_row = elapsed_ns as f64 * 1_000.0 / result.0 as f64;
    black_box(result);
    println!("{{\"scan\":{picoseconds_per_row}}}");
    Ok(())
}
