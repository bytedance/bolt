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

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch};
use arrow_select::filter::filter_record_batch;
use futures::StreamExt;
use lance_core::cache::LanceCache;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
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

fn optional_env_u64(name: &str) -> anyhow::Result<Option<u64>> {
    env::var(name)
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(Into::into)
}

fn env_i64(name: &str, default: i64) -> anyhow::Result<i64> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn apply_filter(
    batch: &RecordBatch,
    column: &str,
    minimum: i64,
    maximum: i64,
) -> anyhow::Result<RecordBatch> {
    let index = batch.schema().index_of(column)?;
    let values = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("benchmark filter column '{column}' must be int64"))?;
    let mask = BooleanArray::from_iter(
        values
            .iter()
            .map(|value| value.map(|value| value >= minimum && value <= maximum)),
    );
    Ok(filter_record_batch(batch, &mask)?)
}

fn checksum(batch: &RecordBatch, column: &str) -> anyhow::Result<u64> {
    let index = batch.schema().index_of(column)?;
    let values = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("benchmark checksum column '{column}' must be int64"))?;
    values.iter().try_fold(0_u64, |sum, value| {
        let value = value.ok_or_else(|| {
            anyhow::anyhow!("benchmark checksum column '{column}' contains nulls")
        })?;
        Ok(sum.wrapping_add(value as u64))
    })
}

async fn scan() -> anyhow::Result<(u64, u64, u64, usize, usize)> {
    let file = env::var("BOLT_LANCE_BENCHMARK_FILE")?;
    let batch_size = u32::try_from(env_u64("BOLT_LANCE_BENCHMARK_BATCH_SIZE", 1024)?)?;
    let batch_readahead = u32::try_from(env_u64("BOLT_LANCE_BENCHMARK_READAHEAD", 16)?)?;
    let expected_rows = optional_env_u64("BOLT_LANCE_BENCHMARK_EXPECTED_ROWS")?;
    let expected_output_rows = optional_env_u64("BOLT_LANCE_BENCHMARK_EXPECTED_OUTPUT_ROWS")?;
    let expected_checksum = optional_env_u64("BOLT_LANCE_BENCHMARK_EXPECTED_CHECKSUM")?;
    let scenario =
        env::var("BOLT_LANCE_BENCHMARK_SCENARIO").unwrap_or_else(|_| "full_scan".to_string());
    anyhow::ensure!(
        matches!(
            scenario.as_str(),
            "full_scan" | "full_scan_all_types" | "filter" | "filter_1pct_all_types"
        ),
        "unsupported benchmark scenario '{scenario}'"
    );
    let uses_filter = matches!(scenario.as_str(), "filter" | "filter_1pct_all_types");
    let uses_checksum = scenario != "full_scan" || expected_checksum.is_some();
    let filter_column =
        env::var("BOLT_LANCE_BENCHMARK_FILTER_COLUMN").unwrap_or_else(|_| "filter_key".to_string());
    let checksum_column =
        env::var("BOLT_LANCE_BENCHMARK_CHECKSUM_COLUMN").unwrap_or_else(|_| "row_id".to_string());
    let filter_minimum = env_i64("BOLT_LANCE_BENCHMARK_FILTER_MIN", 0)?;
    let filter_maximum = env_i64("BOLT_LANCE_BENCHMARK_FILTER_MAX", 0)?;
    anyhow::ensure!(
        filter_minimum <= filter_maximum,
        "filter minimum exceeds maximum"
    );

    let started = Instant::now();
    let (object_store, path) = ObjectStore::from_uri(&file).await?;
    let io_parallelism = object_store.io_parallelism();
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

    let mut input_rows = 0_u64;
    let mut output_rows = 0_u64;
    let mut checksum_value = 0_u64;
    let mut retained_bytes = 0_usize;
    while let Some(batch) = stream.next().await {
        let mut batch = batch?;
        input_rows += batch.num_rows() as u64;
        if uses_filter {
            batch = apply_filter(&batch, &filter_column, filter_minimum, filter_maximum)?;
        }
        output_rows += batch.num_rows() as u64;
        if uses_checksum {
            checksum_value = checksum_value.wrapping_add(checksum(&batch, &checksum_column)?);
        }
        for column in batch.columns() {
            retained_bytes = retained_bytes.wrapping_add(column.get_array_memory_size());
            black_box(column);
        }
    }
    if let Some(expected_rows) = expected_rows {
        anyhow::ensure!(
            input_rows == expected_rows,
            "expected {expected_rows} input rows but read {input_rows}"
        );
    }
    if let Some(expected_output_rows) = expected_output_rows {
        anyhow::ensure!(
            output_rows == expected_output_rows,
            "expected {expected_output_rows} output rows but read {output_rows}"
        );
    }
    if let Some(expected_checksum) = expected_checksum {
        anyhow::ensure!(
            checksum_value == expected_checksum,
            "expected checksum {expected_checksum} but computed {checksum_value}"
        );
    }
    if env::var_os("BOLT_LANCE_BENCHMARK_PRINT_STATS").is_some() {
        eprintln!(
            "BOLT_LANCE_RUST_MAIN_STATS scenario={} open_ns={} stream_ns={} consume_ns={} input_rows={} output_rows={} checksum={} retained_bytes={}",
            scenario,
            opened.duration_since(started).as_nanos(),
            stream_created.duration_since(opened).as_nanos(),
            Instant::now().duration_since(stream_created).as_nanos(),
            input_rows,
            output_rows,
            checksum_value,
            retained_bytes
        );
    }
    Ok((
        input_rows,
        output_rows,
        checksum_value,
        retained_bytes,
        io_parallelism,
    ))
}

fn main() -> anyhow::Result<()> {
    let runtime_threads = usize::try_from(env_u64("BOLT_LANCE_BENCHMARK_RUNTIME_THREADS", 1)?)?;
    anyhow::ensure!(
        runtime_threads > 0,
        "BOLT_LANCE_BENCHMARK_RUNTIME_THREADS must be at least 1"
    );
    let compute_threads = get_num_compute_intensive_cpus();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_threads)
        .enable_all()
        .build()?;
    let start = Instant::now();
    let result = runtime.block_on(scan())?;
    let elapsed_ns = start.elapsed().as_nanos();
    anyhow::ensure!(result.0 > 0, "benchmark scan produced no rows");
    if env::var_os("BOLT_LANCE_BENCHMARK_PRINT_STATS").is_some() {
        eprintln!(
            "BOLT_LANCE_RUST_MAIN_CONFIG runtime_threads={} compute_threads={} io_threads={}",
            runtime_threads, compute_threads, result.4
        );
    }
    // Folly's BENCHMARK_MULTI reports picoseconds per unit of work returned by
    // the benchmark function. NativeLanceReaderBenchmark returns its row count,
    // so emit the same ps/row metric instead of whole-scan nanoseconds.
    let picoseconds_per_row = elapsed_ns as f64 * 1_000.0 / result.0 as f64;
    black_box(result);
    println!("{{\"scan\":{picoseconds_per_row}}}");
    Ok(())
}
