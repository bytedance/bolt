# Bolt ClickBench

This benchmark executes the 43-query ClickBench analytical workload directly
with Bolt plans over the official single-file Parquet dataset. Bolt is an
execution library rather than a SQL database, so the SQL semantics are
expressed explicitly by `ClickBenchQueryBuilder`; no SQL parser, optimizer, or
external plan service is part of the measured path.

The query definitions were independently translated with reference to
[ClickBench](https://github.com/ClickHouse/ClickBench) commit
`642fd174f5edfbbf2b5805b28daab885a89fa217`, especially its Presto and Velox
variants. ClickBench is CC BY-NC-SA 4.0; its source files are not copied into
this Apache-2.0 repository.

## Build

```bash
make benchmarks-build
```

The executable is normally written to:

```text
_build/Release/bolt/benchmarks/clickbench/bolt_clickbench_benchmark
```

## Prepare data

The official file is about 14.8 GB. Download it once outside the source tree:

```bash
CLICKBENCH_DOWNLOAD_PARTS=16 scripts/clickbench/prepare-data.sh \
  /data00/home/liuneng/code/data
```

Both `/data00/home/liuneng/code/data/hits.parquet` and
`/data00/home/liuneng/code/data` are accepted as `--data-path`. A Hive-style
`/data00/home/liuneng/code/data/hits/*.parquet` layout is also supported.

## Run one query

```bash
_build/Release/bolt/benchmarks/clickbench/bolt_clickbench_benchmark \
  --bolt_benchmark_clickbench_data_path=/data00/home/liuneng/code/data \
  --bolt_benchmark_clickbench_query=1 \
  --bolt_benchmark_num_drivers=16
```

Add `--bolt_benchmark_include_results` when validating result values. This is
off by default so result formatting does not contaminate execution timing.

## Run the ClickBench protocol

```bash
scripts/clickbench/run-clickbench.py \
  --binary _build/Release/bolt/benchmarks/clickbench/bolt_clickbench_benchmark \
  --data-path /data00/home/liuneng/code/data \
  --drop-os-cache \
  --output clickbench-results.json
```

The runner prints the standard 43 lines of `[cold,hot1,hot2],` values and also
writes a JSON artifact containing parameters, raw process output, failures,
and per-attempt timings. `--drop-os-cache` requires passwordless sudo and only
flushes before the first try of each query. Without that option the first value
is not a true cold-cache result.

For comparing two Bolt revisions, run seven alternating rounds with the
standalone `bolt-benchmark-compare` tool. The three-try runner here follows the
ClickBench publication protocol; it is not a statistical regression test.
