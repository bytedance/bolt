#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <fmt/core.h>
#include <folly/Benchmark.h>
#include <gflags/gflags.h>

DECLARE_uint64(bm_row_container_data_bytes);
DECLARE_bool(bm_row_container_spill_metrics);

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

void printOldWriteMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    uint64_t storeNs,
    const OldSpillWriteMetrics& metrics) {
  if (!shouldPrintSpillMetrics(
          "spillWriteOld", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] spillWriteOld compression={} dataset={} "
      "iterations={} logical_bytes={} rows={} store_setup_ms={:.3f} "
      "spill_ms={:.3f} spill_bytes={} files={}\n",
      spillCompressionName(opts.compression),
      datasetName(dataset),
      iterations,
      opts.dataBytes,
      metrics.rows,
      nsToMs(storeNs),
      nsToMs(metrics.spillNs),
      metrics.spillBytes,
      metrics.files);
}

void printBmWriteMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    uint64_t storeNs,
    uint64_t flushNs,
    const memory::bm::BufferManagerStats& stats) {
  if (!shouldPrintSpillMetrics("spillWriteBm", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] spillWriteBm compression={} dataset={} "
      "iterations={} logical_bytes={} rows={} store_setup_ms={:.3f} "
      "flush_ms={:.3f} bm_spill_write_count={} bm_spill_write_bytes={} "
      "bm_spill_physical_write_bytes={} bm_compress_ms={:.3f} "
      "bm_compressed_blocks={}\n",
      spillCompressionName(opts.compression),
      datasetName(dataset),
      iterations,
      opts.dataBytes,
      rowCount(opts) * iterations,
      nsToMs(storeNs),
      nsToMs(flushNs),
      stats.spillWriteCount,
      stats.spillWriteBytes,
      stats.spillPhysicalWriteBytes,
      static_cast<double>(stats.spillCompressionTimeUs) / 1000.0,
      stats.spillCompressedBlocks);
}

void spillWriteOld(
    uint32_t iterations,
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupSpillWriteOld(options(dataset, dataBytes(bytes), compression));
    suspender.dismiss();
  }
  OldSpillWriteMetrics metrics;
  BenchmarkOptions printedOpts;
  uint64_t storeNs = 0;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes), compression);
    checkOldRowBasedSpillBenchmarkSupported(opts);
    printedOpts = opts;
    BenchmarkContext context(
        "spill-write-old", opts.dataBytes, 0, opts.compression);
    const auto storeStart = benchmarkNowNs();
    auto stored = storeOldRows(context, opts, false);
    storeNs += benchmarkNowNs() - storeStart;
    suspender.dismiss();
    auto spill = spillOldRows(
        context,
        *stored.container,
        dataset,
        FLAGS_bm_row_container_spill_metrics ? &metrics : nullptr);
    folly::doNotOptimizeAway(spill.partition.rowCount());
    suspender.rehire();
  }
  printOldWriteMetrics(dataset, iterations, printedOpts, storeNs, metrics);
}

void spillWriteBm(
    uint32_t iterations,
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupSpillWriteBm(options(dataset, dataBytes(bytes), compression));
    suspender.dismiss();
  }
  BenchmarkOptions printedOpts;
  uint64_t storeNs = 0;
  uint64_t flushNs = 0;
  memory::bm::BufferManagerStats stats;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes), compression);
    printedOpts = opts;
    BenchmarkContext context(
        "spill-write-bm", opts.dataBytes, 0, opts.compression);
    const auto storeStart = benchmarkNowNs();
    auto stored = storeBmRows(context, opts, false);
    storeNs += benchmarkNowNs() - storeStart;
    const auto statsBefore = context.bufferManager->stats();
    suspender.dismiss();
    const auto flushStart = benchmarkNowNs();
    const auto segment = stored.container->spillActiveSegment();
    flushNs += benchmarkNowNs() - flushStart;
    folly::doNotOptimizeAway(segment);
    suspender.rehire();
    if (FLAGS_bm_row_container_spill_metrics) {
      const auto statsAfter = context.bufferManager->stats();
      stats.spillWriteCount +=
          counterDelta(statsBefore.spillWriteCount, statsAfter.spillWriteCount);
      stats.spillWriteBytes +=
          counterDelta(statsBefore.spillWriteBytes, statsAfter.spillWriteBytes);
      stats.spillPhysicalWriteBytes += counterDelta(
          statsBefore.spillPhysicalWriteBytes,
          statsAfter.spillPhysicalWriteBytes);
      stats.spillCompressionTimeUs += counterDelta(
          statsBefore.spillCompressionTimeUs,
          statsAfter.spillCompressionTimeUs);
      stats.spillCompressedBlocks += counterDelta(
          statsBefore.spillCompressedBlocks,
          statsAfter.spillCompressedBlocks);
    }
  }
  printBmWriteMetrics(
      dataset,
      iterations,
      printedOpts,
      storeNs,
      flushNs,
      stats);
}

BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_raw_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_raw_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_lz4_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_lz4_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_zstd_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_zstd_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_raw_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_raw_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_lz4_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_lz4_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_zstd_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_zstd_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
