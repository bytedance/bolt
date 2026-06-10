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
  if (!shouldPrintSpillMetrics("spillWriteOld", dataset)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] spillWriteOld dataset={} iterations={} "
      "logical_bytes={} rows={} store_setup_ms={:.3f} spill_ms={:.3f} "
      "spill_bytes={} files={}\n",
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
  if (!shouldPrintSpillMetrics("spillWriteBm", dataset)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] spillWriteBm dataset={} iterations={} "
      "logical_bytes={} rows={} store_setup_ms={:.3f} flush_ms={:.3f} "
      "bm_spill_write_count={} bm_spill_write_bytes={} "
      "bm_spill_physical_write_bytes={} bm_compress_ms={:.3f} "
      "bm_compressed_blocks={}\n",
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

void spillWriteOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  OldSpillWriteMetrics metrics;
  BenchmarkOptions printedOpts;
  uint64_t storeNs = 0;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    checkOldRowBasedSpillBenchmarkSupported(opts);
    printedOpts = opts;
    BenchmarkContext context("spill-write-old", opts.dataBytes);
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

void spillWriteBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  BenchmarkOptions printedOpts;
  uint64_t storeNs = 0;
  uint64_t flushNs = 0;
  memory::bm::BufferManagerStats stats;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    printedOpts = opts;
    BenchmarkContext context("spill-write-bm", opts.dataBytes);
    const auto storeStart = benchmarkNowNs();
    auto stored = storeBmRows(context, opts, false);
    storeNs += benchmarkNowNs() - storeStart;
    const auto statsBefore = context.bufferManager->stats();
    suspender.dismiss();
    const auto flushStart = benchmarkNowNs();
    const auto segment = stored.container->flushActiveSegment();
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
  printBmWriteMetrics(dataset, iterations, printedOpts, storeNs, flushNs, stats);
}

BENCHMARK_NAMED_PARAM(spillWriteOld, old_fixed, DatasetKind::kFixed, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_fixed,
    DatasetKind::kFixed,
    0);
BENCHMARK_NAMED_PARAM(spillWriteOld, old_variable, DatasetKind::kVariable, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
