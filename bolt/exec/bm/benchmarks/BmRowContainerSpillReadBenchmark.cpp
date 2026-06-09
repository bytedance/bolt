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

void printOldReadMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    const OldSpillReadMetrics& metrics) {
  if (!shouldPrintSpillMetrics("spillReadOld", dataset)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] spillReadOld dataset={} iterations={} "
      "logical_bytes={} rows={} serialized_bytes={} batches={} "
      "create_reader_ms={:.3f} next_batch_ms={:.3f} copy_rows_ms={:.3f} "
      "list_rows_ms={:.3f}\n",
      datasetName(dataset),
      iterations,
      opts.dataBytes,
      metrics.rows,
      metrics.serializedBytes,
      metrics.batches,
      nsToMs(metrics.createReaderNs),
      nsToMs(metrics.nextBatchNs),
      nsToMs(metrics.copyRowsNs),
      nsToMs(metrics.listRowsNs));
}

void printBmReadMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    const BmSpillReadMetrics& metrics) {
  if (!shouldPrintSpillMetrics("spillReadBm", dataset)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  const auto& stats = metrics.statsDelta;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] spillReadBm dataset={} iterations={} "
      "logical_bytes={} rows={} row_ids={} windows={} result={} "
      "begin_ms={:.3f} try_load_all_ms={:.3f} window_load_ms={:.3f} "
      "bm_batch_pins={} bm_pin_reads={} bm_spill_read_count={} "
      "bm_spill_read_bytes={} bm_spill_physical_read_bytes={} "
      "bm_decompress_ms={:.3f}\n",
      datasetName(dataset),
      iterations,
      opts.dataBytes,
      metrics.rows,
      metrics.rowIds,
      metrics.windows,
      metrics.result == LoadAllResult::kLoadedPointers ? "pointers" : "row_ids",
      nsToMs(metrics.beginNs),
      nsToMs(metrics.tryLoadAllNs),
      nsToMs(metrics.windowLoadNs),
      stats.batchPinCount,
      stats.pinReadCount,
      stats.spillReadCount,
      stats.spillReadBytes,
      stats.spillPhysicalReadBytes,
      static_cast<double>(stats.spillDecompressionTimeUs) / 1000.0);
}

void spillReadOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  OldSpillReadMetrics metrics;
  BenchmarkOptions printedOpts;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    printedOpts = opts;
    BenchmarkContext context("spill-read-old", opts.dataBytes, 8);
    auto stored = storeOldRows(context, opts, false);
    auto spill = spillOldRows(context, *stored.container, dataset);
    stored.container.reset();
    std::vector<char*> restoredRows;
    restoredRows.reserve(rowCount(opts));
    suspender.dismiss();
    auto restored = readOldSpillIntoNewRowContainer(
        context,
        spill,
        dataset,
        FLAGS_bm_row_container_spill_metrics ? &metrics : nullptr,
        &restoredRows);
    folly::doNotOptimizeAway(restored->numRows());
    folly::doNotOptimizeAway(restoredRows.data());
    folly::doNotOptimizeAway(restoredRows.size());
    suspender.rehire();
  }
  printOldReadMetrics(dataset, iterations, printedOpts, metrics);
}

void spillReadBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  BmSpillReadMetrics metrics;
  BenchmarkOptions printedOpts;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    printedOpts = opts;
    BenchmarkContext context("spill-read-bm", opts.dataBytes);
    auto spill = spillBmRows(context, opts);
    const auto statsBefore = context.bufferManager->stats();
    suspender.dismiss();
    readBmSpill(
        *spill.container,
        spill.segment,
        opts,
        FLAGS_bm_row_container_spill_metrics ? &metrics : nullptr);
    suspender.rehire();
    if (FLAGS_bm_row_container_spill_metrics) {
      const auto statsAfter = context.bufferManager->stats();
      metrics.statsDelta.batchPinCount +=
          counterDelta(statsBefore.batchPinCount, statsAfter.batchPinCount);
      metrics.statsDelta.pinReadCount +=
          counterDelta(statsBefore.pinReadCount, statsAfter.pinReadCount);
      metrics.statsDelta.spillReadCount +=
          counterDelta(statsBefore.spillReadCount, statsAfter.spillReadCount);
      metrics.statsDelta.spillReadBytes +=
          counterDelta(statsBefore.spillReadBytes, statsAfter.spillReadBytes);
      metrics.statsDelta.spillPhysicalReadBytes += counterDelta(
          statsBefore.spillPhysicalReadBytes,
          statsAfter.spillPhysicalReadBytes);
      metrics.statsDelta.spillDecompressionTimeUs += counterDelta(
          statsBefore.spillDecompressionTimeUs,
          statsAfter.spillDecompressionTimeUs);
    }
  }
  printBmReadMetrics(dataset, iterations, printedOpts, metrics);
}

BENCHMARK_NAMED_PARAM(spillReadOld, old_fixed, DatasetKind::kFixed, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_fixed,
    DatasetKind::kFixed,
    0);
BENCHMARK_NAMED_PARAM(spillReadOld, old_variable, DatasetKind::kVariable, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
