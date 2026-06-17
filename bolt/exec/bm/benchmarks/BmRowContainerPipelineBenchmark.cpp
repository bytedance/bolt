#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include "bolt/common/base/Exceptions.h"

#include <fmt/core.h>
#include <folly/Benchmark.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

#include <unistd.h>

DECLARE_uint64(bm_row_container_data_bytes);
DECLARE_uint64(bm_row_container_warmup_data_bytes);
DECLARE_uint32(bm_row_container_profile_ready_sleep_seconds);
DECLARE_bool(bm_row_container_spill_metrics);

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

struct PipelineOldMetrics {
  uint64_t storeNs{0};
  uint64_t spillWriteNs{0};
  uint64_t spillReadNs{0};
  uint64_t readNs{0};
  uint64_t rows{0};
  OldSpillWriteMetrics spillWrite;
  OldSpillReadMetrics spillRead;
};

struct PipelineBmMetrics {
  uint64_t storeNs{0};
  uint64_t spillWriteNs{0};
  uint64_t spillReadNs{0};
  uint64_t readNs{0};
  uint64_t rows{0};
  memory::bm::BufferManagerStats statsDelta;
};

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

bool shouldWarmup() {
  return FLAGS_bm_row_container_warmup_data_bytes != 0;
}

void waitForProfileAttachOnce(
    const char* benchmark,
    DatasetKind dataset,
    SpillCompressionKind compression) {
  if (FLAGS_bm_row_container_profile_ready_sleep_seconds == 0) {
    return;
  }

  static std::atomic_bool readyPrinted{false};
  if (readyPrinted.exchange(true)) {
    return;
  }

  fmt::print(
      stderr,
      "[bm-row-container-profile] Ready pid={} benchmark={} dataset={} "
      "compression={} sleep_seconds={}\n",
      static_cast<int>(::getpid()),
      benchmark,
      datasetName(dataset),
      spillCompressionName(compression),
      FLAGS_bm_row_container_profile_ready_sleep_seconds);
  std::fflush(stderr);
  std::this_thread::sleep_for(
      std::chrono::seconds(
          FLAGS_bm_row_container_profile_ready_sleep_seconds));
}

BenchmarkOptions makeOptions(
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  return options(dataset, dataBytes(bytes), compression);
}

BenchmarkOptions makeWarmupOptions(const BenchmarkOptions& opts) {
  return options(
      opts.dataset,
      FLAGS_bm_row_container_warmup_data_bytes,
      opts.compression);
}

void accumulateBmStatsDelta(
    memory::bm::BufferManagerStats& total,
    const memory::bm::BufferManagerStats& before,
    const memory::bm::BufferManagerStats& after) {
  total.spillWriteCount +=
      counterDelta(before.spillWriteCount, after.spillWriteCount);
  total.spillWriteBytes +=
      counterDelta(before.spillWriteBytes, after.spillWriteBytes);
  total.spillPhysicalWriteBytes += counterDelta(
      before.spillPhysicalWriteBytes, after.spillPhysicalWriteBytes);
  total.spillReadCount +=
      counterDelta(before.spillReadCount, after.spillReadCount);
  total.spillReadBytes +=
      counterDelta(before.spillReadBytes, after.spillReadBytes);
  total.spillPhysicalReadBytes += counterDelta(
      before.spillPhysicalReadBytes, after.spillPhysicalReadBytes);
}

void printOldMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    const PipelineOldMetrics& metrics) {
  if (!shouldPrintSpillMetrics("pipelineOld", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] pipelineOld dataset={} compression={} "
      "iterations={} logical_bytes={} rows={} store_ms={:.3f} "
      "spill_write_ms={:.3f} spill_read_ms={:.3f} read_ms={:.3f} "
      "total_ms={:.3f} spill_bytes={} files={} batches={}\n",
      datasetName(dataset),
      spillCompressionName(opts.compression),
      iterations,
      opts.dataBytes,
      metrics.rows,
      nsToMs(metrics.storeNs),
      nsToMs(metrics.spillWriteNs),
      nsToMs(metrics.spillReadNs),
      nsToMs(metrics.readNs),
      nsToMs(
          metrics.storeNs + metrics.spillWriteNs + metrics.spillReadNs +
          metrics.readNs),
      metrics.spillWrite.spillBytes,
      metrics.spillWrite.files,
      metrics.spillRead.batches);
}

void printBmMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    const PipelineBmMetrics& metrics) {
  if (!shouldPrintSpillMetrics("pipelineBm", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  const auto& stats = metrics.statsDelta;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] pipelineBm dataset={} compression={} "
      "iterations={} logical_bytes={} rows={} store_ms={:.3f} "
      "spill_write_ms={:.3f} spill_read_ms={:.3f} read_ms={:.3f} "
      "total_ms={:.3f} spill_write_count={} spill_write_bytes={} "
      "spill_physical_write_bytes={} spill_read_count={} spill_read_bytes={} "
      "spill_physical_read_bytes={}\n",
      datasetName(dataset),
      spillCompressionName(opts.compression),
      iterations,
      opts.dataBytes,
      metrics.rows,
      nsToMs(metrics.storeNs),
      nsToMs(metrics.spillWriteNs),
      nsToMs(metrics.spillReadNs),
      nsToMs(metrics.readNs),
      nsToMs(
          metrics.storeNs + metrics.spillWriteNs + metrics.spillReadNs +
          metrics.readNs),
      stats.spillWriteCount,
      stats.spillWriteBytes,
      stats.spillPhysicalWriteBytes,
      stats.spillReadCount,
      stats.spillReadBytes,
      stats.spillPhysicalReadBytes);
}

void runOldPipelineOnce(
    const BenchmarkOptions& opts,
    PipelineOldMetrics* metrics) {
  checkOldRowBasedSpillBenchmarkSupported(opts);

  folly::BenchmarkSuspender suspender;
  BenchmarkContext context(
      "pipeline-old", opts.dataBytes, 8, opts.compression);
  auto input = makeReusableInputBatches(context.pool.get(), opts);
  auto container = makeOldRowContainer(opts.dataset, context.pool.get());
  if (metrics != nullptr) {
    suspender.dismiss();
  }

  const auto storeStart = metrics == nullptr ? 0 : benchmarkNowNs();
  storeReusableInputBatchesOldBatch(*container, input, opts);
  if (metrics != nullptr) {
    metrics->storeNs += benchmarkNowNs() - storeStart;
    metrics->rows += rowCount(opts);
  }

  const auto spillWriteStart = metrics == nullptr ? 0 : benchmarkNowNs();
  auto spill = spillOldRows(
      context,
      *container,
      opts.dataset,
      metrics != nullptr && FLAGS_bm_row_container_spill_metrics
          ? &metrics->spillWrite
          : nullptr);
  if (metrics != nullptr) {
    metrics->spillWriteNs += benchmarkNowNs() - spillWriteStart;
  }
  container.reset();

  std::vector<char*> restoredRows;
  restoredRows.reserve(rowCount(opts));
  const auto spillReadStart = metrics == nullptr ? 0 : benchmarkNowNs();
  auto restored = readOldSpillIntoNewRowContainer(
      context,
      spill,
      opts.dataset,
      metrics != nullptr && FLAGS_bm_row_container_spill_metrics
          ? &metrics->spillRead
          : nullptr,
      &restoredRows);
  if (metrics != nullptr) {
    metrics->spillReadNs += benchmarkNowNs() - spillReadStart;
  }
  BOLT_CHECK_EQ(rowCount(opts), restoredRows.size());

  const auto readStart = metrics == nullptr ? 0 : benchmarkNowNs();
  extractOldRows(*restored, restoredRows, opts, context.pool.get());
  if (metrics != nullptr) {
    metrics->readNs += benchmarkNowNs() - readStart;
  }
  folly::doNotOptimizeAway(restored->numRows());
  if (metrics != nullptr) {
    suspender.rehire();
  }
}

void runBmPipelineOnce(const BenchmarkOptions& opts, PipelineBmMetrics* metrics) {
  folly::BenchmarkSuspender suspender;
  BenchmarkContext context("pipeline-bm", opts.dataBytes, 0, opts.compression);
  auto input = makeReusableInputBatches(context.pool.get(), opts);
  auto container = makeBmRowContainer(opts.dataset, context.bufferManager);
  if (metrics != nullptr) {
    waitForProfileAttachOnce("pipelineBm", opts.dataset, opts.compression);
  }
  memory::bm::BufferManagerStats statsBefore;
  if (metrics != nullptr && FLAGS_bm_row_container_spill_metrics) {
    statsBefore = context.bufferManager->stats();
  }
  if (metrics != nullptr) {
    suspender.dismiss();
  }

  const auto storeStart = metrics == nullptr ? 0 : benchmarkNowNs();
  storeReusableInputBatchesBmBatch(*container, input, opts);
  if (metrics != nullptr) {
    metrics->storeNs += benchmarkNowNs() - storeStart;
    metrics->rows += rowCount(opts);
  }

  const auto spillWriteStart = metrics == nullptr ? 0 : benchmarkNowNs();
  const auto segment = container->spillActiveSegment();
  if (metrics != nullptr) {
    metrics->spillWriteNs += benchmarkNowNs() - spillWriteStart;
  }

  const auto spillReadStart = metrics == nullptr ? 0 : benchmarkNowNs();
  auto bulk = container->beginBulkReadSegments({&segment, 1});
  auto rows = bulk.loadRows();
  if (metrics != nullptr) {
    metrics->spillReadNs += benchmarkNowNs() - spillReadStart;
  }
  BOLT_CHECK_EQ(rowCount(opts), rows.size());

  const auto readStart = metrics == nullptr ? 0 : benchmarkNowNs();
  extractBmRowsResident(*container, rows, opts, context.pool.get());
  if (metrics != nullptr) {
    metrics->readNs += benchmarkNowNs() - readStart;
  }

  folly::doNotOptimizeAway(container->numRows());
  folly::doNotOptimizeAway(rows.data());
  if (metrics != nullptr) {
    suspender.rehire();
    if (FLAGS_bm_row_container_spill_metrics) {
      const auto statsAfter = context.bufferManager->stats();
      accumulateBmStatsDelta(metrics->statsDelta, statsBefore, statsAfter);
    }
  }
}

void pipelineOld(
    uint32_t iterations,
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  PipelineOldMetrics metrics;
  const auto printedOpts = makeOptions(dataset, compression, bytes);
  for (uint32_t i = 0; i < iterations; ++i) {
    if (shouldWarmup()) {
      runOldPipelineOnce(makeWarmupOptions(printedOpts), nullptr);
    }
    runOldPipelineOnce(printedOpts, &metrics);
  }
  printOldMetrics(dataset, iterations, printedOpts, metrics);
}

void pipelineBm(
    uint32_t iterations,
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  PipelineBmMetrics metrics;
  const auto printedOpts = makeOptions(dataset, compression, bytes);
  for (uint32_t i = 0; i < iterations; ++i) {
    if (shouldWarmup()) {
      runBmPipelineOnce(makeWarmupOptions(printedOpts), nullptr);
    }
    runBmPipelineOnce(printedOpts, &metrics);
  }
  printBmMetrics(dataset, iterations, printedOpts, metrics);
}

BENCHMARK_NAMED_PARAM(
    pipelineOld,
    old_raw_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_raw_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld,
    old_lz4_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_lz4_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld,
    old_zstd_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_zstd_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kZstd,
    0);

BENCHMARK_NAMED_PARAM(
    pipelineOld,
    old_raw_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_raw_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld,
    old_lz4_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_lz4_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld,
    old_zstd_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_zstd_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld,
    old_raw_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_raw_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld,
    old_lz4_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_lz4_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld,
    old_zstd_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_zstd_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
