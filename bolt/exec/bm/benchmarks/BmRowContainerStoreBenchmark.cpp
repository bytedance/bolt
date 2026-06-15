#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <fmt/core.h>
#include <folly/Benchmark.h>
#include <gflags/gflags.h>

#include <algorithm>

DECLARE_uint64(bm_row_container_data_bytes);

DEFINE_uint64(
    bm_row_container_store_metric_data_bytes,
    16ULL << 20,
    "Logical input bytes used by storeRowBm diagnostic metrics. Set to 0 to "
    "use the benchmark data bytes.");

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

void storeRowOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupStoreOld(options(dataset, dataBytes(bytes)));
    suspender.dismiss();
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("store-old", opts.dataBytes);
    auto container = makeOldRowContainer(dataset, context.pool.get());
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();

    storeReusableInputBatchesOld(*container, input, opts);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
  }
}

void storeRowBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupStoreBm(options(dataset, dataBytes(bytes)));
    suspender.dismiss();
  }
  BenchmarkOptions printedOpts;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    printedOpts = opts;
    BenchmarkContext context("store-bm", opts.dataBytes);
    auto container = makeBmRowContainer(dataset, context.bufferManager);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();

    storeReusableInputBatchesBm(*container, input, opts);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
  }
  if (shouldPrintSpillMetrics(
          "storeRowBm", dataset, SpillCompressionKind::kRaw)) {
    folly::BenchmarkSuspender suspender;
    auto metricOpts = printedOpts;
    if (FLAGS_bm_row_container_store_metric_data_bytes != 0) {
      metricOpts.dataBytes = std::min<uint64_t>(
          printedOpts.dataBytes,
          FLAGS_bm_row_container_store_metric_data_bytes);
    }
    BenchmarkContext context("store-bm-metrics", metricOpts.dataBytes);
    auto container = makeBmRowContainer(dataset, context.bufferManager);
    auto input = makeReusableInputBatches(context.pool.get(), metricOpts);
    BmStoreMetrics metrics;
    const auto storeStart = benchmarkNowNs();
    storeReusableInputBatchesBm(
        *container, input, metricOpts, nullptr, &metrics);
    const auto storeNs = benchmarkNowNs() - storeStart;
    folly::doNotOptimizeAway(container->numRows());
    fmt::print(
        stderr,
        "[bm-row-container-metrics] storeRowBm dataset={} "
        "benchmark_iterations={} diagnostic_iterations=1 "
        "benchmark_logical_bytes={} logical_bytes={} "
        "rows={} store_ms={:.3f} append_row_ms={:.3f} "
        "fixed_store_ms={:.3f} string_store_ms={:.3f} "
        "heap_ensure_ms={:.3f} string_copy_ms={:.3f} "
        "heap_record_ms={:.3f} fixed_values={} string_values={} "
        "string_bytes={} heap_allocations={}\n",
        datasetName(dataset),
        iterations,
        printedOpts.dataBytes,
        metricOpts.dataBytes,
        metrics.rows,
        nsToMs(storeNs),
        nsToMs(metrics.appendRowNs),
        nsToMs(metrics.fixedStoreNs),
        nsToMs(metrics.stringStoreNs),
        nsToMs(metrics.heapEnsureNs),
        nsToMs(metrics.stringCopyNs),
        nsToMs(metrics.heapRecordNs),
        metrics.fixedValues,
        metrics.stringValues,
        metrics.stringBytes,
        metrics.heapAllocations);
  }
}

BENCHMARK_NAMED_PARAM(storeRowOld, old_fixed, DatasetKind::kFixed, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(storeRowBm, bm_fixed, DatasetKind::kFixed, 0);
BENCHMARK_NAMED_PARAM(storeRowOld, old_variable, DatasetKind::kVariable, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    storeRowBm,
    bm_variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
