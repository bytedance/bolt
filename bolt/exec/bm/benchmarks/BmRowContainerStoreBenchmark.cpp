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

void storeOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
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

void storeBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
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
          "storeBm", dataset, SpillCompressionKind::kRaw)) {
    folly::BenchmarkSuspender suspender;
    BenchmarkContext context("store-bm-metrics", printedOpts.dataBytes);
    auto container = makeBmRowContainer(dataset, context.bufferManager);
    auto input = makeReusableInputBatches(context.pool.get(), printedOpts);
    BmAppendMetrics metrics;
    const auto storeStart = benchmarkNowNs();
    storeReusableInputBatchesBm(
        *container, input, printedOpts, nullptr, &metrics);
    const auto storeNs = benchmarkNowNs() - storeStart;
    fmt::print(
        stderr,
        "[bm-row-container-metrics] storeBm dataset={} benchmark_iterations={} "
        "diagnostic_iterations=1 logical_bytes={} rows={} store_ms={:.3f} "
        "append_row_alloc_ms={:.3f} append_fixed_store_ms={:.3f} "
        "append_string_store_ms={:.3f} append_heap_alloc_ms={:.3f} "
        "append_string_copy_ms={:.3f} append_heap_record_ms={:.3f} "
        "append_rows={} append_string_rows={} append_string_bytes={} "
        "append_heap_allocations={}\n",
        datasetName(dataset),
        iterations,
        printedOpts.dataBytes,
        metrics.rows,
        nsToMs(storeNs),
        nsToMs(metrics.rowAllocNs),
        nsToMs(metrics.fixedStoreNs),
        nsToMs(metrics.stringStoreNs),
        nsToMs(metrics.heapAllocNs),
        nsToMs(metrics.stringCopyNs),
        nsToMs(metrics.heapRecordNs),
        metrics.rows,
        metrics.stringRows,
        metrics.stringBytes,
        metrics.heapAllocations);
  }
}

BENCHMARK_NAMED_PARAM(storeOld, old_fixed, DatasetKind::kFixed, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(storeBm, bm_fixed, DatasetKind::kFixed, 0);
BENCHMARK_NAMED_PARAM(storeOld, old_variable, DatasetKind::kVariable, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(storeBm, bm_variable, DatasetKind::kVariable, 0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
