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

enum class StorePhase {
  kAppendOnly,
  kAppendFixed,
  kAppendFull,
};

struct StorePhaseTimings {
  uint64_t rows{0};
  uint64_t appendOnlyNs{0};
  uint64_t appendFixedNs{0};
  uint64_t appendFullNs{0};
};

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

bool isStringKind(TypeKind kind) {
  return kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY;
}

RowVectorPtr prefixRows(const RowVectorPtr& batch, vector_size_t size) {
  if (size == batch->size()) {
    return batch;
  }
  return std::dynamic_pointer_cast<RowVector>(batch->slice(0, size));
}

void storeInputBatchBmPhase(
    BmRowContainer& container,
    const RowVectorPtr& batch,
    StorePhase phase) {
  std::vector<DecodedVector> decoded(batch->childrenSize());
  if (phase != StorePhase::kAppendOnly) {
    SelectivityVector rows(batch->size());
    for (auto column = 0; column < batch->childrenSize(); ++column) {
      const auto kind = batch->childAt(column)->type()->kind();
      if (phase == StorePhase::kAppendFixed && isStringKind(kind)) {
        continue;
      }
      decoded[column].decode(*batch->childAt(column), rows);
    }
  }

  for (vector_size_t row = 0; row < batch->size(); ++row) {
    auto context = container.appendRow(kDefaultPartition);
    if (phase == StorePhase::kAppendOnly) {
      continue;
    }
    for (auto column = 0; column < batch->childrenSize(); ++column) {
      const auto kind = batch->childAt(column)->type()->kind();
      if (phase == StorePhase::kAppendFixed && isStringKind(kind)) {
        continue;
      }
      container.store(context, decoded[column], row, column);
    }
  }
}

void storeReusableInputBatchesBmPhase(
    BmRowContainer& container,
    const ReusableInputBatches& input,
    const BenchmarkOptions& options,
    StorePhase phase) {
  BOLT_CHECK(!input.batches.empty());
  size_t nextBatch = 0;
  uint64_t remaining = rowCount(options);
  while (remaining > 0) {
    const auto& batch = input.batches[nextBatch];
    const auto batchRows = static_cast<vector_size_t>(
        std::min<uint64_t>(batch->size(), remaining));
    storeInputBatchBmPhase(container, prefixRows(batch, batchRows), phase);
    remaining -= batchRows;
    nextBatch = (nextBatch + 1) % input.batches.size();
  }
}

uint64_t measureStorePhaseNs(
    DatasetKind dataset,
    const BenchmarkOptions& opts,
    StorePhase phase) {
  BenchmarkContext context("store-bm-phase-metrics", opts.dataBytes);
  auto container = makeBmRowContainer(dataset, context.bufferManager);
  auto input = makeReusableInputBatches(context.pool.get(), opts);
  const auto start = benchmarkNowNs();
  storeReusableInputBatchesBmPhase(*container, input, opts, phase);
  const auto elapsed = benchmarkNowNs() - start;
  folly::doNotOptimizeAway(container->numRows());
  return elapsed;
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
    StorePhaseTimings timings;
    timings.rows = rowCount(metricOpts);
    timings.appendOnlyNs =
        measureStorePhaseNs(dataset, metricOpts, StorePhase::kAppendOnly);
    timings.appendFixedNs =
        measureStorePhaseNs(dataset, metricOpts, StorePhase::kAppendFixed);
    timings.appendFullNs =
        measureStorePhaseNs(dataset, metricOpts, StorePhase::kAppendFull);
    const auto fixedExtraNs =
        timings.appendFixedNs > timings.appendOnlyNs
        ? timings.appendFixedNs - timings.appendOnlyNs
        : 0;
    const auto variableExtraNs =
        timings.appendFullNs > timings.appendFixedNs
        ? timings.appendFullNs - timings.appendFixedNs
        : 0;
    fmt::print(
        stderr,
        "[bm-row-container-metrics] storeRowBm dataset={} "
        "benchmark_iterations={} diagnostic_iterations=1 "
        "benchmark_logical_bytes={} logical_bytes={} "
        "rows={} append_only_ms={:.3f} append_fixed_ms={:.3f} "
        "append_full_ms={:.3f} fixed_store_extra_ms={:.3f} "
        "variable_store_extra_ms={:.3f}\n",
        datasetName(dataset),
        iterations,
        printedOpts.dataBytes,
        metricOpts.dataBytes,
        timings.rows,
        nsToMs(timings.appendOnlyNs),
        nsToMs(timings.appendFixedNs),
        nsToMs(timings.appendFullNs),
        nsToMs(fixedExtraNs),
        nsToMs(variableExtraNs));
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
BENCHMARK_NAMED_PARAM(
    storeRowOld,
    old_variable_large,
    DatasetKind::kVariableLarge,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    storeRowBm,
    bm_variable_large,
    DatasetKind::kVariableLarge,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
