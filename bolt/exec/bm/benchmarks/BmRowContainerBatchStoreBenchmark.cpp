#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <folly/Benchmark.h>
#include <gflags/gflags.h>

DECLARE_uint64(bm_row_container_data_bytes);

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

void storeBatchOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupStoreBatchOld(options(dataset, dataBytes(bytes)));
    suspender.dismiss();
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("store-batch-old", opts.dataBytes);
    auto container = makeOldRowContainer(dataset, context.pool.get());
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();

    storeReusableInputBatchesOldBatch(*container, input, opts);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
  }
}

void storeBatchBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupStoreBatchBm(options(dataset, dataBytes(bytes)));
    suspender.dismiss();
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("store-batch-bm", opts.dataBytes);
    auto container = makeBmRowContainer(dataset, context.bufferManager);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();

    storeReusableInputBatchesBmBatch(*container, input, opts);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
  }
}

BENCHMARK_NAMED_PARAM(storeBatchOld, old_fixed, DatasetKind::kFixed, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    storeBatchBm,
    bm_fixed,
    DatasetKind::kFixed,
    0);
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(
    storeBatchOld,
    old_variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    storeBatchBm,
    bm_variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
