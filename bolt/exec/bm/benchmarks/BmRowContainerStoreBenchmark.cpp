#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <folly/Benchmark.h>
#include <gflags/gflags.h>

DECLARE_uint64(bm_row_container_data_bytes);

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
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("store-bm", opts.dataBytes);
    auto container = makeBmRowContainer(dataset, context.bufferManager);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();

    storeReusableInputBatchesBm(*container, input, opts);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
  }
}

BENCHMARK_NAMED_PARAM(storeOld, old_fixed, DatasetKind::kFixed, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(storeBm, bm_fixed, DatasetKind::kFixed, 0);
BENCHMARK_NAMED_PARAM(storeOld, old_variable, DatasetKind::kVariable, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(storeBm, bm_variable, DatasetKind::kVariable, 0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
