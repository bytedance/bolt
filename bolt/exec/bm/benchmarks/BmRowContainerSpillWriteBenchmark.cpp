#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <folly/Benchmark.h>
#include <gflags/gflags.h>

DECLARE_uint64(bm_row_container_data_bytes);

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

void spillWriteOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("spill-write-old", opts.dataBytes);
    auto stored = storeOldRows(context, opts, false);
    suspender.dismiss();
    auto spill = spillOldRows(context, *stored.container, dataset);
    folly::doNotOptimizeAway(spill.partition.rowCount());
    suspender.rehire();
  }
}

void spillWriteBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("spill-write-bm", opts.dataBytes);
    auto stored = storeBmRows(context, opts, false);
    suspender.dismiss();
    const auto segment = stored.container->flushActiveSegment();
    folly::doNotOptimizeAway(segment);
    suspender.rehire();
  }
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
