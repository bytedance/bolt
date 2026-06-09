#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <folly/Benchmark.h>
#include <gflags/gflags.h>

DECLARE_uint64(bm_row_container_data_bytes);

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

void spillReadOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("spill-read-old", opts.dataBytes, 8);
    auto stored = storeOldRows(context, opts, false);
    auto spill = spillOldRows(context, *stored.container, dataset);
    stored.container.reset();
    suspender.dismiss();
    auto restored = readOldSpillIntoNewRowContainer(context, spill, dataset);
    folly::doNotOptimizeAway(restored->numRows());
    suspender.rehire();
  }
}

void spillReadBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("spill-read-bm", opts.dataBytes);
    auto spill = spillBmRows(context, opts);
    suspender.dismiss();
    readBmSpill(*spill.container, spill.segment, opts);
    suspender.rehire();
  }
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
