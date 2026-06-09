#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <folly/Benchmark.h>
#include <gflags/gflags.h>

DECLARE_uint64(bm_row_container_data_bytes);

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

constexpr uint64_t kOneGiB = 1ULL << 30;
constexpr uint64_t kTenGiB = 10ULL << 30;

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

void spillReadOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("spill-read-old", opts.dataBytes);
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
    std::vector<char*> rows;
    std::vector<RowId> rowIds;
    suspender.dismiss();
    auto session =
        spill.container->beginBulkReadSegments({&spill.segment, 1});
    auto result = session.tryLoadAll(rows, rowIds);
    if (result == LoadAllResult::kLoadedPointers) {
      folly::doNotOptimizeAway(rows.data());
    } else {
      extractBmRowsFromRowIds(
          *spill.container, session, rowIds, opts, context.pool.get());
    }
    suspender.rehire();
  }
}

BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_fixed_1g,
    DatasetKind::kFixed,
    kOneGiB);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_fixed_1g,
    DatasetKind::kFixed,
    kOneGiB);
BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_fixed_10g,
    DatasetKind::kFixed,
    kTenGiB);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_fixed_10g,
    DatasetKind::kFixed,
    kTenGiB);
BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_variable_1g,
    DatasetKind::kVariable,
    kOneGiB);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_variable_1g,
    DatasetKind::kVariable,
    kOneGiB);
BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_variable_10g,
    DatasetKind::kVariable,
    kTenGiB);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_variable_10g,
    DatasetKind::kVariable,
    kTenGiB);
BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_fixed_custom,
    DatasetKind::kFixed,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_fixed_custom,
    DatasetKind::kFixed,
    0);
BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_variable_custom,
    DatasetKind::kVariable,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_variable_custom,
    DatasetKind::kVariable,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
