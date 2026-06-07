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

void readOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("read-old", opts.dataBytes);
    auto stored = storeOldRows(context, opts, true);
    suspender.dismiss();
    extractOldRows(*stored.container, stored.rows, opts, context.pool.get());
    suspender.rehire();
  }
}

void readBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("read-bm", opts.dataBytes);
    auto stored = storeBmRows(context, opts, true);
    suspender.dismiss();
    extractBmRowsResident(
        *stored.container, stored.handles, opts, context.pool.get());
    suspender.rehire();
  }
}

BENCHMARK_NAMED_PARAM(readOld, old_fixed_1g, DatasetKind::kFixed, kOneGiB);
BENCHMARK_RELATIVE_NAMED_PARAM(
    readBm,
    bm_fixed_1g,
    DatasetKind::kFixed,
    kOneGiB);
BENCHMARK_NAMED_PARAM(readOld, old_fixed_10g, DatasetKind::kFixed, kTenGiB);
BENCHMARK_RELATIVE_NAMED_PARAM(
    readBm,
    bm_fixed_10g,
    DatasetKind::kFixed,
    kTenGiB);
BENCHMARK_NAMED_PARAM(readOld, old_variable_1g, DatasetKind::kVariable, kOneGiB);
BENCHMARK_RELATIVE_NAMED_PARAM(
    readBm,
    bm_variable_1g,
    DatasetKind::kVariable,
    kOneGiB);
BENCHMARK_NAMED_PARAM(
    readOld,
    old_variable_10g,
    DatasetKind::kVariable,
    kTenGiB);
BENCHMARK_RELATIVE_NAMED_PARAM(
    readBm,
    bm_variable_10g,
    DatasetKind::kVariable,
    kTenGiB);
BENCHMARK_NAMED_PARAM(readOld, old_fixed_custom, DatasetKind::kFixed, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(readBm, bm_fixed_custom, DatasetKind::kFixed, 0);
BENCHMARK_NAMED_PARAM(readOld, old_variable_custom, DatasetKind::kVariable, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    readBm,
    bm_variable_custom,
    DatasetKind::kVariable,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
