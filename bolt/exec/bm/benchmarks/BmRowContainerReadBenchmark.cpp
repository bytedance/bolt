#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <folly/Benchmark.h>
#include <gflags/gflags.h>

DECLARE_uint64(bm_row_container_data_bytes);

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

void readOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupReadOld(options(dataset, dataBytes(bytes)));
    suspender.dismiss();
  }
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
  {
    folly::BenchmarkSuspender suspender;
    warmupReadBm(options(dataset, dataBytes(bytes)));
    suspender.dismiss();
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("read-bm", opts.dataBytes);
    auto stored = storeBmRows(context, opts, true);
    suspender.dismiss();
    extractBmRowsResident(
        *stored.container, stored.rows, opts, context.pool.get());
    suspender.rehire();
  }
}

BENCHMARK_NAMED_PARAM(readOld, old_fixed, DatasetKind::kFixed, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(readBm, bm_fixed, DatasetKind::kFixed, 0);
BENCHMARK_NAMED_PARAM(readOld, old_variable, DatasetKind::kVariable, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(readBm, bm_variable, DatasetKind::kVariable, 0);
BENCHMARK_NAMED_PARAM(
    readOld,
    old_variable_large,
    DatasetKind::kVariableLarge,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    readBm,
    bm_variable_large,
    DatasetKind::kVariableLarge,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
