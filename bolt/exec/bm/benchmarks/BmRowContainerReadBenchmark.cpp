/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
BENCHMARK_NAMED_PARAM(
    readOld,
    old_variable_small,
    DatasetKind::kVariableSmall,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    readBm,
    bm_variable_small,
    DatasetKind::kVariableSmall,
    0);
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
