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

/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/common/testutil/GPerf.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

DEFINE_int64(
    row_container_varchar_orderby_payload_bytes,
    2L << 30,
    "Approximate varchar payload bytes stored in RowContainer per benchmark "
    "iteration");
DEFINE_int64(
    row_container_varchar_orderby_batch_bytes,
    128L << 20,
    "Approximate max input batch varchar payload bytes. Input batches are "
    "generated outside measured time.");
DEFINE_int32(
    row_container_varchar_orderby_max_batch_rows,
    4096,
    "Max input batch rows");
DEFINE_bool(
    row_container_varchar_orderby_print_stats,
    false,
    "Print per-iteration row count, payload bytes and memory stats");
DEFINE_string(
    row_container_varchar_orderby_cpu_profile,
    "",
    "Write a gperftools CPU profile to this path when set");

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::memory;
using namespace bytedance::bolt::test;

namespace {

constexpr int32_t kNumColumns = 100;
constexpr int32_t kNumOrderByKeys = 3;
constexpr int64_t kAllocatorCapacity = 8L << 30;

enum class StringAllocationMode {
  kHashStringAllocator,
  kMonotonicMemoryResource,
};

struct StringLengthRange {
  const char* name;
  int32_t min;
  int32_t max;
};

struct BenchmarkStats {
  int64_t rows{0};
  int64_t cells{0};
  int64_t payloadBytes{0};
  uint64_t poolPeakBytes{0};
  uint64_t rowContainerAllocatedBytes{0};
  uint64_t rowContainerUsedBytes{0};
};

std::vector<TypePtr> varcharTypes() {
  return std::vector<TypePtr>(kNumColumns, VARCHAR());
}

std::vector<std::string> columnNames() {
  std::vector<std::string> names;
  names.reserve(kNumColumns);
  for (int32_t i = 0; i < kNumColumns; ++i) {
    names.push_back(fmt::format("c{}", i));
  }
  return names;
}

std::shared_ptr<const RowType> varcharRowType() {
  auto names = columnNames();
  return ROW(std::move(names), varcharTypes());
}

int32_t
stringLengthAt(const StringLengthRange& range, int64_t row, int32_t column) {
  const auto width = range.max - range.min + 1;
  return range.min + ((row * 131 + column * 17) % width);
}

std::string makeString(int64_t row, int32_t column, int32_t size) {
  std::string value(size, 'a');
  uint64_t seed = folly::hash::hash_combine(row, column);
  for (int32_t i = 0; i < size; ++i) {
    seed = seed * 1103515245 + 12345;
    value[i] = static_cast<char>('a' + ((seed >> 16) % 26));
  }
  return value;
}

int64_t averageStringLength(const StringLengthRange& range) {
  return (static_cast<int64_t>(range.min) + range.max) / 2;
}

int64_t rowCountForTargetPayload(const StringLengthRange& range) {
  return std::max<int64_t>(
      1,
      FLAGS_row_container_varchar_orderby_payload_bytes /
          (kNumColumns * averageStringLength(range)));
}

int32_t batchRowsForRange(const StringLengthRange& range, int64_t rowsLeft) {
  const auto rowsByBytes = std::max<int64_t>(
      1,
      FLAGS_row_container_varchar_orderby_batch_bytes /
          (kNumColumns * averageStringLength(range)));
  return static_cast<int32_t>(std::min<int64_t>(
      rowsLeft,
      std::min<int64_t>(
          FLAGS_row_container_varchar_orderby_max_batch_rows, rowsByBytes)));
}

RowVectorPtr makeBatch(
    VectorMaker& vectorMaker,
    const std::vector<std::string>& names,
    const StringLengthRange& range,
    int64_t rowOffset,
    int32_t numRows) {
  std::vector<VectorPtr> children;
  children.reserve(kNumColumns);
  for (int32_t column = 0; column < kNumColumns; ++column) {
    std::string temp;
    children.push_back(vectorMaker.flatVector<StringView>(
        numRows,
        [&](vector_size_t row) {
          const auto globalRow = rowOffset + row;
          const auto size = stringLengthAt(range, globalRow, column);
          temp = makeString(globalRow, column, size);
          return StringView(temp);
        },
        nullptr,
        VARCHAR()));
  }
  return vectorMaker.rowVector(names, children);
}

std::unique_ptr<RowContainer> makeRowContainer(
    MemoryPool* pool,
    StringAllocationMode mode) {
  RowContainer::RowContainerParam rowContainerParam;
  if (mode == StringAllocationMode::kMonotonicMemoryResource) {
    rowContainerParam.useMonotonicStringAllocation = true;
  }

  return std::make_unique<RowContainer>(
      varcharTypes(),
      false /*nullableKeys*/,
      std::vector<Accumulator>{},
      std::vector<TypePtr>{},
      false /*hasNext*/,
      false /*isJoinBuild*/,
      false /*hasProbedFlag*/,
      false /*hasNormalizedKey*/,
      false /*useListRowIndex*/,
      pool,
      std::move(rowContainerParam));
}

void storeBatch(
    RowContainer& rowContainer,
    const RowVectorPtr& batch,
    std::vector<char*>& rows) {
  SelectivityVector selectedRows(batch->size());
  auto* input = batch->as<RowVector>();

  std::vector<DecodedVector> decoded;
  decoded.reserve(kNumColumns);
  for (int32_t column = 0; column < kNumColumns; ++column) {
    decoded.emplace_back(*input->childAt(column), selectedRows);
  }

  for (vector_size_t row = 0; row < batch->size(); ++row) {
    auto* storedRow = rowContainer.newRow();
    rows.push_back(storedRow);
    for (int32_t column = 0; column < kNumColumns; ++column) {
      rowContainer.store(decoded[column], row, storedRow, column);
    }
  }
}

void sortByFirstThreeKeys(
    RowContainer& rowContainer,
    std::vector<char*>& rows) {
#ifdef ENABLE_BOLT_JIT
  bytedance::bolt::jit::CompiledModuleSP jitModule;
  RowRowCompare cmp = nullptr;
  const std::vector<TypePtr> sortKeyTypes(kNumOrderByKeys, VARCHAR());
  const std::vector<CompareFlags> compareFlags(kNumOrderByKeys);
  const std::vector<column_index_t> sortKeyIndexes{0, 1, 2};
  if (RowContainer::JITable(sortKeyTypes)) {
    auto [module, functionName] = rowContainer.codegenCompare(
        sortKeyTypes,
        compareFlags,
        bytedance::bolt::jit::CmpType::SORT_LESS,
        false /*hasNullKeys*/,
        sortKeyIndexes);
    jitModule = std::move(module);
    cmp = (RowRowCompare)jitModule->getFuncPtr(functionName);
  }
  if (cmp != nullptr) {
    std::sort(rows.begin(), rows.end(), cmp);
    return;
  }
#endif

  std::sort(rows.begin(), rows.end(), [&](const char* left, const char* right) {
    for (int32_t key = 0; key < kNumOrderByKeys; ++key) {
      const auto result = rowContainer.compare(left, right, key);
      if (result != 0) {
        return result < 0;
      }
    }
    return false;
  });
}

BenchmarkStats runBenchmark(
    StringAllocationMode mode,
    const StringLengthRange& range) {
  MemoryManager::Options options;
  options.allocatorCapacity = kAllocatorCapacity;
  options.arbitratorCapacity = kAllocatorCapacity;
  options.useMmapAllocator = false;
  auto manager = std::make_unique<MemoryManager>(options);
  auto pool = manager->addLeafPool("row_container_varchar_orderby_benchmark");

  auto names = columnNames();
  VectorMaker vectorMaker(pool.get());
  auto rowContainer = makeRowContainer(pool.get(), mode);
  std::vector<char*> rows;
  rows.reserve(rowCountForTargetPayload(range));

  int64_t payloadBytes = 0;
  const auto numRows = rowCountForTargetPayload(range);
  int64_t rowOffset = 0;
  while (rowOffset < numRows) {
    const auto batchRows = batchRowsForRange(range, numRows - rowOffset);
    RowVectorPtr batch;
    {
      folly::BenchmarkSuspender suspender;
      batch = makeBatch(vectorMaker, names, range, rowOffset, batchRows);
    }

    for (int32_t row = 0; row < batchRows; ++row) {
      for (int32_t column = 0; column < kNumColumns; ++column) {
        payloadBytes += stringLengthAt(range, rowOffset + row, column);
      }
    }
    storeBatch(*rowContainer, batch, rows);
    rowOffset += batchRows;
  }

  sortByFirstThreeKeys(*rowContainer, rows);
  folly::doNotOptimizeAway(rows.data());

  return BenchmarkStats{
      .rows = numRows,
      .cells = numRows * kNumColumns,
      .payloadBytes = payloadBytes,
      .poolPeakBytes = pool->stats().peakBytes,
      .rowContainerAllocatedBytes = rowContainer->allocatedBytes(),
      .rowContainerUsedBytes = rowContainer->usedBytes()};
}

void printStats(
    const char* allocatorName,
    const StringLengthRange& range,
    const BenchmarkStats& stats) {
  if (!FLAGS_row_container_varchar_orderby_print_stats) {
    return;
  }
  fmt::print(
      "{}_{} rows={} cells={} payloadBytes={} poolPeakBytes={} "
      "rowContainerAllocatedBytes={} rowContainerUsedBytes={}\n",
      allocatorName,
      range.name,
      stats.rows,
      stats.cells,
      stats.payloadBytes,
      stats.poolPeakBytes,
      stats.rowContainerAllocatedBytes,
      stats.rowContainerUsedBytes);
}

void runHashStringAllocator(uint32_t iterations, StringLengthRange range) {
  for (uint32_t i = 0; i < iterations; ++i) {
    auto stats =
        runBenchmark(StringAllocationMode::kHashStringAllocator, range);
    printStats("HashStringAllocator", range, stats);
  }
}

void runMonotonicMemoryResource(uint32_t iterations, StringLengthRange range) {
  for (uint32_t i = 0; i < iterations; ++i) {
    auto stats =
        runBenchmark(StringAllocationMode::kMonotonicMemoryResource, range);
    printStats("MonotonicMemoryResource", range, stats);
  }
}

} // namespace

BENCHMARK_NAMED_PARAM(
    runHashStringAllocator,
    Len20_30,
    StringLengthRange{"Len20_30", 20, 30});
BENCHMARK_RELATIVE_NAMED_PARAM(
    runMonotonicMemoryResource,
    Len20_30,
    StringLengthRange{"Len20_30", 20, 30});
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    runHashStringAllocator,
    Len100_200,
    StringLengthRange{"Len100_200", 100, 200});
BENCHMARK_RELATIVE_NAMED_PARAM(
    runMonotonicMemoryResource,
    Len100_200,
    StringLengthRange{"Len100_200", 100, 200});
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    runHashStringAllocator,
    Len64K_128K,
    StringLengthRange{"Len64K_128K", 64 << 10, 128 << 10});
BENCHMARK_RELATIVE_NAMED_PARAM(
    runMonotonicMemoryResource,
    Len64K_128K,
    StringLengthRange{"Len64K_128K", 64 << 10, 128 << 10});

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  if (!FLAGS_row_container_varchar_orderby_cpu_profile.empty() &&
      BoltProfilerIsAvailable()) {
    BoltProfilerStart(FLAGS_row_container_varchar_orderby_cpu_profile.c_str());
  }
  folly::runBenchmarks();
  if (!FLAGS_row_container_varchar_orderby_cpu_profile.empty() &&
      BoltProfilerIsAvailable()) {
    BoltProfilerStop();
  }
  return 0;
}
