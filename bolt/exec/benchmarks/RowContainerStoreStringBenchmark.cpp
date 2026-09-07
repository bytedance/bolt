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

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/exec/RowContainer.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

DEFINE_int64(
    row_container_store_string_payload_bytes,
    256L << 20,
    "Approximate varchar payload bytes to store per benchmark iteration");
DEFINE_int32(
    row_container_store_string_input_rows,
    8192,
    "Number of reusable input rows generated outside measured time");
DEFINE_bool(
    row_container_store_string_print_stats,
    true,
    "Print rows, payload bytes and RowContainer memory stats");

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::memory;
using namespace bytedance::bolt::test;

namespace {

constexpr int64_t kAllocatorCapacity = 4L << 30;

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
  int64_t payloadBytes{0};
  uint64_t rowContainerAllocatedBytes{0};
  uint64_t rowContainerUsedBytes{0};
};

const std::array<std::string, 3>& constantStringsForRange(
    const StringLengthRange& range) {
  static const std::array<std::string, 3> kLen20_30{
      "abcdefghijklmnopqrst",
      "abcdefghijklmnopqrstuvwxy",
      "abcdefghijklmnopqrstuvwxyzabcd"};
  static const std::array<std::string, 3> kLen100_200{
      std::string(100, 'a'), std::string(150, 'b'), std::string(200, 'c')};
  static const std::array<std::string, 3> kLen64K_128K{
      std::string(64 << 10, 'a'),
      std::string(96 << 10, 'b'),
      std::string(128 << 10, 'c')};

  if (range.max <= 30) {
    return kLen20_30;
  }
  if (range.max <= 200) {
    return kLen100_200;
  }
  return kLen64K_128K;
}

int64_t averageStringLength(const StringLengthRange& range) {
  return (static_cast<int64_t>(range.min) + range.max) / 2;
}

int64_t rowCountForTargetPayload(const StringLengthRange& range) {
  return std::max<int64_t>(
      1,
      FLAGS_row_container_store_string_payload_bytes /
          averageStringLength(range));
}

RowVectorPtr makeInputBatch(
    VectorMaker& vectorMaker,
    const StringLengthRange& range) {
  return vectorMaker.rowVector(
      {"c0"},
      {vectorMaker.flatVector<StringView>(
          FLAGS_row_container_store_string_input_rows,
          [&](vector_size_t row) {
            const auto& values = constantStringsForRange(range);
            const auto& value = values[row % values.size()];
            return StringView(value);
          },
          nullptr,
          VARCHAR())});
}

std::unique_ptr<RowContainer> makeRowContainer(
    MemoryPool* pool,
    StringAllocationMode mode) {
  RowContainer::RowContainerParam rowContainerParam;
  if (mode == StringAllocationMode::kMonotonicMemoryResource) {
    rowContainerParam.useMonotonicStringAllocation = true;
  }

  return std::make_unique<RowContainer>(
      std::vector<TypePtr>{VARCHAR()},
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

BenchmarkStats runBenchmark(
    StringAllocationMode mode,
    const StringLengthRange& range) {
  MemoryManager::Options options;
  options.allocatorCapacity = kAllocatorCapacity;
  options.arbitratorCapacity = kAllocatorCapacity;
  options.useMmapAllocator = false;
  auto manager = std::make_unique<MemoryManager>(options);
  auto pool = manager->addLeafPool("row_container_store_string_benchmark");

  VectorMaker vectorMaker(pool.get());
  RowVectorPtr input;
  {
    folly::BenchmarkSuspender suspender;
    input = makeInputBatch(vectorMaker, range);
  }

  SelectivityVector selectedRows(input->size());
  DecodedVector decoded(*input->childAt(0), selectedRows);
  auto rowContainer = makeRowContainer(pool.get(), mode);

  const auto numRows = rowCountForTargetPayload(range);
  int64_t payloadBytes = 0;
  for (int64_t row = 0; row < numRows; ++row) {
    const auto sourceRow = row % input->size();
    auto* storedRow = rowContainer->newRow();
    rowContainer->store(decoded, sourceRow, storedRow, 0);
    payloadBytes += decoded.valueAt<StringView>(sourceRow).size();
  }

  return BenchmarkStats{
      .rows = numRows,
      .payloadBytes = payloadBytes,
      .rowContainerAllocatedBytes = rowContainer->allocatedBytes(),
      .rowContainerUsedBytes = rowContainer->usedBytes()};
}

void printStats(
    const char* allocatorName,
    const StringLengthRange& range,
    const BenchmarkStats& stats) {
  if (!FLAGS_row_container_store_string_print_stats) {
    return;
  }
  fmt::print(
      "{}_{} rows={} payloadBytes={} rowContainerAllocatedBytes={} "
      "rowContainerUsedBytes={}\n",
      allocatorName,
      range.name,
      stats.rows,
      stats.payloadBytes,
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
  folly::runBenchmarks();
  return 0;
}
