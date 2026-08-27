/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <vector>

#include "bolt/common/base/CompareFlags.h"
#include "bolt/exec/HybridSorter.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/radixsort/RadixSortKey.h"
#include "bolt/exec/radixsort/RadixSortRunSorter.h"
#include "bolt/exec/radixsort/RadixSortRunStorage.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/jit/CompiledModule.h"
#include "bolt/jit/RowContainer/RowContainerCodeGenerator.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::radixsort;

namespace {

enum class DataPattern : uint8_t {
  kRandom,
  kLowCardinality,
};

struct Scenario {
  const char* name;
  uint32_t rows;
  uint32_t words;
  DataPattern pattern;
};

constexpr std::array<Scenario, 14> kScenarios{{
    {"random_w1_10k", 10'000, 1, DataPattern::kRandom},
    {"random_w1_100k", 100'000, 1, DataPattern::kRandom},
    {"random_w1_1m", 1'000'000, 1, DataPattern::kRandom},
    {"random_w1_10m", 10'000'000, 1, DataPattern::kRandom},
    {"low_cardinality_w1_1m", 1'000'000, 1, DataPattern::kLowCardinality},
    {"random_w2_1m", 1'000'000, 2, DataPattern::kRandom},
    {"random_w4_1m", 1'000'000, 4, DataPattern::kRandom},
    {"random_w8_1m", 1'000'000, 8, DataPattern::kRandom},
    {"low_cardinality_w4_1m", 1'000'000, 4, DataPattern::kLowCardinality},
    {"random_w2_10m", 10'000'000, 2, DataPattern::kRandom},
    {"random_w4_10m", 10'000'000, 4, DataPattern::kRandom},
    {"low_cardinality_w4_10m", 10'000'000, 4, DataPattern::kLowCardinality},
    {"random_w8_10m", 10'000'000, 8, DataPattern::kRandom},
    {"low_cardinality_w8_10m", 10'000'000, 8, DataPattern::kLowCardinality},
}};

std::shared_ptr<memory::MemoryPool> pool;
std::atomic<uint64_t> adaptivePoolSequence{0};

uint64_t randomBits(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

uint64_t keyWord(const Scenario& scenario, uint64_t row, uint32_t word) {
  switch (scenario.pattern) {
    case DataPattern::kLowCardinality:
      return randomBits(row + word * 131) % (word % 2 == 0 ? 64 : 4096);
    case DataPattern::kRandom:
      return randomBits(row + word * 257);
  }
  BOLT_UNREACHABLE("Unsupported radix sort benchmark data pattern");
}

std::vector<uint64_t> makeInput(const Scenario& scenario) {
  std::vector<uint64_t> data(
      static_cast<uint64_t>(scenario.rows) * scenario.words);
  for (uint32_t row = 0; row < scenario.rows; ++row) {
    for (uint32_t word = 0; word < scenario.words; ++word) {
      const auto value = keyWord(scenario, row, word);
      data[static_cast<uint64_t>(row) * scenario.words + word] =
          byteSwap<uint64_t>(value);
    }
  }
  return data;
}

uint64_t unsignedKeyWordForSignedCompare(const uint64_t word) {
  return word ^ (1ULL << 63);
}

std::vector<TypePtr> bigintKeyTypes(const uint32_t words) {
  std::vector<TypePtr> types;
  types.reserve(words);
  for (uint32_t word = 0; word < words; ++word) {
    types.push_back(BIGINT());
  }
  return types;
}

struct JitAdaptiveInput {
  std::shared_ptr<memory::MemoryPool> pool;
  std::unique_ptr<RowContainer> container;
  std::vector<char*> rows;
  jit::CompiledModuleSP module;
  RowRowCompare compare{nullptr};
};

JitAdaptiveInput makeJitAdaptiveInput(const Scenario& scenario) {
  auto keyTypes = bigintKeyTypes(scenario.words);
  auto sortPool = memory::memoryManager()->addLeafPool(fmt::format(
      "adaptive-jit-sort-algorithm-benchmark-{}-{}",
      scenario.name,
      adaptivePoolSequence.fetch_add(1)));
  auto container = std::make_unique<RowContainer>(keyTypes, sortPool.get());
  std::vector<char*> rows(scenario.rows);
  for (uint32_t row = 0; row < scenario.rows; ++row) {
    auto* storedRow = container->newRow();
    for (uint32_t word = 0; word < scenario.words; ++word) {
      const auto signedComparableWord =
          unsignedKeyWordForSignedCompare(keyWord(scenario, row, word));
      RowContainer::valueAt<int64_t>(
          storedRow, container->columnAt(word).offset()) =
          static_cast<int64_t>(signedComparableWord);
    }
    rows[row] = storedRow;
  }

#ifndef ENABLE_BOLT_JIT
  BOLT_FAIL(
      "Sort algorithm benchmark adaptive baseline requires ENABLE_BOLT_JIT");
#else
  BOLT_CHECK(RowContainer::JITable(keyTypes));
  const std::vector<CompareFlags> flags(
      scenario.words,
      CompareFlags{
          .nullsFirst = true,
          .ascending = true,
          .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue});
  auto [module, functionName] = container->codegenCompare(
      keyTypes, flags, jit::CmpType::SORT_LESS, false /* hasNullKeys */);
  auto* compare =
      reinterpret_cast<RowRowCompare>(module->getFuncPtr(functionName));
  BOLT_CHECK_NOT_NULL(compare);
  return JitAdaptiveInput{
      std::move(sortPool),
      std::move(container),
      std::move(rows),
      std::move(module),
      compare};
#endif
}

void legacyAdaptiveSort(unsigned iterations, uint32_t scenarioIndex) {
  folly::BenchmarkSuspender suspender;
  const auto& scenario = kScenarios[scenarioIndex];
  for (unsigned iteration = 0; iteration < iterations; ++iteration) {
    auto input = makeJitAdaptiveInput(scenario);
    HybridSorter sorter;
    suspender.dismiss();
    sorter.sort(input.rows.begin(), input.rows.end(), input.compare);
    folly::doNotOptimizeAway(input.rows.data());
    suspender.rehire();
  }
}

RadixSortKeyLayout layoutForWords(uint32_t words) {
  switch (words) {
    case 1:
      return RadixSortKeyLayout::fromKind(
          RadixSortKeyLayoutKind::kKeyOnlyFixed8);
    case 2:
      return RadixSortKeyLayout::fromKind(
          RadixSortKeyLayoutKind::kKeyOnlyFixed16);
    case 3:
      return RadixSortKeyLayout::fromKind(
          RadixSortKeyLayoutKind::kKeyOnlyFixed24);
    case 4:
      return RadixSortKeyLayout::fromKind(
          RadixSortKeyLayoutKind::kKeyOnlyFixed32);
    default:
      return RadixSortKeyLayout::fromKind(
          RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  }
}

void appendToStorage(
    RadixSortRunStorage& storage,
    const uint64_t* data,
    const Scenario& scenario) {
  if (scenario.words <= 4) {
    storage.appendKeyBlocks(
        scenario.rows,
        [&](vector_size_t source, vector_size_t count, char* out) {
          std::memcpy(
              out,
              data + static_cast<uint64_t>(source) * scenario.words,
              static_cast<size_t>(count) * scenario.words * sizeof(uint64_t));
        });
    return;
  }
  for (uint32_t row = 0; row < scenario.rows; ++row) {
    storage.append(std::string_view(
        reinterpret_cast<const char*>(
            data + static_cast<uint64_t>(row) * scenario.words),
        static_cast<size_t>(scenario.words) * sizeof(uint64_t)));
  }
}

void radixRunSort(unsigned iterations, uint32_t scenarioIndex) {
  folly::BenchmarkSuspender suspender;
  const auto& scenario = kScenarios[scenarioIndex];
  for (unsigned iteration = 0; iteration < iterations; ++iteration) {
    auto sortPool = memory::memoryManager()->addLeafPool(fmt::format(
        "radix-sort-algorithm-benchmark-{}-{}", scenarioIndex, iteration));
    RadixSortRunStorage storage(
        sortPool.get(),
        layoutForWords(scenario.words),
        RadixSortRunStorage::kTestingRowsPerBlock,
        64 * 1024);
    auto data = makeInput(scenario);
    appendToStorage(storage, data.data(), scenario);
    RadixSortRunSorter sorter(storage);
    suspender.dismiss();
    sorter.sort();
    folly::doNotOptimizeAway(storage.keyDataAt(0));
    suspender.rehire();
  }
}

#define SORT_ALGORITHM_BENCHMARKS(name, index)               \
  BENCHMARK_NAMED_PARAM(legacyAdaptiveSort, name, index);    \
  BENCHMARK_RELATIVE_NAMED_PARAM(radixRunSort, name, index); \
  BENCHMARK_DRAW_LINE()

SORT_ALGORITHM_BENCHMARKS(random_w1_10k, 0);
SORT_ALGORITHM_BENCHMARKS(random_w1_100k, 1);
SORT_ALGORITHM_BENCHMARKS(random_w1_1m, 2);
SORT_ALGORITHM_BENCHMARKS(random_w1_10m, 3);
SORT_ALGORITHM_BENCHMARKS(low_cardinality_w1_1m, 4);
SORT_ALGORITHM_BENCHMARKS(random_w2_1m, 5);
SORT_ALGORITHM_BENCHMARKS(random_w4_1m, 6);
SORT_ALGORITHM_BENCHMARKS(random_w8_1m, 7);
SORT_ALGORITHM_BENCHMARKS(low_cardinality_w4_1m, 8);
SORT_ALGORITHM_BENCHMARKS(random_w2_10m, 9);
SORT_ALGORITHM_BENCHMARKS(random_w4_10m, 10);
SORT_ALGORITHM_BENCHMARKS(low_cardinality_w4_10m, 11);
SORT_ALGORITHM_BENCHMARKS(random_w8_10m, 12);
SORT_ALGORITHM_BENCHMARKS(low_cardinality_w8_10m, 13);

#undef SORT_ALGORITHM_BENCHMARKS

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  pool = memory::memoryManager()->addLeafPool("sort-algorithm-inputs");
  folly::runBenchmarks();
  return 0;
}
