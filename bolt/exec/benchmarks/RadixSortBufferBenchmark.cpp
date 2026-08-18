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
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

#include "bolt/core/PlanFragment.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/Driver.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/benchmarks/RadixSortBenchmarkData.h"
#include "bolt/exec/radixsort/RadixSortBuffer.h"

namespace bytedance::bolt::exec::radixsort::benchmark {
namespace {

constexpr vector_size_t kOutputBatchSize = 2048;
#ifdef RADIX_SORT_LARGE_BENCHMARK
constexpr auto& kBenchmarkScenarioSpecs = kInMemoryLargeScenarioSpecs;
#else
constexpr auto& kBenchmarkScenarioSpecs = kInMemoryScenarioSpecs;
#endif

enum class Implementation : uint8_t {
  kLegacy,
  kRadix,
};

struct Measurements {
  uint64_t runs{0};
  uint64_t peakBytes{0};
  uint64_t knownExternalBytes{0};
  uint64_t colToRowTimeUs{0};
  uint64_t sortTimeUs{0};
  uint64_t outputTimeUs{0};
  uint64_t addInputWallTimeUs{0};
  uint64_t finalizeWallTimeUs{0};
  uint64_t outputWallTimeUs{0};

  void add(
      int64_t peak,
      uint64_t knownExternal,
      const common::SortStats& stats,
      uint64_t addInputWall,
      uint64_t finalizeWall,
      uint64_t outputWall) {
    ++runs;
    peakBytes = std::max<uint64_t>(
        peakBytes, static_cast<uint64_t>(std::max(peak, 0L)));
    knownExternalBytes = std::max<uint64_t>(knownExternalBytes, knownExternal);
    colToRowTimeUs += stats.sortColToRowTimeUs;
    sortTimeUs += stats.sortInSortTimeUs;
    outputTimeUs += stats.sortOutputTimeUs;
    addInputWallTimeUs += addInputWall;
    finalizeWallTimeUs += finalizeWall;
    outputWallTimeUs += outputWall;
  }
};

std::shared_ptr<memory::MemoryPool> sourcePool;
std::vector<ScenarioFixture> fixtures;
std::array<std::array<Measurements, 2>, kBenchmarkScenarioSpecs.size()>
    measurements;
std::atomic<uint64_t> poolSequence{0};
std::shared_ptr<folly::CPUThreadPoolExecutor> jitExecutor;
std::shared_ptr<Task> jitTask;
std::unique_ptr<DriverCtx> jitDriverCtx;
std::unique_ptr<OperatorCtx> jitOperatorCtx;

std::shared_ptr<memory::MemoryPool> makeSortPool(
    Implementation implementation,
    uint32_t scenario) {
  auto root = memory::memoryManager()->addRootPool(fmt::format(
      "sort-buffer-e2e-root-{}-{}-{}",
      implementation == Implementation::kLegacy ? "legacy" : "radix",
      scenario,
      poolSequence.fetch_add(1)));
  return root->addLeafChild(fmt::format(
      "sort-buffer-e2e-{}-{}-{}",
      implementation == Implementation::kLegacy ? "legacy" : "radix",
      scenario,
      poolSequence.fetch_add(1)));
}

template <typename SortBufferType>
std::pair<uint64_t, uint64_t> drain(
    SortBufferType& sortBuffer,
    const ScenarioFixture& fixture) {
  uint64_t outputRows = 0;
  uint64_t checksum = 0;
  while (auto output = sortBuffer.getOutput(kOutputBatchSize)) {
    outputRows += output->size();
    if (output->size() > 0) {
      const auto& ids = *output->childAt(fixture.idChannel);
      checksum ^= ids.hashValueAt(0);
      checksum ^= ids.hashValueAt(output->size() - 1);
    }
  }
  return {outputRows, checksum};
}

void record(
    uint32_t scenario,
    Implementation implementation,
    const std::shared_ptr<memory::MemoryPool>& pool,
    const std::optional<common::SortStats>& stats,
    uint64_t addInputWallTimeUs,
    uint64_t finalizeWallTimeUs,
    uint64_t outputWallTimeUs) {
  BOLT_CHECK(stats.has_value());
  const auto knownExternal = implementation == Implementation::kLegacy
      ? static_cast<uint64_t>(kBenchmarkScenarioSpecs[scenario].rows) *
          sizeof(char*)
      : 0;
  measurements[scenario][static_cast<uint32_t>(implementation)].add(
      pool->peakBytes(),
      knownExternal,
      *stats,
      addInputWallTimeUs,
      finalizeWallTimeUs,
      outputWallTimeUs);
}

uint64_t elapsedUs(const std::chrono::steady_clock::time_point& begin) {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - begin)
      .count();
}

void legacyE2E(unsigned iterations, uint32_t scenario) {
  folly::BenchmarkSuspender suspender;
  const auto& fixture = fixtures.at(scenario);
  const auto& spec = kBenchmarkScenarioSpecs.at(scenario);
  for (unsigned iteration = 0; iteration < iterations; ++iteration) {
    auto pool = makeSortPool(Implementation::kLegacy, scenario);
    tsan_atomic<bool> nonReclaimableSection{false};
    auto sortBuffer = std::make_unique<SortBuffer>(
        fixture.rowType,
        fixture.keyChannels,
        fixture.keyFlags,
        pool.get(),
        &nonReclaimableSection,
        nullptr,
        0,
        jitOperatorCtx.get());

    suspender.dismiss();
    const auto addInputBegin = std::chrono::steady_clock::now();
    for (const auto& input : fixture.inputs) {
      sortBuffer->addInput(input);
    }
    const auto addInputWallTimeUs = elapsedUs(addInputBegin);
    const auto finalizeBegin = std::chrono::steady_clock::now();
    sortBuffer->noMoreInput();
    const auto finalizeWallTimeUs = elapsedUs(finalizeBegin);
    const auto outputBegin = std::chrono::steady_clock::now();
    const auto [outputRows, checksum] = drain(*sortBuffer, fixture);
    const auto outputWallTimeUs = elapsedUs(outputBegin);
    folly::doNotOptimizeAway(checksum);
    suspender.rehire();

    BOLT_CHECK_EQ(outputRows, spec.rows);
    record(
        scenario,
        Implementation::kLegacy,
        pool,
        sortBuffer->sortStats(),
        addInputWallTimeUs,
        finalizeWallTimeUs,
        outputWallTimeUs);
    sortBuffer.reset();
    BOLT_CHECK_EQ(pool->currentBytes(), 0);
  }
}

void radixE2E(unsigned iterations, uint32_t scenario) {
  folly::BenchmarkSuspender suspender;
  const auto& fixture = fixtures.at(scenario);
  const auto& spec = kBenchmarkScenarioSpecs.at(scenario);
  for (unsigned iteration = 0; iteration < iterations; ++iteration) {
    auto pool = makeSortPool(Implementation::kRadix, scenario);
    auto sortBuffer = std::make_unique<RadixSortBuffer>(
        fixture.rowType, fixture.keyChannels, fixture.keyFlags, pool.get());

    suspender.dismiss();
    const auto addInputBegin = std::chrono::steady_clock::now();
    for (const auto& input : fixture.inputs) {
      sortBuffer->addInput(input);
    }
    const auto addInputWallTimeUs = elapsedUs(addInputBegin);
    const auto finalizeBegin = std::chrono::steady_clock::now();
    sortBuffer->noMoreInput();
    const auto finalizeWallTimeUs = elapsedUs(finalizeBegin);
    const auto outputBegin = std::chrono::steady_clock::now();
    const auto [outputRows, checksum] = drain(*sortBuffer, fixture);
    const auto outputWallTimeUs = elapsedUs(outputBegin);
    folly::doNotOptimizeAway(checksum);
    suspender.rehire();

    BOLT_CHECK_EQ(outputRows, spec.rows);
    record(
        scenario,
        Implementation::kRadix,
        pool,
        sortBuffer->sortStats(),
        addInputWallTimeUs,
        finalizeWallTimeUs,
        outputWallTimeUs);
    sortBuffer.reset();
    BOLT_CHECK_EQ(pool->currentBytes(), 0);
  }
}

void printMemorySummary() {
  std::printf(
      "\nTracked sort-pool peak and average internal phases "
      "(input generation/source vectors excluded)\n");
  std::printf(
      "%-34s %-8s %10s %10s %10s %10s %10s %10s\n",
      "scenario",
      "impl",
      "pool MiB",
      "ext MiB",
      "lower MiB",
      "encode ms",
      "sort ms",
      "output ms");
  for (uint32_t scenario = 0; scenario < kBenchmarkScenarioSpecs.size();
       ++scenario) {
    for (uint32_t implementation = 0; implementation < 2; ++implementation) {
      const auto& result = measurements[scenario][implementation];
      if (result.runs == 0) {
        continue;
      }
      std::printf(
          "%-34s %-8s %10.2f %10.2f %10.2f %10.2f %10.2f %10.2f\n",
          kBenchmarkScenarioSpecs[scenario].name,
          implementation == 0 ? "legacy" : "radix",
          static_cast<double>(result.peakBytes) / (1024 * 1024),
          static_cast<double>(result.knownExternalBytes) / (1024 * 1024),
          static_cast<double>(result.peakBytes + result.knownExternalBytes) /
              (1024 * 1024),
          static_cast<double>(result.colToRowTimeUs) / result.runs / 1000,
          static_cast<double>(result.sortTimeUs) / result.runs / 1000,
          static_cast<double>(result.outputTimeUs) / result.runs / 1000);
    }
  }
  std::printf(
      "Note: lower MiB adds legacy rows*sizeof(char*) sortedRows to the "
      "tracked pool peak. Small STL/allocator metadata outside MemoryPool is "
      "not included for either implementation.\n");
  std::printf(
      "\nComparable outer wall phases (allocation and projection included)\n");
  std::printf(
      "%-34s %-8s %12s %12s %12s\n",
      "scenario",
      "impl",
      "addInput ms",
      "finalize ms",
      "output ms");
  for (uint32_t scenario = 0; scenario < kBenchmarkScenarioSpecs.size();
       ++scenario) {
    for (uint32_t implementation = 0; implementation < 2; ++implementation) {
      const auto& result = measurements[scenario][implementation];
      if (result.runs == 0) {
        continue;
      }
      std::printf(
          "%-34s %-8s %12.2f %12.2f %12.2f\n",
          kBenchmarkScenarioSpecs[scenario].name,
          implementation == 0 ? "legacy" : "radix",
          static_cast<double>(result.addInputWallTimeUs) / result.runs / 1000,
          static_cast<double>(result.finalizeWallTimeUs) / result.runs / 1000,
          static_cast<double>(result.outputWallTimeUs) / result.runs / 1000);
    }
  }
  std::printf(
      "Legacy row-comparator JIT is enabled with jit.level=-1; JIT codegen "
      "lookup runs inside noMoreInput and is included in E2E time. Each unique "
      "signature compiles once per process, then uses the global module cache. "
      "LLVM process-heap memory is not part of sort-pool peak.\n");
}

void initializeJitContext() {
#ifndef ENABLE_BOLT_JIT
  BOLT_FAIL("SortBuffer E2E benchmark requires ENABLE_BOLT_JIT");
#else
  jitExecutor = std::make_shared<folly::CPUThreadPoolExecutor>(1);
  auto queryCtx = core::QueryCtx::create(jitExecutor.get());
  queryCtx->testingOverrideConfigUnsafe({{core::QueryConfig::kJitLevel, "-1"}});
  BOLT_CHECK(queryCtx->queryConfig().enableJitRowCmpRow());

  core::PlanFragment plan;
  plan.planNode =
      std::make_shared<core::ValuesNode>("jit-values", fixtures[0].inputs);
  jitTask = Task::create(
      "sort-buffer-e2e-jit",
      std::move(plan),
      0,
      queryCtx,
      Task::ExecutionMode::kParallel);
  jitDriverCtx = std::make_unique<DriverCtx>(jitTask, 0, 0, 0, 0);
  jitOperatorCtx = std::make_unique<OperatorCtx>(
      jitDriverCtx.get(), "sort-buffer-e2e", 0, "OrderBy");
#endif
}

void clearJitContext() {
  jitOperatorCtx.reset();
  jitDriverCtx.reset();
  if (jitTask != nullptr) {
    jitTask->testingFinish();
  }
  jitTask.reset();
  jitExecutor.reset();
}

} // namespace

#define RADIX_SORT_BENCHMARK_PAIR(name, index)   \
  BENCHMARK_NAMED_PARAM(legacyE2E, name, index); \
  BENCHMARK_RELATIVE_NAMED_PARAM(radixE2E, name, index)

#ifndef RADIX_SORT_LARGE_BENCHMARK
RADIX_SORT_BENCHMARK_PAIR(random_i64_narrow_256k, 0);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(duplicate_i64_narrow_256k, 1);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(low_cardinality_i64_256k, 2);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(null_heavy_i64_128k, 3);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(eight_key_i64_128k, 4);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(inline_varchar_128k, 5);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(long_varchar_64k, 6);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(varchar_common_prefix_128k, 7);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(nullable_fixed_payload_128k, 8);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(wide_fixed_payload_128k, 9);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(very_wide_fixed_payload_64k, 10);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(wide_string_payload_64k, 11);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(random_i64_narrow_1m, 12);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(low_cardinality_i64_1m, 13);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(eight_key_i64_1m, 14);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(sixteen_key_i64_1m, 15);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(wide_fixed_payload_1m, 16);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(very_wide_fixed_payload_1m, 17);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(wide_string_payload_1m, 18);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(low_card_i32_log_pattern_payload_1m, 19);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(low_card_i32_30_array15_5_payload_1m, 20);
#else
RADIX_SORT_BENCHMARK_PAIR(random_i64_narrow_10m, 0);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(eight_key_i64_10m, 1);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(sixteen_key_i64_10m, 2);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(wide_fixed_payload_10m, 3);
BENCHMARK_DRAW_LINE();
RADIX_SORT_BENCHMARK_PAIR(long_varchar_10m, 4);
#endif

#undef RADIX_SORT_BENCHMARK_PAIR

} // namespace bytedance::bolt::exec::radixsort::benchmark

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  bytedance::bolt::memory::MemoryManager::initialize(
      bytedance::bolt::memory::MemoryManager::Options{});
  using namespace bytedance::bolt::exec::radixsort::benchmark;
  sourcePool =
      bytedance::bolt::memory::memoryManager()->addLeafPool("sort-inputs");
  fixtures.reserve(kBenchmarkScenarioSpecs.size());
  for (const auto& spec : kBenchmarkScenarioSpecs) {
    fixtures.push_back(makeFixture(sourcePool.get(), spec));
  }
  initializeJitContext();
  folly::runBenchmarks();
  printMemorySummary();
  std::fflush(stdout);
  clearJitContext();
  fixtures.clear();
  sourcePool.reset();
  return 0;
}
