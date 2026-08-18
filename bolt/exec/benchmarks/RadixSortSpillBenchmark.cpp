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

#include <gflags/gflags.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <optional>
#include <string>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/benchmarks/RadixSortBenchmarkData.h"
#include "bolt/exec/radixsort/RadixSortBuffer.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/serializers/PrestoSerializer.h"

DEFINE_string(
    spill_compression_kind,
    "none",
    "Compression kind for radix sort spill benchmark: none, lz4, or zstd.");

namespace bytedance::bolt::exec::radixsort::benchmark {
namespace {

constexpr vector_size_t kOutputBatchSize = 2048;
#ifdef RADIX_SORT_LARGE_BENCHMARK
constexpr auto& kBenchmarkScenarioSpecs = kSpillLargeScenarioSpecs;
#else
constexpr auto& kBenchmarkScenarioSpecs = kSpillScenarioSpecs;
#endif

struct Measurements {
  uint64_t runs{0};
  uint64_t addBeforeSpillUs{0};
  uint64_t addAfterSpillUs{0};
  uint64_t spillUs{0};
  uint64_t finalizeUs{0};
  uint64_t outputUs{0};
  uint64_t spilledRows{0};
  uint64_t spilledBytes{0};
  uint64_t spillWrites{0};
};

enum class Implementation : uint8_t {
  kLegacy,
  kRadix,
};

struct SchemaShape {
  uint32_t keys{0};
  uint32_t payloadColumns{0};
  uint32_t bigintPayloadColumns{0};
  uint32_t varcharPayloadColumns{0};
  uint32_t arrayPayloadColumns{0};
};

std::shared_ptr<memory::MemoryPool> sourcePool;
std::vector<std::optional<ScenarioFixture>> fixtures;
std::array<std::array<Measurements, 2>, kBenchmarkScenarioSpecs.size()>
    measurements;

ScenarioFixture& fixtureFor(uint32_t scenario) {
  BOLT_CHECK_LT(scenario, fixtures.size());
  if (!fixtures[scenario].has_value()) {
    fixtures[scenario] = makeFixture(
        sourcePool.get(),
        kBenchmarkScenarioSpecs.at(scenario),
        ScenarioProfile::kSpill);
  }
  return *fixtures[scenario];
}

bool containsChannel(
    const std::vector<column_index_t>& channels,
    column_index_t channel) {
  return std::find(channels.begin(), channels.end(), channel) != channels.end();
}

SchemaShape schemaShape(const ScenarioFixture& fixture) {
  SchemaShape shape;
  shape.keys = fixture.keyChannels.size();
  for (column_index_t channel = 0; channel < fixture.rowType->size();
       ++channel) {
    if (containsChannel(fixture.keyChannels, channel) ||
        channel == fixture.idChannel) {
      continue;
    }
    ++shape.payloadColumns;
    const auto& type = fixture.rowType->childAt(channel);
    switch (type->kind()) {
      case TypeKind::BIGINT:
        ++shape.bigintPayloadColumns;
        break;
      case TypeKind::VARCHAR:
        ++shape.varcharPayloadColumns;
        break;
      case TypeKind::ARRAY:
        ++shape.arrayPayloadColumns;
        break;
      default:
        break;
    }
  }
  return shape;
}

double averageUs(uint64_t totalUs, uint64_t runs) {
  return runs == 0 ? 0 : static_cast<double>(totalUs) / runs;
}

double ratio(
    uint64_t radixUs,
    uint64_t radixRuns,
    uint64_t legacyUs,
    uint64_t legacyRuns) {
  const auto legacyAverage = averageUs(legacyUs, legacyRuns);
  if (legacyAverage == 0) {
    return 0;
  }
  return averageUs(radixUs, radixRuns) / legacyAverage;
}

std::shared_ptr<memory::MemoryPool> makePool(
    Implementation implementation,
    uint32_t scenario) {
  return memory::memoryManager()->addLeafPool(fmt::format(
      "{}-spill-benchmark-{}",
      implementation == Implementation::kLegacy ? "legacy" : "radix",
      scenario));
}

common::SpillConfig spillConfig(
    const std::string& directory,
    const std::string& prefix,
    const std::string& compressionKind,
    const std::string& rowBasedSpillMode) {
  return common::SpillConfig(
      [directory]() -> const std::string& { return directory; },
      [&](uint64_t) {},
      prefix,
      0,
      false,
      1 << 20,
      nullptr,
      5,
      10,
      0,
      0,
      0,
      0,
      0,
      0,
      compressionKind,
      "",
      rowBasedSpillMode);
}

std::string legacyRowBasedSpillMode() {
  return FLAGS_spill_compression_kind == "none" ? "raw" : "compression";
}

void validateSpillCompressionKind() {
  BOLT_CHECK(
      FLAGS_spill_compression_kind == "none" ||
          FLAGS_spill_compression_kind == "lz4" ||
          FLAGS_spill_compression_kind == "zstd",
      "Unsupported spill_compression_kind '{}'. Expected none, lz4, or zstd.",
      FLAGS_spill_compression_kind);
}

uint64_t elapsedUs(const std::chrono::steady_clock::time_point& begin) {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - begin)
      .count();
}

template <typename Buffer>
uint64_t drain(Buffer& buffer) {
  uint64_t rows = 0;
  while (auto output = buffer.getOutput(kOutputBatchSize)) {
    rows += output->size();
    folly::doNotOptimizeAway(output);
  }
  return rows;
}

void record(
    uint32_t scenario,
    Implementation implementation,
    std::optional<common::SpillStats> spillStats,
    uint64_t addBeforeSpillUs,
    uint64_t addAfterSpillUs,
    uint64_t spillUs,
    uint64_t finalizeUs,
    uint64_t outputUs) {
  auto& result = measurements[scenario][static_cast<uint32_t>(implementation)];
  ++result.runs;
  result.addBeforeSpillUs += addBeforeSpillUs;
  result.addAfterSpillUs += addAfterSpillUs;
  result.spillUs += spillUs;
  result.finalizeUs += finalizeUs;
  result.outputUs += outputUs;
  if (spillStats.has_value()) {
    result.spilledRows += spillStats->spilledRows;
    result.spilledBytes += spillStats->spilledBytes;
    result.spillWrites += spillStats->spillWrites;
  }
}

void legacySpillE2E(unsigned iterations, uint32_t scenario) {
  folly::BenchmarkSuspender suspender;
  const auto& fixture = fixtureFor(scenario);
  const auto& spec = kBenchmarkScenarioSpecs.at(scenario);
  for (unsigned iteration = 0; iteration < iterations; ++iteration) {
    auto directory = exec::test::TempDirectoryPath::create();
    auto config = spillConfig(
        directory->path,
        "legacy-sort-spill-benchmark",
        FLAGS_spill_compression_kind,
        legacyRowBasedSpillMode());
    config.setJITenableForSpill(true);
    auto pool = makePool(Implementation::kLegacy, scenario);
    tsan_atomic<bool> nonReclaimableSection{false};
    SortBuffer buffer(
        fixture.rowType,
        fixture.keyChannels,
        fixture.keyFlags,
        pool.get(),
        &nonReclaimableSection,
        &config);

    suspender.dismiss();
    const auto addInputBegin = std::chrono::steady_clock::now();
    const auto split = fixture.inputs.size() / 2;
    for (uint32_t index = 0; index < split; ++index) {
      buffer.addInput(fixture.inputs[index]);
    }
    const auto addInputUs = elapsedUs(addInputBegin);
    const auto spillBegin = std::chrono::steady_clock::now();
    buffer.spill();
    const auto spillUs = elapsedUs(spillBegin);
    const auto addAfterSpillBegin = std::chrono::steady_clock::now();
    for (uint32_t index = split; index < fixture.inputs.size(); ++index) {
      buffer.addInput(fixture.inputs[index]);
    }
    const auto addAfterSpillUs = elapsedUs(addAfterSpillBegin);
    const auto finalizeBegin = std::chrono::steady_clock::now();
    buffer.noMoreInput();
    const auto finalizeUs = elapsedUs(finalizeBegin);
    const auto outputBegin = std::chrono::steady_clock::now();
    const auto outputRows = drain(buffer);
    const auto outputUs = elapsedUs(outputBegin);
    suspender.rehire();

    BOLT_CHECK_EQ(outputRows, spec.rows);
    record(
        scenario,
        Implementation::kLegacy,
        buffer.spilledStats(),
        addInputUs,
        addAfterSpillUs,
        spillUs,
        finalizeUs,
        outputUs);
  }
}

void radixSpillE2E(unsigned iterations, uint32_t scenario) {
  folly::BenchmarkSuspender suspender;
  const auto& fixture = fixtureFor(scenario);
  const auto& spec = kBenchmarkScenarioSpecs.at(scenario);
  for (unsigned iteration = 0; iteration < iterations; ++iteration) {
    auto directory = exec::test::TempDirectoryPath::create();
    auto config = spillConfig(
        directory->path,
        "radix-sort-spill-benchmark",
        FLAGS_spill_compression_kind,
        "disabled");
    auto pool = makePool(Implementation::kRadix, scenario);
    RadixSortBuffer buffer(
        fixture.rowType,
        fixture.keyChannels,
        fixture.keyFlags,
        pool.get(),
        &config);

    suspender.dismiss();
    const auto addInputBegin = std::chrono::steady_clock::now();
    const auto split = fixture.inputs.size() / 2;
    for (uint32_t index = 0; index < split; ++index) {
      buffer.addInput(fixture.inputs[index]);
    }
    const auto addInputUs = elapsedUs(addInputBegin);
    const auto spillBegin = std::chrono::steady_clock::now();
    buffer.spill();
    const auto spillUs = elapsedUs(spillBegin);
    const auto addAfterSpillBegin = std::chrono::steady_clock::now();
    for (uint32_t index = split; index < fixture.inputs.size(); ++index) {
      buffer.addInput(fixture.inputs[index]);
    }
    const auto addAfterSpillUs = elapsedUs(addAfterSpillBegin);
    const auto finalizeBegin = std::chrono::steady_clock::now();
    buffer.noMoreInput();
    const auto finalizeUs = elapsedUs(finalizeBegin);
    const auto outputBegin = std::chrono::steady_clock::now();
    const auto outputRows = drain(buffer);
    const auto outputUs = elapsedUs(outputBegin);
    suspender.rehire();

    BOLT_CHECK_EQ(outputRows, spec.rows);
    record(
        scenario,
        Implementation::kRadix,
        buffer.spilledStats(),
        addInputUs,
        addAfterSpillUs,
        spillUs,
        finalizeUs,
        outputUs);
  }
}

void printSummary() {
  std::printf(
      "\nRadix sort spill benchmark summary (input generation excluded)\n");
  std::printf("\nExecuted scenario schema shape\n");
  std::printf(
      "%-48s %10s %6s %8s %12s %12s %10s\n",
      "scenario",
      "rows",
      "keys",
      "payload",
      "payload i64",
      "payload str",
      "payload arr");
  for (uint32_t scenario = 0; scenario < kBenchmarkScenarioSpecs.size();
       ++scenario) {
    if (!fixtures[scenario].has_value()) {
      continue;
    }
    const auto& fixture = *fixtures[scenario];
    const auto shape = schemaShape(fixture);
    std::printf(
        "%-48s %10d %6u %8u %12u %12u %10u\n",
        kBenchmarkScenarioSpecs[scenario].name,
        kBenchmarkScenarioSpecs[scenario].rows,
        shape.keys,
        shape.payloadColumns,
        shape.bigintPayloadColumns,
        shape.varcharPayloadColumns,
        shape.arrayPayloadColumns);
  }

  std::printf("\nPer-implementation phase timings\n");
  std::printf(
      "%-36s %-8s %10s %10s %10s %10s %10s %12s %10s\n",
      "scenario",
      "impl",
      "add1 ms",
      "add2 ms",
      "spill ms",
      "final ms",
      "out ms",
      "spill MiB",
      "writes");
  for (uint32_t scenario = 0; scenario < kBenchmarkScenarioSpecs.size();
       ++scenario) {
    for (uint32_t impl = 0; impl < 2; ++impl) {
      const auto& result = measurements[scenario][impl];
      if (result.runs == 0) {
        continue;
      }
      std::printf(
          "%-36s %-8s %10.2f %10.2f %10.2f %10.2f %10.2f %12.2f %10.2f\n",
          kBenchmarkScenarioSpecs[scenario].name,
          impl == 0 ? "legacy" : "radix",
          static_cast<double>(result.addBeforeSpillUs) / result.runs / 1000,
          static_cast<double>(result.addAfterSpillUs) / result.runs / 1000,
          static_cast<double>(result.spillUs) / result.runs / 1000,
          static_cast<double>(result.finalizeUs) / result.runs / 1000,
          static_cast<double>(result.outputUs) / result.runs / 1000,
          static_cast<double>(result.spilledBytes) / result.runs /
              (1024 * 1024),
          static_cast<double>(result.spillWrites) / result.runs);
    }
  }
  std::printf("\nRadix / legacy phase ratios\n");
  std::printf(
      "%-36s %10s %10s %10s %10s %10s\n",
      "scenario",
      "add1 x",
      "add2 x",
      "spill x",
      "final x",
      "out x");
  for (uint32_t scenario = 0; scenario < kBenchmarkScenarioSpecs.size();
       ++scenario) {
    const auto& legacy =
        measurements[scenario][static_cast<uint32_t>(Implementation::kLegacy)];
    const auto& radix =
        measurements[scenario][static_cast<uint32_t>(Implementation::kRadix)];
    if (legacy.runs == 0 || radix.runs == 0) {
      continue;
    }
    std::printf(
        "%-36s %10.2f %10.2f %10.2f %10.2f %10.2f\n",
        kBenchmarkScenarioSpecs[scenario].name,
        ratio(
            radix.addBeforeSpillUs,
            radix.runs,
            legacy.addBeforeSpillUs,
            legacy.runs),
        ratio(
            radix.addAfterSpillUs,
            radix.runs,
            legacy.addAfterSpillUs,
            legacy.runs),
        ratio(radix.spillUs, radix.runs, legacy.spillUs, legacy.runs),
        ratio(radix.finalizeUs, radix.runs, legacy.finalizeUs, legacy.runs),
        ratio(radix.outputUs, radix.runs, legacy.outputUs, legacy.runs));
  }
  std::printf(
      "Legacy SortBuffer uses row-based %s spill with SpillConfig JIT enabled "
      "for spill-run sorting and row-based spill merge. Radix spill uses radix "
      "row-format spill with compressionKind=%s.\n",
      legacyRowBasedSpillMode().c_str(),
      FLAGS_spill_compression_kind.c_str());
}

} // namespace

#define RADIX_SORT_SPILL_BENCHMARK_PAIR(name, index)  \
  BENCHMARK_NAMED_PARAM(legacySpillE2E, name, index); \
  BENCHMARK_RELATIVE_NAMED_PARAM(radixSpillE2E, name, index)

#ifndef RADIX_SORT_LARGE_BENCHMARK
RADIX_SORT_SPILL_BENCHMARK_PAIR(random_i64_256k_spill, 0);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(duplicate_i64_256k_spill, 1);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(null_heavy_i64_128k_spill, 2);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(eight_key_i64_128k_spill, 3);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(inline_varchar_128k_spill, 4);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(long_varchar_64k_spill, 5);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(varchar_common_prefix_128k_spill, 6);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(wide_fixed_payload_128k_spill, 7);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(very_wide_fixed_payload_64k_spill, 8);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(wide_string_payload_64k_spill, 9);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(random_i64_1m_spill, 10);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(eight_key_i64_1m_spill, 11);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(sixteen_key_i64_1m_spill, 12);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(wide_fixed_payload_1m_spill, 13);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(wide_string_payload_1m_spill, 14);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(bucket_write_key_only_1m_spill, 15);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(bucket_write_fixed_payload_1m_spill, 16);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(
    bucket_write_key_string_fixed_payload_1m_spill,
    17);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(bucket_write_string_payload_1m_spill, 18);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(bucket_write_complex_payload_1m_spill, 19);
#else
RADIX_SORT_SPILL_BENCHMARK_PAIR(random_i64_10m_spill, 0);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(eight_key_i64_10m_spill, 1);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(sixteen_key_i64_10m_spill, 2);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(wide_fixed_payload_10m_spill, 3);
#endif

#undef RADIX_SORT_SPILL_BENCHMARK_PAIR

} // namespace bytedance::bolt::exec::radixsort::benchmark

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  bytedance::bolt::exec::radixsort::benchmark::validateSpillCompressionKind();
  bytedance::bolt::memory::MemoryManager::initialize(
      bytedance::bolt::memory::MemoryManager::Options{});
  bytedance::bolt::filesystems::registerLocalFileSystem();
  if (!bytedance::bolt::isRegisteredVectorSerde()) {
    bytedance::bolt::serializer::presto::PrestoVectorSerde::
        registerVectorSerde();
  }
  using namespace bytedance::bolt::exec::radixsort::benchmark;
  sourcePool = bytedance::bolt::memory::memoryManager()->addLeafPool(
      "radix-spill-inputs");
  fixtures.resize(kBenchmarkScenarioSpecs.size());
  folly::runBenchmarks();
  printSummary();
  fixtures.clear();
  sourcePool.reset();
  return 0;
}
