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
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/benchmarks/RadixSortBenchmarkData.h"
#include "bolt/exec/radixsort/RadixSortBuffer.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/serializers/PrestoSerializer.h"

FOLLY_GFLAGS_DECLARE_bool(bm_profile);

DEFINE_string(
    spill_compression_kind,
    "none",
    "Compression kind for radix sort spill benchmark: none, lz4, or zstd.");

DEFINE_uint64(
    bolt_benchmark_spill_max_file_size,
    0,
    "Max spill file size for radix sort spill benchmark. 0 keeps the default "
    "unlimited SpillConfig behavior.");

DEFINE_uint64(
    bolt_benchmark_spill_write_buffer_size,
    1 << 20,
    "Write buffer size for both legacy and radix spill benchmark writers.");

DEFINE_uint32(
    bolt_benchmark_spill_runs,
    1,
    "Number of input-stage sorted runs to spill before adding the remaining "
    "input. 1 keeps the default benchmark behavior.");

DEFINE_string(
    bolt_benchmark_treatment,
    "unspecified",
    "External label for the pinned implementation under test, for example "
    "Original-H or Stage-B. The label does not affect the workload.");

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
  uint64_t spillRuns{0};
  uint64_t spilledRows{0};
  uint64_t spilledBytes{0};
  uint64_t spilledFiles{0};
  uint64_t spillWrites{0};
  uint64_t spillReadUs{0};
  uint64_t spillDecompressUs{0};
  uint64_t spillReadIOUs{0};
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

enum class PointerFreeKeyShape : uint8_t {
  kFixed,
  kVariable,
};

struct PointerFreeScenarioSpec {
  const char* name;
  PointerFreeKeyShape keyShape;
  uint32_t payloadColumns;
};

constexpr vector_size_t kPointerFreeRows = 1024 * 1024;
constexpr uint32_t kPointerFreeWidePayloadColumns = 4;
constexpr std::array<PointerFreeScenarioSpec, 6> kPointerFreeScenarioSpecs{{
    {"fixed_key_only_zstd", PointerFreeKeyShape::kFixed, 0},
    {"fixed_key_narrow_payload_zstd", PointerFreeKeyShape::kFixed, 1},
    {"fixed_key_wide_payload_zstd",
     PointerFreeKeyShape::kFixed,
     kPointerFreeWidePayloadColumns},
    {"variable_key_only_zstd", PointerFreeKeyShape::kVariable, 0},
    {"variable_key_narrow_payload_zstd", PointerFreeKeyShape::kVariable, 1},
    {"variable_key_wide_payload_zstd",
     PointerFreeKeyShape::kVariable,
     kPointerFreeWidePayloadColumns},
}};

struct PointerFreeFixture {
  struct ContentFingerprint {
    uint64_t sumPrimary{0};
    uint64_t sumSecondary{0};
    uint64_t xorPrimary{0};
    uint64_t xorSecondary{0};
  };

  ScenarioFixture data;
  uint64_t inputChecksum{0};
  uint64_t fixtureFingerprint{0};
  uint64_t protocolFingerprint{0};
  ContentFingerprint contentFingerprint;
};

struct PointerFreeMeasurements {
  uint64_t runs{0};
  uint64_t e2eTotalUs{0};
  uint64_t addBeforeSpillUs{0};
  uint64_t addAfterSpillUs{0};
  uint64_t spillUs{0};
  uint64_t finalizeUs{0};
  uint64_t outputUs{0};
  uint64_t spillRuns{0};
  uint64_t attributionSpillRuns{0};
  uint64_t attributionSpilledInputBytes{0};
  uint64_t attributionSpilledRows{0};
  uint64_t attributionSpilledBytes{0};
  uint64_t attributionSpilledFiles{0};
  uint64_t attributionSpillWrites{0};
  uint64_t spilledInputBytes{0};
  uint64_t spilledRows{0};
  uint64_t spilledBytes{0};
  uint64_t spilledFiles{0};
  uint64_t spillWrites{0};
  uint64_t attributionRuns{0};
  uint64_t spillTotalUs{0};
  uint64_t spillSortUs{0};
  uint64_t spillSerializationUs{0};
  uint64_t spillCompressUs{0};
  uint64_t spillWriteUs{0};
  uint64_t validationFinalReadUs{0};
  uint64_t validationFinalDecompressUs{0};
  uint64_t validationFinalReadIOUs{0};
  uint64_t validationPreOutputReadUs{0};
  uint64_t validationPreOutputDecompressUs{0};
  uint64_t validationPreOutputReadIOUs{0};
  uint64_t validationOutputUs{0};
  uint64_t validationOutputRows{0};
  uint64_t validationInputDiskRuns{0};
  uint64_t validationMergeStreams{0};
  PointerFreeFixture::ContentFingerprint validationContentFingerprint;
};

struct PointerFreeValidationResult {
  common::SpillStats spillStats;
  common::SpillReadStats finalReadStats;
  common::SpillReadStats preOutputReadStats;
  uint64_t outputUs{0};
  uint64_t outputRows{0};
  uint64_t inputDiskRuns{0};
  uint64_t mergeStreams{0};
  PointerFreeFixture::ContentFingerprint outputContentFingerprint;
};

std::shared_ptr<memory::MemoryPool> sourcePool;
std::vector<std::optional<ScenarioFixture>> fixtures;
std::array<std::array<Measurements, 2>, kBenchmarkScenarioSpecs.size()>
    measurements;
std::array<std::optional<PointerFreeFixture>, kPointerFreeScenarioSpecs.size()>
    pointerFreeFixtures;
std::array<PointerFreeMeasurements, kPointerFreeScenarioSpecs.size()>
    pointerFreeMeasurements;
std::array<bool, kPointerFreeScenarioSpecs.size()> pointerFreeExecuted{};

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

uint64_t checksumMix(uint64_t hash, uint64_t value) {
  hash ^= value + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
  return hash;
}

uint64_t fixtureInputChecksum(const ScenarioFixture& fixture) {
  uint64_t checksum = 0x243f6a8885a308d3ULL;
  for (const auto& input : fixture.inputs) {
    checksum = checksumMix(checksum, input->size());
    for (vector_size_t row = 0; row < input->size(); ++row) {
      for (const auto& child : input->children()) {
        checksum = checksumMix(checksum, child->hashValueAt(row));
      }
    }
  }
  return checksum;
}

uint64_t fingerprintString(uint64_t hash, const std::string& value) {
  for (const auto byte : value) {
    hash = checksumMix(hash, static_cast<uint8_t>(byte));
  }
  return hash;
}

uint64_t pointerFreeFixtureFingerprint(
    const ScenarioFixture& fixture,
    uint64_t inputChecksum) {
  uint64_t fingerprint = 0x13198a2e03707344ULL;
  uint64_t rows = 0;
  for (const auto& input : fixture.inputs) {
    rows += input->size();
  }
  fingerprint = checksumMix(fingerprint, inputChecksum);
  fingerprint = checksumMix(fingerprint, rows);
  fingerprint = fingerprintString(fingerprint, fixture.rowType->toString());
  fingerprint = checksumMix(fingerprint, fixture.inputs.size());
  fingerprint = checksumMix(fingerprint, fixture.keyChannels.size());
  for (const auto channel : fixture.keyChannels) {
    fingerprint = checksumMix(fingerprint, channel);
  }
  for (const auto& flags : fixture.keyFlags) {
    fingerprint = checksumMix(fingerprint, flags.ascending);
    fingerprint = checksumMix(fingerprint, flags.nullsFirst);
    fingerprint = checksumMix(fingerprint, flags.equalsOnly);
    fingerprint =
        checksumMix(fingerprint, static_cast<uint32_t>(flags.nullHandlingMode));
  }
  return fingerprint;
}

uint64_t pointerFreeProtocolFingerprint(uint32_t effectiveSpillRuns) {
  uint64_t fingerprint = 0xa4093822299f31d0ULL;
  fingerprint = checksumMix(fingerprint, kOutputBatchSize);
  fingerprint = checksumMix(fingerprint, effectiveSpillRuns);
  fingerprint =
      checksumMix(fingerprint, FLAGS_bolt_benchmark_spill_write_buffer_size);
  fingerprint =
      checksumMix(fingerprint, FLAGS_bolt_benchmark_spill_max_file_size);
  fingerprint = fingerprintString(fingerprint, "zstd");
  fingerprint = fingerprintString(fingerprint, "disabled");
  return fingerprint;
}

PointerFreeFixture::ContentFingerprint rowContentFingerprint(
    const RowVector& rows,
    vector_size_t row) {
  PointerFreeFixture::ContentFingerprint result{
      .sumPrimary = 0x243f6a8885a308d3ULL,
      .sumSecondary = 0x13198a2e03707344ULL,
      .xorPrimary = 0,
      .xorSecondary = 0,
  };
  for (uint32_t column = 0; column < rows.children().size(); ++column) {
    const auto childHash = rows.childAt(column)->hashValueAt(row);
    result.sumPrimary = checksumMix(result.sumPrimary, childHash);
    result.sumSecondary =
        checksumMix(result.sumSecondary, checksumMix(childHash, column + 1));
  }
  result.xorPrimary = checksumMix(result.sumPrimary, 0x9e3779b97f4a7c15ULL);
  result.xorSecondary = checksumMix(result.sumSecondary, 0xbf58476d1ce4e5b9ULL);
  return result;
}

void addContentFingerprint(
    PointerFreeFixture::ContentFingerprint& aggregate,
    const PointerFreeFixture::ContentFingerprint& rowFingerprint) {
  aggregate.sumPrimary += rowFingerprint.sumPrimary;
  aggregate.sumSecondary += rowFingerprint.sumSecondary;
  aggregate.xorPrimary ^= rowFingerprint.xorPrimary;
  aggregate.xorSecondary ^= rowFingerprint.xorSecondary;
}

PointerFreeFixture::ContentFingerprint fixtureContentFingerprint(
    const ScenarioFixture& fixture) {
  PointerFreeFixture::ContentFingerprint aggregate;
  for (const auto& input : fixture.inputs) {
    for (vector_size_t row = 0; row < input->size(); ++row) {
      addContentFingerprint(aggregate, rowContentFingerprint(*input, row));
    }
  }
  return aggregate;
}

void initializePointerFreeFixtures() {
  if (pointerFreeFixtures.front().has_value()) {
    return;
  }

  auto fixedSource = makeFixture(
      sourcePool.get(),
      {"pointer_free_fixed_source",
       ScenarioKind::kWideFixedPayload,
       kPointerFreeRows},
      ScenarioProfile::kSpill);
  auto variableSource = makeFixture(
      sourcePool.get(),
      {"pointer_free_variable_source",
       ScenarioKind::kVarcharCommonPrefix,
       kPointerFreeRows},
      ScenarioProfile::kSpill);
  BOLT_CHECK_EQ(fixedSource.inputs.size(), variableSource.inputs.size());

  for (uint32_t scenario = 0; scenario < kPointerFreeScenarioSpecs.size();
       ++scenario) {
    const auto& spec = kPointerFreeScenarioSpecs[scenario];
    const auto& keySource = spec.keyShape == PointerFreeKeyShape::kFixed
        ? fixedSource
        : variableSource;

    PointerFreeFixture result;
    result.data.keyChannels = {0};
    result.data.keyFlags = keySource.keyFlags;

    std::vector<std::string> names{keySource.rowType->nameOf(0)};
    std::vector<TypePtr> types{keySource.rowType->childAt(0)};
    for (uint32_t payload = 0; payload < spec.payloadColumns; ++payload) {
      names.push_back(fixedSource.rowType->nameOf(payload + 1));
      types.push_back(fixedSource.rowType->childAt(payload + 1));
    }
    result.data.rowType = ROW(std::move(names), std::move(types));
    result.data.idChannel = std::numeric_limits<column_index_t>::max();

    result.data.inputs.reserve(keySource.inputs.size());
    for (uint32_t batch = 0; batch < keySource.inputs.size(); ++batch) {
      const auto& keyInput = keySource.inputs[batch];
      const auto& payloadInput = fixedSource.inputs[batch];
      BOLT_CHECK_EQ(keyInput->size(), payloadInput->size());
      std::vector<VectorPtr> children{keyInput->childAt(0)};
      for (uint32_t payload = 0; payload < spec.payloadColumns; ++payload) {
        children.push_back(payloadInput->childAt(payload + 1));
      }
      result.data.inputs.push_back(std::make_shared<RowVector>(
          sourcePool.get(),
          result.data.rowType,
          nullptr,
          keyInput->size(),
          std::move(children)));
    }
    pointerFreeFixtures[scenario] = std::move(result);
  }
}

PointerFreeFixture& pointerFreeFixtureFor(uint32_t scenario) {
  BOLT_CHECK_LT(scenario, kPointerFreeScenarioSpecs.size());
  initializePointerFreeFixtures();
  return *pointerFreeFixtures[scenario];
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

double averageMs(uint64_t totalUs, uint64_t runs) {
  return averageUs(totalUs, runs) / 1000;
}

double averageMiB(uint64_t totalBytes, uint64_t runs) {
  return runs == 0 ? 0 : static_cast<double>(totalBytes) / runs / (1024 * 1024);
}

double averagePerRun(uint64_t total, uint64_t runs) {
  return runs == 0 ? 0 : static_cast<double>(total) / runs;
}

double perRow(uint64_t total, uint64_t rows) {
  return rows == 0 ? 0 : static_cast<double>(total) / rows;
}

double nsPerRow(uint64_t totalUs, uint64_t rows) {
  return perRow(totalUs, rows) * 1000;
}

int64_t signedDifference(uint64_t minuend, uint64_t subtrahend) {
  if (minuend >= subtrahend) {
    const auto difference = minuend - subtrahend;
    BOLT_CHECK_LE(
        difference, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    return static_cast<int64_t>(difference);
  }
  const auto difference = subtrahend - minuend;
  BOLT_CHECK_LE(
      difference, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
  return -static_cast<int64_t>(difference);
}

double signedNsPerRow(int64_t totalUs, uint64_t rows) {
  return rows == 0 ? 0 : static_cast<double>(totalUs) / rows * 1000;
}

double averageSignedMs(int64_t totalUs, uint64_t runs) {
  return runs == 0 ? 0 : static_cast<double>(totalUs) / runs / 1000;
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

uint32_t spillInputSplit(uint32_t scenario, uint32_t inputBatches) {
  return kBenchmarkScenarioSpecs[scenario].kind ==
          ScenarioKind::kSingleKeyVeryWideMixedPayload
      ? inputBatches
      : inputBatches / 2;
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
      FLAGS_bolt_benchmark_spill_max_file_size,
      false,
      FLAGS_bolt_benchmark_spill_write_buffer_size,
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

uint32_t normalizedSpillRuns(uint32_t inputBatches) {
  return std::max<uint32_t>(
      1,
      std::min<uint32_t>(
          FLAGS_bolt_benchmark_spill_runs,
          inputBatches == 0 ? 1 : inputBatches));
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
  BOLT_CHECK_GT(
      FLAGS_bolt_benchmark_spill_runs,
      0,
      "bolt_benchmark_spill_runs must be positive");
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

struct InputStageResult {
  uint32_t split;
  uint64_t addInputUs{0};
  uint64_t spillUs{0};
};

template <typename Buffer>
InputStageResult addAndSpillInputStage(
    Buffer& buffer,
    const std::vector<RowVectorPtr>& inputs,
    uint32_t split,
    uint32_t runs) {
  BOLT_CHECK_GT(runs, 0);
  BOLT_CHECK_GE(split, runs);
  InputStageResult result{0, 0, 0};
  uint32_t nextInput = 0;
  for (uint32_t run = 0; run < runs; ++run) {
    const auto end =
        static_cast<uint32_t>((static_cast<uint64_t>(run + 1) * split) / runs);
    BOLT_CHECK_GT(end, nextInput);
    const auto addInputBegin = std::chrono::steady_clock::now();
    for (; nextInput < end; ++nextInput) {
      buffer.addInput(inputs[nextInput]);
    }
    const auto addInputUs = elapsedUs(addInputBegin);
    const auto spillBegin = std::chrono::steady_clock::now();
    buffer.spill();
    const auto spillUs = elapsedUs(spillBegin);
    result.addInputUs += addInputUs;
    result.spillUs += spillUs;
  }
  result.split = nextInput;
  return result;
}

void record(
    uint32_t scenario,
    Implementation implementation,
    std::optional<common::SpillStats> spillStats,
    std::optional<common::SpillReadStats> spillReadStats,
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
    result.spillRuns += spillStats->spillRuns;
    result.spilledRows += spillStats->spilledRows;
    result.spilledBytes += spillStats->spilledBytes;
    result.spilledFiles += spillStats->spilledFiles;
    result.spillWrites += spillStats->spillWrites;
  }
  if (spillReadStats.has_value()) {
    result.spillReadUs += spillReadStats->spillReadTimeUs;
    result.spillDecompressUs += spillReadStats->spillDecompressTimeUs;
    result.spillReadIOUs += spillReadStats->spillReadIOTimeUs;
  }
}

void recordPointerFree(
    uint32_t scenario,
    const common::SpillStats& spillStats,
    uint64_t e2eTotalUs,
    uint64_t addBeforeSpillUs,
    uint64_t addAfterSpillUs,
    uint64_t spillUs,
    uint64_t finalizeUs,
    uint64_t outputUs) {
  auto& result = pointerFreeMeasurements[scenario];
  ++result.runs;
  result.e2eTotalUs += e2eTotalUs;
  result.addBeforeSpillUs += addBeforeSpillUs;
  result.addAfterSpillUs += addAfterSpillUs;
  result.spillUs += spillUs;
  result.finalizeUs += finalizeUs;
  result.outputUs += outputUs;
  result.spillRuns += spillStats.spillRuns;
  result.spilledInputBytes += spillStats.spilledInputBytes;
  result.spilledRows += spillStats.spilledRows;
  result.spilledBytes += spillStats.spilledBytes;
  result.spilledFiles += spillStats.spilledFiles;
  result.spillWrites += spillStats.spillWrites;
}

void recordPointerFreeAttribution(
    uint32_t scenario,
    const common::SpillStats& spillStats,
    const common::SpillReadStats& finalReadStats,
    const common::SpillReadStats& preOutputReadStats,
    uint64_t outputUs,
    uint64_t outputRows,
    uint64_t inputDiskRuns,
    uint64_t mergeStreams,
    const PointerFreeFixture::ContentFingerprint& outputContentFingerprint) {
  auto& result = pointerFreeMeasurements[scenario];
  if (result.attributionRuns == 0) {
    result.validationContentFingerprint = outputContentFingerprint;
  } else {
    BOLT_CHECK_EQ(
        result.validationContentFingerprint.sumPrimary,
        outputContentFingerprint.sumPrimary);
    BOLT_CHECK_EQ(
        result.validationContentFingerprint.sumSecondary,
        outputContentFingerprint.sumSecondary);
    BOLT_CHECK_EQ(
        result.validationContentFingerprint.xorPrimary,
        outputContentFingerprint.xorPrimary);
    BOLT_CHECK_EQ(
        result.validationContentFingerprint.xorSecondary,
        outputContentFingerprint.xorSecondary);
  }
  ++result.attributionRuns;
  result.attributionSpillRuns += spillStats.spillRuns;
  result.attributionSpilledInputBytes += spillStats.spilledInputBytes;
  result.attributionSpilledRows += spillStats.spilledRows;
  result.attributionSpilledBytes += spillStats.spilledBytes;
  result.attributionSpilledFiles += spillStats.spilledFiles;
  result.attributionSpillWrites += spillStats.spillWrites;
  result.spillTotalUs += spillStats.spillTotalTimeUs;
  result.spillSortUs += spillStats.spillSortTimeUs;
  result.spillSerializationUs += spillStats.spillSerializationTimeUs;
  // Radix spill uses writeEncodedBlock(), whose spillFlushTimeUs contribution
  // is the encoded block's ZSTD compression time for this pinned-codec path.
  result.spillCompressUs += spillStats.spillFlushTimeUs;
  result.spillWriteUs += spillStats.spillWriteTimeUs;
  result.validationFinalReadUs += finalReadStats.spillReadTimeUs;
  result.validationFinalDecompressUs += finalReadStats.spillDecompressTimeUs;
  result.validationFinalReadIOUs += finalReadStats.spillReadIOTimeUs;
  result.validationPreOutputReadUs += preOutputReadStats.spillReadTimeUs;
  result.validationPreOutputDecompressUs +=
      preOutputReadStats.spillDecompressTimeUs;
  result.validationPreOutputReadIOUs += preOutputReadStats.spillReadIOTimeUs;
  result.validationOutputUs += outputUs;
  result.validationOutputRows += outputRows;
  result.validationInputDiskRuns += inputDiskRuns;
  result.validationMergeStreams += mergeStreams;
}

int32_t comparePointerFreeKeys(
    const ScenarioFixture& fixture,
    const RowVector& left,
    vector_size_t leftRow,
    const RowVector& right,
    vector_size_t rightRow) {
  for (uint32_t key = 0; key < fixture.keyChannels.size(); ++key) {
    const auto channel = fixture.keyChannels[key];
    const auto result = left.childAt(channel)->compare(
        right.childAt(channel).get(), leftRow, rightRow, fixture.keyFlags[key]);
    BOLT_CHECK(result.has_value());
    if (result.value() != 0) {
      return result.value();
    }
  }
  return 0;
}

void savePointerFreeKeyRow(
    const ScenarioFixture& fixture,
    const RowVector& source,
    vector_size_t sourceRow,
    memory::MemoryPool* pool,
    RowVectorPtr& destination) {
  if (destination == nullptr) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    std::vector<VectorPtr> children;
    names.reserve(fixture.keyChannels.size());
    types.reserve(fixture.keyChannels.size());
    children.reserve(fixture.keyChannels.size());
    for (const auto channel : fixture.keyChannels) {
      names.push_back(fixture.rowType->nameOf(channel));
      const auto& type = fixture.rowType->childAt(channel);
      types.push_back(type);
      children.push_back(BaseVector::create(type, 1, pool));
    }
    destination = std::make_shared<RowVector>(
        pool,
        ROW(std::move(names), std::move(types)),
        nullptr,
        1,
        std::move(children));
  }
  for (uint32_t key = 0; key < fixture.keyChannels.size(); ++key) {
    const auto channel = fixture.keyChannels[key];
    destination->childAt(key)->copy(
        source.childAt(channel).get(), 0, sourceRow, 1);
  }
}

int32_t comparePointerFreeKeySnapshot(
    const ScenarioFixture& fixture,
    const RowVector& leftKey,
    const RowVector& right,
    vector_size_t rightRow) {
  for (uint32_t key = 0; key < fixture.keyChannels.size(); ++key) {
    const auto result = leftKey.childAt(key)->compare(
        right.childAt(fixture.keyChannels[key]).get(),
        0,
        rightRow,
        fixture.keyFlags[key]);
    BOLT_CHECK(result.has_value());
    if (result.value() != 0) {
      return result.value();
    }
  }
  return 0;
}

PointerFreeValidationResult runPointerFreeValidation(uint32_t scenario) {
  auto& fixture = pointerFreeFixtureFor(scenario);
  const auto split = static_cast<uint32_t>(fixture.data.inputs.size() / 2);
  const auto runs = normalizedSpillRuns(split);
  fixture.inputChecksum = fixtureInputChecksum(fixture.data);
  fixture.fixtureFingerprint =
      pointerFreeFixtureFingerprint(fixture.data, fixture.inputChecksum);
  fixture.protocolFingerprint = pointerFreeProtocolFingerprint(runs);
  fixture.contentFingerprint = fixtureContentFingerprint(fixture.data);

  auto directory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(
      directory->path,
      "pointer-free-production-zstd-spill-validation",
      "zstd",
      "disabled");
  auto pool = memory::memoryManager()->addLeafPool(
      fmt::format("pointer-free-production-zstd-validation-{}", scenario));
  RadixSortBuffer buffer(
      fixture.data.rowType,
      fixture.data.keyChannels,
      fixture.data.keyFlags,
      pool.get(),
      &config);

  const auto inputStage =
      addAndSpillInputStage(buffer, fixture.data.inputs, split, runs);
  const auto inputDiskRuns = buffer.testingSpilledRunCount();
  BOLT_CHECK_EQ(inputDiskRuns, runs);
  for (uint32_t index = inputStage.split; index < fixture.data.inputs.size();
       ++index) {
    buffer.addInput(fixture.data.inputs[index]);
  }
  buffer.noMoreInput();
  const auto mergeStreams = buffer.testingMergeStreamCount();
  const auto expectedResidentStreams =
      inputStage.split < fixture.data.inputs.size() ? 1 : 0;
  BOLT_CHECK_EQ(mergeStreams, inputDiskRuns + expectedResidentStreams);
  const auto preOutputReadStats =
      buffer.spillReadStats().value_or(common::SpillReadStats{});

  uint64_t outputRows = 0;
  RowVectorPtr previousBatchLastKey;
  PointerFreeFixture::ContentFingerprint outputFingerprint;
  uint64_t outputUs = 0;
  for (;;) {
    const auto outputBegin = std::chrono::steady_clock::now();
    auto output = buffer.getOutput(kOutputBatchSize);
    outputUs += elapsedUs(outputBegin);
    if (output == nullptr) {
      break;
    }
    for (vector_size_t row = 0; row < output->size(); ++row) {
      if (outputRows != 0) {
        const auto comparison = row == 0
            ? comparePointerFreeKeySnapshot(
                  fixture.data, *previousBatchLastKey, *output, row)
            : comparePointerFreeKeys(
                  fixture.data, *output, row - 1, *output, row);
        BOLT_CHECK_LE(
            comparison,
            0,
            "Pointer-free validation saw unsorted output for scenario '{}'"
            " at output row {}",
            kPointerFreeScenarioSpecs[scenario].name,
            outputRows);
      }
      const auto rowFingerprint = rowContentFingerprint(*output, row);
      addContentFingerprint(outputFingerprint, rowFingerprint);
      ++outputRows;
    }
    BOLT_CHECK_GT(output->size(), 0);
    savePointerFreeKeyRow(
        fixture.data,
        *output,
        output->size() - 1,
        pool.get(),
        previousBatchLastKey);
    folly::doNotOptimizeAway(output);
  }

  BOLT_CHECK_EQ(outputRows, kPointerFreeRows);
  BOLT_CHECK_EQ(
      outputFingerprint.sumPrimary,
      fixture.contentFingerprint.sumPrimary,
      "Pointer-free validation content primary checksum mismatch for scenario "
      "'{}'",
      kPointerFreeScenarioSpecs[scenario].name);
  BOLT_CHECK_EQ(
      outputFingerprint.sumSecondary,
      fixture.contentFingerprint.sumSecondary,
      "Pointer-free validation content secondary checksum mismatch for "
      "scenario '{}'",
      kPointerFreeScenarioSpecs[scenario].name);
  BOLT_CHECK_EQ(
      outputFingerprint.xorPrimary,
      fixture.contentFingerprint.xorPrimary,
      "Pointer-free validation content primary xor mismatch for scenario '{}'",
      kPointerFreeScenarioSpecs[scenario].name);
  BOLT_CHECK_EQ(
      outputFingerprint.xorSecondary,
      fixture.contentFingerprint.xorSecondary,
      "Pointer-free validation content secondary xor mismatch for scenario "
      "'{}'",
      kPointerFreeScenarioSpecs[scenario].name);

  const auto spillStats = buffer.spilledStats();
  const auto finalReadStats = buffer.spillReadStats();
  BOLT_CHECK(spillStats.has_value());
  BOLT_CHECK(finalReadStats.has_value());
  return PointerFreeValidationResult{
      .spillStats = *spillStats,
      .finalReadStats = *finalReadStats,
      .preOutputReadStats = preOutputReadStats,
      .outputUs = outputUs,
      .outputRows = outputRows,
      .inputDiskRuns = inputDiskRuns,
      .mergeStreams = mergeStreams,
      .outputContentFingerprint = outputFingerprint,
  };
}

void pointerFreeProductionZstd(unsigned iterations, uint32_t scenario) {
  folly::BenchmarkSuspender suspender;
  BOLT_CHECK(
      FLAGS_bolt_benchmark_treatment == "Original-H" ||
          FLAGS_bolt_benchmark_treatment == "Stage-B",
      "Pointer-free production benchmark requires "
      "--bolt_benchmark_treatment=Original-H or Stage-B");
  pointerFreeExecuted.at(scenario) = true;
  const auto& fixture = pointerFreeFixtureFor(scenario);
  const auto split = static_cast<uint32_t>(fixture.data.inputs.size() / 2);
  const auto runs = normalizedSpillRuns(split);

  for (unsigned iteration = 0; iteration < iterations; ++iteration) {
    auto directory = exec::test::TempDirectoryPath::create();
    auto config = spillConfig(
        directory->path,
        "pointer-free-production-zstd-spill-benchmark",
        "zstd",
        "disabled");
    auto pool = memory::memoryManager()->addLeafPool(
        fmt::format("pointer-free-production-zstd-spill-{}", scenario));
    RadixSortBuffer buffer(
        fixture.data.rowType,
        fixture.data.keyChannels,
        fixture.data.keyFlags,
        pool.get(),
        &config);

    suspender.dismiss();
    const auto e2eBegin = std::chrono::steady_clock::now();
    const auto inputStage =
        addAndSpillInputStage(buffer, fixture.data.inputs, split, runs);
    const auto addAfterSpillBegin = std::chrono::steady_clock::now();
    for (uint32_t index = inputStage.split; index < fixture.data.inputs.size();
         ++index) {
      buffer.addInput(fixture.data.inputs[index]);
    }
    const auto addAfterSpillUs = elapsedUs(addAfterSpillBegin);
    const auto finalizeBegin = std::chrono::steady_clock::now();
    buffer.noMoreInput();
    const auto finalizeUs = elapsedUs(finalizeBegin);
    const auto outputBegin = std::chrono::steady_clock::now();
    const auto outputRows = drain(buffer);
    const auto outputUs = elapsedUs(outputBegin);
    const auto e2eTotalUs = elapsedUs(e2eBegin);
    suspender.rehire();

    BOLT_CHECK_EQ(outputRows, kPointerFreeRows);
    const auto spillStats = buffer.spilledStats();
    BOLT_CHECK(spillStats.has_value());
    if (FLAGS_bm_profile) {
      recordPointerFree(
          scenario,
          *spillStats,
          e2eTotalUs,
          inputStage.addInputUs,
          addAfterSpillUs,
          inputStage.spillUs,
          finalizeUs,
          outputUs);
    }
  }
}

void validateExecutedPointerFreeScenarios() {
  for (uint32_t scenario = 0; scenario < kPointerFreeScenarioSpecs.size();
       ++scenario) {
    if (!pointerFreeExecuted[scenario]) {
      continue;
    }
    const auto validation = runPointerFreeValidation(scenario);
    recordPointerFreeAttribution(
        scenario,
        validation.spillStats,
        validation.finalReadStats,
        validation.preOutputReadStats,
        validation.outputUs,
        validation.outputRows,
        validation.inputDiskRuns,
        validation.mergeStreams,
        validation.outputContentFingerprint);
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
    const auto split = spillInputSplit(scenario, fixture.inputs.size());
    const auto runs = normalizedSpillRuns(split);
    const auto inputStage =
        addAndSpillInputStage(buffer, fixture.inputs, split, runs);
    const auto addAfterSpillBegin = std::chrono::steady_clock::now();
    for (uint32_t index = inputStage.split; index < fixture.inputs.size();
         ++index) {
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
        buffer.spillReadStats(),
        inputStage.addInputUs,
        addAfterSpillUs,
        inputStage.spillUs,
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
    const auto split = spillInputSplit(scenario, fixture.inputs.size());
    const auto runs = normalizedSpillRuns(split);
    const auto inputStage =
        addAndSpillInputStage(buffer, fixture.inputs, split, runs);
    const auto addAfterSpillBegin = std::chrono::steady_clock::now();
    for (uint32_t index = inputStage.split; index < fixture.inputs.size();
         ++index) {
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
        buffer.spillReadStats(),
        inputStage.addInputUs,
        addAfterSpillUs,
        inputStage.spillUs,
        finalizeUs,
        outputUs);
  }
}

void printPointerFreeSummary() {
  const auto executed = std::any_of(
      pointerFreeExecuted.begin(), pointerFreeExecuted.end(), [](bool value) {
        return value;
      });
  if (!executed) {
    return;
  }

  std::printf(
      "\nPointer-free production ZSTD summary "
      "(operator processing path; setup and teardown excluded)\n");
  std::printf(
      "These scenarios are workload-only fixtures for comparing pinned "
      "Original-H and Stage-B binaries with identical fixture and protocol "
      "fingerprints.\n");
  std::printf(
      "Operator E2E starts at the first addInput and ends after the terminal "
      "getOutput; fixture generation, object construction, destruction, and "
      "file cleanup are excluded.\n");
  std::printf(
      "Parameters: codec=zstd rowMode=disabled rows=%d outputBatch=%d "
      "requestedSpillRuns=%u writeBufferBytes=%llu maxFileSize=%llu "
      "treatment=%s\n",
      kPointerFreeRows,
      kOutputBatchSize,
      FLAGS_bolt_benchmark_spill_runs,
      static_cast<unsigned long long>(
          FLAGS_bolt_benchmark_spill_write_buffer_size),
      static_cast<unsigned long long>(FLAGS_bolt_benchmark_spill_max_file_size),
      FLAGS_bolt_benchmark_treatment.c_str());
  std::printf(
      "Implementation metadata: radixBlockBytes=%llu\n",
      static_cast<unsigned long long>(kRadixSortSpillBufferSize));
  if (FLAGS_bm_profile) {
    std::printf(
        "Custom phase/E2E aggregates come from Folly's single --bm_profile "
        "invocation. All phase/E2E timings are ns/input-row; spill counters "
        "are per-run averages.\n");
    std::printf(
        "Outer phases are disjoint; phase sum plus signed residual explains "
        "the direct E2E measurement.\n");
    std::printf(
        "%-40s %10s %14s %14s %11s %12s %11s %11s %11s %30s %11s %11s %11s %11s\n",
        "scenario",
        "timedIters",
        "spillCalls/run",
        "spillRows/run",
        "files/run",
        "writes/run",
        "add before",
        "spill",
        "add after",
        "finalize+merge/read-init",
        "output",
        "phase sum",
        "residual",
        "operator E2E");
    for (uint32_t scenario = 0; scenario < kPointerFreeScenarioSpecs.size();
         ++scenario) {
      const auto& result = pointerFreeMeasurements[scenario];
      if (result.runs == 0) {
        continue;
      }
      const auto phaseSumUs = result.addBeforeSpillUs + result.spillUs +
          result.addAfterSpillUs + result.finalizeUs + result.outputUs;
      const auto totalRows = result.runs * kPointerFreeRows;
      const auto harnessResidualUs =
          signedDifference(result.e2eTotalUs, phaseSumUs);
      std::printf(
          "%-40s %10llu %14.2f %14.0f %11.2f %12.2f %11.2f %11.2f %11.2f %30.2f %11.2f %11.2f %+11.2f %11.2f\n",
          kPointerFreeScenarioSpecs[scenario].name,
          static_cast<unsigned long long>(result.runs),
          averagePerRun(result.spillRuns, result.runs),
          averagePerRun(result.spilledRows, result.runs),
          averagePerRun(result.spilledFiles, result.runs),
          averagePerRun(result.spillWrites, result.runs),
          nsPerRow(result.addBeforeSpillUs, totalRows),
          nsPerRow(result.spillUs, totalRows),
          nsPerRow(result.addAfterSpillUs, totalRows),
          nsPerRow(result.finalizeUs, totalRows),
          nsPerRow(result.outputUs, totalRows),
          nsPerRow(phaseSumUs, totalRows),
          signedNsPerRow(harnessResidualUs, totalRows),
          nsPerRow(result.e2eTotalUs, totalRows));
    }
  } else {
    std::printf(
        "Custom phase/E2E timing summary omitted; use --bm_profile so it "
        "comes from Folly's single measured invocation.\n");
  }

  std::printf(
      "\nPointer-free nested spill attribution from post-benchmark validation "
      "runs\n");
  std::printf(
      "These nested stats come from one fully untimed correctness/attribution "
      "RadixSortBuffer run per scenario. They are not additive with the timed "
      "outer-phase/E2E summary above.\n");
  std::printf(
      "%-40s %8s %9s %9s %9s %9s %9s %9s %10s %9s %9s %9s %9s %10s %18s %18s %18s\n",
      "scenario",
      "attrRun",
      "spill calls",
      "rows",
      "files",
      "writes",
      "raw B/r",
      "disk B/r",
      "spill ms",
      "sort ms",
      "sect ms",
      "zstd ms",
      "write ms",
      "other ms",
      "input checksum",
      "fixture fingerprint",
      "protocol fingerprint");
  for (uint32_t scenario = 0; scenario < kPointerFreeScenarioSpecs.size();
       ++scenario) {
    const auto& result = pointerFreeMeasurements[scenario];
    if (result.attributionRuns == 0) {
      continue;
    }
    const auto& fixture = pointerFreeFixtureFor(scenario);
    const auto accountedSpillUs = result.spillSortUs +
        result.spillSerializationUs + result.spillCompressUs +
        result.spillWriteUs;
    const auto spillOtherUs =
        signedDifference(result.spillTotalUs, accountedSpillUs);
    std::printf(
        "%-40s %8llu %9.2f %9.0f %9.2f %9.2f %9.2f %9.2f %10.2f %9.2f %9.2f %9.2f %9.2f %+10.2f 0x%016llx 0x%016llx 0x%016llx\n",
        kPointerFreeScenarioSpecs[scenario].name,
        static_cast<unsigned long long>(result.attributionRuns),
        averagePerRun(result.attributionSpillRuns, result.attributionRuns),
        averagePerRun(result.attributionSpilledRows, result.attributionRuns),
        averagePerRun(result.attributionSpilledFiles, result.attributionRuns),
        averagePerRun(result.attributionSpillWrites, result.attributionRuns),
        perRow(
            result.attributionSpilledInputBytes, result.attributionSpilledRows),
        perRow(result.attributionSpilledBytes, result.attributionSpilledRows),
        averageMs(result.spillTotalUs, result.attributionRuns),
        averageMs(result.spillSortUs, result.attributionRuns),
        averageMs(result.spillSerializationUs, result.attributionRuns),
        averageMs(result.spillCompressUs, result.attributionRuns),
        averageMs(result.spillWriteUs, result.attributionRuns),
        averageSignedMs(spillOtherUs, result.attributionRuns),
        static_cast<unsigned long long>(fixture.inputChecksum),
        static_cast<unsigned long long>(fixture.fixtureFingerprint),
        static_cast<unsigned long long>(fixture.protocolFingerprint));
  }

  std::printf(
      "\nPointer-free validation identity and topology from the same untimed "
      "runs\n");
  std::printf(
      "A printed row means sortedness and the complete order-independent "
      "content fingerprint were validated successfully.\n");
  std::printf(
      "%-40s %-12s %9s %9s %9s %9s %9s %9s %13s %18s %18s %18s %71s\n",
      "scenario",
      "treatment",
      "out rows",
      "req runs",
      "eff runs",
      "disk runs",
      "streams",
      "spill calls",
      "unspilled rows",
      "input checksum",
      "fixture fingerprint",
      "protocol fingerprint",
      "output content fingerprint (sum1:sum2:xor1:xor2)");
  for (uint32_t scenario = 0; scenario < kPointerFreeScenarioSpecs.size();
       ++scenario) {
    const auto& result = pointerFreeMeasurements[scenario];
    if (result.attributionRuns == 0) {
      continue;
    }
    const auto& fixture = pointerFreeFixtureFor(scenario);
    const auto split = static_cast<uint32_t>(fixture.data.inputs.size() / 2);
    const auto effectiveRuns = normalizedSpillRuns(split);
    BOLT_CHECK_LE(
        result.attributionSpilledRows,
        result.attributionRuns * static_cast<uint64_t>(kPointerFreeRows));
    const auto residentRows =
        result.attributionRuns * static_cast<uint64_t>(kPointerFreeRows) -
        result.attributionSpilledRows;
    const auto& content = result.validationContentFingerprint;
    std::printf(
        "%-40s %-12s %9.0f %9u %9u %9.2f %9.2f %9.2f %13.0f 0x%016llx 0x%016llx 0x%016llx %016llx:%016llx:%016llx:%016llx\n",
        kPointerFreeScenarioSpecs[scenario].name,
        FLAGS_bolt_benchmark_treatment.c_str(),
        averagePerRun(result.validationOutputRows, result.attributionRuns),
        FLAGS_bolt_benchmark_spill_runs,
        effectiveRuns,
        averagePerRun(result.validationInputDiskRuns, result.attributionRuns),
        averagePerRun(result.validationMergeStreams, result.attributionRuns),
        averagePerRun(result.attributionSpillRuns, result.attributionRuns),
        averagePerRun(residentRows, result.attributionRuns),
        static_cast<unsigned long long>(fixture.inputChecksum),
        static_cast<unsigned long long>(fixture.fixtureFingerprint),
        static_cast<unsigned long long>(fixture.protocolFingerprint),
        static_cast<unsigned long long>(content.sumPrimary),
        static_cast<unsigned long long>(content.sumSecondary),
        static_cast<unsigned long long>(content.xorPrimary),
        static_cast<unsigned long long>(content.xorSecondary));
  }

  std::printf(
      "\nPointer-free nested output/read attribution from the same untimed runs\n");
  std::printf(
      "out read includes decompression and I/O. Nested columns overlap and "
      "are non-additive; out non-read wall is the signed output-wall residual "
      "after subtracting out read, not measured CPU time.\n");
  std::printf(
      "%-40s %9s %9s %10s %10s %10s %10s %17s %10s\n",
      "scenario",
      "pre read",
      "pre zstd",
      "output ms",
      "out read",
      "out zstd",
      "read-nzstd",
      "out non-read wall",
      "out readIO");
  for (uint32_t scenario = 0; scenario < kPointerFreeScenarioSpecs.size();
       ++scenario) {
    const auto& result = pointerFreeMeasurements[scenario];
    if (result.attributionRuns == 0) {
      continue;
    }
    BOLT_CHECK_GE(
        result.validationFinalReadUs, result.validationPreOutputReadUs);
    BOLT_CHECK_GE(
        result.validationFinalDecompressUs,
        result.validationPreOutputDecompressUs);
    BOLT_CHECK_GE(
        result.validationFinalReadIOUs, result.validationPreOutputReadIOUs);
    const auto outputReadUs =
        result.validationFinalReadUs - result.validationPreOutputReadUs;
    const auto outputDecompressUs = result.validationFinalDecompressUs -
        result.validationPreOutputDecompressUs;
    const auto outputReadIOUs =
        result.validationFinalReadIOUs - result.validationPreOutputReadIOUs;
    const auto outputReadNonDecompressUs =
        signedDifference(outputReadUs, outputDecompressUs);
    const auto outputNonReadUs =
        signedDifference(result.validationOutputUs, outputReadUs);
    std::printf(
        "%-40s %9.2f %9.2f %10.2f %10.2f %10.2f %+10.2f %+17.2f %10.2f\n",
        kPointerFreeScenarioSpecs[scenario].name,
        averageMs(result.validationPreOutputReadUs, result.attributionRuns),
        averageMs(
            result.validationPreOutputDecompressUs, result.attributionRuns),
        averageMs(result.validationOutputUs, result.attributionRuns),
        averageMs(outputReadUs, result.attributionRuns),
        averageMs(outputDecompressUs, result.attributionRuns),
        averageSignedMs(outputReadNonDecompressUs, result.attributionRuns),
        averageSignedMs(outputNonReadUs, result.attributionRuns),
        averageMs(outputReadIOUs, result.attributionRuns));
  }
}

void printSummary() {
  std::printf(
      "\nRadix sort spill benchmark summary (input generation excluded)\n");
  std::printf(
      "Benchmark spill parameters: compressionKind=%s maxFileSize=%llu "
      "inputStageRuns=%u\n",
      FLAGS_spill_compression_kind.c_str(),
      static_cast<unsigned long long>(FLAGS_bolt_benchmark_spill_max_file_size),
      FLAGS_bolt_benchmark_spill_runs);
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
      "%-36s %-8s %10s %10s %10s %10s %10s %12s %8s %8s %10s\n",
      "scenario",
      "impl",
      "add1 ms",
      "add2 ms",
      "spill ms",
      "final ms",
      "out ms",
      "spill MiB",
      "runs",
      "files",
      "writes");
  for (uint32_t scenario = 0; scenario < kBenchmarkScenarioSpecs.size();
       ++scenario) {
    for (uint32_t impl = 0; impl < 2; ++impl) {
      const auto& result = measurements[scenario][impl];
      if (result.runs == 0) {
        continue;
      }
      std::printf(
          "%-36s %-8s %10.2f %10.2f %10.2f %10.2f %10.2f %12.2f %8.2f %8.2f %10.2f\n",
          kBenchmarkScenarioSpecs[scenario].name,
          impl == 0 ? "legacy" : "radix",
          static_cast<double>(result.addBeforeSpillUs) / result.runs / 1000,
          static_cast<double>(result.addAfterSpillUs) / result.runs / 1000,
          static_cast<double>(result.spillUs) / result.runs / 1000,
          static_cast<double>(result.finalizeUs) / result.runs / 1000,
          static_cast<double>(result.outputUs) / result.runs / 1000,
          averageMiB(result.spilledBytes, result.runs),
          static_cast<double>(result.spillRuns) / result.runs,
          static_cast<double>(result.spilledFiles) / result.runs,
          static_cast<double>(result.spillWrites) / result.runs);
    }
  }

  std::printf("\nSpill read breakdown\n");
  std::printf(
      "%-48s %-8s %12s %12s %12s %12s\n",
      "scenario",
      "impl",
      "read ms",
      "decomp ms",
      "readIO ms",
      "read MiB/s");
  for (uint32_t scenario = 0; scenario < kBenchmarkScenarioSpecs.size();
       ++scenario) {
    for (uint32_t impl = 0; impl < 2; ++impl) {
      const auto& result = measurements[scenario][impl];
      if (result.runs == 0) {
        continue;
      }
      const auto readSeconds =
          averageUs(result.spillReadUs, result.runs) / 1'000'000;
      const auto readMiBPerSecond = readSeconds == 0
          ? 0
          : averageMiB(result.spilledBytes, result.runs) / readSeconds;
      std::printf(
          "%-48s %-8s %12.2f %12.2f %12.2f %12.2f\n",
          kBenchmarkScenarioSpecs[scenario].name,
          impl == 0 ? "legacy" : "radix",
          averageMs(result.spillReadUs, result.runs),
          averageMs(result.spillDecompressUs, result.runs),
          averageMs(result.spillReadIOUs, result.runs),
          readMiBPerSecond);
    }
  }

  std::printf("\nRadix / legacy phase ratios\n");
  std::printf(
      "%-48s %10s %10s %10s %10s %10s %10s\n",
      "scenario",
      "add1 x",
      "add2 x",
      "spill x",
      "final x",
      "out x",
      "read x");
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
        "%-48s %10.2f %10.2f %10.2f %10.2f %10.2f %10.2f\n",
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
        ratio(radix.outputUs, radix.runs, legacy.outputUs, legacy.runs),
        ratio(radix.spillReadUs, radix.runs, legacy.spillReadUs, legacy.runs));
  }
  std::printf(
      "Legacy SortBuffer uses row-based %s spill with SpillConfig JIT enabled "
      "for spill-run sorting and row-based spill merge. Radix spill uses radix "
      "section-format spill with compressionKind=%s.\n",
      legacyRowBasedSpillMode().c_str(),
      FLAGS_spill_compression_kind.c_str());
}

} // namespace

#define RADIX_SORT_SPILL_BENCHMARK_PAIR(name, index)  \
  BENCHMARK_NAMED_PARAM(legacySpillE2E, name, index); \
  BENCHMARK_RELATIVE_NAMED_PARAM(radixSpillE2E, name, index)

#define POINTER_FREE_ZSTD_BENCHMARK(name, index) \
  BENCHMARK_NAMED_PARAM(pointerFreeProductionZstd, name##_zstd, index)

#ifndef RADIX_SORT_LARGE_BENCHMARK
POINTER_FREE_ZSTD_BENCHMARK(fixed_key_only, 0);
BENCHMARK_DRAW_LINE();
POINTER_FREE_ZSTD_BENCHMARK(fixed_key_narrow_payload, 1);
BENCHMARK_DRAW_LINE();
POINTER_FREE_ZSTD_BENCHMARK(fixed_key_wide_payload, 2);
BENCHMARK_DRAW_LINE();
POINTER_FREE_ZSTD_BENCHMARK(variable_key_only, 3);
BENCHMARK_DRAW_LINE();
POINTER_FREE_ZSTD_BENCHMARK(variable_key_narrow_payload, 4);
BENCHMARK_DRAW_LINE();
POINTER_FREE_ZSTD_BENCHMARK(variable_key_wide_payload, 5);
BENCHMARK_DRAW_LINE();
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
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(
    single_key_very_wide_mixed_payload_256k_spill,
    20);
#else
RADIX_SORT_SPILL_BENCHMARK_PAIR(random_i64_10m_spill, 0);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(eight_key_i64_10m_spill, 1);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(sixteen_key_i64_10m_spill, 2);
BENCHMARK_DRAW_LINE();
RADIX_SORT_SPILL_BENCHMARK_PAIR(wide_fixed_payload_10m_spill, 3);
#endif

#undef POINTER_FREE_ZSTD_BENCHMARK
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
  validateExecutedPointerFreeScenarios();
  printPointerFreeSummary();
  printSummary();
  for (auto& fixture : pointerFreeFixtures) {
    fixture.reset();
  }
  fixtures.clear();
  sourcePool.reset();
  return 0;
}
