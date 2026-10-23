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

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <array>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "bolt/common/config/Config.h"
#include "bolt/connectors/hive/HiveDataSink.h"
#include "bolt/exec/MemoryReclaimer.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/vector/Utf8Utils.h"

namespace bytedance::bolt::connector::hive {
namespace {

using namespace bytedance::bolt::common;
using namespace bytedance::bolt::exec::test;

constexpr const char* kParquetSerdeMarker =
    "spark.gluten.sql.native.writer.hive.parquet.serde";
constexpr vector_size_t kRowsPerBatch = 10'000;
constexpr int32_t kNumBatches = 10;
constexpr int32_t kStringBytes = 64;
constexpr int32_t kLongStringBytes = 1'024;
constexpr int32_t kNumPartitions = 16;
// ASCII, two-byte, three-byte and four-byte code points, interleaved.
constexpr std::string_view kMixedUtf8Pattern =
    "A\xC2\xA2\xE4\xB8\xAD\xF0\x9F\x99\x82";

enum class InputKind {
  kAscii,
  kAsciiKnown,
  kValidUtf8,
  kValidMixedUtf8,
  kLongValidUtf8,
  kInvalidRare,
  kInvalidComplex,
  kInvalidAll,
  kInvalidAlternating,
  kLongInvalidAlternating,
  kInlineOutput,
  kInlineToNonInline,
  kDictionaryValid,
  kDictionaryInvalidRare,
  kNullableValid,
  kNullableInvalidDense,
  kNullableInvalidAlternating,
  kMultiPartitionInvalid,
  kCount,
};

constexpr size_t kNumInputKinds = static_cast<size_t>(InputKind::kCount);

class HiveDataSinkUtf8Benchmark : public HiveConnectorTestBase {
 public:
  void TestBody() override {}

  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  HiveDataSinkUtf8Benchmark() {
    HiveConnectorTestBase::SetUp();
    Type::registerSerDe();
    HiveSortingColumn::registerSerDe();
    HiveBucketProperty::registerSerDe();

    root_ = memory::memoryManager()->addRootPool(
        "HiveDataSinkUtf8Benchmark", 1L << 30, exec::MemoryReclaimer::create());
    opPool_ = root_->addLeafChild("operator");
    connectorPool_ =
        root_->addAggregateChild("connector", exec::MemoryReclaimer::create());
    connectorQueryCtx_ = std::make_unique<ConnectorQueryCtx>(
        opPool_.get(),
        connectorPool_.get(),
        sessionProperties_.get(),
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        "query.HiveDataSinkUtf8Benchmark",
        "task.HiveDataSinkUtf8Benchmark",
        "planNodeId.HiveDataSinkUtf8Benchmark",
        0);

    for (size_t index = 0; index < inputs_.size(); ++index) {
      inputs_[index] = makeInputBatches(static_cast<InputKind>(index));
    }
  }

  ~HiveDataSinkUtf8Benchmark() override {
    for (auto& inputs : inputs_) {
      inputs.clear();
    }
    connectorQueryCtx_.reset();
    connectorPool_.reset();
    opPool_.reset();
    root_.reset();
    HiveConnectorTestBase::TearDown();
  }

  size_t run(InputKind inputKind, bool sanitize, const char* benchmarkName) {
    // Keep input generation, HiveDataSink construction and result inspection
    // outside the timed region. The first append lazily creates the Parquet
    // writer, so the measured interval covers writer creation, all appends,
    // UTF-8 sanitization when enabled, encoding, ZSTD compression, file I/O
    // and close/flush.
    folly::BenchmarkSuspender suspender;
    const auto outputDirectory = TempDirectoryPath::create();
    const auto& inputs = inputsFor(inputKind);
    auto dataSink = createDataSink(outputDirectory->path, inputKind, sanitize);
    suspender.dismiss();

    for (const auto& input : inputs) {
      dataSink->appendData(input);
    }
    dataSink->close();

    suspender.rehire();
    reportOutputSizeOnce(
        benchmarkName,
        outputDirectory->path,
        inputKind == InputKind::kMultiPartitionInvalid ? kNumPartitions : 1);
    return static_cast<size_t>(kRowsPerBatch) * kNumBatches;
  }

  size_t runSanitizer(InputKind inputKind) {
    const auto& inputs = inputsFor(inputKind);
    for (const auto& input : inputs) {
      auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(
          input, connectorQueryCtx_->memoryPool());
      folly::doNotOptimizeAway(output.get());
    }
    return static_cast<size_t>(kRowsPerBatch) * kNumBatches;
  }

 private:
  static std::string makeAsciiValue(int64_t globalRow) {
    std::string value(kStringBytes, 'x');
    uint64_t state = static_cast<uint64_t>(globalRow) + 1;
    for (int32_t index = 0; index < kStringBytes; ++index) {
      state = state * 6364136223846793005ULL + 1442695040888963407ULL;
      value[index] = static_cast<char>('!' + state % 94);
    }
    return value;
  }

  static std::string makeValidUtf8Value(int64_t globalRow) {
    std::string value;
    value.reserve(kStringBytes);
    for (int32_t index = 0; index < 20; ++index) {
      value.append("\xE4\xB8\xAD");
    }
    constexpr char kDigits[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (int32_t index = 0; index < 4; ++index) {
      value.push_back(kDigits[globalRow % 62]);
      globalRow /= 62;
    }
    return value;
  }

  static std::string makeMixedUtf8Value(
      int64_t globalRow,
      int32_t stringBytes) {
    std::string value;
    value.reserve(stringBytes);
    while (value.size() + kMixedUtf8Pattern.size() <= stringBytes - 4) {
      value.append(kMixedUtf8Pattern);
    }
    constexpr std::string_view kDigits =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    while (value.size() < stringBytes) {
      value.push_back(kDigits[globalRow % kDigits.size()]);
      globalRow /= kDigits.size();
    }
    return value;
  }

  static std::string makeAlternatingInvalidValue(
      int64_t globalRow,
      int32_t stringBytes = kStringBytes) {
    // Prevent adjacent malformed units from coalescing into a single run.
    std::string value(stringBytes, '\xD5');
    for (int32_t index = 1; index < stringBytes; index += 2) {
      value[index] = static_cast<char>('a' + (globalRow + index) % 26);
    }
    return value;
  }

  static std::string makeStringValue(InputKind inputKind, int64_t globalRow) {
    switch (inputKind) {
      case InputKind::kAscii:
      case InputKind::kAsciiKnown:
      case InputKind::kDictionaryValid:
        return makeAsciiValue(globalRow);
      case InputKind::kValidUtf8:
        return makeValidUtf8Value(globalRow);
      case InputKind::kValidMixedUtf8:
      case InputKind::kNullableValid:
        return makeMixedUtf8Value(globalRow, kStringBytes);
      case InputKind::kLongValidUtf8:
        return makeMixedUtf8Value(globalRow, kLongStringBytes);
      case InputKind::kInvalidRare:
      case InputKind::kDictionaryInvalidRare: {
        auto value = makeAsciiValue(globalRow);
        if (globalRow % 100 == 0) {
          value[kStringBytes / 2] = '\xD5';
        }
        return value;
      }
      case InputKind::kInvalidComplex: {
        auto value = makeValidUtf8Value(globalRow);
        value[kStringBytes / 2] = '\xD5';
        return value;
      }
      case InputKind::kInvalidAll:
      case InputKind::kNullableInvalidDense:
        return std::string(kStringBytes, '\xD5');
      case InputKind::kInvalidAlternating:
      case InputKind::kNullableInvalidAlternating:
      case InputKind::kMultiPartitionInvalid:
        return makeAlternatingInvalidValue(globalRow);
      case InputKind::kLongInvalidAlternating:
        return makeAlternatingInvalidValue(globalRow, kLongStringBytes);
      case InputKind::kInlineOutput: {
        std::string value(6, 'x');
        value.append("\xD5\xD5", 2);
        return value;
      }
      case InputKind::kInlineToNonInline: {
        std::string value(7, 'x');
        value.append("\xD5\xD5", 2);
        return value;
      }
      case InputKind::kCount:
        BOLT_UNREACHABLE();
    }
    BOLT_UNREACHABLE();
  }

  static bool isDictionaryInput(InputKind inputKind) {
    return inputKind == InputKind::kDictionaryValid ||
        inputKind == InputKind::kDictionaryInvalidRare;
  }

  static bool isNullableInput(InputKind inputKind) {
    return inputKind == InputKind::kNullableValid ||
        inputKind == InputKind::kNullableInvalidDense ||
        inputKind == InputKind::kNullableInvalidAlternating;
  }

  std::vector<RowVectorPtr> makeInputBatches(InputKind inputKind) {
    std::vector<RowVectorPtr> inputs;
    inputs.reserve(kNumBatches);
    for (int32_t batch = 0; batch < kNumBatches; ++batch) {
      if (isDictionaryInput(inputKind)) {
        constexpr vector_size_t kDictionarySize = 100;
        auto base = makeFlatVector<std::string>(
            kDictionarySize, [&](vector_size_t baseRow) {
              return makeStringValue(
                  inputKind,
                  static_cast<int64_t>(batch) * kDictionarySize + baseRow);
            });
        auto indices = makeIndices(kRowsPerBatch, [](vector_size_t row) {
          return row % kDictionarySize;
        });
        inputs.push_back(makeRowVector({BaseVector::wrapInDictionary(
            nullptr, indices, kRowsPerBatch, std::move(base))}));
        continue;
      }

      auto valueAt = [&](vector_size_t row) {
        const int64_t globalRow =
            static_cast<int64_t>(batch) * kRowsPerBatch + row;
        return makeStringValue(inputKind, globalRow);
      };
      auto values = isNullableInput(inputKind)
          ? makeFlatVector<std::string>(
                kRowsPerBatch,
                valueAt,
                // Keep enough non-null rows to make each string shape
                // measurable while forcing the nullable scanner path.
                [](vector_size_t row) { return row % 8 == 0; })
          : makeFlatVector<std::string>(kRowsPerBatch, valueAt);
      if (inputKind == InputKind::kAsciiKnown) {
        values->as<SimpleVector<StringView>>()->setAllIsAscii(true);
      }
      if (inputKind == InputKind::kMultiPartitionInvalid) {
        auto partitions =
            makeFlatVector<int32_t>(kRowsPerBatch, [&](vector_size_t row) {
              const int64_t globalRow =
                  static_cast<int64_t>(batch) * kRowsPerBatch + row;
              return globalRow % kNumPartitions;
            });
        inputs.push_back(makeRowVector({"c0", "p0"}, {values, partitions}));
      } else {
        inputs.push_back(makeRowVector({values}));
      }
    }
    return inputs;
  }

  const std::vector<RowVectorPtr>& inputsFor(InputKind inputKind) const {
    return inputs_.at(static_cast<size_t>(inputKind));
  }

  std::shared_ptr<HiveDataSink> createDataSink(
      const std::string& outputDirectory,
      InputKind inputKind,
      bool sanitize) {
    const bool isPartitioned = inputKind == InputKind::kMultiPartitionInvalid;
    const auto rowType = isPartitioned
        ? ROW({"c0", "p0"}, {VARCHAR(), INTEGER()})
        : ROW({"c0"}, {VARCHAR()});
    auto tableHandle = makeHiveInsertTableHandle(
        rowType->names(),
        rowType->children(),
        isPartitioned ? std::vector<std::string>{"p0"}
                      : std::vector<std::string>{},
        nullptr,
        makeLocationHandle(
            outputDirectory, std::nullopt, LocationHandle::TableType::kNew),
        dwio::common::FileFormat::PARQUET,
        CompressionKind::CompressionKind_ZSTD);
    tableHandle = std::make_shared<HiveInsertTableHandle>(
        tableHandle->inputColumns(),
        tableHandle->locationHandle(),
        tableHandle->storageFormat(),
        nullptr,
        tableHandle->compressionKind(),
        std::unordered_map<std::string, std::string>{
            {kParquetSerdeMarker, sanitize ? "true" : "false"}});
    return std::make_shared<HiveDataSink>(
        rowType,
        std::move(tableHandle),
        connectorQueryCtx_.get(),
        CommitStrategy::kNoCommit,
        hiveConfig_,
        queryConfig_);
  }

  static void reportOutputSizeOnce(
      const std::string& benchmarkName,
      const std::string& outputDirectory,
      int32_t expectedOutputFiles) {
    static std::mutex mutex;
    static std::unordered_set<std::string> reportedBenchmarks;
    std::lock_guard<std::mutex> lock(mutex);
    if (!reportedBenchmarks.insert(benchmarkName).second) {
      return;
    }

    uint64_t outputBytes = 0;
    int32_t outputFiles = 0;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(outputDirectory)) {
      if (entry.is_regular_file()) {
        outputBytes += entry.file_size();
        ++outputFiles;
      }
    }
    BOLT_CHECK_EQ(outputFiles, expectedOutputFiles);
    std::cerr << fmt::format(
        "OUTPUT {} rows={} bytes={}\n",
        benchmarkName,
        static_cast<int64_t>(kRowsPerBatch) * kNumBatches,
        outputBytes);
  }

  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::MemoryPool> opPool_;
  std::shared_ptr<memory::MemoryPool> connectorPool_;
  std::shared_ptr<config::ConfigBase> sessionProperties_ =
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>{});
  std::unique_ptr<ConnectorQueryCtx> connectorQueryCtx_;
  std::shared_ptr<HiveConfig> hiveConfig_ =
      std::make_shared<HiveConfig>(std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>{}));
  core::QueryConfig queryConfig_{{}};
  std::array<std::vector<RowVectorPtr>, kNumInputKinds> inputs_;
};

std::unique_ptr<HiveDataSinkUtf8Benchmark> benchmark;

#define HIVE_DATA_SINK_UTF8_BENCHMARK(name, inputKind, sanitize)  \
  BENCHMARK_MULTI(name) {                                         \
    return benchmark->run(InputKind::inputKind, sanitize, #name); \
  }

#define HIVE_DATA_SINK_UTF8_BENCHMARKS(name, inputKind)           \
  HIVE_DATA_SINK_UTF8_BENCHMARK(name##Disabled, inputKind, false) \
  HIVE_DATA_SINK_UTF8_BENCHMARK(name##Enabled, inputKind, true)

HIVE_DATA_SINK_UTF8_BENCHMARKS(ascii, kAscii)
HIVE_DATA_SINK_UTF8_BENCHMARKS(asciiKnown, kAsciiKnown)
BENCHMARK_DRAW_LINE();
HIVE_DATA_SINK_UTF8_BENCHMARKS(validUtf8, kValidUtf8)
HIVE_DATA_SINK_UTF8_BENCHMARKS(validMixedUtf8, kValidMixedUtf8)
HIVE_DATA_SINK_UTF8_BENCHMARKS(longValidUtf8, kLongValidUtf8)
BENCHMARK_DRAW_LINE();
HIVE_DATA_SINK_UTF8_BENCHMARKS(invalidRare, kInvalidRare)
HIVE_DATA_SINK_UTF8_BENCHMARKS(invalidComplex, kInvalidComplex)
HIVE_DATA_SINK_UTF8_BENCHMARKS(invalidAll, kInvalidAll)
HIVE_DATA_SINK_UTF8_BENCHMARKS(invalidAlternating, kInvalidAlternating)
HIVE_DATA_SINK_UTF8_BENCHMARKS(longInvalidAlternating, kLongInvalidAlternating)
BENCHMARK_DRAW_LINE();
HIVE_DATA_SINK_UTF8_BENCHMARKS(inlineOutput, kInlineOutput)
HIVE_DATA_SINK_UTF8_BENCHMARKS(inlineToNonInline, kInlineToNonInline)
BENCHMARK_DRAW_LINE();
HIVE_DATA_SINK_UTF8_BENCHMARKS(dictionaryValid, kDictionaryValid)
HIVE_DATA_SINK_UTF8_BENCHMARKS(dictionaryInvalidRare, kDictionaryInvalidRare)
BENCHMARK_DRAW_LINE();
HIVE_DATA_SINK_UTF8_BENCHMARKS(nullableValid, kNullableValid)
HIVE_DATA_SINK_UTF8_BENCHMARKS(nullableInvalidDense, kNullableInvalidDense)
HIVE_DATA_SINK_UTF8_BENCHMARKS(
    nullableInvalidAlternating,
    kNullableInvalidAlternating)
BENCHMARK_DRAW_LINE();
HIVE_DATA_SINK_UTF8_BENCHMARKS(multiPartitionInvalid, kMultiPartitionInvalid)

#define UTF8_SANITIZER_BENCHMARK(name, inputKind)         \
  BENCHMARK_MULTI(name) {                                 \
    return benchmark->runSanitizer(InputKind::inputKind); \
  }

BENCHMARK_DRAW_LINE();
UTF8_SANITIZER_BENCHMARK(sanitizeAscii, kAscii);
UTF8_SANITIZER_BENCHMARK(sanitizeAsciiKnown, kAsciiKnown);
UTF8_SANITIZER_BENCHMARK(sanitizeValidUtf8, kValidUtf8);
UTF8_SANITIZER_BENCHMARK(sanitizeValidMixedUtf8, kValidMixedUtf8);
UTF8_SANITIZER_BENCHMARK(sanitizeLongValidUtf8, kLongValidUtf8);
UTF8_SANITIZER_BENCHMARK(sanitizeInvalidRare, kInvalidRare);
UTF8_SANITIZER_BENCHMARK(sanitizeInvalidComplex, kInvalidComplex);
UTF8_SANITIZER_BENCHMARK(sanitizeInvalidAll, kInvalidAll);
UTF8_SANITIZER_BENCHMARK(sanitizeInvalidAlternating, kInvalidAlternating);
UTF8_SANITIZER_BENCHMARK(
    sanitizeLongInvalidAlternating,
    kLongInvalidAlternating);
UTF8_SANITIZER_BENCHMARK(sanitizeInlineOutput, kInlineOutput);
UTF8_SANITIZER_BENCHMARK(sanitizeInlineToNonInline, kInlineToNonInline);
UTF8_SANITIZER_BENCHMARK(sanitizeDictionaryValid, kDictionaryValid);
UTF8_SANITIZER_BENCHMARK(sanitizeDictionaryInvalidRare, kDictionaryInvalidRare);
UTF8_SANITIZER_BENCHMARK(sanitizeNullableValid, kNullableValid);
UTF8_SANITIZER_BENCHMARK(sanitizeNullableInvalidDense, kNullableInvalidDense);
UTF8_SANITIZER_BENCHMARK(
    sanitizeNullableInvalidAlternating,
    kNullableInvalidAlternating);
UTF8_SANITIZER_BENCHMARK(sanitizeMultiPartitionInvalid, kMultiPartitionInvalid);

} // namespace
} // namespace bytedance::bolt::connector::hive

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  using bytedance::bolt::connector::hive::HiveDataSinkUtf8Benchmark;
  HiveDataSinkUtf8Benchmark::SetUpTestCase();
  bytedance::bolt::connector::hive::benchmark =
      std::make_unique<HiveDataSinkUtf8Benchmark>();
  folly::runBenchmarks();
  bytedance::bolt::connector::hive::benchmark.reset();
  HiveDataSinkUtf8Benchmark::TearDownTestCase();
  return 0;
}
