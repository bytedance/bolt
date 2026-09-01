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

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
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

enum class InputKind {
  kAscii,
  kAsciiKnown,
  kValidUtf8,
  kInvalidRare,
  kInvalidComplex,
  kInvalidAll,
  kInlineOutput,
  kInlineToNonInline,
  kDictionaryValid,
  kDictionaryInvalidRare
};

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

    ascii_ = makeInputBatches(InputKind::kAscii);
    asciiKnown_ = makeInputBatches(InputKind::kAsciiKnown);
    validUtf8_ = makeInputBatches(InputKind::kValidUtf8);
    invalidRare_ = makeInputBatches(InputKind::kInvalidRare);
    invalidComplex_ = makeInputBatches(InputKind::kInvalidComplex);
    invalidAll_ = makeInputBatches(InputKind::kInvalidAll);
    inlineOutput_ = makeInputBatches(InputKind::kInlineOutput);
    inlineToNonInline_ = makeInputBatches(InputKind::kInlineToNonInline);
    dictionaryValid_ = makeInputBatches(InputKind::kDictionaryValid);
    dictionaryInvalidRare_ =
        makeInputBatches(InputKind::kDictionaryInvalidRare);
  }

  ~HiveDataSinkUtf8Benchmark() override {
    dictionaryInvalidRare_.clear();
    dictionaryValid_.clear();
    inlineToNonInline_.clear();
    inlineOutput_.clear();
    invalidAll_.clear();
    invalidComplex_.clear();
    invalidRare_.clear();
    validUtf8_.clear();
    asciiKnown_.clear();
    ascii_.clear();
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
    auto dataSink = createDataSink(outputDirectory->path, sanitize);
    const auto& inputs = inputsFor(inputKind);
    suspender.dismiss();

    for (const auto& input : inputs) {
      dataSink->appendData(input);
    }
    dataSink->close();

    suspender.rehire();
    reportOutputSizeOnce(benchmarkName, outputDirectory->path);
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

  std::vector<RowVectorPtr> makeInputBatches(InputKind inputKind) {
    std::vector<RowVectorPtr> inputs;
    inputs.reserve(kNumBatches);
    for (int32_t batch = 0; batch < kNumBatches; ++batch) {
      if (inputKind == InputKind::kDictionaryValid ||
          inputKind == InputKind::kDictionaryInvalidRare) {
        constexpr vector_size_t kDictionarySize = 100;
        auto base = makeFlatVector<std::string>(
            kDictionarySize, [&](vector_size_t baseRow) {
              auto value = makeAsciiValue(
                  static_cast<int64_t>(batch) * kDictionarySize + baseRow);
              if (inputKind == InputKind::kDictionaryInvalidRare &&
                  baseRow == 0) {
                value[kStringBytes / 2] = '\xD5';
              }
              return value;
            });
        auto indices = makeIndices(kRowsPerBatch, [](vector_size_t row) {
          return row % kDictionarySize;
        });
        inputs.push_back(makeRowVector({BaseVector::wrapInDictionary(
            nullptr, indices, kRowsPerBatch, std::move(base))}));
        continue;
      }

      auto values =
          makeFlatVector<std::string>(kRowsPerBatch, [&](vector_size_t row) {
            const int64_t globalRow =
                static_cast<int64_t>(batch) * kRowsPerBatch + row;
            if (inputKind == InputKind::kValidUtf8) {
              return makeValidUtf8Value(globalRow);
            }
            if (inputKind == InputKind::kInvalidComplex) {
              auto value = makeValidUtf8Value(globalRow);
              value[kStringBytes / 2] = '\xD5';
              return value;
            }
            if (inputKind == InputKind::kInvalidAll) {
              return std::string(kStringBytes, '\xD5');
            }
            if (inputKind == InputKind::kInlineOutput ||
                inputKind == InputKind::kInlineToNonInline) {
              std::string value(
                  inputKind == InputKind::kInlineOutput ? 6 : 7, 'x');
              value.append("\xD5\xD5", 2);
              return value;
            }

            auto value = makeAsciiValue(globalRow);
            if (inputKind == InputKind::kInvalidRare && globalRow % 100 == 0) {
              value[kStringBytes / 2] = '\xD5';
            }
            return value;
          });
      if (inputKind == InputKind::kAsciiKnown) {
        values->as<SimpleVector<StringView>>()->setAllIsAscii(true);
      }
      inputs.push_back(makeRowVector({values}));
    }
    return inputs;
  }

  const std::vector<RowVectorPtr>& inputsFor(InputKind inputKind) const {
    switch (inputKind) {
      case InputKind::kAscii:
        return ascii_;
      case InputKind::kAsciiKnown:
        return asciiKnown_;
      case InputKind::kValidUtf8:
        return validUtf8_;
      case InputKind::kInvalidRare:
        return invalidRare_;
      case InputKind::kInvalidComplex:
        return invalidComplex_;
      case InputKind::kInvalidAll:
        return invalidAll_;
      case InputKind::kInlineOutput:
        return inlineOutput_;
      case InputKind::kInlineToNonInline:
        return inlineToNonInline_;
      case InputKind::kDictionaryValid:
        return dictionaryValid_;
      case InputKind::kDictionaryInvalidRare:
        return dictionaryInvalidRare_;
    }
    BOLT_UNREACHABLE();
  }

  std::shared_ptr<HiveDataSink> createDataSink(
      const std::string& outputDirectory,
      bool sanitize) {
    const auto rowType = ROW({"c0"}, {VARCHAR()});
    auto tableHandle = makeHiveInsertTableHandle(
        rowType->names(),
        rowType->children(),
        {},
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
      const std::string& outputDirectory) {
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
    BOLT_CHECK_EQ(outputFiles, 1);
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
  std::vector<RowVectorPtr> ascii_;
  std::vector<RowVectorPtr> asciiKnown_;
  std::vector<RowVectorPtr> validUtf8_;
  std::vector<RowVectorPtr> invalidRare_;
  std::vector<RowVectorPtr> invalidComplex_;
  std::vector<RowVectorPtr> invalidAll_;
  std::vector<RowVectorPtr> inlineOutput_;
  std::vector<RowVectorPtr> inlineToNonInline_;
  std::vector<RowVectorPtr> dictionaryValid_;
  std::vector<RowVectorPtr> dictionaryInvalidRare_;
};

std::unique_ptr<HiveDataSinkUtf8Benchmark> benchmark;

#define HIVE_DATA_SINK_UTF8_BENCHMARK(name, inputKind, sanitize)  \
  BENCHMARK_MULTI(name) {                                         \
    return benchmark->run(InputKind::inputKind, sanitize, #name); \
  }

HIVE_DATA_SINK_UTF8_BENCHMARK(asciiDisabled, kAscii, false);
HIVE_DATA_SINK_UTF8_BENCHMARK(asciiEnabled, kAscii, true);
HIVE_DATA_SINK_UTF8_BENCHMARK(asciiKnownDisabled, kAsciiKnown, false);
HIVE_DATA_SINK_UTF8_BENCHMARK(asciiKnownEnabled, kAsciiKnown, true);
BENCHMARK_DRAW_LINE();
HIVE_DATA_SINK_UTF8_BENCHMARK(validUtf8Disabled, kValidUtf8, false);
HIVE_DATA_SINK_UTF8_BENCHMARK(validUtf8Enabled, kValidUtf8, true);
BENCHMARK_DRAW_LINE();
HIVE_DATA_SINK_UTF8_BENCHMARK(invalidRareDisabled, kInvalidRare, false);
HIVE_DATA_SINK_UTF8_BENCHMARK(invalidRareEnabled, kInvalidRare, true);
HIVE_DATA_SINK_UTF8_BENCHMARK(invalidComplexDisabled, kInvalidComplex, false);
HIVE_DATA_SINK_UTF8_BENCHMARK(invalidComplexEnabled, kInvalidComplex, true);
HIVE_DATA_SINK_UTF8_BENCHMARK(invalidAllDisabled, kInvalidAll, false);
HIVE_DATA_SINK_UTF8_BENCHMARK(invalidAllEnabled, kInvalidAll, true);
BENCHMARK_DRAW_LINE();
HIVE_DATA_SINK_UTF8_BENCHMARK(inlineOutputDisabled, kInlineOutput, false);
HIVE_DATA_SINK_UTF8_BENCHMARK(inlineOutputEnabled, kInlineOutput, true);
HIVE_DATA_SINK_UTF8_BENCHMARK(
    inlineToNonInlineDisabled,
    kInlineToNonInline,
    false);
HIVE_DATA_SINK_UTF8_BENCHMARK(
    inlineToNonInlineEnabled,
    kInlineToNonInline,
    true);
BENCHMARK_DRAW_LINE();
HIVE_DATA_SINK_UTF8_BENCHMARK(dictionaryValidDisabled, kDictionaryValid, false);
HIVE_DATA_SINK_UTF8_BENCHMARK(dictionaryValidEnabled, kDictionaryValid, true);
HIVE_DATA_SINK_UTF8_BENCHMARK(
    dictionaryInvalidRareDisabled,
    kDictionaryInvalidRare,
    false);
HIVE_DATA_SINK_UTF8_BENCHMARK(
    dictionaryInvalidRareEnabled,
    kDictionaryInvalidRare,
    true);

#define UTF8_SANITIZER_BENCHMARK(name, inputKind)         \
  BENCHMARK_MULTI(name) {                                 \
    return benchmark->runSanitizer(InputKind::inputKind); \
  }

BENCHMARK_DRAW_LINE();
UTF8_SANITIZER_BENCHMARK(sanitizeAscii, kAscii);
UTF8_SANITIZER_BENCHMARK(sanitizeValidUtf8, kValidUtf8);
UTF8_SANITIZER_BENCHMARK(sanitizeInvalidRare, kInvalidRare);
UTF8_SANITIZER_BENCHMARK(sanitizeInvalidComplex, kInvalidComplex);
UTF8_SANITIZER_BENCHMARK(sanitizeInvalidAll, kInvalidAll);
UTF8_SANITIZER_BENCHMARK(sanitizeInlineOutput, kInlineOutput);
UTF8_SANITIZER_BENCHMARK(sanitizeDictionaryValid, kDictionaryValid);
UTF8_SANITIZER_BENCHMARK(sanitizeDictionaryInvalidRare, kDictionaryInvalidRare);

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
