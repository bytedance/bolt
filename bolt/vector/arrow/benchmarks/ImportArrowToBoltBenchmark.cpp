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
#include <algorithm>
#include <random>
#include <string>

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/c/bridge.h>

#include "bolt/common/base/tests/ArrowTestUtils.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/vector/arrow/Bridge.h"

static constexpr int32_t kRowsPerVector = 100'000;

namespace {
using namespace bytedance::bolt;

void mockSchemaRelease(ArrowSchema*) {}
void mockArrayRelease(ArrowArray*) {}

memory::MemoryManager memoryManager;

class ArrowBridgeArrayImportBenchmark {
 protected:
  // Used by this base test class to import Arrow data and create Bolt Vector.
  // Derived test classes should call the import function under test.
  virtual VectorPtr importFromArrow(
      ArrowSchema& arrowSchema,
      ArrowArray& arrowArray,
      memory::MemoryPool* pool) = 0;

  virtual bool isViewer() const = 0;
};

class ArrowBridgeArrayImportAsViewerBenchmark
    : public ArrowBridgeArrayImportBenchmark {
  bool isViewer() const override {
    return true;
  }

 public:
  VectorPtr importFromArrow(
      ArrowSchema& arrowSchema,
      ArrowArray& arrowArray,
      memory::MemoryPool* pool) override {
    return bytedance::bolt::importFromArrowAsViewer(
        arrowSchema, arrowArray, {}, pool);
  }
};

class ArrowBridgeArrayImportAsOwnerBenchmark
    : public ArrowBridgeArrayImportAsViewerBenchmark {
  bool isViewer() const override {
    return false;
  }

 public:
  VectorPtr importFromArrow(
      ArrowSchema& arrowSchema,
      ArrowArray& arrowArray,
      memory::MemoryPool* pool) override {
    arrowSchema.release = mockSchemaRelease;
    arrowArray.release = mockArrayRelease;
    return bytedance::bolt::importFromArrowAsOwner(
        arrowSchema, arrowArray, {}, pool);
  }
};

std::shared_ptr<memory::MemoryPool> pool{memoryManager.addLeafPool()};

void runImportFromArrowAsViewer(
    uint32_t,
    ArrowSchema& schema,
    ArrowArray& data) {
  ArrowBridgeArrayImportAsViewerBenchmark instance;
  instance.importFromArrow(schema, data, pool.get());
}

void runImportFromArrowAsOwner(
    uint32_t,
    ArrowSchema& schema,
    ArrowArray& data) {
  ArrowBridgeArrayImportAsOwnerBenchmark instance;
  instance.importFromArrow(schema, data, pool.get());
}

ArrowArray stringArray;
ArrowArray stringViewArray;
ArrowArray stringArray_halfNull;
ArrowArray stringViewArray_halfNull;

ArrowSchema stringSchema;
ArrowSchema stringViewSchema;

template <typename Builder>
void createStringArray(
    std::mt19937& generator,
    ArrowSchema& schema,
    ArrowArray& data,
    size_t minStringLength,
    size_t maxStringLength,
    double nullProbability) {
  Builder builder;
  std::uniform_int_distribution<size_t> lengthDistribution(
      minStringLength, maxStringLength);
  std::uniform_int_distribution<int> characterDistribution('a', 'z');
  std::bernoulli_distribution nullDistribution(nullProbability);

  for (int32_t i = 0; i < kRowsPerVector; ++i) {
    if (nullDistribution(generator)) {
      ASSERT_OK(builder.AppendNull());
      continue;
    }

    std::string value(lengthDistribution(generator), '\0');
    std::generate(value.begin(), value.end(), [&]() {
      return static_cast<char>(characterDistribution(generator));
    });
    ASSERT_OK(builder.Append(value));
  }

  ASSERT_OK_AND_ASSIGN(auto randomStrings, builder.Finish());
  ASSERT_OK(arrow::ExportType(*randomStrings->type(), &schema));
  ASSERT_OK(arrow::ExportArray(*randomStrings, &data));
}

void createArrowArrays() {
  std::mt19937 generator(42);
  size_t minStringLength = 0;
  size_t maxStringLength = 50;
  createStringArray<arrow::StringBuilder>(
      generator,
      stringSchema,
      stringArray,
      minStringLength,
      maxStringLength,
      0);
  createStringArray<arrow::StringViewBuilder>(
      generator,
      stringViewSchema,
      stringViewArray,
      minStringLength,
      maxStringLength,
      0);
  createStringArray<arrow::StringBuilder>(
      generator,
      stringSchema,
      stringArray_halfNull,
      minStringLength,
      maxStringLength,
      0.5);
  createStringArray<arrow::StringViewBuilder>(
      generator,
      stringViewSchema,
      stringViewArray_halfNull,
      minStringLength,
      maxStringLength,
      0.5);
}

BENCHMARK_NAMED_PARAM(
    runImportFromArrowAsViewer,
    string,
    stringSchema,
    stringArray);
BENCHMARK_NAMED_PARAM(
    runImportFromArrowAsViewer,
    stringview,
    stringViewSchema,
    stringViewArray);
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(
    runImportFromArrowAsViewer,
    string_halfnull,
    stringSchema,
    stringArray_halfNull);
BENCHMARK_NAMED_PARAM(
    runImportFromArrowAsViewer,
    stringview_halfnull,
    stringViewSchema,
    stringViewArray_halfNull);
BENCHMARK_DRAW_LINE();
BENCHMARK_DRAW_LINE();
// Add mock release for schema and array
BENCHMARK_NAMED_PARAM(
    runImportFromArrowAsOwner,
    string,
    stringSchema,
    stringArray);
BENCHMARK_NAMED_PARAM(
    runImportFromArrowAsOwner,
    stringview,
    stringViewSchema,
    stringViewArray);

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  createArrowArrays();
  folly::runBenchmarks();
  return 0;
}
