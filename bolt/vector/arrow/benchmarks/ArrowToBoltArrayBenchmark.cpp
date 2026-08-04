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
#include <string>

#include "bolt/common/base/Nulls.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

#include <arrow/api.h>

namespace {
using namespace bytedance::bolt;

void mockSchemaRelease(ArrowSchema*) {}
void mockArrayRelease(ArrowArray*) {}

const int ITERATION_TIMES = 100000;
memory::MemoryManager benchmarkMemoryManager;
auto schemaPool = benchmarkMemoryManager.addLeafPool();

void exportToArrow(const TypePtr& type, ArrowSchema& out) {
  exportToArrow(BaseVector::create(type, 0, schemaPool.get()), out, {});
}

class ArrowBridgeArrayImportBenchmark {
 protected:
  // Used by this base test class to import Arrow data and create Bolt Vector.
  // Derived test classes should call the import function under test.
  virtual VectorPtr importFromArrow(
      ArrowSchema& arrowSchema,
      ArrowArray& arrowArray,
      memory::MemoryPool* pool) = 0;

  virtual bool isViewer() const = 0;

  ArrowSchema makeArrowSchema(const char* format) {
    return ArrowSchema{
        .format = format,
        .name = nullptr,
        .metadata = nullptr,
        .flags = 0,
        .n_children = 0,
        .children = nullptr,
        .dictionary = nullptr,
        .release = mockSchemaRelease,
        .private_data = nullptr,
    };
  }

  ArrowArray makeArrowArray(
      const void** buffers,
      int64_t nBuffers,
      int64_t length,
      int64_t nullCount) {
    return ArrowArray{
        .length = length,
        .null_count = nullCount,
        .offset = 0,
        .n_buffers = nBuffers,
        .n_children = 0,
        .buffers = buffers,
        .children = nullptr,
        .dictionary = nullptr,
        .release = mockArrayRelease,
        .private_data = nullptr,
    };
  }

  // Boiler plate structures required by vectorMaker.
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  std::shared_ptr<memory::MemoryPool> pool_{
      benchmarkMemoryManager.addLeafPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  bytedance::bolt::test::VectorMaker vectorMaker_{execCtx_.pool()};

  // Helper structure to hold buffers required by an ArrowArray.
  struct ArrowContextHolder {
    BufferPtr values;
    BufferPtr nulls;
    BufferPtr offsets;

    // Tests might not use the whole array.
    const void* buffers[3] = {nullptr, nullptr, nullptr};
    ArrowArray* children[10];
  };

  template <typename T>
  ArrowArray fillArrowArray(
      const std::vector<std::optional<T>>& inputValues,
      ArrowContextHolder& holder) {
    int64_t length = inputValues.size();
    int64_t nullCount = 0;

    holder.values = AlignedBuffer::allocate<T>(length, pool_.get());
    holder.nulls = AlignedBuffer::allocate<uint64_t>(length, pool_.get());

    auto rawValues = holder.values->asMutable<T>();
    auto rawNulls = holder.nulls->asMutable<uint64_t>();

    for (size_t i = 0; i < length; ++i) {
      if (inputValues[i] == std::nullopt) {
        bits::setNull(rawNulls, i);
        nullCount++;
      } else {
        bits::clearNull(rawNulls, i);
        if constexpr (std::is_same_v<T, bool>) {
          bits::setBit(rawValues, i, *inputValues[i]);
        } else {
          rawValues[i] = *inputValues[i];
        }
      }
    }

    holder.buffers[0] = (length == 0) ? nullptr : (const void*)rawNulls;
    holder.buffers[1] = (length == 0) ? nullptr : (const void*)rawValues;
    return makeArrowArray(holder.buffers, 2, length, nullCount);
  }

  ArrowArray fillArrowArray(
      const std::vector<std::optional<std::string>>& inputValues,
      ArrowContextHolder& holder) {
    int64_t length = inputValues.size();
    int64_t nullCount = 0;

    // Calculate overall buffer size.
    int64_t bufferSize = 0;
    for (const auto& it : inputValues) {
      if (it.has_value()) {
        bufferSize += it->size();
      }
    }

    holder.nulls = AlignedBuffer::allocate<uint64_t>(length, pool_.get());
    holder.offsets = AlignedBuffer::allocate<int32_t>(length + 1, pool_.get());
    holder.values = AlignedBuffer::allocate<char>(bufferSize, pool_.get());

    auto rawNulls = holder.nulls->asMutable<uint64_t>();
    auto rawOffsets = holder.offsets->asMutable<int32_t>();
    auto rawValues = holder.values->asMutable<char>();
    *rawOffsets = 0;

    holder.buffers[2] = (length == 0) ? nullptr : (const void*)rawValues;
    holder.buffers[1] = (length == 0) ? nullptr : (const void*)rawOffsets;

    for (size_t i = 0; i < length; ++i) {
      if (inputValues[i] == std::nullopt) {
        bits::setNull(rawNulls, i);
        nullCount++;
        *(rawOffsets + 1) = *rawOffsets;
        ++rawOffsets;
      } else {
        bits::clearNull(rawNulls, i);
        const auto& val = *inputValues[i];

        std::memcpy(rawValues, val.data(), val.size());
        rawValues += val.size();
        *(rawOffsets + 1) = *rawOffsets + val.size();
        ++rawOffsets;
      }
    }

    holder.buffers[0] = (length == 0) ? nullptr : (const void*)rawNulls;
    return makeArrowArray(holder.buffers, 3, length, nullCount);
  }

  // Takes a vector with input data, generates an input ArrowArray and Bolt
  // Vector (using vector maker). Then converts ArrowArray into Bolt vector and
  // assert that both Bolt vectors are semantically the same.
  template <typename T>
  void benchmarkArrowImport(
      const char* format,
      const std::vector<std::optional<T>>& inputValues) {
    ArrowContextHolder holder;
    auto arrowArray = fillArrowArray(inputValues, holder);

    auto arrowSchema = makeArrowSchema(format);
    auto output = importFromArrow(arrowSchema, arrowArray, pool_.get());
  }

  void benchmarkImportScalarDouble() {
    try {
      for (int i = 0; i < ITERATION_TIMES; i++) {
        //       Skip below tests temporarily, cause rust doesn't support yet,
        //       need to add them back later. benchmarkArrowImport<double>("g",
        //       {}); benchmarkArrowImport<double>("g", {std::nullopt});
        benchmarkArrowImport<double>("g", {-99.9, 4.3, 31.1, 129.11, -12});
      }
    } catch (const std::exception& e) {
      std::cerr << "Error: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "Unknown error occurred!" << std::endl;
    }
  }

  void benchmarkImportScalarBigint() {
    try {
      for (int i = 0; i < ITERATION_TIMES; i++) {
        benchmarkArrowImport<int64_t>("l", {-99, 4, 318321631, 1211, -12});
        //       Skip below tests temporarily, cause rust doesn't support yet,
        //       need to add them back later. benchmarkArrowImport<int64_t>("l",
        //       {}); benchmarkArrowImport<int64_t>("l", {std::nullopt});
        //       benchmarkArrowImport<int64_t>("l", {std::nullopt, 12345678,
        //       std::nullopt}); benchmarkArrowImport<int64_t>("l",
        //       {std::nullopt, std::nullopt});
      }
    } catch (const std::exception& e) {
      std::cerr << "Error: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "Unknown error occurred!" << std::endl;
    }
  }

  void benchmarkImportString() {
    try {
      benchmarkArrowImport<std::string>("u", {});
      benchmarkArrowImport<std::string>("u", {"single"});
      benchmarkArrowImport<std::string>(
          "u",
          {
              "hello world",
              "larger string which should not be inlined...",
              std::nullopt,
              "hello",
              "from",
              "the",
              "other",
              "side",
              std::nullopt,
              std::nullopt,
          });

      benchmarkArrowImport<std::string>(
          "z",
          {
              std::nullopt,
              "testing",
              "a",
              std::nullopt,
              "varbinary",
              "vector",
              std::nullopt,
          });
    } catch (const std::exception& e) {
      std::cerr << "Error: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "Unknown error occurred!" << std::endl;
    }
  };
};

class ArrowBridgeArrayImportAsViewerBenchmark
    : public ArrowBridgeArrayImportBenchmark {
  bool isViewer() const override {
    return true;
  }

  VectorPtr importFromArrow(
      ArrowSchema& arrowSchema,
      ArrowArray& arrowArray,
      memory::MemoryPool* pool) override {
    return bytedance::bolt::importFromArrowAsViewer(
        arrowSchema, arrowArray, {}, pool);
  }

 public:
  static void benchmarkImportBigintScalarWrapper(uint32_t) {
    ArrowBridgeArrayImportAsViewerBenchmark instance;
    instance.benchmarkImportScalarBigint();
  }
  static void benchmarkImportStringWrapperAsViewer(uint32_t) {
    ArrowBridgeArrayImportAsViewerBenchmark instance;
    instance.benchmarkImportString();
  }
  static void benchmarkImportDoubleScalarWrapper(uint32_t) {
    ArrowBridgeArrayImportAsViewerBenchmark instance;
    instance.benchmarkImportScalarDouble();
  }
};

class ArrowBridgeArrayImportAsOwnerBenchmark
    : public ArrowBridgeArrayImportAsViewerBenchmark {
  bool isViewer() const override {
    return false;
  }

  VectorPtr importFromArrow(
      ArrowSchema& arrowSchema,
      ArrowArray& arrowArray,
      memory::MemoryPool* pool) override {
    return bytedance::bolt::importFromArrowAsOwner(
        arrowSchema, arrowArray, {}, pool);
  }
};

struct BenchmarkReleaseCalled {
  static bool schemaReleaseCalled;
  static bool arrayReleaseCalled;
  static void releaseSchema(ArrowSchema*) {
    schemaReleaseCalled = true;
  }
  static void releaseArray(ArrowArray*) {
    arrayReleaseCalled = true;
  }
};

bool BenchmarkReleaseCalled::schemaReleaseCalled = false;
bool BenchmarkReleaseCalled::arrayReleaseCalled = false;

void globalBenchmarkImportBigintScalarWrapper(uint32_t param) {
  ArrowBridgeArrayImportAsViewerBenchmark::benchmarkImportBigintScalarWrapper(
      param);
}

void globalBenchmarkImportDoubleScalarWrapper(uint32_t param) {
  ArrowBridgeArrayImportAsViewerBenchmark::benchmarkImportDoubleScalarWrapper(
      param);
}

void globalBenchmarkImportStringWrapper(uint32_t param) {
  ArrowBridgeArrayImportAsViewerBenchmark::benchmarkImportStringWrapperAsViewer(
      param);
}

BENCHMARK_NAMED_PARAM(globalBenchmarkImportBigintScalarWrapper, bigint);

BENCHMARK_NAMED_PARAM(globalBenchmarkImportDoubleScalarWrapper, double);

// Skip below temporarily, cause rust doesn't support for now
// BENCHMARK_NAMED_PARAM(globalBenchmarkImportStringWrapper, string);

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
