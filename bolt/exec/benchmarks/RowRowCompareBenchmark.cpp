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

#include "bolt/exec/RowContainer.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/jit/RowContainer/RowContainerCodeGenerator.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

DEFINE_int32(row_count, 4096, "Number of rows to compare");

namespace {

enum class KeyPattern {
  kHighCardinality,
  kStringFallback,
};

class RowRowCompareBenchmark : public OperatorTestBase {
 public:
  RowRowCompareBenchmark(jit::CmpType cmpType, KeyPattern keyPattern)
      : cmpType_(cmpType), keyPattern_(keyPattern) {
    OperatorTestBase::SetUp();
    prepare();
  }

  ~RowRowCompareBenchmark() override {
    rows_.clear();
    decodedVectors_.clear();
    rowVector_.reset();
    rowContainer_.reset();
#ifdef ENABLE_BOLT_JIT
    jitModule_.reset();
#endif
    OperatorTestBase::TearDown();
  }

  void TestBody() override {}

  int64_t runNoJit() {
    int64_t result = 0;
    const auto size = rows_.size();
    for (auto i = 0; i < size; ++i) {
      const auto* left = rows_[i];
      const auto* right = rows_[(i * 131 + 17) & (size - 1)];
      const auto cmp = rowContainer_->compareRows(left, right, compareFlags_);
      result += cmpType_ == jit::CmpType::SORT_LESS ? (cmp < 0) : cmp;
    }
    return result;
  }

  int64_t runJit() {
    int64_t result = 0;
    const auto size = rows_.size();
    for (auto i = 0; i < size; ++i) {
      const auto* left = rows_[i];
      const auto* right = rows_[(i * 131 + 17) & (size - 1)];
      result += rowRowCompare_(left, right);
    }
    return result;
  }

 private:
  void prepare() {
    const auto rowCount = nextPowerOfTwo(std::max(FLAGS_row_count, 2));
    auto int64Vector = makeFlatVector<int64_t>(rowCount, [&](auto row) {
      return keyPattern_ == KeyPattern::kHighCardinality ? row * 17 : row % 16;
    });
    auto int32Vector = makeFlatVector<int32_t>(rowCount, [&](auto row) {
      return keyPattern_ == KeyPattern::kHighCardinality ? row * 31 : row % 16;
    });
    auto stringVector = makeFlatVector<std::string>(
        rowCount, [](auto row) { return fmt::format("k{:010}", row); });

    rowVector_ = makeRowVector(
        {"i64", "i32", "s"}, {int64Vector, int32Vector, stringVector});
    types_ = {BIGINT(), INTEGER(), VARCHAR()};
    compareFlags_ = std::vector<CompareFlags>(types_.size(), CompareFlags());
    rowContainer_ = std::make_shared<RowContainer>(types_, pool());
    decodedVectors_.reserve(types_.size());
    for (auto& child : rowVector_->children()) {
      decodedVectors_.emplace_back(std::make_shared<DecodedVector>(*child));
    }

    rows_.resize(rowCount);
    for (auto row = 0; row < rowCount; ++row) {
      rows_[row] = rowContainer_->newRow();
      for (auto key = 0; key < decodedVectors_.size(); ++key) {
        rowContainer_->store(*decodedVectors_[key], row, rows_[row], key);
      }
    }

#ifdef ENABLE_BOLT_JIT
    auto [jitModule, funcName] =
        rowContainer_->codegenCompare(types_, compareFlags_, cmpType_, false);
    jitModule_ = std::move(jitModule);
    rowRowCompare_ =
        reinterpret_cast<RowRowCompare>(jitModule_->getFuncPtr(funcName));
    BOLT_CHECK_NOT_NULL(rowRowCompare_);
#else
    BOLT_FAIL("Row-row compare benchmark requires ENABLE_BOLT_JIT");
#endif
  }

  static int32_t nextPowerOfTwo(int32_t value) {
    int32_t result = 1;
    while (result < value) {
      result <<= 1;
    }
    return result;
  }

  jit::CmpType cmpType_;
  KeyPattern keyPattern_;
  std::shared_ptr<RowContainer> rowContainer_;
  RowVectorPtr rowVector_;
  std::vector<TypePtr> types_;
  std::vector<CompareFlags> compareFlags_;
  std::vector<std::shared_ptr<DecodedVector>> decodedVectors_;
  std::vector<char*> rows_;
  RowRowCompare rowRowCompare_{nullptr};
#ifdef ENABLE_BOLT_JIT
  bytedance::bolt::jit::CompiledModuleSP jitModule_;
#endif
};

void runSortLessHighCardinality(uint32_t /*iters*/, bool useJit) {
  std::unique_ptr<RowRowCompareBenchmark> benchmark;
  BENCHMARK_SUSPEND {
    benchmark = std::make_unique<RowRowCompareBenchmark>(
        jit::CmpType::SORT_LESS, KeyPattern::kHighCardinality);
  }
  auto result = useJit ? benchmark->runJit() : benchmark->runNoJit();
  folly::doNotOptimizeAway(result);
}

void runSortLessStringFallback(uint32_t /*iters*/, bool useJit) {
  std::unique_ptr<RowRowCompareBenchmark> benchmark;
  BENCHMARK_SUSPEND {
    benchmark = std::make_unique<RowRowCompareBenchmark>(
        jit::CmpType::SORT_LESS, KeyPattern::kStringFallback);
  }
  auto result = useJit ? benchmark->runJit() : benchmark->runNoJit();
  folly::doNotOptimizeAway(result);
}

void runCmpSpillHighCardinality(uint32_t /*iters*/, bool useJit) {
  std::unique_ptr<RowRowCompareBenchmark> benchmark;
  BENCHMARK_SUSPEND {
    benchmark = std::make_unique<RowRowCompareBenchmark>(
        jit::CmpType::CMP_SPILL, KeyPattern::kHighCardinality);
  }
  auto result = useJit ? benchmark->runJit() : benchmark->runNoJit();
  folly::doNotOptimizeAway(result);
}

void runCmpSpillStringFallback(uint32_t /*iters*/, bool useJit) {
  std::unique_ptr<RowRowCompareBenchmark> benchmark;
  BENCHMARK_SUSPEND {
    benchmark = std::make_unique<RowRowCompareBenchmark>(
        jit::CmpType::CMP_SPILL, KeyPattern::kStringFallback);
  }
  auto result = useJit ? benchmark->runJit() : benchmark->runNoJit();
  folly::doNotOptimizeAway(result);
}

BENCHMARK_NAMED_PARAM(
    runSortLessHighCardinality,
    rowRowSortLessHighCardinality_noJIT,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    runSortLessHighCardinality,
    rowRowSortLessHighCardinality_JIT,
    true);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    runSortLessStringFallback,
    rowRowSortLessStringFallback_noJIT,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    runSortLessStringFallback,
    rowRowSortLessStringFallback_JIT,
    true);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    runCmpSpillHighCardinality,
    rowRowCmpSpillHighCardinality_noJIT,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    runCmpSpillHighCardinality,
    rowRowCmpSpillHighCardinality_JIT,
    true);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    runCmpSpillStringFallback,
    rowRowCmpSpillStringFallback_noJIT,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    runCmpSpillStringFallback,
    rowRowCmpSpillStringFallback_JIT,
    true);

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  RowRowCompareBenchmark::SetUpTestCase();
  folly::runBenchmarks();
  RowRowCompareBenchmark::TearDownTestCase();
  return 0;
}
