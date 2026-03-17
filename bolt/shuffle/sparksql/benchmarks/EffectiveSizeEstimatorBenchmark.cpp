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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/EffectiveSizeEstimator.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int32(num_rows, 8192, "Number of rows per batch");
DEFINE_int32(num_string_cols, 10, "Number of VARCHAR columns");
DEFINE_int32(num_int_cols, 5, "Number of BIGINT columns");
DEFINE_int32(avg_string_len, 128, "Average string length for VARCHAR columns");

using namespace bytedance::bolt;
using namespace bytedance::bolt::shuffle::sparksql;

namespace {

std::shared_ptr<memory::MemoryPool> pool_;
RowVectorPtr testBatch_;
RowVectorPtr complexBatch_;

void setup() {
  pool_ = memory::memoryManager()->addLeafPool("benchmark");

  // Build ROW type: num_int_cols BIGINT + num_string_cols VARCHAR
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (int i = 0; i < FLAGS_num_int_cols; ++i) {
    names.push_back("int_" + std::to_string(i));
    types.push_back(BIGINT());
  }
  for (int i = 0; i < FLAGS_num_string_cols; ++i) {
    names.push_back("str_" + std::to_string(i));
    types.push_back(VARCHAR());
  }
  auto rowType = ROW(std::move(names), std::move(types));

  VectorFuzzer::Options opts;
  opts.vectorSize = FLAGS_num_rows;
  opts.nullRatio = 0.05;
  opts.stringLength = FLAGS_avg_string_len;
  opts.stringVariableLength = true;

  VectorFuzzer fuzzer(opts, pool_.get());
  testBatch_ = fuzzer.fuzzInputRow(rowType);

  // Flatten all children to simulate post-ensureFlatten state
  for (auto i = 0; i < testBatch_->childrenSize(); ++i) {
    testBatch_->childAt(i)->loadedVector();
    BaseVector::flattenVector(testBatch_->childAt(i));
  }

  auto flatSize = testBatch_->estimateFlatSize();
  EffectiveSizeEstimator estimator;
  auto effectiveSize = estimator.estimate(testBatch_);
  LOG(INFO) << "Setup flat: rows=" << FLAGS_num_rows
            << ", intCols=" << FLAGS_num_int_cols
            << ", strCols=" << FLAGS_num_string_cols
            << ", avgStrLen=" << FLAGS_avg_string_len
            << ", estimateFlatSize=" << flatSize
            << ", effectiveSize=" << effectiveSize
            << ", ratio=" << estimator.cachedRatio();

  // Build complex type batch: BIGINT + MAP<VARCHAR, VARCHAR> +
  // ARRAY<VARCHAR> + ROW(BIGINT, VARCHAR)
  auto complexType =
      ROW({"id", "map_col", "array_col", "struct_col"},
          {BIGINT(),
           MAP(VARCHAR(), VARCHAR()),
           ARRAY(VARCHAR()),
           ROW({"nested_id", "nested_str"}, {BIGINT(), VARCHAR()})});

  VectorFuzzer complexFuzzer(opts, pool_.get());
  complexBatch_ = complexFuzzer.fuzzInputRow(complexType);

  for (auto i = 0; i < complexBatch_->childrenSize(); ++i) {
    complexBatch_->childAt(i)->loadedVector();
    BaseVector::flattenVector(complexBatch_->childAt(i));
  }

  auto complexFlatSize = complexBatch_->estimateFlatSize();
  EffectiveSizeEstimator complexEstimator;
  auto complexEffectiveSize = complexEstimator.estimate(complexBatch_);
  LOG(INFO) << "Setup complex: rows=" << FLAGS_num_rows
            << ", estimateFlatSize=" << complexFlatSize
            << ", effectiveSize=" << complexEffectiveSize
            << ", ratio=" << complexEstimator.cachedRatio();
}

} // namespace

BENCHMARK(EstimateFlatSize, n) {
  for (unsigned i = 0; i < n; ++i) {
    auto size = testBatch_->estimateFlatSize();
    folly::doNotOptimizeAway(size);
  }
}

BENCHMARK_RELATIVE(EffectiveSizeEstimator_FullScan, n) {
  for (unsigned i = 0; i < n; ++i) {
    EffectiveSizeEstimator estimator;
    auto size = estimator.estimate(testBatch_);
    folly::doNotOptimizeAway(size);
  }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(ComplexType_EstimateFlatSize, n) {
  for (unsigned i = 0; i < n; ++i) {
    auto size = complexBatch_->estimateFlatSize();
    folly::doNotOptimizeAway(size);
  }
}

BENCHMARK_RELATIVE(ComplexType_EffectiveSizeEstimator_FullScan, n) {
  for (unsigned i = 0; i < n; ++i) {
    EffectiveSizeEstimator estimator;
    auto size = estimator.estimate(complexBatch_);
    folly::doNotOptimizeAway(size);
  }
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize({});
  setup();
  folly::runBenchmarks();
  // Explicit cleanup before MemoryManager destruction.
  testBatch_.reset();
  complexBatch_.reset();
  pool_.reset();
  return 0;
}
