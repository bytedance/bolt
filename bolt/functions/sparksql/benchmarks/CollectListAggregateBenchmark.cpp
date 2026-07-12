/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <fmt/format.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/core/PlanFragment.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
#include "bolt/vector/FlatVector.h"

using namespace bytedance::bolt;

namespace {

constexpr vector_size_t kRows = 8 * 1024;
constexpr int32_t kPlanVectors = 16;
constexpr int32_t kPlanGroupCount = 1024;

class CollectListAggregateBenchmark {
 public:
  CollectListAggregateBenchmark()
      : pool_{memory::memoryManager()->addLeafPool("collect_list_bm")},
        bigintPlanInput_{makeBigintPlanInput()},
        stringPlanInput_{makeStringPlanInput()} {
    functions::aggregate::sparksql::registerAggregateFunctions("", false, true);
  }

  void runBigintPartialFinalPlan() {
    runPlan(bigintPlanInput_);
  }

  void runStringPartialFinalPlan() {
    runPlan(stringPlanInput_);
  }

 private:
  template <typename TValue, typename TMakeValue>
  std::vector<RowVectorPtr> makePlanInput(
      const TypePtr& valueType,
      TMakeValue makeValue) {
    std::vector<RowVectorPtr> batches;
    batches.reserve(kPlanVectors);
    for (int32_t batch = 0; batch < kPlanVectors; ++batch) {
      auto keys = BaseVector::create<FlatVector<int64_t>>(
          BIGINT(), kRows, pool_.get());
      auto values =
          BaseVector::create<FlatVector<TValue>>(valueType, kRows, pool_.get());
      auto* rawKeys = keys->mutableRawValues();
      auto flatValues = values->template as<FlatVector<TValue>>();
      for (vector_size_t row = 0; row < kRows; ++row) {
        rawKeys[row] = row % kPlanGroupCount;
        flatValues->set(row, makeValue(batch * kRows + row));
      }
      batches.push_back(std::make_shared<RowVector>(
          pool_.get(),
          ROW({{"k", BIGINT()}, {"v", valueType}}),
          nullptr,
          kRows,
          std::vector<VectorPtr>{keys, values}));
    }
    return batches;
  }

  std::vector<RowVectorPtr> makeBigintPlanInput() {
    return makePlanInput<int64_t>(
        BIGINT(), [](int64_t seed) { return seed; });
  }

  std::vector<RowVectorPtr> makeStringPlanInput() {
    return makePlanInput<StringView>(
        VARCHAR(), [&](int64_t seed) { return makeString(seed); });
  }

  StringView makeString(int64_t seed) {
    auto value = fmt::format("key-{:06d}-payload-{:06d}", seed % 4096, seed);
    auto copy = std::make_unique<std::string>(std::move(value));
    auto view = StringView(copy->data(), copy->size());
    ownedStrings_.push_back(std::move(copy));
    return view;
  }

  core::PlanFragment makePartialFinalPlan(
      const std::vector<RowVectorPtr>& input) {
    return exec::test::PlanBuilder(pool_.get())
        .values(input)
        .partialAggregation({"k"}, {"collect_list(v)"})
        .finalAggregation()
        .planFragment();
  }

  void runPlan(const std::vector<RowVectorPtr>& input) {
    folly::BenchmarkSuspender suspender;
    auto task = exec::Task::create(
        "collect_list_plan",
        makePartialFinalPlan(input),
        0,
        core::QueryCtx::create(),
        exec::Task::ExecutionMode::kSerial);

    vector_size_t numResultRows = 0;
    vector_size_t numCollectedValues = 0;
    suspender.dismiss();

    while (auto result = task->next()) {
      numResultRows += result->size();
      auto arrays = result->childAt(1)->as<ArrayVector>();
      for (vector_size_t row = 0; row < arrays->size(); ++row) {
        numCollectedValues += arrays->sizeAt(row);
      }
    }

    folly::doNotOptimizeAway(numResultRows);
    folly::doNotOptimizeAway(numCollectedValues);
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::vector<std::unique_ptr<std::string>> ownedStrings_;
  std::vector<RowVectorPtr> bigintPlanInput_;
  std::vector<RowVectorPtr> stringPlanInput_;
};

std::unique_ptr<CollectListAggregateBenchmark> benchmark;

BENCHMARK(collect_list_bigint_partial_final_plan) {
  benchmark->runBigintPartialFinalPlan();
}

BENCHMARK(collect_list_string_partial_final_plan) {
  benchmark->runStringPartialFinalPlan();
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  benchmark = std::make_unique<CollectListAggregateBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
