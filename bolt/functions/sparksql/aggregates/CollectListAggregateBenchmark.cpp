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

#include <chrono>
#include <iostream>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/core/Expressions.h"
#include "bolt/core/PlanFragment.h"
#include "bolt/core/PlanNode.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/Task.h"
#include "bolt/functions/sparksql/aggregates/CollectListAggregate.h"
#include "bolt/vector/FlatVector.h"

using namespace bytedance::bolt;

namespace {

constexpr vector_size_t kRows = 8 * 1024;
constexpr int32_t kIterations = 32;
constexpr int32_t kRowSizeOffset = 8;
constexpr int32_t kPlanVectors = 16;
constexpr int32_t kPlanGroupCount = 1024;

class CollectListAggregateBenchmark {
 public:
  CollectListAggregateBenchmark()
      : pool_{memory::memoryManager()->addLeafPool("collect_list_bm")},
        allocator_{pool_.get()},
        bigintInput_{makeBigintInput()},
        stringInput_{makeStringInput()},
        stringPlanInput_{makeStringPlanInput()} {
    functions::aggregate::sparksql::registerCollectListAggregate(
        "", false, true);
  }

  void runBigintSingleGroupAddExtract() {
    SelectivityVector rows{kRows};
    for (int32_t i = 0; i < kIterations; ++i) {
      auto fn = createAggregate(core::AggregationNode::Step::kSingle, BIGINT());
      auto group = makeGroup(*fn);
      char* groupPtr = group.data();
      std::vector<char*> groups{groupPtr};
      std::vector<vector_size_t> indices{0};
      fn->initializeNewGroups(groups.data(), indices);
      fn->addSingleGroupRawInput(groupPtr, rows, {bigintInput_}, false);

      VectorPtr result = BaseVector::create(ARRAY(BIGINT()), 1, pool_.get());
      fn->extractValues(groups.data(), 1, &result);
      fn->destroy(folly::Range(groups.data(), 1));
      folly::doNotOptimizeAway(result);
    }
  }

  void runStringSingleGroupAddExtract() {
    SelectivityVector rows{kRows};
    for (int32_t i = 0; i < kIterations; ++i) {
      auto fn =
          createAggregate(core::AggregationNode::Step::kSingle, VARCHAR());
      auto group = makeGroup(*fn);
      char* groupPtr = group.data();
      std::vector<char*> groups{groupPtr};
      std::vector<vector_size_t> indices{0};
      fn->initializeNewGroups(groups.data(), indices);
      fn->addSingleGroupRawInput(groupPtr, rows, {stringInput_}, false);

      VectorPtr result = BaseVector::create(ARRAY(VARCHAR()), 1, pool_.get());
      fn->extractValues(groups.data(), 1, &result);
      fn->destroy(folly::Range(groups.data(), 1));
      folly::doNotOptimizeAway(result);
    }
  }

  void runStringPartialFinalPlan() {
    for (int32_t i = 0; i < kIterations; ++i) {
      auto task = exec::Task::create(
          fmt::format("collect_list_string_plan_{}", i),
          core::PlanFragment(makeStringPartialFinalPlan()),
          0,
          core::QueryCtx::create(),
          exec::Task::ExecutionMode::kSerial);

      vector_size_t numResultRows = 0;
      vector_size_t totalChars = 0;
      while (auto result = task->next()) {
        numResultRows += result->size();
        auto arrays = result->childAt(1)->as<ArrayVector>();
        auto elements = arrays->elements()->asFlatVector<StringView>();
        for (vector_size_t row = 0; row < arrays->size(); ++row) {
          auto offset = arrays->offsetAt(row);
          auto size = arrays->sizeAt(row);
          for (vector_size_t idx = 0; idx < size; ++idx) {
            totalChars += elements->valueAt(offset + idx).size();
          }
        }
      }
      folly::doNotOptimizeAway(numResultRows);
      folly::doNotOptimizeAway(totalChars);
    }
  }

 private:
  VectorPtr makeBigintInput() {
    auto input = BaseVector::create<FlatVector<int64_t>>(
        BIGINT(), kRows, pool_.get());
    auto* rawValues = input->mutableRawValues();
    for (vector_size_t row = 0; row < kRows; ++row) {
      rawValues[row] = row;
    }
    return input;
  }

  VectorPtr makeStringInput() {
    auto input = BaseVector::create<FlatVector<StringView>>(
        VARCHAR(), kRows, pool_.get());
    auto flatInput = input->as<FlatVector<StringView>>();
    for (vector_size_t row = 0; row < kRows; ++row) {
      flatInput->set(row, makeString(row));
    }
    return input;
  }

  std::vector<RowVectorPtr> makeStringPlanInput() {
    std::vector<RowVectorPtr> batches;
    batches.reserve(kPlanVectors);
    for (int32_t batch = 0; batch < kPlanVectors; ++batch) {
      auto keys = BaseVector::create<FlatVector<int64_t>>(
          BIGINT(), kRows, pool_.get());
      auto values = BaseVector::create<FlatVector<StringView>>(
          VARCHAR(), kRows, pool_.get());
      auto* rawKeys = keys->mutableRawValues();
      auto flatValues = values->as<FlatVector<StringView>>();
      for (vector_size_t row = 0; row < kRows; ++row) {
        rawKeys[row] = row % kPlanGroupCount;
        flatValues->set(row, makeString(batch * kRows + row));
      }
      batches.push_back(std::make_shared<RowVector>(
          pool_.get(),
          ROW({{"k", BIGINT()}, {"v", VARCHAR()}}),
          nullptr,
          kRows,
          std::vector<VectorPtr>{keys, values}));
    }
    return batches;
  }

  StringView makeString(int64_t seed) {
    auto value = fmt::format("key-{:06d}-payload-{:06d}", seed % 4096, seed);
    auto copy = std::make_unique<std::string>(std::move(value));
    auto view = StringView(copy->data(), copy->size());
    ownedStrings_.push_back(std::move(copy));
    return view;
  }

  static core::FieldAccessTypedExprPtr field(
      const std::string& name,
      const TypePtr& type) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, name);
  }

  static core::AggregationNode::Aggregate collectListAggregate(
      const std::string& inputName,
      const TypePtr& inputType,
      const TypePtr& outputType,
      const std::vector<TypePtr>& rawInputTypes) {
    core::AggregationNode::Aggregate aggregate;
    aggregate.call = std::make_shared<core::CallTypedExpr>(
        outputType,
        std::vector<core::TypedExprPtr>{field(inputName, inputType)},
        "collect_list");
    aggregate.rawInputTypes = rawInputTypes;
    return aggregate;
  }

  core::PlanNodePtr makeStringPartialFinalPlan() {
    auto values =
        std::make_shared<core::ValuesNode>("0", stringPlanInput_, false, 1);
    auto key = field("k", BIGINT());
    auto partialAggregate = collectListAggregate(
        "v", VARCHAR(), ARRAY(VARCHAR()), {VARCHAR()});
    auto partial = std::make_shared<core::AggregationNode>(
        "1",
        core::AggregationNode::Step::kPartial,
        std::vector<core::FieldAccessTypedExprPtr>{key},
        std::vector<core::FieldAccessTypedExprPtr>{},
        std::vector<std::string>{"a0"},
        std::vector<core::AggregationNode::Aggregate>{partialAggregate},
        true,
        values);

    auto finalAggregate = collectListAggregate(
        "a0", ARRAY(VARCHAR()), ARRAY(VARCHAR()), {VARCHAR()});
    return std::make_shared<core::AggregationNode>(
        "2",
        core::AggregationNode::Step::kFinal,
        std::vector<core::FieldAccessTypedExprPtr>{key},
        std::vector<core::FieldAccessTypedExprPtr>{},
        std::vector<std::string>{"a0"},
        std::vector<core::AggregationNode::Aggregate>{finalAggregate},
        true,
        partial);
  }

  std::unique_ptr<exec::Aggregate> createAggregate(
      core::AggregationNode::Step step,
      const TypePtr& inputType) {
    auto fn = exec::Aggregate::create(
        "collect_list",
        step,
        std::vector<TypePtr>{inputType},
        ARRAY(inputType),
        core::QueryConfig({}));
    fn->setAllocator(&allocator_);
    fn->setOffsets(
        accumulatorOffset(*fn),
        /*nullByte=*/0,
        /*nullMask=*/1,
        kRowSizeOffset);
    return fn;
  }

  static int32_t accumulatorOffset(const exec::Aggregate& fn) {
    return bits::roundUp(
        kRowSizeOffset + static_cast<int32_t>(sizeof(uint32_t)),
        fn.accumulatorAlignmentSize());
  }

  static std::vector<char> makeGroup(const exec::Aggregate& fn) {
    return std::vector<char>(
        accumulatorOffset(fn) + fn.accumulatorFixedWidthSize());
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  HashStringAllocator allocator_;
  std::vector<std::unique_ptr<std::string>> ownedStrings_;
  VectorPtr bigintInput_;
  VectorPtr stringInput_;
  std::vector<RowVectorPtr> stringPlanInput_;
};

template <typename Func>
double timeMs(Func&& func) {
  auto start = std::chrono::steady_clock::now();
  func();
  auto end = std::chrono::steady_clock::now();
  return std::chrono::duration<double, std::milli>(end - start).count();
}

template <typename Func>
void runCase(const char* name, Func&& func) {
  constexpr int32_t kWarmup = 1;
  constexpr int32_t kRepeat = 3;
  for (int32_t i = 0; i < kWarmup; ++i) {
    func();
  }
  double totalMs = 0;
  for (int32_t i = 0; i < kRepeat; ++i) {
    totalMs += timeMs(func);
  }
  std::cout << name << " avg_ms=" << (totalMs / kRepeat) << std::endl;
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  CollectListAggregateBenchmark benchmark;
  runCase("collectListBigintSingleGroupAddExtract", [&] {
    benchmark.runBigintSingleGroupAddExtract();
  });
  runCase("collectListStringSingleGroupAddExtract", [&] {
    benchmark.runStringSingleGroupAddExtract();
  });
  runCase("collectListStringPartialFinalPlan", [&] {
    benchmark.runStringPartialFinalPlan();
  });
  return 0;
}
