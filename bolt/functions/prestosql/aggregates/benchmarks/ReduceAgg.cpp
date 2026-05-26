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
#include <string>

#include "bolt/common/testutil/TempFilePath.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;

static constexpr int32_t kNumVectors = 10;
static constexpr int32_t kRowsPerVector = 10'000;

namespace {

// Compare performance of sum(x) with equivalent reduce_agg(x,..).
class ReduceAggBenchmark : public HiveConnectorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  explicit ReduceAggBenchmark() {
    OperatorTestBase::SetUpTestCase();
    HiveConnectorTestBase::SetUp();

    inputType_ = ROW({
        {"i16", SMALLINT()},
    });

    VectorFuzzer::Options opts;
    opts.vectorSize = kRowsPerVector;
    opts.nullRatio = 0.1;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<RowVectorPtr> vectors;
    for (auto i = 0; i < kNumVectors; ++i) {
      vectors.emplace_back(fuzzer.fuzzInputRow(inputType_));
    }

    filePath_ = ::bytedance::bolt::test::TempFilePath::create();
    writeToFile(filePath_->path, vectors);
  }

  ~ReduceAggBenchmark() override {
    HiveConnectorTestBase::TearDown();
  }

  void TestBody() override {}

  static constexpr const char* kSum = "sum(i16)";
  static constexpr const char* kReduceAgg =
      "reduce_agg(i16, 0, (x, y) -> (x + y), (x, y) -> (x + y))";

  void verify() {
    // Verify global aggregation.
    {
      auto sumResults = runOnce(makeGlobalPlan(kSum));
      auto reduceAggResults = runOnce(makeGlobalPlan(kReduceAgg));

      assertEqualResults({sumResults}, {reduceAggResults});
    }

    // Verify group by aggregation.
    {
      auto sumResults = runOnce(makeGroupByPlan(kSum));
      auto reduceAggResults = runOnce(makeGroupByPlan(kReduceAgg));

      assertEqualResults({sumResults}, {reduceAggResults});
    }
  }

  void runGlobalSum() {
    runGlobal(kSum);
  }

  void runGroupBySum() {
    runGlobal(kSum);
  }

  void runGlobalReduceAgg() {
    runGlobal(kReduceAgg);
  }

  void runGroupByReduceAgg() {
    runGroupBy(kReduceAgg);
  }

 private:
  RowVectorPtr runOnce(const core::PlanFragment& planFragment) {
    auto task = makeTask(planFragment);

    std::vector<VectorPtr> results;
    while (auto result = task->next()) {
      results.push_back(result);
    }

    return copyResults(planFragment.planNode->outputType(), results);
  }

  RowVectorPtr copyResults(
      const TypePtr& resultType,
      const std::vector<VectorPtr>& results) {
    if (results.empty()) {
      return BaseVector::create<RowVector>(resultType, 0, pool());
    }

    auto totalCount = 0;
    for (const auto& result : results) {
      totalCount += result->size();
    }

    auto copy = BaseVector::create<RowVector>(resultType, totalCount, pool());
    auto copyCount = 0;
    for (const auto& result : results) {
      copy->copy(result.get(), copyCount, 0, result->size());
      copyCount += result->size();
    }

    return copy;
  }

  void runGlobal(const std::string& aggregate) {
    folly::BenchmarkSuspender suspender;

    auto plan = makeGlobalPlan(aggregate);
    auto task = makeTask(plan);

    suspender.dismiss();

    vector_size_t numResultRows = 0;
    while (auto result = task->next()) {
      numResultRows += result->size();
    }

    folly::doNotOptimizeAway(numResultRows);
  }

  void runGroupBy(const std::string& aggregate) {
    folly::BenchmarkSuspender suspender;

    auto plan = makeGroupByPlan(aggregate);
    auto task = makeTask(plan);

    suspender.dismiss();

    vector_size_t numResultRows = 0;
    while (auto result = task->next()) {
      numResultRows += result->size();
    }

    folly::doNotOptimizeAway(numResultRows);
  }

  core::PlanFragment makeGroupByPlan(const std::string& aggregate) {
    return PlanBuilder()
        .tableScan(inputType_)
        .project({"i16 % 1234 as key", "i16"})
        .partialAggregation({"key"}, {aggregate})
        .finalAggregation()
        .planFragment();
  }

  core::PlanFragment makeGlobalPlan(const std::string& aggregate) {
    return PlanBuilder()
        .tableScan(inputType_)
        .partialAggregation({}, {aggregate})
        .finalAggregation()
        .planFragment();
  }

  std::shared_ptr<exec::Task> makeTask(core::PlanFragment plan) {
    auto task = exec::Task::create(
        "t",
        std::move(plan),
        0,
        core::QueryCtx::create(executor_.get()),
        exec::Task::ExecutionMode::kSerial);

    task->addSplit("0", exec::Split(makeHiveConnectorSplit(filePath_->path)));
    task->noMoreSplits("0");
    return task;
  }

  RowTypePtr inputType_;
  std::shared_ptr<::bytedance::bolt::test::TempFilePath> filePath_;
};

std::unique_ptr<ReduceAggBenchmark> benchmark;

BENCHMARK(reduce_agg_global) {
  benchmark->runGlobalReduceAgg();
}

BENCHMARK_RELATIVE(sum_global) {
  benchmark->runGlobalSum();
}

BENCHMARK(reduce_agg_groupby) {
  benchmark->runGroupByReduceAgg();
}

BENCHMARK_RELATIVE(sum_groupby) {
  benchmark->runGroupBySum();
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  ReduceAggBenchmark::SetUpTestCase();
  benchmark = std::make_unique<ReduceAggBenchmark>();
  benchmark->verify();
  folly::runBenchmarks();
  benchmark.reset();
  ReduceAggBenchmark::TearDownTestCase();
  return 0;
}
