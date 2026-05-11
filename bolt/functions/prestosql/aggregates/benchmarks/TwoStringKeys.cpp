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

#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/tests/utils/ConnectorTestBase.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;

static constexpr int32_t kNumVectors = 7'000;
static constexpr int32_t kRowsPerVector = 4'000;

namespace {

// Compare performance of sum(x) with equivalent reduce_agg(x,..).
class TwoStringKeysBenchmark : public ConnectorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  explicit TwoStringKeysBenchmark() {
    ConnectorTestBase::SetUp();

    inputType_ = ROW({
        {"k1", VARCHAR()},
        {"k2", VARCHAR()},
        {"n", SMALLINT()},
    });

    VectorFuzzer::Options opts;
    opts.vectorSize = kRowsPerVector;
    opts.nullRatio = 0.0;
    opts.stringLength = 32;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<RowVectorPtr> vectors;
    for (auto i = 0; i < kNumVectors; ++i) {
      vectors.emplace_back(fuzzer.fuzzInputFlatRow(inputType_));
    }

    filePath_ = TempFilePath::create();
    writeToFile(filePath_->path, vectors);
  }

  ~TwoStringKeysBenchmark() override {
    ConnectorTestBase::TearDown();
  }

  void TestBody() override {}

  void verify() {
    auto plan = PlanBuilder()
                    .tableScan(inputType_)
                    .singleAggregation({"k1", "k2"}, {"sum(n)"})
                    .planFragment();

    auto task = makeTask(plan);

    vector_size_t numResultRows = 0;
    while (auto result = task->next()) {
      numResultRows += result->size();
    }

    LOG(ERROR) << exec::printPlanWithStats(
        *plan.planNode, task->taskStats(), true);
  }

  void run() {
    folly::BenchmarkSuspender suspender;

    auto plan = PlanBuilder()
                    .tableScan(inputType_)
                    .singleAggregation({"k1", "k2"}, {"sum(n)"})
                    .planFragment();

    auto task = makeTask(plan);

    suspender.dismiss();

    vector_size_t numResultRows = 0;
    while (auto result = task->next()) {
      numResultRows += result->size();
    }

    LOG(ERROR) << exec::printPlanWithStats(
        *plan.planNode, task->taskStats(), true);

    folly::doNotOptimizeAway(numResultRows);
  }

 private:
  std::shared_ptr<exec::Task> makeTask(core::PlanFragment plan) {
    auto task = exec::Task::create(
        "t",
        std::move(plan),
        0,
        core::QueryCtx::create(executor_.get()),
        exec::Task::ExecutionMode::kParallel,
        exec::Consumer{});

    task->addSplit("0", exec::Split(makeConnectorSplit(filePath_->path)));
    task->noMoreSplits("0");
    return task;
  }

  RowTypePtr inputType_;
  std::shared_ptr<TempFilePath> filePath_;
};

std::unique_ptr<TwoStringKeysBenchmark> benchmark;

BENCHMARK(two_string_keys) {
  benchmark->run();
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  TwoStringKeysBenchmark::SetUpTestCase();
  benchmark = std::make_unique<TwoStringKeysBenchmark>();
  benchmark->verify();
  //   folly::runBenchmarks();
  benchmark.reset();
  TwoStringKeysBenchmark::TearDownTestCase();
  return 0;
}
