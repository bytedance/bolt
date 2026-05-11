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
#include <string>

#include "bolt/exec/tests/utils/ConnectorTestBase.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;

static constexpr int32_t kNumVectors = 1'0;
static constexpr int32_t kRowsPerVector = 10'000;
namespace bytedance::bolt::aggregate::test {

class AggregationLazyInputTest : public ConnectorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  explicit AggregationLazyInputTest() {}

  ~AggregationLazyInputTest() override {}

  void TestBody() override {}

  void run(const std::string& key, const std::string& aggregate, int count) {
    inputType_ = ROW({{"k_hash", INTEGER()}});
    VectorFuzzer::Options opts;
    opts.vectorSize = kRowsPerVector;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), 0);

    std::vector<RowVectorPtr> vectors;
    for (auto i = 0; i < kNumVectors; ++i) {
      std::vector<VectorPtr> children;
      // Generate key with many unique values from a large range (500K total
      // values).
      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
      vectors.emplace_back(makeRowVector(inputType_->names(), children));
    }

    filePath_ = TempFilePath::create()->path;
    writeToFile(filePath_, vectors);
    auto plan = PlanBuilder()
                    .tableScan(inputType_)
                    .partialAggregation(
                        {key}, std::vector<std::string>(count, aggregate))
                    .finalAggregation()
                    .planFragment();

    vector_size_t numResultRows = 0;
    auto task = makeTask(plan);

    task->addSplit("0", exec::Split(makeConnectorSplit(filePath_)));
    task->noMoreSplits("0");

    while (auto result = task->next()) {
      numResultRows += result->size();
    }
  }

  std::shared_ptr<exec::Task> makeTask(core::PlanFragment plan) {
    auto queryCtx = core::QueryCtx::create(executor_.get());
    std::unordered_map<std::string, std::string> configs;
    configs[core::QueryConfig::kAbandonPartialAggregationMinRows] = "100";
    configs[core::QueryConfig::kAbandonPartialAggregationMinPct] = "50";
    queryCtx->testingOverrideConfigUnsafe(std::move(configs));
    auto task = exec::Task::create(
        "t", std::move(plan), 0, queryCtx, exec::Task::ExecutionMode::kSerial);
    task->setSpillDirectory("/tmp/" + task->uuid(), false);
    return task;
  }

 private:
  RowTypePtr inputType_;
  std::string filePath_;
};

TEST_F(AggregationLazyInputTest, count) {
  run("k_hash", "count(1)", 30);
}

} // namespace bytedance::bolt::aggregate::test
