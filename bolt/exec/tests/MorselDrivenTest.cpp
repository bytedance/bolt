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

#include "bolt/exec/Exchange.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/ConnectorTestBase.h"
#include "bolt/exec/tests/utils/LocalExchangeSource.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

class MorselDrivenTest : public ConnectorTestBase {
 protected:
  void SetUp() override {
    ConnectorTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(createLocalExchangeSource);
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatSequence(T start, vector_size_t size) {
    return makeFlatVector<T>(size, [start](auto row) { return start + row; });
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatSequence(T start, T max, vector_size_t size) {
    return makeFlatVector<T>(
        size, [start, max](auto row) { return (start + row) % max; });
  }

  static std::string makeTaskId(const std::string& prefix, int num) {
    return fmt::format("local://{}-{}", prefix, num);
  }

  std::vector<std::shared_ptr<TempFilePath>> writeToFiles(
      const std::vector<RowVectorPtr>& vectors) {
    auto filePaths = makeFilePaths(vectors.size());
    for (auto i = 0; i < vectors.size(); i++) {
      writeToFile(filePaths[i]->path, vectors[i]);
    }
    return filePaths;
  }

  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      const core::PlanNodePtr& planNode,
      int destination = 0,
      Consumer consumer = nullptr,
      int64_t maxMemory = memory::kMaxMemory) {
    auto configCopy = configSettings_;
    auto queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig(std::move(configCopy)));

    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(), maxMemory, MemoryReclaimer::create()));
    core::PlanFragment planFragment{planNode};
    return Task::create(
        taskId,
        std::move(planFragment),
        destination,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel,
        std::move(consumer));
  }

  exec::Split remoteSplit(const std::string& taskId) {
    return exec::Split(std::make_shared<RemoteConnectorSplit>(taskId));
  }

  void addRemoteSplits(
      std::shared_ptr<Task> task,
      const std::vector<std::string>& remoteTaskIds) {
    for (auto& taskId : remoteTaskIds) {
      task->addSplit("0", remoteSplit(taskId));
    }
    task->noMoreSplits("0");
  }

  bool waitForTaskFinish(
      exec::Task* task,
      TaskState expectedState,
      uint64_t maxWaitMicros) {
    // Wait for task to transition to finished state.
    if (!waitForTaskStateChange(task, expectedState, maxWaitMicros)) {
      return false;
    }
    return waitForTaskDriversToFinish(task, maxWaitMicros);
  }

  bool waitForTaskCompletion(
      exec::Task* task,
      uint64_t maxWaitMicros = 1'000'000) {
    return waitForTaskFinish(task, TaskState::kFinished, maxWaitMicros);
  }

  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::string>& remoteTaskIds,
      const std::string& duckDbSql,
      std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt) {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
    for (auto& taskId : remoteTaskIds) {
      splits.push_back(std::make_shared<RemoteConnectorSplit>(taskId));
    }
    return OperatorTestBase::assertQuery(plan, splits, duckDbSql, sortingKeys);
  }

  std::unordered_map<std::string, std::string> configSettings_;
};

// Test 1: Verify "LocalExchange + PartialAgg" is morsel-driven
TEST_F(MorselDrivenTest, morselDrivenEnabledForPartialAgg) {
  std::vector<RowVectorPtr> vectors = {
      makeRowVector({makeFlatSequence<int32_t>(0, 100)}),
      makeRowVector({makeFlatSequence<int32_t>(53, 100)}),
      makeRowVector({makeFlatSequence<int32_t>(-71, 100)}),
  };

  auto filePaths = writeToFiles(vectors);

  auto rowType = asRowType(vectors[0]->type());

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  std::vector<core::PlanNodeId> scanNodeIds;

  auto scanAggNode = [&]() {
    auto builder = PlanBuilder(planNodeIdGenerator);
    auto scanNode = builder.tableScan(rowType).planNode();
    scanNodeIds.push_back(scanNode->id());
    return builder.partialAggregation({"c0"}, {"count(1)"}).planNode();
  };

  auto op = PlanBuilder(planNodeIdGenerator)
                .localPartition(
                    {"c0"},
                    {
                        scanAggNode(),
                        scanAggNode(),
                        scanAggNode(),
                    })
                .partialAggregation({"c0"}, {"count(1)"})
                .planNode();

  createDuckDbTable(vectors);

  AssertQueryBuilder queryBuilder(op, duckDbQueryRunner_);
  for (auto i = 0; i < filePaths.size(); ++i) {
    queryBuilder.split(scanNodeIds[i], makeConnectorSplit(filePaths[i]->path));
  }

  auto task = queryBuilder.maxDrivers(4)
                  .config(core::QueryConfig::kEnableMorselDriven, "true")
                  .assertResults("SELECT c0, count(1) FROM tmp GROUP BY 1");

  ASSERT_EQ(task->numPipelines(), 4);
  ASSERT_EQ(
      task->isMorselDrivenPipeline(0),
      true); // Make sure "LocalExchange + PartialAgg" can be morsel-driven
}

// Test 2: Verify  morsel-driven is disabled for "LocalExchange + SingleAgg"
TEST_F(MorselDrivenTest, morselDrivenDisabledForSingleAgg) {
  std::vector<RowVectorPtr> vectors;
  for (auto i = 0; i < 21; i++) {
    vectors.emplace_back(makeRowVector({makeFlatVector<int32_t>(
        100, [i](auto row) { return -71 + i * 10 + row; })}));
  }

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto valuesNode = [&](int start, int end) {
    return PlanBuilder(planNodeIdGenerator)
        .values(std::vector<RowVectorPtr>(
            vectors.begin() + start, vectors.begin() + end))
        .planNode();
  };

  auto op = PlanBuilder(planNodeIdGenerator)
                .localPartition(
                    {},
                    {
                        valuesNode(0, 7),
                        valuesNode(7, 14),
                        valuesNode(14, 21),
                    })
                .singleAggregation({}, {"count(1)", "min(c0)", "max(c0)"})
                .planNode();

  auto task = AssertQueryBuilder(op, duckDbQueryRunner_)
                  .maxDrivers(4)
                  .config(core::QueryConfig::kEnableMorselDriven, "true")
                  .assertResults("SELECT 2100, -71, 228");
  ASSERT_EQ(task->numPipelines(), 4);
  ASSERT_EQ(task->isMorselDrivenPipeline(0), false);
}

// Test 3: Verify  morsel-driven is disabled for "LocalExchange + FinalAgg"
TEST_F(MorselDrivenTest, morselDrivenDisabledForFinalAgg) {
  std::vector<RowVectorPtr> vectors = {
      makeRowVector({makeFlatSequence<int32_t>(0, 100)}),
      makeRowVector({makeFlatSequence<int32_t>(53, 100)}),
      makeRowVector({makeFlatSequence<int32_t>(-71, 100)}),
  };

  auto filePaths = writeToFiles(vectors);

  auto rowType = asRowType(vectors[0]->type());

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  std::vector<core::PlanNodeId> scanNodeIds;

  auto scanAggNode = [&]() {
    auto builder = PlanBuilder(planNodeIdGenerator);
    auto scanNode = builder.tableScan(rowType).planNode();
    scanNodeIds.push_back(scanNode->id());
    return builder.partialAggregation({"c0"}, {"count(1)"}).planNode();
  };

  auto op = PlanBuilder(planNodeIdGenerator)
                .localPartition(
                    {"c0"},
                    {
                        scanAggNode(),
                        scanAggNode(),
                        scanAggNode(),
                    })
                .finalAggregation({"c0"}, {"count(1)"}, {{BIGINT()}})
                .planNode();

  createDuckDbTable(vectors);

  AssertQueryBuilder queryBuilder(op, duckDbQueryRunner_);
  for (auto i = 0; i < filePaths.size(); ++i) {
    queryBuilder.split(scanNodeIds[i], makeConnectorSplit(filePaths[i]->path));
  }

  auto task = queryBuilder.maxDrivers(4)
                  .config(core::QueryConfig::kEnableMorselDriven, "true")
                  .assertResults("SELECT c0, count(1) FROM tmp GROUP BY 1");

  ASSERT_EQ(task->numPipelines(), 4);
  ASSERT_EQ(
      task->isMorselDrivenPipeline(0),
      false); // Make sure "LocalExchange + FinalAgg" can not be morsel-driven
}

// Test 4: Verify  morsel-driven is disabled for "LocalExchange + Window"
TEST_F(MorselDrivenTest, morselDrivenDisabledForWindow) {
  const vector_size_t size = 1'000;

  std::vector<RowVectorPtr> vectors = {
      makeRowVector(
          {"d", "p", "s"},
          {makeFlatSequence<int32_t>(0, 100),
           makeFlatSequence<int32_t>(0, 100),
           makeFlatSequence<int32_t>(0, 100)}),
      makeRowVector(
          {"d", "p", "s"},
          {makeFlatSequence<int32_t>(53, 100),
           makeFlatSequence<int32_t>(53, 100),
           makeFlatSequence<int32_t>(53, 100)}),
      makeRowVector(
          {"d", "p", "s"},
          {makeFlatSequence<int32_t>(-71, 100),
           makeFlatSequence<int32_t>(-71, 100),
           makeFlatSequence<int32_t>(-71, 100)}),
  };

  createDuckDbTable(vectors);

  auto filePaths = writeToFiles(vectors);

  auto rowType = asRowType(vectors[0]->type());

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  std::vector<core::PlanNodeId> scanNodeIds;

  auto scanNode = [&]() {
    auto node = PlanBuilder(planNodeIdGenerator).tableScan(rowType).planNode();
    scanNodeIds.push_back(node->id());
    return node;
  };

  auto op = PlanBuilder(planNodeIdGenerator)
                .localPartition({}, {scanNode(), scanNode(), scanNode()})
                .window({"row_number() over (partition by p order by s)"})
                .planNode();

  AssertQueryBuilder queryBuilder(op, duckDbQueryRunner_);

  for (auto i = 0; i < filePaths.size(); ++i) {
    queryBuilder.split(scanNodeIds[i], makeConnectorSplit(filePaths[i]->path));
  }
  auto task =
      queryBuilder.maxDrivers(4)
          .config(core::QueryConfig::kEnableMorselDriven, "true")
          .assertResults(
              "SELECT *, row_number() over (partition by p order by s) FROM tmp");
  ASSERT_EQ(task->numPipelines(), 4);
  ASSERT_EQ(task->isMorselDrivenPipeline(0), false);
}

// Test 5: Verify  morsel-driven is disabled for "HashBuild" for efficiency
// reason
TEST_F(MorselDrivenTest, morselDrivenDisabledForHashBuild) {
  std::vector<RowVectorPtr> probeVectors = {
      makeRowVector(
          {"t_k1", "t_p", "t_s"},
          {makeFlatSequence<int32_t>(0, 100),
           makeFlatSequence<int32_t>(0, 1000, 100),
           makeFlatSequence<int32_t>(0, 10000, 100)}),
      makeRowVector(
          {"t_k1", "t_p", "t_s"},
          {makeFlatSequence<int32_t>(0, 100),
           makeFlatSequence<int32_t>(0, 1000, 100),
           makeFlatSequence<int32_t>(0, 10000, 100)}),
      makeRowVector(
          {"t_k1", "t_p", "t_s"},
          {makeFlatSequence<int32_t>(0, 100),
           makeFlatSequence<int32_t>(0, 1000, 100),
           makeFlatSequence<int32_t>(0, 10000, 100)}),
  };

  std::vector<RowVectorPtr> buildVectors = {
      makeRowVector(
          {"u_k1", "u_p", "u_s"},
          {makeFlatSequence<int32_t>(0, 100),
           makeFlatSequence<int32_t>(0, 2000, 100),
           makeFlatSequence<int32_t>(0, 20000, 100)}),
  };

  createDuckDbTable("probe", probeVectors);
  createDuckDbTable("build", buildVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto op = PlanBuilder(planNodeIdGenerator)
                .values(probeVectors)
                .hashJoin(
                    {"t_k1"},
                    {"u_k1"},
                    PlanBuilder(planNodeIdGenerator)
                        .values(buildVectors)
                        .localPartition({"u_k1"})
                        .planNode(),
                    "",
                    {"t_k1", "t_p", "t_s", "u_p", "u_s"},
                    core::JoinType::kInner)
                .planNode();
  AssertQueryBuilder queryBuilder(op, duckDbQueryRunner_);

  auto task =
      queryBuilder.maxDrivers(4)
          .config(core::QueryConfig::kEnableMorselDriven, "true")
          .assertResults(
              "SELECT t_k1, t_p, t_s, u_p, u_s FROM probe join build on t_k1 = u_k1");
  ASSERT_EQ(task->numPipelines(), 3);
  ASSERT_EQ(task->isMorselDrivenPipeline(1), false);
}

// Test 6: Verify  morsel-driven can rollback plan rewrite (inserting a
// localPartition in between Exchange->HashJoin) when the resulting pipeline
// (hashProbe+singleAgg) cannot be morsel-driven.
TEST_F(MorselDrivenTest, morselDrivenPlanRewriteRollback) {
  std::vector<std::shared_ptr<Task>> tasks;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  configSettings_[core::QueryConfig::kEnableMorselDriven] = "true";

  std::vector<RowVectorPtr> probeVectors = {
      makeRowVector(
          {"t_k1", "t_p", "t_s"},
          {makeFlatSequence<int32_t>(0, 100),
           makeFlatSequence<int32_t>(0, 1000, 100),
           makeFlatSequence<int32_t>(0, 10000, 100)}),
      makeRowVector(
          {"t_k1", "t_p", "t_s"},
          {makeFlatSequence<int32_t>(0, 100),
           makeFlatSequence<int32_t>(0, 1000, 100),
           makeFlatSequence<int32_t>(0, 10000, 100)}),
      makeRowVector(
          {"t_k1", "t_p", "t_s"},
          {makeFlatSequence<int32_t>(0, 100),
           makeFlatSequence<int32_t>(0, 1000, 100),
           makeFlatSequence<int32_t>(0, 10000, 100)}),
  };

  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan =
      PlanBuilder().values(probeVectors).partitionedOutput({}, 1).planNode();

  auto leafTask = makeTask(leafTaskId, leafPlan, tasks.size());
  tasks.push_back(leafTask);
  leafTask->start(1);

  createDuckDbTable("probe", probeVectors);
  std::vector<RowVectorPtr> buildVectors = {
      makeRowVector(
          {"u_k1", "u_p", "u_s"},
          {makeFlatSequence<int32_t>(0, 100),
           makeFlatSequence<int32_t>(0, 2000, 100),
           makeFlatSequence<int32_t>(0, 20000, 100)}),
  };
  createDuckDbTable("build", buildVectors);
  auto joinAggPlan = PlanBuilder(planNodeIdGenerator)
                         .exchange(leafPlan->outputType())
                         .hashJoin(
                             {"t_k1"},
                             {"u_k1"},
                             PlanBuilder(planNodeIdGenerator)
                                 .values(buildVectors)
                                 .localPartition({"u_k1"})
                                 .planNode(),
                             "",
                             {"t_k1", "t_p", "t_s", "u_p", "u_s"},
                             core::JoinType::kInner)
                         .singleAggregation({"t_k1"}, {"max(t_p)"})
                         .partitionedOutput({}, 1)
                         .planNode();
  auto taskId = makeTaskId("join", 0);
  auto task = makeTask(taskId, joinAggPlan);
  tasks.push_back(task);
  task->start(1);
  addRemoteSplits(task, {leafTaskId});
  auto outputPlan = PlanBuilder(planNodeIdGenerator)
                        .exchange(joinAggPlan->outputType())
                        .planNode();
  assertQuery(
      outputPlan,
      {taskId},
      "SELECT t_k1, max(t_p)  FROM probe join build on t_k1 = u_k1 group by t_k1");
  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }

  // # pipelines will be 4 if we do not have planrewrite rollback
  ASSERT_EQ(task->numPipelines(), 3);
  ASSERT_EQ(task->isMorselDrivenPipeline(0), false);

  configSettings_[core::QueryConfig::kEnableMorselDriven] = "false";
}

TEST_F(MorselDrivenTest, improvedEarlyCompletion) {
  std::vector<std::shared_ptr<Task>> tasks;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  configSettings_[core::QueryConfig::kEnableMorselDriven] = "true";
  configSettings_[core::QueryConfig::kMorselSize] = "100";
  configSettings_[core::QueryConfig::kMorselDrivenPrimedQueueSize] = "10";

  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < 100; i++) {
    probeVectors.push_back(makeRowVector(
        {"t_k1", "t_p", "t_s"},
        {makeFlatSequence<int32_t>(0, 100),
         makeFlatSequence<int32_t>(0, 1000, 100),
         makeFlatSequence<int32_t>(0, 10000, 100)}));
  }

  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan =
      PlanBuilder().values(probeVectors).partitionedOutput({}, 1).planNode();

  auto leafTask = makeTask(leafTaskId, leafPlan, tasks.size());
  tasks.push_back(leafTask);

  createDuckDbTable("probe", probeVectors);
  std::vector<RowVectorPtr> buildVectors = {makeRowVector(
      ROW({"u_k1", "u_p", "u_s"}, {INTEGER(), INTEGER(), INTEGER()}), 0)};
  createDuckDbTable("build", buildVectors);
  auto joinAggPlan = PlanBuilder(planNodeIdGenerator)
                         .exchange(leafPlan->outputType())
                         .hashJoin(
                             {"t_k1"},
                             {"u_k1"},
                             PlanBuilder(planNodeIdGenerator)
                                 .values(buildVectors)
                                 .localPartition({"u_k1"})
                                 .planNode(),
                             "",
                             {"t_k1", "t_p", "t_s", "u_p", "u_s"},
                             core::JoinType::kInner)
                         .partitionedOutput({}, 1)
                         .planNode();
  auto taskId = makeTaskId("join", 0);
  auto task = makeTask(taskId, joinAggPlan);
  tasks.push_back(task);
  addRemoteSplits(task, {leafTaskId});
  task->start(1);

  auto outputPlan = PlanBuilder(planNodeIdGenerator)
                        .exchange(joinAggPlan->outputType())
                        .planNode();
  leafTask->start(1);

  assertQuery(
      outputPlan,
      {taskId},
      "SELECT t_k1, t_p, t_s, u_p, u_s FROM probe join build on t_k1 = u_k1");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }

  ASSERT_EQ(task->numPipelines(), 4);
  ASSERT_EQ(task->isMorselDrivenPipeline(0), true);
  // Number of finished drivers should be < 13 (100/10 + 3) due to early
  // completion
  ASSERT_LE(task->numFinishedDrivers(), 13);

  configSettings_[core::QueryConfig::kEnableMorselDriven] = "false";
}
