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

#include <boost/random/uniform_int_distribution.hpp>
#include <gtest/gtest.h>

#include <string>

#include <folly/experimental/EventCount.h>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/file/Utils.h"
#include "bolt/common/hyperloglog/SparseHll.h"
#include "bolt/common/testutil/TempDirectoryPath.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/dwio/dwrf/writer/Writer.h"
#include "bolt/exec/HashJoinBridge.h"
#include "bolt/exec/OperatorTraceReader.h"
#include "bolt/exec/PartitionFunction.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/TableWriter.h"
#include "bolt/exec/TraceUtil.h"
#include "bolt/exec/tests/utils/ArbitratorTestUtil.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/tool/trace/HashJoinReplayer.h"
#include "bolt/tool/trace/TraceReplayRunner.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::core;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::connector;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::common::testutil;
using namespace bytedance::bolt::common::hll;
namespace bytedance::bolt::tool::trace::test {
class HashJoinReplayerTest : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    FLAGS_bolt_testing_enable_arbitration = true;
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    HiveConnectorTestBase::SetUpTestCase();
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::LocationHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    connector::hive::HiveInsertTableHandle::registerSerDe();
    connector::hive::HiveConnectorSplit::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();
  }

  void TearDown() override {
    probeInput_.clear();
    buildInput_.clear();
    HiveConnectorTestBase::TearDown();
  }

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    std::unordered_map<core::PlanNodeId, std::vector<exec::Split>> splits;

    explicit PlanWithSplits(
        const core::PlanNodePtr& _plan,
        const core::PlanNodeId& _probeScanId = "",
        const core::PlanNodeId& _buildScanId = "",
        const std::unordered_map<
            core::PlanNodeId,
            std::vector<bolt::exec::Split>>& _splits = {})
        : plan(_plan),
          probeScanId(_probeScanId),
          buildScanId(_buildScanId),
          splits(_splits) {}
  };

  RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b) {
    std::vector<std::string> names = a->names();
    std::vector<TypePtr> types = a->children();

    for (auto i = 0; i < b->size(); ++i) {
      names.push_back(b->nameOf(i));
      types.push_back(b->childAt(i));
    }

    return ROW(std::move(names), std::move(types));
  }

  std::vector<RowVectorPtr>
  makeVectors(int32_t count, int32_t rowsPerVector, const RowTypePtr& rowType) {
    return HiveConnectorTestBase::makeVectors(rowType, count, rowsPerVector);
  }

  std::vector<Split> makeSplits(
      const std::vector<RowVectorPtr>& inputs,
      const std::string& path,
      memory::MemoryPool* writerPool) {
    std::vector<Split> splits;
    for (auto i = 0; i < 4; ++i) {
      const std::string filePath = fmt::format("{}/{}", path, i);
      writeToFile(filePath, inputs);
      splits.emplace_back(makeHiveConnectorSplit(filePath));
    }

    return splits;
  }

  PlanWithSplits createPlan(
      const std::string& tableDir,
      core::JoinType joinType,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    const std::vector<Split> probeSplits =
        makeSplits(probeInput, fmt::format("{}/probe", tableDir), pool());
    const std::vector<Split> buildSplits =
        makeSplits(buildInput, fmt::format("{}/build", tableDir), pool());
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    const auto outputColumns = concat(
                                   asRowType(probeInput_[0]->type()),
                                   asRowType(buildInput_[0]->type()))
                                   ->names();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .tableScan(probeType_)
                    .capturePlanNodeId(probeScanId)
                    .hashJoin(
                        probeKeys,
                        buildKeys,
                        PlanBuilder(planNodeIdGenerator)
                            .tableScan(buildType_)
                            .capturePlanNodeId(buildScanId)
                            .planNode(),
                        /*filter=*/"",
                        outputColumns,
                        joinType,
                        false)
                    .capturePlanNodeId(traceNodeId_)
                    .planNode();
    return PlanWithSplits{
        plan,
        probeScanId,
        buildScanId,
        {{probeScanId, probeSplits}, {buildScanId, buildSplits}}};
  }

  core::PlanNodeId traceNodeId_;
  RowTypePtr probeType_{
      ROW({"t0", "t1", "t2", "t3"}, {BIGINT(), VARCHAR(), SMALLINT(), REAL()})};

  RowTypePtr buildType_{
      ROW({"u0", "u1", "u2", "u3"},
          {BIGINT(), INTEGER(), SMALLINT(), VARCHAR()})};
  std::vector<RowVectorPtr> probeInput_ = makeVectors(5, 100, probeType_);
  std::vector<RowVectorPtr> buildInput_ = makeVectors(3, 100, buildType_);

  const std::vector<std::string> probeKeys_{"t0"};
  const std::vector<std::string> buildKeys_{"u0"};
  const std::shared_ptr<TempDirectoryPath> testDir_ =
      TempDirectoryPath::create();
  const std::string tableDir_ =
      fmt::format("{}/{}", testDir_->getPath(), "table");
};

TEST_F(HashJoinReplayerTest, basic) {
  const auto planWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder builder(planWithSplits.plan);
  for (const auto& [planNodeId, nodeSplits] : planWithSplits.splits) {
    builder.splits(planNodeId, nodeSplits);
  }
  const auto result = builder.copyResults(pool());

  const auto traceRoot =
      fmt::format("{}/{}/traceRoot/", testDir_->getPath(), "basic");
  std::shared_ptr<Task> task;
  auto tracePlanWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.maxDrivers(4)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_);
  for (const auto& [planNodeId, nodeSplits] : tracePlanWithSplits.splits) {
    traceBuilder.splits(planNodeId, nodeSplits);
  }
  auto traceResult = traceBuilder.copyResults(pool(), task);

  assertEqualResults({result}, {traceResult});

  const auto taskId = task->taskId();
  const auto replayingResult = HashJoinReplayer(
                                   traceRoot,
                                   task->queryCtx()->queryId(),
                                   task->taskId(),
                                   traceNodeId_,
                                   "HashJoin",
                                   "",
                                   0,
                                   executor_.get())
                                   .run();
  assertEqualResults({result}, {replayingResult});
}

TEST_F(HashJoinReplayerTest, partialDriverIds) {
  const std::shared_ptr<TempDirectoryPath> testDir =
      TempDirectoryPath::create();
  const std::string tableDir =
      fmt::format("{}/{}", testDir->getPath(), "table");
  const auto planWithSplits = createPlan(
      tableDir,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder builder(planWithSplits.plan);
  for (const auto& [planNodeId, nodeSplits] : planWithSplits.splits) {
    builder.splits(planNodeId, nodeSplits);
  }
  const auto result = builder.copyResults(pool());

  const auto traceRoot =
      fmt::format("{}/{}/traceRoot/", testDir->getPath(), "basic");
  std::shared_ptr<Task> task;
  auto tracePlanWithSplits = createPlan(
      tableDir,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.maxDrivers(4)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_);
  for (const auto& [planNodeId, nodeSplits] : tracePlanWithSplits.splits) {
    traceBuilder.splits(planNodeId, nodeSplits);
  }
  auto traceResult = traceBuilder.copyResults(pool(), task);

  assertEqualResults({result}, {traceResult});

  const auto taskId = task->taskId();
  const auto taskTraceDir =
      exec::trace::getTaskTraceDirectory(traceRoot, *task);
  const auto opTraceDir =
      exec::trace::getOpTraceDirectory(taskTraceDir, traceNodeId_, 0, 0);
  const auto opTraceDataFile = exec::trace::getOpTraceInputFilePath(opTraceDir);

  auto fs = filesystems::getFileSystem(taskTraceDir, nullptr);
  fs->openFileForRead(opTraceDataFile);
  HashJoinReplayer(
      traceRoot,
      task->queryCtx()->queryId(),
      task->taskId(),
      traceNodeId_,
      "HashJoin",
      "1,3",
      0,
      executor_.get())
      .run();
}

TEST_F(HashJoinReplayerTest, runner) {
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  std::shared_ptr<Task> task;
  auto tracePlanWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_);
  for (const auto& [planNodeId, nodeSplits] : tracePlanWithSplits.splits) {
    traceBuilder.splits(planNodeId, nodeSplits);
  }
  auto traceResult = traceBuilder.copyResults(pool(), task);

  const auto taskTraceDir =
      exec::trace::getTaskTraceDirectory(traceRoot, *task);
  const auto probeOperatorTraceDir = exec::trace::getOpTraceDirectory(
      taskTraceDir,
      traceNodeId_,
      /*pipelineId=*/0,
      /*driverId=*/0);
  const auto probeSummary =
      exec::trace::OperatorTraceSummaryReader(probeOperatorTraceDir, pool())
          .read();
  ASSERT_EQ(probeSummary.opType, "HashProbe");
  ASSERT_GT(probeSummary.peakMemory, 0);
  ASSERT_GT(probeSummary.inputRows, 0);
  ASSERT_GT(probeSummary.inputBytes, 0);
  ASSERT_EQ(probeSummary.rawInputRows, 0);
  ASSERT_EQ(probeSummary.rawInputBytes, 0);

  const auto buildOperatorTraceDir = exec::trace::getOpTraceDirectory(
      taskTraceDir,
      traceNodeId_,
      /*pipelineId=*/1,
      /*driverId=*/0);
  const auto buildSummary =
      exec::trace::OperatorTraceSummaryReader(buildOperatorTraceDir, pool())
          .read();
  ASSERT_EQ(buildSummary.opType, "HashBuild");
  ASSERT_GT(buildSummary.peakMemory, 0);
  ASSERT_GT(buildSummary.inputRows, 0);
  // NOTE: the input bytes is 0 because of the lazy materialization.
  ASSERT_EQ(buildSummary.inputBytes, 0);
  ASSERT_EQ(buildSummary.rawInputRows, 0);
  ASSERT_EQ(buildSummary.rawInputBytes, 0);

  FLAGS_root_dir = traceRoot;
  FLAGS_query_id = task->queryCtx()->queryId();
  FLAGS_task_id = task->taskId();
  FLAGS_node_id = traceNodeId_;
  FLAGS_summary = true;
  {
    TraceReplayRunner runner;
    runner.init();
    runner.run();
  }

  FLAGS_task_id = task->taskId();
  FLAGS_driver_ids = "";
  FLAGS_summary = false;
  {
    TraceReplayRunner runner;
    runner.init();
    runner.run();
  }
}

DEBUG_ONLY_TEST_F(HashJoinReplayerTest, hashBuildSpill) {
  const auto planWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder builder(planWithSplits.plan);
  for (const auto& [planNodeId, nodeSplits] : planWithSplits.splits) {
    builder.splits(planNodeId, nodeSplits);
  }
  const auto result = builder.copyResults(pool());

  const auto traceRoot =
      fmt::format("{}/{}/traceRoot/", testDir_->getPath(), "hash_build_spill");
  const auto spillDir =
      fmt::format("{}/{}/spillDir/", testDir_->getPath(), "hash_build_spill");
  std::shared_ptr<Task> task;
  auto tracePlanWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);

  std::atomic_bool injectSpillOnce{true};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::HashBuild::finishHashBuild",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!injectSpillOnce.exchange(false)) {
          return;
        }
        Operator::ReclaimableSectionGuard guard(op);
        testingRunArbitration(op->pool());
      }));

  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_)
      .config(core::QueryConfig::kSpillEnabled, true)
      .config(core::QueryConfig::kJoinSpillEnabled, true)
      .spillDirectory(spillDir);
  for (const auto& [planNodeId, nodeSplits] : tracePlanWithSplits.splits) {
    traceBuilder.splits(planNodeId, nodeSplits);
  }

  auto traceResult = traceBuilder.copyResults(pool(), task);
  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(traceNodeId_);
  auto opStats = toOperatorStats(task->taskStats());
  ASSERT_GT(
      opStats.at("HashBuild").runtimeStats[Operator::kSpillWrites].sum, 0);
  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 0);
  ASSERT_GT(stats.spilledPartitions, 0);

  assertEqualResults({result}, {traceResult});

  const auto taskId = task->taskId();
  const auto replayingResult = HashJoinReplayer(
                                   traceRoot,
                                   task->queryCtx()->queryId(),
                                   task->taskId(),
                                   traceNodeId_,
                                   "HashJoin",
                                   "",
                                   0,
                                   executor_.get())
                                   .run();
  assertEqualResults({result}, {replayingResult});
}

DEBUG_ONLY_TEST_F(HashJoinReplayerTest, hashProbeSpill) {
  const auto planWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);

  AssertQueryBuilder builder(planWithSplits.plan);
  for (const auto& [planNodeId, nodeSplits] : planWithSplits.splits) {
    builder.splits(planNodeId, nodeSplits);
  }
  const auto result = builder.copyResults(pool());

  const auto traceRoot =
      fmt::format("{}/{}/traceRoot/", testDir_->getPath(), "hash_probe_spill");
  const auto spillDir =
      fmt::format("{}/{}/spillDir/", testDir_->getPath(), "hash_probe_spill");
  std::shared_ptr<Task> task;
  auto tracePlanWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);

  std::atomic_bool injectProbeSpillOnce{true};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!isHashProbeMemoryPool(*op->pool())) {
          return;
        }
        if (!injectProbeSpillOnce.exchange(false)) {
          return;
        }
        testingRunArbitration(op->pool());
      }));

  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_)
      .config(core::QueryConfig::kSpillEnabled, true)
      .config(core::QueryConfig::kJoinSpillEnabled, true)
      .spillDirectory(spillDir);
  for (const auto& [planNodeId, nodeSplits] : tracePlanWithSplits.splits) {
    traceBuilder.splits(planNodeId, nodeSplits);
  }

  auto traceResult = traceBuilder.copyResults(pool(), task);
  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(traceNodeId_);
  auto opStats = toOperatorStats(task->taskStats());
  // TODO (Ebe): Fix this
  // ASSERT_GT(
  //     opStats.at("HashProbe").runtimeStats[Operator::kSpillWrites].sum, 0);

  // ASSERT_GT(stats.spilledBytes, 0);
  // ASSERT_GT(stats.spilledRows, 0);
  // ASSERT_GT(stats.spilledFiles, 0);
  // ASSERT_GT(stats.spilledPartitions, 0);

  assertEqualResults({result}, {traceResult});

  const auto taskId = task->taskId();
  const auto replayingResult = HashJoinReplayer(
                                   traceRoot,
                                   task->queryCtx()->queryId(),
                                   task->taskId(),
                                   traceNodeId_,
                                   "HashJoin",
                                   "",
                                   0,
                                   executor_.get())
                                   .run();
  assertEqualResults({result}, {replayingResult});
}
} // namespace bytedance::bolt::tool::trace::test
