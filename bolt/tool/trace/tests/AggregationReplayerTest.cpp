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

#include <algorithm>
#include <memory>
#include <string>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/hyperloglog/SparseHll.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/HiveDataSink.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/dwio/dwrf/writer/Writer.h"
#include "bolt/exec/PartitionFunction.h"
#include "bolt/exec/TableWriter.h"
#include "bolt/exec/TraceUtil.h"
#include "bolt/exec/tests/utils/ArbitratorTestUtil.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/tool/trace/AggregationReplayer.h"
#include "bolt/tool/trace/TableWriterReplayer.h"
#include "bolt/tool/trace/TraceReplayRunner.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
#include "folly/experimental/EventCount.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::core;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::connector;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::common::testutil;
using namespace bytedance::bolt::common::hll;
namespace bytedance::bolt::tool::trace::test {
class AggregationReplayerTest : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
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
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();
  }

  struct PlanWithName {
    const std::string name;
    const core::PlanNodePtr plan;

    PlanWithName(std::string _name, core::PlanNodePtr _plan)
        : name(std::move(_name)), plan(std::move(_plan)) {}
  };

  std::vector<TypePtr> generateKeyTypes(int32_t numKeys) {
    std::vector<TypePtr> types;
    types.reserve(numKeys);
    for (auto i = 0; i < numKeys; ++i) {
      types.push_back(vectorFuzzer_.randType(0 /*maxDepth*/, false));
    }
    return types;
  }

  std::vector<RowVectorPtr> generateInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes) {
    std::vector<std::string> names = keyNames;
    std::vector<TypePtr> types = keyTypes;

    // Add up to 3 payload columns.
    const auto numPayload = randInt(1, 3);
    for (auto i = 0; i < numPayload; ++i) {
      names.push_back(fmt::format("c{}", i + keyNames.size()));
      types.push_back(vectorFuzzer_.randType(2 /*maxDepth*/, false));
    }

    const auto inputType = ROW(std::move(names), std::move(types));
    std::vector<RowVectorPtr> input;
    for (auto i = 0; i < 10; ++i) {
      input.push_back(vectorFuzzer_.fuzzInputRow(inputType));
    }
    return input;
  }

  std::vector<std::string> makeNames(const std::string& prefix, size_t n) {
    std::vector<std::string> names;
    names.reserve(n);
    for (auto i = 0; i < n; ++i) {
      names.push_back(fmt::format("{}{}", prefix, i));
    }
    return names;
  }

  std::vector<PlanWithName> aggregatePlans(
      const RowTypePtr& rowType,
      const std::string& prefix = "") {
    const std::vector<std::string> aggregates{
        fmt::format("{}count(1)", prefix),
        fmt::format("{}min(c1)", prefix),
        fmt::format("{}count(c2),", prefix)};
    std::vector<PlanWithName> plans;
    // Single aggregation plan.
    plans.emplace_back(
        "Single",
        PlanBuilder()
            .tableScan(rowType)
            .singleAggregation(groupingKeys_, aggregates, {})
            .capturePlanNodeId(traceNodeId_)
            .planNode());
    // Partial -> final aggregation plan.
    plans.emplace_back(
        "Partial-Final",
        PlanBuilder()
            .tableScan(rowType)
            .partialAggregation(groupingKeys_, aggregates, {})
            .capturePlanNodeId(traceNodeId_)
            .finalAggregation()
            .planNode());
    // Partial -> intermediate -> final aggregation plan.
    plans.emplace_back(
        "Partial-Intermediate-Final",
        PlanBuilder()
            .tableScan(rowType)
            .partialAggregation(groupingKeys_, aggregates, {})
            .capturePlanNodeId(traceNodeId_)
            .intermediateAggregation()
            .finalAggregation()
            .planNode());
    return plans;
  }

  int32_t randInt(int32_t min, int32_t max) {
    return boost::random::uniform_int_distribution<int32_t>(min, max)(rng_);
  }

  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = 1000;
    opts.stringVariableLength = true;
    opts.stringLength = 100;
    opts.nullRatio = 0.2;
    return opts;
  }

  core::PlanNodeId traceNodeId_;
  VectorFuzzer vectorFuzzer_{getFuzzerOptions(), pool()};
  std::mt19937 rng_;
  const std::vector<TypePtr> keyTypes_{generateKeyTypes(2)};
  const std::vector<std::string> groupingKeys_{
      makeNames("c", keyTypes_.size())};
};

TEST_F(AggregationReplayerTest, test) {
  for (const auto& prefix : std::vector<std::string>{"", "test."}) {
    const auto data = generateInput(groupingKeys_, keyTypes_);
    const auto planWithNames =
        aggregatePlans(asRowType(data[0]->type()), prefix);
    const auto sourceFilePath = TempFilePath::create();
    writeToFile(sourceFilePath->getPath(), data);

    if (!prefix.empty()) {
      functions::prestosql::registerAllScalarFunctions(prefix);
      aggregate::prestosql::registerAllAggregateFunctions(prefix);
      FLAGS_function_prefix = prefix;
    }

    for (const auto& planWithName : planWithNames) {
      SCOPED_TRACE(planWithName.name);
      const auto& plan = planWithName.plan;
      const auto testDir = TempDirectoryPath::create();
      const auto traceRoot =
          fmt::format("{}/{}", testDir->getPath(), "traceRoot");
      std::shared_ptr<Task> task;
      auto results =
          AssertQueryBuilder(plan)
              .config(core::QueryConfig::kQueryTraceEnabled, true)
              .config(core::QueryConfig::kQueryTraceDir, traceRoot)
              .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
              .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
              .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_)
              .split(makeHiveConnectorSplit(sourceFilePath->getPath()))
              .copyResults(pool(), task);

      const auto replayingResult = AggregationReplayer(
                                       traceRoot,
                                       task->queryCtx()->queryId(),
                                       task->taskId(),
                                       traceNodeId_,
                                       "Aggregation",
                                       "",
                                       0,
                                       executor_.get())
                                       .run();
      assertEqualResults({results}, {replayingResult});

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
  }
}
} // namespace bytedance::bolt::tool::trace::test
