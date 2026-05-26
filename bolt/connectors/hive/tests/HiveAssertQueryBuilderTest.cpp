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

#include "bolt/common/testutil/TempFilePath.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

namespace bytedance::bolt::exec::test {

using connector::hive::HiveConnectorSplitBuilder;

/// Exercises Hive-specific AssertQueryBuilder paths (partition-keyed splits,
/// HiveConnectorSplitBuilder). The connector-agnostic AssertQueryBuilder
/// coverage lives in bolt/exec/tests/AssertQueryBuilderTest.cpp.
class HiveAssertQueryBuilderTest : public HiveConnectorTestBase {};

TEST_F(HiveAssertQueryBuilderTest, hiveSplits) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});

  auto file = ::bytedance::bolt::test::TempFilePath::create();
  writeToFile(file->path, {data});

  // Single leaf node.
  AssertQueryBuilder(
      PlanBuilder().tableScan(asRowType(data->type())).planNode(),
      duckDbQueryRunner_)
      .split(makeHiveConnectorSplit(file->path))
      .assertResults("VALUES (1), (2), (3)");

  // Split with partition key.
  ColumnHandleMap assignments = {
      {"ds", partitionKey("ds", VARCHAR())},
      {"c0", regularColumn("c0", BIGINT())}};

  AssertQueryBuilder(
      PlanBuilder()
          .startTableScan()
          .outputType(ROW({"c0", "ds"}, {INTEGER(), VARCHAR()}))
          .tableHandle(makeTableHandle())
          .assignments(assignments)
          .endTableScan()
          .planNode(),
      duckDbQueryRunner_)
      .split(HiveConnectorSplitBuilder(file->path)
                 .connectorId(kHiveConnectorId)
                 .fileFormat(dwio::common::FileFormat::DWRF)
                 .partitionKey("ds", "2022-05-10")
                 .build())
      .assertResults(
          "VALUES (1, '2022-05-10'), (2, '2022-05-10'), (3, '2022-05-10')");

  // Two leaf nodes.
  auto buildData = makeRowVector({makeFlatVector<int32_t>({2, 3})});
  auto buildFile = ::bytedance::bolt::test::TempFilePath::create();
  writeToFile(buildFile->path, {buildData});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto joinPlan = PlanBuilder(planNodeIdGenerator)
                      .tableScan(asRowType(data->type()))
                      .capturePlanNodeId(probeScanId)
                      .hashJoin(
                          {"c0"},
                          {"b_c0"},
                          PlanBuilder(planNodeIdGenerator)
                              .tableScan(asRowType(data->type()))
                              .capturePlanNodeId(buildScanId)
                              .project({"c0 as b_c0"})
                              .planNode(),
                          "",
                          {"c0", "b_c0"})
                      .singleAggregation({}, {"count(1)"})
                      .planNode();

  AssertQueryBuilder(joinPlan, duckDbQueryRunner_)
      .split(probeScanId, makeHiveConnectorSplit(file->path))
      .split(buildScanId, makeHiveConnectorSplit(buildFile->path))
      .assertResults("SELECT 2");
}

} // namespace bytedance::bolt::exec::test
