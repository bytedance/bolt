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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::exec {

namespace {

class WindowTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
    filesystems::registerLocalFileSystem();
  }
};

TEST_F(WindowTest, spill) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Payload.
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Partition key.
          makeFlatVector<int16_t>(size, [](auto row) { return row % 11; }),
          // Sorting key.
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values(split(data, 10))
                  .window({"row_number() over (partition by p order by s)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->path)
          .assertResults(
              "SELECT *, row_number() over (partition by p order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 0);
  ASSERT_GT(stats.spilledPartitions, 0);
}

TEST_F(WindowTest, missingFunctionSignature) {
  auto input = {makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<std::string>({"A", "B", "C"}),
      makeFlatVector<int64_t>({10, 20, 30}),
  })};

  auto runWindow = [&](const core::CallTypedExprPtr& callExpr) {
    core::WindowNode::Frame frame{
        core::WindowNode::WindowType::kRows,
        core::WindowNode::BoundType::kUnboundedPreceding,
        nullptr,
        core::WindowNode::BoundType::kUnboundedFollowing,
        nullptr};

    core::WindowNode::Function windowFunction{callExpr, frame, false};

    CursorParameters params;
    params.planNode =
        PlanBuilder()
            .values(input)
            .addNode([&](auto nodeId, auto source) -> core::PlanNodePtr {
              return std::make_shared<core::WindowNode>(
                  nodeId,
                  std::vector<core::FieldAccessTypedExprPtr>{
                      std::make_shared<core::FieldAccessTypedExpr>(
                          BIGINT(), "c0")},
                  std::vector<core::FieldAccessTypedExprPtr>{}, // sortingKeys
                  std::vector<core::SortOrder>{}, // sortingOrders
                  std::vector<std::string>{"w"},
                  std::vector<core::WindowNode::Function>{windowFunction},
                  false,
                  0,
                  source);
            })
            .planNode();

    readCursor(params, [](auto*) {});
  };

  auto callExpr = std::make_shared<core::CallTypedExpr>(
      BIGINT(),
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c1")},
      "sum");

  BOLT_ASSERT_THROW(
      runWindow(callExpr),
      "Window function signature is not supported: sum(VARCHAR). Supported signatures:");

  callExpr = std::make_shared<core::CallTypedExpr>(
      VARCHAR(),
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c2")},
      "sum");

  BOLT_ASSERT_THROW(
      runWindow(callExpr),
      "Unexpected return type for window function sum(BIGINT). Expected BIGINT. Got VARCHAR.");
}

TEST_F(WindowTest, duplicateOrOverlappingKeys) {
  auto data = makeRowVector(
      ROW({"a", "b", "c", "d", "e"},
          {
              BIGINT(),
              BIGINT(),
              BIGINT(),
              BIGINT(),
              BIGINT(),
          }),
      10);

  auto plan = [&](const std::vector<std::string>& partitionKeys,
                  const std::vector<std::string>& sortingKeys) {
    std::ostringstream sql;
    sql << "row_number() over (";
    if (!partitionKeys.empty()) {
      sql << " partition by ";
      sql << folly::join(", ", partitionKeys);
    }
    if (!sortingKeys.empty()) {
      sql << " order by ";
      sql << folly::join(", ", sortingKeys);
    }
    sql << ")";

    PlanBuilder().values({data}).window({sql.str()}).planNode();
  };

  BOLT_ASSERT_THROW(
      plan({"a", "a"}, {"b"}),
      "Partitioning keys must be unique. Found duplicate key: a");

  BOLT_ASSERT_THROW(
      plan({"a", "b"}, {"c", "d", "c"}),
      "Sorting keys must be unique and not overlap with partitioning keys. Found duplicate key: c");

  BOLT_ASSERT_THROW(
      plan({"a", "b"}, {"c", "b"}),
      "Sorting keys must be unique and not overlap with partitioning keys. Found duplicate key: b");
}

TEST_F(WindowTest, nagativeFrameArg) {
  const vector_size_t size = 1'000;

  auto sizeAt = [](vector_size_t row) { return row % 5; };
  auto keyAt = [](vector_size_t row) { return row % 11; };
  auto valueAt = [](vector_size_t row) { return row % 13; };
  auto keys = makeArrayVector<float>(size, sizeAt, keyAt);
  auto data = makeRowVector(
      {"c0", "c1", "p0", "p1", "k0", "row_number"},
      {
          // Payload.
          makeFlatVector<float>(size, [](auto row) { return row; }),
          makeFlatVector<float>(size, [](auto row) { return row; }),
          // Partition key.
          keys,
          makeFlatVector<std::string>(
              size, [](auto row) { return fmt::format("{}", row + 20); }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
          // Sorting key.
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  struct {
    std::string fragmentStart;
    std::string fragmentEnd;

    std::string debugString() const {
      if (fragmentStart[0] == '-') {
        return fmt::format(
            "Window frame {} offset must not be negative", fragmentStart);
      } else {
        return fmt::format(
            "Window frame {} offset must not be negative", fragmentEnd);
      }
    }
  } testSettings[] = {
      {"k0", "-1"}, // Negative end
      {"-1", "k0"}, // Negative start
      {"-1", "-3"} // Negative start, negative end
  };
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const auto& startOffset = testData.fragmentStart;
    const auto& endOffset = testData.fragmentEnd;
    auto plan =
        PlanBuilder()
            .values(split(data, 10))
            .window({fmt::format(
                "regr_count(c0, c1) over (partition by p0, p1 order by row_number ROWS between {} PRECEDING and {} FOLLOWING)",
                startOffset,
                endOffset)})
            .planNode();
    BOLT_ASSERT_USER_THROW(
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .assertResults(fmt::format(
                "SELECT *, regr_count(c0, c1) over (partition by p0, p1 order by row_number  ROWS between {} PRECEDING and {} FOLLOWING) from tmp",
                startOffset,
                endOffset)),
        testData.debugString());
  }
}

TEST_F(WindowTest, rankFilter) {
  auto input = {makeRowVector(
      {"user_id",
       "character_uid",
       "anchor_cnt",
       "chat_room_type_cnt",
       "follow_cnt",
       "consume_cnt",
       "consume_diamond",
       "channel_cnt",
       "open_tag_cnt",
       "watch_duration"},
      {
          makeFlatVector<int64_t>(
              {4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669,
               4412429669}),
          makeFlatVector<int64_t>(
              {86404990055,
               100016077620,
               68523029865,
               268735577070183,
               98271619711,
               3689570231654935,
               3061490522526312,
               1660702835088315,
               2357004074495469,
               2786596330944883,
               500974691031467,
               464422839793428,
               3183840738410831,
               2484519257772264,
               1174720669289560}),
          makeFlatVector<int64_t>(
              {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
          makeFlatVector<int64_t>(
              {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
          makeFlatVector<int64_t>(
              {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
          makeFlatVector<int64_t>(
              {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
          makeFlatVector<int64_t>(
              {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
          makeFlatVector<int64_t>(
              {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
          makeFlatVector<int64_t>(
              {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
          makeFlatVector<int64_t>(
              {53, 53, 39, 53, 39, 39, 53, 53, 39, 39, 39, 53, 39, 39, 53}),
      })};

  createDuckDbTable(input);

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(input)
          .window(
              {"RANK() OVER( PARTITION BY user_id ORDER BY consume_diamond DESC, watch_duration DESC, character_uid ASC) AS rnk"},
              10)
          .capturePlanNodeId(windowId)
          .filter("rnk <= 10")
          .orderBy({"rnk"}, false)
          .planNode();

  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "10")
          .assertResults(
              "SELECT * FROM (SELECT *, RANK() OVER( PARTITION BY user_id ORDER BY consume_diamond DESC, watch_duration DESC, character_uid ASC) as rnk FROM tmp) where rnk <= 10 ORDER BY rnk");
}

TEST_F(WindowTest, rankFilterWithoutPartitionKey) {
  auto input = {makeRowVector(
      {"user_id", "score"},
      {
          makeFlatVector<int64_t>(
              {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}),
          makeFlatVector<int64_t>(
              {10, 7, 4, 2, 1, 9, 8, 11, 14, 6, 7, 3, 12, 15, 5}),
      })};

  createDuckDbTable(input);

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values(input)
                  .window({"RANK() OVER(ORDER BY score DESC) AS rnk"}, 10)
                  .capturePlanNodeId(windowId)
                  .filter("rnk <= 10")
                  .orderBy({"rnk"}, false)
                  .planNode();

  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "10")
          .assertResults(
              "SELECT * FROM (SELECT *, RANK() OVER(ORDER BY score DESC) AS rnk FROM tmp) where rnk <= 10 ORDER BY rnk");
}

} // namespace

} // namespace bytedance::bolt::exec
