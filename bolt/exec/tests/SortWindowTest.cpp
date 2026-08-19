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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/Window.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::exec {

namespace {

class SortWindowTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
    filesystems::registerLocalFileSystem();
  }

  struct TestCase {
    VectorPtr payload;
    VectorPtr partitionKey;
    VectorPtr sortKey;
    std::string windowFunction;
    std::string duckDBWindowFunction;
  };

  void runSortWindowTest(
      const std::vector<TestCase>& testCases,
      bool spillEnabled,
      int spillInjectionProbability,
      bool hasSpill) {
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(testCase.windowFunction);

      auto data = makeRowVector(
          {"d", "p", "s"},
          {testCase.payload, testCase.partitionKey, testCase.sortKey});

      createDuckDbTable({data});

      core::PlanNodeId windowId;
      auto plan = PlanBuilder()
                      .values(split(data, 10))
                      .window({testCase.windowFunction})
                      .capturePlanNodeId(windowId)
                      .planNode();
      auto spillDirectory = TempDirectoryPath::create();
      TestScopedSpillInjection scopedSpillInjection(spillInjectionProbability);
      bytedance::bolt::exec::TestWindowInjection windowInjection(
          WindowBuildType::kSortWindowBuild);
      auto task =
          AssertQueryBuilder(plan, duckDbQueryRunner_)
              .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
              .config(
                  core::QueryConfig::kSpillEnabled,
                  spillEnabled ? "true" : "false")
              .config(
                  core::QueryConfig::kWindowSpillEnabled,
                  spillEnabled ? "true" : "false")
              .config(core::QueryConfig::kRowBasedSpillMode, "compression")
              .config(core::QueryConfig::kJitLevel, "-1")
              .spillDirectory(spillDirectory->path)
              .assertResults(fmt::format(
                  "SELECT *, {} over (partition by p order by s) FROM tmp",
                  testCase.duckDBWindowFunction));

      auto taskStats = exec::toPlanStats(task->taskStats());

      const auto& stats = taskStats.at(windowId);

      if (hasSpill) {
        ASSERT_GT(stats.spilledBytes, 0);
        ASSERT_GT(stats.spilledRows, 0);
        ASSERT_GT(stats.spilledFiles, 0);
        ASSERT_GT(stats.spilledPartitions, 0);
      } else {
        ASSERT_EQ(stats.spilledBytes, 0);
        ASSERT_EQ(stats.spilledRows, 0);
        ASSERT_EQ(stats.spilledFiles, 0);
        ASSERT_EQ(stats.spilledPartitions, 0);
      }
    }
  }
};

TEST_F(SortWindowTest, basic) {
  constexpr vector_size_t size = 1000;
  const auto testCases = std::vector<TestCase>{
      {// basic
       .payload = makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       .partitionKey =
           makeFlatVector<int16_t>(size, [](auto row) { return row % 11; }),
       .sortKey = makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       .windowFunction = "sum(d) over (partition by p order by s)",
       .duckDBWindowFunction = "sum(d)"},
      {// spill_with_long_strings
       .payload = makeFlatVector<std::string>(
           size, [](auto row) { return std::string(1001, 'a' + (row % 26)); }),
       .partitionKey =
           makeFlatVector<int16_t>(size, [](auto row) { return row % 11; }),
       .sortKey = makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       .windowFunction = "count(d) over (partition by p order by s)",
       .duckDBWindowFunction = "count(d)"},
      {// spill_with_empty_strings
       .payload = makeFlatVector<std::string>(
           size,
           [](auto row) {
             return row % 2 == 0 ? "" : std::string(100, 'a' + (row % 26));
           }),
       .partitionKey =
           makeFlatVector<int16_t>(size, [](auto row) { return row % 11; }),
       .sortKey = makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       .windowFunction = "count(d) over (partition by p order by s)",
       .duckDBWindowFunction = "count(d)"},
      {// Integer multi-value partitionKey + randomized sortKey
       .payload = makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       .partitionKey =
           makeFlatVector<int32_t>(size, [](auto row) { return row % 23; }),
       //  makeFlatVector<int32_t>(size, [](auto row) { return row % 2; }),
       .sortKey = makeFlatVector<int64_t>(
           size, [](auto row) { return row * (row % 3 - 1); }),
       .windowFunction = "sum(d) over (partition by p order by s)",
       .duckDBWindowFunction = "sum(d)"},
  }; // namespace
  runSortWindowTest(testCases, true, 100, true);
  runSortWindowTest(testCases, false, 0, false);
}

TEST_F(SortWindowTest, multiType) {
  constexpr vector_size_t size = 1000;
  const auto testCases = std::vector<TestCase>{
      {// String partition key + float sort key
       .payload = makeFlatVector<std::string>(
           size, [](auto row) { return std::string(1001, 'a' + (row % 26)); }),
       .partitionKey = makeFlatVector<std::string>(
           size, [](auto row) { return fmt::format("P{:03d}", row % 33); }),
       .sortKey =
           makeFlatVector<double>(size, [](auto row) { return (row)*0.618; }),
       .windowFunction = "count(d) over (partition by p order by s)",
       .duckDBWindowFunction = "count(d)"},
      {// Mixed type test : Boolean payload + timestamp sort key
       .payload = makeFlatVector<bool>(
           size, [](auto row) { return row % 3 == 0; }, nullEvery(7)),
       .partitionKey = makeFlatVector<int16_t>(
           size,
           [](auto row) { return (row % 1000) * (row % 3 - 1); },
           nullEvery(13)),
       .sortKey = makeFlatVector<Timestamp>(
           size,
           [](auto row) {
             return Timestamp(
                 1609459200 + (row % 1000) * 3600, (row % 1000) * 1000);
           }),
       .windowFunction = "count(d) over (partition by p order by s)",
       .duckDBWindowFunction = "count(d)"},

      {// Float payload + float sort
       .payload = makeFlatVector<double>(
           size, [](auto row) { return row * 2; }, nullEvery(5)),
       .partitionKey = makeFlatVector<int32_t>(
           size,
           [](auto row) { return (row % 100) * (row % 7); }, // 大范围分区
           nullEvery(11)),
       .sortKey = makeFlatVector<float>(
           size,
           [](auto row) { return (row % 2 == 0 ? -1 : 1) * (row % 1000); }),
       .windowFunction = "sum(d) over (partition by p order by s)",
       .duckDBWindowFunction = "sum(d)"},

      {//  Large integer payload + sparse partitioning
       .payload = makeFlatVector<int64_t>(
           size,
           [](auto row) { return row * 1000; },
           [](auto row) { return row % 13 == 0; }),
       .partitionKey = makeFlatVector<std::string>(
           size, [](auto row) { return fmt::format("P{:04d}", row % 500); }),
       .sortKey = makeFlatVector<int32_t>(
           size,
           [](auto row) { return (row % 100) * (row % 3 - 1); },
           nullEvery(17)),
       .windowFunction = "avg(d) over (partition by p order by s)",
       .duckDBWindowFunction = "avg(d)"},

      {// Extreme value test
       .payload = makeFlatVector<int8_t>(
           size,
           [](auto row) { return row % 2 == 0 ? INT8_MAX : INT8_MIN; },
           nullEvery(9)),
       .partitionKey = makeFlatVector<double>(
           size,
           [](auto row) {
             return row % 10 == 0 ? std::numeric_limits<double>::infinity()
                                  : row * 0.314;
           }),
       .sortKey = makeFlatVector<int64_t>(
           size, [](auto row) { return row * (row % 5 - 2); }, nullEvery(19)),
       .windowFunction = "max(d) over (partition by p order by s)",
       .duckDBWindowFunction = "max(d)"}};

  runSortWindowTest(testCases, true, 100, true);
  runSortWindowTest(testCases, false, 0, false);
}

TEST_F(SortWindowTest, arrayType) {
  const auto testCases = std::vector<TestCase>{
      {.payload = makeArrayVector<int64_t>(
           {{1, 2, 3},    {5},       {},        {6, 7, 8, 9}, {10},
            {0, -1},      {100},     {2, 4, 6}, {3, 1},       {9, 8, 7},
            {1000},       {5, 5, 5}, {12, 34},  {7},          {8, 9, 10},
            {11, 22, 33}, {-5, -10}, {0},       {999, 888},   {4, 3, 2, 1}}),
       .partitionKey = makeFlatVector<int16_t>(
           {1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7}),
       .sortKey =
           makeFlatVector<int32_t>({1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                    11, 12, 13, 14, 15, 16, 17, 18, 19, 20}),
       .windowFunction = "min(d) over (partition by p order by s)",
       .duckDBWindowFunction = "min(d)"},
      {// array with null
       .payload = makeNullableArrayVector<int64_t>(
           {{1, 2, 3}, // 正常
            {std::nullopt, 5}, // 首元素null
            {2, std::nullopt}, // 中间元素null
            {6, 7, std::nullopt, 9}, // 末尾null
            {}, // 空数组
            {0, -1},
            {std::nullopt}, // 单个null元素
            {2, 4, 6},
            {3, 1, std::nullopt},
            {9, 8, 7},
            {1000, std::nullopt},
            {5, 5, 5},
            {12, 34, std::nullopt},
            {7},
            {std::nullopt, std::nullopt}, // 双null
            {11, 22, 33},
            {-5, -10, std::nullopt},
            {0},
            {999, 888},
            {4, 3, 2, 1}}),
       .partitionKey = makeFlatVector<int16_t>(
           {1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7}),
       .sortKey =
           makeFlatVector<int32_t>({1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                    11, 12, 13, 14, 15, 16, 17, 18, 19, 20}),
       .windowFunction = "max(d) over (partition by p order by s)",
       .duckDBWindowFunction = "max(d)"}};

  runSortWindowTest({testCases[0]}, false, 0, false);
  runSortWindowTest({testCases[0]}, true, 100, true);
}

TEST_F(SortWindowTest, complexAggregates) {
  constexpr vector_size_t size = 100;
  const auto testCases = std::vector<TestCase>{
      {.payload = makeFlatVector<std::string>(
           size, [](auto row) { return fmt::format("str{:03d}", row % 20); }),
       .partitionKey =
           makeFlatVector<int32_t>(size, [](auto row) { return row % 4; }),
       .sortKey = makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       .windowFunction = "last_value(d) over (partition by p order by s)",
       .duckDBWindowFunction = "last_value(d)"},
  };

  runSortWindowTest(testCases, true, 100, true);
  runSortWindowTest(testCases, false, 0, false);
}

TEST_F(SortWindowTest, rankFilter) {
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

  bytedance::bolt::exec::TestWindowInjection windowInjection(
      WindowBuildType::kSortWindowBuild);

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

TEST_F(SortWindowTest, rankFilterWithoutPartitionKey) {
  auto input = {makeRowVector(
      {"user_id", "score"},
      {
          makeFlatVector<int64_t>(
              {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}),
          makeFlatVector<int64_t>(
              {10, 7, 4, 2, 1, 9, 8, 11, 14, 6, 7, 3, 12, 15, 5}),
      })};

  createDuckDbTable(input);

  bytedance::bolt::exec::TestWindowInjection windowInjection(
      WindowBuildType::kSortWindowBuild);

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
              "SELECT * FROM (SELECT *, RANK() OVER(ORDER BY score DESC) AS rnk FROM tmp) where rnk <= 10 ORDER BY rnk ");
}

TEST_F(SortWindowTest, rankFilterReclaimKeepsTopRowsValid) {
  constexpr vector_size_t size = 100;
  auto data = makeRowVector(
      {"user_id", "score"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row % 10; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      });
  createDuckDbTable({data});
  auto spillDirectory = TempDirectoryPath::create();

  auto plan =
      PlanBuilder()
          .values(split(data, 10))
          .window(
              {"RANK() OVER(PARTITION BY user_id ORDER BY score DESC) AS rnk"},
              1)
          .filter("rnk <= 1")
          .planNode();
  auto windowPlan = plan->sources().front();
  auto windowNode =
      std::dynamic_pointer_cast<const core::WindowNode>(windowPlan);
  core::PlanFragment planFragment;
  planFragment.planNode = windowPlan;
  auto queryCtx = core::QueryCtx::create(
      nullptr,
      core::QueryConfig(
          {{core::QueryConfig::kSpillEnabled, "true"},
           {core::QueryConfig::kWindowSpillEnabled, "true"}}));
  auto task = Task::create(
      "rankFilterIsNotReclaimable",
      std::move(planFragment),
      0,
      queryCtx,
      Task::ExecutionMode::kParallel);
  task->setSpillDirectory(spillDirectory->path);
  DriverCtx driverCtx(task, 0, 0, 0, 0);

  auto regularWindowNode = std::dynamic_pointer_cast<const core::WindowNode>(
      PlanBuilder()
          .values(split(data, 10))
          .window(
              {"RANK() OVER(PARTITION BY user_id ORDER BY score DESC) AS rnk"})
          .planNode());
  core::PlanFragment regularPlanFragment;
  regularPlanFragment.planNode = regularWindowNode;
  auto regularTask = Task::create(
      "regularWindowIsReclaimable",
      std::move(regularPlanFragment),
      0,
      core::QueryCtx::create(
          nullptr,
          core::QueryConfig(
              {{core::QueryConfig::kSpillEnabled, "true"},
               {core::QueryConfig::kWindowSpillEnabled, "true"}})),
      Task::ExecutionMode::kParallel);
  regularTask->setSpillDirectory(spillDirectory->path);
  DriverCtx regularDriverCtx(regularTask, 0, 0, 0, 0);
  Window regularWindow(0, &regularDriverCtx, regularWindowNode);
  Window rankLimitWindow(0, &driverCtx, windowNode);
  ASSERT_TRUE(regularWindow.canReclaim());
  ASSERT_TRUE(rankLimitWindow.canReclaim());

  rankLimitWindow.addInput(split(data, 10)[0]);
  memory::MemoryReclaimer::Stats stats;
  rankLimitWindow.reclaim(0, stats);
  ASSERT_TRUE(rankLimitWindow.needsInput());
  rankLimitWindow.addInput(split(data, 10)[1]);

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kSpillEnabled, true)
      .config(core::QueryConfig::kWindowSpillEnabled, true)
      .spillDirectory(spillDirectory->path)
      .assertResults(
          "SELECT * FROM (SELECT *, RANK() OVER(PARTITION BY user_id ORDER BY score DESC) AS rnk FROM tmp) WHERE rnk <= 1");
}

} // namespace

} // namespace bytedance::bolt::exec
