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
#include "bolt/common/testutil/TempDirectoryPath.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;
namespace bytedance::bolt::exec {

namespace {

class SortAndWindowTest : public OperatorTestBase {
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

  void runSortAndWindowTest(
      const std::vector<TestCase>& testCases,
      bool spillEnabled,
      int spillInjectionProbability,
      bool hasSpill) {
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(testCase.windowFunction);

      auto data = makeRowVector(
          {"p", "s", "d"},
          {testCase.partitionKey, testCase.sortKey, testCase.payload});

      createDuckDbTable({data});

      core::PlanNodeId windowId;
      core::PlanNodeId orderByNodeId;

      auto plan = PlanBuilder()
                      .values(split(data, 10))
                      .orderBy({"p", "s"}, false)
                      .capturePlanNodeId(orderByNodeId)
                      .streamingWindow({testCase.windowFunction})
                      .capturePlanNodeId(windowId)
                      .planNode();

      auto spillDirectory = TempDirectoryPath::create();
      TestScopedSpillInjection scopedSpillInjection(spillInjectionProbability);
      auto task =
          AssertQueryBuilder(plan, duckDbQueryRunner_)
              .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
              .config(
                  core::QueryConfig::kSpillEnabled,
                  spillEnabled ? "true" : "false")
              .config(core::QueryConfig::kRowBasedSpillMode, "compression")
              .config(core::QueryConfig::kJitLevel, "-1")
              .config(core::QueryConfig::kSpillEnabled, "true")
              .config(
                  core::QueryConfig::kOrderBySpillEnabled,
                  spillEnabled ? "true" : "false")
              .spillDirectory(spillDirectory->path)
              .assertResults(fmt::format(
                  "SELECT *, {} over (partition by p order by s) FROM tmp",
                  testCase.duckDBWindowFunction));

      auto taskStats = exec::toPlanStats(task->taskStats());

      const auto& stats = taskStats.at(orderByNodeId);

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

TEST_F(SortAndWindowTest, basic) {
  constexpr vector_size_t size = 2000;
  const auto testCases = std::vector<TestCase>{
      {// basic
       .payload = makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       .partitionKey =
           makeFlatVector<int16_t>(size, [](auto row) { return row % 11; }),
       .sortKey = makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       .windowFunction = "sum(d) over (partition by p order by s)",
       .duckDBWindowFunction = "sum(d)"},
      {// P_int64_S_string
       .payload = makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       .partitionKey =
           makeFlatVector<int64_t>(size, [](auto row) { return row % 11; }),
       .sortKey = makeFlatVector<std::string>(
           size, [](auto row) { return std::string(1001, 'a' + (row % 26)); }),
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
       .sortKey = makeFlatVector<int64_t>(
           size, [](auto row) { return row * (row % 3 - 1); }),
       .windowFunction = "sum(d) over (partition by p order by s)",
       .duckDBWindowFunction = "sum(d)"},
  };
  runSortAndWindowTest(testCases, true, 100, true);
  runSortAndWindowTest(testCases, false, 0, false);
}

TEST_F(SortAndWindowTest, multiType) {
  constexpr vector_size_t size = 1000;
  const auto testCases = std::vector<TestCase>{
      {.payload = makeFlatVector<std::string>(
           size, [](auto row) { return std::string(1001, 'a' + (row % 26)); }),
       .partitionKey = makeFlatVector<std::string>(
           size, [](auto row) { return fmt::format("P{:03d}", row % 33); }),
       .sortKey =
           makeFlatVector<double>(size, [](auto row) { return (row)*0.618; }),
       .windowFunction = "count(d) over (partition by p order by s)",
       .duckDBWindowFunction = "count(d)"},
      {.payload =
           makeFlatVector<bool>(size, [](auto row) { return row % 3 == 0; }),
       .partitionKey = makeFlatVector<int16_t>(
           size, [](auto row) { return (row % 1000) * (row % 3 - 1); }),
       .sortKey = makeFlatVector<Timestamp>(
           size,
           [](auto row) {
             return Timestamp(
                 1609459200 + (row % 1000) * 3600, (row % 1000) * 1000);
           }),
       .windowFunction = "count(d) over (partition by p order by s)",
       .duckDBWindowFunction = "count(d)"},

      {.payload =
           makeFlatVector<double>(size, [](auto row) { return row * 2; }),
       .partitionKey = makeFlatVector<int32_t>(
           size, [](auto row) { return (row % 100) * (row % 7); }),
       .sortKey = makeFlatVector<float>(
           size,
           [](auto row) { return (row % 2 == 0 ? -1 : 1) * (row % 1000); }),
       .windowFunction = "sum(d) over (partition by p order by s)",
       .duckDBWindowFunction = "sum(d)"},

      {.payload =
           makeFlatVector<int64_t>(size, [](auto row) { return row * 1000; }),
       .partitionKey = makeFlatVector<std::string>(
           size, [](auto row) { return fmt::format("P{:04d}", row % 500); }),
       .sortKey = makeFlatVector<int32_t>(
           size, [](auto row) { return (row % 100) * (row % 3 - 1); }),
       .windowFunction = "avg(d) over (partition by p order by s)",
       .duckDBWindowFunction = "avg(d)"},

      {.payload = makeFlatVector<int8_t>(
           size, [](auto row) { return row % 2 == 0 ? INT8_MAX : INT8_MIN; }),
       .partitionKey = makeFlatVector<double>(
           size,
           [](auto row) {
             return row % 10 == 0 ? std::numeric_limits<double>::infinity()
                                  : row * 0.314;
           }),
       .sortKey = makeFlatVector<int64_t>(
           size, [](auto row) { return row * (row % 5 - 2); }),
       .windowFunction = "max(d) over (partition by p order by s)",
       .duckDBWindowFunction = "max(d)"}};

  runSortAndWindowTest(testCases, true, 100, true);
  runSortAndWindowTest(testCases, false, 0, false);
}

TEST_F(SortAndWindowTest, arrayType) {
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
      {.payload = makeNullableArrayVector<int64_t>(
           {{1, 2, 3},
            {std::nullopt, 5},
            {2, std::nullopt},
            {6, 7, std::nullopt, 9},
            {},
            {0, -1},
            {std::nullopt},
            {2, 4, 6},
            {3, 1, std::nullopt},
            {9, 8, 7},
            {1000, std::nullopt},
            {5, 5, 5},
            {12, 34, std::nullopt},
            {7},
            {std::nullopt, std::nullopt},
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

  runSortAndWindowTest({testCases[0]}, false, 0, false);
  runSortAndWindowTest({testCases[0]}, true, 100, true);
}

TEST_F(SortAndWindowTest, complexAggregates) {
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

  runSortAndWindowTest(testCases, true, 100, true);
  runSortAndWindowTest(testCases, false, 0, false);
}

TEST_F(SortAndWindowTest, rankFilter) {
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

TEST_F(SortAndWindowTest, rankFilterWithoutPartitionKey) {
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
              "SELECT * FROM (SELECT *, RANK() OVER(ORDER BY score DESC) AS rnk FROM tmp) where rnk <= 10 ORDER BY rnk ");
}

} // namespace

} // namespace bytedance::bolt::exec
