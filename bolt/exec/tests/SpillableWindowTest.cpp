/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::exec {

namespace {

class SpillableWindowTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
    filesystems::registerLocalFileSystem();
  }

  struct TestCase {
    VectorPtr payload;
    VectorPtr payload2;
    VectorPtr partitionKey;
    VectorPtr sortKey;
    std::string windowFunction;
    std::string duckDBWindowFunction;
  };

  void runSortWindowTest(
      const std::vector<TestCase>& testCases,
      bool spillEnabled,
      int spillInjectionProbability,
      bool hasSpill,
      bool ignorePeer = false) {
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(testCase.windowFunction);

      auto data = makeRowVector(
          {"d", "p", "s"},
          {testCase.payload, testCase.partitionKey, testCase.sortKey});

      createDuckDbTable({data});

      std::string frame;
      frame =
          ("over (partition by p order by s ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)");

      core::PlanNodeId windowId;
      auto spillDirectory = TempDirectoryPath::create();
      TestScopedSpillInjection scopedSpillInjection(spillInjectionProbability);
      bytedance::bolt::exec::TestWindowInjection windowInjection(
          WindowBuildType::kSpillableWindowBuild);

      {
        auto plan = PlanBuilder()
                        .values(split(data, 10))
                        .orderBy({"p NULLS FIRST", "s NULLS FIRST"}, false)
                        .streamingWindow({testCase.windowFunction + frame})
                        .capturePlanNodeId(windowId)
                        .planNode();

        auto task =
            AssertQueryBuilder(plan, duckDbQueryRunner_)
                .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
                .config(core::QueryConfig::kPreferredOutputBatchRows, "1024")
                .config(core::QueryConfig::kMaxOutputBatchRows, "1024")
                .config(
                    core::QueryConfig::kSpillEnabled,
                    spillEnabled ? "true" : "false")
                .config(
                    core::QueryConfig::kWindowSpillEnabled,
                    spillEnabled ? "true" : "false")
                .config(core::QueryConfig::kRowBasedSpillMode, "disable")
                .config(core::QueryConfig::kJitLevel, "-1")
                .config(core::QueryConfig::kTestingSpillPct, "100")
                .spillDirectory(spillDirectory->path)
                .assertResults(fmt::format(
                    "SELECT *, {} {} FROM tmp",
                    testCase.duckDBWindowFunction,
                    frame));
      }

      auto plan = PlanBuilder()
                      .values(split(data, 10))
                      .window({testCase.windowFunction + frame})
                      .capturePlanNodeId(windowId)
                      .planNode();

      auto task =
          AssertQueryBuilder(plan, duckDbQueryRunner_)
              .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
              .config(core::QueryConfig::kPreferredOutputBatchRows, "1024")
              .config(core::QueryConfig::kMaxOutputBatchRows, "1024")
              .config(
                  core::QueryConfig::kSpillEnabled,
                  spillEnabled ? "true" : "false")
              .config(
                  core::QueryConfig::kWindowSpillEnabled,
                  spillEnabled ? "true" : "false")
              .config(core::QueryConfig::kRowBasedSpillMode, "compression")
              .config(core::QueryConfig::kJitLevel, "-1")
              .config(core::QueryConfig::kTestingSpillPct, "100")
              .spillDirectory(spillDirectory->path)
              .assertResults(fmt::format(
                  "SELECT *, {} {} FROM tmp",
                  testCase.duckDBWindowFunction,
                  frame));

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

  void runSortWindowLeadLagTest(
      RowVectorPtr data,
      std::string windowFunction,
      bool spillEnabled,
      int spillInjectionProbability,
      bool hasSpill,
      const char* batchSize) {
    SCOPED_TRACE(windowFunction);

    std::string frame;
    frame = ("over (partition by c2 order by c1)");

    core::PlanNodeId windowId;
    auto spillDirectory = TempDirectoryPath::create();
    TestScopedSpillInjection scopedSpillInjection(spillInjectionProbability);
    bytedance::bolt::exec::TestWindowInjection windowInjection(
        WindowBuildType::kSpillableWindowBuild);

    {
      auto plan = PlanBuilder()
                      .values(split(data, 10))
                      .orderBy({"c2", "c1"}, false)
                      .streamingWindow({windowFunction + frame})
                      .capturePlanNodeId(windowId)
                      .planNode();

      auto task =
          AssertQueryBuilder(plan, duckDbQueryRunner_)
              .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
              .config(core::QueryConfig::kPreferredOutputBatchRows, "1024")
              .config(core::QueryConfig::kMaxOutputBatchRows, batchSize)
              .config(
                  core::QueryConfig::kSpillEnabled,
                  spillEnabled ? "true" : "false")
              .config(
                  core::QueryConfig::kWindowSpillEnabled,
                  spillEnabled ? "true" : "false")
              .config(core::QueryConfig::kRowBasedSpillMode, "disable")
              .config(core::QueryConfig::kJitLevel, "-1")
              .config(core::QueryConfig::kTestingSpillPct, "100")
              .spillDirectory(spillDirectory->path)
              .assertResults(fmt::format(
                  "SELECT *, {} {} FROM tmp", windowFunction, frame));
    }

    auto plan = PlanBuilder()
                    .values(split(data, 10))
                    .window({windowFunction + frame})
                    .capturePlanNodeId(windowId)
                    .planNode();

    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
            .config(core::QueryConfig::kPreferredOutputBatchRows, "1024")
            .config(core::QueryConfig::kMaxOutputBatchRows, batchSize)
            .config(
                core::QueryConfig::kSpillEnabled,
                spillEnabled ? "true" : "false")
            .config(
                core::QueryConfig::kWindowSpillEnabled,
                spillEnabled ? "true" : "false")
            .config(core::QueryConfig::kRowBasedSpillMode, "compression")
            .config(core::QueryConfig::kJitLevel, "-1")
            .config(core::QueryConfig::kTestingSpillPct, "100")
            .spillDirectory(spillDirectory->path)
            .assertResults(
                fmt::format("SELECT *, {} {} FROM tmp", windowFunction, frame));

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
    // }
  }
};

TEST_F(SpillableWindowTest, spillAgg) {
  const vector_size_t size = 4096;
  auto data = makeRowVector(
      {"c1", "c2"},
      {
          // aggregate column.
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // partition key.
          makeFlatVector<int32_t>(
              size, [](auto row) { return row < 2048 ? 1 : 2; }),

      });

  createDuckDbTable({data});
  auto spillDirectory = TempDirectoryPath::create();

  core::PlanNodeId windowId;
  {
    auto plan =
        PlanBuilder()
            .values(split(data, 10))
            .streamingWindow(
                {"sum(c1) over (partition by c2 ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)"})
            .capturePlanNodeId(windowId)
            .planNode();

    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
            .config(core::QueryConfig::kMaxOutputBatchRows, "1024")
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kWindowSpillEnabled, "true")
            .config(core::QueryConfig::kTestingSpillPct, "100")
            .spillDirectory(spillDirectory->path)
            .assertResults(
                "SELECT *, sum(c1) over (partition by c2 ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) FROM tmp ");

    auto taskStats = exec::toPlanStats(task->taskStats());
  }

  TestScopedSpillInjection scopedSpillInjection(100);
  bytedance::bolt::exec::TestWindowInjection windowInjection(
      WindowBuildType::kSpillableWindowBuild);

  auto plan =
      PlanBuilder()
          .values(split(data, 10))
          .window(
              {"sum(c1) over (partition by c2 ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kMaxOutputBatchRows, "100")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .config(core::QueryConfig::kRowBasedSpillMode, "compression")
          .config(core::QueryConfig::kJitLevel, "-1")
          .config(core::QueryConfig::kTestingSpillPct, "100")
          .spillDirectory(spillDirectory->path)
          .assertResults(
              "SELECT *, sum(c1) over (partition by c2 ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 0);
  ASSERT_GT(stats.spilledPartitions, 0);
}

TEST_F(SpillableWindowTest, spillLeadLagWindow) {
  const vector_size_t size = 4096;
  auto data = makeRowVector(
      {"c1", "c2"},
      {
          // aggregate column.
          makeFlatVector<int64_t>(
              size, [](auto row) { return row; }, nullEvery(7)),
          // partition key.
          makeFlatVector<int64_t>(
              size, [](auto row) { return row < 2048 ? 1 : 2; }),
      });

  createDuckDbTable({data});

  for (auto batchSize : {"1", "10", "100", "1000"}) {
    std::vector<std::string> windowFunctions = {
        "lag(c1, 1, c1)",
        "lag(c1, 1, c2)",
        "lag(c1, 1, 9)",
        "lag(17, 1)",
        "lag(3, 1, c1)",
        "lag(3, 1, 6)",
        "lead(c1, 1, c1)",
        "lead(c1, 1, c2)",
        "lead(c1, 1, 9)",
        "lead(17, 1)",
        "lead(3, 1, c1)",
        "lead(3, 1, 6)"};
    for (const auto& windowFunction : windowFunctions) {
      runSortWindowLeadLagTest(
          data, windowFunction, true, 100, true, batchSize);
    }
  }
}

TEST_F(SpillableWindowTest, noneSpillable) {
  const vector_size_t size = 4096;
  auto data = makeRowVector(
      {"c1", "c2"},
      {
          // aggregate column.
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // partition key.
          makeFlatVector<int32_t>(
              size, [](auto row) { return row < 2048 ? 1 : 2; }),
      });

  createDuckDbTable({data});
  std::vector<std::string> sqls{
      "sum(c1) over (partition by c2 ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)",
      "sum(c1) over (partition by c2 ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW)",
      "sum(c1) over (partition by c2 ROWS BETWEEN UNBOUNDED PRECEDING and 1 PRECEDING)",
      "lead(c1, 1) over (partition by c2)",
      "row_number() over (partition by c2 ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)"};

  for (auto i = 0; i < sqls.size() - 1; ++i) {
    for (auto j = i + 1; j < sqls.size(); ++j) {
      core::PlanNodeId windowId;
      auto plan = PlanBuilder()
                      .values(split(data, 10))
                      .window({sqls[i], sqls[j]})
                      .capturePlanNodeId(windowId)
                      .planNode();

      auto spillDirectory = TempDirectoryPath::create();
      auto task =
          AssertQueryBuilder(plan, duckDbQueryRunner_)
              .config(core::QueryConfig::kPreferredOutputBatchBytes, "10")
              .config(core::QueryConfig::kMaxOutputBatchRows, "1")
              .config(core::QueryConfig::kSpillEnabled, "true")
              .config(core::QueryConfig::kWindowSpillEnabled, "true")
              .config(core::QueryConfig::kTestingSpillPct, "20")
              .spillDirectory(spillDirectory->path)
              .assertResults(
                  fmt::format("SELECT *, {}, {} FROM tmp", sqls[i], sqls[j]));

      auto taskStats = exec::toPlanStats(task->taskStats());
      const auto& stats = taskStats.at(windowId);

      ASSERT_EQ(stats.spilledBytes, 0);
      ASSERT_EQ(stats.spilledRows, 0);
      ASSERT_EQ(stats.spilledFiles, 0);
      ASSERT_EQ(stats.spilledPartitions, 0);
    }
  }
}

TEST_F(SpillableWindowTest, basic) {
  constexpr vector_size_t size = 4096;
  const auto testCases = std::vector<TestCase>{
      {// basic
       .payload = makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       .payload2 = nullptr,
       .partitionKey =
           makeFlatVector<int16_t>(size, [](auto row) { return row % 11; }),
       .sortKey = makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       .windowFunction = "sum(d)",
       .duckDBWindowFunction = "sum(d)"},
      {// spill_with_long_strings
       .payload = makeFlatVector<std::string>(
           size, [](auto row) { return std::string(1001, 'a' + (row % 26)); }),
       .payload2 = nullptr,
       .partitionKey =
           makeFlatVector<int16_t>(size, [](auto row) { return row % 11; }),
       .sortKey = makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       .windowFunction = "count(d)",
       .duckDBWindowFunction = "count(d)"},
      {// spill_with_empty_strings
       .payload = makeFlatVector<std::string>(
           size,
           [](auto row) {
             return row % 2 == 0 ? "" : std::string(100, 'a' + (row % 26));
           }),
       .payload2 = nullptr,
       .partitionKey =
           makeFlatVector<int16_t>(size, [](auto row) { return row % 11; }),
       .sortKey = makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       .windowFunction = "count(d)",
       .duckDBWindowFunction = "count(d)"},
      {// Integer multi-value partitionKey + randomized sortKey
       .payload = makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       .payload2 = nullptr,
       .partitionKey =
           makeFlatVector<int32_t>(size, [](auto row) { return row % 23; }),
       .sortKey = makeFlatVector<int64_t>(
           size, [](auto row) { return row * (row % 3 - 1); }),
       .windowFunction = "sum(d)",
       .duckDBWindowFunction = "sum(d)"},
  };
  runSortWindowTest(testCases, true, 100, true);
  runSortWindowTest(testCases, false, 0, false);
}

TEST_F(SpillableWindowTest, multiType) {
  constexpr vector_size_t size = 4096;
  const auto testCases = std::vector<TestCase>{
      {.payload = makeFlatVector<std::string>(
           size, [](auto row) { return std::string(1001, 'a' + (row % 26)); }),
       .payload2 = nullptr,
       .partitionKey = makeFlatVector<std::string>(
           size, [](auto row) { return fmt::format("P{:03d}", row % 33); }),
       .sortKey =
           makeFlatVector<double>(size, [](auto row) { return (row)*0.618; }),
       .windowFunction = "count(d)",
       .duckDBWindowFunction = "count(d)"},
      {.payload = makeFlatVector<bool>(
           size, [](auto row) { return row % 3 == 0; }, nullEvery(7)),
       .payload2 = nullptr,
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
       .windowFunction = "count(d)",
       .duckDBWindowFunction = "count(d)"},

      {.payload = makeFlatVector<double>(
           size, [](auto row) { return row * 2; }, nullEvery(5)),
       .payload2 = nullptr,
       .partitionKey = makeFlatVector<int32_t>(
           size,
           [](auto row) { return (row % 100) * (row % 7); }, //大范围分区
           nullEvery(11)),
       .sortKey = makeFlatVector<float>(
           size,
           [](auto row) { return (row % 2 == 0 ? -1 : 1) * (row % 1000); }),
       .windowFunction = "sum(d)",
       .duckDBWindowFunction = "sum(d)"},

      {.payload = makeFlatVector<int64_t>(
           size,
           [](auto row) { return row * 1000; },
           [](auto row) { return row % 13 == 0; }),
       .payload2 = nullptr,
       .partitionKey = makeFlatVector<std::string>(
           size, [](auto row) { return fmt::format("P{:04d}", row % 500); }),
       .sortKey = makeFlatVector<int32_t>(
           size,
           [](auto row) { return (row % 100) * (row % 3 - 1); },
           nullEvery(17)),
       .windowFunction = "avg(d)",
       .duckDBWindowFunction = "avg(d)"},

      {.payload = makeFlatVector<int8_t>(
           size,
           [](auto row) { return row % 2 == 0 ? INT8_MAX : INT8_MIN; },
           nullEvery(9)),
       .payload2 = nullptr,
       .partitionKey = makeFlatVector<double>(
           size,
           [](auto row) {
             return row % 10 == 0 ? std::numeric_limits<double>::infinity()
                                  : row * 0.314;
           }),
       .sortKey = makeFlatVector<int64_t>(
           size, [](auto row) { return row * (row % 5 - 2); }, nullEvery(19)),
       .windowFunction = "max(d)",
       .duckDBWindowFunction = "max(d)"}};

  runSortWindowTest(testCases, true, 100, true);
  runSortWindowTest(testCases, false, 0, false);
}

TEST_F(SpillableWindowTest, arrayType) {
  const auto testCases = std::vector<TestCase>{
      {.payload = makeArrayVector<int64_t>(
           {{1, 2, 3},    {5},       {},        {6, 7, 8, 9}, {10},
            {0, -1},      {100},     {2, 4, 6}, {3, 1},       {9, 8, 7},
            {1000},       {5, 5, 5}, {12, 34},  {7},          {8, 9, 10},
            {11, 22, 33}, {-5, -10}, {0},       {999, 888},   {4, 3, 2, 1}}),
       .payload2 = nullptr,
       .partitionKey = makeFlatVector<int16_t>(
           {1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7}),
       .sortKey =
           makeFlatVector<int32_t>({1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                    11, 12, 13, 14, 15, 16, 17, 18, 19, 20}),
       .windowFunction = "min(d)",
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
       .payload2 = nullptr,
       .partitionKey = makeFlatVector<int16_t>(
           {1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7}),
       .sortKey =
           makeFlatVector<int32_t>({1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                    11, 12, 13, 14, 15, 16, 17, 18, 19, 20}),
       .windowFunction = "max(d)",
       .duckDBWindowFunction = "max(d)"}};

  // 测试正常数组的情况
  runSortWindowTest({testCases[0]}, false, 0, false);
  runSortWindowTest({testCases[0]}, true, 100, true);
}

TEST_F(SpillableWindowTest, ComplexType) {
  const auto testCases = std::vector<TestCase>{
      {.payload = makeFlatVector<std::string>(
           3, [](auto row) { return std::string(30, 'a' + (row % 26)); }),
       .payload2 = makeArrayVector<int64_t>({{1, 2, 3}, {5}, {6, 7}}),
       .partitionKey = makeFlatVector<int16_t>({1, 1, 2}),
       .sortKey = makeFlatVector<int32_t>({4, 5, 6}),
       .windowFunction = "min(d)",
       .duckDBWindowFunction = "min(d)"}};

  auto data = makeRowVector(
      {"p", "s", "d", "d2"},
      {testCases[0].partitionKey,
       testCases[0].sortKey,
       testCases[0].payload2,
       testCases[0].payload});

  createDuckDbTable({data});

  std::string frame;
  frame =
      ("over (partition by p order by s ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)");

  core::PlanNodeId windowId;
  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  bytedance::bolt::exec::TestWindowInjection windowInjection(
      WindowBuildType::kSpillableWindowBuild);

  auto plan = PlanBuilder()
                  .values(split(data, 3))
                  .window({testCases[0].windowFunction + frame})
                  .capturePlanNodeId(windowId)
                  .planNode();

  auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                  .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
                  .config(core::QueryConfig::kPreferredOutputBatchRows, "1024")
                  .config(core::QueryConfig::kMaxOutputBatchRows, "1024")
                  .config(core::QueryConfig::kSpillEnabled, "true")
                  .config(core::QueryConfig::kWindowSpillEnabled, "true")
                  .config(core::QueryConfig::kRowBasedSpillMode, "compression")
                  .config(core::QueryConfig::kJitLevel, "-1")
                  .config(core::QueryConfig::kTestingSpillPct, "100")
                  .spillDirectory(spillDirectory->path)
                  .assertResults(fmt::format(
                      "SELECT *, {} {} FROM tmp",
                      testCases[0].duckDBWindowFunction,
                      frame));

  auto taskStats = exec::toPlanStats(task->taskStats());

  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 0);
  ASSERT_GT(stats.spilledPartitions, 0);
}

} // namespace
} // namespace bytedance::bolt::exec
