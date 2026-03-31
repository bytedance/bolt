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

#include "bolt/core/PlanNode.h"
#include "bolt/exec/NestedLoopJoinBuild.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/VectorTestUtil.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

namespace bytedance::bolt::exec::test {
namespace {

using bytedance::bolt::test::assertEqualVectors;

class NestedLoopJoinTest : public HiveConnectorTestBase {
 protected:
  void setProbeType(const RowTypePtr& probeType) {
    probeType_ = probeType;
  }

  void setBuildType(const RowTypePtr& buildType) {
    buildType_ = buildType;
  }

  void setComparisons(std::vector<std::string> comparisons) {
    comparisons_ = std::move(comparisons);
  }

  void setOutputLayout(std::vector<std::string> outputLayout) {
    outputLayout_ = std::move(outputLayout);
  }

  void setJoinConditionStr(std::string joinConditionStr) {
    joinConditionStr_ = std::move(joinConditionStr);
  }

  void setQueryStr(std::string queryStr) {
    queryStr_ = std::move(queryStr);
  }

  void setJoinTypes(std::vector<core::JoinType> joinTypes) {
    joinTypes_ = std::move(joinTypes);
  }

  template <typename T>
  VectorPtr sequence(vector_size_t size, T start = 0) {
    return makeFlatVector<int32_t>(
        size, [start](auto row) { return start + row; });
  }

  template <typename T>
  VectorPtr lazySequence(vector_size_t size, T start = 0) {
    return vectorMaker_.lazyFlatVector<int32_t>(
        size, [start](auto row) { return start + row; });
  }

  void runSingleAndMultiDriverTest(
      const std::vector<RowVectorPtr>& probeVectors,
      const std::vector<RowVectorPtr>& buildVectors) {
    runTest(probeVectors, buildVectors, 1);
    runTest(probeVectors, buildVectors, 4);
    runTest(
        probeVectors,
        buildVectors,
        4,
        4); // Run with smaller output batch size.
  }

  void runTest(
      const std::vector<RowVectorPtr>& probeVectors,
      const std::vector<RowVectorPtr>& buildVectors,
      int32_t numDrivers,
      size_t preferredOutputBatchSize = 1024) {
    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", buildVectors);
    auto queryCtx = core::QueryCtx::create(executor_.get());

    CursorParameters params;
    params.queryCtx = queryCtx;
    params.queryCtx->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kPreferredOutputBatchRows,
          std::to_string(preferredOutputBatchSize)}});
    params.maxDrivers = numDrivers;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    for (const auto joinType : joinTypes_) {
      for (const auto& comparison : comparisons_) {
        SCOPED_TRACE(fmt::format(
            "maxDrivers:{} joinType:{} comparison:{}",
            std::to_string(numDrivers),
            joinTypeName(joinType),
            comparison));

        params.planNode =
            PlanBuilder(planNodeIdGenerator)
                .values(probeVectors)
                .localPartition({probeKeyName_})
                .nestedLoopJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values(buildVectors)
                        .localPartition({buildKeyName_})
                        .planNode(),
                    fmt::format(fmt::runtime(joinConditionStr_), comparison),
                    outputLayout_,
                    joinType)
                .planNode();

        assertQuery(
            params,
            fmt::format(
                fmt::runtime(queryStr_), joinTypeName(joinType), comparison));
      }
    }
  }

 protected:
  const std::string probeKeyName_{"t0"};
  const std::string buildKeyName_{"u0"};
  RowTypePtr probeType_{ROW({{probeKeyName_, BIGINT()}})};
  RowTypePtr buildType_{ROW({{buildKeyName_, BIGINT()}})};
  std::vector<std::string> comparisons_{"=", "<", "<=", "<>"};
  std::vector<core::JoinType> joinTypes_{
      core::JoinType::kInner,
      core::JoinType::kLeft,
      core::JoinType::kRight,
      core::JoinType::kFull,
  };
  std::vector<std::string> outputLayout_{probeKeyName_, buildKeyName_};
  std::string joinConditionStr_{probeKeyName_ + " {} " + buildKeyName_};
  std::string queryStr_{fmt::format(
      "SELECT {0}, {1} FROM t {{}} JOIN u ON t.{0} {{}} u.{1}",
      probeKeyName_,
      buildKeyName_)};
};

TEST_F(NestedLoopJoinTest, emptyBuildOrProbeWithoutFilter) {
  auto empty = makeRowVector({"u0"}, {makeFlatVector<StringView>({})});
  auto nonEmpty = makeRowVector({"t0"}, {makeFlatVector<StringView>({"foo"})});
  auto expected = makeRowVector({makeFlatVector<StringView>({"foo", "foo"})});

  auto testJoin = [&](const std::vector<RowVectorPtr>& leftVectors,
                      const std::vector<RowVectorPtr>& rightVectors,
                      core::JoinType joinType,
                      const std::vector<std::string>& outputLayout,
                      const VectorPtr& expected) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(leftVectors)
                    .localPartitionRoundRobinRow()
                    .nestedLoopJoin(
                        PlanBuilder(planNodeIdGenerator)
                            .values(rightVectors)
                            .localPartition({})
                            .planNode(),
                        "",
                        outputLayout,
                        joinType)
                    .planNode();
    AssertQueryBuilder builder{plan};
    auto result = builder.copyResults(pool());
    bytedance::bolt::test::assertEqualVectors(expected, result);
  };

  testJoin({nonEmpty}, {empty}, core::JoinType::kLeft, {"t0"}, nonEmpty);
  testJoin(
      {nonEmpty, nonEmpty},
      {empty, empty},
      core::JoinType::kLeft,
      {"t0"},
      expected);
  testJoin({empty}, {nonEmpty}, core::JoinType::kLeft, {"u0"}, empty);

  testJoin({empty}, {nonEmpty}, core::JoinType::kRight, {"t0"}, nonEmpty);
  testJoin(
      {empty, empty},
      {nonEmpty, nonEmpty},
      core::JoinType::kRight,
      {"t0"},
      expected);
  testJoin({nonEmpty}, {empty}, core::JoinType::kRight, {"u0"}, empty);

  testJoin(
      {nonEmpty, nonEmpty},
      {empty, empty},
      core::JoinType::kFull,
      {"t0", "u0"},
      makeRowVector({
          makeFlatVector<StringView>({"foo", "foo"}),
          makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}),
      }));
  testJoin(
      {empty, empty},
      {nonEmpty, nonEmpty},
      core::JoinType::kFull,
      {"u0", "t0"},
      makeRowVector({
          makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}),
          makeFlatVector<StringView>({"foo", "foo"}),
      }));
}

TEST_F(NestedLoopJoinTest, basic) {
  auto probeVectors = makeBatches(20, 5, probeType_, pool_.get());
  auto buildVectors = makeBatches(18, 5, buildType_, pool_.get());
  runSingleAndMultiDriverTest(probeVectors, buildVectors);
}

TEST_F(NestedLoopJoinTest, emptyProbe) {
  auto probeVectors = makeBatches(0, 5, probeType_, pool_.get());
  auto buildVectors = makeBatches(18, 5, buildType_, pool_.get());
  runSingleAndMultiDriverTest(probeVectors, buildVectors);
}

TEST_F(NestedLoopJoinTest, emptyBuild) {
  auto probeVectors = makeBatches(20, 5, probeType_, pool_.get());
  auto buildVectors = makeBatches(0, 5, buildType_, pool_.get());
  runSingleAndMultiDriverTest(probeVectors, buildVectors);
}

TEST_F(NestedLoopJoinTest, basicCrossJoin) {
  auto probeVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({sequence<int32_t>(7, 10 + 100 + 1'000)}),
  };

  auto buildVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({sequence<int32_t>(11, 10 + 100 + 1'000)}),
  };

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  // All x 13. Join output vectors contains multiple probe rows each.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({probeVectors})
                .nestedLoopJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({buildVectors})
                        .filter("c0 < 13")
                        .project({"c0 AS u_c0"})
                        .planNode(),
                    {"c0", "u_c0"})
                .planNode();

  assertQuery(op, "SELECT * FROM t, u WHERE u.c0 < 13");

  // 13 x all. Join output vectors contains single probe row each.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({probeVectors})
           .filter("c0 < 13")
           .nestedLoopJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({buildVectors})
                   .project({"c0 AS u_c0"})
                   .planNode(),
               {"c0", "u_c0"})
           .planNode();

  assertQuery(op, "SELECT * FROM t, u WHERE t.c0 < 13");

  // All x 13. No columns on the build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({probeVectors})
           .nestedLoopJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({vectorMaker_.rowVector(ROW({}, {}), 13)})
                   .planNode(),
               {"c0"})
           .planNode();

  assertQuery(op, "SELECT t.* FROM t, (SELECT * FROM u LIMIT 13) u");

  // 13 x All. No columns on the build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({probeVectors})
           .filter("c0 < 13")
           .nestedLoopJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({vectorMaker_.rowVector(ROW({}, {}), 1121)})
                   .planNode(),
               {"c0"})
           .planNode();

  assertQuery(
      op,
      "SELECT t.* FROM (SELECT * FROM t WHERE c0 < 13) t, (SELECT * FROM u LIMIT 1121) u");

  // Empty build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({probeVectors})
           .nestedLoopJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({buildVectors})
                   .filter("c0 < 0")
                   .project({"c0 AS u_c0"})
                   .planNode(),
               {"c0", "u_c0"})
           .planNode();

  assertQueryReturnsEmptyResult(op);

  // Multi-threaded build side.
  planNodeIdGenerator->reset();
  CursorParameters params;
  params.maxDrivers = 4;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values({probeVectors})
                        .nestedLoopJoin(
                            PlanBuilder(planNodeIdGenerator, pool_.get())
                                .values({buildVectors}, true)
                                .filter("c0 in (10, 17)")
                                .project({"c0 AS u_c0"})
                                .planNode(),
                            {"c0", "u_c0"})
                        .limit(0, 100'000, false)
                        .planNode();

  OperatorTestBase::assertQuery(
      params,
      "SELECT * FROM t, "
      "(SELECT * FROM UNNEST (ARRAY[10, 17, 10, 17, 10, 17, 10, 17])) u");
}

TEST_F(NestedLoopJoinTest, outerJoinWithoutCondition) {
  auto probeVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
  };

  auto buildVectors = {
      makeRowVector({sequence<int32_t>(1, 111)}),
  };

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto testOuterJoin = [&](core::JoinType joinType) {
    auto op = PlanBuilder(planNodeIdGenerator)
                  .values({probeVectors})
                  .nestedLoopJoin(
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildVectors})
                          .project({"c0 AS u_c0"})
                          .planNode(),
                      {"c0", "u_c0"},
                      joinType)
                  .singleAggregation({}, {"count(*)"})
                  .planNode();

    assertQuery(
        op,
        fmt::format(
            "SELECT count(*) FROM t {} join u on 1",
            core::joinTypeName(joinType)));
  };
  testOuterJoin(core::JoinType::kLeft);
  testOuterJoin(core::JoinType::kRight);
  testOuterJoin(core::JoinType::kFull);
}

TEST_F(NestedLoopJoinTest, lazyVectors) {
  auto probeVectors = {
      makeRowVector({lazySequence<int32_t>(10)}),
      makeRowVector({lazySequence<int32_t>(100, 10)}),
      makeRowVector({lazySequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({lazySequence<int32_t>(7, 10 + 100 + 1'000)}),
  };

  auto buildVectors = {
      makeRowVector({lazySequence<int32_t>(10)}),
      makeRowVector({lazySequence<int32_t>(100, 10)}),
      makeRowVector({lazySequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({lazySequence<int32_t>(11, 10 + 100 + 1'000)}),
  };

  createDuckDbTable("t", {makeRowVector({sequence<int32_t>(1117)})});
  createDuckDbTable("u", {makeRowVector({sequence<int32_t>(1121)})});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({probeVectors})
                .nestedLoopJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({buildVectors})
                        .project({"c0 AS u_c0"})
                        .planNode(),
                    "c0+u_c0<100",
                    {"c0", "u_c0"},
                    core::JoinType::kFull)
                .planNode();

  assertQuery(op, "SELECT * FROM t FULL JOIN u ON t.c0 + u.c0 < 100");
}

// Test cross join with a build side that has rows, but no columns.
TEST_F(NestedLoopJoinTest, zeroColumnBuild) {
  auto probeVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({sequence<int32_t>(7, 10 + 100 + 1'000)}),
  };

  auto buildVectors = {
      makeRowVector({sequence<int32_t>(1)}),
      makeRowVector({sequence<int32_t>(4, 1)})};

  createDuckDbTable("t", {probeVectors});

  // Build side has > 1 row.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({probeVectors})
                .nestedLoopJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({buildVectors})
                        .project({})
                        .planNode(),
                    {"c0"})
                .planNode();

  assertQuery(
      op, "SELECT t.* FROM t, (SELECT * FROM UNNEST (ARRAY[0, 1, 2, 3, 4])) u");

  // Build side has exactly 1 row.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({probeVectors})
           .nestedLoopJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({buildVectors})
                   .filter("c0 = 1")
                   .project({})
                   .planNode(),
               {"c0"})
           .planNode();

  assertQuery(op, "SELECT * FROM t");
}

TEST_F(NestedLoopJoinTest, bigintArray) {
  auto probeVectors = makeBatches(100, 5, probeType_, pool_.get());
  auto buildVectors = makeBatches(90, 5, buildType_, pool_.get());
  setComparisons({"="});
  setJoinTypes({core::JoinType::kFull});
  runSingleAndMultiDriverTest(probeVectors, buildVectors);
}

TEST_F(NestedLoopJoinTest, allTypes) {
  RowTypePtr probeType = ROW(
      {{"t0", BIGINT()},
       {"t1", VARCHAR()},
       {"t2", REAL()},
       {"t3", DOUBLE()},
       {"t4", INTEGER()},
       {"t5", SMALLINT()},
       {"t6", TINYINT()}});

  RowTypePtr buildType = ROW(
      {{"u0", BIGINT()},
       {"u1", VARCHAR()},
       {"u2", REAL()},
       {"u3", DOUBLE()},
       {"u4", INTEGER()},
       {"u5", SMALLINT()},
       {"u6", TINYINT()}});

  auto probeVectors = makeBatches(60, 5, probeType, pool_.get());
  auto buildVectors = makeBatches(50, 5, buildType, pool_.get());
  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  setProbeType(probeType);
  setBuildType(buildType);
  setComparisons({"="});
  setJoinConditionStr(
      "t0 {0} u0 AND t1 {0} u1 AND t2 {0} u2 AND t3 {0} u3 AND t4 {0} u4 AND t5 {0} u5 AND t6 {0} u6");
  setQueryStr(
      "SELECT t0, u0 FROM t {0} JOIN u ON t.t0 {1} u0 AND t1 {1} u1 AND t2 {1} u2 AND t3 {1} u3 AND t4 {1} u4 AND t5 {1} u5 AND t6 {1} u6");
  runSingleAndMultiDriverTest(probeVectors, buildVectors);
}

// Ensures output order follows the probe input order for inner and left joins.
TEST_F(NestedLoopJoinTest, outputOrder) {
  auto probeVectors = makeRowVector(
      {"l1", "l2"},
      {
          makeNullableFlatVector<int64_t>({1, 8, 6, std::nullopt, 7, 4}),
          makeFlatVector<StringView>({"a", "b", "c", "d", "e", "f"}),
      });
  auto buildVector1 = makeRowVector(
      {"r1", "r2"},
      {
          makeNullableFlatVector<int64_t>({4, 6, 1}),
          makeFlatVector<StringView>({"z", "x", "y"}),
      });

  auto buildVector2 = makeRowVector(
      {"r1", "r2"},
      {
          makeNullableFlatVector<int64_t>({10, std::nullopt, 6}),
          makeFlatVector<StringView>({"z", "p", "u"}),
      });

  const auto createPlan = [&](core::JoinType joinType) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    return PlanBuilder(planNodeIdGenerator)
        .values({probeVectors})
        .nestedLoopJoin(
            PlanBuilder(planNodeIdGenerator)
                .values({buildVector1, buildVector2})
                .project({"r1", "r2"})
                .planNode(),
            "l1 < r1",
            {"l1", "l2", "r1", "r2"},
            joinType)
        .planNode();
  };

  // Inner.
  auto results = AssertQueryBuilder(createPlan(core::JoinType::kInner))
                     .copyResults(pool());
  auto expectedInner = makeRowVector({
      makeNullableFlatVector<int64_t>({1, 1, 1, 1, 8, 6, 7, 4, 4, 4}),
      makeFlatVector<StringView>(
          {"a", "a", "a", "a", "b", "c", "e", "f", "f", "f"}),
      makeNullableFlatVector<int64_t>({4, 6, 10, 6, 10, 10, 10, 6, 10, 6}),
      makeFlatVector<StringView>(
          {"z", "x", "z", "u", "z", "z", "z", "x", "z", "u"}),
  });
  assertEqualVectors(expectedInner, results);

  // Left.
  results =
      AssertQueryBuilder(createPlan(core::JoinType::kLeft)).copyResults(pool());
  auto expectedLeft = makeRowVector({
      makeNullableFlatVector<int64_t>(
          {1, 1, 1, 1, 8, 6, std::nullopt, 7, 4, 4, 4}),
      makeNullableFlatVector<StringView>(
          {"a", "a", "a", "a", "b", "c", "d", "e", "f", "f", "f"}),
      makeNullableFlatVector<int64_t>(
          {4, 6, 10, 6, 10, 10, std::nullopt, 10, 6, 10, 6}),
      makeNullableFlatVector<StringView>(
          {"z", "x", "z", "u", "z", "z", std::nullopt, "z", "x", "z", "u"}),
  });
  assertEqualVectors(expectedLeft, results);
}

TEST_F(NestedLoopJoinTest, mergeBuildVectors) {
  const std::vector<RowVectorPtr> buildVectors = {
      makeRowVector({makeFlatVector<int64_t>({1, 2})}),
      makeRowVector({makeFlatVector<int64_t>({3, 4})}),
      makeRowVector(
          {makeFlatVector<int64_t>(20, [](auto i) { return 5 + i; })}),
      makeRowVector(
          {makeFlatVector<int64_t>(20, [](auto i) { return 25 + i; })}),
      makeRowVector({makeFlatVector<int64_t>({45, 46})}),
  };
  const std::vector<RowVectorPtr> probeVectors = {
      makeRowVector({makeFlatVector<int64_t>({1, 2})}),
  };
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  CursorParameters params;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values(probeVectors)
                        .nestedLoopJoin(
                            PlanBuilder(planNodeIdGenerator)
                                .values(buildVectors)
                                .project({"c0 as r0"})
                                .planNode(),
                            {"c0", "r0"})
                        .planNode();
  params.queryConfigs[core::QueryConfig::kMaxOutputBatchRows] = "10";
  auto cursor = TaskCursor::create(params);
  // Expect the first 2 build side vectors are merged together since they are
  // under the limit after merge.  Others are left along.
  for (int i = 0; i < 2; ++i) {
    auto makeExpected = [&](vector_size_t size, vector_size_t buildOffset) {
      return makeRowVector({
          makeConstant<int64_t>(1 + i, size),
          makeFlatVector<int64_t>(
              size, [&](auto i) { return buildOffset + i; }),
      });
    };
    ASSERT_TRUE(cursor->moveNext());
    ASSERT_EQ(cursor->current()->size(), 4);
    assertEqualVectors(makeExpected(4, 1), cursor->current());
    ASSERT_TRUE(cursor->moveNext());
    ASSERT_EQ(cursor->current()->size(), 20);
    assertEqualVectors(makeExpected(20, 5), cursor->current());
    ASSERT_TRUE(cursor->moveNext());
    ASSERT_EQ(cursor->current()->size(), 20);
    assertEqualVectors(makeExpected(20, 25), cursor->current());
    ASSERT_TRUE(cursor->moveNext());
    ASSERT_EQ(cursor->current()->size(), 2);
    assertEqualVectors(makeExpected(2, 45), cursor->current());
  }
  ASSERT_FALSE(cursor->moveNext());
  ASSERT_TRUE(waitForTaskCompletion(cursor->task().get()));
}

TEST_F(NestedLoopJoinTest, leftSemiJoinProjectDataValidation) {
  auto probeVectors = makeRowVector({"t0"}, {sequence<int32_t>(5)});

  auto buildVectors = makeRowVector({"u0"}, {sequence<int32_t>(3, 2)});

  auto expected = makeRowVector(
      {"t0", "match"},
      {makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
       makeFlatVector<bool>({false, false, true, true, true})});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({probeVectors})
                .nestedLoopJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({buildVectors})
                        .planNode(),
                    "t0 = u0",
                    {"t0", "match"},
                    core::JoinType::kLeftSemiProject)
                .planNode();

  AssertQueryBuilder builder{op};
  auto result = builder.copyResults(pool());

  assertEqualVectors(expected, result);
}

TEST_F(NestedLoopJoinTest, leftSemiJoinWithNullsAndFilter) {
  auto probeVectors = makeRowVector(
      {"t0"}, {makeNullableFlatVector<int32_t>({0, 1, std::nullopt, 3, 4})});

  auto buildVectors = makeRowVector(
      {"u0"}, {makeNullableFlatVector<int32_t>({3, std::nullopt, 4})});

  auto expected = makeRowVector(
      {"t0", "match"},
      {
          makeNullableFlatVector<int32_t>({0, 1, std::nullopt, 3, 4}),
          makeFlatVector<bool>({false, false, false, true, true}),
      });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({probeVectors})
                .nestedLoopJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({buildVectors})
                        .planNode(),
                    "t0 = u0 AND t0 > 2",
                    {"t0", "match"},
                    core::JoinType::kLeftSemiProject)
                .planNode();

  AssertQueryBuilder builder{op};
  auto result = builder.copyResults(pool());

  assertEqualVectors(expected, result);
}

TEST_F(NestedLoopJoinTest, mergeBuildVectorsOverflow) {
  const std::vector<RowVectorPtr> buildVectors = {
      makeRowVector({makeFlatVector<int64_t>({1, 2})})};
  const std::vector<RowVectorPtr> probeVectors = {
      makeRowVector({makeFlatVector<int64_t>({1, 2})}),
  };
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto planNode = PlanBuilder(planNodeIdGenerator)
                      .values(probeVectors)
                      .nestedLoopJoin(
                          PlanBuilder(planNodeIdGenerator)
                              .values(buildVectors)
                              .project({"c0 as r0"})
                              .planNode(),
                          {"c0", "r0"})
                      .planNode();
  auto joinNode =
      std::dynamic_pointer_cast<const bolt::core::NestedLoopJoinNode>(planNode);
  std::vector<RowVectorPtr> values = {};
  core::PlanFragment fakePlanFragment;
  const core::PlanNodeId id{"0"};
  fakePlanFragment.planNode = std::make_shared<core::ValuesNode>(id, values);

  auto fakeTask = Task::create(
      "NestedLoopJoinTest",
      std::move(fakePlanFragment),
      0,
      core::QueryCtx::create(executor_.get()),
      Task::ExecutionMode::kParallel);
  DriverCtx driverCtx(fakeTask, 0, 0, 0, 0);

  // int32_t 1066768200 + 1108383700 > int32_t.MAX_VALUE
  NestedLoopJoinBuild nestedLoopJoinBuild(1, &driverCtx, joinNode);
  nestedLoopJoinBuild.addInput(makeRowVector({makeConstant(1, 1066768200)}));
  nestedLoopJoinBuild.addInput(makeRowVector({makeConstant(1, 1108383700)}));

  std::vector<RowVectorPtr> mergeResult =
      nestedLoopJoinBuild.mergeDataVectors();
  // Expect the 2 vectors are not merged together since they are
  // over the limit.
  ASSERT_EQ(mergeResult.size(), 2);
}

// Reproduces an OOM scenario where NLJ with wide build-side rows
// (ARRAY<VARCHAR>) causes copyBuildValues() to request huge memory during
// buffer doubling. With the dynamic batch size adjustment in prepareOutput(),
// the outputBatchSize_ is reduced based on avg row size, preventing OOM.
TEST_F(NestedLoopJoinTest, dynamicBatchSizeWithWideBuildRows) {
  // Build side: 4 rows with a large ARRAY<VARCHAR> column.
  // Each array has 200 elements of ~500-byte strings ≈ 100KB per row.
  constexpr int32_t kBuildRows = 4;
  constexpr int32_t kArraySize = 200;
  const std::string kLargeString(500, 'x');

  auto buildArrays = vectorMaker_.arrayVector<StringView>(
      kBuildRows,
      /*sizeAt*/ [&](auto /*row*/) { return kArraySize; },
      /*valueAt*/
      [&](auto /*idx*/) { return StringView(kLargeString); });
  auto buildKeys =
      makeFlatVector<int64_t>(kBuildRows, [](auto row) { return row; });
  auto buildVectors = makeRowVector({"u0", "u1"}, {buildKeys, buildArrays});

  // Probe side: 1000 rows with a key column and a VARCHAR column.
  constexpr int32_t kProbeRows = 1'000;
  const std::string kProbeString(200, 'y');

  auto probeKeys = makeFlatVector<int64_t>(
      kProbeRows, [](auto row) { return row % kBuildRows; });
  auto probeStrings = makeFlatVector<StringView>(
      kProbeRows, [&](auto /*row*/) { return StringView(kProbeString); });
  auto probeVectors = makeRowVector({"t0", "t1"}, {probeKeys, probeStrings});

  // Use a memory pool with a cap small enough to trigger OOM without the fix
  // (~10MB), but large enough to succeed with dynamic batch sizing.
  constexpr int64_t kMemoryCap = 50LL << 20; // 50MB
  auto rootPool =
      memory::memoryManager()->addRootPool("dynamicBatchSizeTest", kMemoryCap);
  auto queryCtx = core::QueryCtx::create(
      executor_.get(),
      core::QueryConfig{{}},
      /*connectorConfigs*/ {},
      /*cache*/ cache::AsyncDataCache::getInstance(),
      std::move(rootPool));

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values({probeVectors})
                  .nestedLoopJoin(
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildVectors})
                          .planNode(),
                      /*joinCondition*/ "t0 = u0",
                      /*outputLayout*/ {"t0", "t1", "u1"},
                      core::JoinType::kInner)
                  .planNode();

  // With dynamic batch sizing, the batch size is reduced before the first
  // batch, preventing the large reallocation.
  auto result = AssertQueryBuilder(plan).queryCtx(queryCtx).copyResults(pool());

  // Verify correctness: each probe row matches exactly 1 build row
  // (t0 = u0 where t0 = row % 4), so output should have kProbeRows rows.
  ASSERT_EQ(result->size(), kProbeRows);
}

// Verifies filter path correctness when build side has a single row.
// The optimization batches all probe rows into one filter evaluation.
TEST_F(NestedLoopJoinTest, filterWithSingleBuildRow) {
  auto probeVectors = {
      makeRowVector({"t0"}, {makeFlatVector<int32_t>({1, 2, 3, 4, 5})}),
  };
  auto buildVectors = {
      makeRowVector({"u0"}, {makeFlatVector<int32_t>(std::vector<int32_t>{3})}),
  };

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  for (auto joinType :
       {core::JoinType::kInner,
        core::JoinType::kLeft,
        core::JoinType::kRight,
        core::JoinType::kFull}) {
    SCOPED_TRACE(core::joinTypeName(joinType));
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({probeVectors})
                    .nestedLoopJoin(
                        PlanBuilder(planNodeIdGenerator)
                            .values({buildVectors})
                            .planNode(),
                        "t0 >= u0",
                        {"t0", "u0"},
                        joinType)
                    .planNode();
    auto duckDbSql = fmt::format(
        "SELECT t0, u0 FROM t {} JOIN u ON t0 >= u0",
        core::joinTypeName(joinType));

    assertQuery(plan, duckDbSql);

    // Small batch size to exercise output-full path in batched mode.
    CursorParameters params;
    params.planNode = plan;
    params.queryCtx = core::QueryCtx::create(executor_.get());
    params.queryCtx->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kPreferredOutputBatchRows, "2"}});
    assertQuery(params, duckDbSql);
  }
}

// Verifies filter path correctness when build side has a single vector with
// multiple rows. The optimization batches multiple probe rows per filter
// evaluation.
TEST_F(NestedLoopJoinTest, filterWithSingleBuildVector) {
  auto probeVectors = {
      makeRowVector({"t0"}, {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6})}),
  };
  auto buildVectors = {
      makeRowVector({"u0"}, {makeFlatVector<int32_t>({2, 4, 6})}),
  };

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  for (auto joinType :
       {core::JoinType::kInner,
        core::JoinType::kLeft,
        core::JoinType::kRight,
        core::JoinType::kFull}) {
    SCOPED_TRACE(core::joinTypeName(joinType));
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({probeVectors})
                    .nestedLoopJoin(
                        PlanBuilder(planNodeIdGenerator)
                            .values({buildVectors})
                            .planNode(),
                        "t0 >= u0",
                        {"t0", "u0"},
                        joinType)
                    .planNode();
    auto duckDbSql = fmt::format(
        "SELECT t0, u0 FROM t {} JOIN u ON t0 >= u0",
        core::joinTypeName(joinType));

    assertQuery(plan, duckDbSql);

    // Small batch size to exercise output-full path; ensure
    // outputBatchSize / buildRowCount > 1 so batching is active.
    CursorParameters params;
    params.planNode = plan;
    params.queryCtx = core::QueryCtx::create(executor_.get());
    params.queryCtx->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kPreferredOutputBatchRows, "9"}});
    assertQuery(params, duckDbSql);
  }
}

// Verifies LeftSemiProject join correctness with single build vector in the
// batched filter path.
TEST_F(NestedLoopJoinTest, leftSemiProjectWithSingleBuildVector) {
  // Single build vector with multiple rows.
  auto probeVector =
      makeRowVector({"t0"}, {makeFlatVector<int32_t>({1, 2, 3, 4, 5})});
  auto buildVector = makeRowVector({"u0"}, {makeFlatVector<int32_t>({2, 4})});

  auto verifyLeftSemiProject = [&](const RowVectorPtr& result,
                                   const std::vector<bool>& expectedMatch) {
    ASSERT_EQ(result->size(), expectedMatch.size());
    for (int r = 0; r < result->size(); ++r) {
      auto t0 = result->childAt(0)->as<SimpleVector<int32_t>>()->valueAt(r);
      bool match = result->childAt(1)->as<SimpleVector<bool>>()->valueAt(r);
      EXPECT_EQ(t0, r + 1) << "row " << r;
      EXPECT_EQ(match, expectedMatch[r]) << "row " << r << ": t0=" << t0;
    }
  };

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({probeVector})
          .nestedLoopJoin(
              PlanBuilder(planNodeIdGenerator).values({buildVector}).planNode(),
              "t0 >= u0",
              {"t0", "match"},
              core::JoinType::kLeftSemiProject)
          .planNode();

  // Default batch size — single build vector.
  auto r1 = AssertQueryBuilder(plan).copyResults(pool());
  verifyLeftSemiProject(r1, {false, true, true, true, true});

  // Single build row.
  buildVector =
      makeRowVector({"u0"}, {makeFlatVector<int32_t>(std::vector<int32_t>{3})});

  planNodeIdGenerator->reset();
  plan =
      PlanBuilder(planNodeIdGenerator)
          .values({probeVector})
          .nestedLoopJoin(
              PlanBuilder(planNodeIdGenerator).values({buildVector}).planNode(),
              "t0 >= u0",
              {"t0", "match"},
              core::JoinType::kLeftSemiProject)
          .planNode();

  auto r2 = AssertQueryBuilder(plan).copyResults(pool());
  verifyLeftSemiProject(r2, {false, false, true, true, true});
}

// Verifies that left/left-semi-project joins produce correct mismatch rows
// when the output batch fills exactly at a probe row boundary.  In this
// scenario probeRowHasMatch_ must be reset before processing the next probe
// row; otherwise it inherits the previous row's "matched" state and silently
// drops the NULL-extended / match=false output for unmatched probe rows.
TEST_F(NestedLoopJoinTest, batchBoundaryProbeMismatch) {
  // Probe: {10, 10, 5, 5}.  Build: {10}.  Filter: t0 = u0.
  // With maxOutputBatchRows=2 the output fills right after the two matches
  // (probes 0-1), so the batch boundary lands exactly at the start of probe 2
  // (the first non-matching row).  Probes 2 and 3 must still produce mismatch
  // output.
  auto probeVectors = {
      makeRowVector({"t0"}, {makeFlatVector<int32_t>({10, 10, 5, 5})}),
  };
  auto buildVectors = {
      makeRowVector(
          {"u0"}, {makeFlatVector<int32_t>(std::vector<int32_t>{10})}),
  };

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  // Use kMaxOutputBatchRows as the hard cap — the dynamic batch size
  // adjustment in prepareOutput() can override kPreferredOutputBatchRows but
  // not the max.
  const std::string kBatchSize = "2";

  // Left join: non-matching probes must produce (t0, NULL).
  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({probeVectors})
                    .nestedLoopJoin(
                        PlanBuilder(planNodeIdGenerator)
                            .values({buildVectors})
                            .planNode(),
                        "t0 = u0",
                        {"t0", "u0"},
                        core::JoinType::kLeft)
                    .planNode();
    CursorParameters params;
    params.planNode = plan;
    params.queryCtx = core::QueryCtx::create(executor_.get());
    params.queryCtx->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kMaxOutputBatchRows, kBatchSize}});
    assertQuery(params, "SELECT t0, u0 FROM t LEFT JOIN u ON t0 = u0");
  }

  // Left semi project: non-matching probes must produce match=false.
  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({probeVectors})
                    .nestedLoopJoin(
                        PlanBuilder(planNodeIdGenerator)
                            .values({buildVectors})
                            .planNode(),
                        "t0 = u0",
                        {"t0", "match"},
                        core::JoinType::kLeftSemiProject)
                    .planNode();
    auto result =
        AssertQueryBuilder(plan)
            .config(core::QueryConfig::kMaxOutputBatchRows, kBatchSize)
            .copyResults(pool());
    ASSERT_EQ(result->size(), 4);
    for (int r = 0; r < 4; ++r) {
      auto t0 = result->childAt(0)->as<SimpleVector<int32_t>>()->valueAt(r);
      bool match = result->childAt(1)->as<SimpleVector<bool>>()->valueAt(r);
      bool expectedMatch = (t0 == 10);
      EXPECT_EQ(match, expectedMatch) << "row " << r << ": t0=" << t0;
    }
  }
}

// Verifies that no spurious mismatch row is produced when the output batch
// fills exactly at the last build row of the last probe row in a batch.  In
// this scenario filterResultRow_ lands at decodedFilterResult_.size() (no more
// filter results to process), so probeRowHasMatch_ must NOT be reset —
// otherwise checkProbeMismatchRow() incorrectly adds a NULL-extended row for a
// probe that already matched.
TEST_F(NestedLoopJoinTest, batchBoundaryLastProbeRow) {
  // 3 probe rows that ALL match the single build row.  maxOutputBatchRows=3
  // makes the output fill exactly when the 3rd match (last probe, last build
  // row) is added.
  auto probeVectors = {
      makeRowVector({"t0"}, {makeFlatVector<int32_t>({1, 2, 3})}),
  };
  auto buildVectors = {
      makeRowVector({"u0"}, {makeFlatVector<int32_t>(std::vector<int32_t>{1})}),
  };

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  // Left join: all 3 probes match, so the result must be exactly 3 rows with
  // no NULL-extended mismatch rows.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values({probeVectors})
                  .nestedLoopJoin(
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildVectors})
                          .planNode(),
                      "t0 >= u0",
                      {"t0", "u0"},
                      core::JoinType::kLeft)
                  .planNode();
  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = core::QueryCtx::create(executor_.get());
  params.queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kMaxOutputBatchRows, "3"}});
  assertQuery(params, "SELECT t0, u0 FROM t LEFT JOIN u ON t0 >= u0");
}

} // namespace
} // namespace bytedance::bolt::exec::test
