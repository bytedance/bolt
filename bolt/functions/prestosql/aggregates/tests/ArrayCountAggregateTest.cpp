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

#include "bolt/dwio/common/tests/utils/BatchMaker.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::aggregate::test {

namespace {

class ArrayCountAggregateTest
    : public functions::aggregate::test::AggregationTestBase {};

TEST_F(ArrayCountAggregateTest, mask) {
  // Global aggregation with a non-constant mask.
  auto data = makeRowVector({makeArrayVector<int32_t>(
      {{0, 1}, {10, 11}, {2, 3}, {12, 13}, {14, 15}})});

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"array_count(c0)"})
                  .planNode();

  assertQuery(plan, "SELECT [5, 5]");

  data = makeRowVector({
      makeFlatVector<int64_t>({10, 20, 10, 20, 20}),
      makeArrayVector<int32_t>({{0, 1}, {10, 11}, {2, 3}, {12, 13}, {14, 15}}),
      makeConstant(true, 5),
  });

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"array_count(c1)"}, {"c2"})
             .planNode();

  assertQuery(plan, "VALUES (10, [2, 2]), (20, [3, 3])");

  data = makeRowVector({
      makeFlatVector<int64_t>({10, 20, 10, 20, 20}),
      makeArrayVector<int32_t>({{0, 1}, {10, 11}, {2, 3}, {12, 13}, {14, 15}}),
      makeConstant(false, 5),
  });

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"array_count(c1)"}, {"c2"})
             .planNode();

  assertQuery(plan, "VALUES (10, null), (20, null)");

  data = makeRowVector({
      makeFlatVector<int64_t>({10, 20, 10, 20, 20}),
      makeArrayVector<int32_t>({{0, 1}, {10, 11}, {2, 3}, {12, 13}, {14, 15}}),
      makeFlatVector<bool>({true, true, true, false, false}),
  });

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"array_count(c1)"}, {"c2"})
             .planNode();

  assertQuery(plan, "VALUES (10, [2, 2]), (20, [1, 1])");
}

TEST_F(ArrayCountAggregateTest, sparseMask) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({10, 10, 10, 20, 20, 20}),
      makeArrayVector<int32_t>(
          {{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, {10, 11}}),
      makeNullableFlatVector<bool>(
          {true, std::nullopt, true, false, true, false}),
  });

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"array_count(c1)"}, {"c2"})
                  .planNode();
  assertQuery(plan, "SELECT [3, 3]");

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"array_count(c1)"}, {"c2"})
             .planNode();
  assertQuery(plan, "VALUES (10, [2, 2]), (20, [1, 1])");
}

TEST_F(ArrayCountAggregateTest, multiArrayTest) {
  auto data = makeRowVector(
      {makeFlatVector<int64_t>({10, 20, 10, 20, 20}),
       makeArrayVector<int64_t>({{0, 1}, {10, 11}, {2, 3}, {12, 13}, {14, 15}}),
       makeArrayVector<int32_t>(
           {{0, 1, 2}, {10, 11, 12}, {3, 4, 5}, {13, 14, 15}, {16, 17, 18}})});

  std::vector<std::string> aggregates = {"array_count(c1)", "array_count(c2)"};

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({"c0"}, aggregates)
                  .planNode();

  assertQuery(plan, "VALUES (10, [2, 2], [2, 2, 2]), (20, [3, 3], [3, 3, 3])");
}

TEST_F(ArrayCountAggregateTest, withNullTest) {
  // Vector is:
  // [
  //   NULL,
  //   [0, 1, 2],
  //   NULL,
  //   [0, 1, 2],
  //   NULL
  // ]
  auto arrayVector = makeArrayVector<int32_t>(
      5,
      [](auto row) { return 3; },
      [](auto row, auto index) { return index; },
      nullEvery(2));

  auto data = makeRowVector(
      {makeFlatVector<int64_t>({10, 20, 10, 20, 20}), arrayVector});

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({"c0"}, {"array_count(c1)"})
                  .planNode();

  assertQuery(plan, "VALUES (10, null), (20, [2, 2, 2])");

  // Vector is:
  // [
  //   [NULL, 1],
  //   [2, 3],
  //   NULL,
  //   [4, 5],
  //   [NULL, 7]
  // ]
  auto vector = makeArrayVector<int32_t>(
      5,
      [](vector_size_t row) { return 2; },
      [](vector_size_t idx) { return idx; },
      [](vector_size_t row) { return row % 5 == 2; },
      [](vector_size_t idx) { return idx % 6 == 0; });

  data = makeRowVector({makeFlatVector<int64_t>({10, 20, 10, 20, 20}), vector});

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"array_count(c1)"})
             .planNode();

  assertQuery(plan, "VALUES (10, [1, 1]), (20, [3, 3])");

  // Vector is:
  // [
  //   NULL,
  //   [NULL, 1],
  //   [2, 3],
  //   [4, 5],
  //   [NULL, 7]
  // ]
  vector = makeArrayVector<int32_t>(
      5,
      [](vector_size_t row) { return 2; },
      [](vector_size_t idx) { return idx; },
      [](vector_size_t row) { return row % 5 == 0; },
      [](vector_size_t idx) { return idx % 6 == 0; });

  data = makeRowVector({makeFlatVector<int64_t>({10, 20, 10, 20, 20}), vector});

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"array_count(c1)"})
             .planNode();

  assertQuery(plan, "VALUES (10, [1, 1]), (20, [3, 3])");
}

TEST_F(ArrayCountAggregateTest, exceptionMemoryLeakTest) {
  auto data = makeRowVector(
      {makeFlatVector<int64_t>({10, 20, 10, 20, 20}),
       makeArrayVector<int64_t>(
           {{0, 1}, {10, 11}, {2, 3}, {12, 13}, {14, 15, 16}}),
       makeArrayVector<int32_t>(
           {{0, 1, 2}, {10, 11, 12}, {3, 4, 5}, {13, 14, 15}, {16, 17, 18}})});

  auto plan =
      PlanBuilder()
          .values({data})
          .singleAggregation({"c0"}, {"array_count(c1)", "array_count(c2)"})
          .planNode();

  EXPECT_THROW(
      {
        assertQuery(
            plan,
            "VALUES (10, [2, 4], [3, 5, 7]), (20, [36, 39], [39, 42, 45])");
      },
      BoltRuntimeError);
}

} // namespace
} // namespace bytedance::bolt::aggregate::test
