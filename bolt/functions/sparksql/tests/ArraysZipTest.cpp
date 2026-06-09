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

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace bytedance::bolt::test;

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class ArraysZipTest : public SparkFunctionBaseTest {
 protected:
  void testExpression(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expected) {
    auto result = evaluate(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }
};

// Single-arg `arrays_zip(a)` is supported in Spark; result is array of single
// field structs. Verifies the Spark-side registration accepts arity 1, which
// Presto `zip` still rejects (Presto signatures are [2, 7]).
TEST_F(ArraysZipTest, singleArg) {
  auto a = makeArrayVector<int64_t>({{1, 2, 3}, {}, {7, 8}});

  auto field0 = makeFlatVector<int64_t>({1, 2, 3, 7, 8});
  auto rowVector = makeRowVector({field0});
  auto expected = makeArrayVector({0, 3, 3}, rowVector);

  testExpression("arrays_zip(c0)", {a}, expected);
}

// Equal-length zip on two arrays.
TEST_F(ArraysZipTest, twoArraysEqualLength) {
  auto a = makeArrayVector<int64_t>({{1, 2, 3}, {4, 5}});
  auto b = makeArrayVector<int64_t>({{10, 20, 30}, {40, 50}});

  auto field0 = makeFlatVector<int64_t>({1, 2, 3, 4, 5});
  auto field1 = makeFlatVector<int64_t>({10, 20, 30, 40, 50});
  auto rowVector = makeRowVector({field0, field1});
  auto expected = makeArrayVector({0, 3}, rowVector);

  testExpression("arrays_zip(c0, c1)", {a, b}, expected);
}

// Unequal-length: shorter array padded with NULL on the right.
TEST_F(ArraysZipTest, unequalLengthPadsNull) {
  auto a = makeArrayVector<int64_t>({{1, 2, 3}, {4}});
  auto b = makeArrayVector<int64_t>({{10, 20}, {40, 50, 60}});

  auto field0 =
      makeNullableFlatVector<int64_t>({1, 2, 3, 4, std::nullopt, std::nullopt});
  auto field1 =
      makeNullableFlatVector<int64_t>({10, 20, std::nullopt, 40, 50, 60});
  auto rowVector = makeRowVector({field0, field1});
  auto expected = makeArrayVector({0, 3}, rowVector);

  testExpression("arrays_zip(c0, c1)", {a, b}, expected);
}

// Inner element NULLs are preserved verbatim and are NOT treated as missing.
TEST_F(ArraysZipTest, innerNullsArePreserved) {
  auto a = makeNullableArrayVector<int64_t>(
      {{{1, std::nullopt, 3}, {std::nullopt}}});
  auto b = makeNullableArrayVector<int64_t>(
      {{{std::nullopt, 20, 30}, {std::nullopt}}});

  auto field0 =
      makeNullableFlatVector<int64_t>({1, std::nullopt, 3, std::nullopt});
  auto field1 =
      makeNullableFlatVector<int64_t>({std::nullopt, 20, 30, std::nullopt});
  auto rowVector = makeRowVector({field0, field1});
  auto expected = makeArrayVector({0, 3}, rowVector);

  testExpression("arrays_zip(c0, c1)", {a, b}, expected);
}

// Cover the original failing pattern: 10 array inputs.
// Before the fix only arity 2..6 were registered (off-by-one + Presto's
// MAX_ARITY=7 cap), so `arrays_zip(c0..c9)` would throw
// "Scalar function arrays_zip not registered".
TEST_F(ArraysZipTest, tenArguments) {
  auto a0 = makeArrayVector<int64_t>({{1, 2}, {3}});
  auto a1 = makeArrayVector<int64_t>({{10}, {30, 31}});
  auto a2 = makeArrayVector<int64_t>({{100, 101}, {300}});
  auto a3 = makeArrayVector<int64_t>({{1}, {3}});
  auto a4 = makeArrayVector<int64_t>({{2}, {4}});
  auto a5 = makeArrayVector<int64_t>({{5}, {6}});
  auto a6 = makeArrayVector<int64_t>({{7}, {8}});
  auto a7 = makeArrayVector<int64_t>({{9}, {10}});
  auto a8 = makeArrayVector<int64_t>({{11}, {12}});
  auto a9 = makeArrayVector<int64_t>({{13}, {14}});

  auto result = evaluate(
      "arrays_zip(c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)",
      makeRowVector({a0, a1, a2, a3, a4, a5, a6, a7, a8, a9}));

  // Sanity: result is array<row<10 fields>> with the right top-level shape.
  auto arrayResult = result->as<ArrayVector>();
  ASSERT_NE(arrayResult, nullptr);
  EXPECT_EQ(arrayResult->size(), 2);
  // Row 0: max len = 2, Row 1: max len = 2.
  EXPECT_EQ(arrayResult->sizeAt(0), 2);
  EXPECT_EQ(arrayResult->sizeAt(1), 2);

  auto rows = arrayResult->elements()->as<RowVector>();
  ASSERT_NE(rows, nullptr);
  EXPECT_EQ(rows->childrenSize(), 10);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
