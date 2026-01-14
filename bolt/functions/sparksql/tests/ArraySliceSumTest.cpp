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

#include <fmt/format.h>
#include <vector/BaseVector.h>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::functions::test;
namespace bytedance::bolt::functions::sparksql::test {
namespace {

class ArraySliceSumTest : public SparkFunctionBaseTest {
 protected:
  // Evaluate an expression.
  template <typename T, typename StartType, typename LengthType>
  void testArraySliceSum(
      const VectorPtr& expected,
      const VectorPtr& input,
      StartType start,
      LengthType length,
      bool expectThrow = false) {
    std::vector<VectorPtr> vecs = {input};
    std::string sql = "array_slice_sum(c0, ";
    if constexpr (!std::is_same_v<StartType, int32_t>) {
      vecs.emplace_back(start);
      sql += "c1, ";
    } else {
      sql += fmt::format("'{}'::INTEGER, ", start);
    }

    if constexpr (!std::is_same_v<LengthType, int32_t>) {
      vecs.emplace_back(length);
      if constexpr (!std::is_same_v<StartType, int32_t>) {
        sql += "c2)";
      } else {
        sql += "c1)";
      }
    } else {
      sql += fmt::format("'{}'::INTEGER)", length);
    }
    auto inputRows = makeRowVector(vecs);
    if (expectThrow) {
      BOLT_ASSERT_THROW(
          evaluate(sql, inputRows), "can't calculate null value in array");
      return;
    }
    auto result = evaluate(sql, inputRows);
    assertEqualVectors(expected, result);

    if constexpr (
        std::is_same_v<StartType, int32_t> &&
        std::is_same_v<LengthType, int32_t>) {
      // Test constant input.
      const vector_size_t firstRow = 0;
      result = evaluate(
          sql,
          makeRowVector({BaseVector::wrapInConstant(10, firstRow, input)}));
      assertEqualVectors(
          BaseVector::wrapInConstant(10, firstRow, expected), result);

      const vector_size_t lastRow = input->size() - 1;
      result = evaluate(
          sql, makeRowVector({BaseVector::wrapInConstant(10, lastRow, input)}));
      assertEqualVectors(
          BaseVector::wrapInConstant(10, lastRow, expected), result);
    }
  }
};

// Test integer arrays.
TEST_F(ArraySliceSumTest, basic) {
  auto input = makeNullableArrayVector<int64_t>(
      std::vector<std::optional<std::vector<std::optional<int64_t>>>>{
          std::vector<std::optional<int64_t>>{1, 2, 3, 4, 5, 6},
          std::vector<std::optional<int64_t>>{std::nullopt, 1, 2},
          std::nullopt,
          std::vector<std::optional<int64_t>>{}});

  {
    auto expected = makeNullableFlatVector<int64_t>({9, 3, 0, 0});
    testArraySliceSum<int64_t>(expected, input, 1, 3);

    testArraySliceSum<int64_t>(expected, input, 0, 3, true);
  }

  // length > array size
  {
    auto expected = makeNullableFlatVector<int64_t>({18, 2, 0, 0});
    testArraySliceSum<int64_t>(expected, input, 2, 6);
  }

  // abs(start) > array_size
  {
    auto expected = makeNullableFlatVector<int64_t>({9, 0, 0, 0});
    testArraySliceSum<int64_t>(expected, input, 3, 2);
  }
  {
    auto expected = makeNullableFlatVector<int64_t>({15, 0, 0, 0});
    testArraySliceSum<int64_t>(expected, input, -3, 3);
  }

  // start < 0
  {
    auto expected = makeNullableFlatVector<int64_t>({11, 3, 0, 0});
    testArraySliceSum<int64_t>(expected, input, -2, 2);
  }

  // overflow
  {
    auto input = makeNullableArrayVector<int64_t>(
        {{1, 2, 3}, {0, std::numeric_limits<int64_t>::max(), 2, 3}, {}});
    auto expected =
        makeNullableFlatVector<int64_t>({6, -9223372036854775804, 0});
    testArraySliceSum<int64_t>(expected, input, 0, 5);
  }
}

TEST_F(ArraySliceSumTest, basicDictionary) {
  auto inputArr = makeNullableArrayVector<int64_t>(
      std::vector<std::optional<std::vector<std::optional<int64_t>>>>{
          std::vector<std::optional<int64_t>>{1, 2, 3, 4, 5, 6},
          std::vector<std::optional<int64_t>>{std::nullopt, 1, 2},
          std::nullopt,
          std::vector<std::optional<int64_t>>{}});
  const vector_size_t size = 10;
  auto indices = makeIndices(size, [](auto row) { return row % 4; });
  auto input = wrapInDictionary(indices, size, inputArr);

  {
    auto expected =
        makeNullableFlatVector<int64_t>({9, 3, 0, 0, 9, 3, 0, 0, 9, 3});

    auto data = makeRowVector({
        input,
        makeFlatVector<int32_t>(size, [](auto row) { return 1; }),
        makeFlatVector<int32_t>(size, [](auto row) { return 3; }),
    });

    auto result = evaluate("array_slice_sum(c0,c1,c2)", data);
    assertEqualVectors(expected, result);
  }
}

TEST_F(ArraySliceSumTest, ConstantInput) {
  auto input = makeNullableArrayVector<int64_t>(
      std::vector<std::optional<std::vector<std::optional<int64_t>>>>{
          std::vector<std::optional<int64_t>>{1, 2, 3, 4, 5, 6},
          std::vector<std::optional<int64_t>>{std::nullopt, 1, 2},
          std::nullopt,
          std::vector<std::optional<int64_t>>{}});
  const vector_size_t size = 10;

  {
    const vector_size_t firstRow = 0;
    auto constantInput = BaseVector::wrapInConstant(10, firstRow, input);

    auto data = makeRowVector({
        constantInput,
        makeFlatVector<int32_t>(size, [](auto row) { return 1; }),
        makeFlatVector<int32_t>(size, [](auto row) { return 3; }),
    });

    auto expected =
        makeNullableFlatVector<int64_t>({9, 3, 0, 0, 9, 3, 0, 0, 9, 3});
    auto expectedConstant = BaseVector::wrapInConstant(10, firstRow, expected);

    auto result = evaluate("array_slice_sum(c0,c1,c2)", data);
    assertEqualVectors(expectedConstant, result);
  }

  {
    const vector_size_t lastRow = input->size() - 1;
    auto constantInput = BaseVector::wrapInConstant(10, lastRow, input);

    auto data = makeRowVector({
        constantInput,
        makeFlatVector<int32_t>(size, [](auto row) { return 1; }),
        makeFlatVector<int32_t>(size, [](auto row) { return 3; }),
    });

    auto expected =
        makeNullableFlatVector<int64_t>({9, 3, 0, 0, 9, 3, 0, 0, 9, 3});
    auto expectedConstant = BaseVector::wrapInConstant(10, lastRow, expected);

    auto result = evaluate("array_slice_sum(c0,c1,c2)", data);
    assertEqualVectors(expectedConstant, result);
  }
}

// Test integer arrays.
TEST_F(ArraySliceSumTest, argVec) {
  auto input = makeNullableArrayVector<int64_t>(
      std::vector<std::optional<std::vector<std::optional<int64_t>>>>{
          std::vector<std::optional<int64_t>>{1, 2, 3, 4, 5, 6},
          std::vector<std::optional<int64_t>>{std::nullopt, 1, 2},
          std::nullopt,
          std::vector<std::optional<int64_t>>{}});
  auto lengths = makeFlatVector<int32_t>({5, 2, 1, 1});
  auto starts = makeFlatVector<int32_t>({-2, 1, 2, 1});

  {
    auto expected = makeNullableFlatVector<int64_t>({20, 3, 0, 0});
    testArraySliceSum<int64_t>(expected, input, 1, lengths);
    testArraySliceSum<int64_t>(expected, input, 0, lengths, true);
  }

  {
    auto expected = makeNullableFlatVector<int64_t>({11, 3, 0, 0});
    testArraySliceSum<int64_t>(expected, input, starts, 2);
  }

  {
    auto expected = makeNullableFlatVector<int64_t>({11, 3, 0, 0});
    testArraySliceSum<int64_t>(expected, input, starts, lengths);
  }
}

TEST_F(ArraySliceSumTest, argVecDouble) {
  auto input = makeNullableArrayVector<double>(
      std::vector<std::optional<std::vector<std::optional<double>>>>{
          std::vector<std::optional<double>>{1, 2, 3, 4, 5, 6},
          std::vector<std::optional<double>>{std::nullopt, 1, 2},
          std::nullopt,
          std::vector<std::optional<double>>{}});
  auto lengths = makeFlatVector<int32_t>({5, 2, 1, 1});
  auto starts = makeFlatVector<int32_t>({-2, 1, 2, 1});

  {
    auto expected = makeNullableFlatVector<double>({20, 3, 0, 0});
    testArraySliceSum<double>(expected, input, 1, lengths);
    testArraySliceSum<double>(expected, input, 0, lengths, true);
  }

  {
    auto expected = makeNullableFlatVector<double>({11, 3, 0, 0});
    testArraySliceSum<double>(expected, input, starts, 2);
  }

  {
    auto expected = makeNullableFlatVector<double>({11, 3, 0, 0});
    testArraySliceSum<double>(expected, input, starts, lengths);
  }
}

TEST_F(ArraySliceSumTest, argVecReal) {
  auto input = makeNullableArrayVector<float>(
      std::vector<std::optional<std::vector<std::optional<float>>>>{
          std::vector<std::optional<float>>{1, 2, 3, 4, 5, 6},
          std::vector<std::optional<float>>{std::nullopt, 1, 2},
          std::nullopt,
          std::vector<std::optional<float>>{}});
  auto lengths = makeFlatVector<int32_t>({5, 2, 1, 1});
  auto starts = makeFlatVector<int32_t>({-2, 1, 2, 1});

  {
    auto expected = makeNullableFlatVector<double>({20, 3, 0, 0});
    testArraySliceSum<float>(expected, input, 1, lengths);
    testArraySliceSum<float>(expected, input, 0, lengths, true);
  }

  {
    auto expected = makeNullableFlatVector<double>({11, 3, 0, 0});
    testArraySliceSum<float>(expected, input, starts, 2);
  }

  {
    auto expected = makeNullableFlatVector<double>({11, 3, 0, 0});
    testArraySliceSum<float>(expected, input, starts, lengths);
  }
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
