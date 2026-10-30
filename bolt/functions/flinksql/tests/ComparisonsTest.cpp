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

#include "bolt/functions/flinksql/Comparisons.h"
#include <numeric>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/Udf.h"
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::functions::flinksql::test {
namespace {

using bytedance::bolt::test::assertEqualVectors;

class ComplexEqualTest : public FlinkFunctionBaseTest {};

TEST_F(ComplexEqualTest, NestedRowsAndLegacyNullPolicy) {
  auto nested = makeRowVector(
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 2}),
       makeFlatVector<std::string>({"a", "b", "c"})});
  auto left =
      makeRowVector({nested, makeFlatVector<bool>({true, true, false})});
  auto right =
      makeRowVector({nested, makeFlatVector<bool>({true, true, true})});
  auto input = makeRowVector({left, right});
  assertEqualVectors(
      makeFlatVector<bool>({true, true, false}),
      evaluate("flink_complex_equal(c0, c1)", input));

  registerFunction<EqFunction, bool, Generic<T1>, Generic<T1>>({"legacy_eq"});
  assertEqualVectors(
      makeNullableFlatVector<bool>({true, std::nullopt, false}),
      evaluate("legacy_eq(c0, c1)", input));
}

TEST_F(ComplexEqualTest, RejectsUnverifiedNestedTypes) {
  auto row = makeRowVector({makeFlatVector<double>({0.0})});
  BOLT_ASSERT_THROW(
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({row, row})),
      "Unsupported Flink ROW equality type");
}

TEST_F(ComplexEqualTest, RowsCompareNestedNullsAsValues) {
  auto left = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 1, 1, 1}),
       makeNullableFlatVector<std::string>({"a", std::nullopt, "a", "a"}),
       makeFlatVector<bool>({true, true, true, true})});
  auto right = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 1, 1, 1}),
       makeNullableFlatVector<std::string>(
           {"a", std::nullopt, std::nullopt, "a"}),
       makeFlatVector<bool>({true, true, true, true})});
  right->setNull(3, true);

  auto actual =
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({left, right}));
  auto expected =
      makeNullableFlatVector<bool>({true, true, false, std::nullopt});
  assertEqualVectors(expected, actual);
}

TEST_F(ComplexEqualTest, TwoStringRowsCompareNestedNullsAsValues) {
  auto left = makeRowVector(
      {makeFlatVector<std::string>({"a", "a"}),
       makeNullableFlatVector<std::string>({std::nullopt, "b"})});
  auto right = makeRowVector(
      {makeFlatVector<std::string>({"a", "a"}),
       makeNullableFlatVector<std::string>({std::nullopt, "c"})});
  auto actual =
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({left, right}));
  assertEqualVectors(makeFlatVector<bool>({true, false}), actual);
}

TEST_F(ComplexEqualTest, MapEncodingsAndEmptyMaps) {
  auto left = makeMapVector(
      {0, 0, 1, 3},
      makeFlatVector<std::string>({"a", "a", "b", "c"}),
      makeFlatVector<int32_t>({1, 1, 2, 3}));
  auto right = makeMapVector(
      {0, 0, 1, 3},
      makeFlatVector<std::string>({"a", "b", "a", "c"}),
      makeFlatVector<int32_t>({1, 2, 1, 4}));
  right->setNull(1, true);
  auto input = makeRowVector({left, right});
  testEncodings(
      makeTypedExpr("flink_complex_equal(c0, c1)", asRowType(input->type())),
      {left, right},
      makeNullableFlatVector<bool>({true, std::nullopt, true, false}));
}

TEST_F(ComplexEqualTest, MapsCompareNullKeysAndValues) {
  auto left = makeMapVector(
      {0, 1, 2, 3},
      makeNullableFlatVector<std::string>({"a", "a", std::nullopt, "z"}),
      makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4}));
  auto right = makeMapVector(
      {0, 1, 2, 3},
      makeNullableFlatVector<std::string>({"a", "a", std::nullopt, "z"}),
      makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 5}));

  auto actual =
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({left, right}));
  auto expected = makeFlatVector<bool>({true, false, true, false});
  assertEqualVectors(expected, actual);
}

TEST_F(ComplexEqualTest, MapsIgnoreEntryOrder) {
  auto left = makeMapVector(
      {0},
      makeFlatVector<std::string>({"a", "b"}),
      makeFlatVector<int32_t>({1, 2}));
  auto right = makeMapVector(
      {0},
      makeFlatVector<std::string>({"b", "a"}),
      makeFlatVector<int32_t>({2, 1}));
  auto actual =
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({left, right}));
  assertEqualVectors(makeFlatVector<bool>(std::vector<bool>{true}), actual);
}

TEST_F(ComplexEqualTest, MapsUseLastDuplicateKeyValue) {
  auto left = makeMapVector(
      {0, 2, 4, 6},
      makeNullableFlatVector<std::string>(
          {"a", "a", "a", "a", "a", "a", std::nullopt, std::nullopt}),
      makeNullableFlatVector<int32_t>({1, 2, 1, 2, std::nullopt, 2, 1, 2}));
  auto right = makeMapVector(
      {0, 2, 4, 6},
      makeNullableFlatVector<std::string>(
          {"a", "a", "a", "b", "a", "a", std::nullopt, std::nullopt}),
      makeFlatVector<int32_t>({2, 2, 2, 0, 1, 2, 2, 2}));
  auto actual =
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({left, right}));
  assertEqualVectors(makeFlatVector<bool>({true, true, true, true}), actual);
}

TEST_F(ComplexEqualTest, DifferentMapSizesDoNotLoadValues) {
  auto leftValues =
      makeLazyFlatVector<int32_t>(2, [](auto row) { return row; });
  auto rightValues =
      makeLazyFlatVector<int32_t>(1, [](auto row) { return row; });
  auto left =
      makeMapVector({0}, makeFlatVector<std::string>({"a", "b"}), leftValues);
  auto right =
      makeMapVector({0}, makeFlatVector<std::string>({"a"}), rightValues);
  assertEqualVectors(
      makeFlatVector<bool>(std::vector<bool>{false}),
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({left, right})));
  EXPECT_FALSE(leftValues->isLoaded());
  EXPECT_FALSE(rightValues->isLoaded());
}

TEST_F(ComplexEqualTest, OnlyMatchingMapSizesLoadValues) {
  auto leftValues = makeLazyFlatVector<int32_t>(
      3,
      [](auto) { return 1; },
      [](auto) { return false; },
      1,
      [](auto) { return 2; });
  auto rightValues = makeLazyFlatVector<int32_t>(
      2,
      [](auto) { return 1; },
      [](auto) { return false; },
      1,
      [](auto) { return 1; });
  auto left = makeMapVector(
      {0, 2}, makeFlatVector<std::string>({"a", "b", "a"}), leftValues);
  auto right = makeMapVector(
      {0, 1}, makeFlatVector<std::string>({"a", "a"}), rightValues);
  assertEqualVectors(
      makeFlatVector<bool>({false, true}),
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({left, right})));
}

TEST_F(ComplexEqualTest, MapsWithDictionaryEncodedChildren) {
  auto indices = makeIndices(2, [](auto index) { return 1 - index; });
  auto left = makeMapVector(
      {0},
      wrapInDictionary(indices, 2, makeFlatVector<std::string>({"b", "a"})),
      wrapInDictionary(indices, 2, makeFlatVector<int32_t>({2, 1})));
  auto right = makeMapVector(
      {0},
      makeFlatVector<std::string>({"b", "a"}),
      makeFlatVector<int32_t>({2, 1}));
  auto actual =
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({left, right}));
  assertEqualVectors(makeFlatVector<bool>(std::vector<bool>{true}), actual);
}

TEST_F(ComplexEqualTest, SparseMapsWithNestedDictionaries) {
  constexpr vector_size_t size = 128;
  std::vector<vector_size_t> offsets(size);
  std::iota(offsets.begin(), offsets.end(), 0);
  auto indices = makeIndices(size, [](auto i) { return size - 1 - i; });
  auto keys = makeFlatVector<std::string>(size, [](auto) { return "a"; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });
  auto left = makeMapVector(
      offsets,
      wrapInDictionary(indices, size, wrapInDictionary(indices, size, keys)),
      wrapInDictionary(indices, size, wrapInDictionary(indices, size, values)));
  auto right = makeMapVector(offsets, keys, values);
  right->setNull(64, true);
  SelectivityVector rows(size, false);
  for (auto row : {3, 64, 127}) {
    rows.setValid(row, true);
  }
  rows.updateBounds();
  auto actual = evaluate(
      "flink_complex_equal(c0, c1)", makeRowVector({left, right}), rows);
  auto expected = makeFlatVector<bool>(size, [](auto) { return true; });
  expected->setNull(64, true);
  assertEqualVectors(expected, actual, rows);
  auto sliced = makeRowVector({left->slice(3, 2), right->slice(3, 2)});
  assertEqualVectors(
      makeFlatVector<bool>({true, true}),
      evaluate("flink_complex_equal(c0, c1)", sliced));
}

TEST_F(ComplexEqualTest, NullMapIsNull) {
  auto left = makeMapVector(
      {0},
      makeFlatVector<std::string>({"a"}),
      makeFlatVector<int32_t>(std::vector<int32_t>{1}));
  auto right = makeMapVector(
      {0},
      makeFlatVector<std::string>({"a"}),
      makeFlatVector<int32_t>(std::vector<int32_t>{1}));
  right->setNull(0, true);
  auto actual =
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({left, right}));
  assertEqualVectors(makeNullableFlatVector<bool>({std::nullopt}), actual);
}

} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
