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
#include <limits>
#include <numeric>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/expression/ConstantExpr.h"
#include "bolt/expression/RegisterSpecialForm.h"
#include "bolt/functions/Udf.h"
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::functions::flinksql::test {
namespace {

using bytedance::bolt::test::assertEqualVectors;

class ComplexEqualTest : public FlinkFunctionBaseTest {
 protected:
  void assertComparisons(
      const VectorPtr& left,
      const VectorPtr& right,
      std::vector<std::optional<bool>> expected) {
    const auto input = makeRowVector({left, right});
    assertEqualVectors(
        makeNullableFlatVector<bool>(expected),
        evaluate("flink_complex_equal(c0, c1)", input));
    for (auto& value : expected) {
      if (value.has_value()) {
        value = !value.value();
      }
    }
    assertEqualVectors(
        makeNullableFlatVector<bool>(expected),
        evaluate("flink_complex_not_equal(c0, c1)", input));
  }
};

TEST_F(ComplexEqualTest, NestedRowsAndLegacyNullPolicy) {
  auto nested = makeRowVector(
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 2}),
       makeFlatVector<std::string>({"a", "b", "c"})});
  auto left =
      makeRowVector({nested, makeFlatVector<bool>({true, true, false})});
  auto right =
      makeRowVector({nested, makeFlatVector<bool>({true, true, true})});
  auto input = makeRowVector({left, right});
  assertComparisons(left, right, {true, true, false});

  registerFunction<EqFunction, bool, Generic<T1>, Generic<T1>>({"legacy_eq"});
  assertEqualVectors(
      makeNullableFlatVector<bool>({true, std::nullopt, false}),
      evaluate("legacy_eq(c0, c1)", input));
}

TEST_F(ComplexEqualTest, RejectsUnsupportedRowFields) {
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

  assertComparisons(left, right, {true, true, false, std::nullopt});
}

TEST_F(ComplexEqualTest, TwoStringRowsCompareNestedNullsAsValues) {
  auto left = makeRowVector(
      {makeFlatVector<std::string>({"a", "a"}),
       makeNullableFlatVector<std::string>({std::nullopt, "b"})});
  auto right = makeRowVector(
      {makeFlatVector<std::string>({"a", "a"}),
       makeNullableFlatVector<std::string>({std::nullopt, "c"})});
  assertComparisons(left, right, {true, false});
}

class MapEqualityModeTest : public ComplexEqualTest,
                            public testing::WithParamInterface<const char*> {
 protected:
  void SetUp() override {
    ComplexEqualTest::SetUp();
    if (*GetParam()) {
      queryCtx_->testingOverrideConfigUnsafe(
          {{core::QueryConfig::kFlinkMapEqualityMode, GetParam()}});
    }
  }

  bool sqlKeys() const {
    return std::string_view(GetParam()) == "sql_keys";
  }

  VectorPtr makeSelectedLazyValues(std::vector<vector_size_t>& loaded) {
    return std::make_shared<LazyVector>(
        pool(),
        INTEGER(),
        3,
        std::make_unique<bolt::test::SimpleVectorLoader>(
            [&, this](RowSet rows) -> VectorPtr {
              loaded.assign(rows.begin(), rows.end());
              auto values =
                  makeFlatVector<int32_t>(3, [](auto) { return -1000; });
              for (auto row : rows) {
                values->set(row, 7);
              }
              return values;
            }));
  }
};

INSTANTIATE_TEST_SUITE_P(
    MapEqualityModes,
    MapEqualityModeTest,
    testing::Values("", "java_map", "sql_keys"));

TEST_P(MapEqualityModeTest, MapEncodingsAndEmptyMaps) {
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
  testEncodings(
      makeTypedExpr(
          "flink_complex_not_equal(c0, c1)", asRowType(input->type())),
      {left, right},
      makeNullableFlatVector<bool>({false, std::nullopt, false, true}));
}

TEST_P(MapEqualityModeTest, MapsCompareNullKeysAndValues) {
  auto left = makeMapVector(
      {0, 1, 2, 3},
      makeNullableFlatVector<std::string>({"a", "a", std::nullopt, "z"}),
      makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4}));
  auto right = makeMapVector(
      {0, 1, 2, 3},
      makeNullableFlatVector<std::string>({"a", "a", std::nullopt, "z"}),
      makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 5}));

  assertComparisons(left, right, {true, false, true, false});
}

TEST_P(MapEqualityModeTest, MapsIgnoreEntryOrder) {
  auto left = makeMapVector(
      {0},
      makeFlatVector<std::string>({"a", "b"}),
      makeFlatVector<int32_t>({1, 2}));
  auto right = makeMapVector(
      {0},
      makeFlatVector<std::string>({"b", "a"}),
      makeFlatVector<int32_t>({2, 1}));
  assertComparisons(left, right, {true});
}

TEST_P(MapEqualityModeTest, MapsUseLastDuplicateKeyValue) {
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
  assertComparisons(left, right, {true, !sqlKeys(), true, true});
}

TEST_P(MapEqualityModeTest, DuplicateKeyGroups) {
  auto left = makeMapVector<int64_t, int32_t>(
      {{{2, 0}, {2, 1}},
       {{1, 0}, {1, 1}, {3, 3}},
       {{1, 0}, {1, 1}},
       {{1, 0}, {1, 1}, {2, 2}},
       {{1, 0}, {1, 1}},
       {{1, 0}, {1, 1}}});
  auto right = makeMapVector<int64_t, int32_t>(
      {{{1, 9}, {2, 1}},
       {{1, 1}, {2, 9}, {3, 3}},
       {{1, 1}, {2, 9}},
       {{1, 1}, {2, 0}, {2, 2}},
       {{1, 1}, {1, 0}},
       {{1, 1}}});
  const std::vector<std::optional<bool>> forward{
      !sqlKeys(), !sqlKeys(), !sqlKeys(), true, false, false};
  const std::vector<std::optional<bool>> reverse{
      false, false, false, true, false, false};
  assertComparisons(left, right, forward);
  assertComparisons(right, left, reverse);
  auto keys = makeFlatVector<int32_t>(6, [](auto) { return 1; });
  assertComparisons(
      makeMapVector({0, 1, 2, 3, 4, 5}, keys, left),
      makeMapVector({0, 1, 2, 3, 4, 5}, keys, right),
      forward);
  assertComparisons(
      makeMapVector({0, 1, 2, 3, 4, 5}, keys, makeRowVector({right})),
      makeMapVector({0, 1, 2, 3, 4, 5}, keys, makeRowVector({left})),
      reverse);
}

TEST_P(MapEqualityModeTest, ConstantDuplicateKeys) {
  auto left = std::make_shared<core::ConstantTypedExpr>(
      makeMapVector<int32_t, int32_t>({{{1, 0}, {1, 1}}}));
  auto right = std::make_shared<core::ConstantTypedExpr>(
      makeMapVector<int32_t, int32_t>({{{1, 1}, {2, 0}}}));
  auto input = makeRowVector({makeFlatVector<int32_t>({1})});
  for (bool negate : {false, true}) {
    for (bool reverse : {false, true}) {
      auto expression = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{
              reverse ? right : left, reverse ? left : right},
          negate ? "flink_complex_not_equal" : "flink_complex_equal");
      exec::ExprSet compiled({expression}, &execCtx_);
      ASSERT_NE(
          dynamic_cast<exec::ConstantExpr*>(compiled.exprs()[0].get()),
          nullptr);
      assertEqualVectors(
          makeFlatVector<bool>({negate != (!sqlKeys() && !reverse)}),
          evaluate(compiled, input));
    }
  }
}

TEST_F(ComplexEqualTest, RejectsInvalidMapEqualityMode) {
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kFlinkMapEqualityMode, "unknown"}});
  auto map = makeMapVector<int32_t, int32_t>({{{1, 1}}});
  BOLT_ASSERT_THROW(
      evaluate("flink_complex_equal(c0, c1)", makeRowVector({map, map})),
      "Unsupported Flink MAP equality mode: unknown");
}

TEST_P(MapEqualityModeTest, NullEmptyRowMapValues) {
  auto values = BaseVector::createNullConstant(ROW({}), 1, pool());
  auto map = makeMapVector({0}, makeFlatVector<int32_t>({1}), values);
  assertComparisons(map, map, {false});
}

TEST_P(MapEqualityModeTest, ConstantLazyMapValues) {
  const auto makeMap = [&](vector_size_t index) {
    auto lazy = makeLazyFlatVector<int32_t>(
        4,
        [](auto row) { return row; },
        [](auto) { return false; },
        1,
        [index](auto) { return index; });
    auto values =
        std::make_shared<ConstantVector<int32_t>>(pool(), 2, index, lazy);
    return std::make_pair(
        makeMapVector({0, 1}, makeFlatVector<int32_t>({1, 2}), values), lazy);
  };
  auto [left, leftLazy] = makeMap(1);
  auto [right, rightLazy] = makeMap(2);
  assertComparisons(left, right, {false, false});
  EXPECT_TRUE(leftLazy->isLoaded());
  EXPECT_TRUE(rightLazy->isLoaded());
  assertComparisons(left, left, {true, true});
}

TEST_P(MapEqualityModeTest, ConstantLazyStringKeys) {
  const auto makeMap = [&](vector_size_t index) {
    auto lazy = makeLazyFlatVector<std::string>(
        4,
        [](auto row) { return "a_long_map_key_" + std::to_string(row); },
        [](auto) { return false; },
        1,
        [index](auto) { return index; });
    auto keys =
        std::make_shared<ConstantVector<StringView>>(pool(), 2, index, lazy);
    return std::make_pair(
        makeMapVector({0, 1}, keys, makeFlatVector<int32_t>({1, 1})), lazy);
  };
  auto [left, leftLazy] = makeMap(1);
  auto [right, rightLazy] = makeMap(2);
  assertComparisons(left, right, {false, false});
  EXPECT_TRUE(leftLazy->isLoaded());
  EXPECT_TRUE(rightLazy->isLoaded());
  assertComparisons(left, left, {true, true});
}

TEST_P(MapEqualityModeTest, SharedLazyChildrenUseUnionOfRows) {
  for (const auto nested : {0, 1, 2}) {
    SCOPED_TRACE(nested);
    std::vector<vector_size_t> loaded;
    auto values = makeSelectedLazyValues(loaded);
    if (nested == 1) {
      values = makeArrayVector({0, 1, 2}, values);
    } else if (nested == 2) {
      values = makeRowVector({values});
    }
    auto map =
        makeMapVector({0, 1, 2}, makeFlatVector<int32_t>({1, 1, 1}), values);
    const auto selectMap = [&](vector_size_t index) {
      return wrapInDictionary(
          makeIndices(1, [index](auto) { return index; }), 1, map);
    };
    assertComparisons(selectMap(0), selectMap(2), {true});
    EXPECT_EQ(loaded, (std::vector<vector_size_t>{0, 2}));
  }
}

TEST_P(MapEqualityModeTest, SharedLazyKeysAndValues) {
  std::vector<vector_size_t> loaded;
  auto lazy = makeSelectedLazyValues(loaded);
  auto left = makeMapVector({0, 1, 2}, lazy, lazy);
  auto right = makeMapVector<int32_t, int32_t>({{{7, 7}}, {{7, 7}}, {{7, 7}}});
  assertComparisons(left, right, {true, true, true});
  EXPECT_EQ(loaded, (std::vector<vector_size_t>{0, 1, 2}));
}

TEST_P(MapEqualityModeTest, LazyValuesAcrossConditionalBranches) {
  exec::registerFunctionCallToSpecialForms();
  for (const auto* function :
       {"flink_complex_equal", "flink_complex_not_equal"}) {
    SCOPED_TRACE(function);
    std::vector<vector_size_t> loaded;
    auto left = makeMapVector(
        {0, 1, 2},
        makeFlatVector<int32_t>({1, 1, 1}),
        makeSelectedLazyValues(loaded));
    const auto other = [&] {
      return makeMapVector<int32_t, int32_t>({{{1, 7}}, {{1, 7}}, {{1, 7}}});
    };
    auto input = makeRowVector(
        {left, makeFlatVector<bool>({true, false, false}), other(), other()});
    assertEqualVectors(
        makeFlatVector<bool>(
            3,
            [&](auto) {
              return std::string_view(function) == "flink_complex_equal";
            }),
        evaluate(
            fmt::format("if(c1, {0}(c0, c2), {0}(c0, c3))", function), input));
    EXPECT_EQ(loaded, (std::vector<vector_size_t>{0, 1, 2}));
  }
}

TEST_P(MapEqualityModeTest, EmptyMapsDoNotLoadConstantLazyValues) {
  auto lazy = makeLazyFlatVector<int32_t>(
      4, [](auto row) { return row; }, [](auto) { return false; }, 0);
  auto values = std::make_shared<ConstantVector<int32_t>>(pool(), 0, 2, lazy);
  auto map = makeMapVector({0}, makeFlatVector<int32_t>({}), values);
  assertComparisons(map, map, {true});
  EXPECT_FALSE(lazy->isLoaded());
}

TEST_P(MapEqualityModeTest, DifferentMapSizesDoNotLoadValues) {
  auto leftValues =
      makeLazyFlatVector<int32_t>(2, [](auto row) { return row; });
  auto rightValues =
      makeLazyFlatVector<int32_t>(1, [](auto row) { return row; });
  auto left =
      makeMapVector({0}, makeFlatVector<std::string>({"a", "b"}), leftValues);
  auto right =
      makeMapVector({0}, makeFlatVector<std::string>({"a"}), rightValues);
  assertComparisons(left, right, {false});
  EXPECT_FALSE(leftValues->isLoaded());
  EXPECT_FALSE(rightValues->isLoaded());
}

TEST_P(MapEqualityModeTest, OnlyMatchingMapSizesLoadValues) {
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

TEST_P(MapEqualityModeTest, MapsWithDictionaryEncodedChildren) {
  auto indices = makeIndices(2, [](auto index) { return 1 - index; });
  auto left = makeMapVector(
      {0},
      wrapInDictionary(indices, 2, makeFlatVector<std::string>({"b", "a"})),
      wrapInDictionary(indices, 2, makeFlatVector<int32_t>({2, 1})));
  auto right = makeMapVector(
      {0},
      makeFlatVector<std::string>({"b", "a"}),
      makeFlatVector<int32_t>({2, 1}));
  assertComparisons(left, right, {true});
}

TEST_P(MapEqualityModeTest, SparseMapsWithNestedDictionaries) {
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

TEST_P(MapEqualityModeTest, NullMapIsNull) {
  auto left = makeMapVector(
      {0},
      makeFlatVector<std::string>({"a"}),
      makeFlatVector<int32_t>(std::vector<int32_t>{1}));
  auto right = makeMapVector(
      {0},
      makeFlatVector<std::string>({"a"}),
      makeFlatVector<int32_t>(std::vector<int32_t>{1}));
  right->setNull(0, true);
  assertComparisons(left, right, {std::nullopt});
}

TEST_P(MapEqualityModeTest, ScalarMapKeyAndValueTypes) {
  const std::vector<VectorPtr> scalars{
      makeFlatVector<bool>({false, true, false}),
      makeFlatVector<int8_t>({1, 2, 3}),
      makeFlatVector<int16_t>({1, 2, 3}),
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<float>({1.0, 2.0, 3.0}),
      makeFlatVector<double>({1.0, 2.0, 3.0}),
      makeFlatVector<std::string>({"a", "b", "c"}),
      makeFlatVector<int64_t>({100, 200, 300}, DECIMAL(12, 2)),
      makeFlatVector<int128_t>({100, 200, 300}, DECIMAL(30, 4)),
      makeFlatVector<int32_t>({1, 2, 3}, DATE()),
      makeFlatVector<int64_t>({1, 2, 3}, INTERVAL_DAY_TIME()),
      makeFlatVector<int32_t>({1, 2, 3}, INTERVAL_YEAR_MONTH()),
      makeFlatVector<Timestamp>(
          {Timestamp(1, 1), Timestamp(2, 2), Timestamp(3, 3)})};
  auto permutation =
      makeIndices(3, [](auto row) { return row == 0 ? 0 : 3 - row; });
  for (const auto& keys : scalars) {
    for (const auto& values : scalars) {
      SCOPED_TRACE(
          keys->type()->toString() + " -> " + values->type()->toString());
      assertComparisons(
          makeMapVector({0, 1, 2}, keys, values),
          makeMapVector(
              {0, 1, 2}, keys, wrapInDictionary(permutation, 3, values)),
          {true, false, false});
      auto reversed = makeIndices(2, [](auto row) { return 1 - row; });
      auto twoKeys = keys->slice(0, 2);
      auto twoValues = values->slice(0, 2);
      assertComparisons(
          makeMapVector({0}, twoKeys, twoValues),
          makeMapVector(
              {0},
              wrapInDictionary(reversed, 2, twoKeys),
              wrapInDictionary(reversed, 2, twoValues)),
          {true});
    }
  }
}

TEST_P(MapEqualityModeTest, NestedMapSorting) {
  auto leftValues = makeMapVector(
      {0, 2, 2, 5},
      makeFlatVector<int32_t>({2, 1, 3, 1, 2, 7}),
      makeFlatVector<int64_t>({20, 10, 30, 10, 20, 70}));
  auto rightValues = makeMapVector(
      {0, 0, 2, 3},
      makeFlatVector<int32_t>({1, 2, 7, 2, 3, 1}),
      makeFlatVector<int64_t>({10, 20, 70, 20, 30, 10}));
  auto left =
      makeMapVector({0, 2}, makeFlatVector<int32_t>({1, 2, 1, 2}), leftValues);
  auto right =
      makeMapVector({0, 2}, makeFlatVector<int32_t>({2, 1, 2, 1}), rightValues);
  assertComparisons(left, right, {true, true});
  rightValues->mapValues()->asFlatVector<int64_t>()->set(5, 99);
  assertComparisons(left, right, {true, false});
}

TEST_P(MapEqualityModeTest, IntegerKeysAndStringValues) {
  auto left = makeMapVector(
      {0, 2, 4, 6},
      makeNullableFlatVector<int32_t>({1, 2, 1, 1, std::nullopt, 2, 1, 2}),
      makeNullableFlatVector<std::string>(
          {"a", "b", "old", "new", "a", "b", "a", std::nullopt}));
  auto right = makeMapVector(
      {0, 2, 4, 6},
      makeNullableFlatVector<int32_t>({2, 1, 1, 2, 2, std::nullopt, 1, 2}),
      makeNullableFlatVector<std::string>(
          {"b", "a", "new", "extra", "b", "a", "a", std::nullopt}));
  assertComparisons(left, right, {true, !sqlKeys(), true, false});
  assertComparisons(right, left, {true, false, true, false});
  right->setNull(2, true);
  assertComparisons(left, right, {true, !sqlKeys(), std::nullopt, false});
}

TEST_P(MapEqualityModeTest, FloatingPointMapKeysAndValues) {
  const auto nan = std::numeric_limits<double>::quiet_NaN();
  auto keys = makeFlatVector<double>({nan, -0.0, 0.0, 1.0});
  auto otherKeys = makeFlatVector<double>({-nan, 0.0, -0.0, 1.0});
  auto ones = makeFlatVector<int32_t>({1, 1, 1, 1});
  assertComparisons(
      makeMapVector({0, 1, 2, 3}, keys, ones),
      makeMapVector({0, 1, 2, 3}, otherKeys, ones),
      {!sqlKeys(), sqlKeys(), sqlKeys(), true});
  auto values = makeFlatVector<double>({nan, -0.0, 0.0, 1.0});
  auto otherValues = makeFlatVector<double>({-nan, 0.0, -0.0, 2.0});
  assertComparisons(
      makeMapVector({0, 1, 2, 3}, ones, values),
      makeMapVector({0, 1, 2, 3}, ones, otherValues),
      {false, true, true, false});
  assertComparisons(
      makeMapVector(
          {0},
          makeFlatVector<double>({0.0, -0.0, nan, nan}),
          makeFlatVector<int32_t>({1, 2, 0, 3})),
      makeMapVector(
          {0},
          makeFlatVector<double>({-nan, -0.0, 0.0, nan}),
          makeFlatVector<int32_t>({0, 2, 1, 3})),
      {!sqlKeys()});
  const auto floatNan = std::numeric_limits<float>::quiet_NaN();
  assertComparisons(
      makeMapVector(
          {0},
          makeFlatVector<float>({0.0f, -0.0f, floatNan, floatNan}),
          makeFlatVector<float>({1, 2, 0, 3})),
      makeMapVector(
          {0},
          makeFlatVector<float>({-floatNan, -0.0f, 0.0f, floatNan}),
          makeFlatVector<float>({0, 2, 1, 3})),
      {!sqlKeys()});
}

TEST_P(MapEqualityModeTest, ConstantFloatingPointMapKeys) {
  const auto check = [&]<typename T>() {
    const auto constantMap = [&](T key) {
      return std::make_shared<core::ConstantTypedExpr>(makeMapVector(
          {0}, makeFlatVector<T>({key}), makeFlatVector<int32_t>({1})));
    };
    auto input = makeRowVector({makeFlatVector<int32_t>({1})});
    for (bool negate : {false, true}) {
      for (bool sameKey : {false, true}) {
        auto expression = std::make_shared<core::CallTypedExpr>(
            BOOLEAN(),
            std::vector<core::TypedExprPtr>{
                constantMap(T(0.0)), constantMap(sameKey ? T(0.0) : T(-0.0))},
            negate ? "flink_complex_not_equal" : "flink_complex_equal");
        assertEqualVectors(
            makeFlatVector<bool>({negate != (sameKey || sqlKeys())}),
            evaluate(expression, input));
      }
    }
  };
  check.operator()<float>();
  check.operator()<double>();
}

TEST_P(MapEqualityModeTest, FloatingDuplicateKeys) {
  const auto check = [&]<typename T>() {
    const auto nan = std::numeric_limits<T>::quiet_NaN();
    auto left = makeMapVector(
        {0, 2, 4, 6, 8, 10},
        makeNullableFlatVector<T>(
            {0,
             -T(0),
             0,
             -T(0),
             nan,
             1,
             1,
             2,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt}),
        makeNullableFlatVector<int32_t>(
            {1, 2, 1, 2, 1, 2, 1, 2, std::nullopt, 2, 2, std::nullopt}));
    auto right = makeMapVector(
        {0, 2, 4, 6, 8, 10},
        makeNullableFlatVector<T>(
            {-T(0),
             0,
             0,
             0,
             1,
             1,
             1,
             nan,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt}),
        makeNullableFlatVector<int32_t>(
            {2, 1, 9, 2, 1, 2, 1, 2, 1, 2, 2, std::nullopt}));
    assertComparisons(
        left, right, {!sqlKeys(), sqlKeys(), false, false, true, false});
    assertComparisons(
        right, left, {!sqlKeys(), sqlKeys(), !sqlKeys(), false, true, false});
    auto keys = makeFlatVector<int32_t>(6, [](auto) { return 1; });
    assertComparisons(
        makeMapVector({0, 1, 2, 3, 4, 5}, keys, left),
        makeMapVector({0, 1, 2, 3, 4, 5}, keys, right),
        {!sqlKeys(), sqlKeys(), false, false, true, false});
  };
  check.operator()<float>();
  check.operator()<double>();
}

TEST_P(MapEqualityModeTest, ArraysRowsAndNestedMapsAsValues) {
  auto keys = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto arrays = makeArrayVector(
      {0, 2, 4, 4},
      makeNullableFlatVector<int32_t>({10, 100, 20, std::nullopt, 30}));
  assertComparisons(
      makeMapVector({0, 1, 2, 3}, keys, arrays),
      makeMapVector({0, 1, 2, 3}, keys, arrays),
      {true, false, true, true});
  arrays->setNull(3, true);
  auto rows = makeRowVector(
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 3, 4}), arrays});
  assertComparisons(
      makeMapVector({0, 1, 2, 3}, keys, rows),
      makeMapVector({0, 1, 2, 3}, keys, rows),
      {true, false, true, true});
  auto nested = makeMapVector(
      {0, 0, 1, 2},
      makeFlatVector<std::string>({"a", "b", "c"}),
      makeNullableFlatVector<int32_t>({1, std::nullopt, 3}));
  assertComparisons(
      makeMapVector({0, 1, 2, 3}, keys, nested),
      makeMapVector({0, 1, 2, 3}, keys, nested),
      {true, true, false, true});
  auto binaries =
      makeFlatVector<std::string>({"a", "b", "c", "d"}, VARBINARY());
  assertComparisons(
      makeMapVector({0, 1, 2, 3}, keys, binaries),
      makeMapVector({0, 1, 2, 3}, keys, binaries),
      {true, true, true, true});
}

TEST_P(MapEqualityModeTest, NestedSparseDictionariesSlicesAndLazyValues) {
  constexpr vector_size_t size = 128;
  std::vector<vector_size_t> offsets(size);
  std::iota(offsets.begin(), offsets.end(), 0);
  auto indices = makeIndices(size, [](auto row) { return size - 1 - row; });
  auto keys = makeFlatVector<int64_t>(size, [](auto) { return 1; });
  auto lazy = makeLazyFlatVector<int32_t>(
      size,
      [](auto row) { return row; },
      [](auto) { return false; },
      4,
      [](auto i) {
        return std::vector<vector_size_t>{3, 4, 64, 127}[i];
      });
  auto arrays = makeArrayVector(
      offsets,
      wrapInDictionary(indices, size, wrapInDictionary(indices, size, lazy)));
  auto left = makeMapVector(offsets, keys, arrays);
  auto right = makeMapVector(
      offsets,
      keys,
      makeArrayVector(offsets, makeFlatVector<int32_t>(size, [](auto row) {
                        return row;
                      })));
  auto input = makeRowVector({left, right});
  SelectivityVector selected(size, false);
  for (auto row : {3, 4, 64, 127})
    selected.setValid(row, true);
  selected.updateBounds();
  assertEqualVectors(
      makeFlatVector<bool>(size, [](auto) { return true; }),
      evaluate("flink_complex_equal(c0, c1)", input, selected),
      selected);
  assertComparisons(left->slice(3, 2), right->slice(3, 2), {true, true});
}

TEST_P(MapEqualityModeTest, MapNotEqualNullAndSizeSemantics) {
  auto keys = makeFlatVector<std::string>({"a", "a"});
  auto values = makeNullableFlatVector<int32_t>({1, std::nullopt});
  auto left = makeMapVector({0, 0, 1}, keys, values);
  auto right = makeMapVector({0, 1, 2}, keys, values);
  assertComparisons(left, right, {false, false, false});
  left->setNull(1, true);
  right->setNull(2, true);
  assertComparisons(left, right, {false, std::nullopt, std::nullopt});
}

TEST_P(MapEqualityModeTest, RejectsRepresentationDependentKeys) {
  auto values = makeFlatVector<int32_t>({1});
  for (const auto& keys : std::vector<VectorPtr>{
           makeFlatVector<std::string>({"a"}, VARBINARY()),
           makeArrayVector<int32_t>({{1}}),
           makeRowVector({values})}) {
    auto map = makeMapVector({0}, keys, values);
    for (const auto* expression :
         {"flink_complex_equal(c0, c1)", "flink_complex_not_equal(c0, c1)"}) {
      BOLT_ASSERT_THROW(
          evaluate(expression, makeRowVector({map, map})),
          "Unsupported Flink MAP equality type");
    }
  }
}

TEST_P(MapEqualityModeTest, FloatingPointNestedValues) {
  const auto nan = std::numeric_limits<double>::quiet_NaN();
  auto keys = makeFlatVector<int32_t>({1, 2, 3});
  auto leftRows =
      makeRowVector({makeNullableFlatVector<double>({nan, 0.0, std::nullopt})});
  auto rightRows = makeRowVector(
      {makeNullableFlatVector<double>({-nan, -0.0, std::nullopt})});
  assertComparisons(
      makeMapVector({0, 1, 2}, keys, leftRows),
      makeMapVector({0, 1, 2}, keys, rightRows),
      {false, true, true});
  assertComparisons(
      makeMapVector(
          {0, 1, 2}, keys, makeArrayVector<double>({{nan}, {0.0}, {}})),
      makeMapVector(
          {0, 1, 2}, keys, makeArrayVector<double>({{-nan}, {-0.0}, {}})),
      {false, true, true});
}

} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
