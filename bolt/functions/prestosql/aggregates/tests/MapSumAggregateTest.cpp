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

#include "bolt/common/base/SparkCompatibility.h"

#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/functions/prestosql/ArithmeticImpl.h"
#include "bolt/type/Conversions.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::functions::aggregate::test;
using bytedance::bolt::functions::aggregate::test::AggregationTestBase;
namespace bytedance::bolt::aggregate::test {

namespace {

class MapSumAggTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    decimalType1_ = DECIMAL(16, 8);
    decimalType2_ = DECIMAL(22, 11);
  }

  template <bool isDecimal, typename TValue>
  void testGlobal(
      std::vector<std::vector<std::pair<StringView, std::optional<TValue>>>>
          input,
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>
          expected,
      TypePtr decimalType = nullptr) {
    if constexpr (isDecimal) {
      BOLT_CHECK(decimalType);
    }
    VectorPtr inputVec;
    if constexpr (isDecimal) {
      inputVec = makeMapVector(input, MAP(VARCHAR(), decimalType));
    } else {
      inputVec = makeMapVector(input);
    }
    auto expectedVec = makeMapVector(expected);
    testAggregations(
        {makeRowVector({inputVec})},
        {},
        {"aggregate_map_sum(c0)"},
        {makeRowVector({expectedVec})});
  }

  template <bool isDecimal, typename TValue>
  void testGroupBy(
      std::vector<int>& key,
      std::vector<std::vector<std::pair<StringView, std::optional<TValue>>>>
          input,
      std::vector<int>& expectedKey,
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>&
          expected,
      TypePtr decimalType = nullptr) {
    if constexpr (isDecimal) {
      BOLT_CHECK(decimalType);
    }

    auto keyVec = makeFlatVector(key);
    VectorPtr inputVec;
    if constexpr (isDecimal) {
      inputVec = makeMapVector(input, MAP(VARCHAR(), decimalType));
    } else {
      inputVec = makeMapVector(input);
    }
    auto expectedVec = makeMapVector(expected);
    auto expectedKeyVec = makeFlatVector(expectedKey);
    testAggregations(
        {makeRowVector({keyVec, inputVec})},
        {"c0"},
        {"aggregate_map_sum(c1)"},
        {makeRowVector({expectedKeyVec, expectedVec})});
  }

  template <bool isDecimal, TypeKind kind>
  auto convertTo(
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>
          input,
      TypePtr decimalType = nullptr)
      -> std::vector<std::vector<std::pair<
          StringView,
          std::optional<typename TypeTraits<kind>::NativeType>>>> {
    using TValue = typename TypeTraits<kind>::NativeType;
    if constexpr (isDecimal) {
      BOLT_CHECK(decimalType);
    }
    TValue scale = 1;
    if constexpr (isDecimal) {
      if constexpr (kind == TypeKind::BIGINT) {
        for (auto i = 0; i < decimalType->asShortDecimal().scale(); ++i) {
          scale *= 10;
        }
      } else {
        for (auto i = 0; i < decimalType->asLongDecimal().scale(); ++i) {
          scale *= 10;
        }
      }
    }
    auto ret = std::vector<
        std::vector<std::pair<StringView, std::optional<TValue>>>>{};

    for (auto map : input) {
      auto convertedMap =
          std::vector<std::pair<StringView, std::optional<TValue>>>{};
      for (auto [key, value] : map) {
        if (!value.has_value()) {
          auto p =
              std::pair<StringView, std::optional<TValue>>(key, std::nullopt);
          convertedMap.emplace_back(std::move(p));
          continue;
        }
        TValue convertedValue;
        if constexpr (isDecimal) {
          convertedValue = value.value() * scale;
        } else {
          convertedValue = util::Converter<kind>::cast(value.value(), nullptr);
          BOLT_CHECK_EQ(value.value(), convertedValue);
        }
        auto p = std::pair<StringView, std::optional<TValue>>(
            key, std::make_optional(convertedValue));
        convertedMap.emplace_back(std::move(p));
      }
      ret.push_back(std::move(convertedMap));
    }
    return ret;
  }

  auto getExpectedResult(
      const std::vector<
          std::vector<std::pair<StringView, std::optional<int64_t>>>>& input)
      -> std::vector<
          std::vector<std::pair<StringView, std::optional<int64_t>>>> {
    auto hashmap = std::unordered_map<StringView, int64_t>();
    for (const auto& map : input) {
      auto set = std::unordered_set<StringView>();
      for (const auto& [key, value] : map) {
        BOLT_CHECK(set.count(key) == 0);
        set.insert(key);
        if (!value.has_value())
          continue;
        hashmap[key] = functions::plus(hashmap[key], value.value());
      }
    }
    auto retMap = std::vector<std::pair<StringView, std::optional<int64_t>>>();
    for (auto [key, value] : hashmap) {
      retMap.emplace_back(key, value);
    }
    return {retMap};
  }

  auto getExpectedResult(
      const std::vector<
          std::vector<std::pair<StringView, std::optional<int64_t>>>>& input,
      const std::vector<int>& groupBy)
      -> std::pair<
          std::vector<int>,
          std::vector<
              std::vector<std::pair<StringView, std::optional<int64_t>>>>> {
    BOLT_CHECK(input.size() == groupBy.size());
    auto hashmap =
        std::unordered_map<int, std::unordered_map<StringView, int64_t>>();
    for (auto i = 0; i < groupBy.size(); ++i) {
      const auto& map = input[i];
      const auto& groupByKey = groupBy[i];
      auto set = std::unordered_set<StringView>();
      for (const auto& [key, value] : map) {
        BOLT_CHECK(set.count(key) == 0);
        set.insert(key);
        if (!value.has_value())
          continue;
        hashmap[groupByKey][key] = hashmap[groupByKey][key] + value.value();
      }
    }
    auto retGroupBy = std::vector<int>(hashmap.size());
    auto retMaps =
        std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>(
            hashmap.size());
    auto i = 0;
    for (auto [key, group] : hashmap) {
      retGroupBy[i] = key;
      auto retGroup =
          std::vector<std::pair<StringView, std::optional<int64_t>>>();
      for (auto [key, value] : group) {
        retGroup.emplace_back(key, value);
      }
      retMaps[i] = std::move(retGroup);
      ++i;
    }
    return {retGroupBy, retMaps};
  }

  void testGlobal(
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>
          input) {
    auto expected = getExpectedResult(input);

    testGlobal<false, int8_t>(
        convertTo<false, TypeKind::TINYINT>(input), expected);

    testGlobal<false, int16_t>(
        convertTo<false, TypeKind::SMALLINT>(input), expected);

    testGlobal<false, int32_t>(
        convertTo<false, TypeKind::INTEGER>(input), expected);

    testGlobal<false, int64_t>(
        convertTo<false, TypeKind::BIGINT>(input), expected);

    testGlobal<false, float>(convertTo<false, TypeKind::REAL>(input), expected);

    testGlobal<false, double>(
        convertTo<false, TypeKind::DOUBLE>(input), expected);

    testGlobal<true, int64_t>(
        convertTo<true, TypeKind::BIGINT>(input, decimalType1_),
        expected,
        decimalType1_);
  }

  void testGroupBy(
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>&
          input,
      std::vector<int> groupBy) {
    auto [expectedGroupBy, expected] = getExpectedResult(input, groupBy);

    testGroupBy<false, int8_t>(
        groupBy,
        convertTo<false, TypeKind::TINYINT>(input),
        expectedGroupBy,
        expected);

    testGroupBy<false, int16_t>(
        groupBy,
        convertTo<false, TypeKind::SMALLINT>(input),
        expectedGroupBy,
        expected);

    testGroupBy<false, int32_t>(
        groupBy,
        convertTo<false, TypeKind::INTEGER>(input),
        expectedGroupBy,
        expected);

    testGroupBy<false, int64_t>(
        groupBy,
        convertTo<false, TypeKind::BIGINT>(input),
        expectedGroupBy,
        expected);

    testGroupBy<false, float>(
        groupBy,
        convertTo<false, TypeKind::REAL>(input),
        expectedGroupBy,
        expected);

    testGroupBy<false, double>(
        groupBy,
        convertTo<false, TypeKind::DOUBLE>(input),
        expectedGroupBy,
        expected);

    testGroupBy<true, int64_t>(
        groupBy,
        convertTo<true, TypeKind::BIGINT>(input, decimalType1_),
        expectedGroupBy,
        expected,
        decimalType1_);
  }

 private:
  TypePtr decimalType1_;
  TypePtr decimalType2_;
  constexpr static auto valueMax = std::numeric_limits<int8_t>::max();
};

TEST_F(MapSumAggTest, simpleGlobal) {
  auto data =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {{"0", 1}}, {{"0", 1}}};

  testGlobal(data);
}

TEST_F(MapSumAggTest, simpleGroupBy) {
  auto data =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {{"0", 1}}, {{"0", 1}}, {{"1", -1}}, {{"1", -1}}};
  auto key = std::vector<int>{0, 0, 1, 1};

  testGroupBy(data, key);
}

TEST_F(MapSumAggTest, global) {
  auto numRows = 1000;
  auto data =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>(
          numRows);
  for (auto i = 0; i < numRows; ++i) {
    auto mapSize = i % 10;
    auto map =
        std::vector<std::pair<StringView, std::optional<int64_t>>>(mapSize);
    for (auto j = 0; j < mapSize; ++j) {
      auto string = std::to_string(j);
      auto stringView = StringView(string);
      BOLT_CHECK(stringView.isInline());
      auto val = i + j;
      val = val > 127 ? 127 : val;
      map[j] = {stringView, val};
    }
    data[i] = std::move(map);
  }
  BOLT_CHECK(data.size() == numRows);
  testGlobal(data);
}

TEST_F(MapSumAggTest, groupBy) {
  auto numRows = 1000;
  auto data =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>(
          numRows);
  for (auto i = 0; i < numRows; ++i) {
    auto mapSize = i % 10;
    auto map =
        std::vector<std::pair<StringView, std::optional<int64_t>>>(mapSize);
    for (auto j = 0; j < mapSize; ++j) {
      auto string = std::to_string(j);
      auto stringView = StringView(string);
      BOLT_CHECK(stringView.isInline());
      auto val = i + j;
      val = val > 127 ? 127 : val;
      map[j] = {stringView, val};
    }
    data[i] = std::move(map);
  }
  auto groupBy = std::vector<int>(numRows);
  for (auto i = 0; i < numRows; ++i) {
    groupBy[i] = i & 7;
  }
  testGroupBy(data, groupBy);
}

TEST_F(MapSumAggTest, overflow) {
  auto data =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {{"0", std::numeric_limits<int64_t>::max()}},
          {{"0", std::numeric_limits<int64_t>::max()}}};
  using Expected =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>;
  auto expected = Expected{{
      {"0",
       ::bytedance::bolt::kSparkCompatible
           ? int64_t{-2}
           : std::numeric_limits<int64_t>::max()},
  }};

  auto inputVec = makeMapVector(data);
  auto expectedVec = makeMapVector(expected);

  testAggregations(
      {makeRowVector({inputVec})},
      {},
      {"aggregate_map_sum(c0)"},
      {makeRowVector({expectedVec})});

  data =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {{"0", std::numeric_limits<int64_t>::min()}},
          {{"0", std::numeric_limits<int64_t>::min()}}};
  expected = Expected{{
      {"0",
       ::bytedance::bolt::kSparkCompatible
           ? int64_t{0}
           : std::numeric_limits<int64_t>::min()},
  }};
  inputVec = makeMapVector(data);
  expectedVec = makeMapVector(expected);

  testAggregations(
      {makeRowVector({inputVec})},
      {},
      {"aggregate_map_sum(c0)"},
      {makeRowVector({expectedVec})});
}

TEST_F(MapSumAggTest, castOverflow) {
  auto data =
      std::vector<std::vector<std::pair<StringView, std::optional<double>>>>{
          {{"0", std::numeric_limits<double>::max()}},
          {{"0", std::numeric_limits<double>::max()}}};
  using Expected =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>;
  auto expected = Expected{{
      {"0",
       ::bytedance::bolt::kSparkCompatible
           ? int64_t{-2}
           : std::numeric_limits<int64_t>::max()},
  }};

  auto inputVec = makeMapVector(data);
  auto expectedVec = makeMapVector(expected);

  testAggregations(
      {makeRowVector({inputVec})},
      {},
      {"aggregate_map_sum(c0)"},
      {makeRowVector({expectedVec})});

  data = std::vector<std::vector<std::pair<StringView, std::optional<double>>>>{
      {{"0", -std::numeric_limits<double>::max()}},
      {{"0", -std::numeric_limits<double>::max()}}};

  expected = Expected{{
      {"0",
       ::bytedance::bolt::kSparkCompatible
           ? int64_t{0}
           : std::numeric_limits<int64_t>::min()},
  }};

  inputVec = makeMapVector(data);
  expectedVec = makeMapVector(expected);

  testAggregations(
      {makeRowVector({inputVec})},
      {},
      {"aggregate_map_sum(c0)"},
      {makeRowVector({expectedVec})});
}

TEST_F(MapSumAggTest, emptyIfValueNull) {
  auto data =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {{"0", std::nullopt}}, {{"0", std::nullopt}}};

  auto expected =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {}};

  auto inputVec = makeMapVector(data);
  auto expectedVec = makeMapVector(expected);

  testAggregations(
      {makeRowVector({inputVec})},
      {},
      {"aggregate_map_sum(c0)"},
      {makeRowVector({expectedVec})});
}

TEST_F(MapSumAggTest, emptyIfSomeNull) {
  auto inputVec = makeNullableMapVector<std::string, int64_t>({
      std::nullopt,
      {{{"0", std::nullopt}}},
      {{{"0", std::nullopt}}},
  });
  auto expected =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {}};
  auto expectedVec = makeMapVector(expected);

  testAggregations(
      {makeRowVector({inputVec})},
      {},
      {"aggregate_map_sum(c0)"},
      {makeRowVector({expectedVec})});
}

TEST_F(MapSumAggTest, emptyIfAllNull) {
  auto inputVec = makeNullableMapVector<std::string, int64_t>({
      std::nullopt,
  });

  auto expected =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {}};
  auto expectedVec = makeMapVector(expected);

  testAggregations(
      {makeRowVector({inputVec})},
      {},
      {"aggregate_map_sum(c0)"},
      {makeRowVector({expectedVec})});
}

TEST_F(MapSumAggTest, castTruncate) {
  auto data =
      std::vector<std::vector<std::pair<StringView, std::optional<double>>>>{
          {{"0", 0.9}}};

  auto expected =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {{"0", 0}}};

  auto inputVec = makeMapVector(data);
  auto expectedVec = makeMapVector(expected);

  testAggregations(
      {makeRowVector({inputVec})},
      {},
      {"aggregate_map_sum(c0)"},
      {makeRowVector({expectedVec})});
}

TEST_F(MapSumAggTest, simpleStringValue) {
  auto data = std::vector<
      std::vector<std::pair<StringView, std::optional<StringView>>>>{
      {{"0", "1"}}, {{"0", "2"}}};
  auto expected = getExpectedResult(
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {{"0", 1}}, {{"0", 2}}});
  testGlobal<false, StringView>(data, expected);
}

TEST_F(MapSumAggTest, stringCastTruncate) {
  auto data = std::vector<
      std::vector<std::pair<StringView, std::optional<StringView>>>>{
      {{"0", "1.9"}}};
  auto expected =
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>{
          {{"0", 1}}};
  auto inputVec = makeMapVector(data);
  auto expectedVec = makeMapVector(expected);
  testAggregations(
      {makeRowVector({inputVec})},
      {},
      {"aggregate_map_sum(c0)"},
      {makeRowVector({expectedVec})});
}

}; // namespace
}; // namespace bytedance::bolt::aggregate::test
