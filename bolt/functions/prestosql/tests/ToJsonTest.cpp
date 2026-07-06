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
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
namespace bytedance::bolt::functions::sparksql::test {
namespace {

template <typename T>
using TwoDimVector = std::vector<std::vector<std::optional<T>>>;
using JsonNativeType = StringView;
template <typename TKey, typename TValue>
using Pair = std::pair<TKey, std::optional<TValue>>;

class ToJsonTest : public SparkFunctionBaseTest {
 protected:
  template <typename T = FlatVector<StringView>>
  void toJsonSimple(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters,
      const VectorPtr& expected,
      const bool expectThrow = false) {
    if (expectThrow) {
      EXPECT_THROW(
          evaluate<T>(expression, makeRowVector(parameters)), BoltRuntimeError);
    } else {
      auto result = evaluate<T>(expression, makeRowVector(parameters));
      ::bytedance::bolt::test::assertEqualVectors(expected, result);
    }
  }

  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  // Makes a row vector whose children vectors are wrapped in a dictionary
  // that reverses all elements and elements at the first row are null.
  template <typename T>
  RowVectorPtr makeRowWithDictionaryElements(
      const TwoDimVector<T>& elements,
      const TypePtr& rowType) {
    BOLT_CHECK_NE(elements.size(), 0, "At least one child must be provided.");

    int childrenSize = elements.size();
    int size = elements[0].size();

    std::vector<VectorPtr> dictChildren;
    for (int i = 0; i < childrenSize; ++i) {
      BOLT_CHECK_EQ(
          elements[i].size(),
          size,
          "All children vectors must have the same size.");
      dictChildren.push_back(
          makeDictionaryVector(elements[i], rowType->childAt(i)));
    }

    return std::make_shared<RowVector>(
        pool(), rowType, nullptr, size, dictChildren);
  }
};

TEST_F(ToJsonTest, fromArray) {
  {
    // Tests array of json elements.
    TwoDimVector<StringView> array{
        {"red"_sv, "blue"_sv}, {std::nullopt, std::nullopt, "purple"_sv}, {}};
    std::vector<std::optional<JsonNativeType>> expected{
        R"(["red","blue"])", R"([null,null,"purple"])", "[]"};
    auto arrayVector =
        makeNullableArrayVector<StringView>(array, ARRAY(VARCHAR()));
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {arrayVector}, expectedVector);
  }
  {
    // Tests array whose elements are of unknown type.
    auto arrayVector = makeArrayWithDictionaryElements<UnknownValue>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
        2,
        ARRAY(UNKNOWN()));
    auto expectedVector = makeNullableFlatVector<JsonNativeType>(
        {"[null,null]", "[null,null]"}, VARCHAR());
    toJsonSimple("to_json(c0)", {arrayVector}, expectedVector);
  }
  {
    // Tests array whose elements are wrapped in a dictionary.
    auto arrayVector =
        makeArrayWithDictionaryElements<int64_t>({1, -2, 3, -4, 5, -6, 7}, 2);
    auto expectedVector = makeNullableFlatVector<JsonNativeType>(
        {"[null,-6]", "[5,-4]", "[3,-2]", "[1]"}, VARCHAR());
    toJsonSimple("to_json(c0)", {arrayVector}, expectedVector);
  }
  {
    // Tests array whose elements are json and wrapped in a dictionary.
    auto arrayVector = makeArrayWithDictionaryElements<JsonNativeType>(
        {"a"_sv, "b"_sv, "c"_sv, "d"_sv, "e"_sv, "f"_sv, "g"_sv},
        2,
        ARRAY(JSON()));
    auto expectedVector = makeNullableFlatVector<JsonNativeType>(
        {"[null,f]", "[e,d]", "[c,b]", "[a]"}, VARCHAR());
    toJsonSimple("to_json(c0)", {arrayVector}, expectedVector);
  }
  {
    // Tests array vector with nulls at all rows.
    auto arrayVector = makeAllNullArrayVector(5, BIGINT());
    auto baseVector = makeNullableFlatVector<JsonNativeType>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
        VARCHAR());
    auto expectedVector =
        std::make_shared<ConstantVector<StringView>>(pool(), 5, 0, baseVector);
    toJsonSimple<ConstantVector<StringView>>(
        "to_json(c0)", {arrayVector}, expectedVector);
  }

#ifdef SPARK_COMPATIBLE
  // Test array with short decimal column
  std::vector<std::vector<int64_t>> decimalArray{
      {0, 100}, {123456, 1234567890}, {84059812, 1234567800}};
  auto arrayVector = makeArrayVector<int64_t>(decimalArray, DECIMAL(10, 5));
  std::vector<std::optional<JsonNativeType>> expected{
      R"([0.00000,0.00100])",
      R"([1.23456,12345.67890])",
      R"([840.59812,12345.67800])"};
  auto expectedVector =
      makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
  toJsonSimple("to_json(c0)", {arrayVector}, expectedVector);

  // Test array with long decimal column
  std::vector<std::vector<int128_t>> longDecimalArray{
      {0, 100}, {123456, 123456789112LL}, {84059812, 12345678000}};
  auto longArrayVector =
      makeArrayVector<int128_t>(longDecimalArray, DECIMAL(30, 10));
  std::vector<std::optional<JsonNativeType>> longExpected{
      R"([0E-10,1.00E-8])",
      R"([0.0000123456,12.3456789112])",
      R"([0.0084059812,1.2345678000])"};
  auto longExpectedVector =
      makeNullableFlatVector<JsonNativeType>(longExpected, VARCHAR());
  toJsonSimple("to_json(c0)", {longArrayVector}, longExpectedVector);
#endif
}

TEST_F(ToJsonTest, fromMap) {
  {
    // Tests map with string keys.
    std::vector<std::vector<Pair<StringView, int64_t>>> mapStringKey{
        {{"blue"_sv, 1}, {"red"_sv, 2}},
        {{"purple", std::nullopt}, {"orange"_sv, -2}},
        {}};
    std::vector<std::optional<JsonNativeType>> expectedStringKey{
        R"({"blue":1,"red":2})", R"({"purple":null,"orange":-2})", "{}"};
    auto mapVector = makeMapVector<StringView, int64_t>(
        mapStringKey, MAP(VARCHAR(), BIGINT()));
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expectedStringKey, VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
  {
    // Tests map with integer keys.
    std::vector<std::vector<Pair<int16_t, int64_t>>> mapIntKey{
        {{3, std::nullopt}, {4, 2}}, {}};
    std::vector<std::optional<JsonNativeType>> expectedIntKey{
        R"({"3":null,"4":2})", "{}"};
    auto mapVector =
        makeMapVector<int16_t, int64_t>(mapIntKey, MAP(SMALLINT(), BIGINT()));
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expectedIntKey, VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
  {
    // Tests map with floating-point keys.
    std::vector<std::vector<Pair<double, int64_t>>> mapDoubleKey{
        {{4.4, std::nullopt}, {3.3, 2}}, {}};
    std::vector<std::optional<JsonNativeType>> expectedDoubleKey{
        R"({"4.4":null,"3.3":2})", "{}"};
    auto mapVector =
        makeMapVector<double, int64_t>(mapDoubleKey, MAP(DOUBLE(), BIGINT()));
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expectedDoubleKey, VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
  {
    // Tests map with boolean keys.
    std::vector<std::vector<Pair<bool, int64_t>>> mapBoolKey{
        {{true, std::nullopt}, {false, 2}}, {}};
    std::vector<std::optional<JsonNativeType>> expectedBoolKey{
        R"({"true":null,"false":2})", "{}"};
    auto mapVector =
        makeMapVector<bool, int64_t>(mapBoolKey, MAP(BOOLEAN(), BIGINT()));
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expectedBoolKey, VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }

#ifdef SPARK_COMPATIBLE
  // Tests map with short decimal values.
  std::vector<std::vector<Pair<StringView, int64_t>>> maps{
      {{"a", 0}, {"b", 100}},
      {{"c", 123456}, {"d", 1234567890}},
      {{"e", 84059812}, {"f", 1234567800}}};
  std::vector<std::optional<JsonNativeType>> expected{
      R"({"a":0.00000,"b":0.00100})",
      R"({"c":1.23456,"d":12345.67890})",
      R"({"e":840.59812,"f":12345.67800})"};
  auto mapVector =
      makeMapVector<StringView, int64_t>(maps, MAP(VARCHAR(), DECIMAL(10, 5)));
  auto expectedVector =
      makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
  toJsonSimple("to_json(c0)", {mapVector}, expectedVector);

  // Tests map with long decimal values.
  std::vector<std::vector<Pair<StringView, int128_t>>> longDecimalMaps{
      {{"a", 0}, {"b", 100}},
      {{"c", 123456}, {"d", 123456789112LL}},
      {{"e", 84059812}, {"f", 12345678000}}};
  std::vector<std::optional<JsonNativeType>> longExpected{
      R"({"a":0E-10,"b":1.00E-8})",
      R"({"c":0.0000123456,"d":12.3456789112})",
      R"({"e":0.0084059812,"f":1.2345678000})"};
  auto longDecimalMapVector = makeMapVector<StringView, int128_t>(
      longDecimalMaps, MAP(VARCHAR(), DECIMAL(30, 10)));
  auto longExpectedVector =
      makeNullableFlatVector<JsonNativeType>(longExpected, VARCHAR());
  toJsonSimple("to_json(c0)", {longDecimalMapVector}, longExpectedVector);
#endif

  {
    // Tests map whose values are of unknown type.
    std::vector<std::optional<StringView>> keys{
        "a"_sv, "b"_sv, "c"_sv, "d"_sv, "e"_sv, "f"_sv, "g"_sv};
    std::vector<std::optional<UnknownValue>> unknownValues{
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt};
    auto mapVector = makeMapWithDictionaryElements<StringView, UnknownValue>(
        keys, unknownValues, 2, MAP(VARCHAR(), UNKNOWN()));

    auto expectedVector = makeNullableFlatVector<JsonNativeType>(
        {R"({"g":null,"f":null})",
         R"({"e":null,"d":null})",
         R"({"c":null,"b":null})",
         R"({"a":null})"},
        VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
  {
    // Tests map whose elements are wrapped in a dictionary.
    std::vector<std::optional<StringView>> keys{
        "a"_sv, "b"_sv, "c"_sv, "d"_sv, "e"_sv, "f"_sv, "g"_sv};
    std::vector<std::optional<double>> values{
        1.1e3, 2.2, 3.14e0, -4.4, std::nullopt, -6e-10, -7.7};
    auto mapVector = makeMapWithDictionaryElements(keys, values, 2);

    auto expectedVector = makeNullableFlatVector<JsonNativeType>(
        {R"({"g":null,"f":-6.0E-10})",
         R"({"e":null,"d":-4.4})",
         R"({"c":3.14,"b":2.2})",
         R"({"a":1100.0})"}, // 1100.0 consistent with java spark
        VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
  {
    // Tests map whose elements are json and wrapped in a dictionary.
    std::vector<std::optional<StringView>> keys{
        "a"_sv, "b"_sv, "c"_sv, "d"_sv, "e"_sv, "f"_sv, "g"_sv};
    std::vector<std::optional<double>> values{
        1.1e3, 2.2, 3.14e0, -4.4, std::nullopt, -6e-10, -7.7};
    auto mapVector =
        makeMapWithDictionaryElements(keys, values, 2, MAP(JSON(), DOUBLE()));
    auto expectedVector = makeNullableFlatVector<JsonNativeType>(
        {"{g:null,f:-6.0E-10}",
         "{e:null,d:-4.4}",
         "{c:3.14,b:2.2}",
         "{a:1100.0}"}, // 1100.0 consistent with java spark
        VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
  {
    // Tests map vector with nulls at all rows.
    auto mapVector = makeAllNullMapVector(5, VARCHAR(), BIGINT());
    auto baseVector = makeNullableFlatVector<JsonNativeType>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
        VARCHAR());
    auto expectedVector =
        std::make_shared<ConstantVector<StringView>>(pool(), 5, 0, baseVector);
    toJsonSimple<ConstantVector<StringView>>(
        "to_json(c0)", {mapVector}, expectedVector);
  }
}

TEST_F(ToJsonTest, fromRow) {
  {
    std::vector<std::optional<int64_t>> child1{
        std::nullopt, 2, 3, std::nullopt, 5, 6, std::nullopt, 8};
    std::vector<std::optional<StringView>> child2{
        "red"_sv,
        std::nullopt,
        "blue"_sv,
        std::nullopt,
        "yellow"_sv,
        std::nullopt,
        "yellow"_sv,
        "blue"_sv};
    std::vector<std::optional<double>> child3{
        1.1,
        2.2,
        std::nullopt,
        std::nullopt,
        5.5,
        1.0,
        0.0008547008547008547,
        0.0009165902841429881};
    auto firstChild = makeNullableFlatVector<int64_t>(child1, BIGINT());
    auto secondChild = makeNullableFlatVector<StringView>(child2, VARCHAR());
    auto thirdChild = makeNullableFlatVector<double>(child3, DOUBLE());

    auto rowVector = makeRowVector({firstChild, secondChild, thirdChild});

    std::vector<std::optional<JsonNativeType>> expected{
        R"({"c1":"red","c2":1.1})",
        R"({"c0":2,"c2":2.2})",
        R"({"c0":3,"c1":"blue"})",
        R"({})",
        R"({"c0":5,"c1":"yellow","c2":5.5})",
        R"({"c0":6,"c2":1.0})",
        R"({"c1":"yellow","c2":8.547008547008547E-4})",
        R"({"c0":8,"c1":"blue","c2":9.165902841429881E-4})"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());

    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }
  {
    // Tests row with json child column.
    std::vector<std::optional<int64_t>> child1{
        std::nullopt, 2, 3, std::nullopt, 5, 6, std::nullopt, 8};
    std::vector<std::optional<StringView>> child2{
        "red"_sv,
        std::nullopt,
        "blue"_sv,
        std::nullopt,
        "yellow"_sv,
        std::nullopt,
        "yellow"_sv,
        "blue"_sv};
    std::vector<std::optional<double>> child3{
        1.1,
        2.2,
        std::nullopt,
        std::nullopt,
        5.5,
        1.0,
        0.0008547008547008547,
        0.0009165902841429881};
    auto firstChild = makeNullableFlatVector<int64_t>(child1, BIGINT());
    auto secondChild = makeNullableFlatVector<StringView>(child2, JSON());
    auto thirdChild = makeNullableFlatVector<double>(child3, DOUBLE());

    auto rowVector = makeRowVector({firstChild, secondChild, thirdChild});

    std::vector<std::optional<JsonNativeType>> expected{
        R"({"c1":red,"c2":1.1})",
        R"({"c0":2,"c2":2.2})",
        R"({"c0":3,"c1":blue})",
        R"({})",
        R"({"c0":5,"c1":yellow,"c2":5.5})",
        R"({"c0":6,"c2":1.0})",
        R"({"c1":yellow,"c2":8.547008547008547E-4})",
        R"({"c0":8,"c1":blue,"c2":9.165902841429881E-4})"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }

#ifdef SPARK_COMPATIBLE
  // Test row with short decimal column
  auto child1 =
      makeFlatVector<StringView>({"a", "b", "c", "d", "e", "f"}, VARCHAR());
  auto child2 = makeFlatVector<int64_t>(
      {0, 100, 123456, 1234567890, 84059812, 1234567800}, DECIMAL(10, 5));
  auto rowVector = makeRowVector({child1, child2});
  std::vector<std::optional<JsonNativeType>> expected{
      R"({"c0":"a","c1":0.00000})",
      R"({"c0":"b","c1":0.00100})",
      R"({"c0":"c","c1":1.23456})",
      R"({"c0":"d","c1":12345.67890})",
      R"({"c0":"e","c1":840.59812})",
      R"({"c0":"f","c1":12345.67800})"};
  auto expectedVector =
      makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
  toJsonSimple("to_json(c0)", {rowVector}, expectedVector);

  // Test row with long decimal column
  auto longChild1 =
      makeFlatVector<StringView>({"a", "b", "c", "d", "e", "f"}, VARCHAR());
  auto longChild2 = makeFlatVector<int128_t>(
      {0, 100, 123456, 123456789112LL, 84059812, 12345678000}, DECIMAL(30, 10));
  auto longRowVector = makeRowVector({longChild1, longChild2});
  std::vector<std::optional<JsonNativeType>> longExpected{
      R"({"c0":"a","c1":0E-10})",
      R"({"c0":"b","c1":1.00E-8})",
      R"({"c0":"c","c1":0.0000123456})",
      R"({"c0":"d","c1":12.3456789112})",
      R"({"c0":"e","c1":0.0084059812})",
      R"({"c0":"f","c1":1.2345678000})"};
  auto longExpectedVector =
      makeNullableFlatVector<JsonNativeType>(longExpected, VARCHAR());
  toJsonSimple("to_json(c0)", {longRowVector}, longExpectedVector);
#endif

  // dorado id 111899126, date = 20240106
  {
    std::vector<std::optional<int64_t>> child1{7306871280580460812};
    std::vector<std::optional<StringView>> child2{"柯琳妮尔/奶盖水光精华水"_sv};
    std::vector<std::optional<StringView>> child3{
        "https://p9-ecom-spu.byteimg.com/tos-cn-i-89jsre2ap7/a6abfd28f7bf4375a341a5a629f289df~tplv-89jsre2ap7-image.image"_sv};
    std::vector<std::optional<int64_t>> child4{33800};
    std::vector<std::optional<int64_t>> child5{27165};
    std::vector<std::optional<std::vector<std::optional<int64_t>>>> child6{
        std::nullopt};
    std::vector<std::optional<int64_t>> child7{277};
    std::vector<std::optional<StringView>> child8{"20240106"_sv};
    auto firstChild = makeNullableFlatVector<int64_t>(child1, BIGINT());
    auto secondChild = makeNullableFlatVector<StringView>(child2, VARCHAR());
    auto thirdChild = makeNullableFlatVector<StringView>(child3, VARCHAR());
    auto fourthChild = makeNullableFlatVector<int64_t>(child4, BIGINT());
    auto fifthChild = makeNullableFlatVector<int64_t>(child5, BIGINT());
    auto sixthChild = makeNullableArrayVector<int64_t>(child6, ARRAY(BIGINT()));
    auto seventhChild = makeNullableFlatVector<int64_t>(child7, BIGINT());
    auto eighthChild = makeNullableFlatVector<StringView>(child8, VARCHAR());

    auto rowVector = makeRowVector(
        {firstChild,
         secondChild,
         thirdChild,
         fourthChild,
         fifthChild,
         sixthChild,
         seventhChild,
         eighthChild});

    std::vector<std::optional<JsonNativeType>> expected{
        R"({"c0":7306871280580460812,"c1":"柯琳妮尔/奶盖水光精华水","c2":"https://p9-ecom-spu.byteimg.com/tos-cn-i-89jsre2ap7/a6abfd28f7bf4375a341a5a629f289df~tplv-89jsre2ap7-image.image","c3":33800,"c4":27165,"c6":277,"c7":"20240106"})"};

    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());

    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }
  // dorado id 106862657, date = 20240508, error: Dictionary vs Row,
  // solution: set isDefaultNullBehavior() = true
  {
    const int kSize = 5;
    std::vector<std::optional<int64_t>> child1{1, 2, 3, 4, 5};
    std::vector<std::optional<StringView>> child2{
        "str1"_sv, "str2"_sv, "str3"_sv, "str4"_sv, "str5"_sv};
    auto a = makeNullableFlatVector<int64_t>(child1, BIGINT());
    auto b = makeNullableFlatVector<StringView>(child2, VARCHAR());

    BufferPtr nulls = AlignedBuffer::allocate<bool>(kSize, pool_.get());
    auto rawNulls = nulls->asMutable<uint64_t>();
    bits::setNull(rawNulls, 0, true);
    bits::setNull(rawNulls, 1, true);
    bits::setNull(rawNulls, 2, true);
    bits::setNull(rawNulls, 3, false);
    bits::setNull(rawNulls, 4, false);

    auto rowVector = std::make_shared<RowVector>(
        pool_.get(),
        ROW({{"a", a->type()}, {"b", b->type()}}),
        nulls,
        kSize,
        std::vector<VectorPtr>({a, b}));

    std::vector<std::optional<JsonNativeType>> expected{
        R"({"c1":{}})",
        R"({"c1":{}})",
        R"({"c1":{}})",
        R"({"c1":{"a":4,"b":"str4"}})",
        R"({"c1":{"a":5,"b":"str5"}})"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());

    toJsonSimple("to_json(row_constructor(c0))", {rowVector}, expectedVector);
  }
}

TEST_F(ToJsonTest, fromNested) {
  // Create map of array vector.
  auto keyVector = makeNullableFlatVector<StringView>(
      {"blue"_sv, "red"_sv, "green"_sv, "yellow"_sv, "purple"_sv, "orange"_sv},
      JSON());
  auto valueVector = makeNullableArrayVector<int64_t>(
      {{1, 2},
       {std::nullopt, 4},
       {std::nullopt, std::nullopt},
       {7, 8},
       {9, std::nullopt},
       {11, 12}});

  auto offsets = allocateOffsets(3, pool());
  auto sizes = allocateSizes(3, pool());
  makeOffsetsAndSizes(6, 2, offsets, sizes);

  auto nulls = makeNulls({false, true, false});

  auto mapVector = std::make_shared<MapVector>(
      pool(),
      MAP(JSON(), ARRAY(BIGINT())),
      nulls,
      3,
      offsets,
      sizes,
      keyVector,
      valueVector);

  // Create array of map vector
  std::vector<Pair<StringView, int64_t>> a{{"blue"_sv, 1}, {"red"_sv, 2}};
  std::vector<Pair<StringView, int64_t>> b{{"green"_sv, std::nullopt}};
  std::vector<Pair<StringView, int64_t>> c{{"yellow"_sv, 4}, {"purple"_sv, 5}};
  std::vector<std::vector<std::vector<Pair<StringView, int64_t>>>> data{
      {a, b}, {b}, {c, a}};

  auto arrayVector = makeArrayOfMapVector<StringView, int64_t>(data);

  // Create row vector of array of map and map of array
  auto rowVector = makeRowVector({mapVector, arrayVector});

  std::vector<std::optional<JsonNativeType>> expected{
      R"({"c0":{blue:[1,2],red:[null,4]},"c1":[{"blue":1,"red":2},{"green":null}]})",
      R"({"c1":[{"green":null}]})",
      R"({"c0":{purple:[9,null],orange:[11,12]},"c1":[{"yellow":4,"purple":5},{"blue":1,"red":2}]})"};
  auto expectedVector =
      makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
  toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
}

TEST_F(ToJsonTest, unsupportedTypes) {
  {
    auto inputVector = makeNullableFlatVector<int32_t>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt}, INTEGER());
    auto expectedVector = makeNullableFlatVector<JsonNativeType>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt}, VARCHAR());
    toJsonSimple("to_json(c0)", {inputVector}, expectedVector, true);
  }
}

TEST_F(ToJsonTest, fromSpark) {
  {
    // to_json escaping
    std::vector<std::optional<StringView>> child{"\"quote"_sv};
    auto rowChild = makeNullableFlatVector<StringView>(child, JSON());
    auto rowVector = makeRowVector({rowChild});
    std::vector<std::optional<JsonNativeType>> expected{R"({"c0":"quote})"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    // auto expectedVector = makeAllNullFlatVector<StringView>(1);
    // bolt can process key with escape char like \", but spark can't
    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }
  {
    // to_json - struct
    std::vector<std::optional<int64_t>> child{1};
    auto rowChild = makeNullableFlatVector<int64_t>(child, BIGINT());
    auto rowVector = makeRowVector({rowChild});
    std::vector<std::optional<JsonNativeType>> expected{R"({"c0":1})"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }
  {
    // to_json - array
    auto rowType = ROW({"a", "b"}, {INTEGER(), VARCHAR()});
    std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
        data = {
            {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
        };
    auto rowVector = makeArrayOfRowVector(data, rowType);
    std::vector<std::optional<JsonNativeType>> expected{
        R"([{"a":1,"b":"red"},{"a":2,"b":"blue"},{"a":3,"b":"green"}])"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }
  {
    // to_json - array with single empty row
    auto rowType = ROW({"a", "b"}, {INTEGER(), VARCHAR()});
    std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
        data = {
            {{std::nullopt}},
        };
    auto rowVector = makeArrayOfRowVector(data, rowType);
    std::vector<std::optional<JsonNativeType>> expected{R"([{}])"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }
  {
    // to_json - empty array
    TwoDimVector<StringView> array{{}};
    std::vector<std::optional<JsonNativeType>> expected{"[]"};
    auto arrayVector =
        makeNullableArrayVector<StringView>(array, ARRAY(VARCHAR()));
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {arrayVector}, expectedVector);
  }
  {
    // to_json null input column
    auto inputVector = makeAllNullFlatVector<StringView>(1);
    auto rowVector = makeRowVector({inputVector});
    rowVector->setNull(0, true);
    auto baseVector = makeAllNullFlatVector<StringView>(1);
    auto expectedVector =
        std::make_shared<ConstantVector<StringView>>(pool(), 1, 0, baseVector);
    toJsonSimple<ConstantVector<StringView>>(
        "to_json(c0)", {rowVector}, expectedVector);
  }
  {
    // to_json with timestamp
    auto rowChild = makeNullableFlatVector<Timestamp>({Timestamp(1, 1)});
    auto rowVector = makeRowVector({rowChild});
    auto expectedVector = makeNullableFlatVector<StringView>(
        {R"({"c0":"1970-01-01T00:00:01.000Z"})"}, VARCHAR());
    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);

    setQueryTimeZone("Asia/Shanghai");
    rowChild = makeNullableFlatVector<Timestamp>({Timestamp(1, 1)});
    rowVector = makeRowVector({rowChild});
    expectedVector = makeNullableFlatVector<StringView>(
        {R"({"c0":"1970-01-01T08:00:01.000+08:00"})"}, VARCHAR());
    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }
  {
    // SPARK-21513: to_json support map[string, struct] to json
    constexpr vector_size_t size = 3;
    auto valuesVector = makeRowVector({
        makeFlatVector<int64_t>(size, [](auto row) { return row; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
    });
    std::vector<std::optional<JsonNativeType>> key{"a", "b", "c"};
    auto keysVector = makeNullableFlatVector<JsonNativeType>(key, VARCHAR());
    std::vector<vector_size_t> offsets;
    for (auto i = 0; i < size; i++) {
      offsets.push_back(i);
    }
    auto mapVector = makeMapVector(offsets, keysVector, valuesVector);

    std::vector<std::optional<JsonNativeType>> expected{
        R"({"a":{"c0":0,"c1":0}})",
        R"({"b":{"c0":1,"c1":1}})",
        R"({"c":{"c0":2,"c1":2}})"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
  {
    // SPARK-21513: to_json support map[struct, struct] to json
    // bolt doesn't support struct type key currently, so will throw exception
    // when running
    constexpr vector_size_t size = 3;
    auto valuesVector = makeRowVector({
        makeFlatVector<int64_t>(size, [](auto row) { return row; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
    });
    auto keysVector = makeRowVector({
        makeFlatVector<int64_t>(size, [](auto row) { return row; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
    });
    std::vector<vector_size_t> offsets;
    for (auto i = 0; i < size; i++) {
      offsets.push_back(i);
    }
    auto mapVector = makeMapVector(offsets, keysVector, valuesVector);
    toJsonSimple("to_json(c0)", {mapVector}, nullptr, /*expectThrow=*/true);
  }
  {
    // SPARK-21513: to_json support map[string, integer] to json
    std::vector<std::vector<Pair<StringView, int32_t>>> mapStringKey{
        {{"a"_sv, 1}}};
    auto mapVector = makeMapVector<StringView, int32_t>(
        mapStringKey, MAP(VARCHAR(), INTEGER()));

    std::vector<std::optional<JsonNativeType>> expectedStringKey{R"({"a":1})"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expectedStringKey, VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
  {
    // to_json - array with maps
    std::vector<Pair<StringView, int64_t>> a{{"blue"_sv, 1}, {"red"_sv, 2}};
    std::vector<std::vector<std::vector<Pair<StringView, int64_t>>>> data{{a}};
    auto arrayVector = makeArrayOfMapVector<StringView, int64_t>(data);

    std::vector<std::optional<JsonNativeType>> expected{
        R"([{"blue":1,"red":2}])"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {arrayVector}, expectedVector);
  }
  {
    // to_json - array with single map
    std::vector<Pair<StringView, int64_t>> a{{"blue"_sv, 1}};
    std::vector<std::vector<std::vector<Pair<StringView, int64_t>>>> data{{a}};
    auto arrayVector = makeArrayOfMapVector<StringView, int64_t>(data);

    std::vector<std::optional<JsonNativeType>> expected{R"([{"blue":1}])"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {arrayVector}, expectedVector);
  }
}

TEST_F(ToJsonTest, doesntSortKeyInMap) {
  {
    // Tests map with string keys.
    std::vector<std::vector<Pair<StringView, int64_t>>> mapStringKey{
        {{"cccc"_sv, 1}, {"aaaa"_sv, 2}},
        {{"zzzz", std::nullopt}, {"aaaa"_sv, -2}},
        {}};
    std::vector<std::optional<JsonNativeType>> expectedStringKey{
        R"({"cccc":1,"aaaa":2})", R"({"zzzz":null,"aaaa":-2})", "{}"};
    auto mapVector = makeMapVector<StringView, int64_t>(
        mapStringKey, MAP(VARCHAR(), BIGINT()));
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expectedStringKey, VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
  {
    // Tests map with integer keys.
    std::vector<std::vector<Pair<int16_t, int64_t>>> mapIntKey{
        {{4, std::nullopt}, {3, 2}}, {}};
    std::vector<std::optional<JsonNativeType>> expectedIntKey{
        R"({"4":null,"3":2})", "{}"};
    auto mapVector =
        makeMapVector<int16_t, int64_t>(mapIntKey, MAP(SMALLINT(), BIGINT()));
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expectedIntKey, VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
}

TEST_F(ToJsonTest, NaN) {
  constexpr double nan = std::numeric_limits<double>::quiet_NaN();
  {
    // NaN - map
    std::vector<std::vector<Pair<double, double>>> mapStringKey{
        {{1, nan}, {2, 2}}, {{nan, nan}, {4, nan}}, {}};
    std::vector<std::optional<JsonNativeType>> expectedStringKey{
        R"({"1.0":"NaN","2.0":2.0})", R"({"NaN":"NaN","4.0":"NaN"})", "{}"};
    auto mapVector =
        makeMapVector<double, double>(mapStringKey, MAP(DOUBLE(), DOUBLE()));
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expectedStringKey, VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }

  {
    // NaN - struct
    std::vector<std::optional<double>> child{1, nan};
    auto rowChild = makeNullableFlatVector<double>(child, DOUBLE());
    auto rowVector = makeRowVector({rowChild});
    std::vector<std::optional<JsonNativeType>> expected{
        R"({"c0":1.0})", R"({"c0":"NaN"})"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }
  {
    // to_json - array
    auto rowType = ROW({"a", "b"}, {DOUBLE(), VARCHAR()});
    std::vector<std::vector<std::optional<std::tuple<double, std::string>>>>
        data = {
            {{{1, "red"}}, {{nan, "blue"}}, {{nan, "green"}}},
        };
    auto rowVector = makeArrayOfRowVector(data, rowType);
    std::vector<std::optional<JsonNativeType>> expected{
        R"([{"a":1.0,"b":"red"},{"a":"NaN","b":"blue"},{"a":"NaN","b":"green"}])"};
    auto expectedVector =
        makeNullableFlatVector<JsonNativeType>(expected, VARCHAR());
    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }
}

TEST_F(ToJsonTest, varbinary) {
  {
    // Tests array with varbinary element.
    auto arrayVector = makeNullableArrayVector<StringView>(
        {{"key1"_sv, "value1"_sv},
         {"key2"_sv, "value2"_sv},
         {"key3"_sv, "value3"_sv}},
        ARRAY(VARBINARY()));
    auto expectedVector = makeNullableFlatVector<JsonNativeType>(
        {R"(["a2V5MQ==","dmFsdWUx"])",
         R"(["a2V5Mg==","dmFsdWUy"])",
         R"(["a2V5Mw==","dmFsdWUz"])"},
        VARCHAR());
    toJsonSimple("to_json(c0)", {arrayVector}, expectedVector);
  }
  {
    // Tests map with string key and varbinary value.
    auto mapVector = makeMapVector<StringView, StringView>(
        {{{"key1"_sv, "value1"_sv}},
         {{"key2"_sv, "value2"_sv}},
         {{"key3"_sv, "value3"_sv}},
         {}},
        MAP(VARCHAR(), VARBINARY()));
    auto expectedVector = makeNullableFlatVector<JsonNativeType>(
        {R"({"key1":"dmFsdWUx"})",
         R"({"key2":"dmFsdWUy"})",
         R"({"key3":"dmFsdWUz"})",
         "{}"},
        VARCHAR());
    toJsonSimple("to_json(c0)", {mapVector}, expectedVector);
  }
  {
    // Tests row with varbinary column.
    auto firstChild = makeNullableFlatVector<StringView>(
        {"key1"_sv, "key2"_sv, "key3"_sv}, VARBINARY());
    auto secondChild = makeNullableFlatVector<StringView>(
        {"value1"_sv, "value2"_sv, "value3"_sv}, VARBINARY());
    auto rowVector = makeRowVector({firstChild, secondChild});
    auto expectedVector = makeNullableFlatVector<JsonNativeType>(
        {R"({"c0":"a2V5MQ==","c1":"dmFsdWUx"})",
         R"({"c0":"a2V5Mg==","c1":"dmFsdWUy"})",
         R"({"c0":"a2V5Mw==","c1":"dmFsdWUz"})"},
        VARCHAR());
    toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
  }
}

TEST_F(ToJsonTest, allDataType) {
  auto c0 = makeFlatVector<int8_t>(std::vector<int8_t>{127}, TINYINT());
  auto c1 = makeFlatVector<int16_t>(std::vector<int16_t>{32767}, SMALLINT());
  auto c2 =
      makeFlatVector<int32_t>(std::vector<int32_t>{2147483647}, INTEGER());
  auto c3 = makeFlatVector<int64_t>(
      std::vector<int64_t>{9223372036854775807}, BIGINT());
  auto c4 = makeFlatVector<float>(std::vector<float>{1.23E7}, REAL());
  auto c5 = makeFlatVector<double>(std::vector<double>{1.23456789}, DOUBLE());
  auto c6 =
      makeFlatVector<int64_t>(std::vector<int64_t>{123456}, DECIMAL(10, 2));
  auto c7 = makeFlatVector<int128_t>(
      std::vector<int128_t>{922337203685478}, DECIMAL(38, 10));
  auto c8 = makeFlatVector<StringView>(
      std::vector<StringView>{"example string"_sv}, VARCHAR());
  auto c9 = makeFlatVector<StringView>(
      std::vector<StringView>{"abc"_sv}, VARBINARY());
  auto c10 = makeFlatVector<bool>(std::vector<bool>{true}, BOOLEAN());
  auto c11 = makeArrayVector<int8_t>({{1, 2}}, TINYINT());
  auto c12 = makeArrayVector<int16_t>({{3, 4}}, SMALLINT());
  auto c13 = makeArrayVector<int32_t>({{5, 6}}, INTEGER());
  auto c14 = makeArrayVector<int64_t>({{7, 8}}, BIGINT());
  auto c15 = makeArrayVector<float>({{1.1, 2.2}}, REAL());
  auto c16 = makeArrayVector<double>({{3.3, 4.4}}, DOUBLE());
  auto c17 = makeArrayVector<int64_t>({{5555, 6666}}, DECIMAL(10, 2));
  auto c18 = makeArrayVector<int128_t>(
      {{922337203685477, 922337203685478}}, DECIMAL(38, 10));
  auto c19 =
      makeArrayVector<StringView>({{"string1"_sv, "string2"_sv}}, VARCHAR());
  auto c20 =
      makeArrayVector<StringView>({{"abcd"_sv, "abcde"_sv}}, VARBINARY());
  auto c21 = makeArrayVector<bool>({{true, false}}, BOOLEAN());
  auto c22 = makeMapVector<StringView, int8_t>(
      {{{"key1"_sv, 1}, {"key2"_sv, 2}}}, MAP(VARCHAR(), TINYINT()));
  auto c23 = makeMapVector<StringView, int16_t>(
      {{{"key1"_sv, 3}, {"key2"_sv, 4}}}, MAP(VARCHAR(), SMALLINT()));
  auto c24 = makeMapVector<StringView, int32_t>(
      {{{"key1"_sv, 5}, {"key2"_sv, 6}}}, MAP(VARCHAR(), INTEGER()));
  auto c25 = makeMapVector<StringView, int64_t>(
      {{{"key1"_sv, 9223372036854775806}, {"key2"_sv, 9223372036854775807}}},
      MAP(VARCHAR(), BIGINT()));
  auto c26 = makeMapVector<StringView, float>(
      {{{"key1"_sv, 1.1}, {"key2"_sv, 2.2}}}, MAP(VARCHAR(), REAL()));
  auto c27 = makeMapVector<StringView, double>(
      {{{"key1"_sv, 3.3}, {"key2"_sv, 4.4}}}, MAP(VARCHAR(), DOUBLE()));
  auto c28 = makeMapVector<StringView, int64_t>(
      {{{"key1"_sv, 7778}, {"key2"_sv, 8889}}}, MAP(VARCHAR(), DECIMAL(10, 2)));
  auto c29 = makeMapVector<StringView, int128_t>(
      {{{"key1"_sv, 922337203685477}, {"key2"_sv, 922337203685478}}},
      MAP(VARCHAR(), DECIMAL(38, 10)));
  auto c30 = makeMapVector<StringView, StringView>(
      {{{"key1"_sv, "value1"_sv}, {"key2"_sv, "value2"_sv}}},
      MAP(VARCHAR(), VARCHAR()));
  auto c31 = makeMapVector<StringView, StringView>(
      {{{"key1"_sv, "abcd"_sv}, {"key2"_sv, "abcde"_sv}}},
      MAP(VARCHAR(), VARBINARY()));
  auto c32 = makeMapVector<StringView, bool>(
      {{{"key1"_sv, true}, {"key2"_sv, false}}}, MAP(VARCHAR(), BOOLEAN()));
  auto c33 =
      makeNullableFlatVector<int32_t>({DATE()->toDays("2025-04-08")}, DATE());

  auto rowVector =
      makeRowVector({c0,  c1,  c2,  c3,  c4,  c5,  c6,  c7,  c8,  c9,  c10, c11,
                     c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23,
                     c24, c25, c26, c27, c28, c29, c30, c31, c32, c33});

  auto expectedVector = makeNullableFlatVector<JsonNativeType>(
      {R"({"c0":127,"c1":32767,"c2":2147483647,"c3":9223372036854775807,"c4":1.23E7,"c5":1.23456789,"c6":1234.56,"c7":92233.7203685478,"c8":"example string","c9":"YWJj","c10":true,"c11":[1,2],"c12":[3,4],"c13":[5,6],"c14":[7,8],"c15":[1.1,2.2],"c16":[3.3,4.4],"c17":[55.55,66.66],"c18":[92233.7203685477,92233.7203685478],"c19":["string1","string2"],"c20":["YWJjZA==","YWJjZGU="],"c21":[true,false],"c22":{"key1":1,"key2":2},"c23":{"key1":3,"key2":4},"c24":{"key1":5,"key2":6},"c25":{"key1":9223372036854775806,"key2":9223372036854775807},"c26":{"key1":1.1,"key2":2.2},"c27":{"key1":3.3,"key2":4.4},"c28":{"key1":77.78,"key2":88.89},"c29":{"key1":92233.7203685477,"key2":92233.7203685478},"c30":{"key1":"value1","key2":"value2"},"c31":{"key1":"YWJjZA==","key2":"YWJjZGU="},"c32":{"key1":true,"key2":false},"c33":"2025-04-08"})"},
      VARCHAR());
  toJsonSimple("to_json(c0)", {rowVector}, expectedVector);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
