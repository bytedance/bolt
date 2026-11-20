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

#include "bolt/substrait/tests/JsonToProtoConverter.h"

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"

#include "bolt/core/QueryCtx.h"
#include "bolt/substrait/SubstraitToBoltExpr.h"
#include "bolt/substrait/SubstraitToBoltPlan.h"
#include "bolt/substrait/TypeUtils.h"
#include "bolt/substrait/VariantToVectorConverter.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::substrait;
namespace vestrait = bytedance::bolt::substrait;

class FunctionTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::shared_ptr<core::QueryCtx> queryCtx_ = core::QueryCtx::create();

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();

  std::shared_ptr<vestrait::SubstraitParser> substraitParser_ =
      std::make_shared<vestrait::SubstraitParser>();

  std::shared_ptr<vestrait::SubstraitBoltPlanConverter> planConverter_ =
      std::make_shared<vestrait::SubstraitBoltPlanConverter>(pool_.get());
};

TEST_F(FunctionTest, makeNames) {
  std::string prefix = "n";
  int size = 0;
  std::vector<std::string> names = substraitParser_->makeNames(prefix, size);
  ASSERT_EQ(names.size(), size);

  size = 5;
  names = substraitParser_->makeNames(prefix, size);
  ASSERT_EQ(names.size(), size);
  for (int i = 0; i < size; i++) {
    std::string expected = "n_" + std::to_string(i);
    ASSERT_EQ(names[i], expected);
  }
}

TEST_F(FunctionTest, makeNodeName) {
  std::string nodeName = substraitParser_->makeNodeName(1, 0);
  ASSERT_EQ(nodeName, "n1_0");
}

TEST_F(FunctionTest, getIdxFromNodeName) {
  std::string nodeName = "n1_0";
  int index = substraitParser_->getIdxFromNodeName(nodeName);
  ASSERT_EQ(index, 0);
}

TEST_F(FunctionTest, getNameBeforeDelimiter) {
  std::string functionSpec = "lte:fp64_fp64";
  std::string_view funcName = getNameBeforeDelimiter(functionSpec, ":");
  ASSERT_EQ(funcName, "lte");

  functionSpec = "lte:";
  funcName = getNameBeforeDelimiter(functionSpec, ":");
  ASSERT_EQ(funcName, "lte");

  functionSpec = "lte";
  funcName = getNameBeforeDelimiter(functionSpec, ":");
  ASSERT_EQ(funcName, "lte");
}

TEST_F(FunctionTest, constructFunctionMap) {
  std::string planPath = getDataFilePath("data/q1_first_stage.json");
  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(planPath, substraitPlan);
  planConverter_->constructFunctionMap(substraitPlan);

  auto functionMap = planConverter_->getFunctionMap();
  ASSERT_EQ(functionMap.size(), 9);

  std::string function = planConverter_->findFunction(1);
  ASSERT_EQ(function, "lte:fp64_fp64");

  function = planConverter_->findFunction(2);
  ASSERT_EQ(function, "and:bool_bool");

  function = planConverter_->findFunction(3);
  ASSERT_EQ(function, "subtract:opt_fp64_fp64");

  function = planConverter_->findFunction(4);
  ASSERT_EQ(function, "multiply:opt_fp64_fp64");

  function = planConverter_->findFunction(5);
  ASSERT_EQ(function, "add:opt_fp64_fp64");

  function = planConverter_->findFunction(6);
  ASSERT_EQ(function, "sum:opt_fp64");

  function = planConverter_->findFunction(7);
  ASSERT_EQ(function, "count:opt_fp64");

  function = planConverter_->findFunction(8);
  ASSERT_EQ(function, "count:opt_i32");

  function = planConverter_->findFunction(9);
  ASSERT_EQ(function, "is_not_null:fp64");
}

TEST_F(FunctionTest, setVectorFromVariants) {
  auto resultVec = setVectorFromVariants(
      BOOLEAN(), {variant(false), variant(true)}, pool_.get());
  ASSERT_EQ(false, resultVec->asFlatVector<bool>()->valueAt(0));
  ASSERT_EQ(true, resultVec->asFlatVector<bool>()->valueAt(1));

  auto min8 = std::numeric_limits<int8_t>::min();
  auto max8 = std::numeric_limits<int8_t>::max();
  resultVec = setVectorFromVariants(
      TINYINT(), {variant(min8), variant(max8)}, pool_.get());
  EXPECT_EQ(min8, resultVec->asFlatVector<int8_t>()->valueAt(0));
  EXPECT_EQ(max8, resultVec->asFlatVector<int8_t>()->valueAt(1));

  auto min16 = std::numeric_limits<int16_t>::min();
  auto max16 = std::numeric_limits<int16_t>::max();
  resultVec = setVectorFromVariants(
      SMALLINT(), {variant(min16), variant(max16)}, pool_.get());
  EXPECT_EQ(min16, resultVec->asFlatVector<int16_t>()->valueAt(0));
  EXPECT_EQ(max16, resultVec->asFlatVector<int16_t>()->valueAt(1));

  auto min32 = std::numeric_limits<int32_t>::min();
  auto max32 = std::numeric_limits<int32_t>::max();
  resultVec = setVectorFromVariants(
      INTEGER(), {variant(min32), variant(max32)}, pool_.get());
  EXPECT_EQ(min32, resultVec->asFlatVector<int32_t>()->valueAt(0));
  EXPECT_EQ(max32, resultVec->asFlatVector<int32_t>()->valueAt(1));

  auto min64 = std::numeric_limits<int64_t>::min();
  auto max64 = std::numeric_limits<int64_t>::max();
  resultVec = setVectorFromVariants(
      BIGINT(), {variant(min64), variant(max64)}, pool_.get());
  EXPECT_EQ(min64, resultVec->asFlatVector<int64_t>()->valueAt(0));
  EXPECT_EQ(max64, resultVec->asFlatVector<int64_t>()->valueAt(1));

  // Floats are harder to compare because of low-precision. Just making sure
  // they don't throw.
  EXPECT_NO_THROW(setVectorFromVariants(
      REAL(), {variant(float(0.99L)), variant(float(-1.99L))}, pool_.get()));

  resultVec = setVectorFromVariants(
      DOUBLE(), {variant(double(0.99L)), variant(double(-1.99L))}, pool_.get());
  ASSERT_EQ(double(0.99L), resultVec->asFlatVector<double>()->valueAt(0));
  ASSERT_EQ(double(-1.99L), resultVec->asFlatVector<double>()->valueAt(1));

  resultVec = setVectorFromVariants(
      VARCHAR(), {variant(""), variant("asdf")}, pool_.get());
  ASSERT_EQ("", resultVec->asFlatVector<StringView>()->valueAt(0).str());
  ASSERT_EQ("asdf", resultVec->asFlatVector<StringView>()->valueAt(1).str());

  resultVec = setVectorFromVariants(
      VARBINARY(), {variant(""), variant("asdf")}, pool_.get());
  ASSERT_EQ("", resultVec->asFlatVector<StringView>()->valueAt(0).str());
  ASSERT_EQ("asdf", resultVec->asFlatVector<StringView>()->valueAt(1).str());

  resultVec = setVectorFromVariants(
      TIMESTAMP(),
      {variant(Timestamp(9020, 0)), variant(Timestamp(8875, 0))},
      pool_.get());
  ASSERT_EQ(
      "1970-01-01 02:30:20.000000000",
      resultVec->asFlatVector<Timestamp>()->valueAt(0).toString());
  ASSERT_EQ(
      "1970-01-01 02:27:55.000000000",
      resultVec->asFlatVector<Timestamp>()->valueAt(1).toString());

  resultVec = setVectorFromVariants(
      DATE(), {variant(9020), variant(8875)}, pool_.get());
  ASSERT_EQ(
      "1994-09-12",
      DATE()->toString(resultVec->asFlatVector<int32_t>()->valueAt(0)));
  ASSERT_EQ(
      "1994-04-20",
      DATE()->toString(resultVec->asFlatVector<int32_t>()->valueAt(1)));

  resultVec = setVectorFromVariants(
      INTERVAL_DAY_TIME(), {variant(9020LL), variant(8875LL)}, pool_.get());
  ASSERT_TRUE(resultVec->type()->isIntervalDayTime());
  ASSERT_EQ(9020, resultVec->asFlatVector<int64_t>()->valueAt(0));
  ASSERT_EQ(8875, resultVec->asFlatVector<int64_t>()->valueAt(1));

  resultVec = setVectorFromVariants(
      INTERVAL_YEAR_MONTH(), {variant(20), variant(30)}, pool_.get());
  ASSERT_TRUE(resultVec->type()->isIntervalYearMonth());
  ASSERT_EQ(20, resultVec->asFlatVector<int32_t>()->valueAt(0));
  ASSERT_EQ(30, resultVec->asFlatVector<int32_t>()->valueAt(1));
}

TEST_F(FunctionTest, getFunctionType) {
  std::vector<std::string> types =
      SubstraitParser::getSubFunctionTypes("sum:opt_i32");
  ASSERT_EQ("i32", types[0]);

  types = SubstraitParser::getSubFunctionTypes("sum:i32");
  ASSERT_EQ("i32", types[0]);

  types = SubstraitParser::getSubFunctionTypes("split:req_str_str");
  ASSERT_EQ(2, types.size());
  ASSERT_EQ("str", types[0]);
  ASSERT_EQ("str", types[1]);
}

TEST_F(FunctionTest, getInputTypes) {
  std::vector<TypePtr> types = SubstraitParser::getInputTypes("sum:opt_i32");
  ASSERT_EQ(types[0]->kind(), TypeKind::INTEGER);

  types = SubstraitParser::getInputTypes("and:opt_bool_bool");
  ASSERT_EQ(2, types.size());
  ASSERT_EQ(types[0]->kind(), TypeKind::BOOLEAN);
  ASSERT_EQ(types[1]->kind(), TypeKind::BOOLEAN);

  types = SubstraitParser::getInputTypes("function:i32_str_ts_date_fp64");
  ASSERT_EQ(types[0]->kind(), TypeKind::INTEGER);
  ASSERT_EQ(types[1]->kind(), TypeKind::VARCHAR);
  ASSERT_EQ(types[2]->kind(), TypeKind::TIMESTAMP);
  ASSERT_EQ(types[3]->kind(), TypeKind::INTEGER);
  ASSERT_EQ(types[4]->kind(), TypeKind::DOUBLE);
}

TEST_F(FunctionTest, timestampLiterals) {
  SubstraitBoltExprConverter converter(pool_.get(), substraitParser_);
  struct TestCase {
    int32_t precision;
    int64_t value;
    Timestamp expected;
  };
  const std::vector<TestCase> testCases{
      {0, 1, Timestamp(1, 0)},
      {0, -1, Timestamp(-1, 0)},
      {3, 1234, Timestamp(1, 234'000'000)},
      {3, -1234, Timestamp(-2, 766'000'000)},
      {6, 1, Timestamp(0, 1'000)},
      {6, -1, Timestamp(-1, 999'999'000)},
      {9, 1'000'000'001, Timestamp(1, 1)},
      {9, -1, Timestamp(-1, 999'999'999)},
      {9,
       std::numeric_limits<int64_t>::min(),
       Timestamp::fromNanos(std::numeric_limits<int64_t>::min())},
      {9,
       std::numeric_limits<int64_t>::max(),
       Timestamp::fromNanos(std::numeric_limits<int64_t>::max())}};
  for (const auto& test : testCases) {
    SCOPED_TRACE(fmt::format("{} / {}", test.precision, test.value));
    ::substrait::Expression::Literal literal;
    literal.mutable_precision_timestamp()->set_precision(test.precision);
    literal.mutable_precision_timestamp()->set_value(test.value);
    auto expression = converter.toBoltExpr(literal);
    EXPECT_EQ(expression->value().value<Timestamp>(), test.expected);

    ::substrait::Expression::Literal list;
    *list.mutable_list()->add_values() = literal;
    list.mutable_list()->add_values()->mutable_null()->mutable_timestamp();
    expression = converter.toBoltExpr(list);
    const auto* array =
        expression->valueVector()->wrappedVector()->as<ArrayVector>();
    EXPECT_EQ(
        array->elements()->asFlatVector<Timestamp>()->valueAt(0),
        test.expected);
    EXPECT_TRUE(array->elements()->isNullAt(1));
  }

  ::substrait::Expression::Literal literal;
  literal.set_timestamp(-1);
  EXPECT_EQ(
      converter.toBoltExpr(literal)->value().value<Timestamp>(),
      Timestamp(-1, 999'999'000));
  for (const auto precision : {-1, 10, 12}) {
    literal.mutable_precision_timestamp()->set_precision(precision);
    BOLT_ASSERT_THROW(converter.toBoltExpr(literal), "Timestamp precision");
  }
  literal.mutable_precision_timestamp()->set_precision(0);
  literal.mutable_precision_timestamp()->set_value(
      std::numeric_limits<int64_t>::max());
  BOLT_ASSERT_THROW(
      converter.toBoltExpr(literal), "Timestamp seconds out of range");
}

TEST_F(FunctionTest, typedEmptyCollectionLiterals) {
  SubstraitBoltExprConverter converter(pool_.get(), substraitParser_);
  ::substrait::Expression::Literal emptyList;
  emptyList.mutable_empty_list()->mutable_type()->mutable_binary();
  auto expression = converter.toBoltExpr(emptyList);
  EXPECT_EQ(*expression->type(), *ARRAY(VARBINARY()));
  const auto* array =
      expression->valueVector()->wrappedVector()->as<ArrayVector>();
  ASSERT_NE(array->elements(), nullptr);
  EXPECT_EQ(array->elements()->size(), 0);
  EXPECT_EQ(array->offsetAt(0), 0);
  EXPECT_EQ(array->sizeAt(0), 0);
  EXPECT_FALSE(array->isNullAt(0));

  ::substrait::Expression::Literal nestedList;
  *nestedList.mutable_list()->add_values() = emptyList;
  nestedList.mutable_list()
      ->add_values()
      ->mutable_list()
      ->add_values()
      ->set_binary("value");
  expression = converter.toBoltExpr(nestedList);
  EXPECT_EQ(*expression->type(), *ARRAY(ARRAY(VARBINARY())));
  array = expression->valueVector()->wrappedVector()->as<ArrayVector>();
  const auto* nestedArray = array->elements()->as<ArrayVector>();
  EXPECT_EQ(nestedArray->sizeAt(0), 0);
  EXPECT_EQ(nestedArray->sizeAt(1), 1);
  EXPECT_EQ(
      nestedArray->elements()->asFlatVector<StringView>()->valueAt(0).str(),
      "value");

  ::substrait::Expression::Literal emptyMap;
  emptyMap.mutable_empty_map()->mutable_key()->mutable_i64();
  emptyMap.mutable_empty_map()
      ->mutable_value()
      ->mutable_list()
      ->mutable_type()
      ->mutable_binary();
  expression = converter.toBoltExpr(emptyMap);
  EXPECT_EQ(*expression->type(), *MAP(BIGINT(), ARRAY(VARBINARY())));
  const auto* map = expression->valueVector()->wrappedVector()->as<MapVector>();
  ASSERT_NE(map->mapKeys(), nullptr);
  ASSERT_NE(map->mapValues(), nullptr);
  EXPECT_EQ(map->mapKeys()->size(), 0);
  EXPECT_EQ(map->mapValues()->size(), 0);
  EXPECT_EQ(map->offsetAt(0), 0);
  EXPECT_EQ(map->sizeAt(0), 0);
  EXPECT_FALSE(map->isNullAt(0));
}

TEST_F(FunctionTest, binaryCollectionLiterals) {
  SubstraitBoltExprConverter converter(pool_.get(), substraitParser_);
  const std::string bytes("binary\0payload exceeds inline size", 34);
  ::substrait::Expression::Literal list;
  list.mutable_list()->add_values()->set_binary(bytes);
  list.mutable_list()->add_values()->mutable_null()->mutable_binary();
  auto expression = converter.toBoltExpr(list);
  EXPECT_EQ(*expression->type(), *ARRAY(VARBINARY()));
  const auto* array =
      expression->valueVector()->wrappedVector()->as<ArrayVector>();
  EXPECT_EQ(
      array->elements()->asFlatVector<StringView>()->valueAt(0).str(), bytes);
  EXPECT_TRUE(array->elements()->isNullAt(1));

  ::substrait::Expression::Literal mapLiteral;
  auto* entry = mapLiteral.mutable_map()->add_key_values();
  entry->mutable_key()->set_binary(bytes);
  entry->mutable_value()->set_binary(bytes);
  expression = converter.toBoltExpr(mapLiteral);
  EXPECT_EQ(*expression->type(), *MAP(VARBINARY(), VARBINARY()));
  const auto* map = expression->valueVector()->wrappedVector()->as<MapVector>();
  EXPECT_EQ(
      map->mapKeys()->asFlatVector<StringView>()->valueAt(0).str(), bytes);
  EXPECT_EQ(
      map->mapValues()->asFlatVector<StringView>()->valueAt(0).str(), bytes);
}

TEST_F(FunctionTest, rowVariantNulls) {
  const auto type = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  const std::vector<variant> rows{
      variant::row({variant(int64_t{1}), variant::null(TypeKind::BIGINT)}),
      variant::null(TypeKind::ROW),
      variant::row({variant(int64_t{2}), variant(int64_t{3})})};
  const auto checkRows = [](const RowVector* result) {
    EXPECT_FALSE(result->isNullAt(0));
    EXPECT_EQ(result->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 1);
    EXPECT_TRUE(result->childAt(1)->isNullAt(0));
    EXPECT_TRUE(result->isNullAt(1));
    EXPECT_TRUE(result->childAt(0)->isNullAt(1));
    EXPECT_TRUE(result->childAt(1)->isNullAt(1));
    EXPECT_FALSE(result->isNullAt(2));
    EXPECT_EQ(result->childAt(0)->asFlatVector<int64_t>()->valueAt(2), 2);
    EXPECT_EQ(result->childAt(1)->asFlatVector<int64_t>()->valueAt(2), 3);
  };
  auto result = setVectorFromVariants(type, rows, pool_.get());
  checkRows(result->as<RowVector>());
  result =
      setVectorFromVariants(ARRAY(type), {variant::array(rows)}, pool_.get());
  checkRows(result->as<ArrayVector>()->elements()->as<RowVector>());

  result = setVectorFromVariants(
      ROW({}, {}),
      {variant::row(std::vector<variant>{}), variant::null(TypeKind::ROW)},
      pool_.get());
  EXPECT_FALSE(result->isNullAt(0));
  EXPECT_TRUE(result->isNullAt(1));
}

TEST_F(FunctionTest, stringAndBinaryVectorsOwnTheirValues) {
  const std::string text(100, 's');
  const std::string bytes(100, '\0');
  VectorPtr strings;
  VectorPtr binary;
  {
    const std::vector<variant> stringValues{variant(text)};
    const std::vector<variant> binaryValues{
        variant::binary(bytes),
        variant(text),
        variant::null(TypeKind::VARBINARY)};
    strings = setVectorFromVariants(VARCHAR(), stringValues, pool_.get());
    binary = setVectorFromVariants(VARBINARY(), binaryValues, pool_.get());
  }
  EXPECT_EQ(strings->asFlatVector<StringView>()->valueAt(0).str(), text);
  EXPECT_EQ(binary->asFlatVector<StringView>()->valueAt(0).str(), bytes);
  EXPECT_EQ(binary->asFlatVector<StringView>()->valueAt(1).str(), text);
  EXPECT_TRUE(binary->isNullAt(2));
}

TEST_F(FunctionTest, expressionConverterFunctionMapConstructor) {
  std::unordered_map<uint64_t, std::string> functionMap;
  SubstraitBoltExprConverter converter(pool_.get(), functionMap);
  functionMap.emplace(7, "is_null:i64");
  ::substrait::Expression::ScalarFunction function;
  function.set_function_reference(7);
  function.mutable_output_type()->mutable_bool_();
  function.add_arguments()->mutable_value()->mutable_literal()->set_i64(1);
  const auto expression = converter.toBoltExpr(function, ROW({}, {}));
  const auto* call = dynamic_cast<const core::CallTypedExpr*>(expression.get());
  ASSERT_NE(call, nullptr);
  EXPECT_EQ(call->name(), "is_null");
  EXPECT_EQ(*call->type(), *BOOLEAN());
}
