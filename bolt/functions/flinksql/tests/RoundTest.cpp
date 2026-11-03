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
#include "bolt/expression/RegisterSpecialForm.h"
#include "bolt/functions/flinksql/specialforms/DecimalRound.h"
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/sparksql/registration/Register.h"
#include "bolt/type/DecimalUtil.h"

#include <limits>

namespace bytedance::bolt::functions::flinksql::test {
namespace {

class RoundTest : public FlinkFunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    exec::registerFunctionCallToSpecialForms();
    FlinkFunctionBaseTest::SetUpTestCase();
  }

  VectorPtr makeDecimalVector(
      const TypePtr& type,
      const std::vector<std::optional<int64_t>>& values) {
    if (type->isShortDecimal()) {
      return makeNullableFlatVector<int64_t>(values, type);
    }
    return makeNullableFlatVector<int128_t>(
        std::vector<std::optional<int128_t>>(values.begin(), values.end()),
        type);
  }

  core::CallTypedExprPtr createDecimalRound(
      const TypePtr& inputType,
      const TypePtr& resultType,
      const std::optional<int32_t>& scale,
      bool includeScale = true) {
    std::vector<core::TypedExprPtr> inputs = {
        std::make_shared<core::FieldAccessTypedExpr>(inputType, "c0")};
    if (includeScale) {
      inputs.emplace_back(std::make_shared<core::ConstantTypedExpr>(
          INTEGER(),
          scale.has_value() ? variant(scale.value())
                            : variant::null(TypeKind::INTEGER)));
    }
    return std::make_shared<const core::CallTypedExpr>(
        resultType,
        std::move(inputs),
        DecimalRoundCallToSpecialForm::kRoundDecimal);
  }

  void testDecimalRound(
      const VectorPtr& input,
      int32_t scale,
      const VectorPtr& expected) {
    testEncodings(
        createDecimalRound(input->type(), expected->type(), scale),
        {input},
        expected);
  }
};

TEST_F(RoundTest, primitiveTypes) {
  EXPECT_EQ(
      120,
      evaluateOnce<int8_t>(
          "round(c0, c1)",
          std::optional<int8_t>{115},
          std::optional<int32_t>{-1}));
  EXPECT_EQ(
      130,
      evaluateOnce<int16_t>(
          "round(c0, c1)",
          std::optional<int16_t>{125},
          std::optional<int32_t>{-1}));
  EXPECT_EQ(
      130,
      evaluateOnce<int32_t>(
          "round(c0, c1)",
          std::optional<int32_t>{125},
          std::optional<int32_t>{-1}));
  EXPECT_EQ(
      -130,
      evaluateOnce<int32_t>(
          "round(c0, c1)",
          std::optional<int32_t>{-125},
          std::optional<int32_t>{-1}));
  EXPECT_EQ(
      100,
      evaluateOnce<int32_t>(
          "round(c0, c1)",
          std::optional<int32_t>{149},
          std::optional<int32_t>{-2}));
  EXPECT_EQ(
      200,
      evaluateOnce<int32_t>(
          "round(c0, c1)",
          std::optional<int32_t>{150},
          std::optional<int32_t>{-2}));
  EXPECT_EQ(
      1'234'567'900,
      evaluateOnce<int64_t>(
          "round(c0, c1)",
          std::optional<int64_t>{1'234'567'890},
          std::optional<int32_t>{-2}));
  EXPECT_EQ(
      125, evaluateOnce<int32_t>("round(c0)", std::optional<int32_t>{125}));
  EXPECT_FLOAT_EQ(
      1.13f,
      evaluateOnce<float>(
          "round(c0, c1)",
          std::optional<float>{1.125f},
          std::optional<int32_t>{2})
          .value());
  EXPECT_EQ(
      1.13,
      evaluateOnce<double>(
          "round(c0, c1)",
          std::optional<double>{1.125},
          std::optional<int32_t>{2}));
  EXPECT_EQ(
      -1.13,
      evaluateOnce<double>(
          "round(c0, c1)",
          std::optional<double>{-1.125},
          std::optional<int32_t>{2}));
  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<int64_t>(
          "round(c0, c1)",
          std::optional<int64_t>{125},
          std::optional<int32_t>{}));
}

TEST_F(RoundTest, decimalResultTypes) {
  const auto expectType = [](uint8_t precision,
                             uint8_t scale,
                             int32_t roundScale,
                             uint8_t expectedPrecision,
                             uint8_t expectedScale) {
    EXPECT_EQ(
        std::make_pair(expectedPrecision, expectedScale),
        DecimalRoundCallToSpecialForm::getResultPrecisionScale(
            precision, scale, roundScale));
  };
  expectType(5, 2, 2, 5, 2);
  expectType(5, 2, 30, 5, 2);
  expectType(5, 2, 1, 5, 1);
  expectType(5, 2, 0, 4, 0);
  expectType(5, 2, -30, 4, 0);
  expectType(38, 0, -1, 38, 0);
}

TEST_F(RoundTest, shortDecimal) {
  const auto input = makeNullableFlatVector<int64_t>(
      {12345, 12555, -12555, 99999, 0, std::nullopt}, DECIMAL(5, 2));

  testDecimalRound(
      input,
      2,
      makeNullableFlatVector<int64_t>(
          {12345, 12555, -12555, 99999, 0, std::nullopt}, DECIMAL(5, 2)));
  testDecimalRound(
      input,
      30,
      makeNullableFlatVector<int64_t>(
          {12345, 12555, -12555, 99999, 0, std::nullopt}, DECIMAL(5, 2)));
  testDecimalRound(
      input,
      1,
      makeNullableFlatVector<int64_t>(
          {1235, 1256, -1256, 10000, 0, std::nullopt}, DECIMAL(5, 1)));
  testDecimalRound(
      input,
      0,
      makeNullableFlatVector<int64_t>(
          {123, 126, -126, 1000, 0, std::nullopt}, DECIMAL(4, 0)));
  testDecimalRound(
      input,
      -1,
      makeNullableFlatVector<int64_t>(
          {120, 130, -130, 1000, 0, std::nullopt}, DECIMAL(4, 0)));
  testDecimalRound(
      input,
      -30,
      makeNullableFlatVector<int64_t>(
          {0, 0, 0, 0, 0, std::nullopt}, DECIMAL(4, 0)));

  const auto maxShortDecimal =
      static_cast<int64_t>(DecimalUtil::kPowersOfTen[18] - 1);
  testDecimalRound(
      makeFlatVector<int64_t>(
          {maxShortDecimal, -maxShortDecimal, 0}, DECIMAL(18, 0)),
      -1,
      makeFlatVector<int128_t>(
          {DecimalUtil::kPowersOfTen[18], -DecimalUtil::kPowersOfTen[18], 0},
          DECIMAL(19, 0)));
}

TEST_F(RoundTest, flinkReferenceCases) {
  const auto input =
      makeFlatVector<int64_t>({646646, -646646, 0}, DECIMAL(10, 3));

  testDecimalRound(
      input, 0, makeFlatVector<int64_t>({647, -647, 0}, DECIMAL(8, 0)));
  testDecimalRound(
      input, 1, makeFlatVector<int64_t>({6466, -6466, 0}, DECIMAL(9, 1)));
  testDecimalRound(
      input, 2, makeFlatVector<int64_t>({64665, -64665, 0}, DECIMAL(10, 2)));
  testDecimalRound(
      input, 3, makeFlatVector<int64_t>({646646, -646646, 0}, DECIMAL(10, 3)));
  testDecimalRound(
      input, 4, makeFlatVector<int64_t>({646646, -646646, 0}, DECIMAL(10, 3)));
  testDecimalRound(
      input, -1, makeFlatVector<int64_t>({650, -650, 0}, DECIMAL(8, 0)));
  testDecimalRound(
      input, -2, makeFlatVector<int64_t>({600, -600, 0}, DECIMAL(8, 0)));
  testDecimalRound(
      input, -3, makeFlatVector<int64_t>({1000, -1000, 0}, DECIMAL(8, 0)));
  testDecimalRound(
      input, -4, makeFlatVector<int64_t>({0, 0, 0}, DECIMAL(8, 0)));
}

TEST_F(RoundTest, longDecimalToShortDecimal) {
  testDecimalRound(
      makeFlatVector<int128_t>(
          {1234567890123456789, -1234567890555555555, 0}, DECIMAL(20, 10)),
      2,
      makeFlatVector<int64_t>({12345678901, -12345678906, 0}, DECIMAL(13, 2)));

  const auto maxDecimal20 = DecimalUtil::kPowersOfTen[20] - 1;
  testDecimalRound(
      makeFlatVector<int128_t>(
          {maxDecimal20, -maxDecimal20, 0}, DECIMAL(20, 10)),
      -1,
      makeFlatVector<int64_t>(
          {10'000'000'000, -10'000'000'000, 0}, DECIMAL(11, 0)));
}

TEST_F(RoundTest, decimalOverflowAndExtremeScale) {
  const auto maxDecimal = DecimalUtil::kPowersOfTen[38] - 1;
  const auto input =
      makeFlatVector<int128_t>({maxDecimal, -maxDecimal, 0}, DECIMAL(38, 0));

  testDecimalRound(
      input,
      -1,
      makeNullableFlatVector<int128_t>(
          {std::nullopt, std::nullopt, 0}, DECIMAL(38, 0)));
  testDecimalRound(
      input,
      -38,
      makeNullableFlatVector<int128_t>(
          {std::nullopt, std::nullopt, 0}, DECIMAL(38, 0)));
  testDecimalRound(
      input, -100, makeFlatVector<int128_t>({0, 0, 0}, DECIMAL(38, 0)));

  const auto scaledInput =
      makeFlatVector<int128_t>({maxDecimal, -maxDecimal, 0}, DECIMAL(38, 10));
  testDecimalRound(
      scaledInput, -29, makeFlatVector<int128_t>({0, 0, 0}, DECIMAL(29, 0)));
}

TEST_F(RoundTest, defaultAndNullScale) {
  const auto input =
      makeNullableFlatVector<int64_t>({12345, -12555, 0}, DECIMAL(5, 2));
  const auto expected =
      makeNullableFlatVector<int64_t>({123, -126, 0}, DECIMAL(4, 0));
  testEncodings(
      createDecimalRound(input->type(), expected->type(), std::nullopt, false),
      {input},
      expected);

  const auto nullExpected = makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt}, DECIMAL(5, 2));
  testEncodings(
      createDecimalRound(
          input->type(), nullExpected->type(), std::nullopt, true),
      {input},
      nullExpected);
}

TEST_F(RoundTest, dynamicScalePreservesDecimalType) {
  for (const auto& type : {DECIMAL(5, 2), DECIMAL(20, 2), DECIMAL(38, 2)}) {
    auto input = makeDecimalVector(
        type, {12345, -12555, 99999, 12345, 12345, 12345, std::nullopt, 12345});
    auto scales = makeNullableFlatVector<int32_t>(
        {-2,
         0,
         1,
         2,
         -39,
         std::numeric_limits<int32_t>::max(),
         0,
         std::nullopt});
    auto expression = std::make_shared<const core::CallTypedExpr>(
        type,
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(type, "c0"),
            std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1")},
        DecimalRoundCallToSpecialForm::kRoundDecimal);
    auto expected = makeDecimalVector(
        type,
        {10000,
         -12600,
         type->isShortDecimal() ? std::nullopt : std::optional<int64_t>(100000),
         12345,
         0,
         12345,
         std::nullopt,
         std::nullopt});
    testEncodings(expression, {input, scales}, expected);
  }
}

TEST_F(RoundTest, foldedScalePreservesDeclaredDecimalType) {
  const auto type = DECIMAL(5, 2);
  const auto input = makeNullableFlatVector<int64_t>(
      {12345, -12555, 99999, std::nullopt}, type);
  auto scale = std::make_shared<core::CallTypedExpr>(
      INTEGER(),
      std::vector<core::TypedExprPtr>{std::make_shared<core::ConstantTypedExpr>(
          BIGINT(), variant(int64_t{1}))},
      "cast");
  auto expression = std::make_shared<core::CallTypedExpr>(
      type,
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(type, "c0"), scale},
      "decimal_round");
  testEncodings(
      expression,
      {input},
      makeNullableFlatVector<int64_t>(
          {12350, -12560, std::nullopt, std::nullopt}, type));
  const auto invalidType = DECIMAL(6, 2);
  auto invalid = std::make_shared<core::CallTypedExpr>(
      invalidType, expression->inputs(), "decimal_round");
  EXPECT_THROW(evaluate(invalid, makeRowVector({input})), BoltUserError);
}

TEST_F(RoundTest, decimalTruncatePreservesDecimalType) {
  for (const auto& type : {DECIMAL(5, 2), DECIMAL(20, 2), DECIMAL(38, 2)}) {
    auto input = makeDecimalVector(
        type, {12345, -12555, 99999, 12345, 12345, std::nullopt});
    const std::vector<std::pair<int32_t, std::vector<int64_t>>> cases = {
        {-38, {0, 0, 0, 0, 0}},
        {-2, {10000, -10000, 90000, 10000, 10000}},
        {-1, {12000, -12000, 99000, 12000, 12000}},
        {0, {12300, -12500, 99900, 12300, 12300}},
        {1, {12340, -12550, 99990, 12340, 12340}},
        {2, {12345, -12555, 99999, 12345, 12345}},
        {38, {12345, -12555, 99999, 12345, 12345}}};
    for (const auto& [scale, unscaled] : cases) {
      std::vector<std::optional<int64_t>> values(
          unscaled.begin(), unscaled.end());
      values.emplace_back(std::nullopt);
      auto expected = makeDecimalVector(type, values);
      auto expression = createDecimalRound(type, type, scale);
      auto truncate = std::make_shared<const core::CallTypedExpr>(
          type,
          expression->inputs(),
          DecimalTruncateCallToSpecialForm::kTruncateDecimal);
      testEncodings(truncate, {input}, expected);
    }
    auto scales = makeNullableFlatVector<int32_t>(
        {-2, 0, 1, -39, std::numeric_limits<int32_t>::max(), 0});
    auto expression = std::make_shared<const core::CallTypedExpr>(
        type,
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(type, "c0"),
            std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1")},
        DecimalTruncateCallToSpecialForm::kTruncateDecimal);
    auto expected =
        makeDecimalVector(type, {10000, -12500, 99990, 0, 12345, std::nullopt});
    testEncodings(expression, {input, scales}, expected);
  }
}

TEST_F(RoundTest, decimalTruncateSqlRoundTrip) {
  const auto type = DECIMAL(5, 2);
  auto input = makeRowVector({makeFlatVector<int64_t>({12345, -12555}, type)});
  for (const auto scale :
       std::vector<std::optional<int32_t>>{1, -1, std::nullopt}) {
    auto expression = std::make_shared<core::CallTypedExpr>(
        type,
        createDecimalRound(type, type, scale)->inputs(),
        "decimal_truncate");
    exec::ExprSet expressions({expression}, &execCtx_);
    auto expected = evaluate(expressions, input);
    const auto sql = expressions.expr(0)->toSql();
    SCOPED_TRACE(sql);
    bolt::test::assertEqualVectors(expected, evaluate(sql, input));
  }
}

TEST_F(RoundTest, decimalScaleOverflowIsRecoverable) {
  auto input = makeFlatVector<int64_t>({12345, 0, -12345}, DECIMAL(5, 2));
  auto scales = makeFlatVector<int32_t>(
      3, [](auto) { return std::numeric_limits<int32_t>::min(); });
  for (const auto* name : {"decimal_round", "decimal_truncate"}) {
    auto inner = std::make_shared<core::CallTypedExpr>(
        input->type(),
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(input->type(), "c0"),
            std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1")},
        name);
    auto expression = std::make_shared<core::CallTypedExpr>(
        input->type(), std::vector<core::TypedExprPtr>{inner}, "try");
    testEncodings(
        expression,
        {input, scales},
        makeNullableFlatVector<int64_t>(
            {std::nullopt, 0, std::nullopt}, input->type()));
  }
}

TEST_F(RoundTest, primitiveTruncate) {
  for (int32_t scale :
       {std::numeric_limits<int32_t>::min(),
        -2,
        -1,
        0,
        1,
        std::numeric_limits<int32_t>::max()}) {
    const int64_t expected = scale < -2 ? 0
        : scale == -2                   ? -100
        : scale == -1                   ? -120
                                        : -125;
    auto test = [&]<typename T>() {
      EXPECT_EQ(
          static_cast<T>(expected),
          evaluateOnce<T>(
              "flink_truncate(c0,c1)",
              std::optional<T>{-125},
              std::optional<int32_t>{scale}));
    };
    test.template operator()<int8_t>();
    test.template operator()<int16_t>();
    test.template operator()<int32_t>();
    test.template operator()<int64_t>();
  }
  EXPECT_EQ(
      -125,
      evaluateOnce<int32_t>(
          "flink_truncate(c0)", std::optional<int32_t>{-125}));
  EXPECT_EQ(
      1.15,
      evaluateOnce<double>(
          "flink_truncate(c0,c1)",
          std::optional<double>{1.15},
          std::optional<int32_t>{2}));
  EXPECT_EQ(
      1.0,
      evaluateOnce<double>(
          "flink_truncate(c0,c1)",
          std::optional<double>{1.005},
          std::optional<int32_t>{2}));
  EXPECT_EQ(
      -0.12,
      evaluateOnce<double>(
          "flink_truncate(c0,c1)",
          std::optional<double>{-0.123},
          std::optional<int32_t>{2}));
  EXPECT_EQ(
      1.14f,
      evaluateOnce<float>(
          "flink_truncate(c0,c1)",
          std::optional<float>{1.15f},
          std::optional<int32_t>{2}));
  EXPECT_EQ(
      0.0f,
      evaluateOnce<float>(
          "flink_truncate(c0,c1)",
          std::optional<float>{std::numeric_limits<float>::denorm_min()},
          std::optional<int32_t>{45}));
  EXPECT_EQ(
      0.0,
      evaluateOnce<double>(
          "flink_truncate(c0,c1)",
          std::optional<double>{std::numeric_limits<double>::denorm_min()},
          std::optional<int32_t>{45}));
  EXPECT_EQ(
      std::numeric_limits<double>::max(),
      evaluateOnce<double>(
          "flink_truncate(c0,c1)",
          std::optional<double>{std::numeric_limits<double>::max()},
          std::optional<int32_t>{-2}));
  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<double>(
          "try(flink_truncate(c0,c1))",
          std::optional<double>{std::numeric_limits<double>::infinity()},
          std::optional<int32_t>{2}));
  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<float>(
          "try(flink_truncate(c0,c1))",
          std::optional<float>{std::numeric_limits<float>::max()},
          std::optional<int32_t>{2}));
}

} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
