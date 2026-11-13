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

#include "bolt/expression/CastExpr.h"
#include "bolt/expression/RegisterSpecialForm.h"
#include "bolt/expression/SpecialFormRegistry.h"
#include "bolt/functions/flinksql/registration/Register.h"
#include "bolt/functions/lib/IsNull.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/prestosql/tests/CastBaseTest.h"
#include "bolt/parse/TypeResolver.h"

namespace bytedance::bolt::functions::flinksql::test {

using bolt::test::assertEqualVectors;

class FlinkCastExprTest : public functions::test::CastBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    exec::registerFunctionCallToSpecialForms();
    prestosql::registerComparisonFunctions();
    registerIsNullFunction("is_null");
    flinksql::registerFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(FlinkCastExprTest, stringToDateBoundaries) {
  const std::vector<std::optional<std::string>> input = {
      "1970-01-01",
      "2026-11-11",
      "2026-11-11 12:34:56",
      "2026-11-11 arbitrary suffix",
      "2026-11",
      "2026",
      " 2026-11-11",
      "2026-11-11\t",
      "2026-\t11-\t11",
      "2026-1-2",
      "0000-01-01",
      "0000-02-29",
      "0001-01-01",
      "1582-10-10",
      "2000-02-29",
      "9999-12-31",
      "0000002026-00011-00011",
      "2026-11-11T12:34:56",
      "10000-01-01",
      "0000-00-00",
      "2026-02-29",
      "1900-02-29",
      "2026-04-31",
      "2026-00-01",
      "2026-13-01",
      "2026-11-00",
      "2026-11-32",
      "2026-",
      "2026-11-",
      "2026-11-11-1",
      "+2026-11-11",
      "-0001-01-01",
      "2026/11/11",
      " 2026-11-11 12:34:56",
      "2026-11-11\t12:34:56",
      "2026-11-11\xc2\xa0",
      "invalid",
      "",
      std::nullopt};
  // Epoch days from Flink SqlDateTimeUtils.dateStringToUnixDate.
  std::vector<std::optional<int32_t>> expected = {
      0,
      20768,
      20768,
      20768,
      20758,
      20454,
      20768,
      20768,
      20768,
      20455,
      -719528,
      -719469,
      -719162,
      -141432,
      11016,
      2932896,
      20768};
  expected.resize(input.size(), std::nullopt);
  testCast<std::string, int32_t>(VARCHAR(), DATE(), input, expected);
  testCast<std::string, int32_t>(
      VARCHAR(),
      DATE(),
      {"invalid", std::nullopt},
      {std::nullopt, std::nullopt});
  testCast<std::string, int32_t>(
      VARCHAR(), DATE(), {std::nullopt}, {std::nullopt});
}

TEST_F(FlinkCastExprTest, stringToDateIntegerOverflow) {
  const std::vector<std::string> overflow = {
      "2147483648",
      "2147483648-01-01",
      "2026-2147483648-01",
      "2026-01-2147483648",
      "2147483648\t-01-01",
      "2026-01-2147483648\t",
      "999999999999999999999-01-01",
      "10000-2147483648-01",
      "2026-13-2147483648"};
  for (const auto& value : overflow) {
    SCOPED_TRACE(value);
    auto input = makeRowVector({makeFlatVector<std::string>({value})});
    BOLT_ASSERT_THROW(
        evaluate("cast(c0 as date)", input),
        "Flink DATE component exceeds the 32-bit integer range");
    assertEqualVectors(
        makeNullableFlatVector<int32_t>({std::nullopt}, DATE()),
        evaluate("try_cast(c0 as date)", input));
  }

  // Java validates all digits within a component before parsing its integer,
  // and parses all components before checking the calendar date range.
  testCast<std::string, int32_t>(
      VARCHAR(),
      DATE(),
      {"2147483647-01-01",
       "2026-2147483647-01",
       "2026-01-2147483647",
       "2147483648x-01-01",
       "2026-2147483648x-01",
       "2147483648\t0-01-01",
       "2026-01-2147483648-1",
       "invalid-2147483648-01",
       "0000000000000000000002026-01-01",
       std::nullopt},
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       20454,
       std::nullopt});

  auto input = makeRowVector({makeNullableFlatVector<std::string>(
      {"2147483648-01-01", "2026-01-01", std::nullopt, "invalid"})});
  auto expected = makeNullableFlatVector<int32_t>(
      {std::nullopt, 20454, std::nullopt, std::nullopt}, DATE());
  for (const auto* expression :
       {"try_cast(c0 as date)", "try(cast(c0 as date))"}) {
    assertEqualVectors(expected, evaluate(expression, input));
    auto indices = makeIndices({3, 0, 1, 2});
    assertEqualVectors(
        wrapInDictionary(indices, 4, expected),
        evaluate(
            expression,
            makeRowVector({wrapInDictionary(indices, 4, input->childAt(0))})));
  }
  SelectivityVector selected(4, false);
  selected.setValid(1, true);
  selected.updateBounds();
  assertEqualVectors(
      expected, evaluate("cast(c0 as date)", input, selected), selected);
  assertEqualVectors(
      makeFlatVector<int32_t>({0, 20454, 0, 0}, DATE()),
      evaluate(
          "if(c0 = '2026-01-01', cast(c0 as date), cast('1970-01-01' as date))",
          input));
}

TEST_F(FlinkCastExprTest, stringToDateCharacters) {
  std::vector<std::optional<std::string>> input = {
      "2000-02-29",
      "2000-02-29 arbitrary suffix",
      "2000-02-30 arbitrary suffix",
      "2026-01-02 ",
      "\t026-01-02",
      " 026-01-02",
      "2026-\t1-02",
      "2026-01-\t2",
      "2026-01-2\t",
      "2026-01-2 ",
      "2026-01- 2",
      "2026 -1-02",
      " 2026 -1-02",
      "\t 2026-01-02",
      "\t2026-01-02 arbitrary suffix",
      "2000/02-29",
      "2000-02/29"};
  std::vector<std::optional<int32_t>> expected = {
      11016,
      11016,
      std::nullopt,
      20455,
      -710030,
      -710030,
      20455,
      20455,
      20455,
      20455,
      std::nullopt,
      20454,
      20455,
      std::nullopt,
      20455,
      std::nullopt,
      std::nullopt};
  const std::string date = "2000-02-29";
  for (size_t i = 0; i < date.size(); ++i) {
    for (const auto c : {'/', ':', 'x', '\x7f', '\xff'}) {
      auto value = date;
      value[i] = c;
      input.emplace_back(value);
      expected.emplace_back(std::nullopt);
    }
  }
  testCast<std::string, int32_t>(VARCHAR(), DATE(), input, expected);
}

TEST_F(FlinkCastExprTest, stringToDateMysqlCompatibility) {
  for (bool compatibleWithMysql : {false, true}) {
    SCOPED_TRACE(compatibleWithMysql);
    queryCtx_->testingOverrideConfigUnsafe(
        {{"table.exec.compatible-with-mysql",
          compatibleWithMysql ? "true" : "false"}});
    testCast<std::string, int32_t>(
        VARCHAR(),
        DATE(),
        {"0000-00-00",
         "0001-01-01",
         "0000-01-01",
         "0000-00-00 00:00:00",
         "0000-00-00 ",
         " 0000-00-00",
         "0000-00-00\t",
         "0-0-0",
         std::nullopt},
        {compatibleWithMysql ? std::optional<int32_t>(-719162) : std::nullopt,
         -719162,
         -719528,
         std::nullopt,
         std::nullopt,
         std::nullopt,
         std::nullopt,
         std::nullopt,
         std::nullopt});
  }
}

TEST_F(FlinkCastExprTest, stringToDateComparisonAndSelection) {
  auto input = makeRowVector({
      makeNullableFlatVector<std::string>(
          {"2026-11-11T12:34:56",
           "2026-11-11",
           "10000-01-01",
           "2026-11-12",
           std::nullopt,
           "2026-11-11",
           std::nullopt}),
      makeNullableFlatVector<int32_t>(
          {20768, 20768, 20768, 20768, 20768, std::nullopt, std::nullopt},
          DATE()),
  });
  auto expected = makeNullableFlatVector<bool>(
      {std::nullopt,
       true,
       std::nullopt,
       false,
       std::nullopt,
       std::nullopt,
       std::nullopt});
  for (const auto* expression :
       {"cast(c0 as date) = c1",
        "c1 = cast(c0 as date)",
        "try_cast(c0 as date) = c1",
        "c1 = try_cast(c0 as date)"}) {
    SCOPED_TRACE(expression);
    assertEqualVectors(expected, evaluate(expression, input));
    SelectivityVector selected(input->size(), false);
    for (auto row : {0, 1, 3, 6}) {
      selected.setValid(row, true);
    }
    selected.updateBounds();
    assertEqualVectors(
        expected, evaluate(expression, input, selected), selected);
  }
  assertEqualVectors(
      makeNullableFlatVector<int32_t>(
          {20768, 20768, 20768, 20769, 20768, 20768, std::nullopt}, DATE()),
      evaluate(
          "if(c1 is null, cast(c0 as date), coalesce(try_cast(c0 as date), c1))",
          input));
}

TEST_F(FlinkCastExprTest, mysqlCompatibilityIsSpecificToFlinkCast) {
  exec::registerFunctionCallToSpecialForm(
      "testing_default_try_cast",
      std::make_unique<exec::TryCastCallToSpecialForm>());
  auto input = makeRowVector({makeNullableFlatVector<std::string>(
      {"10000-01-01", "0000-00-00", "+1970-01-01", std::nullopt})});
  auto defaultCast = std::make_shared<core::CallTypedExpr>(
      DATE(),
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0")},
      "testing_default_try_cast");
  auto original = evaluate(defaultCast, input);
  assertEqualVectors(
      makeNullableFlatVector<int32_t>(
          {2932897, std::nullopt, 0, std::nullopt}, DATE()),
      original);
  queryCtx_->testingOverrideConfigUnsafe(
      {{"table.exec.compatible-with-mysql", "true"}});
  assertEqualVectors(original, evaluate(defaultCast, input));
  assertEqualVectors(
      makeNullableFlatVector<int32_t>(
          {std::nullopt, -719162, std::nullopt, std::nullopt}, DATE()),
      evaluate("try_cast(c0 as date)", input));
}

TEST_F(FlinkCastExprTest, decimalToVarcharPreservesPlainScale) {
  const auto decimalType = DECIMAL(10, 8);

  testCast<int64_t, StringView>(
      "varchar",
      {0, 1, 100000000, -1, std::nullopt},
      {"0.00000000", "0.00000001", "1.00000000", "-0.00000001", std::nullopt},
      decimalType,
      VARCHAR());

  testTryCast<int64_t, StringView>(
      "varchar",
      {0, 1, 100000000, -1, std::nullopt},
      {"0.00000000", "0.00000001", "1.00000000", "-0.00000001", std::nullopt},
      decimalType,
      VARCHAR());
}

TEST_F(FlinkCastExprTest, longDecimalToVarcharPreservesPlainScale) {
  const auto decimalType = DECIMAL(30, 10);

  testCast<int128_t, StringView>(
      "varchar",
      {0, 1, 10000000000LL, -1, std::nullopt},
      {"0.0000000000",
       "0.0000000001",
       "1.0000000000",
       "-0.0000000001",
       std::nullopt},
      decimalType,
      VARCHAR());

  testTryCast<int128_t, StringView>(
      "varchar",
      {0, 1, 10000000000LL, -1, std::nullopt},
      {"0.0000000000",
       "0.0000000001",
       "1.0000000000",
       "-0.0000000001",
       std::nullopt},
      decimalType,
      VARCHAR());
}

TEST_F(FlinkCastExprTest, nonDecimalToVarcharUsesDefaultCast) {
  testCast<int64_t, StringView>(
      "varchar", {0, 12, -34, std::nullopt}, {"0", "12", "-34", std::nullopt});
}

} // namespace bytedance::bolt::functions::flinksql::test
