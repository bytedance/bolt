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

#include <common/base/BoltException.h>
#include <optional>
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "bolt/version/version.h"

namespace bytedance::bolt::functions::sparksql::test {
using namespace bytedance::bolt::test;
namespace {

class SplitTest : public SparkFunctionBaseTest {
 protected:
  void testSplit(
      const std::vector<std::optional<std::string>>& input,
      const std::string& pattern,
      const std::vector<std::optional<std::vector<std::string>>>& output,
      std::optional<int> limit = std::nullopt);
};

void SplitTest::testSplit(
    const std::vector<std::optional<std::string>>& input,
    const std::string& pattern,
    const std::vector<std::optional<std::vector<std::string>>>& output,
    std::optional<int> limit) {
  auto valueAt = [&input](vector_size_t row) {
    return input[row] ? StringView(*input[row]) : StringView();
  };

  // Creating vectors for input strings
  auto nullAt = [&input](vector_size_t row) { return !input[row].has_value(); };

  auto resultFunc = [&](bool constPattern, bool constLimit) {
    SCOPED_TRACE(
        "Run with constPattern: " + std::to_string(constPattern) +
        ", constLimit: " + std::to_string(constLimit));
    auto inputString =
        makeFlatVector<StringView>(input.size(), valueAt, nullAt);

    auto delimiterVector = makeFlatVector<StringView>(
        input.size(),
        [&](vector_size_t /*row*/) { return StringView(pattern); });

    auto limits = makeFlatVector<int32_t>(
        input.size(), [&](auto row) { return limit.value_or(-1); });

    auto rowVector = makeRowVector({inputString, delimiterVector, limits});

    // Evaluating the function for each input and seed
    std::string expressionString;
    if (constPattern && constLimit) {
      expressionString = fmt::format(
          "split(c0, '{}', CAST({} AS INT))", pattern, limit.value_or(-1));
    } else if (constLimit) {
      expressionString =
          fmt::format("split(c0, c1, CAST({} AS INT))", limit.value_or(-1));
    } else {
      BOLT_CHECK(!constPattern, "split(c0, 'constPattern', c1) is invalid");
      expressionString = "split(c0, c1, c2)";
    }
    // TODO: if (!limit.has_value()), test -1 column

    auto typedExpr =
        makeTypedExpr(expressionString, asRowType(rowVector->type()));
    bytedance::bolt::exec::ExprSet exprSet({typedExpr}, &execCtx_);

    exec::EvalCtx context(&execCtx_, &exprSet, rowVector.get());
    std::vector<VectorPtr> result(1);

    SelectivityVector rows(rowVector->size());
    BaseVector::ensureWritable(
        rows, ARRAY(VARCHAR()), context.pool(), result[0]);
    auto buffer =
        result[0]->as<ArrayVector>()->elements()->mutableNulls(32 * 1024);
    // set null buffer to null for testing buffer reuse
    memset(
        buffer->asMutable<char>(),
        bytedance::bolt::bits::kNullByte,
        buffer->capacity());
    exprSet.eval(rows, context, result);
    return result[0];
  };

  // Creating vectors for output string vectors
  auto sizeAtOutput = [&output](vector_size_t row) {
    return output[row] ? output[row]->size() : 0;
  };
  auto valueAtOutput = [&output](vector_size_t row, vector_size_t idx) {
    return output[row] ? StringView(output[row]->at(idx)) : StringView("");
  };
  auto nullAtOutput = [&output](vector_size_t row) {
    return !output[row].has_value();
  };
  auto expectedResult = makeArrayVector<StringView>(
      output.size(), sizeAtOutput, valueAtOutput, nullAtOutput);

  // Checking the results
  assertEqualVectors(expectedResult, resultFunc(true, true));
  assertEqualVectors(expectedResult, resultFunc(false, true));
  assertEqualVectors(expectedResult, resultFunc(false, false));
}

TEST_F(SplitTest, reallocationAndCornerCases) {
  testSplit(
      {"boo:and:foo", "abcfd", "abcfd:", "", ":ab::cfd::::"},
      ":",
      {{{"boo", "and", "foo"}},
       {{"abcfd"}},
       {{"abcfd", ""}},
       {{""}},
       {{"", "ab", "", "cfd", "", "", "", ""}}});
}

TEST_F(SplitTest, nulls) {
  testSplit(
      {std::nullopt, "abcfd", "abcfd:", std::nullopt, ":ab::cfd::::"},
      ":",
      {{std::nullopt},
       {{"abcfd"}},
       {{"abcfd", ""}},
       {{std::nullopt}},
       {{"", "ab", "", "cfd", "", "", "", ""}}});
}

TEST_F(SplitTest, defaultArguments) {
  testSplit(
      {"boo:and:foo", "abcfd"}, ":", {{{"boo", "and", "foo"}}, {{"abcfd"}}});
}

TEST_F(SplitTest, longStrings) {
  testSplit(
      {"abcdefghijklkmnopqrstuvwxyz"},
      ",",
      {{{"abcdefghijklkmnopqrstuvwxyz"}}});
}

TEST_F(SplitTest, Basic) {
  testSplit(
      {"oneAtwoBthreeC", "abcfd"},
      "[ABC]",
      {{{"one", "two", "three", ""}}, {{"abcfd"}}});
}

TEST_F(SplitTest, UnderCase) {
  testSplit(
      {"hello___world__", "abcd"},
      "_",
      {{{"hello", "", "", "world", "", ""}}, {{"abcd"}}});
}

TEST_F(SplitTest, Utf8Case) {
  testSplit({"你好世界", "你a好b"}, "好", {{{"你", "世界"}}, {{"你a", "b"}}});
}

TEST_F(SplitTest, EmptyDelimeter) {
  if constexpr (kSparkVersion < SparkVersion::SPARK_3_4) {
    testSplit(
        {"AAA", "你好a"}, "", {{{"A", "A", "A", ""}}, {{"你", "好", "a", ""}}});
  } else {
    testSplit({"AAA", "你好a"}, "", {{{"A", "A", "A"}}, {{"你", "好", "a"}}});
  }
}

TEST_F(SplitTest, EmptyInput) {
  testSplit({""}, "delimiter", {{{""}}});
}
TEST_F(SplitTest, DelimeterNotExists) {
  testSplit(
      {"", "a", "abc", "甲", "a甲乙丙"},
      "delimiter",
      {{{""}}, {{"a"}}, {{"abc"}}, {{"甲"}}, {{"a甲乙丙"}}});
}

// TODO: use RegexSplit
TEST_F(SplitTest, RegexEmptyDelimeter) {
  if constexpr (kSparkVersion < SparkVersion::SPARK_3_4) {
    testSplit(
        {"AAA", "你你"}, "|", {{{"A", "A", "A", ""}}, {{"你", "你", ""}}});
    testSplit({"AAA", "你你"}, "", {{{"A", "A", "A", ""}}, {{"你", "你", ""}}});
    testSplit(
        {"", "a", "abc", "甲", "a甲乙丙"},
        "$",
        {{{""}}, {{"a", ""}}, {{"abc", ""}}, {{"甲", ""}}, {{"a甲乙丙", ""}}});
    testSplit(
        {"", "a", "abc", "甲", "a甲乙丙"},
        ".",
        {{{""}},
         {{"", ""}},
         {{"", "", "", ""}},
         {{"", ""}},
         {{"", "", "", "", ""}}});
    testSplit(
        {"AAA", "你你"}, "", {{{"A", "A", "A", ""}}, {{"你", "你", ""}}}, -1);

    // https://github.com/apache/spark/blob/master/common/unsafe/src/main/java/org/apache/spark/unsafe/types/UTF8String.java#L1046
    // Java String's split method supports "ignore empty string" behavior when
    // the limit is 0 whereas other languages do not. To avoid this java
    // specific behavior, we fall back to -1 when the limit is 0.
    testSplit(
        {"AAA", "你你"}, "", {{{"A", "A", "A", ""}}, {{"你", "你", ""}}}, 0);
    testSplit({"AAA", "你你"}, "", {{{"AAA"}}, {{"你你"}}}, 1);
    testSplit({"AAA", "你你"}, "", {{{"A", "AA"}}, {{"你", "你"}}}, 2);
  }
}

// TODO: use RegexSplit
TEST_F(SplitTest, ICURegexInvalidPattern) {
  testSplit(
      {"{\"1\":\"2\"},{\"3\":\"4\"}", "{1:2},{3,4}"},
      "},\\{",
      {{{"{\"1\":\"2\"", "\"3\":\"4\"}"}}, {{"{1:2", "3,4}"}}});
  EXPECT_THROW(
      testSplit(
          {"{\"1\":\"2\"},{\"3\":\"4\"}", "{1:2},{3,4}"},
          "\"},\{\"",
          {{{"{\"1\":\"2\"", "\"3\":\"4\"}", ""}}, {{"{1:2", "3,4}", ""}}}),
      BoltUserError);
}

TEST_F(SplitTest, WithConstLimit) {
  testSplit({"oneAtwoBthreeC"}, "[ABC]", {{{"oneAtwoBthreeC"}}}, 1);

  testSplit({"oneAtwoBthreeC"}, "[ABC]", {{{"one", "twoBthreeC"}}}, 2);

  testSplit({"oneAtwoBthreeC"}, "[ABC]", {{{"one", "two", "threeC"}}}, 3);

  testSplit({"oneAtwoBthreeC"}, "[ABC]", {{{"one", "two", "three", ""}}}, 4);
}

TEST_F(SplitTest, SpecialCaseWithConstLimit) {
  testSplit({"CatCatCat"}, "Cat", {{{"CatCatCat"}}}, 1);

  testSplit({"CatCatCat"}, "Cat", {{{"", "CatCat"}}}, 2);

  testSplit({"CatCatCat"}, "Cat", {{{"", "", "Cat"}}}, 3);

  testSplit({"CatCatCat"}, "Cat", {{{"", "", "", ""}}}, 4);
}

TEST_F(SplitTest, sparkUT) {
  if constexpr (kSparkVersion < SparkVersion::SPARK_3_4) {
    testSplit(
        {"abcdefgh"}, "", {{{"a", "b", "c", "d", "e", "f", "g", "h", ""}}});
    testSplit({"hello"}, "", {{{"h", "e", "l", "l", "o", ""}}});
    testSplit({"hello"}, "", {{{"h", "e", "llo"}}}, 3);
    testSplit({"一二三四"}, "", {{{"一", "二", "三四"}}}, 3);
  } else {
    testSplit({"abcdefgh"}, "", {{{"a", "b", "c", "d", "e", "f", "g", "h"}}});
    testSplit({"hello"}, "", {{{"h", "e", "l"}}}, 3);
    testSplit({"一二三四"}, "", {{{"一", "二", "三"}}}, 3);
  }
  testSplit({"aa2bb3cc"}, "[1-9]+", {{{"aa", "bb", "cc"}}});
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
