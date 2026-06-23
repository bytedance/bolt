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

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace bytedance::bolt::functions::sparksql::test {
namespace {

using namespace bytedance::bolt::test;

class StringSplitSQLTest : public SparkFunctionBaseTest {};

TEST_F(StringSplitSQLTest, basicAndUtf8Cases) {
  auto input = makeNullableFlatVector<std::string>({
      "a-b-c-d", // simple ascii
      "a--b", // consecutive delimiters
      "", // empty input, non-empty delimiter
      "abc", // non-empty input, empty delimiter
      "", // empty input, empty delimiter
      "one,,,four,", // trailing empty segment
      "a-b-c", // multi-char delimiter
      "苹果@@香蕉@@芒果", // multi-byte delimiter
      "aлbлcлd", // non-ascii single-byte delimiter in UTF-8
  });

  auto delimiters = makeNullableFlatVector<std::string>({
      "-", // for "a-b-c-d"
      "-", // for "a--b"
      "-", // for ""
      "", // for "abc"
      "", // for ""
      ",", // for "one,,,four,"
      "-b-", // for "a-b-c"
      "@@", // for "苹果@@香蕉@@芒果"
      "л", // for "aлbлcлd"
  });

  auto expected = makeArrayVector<std::string>({
      {"a", "b", "c", "d"},
      {"a", "", "b"},
      {""},
      {"abc"},
      {""},
      {"one", "", "", "four", ""},
      {"a", "c"},
      {"苹果", "香蕉", "芒果"},
      {"a", "b", "c", "d"},
  });

  auto result = evaluate<ArrayVector>(
      "string_split_sql(c0, c1)", makeRowVector({input, delimiters}));

  assertEqualVectors(expected, result);
}

TEST_F(StringSplitSQLTest, nullInputOrDelimiter) {
  auto input = makeNullableFlatVector<std::string>({
      std::nullopt,
      "abc",
      std::nullopt,
  });

  auto delimiters = makeNullableFlatVector<std::string>({
      "-", // input is null
      std::nullopt, // delimiter is null
      std::nullopt, // both are null
  });

  auto result = evaluate<ArrayVector>(
      "string_split_sql(c0, c1)", makeRowVector({input, delimiters}));

  ASSERT_EQ(result->size(), 3);
  EXPECT_TRUE(result->isNullAt(0));
  EXPECT_TRUE(result->isNullAt(1));
  EXPECT_TRUE(result->isNullAt(2));
}

TEST_F(StringSplitSQLTest, delimiterBoundaryCases) {
  auto input = makeNullableFlatVector<std::string>({
      "abc", // delimiter not found anywhere in input
      "-abc", // delimiter at the very start of input
      "aa", // delimiter equals the whole input
  });

  auto delimiters = makeNullableFlatVector<std::string>({
      "/",
      "-",
      "aa",
  });

  auto expected = makeArrayVector<std::string>({
      {"abc"},
      {"", "abc"},
      {"", ""},
  });

  auto result = evaluate<ArrayVector>(
      "string_split_sql(c0, c1)", makeRowVector({input, delimiters}));

  assertEqualVectors(expected, result);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
