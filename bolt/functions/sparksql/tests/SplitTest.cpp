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

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace bytedance::bolt::test;
namespace bytedance::bolt::functions::sparksql::test {
namespace {

class SplitTest : public SparkFunctionBaseTest {
 protected:
  void testSplit(
      const std::vector<std::string>& input,
      const std::string& delim,
      std::optional<int32_t> limit,
      size_t numRows,
      const std::vector<std::vector<std::string>>& expected) {
    auto strings = makeFlatVector<std::string>(input);
    auto delims = makeConstant(StringView{delim}, numRows);

    std::vector<std::vector<StringView>> expectedSv;
    expectedSv.reserve(expected.size());
    for (const auto& row : expected) {
      std::vector<StringView> rowSv;
      rowSv.reserve(row.size());
      for (const auto& s : row) {
        rowSv.emplace_back(StringView(s));
      }
      expectedSv.emplace_back(std::move(rowSv));
    }
    auto expectedResult = makeArrayVector<StringView>(expectedSv);

    VectorPtr result;
    if (limit.has_value()) {
      auto limits = makeConstant<int32_t>(limit.value(), numRows);
      auto input = makeRowVector({strings, delims, limits});
      result = evaluate("split(c0, c1, c2)", input);
    } else {
      auto input = makeRowVector({strings, delims});
      result = evaluate("split(c0, c1)", input);
    }
    assertEqualVectors(expectedResult, result);
  };
};

TEST_F(SplitTest, basic) {
  auto numRows = 4;
  auto input = std::vector<std::string>{
      {"I,he,she,they"},
      {"one,,,four,"},
      {"a,\xED,\xA0,123"},
      {""},
  };
  auto expected = std::vector<std::vector<std::string>>({
      {"I", "he", "she", "they"},
      {"one", "", "", "four", ""},
      {"a", "\xED", "\xA0", "123"},
      {""},
  });

  testSplit(input, ",", std::nullopt, numRows, expected);
  testSplit(input, ",", -1, numRows, expected);
  testSplit(input, ",", 10, numRows, expected);

  expected = {
      {"I", "he", "she,they"},
      {"one", "", ",four,"},
      {"a", "\xED", "\xA0,123"},
      {""},
  };
  testSplit(input, ",", 3, numRows, expected);

  expected = {
      {"I,he,she,they"},
      {"one,,,four,"},
      {"a,\xED,\xA0,123"},
      {""},
  };
  testSplit(input, ",", 1, numRows, expected);

  input = {
      {"синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear"},
      {"зелёное небоలేదాలేదాలేదా緑の空లేదా"},
      {"aలేదా\xEDలేదా\xA0లేదా123"},
      {""},
  };
  expected = {
      {"синяя слива", "赤いトマト", "黃苹果", "brown pear"},
      {"зелёное небо", "", "", "緑の空", ""},
      {"a", "\xED", "\xA0", "123"},
      {""},
  };
  testSplit(input, "లేదా", std::nullopt, numRows, expected);
  testSplit(input, "లేదా", -1, numRows, expected);
  testSplit(input, "లేదా", 10, numRows, expected);

  expected = {
      {"синяя слива", "赤いトマト", "黃苹果లేదాbrown pear"},
      {"зелёное небо", "", "లేదా緑の空లేదా"},
      {"a", "\xED", "\xA0లేదా123"},
      {""},
  };
  testSplit(input, "లేదా", 3, numRows, expected);

  expected = {
      {"синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear"},
      {"зелёное небоలేదాలేదాలేదా緑の空లేదా"},
      {"aలేదా\xEDలేదా\xA0లేదా123"},
      {""},
  };
  testSplit(input, "లేదా", 1, numRows, expected);

  testSplit(input, "A", std::nullopt, numRows, expected);
}

TEST_F(SplitTest, emptyDelimiter) {
  auto numRows = 4;
  auto input = std::vector<std::string>{
      {"I,he,she,they"},
      {"one,,,four,"},
      {"a\xED\xA0@123"},
      {""},
  };
  auto expected = std::vector<std::vector<std::string>>({
      {"I", ",", "h", "e", ",", "s", "h", "e", ",", "t", "h", "e", "y"},
      {"o", "n", "e", ",", ",", ",", "f", "o", "u", "r", ","},
      {"a", "\xED\xA0", "@", "1", "2", "3"},
      {""},
  });

  testSplit(input, "", std::nullopt, numRows, expected);
  testSplit(input, "", -1, numRows, expected);
  testSplit(input, "", 20, numRows, expected);

  expected = {
      {"I", ",", "h"},
      {"o", "n", "e"},
      {"a", "\xED\xA0", "@"},
      {""},
  };
  testSplit(input, "", 3, numRows, expected);

  expected = {
      {"I"},
      {"o"},
      {"a"},
      {""},
  };
  testSplit(input, "", 1, numRows, expected);

  input = std::vector<std::string>{
      {"синяя赤いトマト緑の"},
      {"Hello世界🙂"},
      {"a\xED\xA0@123"},
      {""},
  };
  expected = {
      {"с", "и", "н", "я", "я", "赤", "い", "ト", "マ", "ト", "緑", "の"},
      {"H", "e", "l", "l", "o", "世", "界", "🙂"},
      {"a", "\xED\xA0", "@", "1", "2", "3"},
      {""},
  };
  testSplit(input, "", std::nullopt, numRows, expected);

  expected = {
      {"с", "и"},
      {"H", "e"},
      {"a", "\xED\xA0"},
      {""},
  };
  testSplit(input, "", 2, numRows, expected);
}

TEST_F(SplitTest, regexDelimiter) {
  auto input = std::vector<std::string>{
      "1a 2b \xA0 14m",
      "1a 2b \xA0 14",
      "",
      "a123b",
  };
  auto numRows = 4;
  auto expected = std::vector<std::vector<std::string>>({
      {"1", "2", "\xA0 14", ""},
      {"1", "2", "\xA0 14"},
      {""},
      {"", "123", ""},
  });

  testSplit(input, "\\s*[a-z]+\\s*", std::nullopt, numRows, expected);
  testSplit(input, "\\s*[a-z]+\\s*", -1, numRows, expected);
  testSplit(input, "\\s*[a-z]+\\s*", 10, numRows, expected);

  expected = {
      {"1", "2", "\xA0 14m"},
      {"1", "2", "\xA0 14"},
      {""},
      {"", "123", ""},
  };
  testSplit(input, "\\s*[a-z]+\\s*", 3, numRows, expected);

  expected = {
      {"1a 2b \xA0 14m"},
      {"1a 2b \xA0 14"},
      {""},
      {"a123b"},
  };
  testSplit(input, "\\s*[a-z]+\\s*", 1, numRows, expected);

  numRows = 3;
  input = std::vector<std::string>{
      {"синя🙂赤トマト🙂緑の"},
      {"Hello🙂世界\xED🙂"},
      {""},
  };
  expected = std::vector<std::vector<std::string>>({
      {"с",
       "и",
       "н",
       "я",
       "🙂",
       "赤",
       "ト",
       "マ",
       "ト",
       "🙂",
       "緑",
       "の",
       ""},
      {"H", "e", "l", "l", "o", "🙂", "世", "界", "\xED", "🙂", ""},
      {""},
  });

  testSplit(input, "A|", std::nullopt, numRows, expected);

  expected = {
      {"с", "иня🙂赤トマト🙂緑の"},
      {"H", "ello🙂世界\xED🙂"},
      {""},
  };
  testSplit(input, "A|", 2, numRows, expected);

  expected = {
      {"синя", "赤トマト", "緑の"},
      {"Hello", "世界\xED", ""},
      {""},
  };
  testSplit(input, "🙂", -1, numRows, expected);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
