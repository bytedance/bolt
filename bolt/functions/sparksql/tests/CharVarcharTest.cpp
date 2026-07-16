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
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include <optional>
#include <string>

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class CharVarcharTest : public SparkFunctionBaseTest {
 protected:
  std::optional<std::string> charWrite(
      std::optional<std::string> input,
      int32_t limit) {
    return evaluateOnce<std::string>(
        "char_type_write_side_check(c0, c1)",
        input,
        std::optional<int32_t>{limit});
  }

  std::optional<std::string> charWrite(
      const std::string& input,
      int32_t limit) {
    return charWrite(std::optional<std::string>{input}, limit);
  }

  std::optional<std::string> charWrite(const char* input, int32_t limit) {
    return charWrite(std::optional<std::string>{input}, limit);
  }

  std::optional<std::string> varcharWrite(
      std::optional<std::string> input,
      int32_t limit) {
    return evaluateOnce<std::string>(
        "varchar_type_write_side_check(c0, c1)",
        input,
        std::optional<int32_t>{limit});
  }

  std::optional<std::string> varcharWrite(
      const std::string& input,
      int32_t limit) {
    return varcharWrite(std::optional<std::string>{input}, limit);
  }

  std::optional<std::string> varcharWrite(const char* input, int32_t limit) {
    return varcharWrite(std::optional<std::string>{input}, limit);
  }

  std::optional<std::string> readPadding(
      std::optional<std::string> input,
      int32_t limit) {
    return evaluateOnce<std::string>(
        "read_side_padding(c0, c1)", input, std::optional<int32_t>{limit});
  }

  std::optional<std::string> readPadding(
      const std::string& input,
      int32_t limit) {
    return readPadding(std::optional<std::string>{input}, limit);
  }

  std::optional<std::string> readPadding(const char* input, int32_t limit) {
    return readPadding(std::optional<std::string>{input}, limit);
  }
};

TEST_F(CharVarcharTest, charTypeWriteSideCheck) {
  EXPECT_EQ("abcde", charWrite("abcde", 5).value());
  EXPECT_EQ("abc  ", charWrite("abc", 5).value());
  EXPECT_EQ("abcde", charWrite("abcde ", 5).value());
  EXPECT_EQ("abcd ", charWrite("abcd  ", 5).value());

  BOLT_ASSERT_USER_THROW(
      charWrite("abcdef", 5), "Exceeds char/varchar type length limitation: 5");
  BOLT_ASSERT_USER_THROW(
      charWrite("abcdef ", 5),
      "Exceeds char/varchar type length limitation: 5");
}

TEST_F(CharVarcharTest, varcharTypeWriteSideCheck) {
  EXPECT_EQ("abc", varcharWrite("abc", 5).value());
  EXPECT_EQ("abcde", varcharWrite("abcde", 5).value());
  EXPECT_EQ("abcde", varcharWrite("abcde ", 5).value());
  EXPECT_EQ("abcd ", varcharWrite("abcd  ", 5).value());

  BOLT_ASSERT_USER_THROW(
      varcharWrite("abcdef", 5),
      "Exceeds char/varchar type length limitation: 5");
  BOLT_ASSERT_USER_THROW(
      varcharWrite("abcdef ", 5),
      "Exceeds char/varchar type length limitation: 5");
}

TEST_F(CharVarcharTest, readSidePadding) {
  EXPECT_EQ("abc  ", readPadding("abc", 5).value());
  EXPECT_EQ("abcde", readPadding("abcde", 5).value());
  EXPECT_EQ("abcdef", readPadding("abcdef", 5).value());
}

TEST_F(CharVarcharTest, unicodeCharactersCountByCodePoint) {
  const std::string hello = "\xE4\xBD\xA0\xE5\xA5\xBD";
  const std::string face = "\xF0\x9F\x98\x80";
  const std::string pound = "\xC2\xA3";
  const std::string ideographicSpace = "\xE3\x80\x80";

  EXPECT_EQ(hello + " ", charWrite(hello, 3).value());
  EXPECT_EQ(hello, charWrite(hello + " ", 2).value());
  EXPECT_EQ(hello + "x", charWrite(hello + "x ", 3).value());
  EXPECT_EQ(hello + " ", readPadding(hello, 3).value());
  EXPECT_EQ(hello + "  ", readPadding(hello, 4).value());
  EXPECT_EQ(face + " ", charWrite(face, 2).value());
  EXPECT_EQ(face, varcharWrite(face + " ", 1).value());
  EXPECT_EQ(pound + "ab", varcharWrite(pound + "ab ", 3).value());

  BOLT_ASSERT_USER_THROW(
      charWrite(hello + "x", 2),
      "Exceeds char/varchar type length limitation: 2");
  BOLT_ASSERT_USER_THROW(
      varcharWrite(hello + ideographicSpace, 2),
      "Exceeds char/varchar type length limitation: 2");
}

TEST_F(CharVarcharTest, nullInput) {
  EXPECT_EQ(std::nullopt, charWrite(std::optional<std::string>{}, 5));
  EXPECT_EQ(std::nullopt, varcharWrite(std::optional<std::string>{}, 5));
  EXPECT_EQ(std::nullopt, readPadding(std::optional<std::string>{}, 5));
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
