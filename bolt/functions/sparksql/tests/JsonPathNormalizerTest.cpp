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

#include "bolt/functions/prestosql/json/JsonExtractor.h"

#include <string_view>

#include "gtest/gtest.h"

namespace bytedance::bolt::functions::detail {
namespace {

std::string normalize(std::string_view path) {
  return normalizeJsonPath(StringView(path.data(), path.size()));
}

TEST(JsonPathNormalizerTest, root) {
  EXPECT_EQ(normalize(""), "");
  EXPECT_EQ(normalize("foo"), "foo");
  EXPECT_EQ(normalize("$"), "$");
  EXPECT_EQ(normalize("$ .foo"), "$ .foo");

  for (const char whitespace : {' ', '\t', '\n', '\v', '\f', '\r'}) {
    std::string path{"$"};
    path.append(3, whitespace);

    EXPECT_EQ(normalize(path), "$")
        << "whitespace code: "
        << static_cast<int>(static_cast<unsigned char>(whitespace));
  }
}

TEST(JsonPathNormalizerTest, dotNotation) {
  EXPECT_EQ(normalize("$.foo"), "$.foo");
  EXPECT_EQ(normalize("$. foo"), "$.foo");
  EXPECT_EQ(normalize("$.  foo.   bar"), "$.foo.bar");
  EXPECT_EQ(normalize("$.foo bar"), "$.foo bar");
  EXPECT_EQ(normalize("$.foo .bar"), "$.foo .bar");

  EXPECT_EQ(normalize("$..foo"), "$..foo");
  EXPECT_EQ(normalize("$.foo..bar"), "$.foo..bar");
  EXPECT_EQ(normalize("$.foo.. bar"), "$.foo..bar");
  EXPECT_EQ(normalize("$.foo.  .bar"), "$.foo..bar");
  EXPECT_EQ(normalize("$.foo."), "$.foo.");
  EXPECT_EQ(normalize("$.foo.  "), "$.foo.");
}

TEST(JsonPathNormalizerTest, whitespaceAfterDot) {
  for (const char whitespace : {' ', '\t', '\n', '\v', '\f', '\r'}) {
    std::string path = "$.";
    path.push_back(whitespace);
    path.append("outer.");
    path.push_back(whitespace);
    path.append("value");

    EXPECT_EQ(normalize(path), "$.outer.value")
        << "whitespace code: "
        << static_cast<int>(static_cast<unsigned char>(whitespace));
  }
}

TEST(JsonPathNormalizerTest, bracketNotation) {
  EXPECT_EQ(normalize("$[0]"), "$[0]");
  EXPECT_EQ(normalize("$[*]"), "$[*]");
  EXPECT_EQ(normalize("$['a-b']"), "$['a-b']");
  EXPECT_EQ(normalize("$['a:b']. value"), "$['a:b'].value");
  EXPECT_EQ(normalize(R"($["a. b"])"), R"($["a. b"])");
  EXPECT_EQ(normalize(R"($["a]b"])"), R"($["a]b"])");
}

TEST(JsonPathNormalizerTest, escapedQuotedKeys) {
  EXPECT_EQ(normalize(R"($['a\'b'])"), R"($['a\'b'])");
  EXPECT_EQ(normalize(R"($["a\"b"])"), R"($["a\"b"])");
  EXPECT_EQ(normalize(R"($["a\\b"])"), R"($["a\\b"])");
}

TEST(JsonPathNormalizerTest, preservesInvalidSyntaxForTokenizer) {
  EXPECT_EQ(normalize("$["), "$[");
  EXPECT_EQ(normalize("$[ 0]"), "$[ 0]");
  EXPECT_EQ(normalize("$[' a']"), "$[' a']");
  EXPECT_EQ(normalize("$[' a\\']. '].  b"), "$[' a\\']. '].b");
  EXPECT_EQ(normalize(R"($[" a"])"), R"($[" a"])");
  EXPECT_EQ(normalize("$[0] .foo"), "$[0] .foo");
}

TEST(JsonPathNormalizerTest, leavesStructuralValidationToTokenizer) {
  EXPECT_EQ(normalize("$[abc"), "$[abc");
  EXPECT_EQ(normalize("$['"), "$['");
  EXPECT_EQ(normalize("$['abc"), "$['abc");
  EXPECT_EQ(normalize("$['a' ]"), "$['a' ]");
}

} // namespace
} // namespace bytedance::bolt::functions::detail
