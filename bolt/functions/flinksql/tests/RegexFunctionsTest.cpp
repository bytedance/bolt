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

#include <limits>

#include <fmt/format.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"

namespace bytedance::bolt::functions::flinksql::test {
namespace {

class FlinkRegexFunctionsTest : public FlinkFunctionBaseTest {};

class FlinkRegexpExtractTest : public FlinkFunctionBaseTest,
                               public testing::WithParamInterface<std::string> {
 protected:
  const std::string& functionName() const {
    return GetParam();
  }

  template <typename T>
  std::optional<std::string> extract(
      const std::optional<std::string>& input,
      const std::string& pattern,
      const std::optional<T>& groupId) {
    return evaluateOnce<std::string>(
        fmt::format("{}(c0, '{}', c1)", functionName(), pattern),
        input,
        groupId);
  }
};

class FlinkICURegexpExtractTest : public FlinkFunctionBaseTest {};

TEST_F(FlinkRegexFunctionsTest, invalidRegexReturnsFalse) {
  EXPECT_EQ(
      false,
      evaluateOnce<bool>(
          "rlike(c0, c1)",
          makeRowVector({
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"abc"}),
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"**"}),
          })));

  EXPECT_EQ(
      false,
      evaluateOnce<bool>(
          "rlike(c0, '**')",
          makeRowVector({makeNullableFlatVector(
              std::vector<std::optional<std::string>>{"abc"})})));
}

TEST_F(FlinkRegexFunctionsTest, validRegexStillWorks) {
  EXPECT_EQ(
      true,
      evaluateOnce<bool>(
          "rlike(c0, c1)",
          makeRowVector({
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"abc123"}),
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"[0-9]+"}),
          })));

  EXPECT_EQ(
      false,
      evaluateOnce<bool>(
          "rlike(c0, c1)",
          makeRowVector({
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"abc"}),
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"[0-9]+"}),
          })));

  EXPECT_EQ(
      true,
      evaluateOnce<bool>(
          "rlike(c0, '[0-9]+')",
          makeRowVector({makeNullableFlatVector(
              std::vector<std::optional<std::string>>{"abc123"})})));
}

TEST_F(FlinkRegexFunctionsTest, nullInputPreservesNullSemantics) {
  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<bool>(
          "rlike(c0, c1)",
          makeRowVector({
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{std::nullopt}),
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"[0-9]+"}),
          })));

  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<bool>(
          "rlike(c0, c1)",
          makeRowVector({
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"abc"}),
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{std::nullopt}),
          })));
}

TEST_P(FlinkRegexpExtractTest, basicSemantics) {
  EXPECT_EQ(
      "bar",
      extract(
          std::optional<std::string>{"foothebar"},
          "foo(.*?)(bar)",
          std::optional<int32_t>{2}));
  EXPECT_EQ(
      "foothebar",
      extract(
          std::optional<std::string>{"foothebar"},
          "foo(.*?)(bar)",
          std::optional<int32_t>{0}));
  EXPECT_EQ(
      "the",
      extract(
          std::optional<std::string>{"foothebar"},
          "foo(.*?)(bar)",
          std::optional<int32_t>{1}));
  EXPECT_EQ(
      "thebar",
      extract(
          std::optional<std::string>{"foothebar"},
          "foo([\\w]+)",
          std::optional<int32_t>{1}));
  EXPECT_EQ(
      std::nullopt,
      extract(
          std::optional<std::string>{"foothebar"},
          "foo([\\d]+)",
          std::optional<int32_t>{1}));
  EXPECT_EQ(
      std::nullopt,
      extract(
          std::optional<std::string>{std::nullopt},
          "foo(.*?)(bar)",
          std::optional<int32_t>{2}));
  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<std::string>(
          fmt::format("{}(c0, cast(null as varchar), c1)", functionName()),
          std::optional<std::string>{"foothebar"},
          std::optional<int32_t>{2}));
  EXPECT_EQ(
      "foothebar",
      evaluateOnce<std::string>(
          fmt::format("{}(c0, 'foo(.*?)(bar)')", functionName()),
          std::optional<std::string>{"foothebar"}));
}

TEST_P(FlinkRegexpExtractTest, returnsNullOnFailure) {
  EXPECT_EQ(
      std::nullopt,
      extract(
          std::optional<std::string>{"foothebar"},
          "foo([\\d]+)",
          std::optional<int32_t>{1}));
  EXPECT_EQ(
      std::nullopt,
      extract(
          std::optional<std::string>{"foothebar"},
          "*",
          std::optional<int32_t>{0}));
  EXPECT_EQ(
      std::nullopt,
      extract(
          std::optional<std::string>{"foothebar"},
          "foo(.*?)(bar)",
          std::optional<int32_t>{-1}));
  EXPECT_EQ(
      std::nullopt,
      extract(
          std::optional<std::string>{"foothebar"},
          "foo(.*?)(bar)",
          std::optional<int32_t>{3}));
  EXPECT_EQ(
      std::nullopt,
      extract(
          std::optional<std::string>{"foothebar"},
          "foo(.*?)(bar)",
          std::optional<int64_t>{std::numeric_limits<int64_t>::max()}));
  EXPECT_EQ(
      std::nullopt,
      extract(
          std::optional<std::string>{"b"}, "(a)?b", std::optional<int32_t>{1}));
  EXPECT_EQ(
      "",
      extract(
          std::optional<std::string>{"b"}, "(a*)b", std::optional<int32_t>{1}));
}

TEST_P(FlinkRegexpExtractTest, hostnameNoMatch) {
  EXPECT_EQ(
      std::nullopt,
      extract(
          std::optional<std::string>{"dc05-p2-ta22-n146.byted.org:8052"},
          "n\\d{1,3}\\-\\d{1,3}\\-\\d{1,3}",
          std::optional<int32_t>{0}));
}

TEST_P(FlinkRegexpExtractTest, rejectsVariablePattern) {
  auto input = makeRowVector({
      makeFlatVector<std::string>({"foothebar"}),
      makeFlatVector<std::string>({"foo(.*?)(bar)"}),
  });
  BOLT_ASSERT_THROW(
      evaluate(fmt::format("{}(c0, c1, 2)", functionName()), input),
      fmt::format("{} requires a constant pattern", functionName()));
}

TEST_P(FlinkRegexpExtractTest, preservesRowsAndInputBuffers) {
  auto input = makeRowVector({
      makeNullableFlatVector<std::string>(
          {"foothebar",
           std::nullopt,
           "foo123",
           "foo_string_with_more_than_12_chars"}),
  });
  auto expected = makeNullableFlatVector<std::string>(
      {"thebar", std::nullopt, "123", "_string_with_more_than_12_chars"});
  bytedance::bolt::test::assertEqualVectors(
      expected,
      evaluate(fmt::format("{}(c0, 'foo([\\w]+)', 1)", functionName()), input));
}

TEST_F(FlinkICURegexpExtractTest, javaRegexSemantics) {
  EXPECT_EQ(
      "foo",
      evaluateOnce<std::string>(
          "icu_regexp_extract(c0, 'foo(?=bar)', 0)",
          std::optional<std::string>{"foobar"}));
  EXPECT_EQ(
      "b",
      evaluateOnce<std::string>(
          "icu_regexp_extract(c0, '[a[b]]', 0)",
          std::optional<std::string>{"b"}));
}

TEST_F(FlinkICURegexpExtractTest, bigintGroupIdUsesJavaNarrowing) {
  constexpr int64_t kGroupOneAfterNarrowing = (int64_t{1} << 32) + 1;

  EXPECT_EQ(
      "foothebar",
      evaluateOnce<std::string>(
          "icu_regexp_extract(c0, 'foo(.*?)(bar)', "
          "cast(4294967296 as bigint))",
          std::optional<std::string>{"foothebar"}));
  EXPECT_EQ(
      "the",
      evaluateOnce<std::string>(
          "icu_regexp_extract(c0, 'foo(.*?)(bar)', c1)",
          std::optional<std::string>{"foothebar"},
          std::optional<int64_t>{kGroupOneAfterNarrowing}));
}

TEST_F(FlinkICURegexpExtractTest, unicode) {
  EXPECT_EQ(
      "一龥三",
      evaluateOnce<std::string>(
          "icu_regexp_extract(c0, '(.*)-(.*)', 1)",
          std::optional<std::string>{"一龥三-一龥三"}));
}

std::string regexpExtractImplementationName(
    const testing::TestParamInfo<std::string>& info) {
  return info.param == "regexp_extract" ? "Re2" : "Icu";
}

INSTANTIATE_TEST_SUITE_P(
    Implementations,
    FlinkRegexpExtractTest,
    testing::Values("regexp_extract", "icu_regexp_extract"),
    regexpExtractImplementationName);

} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
