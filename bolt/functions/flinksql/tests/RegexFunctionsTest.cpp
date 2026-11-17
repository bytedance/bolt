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

#include <unicode/regex.h>
#include <limits>
#include <memory>

#include "bolt/functions/lib/RegexCache.h"
#include "bolt/functions/lib/string/RegexUtils.h"

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <unicode/ucnv.h>

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

TEST_F(FlinkRegexFunctionsTest, unterminatedPatternWithTrailingBackslash) {
  const std::string pattern = "abcdefghijkl\\";
  auto buffer = std::make_unique<char[]>(pattern.size());
  std::copy(pattern.begin(), pattern.end(), buffer.get());
  const StringView view(buffer.get(), pattern.size());
  ASSERT_FALSE(view.isInline());
  EXPECT_EQ(regex::transDanglingRightBrackets(view), pattern);
}

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

TEST_P(FlinkRegexpExtractTest, variablePatternsAndGroups) {
  auto inputs = makeNullableFlatVector<std::string>(
      {"foo123",
       "fooabc",
       "foo123",
       "foo123",
       "b",
       "b",
       std::nullopt,
       "foo123"});
  auto patterns = makeNullableFlatVector<std::string>(
      {"foo([0-9]+)",
       "foo([a-z]+)",
       "*",
       "foo([0-9]+)",
       "(a)?b",
       "(a*)b",
       "foo(.*)",
       std::nullopt});
  for (const auto& groupType : std::vector<TypePtr>{INTEGER(), BIGINT()}) {
    VectorPtr groups = groupType->isInteger()
        ? VectorPtr(makeNullableFlatVector<int32_t>({1, 1, 0, -1, 1, 1, 1, 1}))
        : VectorPtr(makeNullableFlatVector<int64_t>({1, 1, 0, -1, 1, 1, 1, 1}));
    auto input = makeRowVector({inputs, patterns, groups});
    auto expected = makeNullableFlatVector<std::string>(
        {"123",
         "abc",
         std::nullopt,
         std::nullopt,
         std::nullopt,
         "",
         std::nullopt,
         std::nullopt});
    const auto expression = fmt::format("{}(c0, c1, c2)", functionName());
    testEncodings(
        parseExpression(expression, asRowType(input->type())),
        input->children(),
        expected);
  }
  auto input = makeRowVector({inputs, patterns});
  testEncodings(
      parseExpression(
          fmt::format("{}(c0, c1)", functionName()), asRowType(input->type())),
      input->children(),
      makeNullableFlatVector<std::string>(
          {"foo123",
           "fooabc",
           std::nullopt,
           "foo123",
           "b",
           "b",
           std::nullopt,
           std::nullopt}));
}

TEST_P(FlinkRegexpExtractTest, variablePatternsAcrossBatches) {
  auto expression = compileExpression(
      fmt::format("{}(c0, c1, 1)", functionName()),
      ROW({"c0", "c1"}, {VARCHAR(), VARCHAR()}));
  VectorPtr retained;
  for (int batch = 0; batch < 3; ++batch) {
    auto input = makeRowVector(
        {makeFlatVector<std::string>(
             96,
             [](auto) {
               return "long_result_with_more_than_twelve_characters";
             }),
         makeFlatVector<std::string>(96, [&](auto row) {
           return fmt::format(
               "(long_result_with_more_than_twelve_characters)(?:x{{{}}})?",
               batch * 96 + row);
         })});
    auto result = evaluate(*expression, input);
    bytedance::bolt::test::assertEqualVectors(input->childAt(0), result);
    if (batch == 0) {
      retained = result;
    }
  }
  bytedance::bolt::test::assertEqualVectors(
      makeFlatVector<std::string>(
          96,
          [](auto) { return "long_result_with_more_than_twelve_characters"; }),
      retained);
}

TEST_P(FlinkRegexpExtractTest, constantPatternsAcrossBatches) {
  const std::string inputText =
      "foo_long_result_with_more_than_twelve_characters";
  const std::string extracted = "_long_result_with_more_than_twelve_characters";
  for (const auto& groupSql : {"", ", 1", ", c2"}) {
    SCOPED_TRACE(groupSql);
    auto expression = compileExpression(
        fmt::format("{}(c0, c1{})", functionName(), groupSql),
        ROW({"c0", "c1", "c2"}, {VARCHAR(), VARCHAR(), INTEGER()}));
    VectorPtr retained;
    for (const auto& pattern : {"foo(.*)", "foo([a-z]+)", "*", "foo(.*)"}) {
      SCOPED_TRACE(pattern);
      auto input = makeRowVector(
          {makeNullableFlatVector<std::string>(
               {inputText, "fooabc", std::nullopt}),
           makeConstant(StringView(pattern), 3),
           makeFlatVector<int32_t>({1, 1, 0})});
      auto result = evaluate(*expression, input);
      const bool wholeMatch = std::string_view(groupSql).empty();
      const bool invalid = std::string_view(pattern) == "*";
      const bool matchFirst = std::string_view(pattern) == "foo(.*)";
      bytedance::bolt::test::assertEqualVectors(
          makeNullableFlatVector<std::string>(
              {matchFirst ? std::optional<std::string>(
                                wholeMatch ? inputText : extracted)
                          : std::nullopt,
               invalid
                   ? std::nullopt
                   : std::optional<std::string>(wholeMatch ? "fooabc" : "abc"),
               std::nullopt}),
          result);
      if (!retained) {
        retained = result;
      }
    }
    bytedance::bolt::test::assertEqualVectors(
        makeNullableFlatVector<std::string>(
            {std::string_view(groupSql).empty() ? inputText : extracted,
             std::string_view(groupSql).empty() ? "fooabc" : "abc",
             std::nullopt}),
        retained);
  }
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

TEST_F(FlinkRegexFunctionsTest, literalReplacement) {
  auto input = makeRowVector(
      {makeNullableFlatVector<std::string>(
           {"foobar",
            "foobar",
            "ö😀ö",
            "",
            "abc",
            "foobar",
            "foobar",
            std::nullopt}),
       makeNullableFlatVector<std::string>(
           {"o", "o", "ö", "", "", "*", std::nullopt, "o"}),
       makeNullableFlatVector<std::string>(
           {"\\", "$1", "[$1\\]", "#", "#", "x", "x", "x"})});
  auto expected = makeNullableFlatVector<std::string>(
      {"f\\\\bar",
       "f$1$1bar",
       "[$1\\]😀[$1\\]",
       "#",
       "#a#b#c#",
       std::nullopt,
       std::nullopt,
       std::nullopt});
  testEncodings(
      parseExpression(
          "flink_regexp_replace(c0,c1,c2)", asRowType(input->type())),
      input->children(),
      expected);

  auto values =
      makeRowVector({makeFlatVector<std::string>({"foobar", "foofoo", "foo"})});
  testEncodings(
      parseExpression(
          "flink_regexp_replace(c0,'(foo)','$1')", asRowType(values->type())),
      values->children(),
      makeFlatVector<std::string>({"$1bar", "$1$1", "$1"}));
  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<std::string>(
          "flink_regexp_replace(c0,'*','x')",
          std::optional<std::string>{"abc"}));
}

TEST_F(FlinkRegexFunctionsTest, literalReplacementUsesUtf8) {
  const std::string originalCharset = ucnv_getDefaultName();
  auto restore =
      folly::makeGuard([&]() { ucnv_setDefaultName(originalCharset.c_str()); });
  ucnv_setDefaultName("ISO-8859-1");
  EXPECT_EQ(
      "X😀X",
      evaluateOnce<std::string>(
          "flink_regexp_replace(c0, 'ö', 'X')",
          std::optional<std::string>{"ö😀ö"}));
  auto input = makeRowVector(
      {makeFlatVector<std::string>({"ö😀ö", "ö", "abc"}),
       makeFlatVector<std::string>({"ö", "ö", "a"})});
  testEncodings(
      parseExpression(
          "flink_regexp_replace(c0,c1,'$1')", asRowType(input->type())),
      input->children(),
      makeFlatVector<std::string>({"$1😀$1", "$1", "$1bc"}));
}

TEST_F(FlinkRegexFunctionsTest, literalReplacementAcrossBatches) {
  auto expression = compileExpression(
      "flink_regexp_replace(c0,c1,c2)",
      ROW({"c0", "c1", "c2"}, {VARCHAR(), VARCHAR(), VARCHAR()}));
  const std::string text = "aaa01234567890123456789";
  const std::string unicode = "ö😀ö01234567890123456789";
  VectorPtr retained;
  for (const auto& pattern : {"a", "missing", "*", "a"}) {
    auto input = makeRowVector(
        {makeFlatVector<std::string>({text, unicode}),
         makeConstant(StringView(pattern), 2),
         makeConstant(StringView(R"($1\)"), 2)});
    auto result = evaluate(*expression, input);
    const bool invalid = std::string_view(pattern) == "*";
    const bool matching = std::string_view(pattern) == "a";
    bytedance::bolt::test::assertEqualVectors(
        makeNullableFlatVector<std::string>(
            {invalid
                 ? std::nullopt
                 : std::optional<std::string>(
                       matching ? R"($1\$1\$1\01234567890123456789)" : text),
             invalid ? std::nullopt : std::optional<std::string>(unicode)}),
        result);
    if (std::string_view(pattern) == "missing") {
      EXPECT_EQ(
          result->asFlatVector<StringView>()->valueAt(0).data(),
          input->childAt(0)->asFlatVector<StringView>()->valueAt(0).data());
      retained = result;
    }
  }
  bytedance::bolt::test::assertEqualVectors(
      makeFlatVector<std::string>({text, unicode}), retained);
  EXPECT_EQ(
      std::string("abc\xef\xbf\xbd"),
      evaluateOnce<std::string>(
          "flink_regexp_replace(c0,'missing','x')",
          std::optional<std::string>(std::string("abc\xff", 4))));
}

TEST_F(FlinkICURegexpExtractTest, variableJavaPatterns) {
  auto input = makeRowVector(
      {makeFlatVector<std::string>({"foobar", "foobar", "ö😀ö"}),
       makeFlatVector<std::string>({"foo(?=bar)", "(?<=foo)bar", "(ö😀)"})});
  testEncodings(
      parseExpression("icu_regexp_extract(c0,c1)", asRowType(input->type())),
      input->children(),
      makeFlatVector<std::string>({"foo", "bar", "ö😀"}));
}

TEST_F(FlinkRegexFunctionsTest, regexCacheOwnershipAndEviction) {
  regex::RegexCache<int> cache;
  int compilations = 0;
  const auto compile = [&](StringView) {
    return std::make_shared<const int>(++compilations);
  };
  auto pinned = cache.get(StringView("first"), compile);
  std::weak_ptr<const int> weak = pinned;
  EXPECT_EQ(pinned, cache.get(StringView("first"), compile));
  for (int i = 0; i < 80; ++i) {
    auto key = fmt::format("pattern_{}", i);
    cache.get(StringView(key), compile);
  }
  EXPECT_FALSE(weak.expired());
  pinned.reset();
  EXPECT_TRUE(weak.expired());
  auto key = std::string(4097, 'x');
  auto large = cache.get(StringView(key), compile);
  EXPECT_NE(large, cache.get(StringView(key), compile));
  int invalidCompilations = 0;
  const auto invalid = [&](StringView) {
    ++invalidCompilations;
    return std::shared_ptr<const int>();
  };
  EXPECT_EQ(nullptr, cache.get(StringView("invalid"), invalid));
  EXPECT_EQ(nullptr, cache.get(StringView("invalid"), invalid));
  EXPECT_EQ(1, invalidCompilations);
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

TEST_P(FlinkRegexpExtractTest, bigintGroupIdUsesJavaNarrowing) {
  constexpr int64_t kIntRange = int64_t{1} << 32;
  const std::vector<std::optional<int64_t>> groups = {
      0,
      1,
      2,
      -1,
      kIntRange,
      kIntRange + 1,
      -kIntRange,
      -kIntRange + 1,
      -kIntRange - 1,
      std::numeric_limits<int32_t>::max(),
      std::numeric_limits<int32_t>::min(),
      int64_t{1} << 31,
      std::numeric_limits<int64_t>::max(),
      std::numeric_limits<int64_t>::min(),
      std::numeric_limits<int64_t>::min() + 1,
      std::nullopt};
  const std::vector<std::optional<std::string>> expected = {
      "foo123",
      "123",
      std::nullopt,
      std::nullopt,
      "foo123",
      "123",
      "foo123",
      "123",
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      "foo123",
      "123",
      std::nullopt};
  auto values =
      makeFlatVector<std::string>(groups.size(), [](auto) { return "foo123"; });
  auto pattern = makeFlatVector<std::string>(
      groups.size(), [](auto) { return "foo([0-9]+)"; });
  for (const auto& patterns : std::vector<VectorPtr>{
           pattern, makeConstant(StringView("foo([0-9]+)"), groups.size())}) {
    auto input = makeRowVector(
        {values, patterns, makeNullableFlatVector<int64_t>(groups)});
    for (const auto& patternSql : {"'foo([0-9]+)'", "c1"}) {
      SCOPED_TRACE(patternSql);
      bytedance::bolt::test::assertEqualVectors(
          makeNullableFlatVector<std::string>(expected),
          evaluate(
              fmt::format("{}(c0, {}, c2)", functionName(), patternSql),
              input));
      for (size_t i = 0; i < groups.size(); ++i) {
        SCOPED_TRACE(i);
        const auto groupSql = groups[i]
            ? fmt::format("cast('{}' as bigint)", *groups[i])
            : "cast(null as bigint)";
        bytedance::bolt::test::assertEqualVectors(
            makeNullableFlatVector<std::string>(
                std::vector<std::optional<std::string>>(
                    groups.size(), expected[i])),
            evaluate(
                fmt::format(
                    "{}(c0, {}, {})", functionName(), patternSql, groupSql),
                input));
      }
    }
  }
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
TEST_F(FlinkICURegexpExtractTest, malformedUtf8) {
  for (const auto& input :
       {std::string("a\xff"
                    "suffix"),
        std::string("a long prefix\xff"
                    "suffix")}) {
    EXPECT_EQ(
        "suffix",
        evaluateOnce<std::string>(
            "icu_regexp_extract(c0, '(suffix)$', 1)",
            std::optional<std::string>(input)));
  }
  EXPECT_EQ(
      "\xef\xbf\xbd",
      evaluateOnce<std::string>(
          "icu_regexp_extract(c0, '(.)', 1)",
          std::optional<std::string>("\xff")));
}

} // namespace bytedance::bolt::functions::flinksql::test
