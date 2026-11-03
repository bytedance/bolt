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

#include "bolt/functions/sparksql/ICURegexFunctions.h"

#include <unicode/localpointer.h>
#include <unicode/regex.h>
#include <unicode/stringpiece.h>
#include <unicode/unistr.h>

#include "StringView.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/expression/VectorWriters.h"
#include "bolt/functions/lib/string/RegexUtils.h"
#include "bolt/functions/lib/string/StringCore.h"
#include "bolt/functions/lib/string/StringImpl.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ConstantVector.h"
namespace bytedance::bolt::functions::sparksql {
namespace {

icu::LocalPointer<icu::RegexPattern> getRegexPattern(
    StringView pattern,
    bool matchDanglingRightBrackets) {
  auto newPattern = regex::transRegexPattern(pattern);
  UErrorCode status = U_ZERO_ERROR;
  uint32_t flags = 0;
  icu::UnicodeString uPattern(newPattern.data(), newPattern.size());
  icu::LocalPointer<icu::RegexPattern> regex(
      icu::RegexPattern::compile(uPattern, flags, status));

  if (status == U_REGEX_RULE_SYNTAX && matchDanglingRightBrackets) {
    status = U_ZERO_ERROR;
    auto transPattern = regex::transDanglingRightBrackets(newPattern);
    icu::UnicodeString uPattern(transPattern.data(), transPattern.size());
    regex.adoptInstead(icu::RegexPattern::compile(uPattern, flags, status));
    if (UNLIKELY(U_FAILURE(status))) {
      BOLT_USER_FAIL(
          "failed to construct a RegexMatcher for trans regular expression '{}', error code : {}",
          transPattern,
          u_errorName(status));
    }
  }

  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to construct a RegexMatcher for trans regular expression '{}', error code : {}",
        newPattern,
        u_errorName(status));
  }

  return regex;
}

// Using icu regex library, need to copy characters from UnicodeString from
// StringView to execute regex operations and extract characters to string from
// UnicodeString to meet difference between UTF-8 and UTF-16,
bool partialMatch(
    StringView str,
    const icu::LocalPointer<icu::RegexPattern>& regexPattern) {
  UErrorCode status = U_ZERO_ERROR;
  icu::UnicodeString uStr(str.data(), str.size());
  icu::LocalPointer<icu::RegexMatcher> matcher(
      regexPattern->matcher(uStr, status));
  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to match the str '{}', error code : {}",
        str,
        u_errorName(status));
  }
  bool match = matcher->find(status);
  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to find the next pattern match in the input string '{}', error code : {}",
        str,
        u_errorName(status));
  }
  return match;
}

bool fullMatch(
    StringView str,
    const icu::LocalPointer<icu::RegexPattern>& regexPattern) {
  UErrorCode status = U_ZERO_ERROR;
  icu::UnicodeString uStr(str.data(), str.size());
  icu::LocalPointer<icu::RegexMatcher> matcher(
      regexPattern->matcher(uStr, status));
  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to match the str '{}', error code : {}",
        str,
        u_errorName(status));
  }
  bool match = matcher->matches(status);
  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to match the entire input region in the input string '{}', error code : {}",
        str,
        u_errorName(status));
  }
  return match;
}

bool fastPartialMatch(StringView str, StringView pattern) {
  return stringImpl::stringPosition<true>(
             std::string_view(str), std::string_view(pattern), 1) != 0;
}

bool fastFullMatch(StringView str, StringView pattern) {
  return str == pattern;
}

// Regex Vector Functions suffixed with 'constantPatternFunction' suffixes
// initialize the RegexMatcher when initialized to avoid recompiles
template <
    bool (*Fn)(StringView, const icu::LocalPointer<icu::RegexPattern>&),
    bool (*FastPathFn)(StringView, StringView)>
class ICURegexpMatchConstantPatternFunction : public exec::VectorFunction {
 public:
  explicit ICURegexpMatchConstantPatternFunction(
      StringView pattern,
      bool matchDanglingRightBrackets)
      : pattern_(pattern) {
    newPattern_ = std::string(pattern);
    isReg_ =
        stringCore::checkRegexPattern(newPattern_, matchDanglingRightBrackets);
    if (isReg_) {
      regexPattern_ = getRegexPattern(pattern, matchDanglingRightBrackets);
    } else {
      pattern_ = StringView(newPattern_);
    }
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_EQ(args.size(), 2);
    BOLT_CHECK_EQ(args[0]->typeKind(), TypeKind::VARCHAR);
    BOLT_CHECK_EQ(args[1]->typeKind(), TypeKind::VARCHAR);
    context.ensureWritable(rows, BOOLEAN(), result);
    auto resultFlat = result->asFlatVector<bool>();
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    if (isReg_) {
      rows.applyToSelected([&](vector_size_t row) {
        resultFlat->set(
            row, Fn(toSearch->valueAt<StringView>(row), regexPattern_));
      });
    } else {
      rows.applyToSelected([&](vector_size_t row) {
        resultFlat->set(
            row, FastPathFn(toSearch->valueAt<StringView>(row), pattern_));
      });
    }
  }

 private:
  std::string newPattern_;
  StringView pattern_;
  icu::LocalPointer<icu::RegexPattern> regexPattern_;
  bool isReg_;
};

template <
    bool (*Fn)(StringView, const icu::LocalPointer<icu::RegexPattern>&),
    bool (*FastPathFn)(StringView, StringView)>
class ICURegexpMatchFunction : public exec::VectorFunction {
 public:
  explicit ICURegexpMatchFunction(bool matchDanglingRightBrackets)
      : matchDanglingRightBrackets_(matchDanglingRightBrackets) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_EQ(args.size(), 2);
    BOLT_CHECK_EQ(args[0]->typeKind(), TypeKind::VARCHAR);
    BOLT_CHECK_EQ(args[1]->typeKind(), TypeKind::VARCHAR);
    // Handle the common case of a constant pattern.
    if (args[1]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[1]->isNullAt(0) == false) {
      auto pattern = (args[1])->as<ConstantVector<StringView>>()->valueAt(0);
      ICURegexpMatchConstantPatternFunction<Fn, FastPathFn>(
          pattern, matchDanglingRightBrackets_)
          .apply(rows, args, outputType, context, result);
      return;
    }
    context.ensureWritable(rows, BOOLEAN(), result);
    auto resultFlat = result->asFlatVector<bool>();
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    exec::LocalDecodedVector patterns(context, *args[1], rows);

    UErrorCode status = U_ZERO_ERROR;
    uint32_t flags = 0;

    rows.applyToSelected([&](vector_size_t row) -> void {
      status = U_ZERO_ERROR;
      StringView pattern = patterns->valueAt<StringView>(row);
      auto newPattern = std::string(pattern);
      if (stringCore::checkRegexPattern(
              newPattern, matchDanglingRightBrackets_)) {
        auto regexPattern =
            getRegexPattern(pattern, matchDanglingRightBrackets_);
        resultFlat->set(
            row, Fn(toSearch->valueAt<StringView>(row), regexPattern));
      } else {
        resultFlat->set(
            row,
            FastPathFn(
                toSearch->valueAt<StringView>(row), StringView(newPattern)));
      }
    });
  }

 private:
  bool matchDanglingRightBrackets_;
};

void checkGroupIndex(
    int32_t groupCount,
    int32_t groupId,
    const StringView& pattern) {
  BOLT_USER_CHECK(
      groupId >= 0 && groupId <= groupCount,
      "No group {} in regex '{}'",
      groupId,
      pattern);
}

// to use memory in input vector, calculate the index of extracted string in
// input string according to extracted UnicodeString, which is a one-pass
// traversal
bool extract(
    FlatVector<StringView>* result,
    int row,
    const exec::LocalDecodedVector& toExtract,
    int32_t groupId,
    const icu::LocalPointer<icu::RegexPattern>& regexPattern,
    const StringView& pattern) {
  const StringView str = toExtract->valueAt<StringView>(row);
  UErrorCode status = U_ZERO_ERROR;
  icu::UnicodeString uStr(str.data(), str.size());
  icu::LocalPointer<icu::RegexMatcher> matcher(
      regexPattern->matcher(uStr, status));

  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to match the str '{}', error code : {}",
        str,
        u_errorName(status));
  }

  if (!matcher->find()) {
    result->setNoCopy(row, StringView(nullptr, 0));
    return true;
  }

  checkGroupIndex(matcher->groupCount(), groupId, pattern);
  int32_t start = matcher->start(groupId, status);
  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to return the index in the input string '{}' of the start of the text matched by group {}, error code : {}",
        str,
        groupId,
        u_errorName(status));
  }
  int32_t end = matcher->end(groupId, status);
  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to return the index in the input string '{}' of the character following the text matched by group {}, error code : {}",
        str,
        groupId,
        u_errorName(status));
  }
  if (start == -1 || end == -1 || start == end) {
    result->setNoCopy(row, StringView(nullptr, 0));
    return true;
  }

  size_t size = str.size();
  int32_t startIndex =
      stringCore::char16IndexToByteIndex(uStr.getBuffer(), size, start);
  int32_t groupSize = stringCore::char16IndexToByteIndex(
      uStr.getBuffer() + start, size, end - start);

  result->setNoCopy(row, StringView(str.data() + startIndex, groupSize));
  return !StringView::isInline(groupSize);
}

template <typename T>
class ICURegexpExtractConstantPatternFunction final
    : public exec::VectorFunction {
 public:
  explicit ICURegexpExtractConstantPatternFunction(
      StringView pattern,
      bool matchDanglingRightBrackets)
      : pattern_(std::move(pattern)),
        regexPattern_(getRegexPattern(pattern_, matchDanglingRightBrackets)) {}
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK(args.size() == 2 || args.size() == 3);
    context.ensureWritable(rows, VARCHAR(), result);
    auto resultFlat = result->asFlatVector<StringView>();
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    bool mustRefSourceStrings = false;
    // Common case: constant group id.
    if (args.size() == 2) {
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        mustRefSourceStrings |=
            extract(resultFlat, row, toSearch, 1, regexPattern_, pattern_);
      });
      if (mustRefSourceStrings) {
        resultFlat->acquireSharedStringBuffers(toSearch->base());
      }
      return;
    }

    if (args[2]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[2]->isNullAt(0) == false) {
      const auto groupId = (args[2])->as<ConstantVector<T>>()->valueAt(0);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        mustRefSourceStrings |= extract(
            resultFlat, row, toSearch, groupId, regexPattern_, pattern_);
      });
      if (mustRefSourceStrings) {
        resultFlat->acquireSharedStringBuffers(toSearch->base());
      }
      return;
    }
    // Less common case: variable group id.
    exec::LocalDecodedVector groupIds(context, *args[2], rows);
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      T groupId = groupIds->valueAt<T>(row);
      mustRefSourceStrings |=
          extract(resultFlat, row, toSearch, groupId, regexPattern_, pattern_);
    });
    if (mustRefSourceStrings) {
      resultFlat->acquireSharedStringBuffers(toSearch->base());
    }
  }

 private:
  StringView pattern_;
  const icu::LocalPointer<icu::RegexPattern> regexPattern_;
};

template <typename T>
class ICURegexpExtractFunction final : public exec::VectorFunction {
 public:
  explicit ICURegexpExtractFunction(bool matchDanglingRightBrackets)
      : matchDanglingRightBrackets_(matchDanglingRightBrackets) {}
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK(args.size() == 2 || args.size() == 3);
    // Handle the common case of a constant pattern.
    if (args[1]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[1]->isNullAt(0) == false) {
      auto pattern = (args[1])->as<ConstantVector<StringView>>()->valueAt(0);
      ICURegexpExtractConstantPatternFunction<T>(
          pattern, matchDanglingRightBrackets_)
          .apply(rows, args, outputType, context, result);
      return;
    }
    context.ensureWritable(rows, VARCHAR(), result);
    auto resultFlat = result->asFlatVector<StringView>();
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    exec::LocalDecodedVector patterns(context, *args[1], rows);
    bool mustRefSourceStrings = false;

    UErrorCode status = U_ZERO_ERROR;
    uint32_t flags = 0;
    // Common case: constant group id.
    if (args.size() == 2) {
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        status = U_ZERO_ERROR;
        const auto pattern = patterns->valueAt<StringView>(row);
        auto regexPattern =
            getRegexPattern(pattern, matchDanglingRightBrackets_);
        mustRefSourceStrings |=
            extract(resultFlat, row, toSearch, 1, regexPattern, pattern);
      });
      if (mustRefSourceStrings) {
        resultFlat->acquireSharedStringBuffers(toSearch->base());
      }
      return;
    }

    if (args[2]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[2]->isNullAt(0) == false) {
      const auto groupId = (args[2])->as<ConstantVector<T>>()->valueAt(0);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        status = U_ZERO_ERROR;
        const auto pattern = patterns->valueAt<StringView>(row);
        auto regexPattern =
            getRegexPattern(pattern, matchDanglingRightBrackets_);
        mustRefSourceStrings |=
            extract(resultFlat, row, toSearch, groupId, regexPattern, pattern);
      });
      if (mustRefSourceStrings) {
        resultFlat->acquireSharedStringBuffers(toSearch->base());
      }
      return;
    }

    // Less common case: variable group id
    exec::LocalDecodedVector groupIds(context, *args[2], rows);
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      status = U_ZERO_ERROR;
      T groupId = groupIds->valueAt<T>(row);
      const auto pattern = patterns->valueAt<StringView>(row);
      auto regexPattern = getRegexPattern(pattern, matchDanglingRightBrackets_);
      mustRefSourceStrings |=
          extract(resultFlat, row, toSearch, groupId, regexPattern, pattern);
    });
    if (mustRefSourceStrings) {
      resultFlat->acquireSharedStringBuffers(toSearch->base());
    }
  }

 private:
  bool matchDanglingRightBrackets_;
};

void extractAll(
    exec::VectorWriter<Array<Varchar>>& resultWriter,
    int row,
    const exec::LocalDecodedVector& toExtract,
    int32_t groupId,
    const icu::LocalPointer<icu::RegexPattern>& regexPattern,
    const StringView& pattern) {
  resultWriter.setOffset(row);
  auto& arrayWriter = resultWriter.current();

  const StringView str = toExtract->valueAt<StringView>(row);

  UErrorCode status = U_ZERO_ERROR;
  icu::UnicodeString uStr(str.data(), str.size());
  icu::LocalPointer<icu::RegexMatcher> matcher(
      regexPattern->matcher(uStr, status));
  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to match the str '{}', error code : {}",
        str,
        u_errorName(status));
  }

  size_t pos = 0;
  size_t size = str.size();

  // to avoid the performance loss of redundant traversal, track the end index
  // in both input StringView and input UnicodeString, which is at most one full
  // traversal overhead
  int32_t pre = 0;
  int32_t preIndex = 0;
  while (matcher->find()) {
    checkGroupIndex(matcher->groupCount(), groupId, pattern);
    int32_t start = matcher->start(groupId, status);
    if (UNLIKELY(U_FAILURE(status))) {
      BOLT_USER_FAIL(
          "failed to return the index in the input string '{}' of the start of the text matched by group {}, error code : {}",
          str,
          groupId,
          u_errorName(status));
    }
    int32_t end = matcher->end(groupId, status);
    if (UNLIKELY(U_FAILURE(status))) {
      BOLT_USER_FAIL(
          "failed to return the index in the input string '{}' of the character following the text matched by group {}, error code : {}",
          str,
          groupId,
          u_errorName(status));
    }
    if (start == -1 || end == -1 || start == end) {
      arrayWriter.add_item().setNoCopy(StringView(nullptr, 0));
      continue;
    }

    int32_t startIndex = stringCore::char16IndexToByteIndex(
                             uStr.getBuffer() + pre, size, start - pre) +
        preIndex;
    int32_t groupSize = stringCore::char16IndexToByteIndex(
        uStr.getBuffer() + start, size, end - start);
    pre = end;
    preIndex = startIndex + groupSize;
    arrayWriter.add_item().setNoCopy(
        StringView(str.data() + startIndex, groupSize));
  }
  resultWriter.commit();
}

template <typename T>
class ICURegexpExtractAllConstantPatternFunction final
    : public exec::VectorFunction {
 public:
  explicit ICURegexpExtractAllConstantPatternFunction(
      StringView pattern,
      bool matchDanglingRightBrackets)
      : pattern_(std::move(pattern)),
        regexPattern_(getRegexPattern(pattern_, matchDanglingRightBrackets)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const final {
    BOLT_CHECK(args.size() == 2 || args.size() == 3);

    BaseVector::ensureWritable(rows, ARRAY(VARCHAR()), context.pool(), result);
    exec::VectorWriter<Array<Varchar>> resultWriter;
    resultWriter.init(*result->as<ArrayVector>());

    exec::LocalDecodedVector inputStrs(context, *args[0], rows);

    if (args.size() == 2) {
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        extractAll(resultWriter, row, inputStrs, 1, regexPattern_, pattern_);
      });
    } else if (
        args[2]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[2]->isNullAt(0) == false) {
      const auto groupId = (args[2])->as<ConstantVector<T>>()->valueAt(0);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        extractAll(
            resultWriter, row, inputStrs, groupId, regexPattern_, pattern_);
      });
    } else {
      // Case 3: Variable groupId
      exec::LocalDecodedVector groupIds(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        const T groupId = groupIds->valueAt<T>(row);
        extractAll(
            resultWriter, row, inputStrs, groupId, regexPattern_, pattern_);
      });
    }

    resultWriter.finish();

    result->as<ArrayVector>()
        ->elements()
        ->asFlatVector<StringView>()
        ->acquireSharedStringBuffers(inputStrs->base());
  }

 private:
  StringView pattern_;
  icu::LocalPointer<icu::RegexPattern> regexPattern_;
};

template <typename T>
class ICURegexpExtractAllFunction final : public exec::VectorFunction {
 public:
  explicit ICURegexpExtractAllFunction(bool matchDanglingRightBrackets)
      : matchDanglingRightBrackets_(matchDanglingRightBrackets) {}
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const final {
    BOLT_CHECK(args.size() == 2 || args.size() == 3);

    if (args[1]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[1]->isNullAt(0) == false) {
      auto pattern = (args[1])->as<ConstantVector<StringView>>()->valueAt(0);
      ICURegexpExtractAllConstantPatternFunction<T>(
          pattern, matchDanglingRightBrackets_)
          .apply(rows, args, outputType, context, result);
      return;
    }

    BaseVector::ensureWritable(rows, ARRAY(VARCHAR()), context.pool(), result);
    exec::VectorWriter<Array<Varchar>> resultWriter;
    resultWriter.init(*result->as<ArrayVector>());

    exec::LocalDecodedVector inputStrs(context, *args[0], rows);
    exec::LocalDecodedVector patterns(context, *args[1], rows);

    UErrorCode status = U_ZERO_ERROR;
    uint32_t flags = 0;
    if (args.size() == 2) {
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        status = U_ZERO_ERROR;
        const auto pattern = patterns->valueAt<StringView>(row);
        auto regexPattern =
            getRegexPattern(pattern, matchDanglingRightBrackets_);
        extractAll(resultWriter, row, inputStrs, 1, regexPattern, pattern);
      });
    } else if (
        args[2]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[2]->isNullAt(0) == false) {
      const auto groupId = (args[2])->as<ConstantVector<T>>()->valueAt(0);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        status = U_ZERO_ERROR;
        const auto pattern = patterns->valueAt<StringView>(row);
        auto regexPattern =
            getRegexPattern(pattern, matchDanglingRightBrackets_);
        extractAll(
            resultWriter, row, inputStrs, groupId, regexPattern, pattern);
      });
    } else {
      exec::LocalDecodedVector groupIds(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        status = U_ZERO_ERROR;
        T groupId = groupIds->valueAt<T>(row);
        const auto pattern = patterns->valueAt<StringView>(row);
        auto regexPattern =
            getRegexPattern(pattern, matchDanglingRightBrackets_);
        extractAll(
            resultWriter, row, inputStrs, groupId, regexPattern, pattern);
      });
    }

    resultWriter.finish();
    result->as<ArrayVector>()
        ->elements()
        ->asFlatVector<StringView>()
        ->acquireSharedStringBuffers(inputStrs->base());
  }

 private:
  bool matchDanglingRightBrackets_;
};

template <typename T>
void replace(
    FlatVector<StringView>* result,
    int row,
    const exec::LocalDecodedVector& toReplace,
    StringView replacement,
    const icu::LocalPointer<icu::RegexPattern>& regexPattern,
    T pos = 1) {
  const StringView str = toReplace->valueAt<StringView>(row);
  icu::UnicodeString uStr(str.data(), str.size());
  if (pos - 1 >= uStr.length()) {
    result->setNoCopy(row, StringView(str.data(), str.size()));
    return;
  }

  UErrorCode status = U_ZERO_ERROR;
  icu::LocalPointer<icu::RegexMatcher> matcher(
      regexPattern->matcher(uStr, status));

  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to match the str '{}', error code : {}",
        str,
        u_errorName(status));
  }

  icu::UnicodeString uReplacement(replacement.data(), replacement.size());
  icu::UnicodeString uReplaced;

  matcher->region(pos - 1, uStr.length(), status);
  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to set the limits of matcher's region for the input string '{}', error code : {}",
        str,
        u_errorName(status));
  }
  while (matcher->find(status) && U_SUCCESS(status)) {
    matcher->appendReplacement(uReplaced, uReplacement, status);
  }
  if (UNLIKELY(U_FAILURE(status))) {
    BOLT_USER_FAIL(
        "failed to find the next pattern match in the input string '{}', error code : {}",
        str,
        u_errorName(status));
  }
  matcher->appendTail(uReplaced);

  std::string replaced;
  uReplaced.toUTF8String(replaced);
  auto size = replaced.size();
  // Allocate a string buffer.
  auto buffer = result->getBufferWithSpace(size);
  // getBufferWithSpace() may return a buffer that already has content, so we
  // only use the space after that.
  auto rawBuffer = buffer->asMutable<char>() + buffer->size();
  buffer->setSize(buffer->size() + size);
  memcpy(rawBuffer, replaced.data(), size);
  result->setNoCopy(row, StringView(rawBuffer, size));
}

template <typename T>
class ICURegexpReplaceConstantPatternFunction : public exec::VectorFunction {
 public:
  explicit ICURegexpReplaceConstantPatternFunction(
      StringView pattern,
      bool matchDanglingRightBrackets,
      T pos = 1)
      : regexPattern_(getRegexPattern(pattern, matchDanglingRightBrackets)),
        pos_(pos) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK(args.size() == 3 || args.size() == 4);
    context.ensureWritable(rows, VARCHAR(), result);
    auto resultFlat = result->asFlatVector<StringView>();
    exec::LocalDecodedVector toReplace(context, *args[0], rows);

    if (args[2]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[2]->isNullAt(0) == false) {
      const auto replacement =
          (args[2])->as<ConstantVector<StringView>>()->valueAt(0);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        replace<T>(
            resultFlat, row, toReplace, replacement, regexPattern_, pos_);
      });
      return;
    }

    exec::LocalDecodedVector replacements(context, *args[2], rows);
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      StringView replacement = replacements->valueAt<StringView>(row);
      replace<T>(resultFlat, row, toReplace, replacement, regexPattern_, pos_);
    });
  }

 private:
  icu::LocalPointer<icu::RegexPattern> regexPattern_;
  T pos_;
};

template <typename T>
class ICURegexpReplaceFunction : public exec::VectorFunction {
 public:
  explicit ICURegexpReplaceFunction(bool matchDanglingRightBrackets, T pos = 1)
      : matchDanglingRightBrackets_(matchDanglingRightBrackets), pos_(pos) {}
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK(args.size() == 3 || args.size() == 4);
    BOLT_CHECK_EQ(args[0]->typeKind(), TypeKind::VARCHAR);
    BOLT_CHECK_EQ(args[1]->typeKind(), TypeKind::VARCHAR);
    BOLT_CHECK_EQ(args[2]->typeKind(), TypeKind::VARCHAR);
    // Handle the common case of a constant pattern.
    if (args[1]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[1]->isNullAt(0) == false) {
      auto pattern = (args[1])->as<ConstantVector<StringView>>()->valueAt(0);
      ICURegexpReplaceConstantPatternFunction<T>(pattern, pos_)
          .apply(rows, args, outputType, context, result);
      return;
    }
    context.ensureWritable(rows, VARCHAR(), result);
    auto resultFlat = result->asFlatVector<StringView>();
    exec::LocalDecodedVector toReplace(context, *args[0], rows);
    exec::LocalDecodedVector patterns(context, *args[1], rows);

    UErrorCode status = U_ZERO_ERROR;
    uint32_t flags = 0;
    if (args[2]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[2]->isNullAt(0) == false) {
      const auto replacement =
          (args[2])->as<ConstantVector<StringView>>()->valueAt(0);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        status = U_ZERO_ERROR;
        const auto pattern = patterns->valueAt<StringView>(row);
        auto regexPattern =
            getRegexPattern(pattern, matchDanglingRightBrackets_);
        replace<T>(resultFlat, row, toReplace, replacement, regexPattern, pos_);
      });

      return;
    }
    // Less common case: variable group id
    exec::LocalDecodedVector replacements(context, *args[2], rows);
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      status = U_ZERO_ERROR;
      StringView replacement = replacements->valueAt<StringView>(row);
      const auto pattern = patterns->valueAt<StringView>(row);
      auto regexPattern = getRegexPattern(pattern, matchDanglingRightBrackets_);
      replace<T>(resultFlat, row, toReplace, replacement, regexPattern, pos_);
    });
  }

 private:
  bool matchDanglingRightBrackets_;
  T pos_;
};

template <
    bool (*Fn)(StringView, const icu::LocalPointer<icu::RegexPattern>&),
    bool (*FastPathFn)(StringView, StringView)>
std::shared_ptr<exec::VectorFunction> makeICURegexpMatchImpl(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    bool matchDanglingRightBrackets) {
  if (inputArgs.size() != 2 || !inputArgs[0].type->isVarchar() ||
      !inputArgs[1].type->isVarchar()) {
    BOLT_UNSUPPORTED("{} expected (VARCHAR, VARCHAR)", name);
  }

  BaseVector* constantPattern = inputArgs[1].constantValue.get();
  if (constantPattern != nullptr && !constantPattern->isNullAt(0)) {
    return std::make_shared<
        ICURegexpMatchConstantPatternFunction<Fn, FastPathFn>>(
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0),
        matchDanglingRightBrackets);
  }
  return std::make_shared<ICURegexpMatchFunction<Fn, FastPathFn>>(
      matchDanglingRightBrackets);
}

} // namespace

std::shared_ptr<exec::VectorFunction> makeICURegexLike(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  bool matchDanglingRightBrackets = config.regexMatchDanglingRightBrackets();
  return makeICURegexpMatchImpl<partialMatch, fastPartialMatch>(
      name, inputArgs, matchDanglingRightBrackets);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> icuRegexLikeSignatures() {
  // varchar, varchar -> boolean
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .argumentType("varchar")
              .argumentType("varchar")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeICURegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  auto numArgs = inputArgs.size();
  BOLT_USER_CHECK(
      numArgs == 2 || numArgs == 3,
      "{} requires 2 or 3 arguments, but got {}",
      name,
      numArgs);

  BOLT_USER_CHECK(
      inputArgs[0].type->isVarchar(),
      "{} requires first argument of type VARCHAR, but got {}",
      name,
      inputArgs[0].type->toString());

  BOLT_USER_CHECK(
      inputArgs[1].type->isVarchar(),
      "{} requires second argument of type VARCHAR, but got {}",
      name,
      inputArgs[1].type->toString());

  TypeKind groupIdTypeKind = TypeKind::INTEGER;
  if (numArgs == 3) {
    groupIdTypeKind = inputArgs[2].type->kind();
    BOLT_USER_CHECK(
        groupIdTypeKind == TypeKind::INTEGER ||
            groupIdTypeKind == TypeKind::BIGINT,
        "{} requires third argument of type INTEGER or BIGINT, but got {}",
        name,
        mapTypeKindToName(groupIdTypeKind));
  }

  bool matchDanglingRightBrackets = config.regexMatchDanglingRightBrackets();
  BaseVector* constantPattern = inputArgs[1].constantValue.get();
  if (constantPattern != nullptr && !constantPattern->isNullAt(0)) {
    auto pattern =
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0);
    switch (groupIdTypeKind) {
      case TypeKind::INTEGER:
        return std::make_shared<
            ICURegexpExtractConstantPatternFunction<int32_t>>(
            pattern, matchDanglingRightBrackets);
      case TypeKind::BIGINT:
        return std::make_shared<
            ICURegexpExtractConstantPatternFunction<int64_t>>(
            pattern, matchDanglingRightBrackets);
      default:
        BOLT_UNREACHABLE();
    }
  }

  switch (groupIdTypeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<ICURegexpExtractFunction<int32_t>>(
          matchDanglingRightBrackets);
    case TypeKind::BIGINT:
      return std::make_shared<ICURegexpExtractFunction<int64_t>>(
          matchDanglingRightBrackets);
    default:
      BOLT_UNREACHABLE();
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
icuRegexExtractSignatures() {
  // varchar, varchar -> varchar
  // varchar, varchar, integer | bigint -> varchar
  return {
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("bigint")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("integer")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeICURegexExtractAll(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  auto numArgs = inputArgs.size();
  BOLT_USER_CHECK(
      numArgs == 3 || numArgs == 2,
      "{} requires 3 arguments, but got {}",
      name,
      numArgs);

  BOLT_USER_CHECK(
      inputArgs[0].type->isVarchar(),
      "{} requires first argument of type VARCHAR, but got {}",
      name,
      inputArgs[0].type->toString());

  BOLT_USER_CHECK(
      inputArgs[1].type->isVarchar(),
      "{} requires second argument of type VARCHAR, but got {}",
      name,
      inputArgs[1].type->toString());

  TypeKind groupIdTypeKind = TypeKind::INTEGER;
  if (numArgs == 3) {
    groupIdTypeKind = inputArgs[2].type->kind();
    BOLT_USER_CHECK(
        groupIdTypeKind == TypeKind::INTEGER ||
            groupIdTypeKind == TypeKind::BIGINT,
        "{} requires third argument of type INTEGER or BIGINT, but got {}",
        name,
        mapTypeKindToName(groupIdTypeKind));
  }

  bool matchDanglingRightBrackets = config.regexMatchDanglingRightBrackets();
  BaseVector* constantPattern = inputArgs[1].constantValue.get();
  if (constantPattern != nullptr && !constantPattern->isNullAt(0)) {
    auto pattern =
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0);
    switch (groupIdTypeKind) {
      case TypeKind::INTEGER:
        return std::make_shared<
            ICURegexpExtractAllConstantPatternFunction<int32_t>>(
            pattern, matchDanglingRightBrackets);
      case TypeKind::BIGINT:
        return std::make_shared<
            ICURegexpExtractAllConstantPatternFunction<int64_t>>(
            pattern, matchDanglingRightBrackets);
      default:
        BOLT_UNREACHABLE();
    }
  }

  switch (groupIdTypeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<ICURegexpExtractAllFunction<int32_t>>(
          matchDanglingRightBrackets);
    case TypeKind::BIGINT:
      return std::make_shared<ICURegexpExtractAllFunction<int64_t>>(
          matchDanglingRightBrackets);
    default:
      BOLT_UNREACHABLE();
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
icuRegexExtractAllSignatures() {
  // varchar, varchar -> array<varchar>
  // varchar, varchar, integer|bigint -> array<varchar>
  return {
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("bigint")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("integer")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeICURegexReplace(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  auto numArgs = inputArgs.size();
  BOLT_USER_CHECK(
      numArgs == 3 || numArgs == 4,
      "{} requires 3 or 4 arguments, but got {}",
      name,
      numArgs);

  BOLT_USER_CHECK(
      inputArgs[0].type->isVarchar(),
      "{} requires first argument of type VARCHAR, but got {}",
      name,
      inputArgs[0].type->toString());

  BOLT_USER_CHECK(
      inputArgs[1].type->isVarchar(),
      "{} requires second argument of type VARCHAR, but got {}",
      name,
      inputArgs[1].type->toString());

  BOLT_USER_CHECK(
      inputArgs[2].type->isVarchar(),
      "{} requires third argument of type VARCHAR, but got {}",
      name,
      inputArgs[2].type->toString());

  TypeKind posTypeKind = TypeKind::INTEGER;
  int32_t pos_32 = 1;
  int64_t pos_64 = 1;
  if (numArgs == 4) {
    posTypeKind = inputArgs[3].type->kind();
    BOLT_USER_CHECK(
        posTypeKind == TypeKind::INTEGER || posTypeKind == TypeKind::BIGINT,
        "{} requires third argument of type INTEGER or BIGINT, but got {}",
        name,
        mapTypeKindToName(posTypeKind));
    BaseVector* constantPos = inputArgs[3].constantValue.get();
    if (constantPos != nullptr && !constantPos->isNullAt(0)) {
      switch (posTypeKind) {
        case TypeKind::INTEGER:
          pos_32 = constantPos->as<ConstantVector<int32_t>>()->valueAt(0);
          break;
        case TypeKind::BIGINT:
          pos_64 = constantPos->as<ConstantVector<int64_t>>()->valueAt(0);
          break;
        default:
          BOLT_UNREACHABLE();
      }
    } else {
      BOLT_USER_FAIL("{} requires 'pos' argument foldable", name);
    }
  }

  bool matchDanglingRightBrackets = config.regexMatchDanglingRightBrackets();
  BaseVector* constantPattern = inputArgs[1].constantValue.get();
  if (constantPattern != nullptr && !constantPattern->isNullAt(0)) {
    auto pattern =
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0);
    switch (posTypeKind) {
      case TypeKind::INTEGER:
        return std::make_shared<
            ICURegexpReplaceConstantPatternFunction<int32_t>>(
            pattern, matchDanglingRightBrackets, pos_32);
      case TypeKind::BIGINT:
        return std::make_shared<
            ICURegexpReplaceConstantPatternFunction<int64_t>>(
            pattern, matchDanglingRightBrackets, pos_64);
      default:
        BOLT_UNREACHABLE();
    }
  }

  switch (posTypeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<ICURegexpReplaceFunction<int32_t>>(
          matchDanglingRightBrackets, pos_32);
    case TypeKind::BIGINT:
      return std::make_shared<ICURegexpReplaceFunction<int64_t>>(
          matchDanglingRightBrackets, pos_64);
    default:
      BOLT_UNREACHABLE();
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
icuRegexReplaceSignatures() {
  // varchar, varchar, varchar -> varchar
  // varchar, varchar, varchar, integer|bigint -> varchar
  return {
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("integer")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("bigint")
          .build(),
  };
}

} // namespace bytedance::bolt::functions::sparksql
