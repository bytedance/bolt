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

#include "bolt/functions/flinksql/ICURegexFunctions.h"

#include <unicode/ustring.h>
#include <bit>
#include <optional>

#include <unicode/localpointer.h>
#include <unicode/regex.h>
#include <unicode/stringpiece.h>
#include <unicode/unistr.h>

#include "bolt/expression/VectorWriters.h"
#include "bolt/functions/lib/RegexCache.h"
#include "bolt/functions/lib/string/RegexUtils.h"
#include "bolt/functions/lib/string/StringCore.h"
#include "bolt/vector/ConstantVector.h"

namespace bytedance::bolt::functions::flinksql {
namespace {

using RegexPatternPtr = std::shared_ptr<const icu::RegexPattern>;

RegexPatternPtr compileJavaRegex(StringView pattern) {
  try {
    auto compiled =
        regex::compileRegexPattern(regex::transRegexPattern(pattern));
    return RegexPatternPtr(compiled.orphan());
  } catch (const BoltUserError&) {
    // Flink returns NULL for invalid patterns.
    return nullptr;
  }
}

struct RegexInput {
  UErrorCode reset(RegexPatternPtr pattern, StringView input) {
    UErrorCode status = U_ZERO_ERROR;
    if (pattern != pattern_ || matcher_.isNull()) {
      matcher_.adoptInstead(nullptr);
      pattern_ = std::move(pattern);
      matcher_.adoptInstead(pattern_->matcher(status));
      if (U_FAILURE(status)) {
        return status;
      }
    }
    auto* buffer = text_.getBuffer(input.size());
    if (!buffer) {
      return U_MEMORY_ALLOCATION_ERROR;
    }
    int32_t length = 0;
    int32_t substitutions = 0;
    u_strFromUTF8WithSub(
        buffer,
        input.size(),
        &length,
        input.empty() ? "" : input.data(),
        input.size(),
        0xfffd,
        &substitutions,
        &status);
    text_.releaseBuffer(length);
    substituted_ = substitutions != 0;
    if (U_SUCCESS(status)) {
      matcher_->reset(text_);
    }
    return status;
  }

  RegexPatternPtr pattern_;
  icu::UnicodeString text_;
  icu::UnicodeString replaced_;
  icu::LocalPointer<icu::RegexMatcher> matcher_;
  bool substituted_{false};
};

void writeUnicode(
    FlatVector<StringView>& result,
    vector_size_t row,
    const icu::UnicodeString& text) {
  UErrorCode status = U_ZERO_ERROR;
  int32_t length = 0;
  u_strToUTF8WithSub(
      nullptr,
      0,
      &length,
      text.getBuffer(),
      text.length(),
      0xfffd,
      nullptr,
      &status);
  BOLT_USER_CHECK(
      status == U_BUFFER_OVERFLOW_ERROR || U_SUCCESS(status),
      "Regex result conversion failed: {}",
      u_errorName(status));
  exec::StringWriter<> writer(&result, row);
  writer.resize(length);
  status = U_ZERO_ERROR;
  u_strToUTF8WithSub(
      writer.data(),
      length,
      &length,
      text.getBuffer(),
      text.length(),
      0xfffd,
      nullptr,
      &status);
  BOLT_USER_CHECK(
      U_SUCCESS(status),
      "Regex result conversion failed: {}",
      u_errorName(status));
  writer.resize(length);
  writer.finalize();
}

bool applyRegex(
    FlatVector<StringView>& result,
    vector_size_t row,
    StringView input,
    int32_t groupId,
    RegexInput& regex,
    UErrorCode status) {
  auto* matcher = regex.matcher_.getAlias();
  if (U_FAILURE(status) || !matcher->find(status) || U_FAILURE(status)) {
    result.setNull(row, true);
    return false;
  }
  if (groupId < 0 || groupId > matcher->groupCount()) {
    result.setNull(row, true);
    return false;
  }
  const auto start = matcher->start(groupId, status);
  const auto end = matcher->end(groupId, status);
  if (U_FAILURE(status) || start < 0 || end < 0) {
    result.setNull(row, true);
    return false;
  }
  if (start == end) {
    result.setNoCopy(row, StringView(nullptr, 0));
    return false;
  }
  if (regex.substituted_) {
    writeUnicode(result, row, regex.text_.tempSubStringBetween(start, end));
    return false;
  }
  const auto startIndex = stringCore::char16IndexToByteIndex(
      regex.text_.getBuffer(), input.size(), start);
  const auto resultSize = stringCore::char16IndexToByteIndex(
      regex.text_.getBuffer() + start, input.size(), end - start);
  result.setNoCopy(
      row,
      StringView(input.data() + startIndex, static_cast<int32_t>(resultSize)));
  return !StringView::isInline(resultSize);
}

bool applyRegex(
    FlatVector<StringView>& result,
    vector_size_t row,
    StringView input,
    int64_t groupId,
    RegexInput& regex,
    UErrorCode status) {
  return applyRegex(
      result,
      row,
      input,
      std::bit_cast<int32_t>(static_cast<uint32_t>(groupId)),
      regex,
      status);
}

bool applyRegex(
    FlatVector<StringView>& result,
    vector_size_t row,
    StringView input,
    const icu::UnicodeString& replacement,
    RegexInput& regex,
    UErrorCode status) {
  BOLT_USER_CHECK(
      U_SUCCESS(status), "Regex matcher failed: {}", u_errorName(status));
  auto& matcher = *regex.matcher_;
  const bool found = matcher.find(status);
  BOLT_USER_CHECK(
      U_SUCCESS(status), "Regex matching failed: {}", u_errorName(status));
  if (!found) {
    if (!regex.substituted_) {
      result.setNoCopy(row, input);
      return !input.isInline();
    }
    writeUnicode(result, row, regex.text_);
    return false;
  }
  auto& replaced = regex.replaced_;
  replaced.remove();
  int32_t previousEnd = 0;
  do {
    const auto start = matcher.start(status);
    replaced.append(regex.text_, previousEnd, start - previousEnd);
    replaced.append(replacement);
    previousEnd = matcher.end(status);
  } while (U_SUCCESS(status) && matcher.find(status));
  BOLT_USER_CHECK(
      U_SUCCESS(status), "Regex matching failed: {}", u_errorName(status));
  replaced.append(regex.text_, previousEnd, regex.text_.length() - previousEnd);
  BOLT_USER_CHECK(!replaced.isBogus(), "Regex result exceeds string capacity");
  writeUnicode(result, row, replaced);
  return false;
}

template <typename T>
auto regexArgument(T argument) {
  if constexpr (std::is_same_v<T, StringView>) {
    return icu::UnicodeString::fromUTF8(
        icu::StringPiece(argument.data(), argument.size()));
  } else {
    return argument;
  }
}

template <typename T>
class ICURegexFunction final : public exec::VectorFunction {
 public:
  ICURegexFunction() = default;

  explicit ICURegexFunction(RegexPatternPtr pattern)
      : constantPattern_(true), pattern_(std::move(pattern)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr&,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const override {
    BOLT_CHECK(args.size() == 2 || args.size() == 3);
    if (!rows.hasSelections()) {
      return;
    }
    context.ensureWritable(rows, VARCHAR(), resultRef);
    auto& result = *resultRef->asFlatVector<StringView>();
    const bool constantPattern =
        constantPattern_ || args[1]->isConstantEncoding();
    auto pattern = pattern_;
    if (!constantPattern_ && constantPattern && !args[1]->isNullAt(0)) {
      pattern = cache_.get(
          args[1]->as<ConstantVector<StringView>>()->valueAt(0),
          compileJavaRegex);
    }
    if (constantPattern && !pattern) {
      rows.applyToSelected(
          [&](vector_size_t row) { result.setNull(row, true); });
      return;
    }
    exec::LocalDecodedVector inputs(context, *args[0], rows);
    exec::LocalDecodedVector patterns(context);
    if (!constantPattern) {
      patterns.get()->decode(*args[1], rows);
    }
    exec::LocalDecodedVector arguments(context);
    using Argument = decltype(regexArgument(T{}));
    std::optional<Argument> constantArgument;
    if (args.size() == 2) {
      constantArgument = regexArgument(T{});
    } else {
      arguments.get()->decode(*args[2], rows);
      if (arguments->isConstantMapping()) {
        constantArgument = regexArgument(arguments->valueAt<T>(rows.begin()));
      }
    }
    RegexInput regex;
    bool mustRefInputStrings = false;
    const auto applyRow = [&](vector_size_t row, const Argument& argument) {
      auto rowPattern = constantPattern
          ? pattern
          : cache_.get(patterns->valueAt<StringView>(row), compileJavaRegex);
      if (!rowPattern) {
        result.setNull(row, true);
        return;
      }
      const auto input = inputs->valueAt<StringView>(row);
      const auto status = regex.reset(std::move(rowPattern), input);
      mustRefInputStrings |=
          applyRegex(result, row, input, argument, regex, status);
    };
    if (constantArgument) {
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t row) { applyRow(row, *constantArgument); });
    } else {
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        applyRow(row, regexArgument(arguments->valueAt<T>(row)));
      });
    }
    if (mustRefInputStrings) {
      result.acquireSharedStringBuffers(inputs->base());
    }
  }

 private:
  const bool constantPattern_{false};
  const RegexPatternPtr pattern_;
  regex::RegexCache<icu::RegexPattern> cache_;
};

template <typename T>
std::shared_ptr<exec::VectorFunction> makeICURegexFunction(
    const VectorPtr& pattern) {
  if (!pattern) {
    return std::make_shared<ICURegexFunction<T>>();
  }
  return std::make_shared<ICURegexFunction<T>>(
      pattern->isNullAt(0)
          ? nullptr
          : compileJavaRegex(
                pattern->as<ConstantVector<StringView>>()->valueAt(0)));
}

} // namespace

std::shared_ptr<exec::VectorFunction> makeICURegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /* config */) {
  const auto numArgs = inputArgs.size();
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

  auto groupIdType = TypeKind::INTEGER;
  if (numArgs == 3) {
    groupIdType = inputArgs[2].type->kind();
    BOLT_USER_CHECK(
        groupIdType == TypeKind::INTEGER || groupIdType == TypeKind::BIGINT,
        "{} requires third argument of type INTEGER or BIGINT, but got {}",
        name,
        mapTypeKindToName(groupIdType));
  }

  if (groupIdType == TypeKind::INTEGER) {
    return makeICURegexFunction<int32_t>(inputArgs[1].constantValue);
  }
  return makeICURegexFunction<int64_t>(inputArgs[1].constantValue);
}

std::shared_ptr<exec::VectorFunction> makeICURegexReplace(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig&) {
  BOLT_USER_CHECK_EQ(inputArgs.size(), 3, "{} requires 3 arguments", name);
  for (const auto& argument : inputArgs) {
    BOLT_USER_CHECK(
        argument.type->isVarchar(), "{} requires VARCHAR arguments", name);
  }
  return makeICURegexFunction<StringView>(inputArgs[1].constantValue);
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
icuRegexExtractSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
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
          .argumentType("bigint")
          .build(),
  };
}

} // namespace bytedance::bolt::functions::flinksql
