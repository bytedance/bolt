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

#include <bit>

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

icu::LocalPointer<icu::RegexPattern> compileJavaRegex(StringView pattern) {
  try {
    return regex::compileRegexPattern(regex::transRegexPattern(pattern));
  } catch (const BoltUserError&) {
    // Flink returns NULL for invalid patterns.
    return icu::LocalPointer<icu::RegexPattern>(nullptr);
  }
}

bool applyRegex(
    FlatVector<StringView>& result,
    vector_size_t row,
    const exec::LocalDecodedVector& inputs,
    int32_t groupId,
    const icu::RegexPattern& regexPattern) {
  const auto input = inputs->valueAt<StringView>(row);
  const auto unicodeInput = icu::UnicodeString::fromUTF8(
      icu::StringPiece(input.data(), input.size()));

  UErrorCode status = U_ZERO_ERROR;
  icu::LocalPointer<icu::RegexMatcher> matcher(
      regexPattern.matcher(unicodeInput, status));
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

  const auto startIndex = stringCore::char16IndexToByteIndex(
      unicodeInput.getBuffer(), input.size(), start);
  const auto resultSize = stringCore::char16IndexToByteIndex(
      unicodeInput.getBuffer() + start, input.size(), end - start);
  result.setNoCopy(
      row,
      StringView(input.data() + startIndex, static_cast<int32_t>(resultSize)));
  return !StringView::isInline(resultSize);
}

inline bool applyRegex(
    FlatVector<StringView>& result,
    vector_size_t row,
    const exec::LocalDecodedVector& inputs,
    int64_t groupId,
    const icu::RegexPattern& pattern) {
  return applyRegex(
      result,
      row,
      inputs,
      std::bit_cast<int32_t>(static_cast<uint32_t>(groupId)),
      pattern);
}

bool applyRegex(
    FlatVector<StringView>& result,
    vector_size_t row,
    const exec::LocalDecodedVector& inputs,
    StringView argument,
    const icu::RegexPattern& pattern) {
  const auto input = inputs->valueAt<StringView>(row);
  const auto unicodeInput = icu::UnicodeString::fromUTF8(
      icu::StringPiece(input.data(), input.size()));
  UErrorCode status = U_ZERO_ERROR;
  icu::LocalPointer<icu::RegexMatcher> matcher(
      pattern.matcher(unicodeInput, status));
  BOLT_USER_CHECK(
      U_SUCCESS(status), "Regex matcher failed: {}", u_errorName(status));
  const auto replacement = icu::UnicodeString::fromUTF8(
      icu::StringPiece(argument.data(), argument.size()));
  icu::UnicodeString replaced;
  int32_t previousEnd = 0;
  while (matcher->find(status) && U_SUCCESS(status)) {
    const auto start = matcher->start(status);
    replaced.append(unicodeInput, previousEnd, start - previousEnd);
    replaced.append(replacement);
    previousEnd = matcher->end(status);
  }
  BOLT_USER_CHECK(
      U_SUCCESS(status), "Regex matching failed: {}", u_errorName(status));
  replaced.append(
      unicodeInput, previousEnd, unicodeInput.length() - previousEnd);
  std::string text;
  replaced.toUTF8String(text);
  exec::StringWriter<> writer(&result, row);
  writer.copy_from(text);
  writer.finalize();
  return false;
}

template <typename T>
class ICURegexConstantPatternFunction final : public exec::VectorFunction {
 public:
  ICURegexConstantPatternFunction() = default;

  explicit ICURegexConstantPatternFunction(StringView pattern)
      : regexPattern_(compileJavaRegex(pattern)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const override {
    BOLT_CHECK(args.size() == 2 || args.size() == 3);
    context.ensureWritable(rows, VARCHAR(), resultRef);
    auto& result = *resultRef->asFlatVector<StringView>();

    if (regexPattern_.isNull()) {
      rows.applyToSelected(
          [&](vector_size_t row) { result.setNull(row, true); });
      return;
    }

    exec::LocalDecodedVector inputs(context, *args[0], rows);
    bool mustRefInputStrings = false;
    const auto applyRow = [&](vector_size_t row, T argument) {
      mustRefInputStrings |=
          applyRegex(result, row, inputs, argument, *regexPattern_);
    };

    if (args.size() == 2) {
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t row) { applyRow(row, T{}); });
    } else if (args[2]->isConstantEncoding() && !args[2]->isNullAt(0)) {
      const auto argument = args[2]->as<ConstantVector<T>>()->valueAt(0);
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t row) { applyRow(row, argument); });
    } else {
      exec::LocalDecodedVector arguments(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        applyRow(row, arguments->valueAt<T>(row));
      });
    }

    if (mustRefInputStrings) {
      result.acquireSharedStringBuffers(inputs->base());
    }
  }

 private:
  const icu::LocalPointer<icu::RegexPattern> regexPattern_;
};

template <typename T>
class ICURegexFunction final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const override {
    if (args[1]->isConstantEncoding() && !args[1]->isNullAt(0)) {
      ICURegexConstantPatternFunction<T>(
          args[1]->as<ConstantVector<StringView>>()->valueAt(0))
          .apply(rows, args, outputType, context, resultRef);
      return;
    }
    context.ensureWritable(rows, VARCHAR(), resultRef);
    auto& result = *resultRef->asFlatVector<StringView>();
    exec::LocalDecodedVector inputs(context, *args[0], rows);
    exec::LocalDecodedVector patterns(context, *args[1], rows);
    bool mustRefInputStrings = false;
    const auto applyRow = [&](vector_size_t row, T argument) {
      auto pattern =
          cache_.get(patterns->valueAt<StringView>(row), [](StringView value) {
            auto compiled = compileJavaRegex(value);
            return std::shared_ptr<const icu::RegexPattern>(compiled.orphan());
          });
      if (!pattern) {
        result.setNull(row, true);
        return;
      }
      mustRefInputStrings |=
          applyRegex(result, row, inputs, argument, *pattern);
    };
    if (args.size() == 2) {
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t row) { applyRow(row, T{}); });
    } else {
      exec::LocalDecodedVector arguments(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        applyRow(row, arguments->valueAt<T>(row));
      });
    }
    if (mustRefInputStrings) {
      result.acquireSharedStringBuffers(inputs->base());
    }
  }

 private:
  regex::RegexCache<icu::RegexPattern> cache_;
};

template <typename T>
std::shared_ptr<exec::VectorFunction> makeICURegexFunction(
    const VectorPtr& pattern) {
  if (!pattern) {
    return std::make_shared<ICURegexFunction<T>>();
  }
  if (pattern->isNullAt(0)) {
    return std::make_shared<ICURegexConstantPatternFunction<T>>();
  }
  return std::make_shared<ICURegexConstantPatternFunction<T>>(
      pattern->as<ConstantVector<StringView>>()->valueAt(0));
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
