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

#include <cstring>
#include <type_traits>

#include <unicode/localpointer.h>
#include <unicode/regex.h>
#include <unicode/stringpiece.h>
#include <unicode/unistr.h>

#include "bolt/functions/lib/string/RegexUtils.h"
#include "bolt/functions/lib/string/StringCore.h"
#include "bolt/vector/ConstantVector.h"

namespace bytedance::bolt::functions::flinksql {
namespace {

icu::LocalPointer<icu::RegexPattern> compileJavaRegex(StringView pattern) {
  try {
    return regex::compileRegexPattern(regex::transRegexPattern(pattern));
  } catch (const BoltUserError&) {
    // Flink's regexpExtract catches PatternSyntaxException and returns null.
    return icu::LocalPointer<icu::RegexPattern>(nullptr);
  }
}

template <typename T>
int32_t narrowGroupIdLikeJava(T groupId) {
  if constexpr (std::is_same_v<T, int32_t>) {
    return groupId;
  }

  // Java narrowing from long to int keeps the low 32 bits. Use memcpy to
  // preserve the bit pattern without relying on implementation-defined signed
  // integer conversion.
  const uint32_t lowBits = static_cast<uint32_t>(groupId);
  int32_t narrowed;
  static_assert(sizeof(narrowed) == sizeof(lowBits));
  std::memcpy(&narrowed, &lowBits, sizeof(narrowed));
  return narrowed;
}

bool extract(
    FlatVector<StringView>& result,
    vector_size_t row,
    const exec::LocalDecodedVector& inputs,
    int32_t groupId,
    const icu::LocalPointer<icu::RegexPattern>& regexPattern) {
  const auto input = inputs->valueAt<StringView>(row);
  const auto unicodeInput = icu::UnicodeString::fromUTF8(
      icu::StringPiece(input.data(), input.size()));

  UErrorCode status = U_ZERO_ERROR;
  icu::LocalPointer<icu::RegexMatcher> matcher(
      regexPattern->matcher(unicodeInput, status));
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

template <typename T>
class ICURegexpExtractConstantPatternFunction final
    : public exec::VectorFunction {
 public:
  ICURegexpExtractConstantPatternFunction() = default;

  explicit ICURegexpExtractConstantPatternFunction(StringView pattern)
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
    const auto extractGroup = [&](vector_size_t row, T groupId) {
      mustRefInputStrings |= extract(
          result, row, inputs, narrowGroupIdLikeJava(groupId), regexPattern_);
    };

    if (args.size() == 2) {
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t row) { extractGroup(row, T{0}); });
    } else if (args[2]->isConstantEncoding() && !args[2]->isNullAt(0)) {
      const auto groupId = args[2]->as<ConstantVector<T>>()->valueAt(0);
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t row) { extractGroup(row, groupId); });
    } else {
      exec::LocalDecodedVector groupIds(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        extractGroup(row, groupIds->valueAt<T>(row));
      });
    }

    if (mustRefInputStrings) {
      result.acquireSharedStringBuffers(inputs->base());
    }
  }

 private:
  const icu::LocalPointer<icu::RegexPattern> regexPattern_;
};

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
  BOLT_USER_CHECK(
      inputArgs[1].constantValue != nullptr &&
          inputArgs[1].constantValue->isConstantEncoding(),
      "{} requires a constant pattern.",
      name);

  auto groupIdType = TypeKind::INTEGER;
  if (numArgs == 3) {
    groupIdType = inputArgs[2].type->kind();
    BOLT_USER_CHECK(
        groupIdType == TypeKind::INTEGER || groupIdType == TypeKind::BIGINT,
        "{} requires third argument of type INTEGER or BIGINT, but got {}",
        name,
        mapTypeKindToName(groupIdType));
  }

  const auto* patternVector = inputArgs[1].constantValue.get();
  if (patternVector->isNullAt(0)) {
    if (groupIdType == TypeKind::INTEGER) {
      return std::make_shared<
          ICURegexpExtractConstantPatternFunction<int32_t>>();
    }
    return std::make_shared<ICURegexpExtractConstantPatternFunction<int64_t>>();
  }

  const auto pattern =
      patternVector->as<ConstantVector<StringView>>()->valueAt(0);
  if (groupIdType == TypeKind::INTEGER) {
    return std::make_shared<ICURegexpExtractConstantPatternFunction<int32_t>>(
        pattern);
  }
  return std::make_shared<ICURegexpExtractConstantPatternFunction<int64_t>>(
      pattern);
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
