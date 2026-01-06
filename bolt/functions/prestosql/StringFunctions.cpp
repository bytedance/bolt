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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include "bolt/expression/EvalCtx.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/StringWriter.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/lib/StringEncodingUtils.h"
#include "bolt/functions/lib/StringUtil.h"
#include "bolt/functions/lib/TimeUtils.h"
#include "bolt/functions/lib/string/StringImpl.h"
#include "bolt/vector/FlatVector.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/printf.h>
#include <type_traits>
namespace bytedance::bolt::functions {

using namespace stringCore;

namespace {
/**
 * Upper and Lower functions have a fast path for ascii where the functions
 * can be applied in place.
 * */
template <bool isLower /*instantiate for upper or lower*/>
class UpperLowerTemplateFunction : public exec::VectorFunction {
 private:
  /// String encoding wrappable function
  template <bool isAscii>
  struct ApplyInternal {
    static void apply(
        const SelectivityVector& rows,
        const DecodedVector* decodedInput,
        FlatVector<StringView>* results) {
      rows.applyToSelected([&](int row) {
        auto proxy = exec::StringWriter<>(results, row);
        if constexpr (isLower) {
          stringImpl::lower<isAscii>(
              proxy, decodedInput->valueAt<StringView>(row));
        } else {
          stringImpl::upper<isAscii>(
              proxy, decodedInput->valueAt<StringView>(row));
        }
        proxy.finalize();
      });
    }
  };

  void applyInternalInPlace(
      const SelectivityVector& rows,
      DecodedVector* decodedInput,
      FlatVector<StringView>* results) const {
    rows.applyToSelected([&](int row) {
      auto proxy = exec::StringWriter<true /*reuseInput*/>(
          results,
          row,
          decodedInput->valueAt<StringView>(row) /*reusedInput*/,
          true /*inPlace*/);
      if constexpr (isLower) {
        stringImpl::lowerAsciiInPlace(proxy);
      } else {
        stringImpl::upperAsciiInPlace(proxy);
      }
      proxy.finalize();
    });
  }

 public:
  bool isDefaultNullBehavior() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK(args.size() == 1);
    BOLT_CHECK(args[0]->typeKind() == TypeKind::VARCHAR);

    // Read content before calling prepare results
    BaseVector* inputStringsVector = args[0].get();
    exec::LocalDecodedVector inputHolder(context, *inputStringsVector, rows);
    auto decodedInput = inputHolder.get();

    auto ascii = isAscii(inputStringsVector, rows);

    bool tryInplace = ascii &&
        (inputStringsVector->encoding() == VectorEncoding::Simple::FLAT);

    // If tryInplace, then call prepareFlatResultsVector(). If the latter
    // returns true, note that the input arg was moved to result, so that the
    // buffer can be reused as output.
    if (tryInplace &&
        prepareFlatResultsVector(result, rows, context, args.at(0))) {
      auto* resultFlatVector = result->as<FlatVector<StringView>>();
      applyInternalInPlace(rows, decodedInput, resultFlatVector);
      return;
    }

    // Not in place path.
    VectorPtr emptyVectorPtr;
    prepareFlatResultsVector(result, rows, context, emptyVectorPtr);
    auto* resultFlatVector = result->as<FlatVector<StringView>>();

    StringEncodingTemplateWrapper<ApplyInternal>::apply(
        ascii, rows, decodedInput, resultFlatVector);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("varchar")
                .build()};
  }

  bool ensureStringEncodingSetAtAllInputs() const override {
    return true;
  }

  bool propagateStringEncodingFromAllInputs() const override {
    return true;
  }
};

/**
 * concat(string1, ..., stringN) → varchar
 * Returns the concatenation of string1, string2, ..., stringN. This function
 * provides the same functionality as the SQL-standard concatenation operator
 * (||).
 * */
class ConcatFunction : public exec::VectorFunction {
 public:
  ConcatFunction(
      const std::string& /* name */,
      const std::vector<exec::VectorFunctionArg>& inputArgs) {
    auto numArgs = inputArgs.size();

    // Save constant values to constantStrings_.
    // Identify and combine consecutive constant inputs.
    argMapping_.reserve(numArgs);
    constantStrings_.reserve(numArgs);

    for (auto i = 0; i < numArgs; ++i) {
      argMapping_.push_back(i);

      const auto& arg = inputArgs[i];
      if (arg.constantValue) {
        std::string value = arg.constantValue->as<ConstantVector<StringView>>()
                                ->valueAt(0)
                                .str();

        column_index_t j = i + 1;
        for (; j < inputArgs.size(); ++j) {
          if (!inputArgs[j].constantValue) {
            break;
          }

          value += inputArgs[j]
                       .constantValue->as<ConstantVector<StringView>>()
                       ->valueAt(0)
                       .str();
        }

        constantStrings_.push_back(std::string(value.data(), value.size()));

        i = j - 1;
      } else {
        constantStrings_.push_back(std::string());
      }
    }

    // Create StringViews for constant strings.
    constantStringViews_.reserve(numArgs);
    for (const auto& constantString : constantStrings_) {
      constantStringViews_.push_back(
          StringView(constantString.data(), constantString.size()));
    }
  }

  bool propagateStringEncodingFromAllInputs() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, VARCHAR(), result);
    auto flatResult = result->asFlatVector<StringView>();

    auto numArgs = argMapping_.size();

    std::vector<exec::LocalDecodedVector> decodedArgs;
    decodedArgs.reserve(numArgs);

    for (auto i = 0; i < numArgs; ++i) {
      auto index = argMapping_[i];
      if (constantStringViews_[i].empty()) {
        decodedArgs.emplace_back(context, *args[index], rows);
      } else {
        // Do not decode constant inputs.
        decodedArgs.emplace_back(context);
      }
    }

    rows.applyToSelected([&](int row) {
      auto result = InPlaceString(flatResult);
      for (int i = 0; i < numArgs; i++) {
        StringView value;
        if (constantStringViews_[i].empty()) {
          value = decodedArgs[i]->valueAt<StringView>(row);
        } else {
          value = constantStringViews_[i];
        }
        result.append(value, flatResult);
      }
      result.set(row, flatResult);
    });
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar, varchar,.. -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("varchar")
                .argumentType("varchar")
                .variableArity()
                .build()};
  }

  static exec::VectorFunctionMetadata metadata() {
    return {true /* supportsFlattening */};
  }

 private:
  std::vector<column_index_t> argMapping_;
  std::vector<std::string> constantStrings_;
  std::vector<StringView> constantStringViews_;
};

/**
 * replace(string, search) → varchar
 * Removes all instances of search from string.
 *
 * replace(string, search, replace) → varchar
 * Replaces all instances of search with replace in string.
 * If search is an empty string, inserts replace in front of every character
 *and at the end of the string.
 **/
template <bool ignoreEmptyReplaced>
class ReplaceBase : public exec::VectorFunction {
 private:
  template <
      typename StringReader,
      typename SearchReader,
      typename ReplaceReader>
  void applyInternal(
      StringReader stringReader,
      SearchReader searchReader,
      ReplaceReader replaceReader,
      const SelectivityVector& rows,
      FlatVector<StringView>* results) const {
    rows.applyToSelected([&](int row) {
      auto proxy = exec::StringWriter<>(results, row);
      stringImpl::replace<
          decltype(proxy),
          decltype(searchReader(row)),
          ignoreEmptyReplaced>(
          proxy, stringReader(row), searchReader(row), replaceReader(row));
      proxy.finalize();
    });
  }

  template <
      typename StringReader,
      typename SearchReader,
      typename ReplaceReader>
  void applyInPlace(
      StringReader stringReader,
      SearchReader searchReader,
      ReplaceReader replaceReader,
      const SelectivityVector& rows,
      FlatVector<StringView>* results) const {
    rows.applyToSelected([&](int row) {
      auto proxy = exec::StringWriter<true /*reuseInput*/>(
          results, row, stringReader(row) /*reusedInput*/, true /*inPlace*/);
      stringImpl::replaceInPlace<
          decltype(proxy),
          decltype(searchReader(row)),
          ignoreEmptyReplaced>(proxy, searchReader(row), replaceReader(row));
      proxy.finalize();
    });
  }

 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Read string input
    exec::LocalDecodedVector decodedStringHolder(context, *args[0], rows);
    auto decodedStringInput = decodedStringHolder.get();

    // Read search argument
    exec::LocalDecodedVector decodedSearchHolder(context, *args[1], rows);
    auto decodedSearchInput = decodedSearchHolder.get();

    std::optional<StringView> searchArgValue;
    if (decodedSearchInput->isConstantMapping()) {
      searchArgValue = decodedSearchInput->valueAt<StringView>(0);
    }

    // Read replace argument
    exec::LocalDecodedVector decodedReplaceHolder(context);
    auto decodedReplaceInput = decodedReplaceHolder.get();
    std::optional<StringView> replaceArgValue;

    if (args.size() <= 2) {
      replaceArgValue = StringView("");
    } else {
      decodedReplaceInput->decode(*args.at(2), rows);
      if (decodedReplaceInput->isConstantMapping()) {
        replaceArgValue = decodedReplaceInput->valueAt<StringView>(0);
      }
    }

    auto stringReader = [&](const vector_size_t row) {
      return decodedStringInput->valueAt<StringView>(row);
    };

    auto searchReader = [&](const vector_size_t row) {
      return decodedSearchInput->valueAt<StringView>(row);
    };

    auto replaceReader = [&](const vector_size_t row) {
      if (replaceArgValue.has_value()) {
        return replaceArgValue.value();
      } else {
        return decodedReplaceInput->valueAt<StringView>(row);
      }
    };

    // Right now we enable the inplace if 'search' and 'replace' are constants
    // and 'search' size is larger than or equal to 'replace' and if the input
    // vector is reused.

    // TODO: analyze other options for enabling inplace i.e.:
    // 1. Decide per row.
    // 2. Scan inputs for max lengths and decide based on that. ..etc
    bool tryInplace = replaceArgValue.has_value() &&
        searchArgValue.has_value() &&
        (searchArgValue.value().size() >= replaceArgValue.value().size()) &&
        (args.at(0)->encoding() == VectorEncoding::Simple::FLAT);

    if (tryInplace) {
      if (prepareFlatResultsVector(result, rows, context, args.at(0))) {
        auto* resultFlatVector = result->as<FlatVector<StringView>>();
        applyInPlace(
            stringReader, searchReader, replaceReader, rows, resultFlatVector);
        return;
      }
    }

    // Not in place path
    VectorPtr emptyVectorPtr;
    prepareFlatResultsVector(result, rows, context, emptyVectorPtr);
    auto* resultFlatVector = result->as<FlatVector<StringView>>();

    applyInternal(
        stringReader, searchReader, replaceReader, rows, resultFlatVector);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        // varchar, varchar -> varchar
        exec::FunctionSignatureBuilder()
            .returnType("varchar")
            .argumentType("varchar")
            .argumentType("varchar")
            .build(),
        // varchar, varchar, varchar -> varchar
        exec::FunctionSignatureBuilder()
            .returnType("varchar")
            .argumentType("varchar")
            .argumentType("varchar")
            .argumentType("varchar")
            .build(),
    };
  }

  // Only the original string and the replacement are relevant to the result
  // encoding.
  // TODO: The propagation is a safe approximation here, it might be better
  // for some cases to keep it unset and then rescan.
  std::optional<std::vector<size_t>> propagateStringEncodingFrom()
      const override {
    return {{0, 2}};
  }
};

class Replace : public ReplaceBase<false /*ignoreEmptyReplaced*/> {};

class ReplaceIgnoreEmptyReplaced
    : public ReplaceBase<true /*ignoreEmptyReplaced*/> {};
} // namespace

} // namespace bytedance::bolt::functions
namespace bytedance::bolt::functions {
class PrintfFunction : public exec::VectorFunction {
 public:
  PrintfFunction(const core::QueryConfig& config) {
    setTimezone(config);
  }

 private:
  void setTimezone(const core::QueryConfig& config) {
    const tz::TimeZone* sessionTimeZone = getTimeZoneFromConfig(config);
    if (sessionTimeZone) {
      time_zone_ = sessionTimeZone;
    }
  }

  bool isDefaultNullBehavior() const override {
    // If the first argument is null, return null. (It's okay for other
    // arguments to be null, in which case, "null" will be printed.)
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto typeKinds = std::vector<TypeKind>(args.size() - 1);
    for (auto i = 1; i < args.size(); ++i) {
      auto typeKind = args[i]->type()->kind();
      checkScalarType(typeKind);
      typeKinds[i - 1] = typeKind;
    }

    auto decodedArgs = exec::DecodedArgs(rows, args, context);

    if (args.size() == 1) {
      // reuse the input as the output
      result = args[0];
      return;
    }

    BaseVector::ensureWritable(rows, VARCHAR(), context.pool(), result);
    auto flatResult = result->asFlatVector<StringView>();
    if (args[0]->isConstantEncoding()) {
      auto format = args[0]->as<ConstantVector<StringView>>();
      if (format->isNullAt(0)) {
        result = BaseVector::createNullConstant(
            VARCHAR(), rows.size(), context.pool());
        return;
      }

      auto formatIndicles = parse(format->valueAt(0));
      BOLT_CHECK(formatIndicles.size() == args.size() - 1, "invalid format!");

      rows.applyToSelected([&](vector_size_t i) {
        try {
          formatRow(
              flatResult,
              format->valueAt(i),
              formatIndicles,
              typeKinds,
              decodedArgs,
              i);
        } catch (std::exception&) {
          context.setError(i, std::current_exception());
          flatResult->setNull(i, false);
          return;
        }
      });
      return;
    }

    auto formats = decodedArgs.at(0)->base()->as<FlatVector<StringView>>();
    if (formats->mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        try {
          if (formats->isNullAt(i)) {
            flatResult->setNull(i, false);
            return;
          }

          auto formatIndicles = parse(formats->valueAt(0));
          BOLT_CHECK(
              formatIndicles.size() == args.size() - 1, "invalid format!");

          formatRow(
              flatResult,
              formats->valueAt(i),
              formatIndicles,
              typeKinds,
              decodedArgs,
              i);

        } catch (std::exception& e) {
          context.setError(i, std::current_exception());
          flatResult->setNull(i, false);
        }
      });
      return;
    }
    rows.applyToSelected([&](vector_size_t i) {
      try {
        auto formatIndicles = parse(formats->valueAt(0));
        BOLT_CHECK(formatIndicles.size() == args.size() - 1, "invalid format!");
        formatRow(
            flatResult,
            formats->valueAt(i),
            formatIndicles,
            typeKinds,
            decodedArgs,
            i);
      } catch (std::exception&) {
        context.setError(i, std::current_exception());
        flatResult->setNull(i, false);
      }
    });
  }

 protected:
  void checkScalarType(TypeKind kind) const {
    switch (kind) {
      case TypeKind::ARRAY:
      case TypeKind::MAP:
      case TypeKind::ROW:
      case TypeKind::UNKNOWN:
      case TypeKind::FUNCTION:
      case TypeKind::OPAQUE:
      case TypeKind::INVALID:
        BOLT_USER_FAIL("arguments of printf must be scalar type!");
        break;
      default:;
    }
  }

  void formatRow(
      FlatVector<StringView>* flatResult,
      StringView format,
      const std::vector<std::pair<int32_t, int32_t>>& formatIndicles,
      const std::vector<TypeKind>& typeKinds,
      const exec::DecodedArgs& args,
      vector_size_t row) const {
    auto result = InPlaceString(flatResult);
    auto offset = 0;
    for (auto i = 1; i < args.size(); ++i) {
      const auto [begin, end] = formatIndicles[i - 1];
      result.append(format.data() + offset, begin - offset, flatResult);
      auto vec = args.at(i);
      result.append(
          formatOne(
              std::string_view(format.data() + begin, end - begin),
              typeKinds[i - 1],
              row,
              vec,
              vec->isNullAt(row)),
          flatResult);
      offset = end;
    }
    result.append(format.data() + offset, format.size() - offset, flatResult);
    result.set(row, flatResult);
  }

#define BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_WITHOUT_TIMESTAMP(                   \
    TEMPLATE_FUNC, typeKind, ...)                                              \
  [&]() {                                                                      \
    switch (typeKind) {                                                        \
      case ::bytedance::bolt::TypeKind::BOOLEAN: {                             \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::BOOLEAN>(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::INTEGER: {                             \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::INTEGER>(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::TINYINT: {                             \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::TINYINT>(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::SMALLINT: {                            \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::SMALLINT>(           \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::BIGINT: {                              \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::BIGINT>(             \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::HUGEINT: {                             \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::HUGEINT>(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::REAL: {                                \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::REAL>(__VA_ARGS__);  \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::DOUBLE: {                              \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::DOUBLE>(             \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::VARCHAR: {                             \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::VARCHAR>(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::VARBINARY: {                           \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::VARBINARY>(          \
            __VA_ARGS__);                                                      \
      }                                                                        \
      default:                                                                 \
        BOLT_FAIL("not a scalar type! kind: {}", mapTypeKindToName(typeKind)); \
    }                                                                          \
  }()

  std::string formatOne(
      std::string_view format,
      TypeKind kind,
      vector_size_t row,
      DecodedVector* vec,
      bool isNull) const {
    if (kind == TypeKind::BOOLEAN) {
      return vec->valueAt<bool>(row) ? "true" : "false";
    } else if (kind == TypeKind::TIMESTAMP) {
      auto timestamp = vec->valueAt<Timestamp>(row);
      if (time_zone_) {
        timestamp.toTimezone(*time_zone_);
      }
      return timestamp.toString(timestampToStringOptions_);
    } else if (kind == TypeKind::VARBINARY) {
      auto stringView = vec->valueAt<StringView>(row);
      return fmt::format("{:02x}", fmt::join(stringView, " "));
    }
    return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_WITHOUT_TIMESTAMP(
        formatOneImpl, kind, format, row, vec, isNull);
  }

#undef BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_WITHOUT_TIMESTAMP

  template <TypeKind TKind>
  std::string formatOneImpl(
      std::string_view format,
      vector_size_t row,
      DecodedVector* vec,
      bool isNull) const {
    if (isNull) {
      auto xFormat = std::string(format);
      xFormat[format.size() - 1] = 's';
      return fmt::sprintf(xFormat, "null");
    }

    std::string result;
    auto&& ch = format[format.size() - 1];
    if (UNLIKELY(ch == 'h' || ch == 'H')) {
      auto xFormat = std::string(format);
      xFormat[format.size() - 1] = (ch + 'x' - 'h');
      if constexpr (std::is_same_v<
                        StringView,
                        typename TypeTraits<TKind>::NativeType>) {
        auto&& sv = vec->valueAt<typename TypeTraits<TKind>::NativeType>(row);
        return fmt::sprintf(format, fmt::string_view(sv.data(), sv.size()));
      } else {
        return fmt::sprintf(
            xFormat, vec->valueAt<typename TypeTraits<TKind>::NativeType>(row));
      }
    }
    if constexpr (std::is_same_v<
                      StringView,
                      typename TypeTraits<TKind>::NativeType>) {
      auto&& sv = vec->valueAt<typename TypeTraits<TKind>::NativeType>(row);

      result = fmt::sprintf(format, fmt::string_view(sv.data(), sv.size()));
    } else {
      result = fmt::sprintf(
          format, vec->valueAt<typename TypeTraits<TKind>::NativeType>(row));
    }

    if (UNLIKELY(
            format[format.size() - 1] == 'a' ||
            format[format.size() - 1] == 'A')) {
      auto pos = result.find('+');
      if (pos != std::string::npos) {
        result.erase(pos, 1);
      }
    }
    return result;
  }

  // return {beginIndex, endIndex}... of formats
  std::vector<std::pair<int32_t, int32_t>> parse(StringView format) const {
    auto ret = std::vector<std::pair<int32_t, int32_t>>{};
    for (auto i = 0; i < format.size(); ++i) {
      if (format.data()[i] != '%') {
        continue;
      }

      auto beginIndex = i;
      ++i;

      BOLT_CHECK(i < format.size(), "invalid format: %");
      if (format.data()[i] == '%') {
        // %% is escape code of %
        continue;
      }

#define PRINTF_PARSE_USER_CHECK(expr) \
  BOLT_CHECK(                         \
      (expr),                         \
      fmt::format(                    \
          "invalid format: {}",       \
          std::string_view(format.data() + beginIndex, i - beginIndex + 1)));

      while (isFlagChar(format.data()[i])) {
        ++i;
        PRINTF_PARSE_USER_CHECK(i < format.size())
      }

      PRINTF_PARSE_USER_CHECK(isFormatChar(format.data()[i]))
      ret.emplace_back(beginIndex, i + 1);
    }
    return ret;
  }

  bool isFormatChar(char ch) const {
    static const char formatCharSet[] = "diouxXhHfFeEgGaAcspnCSb";
    for (const auto& formatChar : formatCharSet) {
      if (ch == formatChar) {
        return true;
      }
    }
    return false;
  }

  bool isFlagChar(char ch) const {
    static const char flagCharSet[] = "'-+ #ljztL.1234567890";
    for (const auto& flagChar : flagCharSet) {
      if (ch == flagChar) {
        return true;
      }
    }
    return false;
  }

  const tz::TimeZone* time_zone_;
  int64_t sessionTzOffsetInSeconds_{0};
  constexpr static TimestampToStringOptions timestampToStringOptions_ =
      TimestampToStringOptions{
          .precision = TimestampToStringOptions::Precision::kMilliseconds};

 public:
  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar, ... -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("varchar")
                .argumentType("any")
                .variableArity()
                .build()};
  }
};

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_upper,
    UpperLowerTemplateFunction<false /*isLower*/>::signatures(),
    std::make_unique<UpperLowerTemplateFunction<false /*isLower*/>>());

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_lower,
    UpperLowerTemplateFunction<true /*isLower*/>::signatures(),
    std::make_unique<UpperLowerTemplateFunction<true /*isLower*/>>());

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION_WITH_METADATA(
    udf_concat,
    ConcatFunction::signatures(),
    ConcatFunction::metadata(),
    [](const auto& name,
       const auto& inputs,
       const core::QueryConfig& /*config*/) {
      return std::make_unique<ConcatFunction>(name, inputs);
    });

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_replace,
    Replace::signatures(),
    std::make_unique<Replace>());

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_replace_ignore_empty_replaced,
    ReplaceIgnoreEmptyReplaced::signatures(),
    std::make_unique<ReplaceIgnoreEmptyReplaced>());

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_printf,
    PrintfFunction::signatures(),
    [](const std::string& name,
       const std::vector<exec::VectorFunctionArg>& inputArgs,
       const core::QueryConfig& config) {
      return std::make_unique<PrintfFunction>(config);
    });

} // namespace bytedance::bolt::functions
