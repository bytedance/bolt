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

#pragma once

#include <unicode/utf8.h>

#include "bolt/functions/Macros.h"
#include "bolt/functions/lib/string/StringCore.h"
#include "bolt/functions/lib/string/StringImpl.h"
#include "bolt/functions/prestosql/StringFunctions.h"
#include "bolt/functions/sparksql/String.h"
namespace bytedance::bolt::functions::flinksql {

template <typename T>
struct FlinkLPadFunction : public sparksql::PadFunctionBase<
                               T,
                               true,
                               sparksql::PadInvalidInputPolicy::kReturnNull> {};

template <typename T>
struct FlinkRPadFunction : public sparksql::PadFunctionBase<
                               T,
                               false,
                               sparksql::PadInvalidInputPolicy::kReturnNull> {};

/// substr(string, start[, length]) -> varchar
/// Flink-compatible substring: zero start uses the first character and a
/// negative length returns null.
template <typename T>
struct FlinkSubstrFunction : public SubstrFunctionBase<
                                 T,
                                 true,
                                 SubstrInvalidInputPolicy::kReturnNull> {};

/// isDigit function
/// isDigit(str) -> boolean
/// returns true if the given string is a digit, false otherwise
template <typename T>
struct IsDigitFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullable(
      bool& result,
      const arg_type<Varchar>* str) {
    if (str == nullptr) {
      result = false;
    } else {
      result = stringCore::isDigit(str->data(), str->size());
    }
    return true;
  }
};

/// isAlpha function
/// isAlpha(str) -> boolean
/// returns true if the given string is an alphabet, false otherwise
template <typename T>
struct IsAlphaFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullable(
      bool& result,
      const arg_type<Varchar>* str) {
    if (str == nullptr) {
      result = false;
    } else {
      result = stringCore::isAlpha(str->data(), str->size());
    }
    return true;
  }
};

/// isDecimal function
/// isDecimal(str) -> boolean
/// returns true if the given string is a decimal, false otherwise
template <typename T>
struct IsDecimalFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullable(
      bool& result,
      const arg_type<Varchar>* str) {
    if (str == nullptr) {
      result = false;
    } else {
      result = stringCore::isDecimal(str->data(), str->size());
    }
    return true;
  }
};

// Field indexes start with 0.
template <typename T, bool flinkCompatible = false>
struct SplitIndex {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      const int64_t& index) {
    if (input.empty()) {
      return false;
    }
    if constexpr (flinkCompatible) {
      if (index < 0) {
        return false;
      }
      if (delimiter.empty()) {
        return splitWhitespace(result, input, index);
      }
    }
    return stringImpl::splitPart(result, input, delimiter, index, 0);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<int32_t>& delimiter,
      const int64_t& index) {
    if (delimiter > 255 || delimiter < 1 || index < 0 || input.empty()) {
      return false;
    }
    if constexpr (flinkCompatible) {
      char encoded[2];
      size_t size = 1;
      if (delimiter < 128) {
        encoded[0] = static_cast<char>(delimiter);
      } else {
        encoded[0] = static_cast<char>(0xc0 | (delimiter >> 6));
        encoded[1] = static_cast<char>(0x80 | (delimiter & 0x3f));
        size = 2;
      }
      return stringImpl::splitPart(
          result, input, StringView(encoded, size), index, 0);
    } else {
      auto delimiterStr = std::string(1, static_cast<char>(delimiter));
      return stringImpl::splitPart(
          result, input, arg_type<Varchar>{delimiterStr}, index, 0);
    }
  }

 private:
  static bool isJavaWhitespace(UChar32 codePoint) {
    // Character.isWhitespace in Java 8 includes U+180E and excludes NBSP.
    return (codePoint >= 0x09 && codePoint <= 0x0d) ||
        (codePoint >= 0x1c && codePoint <= 0x20) || codePoint == 0x1680 ||
        codePoint == 0x180e || (codePoint >= 0x2000 && codePoint <= 0x2006) ||
        (codePoint >= 0x2008 && codePoint <= 0x200a) || codePoint == 0x2028 ||
        codePoint == 0x2029 || codePoint == 0x205f || codePoint == 0x3000;
  }

  static bool splitWhitespace(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int64_t index) {
    int32_t begin = 0;
    int32_t pos = 0;
    int64_t token = 0;
    const auto size = static_cast<int32_t>(input.size());
    while (pos < size) {
      const auto end = pos;
      UChar32 codePoint;
      U8_NEXT(input.data(), pos, size, codePoint);
      if (isJavaWhitespace(codePoint)) {
        if (token == index) {
          result.setNoCopy(StringView(input.data() + begin, end - begin));
          return true;
        }
        ++token;
        begin = pos;
      }
    }
    if (token == index) {
      result.setNoCopy(StringView(input.data() + begin, size - begin));
      return true;
    }
    return false;
  }
};

template <typename T>
using FlinkSplitIndex = SplitIndex<T, true>;

} // namespace bytedance::bolt::functions::flinksql
