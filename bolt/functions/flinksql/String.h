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

#include "bolt/functions/Macros.h"
#include "bolt/functions/lib/string/StringCore.h"
#include "bolt/functions/lib/string/StringImpl.h"
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
template <typename T>
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
    auto delimiterStr = std::string(1, static_cast<char>(delimiter));
    return stringImpl::splitPart(
        result, input, arg_type<Varchar>{delimiterStr}, index, 0);
  }
};
} // namespace bytedance::bolt::functions::flinksql
