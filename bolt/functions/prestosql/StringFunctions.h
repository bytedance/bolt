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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#pragma once

#include "bolt/functions/Udf.h"
#include "bolt/functions/lib/string/StringCore.h"
#include "bolt/functions/lib/string/StringImpl.h"

#include <boost/url.hpp>
#include <re2/re2.h>
namespace bytedance::bolt::functions {

/// chr(n) → varchar
/// Returns the Unicode code point n as a single character string.
template <typename T>
struct ChrFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const int64_t& codePoint) {
    stringImpl::codePointToString(result, codePoint);
  }
};

/// codepoint(string) → integer
/// Returns the Unicode code point of the only character of string.
template <typename T>
struct CodePointFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Varchar>& inputChar) {
    result = stringImpl::charToCodePoint(inputChar);
  }
};

/// substr(string, start) -> varchar
///
///     Returns the rest of string from the starting position start.
///     Positions start with 1. A negative starting position is interpreted as
///     being relative to the end of the string. Returns empty string if
///     absolute value of start is greater then length of the string.

///
/// substr(string, start, length) -> varchar
///
///     Returns a substring from string of length length from the
///     starting position start. Positions start with 1. A negative starting
///     position is interpreted as being relative to the end of the string.
///     Returns empty string if absolute value of start is greater then length
///     of the string.
template <typename T, bool supportZeroIndex = false>
struct SubstrFunctionBase {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  template <typename I>
  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      I start,
      I length = std::numeric_limits<I>::max()) {
    doCall<false>(result, input, start, length);
  }

  template <typename I>
  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      I start,
      I length = std::numeric_limits<I>::max()) {
    doCall<true>(result, input, start, length);
  }

  template <bool isAscii, typename I>
  FOLLY_ALWAYS_INLINE void doCall(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      I start,
      I length = std::numeric_limits<I>::max()) {
    // Following Presto semantics
    if (length <= 0) {
      result.setEmpty();
      return;
    }

    if (start == 0) {
      if constexpr (supportZeroIndex) {
        start = 1;
      } else {
        result.setEmpty();
        return;
      }
    }

    I numCharacters = stringImpl::length<isAscii>(input);

    // Adjusting start
    if (start < 0) {
      start = numCharacters + start + 1;
    }

    // Following Presto semantics
    if (start <= 0 || start > numCharacters) {
      result.setEmpty();
      return;
    }

    // Adjusting length
    if (numCharacters - start + 1 < length) {
      // set length to the max valid length
      length = numCharacters - start + 1;
    }

    auto byteRange =
        stringCore::getByteRange<isAscii>(input.data(), start, length);

    // Generating output string
    result.setNoCopy(StringView(
        input.data() + byteRange.first, byteRange.second - byteRange.first));
  }
};

template <typename T>
struct SubstrFunction : SubstrFunctionBase<T, false> {};

/// Trim functions
/// ltrim(string) -> varchar
///    Removes leading whitespaces from the string.
/// rtrim(string) -> varchar
///    Removes trailing whitespaces from the string.
/// trim(string) -> varchar
///    Removes leading and trailing whitespaces from the string.
template <typename T, bool leftTrim, bool rightTrim>
struct TrimFunctionBase {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    stringImpl::trimUnicodeWhiteSpace<leftTrim, rightTrim>(result, input);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& trimCharacters) {
    if (stringCore::isAscii(trimCharacters.data(), trimCharacters.size())) {
      callAscii(result, input, trimCharacters);
    } else {
      BOLT_UNSUPPORTED(
          "trim functions with custom trim characters and non-ASCII inputs are not supported yet");
    }
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    stringImpl::trimAscii<leftTrim, rightTrim>(
        result, input, stringImpl::isAsciiWhiteSpace);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& trimCharacters) {
    const auto numChars = trimCharacters.size();
    const auto* chars = trimCharacters.data();
    stringImpl::trimAscii<leftTrim, rightTrim>(result, input, [&](char c) {
      for (auto i = 0; i < numChars; ++i) {
        if (c == chars[i]) {
          return true;
        }
      }
      return false;
    });
  }
};

template <typename T>
struct TrimFunction : public TrimFunctionBase<T, true, true> {};

template <typename T>
struct LTrimFunction : public TrimFunctionBase<T, true, false> {};

template <typename T>
struct RTrimFunction : public TrimFunctionBase<T, false, true> {};

/// Returns number of characters in the specified string.
template <typename T>
struct LengthFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const StringView& input) {
    result = stringImpl::length<false>(input);
  }

  FOLLY_ALWAYS_INLINE void callAscii(int64_t& result, const StringView& input) {
    result = stringImpl::length<true>(input);
  }
};

/// Returns number of bytes in the specified varbinary.
template <typename T>
struct LengthVarbinaryFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const StringView& input) {
    result = input.size();
  }
};

template <typename T>
struct StartsWithFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<bool>& result,
      const arg_type<Varchar>& x,
      const arg_type<Varchar>& y) {
    if (x.size() < y.size()) {
      result = false;
      return;
    }

    result = (memcmp(x.data(), y.data(), y.size()) == 0);
  }
};

template <typename T>
struct EndsWithFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<bool>& result,
      const arg_type<Varchar>& x,
      const arg_type<Varchar>& y) {
    if (x.size() < y.size()) {
      result = false;
      return;
    }

    result =
        (memcmp(x.data() + (x.size() - y.size()), y.data(), y.size()) == 0);
  }
};

/// Pad functions
/// lpad(string, size, padString) → varchar
///     Left pads string to size characters with padString.  If size is
///     less than the length of string, the result is truncated to size
///     characters.  size must not be negative and padString must be non-empty.
/// rpad(string, size, padString) → varchar
///     Right pads string to size characters with padString.  If size is
///     less than the length of string, the result is truncated to size
///     characters.  size must not be negative and padString must be non-empty.
template <typename T, bool lpad>
struct PadFunctionBase {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& string,
      const arg_type<int64_t>& size,
      const arg_type<Varchar>& padString) {
    stringImpl::pad<lpad, false /*isAscii*/>(result, string, size, padString);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& string,
      const arg_type<int64_t>& size,
      const arg_type<Varchar>& padString) {
    stringImpl::pad<lpad, true /*isAscii*/>(result, string, size, padString);
  }
};

template <typename T>
struct LPadFunction : public PadFunctionBase<T, true> {};

template <typename T>
struct RPadFunction : public PadFunctionBase<T, false> {};

/// strpos and strrpos functions
/// strpos(string, substring) → bigint
///     Returns the starting position of the first instance of substring in
///     string. Positions start with 1. If not found, 0 is returned.
/// strpos(string, substring, instance) → bigint
///     Returns the position of the N-th instance of substring in string.
///     instance must be a positive number. Positions start with 1. If not
///     found, 0 is returned.
/// strrpos(string, substring) → bigint
///     Returns the starting position of the first instance of substring in
///     string counting from the end. Positions start with 1. If not found, 0 is
///     returned.
/// strrpos(string, substring, instance) → bigint
///     Returns the position of the N-th instance of substring in string
///     counting from the end. Instance must be a positive number. Positions
///     start with 1. If not found, 0 is returned.
template <typename T, bool lpos>
struct StrPosFunctionBase {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<int64_t>& result,
      const arg_type<Varchar>& string,
      const arg_type<Varchar>& subString,
      const arg_type<int64_t>& instance = 1) {
    result = stringImpl::stringPosition<false /*isAscii*/, lpos>(
        std::string_view(string.data(), string.size()),
        std::string_view(subString.data(), subString.size()),
        instance);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<int64_t>& result,
      const arg_type<Varchar>& string,
      const arg_type<Varchar>& subString,
      const arg_type<int64_t>& instance = 1) {
    result = stringImpl::stringPosition<true /*isAscii*/, lpos>(
        std::string_view(string.data(), string.size()),
        std::string_view(subString.data(), subString.size()),
        instance);
  }
};

template <typename T>
struct StrLPosFunction : public StrPosFunctionBase<T, true> {};

template <typename T>
struct StrRPosFunction : public StrPosFunctionBase<T, false> {};

// this function must be linked with Boost::url
// or include boost/url/src.hpp, see
// https://www.boost.org/doc/libs/1_81_0/libs/url/doc/html/url/overview.html#url.overview.requirements
template <typename T>
struct ParseURLFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void initialize(
      const core::QueryConfig& /* config */,
      arg_type<Varchar>* urlStr,
      arg_type<Varchar>* part,
      arg_type<Varchar>* key) {
    if (key) {
      constPatern_ = std::make_unique<re2::RE2>(
          "(&|^)" + std::string(key->data(), key->size()) + "=([^&]*)");
      BOLT_USER_CHECK(
          constPatern_->ok(),
          fmt::format(
              "invalid key: {}", std::string_view(key->data, key->size())));
    }
  }

  FOLLY_ALWAYS_INLINE
  bool call(
      out_type<Varchar>& output,
      const arg_type<Varchar>& urlStr,
      const arg_type<Varchar>& part) {
    auto urlResult = boost::urls::parse_uri(std::string_view(urlStr));
    if (urlResult.has_error()) {
      return false;
    }
    auto& url = urlResult.value();

    std::string outputStr;
    if (!extractPart(outputStr, url, part) || outputStr.empty()) {
      return false;
    }
    output = outputStr;
    return true;
  }

  FOLLY_ALWAYS_INLINE
  bool call(
      out_type<Varchar>& output,
      const arg_type<Varchar>& urlStr,
      const arg_type<Varchar>& part,
      const arg_type<Varchar>& key) {
    auto urlResult = boost::urls::parse_uri(std::string_view(urlStr));
    if (urlResult.has_error()) {
      return false;
    }
    auto& url = urlResult.value();

    std::string partStr;
    if (!extractPart(partStr, url, part) || key.empty()) {
      return false;
    }

    std::string outputStr;
    if (constPatern_) {
      if (RE2::PartialMatch(
              re2::StringPiece(partStr), *constPatern_, nullptr, &outputStr)) {
        output = outputStr;
        return true;
      }
    } else {
      if (RE2::PartialMatch(
              re2::StringPiece(partStr),
              "(&|^)" + std::string(key.data(), key.size()) + "=([^&]*)",
              nullptr,
              &outputStr)) {
        output = outputStr;
        return true;
      }
    }
    return false;
  }

 protected:
  bool extractPart(
      std::string& output,
      const boost::url_view& url,
      const arg_type<Varchar>& part) {
    if (part == "HOST") {
      output = url.host();
    } else if (part == "PATH") {
      output = url.path();
    } else if (part == "QUERY") {
      if (!url.has_query()) {
        return false;
      }
      output = url.query();
    } else if (part == "REF") {
      if (!url.has_fragment()) {
        return false;
      }
      output = url.fragment();
    } else if (part == "PROTOCOL") {
      if (!url.has_scheme()) {
        return false;
      }
      output = url.scheme();
    } else if (part == "FILE") {
      output = url.path();
      if (url.has_query()) {
        output += '?' + url.query();
      }
    } else if (part == "AUTHORITY") {
      if (!url.has_authority()) {
        return false;
      }
      auto authority = url.authority();
      output = std::string(authority.data(), authority.size());
    } else if (part == "USERINFO") {
      if (!url.has_userinfo()) {
        return false;
      }
      output = url.userinfo();
    } else {
      return false;
    }
    return true;
  }

  std::unique_ptr<re2::RE2> constPatern_;
};

template <typename T>
struct LevenshteinDistanceFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<int64_t>& result,
      const arg_type<Varchar>& left,
      const arg_type<Varchar>& right) {
    auto leftCodePoints = stringImpl::stringToCodePoints(left);
    auto rightCodePoints = stringImpl::stringToCodePoints(right);
    doCall<int32_t>(
        result,
        leftCodePoints.data(),
        rightCodePoints.data(),
        leftCodePoints.size(),
        rightCodePoints.size());
  }

  void callAscii(
      out_type<int64_t>& result,
      const arg_type<Varchar>& left,
      const arg_type<Varchar>& right) {
    auto leftCodePoints = reinterpret_cast<const uint8_t*>(left.data());
    auto rightCodePoints = reinterpret_cast<const uint8_t*>(right.data());
    doCall<uint8_t>(
        result, leftCodePoints, rightCodePoints, left.size(), right.size());
  }

  FOLLY_ALWAYS_INLINE
  bool call(
      out_type<Varchar>& output,
      const arg_type<Varchar>& urlStr,
      const arg_type<Varchar>& part) {
    auto urlResult = boost::urls::parse_uri(std::string_view(urlStr));
    if (urlResult.has_error()) {
      return false;
    }
    auto& url = urlResult.value();

    std::string outputStr;
    if (!extractPart(outputStr, url, part) || outputStr.empty()) {
      return false;
    }
    output = outputStr;
    return true;
  }

  template <typename TCodePoint>
  void doCall(
      out_type<int64_t>& result,
      const TCodePoint* leftCodePoints,
      const TCodePoint* rightCodePoints,
      size_t leftCodePointsSize,
      size_t rightCodePointsSize) {
    if (leftCodePointsSize < rightCodePointsSize) {
      doCall(
          result,
          rightCodePoints,
          leftCodePoints,
          rightCodePointsSize,
          leftCodePointsSize);
      return;
    }
    if (rightCodePointsSize == 0) {
      result = leftCodePointsSize;
      return;
    }

    static const int32_t kMaxCombinedInputSize = 1'000'000;
    auto combinedInputSize = leftCodePointsSize * rightCodePointsSize;
    BOLT_USER_CHECK_LE(
        combinedInputSize,
        kMaxCombinedInputSize,
        "The combined inputs size exceeded max Levenshtein distance combined input size,"
        " the code points size of left is {}, code points size of right is {}",
        leftCodePointsSize,
        rightCodePointsSize);

    std::vector<int32_t> distances;
    distances.reserve(rightCodePointsSize);
    for (int i = 0; i < rightCodePointsSize; i++) {
      distances.push_back(i + 1);
    }

    for (int i = 0; i < leftCodePointsSize; i++) {
      auto leftUpDistance = distances[0];
      if (leftCodePoints[i] == rightCodePoints[0]) {
        distances[0] = i;
      } else {
        distances[0] = std::min(i, distances[0]) + 1;
      }
      for (int j = 1; j < rightCodePointsSize; j++) {
        auto leftUpDistanceNext = distances[j];
        if (leftCodePoints[i] == rightCodePoints[j]) {
          distances[j] = leftUpDistance;
        } else {
          distances[j] =
              std::min(
                  distances[j - 1], std::min(leftUpDistance, distances[j])) +
              1;
        }
        leftUpDistance = leftUpDistanceNext;
      }
    }
    result = distances[rightCodePointsSize - 1];
  }
};

/// locate function
/// locate(substr, str[, pos]) -> int
/// Returns the position of the first occurrence of substr in str after position
/// pos. The given pos and return value are 1-based.
bool locateEmptyStringsReturnZero();

template <typename T>
struct LocateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Varchar>& substr,
      const arg_type<Varchar>& str,
      const int32_t pos = 1) {
    if (UNLIKELY(pos < 1)) {
      result = 0;
      return;
    }
    if (UNLIKELY(
            substr.size() == 0 && str.size() == 0 &&
            locateEmptyStringsReturnZero())) {
      result = 0;
      return;
    }
    auto ans = stringCore::findNthInstanceByteIndexFromStart(
        std::string_view(str.data(), str.size()),
        std::string_view(substr.data(), substr.size()),
        1,
        pos - 1);
    result = ans + 1;
  }
};

} // namespace bytedance::bolt::functions
