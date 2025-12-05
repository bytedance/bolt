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

#pragma once

#include <folly/Conv.h>
#include <folly/Range.h>
#include <folly/String.h>
#include <ryu/ryu.h>
#include <cctype>
#include <string>
#include <type_traits>
#include "bolt/common/base/Exceptions.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/Type.h"

DECLARE_bool(experimental_enable_legacy_cast);
namespace bytedance::bolt::util {

struct DefaultCastPolicy {
  static constexpr bool truncate = false;
  static constexpr bool legacyCast = false;
};

struct TruncateCastPolicy {
  static constexpr bool truncate = true;
  static constexpr bool legacyCast = false;
};

struct LegacyCastPolicy {
  static constexpr bool truncate = false;
  static constexpr bool legacyCast = true;
};

struct TruncateLegacyCastPolicy {
  static constexpr bool truncate = true;
  static constexpr bool legacyCast = true;
};

template <TypeKind KIND, typename = void, typename TPolicy = DefaultCastPolicy>
struct Converter {
  template <typename T>
  static typename TypeTraits<KIND>::NativeType cast(T, bool* nullOutput) {
    BOLT_UNSUPPORTED(
        "Conversion to {} is not supported", TypeTraits<KIND>::name);
  }
};

template <typename TPolicy>
struct Converter<TypeKind::BOOLEAN, void, TPolicy> {
  using T = bool;

  template <typename From>
  static T cast(const From& v, bool* nullOutput) {
    // if constexpr (TPolicy::truncate) {
    //   BOLT_UNSUPPORTED("Conversion to BOOLEAN is not supported");
    // }
    return folly::to<T>(v);
  }

  static T cast(folly::StringPiece v, bool* nullOutput) {
    return folly::to<T>(v);
  }

  static T cast(const StringView& v, bool* nullOutput) {
    return folly::to<T>(folly::StringPiece(v));
  }

  static T cast(const std::string& v, bool* nullOutput) {
    return folly::to<T>(v);
  }

  static T cast(const bool& v) {
    return folly::to<T>(v);
  }

  static T cast(const float& v) {
    if constexpr (TPolicy::truncate) {
      if (std::isnan(v)) {
        return false;
      }
      return v != 0;
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const double& v) {
    if constexpr (TPolicy::truncate) {
      if (std::isnan(v)) {
        return false;
      }
      return v != 0;
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int8_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int16_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int32_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int64_t& v) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const Timestamp&, bool* nullOutput) {
    BOLT_UNSUPPORTED("Conversion of Timestamp to Boolean is not supported");
  }
};

template <TypeKind KIND, typename TPolicy>
struct Converter<
    KIND,
    std::enable_if_t<
        KIND == TypeKind::TINYINT || KIND == TypeKind::SMALLINT ||
            KIND == TypeKind::INTEGER || KIND == TypeKind::BIGINT ||
            KIND == TypeKind::HUGEINT,
        void>,
    TPolicy> {
  using T = typename TypeTraits<KIND>::NativeType;

  template <typename From>
  static T cast(const From&, bool* nullOutput) {
    BOLT_UNSUPPORTED(
        "Conversion to {} is not supported", TypeTraits<KIND>::name);
  }

  static T convertStringToInt(
      folly::StringPiece v,
      bool* nullOutput,
      bool* hasPoint = nullptr) {
    // Handling boolean target case fist because it is in this scope
    if constexpr (std::is_same_v<T, bool>) {
      return folly::to<T>(v);
    }
    // following spark & presto semantics
    // trim white spaces(' ', '\r', '\t' and '\n')
    v = folly::trimWhitespace(v);

    // original presto semantics
    // remove leading ' ', '\r' and '\t'
    // v = folly::ltrim(v, [](char ch) -> bool {
    //   return ch == ' ' || ch == '\r' || ch == '\t';
    // });

    // Handling integer target cases
    T result = 0;
    int index = 0;
    int len = v.size();
    if (len == 0) {
      if (nullOutput != nullptr) {
        *nullOutput = true;
        return 0;
      } else {
        BOLT_USER_FAIL("Cannot cast an empty string to an integral value.");
      }
    }

    // Setting negative flag
    bool negative = false;
    // Setting decimalPoint flag
    bool decimalPoint = false;
    if (v[index] == '-' || v[index] == '+') {
      if (len == 1) {
        if (nullOutput != nullptr) {
          *nullOutput = true;
          return 0;
        } else {
          BOLT_USER_FAIL("Cannot cast an '-' string to an integral value.");
        }
      }
      negative = v[index] == '-';
      index = 1;
    }
    if (negative) {
      for (; index < len; index++) {
        // Truncate the decimal
        if (!decimalPoint && v[index] == '.') {
          decimalPoint = true;
          if (++index == len) {
            break;
          }
        }
        if (!std::isdigit(v[index])) {
          if (nullOutput != nullptr) {
            *nullOutput = true;
            return 0;
          } else {
            BOLT_USER_FAIL("Encountered a non-digit character");
          }
        }
        if (!decimalPoint) {
          auto overflow = __builtin_mul_overflow(result, 10, &result);
          overflow |= __builtin_sub_overflow(result, v[index] - '0', &result);
          if (UNLIKELY(overflow)) {
            if (nullOutput != nullptr) {
              *nullOutput = true;
              return 0;
            } else {
              BOLT_USER_FAIL("Value is too large for type");
            }
          }
        }
      }
    } else {
      for (; index < len; index++) {
        // Truncate the decimal
        if (!decimalPoint && v[index] == '.') {
          decimalPoint = true;
          if (++index == len) {
            break;
          }
        }
        if (!std::isdigit(v[index])) {
          if (nullOutput != nullptr) {
            *nullOutput = true;
            return 0;
          } else {
            BOLT_USER_FAIL("Encountered a non-digit character");
          }
        }
        if (!decimalPoint) {
          auto overflow = __builtin_mul_overflow(result, 10, &result);
          overflow |= __builtin_add_overflow(result, v[index] - '0', &result);
          if (UNLIKELY(overflow)) {
            if (nullOutput != nullptr) {
              *nullOutput = true;
              return 0;
            } else {
              BOLT_USER_FAIL("Value is too large for type");
            }
          }
        }
      }
    }
    if (hasPoint != nullptr) {
      *hasPoint = decimalPoint;
    }
    // Final result
    return result;
  }

  static T cast(folly::StringPiece v, bool* nullOutput) {
    if constexpr (TPolicy::truncate) {
      return convertStringToInt(v, nullOutput);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const StringView& v, bool* nullOutput) {
    if constexpr (TPolicy::truncate) {
      return convertStringToInt(folly::StringPiece(v), nullOutput);
    } else {
      return folly::to<T>(folly::StringPiece(v));
    }
  }

  static T cast(const std::string& v, bool* nullOutput) {
    if constexpr (TPolicy::truncate) {
      return convertStringToInt(v, nullOutput);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const bool& v, bool* nullOutput) {
    return folly::to<T>(v);
  }

  struct LimitType {
    static constexpr bool kByteOrSmallInt =
        std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t>;
    static int64_t minLimit() {
      if (kByteOrSmallInt) {
        return std::numeric_limits<int32_t>::min();
      }
      return std::numeric_limits<T>::min();
    }
    static int64_t maxLimit() {
      if (kByteOrSmallInt) {
        return std::numeric_limits<int32_t>::max();
      }
      return std::numeric_limits<T>::max();
    }
    static T min() {
      if (kByteOrSmallInt) {
        return 0;
      }
      return std::numeric_limits<T>::min();
    }
    static T max() {
      if (kByteOrSmallInt) {
        return -1;
      }
      return std::numeric_limits<T>::max();
    }
    template <typename FP>
    static T cast(const FP& v) {
      if (kByteOrSmallInt) {
        return T(int32_t(v));
      }
      return T(v);
    }
  };

  static T cast(const float& v, bool* nullOutput) {
    if constexpr (TPolicy::truncate) {
      if (std::isnan(v)) {
        return 0;
      }
      if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::max();
      } else if (v > LimitType::maxLimit()) {
        return LimitType::max();
      }
      if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::min();
      } else if (v < LimitType::minLimit()) {
        return LimitType::min();
      }
      return LimitType::cast(v);
    } else {
      if (std::isnan(v)) {
        if (nullOutput != nullptr) {
          *nullOutput = true;
          return 0;
        } else {
          BOLT_USER_FAIL("Cannot cast NaN to an integral value.");
        }
      }
      return folly::to<T>(std::round(v));
    }
  }

  static T cast(const double& v, bool* nullOutput) {
    if constexpr (TPolicy::truncate) {
      if (std::isnan(v)) {
        return 0;
      }
      if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::max();
      } else if (v > LimitType::maxLimit()) {
        return LimitType::max();
      }
      if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::min();
      } else if (v < LimitType::minLimit()) {
        return LimitType::min();
      }
      return LimitType::cast(v);
    } else {
      if (std::isnan(v)) {
        if (nullOutput != nullptr) {
          *nullOutput = true;
          return 0;
        } else {
          BOLT_USER_FAIL("Cannot cast NaN to an integral value.");
        }
      }
      return folly::to<T>(std::round(v));
    }
  }

  static T cast(const int8_t& v, bool* nullOutput) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int16_t& v, bool* nullOutput) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int32_t& v, bool* nullOutput) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }

  static T cast(const int64_t& v, bool* nullOutput) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return folly::to<T>(v);
    }
  }
};

template <TypeKind KIND, typename TPolicy>
struct Converter<
    KIND,
    std::enable_if_t<KIND == TypeKind::REAL || KIND == TypeKind::DOUBLE, void>,
    TPolicy> {
  using T = typename TypeTraits<KIND>::NativeType;

  template <typename From>
  static T cast(const From& v, bool* nullOutput) {
    return folly::to<T>(v);
  }

  static T cast(folly::StringPiece v, bool* nullOutput) {
    folly::StringPiece newV = folly::trimWhitespace(v);
    if (newV.empty()) {
      return folly::to<T>(newV);
    }

    auto pos = 0;
    if (newV.front() == '+' || newV.front() == '-') {
      pos++;
    }

    bool noPop = pos < newV.size() && (newV[pos] == 'n' || newV[pos] == 'i');

    if (!noPop && !newV.empty() &&
        (newV.back() == 'f' || newV.back() == 'F' || newV.back() == 'd' ||
         newV.back() == 'D')) {
      newV.pop_back();
    }

    return folly::to<T>(newV);
  }

  static T cast(const StringView& v, bool* nullOutput) {
    return cast(folly::StringPiece(v), nullOutput);
  }

  static T cast(const std::string& v, bool* nullOutput) {
    return cast(folly::StringPiece(v), nullOutput);
  }

  static T cast(const bool& v, bool* nullOutput) {
    return cast<bool>(v, nullOutput);
  }

  static T cast(const float& v, bool* nullOutput) {
    return cast<float>(v, nullOutput);
  }

  static T cast(const double& v, bool* nullOutput) {
    if constexpr (TPolicy::truncate) {
      return T(v);
    } else {
      return cast<double>(v, nullOutput);
    }
  }

  static T cast(const int8_t& v, bool* nullOutput) {
    return cast<int8_t>(v, nullOutput);
  }

  static T cast(const int16_t& v, bool* nullOutput) {
    return cast<int16_t>(v, nullOutput);
  }

  // Convert integer to double or float directly, not using folly, as it
  // might throw 'loss of precision' error.
  static T cast(const int32_t& v, bool* nullOutput) {
    return static_cast<T>(v);
  }

  // Convert large integer to double or float directly, not using folly, as it
  // might throw 'loss of precision' error.
  static T cast(const int64_t& v, bool* nullOutput) {
    return static_cast<T>(v);
  }

  // Convert large integer to double or float directly, not using folly, as it
  // might throw 'loss of precision' error.
  static T cast(const int128_t& v, bool* nullOutput) {
    return static_cast<T>(v);
  }

  static T cast(const Timestamp&, bool* nullOutput) {
    BOLT_UNSUPPORTED(
        "Conversion of Timestamp to Real or Double is not supported");
  }
};

template <typename TPolicy>
struct Converter<TypeKind::VARBINARY, void, TPolicy> {
  // Same semantics of TypeKind::VARCHAR converter.
  template <typename T>
  static std::string cast(const T& val, bool* nullOutput) {
    return Converter<TypeKind::VARCHAR, void, TPolicy>::cast(val, nullOutput);
  }
};

template <typename TPolicy>
struct Converter<TypeKind::VARCHAR, void, TPolicy> {
  template <typename T>
  static std::string cast(const T& val, bool* nullOutput) {
    if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
      if constexpr (TPolicy::legacyCast) {
        auto str = folly::to<std::string>(val);
        normalizeStandardNotation(str);
        return str;
      }
      if constexpr (std::is_same_v<T, double>) {
        auto doublePtr = d2s(val);
        std::string res(doublePtr);
        free(doublePtr);
        return res;
      } else { // float use f2s
        auto floatPtr = f2s(val);
        std::string res(floatPtr);
        free(floatPtr);
        return res;
      }
    }

    return folly::to<std::string>(val);
  }

  static std::string cast(const Timestamp& val, bool* nullOutput) {
    TimestampToStringOptions options;
    options.precision = TimestampToStringOptions::Precision::kMilliseconds;
    if constexpr (!TPolicy::legacyCast) {
      options.zeroPaddingYear = true;
      options.dateTimeSeparator = ' ';
    }
    return val.toString(options);
  }

  static std::string cast(const bool& val, bool* nullOutput) {
    return val ? "true" : "false";
  }

  /// Normalize the given floating-point standard notation string in place, by
  /// appending '.0' if it has only the integer part but no fractional part. For
  /// example, for the given string '12345', replace it with '12345.0'.
  static void normalizeStandardNotation(std::string& str) {
    if (!FLAGS_experimental_enable_legacy_cast &&
        str.find(".") == std::string::npos && isdigit(str[str.length() - 1])) {
      str += ".0";
    }
  }

  /// Normalize the given floating-point scientific notation string in place, by
  /// removing the trailing 0s of the coefficient as well as the leading '+' and
  /// 0s of the exponent. For example, for the given string '3.0000000E+005',
  /// replace it with '3.0E5'. For '-1.2340000E-010', replace it with
  /// '-1.234E-10'.
  static void normalizeScientificNotation(std::string& str) {
    size_t idxE = str.find('E');
    BOLT_DCHECK_NE(
        idxE,
        std::string::npos,
        "Expect a character 'E' in scientific notation.");

    int endCoef = idxE - 1;
    while (endCoef >= 0 && str[endCoef] == '0') {
      --endCoef;
    }
    BOLT_DCHECK_GT(endCoef, 0, "Coefficient should not be all zeros.");

    int pos = endCoef + 1;
    if (str[endCoef] == '.') {
      pos++;
    }
    str[pos++] = 'E';

    int startExp = idxE + 1;
    if (str[startExp] == '-') {
      str[pos++] = '-';
      startExp++;
    }
    while (startExp < str.length() &&
           (str[startExp] == '0' || str[startExp] == '+')) {
      startExp++;
    }
    BOLT_DCHECK_LT(startExp, str.length(), "Exponent should not be all zeros.");
    str.replace(pos, str.length() - startExp, str, startExp);
    pos += str.length() - startExp;

    str.resize(pos);
  }
};

// Allow conversions from string to TIMESTAMP type.
template <typename TPolicy>
struct Converter<TypeKind::TIMESTAMP, void, TPolicy> {
  using T = typename TypeTraits<TypeKind::TIMESTAMP>::NativeType;

  template <typename From>
  static T cast(const From& /* v */, bool* nullOutput) {
    BOLT_UNSUPPORTED("Conversion to Timestamp is not supported");
    return T();
  }

  static T cast(folly::StringPiece v, bool* nullOutput) {
    return fromTimestampString(v.data(), v.size(), nullOutput);
  }

  static T cast(const StringView& v, bool* nullOutput) {
    return fromTimestampString(v.data(), v.size(), nullOutput);
  }

  static T cast(const std::string& v, bool* nullOutput) {
    return fromTimestampString(v.data(), v.size(), nullOutput);
  }
};

} // namespace bytedance::bolt::util
