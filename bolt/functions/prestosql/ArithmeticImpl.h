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

#include <algorithm>
#include <cmath>
#include <type_traits>
#include "bolt/type/BigDecimal.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/type/FloatingPointUtil.h"
#include "folly/CPortability.h"
namespace bytedance::bolt::functions {

/// Round function
/// when AlwaysRoundNegDec is false, presto semantics is followed which does not
/// round negative decimals for integrals and round it otherwise
template <typename TNum, typename TDecimals, bool alwaysRoundNegDec = false>
FOLLY_ALWAYS_INLINE TNum
round(const TNum& number, const TDecimals& decimals = 0) {
  static_assert(!std::is_same_v<TNum, bool> && "round not supported for bool");

  if constexpr (std::is_integral_v<TNum>) {
    if constexpr (alwaysRoundNegDec) {
      if (decimals >= 0) {
        return number;
      }
      // Fixed integral rounding in round when alwaysRoundNegDec is true and
      // decimals are negative by rounding to the
      //  nearest power of ten, preventing fall-through and matching Spark
      //  expectations. Without this, there will be compilation error when
      //  alwaysRoundNegDec is true
      constexpr auto kMaxScale = sizeof(DecimalUtil::kPowersOfTen) /
          sizeof(DecimalUtil::kPowersOfTen[0]);
      const auto scale = static_cast<uint64_t>(-decimals);
      if (scale >= kMaxScale) {
        return static_cast<TNum>(0);
      }

      const auto scalingFactor = DecimalUtil::kPowersOfTen[scale];
      int128_t value = static_cast<int128_t>(number);
      int128_t quotient = value / scalingFactor;
      int128_t remainder = value % scalingFactor;
      if (value >= 0 && remainder >= scalingFactor / 2) {
        ++quotient;
      } else if (remainder <= -scalingFactor / 2) {
        --quotient;
      }
      int128_t rounded = quotient * scalingFactor;
      return static_cast<TNum>(rounded);
    } else {
      return number;
    }
  }
  if (!std::isfinite(number)) {
    return number;
  }

  double dNumber = static_cast<double>(number);
  BigDecimal decimal(dNumber);
  decimal.setScale(decimals);
  if constexpr (std::is_same_v<TNum, double>) {
    auto res = decimal.doubleValue();
    return res;
  } else if constexpr (std::is_same_v<TNum, float>) {
    auto res = decimal.floatValue();
    return res;
  }
}

// This is used by Bolt for floating points plus.
template <typename T>
T plus(const T a, const T b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
{
  return a + b;
}

// This is used by Bolt for floating points minus.
template <typename T>
T minus(const T a, const T b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
{
  return a - b;
}

// This is used by Bolt for floating points multiply.
template <typename T>
T multiply(const T a, const T b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
{
  return a * b;
}

// This is used by Bolt for floating points divide.
template <typename T>
T divide(const T& a, const T& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((__no_sanitize__("float-divide-by-zero")))
#endif
#endif
{
  T result = a / b;
  return result;
}

// This is used by Bolt for floating points modulus.
template <typename T>
T modulus(const T a, const T b) {
  if (b == 0) {
    // Match Presto semantics
    return std::numeric_limits<T>::quiet_NaN();
  }
  return std::fmod(a, b);
}

template <typename T>
T negate(const T& arg) {
  T results = std::negate<std::remove_cv_t<T>>()(arg);
  return results;
}

template <typename T>
T abs(const T& arg) {
  T results = std::abs(arg);
  return results;
}

template <typename T>
T floor(const T& arg) {
  T results = std::floor(arg);
  return results;
}

template <typename T>
T ceil(const T& arg) {
  T results = std::ceil(arg);
  return results;
}

FOLLY_ALWAYS_INLINE double truncate(
    const double& number,
    const int32_t& decimals = 0) {
  const bool decNegative = (decimals < 0);
  const auto log10Size = DoubleUtil::kPowersOfTen.size(); // 309
  if (decNegative && decimals <= -log10Size) {
    return 0.0;
  }

  const uint64_t absDec = decNegative ? -decimals : decimals;
  const double tmp = (absDec < log10Size) ? DoubleUtil::kPowersOfTen[absDec]
                                          : std::pow(10.0, (double)absDec);

  const double valueMulTmp = number * tmp;
  if (!decNegative && !std::isfinite(valueMulTmp)) {
    return number;
  }

  const double valueDivTmp = number / tmp;
  if (number >= 0.0) {
    return decimals < 0 ? std::floor(valueDivTmp) * tmp
                        : std::floor(valueMulTmp) / tmp;
  } else {
    return decimals < 0 ? std::ceil(valueDivTmp) * tmp
                        : std::ceil(valueMulTmp) / tmp;
  }
}

// helper function for calculating upper and lower limit of wilson interval
template <bool isUpper>
FOLLY_ALWAYS_INLINE double
wilsonInterval(int64_t successes, int64_t trials, double z) {
  BOLT_USER_CHECK_GE(successes, 0, "number of successes must not be negative");
  BOLT_USER_CHECK_GT(trials, 0, "number of trials must be positive");
  BOLT_USER_CHECK_LE(
      successes,
      trials,
      "number of successes must not be larger than number of trials");
  BOLT_USER_CHECK_GE(z, 0, "z-score must not be negative");

  double s{static_cast<double>(successes)};
  double n{static_cast<double>(trials)};
  double p{s / n};

  // Wilson interval limits are solutions of a quadratic equation.
  // Let the equation be {ax^2 + bx + c = 0}.
  // r will store the value (-b + sqrt(b*b - 4*a*c)).
  double a, c, r;

  // Compute the equations differently depending on whether z is large or small.
  // This helps to avoid computations like (INFINITY/INFINITY),
  // yielding accurate results in the limit as z approaches infinity.
  if (z < 1) {
    a = n + z * z;
    c = s * p;
    r = 2 * s + z * z + z * std::sqrt(z * z + 4 * s * (1 - p));
  } else {
    a = n / (z * z) + 1;
    c = s * p / (z * z);
    r = 2 * s / (z * z) + 1 + std::sqrt(1 + 4 * s * (1 - p) / (z * z));
  }

  // Since (s, n, z >= 0), r >= 0 is guaranteed, but r == 0 needs to be handled.
  if constexpr (isUpper) {
    return r / (2 * a);
  } else {
    return (r > 0) ? (2 * c) / r : 0;
  }
}

} // namespace bytedance::bolt::functions
