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

#include <iostream>
#include <string>
#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/sparksql/DecimalUtil.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/type/HugeInt.h"
namespace bytedance::bolt::functions::sparksql {

// Returns the whole and fraction parts of a decimal value.
template <typename T>
inline std::pair<T, T> getWholeAndFraction(T value, uint8_t scale) {
  const auto scaleFactor = bolt::DecimalUtil::getPowersOfTen(scale);
  const T whole = value / scaleFactor;
  return {whole, value - whole * scaleFactor};
}

// Increases the scale of input value by 'delta'. Returns the input value if
// delta is not positive.
inline int128_t increaseScale(int128_t in, int16_t delta) {
  // No need to consider overflow as 'delta == higher scale - input scale', so
  // the scaled value will not exceed the maximum of long decimal.
  return delta <= 0 ? in : in * bolt::DecimalUtil::getPowersOfTen(delta);
}

// Scales up the whole part to result scale, and combine it with fraction part
// to produce a full result for decimal add. Checks whether the result
// overflows.
template <typename T>
inline T
decimalAddResult(T whole, T fraction, uint8_t resultScale, bool& overflow) {
  T scaledWhole = DecimalUtil::multiply<T>(
      whole, bolt::DecimalUtil::getPowersOfTen(resultScale), overflow);
  if (FOLLY_UNLIKELY(overflow)) {
    return 0;
  }
  const auto result = scaledWhole + fraction;
  if constexpr (std::is_same_v<T, int64_t>) {
    overflow = (result > bolt::DecimalUtil::kShortDecimalMax) ||
        (result < bolt::DecimalUtil::kShortDecimalMin);
  } else {
    overflow = (result > bolt::DecimalUtil::kLongDecimalMax) ||
        (result < bolt::DecimalUtil::kLongDecimalMin);
  }
  return result;
}

// Reduces the scale of input value by 'delta'. Returns the input value if delta
// is not positive.
template <typename T>
inline static T reduceScale(T in, int32_t delta) {
  if (delta <= 0) {
    return in;
  }
  T result;
  bool overflow;
  const auto scaleFactor = bolt::DecimalUtil::getPowersOfTen(delta);
  if constexpr (std::is_same_v<T, int64_t>) {
    BOLT_DCHECK_LE(
        scaleFactor,
        std::numeric_limits<int64_t>::max(),
        "Scale factor should not exceed the maximum of int64_t.");
  }
  DecimalUtil::divideWithRoundUp<T, T, T>(
      result, in, T(scaleFactor), 0, overflow);
  BOLT_DCHECK(!overflow);
  return result;
}

// Adds two non-negative values by adding the whole and fraction parts
// separately.
template <typename TResult, typename A, typename B>
inline static TResult addLargeNonNegative(
    A a,
    B b,
    uint8_t aScale,
    uint8_t bScale,
    uint8_t rScale,
    bool& overflow) {
  BOLT_DCHECK_GE(
      a, 0, "Non-negative value is expected in addLargeNonNegative.");
  BOLT_DCHECK_GE(
      b, 0, "Non-negative value is expected in addLargeNonNegative.");

  // Separate whole and fraction parts.
  const auto [aWhole, aFraction] = getWholeAndFraction<A>(a, aScale);
  const auto [bWhole, bFraction] = getWholeAndFraction<B>(b, bScale);

  // Adjust fractional parts to higher scale.
  const auto higherScale = std::max(aScale, bScale);
  const auto aFractionScaled =
      increaseScale((int128_t)aFraction, higherScale - aScale);
  const auto bFractionScaled =
      increaseScale((int128_t)bFraction, higherScale - bScale);

  int128_t fraction;
  bool carryToLeft = false;
  const auto carrier = bolt::DecimalUtil::getPowersOfTen(higherScale);
  if (aFractionScaled >= carrier - bFractionScaled) {
    fraction = aFractionScaled + bFractionScaled - carrier;
    carryToLeft = true;
  } else {
    fraction = aFractionScaled + bFractionScaled;
  }

  // Scale up the whole part and scale down the fraction part to combine them.
  fraction = reduceScale(TResult(fraction), higherScale - rScale);
  const auto whole = TResult(aWhole) + TResult(bWhole) + TResult(carryToLeft);
  return decimalAddResult(whole, TResult(fraction), rScale, overflow);
}

// Adds two opposite values by adding the whole and fraction parts separately.
template <typename TResult, typename A, typename B>
inline static TResult addLargeOpposite(
    A a,
    B b,
    uint8_t aScale,
    uint8_t bScale,
    int32_t rScale,
    bool& overflow) {
  BOLT_DCHECK(
      (a < 0 && b > 0) || (a > 0 && b < 0),
      "One positive and one negative value are expected in addLargeOpposite.");

  // Separate whole and fraction parts.
  const auto [aWhole, aFraction] = getWholeAndFraction<A>(a, aScale);
  const auto [bWhole, bFraction] = getWholeAndFraction<B>(b, bScale);

  // Adjust fractional parts to higher scale.
  const auto higherScale = std::max(aScale, bScale);
  const auto aFractionScaled =
      increaseScale((int128_t)aFraction, higherScale - aScale);
  const auto bFractionScaled =
      increaseScale((int128_t)bFraction, higherScale - bScale);

  // No need to consider overflow because two inputs are opposite.
  int128_t whole = (int128_t)aWhole + (int128_t)bWhole;
  int128_t fraction = aFractionScaled + bFractionScaled;

  // If the whole and fractional parts have different signs, adjust them to the
  // same sign.
  const auto scaleFactor = bolt::DecimalUtil::getPowersOfTen(higherScale);
  if (whole < 0 && fraction > 0) {
    whole += 1;
    fraction -= scaleFactor;
  } else if (whole > 0 && fraction < 0) {
    whole -= 1;
    fraction += scaleFactor;
  }

  // Scale up the whole part and scale down the fraction part to combine them.
  fraction = reduceScale(TResult(fraction), higherScale - rScale);
  return decimalAddResult(TResult(whole), TResult(fraction), rScale, overflow);
}

template <typename TResult, typename A, typename B>
inline static TResult addLarge(
    A a,
    B b,
    uint8_t aScale,
    uint8_t bScale,
    int32_t rScale,
    bool& overflow) {
  if (a >= 0 && b >= 0) {
    // Both non-negative.
    return addLargeNonNegative<TResult, A, B>(
        a, b, aScale, bScale, rScale, overflow);
  } else if (a <= 0 && b <= 0) {
    // Both non-positive.
    return TResult(-addLargeNonNegative<TResult, A, B>(
        A(-a), B(-b), aScale, bScale, rScale, overflow));
  } else {
    // One positive and the other negative.
    return addLargeOpposite<TResult, A, B>(
        a, b, aScale, bScale, rScale, overflow);
  }
}

class Addition {
 public:
  template <typename TResult, typename A, typename B>
  inline static void apply(
      TResult& r,
      A a,
      B b,
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t /* aPrecision */,
      uint8_t aScale,
      uint8_t /* bPrecision */,
      uint8_t bScale,
      uint8_t rPrecision,
      uint8_t rScale,
      bool& overflow) {
    if (rPrecision < LongDecimalType::kMaxPrecision) {
      const int128_t aRescaled =
          a * bolt::DecimalUtil::getPowersOfTen(aRescale);
      const int128_t bRescaled =
          b * bolt::DecimalUtil::getPowersOfTen(bRescale);
      r = TResult(aRescaled + bRescaled);
    } else {
      const uint32_t minLeadingZeros =
          DecimalUtil::minLeadingZeros<A, B>(a, b, aRescale, bRescale);
      if (minLeadingZeros >= 3) {
        // Fast path for no overflow. If both numbers contain at least 3 leading
        // zeros, they can be added directly without the risk of overflow.
        // The reason is if a number contains at least 2 leading zeros, it is
        // ensured that the number fits in the maximum of decimal, because
        // '2^126 - 1 < 10^38 - 1'. If both numbers contain at least 3 leading
        // zeros, we are guaranteed that the result will have at least 2 leading
        // zeros.
        int128_t aRescaled = a * bolt::DecimalUtil::getPowersOfTen(aRescale);
        int128_t bRescaled = b * bolt::DecimalUtil::getPowersOfTen(bRescale);
        r = reduceScale(
            TResult(aRescaled + bRescaled), std::max(aScale, bScale) - rScale);
      } else {
        // The risk of overflow should be considered. Add whole and fraction
        // parts separately, and then combine.
        r = addLarge<TResult, A, B>(a, b, aScale, bScale, rScale, overflow);
      }
    }
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return std::max(0, toScale - fromScale);
  }

  template <bool allowPrecisionLoss>
  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale) {
    auto precision = std::max(aPrecision - aScale, bPrecision - bScale) +
        std::max(aScale, bScale) + 1;
    auto scale = std::max(aScale, bScale);
    if constexpr (allowPrecisionLoss) {
      return sparksql::DecimalUtil::adjustPrecisionScale(precision, scale);
    } else {
      return sparksql::DecimalUtil::bounded(precision, scale);
    }
  }
};

class Subtraction {
 public:
  template <typename TResult, typename A, typename B>
  inline static void apply(
      TResult& r,
      A a,
      B b,
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale,
      uint8_t rPrecision,
      uint8_t rScale,
      bool& overflow) {
    Addition::apply<TResult, A, B>(
        r,
        a,
        B(-b),
        aRescale,
        bRescale,
        aPrecision,
        aScale,
        bPrecision,
        bScale,
        rPrecision,
        rScale,
        overflow);
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return std::max(0, toScale - fromScale);
  }
  template <bool allowPrecisionLoss>
  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale) {
    return Addition::computeResultPrecisionScale<allowPrecisionLoss>(
        aPrecision, aScale, bPrecision, bScale);
  }
};

static std::array<
    functions::sparksql::int256_t,
    LongDecimalType::kMaxPrecision + LongDecimalType::kMaxPrecision + 1>
    kLargeScaleMultipliers =
        ([]() -> std::array<
                  functions::sparksql::int256_t,
                  LongDecimalType::kMaxPrecision +
                      LongDecimalType::kMaxPrecision + 1> {
          std::array<
              functions::sparksql::int256_t,
              LongDecimalType::kMaxPrecision + LongDecimalType::kMaxPrecision +
                  1>
              values;
          values[0] = 1;
          for (int32_t idx = 1; idx <=
               LongDecimalType::kMaxPrecision + LongDecimalType::kMaxPrecision;
               idx++) {
            values[idx] = values[idx - 1] * 10;
          }
          return values;
        })();

class Multiply {
 public:
  // Derive from Arrow.
  // https://github.com/apache/arrow/blob/release-12.0.1-rc1/cpp/src/gandiva/precompiled/decimal_ops.cc#L331
  template <typename R, typename A, typename B>
  inline static void apply(
      R& r,
      A a,
      B b,
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale,
      uint8_t rPrecision,
      uint8_t rScale,
      bool& overflow) {
    if (rPrecision < 38) {
      R result = DecimalUtil::multiply<R>(R(a), R(b), overflow);
      BOLT_DCHECK(!overflow);
      r = DecimalUtil::multiply<R>(
          result,
          R(bolt::DecimalUtil::getPowersOfTen(aRescale + bRescale)),
          overflow);
      BOLT_DCHECK(!overflow);
    } else if (a == 0 && b == 0) {
      // Handle this separately to avoid divide-by-zero errors.
      r = R(0);
    } else {
      auto deltaScale = aScale + bScale - rScale;
      if (deltaScale == 0) {
        // No scale down.
        // Multiply when the out_precision is 38, and there is no trimming of
        // the scale i.e the intermediate value is the same as the final value.
        r = DecimalUtil::multiply<R>(R(a), R(b), overflow);
      } else {
        // Scale down.
        // It's possible that the intermediate value does not fit in 128-bits,
        // but the final value will (after scaling down).
        int32_t totalLeadingZeros =
            bits::countLeadingZeros(DecimalUtil::absValue<A>(a)) +
            bits::countLeadingZeros(DecimalUtil::absValue<B>(b));
        // This check is quick, but conservative. In some cases it will
        // indicate that converting to 256 bits is necessary, when it's not
        // actually the case.
        if (UNLIKELY(totalLeadingZeros <= 128)) {
          // Needs int256.
          int256_t reslarge =
              static_cast<int256_t>(a) * static_cast<int256_t>(b);
          reslarge = reduceScaleBy(reslarge, deltaScale);
          r = DecimalUtil::convert<R>(reslarge, overflow);
        } else {
          if (LIKELY(deltaScale <= 38)) {
            // The largest value that result can have here is (2^64 - 1) * (2^63
            // - 1) = 1.70141E+38,which is greater than
            // DecimalUtil::kLongDecimalMax.
            R result = DecimalUtil::multiply<R>(R(a), R(b), overflow);
            BOLT_DCHECK(!overflow);
            // Since deltaScale is greater than zero, result can now be at most
            // ((2^64 - 1) * (2^63 - 1)) / 10, which is less than
            // DecimalUtil::kLongDecimalMax, so there cannot be any overflow.
            DecimalUtil::divideWithRoundUp<R, R, R>(
                r,
                result,
                R(bolt::DecimalUtil::kPowersOfTen[deltaScale]),
                0,
                overflow);
            BOLT_DCHECK(!overflow);
          } else {
            // We are multiplying decimal(38, 38) by decimal(38, 38). The result
            // should be a
            // decimal(38, 37), so delta scale = 38 + 38 - 37 = 39. Since we are
            // not in the 256 bit intermediate value case and we are scaling
            // down by 39, then we are guaranteed that the result is 0 (even if
            // we try to round). The largest possible intermediate result is 38
            // "9"s. If we scale down by 39, the leftmost 9 is now two digits to
            // the right of the rightmost "visible" one. The reason why we have
            // to handle this case separately is because a scale multiplier with
            // a deltaScale 39 does not fit into 128 bit.
            r = R(0);
          }
        }
      }
    }
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return 0;
  }

  template <bool allowPrecisionLoss>
  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale) {
    std::pair<uint8_t, uint8_t> rPrecisionScale;
    if constexpr (allowPrecisionLoss) {
      rPrecisionScale = DecimalUtil::adjustPrecisionScale(
          aPrecision + bPrecision + 1, aScale + bScale);
    } else {
      rPrecisionScale =
          DecimalUtil::bounded(aPrecision + bPrecision + 1, aScale + bScale);
    }
    return rPrecisionScale;
  }

 private:
  inline static int256_t reduceScaleBy(int256_t in, int32_t reduceBy) {
    if (reduceBy == 0) {
      return in;
    }

    int256_t divisor = getScaleMultiplier(reduceBy);
    auto result = in / divisor;
    auto remainder = in % divisor;
    // Round up.
    if (abs(remainder) >= (divisor >> 1)) {
      result += (in > 0 ? 1 : -1);
    }
    return result;
  }
  // Compute the scale multipliers once.

  static functions::sparksql::int256_t getScaleMultiplier(int scale) {
    DCHECK_GE(scale, 0);
    DCHECK_LE(scale, 2 * LongDecimalType::kMaxPrecision);

    return kLargeScaleMultipliers[scale];
  }
};

class Divide {
 public:
  template <typename R, typename A, typename B>
  inline static void apply(
      R& r,
      A a,
      B b,
      uint8_t aRescale,
      uint8_t /* bRescale */,
      uint8_t /* aPrecision */,
      uint8_t /* aScale */,
      uint8_t /* bPrecision */,
      uint8_t /* bScale */,
      uint8_t /* rPrecision */,
      uint8_t /* rScale */,
      bool& overflow) {
    DecimalUtil::divideWithRoundUp<R, A, B>(r, a, b, aRescale, overflow);
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale) {
    return rScale - fromScale + toScale;
  }

  template <bool allowPrecisionLoss>
  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale) {
    if constexpr (allowPrecisionLoss) {
      auto scale = std::max(6, aScale + bPrecision + 1);
      auto precision = aPrecision - aScale + bScale + scale;
      return DecimalUtil::adjustPrecisionScale(precision, scale);
    } else {
      auto wholeDigits = std::min(38, aPrecision - aScale + bScale);
      auto fractionDigits = std::min(38, std::max(6, aScale + bPrecision + 1));
      auto diff = (wholeDigits + fractionDigits) - 38;
      if (diff > 0) {
        fractionDigits -= diff / 2 + 1;
        wholeDigits = 38 - fractionDigits;
      }
      return DecimalUtil::bounded(wholeDigits + fractionDigits, fractionDigits);
    }
  }
};

} // namespace bytedance::bolt::functions::sparksql
