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

#include <boost/multiprecision/cpp_int.hpp>
#include <charconv>
#include <string>
#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/common/base/CountBits.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/Nulls.h"
#include "bolt/common/base/Status.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt {
/// A static class that holds helper functions for DECIMAL type.
class DecimalUtil {
 public:
  static constexpr int128_t kPowersOfTen[LongDecimalType::kMaxPrecision + 1] = {
      1,
      10,
      100,
      1'000,
      10'000,
      100'000,
      1'000'000,
      10'000'000,
      100'000'000,
      1'000'000'000,
      10'000'000'000,
      100'000'000'000,
      1'000'000'000'000,
      10'000'000'000'000,
      100'000'000'000'000,
      1'000'000'000'000'000,
      10'000'000'000'000'000,
      100'000'000'000'000'000,
      1'000'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10,
      1'000'000'000'000'000'000 * (int128_t)100,
      1'000'000'000'000'000'000 * (int128_t)1'000,
      1'000'000'000'000'000'000 * (int128_t)10'000,
      1'000'000'000'000'000'000 * (int128_t)100'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000'000 *
          (int128_t)10,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000'000 *
          (int128_t)100};

  // ceil(log2(10 ^ k)) for k in [0...LongDecimalType::kMaxPrecision]
  static constexpr int
      kCeilLog2PowersOfTen[LongDecimalType::kMaxPrecision + 1] = {
          0,  4,  7,  10, 14,  17,  20,  24,  27,  30,  34,  37,  40,
          44, 47, 50, 54, 57,  60,  64,  67,  70,  74,  77,  80,  84,
          87, 90, 94, 97, 100, 103, 107, 110, 113, 117, 120, 123, 127};

  FOLLY_ALWAYS_INLINE static int128_t getPowersOfTen(uint8_t scale) {
    // TODO: change to DCHECK later
    BOLT_CHECK_GE(scale, 0);
    BOLT_CHECK_LE(scale, LongDecimalType::kMaxPrecision);
    return kPowersOfTen[scale];
  }

  static constexpr int128_t kLongDecimalMin =
      -kPowersOfTen[LongDecimalType::kMaxPrecision] + 1;
  static constexpr int128_t kLongDecimalMax =
      kPowersOfTen[LongDecimalType::kMaxPrecision] - 1;
  static constexpr int128_t kShortDecimalMin =
      -kPowersOfTen[ShortDecimalType::kMaxPrecision] + 1;
  static constexpr int128_t kShortDecimalMax =
      kPowersOfTen[ShortDecimalType::kMaxPrecision] - 1;

  static constexpr uint64_t kInt64Mask = ~(static_cast<uint64_t>(1) << 63);
  static constexpr uint128_t kInt128Mask = (static_cast<uint128_t>(1) << 127);

  FOLLY_ALWAYS_INLINE static void valueInRange(int128_t value) {
    BOLT_USER_CHECK(
        (value >= kLongDecimalMin && value <= kLongDecimalMax),
        "Decimal overflow. Value '{}' is not in the range of Decimal Type",
        value);
  }

  // Returns true if the precision can represent the value.
  template <typename T>
  FOLLY_ALWAYS_INLINE static bool valueInPrecisionRange(
      T value,
      uint8_t precision) {
    return value < kPowersOfTen[precision] && value > -kPowersOfTen[precision];
  }

  /// Helper function to convert a decimal value to string.
  static std::string toString(int128_t value, const TypePtr& type);

  /// Helper function to calculate max length for decimal to string conversion
  static int32_t stringSize(int32_t precision, int32_t scale);

  enum class DecimalStringFormat { kPlain, kSpark };

  template <typename T>
  inline static size_t convertToPlainString(
      T unscaledValue,
      int32_t scale,
      int32_t maxVarcharSize,
      char* const startPosition) {
    char* const endPosition = startPosition + maxVarcharSize;
    char* writePosition = startPosition;
    if (unscaledValue == 0) {
      *writePosition++ = '0';
      if (scale > 0) {
        *writePosition++ = '.';
        // Append leading zeros.
        std::memset(writePosition, '0', scale);
        writePosition += scale;
      }
    } else {
      if (unscaledValue < 0) {
        *writePosition++ = '-';
        unscaledValue = -unscaledValue;
      }
      auto [position, errorCode] = std::to_chars(
          writePosition,
          endPosition,
          unscaledValue / (T)DecimalUtil::kPowersOfTen[scale]);
      BOLT_DCHECK_EQ(
          errorCode,
          std::errc(),
          "Failed to cast decimal to varchar: {}",
          std::make_error_code(errorCode).message());
      writePosition = position;

      if (scale > 0) {
        *writePosition++ = '.';
        uint128_t fraction =
            unscaledValue % (T)DecimalUtil::kPowersOfTen[scale];
        // Append leading zeros.
        int numLeadingZeros = std::max(scale - countDigits(fraction), 0);
        std::memset(writePosition, '0', numLeadingZeros);
        writePosition += numLeadingZeros;
        // Append remaining fraction digits.
        auto result = std::to_chars(writePosition, endPosition, fraction);
        BOLT_DCHECK_EQ(
            result.ec,
            std::errc(),
            "Failed to cast decimal to varchar: {}",
            std::make_error_code(result.ec).message());
        writePosition = result.ptr;
      }
    }
    return writePosition - startPosition;
  }

  template <typename T>
  inline static size_t convertToSparkString(
      T unscaledValue,
      int32_t scale,
      int32_t maxVarcharSize,
      char* const startPosition) {
    if (scale == 0) {
      auto [endPosition, errorCode] = std::to_chars(
          startPosition, startPosition + maxVarcharSize, unscaledValue);
      BOLT_DCHECK_EQ(
          errorCode,
          std::errc(),
          "Failed to cast decimal to varchar: {}",
          std::make_error_code(errorCode).message());
      return endPosition - startPosition;
    }
    char* writePosition = startPosition;
    if (unscaledValue < 0) {
      *writePosition++ = '-';
      unscaledValue = -unscaledValue;
    }
    auto coeff = std::to_string(unscaledValue);
    int32_t coeffLen = coeff.size();
    auto adjusted = -scale + (coeffLen - 1);
    if (adjusted >= -6) { // plain number
      int pad = scale - coeffLen; // count of padding zeros
      if (pad >= 0) { // 0.xxx form
        *writePosition++ = '0';
        *writePosition++ = '.';
        std::memset(writePosition, '0', pad);
        writePosition += pad;
        std::memcpy(writePosition, coeff.data(), coeffLen);
        writePosition += coeffLen;
      } else { // xx.xx form
        std::memcpy(writePosition, coeff.data(), -pad);
        writePosition -= pad;
        *writePosition++ = '.';
        std::memcpy(writePosition, coeff.data() - pad, scale);
        writePosition += scale;
      }
    } else { // E-notation is needed
      *writePosition++ = coeff[0];
      if (coeffLen > 1) { // more to come
        *writePosition++ = '.';
        std::memcpy(writePosition, coeff.data() + 1, coeffLen - 1);
        writePosition += coeffLen - 1;
      }
      // [!sci could have made 0]
      *writePosition++ = 'E';
      auto [position, errorCode] = std::to_chars(
          writePosition, startPosition + maxVarcharSize, adjusted);
      BOLT_DCHECK_EQ(
          errorCode,
          std::errc(),
          "Failed to cast decimal to varchar: {}",
          std::make_error_code(errorCode).message());
      writePosition = position;
    }
    return writePosition - startPosition;
  }

  template <DecimalStringFormat format, typename T>
  inline static size_t convertToString(
      T unscaledValue,
      int32_t scale,
      int32_t maxVarcharSize,
      char* const startPosition) {
    if constexpr (format == DecimalStringFormat::kSpark) {
      return convertToSparkString(
          unscaledValue, scale, maxVarcharSize, startPosition);
    } else {
      return convertToPlainString(
          unscaledValue, scale, maxVarcharSize, startPosition);
    }
  }

  /// @brief Convert the unscaled value of a decimal to varchar and write to raw
  /// string buffer from start position.
  /// @tparam T The type of input value.
  /// @param unscaledValue The input unscaled value.
  /// @param scale The scale of decimal.
  /// @param maxVarcharSize The estimated max size of a varchar.
  /// @param startPosition The start position to write from.
  /// @return write size
  template <typename T>
  inline static size_t convertToString(
      T unscaledValue,
      int32_t scale,
      int32_t maxVarcharSize,
      char* const startPosition) {
#ifdef SPARK_COMPATIBLE
    return convertToString<DecimalStringFormat::kSpark>(
        unscaledValue, scale, maxVarcharSize, startPosition);
#else
    return convertToString<DecimalStringFormat::kPlain>(
        unscaledValue, scale, maxVarcharSize, startPosition);
#endif
  }

  template <typename T>
  inline static void fillDecimals(
      T* decimals,
      const uint64_t* nullsPtr,
      const T* values,
      const int64_t* scales,
      int32_t numValues,
      int32_t targetScale) {
    for (int32_t i = 0; i < numValues; i++) {
      if (!nullsPtr || !bits::isBitNull(nullsPtr, i)) {
        int32_t currentScale = scales[i];
        T value = values[i];
        if constexpr (std::is_same_v<T, std::int64_t>) { // Short Decimal
          if (targetScale > currentScale &&
              targetScale - currentScale <= ShortDecimalType::kMaxPrecision) {
            value *= static_cast<T>(kPowersOfTen[targetScale - currentScale]);
          } else if (
              targetScale < currentScale &&
              currentScale - targetScale <= ShortDecimalType::kMaxPrecision) {
            value /= static_cast<T>(kPowersOfTen[currentScale - targetScale]);
          } else if (targetScale != currentScale) {
            BOLT_FAIL("Decimal scale out of range");
          }
        } else { // Long Decimal
          if (targetScale > currentScale) {
            while (targetScale > currentScale) {
              int32_t scaleAdjust = std::min<int32_t>(
                  ShortDecimalType::kMaxPrecision, targetScale - currentScale);
              value *= kPowersOfTen[scaleAdjust];
              currentScale += scaleAdjust;
            }
          } else if (targetScale < currentScale) {
            while (currentScale > targetScale) {
              int32_t scaleAdjust = std::min<int32_t>(
                  ShortDecimalType::kMaxPrecision, currentScale - targetScale);
              value /= kPowersOfTen[scaleAdjust];
              currentScale -= scaleAdjust;
            }
          }
        }
        decimals[i] = value;
      }
    }
  }

  template <typename TInput, typename TOutput>
  inline static Status rescaleWithRoundUp(
      TInput inputValue,
      int fromPrecision,
      int fromScale,
      int toPrecision,
      int toScale,
      TOutput& output) {
    int128_t rescaledValue = inputValue;
    auto scaleDifference = toScale - fromScale;
    bool isOverflow = false;
    if (scaleDifference >= 0) {
      isOverflow = __builtin_mul_overflow(
          rescaledValue,
          DecimalUtil::kPowersOfTen[scaleDifference],
          &rescaledValue);
    } else {
      scaleDifference = -scaleDifference;
      const auto scalingFactor = DecimalUtil::kPowersOfTen[scaleDifference];
      rescaledValue /= scalingFactor;
      int128_t remainder = inputValue % scalingFactor;
      if (inputValue >= 0 && remainder >= scalingFactor / 2) {
        ++rescaledValue;
      } else if (remainder <= -scalingFactor / 2) {
        --rescaledValue;
      }
    }
    // Check overflow.
    if (!valueInPrecisionRange(rescaledValue, toPrecision) || isOverflow) {
      return Status::UserError(
          "Cannot cast DECIMAL '{}' to DECIMAL({}, {})",
          DecimalUtil::toString(inputValue, DECIMAL(fromPrecision, fromScale)),
          toPrecision,
          toScale);
    }
    output = static_cast<TOutput>(rescaledValue);
    return Status::OK();
  }

  template <typename TInput, typename TOutput>
  inline static std::optional<TOutput>
  rescaleInt(TInput inputValue, int toPrecision, int toScale) {
    int128_t rescaledValue = static_cast<int128_t>(inputValue);
    bool isOverflow = __builtin_mul_overflow(
        rescaledValue, DecimalUtil::kPowersOfTen[toScale], &rescaledValue);
    // Check overflow.
    if (!valueInPrecisionRange(rescaledValue, toPrecision) || isOverflow) {
      return std::nullopt;
    }
    return static_cast<TOutput>(rescaledValue);
  }

  /// Rescales a floating point value to decimal value of given precision and
  /// scale. The output is rescaled value of int128_t or int64_t type. Returns
  /// error status if fails.
  template <typename TIntput, typename TOutput>
  inline static Status rescaleFloatingPoint(
      TIntput value,
      int precision,
      int scale,
      TOutput& output) {
    if (!std::isfinite(value)) {
      return Status::UserError("The input value should be finite.");
    }
    if (value <= static_cast<TIntput>(std::numeric_limits<TOutput>::min()) ||
        value >= static_cast<TIntput>(std::numeric_limits<TOutput>::max())) {
      return Status::UserError("Result overflows.");
    }

    uint8_t digits;
    if constexpr (std::is_same_v<TIntput, float>) {
      // A float provides between 6 and 7 decimal digits, so at least 6 digits
      // are precise.
      digits = 6;
    } else {
      // A double provides from 15 to 17 decimal digits, so at least 15 digits
      // are precise.
      digits = 15;
    }

    // Calculate the precise fractional digits.
    const auto integralValue =
        static_cast<uint128_t>(value > 0 ? value : -value);
    const auto integralDigits =
        integralValue == 0 ? 0 : countDigits(integralValue);
    const auto fractionDigits = std::max(digits - integralDigits, 0);
    // Scales up the input value with all the precise fractional digits kept.
    // Convert value as long double type because 1) double * int128_t returns
    // int128_t and fractional digits are lost. 2) we could also convert the
    // int128_t value as double to avoid 'double * int128_t', but double
    // multiplication gives inaccurate result on large numbers. For example,
    // -3333030000000000000 * 1e3 = -3333030000000000065536. No need to
    // consider the result becoming infinite as DOUBLE_MAX * 10^38 <
    // LONG_DOUBLE_MAX.
    long double scaledValue = std::round(
        (long double)value * DecimalUtil::kPowersOfTen[fractionDigits]);
    if (scale > fractionDigits) {
      scaledValue *= DecimalUtil::kPowersOfTen[scale - fractionDigits];
    } else {
      scaledValue /= DecimalUtil::kPowersOfTen[fractionDigits - scale];
    }

    const auto result = folly::tryTo<TOutput>(std::round(scaledValue));
    if (result.hasError()) {
      return Status::UserError("Result overflows.");
    }
    const TOutput rescaledValue = result.value();
    if (!valueInPrecisionRange<TOutput>(rescaledValue, precision)) {
      return Status::UserError(
          "Result cannot fit in the given precision {}.", precision);
    }
    output = rescaledValue;
    return Status::OK();
  }

  // rescale floating point value to decimal value with full precision.
  template <typename TInput, typename TOutput>
  inline static Status rescaleFullFloatingPoint(
      TInput value,
      int precision,
      int scale,
      TOutput& output) {
    constexpr int kMantissaBits = std::numeric_limits<TInput>::digits;
    constexpr int kMantissaDigits = std::numeric_limits<TInput>::max_digits10;
    using DecimalType = typename std::conditional<
        std::is_same<TOutput, int64_t>::value,
        ShortDecimalType,
        LongDecimalType>::type;
    constexpr int kMaxPrecision = DecimalType::kMaxPrecision;
    if (!std::isfinite(value)) {
      return Status::UserError("The input value should be finite.");
    }
    bool negative = value < 0;
    if (negative) {
      value = -value;
    }

    auto roundedRightShift = [](TOutput value, int shift) {
      if (shift == 0) {
        return value;
      }
      if (shift >= sizeof(TOutput) * 8) {
        // If the shift is larger than the size of TOutput, we return 0.
        return static_cast<TOutput>(0);
      }
      auto shifted = value >> shift;
      using unsigned_type = typename std::make_unsigned<TOutput>::type;
      auto threshold = static_cast<unsigned_type>(1) << (shift - 1);
      auto rest = value & ((((unsigned_type)1) << shift) - 1);
      if (rest >= threshold) {
        // Round up if the last bit is 1.
        shifted += 1;
      }
      return shifted;
    };

    // 1. Check that `real` is within acceptable bounds.
    const TInput limit = DecimalUtil::kPowersOfTen[precision - scale];
    if (value > limit) {
      // Checking the limit early helps ensure the computations below do not
      // overflow.
      // NOTE: `limit` is allowed here as rounding can make it smaller than
      // the theoretical limit (for example, 1.0e23 < 10^23).
      return Status::UserError("Result overflows.");
    }

    // 2. Losslessly convert `real` to `mant * 2**k`
    int binary_exp = 0;
    const TInput real_mant = std::frexp(value, &binary_exp);
    // `real_mant` is within 0.5 and 1 and has M bits of precision.
    // Multiply it by 2^M to get an exact integer.
    const uint64_t mant =
        static_cast<uint64_t>(std::ldexp(real_mant, kMantissaBits));
    const int k = binary_exp - kMantissaBits;
    // (note that `real = mant * 2^k`)

    // 3. Start with `mant`.
    // We want to end up with `real * 10^scale` i.e. `mant * 2^k * 10^scale`.
    TOutput x(mant);

    if (k < 0) {
      // k < 0 (i.e. binary_exp < kMantissaBits), is probably the common case
      // when converting to decimal. It implies right-shifting by -k bits,
      // while multiplying by 10^scale. We also must avoid overflow (losing
      // bits on the left) and precision loss (losing bits on the right).
      int right_shift_by = -k;
      int mul_by_ten_to = scale;

      // At this point, `x` has kMantissaDigits significant digits but it can
      // fit kMaxPrecision (excluding sign). We can therefore multiply by up
      // to 10^(kMaxPrecision - kMantissaDigits).
      constexpr int kSafeMulByTenTo = kMaxPrecision - kMantissaDigits;

      if (mul_by_ten_to <= kSafeMulByTenTo) {
        // Scale is small enough, so we can do it all at once.
        x *= kPowersOfTen[mul_by_ten_to];
        x = roundedRightShift(x, right_shift_by);
      } else {
        // Scale is too large, we cannot multiply at once without overflow.
        // We use an iterative algorithm which alternately shifts left by
        // multiplying by a power of ten, and shifts right by a number of bits.

        // First multiply `x` by as large a power of ten as possible
        // without overflowing.
        x *= kPowersOfTen[kSafeMulByTenTo];
        mul_by_ten_to -= kSafeMulByTenTo;

        // `x` now has full precision. However, we know we'll only
        // keep `precision` digits at the end. Extraneous bits/digits
        // on the right can be safely shifted away, before multiplying
        // again.
        // NOTE: if `precision` is the full precision then the algorithm will
        // lose the last digit. If `precision` is almost the full precision,
        // there can be an off-by-one error due to rounding.
        const int mul_step = std::max(1, kMaxPrecision - precision);

        // The running exponent, useful to compute by how much we must
        // shift right to make place on the left before the next multiply.
        int total_exp = 0;
        int total_shift = 0;
        while (mul_by_ten_to > 0 && right_shift_by > 0) {
          const int exp = std::min(mul_by_ten_to, mul_step);
          total_exp += exp;
          // The supplementary right shift required so that
          // `x * 10^total_exp / 2^total_shift` fits in the decimal.
          DCHECK_LT(
              static_cast<size_t>(total_exp), sizeof(kCeilLog2PowersOfTen));
          const int bits = std::min(
              right_shift_by, kCeilLog2PowersOfTen[total_exp] - total_shift);
          total_shift += bits;
          // Right shift to make place on the left, then multiply
          x = roundedRightShift(x, bits);
          right_shift_by -= bits;
          // Should not overflow thanks to the precautions taken
          x *= kPowersOfTen[exp];
          mul_by_ten_to -= exp;
        }
        if (mul_by_ten_to > 0) {
          x *= kPowersOfTen[mul_by_ten_to];
        }
        if (right_shift_by > 0) {
          x = roundedRightShift(x, right_shift_by);
        }
      }
    } else {
      // k >= 0 implies left-shifting by k bits and multiplying by 10^scale.
      // The order of these operations therefore doesn't matter. We know
      // we won't overflow because of the limit check above, and we also
      // won't lose any significant bits on the right.
      x *= kPowersOfTen[scale];
      x <<= k;
    }

    // Rounding might have pushed `x` just above the max precision, check again
    if (!valueInPrecisionRange<TOutput>(x, precision)) {
      return Status::UserError(
          "Result cannot fit in the given precision {}.", precision);
    }
    output = negative ? -x : x;
    return Status::OK();
  }

  using int256_t = boost::multiprecision::int256_t;

  template <typename R, typename A, typename B>
  inline static R divideWithRoundUp(
      R& r,
      A a,
      B b,
      bool noRoundUp,
      uint8_t aRescale,
      uint8_t /*bRescale*/) {
    BOLT_USER_CHECK_NE(b, 0, "Division by zero");
    int resultSign = 1;
    R unsignedDividendRescaled(a);
    if (a < 0) {
      resultSign = -1;
      unsignedDividendRescaled *= -1;
    }
    B unsignedDivisor(b);
    if (b < 0) {
      resultSign *= -1;
      unsignedDivisor *= -1;
    }

    if (aRescale <= LongDecimalType::kMaxPrecision &&
        (DecimalUtil::kPowersOfTen[aRescale] * unsignedDividendRescaled /
             unsignedDividendRescaled ==
         DecimalUtil::kPowersOfTen[aRescale])) {
      unsignedDividendRescaled = checkedMultiply<R>(
          unsignedDividendRescaled,
          R(DecimalUtil::kPowersOfTen[aRescale]),
          "Decimal");
      R quotient = unsignedDividendRescaled / unsignedDivisor;
      R remainder = unsignedDividendRescaled % unsignedDivisor;
      if (!noRoundUp &&
          static_cast<const B>(remainder) * 2 >= unsignedDivisor) {
        ++quotient;
      }
      r = quotient * resultSign;
      return remainder * resultSign;
    } else {
      // use int256_t for divide
      int256_t aLarge = a;
      for (int i = 0; i < aRescale / LongDecimalType::kMaxPrecision; i++) {
        aLarge *=
            bolt::DecimalUtil::getPowersOfTen(LongDecimalType::kMaxPrecision);
      }
      aLarge *= bolt::DecimalUtil::getPowersOfTen(
          aRescale % LongDecimalType::kMaxPrecision);

      int256_t bLarge = b;
      int256_t resultLarge = aLarge / bLarge;
      int256_t remainderLarge = aLarge % bLarge;
      if (!noRoundUp && remainderLarge * 2 >= bLarge) {
        ++resultLarge;
      }
      auto checkOverflow = [](int256_t value) {
        BOLT_CHECK(
            value <= std::numeric_limits<R>::max() &&
            value >= std::numeric_limits<R>::min());
      };

      int256_t result = resultLarge * resultSign;
      int256_t remainder = remainderLarge * resultSign;
      checkOverflow(result);
      checkOverflow(remainder);
      r = result.convert_to<R>();
      return remainder.convert_to<R>();
    }
  }

  /*
   * sum up and return overflow/underflow.
   */
  inline static int64_t addUnsignedValues(
      int128_t& sum,
      int128_t lhs,
      int128_t rhs,
      bool isResultNegative) {
    __uint128_t unsignedSum = (__uint128_t)lhs + (__uint128_t)rhs;
    // Ignore overflow value.
    sum = (int128_t)unsignedSum & ~kOverflowMultiplier;
    sum = isResultNegative ? -sum : sum;
    return (unsignedSum >> 127);
  }

  /// Adds two signed 128-bit numbers (int128_t), calculates the sum, and
  /// returns the overflow. It can be used to track the number of overflow when
  /// adding a batch of input numbers. It takes lhs and rhs as input, and stores
  /// their sum in result. overflow == 1 indicates upward overflow. overflow ==
  /// -1 indicates downward overflow. overflow == 0 indicates no overflow.
  /// Adding negative and non-negative numbers never overflows, so we can
  /// directly add them. Adding two negative or two positive numbers may
  /// overflow. To add numbers that may overflow, first convert both numbers to
  /// unsigned 128-bit number (uint128_t), and perform the addition. The highest
  /// bits in the result indicates overflow. Adjust the signs of sum and
  /// overflow based on the signs of the inputs. The caller must sum up overflow
  /// values and call adjustSumForOverflow after processing all inputs.
  inline static int64_t
  addWithOverflow(int128_t& result, int128_t lhs, int128_t rhs) {
    bool isLhsNegative = lhs < 0;
    bool isRhsNegative = rhs < 0;
    int64_t overflow = 0;
    if (isLhsNegative == isRhsNegative) {
      // Both inputs of same time.
      if (isLhsNegative) {
        // Both negative, ignore signs and add.
        BOLT_DCHECK_NE(lhs, std::numeric_limits<int128_t>::min());
        BOLT_DCHECK_NE(rhs, std::numeric_limits<int128_t>::min());
        overflow = addUnsignedValues(result, -lhs, -rhs, true);
        overflow = -overflow;
      } else {
        overflow = addUnsignedValues(result, lhs, rhs, false);
      }
    } else {
      // If one of them is negative, use addition.
      result = lhs + rhs;
    }
    return overflow;
  }

  /// Corrects the sum result calculated using addWithOverflow. Since the sum
  /// calculated by addWithOverflow only retains the lower 127 bits,
  /// it may miss one calculation of +(1 << 127) or -(1 << 127).
  /// Therefore, we need to make the following adjustments:
  /// 1. If overflow = 1 && sum < 0, the calculation missed +(1 << 127).
  /// Add 1 << 127 to the sum.
  /// 2. If overflow = -1 && sum > 0, the calculation missed -(1 << 127).
  /// Subtract 1 << 127 to the sum.
  /// If an overflow indeed occurs and the result cannot be adjusted,
  /// it will return std::nullopt.
  inline static std::optional<int128_t> adjustSumForOverflow(
      int128_t sum,
      int64_t overflow) {
    // Value is valid if the conditions below are true.
    if ((overflow == 1 && sum < 0) || (overflow == -1 && sum > 0)) {
      return static_cast<int128_t>(
          DecimalUtil::kOverflowMultiplier * overflow + sum);
    }
    if (overflow != 0) {
      // The actual overflow occurred.
      return std::nullopt;
    }

    return sum;
  }

  /// avg = (sum + overflow * kOverflowMultiplier) / count
  static void
  computeAverage(int128_t& avg, int128_t sum, int64_t count, int64_t overflow);

  /// Origins from java side BigInteger#bitLength.
  ///
  /// Returns the number of bits in the minimal two's-complement
  /// representation of this BigInteger, <em>excluding</em> a sign bit.
  /// For positive BigIntegers, this is equivalent to the number of bits in
  /// the ordinary binary representation.  For zero this method returns
  /// {@code 0}.  (Computes {@code (ceil(log2(this < 0 ? -this : this+1)))}.)
  ///
  /// @return number of bits in the minimal two's-complement
  ///         representation of this BigInteger, <em>excluding</em> a sign bit.
  static int32_t getByteArrayLength(int128_t value);

  /// This method return the same result with the BigInteger#toByteArray()
  /// method in Java side.
  ///
  /// Returns a byte array containing the two's-complement representation of
  /// this BigInteger. The byte array will be in big-endian byte-order: the most
  /// significant byte is in the zeroth element. The array will contain the
  /// minimum number of bytes required to represent this BigInteger, including
  /// at least one sign bit, which is (ceil((this.bitLength() + 1)/8)).
  ///
  /// @return The length of out.
  static int32_t toByteArray(int128_t value, char* out);

  template <typename InputType>
  inline static void stripTrailingZeros(InputType& input, uint8_t& scale) {
    while (std::abs(input) >= 10L && scale > 0) {
      if (bits::isBitSet(&input, 0)) { // odd
        break;
      }
      if (input % 10) { // to be optimized
        break;
      }
      input /= 10;
      // todo : checkScale
      scale -= 1;
    }
    return;
  }

  static constexpr __uint128_t kOverflowMultiplier = ((__uint128_t)1 << 127);

  /// Represent the varchar fragment.
  ///
  /// For example:
  /// | value | wholeDigits | fractionalDigits | exponent | sign |
  /// | 9999999999.99 | 9999999999 | 99 | nullopt | 1 |
  /// | 15 | 15 |  | nullopt | 1 |
  /// | 1.5 | 1 | 5 | nullopt | 1 |
  /// | -1.5 | 1 | 5 | nullopt | -1 |
  /// | 31.523e-2 | 31 | 523 | -2 | 1 |
  struct DecimalComponents {
    std::string_view wholeDigits;
    std::string_view fractionalDigits;
    std::optional<int32_t> exponent = std::nullopt;
    int8_t sign = 1;
  };

  // Extract a string view of continuous digits.
  static std::string_view
  extractDigits(const char* s, size_t start, size_t size);

  /// Parse decimal components, including whole digits, fractional digits,
  /// exponent and sign, from input chars. Returns error status if input chars
  /// do not represent a valid value.
  static Status
  parseDecimalComponents(const char* s, size_t size, DecimalComponents& out);

  /// Parse huge int from decimal components. The fractional part is scaled up
  /// by required power of 10, and added with the whole part. Returns error
  /// status if overflows.
  static Status parseHugeInt(
      const DecimalComponents& decimalComponents,
      int128_t& out);

  /// Modified from DecimalUtil::parseHugeInt.
  /// Generate decimals based on the provided DecimalComponents.
  ///
  /// For example:
  /// "123.456" is represented as an input argument
  /// DecimalComponents{.wholeDigits = "123", .fractionalDigits = "456"}.
  /// It will converted to an int64_t of the value 123.456 (DECIMAL(6,3)).
  template <typename T>
  static T parseDecimalFromComponents(
      const DecimalComponents& decimalComponents) {
    T out;

    // Parse the whole digits.
    auto sizeWholeDigits = decimalComponents.wholeDigits.size();
    if (sizeWholeDigits > 0) {
      const auto tryValue = folly::tryTo<T>(folly::StringPiece(
          decimalComponents.wholeDigits.data(), sizeWholeDigits));
      if (tryValue.hasError()) {
        return static_cast<T>(0);
      }
      out = tryValue.value();
    }

    // Parse the fractional digits.
    auto sizeFractionDigits = decimalComponents.fractionalDigits.size();
    if (sizeFractionDigits > 0) {
      const auto length = sizeFractionDigits;
      bool overflow = __builtin_mul_overflow(
          sizeWholeDigits > 0 ? out : 0,
          DecimalUtil::getPowersOfTen(length),
          &out);
      if (overflow) {
        return static_cast<T>(0);
      }
      const auto tryValue = folly::tryTo<T>(folly::StringPiece(
          decimalComponents.fractionalDigits.data(), length));
      if (tryValue.hasError()) {
        return static_cast<T>(0);
      }

      if (sizeWholeDigits > 0 && decimalComponents.wholeDigits[0] == '-') {
        overflow = __builtin_sub_overflow(out, tryValue.value(), &out);
      } else {
        overflow = __builtin_add_overflow(out, tryValue.value(), &out);
      }
      BOLT_DCHECK(!overflow);
    }

    if (sizeWholeDigits <= 0 && sizeFractionDigits <= 0) {
      out = 0;
    }

    return out;
  };

  /// Converts string view to decimal value of given precision and scale.
  /// Derives from Arrow function DecimalFromString. Arrow implementation:
  /// https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/decimal.cc#L637.
  ///
  /// Firstly, it parses the varchar to DecimalComponents which contains the
  /// message that can represent a decimal value. Secondly, processes the
  /// exponent to get the scale. Thirdly, compute the rescaled value. Returns
  /// status for the outcome of computing.
  template <typename T>
  static Status toDecimalValue(
      const StringView s,
      int toPrecision,
      int toScale,
      T& decimalValue) {
    DecimalComponents decimalComponents;
    if (auto status =
            parseDecimalComponents(s.data(), s.size(), decimalComponents);
        !status.ok()) {
      return Status::UserError("Value is not a number. " + status.message());
    }

    // Count number of significant digits (without leading zeros).
    const size_t firstNonZero =
        decimalComponents.wholeDigits.find_first_not_of('0');
    size_t significantDigits = decimalComponents.fractionalDigits.size();
    if (firstNonZero != std::string::npos) {
      significantDigits += decimalComponents.wholeDigits.size() - firstNonZero;
    }
    int32_t parsedPrecision = static_cast<int32_t>(significantDigits);

    int32_t parsedScale = 0;
    bool roundUp = false;
    const int32_t fractionSize = decimalComponents.fractionalDigits.size();
    if (!decimalComponents.exponent.has_value()) {
      if (fractionSize > toScale) {
        if (decimalComponents.fractionalDigits[toScale] >= '5') {
          roundUp = true;
        }
        parsedScale = toScale;
        decimalComponents.fractionalDigits = std::string_view(
            decimalComponents.fractionalDigits.data(), toScale);
      } else {
        parsedScale = fractionSize;
      }
    } else {
      const auto exponent = decimalComponents.exponent.value();
      parsedScale = -exponent + fractionSize;
      // Truncate the fractionalDigits.
      if (parsedScale > toScale) {
        if (-exponent >= toScale) {
          // The fractional digits could be dropped.
          if (fractionSize > 0 &&
              decimalComponents.fractionalDigits[0] >= '5') {
            roundUp = true;
          }
          decimalComponents.fractionalDigits = "";
          parsedScale -= fractionSize;
          // parsedPrecision -= fractionSize;
        } else {
          const auto reduceDigits = exponent + toScale;
          if (fractionSize > reduceDigits &&
              decimalComponents.fractionalDigits[reduceDigits] >= '5') {
            roundUp = true;
          }
          decimalComponents.fractionalDigits = std::string_view(
              decimalComponents.fractionalDigits.data(),
              std::min(reduceDigits, fractionSize));
          parsedScale -=
              fractionSize - decimalComponents.fractionalDigits.size();
          // parsedPrecision -=
          //     fractionSize - decimalComponents.fractionalDigits.size();
        }
      }
    }

    int128_t out = 0;
    if (auto status = parseHugeInt(decimalComponents, out); !status.ok()) {
      return status;
    }

    if (roundUp) {
      bool overflow = __builtin_add_overflow(out, 1, &out);
      if (UNLIKELY(overflow)) {
        return Status::UserError("Value too large.");
      }
    }

    if (out >= DecimalUtil::kPowersOfTen
                   [DecimalType<TypeKind::HUGEINT>::kMaxPrecision]) {
      return Status::UserError("Value too large.");
    }
    out = out * decimalComponents.sign;

    if (parsedScale < 0) {
      /// Force the scale to be zero, to avoid negative scales (due to
      /// compatibility issues with external systems such as databases).
      if (-parsedScale + toScale > LongDecimalType::kMaxPrecision) {
        return Status::UserError("Value too large.");
      }

      bool overflow = __builtin_mul_overflow(
          out, DecimalUtil::getPowersOfTen(-parsedScale + toScale), &out);
      if (UNLIKELY(overflow)) {
        return Status::UserError("Value too large.");
      }
      parsedPrecision -= parsedScale;
      parsedScale = toScale;
    }

    const auto status = DecimalUtil::rescaleWithRoundUp<int128_t, T>(
        out,
        std::min((uint8_t)parsedPrecision, LongDecimalType::kMaxPrecision),
        parsedScale,
        toPrecision,
        toScale,
        decimalValue);
    if (!status.ok()) {
      return Status::UserError("Value too large.");
    }
    return status;
  }

}; // DecimalUtil
} // namespace bytedance::bolt
