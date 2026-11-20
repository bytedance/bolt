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

#include "bolt/type/FloatingDecimal.h"
#include <boost/multiprecision/detail/default_ops.hpp>
#include <common/base/Exceptions.h>
#include <fmt/core.h>
#include <type/BigDecimal.h>
#include <type/StringView.h>
#include <array>
#include <sstream>
#include <string>
#include "bolt/type/Conversions.h"
#include "bolt/type/DecimalUtil.h"
using namespace bytedance::bolt::util;
namespace bytedance::bolt {

namespace {

using BigInt = boost::multiprecision::cpp_int;

constexpr int32_t kMaxCachedPowerOfFive = 340;

const std::array<BigInt, kMaxCachedPowerOfFive + 1>& powersOfFive() {
  static const auto powers = [] {
    std::array<BigInt, kMaxCachedPowerOfFive + 1> values;
    values[0] = 1;
    for (int32_t i = 1; i <= kMaxCachedPowerOfFive; ++i) {
      values[i] = values[i - 1] * 5;
    }
    return values;
  }();
  return powers;
}

std::string valueToString(int128_t value, int32_t& scale, int32_t& nDigits) {
  while (value > 0 && value % 10 == 0) {
    scale--;
    value /= 10;
  }
  std::string valueStr;
  valueStr = std::to_string(value);
  nDigits = valueStr.size();
  return valueStr;
}

int countBits(int64_t v) {
  if (v == 0L) {
    return 0;
  }

  constexpr int64_t highByte = 0xff00000000000000L;
  constexpr int64_t lowByte = ~highByte;
  while ((v & highByte) == 0L) {
    v <<= 8;
  }
  while (v > 0L) {
    v <<= 1;
  }

  int n = 0;
  while ((v & lowByte) != 0L) {
    v <<= 8;
    n += 8;
  }
  while (v != 0L) {
    v <<= 1;
    n++;
  }
  return n;
}

} // namespace

boost::multiprecision::cpp_int FloatingDecimal::big5pow(int p) {
  BOLT_USER_CHECK(p >= 0);

  if (p <= kMaxCachedPowerOfFive) {
    return powersOfFive()[p];
  }

  return boost::multiprecision::pow(BigInt{5}, p);
}

boost::multiprecision::cpp_int
FloatingDecimal::doubleToBigInt(double dVal, int& bigIntExp, int& bigIntNBits) {
  int64_t lbits = *reinterpret_cast<int64_t*>(&dVal) & ~signMask;
  int binexp = static_cast<int>(lbits >> expShift);
  lbits &= fractMask;
  if (binexp > 0) {
    lbits |= fractHOB;
  } else {
    binexp += 1;
    while ((lbits & fractHOB) == 0L) {
      lbits <<= 1;
      binexp -= 1;
    }
  }
  binexp -= expBias;
  int nbits = countBits(lbits);
  /*
   * We now know where the high-order 1 bit is,
   * and we know how many there are.
   */
  int lowOrderZeros = expShift + 1 - nbits;
  lbits >>= lowOrderZeros;

  bigIntExp = binexp + 1 - nbits;
  bigIntNBits = nbits;
  return boost::multiprecision::cpp_int(lbits);
}

double FloatingDecimal::ulp(double dVal, bool subtracting) {
  int64_t lbits = *reinterpret_cast<int64_t*>(&dVal) & ~signMask;
  int32_t binexp = static_cast<int32_t>(lbits >> expShift);
  double ulpval;
  if (subtracting && (binexp >= expShift) && ((lbits & fractMask) == 0L)) {
    // for subtraction from normalized, powers of 2,
    // use next-smaller exponent
    binexp -= 1;
  }
  if (binexp > expShift) {
    int64_t lVal = static_cast<int64_t>(binexp - expShift) << expShift;
    ulpval = *reinterpret_cast<double*>(&lVal);
  } else if (binexp == 0) {
    ulpval = DBL_MIN;
  } else {
    int64_t lVal = 1L << (binexp - 1);
    ulpval = *reinterpret_cast<double*>(&lVal);
  }
  if (subtracting) {
    ulpval = -ulpval;
  }
  return ulpval;
}

std::optional<double> FloatingDecimal::toDoubleFromValue(
    int128_t value,
    int32_t scale) {
  double sign = 1;
  if (value < 0) {
    value = -value;
    sign = -1;
  }
  int32_t nDigits = 0;
  std::string valueStr = valueToString(value, scale, nDigits);
  int32_t decExponent = nDigits - scale;
  auto res = doubleValue(valueStr, nDigits, decExponent);
  if (res.has_value()) {
    return *res * sign;
  }
  return {};
}

std::optional<double> FloatingDecimal::doubleValue(
    std::string& value,
    int32_t nDigits,
    int32_t decExponent) {
  int32_t kDigits = std::min(nDigits, maxDecimalDigits + 1);
  int32_t iDigits = std::min(kDigits, intDecimalDigits);
  int64_t trunValue = value[0] - '0';
  for (int i = 1; i < kDigits; i++) {
    trunValue = trunValue * 10 + value[i] - '0';
  }
  int32_t exp = decExponent - kDigits;
  bool nullOutput = false;
  auto trunOutput = bytedance::bolt::util::
      Converter<TypeKind::DOUBLE, void, DefaultCastPolicy>::cast(
          trunValue, &nullOutput);
  if (nullOutput) {
    return {};
  }

  if (nDigits <= maxDecimalDigits) {
    if (exp == 0 || trunOutput == 0.0f) {
      return trunOutput;
    }
    if (exp > 0) {
      if (exp <= maxSmallTen + maxDecimalDigits - kDigits) {
        return trunOutput * DecimalUtil::getPowersOfTen(exp);
      }
    } else if (exp >= -maxSmallTen) {
      return trunOutput / DecimalUtil::getPowersOfTen(-exp);
    }
  }
  if (exp > 0) {
    if ((exp & 15) != 0) {
      trunOutput *= DecimalUtil::getPowersOfTen(exp & 15);
    }
    if (exp >= 16) {
      int j = 0;
      exp >>= 4;
      for (; exp > 1; j++, exp >>= 1) {
        if ((exp & 1) != 0) {
          trunOutput *= big10pow[j];
        }
      }
      auto t = trunOutput * big10pow[j];
      if (std::isinf(t)) {
        t = trunOutput / 2.0;
        t *= big10pow[j];
        if (std::isinf(t)) {
          return t;
        }
        t = DBL_MAX;
      }
      trunOutput = t;
    }
  } else if (exp < 0) {
    exp = -exp;
    if ((exp & 15) != 0) {
      trunOutput /= DecimalUtil::getPowersOfTen(exp & 15);
    }
    if (exp >= 16) {
      exp >>= 4;
      int j = 0;
      for (; exp > 1; j++, exp >>= 1) {
        if ((exp & 1) != 0) {
          trunOutput *= tiny10pow[j];
        }
      }
      auto t = trunOutput * tiny10pow[j];
      if (t == 0.0) {
        t = trunOutput * 2.0;
        t *= tiny10pow[j];
        if (t == 0.0) {
          return t;
        }
        t = DBL_MIN;
      }
      trunOutput = t;
    }
  }

  boost::multiprecision::cpp_int bigD0(value);
  // FDBigInt bigD0{trunValue, value, kDigits, nDigits};
  exp = decExponent - nDigits;

  while (true) {
    /* AS A SIDE EFFECT, THIS METHOD WILL SET THE INSTANCE VARIABLES
     * bigIntExp and bigIntNBits
     */
    int32_t bigIntExp;
    int32_t bigIntNBits;
    auto bigB = doubleToBigInt(trunOutput, bigIntExp, bigIntNBits);
    // FDBigInt bigB = doubleToBigInt(trunOutput, bigIntExp, bigIntNBits);

    /*
     * Scale bigD, bigB appropriately for
     * big-integer operations.
     * Naively, we multiply by powers of ten
     * and powers of two. What we actually do
     * is keep track of the powers of 5 and
     * powers of 2 we would use, then factor out
     * common divisors before doing the work.
     */
    int B2, B5; // powers of 2, 5 in bigB
    int D2, D5; // powers of 2, 5 in bigD
    int Ulp2; // powers of 2 in halfUlp.
    if (exp >= 0) {
      B2 = B5 = 0;
      D2 = D5 = exp;
    } else {
      B2 = B5 = -exp;
      D2 = D5 = 0;
    }
    if (bigIntExp >= 0) {
      B2 += bigIntExp;
    } else {
      D2 -= bigIntExp;
    }
    Ulp2 = B2;
    // shift bigB and bigD left by a number s. t.
    // halfUlp is still an integer.
    int hulpbias;
    if (bigIntExp + bigIntNBits <= -expBias + 1) {
      // This is going to be a denormalized number
      // (if not actually zero).
      // half an ULP is at 2^-(expBias+expShift+1)
      hulpbias = bigIntExp + expBias + expShift;
    } else {
      hulpbias = expShift + 2 - bigIntNBits;
    }
    B2 += hulpbias;
    D2 += hulpbias;
    // if there are common factors of 2, we might just as well
    // factor them out, as they add nothing useful.
    int common2 = std::min(B2, std::min(D2, Ulp2));
    B2 -= common2;
    D2 -= common2;
    Ulp2 -= common2;
    // do multiplications by powers of 5 and 2
    bigB = (bigB * big5pow(B5)) << B2;
    auto bigD = (bigD0 * big5pow(D5)) << D2;

    //
    // to recap:
    // bigB is the scaled-big-int version of our floating-point
    // candidate.
    // bigD is the scaled-big-int version of the exact value
    // as we understand it.
    // halfUlp is 1/2 an ulp of bigB, except for special cases
    // of exact powers of 2
    //
    // the plan is to compare bigB with bigD, and if the difference
    // is less than halfUlp, then we're satisfied. Otherwise,
    // use the ratio of difference to halfUlp to calculate a fudge
    // factor to add to the floating value, then go 'round again.
    //
    boost::multiprecision::cpp_int diff;
    boost::multiprecision::cpp_int cmpResult;
    bool overvalue;
    if ((cmpResult = bigB - bigD) > 0) {
      overvalue = true; // our candidate is too big.
      diff = bigB - bigD;
      if ((bigIntNBits == 1) && (bigIntExp > -expBias + 1)) {
        // candidate is a normalized exact power of 2 and
        // is too big. We will be subtracting.
        // For our purposes, ulp is the ulp of the
        // next smaller range.
        Ulp2 -= 1;
        if (Ulp2 < 0) {
          // rats. Cannot de-scale ulp this far.
          // must scale diff in other direction.
          Ulp2 = 0;
          diff <<= 1;
          // diff.lshiftMe(1);
        }
      }
    } else if (cmpResult < 0) {
      overvalue = false; // our candidate is too small.
      diff = bigD - bigB;
    } else {
      // the candidate is exactly right!
      // this happens with surprising frequency
      break;
    }
    boost::multiprecision::cpp_int halfUlp =
        boost::multiprecision::pow(boost::multiprecision::cpp_int(5), B5) *
        boost::multiprecision::pow(boost::multiprecision::cpp_int(2), Ulp2);
    if ((cmpResult = diff - halfUlp) < 0) {
      // difference is small.
      // this is close enough
      break;
    } else if (cmpResult == 0) {
      // difference is exactly half an ULP
      // round to some other value maybe, then finish
      trunOutput += 0.5 * ulp(trunOutput, overvalue);
      // should check for bigIntNBits == 1 here??
      break;
    } else {
      // difference is non-trivial.
      // could scale addend by ratio of difference to
      // halfUlp here, if we bothered to compute that difference.
      // Most of the time ( I hope ) it is about 1 anyway.
      trunOutput += ulp(trunOutput, overvalue);
      if (trunOutput == 0.0 ||
          trunOutput == std::numeric_limits<double>::infinity()) {
        break; // oops. Fell off end of range.
      }
      continue; // try again.
    }
  }
  return trunOutput;
}

std::optional<float> FloatingDecimal::toFloatFromValue(
    int128_t value,
    int32_t scale) {
  float sign = 1;
  if (value < 0) {
    value = -value;
    sign = -1;
  }
  int32_t nDigits = 0;
  std::string valueStr = valueToString(value, scale, nDigits);
  int32_t decExponent = nDigits - scale;
  auto res = floatValue(valueStr, nDigits, decExponent);
  if (res.has_value()) {
    return *res * sign;
  }
  return {};
}

std::optional<float> FloatingDecimal::floatValue(
    std::string& value,
    int32_t nDigits,
    int32_t decExponent) {
  int32_t kDigits = std::min(nDigits, singleMaxDecimalDigits + 1);
  int64_t trunValue = value[0] - '0';
  for (int i = 1; i < kDigits; i++) {
    trunValue = trunValue * 10 + value[i] - '0';
  }
  int32_t exp = decExponent - kDigits;
  bool nullOutput = false;
  auto trunOutput = bytedance::bolt::util::
      Converter<TypeKind::REAL, void, DefaultCastPolicy>::cast(
          trunValue, &nullOutput);
  if (nullOutput) {
    return {};
  }

  if (nDigits <= singleMaxDecimalDigits) {
    if (exp == 0 || trunOutput == 0.0f) {
      return trunOutput;
    }
    if (exp > 0) {
      return trunOutput * DecimalUtil::getPowersOfTen(exp);
    } else if (exp >= -maxSingleSmallTen) {
      return trunOutput / DecimalUtil::getPowersOfTen(-exp);
    }
  } else if (
      (decExponent >= nDigits) &&
      (nDigits + decExponent <= FloatingDecimal::maxDecimalDigits)) {
    auto doubleOutput = doubleValue(value, nDigits, decExponent);
    if (doubleOutput.has_value()) {
      return static_cast<float>(*doubleOutput);
    }
    return {};
  }
  if (decExponent > FloatingDecimal::singleMaxDecimalExponent + 1) {
    return std::numeric_limits<float>::infinity();
  }
  if (decExponent < FloatingDecimal::singleMinDecimalExponent - 1) {
    return 0.0f;
  }
  auto doubleOutput = doubleValue(value, nDigits, decExponent);
  if (doubleOutput.has_value()) {
    return static_cast<float>(*doubleOutput);
  }
  return {};
}

} // namespace bytedance::bolt
