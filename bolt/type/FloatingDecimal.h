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
#include <boost/multiprecision/cpp_int.hpp>
#include <cstdint>
#include "bolt/type/Type.h"
namespace bytedance::bolt {

class FloatingDecimal {
 public:
  static constexpr double big10pow[] = {1e16, 1e32, 1e64, 1e128, 1e256};
  static constexpr double tiny10pow[] = {1e-16, 1e-32, 1e-64, 1e-128, 1e-256};
  static constexpr int64_t mask = 0xffffffffL;
  static constexpr int64_t signMask = 0x8000000000000000L;
  static constexpr int64_t expMask = 0x7ff0000000000000L;
  static constexpr int64_t fractMask = ~(signMask | expMask);
  static constexpr int32_t expShift = 52;
  static constexpr int32_t expBias = 1023;
  static constexpr int64_t fractHOB = 1L << expShift; // assumed High-Order bit

  static constexpr int32_t maxDecimalDigits = 15;
  static constexpr int32_t singleMaxDecimalDigits = 7;
  static constexpr int32_t singleMaxDecimalExponent = 38;
  static constexpr int32_t singleMinDecimalExponent = -45;
  static constexpr int32_t intDecimalDigits = 9;
  static constexpr int32_t maxSmallTen = 22;
  static constexpr int32_t maxSingleSmallTen = 10;
  static std::optional<double> toDoubleFromValue(int128_t value, int32_t scale);

  static std::optional<double>
  doubleValue(std::string& value, int32_t nDigits, int32_t decExponent);

  static std::optional<float> toFloatFromValue(int128_t value, int32_t scale);

  static std::optional<float>
  floatValue(std::string& value, int32_t nDigits, int32_t decExponent);

  static boost::multiprecision::cpp_int
  doubleToBigInt(double dVal, int& bigIntExp, int& bigIntNBits);

  static boost::multiprecision::cpp_int big5pow(int p);

  static double ulp(double dVal, bool subtracting);
};

} // namespace bytedance::bolt
