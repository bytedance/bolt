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

#include "bolt/type/BigDecimal.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/type/FloatingDecimal.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Type.h"

#include <string>
namespace bytedance::bolt {

void BigDecimal::compactVal() {
  if ((intVal >> 127) == 0) {
    intCompact = intVal.convert_to<int128_t>();
  } else {
    intCompact = -1;
  }
}

void BigDecimal::setScale(int newScale, RoundingMode mode) {
  if (scale == newScale || isZero) {
    return;
  }

  if (scale > newScale) {
    int changeScale = scale - newScale;
    boost::multiprecision::cpp_int scaleVal = boost::multiprecision::pow(
        boost::multiprecision::cpp_int(10), changeScale);
    boost::multiprecision::cpp_int remainder = intVal % scaleVal;
    intVal /= scaleVal;
    // intVal is the magnitude and sign is tracked separately, so incrementing
    // rounds away from zero, matching java.math.RoundingMode semantics.
    const boost::multiprecision::cpp_int half = scaleVal / 2;
    bool increment;
    if (mode == RoundingMode::kHalfEven) {
      increment = remainder > half || (remainder == half && intVal % 2 != 0);
    } else {
      increment = remainder >= half;
    }
    if (increment) {
      intVal++;
    }
    scale = newScale;
  }

  if (intVal == 0) {
    isZero = true;
    return;
  }

  compactVal();
}

double BigDecimal::doubleValue() {
  if (isZero) {
    return 0;
  }
  std::string decimalStr = toString();
  int32_t nDigits = decimalStr.size();
  auto res = FloatingDecimal::doubleValue(decimalStr, nDigits, decExponent);
  BOLT_USER_CHECK(res.has_value(), "should get double value");
  return *res * sign;
}

float BigDecimal::floatValue() {
  if (isZero) {
    return 0;
  }
  std::string decimalStr = toString();
  int32_t nDigits = decimalStr.size();
  auto res = FloatingDecimal::floatValue(decimalStr, nDigits, decExponent);
  BOLT_USER_CHECK(res.has_value(), "should get float value");
  return *res * static_cast<float>(sign);
}

std::string BigDecimal::toString() {
  if (intCompact != -1) {
    std::string valStr = std::to_string(intCompact);
    decExponent = valStr.size() - scale;
    return valStr;
  }
  std::string valStr{intVal.str()};
  decExponent = valStr.size() - scale;
  return valStr;
}

} // namespace bytedance::bolt
