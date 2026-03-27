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
#include "bolt/common/base/Exceptions.h"
#include "bolt/type/Conversions.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt {

class BigDecimal {
 public:
  template <typename Tfloat>
  BigDecimal(Tfloat val) {
    static_assert(
        std::is_same_v<Tfloat, float> || std::is_same_v<Tfloat, double>);
    BOLT_USER_CHECK(std::isfinite(val), "Infinite or NaN");
    if (val == 0) {
      isZero = true;
      return;
    }
    if (val < 0) {
      sign = -1;
      val = -val;
    }
    bool nullOutput = false;
    auto output =
        util::Converter<TypeKind::VARCHAR, void, util::DefaultCastPolicy>::cast<
            double>(val, &nullOutput);
    if (nullOutput) {
      BOLT_FAIL("Invalid data");
    }

    int dotIndex = -1;
    int expIndex = -1;

    if (output.find('e') != std::string::npos) {
      expIndex = output.find('e');
    } else if (output.find('E') != std::string::npos) {
      expIndex = output.find('E');
    }

    int size = output.size();
    int exponent = 0;
    if (expIndex != -1) {
      int pos = expIndex;
      ++pos;
      if (pos != size && output[pos] == '+') {
        ++pos;
      }
      auto tryExp = folly::tryTo<int32_t>(
          folly::StringPiece(output.data() + pos, size - pos));
      if (tryExp.hasError()) {
        BOLT_FAIL("Invalid data");
      }
      exponent = tryExp.value();
    }

    std::string intStr;
    if (output.find('.') != std::string::npos) {
      dotIndex = output.find('.');
      intStr = output.substr(0, dotIndex);
      scale = expIndex == -1 ? size - dotIndex - 1 : expIndex - dotIndex - 1;
      intStr += output.substr(dotIndex + 1, scale);
    } else {
      intStr = output.substr(0, expIndex);
    }
    int beginIndex = 0;
    while (intStr[beginIndex] == '0') {
      beginIndex++;
    }
    intVal = boost::multiprecision::cpp_int(intStr.substr(beginIndex));
    scale -= exponent;
    compactVal();
  }

  void setScale(int newScale);

  int getScale() {
    return scale;
  }

  double doubleValue();

  float floatValue();

  std::string toString();

  void compactVal();

  int128_t getCompactVal() {
    return intCompact;
  }

 private:
  int128_t intCompact = -1;
  int scale = 0;
  boost::multiprecision::cpp_int intVal;
  int sign = 1;
  int decExponent = 1;
  bool isZero = false;
};

} // namespace bytedance::bolt
