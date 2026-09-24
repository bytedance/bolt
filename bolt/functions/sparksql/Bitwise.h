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

#include "bolt/functions/Macros.h"
namespace bytedance::bolt::functions::sparksql {

template <typename T>
struct BitwiseAndFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a, TInput b) {
    result = a & b;
  }
};

template <typename T>
struct BitwiseOrFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a, TInput b) {
    result = a | b;
  }
};

template <typename T>
struct BitwiseXorFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a, TInput b) {
    result = a ^ b;
  }
};

template <typename T>
struct BitwiseNotFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = ~a;
  }
};

template <typename T>
struct ShiftLeftFunction {
  template <typename TInput1, typename TInput2>
  FOLLY_ALWAYS_INLINE void call(TInput1& result, TInput1 a, TInput2 b) {
    if constexpr (std::is_same_v<TInput1, int32_t>) {
      if (b < 0) {
        b = b % 32 + 32;
      }
      if (b >= 32) {
        b = b % 32;
      }
    }
    if constexpr (std::is_same_v<TInput1, int64_t>) {
      if (b < 0) {
        b = b % 64 + 64;
      }
      if (b >= 64) {
        b = b % 64;
      }
    }
    result = a << b;
  }
};

template <typename T>
struct ShiftRightFunction {
  template <typename TInput1, typename TInput2>
  FOLLY_ALWAYS_INLINE void call(TInput1& result, TInput1 a, TInput2 b) {
    if constexpr (std::is_same_v<TInput1, int32_t>) {
      if (b < 0) {
        b = b % 32 + 32;
      }
      if (b >= 32) {
        b = b % 32;
      }
    }
    if constexpr (std::is_same_v<TInput1, int64_t>) {
      if (b < 0) {
        b = b % 64 + 64;
      }
      if (b >= 64) {
        b = b % 64;
      }
    }
    result = a >> b;
  }
};

template <typename T>
struct ShiftRightUnsignedFunction {
  template <typename TInput1, typename TInput2>
  FOLLY_ALWAYS_INLINE void call(TInput1& result, TInput1 a, TInput2 b) {
    if constexpr (std::is_same_v<TInput1, int32_t>) {
      if (b < 0) {
        b = b % 32 + 32;
      }
      if (b >= 32) {
        b = b % 32;
      }
    }
    if constexpr (std::is_same_v<TInput1, int64_t>) {
      if (b < 0) {
        b = b % 64 + 64;
      }
      if (b >= 64) {
        b = b % 64;
      }
    }
    // Java's >>> : shift the bit pattern, not the signed value.
    using TUnsigned = std::make_unsigned_t<TInput1>;
    result = static_cast<TInput1>(static_cast<TUnsigned>(a) >> b);
  }
};

template <typename T>
struct BitCountFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(int32_t& result, TInput num) {
    constexpr int kMaxBits = sizeof(TInput) * CHAR_BIT;
    auto value = static_cast<uint64_t>(num);
    result = bits::countBits(&value, 0, kMaxBits);
  }
};

template <typename T>
struct BitDaysCountFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(int32_t& result, const TInput& binaryDays) {
    if (UNLIKELY(binaryDays.empty())) {
      result = 0;
      return;
    }
    int32_t count = 0;
    for (auto element : binaryDays) {
      count += __builtin_popcountll(element.value_or(0)) - 1;
    }
    result = count;
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(int32_t& result, const TInput& binaryDays, int32_t lastNDays) {
    if (UNLIKELY(binaryDays.empty())) {
      result = 0;
      return;
    }

    if (UNLIKELY(lastNDays < 1 || lastNDays > 60)) {
      BOLT_USER_FAIL("lastNDays {} should be within 1 and 60", lastNDays);
    }
    int64_t lastNDaysBits = (1LL << lastNDays) - 1;
    size_t size = binaryDays.size();
    if (UNLIKELY(!binaryDays[size - 1].has_value())) {
      BOLT_USER_FAIL("The last element of binaryDays should not be NULL.");
    }
    int64_t num = binaryDays[size - 1].value();
    // Check if the list has only one element
    if (size == 1) {
      num -=
          (1LL
           << (63 -
               __builtin_clzll(
                   num))); // Equivalent to Long.highestOneBit(num) in Java
    }
    // Perform bitwise AND with lastNDaysBits
    num &= lastNDaysBits;

    // Return the bit count of the result
    result = __builtin_popcountll(num);
  }
};

template <typename T>
struct BitGetFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(int8_t& result, TInput num, int32_t pos) {
    constexpr int kMaxBits = sizeof(TInput) * CHAR_BIT;
    BOLT_USER_CHECK_GE(
        pos,
        0,
        "The value of 'pos' argument must be greater than or equal to zero.");
    BOLT_USER_CHECK_LT(
        pos,
        kMaxBits,
        "The value of 'pos' argument must not exceed the number of bits in 'x' - 1.");
    result = (num >> pos) & 1;
  }
};
} // namespace bytedance::bolt::functions::sparksql
