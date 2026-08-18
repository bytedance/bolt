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

#include <folly/dynamic.h>
#include <sstream>
#include <string>
#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/Portability.h"
#include "bolt/type/StringView.h"

#if defined(BOLT_CLANG_LIBSTDCXX_INT128_COMPAT)
#include <folly/hash/Hash.h>
#endif

#pragma once
namespace bytedance::bolt {

using int128_t = __int128_t;
using uint128_t = __uint128_t;

class HugeInt {
 public:
  static constexpr FOLLY_ALWAYS_INLINE int128_t
  build(uint64_t hi, uint64_t lo) {
    // GCC does not allow left shift negative value.
    return (static_cast<__uint128_t>(hi) << 64) | lo;
  }

  static FOLLY_ALWAYS_INLINE int128_t fromString(const std::string& str) {
    int128_t value = 0;
    // consider if str is negative
    bool isNegative = false;
    size_t startPos = 0;

    if (!str.empty() && str[0] == '-') {
      isNegative = true;
      startPos = 1;
    }

    for (size_t i = startPos; i < str.length(); ++i) {
      if (str[i] < '0' || str[i] > '9') {
        throw std::invalid_argument("Invalid character in string: " + str);
      }
      value = value * 10 + (str[i] - '0');
    }

    if (isNegative) {
      value = -value;
    }

    return value;
  }

  static constexpr FOLLY_ALWAYS_INLINE uint64_t lower(int128_t value) {
    return static_cast<uint64_t>(value);
  }

  static constexpr FOLLY_ALWAYS_INLINE uint64_t upper(int128_t value) {
    return static_cast<uint64_t>(value >> 64);
  }

#if defined(BOLT_CLANG_LIBSTDCXX_INT128_COMPAT)
  static FOLLY_ALWAYS_INLINE size_t hash(int128_t value) {
    auto unsignedValue = static_cast<uint128_t>(value);
    return folly::hash::hash_128_to_64(
        static_cast<uint64_t>(unsignedValue >> 64),
        static_cast<uint64_t>(unsignedValue));
  }

  static FOLLY_ALWAYS_INLINE size_t hash(uint128_t value) {
    return folly::hash::hash_128_to_64(
        static_cast<uint64_t>(value >> 64), static_cast<uint64_t>(value));
  }
#endif

  static FOLLY_ALWAYS_INLINE int128_t deserialize(const char* serializedData) {
    int128_t value;
    memcpy(&value, serializedData, sizeof(int128_t));
    return value;
  }

  static FOLLY_ALWAYS_INLINE void serialize(
      const int128_t& value,
      char* serializedData) {
    memcpy(serializedData, &value, sizeof(int128_t));
  }

  static int128_t parse(const std::string& str);
};

} // namespace bytedance::bolt

namespace std {
string to_string(__int128_t x);
std::ostream& operator<<(std::ostream& os, const __int128 i) noexcept;
} // namespace std
