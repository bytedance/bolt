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

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>

#include "bolt/core/QueryConfig.h"
#include "bolt/functions/Macros.h"
#include "bolt/functions/lib/string/StringCore.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::functions::flinksql {

namespace hash_code_detail {

FOLLY_ALWAYS_INLINE int32_t toJavaInt(uint32_t bits) {
  int32_t value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

FOLLY_ALWAYS_INLINE uint32_t combine(uint32_t hash, uint32_t unit) {
  return hash * 31u + unit;
}

FOLLY_ALWAYS_INLINE uint32_t rotl32(uint32_t value, int8_t distance) {
  return (value << distance) | (value >> (32 - distance));
}

FOLLY_ALWAYS_INLINE uint32_t mixK1(uint32_t k1) {
  k1 *= 0xcc9e2d51u;
  k1 = rotl32(k1, 15);
  k1 *= 0x1b873593u;
  return k1;
}

FOLLY_ALWAYS_INLINE uint32_t mixH1(uint32_t h1, uint32_t k1) {
  h1 ^= k1;
  h1 = rotl32(h1, 13);
  h1 = h1 * 5u + 0xe6546b64u;
  return h1;
}

FOLLY_ALWAYS_INLINE uint32_t fmix(uint32_t h1, uint32_t length) {
  h1 ^= length;
  h1 ^= h1 >> 16;
  h1 *= 0x85ebca6bu;
  h1 ^= h1 >> 13;
  h1 *= 0xc2b2ae35u;
  h1 ^= h1 >> 16;
  return h1;
}

FOLLY_ALWAYS_INLINE uint32_t hashLong(int64_t val) {
  const uint64_t bits = static_cast<uint64_t>(val);
  return static_cast<uint32_t>(bits ^ (bits >> 32));
}

FOLLY_ALWAYS_INLINE int32_t javaAbs(uint32_t bits) {
  if ((bits & 0x80000000u) == 0) {
    return static_cast<int32_t>(bits);
  }
  if (bits == 0x80000000u) {
    return std::numeric_limits<int32_t>::min();
  }
  return static_cast<int32_t>(0u - bits);
}

FOLLY_ALWAYS_INLINE uint32_t hashFloat(float val) {
  uint32_t bits;
  std::memcpy(&bits, &val, sizeof(bits));
  return std::isnan(val) ? 0x7fc00000u : bits;
}

FOLLY_ALWAYS_INLINE uint32_t hashDouble(double val) {
  uint64_t bits;
  std::memcpy(&bits, &val, sizeof(bits));
  if (std::isnan(val)) {
    bits = 0x7ff8000000000000ULL;
  }
  return static_cast<uint32_t>(bits ^ (bits >> 32));
}

FOLLY_ALWAYS_INLINE uint32_t hashString(StringView val) {
  uint32_t hash = 0;
  const char* p = val.data();
  const char* end = p + val.size();
  while (p < end) {
    const uint8_t byte = static_cast<uint8_t>(*p);
    if (byte < 0x80) {
      hash = combine(hash, byte);
      ++p;
      continue;
    }

    int32_t size = 0;
    const int32_t cp = stringCore::utf8proc_codepoint(p, end, &size);
    if (cp < 0) {
      hash = combine(hash, byte);
      ++p;
      continue;
    }
    if (cp <= 0xFFFF) {
      hash = combine(hash, static_cast<uint32_t>(cp));
    } else {
      const int32_t c = cp - 0x10000;
      hash = combine(hash, static_cast<uint32_t>(0xD800 + (c >> 10)));
      hash = combine(hash, static_cast<uint32_t>(0xDC00 + (c & 0x3FF)));
    }
    p += size;
  }
  return hash;
}

FOLLY_ALWAYS_INLINE uint32_t signedTailByte(char byte) {
  return static_cast<uint32_t>(
      static_cast<int32_t>(static_cast<int8_t>(static_cast<uint8_t>(byte))));
}

FOLLY_ALWAYS_INLINE uint32_t hashUnsafeBytes(StringView val) {
  constexpr uint32_t kDefaultSeed = 42u;
  uint32_t h1 = kDefaultSeed;
  const char* data = val.data();
  const uint32_t length = static_cast<uint32_t>(val.size());
  const uint32_t lengthAligned = length & 0xfffffffcu;

  for (uint32_t i = 0; i < lengthAligned; i += 4) {
    const auto b0 = static_cast<uint32_t>(static_cast<uint8_t>(data[i]));
    const auto b1 = static_cast<uint32_t>(static_cast<uint8_t>(data[i + 1]));
    const auto b2 = static_cast<uint32_t>(static_cast<uint8_t>(data[i + 2]));
    const auto b3 = static_cast<uint32_t>(static_cast<uint8_t>(data[i + 3]));
    h1 = mixH1(h1, mixK1(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)));
  }

  for (uint32_t i = lengthAligned; i < length; ++i) {
    h1 = mixH1(h1, mixK1(signedTailByte(data[i])));
  }
  return fmix(h1, length);
}

FOLLY_ALWAYS_INLINE uint32_t hashTimestamp(const Timestamp& val) {
  return combine(
      hashLong(val.toMillis()),
      static_cast<uint32_t>(val.getNanos() % 1'000'000));
}

FOLLY_ALWAYS_INLINE int32_t hashShortDecimal(int64_t value, int32_t scale) {
  const uint64_t magnitude = value < 0 ? 0u - static_cast<uint64_t>(value)
                                       : static_cast<uint64_t>(value);
  const uint32_t compactHash = static_cast<uint32_t>(
      static_cast<uint32_t>(magnitude >> 32) * 31u +
      static_cast<uint32_t>(magnitude));
  const uint32_t signedHash = value < 0 ? 0u - compactHash : compactHash;
  return toJavaInt(signedHash * 31u + static_cast<uint32_t>(scale));
}

FOLLY_ALWAYS_INLINE int32_t hashLongDecimal(int128_t value, int32_t scale) {
  const bool negative = value < 0;
  const __uint128_t magnitude = negative
      ? __uint128_t{0} - static_cast<__uint128_t>(value)
      : static_cast<__uint128_t>(value);

  uint32_t hash = 0;
  bool hasWord = false;
  for (int32_t shift = 96; shift >= 0; shift -= 32) {
    const auto word = static_cast<uint32_t>(magnitude >> shift);
    if (!hasWord && word == 0) {
      continue;
    }
    hasWord = true;
    hash = hash * 31u + word;
  }

  const uint32_t signedHash = negative ? 0u - hash : hash;
  return toJavaInt(signedHash * 31u + static_cast<uint32_t>(scale));
}

} // namespace hash_code_detail

/// Flink HASH_CODE(expr). Returns each type's Java-style hashCode as INT.
template <typename T>
struct FlinkHashCodeFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename... Args>
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig&,
      const Args*...) {
    if (inputTypes.empty()) {
      return;
    }
    isVarbinary_ = inputTypes[0]->isVarbinary();
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<int8_t>& val) {
    result = static_cast<int32_t>(val);
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<int16_t>& val) {
    result = static_cast<int32_t>(val);
  }

  // Also serves DATE inputs (DATE is physically int32 days-since-epoch, and
  // Java Integer.hashCode of the day count is the identity).
  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<int32_t>& val) {
    result = val;
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<int64_t>& val) {
    result = hash_code_detail::toJavaInt(hash_code_detail::hashLong(val));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<bool>& val) {
    result = val ? 1231 : 1237;
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<float>& val) {
    result = hash_code_detail::toJavaInt(hash_code_detail::hashFloat(val));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<double>& val) {
    result = hash_code_detail::toJavaInt(hash_code_detail::hashDouble(val));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Varchar>& val) {
    if (isVarbinary_) {
      result =
          hash_code_detail::toJavaInt(hash_code_detail::hashUnsafeBytes(val));
    } else {
      result = hash_code_detail::javaAbs(hash_code_detail::hashString(val));
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& val) {
    result = hash_code_detail::toJavaInt(hash_code_detail::hashTimestamp(val));
  }

 private:
  bool isVarbinary_{false};
};

} // namespace bytedance::bolt::functions::flinksql
