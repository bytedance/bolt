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

#include <bit>
#include <cstdint>
#include <cstring>
#include <optional>
#include <type_traits>

namespace bytedance::bolt::exec::radixsort {

static_assert(
    std::endian::native == std::endian::little ||
    std::endian::native == std::endian::big);

enum class NativeEndianness : uint8_t {
  kLittle = 1,
  kBig = 2,
};

template <typename T>
T loadUnaligned(const void* source) {
  static_assert(std::is_trivially_copyable_v<T>);
  T value;
  std::memcpy(&value, source, sizeof(T));
  return value;
}

template <typename T>
void storeUnaligned(void* destination, const T& value) {
  static_assert(std::is_trivially_copyable_v<T>);
  std::memcpy(destination, &value, sizeof(T));
}

template <typename T>
std::optional<T> checkedAdd(T left, T right) {
  static_assert(std::is_integral_v<T>);
  T result;
  if (__builtin_add_overflow(left, right, &result)) {
    return std::nullopt;
  }
  return result;
}

template <typename T>
std::optional<T> checkedMultiply(T left, T right) {
  static_assert(std::is_integral_v<T>);
  T result;
  if (__builtin_mul_overflow(left, right, &result)) {
    return std::nullopt;
  }
  return result;
}

template <typename T>
std::optional<T> checkedAlignUp(T value, T alignment) {
  static_assert(std::is_unsigned_v<T>);
  if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
    return std::nullopt;
  }
  auto adjusted = checkedAdd(value, alignment - 1);
  if (!adjusted.has_value()) {
    return std::nullopt;
  }
  return *adjusted & ~(alignment - 1);
}

constexpr NativeEndianness nativeEndianness() {
  if constexpr (std::endian::native == std::endian::little) {
    return NativeEndianness::kLittle;
  }
  return NativeEndianness::kBig;
}

template <typename T>
constexpr T byteSwap(T value) {
  static_assert(std::is_unsigned_v<T>);
  if constexpr (sizeof(T) == 1) {
    return value;
  } else if constexpr (sizeof(T) == 2) {
    return __builtin_bswap16(value);
  } else if constexpr (sizeof(T) == 4) {
    return __builtin_bswap32(value);
  } else {
    static_assert(sizeof(T) == 8);
    return __builtin_bswap64(value);
  }
}

template <typename T>
constexpr T toBigEndian(T value) {
  static_assert(std::is_unsigned_v<T>);
  if constexpr (std::endian::native == std::endian::little) {
    return byteSwap(value);
  }
  return value;
}

template <typename T>
constexpr T fromBigEndian(T value) {
  return toBigEndian(value);
}

inline bool isValidRecordRelativeRange(
    uint64_t recordSize,
    uint64_t offset,
    uint64_t length,
    uint64_t minimumOffset = 1,
    bool allowZeroOffset = false) {
  if (offset == 0) {
    return allowZeroOffset && length == 0;
  }
  if (offset < minimumOffset || offset >= recordSize) {
    return false;
  }
  auto end = checkedAdd(offset, length);
  return end.has_value() && *end <= recordSize;
}

} // namespace bytedance::bolt::exec::radixsort
