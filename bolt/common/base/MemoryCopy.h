/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include <folly/CPortability.h>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::memory {

/// Reads exactly sizeof(T) bytes in native byte order, without requiring source
/// alignment. The bytes must contain a valid representation of the POD type T.
template <typename T>
FOLLY_ALWAYS_INLINE T loadUnaligned(const void* source) {
  static_assert(std::is_trivial_v<T> && std::is_standard_layout_v<T>);
  T value;
  ::memcpy(&value, source, sizeof(T));
  return value;
}

/// Writes exactly sizeof(T) bytes in native byte order, without requiring
/// destination alignment.
template <typename T>
FOLLY_ALWAYS_INLINE void storeUnaligned(void* destination, T value) {
  static_assert(std::is_trivial_v<T> && std::is_standard_layout_v<T>);
  ::memcpy(destination, &value, sizeof(T));
}

/// Reads `bytes` (0..8) into the low bytes of a uint64_t (little-endian host
/// assumed) without reading past `source + bytes`.
FOLLY_ALWAYS_INLINE uint64_t loadLowBytes(const void* source, size_t bytes) {
  BOLT_DCHECK_LE(bytes, sizeof(uint64_t));
  uint64_t value = 0;
  if (bytes != 0) {
    ::memcpy(&value, source, bytes);
  }
  return value;
}

namespace detail {

template <size_t kWidth>
FOLLY_ALWAYS_INLINE void copyEnds(char* dst, const char* src, size_t bytes) {
  ::memcpy(dst, src, kWidth);
  ::memcpy(dst + bytes - kWidth, src + bytes - kWidth, kWidth);
}

} // namespace detail

/// Copies 0 <= bytes < 64 without accessing outside either range. The ranges
/// must not overlap; neither pointer needs alignment. The head/tail writes
/// may overlap each other but reproduce the same bytes. Zero bytes is a no-op.
FOLLY_ALWAYS_INLINE void
copyUnder64(void* destination, const void* source, size_t bytes) {
  BOLT_DCHECK_LT(bytes, 64);
  auto* dst = static_cast<char*>(destination);
  const auto* src = static_cast<const char*>(source);
  if (bytes >= 32) {
    detail::copyEnds<32>(dst, src, bytes);
  } else if (bytes >= 16) {
    detail::copyEnds<16>(dst, src, bytes);
  } else if (bytes >= 8) {
    detail::copyEnds<8>(dst, src, bytes);
  } else if (bytes >= 4) {
    detail::copyEnds<4>(dst, src, bytes);
  } else if (bytes >= 2) {
    detail::copyEnds<2>(dst, src, bytes);
  } else if (bytes != 0) {
    *dst = *src;
  }
}

/// Copies exactly 64 bytes to a 64-byte-aligned destination. Source alignment
/// is unrestricted. The ranges must not overlap. Uses two AVX2 non-temporal
/// stores, or a fixed-size memcpy when AVX2 is unavailable.
/// Returns whether non-temporal stores were issued. Does not fence: before
/// publishing the destination or allowing another thread to reuse it, callers
/// must order the stores with nonTemporalStoreFence() or equivalent existing
/// synchronization. Same-thread reads of ordinary WB memory need no fence.
FOLLY_ALWAYS_INLINE bool copyNonTemporalAligned(
    void* destination,
    const void* source) {
  BOLT_DCHECK_EQ(reinterpret_cast<uintptr_t>(destination) % 64, 0);
#if defined(__AVX2__)
  auto* dst = static_cast<char*>(destination);
  const auto* src = static_cast<const char*>(source);
  const auto first = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
  const auto second =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 32));
  _mm256_stream_si256(reinterpret_cast<__m256i*>(dst), first);
  _mm256_stream_si256(reinterpret_cast<__m256i*>(dst + 32), second);
  return true;
#else
  ::memcpy(destination, source, 64);
  return false;
#endif
}

/// Copies a positive multiple of 64 bytes with the same alignment,
/// non-overlap, return value, and completion requirements as the single-line
/// overload. Without AVX2 the whole copy uses memcpy.
FOLLY_ALWAYS_INLINE bool
copyNonTemporalAligned(void* destination, const void* source, size_t bytes) {
  BOLT_DCHECK_GT(bytes, 0);
  BOLT_DCHECK_EQ(bytes % 64, 0);
  BOLT_DCHECK_EQ(reinterpret_cast<uintptr_t>(destination) % 64, 0);
#if defined(__AVX2__)
  auto* dst = static_cast<char*>(destination);
  const auto* src = static_cast<const char*>(source);
  do {
    copyNonTemporalAligned(dst, src);
    dst += 64;
    src += 64;
    bytes -= 64;
  } while (bytes != 0);
  return true;
#else
  ::memcpy(destination, source, bytes);
  return false;
#endif
}

/// Copies disjoint ranges of arbitrary size and alignment. Only complete,
/// aligned 64-byte destination lines use non-temporal stores; any prefix and
/// tail use memcpy. Returns whether any non-temporal stores were issued, with
/// the same completion requirement as copyNonTemporalAligned(). Zero bytes
/// is a no-op. Without AVX2 the whole copy uses memcpy and returns false.
FOLLY_ALWAYS_INLINE bool
copyNonTemporal(void* destination, const void* source, size_t bytes) {
  auto* dst = static_cast<char*>(destination);
  const auto* src = static_cast<const char*>(source);
  bool streamed = false;
#if defined(__AVX2__)
  if (bytes >= 64) {
    const size_t prefix = -reinterpret_cast<uintptr_t>(dst) & 63;
    if (bytes >= prefix + 64) {
      if (prefix != 0) {
        ::memcpy(dst, src, prefix);
        dst += prefix;
        src += prefix;
        bytes -= prefix;
      }
      const size_t fullBytes = bytes & ~size_t{63};
      streamed = copyNonTemporalAligned(dst, src, fullBytes);
      dst += fullBytes;
      src += fullBytes;
      bytes -= fullBytes;
    }
  }
#endif
  if (bytes != 0) {
    ::memcpy(dst, src, bytes);
  }
  return streamed;
}

/// Orders this thread's outstanding non-temporal stores. On AVX2 builds this
/// emits SFENCE, a store fence, not a general load barrier. Cross-thread
/// publication still requires the caller's normal synchronization. No-op
/// when the copy helpers cannot issue non-temporal stores.
FOLLY_ALWAYS_INLINE void nonTemporalStoreFence() {
#if defined(__AVX2__)
  _mm_sfence();
#endif
}

} // namespace bytedance::bolt::memory
