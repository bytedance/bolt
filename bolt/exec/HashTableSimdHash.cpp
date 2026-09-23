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

#include "bolt/exec/HashTableSimd.h"

#include "bolt/common/base/SimdUtil.h"
#include "bolt/exec/VectorHasher.h"

#include <folly/Bits.h>

#include <bit>
#include <cstring>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace bytedance::bolt::exec::hash_table_simd {

namespace {

#if defined(__x86_64__) || defined(__i386__)
// Load 4 int64_t values from 4 independent addresses into __m256i.
// Uses 4 scalar loads (OOO-parallel) instead of _mm256_i64gather_epi64
// which serializes loads internally, negating MLP benefits.
FOLLY_ALWAYS_INLINE __m256i
load4x64(const char* p0, const char* p1, const char* p2, const char* p3) {
  return _mm256_set_epi64x(
      folly::loadUnaligned<int64_t>(p3),
      folly::loadUnaligned<int64_t>(p2),
      folly::loadUnaligned<int64_t>(p1),
      folly::loadUnaligned<int64_t>(p0));
}
#endif

#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#define BOLT_HASH_TABLE_SIMD_NO_MSAN __attribute__((no_sanitize("memory")))
#endif
#endif

#ifndef BOLT_HASH_TABLE_SIMD_NO_MSAN
#define BOLT_HASH_TABLE_SIMD_NO_MSAN
#endif

#if defined(__x86_64__) || defined(__i386__)
// Must produce bit-identical results to folly::hash::jenkins_rev_mix32.
FOLLY_ALWAYS_INLINE __m128i jenkinsRevMix32Sse(__m128i key) {
  key = _mm_add_epi32(key, _mm_slli_epi32(key, 12));
  key = _mm_xor_si128(key, _mm_srli_epi32(key, 22));
  key = _mm_add_epi32(key, _mm_slli_epi32(key, 4));
  key = _mm_xor_si128(key, _mm_srli_epi32(key, 9));
  key = _mm_add_epi32(key, _mm_slli_epi32(key, 10));
  key = _mm_xor_si128(key, _mm_srli_epi32(key, 2));
  key = _mm_add_epi32(key, _mm_slli_epi32(key, 7));
  key = _mm_add_epi32(key, _mm_slli_epi32(key, 12));
  return key;
}
#endif

} // namespace

#if defined(__x86_64__) || defined(__i386__)

#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
static inline __m256i
twangMix64Avx2(__m256i key) {
  auto allOnes = _mm256_set1_epi64x(-1LL);
  auto shifted = _mm256_slli_epi64(key, 21);
  key = _mm256_add_epi64(_mm256_xor_si256(key, allOnes), shifted);
  key = _mm256_xor_si256(key, _mm256_srli_epi64(key, 24));
  key = _mm256_add_epi64(
      _mm256_add_epi64(key, _mm256_slli_epi64(key, 3)),
      _mm256_slli_epi64(key, 8));
  key = _mm256_xor_si256(key, _mm256_srli_epi64(key, 14));
  key = _mm256_add_epi64(
      _mm256_add_epi64(key, _mm256_slli_epi64(key, 2)),
      _mm256_slli_epi64(key, 4));
  key = _mm256_xor_si256(key, _mm256_srli_epi64(key, 28));
  key = _mm256_add_epi64(key, _mm256_slli_epi64(key, 31));
  return key;
}

FOLLY_ALWAYS_INLINE uint64_t hashInt64Bits(uint64_t value) {
  return folly::hasher<int64_t>()(std::bit_cast<int64_t>(value));
}

// Matches folly::hash::jenkins_rev_mix32 used by folly::hasher<int32_t>.
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
static inline __m256i
jenkinsRevMix32Avx2(__m256i key) {
  key = _mm256_add_epi32(key, _mm256_slli_epi32(key, 12));
  key = _mm256_xor_si256(key, _mm256_srli_epi32(key, 22));
  key = _mm256_add_epi32(key, _mm256_slli_epi32(key, 4));
  key = _mm256_xor_si256(key, _mm256_srli_epi32(key, 9));
  key = _mm256_add_epi32(key, _mm256_slli_epi32(key, 10));
  key = _mm256_xor_si256(key, _mm256_srli_epi32(key, 2));
  key = _mm256_add_epi32(key, _mm256_slli_epi32(key, 7));
  key = _mm256_add_epi32(key, _mm256_slli_epi32(key, 12));
  return key;
}

// a*b = (a_lo*b_lo) + ((a_lo*bHigh + aHigh*b_lo) << 32)
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
static inline __m256i
multiply64Avx2(__m256i a, __m256i b) {
  auto lowLow = _mm256_mul_epu32(a, b);
  auto aHigh = _mm256_srli_epi64(a, 32);
  auto bHigh = _mm256_srli_epi64(b, 32);
  auto lowHigh = _mm256_mul_epu32(a, bHigh);
  auto highLow = _mm256_mul_epu32(aHigh, b);
  auto cross = _mm256_add_epi64(lowHigh, highLow);
  auto shiftedCross = _mm256_slli_epi64(cross, 32);
  return _mm256_add_epi64(lowLow, shiftedCross);
}

// hashMix(upper, lower) = {
//   a = (lower ^ upper) * kMul; a ^= a >> 47;
//   b = (upper ^ a) * kMul; b ^= b >> 47; b *= kMul;
//   return b;
// }
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
static inline __m256i
hashMixAvx2(__m256i upper, __m256i lower) {
  auto kMul = _mm256_set1_epi64x(0x9ddfea08eb382d69ULL);
  auto a = multiply64Avx2(_mm256_xor_si256(lower, upper), kMul);
  a = _mm256_xor_si256(a, _mm256_srli_epi64(a, 47));
  auto b = multiply64Avx2(_mm256_xor_si256(upper, a), kMul);
  b = _mm256_xor_si256(b, _mm256_srli_epi64(b, 47));
  b = multiply64Avx2(b, kMul);
  return b;
}

#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
static inline __m256i
expandBits4(const uint64_t* bitmap, int32_t startRow) {
  int32_t word = startRow / 64;
  int32_t bit = startRow % 64;
  uint64_t combined = bitmap[word] >> bit;
  if (bit > 60) {
    combined |= bitmap[word + 1] << (64 - bit);
  }
  uint8_t nibble = combined & 0xF;
  auto bcast = _mm256_set1_epi64x(nibble);
  auto laneMask = _mm256_set_epi64x(8, 4, 2, 1);
  return _mm256_cmpeq_epi64(_mm256_and_si256(bcast, laneMask), laneMask);
}

static constexpr int32_t kMinSelectivityDenominator = 5; // 1/5 = 20%

// Nullable fixed-width SIMD loops intentionally load all lanes before applying
// null masks. Null-lane payloads only feed temporary lane values that are
// replaced with BaseVector::kNullHash before hashes become observable.

template <typename T, bool kMix, bool kHasNulls>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
BOLT_HASH_TABLE_SIMD_NO_MSAN static void
hashDense(
    const T* __restrict values,
    const uint64_t* nullBits,
    int32_t begin,
    int32_t end,
    uint64_t* __restrict output) {
  constexpr bool kIsInt64 = sizeof(T) == sizeof(int64_t);
  constexpr bool kIsInt32 = sizeof(T) == sizeof(int32_t);
  constexpr bool kIsSmall = sizeof(T) == 1 || sizeof(T) == 2;
  int32_t row = begin;

  if constexpr (kIsInt64) {
    constexpr int kLanes = 4;
    auto kNullVec = _mm256_set1_epi64x(BaseVector::kNullHash);
    for (; row + kLanes <= end; row += kLanes) {
      auto loadedValues = _mm256_loadu_si256((__m256i*)(values + row));
      auto hashed = twangMix64Avx2(loadedValues);

      if constexpr (kHasNulls) {
        auto validMask = expandBits4(nullBits, row);
        auto nullMask = _mm256_xor_si256(validMask, _mm256_set1_epi64x(-1LL));
        hashed = _mm256_blendv_epi8(hashed, kNullVec, nullMask);
      }

      if constexpr (kMix) {
        auto previous = _mm256_loadu_si256((__m256i*)(output + row));
        auto mixed = hashMixAvx2(previous, hashed);
        _mm256_storeu_si256((__m256i*)(output + row), mixed);
      } else {
        _mm256_storeu_si256((__m256i*)(output + row), hashed);
      }
    }
  } else if constexpr (kIsInt32) {
    constexpr int kLanes = 8;
    auto kNullHash32 =
        _mm256_set1_epi32(static_cast<int32_t>(BaseVector::kNullHash));
    for (; row + kLanes <= end; row += kLanes) {
      auto vals32 = _mm256_loadu_si256((__m256i*)(values + row));
      auto hashed32 = jenkinsRevMix32Avx2(vals32);

      if constexpr (kHasNulls) {
        alignas(32) int32_t nullMask32[kLanes];
        for (int l = 0; l < kLanes; ++l) {
          nullMask32[l] = bits::isBitNull(nullBits, row + l) ? -1 : 0;
        }
        auto nullLaneMask = _mm256_load_si256((__m256i*)nullMask32);
        hashed32 = _mm256_blendv_epi8(hashed32, kNullHash32, nullLaneMask);
      }

      auto lo128 = _mm256_castsi256_si128(hashed32);
      auto hi128 = _mm256_extracti128_si256(hashed32, 1);
      auto hashLo = _mm256_cvtepu32_epi64(lo128);
      auto hashHi = _mm256_cvtepu32_epi64(hi128);

      if constexpr (kMix) {
        auto previousLow = _mm256_loadu_si256((__m256i*)(output + row));
        _mm256_storeu_si256(
            (__m256i*)(output + row), hashMixAvx2(previousLow, hashLo));
        auto previousHigh = _mm256_loadu_si256((__m256i*)(output + row + 4));
        _mm256_storeu_si256(
            (__m256i*)(output + row + 4), hashMixAvx2(previousHigh, hashHi));
      } else {
        _mm256_storeu_si256((__m256i*)(output + row), hashLo);
        _mm256_storeu_si256((__m256i*)(output + row + 4), hashHi);
      }
    }
  } else if constexpr (kIsSmall) {
    // folly::hasher<int8_t/int16_t> uses jenkins_rev_mix32.
    constexpr int kLanes = 4;
    auto kNullHash32 =
        _mm_set1_epi32(static_cast<int32_t>(BaseVector::kNullHash));
    for (; row + kLanes <= end; row += kLanes) {
      __m128i vals32;
      if constexpr (sizeof(T) == 1) {
        auto narrow =
            _mm_cvtsi32_si128(folly::loadUnaligned<int32_t>(values + row));
        vals32 = _mm_cvtepi8_epi32(narrow);
      } else {
        auto narrow =
            _mm_cvtsi64_si128(folly::loadUnaligned<int64_t>(values + row));
        vals32 = _mm_cvtepi16_epi32(narrow);
      }
      auto hashed32 = jenkinsRevMix32Sse(vals32);

      if constexpr (kHasNulls) {
        alignas(16) int32_t nullMask32[kLanes];
        for (int l = 0; l < kLanes; ++l) {
          nullMask32[l] = bits::isBitNull(nullBits, row + l) ? -1 : 0;
        }
        auto nullLaneMask = _mm_load_si128((__m128i*)nullMask32);
        hashed32 = _mm_blendv_epi8(hashed32, kNullHash32, nullLaneMask);
      }

      auto hash64 = _mm256_cvtepu32_epi64(hashed32);

      if constexpr (kMix) {
        auto previous = _mm256_loadu_si256((__m256i*)(output + row));
        _mm256_storeu_si256(
            (__m256i*)(output + row), hashMixAvx2(previous, hash64));
      } else {
        _mm256_storeu_si256((__m256i*)(output + row), hash64);
      }
    }
  }

  for (; row < end; ++row) {
    if constexpr (kHasNulls) {
      if (bits::isBitNull(nullBits, row)) {
        if constexpr (kMix) {
          output[row] = bits::hashMix(output[row], BaseVector::kNullHash);
        } else {
          output[row] = BaseVector::kNullHash;
        }
        continue;
      }
    }
    auto hashValue = folly::hasher<T>()(values[row]);
    if constexpr (kMix) {
      output[row] = bits::hashMix(output[row], hashValue);
    } else {
      output[row] = hashValue;
    }
  }
}

template <bool kMix, bool kHasNulls>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
BOLT_HASH_TABLE_SIMD_NO_MSAN static void
hashInt64WithSelection(
    const int64_t* __restrict values,
    const uint64_t* nullBits,
    const uint64_t* selectionBits,
    int32_t begin,
    int32_t end,
    uint64_t* __restrict output) {
  constexpr int kLanes = 4;
  auto kNullVec = _mm256_set1_epi64x(BaseVector::kNullHash);
  int32_t row = begin;

  for (; row + kLanes <= end; row += kLanes) {
    auto loadedValues = _mm256_loadu_si256((__m256i*)(values + row));
    auto hashed = twangMix64Avx2(loadedValues);

    if constexpr (kHasNulls) {
      auto validMask = expandBits4(nullBits, row);
      auto nullMask = _mm256_xor_si256(validMask, _mm256_set1_epi64x(-1LL));
      hashed = _mm256_blendv_epi8(hashed, kNullVec, nullMask);
    }

    if constexpr (kMix) {
      auto previous = _mm256_loadu_si256((__m256i*)(output + row));
      hashed = hashMixAvx2(previous, hashed);
    }

    auto selMask = expandBits4(selectionBits, row);
    auto previous = _mm256_loadu_si256((__m256i*)(output + row));
    _mm256_storeu_si256(
        (__m256i*)(output + row),
        _mm256_blendv_epi8(previous, hashed, selMask));
  }

  for (; row < end; ++row) {
    if (!bits::isBitSet(selectionBits, row)) {
      continue;
    }
    if constexpr (kHasNulls) {
      if (bits::isBitNull(nullBits, row)) {
        if constexpr (kMix) {
          output[row] = bits::hashMix(output[row], BaseVector::kNullHash);
        } else {
          output[row] = BaseVector::kNullHash;
        }
        continue;
      }
    }
    auto hashValue = folly::hasher<int64_t>()(values[row]);
    if constexpr (kMix) {
      output[row] = bits::hashMix(output[row], hashValue);
    } else {
      output[row] = hashValue;
    }
  }
}

template <bool kMix, bool kHasNulls>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
BOLT_HASH_TABLE_SIMD_NO_MSAN static void
hashInt32WithSelection(
    const int32_t* __restrict values,
    const uint64_t* nullBits,
    const uint64_t* selectionBits,
    int32_t begin,
    int32_t end,
    uint64_t* __restrict output) {
  constexpr int kLanes = 8;
  const auto kNullHash32 =
      _mm256_set1_epi32(static_cast<int32_t>(BaseVector::kNullHash));
  int32_t row = begin;

  for (; row + kLanes <= end; row += kLanes) {
    auto vals32 =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + row));
    auto hashed32 = jenkinsRevMix32Avx2(vals32);

    if constexpr (kHasNulls) {
      alignas(32) int32_t nullMask32[kLanes];
      for (int l = 0; l < kLanes; ++l) {
        nullMask32[l] = bits::isBitNull(nullBits, row + l) ? -1 : 0;
      }
      auto nullLaneMask =
          _mm256_load_si256(reinterpret_cast<const __m256i*>(nullMask32));
      hashed32 = _mm256_blendv_epi8(hashed32, kNullHash32, nullLaneMask);
    }

    auto lo128 = _mm256_castsi256_si128(hashed32);
    auto hi128 = _mm256_extracti128_si256(hashed32, 1);
    auto hashLo = _mm256_cvtepu32_epi64(lo128);
    auto hashHi = _mm256_cvtepu32_epi64(hi128);

    if constexpr (kMix) {
      auto previousLow =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(output + row));
      auto previousHigh = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(output + row + 4));
      hashLo = hashMixAvx2(previousLow, hashLo);
      hashHi = hashMixAvx2(previousHigh, hashHi);
    }

    auto selMaskLo = expandBits4(selectionBits, row);
    auto selMaskHi = expandBits4(selectionBits, row + 4);
    auto previousLow =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(output + row));
    auto previousHigh =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(output + row + 4));
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(output + row),
        _mm256_blendv_epi8(previousLow, hashLo, selMaskLo));
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(output + row + 4),
        _mm256_blendv_epi8(previousHigh, hashHi, selMaskHi));
  }

  for (; row < end; ++row) {
    if (!bits::isBitSet(selectionBits, row)) {
      continue;
    }
    if constexpr (kHasNulls) {
      if (bits::isBitNull(nullBits, row)) {
        if constexpr (kMix) {
          output[row] = bits::hashMix(output[row], BaseVector::kNullHash);
        } else {
          output[row] = BaseVector::kNullHash;
        }
        continue;
      }
    }
    auto hashValue = folly::hasher<int32_t>()(values[row]);
    if constexpr (kMix) {
      output[row] = bits::hashMix(output[row], hashValue);
    } else {
      output[row] = hashValue;
    }
  }
}

template <bool kMix, bool kHasNulls>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
static void
hashDictionaryInt64(
    const int64_t* __restrict baseValues,
    const int32_t* __restrict indices,
    const uint64_t* nullBits,
    int32_t begin,
    int32_t end,
    uint64_t* __restrict output) {
  constexpr int kLanes = 4;
  auto kNullVec = _mm256_set1_epi64x(BaseVector::kNullHash);
  int32_t row = begin;

  for (; row + kLanes <= end; row += kLanes) {
    int32_t idx0 = indices[row];
    int32_t idx1 = indices[row + 1];
    int32_t idx2 = indices[row + 2];
    int32_t idx3 = indices[row + 3];
    if constexpr (kHasNulls) {
      idx0 = bits::isBitNull(nullBits, row) ? 0 : idx0;
      idx1 = bits::isBitNull(nullBits, row + 1) ? 0 : idx1;
      idx2 = bits::isBitNull(nullBits, row + 2) ? 0 : idx2;
      idx3 = bits::isBitNull(nullBits, row + 3) ? 0 : idx3;
    }
    auto loadedValues = load4x64(
        reinterpret_cast<const char*>(&baseValues[idx0]),
        reinterpret_cast<const char*>(&baseValues[idx1]),
        reinterpret_cast<const char*>(&baseValues[idx2]),
        reinterpret_cast<const char*>(&baseValues[idx3]));
    auto hashed = twangMix64Avx2(loadedValues);

    if constexpr (kHasNulls) {
      auto validMask = expandBits4(nullBits, row);
      auto nullMask = _mm256_xor_si256(validMask, _mm256_set1_epi64x(-1LL));
      hashed = _mm256_blendv_epi8(hashed, kNullVec, nullMask);
    }

    if constexpr (kMix) {
      auto previous = _mm256_loadu_si256((__m256i*)(output + row));
      _mm256_storeu_si256(
          (__m256i*)(output + row), hashMixAvx2(previous, hashed));
    } else {
      _mm256_storeu_si256((__m256i*)(output + row), hashed);
    }
  }

  for (; row < end; ++row) {
    if constexpr (kHasNulls) {
      // nullBits indexed by top-level row, not dict base index.
      if (bits::isBitNull(nullBits, row)) {
        if constexpr (kMix) {
          output[row] = bits::hashMix(output[row], BaseVector::kNullHash);
        } else {
          output[row] = BaseVector::kNullHash;
        }
        continue;
      }
    }
    auto baseIndex = indices[row];
    auto hashValue = folly::hasher<int64_t>()(baseValues[baseIndex]);
    if constexpr (kMix) {
      output[row] = bits::hashMix(output[row], hashValue);
    } else {
      output[row] = hashValue;
    }
  }
}

template <bool kMix, bool kHasNulls>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
static void
hashDictionaryInt32(
    const int32_t* __restrict baseValues,
    const int32_t* __restrict indices,
    const uint64_t* nullBits,
    int32_t begin,
    int32_t end,
    uint64_t* __restrict output) {
  constexpr int kLanes = 8;
  const auto kNullHash32 =
      _mm256_set1_epi32(static_cast<int32_t>(BaseVector::kNullHash));
  int32_t row = begin;

  for (; row + kLanes <= end; row += kLanes) {
    __m256i baseIndex;
    if constexpr (kHasNulls) {
      alignas(32) int32_t safeIndices[kLanes];
      for (int l = 0; l < kLanes; ++l) {
        safeIndices[l] =
            bits::isBitNull(nullBits, row + l) ? 0 : indices[row + l];
      }
      baseIndex =
          _mm256_load_si256(reinterpret_cast<const __m256i*>(safeIndices));
    } else {
      baseIndex =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices + row));
    }
    auto vals32 = _mm256_i32gather_epi32(baseValues, baseIndex, 4);
    auto hashed32 = jenkinsRevMix32Avx2(vals32);

    if constexpr (kHasNulls) {
      alignas(32) int32_t nullMask32[kLanes];
      for (int l = 0; l < kLanes; ++l) {
        // nullBits indexed by top-level row, not by dict base index.
        nullMask32[l] = bits::isBitNull(nullBits, row + l) ? -1 : 0;
      }
      auto nullLaneMask =
          _mm256_load_si256(reinterpret_cast<const __m256i*>(nullMask32));
      hashed32 = _mm256_blendv_epi8(hashed32, kNullHash32, nullLaneMask);
    }

    auto lo128 = _mm256_castsi256_si128(hashed32);
    auto hi128 = _mm256_extracti128_si256(hashed32, 1);
    auto hashLo = _mm256_cvtepu32_epi64(lo128);
    auto hashHi = _mm256_cvtepu32_epi64(hi128);

    if constexpr (kMix) {
      auto previousLow =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(output + row));
      auto previousHigh = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(output + row + 4));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(output + row),
          hashMixAvx2(previousLow, hashLo));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(output + row + 4),
          hashMixAvx2(previousHigh, hashHi));
    } else {
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + row), hashLo);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + row + 4), hashHi);
    }
  }

  for (; row < end; ++row) {
    if constexpr (kHasNulls) {
      if (bits::isBitNull(nullBits, row)) {
        if constexpr (kMix) {
          output[row] = bits::hashMix(output[row], BaseVector::kNullHash);
        } else {
          output[row] = BaseVector::kNullHash;
        }
        continue;
      }
    }
    auto baseIndex = indices[row];
    auto hashValue = folly::hasher<int32_t>()(baseValues[baseIndex]);
    if constexpr (kMix) {
      output[row] = bits::hashMix(output[row], hashValue);
    } else {
      output[row] = hashValue;
    }
  }
}

// Not SIMD (CRC32 is inherently serial), but bypasses DecodedVector dispatch
// and adds short-string fast path + long-string prefetch.

template <bool kMix, bool kHasNulls>
static void hashDenseVarchar(
    const StringView* __restrict views,
    const uint64_t* nullBits,
    int32_t begin,
    int32_t end,
    uint64_t* __restrict output) {
  constexpr int kBatch = 4;
  int32_t row = begin;

  for (; row + kBatch <= end; row += kBatch) {
    if (row + 2 * kBatch <= end) {
      for (int b = 0; b < kBatch; ++b) {
        const auto aheadRow = row + kBatch + b;
        if constexpr (kHasNulls) {
          if (bits::isBitNull(nullBits, aheadRow)) {
            continue;
          }
        }
        auto& ahead = views[aheadRow];
        if (ahead.size() > 12) {
          __builtin_prefetch(ahead.data(), 0, 0);
        }
      }
    }

    uint64_t hashValue[kBatch];
    for (int b = 0; b < kBatch; ++b) {
      if constexpr (kHasNulls) {
        if (bits::isBitNull(nullBits, row + b)) {
          hashValue[b] = BaseVector::kNullHash;
          continue;
        }
      }
      hashValue[b] = folly::hasher<StringView>()(views[row + b]);
    }

    for (int b = 0; b < kBatch; ++b) {
      if constexpr (kMix) {
        output[row + b] = bits::hashMix(output[row + b], hashValue[b]);
      } else {
        output[row + b] = hashValue[b];
      }
    }
  }

  for (; row < end; ++row) {
    if constexpr (kHasNulls) {
      if (bits::isBitNull(nullBits, row)) {
        if constexpr (kMix) {
          output[row] = bits::hashMix(output[row], BaseVector::kNullHash);
        } else {
          output[row] = BaseVector::kNullHash;
        }
        continue;
      }
    }
    auto hashValue = folly::hasher<StringView>()(views[row]);
    if constexpr (kMix) {
      output[row] = bits::hashMix(output[row], hashValue);
    } else {
      output[row] = hashValue;
    }
  }
}

template <typename T, bool kMix>
static bool tryHashTyped(
    DecodedVector& decoded,
    const SelectivityVector& rows,
    uint64_t* output) {
  const bool hasNulls = decoded.mayHaveNulls();
  const uint64_t* nullBits = hasNulls ? decoded.nulls(&rows) : nullptr;
  const auto begin = rows.begin();
  const auto end = rows.end();

  if (decoded.isIdentityMapping()) {
    const auto* values = decoded.data<T>();
    if (!values) {
      return false;
    }

    if (rows.isAllSelected()) {
      if (hasNulls) {
        hashDense<T, kMix, true>(values, nullBits, begin, end, output);
      } else {
        hashDense<T, kMix, false>(values, nullptr, begin, end, output);
      }
    } else {
      if (rows.countSelected() < (end - begin) / kMinSelectivityDenominator) {
        return false;
      }
      const uint64_t* selectionBits = rows.allBits();
      if constexpr (std::is_same_v<T, int64_t>) {
        if (hasNulls) {
          hashInt64WithSelection<kMix, true>(
              values, nullBits, selectionBits, begin, end, output);
        } else {
          hashInt64WithSelection<kMix, false>(
              values, nullptr, selectionBits, begin, end, output);
        }
      } else if constexpr (std::is_same_v<T, int32_t>) {
        if (hasNulls) {
          hashInt32WithSelection<kMix, true>(
              values, nullBits, selectionBits, begin, end, output);
        } else {
          hashInt32WithSelection<kMix, false>(
              values, nullptr, selectionBits, begin, end, output);
        }
      } else {
        return false;
      }
    }
    return true;
  }

  if (!decoded.isConstantMapping() && rows.isAllSelected()) {
    const auto* indices = decoded.indices();
    if (!indices) {
      return false;
    }
    if constexpr (std::is_same_v<T, int64_t>) {
      const auto* baseValues = decoded.data<int64_t>();
      if (!baseValues) {
        return false;
      }
      if (hasNulls) {
        hashDictionaryInt64<kMix, true>(
            baseValues, indices, nullBits, begin, end, output);
      } else {
        hashDictionaryInt64<kMix, false>(
            baseValues, indices, nullptr, begin, end, output);
      }
      return true;
    } else if constexpr (std::is_same_v<T, int32_t>) {
      const auto* baseValues = decoded.data<int32_t>();
      if (!baseValues) {
        return false;
      }
      if (hasNulls) {
        hashDictionaryInt32<kMix, true>(
            baseValues, indices, nullBits, begin, end, output);
      } else {
        hashDictionaryInt32<kMix, false>(
            baseValues, indices, nullptr, begin, end, output);
      }
      return true;
    }
  }

  return false;
}

bool tryHash(
    VectorHasher& hasher,
    const SelectivityVector& rows,
    bool mixWithExisting,
    uint64_t* hashes) {
  auto& decoded = hasher.decodedVector();
  auto kind = hasher.typeKind();

  if (kind == TypeKind::BIGINT) {
    return mixWithExisting
        ? tryHashTyped<int64_t, true>(decoded, rows, hashes)
        : tryHashTyped<int64_t, false>(decoded, rows, hashes);
  } else if (kind == TypeKind::INTEGER) {
    return mixWithExisting
        ? tryHashTyped<int32_t, true>(decoded, rows, hashes)
        : tryHashTyped<int32_t, false>(decoded, rows, hashes);
  } else if (kind == TypeKind::SMALLINT) {
    return mixWithExisting
        ? tryHashTyped<int16_t, true>(decoded, rows, hashes)
        : tryHashTyped<int16_t, false>(decoded, rows, hashes);
  } else if (kind == TypeKind::TINYINT) {
    return mixWithExisting ? tryHashTyped<int8_t, true>(decoded, rows, hashes)
                           : tryHashTyped<int8_t, false>(decoded, rows, hashes);
  } else if (kind == TypeKind::DOUBLE) {
    // Preserve NaNAwareHash semantics in VectorHasher.
    return false;
  } else if (kind == TypeKind::REAL) {
    // Preserve NaNAwareHash semantics in VectorHasher.
    return false;
  } else if (kind == TypeKind::VARCHAR) {
    if (!decoded.isIdentityMapping()) {
      return false;
    }
    if (!rows.isAllSelected()) {
      return false;
    }
    const auto* views = decoded.data<StringView>();
    if (!views) {
      return false;
    }
    const auto begin = rows.begin();
    const auto end = rows.end();
    const bool hasNulls = decoded.mayHaveNulls();
    const uint64_t* nullBits = hasNulls ? decoded.nulls(&rows) : nullptr;
    if (mixWithExisting) {
      hasNulls
          ? hashDenseVarchar<true, true>(views, nullBits, begin, end, hashes)
          : hashDenseVarchar<true, false>(views, nullptr, begin, end, hashes);
    } else {
      hasNulls
          ? hashDenseVarchar<false, true>(views, nullBits, begin, end, hashes)
          : hashDenseVarchar<false, false>(views, nullptr, begin, end, hashes);
    }
    return true;
  }
  return false;
}

#if defined(__x86_64__) || defined(__i386__)
template <bool kMix, bool kHasNulls>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
BOLT_HASH_TABLE_SIMD_NO_MSAN static bool
mapInt64RangeValueIds(
    const int64_t* __restrict values,
    const uint64_t* nullBits,
    int64_t rangeMin,
    uint64_t multiplier,
    int64_t rangeMax,
    int32_t begin,
    int32_t end,
    uint64_t* __restrict output) {
  const auto rangeMinVec = _mm256_set1_epi64x(rangeMin);
  const auto oneVec = _mm256_set1_epi64x(1);
  const auto rangeMaxVec = _mm256_set1_epi64x(rangeMax);
  constexpr int kLanes = 4;

  int32_t row = begin;
  for (; row + kLanes <= end; row += kLanes) {
    auto loadedValues =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + row));

    auto belowMin =
        _mm256_cmpgt_epi64(rangeMinVec, loadedValues); // rangeMin > val
    auto aboveMax =
        _mm256_cmpgt_epi64(loadedValues, rangeMaxVec); // val > rangeMax
    auto outOfRange = _mm256_or_si256(belowMin, aboveMax);

    uint8_t nullMask = 0;
    if constexpr (kHasNulls) {
      for (int lane = 0; lane < kLanes; ++lane) {
        if (bits::isBitNull(nullBits, row + lane)) {
          nullMask |= (1 << lane);
        }
      }
      if (nullMask) {
        alignas(32) int64_t clearMask[kLanes];
        for (int lane = 0; lane < kLanes; ++lane) {
          clearMask[lane] = (nullMask & (1 << lane)) ? 0 : -1;
        }
        outOfRange = _mm256_and_si256(
            outOfRange,
            _mm256_load_si256(reinterpret_cast<const __m256i*>(clearMask)));
      }
    }

    if (_mm256_movemask_pd(_mm256_castsi256_pd(outOfRange)) != 0) {
      return false;
    }

    // Compute id = value - rangeMin + 1. Do not rewrite this as
    // value - (rangeMin - 1): rangeMin can be int64_t::min().
    auto ids =
        _mm256_add_epi64(_mm256_sub_epi64(loadedValues, rangeMinVec), oneVec);

    if constexpr (kMix) {
      alignas(32) int64_t valueIds[kLanes];
      _mm256_store_si256(reinterpret_cast<__m256i*>(valueIds), ids);
      for (int lane = 0; lane < kLanes; ++lane) {
        if (kHasNulls && (nullMask & (1 << lane))) {
          continue;
        }
        output[row + lane] += multiplier * valueIds[lane];
      }
    } else if (multiplier == 1) {
      if (kHasNulls && nullMask) {
        alignas(32) int64_t valueIds[kLanes];
        _mm256_store_si256(reinterpret_cast<__m256i*>(valueIds), ids);
        for (int lane = 0; lane < kLanes; ++lane) {
          output[row + lane] = (nullMask & (1 << lane)) ? 0 : valueIds[lane];
        }
      } else {
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + row), ids);
      }
    } else {
      alignas(32) int64_t valueIds[kLanes];
      _mm256_store_si256(reinterpret_cast<__m256i*>(valueIds), ids);
      for (int lane = 0; lane < kLanes; ++lane) {
        if (kHasNulls && (nullMask & (1 << lane))) {
          output[row + lane] = 0;
        } else {
          output[row + lane] = output[row + lane] + multiplier * valueIds[lane];
        }
      }
    }
  }

  for (; row < end; ++row) {
    if (kHasNulls && bits::isBitNull(nullBits, row)) {
      if (!kMix) {
        output[row] = 0;
      }
      continue;
    }
    auto value = values[row];
    if (value < rangeMin || value > rangeMax) {
      return false;
    }
    auto id =
        static_cast<uint64_t>(value) - static_cast<uint64_t>(rangeMin) + 1;
    if (kMix) {
      output[row] += multiplier * id;
    } else {
      output[row] = multiplier == 1 ? id : output[row] + multiplier * id;
    }
  }
  return true;
}
#else
template <bool kMix, bool kHasNulls>
static bool mapInt64RangeValueIds(
    const int64_t* __restrict values,
    const uint64_t* nullBits,
    int64_t rangeMin,
    uint64_t multiplier,
    int64_t rangeMax,
    int32_t begin,
    int32_t end,
    uint64_t* __restrict output) {
  for (auto row = begin; row < end; ++row) {
    if (kHasNulls && bits::isBitNull(nullBits, row)) {
      if (!kMix) {
        output[row] = 0;
      }
      continue;
    }
    auto value = values[row];
    if (value < rangeMin || value > rangeMax) {
      return false;
    }
    auto id = static_cast<uint64_t>(value - rangeMin + 1);
    if (kMix) {
      output[row] += multiplier * id;
    } else {
      output[row] = multiplier == 1 ? id : output[row] + multiplier * id;
    }
  }
  return true;
}
#endif

bool tryValueIds(
    VectorHasher& hasher,
    const SelectivityVector& rows,
    bool mixWithExisting,
    uint64_t* hashes) {
  if (!hasher.isRange()) {
    return false;
  }
  if (hasher.typeKind() != TypeKind::BIGINT) {
    return false;
  }
  auto& decoded = hasher.decodedVector();
  if (!decoded.isIdentityMapping()) {
    return false;
  }
  if (!rows.isAllSelected()) {
    return false;
  }
  const auto* values = decoded.data<int64_t>();
  if (!values) {
    return false;
  }

  const auto begin = rows.begin();
  const auto end = rows.end();
  const auto rangeMin = hasher.rangeMin();
  const auto rangeMax = hasher.rangeMax();
  const auto multiplier = hasher.multiplier();
  const bool hasNulls = decoded.mayHaveNulls();
  const uint64_t* nullBits = hasNulls ? decoded.nulls(&rows) : nullptr;

  auto call = [&](auto mixTag, auto nullTag) {
    return mapInt64RangeValueIds<
        decltype(mixTag)::value,
        decltype(nullTag)::value>(
        values, nullBits, rangeMin, multiplier, rangeMax, begin, end, hashes);
  };
  if (mixWithExisting) {
    return hasNulls ? call(std::true_type{}, std::true_type{})
                    : call(std::true_type{}, std::false_type{});
  } else {
    return hasNulls ? call(std::false_type{}, std::true_type{})
                    : call(std::false_type{}, std::false_type{});
  }
}

#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
FOLLY_ALWAYS_INLINE __m256i
loadNullMask4(
    char* const* groups,
    int32_t base,
    int32_t nullByte,
    uint8_t nullMask) {
  const int64_t m0 = (groups[base + 0][nullByte] & nullMask) ? -1LL : 0LL;
  const int64_t m1 = (groups[base + 1][nullByte] & nullMask) ? -1LL : 0LL;
  const int64_t m2 = (groups[base + 2][nullByte] & nullMask) ? -1LL : 0LL;
  const int64_t m3 = (groups[base + 3][nullByte] & nullMask) ? -1LL : 0LL;
  return _mm256_setr_epi64x(m0, m1, m2, m3);
}

#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
FOLLY_ALWAYS_INLINE __m256i
loadNullMask8_32(
    char* const* groups,
    int32_t base,
    int32_t nullByte,
    uint8_t nullMask) {
  const int32_t m0 = (groups[base + 0][nullByte] & nullMask) ? -1 : 0;
  const int32_t m1 = (groups[base + 1][nullByte] & nullMask) ? -1 : 0;
  const int32_t m2 = (groups[base + 2][nullByte] & nullMask) ? -1 : 0;
  const int32_t m3 = (groups[base + 3][nullByte] & nullMask) ? -1 : 0;
  const int32_t m4 = (groups[base + 4][nullByte] & nullMask) ? -1 : 0;
  const int32_t m5 = (groups[base + 5][nullByte] & nullMask) ? -1 : 0;
  const int32_t m6 = (groups[base + 6][nullByte] & nullMask) ? -1 : 0;
  const int32_t m7 = (groups[base + 7][nullByte] & nullMask) ? -1 : 0;
  return _mm256_setr_epi32(m0, m1, m2, m3, m4, m5, m6, m7);
}

#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
FOLLY_ALWAYS_INLINE __m128i
loadNullMask4_32(
    char* const* groups,
    int32_t base,
    int32_t nullByte,
    uint8_t nullMask) {
  const int32_t m0 = (groups[base + 0][nullByte] & nullMask) ? -1 : 0;
  const int32_t m1 = (groups[base + 1][nullByte] & nullMask) ? -1 : 0;
  const int32_t m2 = (groups[base + 2][nullByte] & nullMask) ? -1 : 0;
  const int32_t m3 = (groups[base + 3][nullByte] & nullMask) ? -1 : 0;
  return _mm_setr_epi32(m0, m1, m2, m3);
}

template <bool kMix, bool kHasNulls>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
static void
rehashInt64(
    char** __restrict groups,
    int32_t columnOffset,
    int32_t numGroups,
    int32_t nullByte,
    uint8_t nullMask,
    uint64_t* __restrict hashes) {
  constexpr int kLanes = 4;
  const auto kNullVec = _mm256_set1_epi64x(BaseVector::kNullHash);

  if constexpr (!kMix) {
    for (int32_t i = 0; i < numGroups; ++i) {
      hashes[i] = folly::loadUnaligned<uint64_t>(groups[i] + columnOffset);
    }
    int32_t i = 0;
    for (; i + kLanes <= numGroups; i += kLanes) {
      auto loadedValues =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&hashes[i]));
      auto hashed = twangMix64Avx2(loadedValues);
      if constexpr (kHasNulls) {
        auto nullLaneMask = loadNullMask4(groups, i, nullByte, nullMask);
        hashed = _mm256_blendv_epi8(hashed, kNullVec, nullLaneMask);
      }
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(&hashes[i]), hashed);
    }
    for (; i < numGroups; ++i) {
      if constexpr (kHasNulls) {
        if (groups[i][nullByte] & nullMask) {
          hashes[i] = BaseVector::kNullHash;
          continue;
        }
      }
      hashes[i] = hashInt64Bits(hashes[i]);
    }
  } else {
    constexpr int32_t kStackBuf = 1024;
    uint64_t stackBuf[kStackBuf];
    std::unique_ptr<uint64_t[]> heapBuf;
    uint64_t* loadedValues = stackBuf;
    if (numGroups > kStackBuf) {
      heapBuf = std::make_unique<uint64_t[]>(numGroups);
      loadedValues = heapBuf.get();
    }
    for (int32_t i = 0; i < numGroups; ++i) {
      loadedValues[i] =
          folly::loadUnaligned<uint64_t>(groups[i] + columnOffset);
    }
    int32_t i = 0;
    for (; i + kLanes <= numGroups; i += kLanes) {
      auto v = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(&loadedValues[i]));
      auto hashed = twangMix64Avx2(v);
      if constexpr (kHasNulls) {
        auto nullLaneMask = loadNullMask4(groups, i, nullByte, nullMask);
        hashed = _mm256_blendv_epi8(hashed, kNullVec, nullLaneMask);
      }
      auto previous =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&hashes[i]));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(&hashes[i]),
          hashMixAvx2(previous, hashed));
    }
    for (; i < numGroups; ++i) {
      uint64_t hashValue;
      if constexpr (kHasNulls) {
        if (groups[i][nullByte] & nullMask) {
          hashValue = BaseVector::kNullHash;
        } else {
          hashValue = hashInt64Bits(loadedValues[i]);
        }
      } else {
        hashValue = hashInt64Bits(loadedValues[i]);
      }
      hashes[i] = bits::hashMix(hashes[i], hashValue);
    }
  }
}

template <bool kMix, bool kHasNulls>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
static void
rehashInt32(
    char** __restrict groups,
    int32_t columnOffset,
    int32_t numGroups,
    int32_t nullByte,
    uint8_t nullMask,
    uint64_t* __restrict hashes) {
  constexpr int kLanes = 8;
  const auto kNullHash32 =
      _mm256_set1_epi32(static_cast<int32_t>(BaseVector::kNullHash));

  constexpr int32_t kStackBuf = 1024;
  int32_t stackBuf[kStackBuf];
  std::unique_ptr<int32_t[]> heapBuf;
  int32_t* loadedValues = stackBuf;
  if (numGroups > kStackBuf) {
    heapBuf = std::make_unique<int32_t[]>(numGroups);
    loadedValues = heapBuf.get();
  }
  for (int32_t i = 0; i < numGroups; ++i) {
    loadedValues[i] = folly::loadUnaligned<int32_t>(groups[i] + columnOffset);
  }

  int32_t i = 0;
  for (; i + kLanes <= numGroups; i += kLanes) {
    auto v =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&loadedValues[i]));
    auto hashed32 = jenkinsRevMix32Avx2(v);
    if constexpr (kHasNulls) {
      auto nullLaneMask = loadNullMask8_32(groups, i, nullByte, nullMask);
      hashed32 = _mm256_blendv_epi8(hashed32, kNullHash32, nullLaneMask);
    }
    auto lo128 = _mm256_castsi256_si128(hashed32);
    auto hi128 = _mm256_extracti128_si256(hashed32, 1);
    auto hashLo = _mm256_cvtepu32_epi64(lo128);
    auto hashHi = _mm256_cvtepu32_epi64(hi128);

    if constexpr (kMix) {
      auto previousLow =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&hashes[i]));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(&hashes[i]),
          hashMixAvx2(previousLow, hashLo));
      auto previousHigh =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&hashes[i + 4]));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(&hashes[i + 4]),
          hashMixAvx2(previousHigh, hashHi));
    } else {
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(&hashes[i]), hashLo);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(&hashes[i + 4]), hashHi);
    }
  }
  for (; i < numGroups; ++i) {
    uint64_t hashValue;
    if constexpr (kHasNulls) {
      if (groups[i][nullByte] & nullMask) {
        hashValue = BaseVector::kNullHash;
      } else {
        hashValue = folly::hasher<int32_t>()(loadedValues[i]);
      }
    } else {
      hashValue = folly::hasher<int32_t>()(loadedValues[i]);
    }
    if constexpr (kMix) {
      hashes[i] = bits::hashMix(hashes[i], hashValue);
    } else {
      hashes[i] = hashValue;
    }
  }
}

template <typename T, bool kMix, bool kHasNulls>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
static void
rehashSmallInteger(
    char** __restrict groups,
    int32_t columnOffset,
    int32_t numGroups,
    int32_t nullByte,
    uint8_t nullMask,
    uint64_t* __restrict hashes) {
  static_assert(sizeof(T) == 1 || sizeof(T) == 2);
  constexpr int kLanes = 4;
  const auto kNullHash32 =
      _mm_set1_epi32(static_cast<int32_t>(BaseVector::kNullHash));

  constexpr int32_t kStackBuf = 1024;
  int32_t stackBuf[kStackBuf];
  std::unique_ptr<int32_t[]> heapBuf;
  int32_t* loadedValues = stackBuf;
  if (numGroups > kStackBuf) {
    heapBuf = std::make_unique<int32_t[]>(numGroups);
    loadedValues = heapBuf.get();
  }
  for (int32_t i = 0; i < numGroups; ++i) {
    loadedValues[i] =
        static_cast<int32_t>(folly::loadUnaligned<T>(groups[i] + columnOffset));
  }

  int32_t i = 0;
  for (; i + kLanes <= numGroups; i += kLanes) {
    auto v =
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(&loadedValues[i]));
    auto key = jenkinsRevMix32Sse(v);
    if constexpr (kHasNulls) {
      auto nullLaneMask = loadNullMask4_32(groups, i, nullByte, nullMask);
      key = _mm_blendv_epi8(key, kNullHash32, nullLaneMask);
    }
    auto hash64 = _mm256_cvtepu32_epi64(key);

    if constexpr (kMix) {
      auto previous =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&hashes[i]));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(&hashes[i]),
          hashMixAvx2(previous, hash64));
    } else {
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(&hashes[i]), hash64);
    }
  }
  for (; i < numGroups; ++i) {
    uint64_t hashValue;
    if constexpr (kHasNulls) {
      if (groups[i][nullByte] & nullMask) {
        hashValue = BaseVector::kNullHash;
      } else {
        hashValue = folly::hasher<T>()(static_cast<T>(loadedValues[i]));
      }
    } else {
      hashValue = folly::hasher<T>()(static_cast<T>(loadedValues[i]));
    }
    if constexpr (kMix) {
      hashes[i] = bits::hashMix(hashes[i], hashValue);
    } else {
      hashes[i] = hashValue;
    }
  }
}

#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
void rehashNormalizedKeys(
    char** __restrict groups,
    int32_t numGroups,
    uint64_t* __restrict hashes) {
  constexpr int kLanes = 4;
  for (int32_t i = 0; i < numGroups; ++i) {
    hashes[i] = RowContainer::normalizedKey(groups[i]);
  }

  int32_t i = 0;
  for (; i + kLanes <= numGroups; i += kLanes) {
    auto normalizedKeys =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&hashes[i]));
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(&hashes[i]), twangMix64Avx2(normalizedKeys));
  }
  for (; i < numGroups; ++i) {
    hashes[i] = folly::hasher<uint64_t>()(hashes[i]);
  }
}

// A zero nullMask denotes a non-nullable RowColumn; its nullByte is zero.
bool tryRehash(
    TypeKind kind,
    char** __restrict groups,
    int32_t columnOffset,
    int32_t numGroups,
    bool mixWithExisting,
    int32_t nullByte,
    uint8_t nullMask,
    uint64_t* __restrict hashes) {
  const bool hasNulls = nullMask != 0;
  switch (kind) {
    case TypeKind::BIGINT:
      if (hasNulls) {
        mixWithExisting
            ? rehashInt64<true, true>(
                  groups, columnOffset, numGroups, nullByte, nullMask, hashes)
            : rehashInt64<false, true>(
                  groups, columnOffset, numGroups, nullByte, nullMask, hashes);
      } else {
        mixWithExisting ? rehashInt64<true, false>(
                              groups, columnOffset, numGroups, 0, 0, hashes)
                        : rehashInt64<false, false>(
                              groups, columnOffset, numGroups, 0, 0, hashes);
      }
      return true;
    case TypeKind::INTEGER:
      if (hasNulls) {
        mixWithExisting
            ? rehashInt32<true, true>(
                  groups, columnOffset, numGroups, nullByte, nullMask, hashes)
            : rehashInt32<false, true>(
                  groups, columnOffset, numGroups, nullByte, nullMask, hashes);
      } else {
        mixWithExisting ? rehashInt32<true, false>(
                              groups, columnOffset, numGroups, 0, 0, hashes)
                        : rehashInt32<false, false>(
                              groups, columnOffset, numGroups, 0, 0, hashes);
      }
      return true;
    case TypeKind::SMALLINT:
      if (hasNulls) {
        mixWithExisting
            ? rehashSmallInteger<int16_t, true, true>(
                  groups, columnOffset, numGroups, nullByte, nullMask, hashes)
            : rehashSmallInteger<int16_t, false, true>(
                  groups, columnOffset, numGroups, nullByte, nullMask, hashes);
      } else {
        mixWithExisting ? rehashSmallInteger<int16_t, true, false>(
                              groups, columnOffset, numGroups, 0, 0, hashes)
                        : rehashSmallInteger<int16_t, false, false>(
                              groups, columnOffset, numGroups, 0, 0, hashes);
      }
      return true;
    case TypeKind::TINYINT:
      if (hasNulls) {
        mixWithExisting
            ? rehashSmallInteger<int8_t, true, true>(
                  groups, columnOffset, numGroups, nullByte, nullMask, hashes)
            : rehashSmallInteger<int8_t, false, true>(
                  groups, columnOffset, numGroups, nullByte, nullMask, hashes);
      } else {
        mixWithExisting ? rehashSmallInteger<int8_t, true, false>(
                              groups, columnOffset, numGroups, 0, 0, hashes)
                        : rehashSmallInteger<int8_t, false, false>(
                              groups, columnOffset, numGroups, 0, 0, hashes);
      }
      return true;
    default:
      // REAL and DOUBLE use NaNAwareHash semantics in RowContainer::hash.
      // Fall back until the SIMD path can preserve those floating-point rules.
      return false;
  }
}

#else

bool tryHash(
    VectorHasher& /*hasher*/,
    const SelectivityVector& /*rows*/,
    bool /*mixWithExisting*/,
    uint64_t* /*hashes*/) {
  return false;
}

bool tryValueIds(
    VectorHasher& /*hasher*/,
    const SelectivityVector& /*rows*/,
    bool /*mixWithExisting*/,
    uint64_t* /*hashes*/) {
  return false;
}

void rehashNormalizedKeys(
    char** /*groups*/,
    int32_t /*numGroups*/,
    uint64_t* /*hashes*/) {}

bool tryRehash(
    TypeKind /*kind*/,
    char** /*groups*/,
    int32_t /*columnOffset*/,
    int32_t /*numGroups*/,
    bool /*mixWithExisting*/,
    int32_t /*nullByte*/,
    uint8_t /*nullMask*/,
    uint64_t* /*hashes*/) {
  return false;
}

#endif // defined(__x86_64__) || defined(__i386__)

} // namespace bytedance::bolt::exec::hash_table_simd
