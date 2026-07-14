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

#include "bolt/dwio/common/BitPackDecoder.h"
#if defined(__ARM_FEATURE_SVE) && defined(aarch64)
#include <arm_sve.h>
#endif
namespace bytedance::bolt::dwio::common {

using int128_t = __int128_t;

#if XSIMD_WITH_AVX2

typedef int32_t __m256si __attribute__((__vector_size__(32), __may_alias__));

typedef int32_t __m256si_u
    __attribute__((__vector_size__(32), __may_alias__, __aligned__(1)));

namespace {

template <int8_t i>
auto as4x64(__m256i x) {
  return _mm256_cvtepu32_epi64(_mm256_extracti128_si256(x, i));
}

template <typename T>
void store8Ints(__m256i eightInts, int32_t i, T* FOLLY_NONNULL result) {
  if (sizeof(T) == 4) {
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result + i), eightInts);
  } else {
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(result + i), as4x64<0>(eightInts));
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(result + i + 4), as4x64<1>(eightInts));
  }
}

template <typename T>
inline T* addBytes(T* pointer, int32_t bytes) {
  return reinterpret_cast<T*>(reinterpret_cast<uint64_t>(pointer) + bytes);
}

template <typename T>
inline __m256i as256i(T x) {
  return reinterpret_cast<__m256i>(x);
}

template <typename T>
inline __m256si as8x32(T x) {
  return reinterpret_cast<__m256si>(x);
}

template <uint8_t width, typename T>
FOLLY_ALWAYS_INLINE __m256i gather8Sparse(
    const uint64_t* bits,
    int32_t bitOffset,
    const int32_t* rows,
    int32_t i,
    __m256si masks,
    T* result) {
  constexpr __m256si kMultipliers = {256, 128, 64, 32, 16, 8, 4, 2};
  // workaround for:
  // https://github.com/llvm/llvm-project/issues/64819#issuecomment-1684943890
  constexpr __m256si kWidthSplat = {
      width, width, width, width, width, width, width, width};

  auto indices =
      *reinterpret_cast<const __m256si_u*>(rows + i) * kWidthSplat + bitOffset;
  __m256si multipliers;
  if (width % 8 != 0) {
    multipliers = (__m256si)_mm256_permutevar8x32_epi32(
        as256i(kMultipliers), as256i(indices & 7));
  }
  auto byteIndices = indices >> 3;
  auto data = as8x32(_mm256_i32gather_epi32(
      reinterpret_cast<const int*>(bits), as256i(byteIndices), 1));
  if (width % 8 != 0) {
    data = (data * multipliers) >> 8;
  }
  return as256i(data & masks);
}

template <uint8_t width, typename T>
int32_t decode1To24(
    const uint64_t* bits,
    int32_t bitOffset,
    const int* rows,
    int32_t numRows,
    T* result) {
  constexpr uint64_t kMask = bits::lowMask(width);
  constexpr uint64_t kMask2 = kMask | (kMask << 8);
  constexpr uint64_t kMask4 = kMask2 | (kMask2 << 16);
  constexpr uint64_t kDepMask8 = kMask4 | (kMask4 << 32);
  constexpr uint64_t kMask16 = kMask | (kMask << 16);
  constexpr uint64_t kDepMask16 = kMask16 | (kMask16 << 32);
  int32_t i = 0;
  const auto masks = as8x32(_mm256_set1_epi32(kMask));
  for (; i + 8 <= numRows; i += 8) {
    auto row = rows[i];
    auto endRow = rows[i + 7];
    __m256i eightInts;
    if (width <= 16 && endRow - row == 7) {
      // Special cases for 8 contiguous values with <= 16 bits.
      if (width <= 8) {
        uint64_t eightBytes;
        if (width == 8) {
          if (!bitOffset) {
            eightBytes = *addBytes(bits, row);
          } else {
            eightBytes =
                bits::detail::loadBits<uint64_t>(bits, bitOffset + 8 * row, 64);
          }
        } else {
          auto bit = row * width + bitOffset;
          auto byte = bit >> 3;
          auto shift = bit & 7;
          uint64_t word = *addBytes(bits, byte) >> shift;
          eightBytes = _pdep_u64(word, kDepMask8);
        }
        eightInts = _mm256_cvtepu8_epi32(
            _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&eightBytes)));
      } else {
        // Use pdep to shift 2 words of bit packed data with width
        // 9-16. For widts <= 14 four bit packed fields can always be
        // loaded with a single uint64_t load. For 15 and 16 bits this
        // depends on the start bit position. For either case we fill
        // an array of 2x64 bits and widen that to a 8x32 word.
        uint64_t words[2];
        if (width <= 14) {
          auto bit = row * width + bitOffset;
          auto byte = bit >> 3;
          auto shift = bit & 7;
          uint64_t word = *addBytes(bits, byte) >> shift;
          words[0] = _pdep_u64(word, kDepMask16);
          bit += 4 * width;
          byte = bit >> 3;
          shift = bit & 7;
          word = *addBytes(bits, byte) >> shift;
          words[1] = _pdep_u64(word, kDepMask16);
        } else {
          words[0] = bits::detail::loadBits<uint64_t>(
              bits, bitOffset + width * row, 64);
          words[1] = bits::detail::loadBits<uint64_t>(
              bits, bitOffset + width * (row + 4), 64);
          if (width == 15) {
            words[0] = _pdep_u64(words[0], kDepMask16);
            words[1] = _pdep_u64(words[1], kDepMask16);
          }
        }
        eightInts = _mm256_cvtepu16_epi32(
            _mm_load_si128(reinterpret_cast<const __m128i*>(&words)));
      }
    } else {
      eightInts = gather8Sparse<width>(bits, bitOffset, rows, i, masks, result);
    }
    store8Ints(eightInts, i, result);
  }
  return i;
}

#define WIDTH_CASE(width)                                                      \
  case width:                                                                  \
    i = decode1To24<width>(bits, bitOffset, rows.data(), numSafeRows, result); \
    break;

} // namespace

#endif

#if defined(__ARM_FEATURE_SVE) && defined(__aarch64__) && SVE_BITS == 256

namespace {

template <typename T>
inline T* addBytes(T* pointer, int32_t offset) {
  return reinterpret_cast<T*>(reinterpret_cast<uint64_t>(pointer) + offset);
}

template <typename T>
void store8Ints_sve(svint32_t eightInts_sve, int32_t i, T* result) {
  if constexpr (sizeof(T) == 2) {
    svint16_t eightInts_s16 = svqxtnb_s32(eightInts_sve);
    svst1_s16(svwhilelt_b16(0, 8), &result[i], eightInts_s16);
  } else if constexpr (sizeof(T) == 4) {
    svst1_s32(svwhilelt_b32(0, 8), &result[i], eightInts_sve);
  } else if constexpr (sizeof(T) == 8) {
    svint64_t eightInts_s64_lo = svunpklo_s64(eightInts_sve);
    svint64_t eightInts_s64_hi = svunpkhi_s64(eightInts_sve);
    svst1_s64(svwhilelt_b64(0, 4), &result[i], eightInts_s64_lo);
    svst1_s64(svwhilelt_b64(0, 4), &result[i + 4], eightInts_s64_hi);
  }
}

template <uint8_t width>
void gather8Sparse_sve(
    const uint64_t* bits,
    int32_t bitOffset,
    const int32_t* rows,
    int32_t i,
    uint32_t eightInts[8]) {
  svbool_t pg = svwhilelt_b32(i, i + 8);
  svbool_t pg8 = svwhilelt_b32(0, 8);

  svuint32_t row_vec = svreinterpret_u32_s32(svld1_s32(pg, rows + i));
  svuint32_t indices = svmla_u32_z(
      pg8,
      svdup_u32(static_cast<uint32_t>(bitOffset)),
      row_vec,
      svdup_n_u32(width));
  svuint32_t byte_indices =
      svlsr_u32_z(pg8, indices, svdup_n_u32(static_cast<uint32_t>(3)));

  svuint32_t data = svld1_gather_u32offset_u32(
      pg8, reinterpret_cast<const uint32_t*>(bits), byte_indices);

  if constexpr (width % 8 != 0) {
    constexpr static uint32_t kMultipliers_arr[8] = {
        256, 128, 64, 32, 16, 8, 4, 2};
    svuint32_t kMultipliers_vec = svld1_u32(pg8, kMultipliers_arr);
    svuint32_t bit_in_byte =
        svand_u32_z(pg8, indices, svdup_n_u32(static_cast<uint32_t>(7)));
    svuint32_t multipliers = svtbl_u32(kMultipliers_vec, bit_in_byte);
    data = svlsr_n_u32_z(pg8, svmul_u32_z(pg8, data, multipliers), 8);
  }

  uint32_t mask_val = (width == 0) ? 0 : (1U << width) - 1;
  svuint32_t result_vec = svand_n_u32_z(pg8, data, mask_val);
  svst1_u32(pg8, eightInts, result_vec);
}

template <uint8_t width, typename T>
int32_t decode1To24_sve(
    const uint64_t* bits,
    int32_t bitOffset,
    const int* rows,
    int32_t numRows,
    T* result) {
  constexpr uint64_t kMask = bits::lowMask(width);
  constexpr uint64_t kMask2 = kMask | (kMask << 8);
  constexpr uint64_t kMask4 = kMask2 | (kMask2 << 16);
  constexpr uint64_t kDepMask8 = kMask4 | (kMask4 << 32);
  constexpr uint64_t kMask16 = kMask | (kMask << 16);
  constexpr uint64_t kDepMask16 = kMask16 | (kMask16 << 32);
  svuint64_t kBdepMask8 = svdup_u64(kDepMask8);
  svuint64_t kBdepMask16 = svdup_u64(kDepMask16);
  svbool_t pg1 = svwhilelt_b64(0, 1);
  svbool_t pg8 = svwhilelt_b32(0, 8);
  uint64_t mask = bits::lowMask(width);
  int32_t i = 0;

  for (; i + 8 <= numRows; i += 8) {
    auto row = rows[i];
    auto endRow = rows[i + 7];
    uint32_t eightInts[8];
    if (width <= 16 && endRow - row == 7) {
      // Special cases for 8 contiguous values with <= 16 bits.
      if constexpr (width <= 8) {
        uint64_t eightBytes;
        if (width == 8) {
          if (!bitOffset) {
            eightBytes = *addBytes(bits, row);
          } else {
            eightBytes =
                bits::detail::loadBits<uint64_t>(bits, bitOffset + 8 * row, 64);
          }
        } else {
          auto bit = row * width + bitOffset;
          auto byte = bit >> 3;
          auto shift = bit & 7;
          uint64_t word = *addBytes(bits, byte) >> shift;
          svuint64_t word_vec = svdup_u64(word);
          svuint64_t result_vec = svbdep_u64(word_vec, kBdepMask8);
          eightBytes = svlastb_u64(pg1, result_vec);
        }
        svst1_u32(
            pg8,
            eightInts,
            svld1ub_u32(svwhilelt_b32(0, 8), (uint8_t*)&eightBytes));
      } else {
        // Use pdep to shift 2 words of bit packed data with width
        // 9-16. For widts <= 14 four bit packed fields can always be
        // loaded with a single uint64_t load. For 15 and 16 bits this
        // depends on the start bit position. For either case we fill
        // an array of 2x64 bits and widen that to a 8x32 word.
        uint64_t words[2];
        if constexpr (width <= 14) {
          auto bit = row * width + bitOffset;
          auto byte = bit >> 3;
          auto shift = bit & 7;
          uint64_t word = *addBytes(bits, byte) >> shift;
          svuint64_t word_vec = svdup_u64(word);
          svuint64_t result_vec = svbdep_u64(word_vec, kBdepMask16);
          words[0] = svlastb_u64(pg1, result_vec);
          bit += 4 * width;
          byte = bit >> 3;
          shift = bit & 7;
          word = *addBytes(bits, byte) >> shift;
          word_vec = svdup_u64(word);
          result_vec = svbdep_u64(word_vec, kBdepMask16);
          words[1] = svlastb_u64(pg1, result_vec);
        } else {
          words[0] = bits::detail::loadBits<uint64_t>(
              bits, bitOffset + width * row, 64);
          words[1] = bits::detail::loadBits<uint64_t>(
              bits, bitOffset + width * (row + 4), 64);
          if constexpr (width == 15) {
            svuint64_t word_vec = svdup_u64(words[0]);
            svuint64_t result_vec = svbdep_u64(word_vec, kBdepMask16);
            words[0] = svlastb_u64(pg1, result_vec);
            word_vec = svdup_u64(words[1]);
            result_vec = svbdep_u64(word_vec, kBdepMask16);
            words[1] = svlastb_u64(pg1, result_vec);
          }
        }
        eightInts[0] = (words[0] >> 0) & 0xFFFF;
        eightInts[1] = (words[0] >> 16) & 0xFFFF;
        eightInts[2] = (words[0] >> 32) & 0xFFFF;
        eightInts[3] = (words[0] >> 48) & 0xFFFF;
        eightInts[4] = (words[1] >> 0) & 0xFFFF;
        eightInts[5] = (words[1] >> 16) & 0xFFFF;
        eightInts[6] = (words[1] >> 32) & 0xFFFF;
        eightInts[7] = (words[1] >> 48) & 0xFFFF;
      }
    } else {
      gather8Sparse_sve<width>(bits, bitOffset, rows, i, eightInts);
    }
    svint32_t eightInts_sve = svreinterpret_s32_u32(svld1_u32(pg8, eightInts));
    if constexpr (sizeof(T) <= 8) {
      store8Ints_sve(eightInts_sve, i, result);
    } else {
      for (int j = 0; j < 8; ++j) {
        result[i + j] = static_cast<T>(eightInts[j]);
      }
    }
  }
  return i;
}

#define WIDTH_CASE_SVE(width)                               \
  case width:                                               \
    i = decode1To24_sve<width>(                             \
        bits, bitOffset, rows.data(), numSafeRows, result); \
    break;

} // namespace

#endif

template <typename T>
void unpack(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    T* result) {
  uint64_t mask = bits::lowMask(bitWidth);

  if (bitWidth == 0) {
    // A column of dictionary indices can be 0 bits wide if all indices are 0.
    memset(result, 0, rows.size() * sizeof(T));
    return;
  }
  BOLT_DCHECK_NOT_NULL(bits);

  // We subtract rowBias * bitWidth bits from the starting position.
  bitOffset -= rowBias * bitWidth;
  if (bitOffset < 0) {
    // Decrement the pointer by enough bytes to have a non-negative bitOffset.
    auto bytes = bits::roundUp(-bitOffset, 8) / 8;
    bitOffset += bytes * 8;
    bits = reinterpret_cast<const uint64_t*>(
        reinterpret_cast<const char*>(bits) - bytes);
  }
  auto numRows = rows.size();
  if (bitWidth > 56) {
    for (auto i = 0; i < numRows; ++i) {
      auto bit = bitOffset + (rows[i]) * bitWidth;
      result[i] = bits::detail::loadBits<T>(bits, bit, bitWidth) & mask;
    }
    return;
  }
  auto FOLLY_NONNULL lastSafe = bufferEnd - sizeof(uint64_t);
  int32_t numSafeRows = numRows;
  bool anyUnsafe = false;
  if (bufferEnd) {
    const char* endByte = reinterpret_cast<const char*>(bits) +
        bits::roundUp(bitOffset + (rows.back() + 1) * bitWidth, 8) / 8;
    // redzone is the number of bytes at the end of the accessed range that
    // could overflow the buffer if accessed 64 its wide.
    int64_t redZone =
        sizeof(uint64_t) - static_cast<int64_t>(bufferEnd - endByte);
    if (redZone > 0) {
      anyUnsafe = true;
      auto numRed = (redZone + 1) * 8 / bitWidth;
      int32_t lastSafeIndex = rows.back() - numRed;
      --numSafeRows;
      for (; numSafeRows >= 1; --numSafeRows) {
        if (rows[numSafeRows - 1] < lastSafeIndex) {
          break;
        }
      }
    }
  }
  int32_t i = 0;

#if XSIMD_WITH_AVX2
  // Use AVX2 for specific widths.
  switch (bitWidth) {
    WIDTH_CASE(1);
    WIDTH_CASE(2);
    WIDTH_CASE(3);
    WIDTH_CASE(4);
    WIDTH_CASE(5);
    WIDTH_CASE(6);
    WIDTH_CASE(7);
    WIDTH_CASE(8);
    WIDTH_CASE(9);
    WIDTH_CASE(10);
    WIDTH_CASE(11);
    WIDTH_CASE(12);
    WIDTH_CASE(13);
    WIDTH_CASE(14);
    WIDTH_CASE(15);
    WIDTH_CASE(16);
    WIDTH_CASE(17);
    WIDTH_CASE(18);
    WIDTH_CASE(19);
    WIDTH_CASE(20);
    WIDTH_CASE(21);
    WIDTH_CASE(22);
    WIDTH_CASE(23);
    WIDTH_CASE(24);
    default:
      break;
  }
#endif

#if defined(__ARM_FEATURE_SVE) && defined(__aarch64__) && SVE_BITS == 256
  // Use SVE for specific widths.
  switch (bitWidth) {
    WIDTH_CASE_SVE(1);
    WIDTH_CASE_SVE(2);
    WIDTH_CASE_SVE(3);
    WIDTH_CASE_SVE(4);
    WIDTH_CASE_SVE(5);
    WIDTH_CASE_SVE(6);
    WIDTH_CASE_SVE(7);
    WIDTH_CASE_SVE(8);
    WIDTH_CASE_SVE(9);
    WIDTH_CASE_SVE(10);
    WIDTH_CASE_SVE(11);
    WIDTH_CASE_SVE(12);
    WIDTH_CASE_SVE(13);
    WIDTH_CASE_SVE(14);
    WIDTH_CASE_SVE(15);
    WIDTH_CASE_SVE(16);
    WIDTH_CASE_SVE(17);
    WIDTH_CASE_SVE(18);
    WIDTH_CASE_SVE(19);
    WIDTH_CASE_SVE(20);
    WIDTH_CASE_SVE(21);
    WIDTH_CASE_SVE(22);
    WIDTH_CASE_SVE(23);
    WIDTH_CASE_SVE(24);
    default:
      break;
  }
#endif

  for (; i < numSafeRows; ++i) {
    auto bit = bitOffset + (rows[i]) * bitWidth;
    auto byte = bit / 8;
    auto shift = bit & 7;
    result[i] = (*reinterpret_cast<const uint64_t*>(
                     reinterpret_cast<const char*>(bits) + byte) >>
                 shift) &
        mask;
  }
  if (anyUnsafe) {
    auto lastSafeWord = bufferEnd - sizeof(uint64_t);
    BOLT_DCHECK(lastSafeWord);
    for (auto i_2 = numSafeRows; i_2 < numRows; ++i_2) {
      auto bit = bitOffset + (rows[i_2]) * bitWidth;
      auto byte = bit / 8;
      auto shift = bit & 7;
      result[i_2] = safeLoadBits(
                        reinterpret_cast<const char*>(bits) + byte,
                        shift,
                        bitWidth,
                        lastSafeWord) &
          mask;
    }
  }
}

template void unpack(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int32_t* result);

template void unpack(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int64_t* result);

template void unpack(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int128_t* result);

template void unpack(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int16_t* result);

} // namespace bytedance::bolt::dwio::common
