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

#include "bolt/dwio/lance/NativeLanceListOffsets.h"

#include <bit>
#include <limits>

#include <folly/lang/Bits.h>
#if defined(__AVX2__)
#include <immintrin.h>
#endif

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/Nulls.h"
#include "bolt/common/process/ProcessBase.h"

namespace bytedance::bolt::lance::reader {
namespace {

NativeLanceListOffsetDecodeResult decodeScalarRange(
    const char* encodedEnds,
    uint64_t begin,
    uint64_t count,
    uint64_t nullOffsetAdjustment,
    uint64_t firstItem,
    uint64_t previousItem,
    vector_size_t* offsets,
    vector_size_t* sizes,
    uint64_t* rawNulls) {
  bool hasNulls = false;
  for (uint64_t i = begin; i < count; ++i) {
    const auto encodedEnd = folly::Endian::little(
        folly::loadUnaligned<uint64_t>(encodedEnds + i * sizeof(uint64_t)));
    const auto isNull = encodedEnd >= nullOffsetAdjustment;
    const auto endItem =
        encodedEnd - (isNull ? nullOffsetAdjustment : uint64_t{0});
    BOLT_CHECK_LT(
        endItem,
        nullOffsetAdjustment,
        "Lance list offset contains more than one null adjustment");
    BOLT_CHECK_LE(previousItem, endItem);
    BOLT_CHECK_LE(
        previousItem - firstItem,
        static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()));
    BOLT_CHECK_LE(
        endItem - previousItem,
        static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()));
    offsets[i] = static_cast<vector_size_t>(previousItem - firstItem);
    sizes[i] = static_cast<vector_size_t>(endItem - previousItem);
    if (isNull) {
      hasNulls = true;
      if (rawNulls != nullptr) {
        bits::setNull(rawNulls, i);
      }
    }
    previousItem = endItem;
  }
  return {.lastItem = previousItem, .hasNulls = hasNulls};
}

#if defined(__AVX2__)

FOLLY_ALWAYS_INLINE __m128i narrowLow32(__m256i values) {
  const auto shuffled = _mm256_shuffle_epi32(values, _MM_SHUFFLE(2, 0, 2, 0));
  return _mm_unpacklo_epi64(
      _mm256_castsi256_si128(shuffled), _mm256_extracti128_si256(shuffled, 1));
}

FOLLY_ALWAYS_INLINE __m256i
unsignedGreaterThan(__m256i left, __m256i right, __m256i signBit) {
  return _mm256_cmpgt_epi64(
      _mm256_xor_si256(left, signBit), _mm256_xor_si256(right, signBit));
}

#endif

} // namespace

NativeLanceListOffsetDecodeResult decodeLanceListOffsetsScalar(
    const char* encodedEnds,
    uint64_t count,
    uint64_t nullOffsetAdjustment,
    uint64_t firstItem,
    vector_size_t* offsets,
    vector_size_t* sizes,
    uint64_t* rawNulls) {
  BOLT_CHECK_GT(nullOffsetAdjustment, 0);
  BOLT_CHECK_LT(firstItem, nullOffsetAdjustment);
  return decodeScalarRange(
      encodedEnds,
      0,
      count,
      nullOffsetAdjustment,
      firstItem,
      firstItem,
      offsets,
      sizes,
      rawNulls);
}

NativeLanceListOffsetDecodeResult decodeLanceListOffsetsAvx2(
    const char* encodedEnds,
    uint64_t count,
    uint64_t nullOffsetAdjustment,
    uint64_t firstItem,
    vector_size_t* offsets,
    vector_size_t* sizes,
    uint64_t* rawNulls) {
#if defined(__AVX2__)
  BOLT_CHECK_GT(nullOffsetAdjustment, 0);
  BOLT_CHECK_LT(firstItem, nullOffsetAdjustment);
  const auto signBit = _mm256_set1_epi64x(std::numeric_limits<int64_t>::min());
  const auto adjustment =
      _mm256_set1_epi64x(std::bit_cast<int64_t>(nullOffsetAdjustment));
  const auto adjustmentMinusOne =
      _mm256_set1_epi64x(std::bit_cast<int64_t>(nullOffsetAdjustment - 1));
  const auto first = _mm256_set1_epi64x(std::bit_cast<int64_t>(firstItem));
  const auto maxVectorSize =
      _mm256_set1_epi64x(std::numeric_limits<vector_size_t>::max());

  uint64_t previousItem = firstItem;
  bool hasNulls = false;
  uint64_t i = 0;
  for (; i + 4 <= count; i += 4) {
    const auto encoded = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(encodedEnds + i * sizeof(uint64_t)));
    const auto isNull =
        unsignedGreaterThan(encoded, adjustmentMinusOne, signBit);
    const auto normalized =
        _mm256_sub_epi64(encoded, _mm256_and_si256(isNull, adjustment));
    BOLT_CHECK_EQ(
        _mm256_movemask_pd(_mm256_castsi256_pd(
            unsignedGreaterThan(normalized, adjustmentMinusOne, signBit))),
        0,
        "Lance list offset contains more than one null adjustment");

    auto previous =
        _mm256_permute4x64_epi64(normalized, _MM_SHUFFLE(2, 1, 0, 0));
    previous = _mm256_blend_epi32(
        previous,
        _mm256_set1_epi64x(std::bit_cast<int64_t>(previousItem)),
        0x03);
    BOLT_CHECK_EQ(
        _mm256_movemask_pd(_mm256_castsi256_pd(
            unsignedGreaterThan(previous, normalized, signBit))),
        0,
        "Lance list offsets are not monotonic");

    const auto relativeOffsets = _mm256_sub_epi64(previous, first);
    const auto lengths = _mm256_sub_epi64(normalized, previous);
    BOLT_CHECK_EQ(
        _mm256_movemask_pd(_mm256_castsi256_pd(
            unsignedGreaterThan(relativeOffsets, maxVectorSize, signBit))),
        0,
        "Lance list offset exceeds Bolt vector_size_t");
    BOLT_CHECK_EQ(
        _mm256_movemask_pd(_mm256_castsi256_pd(
            unsignedGreaterThan(lengths, maxVectorSize, signBit))),
        0,
        "Lance list size exceeds Bolt vector_size_t");
    _mm_storeu_si128(
        reinterpret_cast<__m128i*>(offsets + i), narrowLow32(relativeOffsets));
    _mm_storeu_si128(
        reinterpret_cast<__m128i*>(sizes + i), narrowLow32(lengths));

    const auto nullMask =
        static_cast<uint32_t>(_mm256_movemask_pd(_mm256_castsi256_pd(isNull)));
    hasNulls |= nullMask != 0;
    if (rawNulls != nullptr && nullMask != 0) {
      for (uint32_t lane = 0; lane < 4; ++lane) {
        if ((nullMask & (1U << lane)) != 0) {
          bits::setNull(rawNulls, i + lane);
        }
      }
    }
    previousItem = static_cast<uint64_t>(_mm256_extract_epi64(normalized, 3));
  }
  const auto tail = decodeScalarRange(
      encodedEnds,
      i,
      count,
      nullOffsetAdjustment,
      firstItem,
      previousItem,
      offsets,
      sizes,
      rawNulls);
  return {.lastItem = tail.lastItem, .hasNulls = hasNulls || tail.hasNulls};
#else
  return decodeLanceListOffsetsScalar(
      encodedEnds,
      count,
      nullOffsetAdjustment,
      firstItem,
      offsets,
      sizes,
      rawNulls);
#endif
}

NativeLanceListOffsetDecodeResult decodeLanceListOffsets(
    const char* encodedEnds,
    uint64_t count,
    uint64_t nullOffsetAdjustment,
    uint64_t firstItem,
    vector_size_t* offsets,
    vector_size_t* sizes,
    uint64_t* rawNulls) {
#if defined(__AVX2__)
  if (folly::kIsLittleEndian && process::hasAvx2()) {
    return decodeLanceListOffsetsAvx2(
        encodedEnds,
        count,
        nullOffsetAdjustment,
        firstItem,
        offsets,
        sizes,
        rawNulls);
  }
#endif
  return decodeLanceListOffsetsScalar(
      encodedEnds,
      count,
      nullOffsetAdjustment,
      firstItem,
      offsets,
      sizes,
      rawNulls);
}

} // namespace bytedance::bolt::lance::reader
