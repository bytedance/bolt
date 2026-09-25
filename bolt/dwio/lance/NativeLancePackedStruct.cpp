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

#include "bolt/dwio/lance/NativeLancePackedStruct.h"

#include <cstring>
#include <limits>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/process/ProcessBase.h"

namespace bytedance::bolt::lance::reader {
namespace {

void validateColumns(
    uint64_t rowWidth,
    const NativeLancePackedStructColumn* columns,
    uint32_t columnCount) {
  uint64_t expectedOffset = 0;
  for (uint32_t column = 0; column < columnCount; ++column) {
    BOLT_CHECK_EQ(columns[column].offset, expectedOffset);
    BOLT_CHECK_GT(columns[column].width, 0);
    BOLT_CHECK_NOT_NULL(columns[column].output);
    BOLT_CHECK_LE(
        expectedOffset,
        std::numeric_limits<uint64_t>::max() - columns[column].width);
    expectedOffset += columns[column].width;
  }
  BOLT_CHECK_EQ(expectedOffset, rowWidth);
}

void decodeScalarColumn(
    const uint8_t* packedRows,
    uint64_t rowBegin,
    uint64_t rowCount,
    uint64_t rowWidth,
    const NativeLancePackedStructColumn& column) {
  for (uint64_t row = rowBegin; row < rowCount; ++row) {
    std::memcpy(
        column.output + row * column.width,
        packedRows + row * rowWidth + column.offset,
        column.width);
  }
}

#if defined(__AVX2__)

void decodeFourInt64Columns(
    const uint8_t* packedRows,
    uint64_t rowCount,
    uint64_t rowWidth,
    const NativeLancePackedStructColumn* columns) {
  uint64_t row = 0;
  for (; row + 4 <= rowCount; row += 4) {
    const auto* input = packedRows + row * rowWidth + columns[0].offset;
    const auto row0 =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(input));
    const auto row1 =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(input + rowWidth));
    const auto row2 = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(input + 2 * rowWidth));
    const auto row3 = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(input + 3 * rowWidth));

    const auto low01 = _mm256_unpacklo_epi64(row0, row1);
    const auto high01 = _mm256_unpackhi_epi64(row0, row1);
    const auto low23 = _mm256_unpacklo_epi64(row2, row3);
    const auto high23 = _mm256_unpackhi_epi64(row2, row3);

    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(columns[0].output + row * 8),
        _mm256_permute2x128_si256(low01, low23, 0x20));
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(columns[1].output + row * 8),
        _mm256_permute2x128_si256(high01, high23, 0x20));
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(columns[2].output + row * 8),
        _mm256_permute2x128_si256(low01, low23, 0x31));
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(columns[3].output + row * 8),
        _mm256_permute2x128_si256(high01, high23, 0x31));
  }
  for (uint32_t column = 0; column < 4; ++column) {
    decodeScalarColumn(packedRows, row, rowCount, rowWidth, columns[column]);
  }
}

#endif

} // namespace

void decodeLancePackedStructScalar(
    const uint8_t* packedRows,
    uint64_t rowCount,
    uint64_t rowWidth,
    const NativeLancePackedStructColumn* columns,
    uint32_t columnCount) {
  validateColumns(rowWidth, columns, columnCount);
  for (uint32_t column = 0; column < columnCount; ++column) {
    decodeScalarColumn(packedRows, 0, rowCount, rowWidth, columns[column]);
  }
}

void decodeLancePackedStructAvx2(
    const uint8_t* packedRows,
    uint64_t rowCount,
    uint64_t rowWidth,
    const NativeLancePackedStructColumn* columns,
    uint32_t columnCount) {
  validateColumns(rowWidth, columns, columnCount);
#if defined(__AVX2__)
  uint32_t column = 0;
  while (column + 4 <= columnCount) {
    const auto& first = columns[column];
    bool isFourInt64 = first.width == sizeof(uint64_t);
    for (uint32_t lane = 1; lane < 4 && isFourInt64; ++lane) {
      isFourInt64 = columns[column + lane].width == sizeof(uint64_t) &&
          columns[column + lane].offset ==
              first.offset + lane * sizeof(uint64_t);
    }
    if (isFourInt64) {
      decodeFourInt64Columns(packedRows, rowCount, rowWidth, columns + column);
      column += 4;
    } else {
      decodeScalarColumn(packedRows, 0, rowCount, rowWidth, columns[column]);
      ++column;
    }
  }
  for (; column < columnCount; ++column) {
    decodeScalarColumn(packedRows, 0, rowCount, rowWidth, columns[column]);
  }
#else
  decodeLancePackedStructScalar(
      packedRows, rowCount, rowWidth, columns, columnCount);
#endif
}

void decodeLancePackedStruct(
    const uint8_t* packedRows,
    uint64_t rowCount,
    uint64_t rowWidth,
    const NativeLancePackedStructColumn* columns,
    uint32_t columnCount) {
#if defined(__AVX2__)
  if (process::hasAvx2()) {
    decodeLancePackedStructAvx2(
        packedRows, rowCount, rowWidth, columns, columnCount);
    return;
  }
#endif
  decodeLancePackedStructScalar(
      packedRows, rowCount, rowWidth, columns, columnCount);
}

} // namespace bytedance::bolt::lance::reader
