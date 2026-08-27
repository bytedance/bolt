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

/// L2 of the Cell shuffle writer and reader: the Encoding Loop kernels of
/// ColumnarPayloadFormat.md section 7 and the null-region bit helpers of
/// section 4.
///
/// Implemented from the spec alone: no dependency on the engine's existing
/// varint/bitpack code, and none on the test-only reference implementation.
/// Pure functions over caller-provided memory; nothing here allocates.

#pragma once

#include <cstdint>
#include <cstring>
#include <string>

#include "bolt/shuffle/sparksql/cell/CellTypes.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// EncodingByte: low 2 bits kind, high 6 bits parameter (spec section 7.3).
inline uint8_t makeEncodingByte(EncodingKind kind, uint32_t param) {
  return static_cast<uint8_t>(
      static_cast<uint8_t>(kind) | static_cast<uint8_t>(param << 2));
}

/// Encodes one Encoding Block of `count` source values (count * sizeof(T)
/// must be <= 64, a full block or the stream's tail). Chooses the smallest
/// legal body per spec section 12.4; ties resolve PLAIN, CONST_NARROW,
/// BIT_PACK, FOR_BIT_PACK in first-wins order, matching the reference
/// encoder. `out` must hold at least kMaxBlockBytes. Returns bytes written.
///
/// T is one of int16_t, int32_t, int64_t: exactly the Encoding Loop source
/// types (SmallInt / Integer, Date / Bigint, String lengths).
template <typename T>
uint32_t encodeBlock(const T* values, uint32_t count, uint8_t* out);

/// Decodes one Encoding Block holding `count` values. Applies the L1 header
/// and bounds rules (spec section 10.1, rules 13-17): on any violation
/// returns 0 and writes nothing. Otherwise fills dst[0, count) and returns
/// the bytes consumed (1 + body).
template <typename T>
uint32_t decodeBlock(const uint8_t* in, size_t inBytes, uint32_t count, T* dst);

/// Encodes a whole source array as the spec's block sequence: full 64-byte
/// source blocks then at most one tail block (spec section 7.2). Appends to
/// out. Used for stream tails at checkpoint time and by tests; the split hot
/// path encodes full blocks one at a time as its cache lines fill.
template <typename T>
void encodeStream(const T* values, size_t count, std::string& out);

/// Decodes an Encoding Loop stream of exactly `valueCount` values from
/// in[0, inBytes). The block sequence must consume the input exactly
/// (spec section 7.2 and rule 20). Returns false on any violation.
template <typename T>
bool decodeStream(const uint8_t* in, size_t inBytes, size_t valueCount, T* dst);

/// --- Null region helpers (spec section 4.2) ---

/// ceil(numColumns * 2 / 8) bytes of tags.
inline uint32_t nullTagBytes(uint32_t numColumns) {
  return (numColumns * 2 + 7) / 8;
}

/// Requires the tag byte's slot to be zero (tags buffer starts zeroed).
inline void setNullTag(uint8_t* tags, uint32_t col, NullTag tag) {
  tags[col / 4] |= static_cast<uint8_t>(
      static_cast<uint8_t>(tag) << ((col % 4) * 2));
}

inline NullTag getNullTag(const uint8_t* tags, uint32_t col) {
  return static_cast<NullTag>((tags[col / 4] >> ((col % 4) * 2)) & 0x03);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
