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


namespace detail {

/// Minimal two's-complement bit width of `value`: 1 for 0 and -1, up to 64.
inline uint32_t signedBitWidth(int64_t value) {
  return 64 - static_cast<uint32_t>(__builtin_clrsbll(value));
}

/// Bits needed for an unsigned delta; 0 for 0.
inline uint32_t unsignedBitWidth(uint64_t value) {
  return value == 0 ? 0 : 64 - static_cast<uint32_t>(__builtin_clzll(value));
}

/// Smallest byte count whose sign extension reproduces `value`, capped at
/// maxBytes (always reachable: maxBytes bytes hold the full value).
inline uint32_t narrowBytesFor(int64_t value, uint32_t maxBytes) {
  const uint32_t bytes = (signedBitWidth(value) + 7) / 8;
  return bytes < maxBytes ? bytes : maxBytes;
}

/// Packs `count` staged values, `bits` wide each (1..63), LSB-first into
/// out. Writes ceil(count * bits / 8) bytes; trailing bits stay zero. The
/// staging split is deliberate: masking / delta subtraction is data-parallel
/// and auto-vectorizes, while this bit-stitch is a serial dependency chain
/// the compiler can only unroll.
inline uint8_t* packBits(
    const uint64_t* staged,
    uint32_t count,
    uint32_t bits,
    uint8_t* out) {
  uint64_t acc = 0;
  uint32_t accBits = 0;
  for (uint32_t i = 0; i < count; ++i) {
    const uint64_t v = staged[i];
    acc |= v << accBits;
    accBits += bits;
    if (accBits >= 64) {
      ::memcpy(out, &acc, 8);
      out += 8;
      accBits -= 64;
      acc = accBits == 0 ? 0 : v >> (bits - accBits);
    }
  }
  if (accBits > 0) {
    const uint32_t bytes = (accBits + 7) / 8;
    ::memcpy(out, &acc, bytes);
    out += bytes;
  }
  return out;
}

inline void storeLeEnc(uint8_t* out, uint64_t value, uint32_t bytes) {
  ::memcpy(out, &value, bytes);
}

template <typename T, typename Count>
inline uint32_t encodeBlockImpl(const T* values, const Count count, uint8_t* out) {
  constexpr uint32_t kWidth = sizeof(T);
  constexpr uint32_t kMaxPackBits = kWidth * 8 < 63 ? kWidth * 8 : 63;
  const uint32_t sourceBytes = count * kWidth;

  // Two independent reductions vectorize; the equality test folds into
  // them (all values equal iff min == max).
  T minValue = values[0];
  T maxValue = values[0];
  for (uint32_t i = 1; i < count; ++i) {
    const T value = values[i];
    minValue = value < minValue ? value : minValue;
    maxValue = value > maxValue ? value : maxValue;
  }
  const bool allEqual = minValue == maxValue;

  // Candidate body sizes (spec section 7.3); selection mirrors the reference
  // encoder: strict improvement in PLAIN, CONST, BIT_PACK, FOR order.
  const uint32_t constBytes =
      allEqual ? narrowBytesFor(static_cast<int64_t>(values[0]), kWidth) : 0;
  const uint32_t packBitsNeeded = signedBitWidth(static_cast<int64_t>(minValue)) >
          signedBitWidth(static_cast<int64_t>(maxValue))
      ? signedBitWidth(static_cast<int64_t>(minValue))
      : signedBitWidth(static_cast<int64_t>(maxValue));
  const uint32_t bitWidth = packBitsNeeded <= kMaxPackBits ? packBitsNeeded : 0;
  const uint64_t maxDelta = static_cast<uint64_t>(maxValue) -
      static_cast<uint64_t>(minValue);
  const uint32_t deltaBits = unsignedBitWidth(maxDelta);

  uint32_t bestSize = sourceBytes;
  EncodingKind bestKind = EncodingKind::kPlain;
  if (constBytes != 0 && constBytes < bestSize) {
    bestSize = constBytes;
    bestKind = EncodingKind::kConstNarrow;
  }
  if (bitWidth != 0) {
    const uint32_t size = (count * bitWidth + 7) / 8;
    if (size < bestSize) {
      bestSize = size;
      bestKind = EncodingKind::kBitPack;
    }
  }
  if (deltaBits <= 63) {
    const uint32_t size = kWidth + (count * deltaBits + 7) / 8;
    if (size < bestSize) {
      bestSize = size;
      bestKind = EncodingKind::kForBitPack;
    }
  }

  uint8_t* pos = out;
  switch (bestKind) {
    case EncodingKind::kConstNarrow:
      *pos++ = makeEncodingByte(EncodingKind::kConstNarrow, constBytes);
      storeLeEnc(pos, static_cast<uint64_t>(static_cast<int64_t>(values[0])), constBytes);
      pos += constBytes;
      break;
    case EncodingKind::kBitPack: {
      *pos++ = makeEncodingByte(EncodingKind::kBitPack, bitWidth);
      const uint64_t mask = (uint64_t{1} << bitWidth) - 1;
      uint64_t staged[kBlockSourceBytes / sizeof(int16_t)];
      for (uint32_t i = 0; i < count; ++i) { // widen + mask: vectorizes
        staged[i] = static_cast<uint64_t>(static_cast<int64_t>(values[i])) & mask;
      }
      pos = packBits(staged, count, bitWidth, pos);
      break;
    }
    case EncodingKind::kForBitPack: {
      *pos++ = makeEncodingByte(EncodingKind::kForBitPack, deltaBits);
      storeLeEnc(pos, static_cast<uint64_t>(static_cast<int64_t>(minValue)), kWidth);
      pos += kWidth;
      if (deltaBits > 0) {
        const uint64_t base = static_cast<uint64_t>(minValue);
        uint64_t staged[kBlockSourceBytes / sizeof(int16_t)];
        for (uint32_t i = 0; i < count; ++i) { // widen + subtract: vectorizes
          staged[i] = static_cast<uint64_t>(values[i]) - base;
        }
        pos = packBits(staged, count, deltaBits, pos);
      }
      break;
    }
    case EncodingKind::kPlain:
      *pos++ = makeEncodingByte(EncodingKind::kPlain, 0);
      ::memcpy(pos, values, sourceBytes);
      pos += sourceBytes;
      break;
  }
  return static_cast<uint32_t>(pos - out);
}

} // namespace detail

/// Encodes one Encoding Block of `count` source values (count * sizeof(T)
/// must be <= 64, a full block or the stream's tail). Chooses the smallest
/// legal body per spec section 12.4; ties resolve PLAIN, CONST_NARROW,
/// BIT_PACK, FOR_BIT_PACK in first-wins order, matching the reference
/// encoder. `out` must hold at least kMaxBlockBytes. Returns bytes written.
///
/// Header-inline so the flush call sites can fold the encode in; the
/// compiler keeps or drops the inlining per site.
///
/// T is one of int16_t, int32_t, int64_t: exactly the Encoding Loop source
/// types (SmallInt / Integer, Date / Bigint, String lengths).
template <typename T>
inline uint32_t encodeBlock(const T* values, uint32_t count, uint8_t* out) {
  return detail::encodeBlockImpl(values, count, out);
}

/// encodeBlock for a full 64-source-byte block: the count is a compile-time
/// constant (8/16/32), so the scan and pack loops fully unroll. The split
/// hot path only ever flushes full blocks; tails appear at window close.
template <typename T>
inline uint32_t encodeBlockFull(const T* values, uint8_t* out) {
  return detail::encodeBlockImpl(
      static_cast<const T*>(
          __builtin_assume_aligned(values, kBlockSourceBytes)),
      std::integral_constant<uint32_t, kBlockSourceBytes / sizeof(T)>{},
      out);
}

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
