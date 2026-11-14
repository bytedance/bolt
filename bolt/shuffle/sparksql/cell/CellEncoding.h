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

/// The Encoding Block kernels of the Cell shuffle writer and reader, and the
/// null body's tag helpers; CellFormat.h defines both formats.
///
/// Pure functions over caller-provided memory; nothing here allocates.
/// Header-only, so the block kernels inline into the stream loops and
/// specialize on the full-block value count.

#pragma once

#include <folly/Portability.h>
#include <folly/lang/Bits.h>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#include "bolt/shuffle/sparksql/cell/CellFormat.h"

#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace bytedance::bolt::shuffle::sparksql::cell {

/// EncodingByte: low 2 bits kind, high 6 bits parameter.
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

/// The width the writer packs fields of `bits` significant bits at: `bits`
/// up to kExactPackMaxBits, else whole bytes.
inline uint32_t packWidth(uint32_t bits) {
  return bits <= kExactPackMaxBits ? bits : (bits + 7) & ~7u;
}

/// Packs `count` staged values, `bits` wide each (1..63), LSB-first into
/// out. Packs ceil(count * bits / 8) bytes, trailing bits zero, and returns
/// their end; the last partial word goes out as one 8-byte store, so up to
/// 8 bytes from that end may be written (see encodeBlockImpl). The staging
/// split is deliberate: masking / delta subtraction is data-parallel
/// and auto-vectorizes, while this bit-stitch is a serial dependency chain
/// the compiler can only unroll.
BOLT_CELL_KERNEL inline uint8_t*
packBits(const uint64_t* staged, uint32_t count, uint32_t bits, uint8_t* out) {
  uint64_t acc = 0;
  uint32_t accBits = 0;
  for (uint32_t i = 0; i < count; ++i) {
    const uint64_t v = staged[i];
    acc |= v << accBits;
    accBits += bits;
    if (accBits >= 64) {
      folly::storeUnaligned<uint64_t>(out, acc);
      out += 8;
      accBits -= 64;
      acc = accBits == 0 ? 0 : v >> (bits - accBits);
    }
  }
  folly::storeUnaligned<uint64_t>(out, acc);
  return out + (accBits + 7) / 8;
}

/// Values one block holds at most: a line of SmallInts. A block of T holds
/// at most kCacheLineBytes / sizeof(T).
inline constexpr uint32_t kMaxBlockValues = kCacheLineBytes / sizeof(int16_t);

/// The low `bytes` bytes of `value`, little-endian host assumed.
inline void storeLowBytes(uint8_t* out, uint64_t value, uint32_t bytes) {
  ::memcpy(out, &value, bytes);
}

/// Encodes one block of `count` values at `out`, which must hold
/// kMaxBlockBytes: the kConstNarrow body and the packed tail go out as whole
/// 8-byte stores past the block's end. A kind is chosen only when strictly
/// smaller than the values' 64 bytes or fewer, so a kBitPack body is at most
/// 63 bytes and its last store ends by out + 65; a kForBitPack body after
/// its kWidth-byte base at most 63 - kWidth, its last store ending by
/// out + 72; kConstNarrow's by out + 9.
template <typename T, typename Count>
BOLT_CELL_KERNEL FOLLY_ALWAYS_INLINE uint32_t
encodeBlockImpl(const T* values, const Count count, uint8_t* out) {
  static_assert(
      kMaxBlockBytes >= 1 + kCacheLineBytes + sizeof(uint64_t),
      "an 8-byte store past a packed body stays within the block");
  constexpr uint32_t kWidth = sizeof(T);
  constexpr uint32_t kMaxPackBits = kWidth * 8 < 63 ? kWidth * 8 : 63;
  // The kBitPack width from which the writer stores kPlain instead.
  constexpr uint32_t kPlainFromBits =
      kWidth == 8 ? kBigintPlainMinBits : kMaxPackBits + 1;
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

  // The kinds' parameters and body sizes, from the minimum and maximum
  // alone. Widths past kExactPackMaxBits are whole bytes (packWidth), and
  // packSize is kBitPack's, or kPlain's from kPlainFromBits; it is never
  // above kPlain's.
  const uint32_t constBytes =
      allEqual ? narrowBytesFor(static_cast<int64_t>(values[0]), kWidth) : 0;
  const uint32_t exactWidth = std::max(
      signedBitWidth(static_cast<int64_t>(minValue)),
      signedBitWidth(static_cast<int64_t>(maxValue)));
  const bool wide = exactWidth > kExactPackMaxBits;
  const uint32_t bitWidth = packWidth(exactWidth);
  const uint32_t deltaBits = packWidth(unsignedBitWidth(
      static_cast<uint64_t>(maxValue) - static_cast<uint64_t>(minValue)));
  const uint32_t packSize =
      bitWidth < kPlainFromBits ? (count * bitWidth + 7) / 8 : sourceBytes;
  const uint32_t forSize = kWidth + (count * deltaBits + 7) / 8;

  // Equal values take kConstNarrow when it is smaller than they are.
  // Otherwise kForBitPack takes a block it makes smaller than packSize - a
  // wide one only below three quarters of it, a tie going to the
  // alternative, as a base per block scatters a recurring value's bytes
  // from block to block - and else kBitPack one it makes strictly smaller
  // than the values.
  const bool forWins = wide ? 4 * forSize < 3 * packSize : forSize < packSize;
  EncodingKind bestKind = EncodingKind::kPlain;
  if (packSize < sourceBytes) {
    bestKind = EncodingKind::kBitPack;
  }
  if (deltaBits <= 63 && forWins) {
    bestKind = EncodingKind::kForBitPack;
  }
  if (constBytes != 0 && constBytes < sourceBytes) {
    bestKind = EncodingKind::kConstNarrow;
  }

  uint8_t* pos = out;
  switch (bestKind) {
    case EncodingKind::kConstNarrow:
      *pos++ = makeEncodingByte(EncodingKind::kConstNarrow, constBytes);
      folly::storeUnaligned<uint64_t>(
          pos, static_cast<uint64_t>(static_cast<int64_t>(values[0])));
      pos += constBytes;
      break;
    case EncodingKind::kBitPack: {
      *pos++ = makeEncodingByte(EncodingKind::kBitPack, bitWidth);
      const uint64_t mask = (uint64_t{1} << bitWidth) - 1;
      uint64_t staged[kMaxBlockValues];
      for (uint32_t i = 0; i < count; ++i) { // widen + mask: vectorizes
        staged[i] =
            static_cast<uint64_t>(static_cast<int64_t>(values[i])) & mask;
      }
      pos = packBits(staged, count, bitWidth, pos);
      break;
    }
    case EncodingKind::kForBitPack: {
      *pos++ = makeEncodingByte(EncodingKind::kForBitPack, deltaBits);
      storeLowBytes(
          pos, static_cast<uint64_t>(static_cast<int64_t>(minValue)), kWidth);
      pos += kWidth;
      if (deltaBits > 0) {
        const uint64_t base = static_cast<uint64_t>(minValue);
        uint64_t staged[kMaxBlockValues];
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

/// Unpacks `count` values, `bits` wide each (1..63), LSB-first from `in`;
/// the caller has verified in holds ceil(count * bits / 8) bytes. `put(i, v)`
/// receives the raw unsigned bit pattern.
template <typename Count, typename Put>
inline void
unpackBits(const uint8_t* in, Count count, uint32_t bits, const Put& put) {
  const uint64_t mask = (uint64_t{1} << bits) - 1;
  // 128-bit accumulator: refilling by whole bytes can hold up to
  // bits - 1 + 8 = 70 pending bits when bits is large.
  unsigned __int128 acc = 0;
  uint32_t accBits = 0;
  for (uint32_t i = 0; i < count; ++i) {
    while (accBits < bits) {
      acc |= static_cast<unsigned __int128>(*in++) << accBits;
      accBits += 8;
    }
    put(i, static_cast<uint64_t>(acc) & mask);
    acc >>= bits;
    accBits -= bits;
  }
}

/// Sign-extends the low `bits` bits of `raw`.
inline int64_t signExtend(uint64_t raw, uint32_t bits) {
  const uint32_t shift = 64 - bits;
  return static_cast<int64_t>(raw << shift) >> shift;
}

/// `bytes` bytes as the low bytes of a u64, little-endian host assumed.
inline uint64_t loadLowBytes(const uint8_t* in, uint32_t bytes) {
  uint64_t value = 0;
  ::memcpy(&value, in, bytes);
  return value;
}

/// Sign-extends a validated 1..8-byte value without reading past its body.
/// Fixed-width loads avoid a partial stack store followed by a wider reload
/// for the common widths.
FOLLY_ALWAYS_INLINE int64_t loadSignedBytes(const uint8_t* in, uint32_t bytes) {
  switch (bytes) {
    case 1:
      return folly::loadUnaligned<int8_t>(in);
    case 2:
      return folly::loadUnaligned<int16_t>(in);
    case 4:
      return folly::loadUnaligned<int32_t>(in);
    case 8:
      return folly::loadUnaligned<int64_t>(in);
    default:
      return signExtend(loadLowBytes(in, bytes), bytes * 8);
  }
}

template <typename T, typename Count>
FOLLY_ALWAYS_INLINE uint32_t
decodeBlockImpl(const uint8_t* in, size_t inBytes, const Count count, T* dst) {
  constexpr uint32_t kWidth = sizeof(T);
  constexpr uint32_t kMaxPackBits = kWidth * 8 < 63 ? kWidth * 8 : 63;
  if (inBytes < 1) {
    return 0;
  }
  const uint8_t header = in[0];
  const auto kind = static_cast<EncodingKind>(header & 0x03);
  const uint32_t param = header >> 2;
  const uint8_t* body = in + 1;
  const size_t bodyAvail = inBytes - 1;

  switch (kind) {
    case EncodingKind::kConstNarrow: {
      // 1 <= param <= kWidth.
      if (param < 1 || param > kWidth || bodyAvail < param) {
        return 0;
      }
      const T value = static_cast<T>(loadSignedBytes(body, param));
      for (uint32_t i = 0; i < count; ++i) {
        dst[i] = value;
      }
      return 1 + param;
    }
    case EncodingKind::kBitPack: {
      // 1 <= param <= kMaxPackBits.
      if (param < 1 || param > kMaxPackBits) {
        return 0;
      }
      const size_t bodyBytes = (static_cast<size_t>(count) * param + 7) / 8;
      if (bodyAvail < bodyBytes) {
        return 0;
      }
      unpackBits(body, count, param, [&](uint32_t i, uint64_t raw) {
        dst[i] = static_cast<T>(signExtend(raw, param));
      });
      return static_cast<uint32_t>(1 + bodyBytes);
    }
    case EncodingKind::kForBitPack: {
      // param <= 63.
      if (param > 63 || bodyAvail < kWidth) {
        return 0;
      }
      const uint64_t base = static_cast<uint64_t>(
          signExtend(loadLowBytes(body, kWidth), kWidth * 8));
      if (param == 0) {
        const T value = static_cast<T>(base);
        for (uint32_t i = 0; i < count; ++i) {
          dst[i] = value;
        }
        return 1 + kWidth;
      }
      const size_t deltaBytes = (static_cast<size_t>(count) * param + 7) / 8;
      if (bodyAvail < kWidth + deltaBytes) {
        return 0;
      }
      unpackBits(body + kWidth, count, param, [&](uint32_t i, uint64_t delta) {
        dst[i] = static_cast<T>(base + delta);
      });
      return static_cast<uint32_t>(1 + kWidth + deltaBytes);
    }
    case EncodingKind::kPlain: {
      // kPlain's parameter must be zero.
      const size_t bodyBytes = static_cast<size_t>(count) * kWidth;
      if (param != 0 || bodyAvail < bodyBytes) {
        return 0;
      }
      ::memcpy(dst, body, bodyBytes);
      return static_cast<uint32_t>(1 + bodyBytes);
    }
  }
  return 0;
}

#if defined(__AVX2__)

/// Applies the decoded fields' final step on T lanes, in place: the base of
/// kForBitPack, or the sign extension of a kBitPack field narrower than T
/// as (x ^ m) - m with m its sign bit (AVX2 has no 64-bit arithmetic
/// shift).
template <typename T>
inline void finishFieldsAvx2(T* dst, uint32_t bits, bool isForBitPack, T base) {
  constexpr uint32_t kCount = kCacheLineBytes / sizeof(T);
  constexpr uint32_t kLanes = 32 / sizeof(T);
  if (!isForBitPack && bits == sizeof(T) * 8) {
    return;
  }
  __m256i addend;
  __m256i flip;
  if constexpr (sizeof(T) == 8) {
    addend = _mm256_set1_epi64x(isForBitPack ? base : int64_t{1} << (bits - 1));
  } else if constexpr (sizeof(T) == 4) {
    addend = _mm256_set1_epi32(
        isForBitPack ? base : static_cast<int32_t>(uint32_t{1} << (bits - 1)));
  } else {
    addend = _mm256_set1_epi16(
        isForBitPack ? base : static_cast<int16_t>(uint16_t{1} << (bits - 1)));
  }
  flip = isForBitPack ? _mm256_setzero_si256() : addend;
  for (uint32_t i = 0; i < kCount; i += kLanes) {
    auto* lane = reinterpret_cast<__m256i*>(dst + i);
    __m256i v = _mm256_xor_si256(_mm256_loadu_si256(lane), flip);
    if constexpr (sizeof(T) == 8) {
      v = isForBitPack ? _mm256_add_epi64(v, addend)
                       : _mm256_sub_epi64(v, addend);
    } else if constexpr (sizeof(T) == 4) {
      v = isForBitPack ? _mm256_add_epi32(v, addend)
                       : _mm256_sub_epi32(v, addend);
    } else {
      v = isForBitPack ? _mm256_add_epi16(v, addend)
                       : _mm256_sub_epi16(v, addend);
    }
    _mm256_storeu_si256(lane, v);
  }
}

/// Zero-extended `bits`-wide fields of a full block, LSB-first from `in`:
/// field i is gathered from byte (i * bits) / 8 and shifted right by the
/// rest. 32-bit lanes hold fields up to 25 bits (shift <= 7), 64-bit lanes
/// up to 57; every gather reads 8 bytes or fewer from a field's first byte.
template <typename T>
inline void unpackFieldsAvx2(const uint8_t* in, uint32_t bits, T* dst) {
  constexpr uint32_t kCount = kCacheLineBytes / sizeof(T);
  if (sizeof(T) == 8 || bits > 25) {
    const __m256i mask =
        _mm256_set1_epi64x(static_cast<int64_t>((uint64_t{1} << bits) - 1));
    const __m256i width = _mm256_set1_epi64x(bits);
    for (uint32_t i = 0; i < kCount; i += 4) {
      const __m256i bit = _mm256_mul_epu32(
          _mm256_add_epi64(
              _mm256_setr_epi64x(0, 1, 2, 3), _mm256_set1_epi64x(i)),
          width);
      __m256i v = _mm256_i64gather_epi64(
          reinterpret_cast<const long long*>(in), _mm256_srli_epi64(bit, 3), 1);
      v = _mm256_and_si256(
          _mm256_srlv_epi64(v, _mm256_and_si256(bit, _mm256_set1_epi64x(7))),
          mask);
      if constexpr (sizeof(T) == 8) {
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), v);
      } else {
        // 4 x u64 to 4 x u32: only int32 fields of 26..32 bits come here.
        const __m256i narrow = _mm256_permutevar8x32_epi32(
            v, _mm256_setr_epi32(0, 2, 4, 6, 1, 3, 5, 7));
        _mm_storeu_si128(
            reinterpret_cast<__m128i*>(dst + i),
            _mm256_castsi256_si128(narrow));
      }
    }
    return;
  }
  const __m256i mask =
      _mm256_set1_epi32(static_cast<int32_t>((uint32_t{1} << bits) - 1));
  const __m256i width = _mm256_set1_epi32(bits);
  for (uint32_t i = 0; i < kCount; i += 8) {
    const __m256i bit = _mm256_mullo_epi32(
        _mm256_add_epi32(
            _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7), _mm256_set1_epi32(i)),
        width);
    __m256i v = _mm256_i32gather_epi32(
        reinterpret_cast<const int*>(in), _mm256_srli_epi32(bit, 3), 1);
    v = _mm256_and_si256(
        _mm256_srlv_epi32(v, _mm256_and_si256(bit, _mm256_set1_epi32(7))),
        mask);
    if constexpr (sizeof(T) == 4) {
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), v);
    } else {
      // 8 x u32 holding at most 16 bits to 8 x u16.
      const __m256i packed =
          _mm256_permute4x64_epi64(_mm256_packus_epi32(v, v), 0xD8);
      _mm_storeu_si128(
          reinterpret_cast<__m128i*>(dst + i), _mm256_castsi256_si128(packed));
    }
  }
}

/// The AVX2 form of a full kBitPack or kForBitPack block. Returns 0, having
/// written nothing, when the block is not one it handles: another kind, a
/// width past its lanes (int64 fields over 57 bits), a malformed block, or
/// too little input after the block for the gathers' 8-byte reads (a
/// stream's last blocks). The scalar decoder then takes the block.
template <typename T>
FOLLY_ALWAYS_INLINE uint32_t
decodeFullBlockAvx2(const uint8_t* in, size_t inBytes, T* dst) {
  constexpr uint32_t kWidth = sizeof(T);
  constexpr uint32_t kCount = kCacheLineBytes / kWidth;
  constexpr uint32_t kMaxBits = kWidth == 8 ? 57 : kWidth * 8;
  if (inBytes < 1) {
    return 0;
  }
  const auto kind = static_cast<EncodingKind>(in[0] & 0x03);
  const uint32_t bits = in[0] >> 2;
  const bool isForBitPack = kind == EncodingKind::kForBitPack;
  if ((!isForBitPack && kind != EncodingKind::kBitPack) || bits < 1 ||
      bits > kMaxBits) {
    return 0;
  }
  const size_t headerBytes = 1 + (isForBitPack ? kWidth : 0);
  const size_t bodyBytes = (static_cast<size_t>(kCount) * bits + 7) / 8;
  // The last field's gather loads 8 bytes from its first byte.
  const size_t reach = ((static_cast<size_t>(kCount - 1) * bits) >> 3) + 8;
  if (inBytes < headerBytes ||
      inBytes - headerBytes < std::max(bodyBytes, reach)) {
    return 0;
  }
  T base{};
  if (isForBitPack) {
    base = static_cast<T>(signExtend(loadLowBytes(in + 1, kWidth), kWidth * 8));
  }
  unpackFieldsAvx2(in + headerBytes, bits, dst);
  finishFieldsAvx2(dst, bits, isForBitPack, base);
  return static_cast<uint32_t>(headerBytes + bodyBytes);
}

#endif

} // namespace detail

/// Encodes one Encoding Block of `count` source values (count * sizeof(T)
/// must be <= 64, a full block or the stream's tail). Chooses from the
/// values' minimum and maximum alone, so the bytes for given values are
/// deterministic. Equal values take kConstNarrow when it is smaller than
/// they are. Values within kExactPackMaxBits bits take the smallest body,
/// ties resolving kPlain, kBitPack, kForBitPack in first-wins order. Wider
/// ones pack at whole bytes, and so do residuals past kExactPackMaxBits.
/// Their alternative is kBitPack when strictly smaller than the values and,
/// for 8-byte values, narrower than kBigintPlainMinBits, else kPlain;
/// kForBitPack takes the block instead below three quarters of the
/// alternative's size, a tie going to the alternative. `out` must hold at
/// least kMaxBlockBytes. Returns bytes written.
///
/// T is one of int16_t, int32_t, int64_t: exactly the encoded stream source
/// types (SmallInt / Integer, Date / Bigint, Timestamp halves, String
/// lengths).
template <typename T>
inline uint32_t encodeBlock(const T* values, uint32_t count, uint8_t* out) {
  return detail::encodeBlockImpl(values, count, out);
}

/// encodeBlock for a full block, a kCacheLineBytes line: the count is a
/// compile-time constant (8/16/32), so the scan and pack loops fully unroll.
/// The split hot path only ever flushes full blocks; tails appear at window
/// close. Keep full-block encoding inlined as the split kernels specialize
/// on more input mappings.
template <typename T>
FOLLY_ALWAYS_INLINE uint32_t encodeBlockFull(const T* values, uint8_t* out) {
  return detail::encodeBlockImpl(
      static_cast<const T*>(__builtin_assume_aligned(values, kCacheLineBytes)),
      std::integral_constant<uint32_t, kCacheLineBytes / sizeof(T)>{},
      out);
}

/// Decodes one Encoding Block holding `count` values, count * sizeof(T) <=
/// kCacheLineBytes. Validates it fully - a legal parameter for its kind,
/// and a body within inBytes - and on any violation returns 0 and writes
/// nothing. Otherwise fills dst[0, count) and returns the bytes consumed
/// (1 + body).
///
/// T is one of int16_t, int32_t, int64_t, like encodeBlock. Keep the runtime
/// count decoder separate so stream cursors inline only their full blocks.
template <typename T>
FOLLY_NOINLINE uint32_t
decodeBlock(const uint8_t* in, size_t inBytes, uint32_t count, T* dst) {
  return detail::decodeBlockImpl(in, inBytes, count, dst);
}

/// decodeBlock for a full block, a kCacheLineBytes line: the count is a
/// compile-time constant (8/16/32), so the fill and unpack loops fully unroll.
/// Every block of a stream but its tail is full. With AVX2, kBitPack and
/// kForBitPack blocks unpack in vector lanes; the scalar path takes the
/// rest, and the last blocks of an input that the 8-byte gathers would
/// read past. Keep full-block decoding inside the stream cursor rather than
/// adding a call for each group of eight string lengths.
template <typename T>
FOLLY_ALWAYS_INLINE uint32_t
decodeBlockFull(const uint8_t* in, size_t inBytes, T* dst) {
#if defined(__AVX2__)
  if (const uint32_t consumed = detail::decodeFullBlockAvx2(in, inBytes, dst)) {
    return consumed;
  }
#endif
  return detail::decodeBlockImpl(
      in,
      inBytes,
      std::integral_constant<uint32_t, kCacheLineBytes / sizeof(T)>{},
      dst);
}

/// Encodes a whole source array as an encoded stream: full 64-byte source
/// blocks then at most one tail block. Appends to out. Used by tests only;
/// the split path encodes a block at a time as its cache lines fill.
template <typename T>
inline void encodeStream(const T* values, size_t count, std::string& out) {
  constexpr uint32_t kValuesPerBlock = kCacheLineBytes / sizeof(T);
  uint8_t block[kMaxBlockBytes];
  size_t i = 0;
  for (; i + kValuesPerBlock <= count; i += kValuesPerBlock) {
    const uint32_t bytes = encodeBlock(values + i, kValuesPerBlock, block);
    out.append(reinterpret_cast<const char*>(block), bytes);
  }
  if (i < count) {
    const uint32_t bytes =
        encodeBlock(values + i, static_cast<uint32_t>(count - i), block);
    out.append(reinterpret_cast<const char*>(block), bytes);
  }
}

/// Decodes an encoded stream of `valueCount` values in pieces, handed out
/// in arbitrary-sized reads. A block that straddles two reads is decoded
/// once and its rest staged here, so the caller never needs room for more
/// than one read.
template <typename T>
class StreamCursor {
 public:
  void reset(const uint8_t* in, size_t inBytes, uint64_t valueCount) {
    in_ = in;
    size_ = inBytes;
    offset_ = 0;
    valuesLeft_ = valueCount;
    stagedPos_ = 0;
    stagedCount_ = 0;
  }

  /// Writes the next `n` values to dst. False when the stream holds fewer
  /// values or a block is malformed.
  BOLT_CELL_KERNEL bool next(uint64_t n, T* dst) {
    const uint32_t fromStaged =
        std::min<uint64_t>(n, stagedCount_ - stagedPos_);
    ::memcpy(dst, staged_ + stagedPos_, fromStaged * sizeof(T));
    stagedPos_ += fromStaged;
    dst += fromStaged;
    n -= fromStaged;
    while (n > 0) {
      if (valuesLeft_ == 0) {
        return false;
      }
      const uint32_t blockValues =
          std::min<uint64_t>(kValuesPerBlock, valuesLeft_);
      T* target = n >= blockValues ? dst : staged_;
      const uint32_t consumed = blockValues == kValuesPerBlock
          ? decodeBlockFull(in_ + offset_, size_ - offset_, target)
          : decodeBlock(in_ + offset_, size_ - offset_, blockValues, target);
      if (consumed == 0) {
        return false;
      }
      offset_ += consumed;
      valuesLeft_ -= blockValues;
      if (target == dst) {
        dst += blockValues;
        n -= blockValues;
      } else {
        stagedCount_ = blockValues;
        stagedPos_ = static_cast<uint32_t>(n);
        ::memcpy(dst, staged_, n * sizeof(T));
        n = 0;
      }
    }
    return true;
  }

  /// Returns up to maxValues from the current Encoding Block, without
  /// combining its remainder with the next block. The values point into
  /// this cursor and stay valid until its next operation. May be mixed with
  /// next(). Returns 0 and clears values when maxValues is zero, no values
  /// remain, or the next block is malformed; a zero limit does not advance.
  BOLT_CELL_KERNEL FOLLY_NOINLINE uint32_t
  nextBlock(uint32_t maxValues, const T*& values) {
    values = nullptr;
    if (maxValues == 0) {
      return 0;
    }
    if (stagedPos_ == stagedCount_) {
      if (valuesLeft_ == 0) {
        return 0;
      }
      const uint32_t blockValues =
          std::min<uint64_t>(kValuesPerBlock, valuesLeft_);
      const uint32_t consumed = blockValues == kValuesPerBlock
          ? decodeBlockFull(in_ + offset_, size_ - offset_, staged_)
          : decodeBlock(in_ + offset_, size_ - offset_, blockValues, staged_);
      if (consumed == 0) {
        return 0;
      }
      offset_ += consumed;
      valuesLeft_ -= blockValues;
      stagedPos_ = 0;
      stagedCount_ = blockValues;
    }
    const uint32_t count = std::min(maxValues, stagedCount_ - stagedPos_);
    values = staged_ + stagedPos_;
    stagedPos_ += count;
    return count;
  }

  /// Once every value was read: whether the blocks consumed the stream
  /// exactly.
  bool finished() const {
    return valuesLeft_ == 0 && stagedPos_ == stagedCount_ && offset_ == size_;
  }

 private:
  static constexpr uint32_t kValuesPerBlock = kCacheLineBytes / sizeof(T);

  const uint8_t* in_{nullptr};
  size_t size_{0};
  size_t offset_{0};
  uint64_t valuesLeft_{0};
  uint32_t stagedPos_{0};
  uint32_t stagedCount_{0};
  T staged_[kValuesPerBlock];
};

/// --- Null body tag helpers ---

/// ceil(numColumns * 2 / 8) bytes of tags.
inline uint32_t nullTagBytes(uint32_t numColumns) {
  return (numColumns * 2 + 7) / 8;
}

/// Requires the tag byte's slot to be zero (tags buffer starts zeroed).
inline void setNullTag(uint8_t* tags, uint32_t col, NullTag tag) {
  tags[col / 4] |=
      static_cast<uint8_t>(static_cast<uint8_t>(tag) << ((col % 4) * 2));
}

inline NullTag getNullTag(const uint8_t* tags, uint32_t col) {
  return static_cast<NullTag>((tags[col / 4] >> ((col % 4) * 2)) & 0x03);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
