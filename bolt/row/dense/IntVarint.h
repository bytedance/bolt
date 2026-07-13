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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>

#include <folly/CPortability.h>
#include <folly/Likely.h>
#include <xsimd/xsimd.hpp>

#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64)
#include <immintrin.h>
#endif

#include "bolt/common/base/BitUtil.h"
#include "bolt/type/HugeInt.h"

// Integer varint codec for the dense row format. Organized bottom-up in five
// layers; each layer only calls the ones below it:
//
//   L1  raw varint        — LEB128 read/write (scalar / BMI2 / dispatchers)
//   L2  zigzag            — 64/128-bit sign-folding primitives
//   L3  nullable codec    — single-value wire mapping: null = 0x00, INT64_MIN
//                           sentinel, else varint(zigzag(adjust(v)))
//   L4  SIMD size kernels — per-batch encoded-size computation (xsimd lanes)
//   L5  column-level      — whole-column size sum / per-row scatter loops
//
// Naming: a `*Batch` suffix marks an L4 pure kernel over one SIMD batch;
// un-suffixed L5 functions loop over a whole array. The scalar L3 size math and
// the L4 SIMD kernels intentionally duplicate the same formula — SIMD main
// loops need a scalar tail, and encode correctness relies on the two agreeing.
namespace bytedance::bolt::row::detail {

// =============================================================================
// L1 — Raw varint (LEB128): scalar + BMI2 implementations and dispatchers.
// =============================================================================

// A varint byte carries 7 payload bits; the high bit (0x80) is the
// continuation flag. The final byte of a varint is the one with it clear.
constexpr uint64_t kVarintPayloadBits{0x7f};
// 8-byte-wide versions of the payload / continuation bit patterns, for the
// BMI2 pdep/pext paths that process 8 wire bytes per step.
constexpr uint64_t kVarintPayloadMask64{0x7f7f7f7f7f7f7f7fULL};
constexpr uint64_t kVarintContinuationMask64{0x8080808080808080ULL};

FOLLY_ALWAYS_INLINE bool varintIsLastByte(uint8_t b) {
  return (b & 0x80) == 0;
}

FOLLY_ALWAYS_INLINE size_t varintSize(uint64_t value) {
  const auto bits = 64 - __builtin_clzll(value | 1ULL);
  return static_cast<size_t>((bits + 6) / 7);
}

FOLLY_ALWAYS_INLINE uint8_t* writeVarintScalar(uint64_t value, uint8_t* out) {
  while (value >= 0x80) {
    *out++ = static_cast<uint8_t>(value) | 0x80;
    value >>= 7;
  }
  *out++ = static_cast<uint8_t>(value);
  return out;
}

// No bounds check: reads until the terminator, capped at 10 bytes by the
// varint structural limit (shift < 64). Over-reads only on malformed input;
// readVarint's single in <= end check validates the consumed length.
FOLLY_ALWAYS_INLINE bool readVarintScalar(const uint8_t*& in, uint64_t& value) {
  uint64_t result{0};
  uint32_t shift{0};
  while (shift < 64) {
    auto byte = *in++;
    result |= ((byte & kVarintPayloadBits) << shift);
    if (varintIsLastByte(byte)) {
      value = result;
      return true;
    }
    shift += 7;
  }
  return false;
}

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))

constexpr std::array<uint64_t, 9> makeVarintContinuationMasks() {
  std::array<uint64_t, 9> masks{};
  for (size_t len = 1; len < masks.size(); ++len) {
    uint64_t mask{0};
    for (size_t i = 0; i + 1 < len; ++i) {
      mask |= (0x80ULL << (i * 8));
    }
    masks[len] = mask;
  }
  return masks;
}

inline constexpr std::array<uint64_t, 9> kVarintContinuationMasks =
    makeVarintContinuationMasks();

inline __attribute__((target("bmi2"))) uint8_t* writeVarintBmi2(
    uint64_t value,
    uint8_t* out) {
  if (value < (1ULL << 56)) {
    const auto bits = 64 - __builtin_clzll(value | 1ULL);
    const auto len = static_cast<size_t>((bits + 6) / 7);

    uint64_t packed = _pdep_u64(value, kVarintPayloadMask64);
    // _pdep places only the 7 data bits. Set continuation bits (MSB=1) for
    // the first len - 1 bytes; the last byte keeps MSB=0.
    packed |= kVarintContinuationMasks[len];
    std::memcpy(out, &packed, len);
    return out + len;
  }

  // Values >= 2^63 require 10 bytes in unsigned varint form (e.g.
  // zigzag(INT64_MAX) == 2^64 - 2). Encode the first 8 bytes with BMI2,
  // then encode the remaining <=8 bits with scalar (1-2 bytes).
  uint64_t packed = _pdep_u64(value, kVarintPayloadMask64);
  packed |= kVarintContinuationMask64;
  std::memcpy(out, &packed, 8);
  out += 8;
  return writeVarintScalar(value >> 56, out);
}

inline __attribute__((target("bmi2"))) bool
readVarintBmi2(const uint8_t*& in, const uint8_t* end, uint64_t& value) {
  // `end - in >= 8` is a memory-safety guard, NOT a redundant validity check:
  // the 8-byte bulk load below would read past the buffer for a valid 5-7 byte
  // varint in the final bytes (buffers are sized exactly, no tail padding).
  // With < 8 bytes left, fall to the byte-at-a-time scalar reader. Truncation
  // is caught by readVarint's single in <= end check.
  if (end - in >= 8) {
    uint64_t word;
    std::memcpy(&word, in, sizeof(word));

    const uint64_t stopMask = (~word) & kVarintContinuationMask64;
    if (stopMask != 0) {
      const auto len =
          static_cast<size_t>((__builtin_ctzll(stopMask) >> 3) + 1);
      uint64_t decoded = _pext_u64(word, kVarintPayloadMask64);
      if (len < 8) {
        decoded &= ((1ULL << (len * 7)) - 1);
      }
      value = decoded;
      in += len;
      return true;
    }

    // 9-10 byte varint: the first 8 bytes all continue, so a well-formed varint
    // has its terminator within in[8..9] (in-bounds when end - in >= 9/10). No
    // per-byte truncation check; readVarint's in <= end catches a short input.
    uint64_t decoded = _pext_u64(word, kVarintPayloadMask64);
    auto* cursor = in + 8;
    const auto byte8 = *cursor++;
    decoded |= (static_cast<uint64_t>(byte8 & 0x7f) << 56);
    if ((byte8 & 0x80) == 0) {
      value = decoded;
      in = cursor;
      return true;
    }

    const auto byte9 = *cursor++;
    decoded |= (static_cast<uint64_t>(byte9 & 0x1) << 63);
    value = decoded;
    in = cursor;
    return true;
  }

  return readVarintScalar(in, value);
}

#endif

// Inlined fast path for varints up to 3 bytes (values 0..2^21-1 = 2_097_151).
// Covers the dominant cases in null-fused encodings:
//   - row markers (varint(0/1) → 1 byte)
//   - VARCHAR lengths up to ~2M
//   - BIGINT values in [-2^20, 2^20-1] after zigzag+adjust (covers lt_2pow8,
//     lt_2pow16, and ~half of lt_2pow32 entries)
//   - ARRAY/MAP cardinalities 0..2_097_151
//
// The BMI2 path costs an 8-byte load + tzcnt + pext (10-15 cycle dep chain).
// Inlining up to 3 byte checks (each ~2 cycles) keeps the dep chain short
// and lets the OoO window see much more parallelism across rows.
//
// On failure (4+ byte varint or truncated input), caller falls back to
// BMI2/scalar. No bounds checks here: a well-formed varint stops at its
// terminator within the buffer; reads run past `end` only on malformed input,
// bounded to 4 bytes, and readVarint's single in <= end check validates the
// consumed length.
// Each length below reconstructs its whole value inside its own return, so the
// fall-through path (5+ byte varint) does no value arithmetic and the 1-byte
// case skips the payload mask — this is why it stays hand-unrolled rather than
// looped (a loop that accumulated the value each step measured ~1% slower on
// decode). Earlier bytes (b0..b{k-1}) are known to carry continuation bits, so
// only their payload (low 7) bits contribute; the terminating byte is whole.
FOLLY_ALWAYS_INLINE bool readVarintShortFastPath(
    const uint8_t*& in,
    uint64_t& value) {
  constexpr uint64_t kP = kVarintPayloadBits;

  const uint8_t b0 = in[0];
  if (FOLLY_LIKELY(varintIsLastByte(b0))) { // 1 byte (< 2^7)
    value = b0;
    in += 1;
    return true;
  }

  const uint8_t b1 = in[1];
  if (FOLLY_LIKELY(varintIsLastByte(b1))) { // 2 bytes (< 2^14)
    value = (b0 & kP) | (uint64_t{b1} << 7);
    in += 2;
    return true;
  }

  const uint8_t b2 = in[2];
  if (FOLLY_LIKELY(varintIsLastByte(b2))) { // 3 bytes (< 2^21)
    value = (b0 & kP) | ((b1 & kP) << 7) | (uint64_t{b2} << 14);
    in += 3;
    return true;
  }

  const uint8_t b3 = in[3];
  if (FOLLY_LIKELY(varintIsLastByte(b3))) { // 4 bytes (< 2^28)
    value =
        (b0 & kP) | ((b1 & kP) << 7) | ((b2 & kP) << 14) | (uint64_t{b3} << 21);
    in += 4;
    return true;
  }
  return false;
}

FOLLY_ALWAYS_INLINE uint8_t* writeVarint(uint64_t value, uint8_t* out) {
  if (value < 0x80) {
    *out++ = static_cast<uint8_t>(value);
    return out;
  }

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
  if (value >= (1ULL << 35)) {
    return writeVarintBmi2(value, out);
  }
#endif
  return writeVarintScalar(value, out);
}

// Bounds are validated once here: the inner readers parse optimistically (no
// per-byte end checks) and may run a few bytes past `end` on malformed input;
// the single in <= end check rejects any read that over-ran the buffer.
FOLLY_ALWAYS_INLINE bool
readVarint(const uint8_t*& in, const uint8_t* end, uint64_t& value) {
  if (FOLLY_UNLIKELY(in >= end)) {
    return false;
  }
  if (FOLLY_LIKELY(readVarintShortFastPath(in, value))) {
    return in <= end;
  }
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
  return readVarintBmi2(in, end, value) && in <= end;
#else
  return readVarintScalar(in, value) && in <= end;
#endif
}

// =============================================================================
// L2 — ZigZag sign folding (64- and 128-bit).
// =============================================================================

FOLLY_ALWAYS_INLINE constexpr uint64_t zigZagEncode64(int64_t value) {
  return (static_cast<uint64_t>(value) << 1) ^
      static_cast<uint64_t>(value >> 63);
}

FOLLY_ALWAYS_INLINE constexpr int64_t zigZagDecode64(uint64_t encoded) {
  return static_cast<int64_t>((encoded >> 1) ^ (0 - (encoded & 1)));
}

FOLLY_ALWAYS_INLINE constexpr uint128_t zigZagEncode128(int128_t value) {
  return (static_cast<uint128_t>(value) << 1) ^
      static_cast<uint128_t>(value >> 127);
}

FOLLY_ALWAYS_INLINE constexpr int128_t zigZagDecode128(uint128_t encoded) {
  return static_cast<int128_t>(
      (encoded >> 1) ^ (~static_cast<uint128_t>(0) * (encoded & 1)));
}

// =============================================================================
// L3 — Nullable-int wire codec (single value).
// =============================================================================

// Nullable int64 wire mapping:
// - null -> 0x00.
// - INT64_MIN -> 0x80 0x00 (reserved sentinel).
// - all other values -> varint(zigzag(adjust(v))), where adjust(v) keeps
//   positive values unchanged and shifts non-positive values by -1.
//
// The reserved sentinel keeps null as a single-byte marker while preserving a
// one-to-one mapping for the full int64 domain.
constexpr std::array<uint8_t, 2> kInt64MinSentinel{0x80, 0x00};

FOLLY_ALWAYS_INLINE constexpr bool needsExtendedInt64Encoding(int64_t value) {
  return value == std::numeric_limits<int64_t>::min();
}

FOLLY_ALWAYS_INLINE constexpr int64_t adjustInt64ForNullableEncoding(
    int64_t value) {
  return value > 0 ? value : value - 1;
}

FOLLY_ALWAYS_INLINE constexpr int64_t restoreInt64FromNullableEncoding(
    int64_t value) {
  return value > 0 ? value : value + 1;
}

FOLLY_ALWAYS_INLINE size_t
nullableInt64SerializedSize(int64_t value, bool isNull) {
  if (isNull) {
    return 1;
  }

  if (needsExtendedInt64Encoding(value)) {
    return 2;
  }

  // size == ceil((bitlen(|v|)+1)/7) == varintSize(zigzag(adjust(v))) — the wire
  // mapping only moves which 2^(7k-1) bucket the value lands in, which |v|
  // already captures, so we skip the zigzag/adjust and clz |v| directly.
  // INT64_MIN is excluded above, so the unsigned abs is exact.
  const auto uv = static_cast<uint64_t>(value);
  const auto sign = static_cast<uint64_t>(value >> 63);
  const uint64_t mag = (uv ^ sign) - sign; // |value|, no signed-overflow UB
  const auto bits = 64 - __builtin_clzll(mag | 1ULL);
  return static_cast<size_t>((bits + 7) / 7);
}

FOLLY_ALWAYS_INLINE uint8_t*
writeNullableInt64(int64_t value, bool isNull, uint8_t* out) {
  if (isNull) {
    *out++ = 0;
    return out;
  }

  if (FOLLY_UNLIKELY(needsExtendedInt64Encoding(value))) {
    *out++ = kInt64MinSentinel[0];
    *out++ = kInt64MinSentinel[1];
    return out;
  }

  return writeVarint(
      zigZagEncode64(adjustInt64ForNullableEncoding(value)), out);
}

FOLLY_ALWAYS_INLINE bool readNullableInt64(
    const uint8_t*& in,
    const uint8_t* end,
    bool& isNull,
    int64_t& value) {
  if (FOLLY_UNLIKELY(in >= end)) {
    return false;
  }
  if (*in == 0) {
    ++in;
    isNull = true;
    value = 0;
    return in <= end;
  }

  if (FOLLY_UNLIKELY(
          in[0] == kInt64MinSentinel[0] && in[1] == kInt64MinSentinel[1])) {
    in += 2;
    isNull = false;
    value = std::numeric_limits<int64_t>::min();
    return in <= end;
  }

  uint64_t encoded{0};
  if (!readVarint(in, end, encoded)) {
    return false;
  }

  isNull = false;
  value = restoreInt64FromNullableEncoding(zigZagDecode64(encoded));
  return in <= end;
}

// Nullable int128 wire mapping (two halves of zigzag128(v), no separate tag):
//   null     -> nullableInt64(_, null)            (a single 0x00 byte)
//   non-null -> nullableInt64(low 64 of zigzag128(v)), varint(high 64).
// The null marker is folded into the low int64 slot via the nullable-int64
// codec's own 0x00 sentinel, so there is no extra present/null tag byte: a
// non-null value's low half just rides the same slot, and the high half follows
// only when present. zigzag128 keeps small-magnitude values (either sign)
// short, and the two halves reuse the 64-bit varint path (no 128-bit varint).
// The low half is reinterpreted as int64 for the nullable-int64 codec; that
// round-trips bit-for-bit (it is a bijection over int64 plus null).
FOLLY_ALWAYS_INLINE size_t
nullableInt128SerializedSize(int128_t value, bool isNull) {
  if (isNull) {
    return nullableInt64SerializedSize(0, /*isNull=*/true);
  }
  const uint128_t zz = zigZagEncode128(value);
  return nullableInt64SerializedSize(
             static_cast<int64_t>(static_cast<uint64_t>(zz)),
             /*isNull=*/false) +
      varintSize(static_cast<uint64_t>(zz >> 64));
}

FOLLY_ALWAYS_INLINE uint8_t*
writeNullableInt128(int128_t value, bool isNull, uint8_t* out) {
  if (isNull) {
    return writeNullableInt64(0, /*isNull=*/true, out);
  }
  const uint128_t zz = zigZagEncode128(value);
  out = writeNullableInt64(
      static_cast<int64_t>(static_cast<uint64_t>(zz)), /*isNull=*/false, out);
  return writeVarint(static_cast<uint64_t>(zz >> 64), out);
}

FOLLY_ALWAYS_INLINE bool readNullableInt128(
    const uint8_t*& in,
    const uint8_t* end,
    bool& isNull,
    int128_t& value) {
  int64_t low{0};
  if (!readNullableInt64(in, end, isNull, low)) {
    return false;
  }
  if (isNull) {
    value = 0;
    return true;
  }
  uint64_t hi{0};
  if (!readVarint(in, end, hi)) {
    return false;
  }
  value = zigZagDecode128(
      (static_cast<uint128_t>(hi) << 64) | static_cast<uint64_t>(low));
  return true;
}

// =============================================================================
// L4 — Portable (xsimd) nullable-int size kernels: encoded byte counts for one
// SIMD batch of values. Two kernels: int32 and int64; int8/int16 widen to
// int32. The size math is pure xsimd::batch, so it runs on AVX2 / AVX-512 /
// SSE / NEON (compatibility). These must agree with the scalar
// nullableInt64SerializedSize above.
// =============================================================================

// int32: width-adaptive, native uint32 lanes. zigzag of an int32 fits uint32
// for every value except INT32_MIN (special-cased to 5). 4 thresholds; xsimd's
// unsigned-batch comparison handles the unsigned compare portably.
FOLLY_ALWAYS_INLINE xsimd::batch<uint32_t> nullableInt32SizesBatch(
    xsimd::batch<int32_t> v) {
  using S = xsimd::batch<int32_t>;
  using U = xsimd::batch<uint32_t>;
  const S zero(0);
  const S adj = v - S(v <= zero); // v > 0 ? v : v - 1
  const U zz = xsimd::bitwise_cast<U>((adj << 1) ^ (adj >> 31)); // zigzag
  U s(1);
  s += U(zz > U(static_cast<uint32_t>((1 << 7) - 1)));
  s += U(zz > U(static_cast<uint32_t>((1 << 14) - 1)));
  s += U(zz > U(static_cast<uint32_t>((1 << 21) - 1)));
  s += U(zz > U(static_cast<uint32_t>((1 << 28) - 1)));
  return xsimd::select(
      xsimd::batch_bool_cast<uint32_t>(
          v == S(std::numeric_limits<int32_t>::min())),
      U(5),
      s);
}

// int64 size kernel, computed straight from |v| (no zigzag/adjust): size(v) =
// min k with |v| < 2^(7k-1), i.e. threshold |v| against {2^6, 2^13, ... 2^62}.
// The >=2^63 (10-byte) case is just the top threshold — no separate sign fixup.
// INT64_MIN's abs overflows back to a negative value, so all thresholds miss
// (s=1) and the final select sets it to the 2-byte sentinel. Branchless.
//
// This abs form measured ~20% faster than an equivalent zigzag-based kernel (9
// magnitude thresholds replace zigzag's 8 thresholds + a zz<0 select, and they
// sit on a shorter dependency chain since |v| is cheaper to derive than the
// zigzag key), so it is the single int64 size kernel used everywhere.
FOLLY_ALWAYS_INLINE xsimd::batch<int64_t> nullableInt64SizesBatch(
    xsimd::batch<int64_t> v) {
  using B = xsimd::batch<int64_t>;
  const B sign = v >> 63;
  const B m = (v ^ sign) - sign; // abs(v); INT64_MIN stays negative
  // 9 magnitude thresholds (emulating a clz, which AVX2 can't vectorize). The
  // serial `s +=` is not the bottleneck — the compiler reassociates these
  // associative adds into a tree, and an explicit tree measured identically.
  B s = B(1);
  s += B(m > B((1LL << 6) - 1));
  s += B(m > B((1LL << 13) - 1));
  s += B(m > B((1LL << 20) - 1));
  s += B(m > B((1LL << 27) - 1));
  s += B(m > B((1LL << 34) - 1));
  s += B(m > B((1LL << 41) - 1));
  s += B(m > B((1LL << 48) - 1));
  s += B(m > B((1LL << 55) - 1));
  s += B(m > B((1LL << 62) - 1));
  return xsimd::select(v == B(std::numeric_limits<int64_t>::min()), B(2), s);
}

// =============================================================================
// L5 — Column-level loops: size sums and per-row scatters over whole arrays,
// built on the L4 kernels with scalar (L3) tails.
//
// Narrow int8/int16 inputs widen to the int32 size kernel via xsimd's
// converting load `batch<int32_t>::load_unaligned(const T*)`, which reads
// batch<int32_t>::size narrow values and sign-extends each to an int32 lane.
// It picks the right widening per ISA (AVX2/AVX-512/SSE/NEON) with no
// target-specific intrinsics; for int32 input it is a plain load.
// =============================================================================

// Sum of nullable-int sizes for a contiguous non-null range of any of
// int8/int16/int32/int64. Narrow types widen to the int32 kernel. int64 gets a
// testz-style fast path: if every zigzag in a batch fits 32 bits (common
// small-magnitude BIGINT) only 4 thresholds are needed; xsimd::all keeps it
// portable.
template <typename T>
FOLLY_ALWAYS_INLINE size_t sumNullableIntSizes(const T* raw, size_t count) {
  static_assert(
      std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
          std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>,
      "sumNullableIntSizes supports int8/int16/int32/int64");
  if constexpr (std::is_same_v<T, int64_t>) {
    using B = xsimd::batch<int64_t>;
    constexpr std::size_t kBatchSize = B::size;
    const B zero(0);
    const B one(1);
    B acc(0);
    std::size_t j = 0;
    for (; j + kBatchSize <= count; j += kBatchSize) {
      const B v = B::load_unaligned(raw + j);
      const B adj = v - B(v <= zero);
      const B zz = (adj << 1) ^ (adj >> 63);
      if (xsimd::all((zz >> 32) == zero)) {
        B s = one;
        s += B(zz > B((1LL << 7) - 1));
        s += B(zz > B((1LL << 14) - 1));
        s += B(zz > B((1LL << 21) - 1));
        s += B(zz > B((1LL << 28) - 1));
        acc += s;
      } else {
        acc += nullableInt64SizesBatch(v);
      }
    }
    auto total = static_cast<size_t>(xsimd::reduce_add(acc));
    for (; j < count; ++j) {
      total += nullableInt64SerializedSize(raw[j], false);
    }
    return total;
  } else {
    using U = xsimd::batch<uint32_t>;
    constexpr std::size_t kBatchSize = xsimd::batch<int32_t>::size;
    U acc(0U);
    std::size_t j = 0;
    for (; j + kBatchSize <= count; j += kBatchSize) {
      acc += nullableInt32SizesBatch(
          xsimd::batch<int32_t>::load_unaligned(raw + j));
    }
    auto total = static_cast<size_t>(xsimd::reduce_add(acc));
    for (; j < count; ++j) {
      total += nullableInt64SerializedSize(static_cast<int64_t>(raw[j]), false);
    }
    return total;
  }
}

// Per-row scatter for a non-null column: add each value's size into its own
// rowSizes[r].
template <typename T>
FOLLY_ALWAYS_INLINE void
addNoNullIntColumnSizes(const T* raw, size_t* rowSizes, size_t count) {
  std::size_t j = 0;
  if constexpr (std::is_same_v<T, int64_t>) {
    // Sizes are int64 already: add the batch straight into rowSizes (portable).
    // Branchless (no testz fastpath): a per-batch "all small" branch regresses
    // mixed/full-range BIGINT via misprediction, which dominates the
    // small-value saving.
    using B = xsimd::batch<int64_t>;
    constexpr std::size_t kWidth = B::size;
    auto* rs = reinterpret_cast<int64_t*>(rowSizes);
    for (; j + kWidth <= count; j += kWidth) {
      B sz = nullableInt64SizesBatch(B::load_unaligned(raw + j));
      (B::load_unaligned(rs + j) + sz).store_unaligned(rs + j);
    }
  } else {
    constexpr std::size_t kWidth = xsimd::batch<int32_t>::size;
    alignas(64) uint32_t sz[kWidth];
    for (; j + kWidth <= count; j += kWidth) {
      nullableInt32SizesBatch(xsimd::batch<int32_t>::load_unaligned(raw + j))
          .store_aligned(sz);
      for (std::size_t k = 0; k < kWidth; ++k) {
        rowSizes[j + k] += sz[k];
      }
    }
  }
  // reset of rows
  for (; j < count; ++j) {
    rowSizes[j] +=
        nullableInt64SerializedSize(static_cast<int64_t>(raw[j]), false);
  }
}

// Overload for a FLAT int column WITH nulls (the common Spark case that
// otherwise falls to the scalar loop). A null row contributes exactly 1 byte
// (the 0x00 marker), independent of the (garbage) value stored at its slot.
// `nulls` is the row-indexed validity bitmap (bit set = non-null), which the
// caller guarantees non-null. Supports int8/int16/int32/int64; narrow types
// widen to the int32 size kernel exactly like addNoNullIntColumnSizes.
template <typename T>
FOLLY_ALWAYS_INLINE void addNullableIntColumnSizes(
    const T* raw,
    const uint64_t* nulls,
    size_t* rowSizes,
    size_t count) {
  std::size_t j = 0;
  if constexpr (std::is_same_v<T, int64_t>) {
    using B = xsimd::batch<int64_t>;
    constexpr std::size_t kBatchSize = B::size;
    // Validity bitmaps are addressed in 64-bit words.
    constexpr std::size_t kWordBits = 64;
    auto* rs = reinterpret_cast<int64_t*>(rowSizes);

    // Per-lane bit selector {1<<0, 1<<1, ...}, built once.
    int64_t selArr[kBatchSize];
    for (std::size_t i = 0; i < kBatchSize; ++i) {
      selArr[i] = static_cast<int64_t>(int64_t{1} << i);
    }
    const B laneSel = B::load_aligned(selArr);
    const B one(1);

    for (; j + kBatchSize <= count; j += kBatchSize) {
      B sz = nullableInt64SizesBatch(B::load_unaligned(raw + j));
      // kBatchSize validity bits for rows [j, j+kBatchSize). kBatchSize divides
      // kWordBits and j is a multiple of kBatchSize, so the bits never straddle
      // a word.
      const uint64_t word = nulls[j / kWordBits];
      const uint64_t validBits = (word >> (j % kWordBits)) &
          bits::lowMask(static_cast<int32_t>(kBatchSize));
      // lane i valid iff bit i set: (broadcast(validBits) & laneSel) ==
      // laneSel.
      const auto isValid =
          (B(static_cast<int64_t>(validBits)) & laneSel) == laneSel;
      sz = xsimd::select(isValid, sz, one); // null -> 1 byte
      (B::load_unaligned(rs + j) + sz).store_unaligned(rs + j);
    }
  } else {
    // Narrow int8/int16/int32: widen to the int32 size kernel, then override
    // null lanes to 1 byte.
    constexpr std::size_t kWidth = xsimd::batch<int32_t>::size;
    uint32_t sz[kWidth];
    for (; j + kWidth <= count; j += kWidth) {
      nullableInt32SizesBatch(xsimd::batch<int32_t>::load_unaligned(raw + j))
          .store_aligned(sz);
      for (std::size_t k = 0; k < kWidth; ++k) {
        const bool isNull = !bits::isBitSet(nulls, static_cast<int32_t>(j + k));
        rowSizes[j + k] += isNull ? 1U : sz[k];
      }
    }
  }
  for (; j < count; ++j) {
    const bool isNull = !bits::isBitSet(nulls, static_cast<int32_t>(j));
    rowSizes[j] += nullableInt64SerializedSize(
        isNull ? 0 : static_cast<int64_t>(raw[j]), isNull);
  }
}

} // namespace bytedance::bolt::row::detail
