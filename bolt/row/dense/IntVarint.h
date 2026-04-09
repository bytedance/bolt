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
#include <xsimd/xsimd.hpp>

#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace bytedance::bolt::row::detail {

constexpr uint64_t kVarintDataBitsMask{0x7f7f7f7f7f7f7f7fULL};
constexpr uint64_t kVarintMsbBitsMask{0x8080808080808080ULL};

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

// A varint byte carries 7 payload bits; the high bit (0x80) is the
// continuation flag. The final byte of a varint is the one with it clear.
constexpr uint64_t kVarintPayloadBits{0x7f};
FOLLY_ALWAYS_INLINE bool varintIsLastByte(uint8_t b) {
  return (b & 0x80) == 0;
}

FOLLY_ALWAYS_INLINE bool
readVarintScalar(const uint8_t*& in, const uint8_t* end, uint64_t& value) {
  uint64_t result{0};
  uint32_t shift{0};
  while (in < end && shift < 64) {
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

    uint64_t packed = _pdep_u64(value, kVarintDataBitsMask);
    // _pdep places only the 7 data bits. Set continuation bits (MSB=1) for
    // the first len - 1 bytes; the last byte keeps MSB=0.
    packed |= kVarintContinuationMasks[len];
    std::memcpy(out, &packed, len);
    return out + len;
  }

  // Values >= 2^63 require 10 bytes in unsigned varint form (e.g.
  // zigzag(INT64_MAX) == 2^64 - 2). Encode the first 8 bytes with BMI2,
  // then encode the remaining <=8 bits with scalar (1-2 bytes).
  uint64_t packed = _pdep_u64(value, kVarintDataBitsMask);
  packed |= kVarintMsbBitsMask;
  std::memcpy(out, &packed, 8);
  out += 8;
  return writeVarintScalar(value >> 56, out);
}

inline __attribute__((target("bmi2"))) bool
readVarintBmi2(const uint8_t*& in, const uint8_t* end, uint64_t& value) {
  if (end - in >= 8) {
    uint64_t word;
    std::memcpy(&word, in, sizeof(word));

    const uint64_t stopMask = (~word) & kVarintMsbBitsMask;
    if (stopMask != 0) {
      const auto len =
          static_cast<size_t>((__builtin_ctzll(stopMask) >> 3) + 1);
      uint64_t decoded = _pext_u64(word, kVarintDataBitsMask);
      if (len < 8) {
        decoded &= ((1ULL << (len * 7)) - 1);
      }
      value = decoded;
      in += len;
      return true;
    }

    // Fast path for 9-10 byte varints where the first 8 bytes all continue.
    uint64_t decoded = _pext_u64(word, kVarintDataBitsMask);
    auto* cursor = in + 8;
    if (cursor >= end) {
      return false;
    }

    const auto byte8 = *cursor++;
    decoded |= (static_cast<uint64_t>(byte8 & 0x7f) << 56);
    if ((byte8 & 0x80) == 0) {
      value = decoded;
      in = cursor;
      return true;
    }

    if (cursor >= end) {
      return false;
    }

    const auto byte9 = *cursor++;
    if ((byte9 & 0x80) != 0) {
      return false;
    }

    decoded |= (static_cast<uint64_t>(byte9 & 0x1) << 63);
    value = decoded;
    in = cursor;
    return true;
  }

  return readVarintScalar(in, end, value);
}

#endif

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
// BMI2/scalar; both handle truncation correctly.
// Each length below reconstructs its whole value inside its own return, so the
// fall-through path (5+ byte varint) does no value arithmetic and the 1-byte
// case skips the payload mask — this is why it stays hand-unrolled rather than
// looped (a loop that accumulated the value each step measured ~1% slower on
// decode). Earlier bytes (b0..b{k-1}) are known to carry continuation bits, so
// only their payload (low 7) bits contribute; the terminating byte is whole.
FOLLY_ALWAYS_INLINE bool readVarintShortFastPath(
    const uint8_t*& in,
    const uint8_t* end,
    uint64_t& value) {
  constexpr uint64_t kP = kVarintPayloadBits;

  if (FOLLY_UNLIKELY(in >= end)) {
    return false;
  }
  const uint8_t b0 = in[0];
  if (FOLLY_LIKELY(varintIsLastByte(b0))) { // 1 byte (< 2^7)
    value = b0;
    in += 1;
    return true;
  }

  if (FOLLY_UNLIKELY(in + 1 >= end)) {
    return false;
  }
  const uint8_t b1 = in[1];
  if (FOLLY_LIKELY(varintIsLastByte(b1))) { // 2 bytes (< 2^14)
    value = (b0 & kP) | (uint64_t{b1} << 7);
    in += 2;
    return true;
  }

  if (FOLLY_UNLIKELY(in + 2 >= end)) {
    return false;
  }
  const uint8_t b2 = in[2];
  if (FOLLY_LIKELY(varintIsLastByte(b2))) { // 3 bytes (< 2^21)
    value = (b0 & kP) | ((b1 & kP) << 7) | (uint64_t{b2} << 14);
    in += 3;
    return true;
  }

  if (FOLLY_UNLIKELY(in + 3 >= end)) {
    return false;
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

FOLLY_ALWAYS_INLINE bool
readVarint(const uint8_t*& in, const uint8_t* end, uint64_t& value) {
  if (readVarintShortFastPath(in, end, value)) {
    return true;
  }
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
  return readVarintBmi2(in, end, value);
#else
  return readVarintScalar(in, end, value);
#endif
}

// Nullable int64 wire mapping:
// - null -> 0x00.
// - INT64_MIN -> 0x80 0x00 (reserved sentinel).
// - all other values -> varint(zigzag(adjust(v))), where adjust(v) keeps
//   positive values unchanged and shifts non-positive values by -1.
//
// The reserved sentinel keeps null as a single-byte marker while preserving a
// one-to-one mapping for the full int64 domain.
constexpr uint8_t kInt64MinSentinelFirstByte{0x80};
constexpr uint8_t kInt64MinSentinelSecondByte{0x00};

FOLLY_ALWAYS_INLINE constexpr bool
canRead(const uint8_t* ptr, const uint8_t* end, size_t bytes) {
  return static_cast<size_t>(end - ptr) >= bytes;
}

FOLLY_ALWAYS_INLINE constexpr uint64_t zigZagEncode64(int64_t value) {
  return (static_cast<uint64_t>(value) << 1) ^
      static_cast<uint64_t>(value >> 63);
}

FOLLY_ALWAYS_INLINE constexpr int64_t zigZagDecode64(uint64_t encoded) {
  return static_cast<int64_t>((encoded >> 1) ^ (0 - (encoded & 1)));
}

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
  const uint64_t uv = static_cast<uint64_t>(value);
  const uint64_t sign = static_cast<uint64_t>(value >> 63);
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

  if (needsExtendedInt64Encoding(value)) {
    *out++ = kInt64MinSentinelFirstByte;
    *out++ = kInt64MinSentinelSecondByte;
    return out;
  }

  return writeVarint(
      zigZagEncode64(adjustInt64ForNullableEncoding(value)), out);
}

FOLLY_ALWAYS_INLINE bool decodeNullableInt64(
    const uint8_t*& in,
    const uint8_t* end,
    bool& isNull,
    int64_t& value) {
  if (!canRead(in, end, 1)) {
    return false;
  }

  if (*in == 0) {
    ++in;
    isNull = true;
    value = 0;
    return true;
  }

  if (canRead(in, end, 2) && in[0] == kInt64MinSentinelFirstByte &&
      in[1] == kInt64MinSentinelSecondByte) {
    in += 2;
    isNull = false;
    value = std::numeric_limits<int64_t>::min();
    return true;
  }

  uint64_t encoded{0};
  if (!readVarint(in, end, encoded)) {
    return false;
  }

  if (encoded == 0) {
    // Reject non-canonical multi-byte encodings of zero.
    return false;
  }

  isNull = false;
  value = restoreInt64FromNullableEncoding(zigZagDecode64(encoded));
  return true;
}

// Nullable int128 wire mapping (two halves of zigzag128(v), no separate tag):
//   null     -> nullableInt64(_, null)            (a single 0x00 byte)
//   non-null -> nullableInt64(low 64 of zigzag128(v)), varint(high 64).
// The null marker is folded into the low int64 slot.via the nullable-int64
// codec's own 0x00 sentinel, so there is no extra present/null tag byte: a
// non-null value's low half just rides the same slot, and the high half follows
// only when present. zigzag128 keeps small-magnitude values (either sign)
// short, and the two halves reuse the 64-bit varint path (no 128-bit varint).
// The low half is reinterpreted as int64 for the nullable-int64 codec; that
// round-trips bit-for-bit (it is a bijection over int64 plus null).
FOLLY_ALWAYS_INLINE constexpr unsigned __int128 zigZagEncode128(
    __int128 value) {
  return (static_cast<unsigned __int128>(value) << 1) ^
      static_cast<unsigned __int128>(value >> 127);
}

FOLLY_ALWAYS_INLINE constexpr __int128 zigZagDecode128(
    unsigned __int128 encoded) {
  return static_cast<__int128>(
      (encoded >> 1) ^ (~static_cast<unsigned __int128>(0) * (encoded & 1)));
}

FOLLY_ALWAYS_INLINE size_t
nullableInt128SerializedSize(__int128 value, bool isNull) {
  if (isNull) {
    return nullableInt64SerializedSize(0, /*isNull=*/true);
  }
  const unsigned __int128 zz = zigZagEncode128(value);
  return nullableInt64SerializedSize(
             static_cast<int64_t>(static_cast<uint64_t>(zz)),
             /*isNull=*/false) +
      varintSize(static_cast<uint64_t>(zz >> 64));
}

FOLLY_ALWAYS_INLINE uint8_t*
writeNullableInt128(__int128 value, bool isNull, uint8_t* out) {
  if (isNull) {
    return writeNullableInt64(0, /*isNull=*/true, out);
  }
  const unsigned __int128 zz = zigZagEncode128(value);
  out = writeNullableInt64(
      static_cast<int64_t>(static_cast<uint64_t>(zz)), /*isNull=*/false, out);
  return writeVarint(static_cast<uint64_t>(zz >> 64), out);
}

FOLLY_ALWAYS_INLINE bool decodeNullableInt128(
    const uint8_t*& in,
    const uint8_t* end,
    bool& isNull,
    __int128& value) {
  int64_t low{0};
  if (!decodeNullableInt64(in, end, isNull, low)) {
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
      (static_cast<unsigned __int128>(hi) << 64) | static_cast<uint64_t>(low));
  return true;
}

// ============================================================================
// Portable (xsimd) nullable-int size kernels. Two kernels: int32 and int64;
// int8/int16 widen to int32. The size math is pure xsimd::batch, so it runs on
// AVX2 / AVX-512 / SSE / NEON (compatibility); only the narrow->int32 widening
// load and the size_t scatter keep an x86 fast path with a scalar fallback.
// ============================================================================

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
  U s(1u);
  s += U(zz > U(static_cast<uint32_t>((1u << 7) - 1)));
  s += U(zz > U(static_cast<uint32_t>((1u << 14) - 1)));
  s += U(zz > U(static_cast<uint32_t>((1u << 21) - 1)));
  s += U(zz > U(static_cast<uint32_t>((1u << 28) - 1)));
  return xsimd::select(
      xsimd::batch_bool_cast<uint32_t>(
          v == S(std::numeric_limits<int32_t>::min())),
      U(5u),
      s);
}

// int64: 8 signed thresholds + the zigzag>=2^63 (+9) and INT64_MIN (->2)
// fixups. Branchless; matches the original kernel's algorithm/cost.
FOLLY_ALWAYS_INLINE xsimd::batch<int64_t> nullableInt64SizesBatch(
    xsimd::batch<int64_t> v) {
  using B = xsimd::batch<int64_t>;
  const B zero(0);
  const B one(1);
  const B adj = v - B(v <= zero);
  const B zz = (adj << 1) ^ (adj >> 63);
  B s = one;
  s += B(zz > B((1LL << 7) - 1));
  s += B(zz > B((1LL << 14) - 1));
  s += B(zz > B((1LL << 21) - 1));
  s += B(zz > B((1LL << 28) - 1));
  s += B(zz > B((1LL << 35) - 1));
  s += B(zz > B((1LL << 42) - 1));
  s += B(zz > B((1LL << 49) - 1));
  s += B(zz > B((1LL << 56) - 1));
  s += xsimd::select(zz < zero, B(9), zero); // zz >= 2^63 -> 10 bytes
  return xsimd::select(v == B(std::numeric_limits<int64_t>::min()), B(2), s);
}

// Same result as nullableInt64SizesBatch, computed straight from |v| without
// zigzag/adjust: size(v) = min k with |v| < 2^(7k-1), i.e. threshold |v|
// against {2^6, 2^13, ... 2^62}. The >=2^63 (10-byte) case is just the top
// threshold — no separate zz<0 fixup. INT64_MIN's abs overflows back to a
// negative value, so all thresholds miss (s=1) and the final select sets it to
// the 2-byte sentinel. 9 thresholds vs the zigzag kernel's 8 + the zz<0 select.
FOLLY_ALWAYS_INLINE xsimd::batch<int64_t> nullableInt64SizesBatchAbs(
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

// Load batch<int32>::size consecutive T (int8/int16/int32) sign-extended to
// int32. x86 fast path; portable scalar fallback elsewhere.
template <typename T>
FOLLY_ALWAYS_INLINE xsimd::batch<int32_t> loadWidenInt32(const T* p) {
  using S = xsimd::batch<int32_t>;
  if constexpr (std::is_same_v<T, int32_t>) {
    return S::load_unaligned(p);
  } else {
#if defined(__AVX2__)
    if constexpr (std::is_same_v<T, int16_t>) {
      return S(_mm256_cvtepi16_epi32(
          _mm_loadu_si128(reinterpret_cast<const __m128i*>(p))));
    } else {
      return S(_mm256_cvtepi8_epi32(
          _mm_loadl_epi64(reinterpret_cast<const __m128i*>(p))));
    }
#else
    alignas(64) int32_t tmp[S::size];
    for (std::size_t k = 0; k < S::size; ++k) {
      tmp[k] = static_cast<int32_t>(p[k]);
    }
    return S::load_aligned(tmp);
#endif
  }
}

// Sum of nullable-int sizes for int8/int16/int32 (widen to int32).
template <typename T>
FOLLY_ALWAYS_INLINE size_t sumNullableIntSizesSimd(const T* raw, size_t count) {
  static_assert(
      std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
          std::is_same_v<T, int32_t>,
      "Use sumNullableInt64SizesSimd for int64_t");
  using U = xsimd::batch<uint32_t>;
  constexpr std::size_t W = xsimd::batch<int32_t>::size;
  U acc(0u);
  std::size_t j = 0;
  for (; j + W <= count; j += W) {
    acc += nullableInt32SizesBatch(loadWidenInt32<T>(raw + j));
  }
  size_t total = static_cast<size_t>(xsimd::reduce_add(acc));
  for (; j < count; ++j) {
    total += nullableInt64SerializedSize(static_cast<int64_t>(raw[j]), false);
  }
  return total;
}

// Sum of nullable-int64 sizes, with a testz-style fastpath: if every zigzag in
// a batch fits 32 bits (common small-magnitude BIGINT) only 4 thresholds are
// needed. xsimd::all keeps it portable.
FOLLY_ALWAYS_INLINE size_t
sumNullableInt64SizesSimd(const int64_t* raw, size_t count) {
  using B = xsimd::batch<int64_t>;
  constexpr std::size_t W = B::size;
  const B zero(0);
  const B one(1);
  B acc(0);
  std::size_t j = 0;
  for (; j + W <= count; j += W) {
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
  size_t total = static_cast<size_t>(xsimd::reduce_add(acc));
  for (; j < count; ++j) {
    total += nullableInt64SerializedSize(raw[j], false);
  }
  return total;
}

// Per-row scatter: add each value's size into its own rowSizes[r].
template <typename T>
FOLLY_ALWAYS_INLINE void
addIntColumnSizesSimd(const T* raw, size_t* rowSizes, size_t count) {
  static_assert(sizeof(size_t) == 8);
  std::size_t j = 0;
  if constexpr (std::is_same_v<T, int64_t>) {
    // Sizes are int64 already: add the batch straight into rowSizes (portable).
    // Branchless 8-threshold (no testz fastpath): a per-batch "all small"
    // branch regresses mixed/full-range BIGINT via misprediction, which
    // dominates the small-value saving.
    using B = xsimd::batch<int64_t>;
    constexpr std::size_t W = B::size;
    auto* rs = reinterpret_cast<int64_t*>(rowSizes);
    for (; j + W <= count; j += W) {
      B sz = nullableInt64SizesBatch(B::load_unaligned(raw + j));
      (B::load_unaligned(rs + j) + sz).store_unaligned(rs + j);
    }
  } else {
    constexpr std::size_t W = xsimd::batch<int32_t>::size;
    alignas(64) uint32_t sz[W];
    for (; j + W <= count; j += W) {
      nullableInt32SizesBatch(loadWidenInt32<T>(raw + j)).store_aligned(sz);
#if defined(__AVX2__)
      if constexpr (W == 8) {
        // Widen the 8 uint32 sizes to size_t and add in two vector groups.
        __m256i s = _mm256_load_si256(reinterpret_cast<const __m256i*>(sz));
        __m256i lo = _mm256_cvtepu32_epi64(_mm256_castsi256_si128(s));
        __m256i hi = _mm256_cvtepu32_epi64(_mm256_extracti128_si256(s, 1));
        auto* p0 = reinterpret_cast<__m256i*>(rowSizes + j);
        auto* p1 = reinterpret_cast<__m256i*>(rowSizes + j + 4);
        _mm256_storeu_si256(p0, _mm256_add_epi64(_mm256_loadu_si256(p0), lo));
        _mm256_storeu_si256(p1, _mm256_add_epi64(_mm256_loadu_si256(p1), hi));
      } else
#endif
      {
        for (std::size_t k = 0; k < W; ++k) {
          rowSizes[j + k] += sz[k];
        }
      }
    }
  }
  for (; j < count; ++j) {
    rowSizes[j] +=
        nullableInt64SerializedSize(static_cast<int64_t>(raw[j]), false);
  }
}

// Nullable counterpart of addIntColumnSizesSimd<int64_t> for a FLAT column with
// nulls (the common Spark BIGINT case that otherwise falls to the scalar loop).
// A null row contributes exactly 1 byte (the 0x00 marker), independent of the
// (garbage) value stored at its slot. `nulls` is the row-indexed validity
// bitmap (bit set = non-null), which the caller guarantees non-null.
FOLLY_ALWAYS_INLINE void addNullableInt64ColumnSizesSimd(
    const int64_t* raw,
    const uint64_t* nulls,
    size_t* rowSizes,
    size_t count) {
  static_assert(sizeof(size_t) == 8);
  using B = xsimd::batch<int64_t>;
  constexpr std::size_t W = B::size; // 2 (SSE) / 4 (AVX2) / 8 (AVX-512)
  auto* rs = reinterpret_cast<int64_t*>(rowSizes);

  // Per-lane bit selector {1<<0, 1<<1, ...}, built once.
  alignas(64) int64_t selArr[W];
  for (std::size_t i = 0; i < W; ++i) {
    selArr[i] = static_cast<int64_t>(int64_t{1} << i);
  }
  const B laneSel = B::load_aligned(selArr);
  const B one(1);

  std::size_t j = 0;
  for (; j + W <= count; j += W) {
    B sz = nullableInt64SizesBatchAbs(B::load_unaligned(raw + j));
    // W validity bits for rows [j, j+W). W divides 64 and j is a multiple of W,
    // so the W bits never straddle a 64-bit word.
    const uint64_t word = nulls[j >> 6];
    const uint64_t bits = (word >> (j & 63)) & ((uint64_t{1} << W) - 1);
    // lane i valid iff bit i set: (broadcast(bits) & laneSel) == laneSel.
    const auto isValid = (B(static_cast<int64_t>(bits)) & laneSel) == laneSel;
    sz = xsimd::select(isValid, sz, one); // null -> 1 byte
    (B::load_unaligned(rs + j) + sz).store_unaligned(rs + j);
  }
  for (; j < count; ++j) {
    const bool isNull = ((nulls[j >> 6] >> (j & 63)) & 1ull) == 0;
    rs[j] += static_cast<int64_t>(
        nullableInt64SerializedSize(isNull ? 0 : raw[j], isNull));
  }
}

} // namespace bytedance::bolt::row::detail
