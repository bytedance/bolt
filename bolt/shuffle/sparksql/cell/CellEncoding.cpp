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

#include "bolt/shuffle/sparksql/cell/CellEncoding.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

/// Unpacks `count` values, `bits` wide each (1..63), LSB-first from `in`;
/// the caller has verified in holds ceil(count * bits / 8) bytes. `put(i, v)`
/// receives the raw unsigned bit pattern.
template <typename Put>
inline void unpackBits(const uint8_t* in, uint32_t count, uint32_t bits, const Put& put) {
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

inline void storeLe(uint8_t* out, uint64_t value, uint32_t bytes) {
  ::memcpy(out, &value, bytes);
}

inline uint64_t loadLe(const uint8_t* in, uint32_t bytes) {
  uint64_t value = 0;
  ::memcpy(&value, in, bytes);
  return value;
}

} // namespace

template <typename T>
uint32_t decodeBlock(const uint8_t* in, size_t inBytes, uint32_t count, T* dst) {
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
      // Rule 14: 1 <= narrow_bytes <= type_width.
      if (param < 1 || param > kWidth || bodyAvail < param) {
        return 0;
      }
      const T value =
          static_cast<T>(signExtend(loadLe(body, param), param * 8));
      for (uint32_t i = 0; i < count; ++i) {
        dst[i] = value;
      }
      return 1 + param;
    }
    case EncodingKind::kBitPack: {
      // Rule 15: 1 <= bit_width <= min(63, type_width * 8).
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
      // Rule 16: delta_bit_width <= 63.
      if (param > 63 || bodyAvail < kWidth) {
        return 0;
      }
      const uint64_t base = static_cast<uint64_t>(
          signExtend(loadLe(body, kWidth), kWidth * 8));
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
      // Rule 13: PLAIN's parameter must be zero.
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

template <typename T>
void encodeStream(const T* values, size_t count, std::string& out) {
  constexpr uint32_t kValuesPerBlock = kBlockSourceBytes / sizeof(T);
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

template <typename T>
bool decodeStream(
    const uint8_t* in,
    size_t inBytes,
    size_t valueCount,
    T* dst) {
  constexpr uint32_t kValuesPerBlock = kBlockSourceBytes / sizeof(T);
  size_t offset = 0;
  size_t decoded = 0;
  while (decoded < valueCount) {
    const uint32_t n = valueCount - decoded >= kValuesPerBlock
        ? kValuesPerBlock
        : static_cast<uint32_t>(valueCount - decoded);
    const uint32_t consumed =
        decodeBlock(in + offset, inBytes - offset, n, dst + decoded);
    if (consumed == 0) {
      return false;
    }
    offset += consumed;
    decoded += n;
  }
  // Rule 20: the block sequence consumes the stream exactly.
  return offset == inBytes;
}

template uint32_t
decodeBlock<int16_t>(const uint8_t*, size_t, uint32_t, int16_t*);
template uint32_t
decodeBlock<int32_t>(const uint8_t*, size_t, uint32_t, int32_t*);
template uint32_t
decodeBlock<int64_t>(const uint8_t*, size_t, uint32_t, int64_t*);
template void encodeStream<int16_t>(const int16_t*, size_t, std::string&);
template void encodeStream<int32_t>(const int32_t*, size_t, std::string&);
template void encodeStream<int64_t>(const int64_t*, size_t, std::string&);
template bool decodeStream<int16_t>(const uint8_t*, size_t, size_t, int16_t*);
template bool decodeStream<int32_t>(const uint8_t*, size_t, size_t, int32_t*);
template bool decodeStream<int64_t>(const uint8_t*, size_t, size_t, int64_t*);

} // namespace bytedance::bolt::shuffle::sparksql::cell
