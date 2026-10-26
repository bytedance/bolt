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

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <random>

#include "bolt/shuffle/sparksql/cell/CellEncoding.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

template <typename T>
std::vector<T> roundtrip(const std::vector<T>& values) {
  std::string encoded;
  encodeStream(values.data(), values.size(), encoded);
  std::vector<T> decoded(values.size());
  EXPECT_TRUE(decodeStream(
      reinterpret_cast<const uint8_t*>(encoded.data()),
      encoded.size(),
      values.size(),
      decoded.data()))
      << "stream must parse and consume exactly";
  return decoded;
}

template <typename T>
void expectRoundtrip(const std::vector<T>& values) {
  EXPECT_EQ(roundtrip(values), values);
}

TEST(CellEncodingTest, headerByteLayout) {
  EXPECT_EQ(makeEncodingByte(EncodingKind::kPlain, 0), 0x03);
  EXPECT_EQ(makeEncodingByte(EncodingKind::kConstNarrow, 1), 0x04);
  EXPECT_EQ(makeEncodingByte(EncodingKind::kBitPack, 5), 0x15);
  EXPECT_EQ(makeEncodingByte(EncodingKind::kForBitPack, 63), 0xFE);
}

TEST(CellEncodingTest, constBlocks) {
  uint8_t out[kMaxBlockBytes];
  // 8 equal Bigints narrow to a single byte.
  std::vector<int64_t> values(8, 42);
  const auto bytes = encodeBlock(values.data(), 8, out);
  EXPECT_EQ(bytes, 2u);
  EXPECT_EQ(out[0], makeEncodingByte(EncodingKind::kConstNarrow, 1));
  EXPECT_EQ(out[1], 42);
  std::vector<int64_t> decoded(8);
  EXPECT_EQ(decodeBlock(out, bytes, 8, decoded.data()), bytes);
  EXPECT_EQ(decoded, values);

  // Negative constant sign-extends through the narrow bytes.
  expectRoundtrip<int64_t>(std::vector<int64_t>(8, -1));
  expectRoundtrip<int64_t>(std::vector<int64_t>(8, -300));
  expectRoundtrip<int32_t>(std::vector<int32_t>(16, -1));
  expectRoundtrip<int16_t>(
      std::vector<int16_t>(32, std::numeric_limits<int16_t>::min()));
  expectRoundtrip<int64_t>(
      std::vector<int64_t>(8, std::numeric_limits<int64_t>::min()));
}

TEST(CellEncodingTest, bitPackBlocks) {
  // Small non-negative Integers bit-pack.
  std::vector<int32_t> values;
  for (int32_t i = 0; i < 16; ++i) {
    values.push_back(i);
  }
  uint8_t out[kMaxBlockBytes];
  const auto bytes = encodeBlock(values.data(), 16, out);
  // width(15) = 5 signed bits; 16 * 5 / 8 = 10 body bytes.
  EXPECT_EQ(out[0], makeEncodingByte(EncodingKind::kBitPack, 5));
  EXPECT_EQ(bytes, 11u);
  std::vector<int32_t> decoded(16);
  EXPECT_EQ(decodeBlock(out, bytes, 16, decoded.data()), bytes);
  EXPECT_EQ(decoded, values);

  // Symmetric-around-zero values keep the sign bit through the pack.
  expectRoundtrip<int32_t>(
      {-8, 7, -1, 0, 3, -5, 2, -7, 6, 1, -2, 4, -3, 5, 0, -6});
  expectRoundtrip<int16_t>({-1, 1, -1, 1});
  expectRoundtrip<int64_t>({31, -32, 0, 15, -16, 7, -8, 1});
}

TEST(CellEncodingTest, forBitPackBlocks) {
  // Large close values: FOR with a small delta beats everything else.
  std::vector<int64_t> values;
  for (int64_t i = 0; i < 8; ++i) {
    values.push_back(1'000'000'000'000 + (i * 3) % 7);
  }
  uint8_t out[kMaxBlockBytes];
  const auto bytes = encodeBlock(values.data(), 8, out);
  EXPECT_EQ(out[0] & 0x03, static_cast<uint8_t>(EncodingKind::kForBitPack));
  std::vector<int64_t> decoded(8);
  EXPECT_EQ(decodeBlock(out, bytes, 8, decoded.data()), bytes);
  EXPECT_EQ(decoded, values);

  // delta_bit_width == 0: all equal, but too wide for one narrow byte would
  // still prefer CONST; force the FOR-0 shape through the decoder directly.
  uint8_t forZero[9];
  forZero[0] = makeEncodingByte(EncodingKind::kForBitPack, 0);
  const int64_t base = -123456789;
  ::memcpy(forZero + 1, &base, 8);
  std::vector<int64_t> constant(8);
  EXPECT_EQ(decodeBlock(forZero, sizeof(forZero), 8, constant.data()), 9u);
  EXPECT_EQ(constant, std::vector<int64_t>(8, base));

  // Negative bases round-trip.
  expectRoundtrip<int64_t>(
      {-1'000'000'000'007,
       -1'000'000'000'001,
       -1'000'000'000'003,
       -1'000'000'000'002,
       -1'000'000'000'007,
       -1'000'000'000'004,
       -1'000'000'000'006,
       -1'000'000'000'005});
}

TEST(CellEncodingTest, plainFallback) {
  // Values spanning the full 64-bit range: BIT_PACK needs 64 bits (illegal)
  // and the min-max delta overflows 63 bits, so PLAIN is the only choice.
  std::vector<int64_t> values{
      std::numeric_limits<int64_t>::min(),
      std::numeric_limits<int64_t>::max(),
      -1,
      0,
      42,
      -723486128736412387,
      8123671623987123659,
      std::numeric_limits<int64_t>::min() + 1};
  uint8_t out[kMaxBlockBytes];
  const auto bytes = encodeBlock(values.data(), 8, out);
  EXPECT_EQ(out[0], makeEncodingByte(EncodingKind::kPlain, 0));
  EXPECT_EQ(bytes, 65u);
  std::vector<int64_t> decoded(8);
  EXPECT_EQ(decodeBlock(out, bytes, 8, decoded.data()), bytes);
  EXPECT_EQ(decoded, values);

  // 63-bit-wide randoms legally BIT_PACK at width 63 and still round-trip.
  std::mt19937_64 rng(7);
  std::vector<int64_t> wide;
  for (int i = 0; i < 8; ++i) {
    wide.push_back(static_cast<int64_t>(rng() >> 1) - (int64_t{1} << 61));
  }
  expectRoundtrip(wide);
}

TEST(CellEncodingTest, streamBlockingAndTail) {
  // 41 Integers: 2 full blocks of 16 plus a tail of 9.
  std::vector<int32_t> values;
  for (int32_t i = 0; i < 41; ++i) {
    values.push_back(i * 17 - 300);
  }
  expectRoundtrip(values);

  // Tail-only streams of every length.
  for (uint32_t n = 1; n < 8; ++n) {
    std::vector<int64_t> tail;
    for (uint32_t i = 0; i < n; ++i) {
      tail.push_back(static_cast<int64_t>(i) * 1'000'003);
    }
    expectRoundtrip(tail);
  }

  // Empty stream: zero bytes, zero values.
  std::vector<int16_t> empty;
  std::string encoded;
  encodeStream(empty.data(), 0, encoded);
  EXPECT_TRUE(encoded.empty());
  EXPECT_TRUE(decodeStream<int16_t>(nullptr, 0, 0, nullptr));
}

TEST(CellEncodingTest, streamMustConsumeExactly) {
  std::vector<int32_t> values(16, 5);
  std::string encoded;
  encodeStream(values.data(), values.size(), encoded);
  std::vector<int32_t> decoded(16);
  // Trailing garbage: the blocks must consume the stream exactly.
  std::string padded = encoded + std::string(1, '\0');
  EXPECT_FALSE(decodeStream(
      reinterpret_cast<const uint8_t*>(padded.data()),
      padded.size(),
      16,
      decoded.data()));
  // Truncation fails block decoding.
  EXPECT_FALSE(decodeStream(
      reinterpret_cast<const uint8_t*>(encoded.data()),
      encoded.size() - 1,
      16,
      decoded.data()));
}

TEST(CellEncodingTest, malformedBlocksRejected) {
  std::vector<int32_t> dst(16);
  {
    // PLAIN with a non-zero parameter.
    uint8_t bad[65] = {makeEncodingByte(EncodingKind::kPlain, 1)};
    EXPECT_EQ(decodeBlock(bad, sizeof(bad), 16, dst.data()), 0u);
  }
  {
    // CONST_NARROW with narrow_bytes of 0 and beyond type width.
    uint8_t bad[9] = {makeEncodingByte(EncodingKind::kConstNarrow, 0)};
    EXPECT_EQ(decodeBlock(bad, sizeof(bad), 16, dst.data()), 0u);
    bad[0] = makeEncodingByte(EncodingKind::kConstNarrow, 5); // > 4 for i32
    EXPECT_EQ(decodeBlock(bad, sizeof(bad), 16, dst.data()), 0u);
  }
  {
    // BIT_PACK width 0 and width beyond min(63, 8 * width).
    uint8_t bad[70] = {makeEncodingByte(EncodingKind::kBitPack, 0)};
    EXPECT_EQ(decodeBlock(bad, sizeof(bad), 16, dst.data()), 0u);
    bad[0] = makeEncodingByte(EncodingKind::kBitPack, 33); // > 32 for i32
    EXPECT_EQ(decodeBlock(bad, sizeof(bad), 16, dst.data()), 0u);
    std::vector<int64_t> wide(8);
    uint8_t bad64[73] = {makeEncodingByte(EncodingKind::kBitPack, 63)};
    EXPECT_NE(decodeBlock(bad64, sizeof(bad64), 8, wide.data()), 0u);
  }
  {
    // Bodies larger than the remaining bytes.
    std::vector<int64_t> wide(8);
    uint8_t tiny[3] = {makeEncodingByte(EncodingKind::kPlain, 0), 0, 0};
    EXPECT_EQ(decodeBlock(tiny, sizeof(tiny), 8, wide.data()), 0u);
    uint8_t forShort[5] = {makeEncodingByte(EncodingKind::kForBitPack, 1)};
    EXPECT_EQ(decodeBlock(forShort, sizeof(forShort), 8, wide.data()), 0u);
  }
}

TEST(CellEncodingTest, fuzzRoundtripAcrossShapes) {
  std::mt19937_64 rng(1234);
  const auto fuzzType = [&](auto tag) {
    using T = decltype(tag);
    for (int iter = 0; iter < 500; ++iter) {
      const uint32_t count = 1 + rng() % 300;
      std::vector<T> values(count);
      // Alternate between shapes that exercise each encoding.
      switch (iter % 5) {
        case 0: { // constant
          const T v = static_cast<T>(rng());
          std::fill(values.begin(), values.end(), v);
          break;
        }
        case 1: // small around zero
          for (auto& v : values) {
            v = static_cast<T>(static_cast<int64_t>(rng() % 61) - 30);
          }
          break;
        case 2: { // offset band
          const int64_t base = static_cast<int64_t>(rng());
          for (auto& v : values) {
            v = static_cast<T>(base + static_cast<int64_t>(rng() % 1000));
          }
          break;
        }
        case 3: // full random
          for (auto& v : values) {
            v = static_cast<T>(rng());
          }
          break;
        case 4: // mixed extremes
          for (auto& v : values) {
            switch (rng() % 4) {
              case 0:
                v = std::numeric_limits<T>::min();
                break;
              case 1:
                v = std::numeric_limits<T>::max();
                break;
              case 2:
                v = 0;
                break;
              default:
                v = static_cast<T>(rng());
            }
          }
          break;
      }
      const auto decoded = roundtrip(values);
      ASSERT_EQ(decoded, values)
          << "type width " << sizeof(T) << " iter " << iter;
    }
  };
  fuzzType(int16_t{});
  fuzzType(int32_t{});
  fuzzType(int64_t{});
}

TEST(CellEncodingTest, packedBlocksOfEveryWidth) {
  // BIT_PACK and FOR_BIT_PACK streams of every legal width, built block by
  // block: the full blocks with input after them take the vector path, the
  // last ones the scalar path, and both must agree with the source.
  std::mt19937_64 rng(11);
  const auto run = [&](auto tag) {
    using T = decltype(tag);
    constexpr uint32_t kPerBlock = kBlockSourceBytes / sizeof(T);
    constexpr uint32_t kMaxBits = sizeof(T) * 8 < 63 ? sizeof(T) * 8 : 63;
    for (const bool isFor : {false, true}) {
      for (uint32_t bits = 1; bits <= kMaxBits; ++bits) {
        constexpr uint32_t kBlocks = 5;
        std::vector<T> expected;
        std::string stream;
        for (uint32_t block = 0; block < kBlocks; ++block) {
          const T base = isFor ? static_cast<T>(rng()) : T{};
          uint64_t staged[kPerBlock];
          for (uint32_t i = 0; i < kPerBlock; ++i) {
            staged[i] = rng() & ((uint64_t{1} << bits) - 1);
            expected.push_back(
                isFor ? static_cast<T>(static_cast<uint64_t>(base) + staged[i])
                      : static_cast<T>(detail::signExtend(staged[i], bits)));
          }
          uint8_t out[kMaxBlockBytes];
          uint8_t* pos = out;
          *pos++ = makeEncodingByte(
              isFor ? EncodingKind::kForBitPack : EncodingKind::kBitPack, bits);
          if (isFor) {
            ::memcpy(pos, &base, sizeof(T));
            pos += sizeof(T);
          }
          pos = detail::packBits(staged, kPerBlock, bits, pos);
          stream.append(reinterpret_cast<const char*>(out), pos - out);
        }
        std::vector<T> decoded(expected.size());
        ASSERT_TRUE(decodeStream(
            reinterpret_cast<const uint8_t*>(stream.data()),
            stream.size(),
            decoded.size(),
            decoded.data()))
            << "width " << sizeof(T) << " bits " << bits << " for " << isFor;
        ASSERT_EQ(decoded, expected)
            << "width " << sizeof(T) << " bits " << bits << " for " << isFor;
        // One byte short: the last block is truncated, never over-read.
        EXPECT_FALSE(decodeStream(
            reinterpret_cast<const uint8_t*>(stream.data()),
            stream.size() - 1,
            decoded.size(),
            decoded.data()));
      }
    }
  };
  run(int16_t{});
  run(int32_t{});
  run(int64_t{});
}

TEST(CellEncodingTest, streamCursorReadsInArbitraryPieces) {
  std::mt19937_64 rng(77);
  const auto fuzzType = [&](auto tag) {
    using T = decltype(tag);
    for (int iter = 0; iter < 200; ++iter) {
      const uint32_t count = rng() % 500;
      std::vector<T> values(count);
      for (auto& v : values) {
        v = static_cast<T>(iter % 2 ? rng() : rng() % 100);
      }
      std::string encoded;
      encodeStream(values.data(), values.size(), encoded);
      const auto* in = reinterpret_cast<const uint8_t*>(encoded.data());
      StreamCursor<T> cursor;
      cursor.reset(in, encoded.size(), count);
      std::vector<T> decoded(count);
      uint32_t at = 0;
      while (at < count) {
        // Pieces of 1..70 values straddle the 8/16/32-value blocks.
        const uint32_t n = std::min<uint32_t>(count - at, 1 + rng() % 70);
        ASSERT_TRUE(cursor.next(n, decoded.data() + at));
        at += n;
      }
      EXPECT_TRUE(cursor.finished());
      ASSERT_EQ(decoded, values)
          << "type width " << sizeof(T) << " iter " << iter;
      T extra;
      EXPECT_FALSE(cursor.next(1, &extra));
    }
  };
  fuzzType(int16_t{});
  fuzzType(int32_t{});
  fuzzType(int64_t{});

  // A trailing byte past the last block is not consumed.
  std::vector<int64_t> values(9, 5);
  std::string encoded;
  encodeStream(values.data(), values.size(), encoded);
  encoded.push_back('\0');
  StreamCursor<int64_t> cursor;
  cursor.reset(
      reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size(), 9);
  std::vector<int64_t> decoded(9);
  ASSERT_TRUE(cursor.next(9, decoded.data()));
  EXPECT_FALSE(cursor.finished());
}

TEST(CellEncodingTest, pinnedWireBytes) {
  // Reader side: fixed wire bytes decode to known values, so a format
  // change cannot pass unnoticed.
  {
    // Stream 0: PLAIN tail block of two Integers 10, 12.
    const uint8_t block[] = {0x03, 0x0A, 0, 0, 0, 0x0C, 0, 0, 0};
    std::vector<int32_t> decoded(2);
    EXPECT_TRUE(decodeStream(block, sizeof(block), 2, decoded.data()));
    EXPECT_EQ(decoded, (std::vector<int32_t>{10, 12}));
  }
  {
    // Stream 1: CONST_NARROW(1) tail block of three Bigint lengths of 2.
    const uint8_t block[] = {0x04, 0x02};
    std::vector<int64_t> decoded(3);
    EXPECT_TRUE(decodeStream(block, sizeof(block), 3, decoded.data()));
    EXPECT_EQ(decoded, (std::vector<int64_t>{2, 2, 2}));
  }
  // Writer side: the same length column encodes to exactly these bytes
  // (CONST_NARROW is the unique minimal choice).
  {
    const std::vector<int64_t> lengths{2, 2, 2};
    std::string encoded;
    encodeStream(lengths.data(), lengths.size(), encoded);
    EXPECT_EQ(encoded, std::string("\x04\x02", 2));
  }
}

TEST(CellEncodingTest, nullTagHelpers) {
  EXPECT_EQ(nullTagBytes(1), 1u);
  EXPECT_EQ(nullTagBytes(4), 1u);
  EXPECT_EQ(nullTagBytes(5), 2u);

  uint8_t tags[2] = {0, 0};
  setNullTag(tags, 0, NullTag::kRawNull);
  setNullTag(tags, 1, NullTag::kNoNull);
  setNullTag(tags, 4, NullTag::kAllNull);
  setNullTag(tags, 5, NullTag::kRawNull);
  // col0 RAW_NULL, col1 NO_NULL -> 0b0000_0110.
  EXPECT_EQ(tags[0], 0x06);
  EXPECT_EQ(getNullTag(tags, 0), NullTag::kRawNull);
  EXPECT_EQ(getNullTag(tags, 1), NullTag::kNoNull);
  EXPECT_EQ(getNullTag(tags, 2), NullTag::kAllNull);
  EXPECT_EQ(getNullTag(tags, 4), NullTag::kAllNull);
  EXPECT_EQ(getNullTag(tags, 5), NullTag::kRawNull);
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
