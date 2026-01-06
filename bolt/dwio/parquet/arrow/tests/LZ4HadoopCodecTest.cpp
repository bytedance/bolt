/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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
#include <lz4.h>
#include <cstring>
#include <fstream>
#include <random>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "bolt/dwio/parquet/arrow/util/CompressionInternal.h"
namespace bytedance::bolt::parquet::arrow::util {
namespace {
// Utility: generate random data with given size and pattern
std::vector<uint8_t> MakeTestData(size_t size, bool repetitive = false) {
  std::vector<uint8_t> data(size);
  if (repetitive) {
    for (size_t i = 0; i < size; ++i) {
      data[i] = static_cast<uint8_t>(i % 13);
    }
  } else {
    std::mt19937_64 gen(42);
    std::uniform_int_distribution<int> dist(0, 255);
    for (size_t i = 0; i < size; ++i) {
      data[i] = static_cast<uint8_t>(dist(gen));
    }
  }
  return data;
}

// Utility: big-endian 4-byte integer load
uint32_t LoadBE(const uint8_t* ptr) {
  return (ptr[0] << 24) | (ptr[1] << 16) | (ptr[2] << 8) | ptr[3];
}

// Verify Hadoop block framing correctness
static void VerifyHadoopFraming(const uint8_t* buf, size_t len) {
  size_t offset = 0;
  bool eof_seen = false;

  while (offset + 8 <= len) {
    uint32_t dec = LoadBE(buf + offset);
    uint32_t comp = LoadBE(buf + offset + 4);
    offset += 8;

    if (dec == 0 && comp == 0) {
      eof_seen = true;
      EXPECT_EQ(offset, len) << "EOF block should be the last one";
      break;
    }
    ASSERT_GT(dec, 0);
    ASSERT_GT(comp, 0);
    ASSERT_LE(offset + comp, len);
    offset += comp;
  }

  EXPECT_TRUE(eof_seen) << "Missing EOF block [0,0]";
}

// Core test routine
static void RoundTripTest(size_t data_size, bool repetitive) {
  std::unique_ptr<Codec> codec = internal::MakeLz4HadoopCompatibleCodec();

  // 1. Generate random input
  std::vector<uint8_t> input = MakeTestData(data_size, repetitive);

  // 2. Allocate output buffer
  int64_t max_compressed_len = codec->MaxCompressedLen(input.size(), nullptr);
  std::vector<uint8_t> compressed(max_compressed_len);

  // 3. Compress
  Result<int64_t> comp_res = codec->Compress(
      input.size(), input.data(), compressed.size(), compressed.data());
  ASSERT_TRUE(comp_res.ok()) << comp_res.status().ToString();
  int64_t compressed_size = comp_res.ValueUnsafe();
  ASSERT_GT(compressed_size, 0);
  ASSERT_LT(compressed_size, max_compressed_len);

  // 4. Verify Hadoop block structure
  VerifyHadoopFraming(compressed.data(), compressed_size);

  // 5. Decompress
  std::vector<uint8_t> decompressed(input.size());
  Result<int64_t> decomp_res = codec->Decompress(
      compressed_size,
      compressed.data(),
      decompressed.size(),
      decompressed.data());
  ASSERT_TRUE(decomp_res.ok()) << decomp_res.status().ToString();
  ASSERT_EQ(decomp_res.ValueUnsafe(), static_cast<int64_t>(input.size()));

  // 6. Compare results
  ASSERT_EQ(memcmp(input.data(), decompressed.data(), input.size()), 0)
      << "Round-trip data mismatch for size " << data_size;
}
} // namespace

// ---- TEST CASES ----

// Small input
TEST(TestLz4HadoopCodec, SmallData) {
  RoundTripTest(1024, false);
}

// Highly repetitive data (should compress well)
TEST(TestLz4HadoopCodec, RepetitiveData) {
  RoundTripTest(256 * 1024, true);
}

// Large random input
TEST(TestLz4HadoopCodec, LargeData) {
  RoundTripTest(2 * 1024 * 1024, false);
}

// Very small input (< block size)
TEST(TestLz4HadoopCodec, TinyData) {
  RoundTripTest(10, false);
}

// Multiple block case (force >64KB)
TEST(TestLz4HadoopCodec, MultiBlockData) {
  RoundTripTest(300 * 1024, false);
}

// EOF block test — ensure [0,0] is appended
TEST(TestLz4HadoopCodec, EOFBlockPresent) {
  std::unique_ptr<Codec> codec = internal::MakeLz4HadoopCompatibleCodec();
  std::vector<uint8_t> input = MakeTestData(1000);
  std::vector<uint8_t> output(codec->MaxCompressedLen(input.size(), nullptr));
  auto res =
      codec->Compress(input.size(), input.data(), output.size(), output.data());
  ASSERT_TRUE(res.ok());
  int64_t written = res.ValueUnsafe();

  // Check last 8 bytes are 0x00
  EXPECT_EQ(output[written - 8], 0);
  EXPECT_EQ(output[written - 7], 0);
  EXPECT_EQ(output[written - 6], 0);
  EXPECT_EQ(output[written - 5], 0);
  EXPECT_EQ(output[written - 4], 0);
  EXPECT_EQ(output[written - 3], 0);
  EXPECT_EQ(output[written - 2], 0);
  EXPECT_EQ(output[written - 1], 0);
}

// Truncated input should fail decompression
TEST(TestLz4HadoopCodec, TruncatedInputFails) {
  std::unique_ptr<Codec> codec = internal::MakeLz4HadoopCompatibleCodec();
  std::vector<uint8_t> input = MakeTestData(10000);
  std::vector<uint8_t> compressed(
      codec->MaxCompressedLen(input.size(), nullptr));
  auto comp_res = codec->Compress(
      input.size(), input.data(), compressed.size(), compressed.data());
  ASSERT_TRUE(comp_res.ok());
  int64_t comp_len = comp_res.ValueUnsafe();

  std::vector<uint8_t> decompressed(input.size());
  // Truncate by 16 bytes
  auto res = codec->Decompress(
      comp_len - 16,
      compressed.data(),
      decompressed.size(),
      decompressed.data());
  ASSERT_FALSE(res.ok());
}
} // namespace bytedance::bolt::parquet::arrow::util
