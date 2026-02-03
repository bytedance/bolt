/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/StreamCodec.h"

namespace bytedance::bolt::shuffle::sparksql::test {

namespace {

// Get codec type name for test naming
std::string codecTypeName(CodecType type) {
  switch (type) {
    case CodecType::ZSTD:
      return "ZSTD";
    case CodecType::GZIP:
      return "GZIP";
    case CodecType::LZ4:
      return "LZ4";
    case CodecType::LZ4_FRAME:
      return "LZ4_FRAME";
    case CodecType::SNAPPY:
      return "SNAPPY";
    default:
      return "UNKNOWN";
  }
}

// Check if codec type supports checksum
bool supportsChecksum(CodecType type) {
  return type == CodecType::ZSTD || type == CodecType::GZIP;
}

// Check if codec type supports streaming
bool supportsStreaming(CodecType type) {
  return type == CodecType::ZSTD || type == CodecType::GZIP ||
      type == CodecType::LZ4_FRAME;
}

// Generate compressible test data with repeating patterns
std::vector<uint8_t> generateCompressibleData(size_t size) {
  std::vector<uint8_t> data(size);
  for (size_t i = 0; i < size; i++) {
    data[i] = static_cast<uint8_t>((i % 64) ^ ((i / 64) % 256));
  }
  return data;
}

// Generate random test data
std::vector<uint8_t> generateRandomData(size_t size, uint32_t seed = 42) {
  std::vector<uint8_t> data(size);
  std::mt19937 gen(seed);
  std::uniform_int_distribution<> dis(0, 255);
  for (auto& byte : data) {
    byte = static_cast<uint8_t>(dis(gen));
  }
  return data;
}

} // namespace

// Test parameter structure
struct CodecTestParam {
  CodecType type;
  bool checksumEnabled;
  bool isStream;

  std::string toString() const {
    std::string name = codecTypeName(type);
    name += checksumEnabled ? "_Checksum" : "_NoChecksum";
    name += isStream ? "_Stream" : "_OneShot";
    return name;
  }
};

// Parameterized test class
class CodecTest : public testing::TestWithParam<CodecTestParam> {
 protected:
  // Verify one-shot codec round trip
  void verifyOneShotRoundTrip(const std::vector<uint8_t>& original) {
    auto param = GetParam();
    CodecOptions options;
    options.checksumEnabled = param.checksumEnabled;

    auto codec = Codec::create(param.type, options);

    // Compress
    int64_t maxCompressedSize = codec->maxCompressedLen(original.size());
    std::vector<uint8_t> compressed(maxCompressedSize);

    int64_t compressedSize = codec->compress(
        original.data(), original.size(), compressed.data(), compressed.size());

    ASSERT_GT(compressedSize, 0);
    ASSERT_LE(compressedSize, maxCompressedSize);

    // Decompress
    std::vector<uint8_t> decompressed(original.size());
    int64_t decompressedSize = codec->decompress(
        compressed.data(),
        compressedSize,
        decompressed.data(),
        decompressed.size());

    ASSERT_EQ(decompressedSize, static_cast<int64_t>(original.size()));

    // Verify data matches
    ASSERT_EQ(original.size(), decompressed.size());
    ASSERT_EQ(0, memcmp(original.data(), decompressed.data(), original.size()));
  }

  // Verify stream codec round trip
  void verifyStreamRoundTrip(const std::vector<uint8_t>& original) {
    auto param = GetParam();
    CodecOptions options;
    options.checksumEnabled = param.checksumEnabled;

    // Create compressor
    auto compressor = StreamCompressor::create(param.type, options);

    // Allocate output buffer with extra space
    int64_t outputSize = compressor->recommendedOutputSize(original.size());
    std::vector<uint8_t> compressed(outputSize * 2);

    // Compress
    auto compressResult = compressor->compress(
        original.data(), original.size(), compressed.data(), compressed.size());

    ASSERT_EQ(compressResult.bytesRead, static_cast<int64_t>(original.size()));
    int64_t totalWritten = compressResult.bytesWritten;

    // End stream
    auto endResult = compressor->end(
        compressed.data() + totalWritten, compressed.size() - totalWritten);
    while (endResult.shouldRetry) {
      totalWritten += endResult.bytesWritten;
      compressed.resize(compressed.size() * 2);
      endResult = compressor->end(
          compressed.data() + totalWritten, compressed.size() - totalWritten);
    }
    totalWritten += endResult.bytesWritten;

    ASSERT_GT(totalWritten, 0);

    // Create decompressor
    auto decompressor = StreamDecompressor::create(param.type, options);
    std::vector<uint8_t> decompressed(original.size());

    // Decompress
    auto decompressResult = decompressor->decompress(
        compressed.data(),
        totalWritten,
        decompressed.data(),
        decompressed.size());

    ASSERT_EQ(
        decompressResult.bytesWritten, static_cast<int64_t>(original.size()));

    // Verify data matches
    ASSERT_EQ(original.size(), decompressed.size());
    ASSERT_EQ(0, memcmp(original.data(), decompressed.data(), original.size()));
  }
};

// Round trip test with 1MB data
TEST_P(CodecTest, RoundTrip) {
  auto param = GetParam();
  auto data = generateCompressibleData(1 * 1024 * 1024); // 1MB

  if (param.isStream) {
    verifyStreamRoundTrip(data);
  } else {
    verifyOneShotRoundTrip(data);
  }
}

// Empty input test
TEST_P(CodecTest, EmptyInput) {
  auto param = GetParam();
  std::vector<uint8_t> empty;

  if (param.isStream) {
    // Stream codec with empty input
    CodecOptions options;
    options.checksumEnabled = param.checksumEnabled;

    auto compressor = StreamCompressor::create(param.type, options);
    int64_t outputSize = compressor->recommendedOutputSize(0);
    if (outputSize <= 0) {
      outputSize = 1024;
    }
    std::vector<uint8_t> compressed(outputSize);

    // Compress empty data
    auto compressResult =
        compressor->compress(nullptr, 0, compressed.data(), compressed.size());
    ASSERT_EQ(compressResult.bytesRead, 0);

    int64_t totalWritten = compressResult.bytesWritten;

    // End stream
    auto endResult = compressor->end(
        compressed.data() + totalWritten, compressed.size() - totalWritten);
    int32_t retryCount = 0;
    while (endResult.shouldRetry) {
      totalWritten += endResult.bytesWritten;
      compressed.resize(compressed.size() * 2);
      endResult = compressor->end(
          compressed.data() + totalWritten, compressed.size() - totalWritten);
      ASSERT_LT(retryCount++, 8);
    }
    totalWritten += endResult.bytesWritten;

    // Decompress should produce empty output
    auto decompressor = StreamDecompressor::create(param.type, options);
    std::vector<uint8_t> decompressed(1024);

    auto decompressResult = decompressor->decompress(
        compressed.data(),
        totalWritten,
        decompressed.data(),
        decompressed.size());

    ASSERT_EQ(decompressResult.bytesWritten, 0);
  } else {
    // One-shot codec with empty input
    CodecOptions options;
    options.checksumEnabled = param.checksumEnabled;

    auto codec = Codec::create(param.type, options);
    std::vector<uint8_t> compressed(1024);

    int64_t compressedSize =
        codec->compress(nullptr, 0, compressed.data(), compressed.size());

    // Decompress
    std::vector<uint8_t> decompressed(1024);
    int64_t decompressedSize = codec->decompress(
        compressed.data(),
        compressedSize,
        decompressed.data(),
        decompressed.size());

    ASSERT_EQ(decompressedSize, 0);
  }
}

// Small input test (1KB)
TEST_P(CodecTest, SmallInput) {
  auto param = GetParam();
  auto data = generateCompressibleData(1024); // 1KB

  if (param.isStream) {
    verifyStreamRoundTrip(data);
  } else {
    verifyOneShotRoundTrip(data);
  }
}

// Multiple chunks test (stream only)
TEST_P(CodecTest, MultipleChunks) {
  auto param = GetParam();
  if (!param.isStream) {
    GTEST_SKIP() << "Only for stream codec";
  }

  CodecOptions options;
  options.checksumEnabled = param.checksumEnabled;

  // Generate test data
  auto data = generateCompressibleData(256 * 1024); // 256KB
  size_t chunkSize = 64 * 1024; // 64KB chunks

  // Create compressor
  auto compressor = StreamCompressor::create(param.type, options);
  std::vector<uint8_t> compressed(
      compressor->recommendedOutputSize(data.size()) * 2);

  int64_t totalWritten = 0;
  size_t inputOffset = 0;

  // Compress in chunks
  while (inputOffset < data.size()) {
    size_t remaining = data.size() - inputOffset;
    size_t currentChunk = std::min(chunkSize, remaining);

    auto compressResult = compressor->compress(
        data.data() + inputOffset,
        currentChunk,
        compressed.data() + totalWritten,
        compressed.size() - totalWritten);

    ASSERT_EQ(compressResult.bytesRead, static_cast<int64_t>(currentChunk));
    totalWritten += compressResult.bytesWritten;
    inputOffset += currentChunk;
  }

  // End stream
  auto endResult = compressor->end(
      compressed.data() + totalWritten, compressed.size() - totalWritten);
  while (endResult.shouldRetry) {
    totalWritten += endResult.bytesWritten;
    compressed.resize(compressed.size() * 2);
    endResult = compressor->end(
        compressed.data() + totalWritten, compressed.size() - totalWritten);
  }
  totalWritten += endResult.bytesWritten;

  // Decompress all at once
  auto decompressor = StreamDecompressor::create(param.type, options);
  std::vector<uint8_t> decompressed(data.size());

  auto decompressResult = decompressor->decompress(
      compressed.data(),
      totalWritten,
      decompressed.data(),
      decompressed.size());

  ASSERT_EQ(decompressResult.bytesWritten, static_cast<int64_t>(data.size()));
  ASSERT_EQ(0, memcmp(data.data(), decompressed.data(), data.size()));
}

// Reset test
TEST_P(CodecTest, Reset) {
  auto param = GetParam();
  auto data1 = generateCompressibleData(64 * 1024); // 64KB
  auto data2 = generateRandomData(64 * 1024, 123); // Different data

  if (param.isStream) {
    CodecOptions options;
    options.checksumEnabled = param.checksumEnabled;

    auto compressor = StreamCompressor::create(param.type, options);
    auto decompressor = StreamDecompressor::create(param.type, options);

    // First compression
    std::vector<uint8_t> compressed1(
        compressor->recommendedOutputSize(data1.size()) * 2);
    auto result1 = compressor->compress(
        data1.data(), data1.size(), compressed1.data(), compressed1.size());
    int64_t written1 = result1.bytesWritten;
    auto end1 = compressor->end(
        compressed1.data() + written1, compressed1.size() - written1);
    while (end1.shouldRetry) {
      written1 += end1.bytesWritten;
      compressed1.resize(compressed1.size() * 2);
      end1 = compressor->end(
          compressed1.data() + written1, compressed1.size() - written1);
    }
    written1 += end1.bytesWritten;

    // Reset compressor
    compressor->reset();

    // Second compression
    std::vector<uint8_t> compressed2(
        compressor->recommendedOutputSize(data2.size()) * 2);
    auto result2 = compressor->compress(
        data2.data(), data2.size(), compressed2.data(), compressed2.size());
    int64_t written2 = result2.bytesWritten;
    auto end2 = compressor->end(
        compressed2.data() + written2, compressed2.size() - written2);
    while (end2.shouldRetry) {
      written2 += end2.bytesWritten;
      compressed2.resize(compressed2.size() * 2);
      end2 = compressor->end(
          compressed2.data() + written2, compressed2.size() - written2);
    }
    written2 += end2.bytesWritten;

    // Decompress and verify both
    std::vector<uint8_t> decompressed1(data1.size());
    decompressor->decompress(
        compressed1.data(),
        written1,
        decompressed1.data(),
        decompressed1.size());
    ASSERT_EQ(0, memcmp(data1.data(), decompressed1.data(), data1.size()));

    // Reset decompressor
    decompressor->reset();

    std::vector<uint8_t> decompressed2(data2.size());
    decompressor->decompress(
        compressed2.data(),
        written2,
        decompressed2.data(),
        decompressed2.size());
    ASSERT_EQ(0, memcmp(data2.data(), decompressed2.data(), data2.size()));
  } else {
    // One-shot codec doesn't have state, just verify it works twice
    CodecOptions options;
    options.checksumEnabled = param.checksumEnabled;

    auto codec = Codec::create(param.type, options);

    // First compression
    std::vector<uint8_t> compressed1(codec->maxCompressedLen(data1.size()));
    int64_t size1 = codec->compress(
        data1.data(), data1.size(), compressed1.data(), compressed1.size());

    // Second compression (verify codec still works)
    std::vector<uint8_t> compressed2(codec->maxCompressedLen(data2.size()));
    int64_t size2 = codec->compress(
        data2.data(), data2.size(), compressed2.data(), compressed2.size());

    // Decompress and verify both
    std::vector<uint8_t> decompressed1(data1.size());
    codec->decompress(
        compressed1.data(), size1, decompressed1.data(), decompressed1.size());
    ASSERT_EQ(0, memcmp(data1.data(), decompressed1.data(), data1.size()));

    std::vector<uint8_t> decompressed2(data2.size());
    codec->decompress(
        compressed2.data(), size2, decompressed2.data(), decompressed2.size());
    ASSERT_EQ(0, memcmp(data2.data(), decompressed2.data(), data2.size()));
  }
}

// Generate test parameters
std::vector<CodecTestParam> buildCodecTestParams() {
  std::vector<CodecTestParam> params;

  // One-shot compression: all types
  std::vector<CodecType> allTypes = {
      CodecType::ZSTD,
      CodecType::GZIP,
      CodecType::LZ4,
      CodecType::LZ4_FRAME,
      CodecType::SNAPPY};

  for (auto type : allTypes) {
    // Without checksum (all types)
    params.push_back({type, false, false});

    // With checksum (only supported types)
    if (supportsChecksum(type)) {
      params.push_back({type, true, false});
    }
  }

  // Stream compression: only supported types, no checksum
  std::vector<CodecType> streamTypes = {
      CodecType::ZSTD, CodecType::GZIP, CodecType::LZ4_FRAME};

  for (auto type : streamTypes) {
    params.push_back({type, false, true});
  }

  return params;
}

INSTANTIATE_TEST_SUITE_P(
    Codec,
    CodecTest,
    testing::ValuesIn(buildCodecTestParams()),
    [](const testing::TestParamInfo<CodecTestParam>& info) {
      return info.param.toString();
    });

} // namespace bytedance::bolt::shuffle::sparksql::test
