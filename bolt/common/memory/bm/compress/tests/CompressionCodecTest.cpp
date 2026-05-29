#include "bolt/common/memory/bm/compress/CompressionCodec.h"
#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"

#include <cstring>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm::compress {
namespace {

class CompressionCodecTest : public testing::Test {
 protected:
  IoBuffer makePayload(const std::string& text) {
    IoBuffer buffer{std::make_unique<char[]>(text.size()), text.size(), 0, text.size()};
    std::memcpy(buffer.data(), text.data(), text.size());
    return buffer;
  }

  std::string readPayload(const IoBuffer& buffer, size_t size) {
    return std::string(buffer.data(), buffer.data() + size);
  }

};

std::string compressiblePayload(size_t size) {
  std::string payload;
  payload.reserve(size);
  while (payload.size() < size) {
    payload.append("aaaaabbbbbcccccdddddeeeee");
  }
  payload.resize(size);
  return payload;
}

std::string incompressiblePayload(size_t size) {
  std::string payload(size, '\0');
  uint32_t state = 0x12345678;
  for (size_t i = 0; i < size; ++i) {
    state = state * 1103515245 + 12345;
    payload[i] = static_cast<char>((state >> 16) & 0xff);
  }
  return payload;
}

class CompressionKindTest
    : public CompressionCodecTest,
      public testing::WithParamInterface<CompressionKind> {};

TEST_P(CompressionKindTest, RoundTripsCompressedPayload) {
  CompressionConfig config;
  config.kind = GetParam();
  config.minCompressBytes = 1;
  config.minCompressionRatio = 1.0;
  const auto original = compressiblePayload(512 * 1024);

  auto compressed = TryCompress(makePayload(original), config, nullptr);

  ASSERT_TRUE(compressed.compressed);
  EXPECT_EQ(original.size(), compressed.rawSize);
  EXPECT_LT(compressed.storedSize, compressed.rawSize);
  EXPECT_EQ(config.kind, compressed.storedKind);

  uint64_t decompressionTimeUs = 0;
  auto decompressed = Decompress(
      std::move(compressed.buffer),
      compressed.rawSize,
      compressed.storedSize,
      compressed.storedKind,
      nullptr,
      &decompressionTimeUs);

  EXPECT_EQ(original, readPayload(decompressed, original.size()));
}

TEST_F(CompressionCodecTest, ZstdContextCodecCanBeReusedAcrossBlocks) {
  CompressionConfig config;
  config.kind = CompressionKind::kZstdContext;
  config.minCompressBytes = 1;
  config.minCompressionRatio = 1.0;
  CompressionCodec codec;

  for (auto size : {256 * 1024, 512 * 1024}) {
    const auto original = compressiblePayload(size);
    auto compressed = codec.TryCompress(makePayload(original), config, nullptr);

    ASSERT_TRUE(compressed.compressed);
    EXPECT_EQ(CompressionKind::kZstdContext, compressed.storedKind);

    uint64_t decompressionTimeUs = 0;
    auto decompressed = Decompress(
        std::move(compressed.buffer),
        compressed.rawSize,
        compressed.storedSize,
        compressed.storedKind,
        nullptr,
        &decompressionTimeUs);

    EXPECT_EQ(original, readPayload(decompressed, original.size()));
  }
}

INSTANTIATE_TEST_SUITE_P(
    Algorithms,
    CompressionKindTest,
    testing::Values(
        CompressionKind::kLz4,
        CompressionKind::kLz4Default,
        CompressionKind::kLz4Fast,
        CompressionKind::kLz4Context,
        CompressionKind::kZstd,
        CompressionKind::kZstdOneShot,
        CompressionKind::kZstdContext,
        CompressionKind::kSnappy,
        CompressionKind::kSnappyRaw,
        CompressionKind::kSnappyLevel));

TEST_F(CompressionCodecTest, NoneKeepsOriginalPayload) {
  CompressionConfig config;
  config.kind = CompressionKind::kNone;
  config.minCompressBytes = 1;
  const auto original = compressiblePayload(256 * 1024);

  auto result = TryCompress(makePayload(original), config, nullptr);

  EXPECT_FALSE(result.compressed);
  EXPECT_EQ(CompressionKind::kNone, result.storedKind);
  EXPECT_EQ(original.size(), result.rawSize);
  EXPECT_EQ(original.size(), result.storedSize);
  EXPECT_EQ(original, readPayload(result.buffer, original.size()));
}

TEST_F(CompressionCodecTest, PayloadBelowThresholdKeepsOriginalPayload) {
  CompressionConfig config;
  config.kind = CompressionKind::kLz4;
  config.minCompressBytes = 1024;
  const auto original = compressiblePayload(128);

  auto result = TryCompress(makePayload(original), config, nullptr);

  EXPECT_FALSE(result.compressed);
  EXPECT_EQ(CompressionKind::kNone, result.storedKind);
  EXPECT_EQ(original, readPayload(result.buffer, original.size()));
}

TEST_F(CompressionCodecTest, IncompressiblePayloadFallsBackToOriginal) {
  CompressionConfig config;
  config.kind = CompressionKind::kLz4;
  config.minCompressBytes = 1;
  config.minCompressionRatio = 0.5;
  const auto original = incompressiblePayload(256 * 1024);

  auto result = TryCompress(makePayload(original), config, nullptr);

  EXPECT_FALSE(result.compressed);
  EXPECT_EQ(CompressionKind::kNone, result.storedKind);
  EXPECT_EQ(original.size(), result.storedSize);
  EXPECT_EQ(original, readPayload(result.buffer, original.size()));
}

TEST_F(CompressionCodecTest, HeaderRoundTripValidatesRecordMetadata) {
  SpillRecordHeader header;
  header.compressionKind = static_cast<uint32_t>(CompressionKind::kZstd);
  header.rawSize = 4096;
  header.storedSize = 512;

  auto encoded = EncodeSpillRecordHeader(header);
  std::vector<char> record(encoded.size() + header.storedSize);
  std::memcpy(record.data(), encoded.data(), encoded.size());
  auto decoded = DecodeSpillRecordHeader(record.data(), record.size(), 4096);

  EXPECT_EQ(
      static_cast<uint32_t>(CompressionKind::kZstd),
      decoded.compressionKind);
  EXPECT_EQ(4096, decoded.rawSize);
  EXPECT_EQ(512, decoded.storedSize);
}

TEST_F(CompressionCodecTest, HeaderRejectsWrongExpectedRawSize) {
  SpillRecordHeader header;
  header.compressionKind = static_cast<uint32_t>(CompressionKind::kNone);
  header.rawSize = 4096;
  header.storedSize = 4096;

  auto encoded = EncodeSpillRecordHeader(header);

  EXPECT_THROW(
      DecodeSpillRecordHeader(encoded.data(), encoded.size(), 2048),
      std::exception);
}

} // namespace
} // namespace bytedance::bolt::memory::bm::compress
