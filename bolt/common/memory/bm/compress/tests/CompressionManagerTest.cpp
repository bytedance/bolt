#include "bolt/common/memory/bm/compress/CompressionManager.h"
#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"

#include <cstring>
#include <span>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm::compress {
namespace {

class CompressionManagerTest : public testing::Test {
 protected:
  IoBuffer makePayload(const std::string& text) {
    auto buffer = IoBuffer::allocateFromMalloc(text.size());
    std::memcpy(buffer.data(), text.data(), text.size());
    return buffer;
  }

  std::string readPayload(const IoBuffer& buffer, size_t size) {
    return std::string(buffer.data(), buffer.data() + size);
  }

  IoBuffer decodeRecord(
      CompressionManager& manager,
      const IoBuffer& record,
      size_t expectedRawSize) {
    return manager.DecodeSpillRecord(
        std::span<const char>(record.data(), record.length()),
        expectedRawSize,
        nullptr,
        nullptr);
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

CompressionKind recordKind(const IoBuffer& record, size_t expectedRawSize) {
  return static_cast<CompressionKind>(
      DecodeSpillRecordHeader(record.data(), record.length(), expectedRawSize)
          .compressionKind);
}

uint64_t recordStoredSize(const IoBuffer& record, size_t expectedRawSize) {
  return DecodeSpillRecordHeader(record.data(), record.length(), expectedRawSize)
      .storedSize;
}

TEST(IoBufferTest, AllocateFromMallocOwnsWritableMemory) {
  auto buffer = IoBuffer::allocateFromMalloc(128);

  ASSERT_TRUE(buffer.valid());
  EXPECT_EQ(128, buffer.size());
  EXPECT_EQ(128, buffer.length());
  std::memset(buffer.data(), 42, buffer.length());
  EXPECT_EQ(42, buffer.data()[127]);
}

TEST_F(CompressionManagerTest, NoneBuildsUncompressedMallocBackedRecord) {
  CompressionConfig config;
  config.kind = CompressionKind::kNone;
  CompressionManager manager(config);
  const auto original = compressiblePayload(128 * 1024);
  auto payload = makePayload(original);

  auto result = manager.BuildSpillRecord(
      std::span<const char>(payload.data(), payload.length()));

  EXPECT_FALSE(result.compressed);
  EXPECT_EQ(CompressionKind::kNone, result.storedKind);
  EXPECT_EQ(CompressionKind::kNone, recordKind(result.record, original.size()));
  EXPECT_EQ(original.size(), recordStoredSize(result.record, original.size()));
  auto decoded = decodeRecord(manager, result.record, original.size());
  EXPECT_EQ(original, readPayload(decoded, original.size()));
}

TEST_F(CompressionManagerTest, PayloadBelowThresholdBuildsUncompressedRecord) {
  CompressionConfig config;
  config.kind = CompressionKind::kLz4Block;
  config.minCompressBytes = 1024;
  CompressionManager manager(config);
  const auto original = compressiblePayload(128);
  auto payload = makePayload(original);

  auto result = manager.BuildSpillRecord(
      std::span<const char>(payload.data(), payload.length()));

  EXPECT_FALSE(result.compressed);
  EXPECT_EQ(CompressionKind::kNone, result.storedKind);
  EXPECT_EQ(CompressionKind::kNone, recordKind(result.record, original.size()));
  auto decoded = decodeRecord(manager, result.record, original.size());
  EXPECT_EQ(original, readPayload(decoded, original.size()));
}

TEST_F(CompressionManagerTest, Lz4StrategiesWriteStableLz4BlockKind) {
  for (const auto strategy : {
           Lz4Strategy::kDefault,
           Lz4Strategy::kFast,
           Lz4Strategy::kPooledContext,
       }) {
    CompressionConfig config;
    config.kind = CompressionKind::kLz4Block;
    config.minCompressBytes = 1;
    config.lz4.strategy = strategy;
    config.lz4.acceleration = 2;
    CompressionManager manager(config);
    const auto original = compressiblePayload(512 * 1024);
    auto payload = makePayload(original);
    const auto before = readPayload(payload, original.size());

    auto result = manager.BuildSpillRecord(
        std::span<const char>(payload.data(), payload.length()));

    EXPECT_EQ(before, readPayload(payload, original.size()));
    ASSERT_TRUE(result.compressed);
    EXPECT_EQ(CompressionKind::kLz4Block, result.storedKind);
    EXPECT_EQ(
        CompressionKind::kLz4Block, recordKind(result.record, original.size()));
    auto decoded = decodeRecord(manager, result.record, original.size());
    EXPECT_EQ(original, readPayload(decoded, original.size()));
  }
}

TEST_F(CompressionManagerTest, ZstdStrategiesWriteStableZstdFrameKind) {
  for (const auto strategy : {
           ZstdStrategy::kOneShot,
           ZstdStrategy::kPooledContext,
       }) {
    CompressionConfig config;
    config.kind = CompressionKind::kZstdFrame;
    config.minCompressBytes = 1;
    config.zstd.strategy = strategy;
    config.zstd.compressionLevel = 3;
    CompressionManager manager(config);
    const auto original = compressiblePayload(512 * 1024);
    auto payload = makePayload(original);

    auto result = manager.BuildSpillRecord(
        std::span<const char>(payload.data(), payload.length()));

    ASSERT_TRUE(result.compressed);
    EXPECT_EQ(CompressionKind::kZstdFrame, result.storedKind);
    EXPECT_EQ(
        CompressionKind::kZstdFrame, recordKind(result.record, original.size()));
    auto decoded = decodeRecord(manager, result.record, original.size());
    EXPECT_EQ(original, readPayload(decoded, original.size()));
  }
}

TEST_F(CompressionManagerTest, SnappyStrategiesWriteStableSnappyRawKind) {
  for (const auto strategy : {
           SnappyStrategy::kRaw,
           SnappyStrategy::kWithOptions,
       }) {
    CompressionConfig config;
    config.kind = CompressionKind::kSnappyRaw;
    config.minCompressBytes = 1;
    config.snappy.strategy = strategy;
    config.snappy.compressionLevel = 2;
    CompressionManager manager(config);
    const auto original = compressiblePayload(512 * 1024);
    auto payload = makePayload(original);

    auto result = manager.BuildSpillRecord(
        std::span<const char>(payload.data(), payload.length()));

    ASSERT_TRUE(result.compressed);
    EXPECT_EQ(CompressionKind::kSnappyRaw, result.storedKind);
    EXPECT_EQ(
        CompressionKind::kSnappyRaw, recordKind(result.record, original.size()));
    auto decoded = decodeRecord(manager, result.record, original.size());
    EXPECT_EQ(original, readPayload(decoded, original.size()));
  }
}

TEST_F(CompressionManagerTest, HeaderRejectsUnknownCompressionKind) {
  SpillRecordHeader header;
  header.compressionKind = 99;
  header.rawSize = 4096;
  header.storedSize = 4096;

  auto encoded = EncodeSpillRecordHeader(header);

  EXPECT_THROW(
      DecodeSpillRecordHeader(encoded.data(), encoded.size(), 4096),
      std::exception);
}

} // namespace
} // namespace bytedance::bolt::memory::bm::compress
