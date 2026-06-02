#include "bolt/common/memory/bm/compress/CompressionManager.h"
#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"
#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"

#include <cstring>
#include <limits>
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
  return DecodeSpillRecordHeader(
             record.data(), record.length(), expectedRawSize)
      .storedSize;
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
        CompressionKind::kZstdFrame,
        recordKind(result.record, original.size()));
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
        CompressionKind::kSnappyRaw,
        recordKind(result.record, original.size()));
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

TEST_F(CompressionManagerTest, HeaderRejectsMalformedRecords) {
  SpillRecordHeader header;
  header.compressionKind = static_cast<uint32_t>(CompressionKind::kNone);
  header.rawSize = 4096;
  header.storedSize = 4096;

  auto encoded = EncodeSpillRecordHeader(header);

  EXPECT_THROW(
      DecodeSpillRecordHeader(
          encoded.data(), sizeof(SpillRecordHeader) - 1, 4096),
      std::exception);

  auto badMagic = encoded;
  reinterpret_cast<SpillRecordHeader*>(badMagic.data())->magic = 1;
  EXPECT_THROW(
      DecodeSpillRecordHeader(badMagic.data(), badMagic.size(), 4096),
      std::exception);

  auto badVersion = encoded;
  reinterpret_cast<SpillRecordHeader*>(badVersion.data())->version = 99;
  EXPECT_THROW(
      DecodeSpillRecordHeader(badVersion.data(), badVersion.size(), 4096),
      std::exception);

  auto smallHeader = encoded;
  reinterpret_cast<SpillRecordHeader*>(smallHeader.data())->headerSize =
      sizeof(SpillRecordHeader) - 1;
  EXPECT_THROW(
      DecodeSpillRecordHeader(smallHeader.data(), smallHeader.size(), 4096),
      std::exception);

  auto wrongRawSize = encoded;
  reinterpret_cast<SpillRecordHeader*>(wrongRawSize.data())->rawSize = 2048;
  EXPECT_THROW(
      DecodeSpillRecordHeader(wrongRawSize.data(), wrongRawSize.size(), 4096),
      std::exception);

  auto payloadOverflow = encoded;
  reinterpret_cast<SpillRecordHeader*>(payloadOverflow.data())->storedSize =
      4096;
  EXPECT_THROW(
      DecodeSpillRecordHeader(
          payloadOverflow.data(), sizeof(SpillRecordHeader), 4096),
      std::exception);
}

TEST_F(CompressionManagerTest, AlgorithmRejectsUnsupportedKindsAndStrategies) {
  CompressionConfig config;
  CompressionContextSet contexts;
  DecompressionContextSet decompressionContexts;
  const auto original = compressiblePayload(1024);
  std::vector<char> compressed(4096);
  std::vector<char> decoded(original.size());

  EXPECT_THROW(
      MaxCompressedLength(static_cast<CompressionKind>(99), original.size()),
      std::exception);
  EXPECT_THROW(
      CompressWithAlgorithm(
          contexts,
          static_cast<CompressionKind>(99),
          config,
          original.data(),
          original.size(),
          compressed.data(),
          compressed.size()),
      std::exception);
  EXPECT_THROW(
      DecompressWithAlgorithm(
          decompressionContexts,
          static_cast<CompressionKind>(99),
          compressed.data(),
          compressed.size(),
          decoded.data(),
          decoded.size()),
      std::exception);

  Lz4Options lz4;
  lz4.strategy = static_cast<Lz4Strategy>(99);
  EXPECT_THROW(
      Lz4Compress(
          nullptr,
          lz4,
          original.data(),
          original.size(),
          compressed.data(),
          compressed.size()),
      std::exception);

  ZstdOptions zstd;
  zstd.strategy = static_cast<ZstdStrategy>(99);
  EXPECT_THROW(
      ZstdCompress(
          nullptr,
          zstd,
          original.data(),
          original.size(),
          compressed.data(),
          compressed.size()),
      std::exception);

  SnappyOptions snappy;
  snappy.strategy = static_cast<SnappyStrategy>(99);
  EXPECT_THROW(
      SnappyCompress(
          snappy,
          original.data(),
          original.size(),
          compressed.data(),
          compressed.size()),
      std::exception);
}

TEST_F(CompressionManagerTest, AlgorithmsDecodeWithReusableContexts) {
  const auto original = compressiblePayload(64 * 1024);
  std::vector<char> compressed(
      MaxCompressedLength(CompressionKind::kZstdFrame, original.size()));
  std::vector<char> decoded(original.size());

  Lz4CompressionContext lz4Compression;
  Lz4DecompressionContext lz4Decompression;
  Lz4Options lz4;
  lz4.strategy = Lz4Strategy::kPooledContext;
  const auto lz4Bytes = Lz4Compress(
      &lz4Compression,
      lz4,
      original.data(),
      original.size(),
      compressed.data(),
      compressed.size());
  DecompressionContextSet lz4Contexts;
  lz4Contexts.lz4 = &lz4Decompression;
  DecompressWithAlgorithm(
      lz4Contexts,
      CompressionKind::kLz4Block,
      compressed.data(),
      lz4Bytes,
      decoded.data(),
      decoded.size());
  EXPECT_EQ(original, std::string(decoded.data(), decoded.size()));

  ZstdCompressionContext zstdCompression;
  ZstdDecompressionContext zstdDecompression;
  ZstdOptions zstd;
  zstd.strategy = ZstdStrategy::kPooledContext;
  const auto zstdBytes = ZstdCompress(
      &zstdCompression,
      zstd,
      original.data(),
      original.size(),
      compressed.data(),
      compressed.size());
  std::fill(decoded.begin(), decoded.end(), '\0');
  DecompressionContextSet zstdContexts;
  zstdContexts.zstd = &zstdDecompression;
  DecompressWithAlgorithm(
      zstdContexts,
      CompressionKind::kZstdFrame,
      compressed.data(),
      zstdBytes,
      decoded.data(),
      decoded.size());
  EXPECT_EQ(original, std::string(decoded.data(), decoded.size()));

  SnappyDecompressionContext snappyDecompression;
  SnappyOptions snappy;
  const auto snappyBytes = SnappyCompress(
      snappy,
      original.data(),
      original.size(),
      compressed.data(),
      compressed.size());
  std::fill(decoded.begin(), decoded.end(), '\0');
  DecompressionContextSet snappyContexts;
  snappyContexts.snappy = &snappyDecompression;
  DecompressWithAlgorithm(
      snappyContexts,
      CompressionKind::kSnappyRaw,
      compressed.data(),
      snappyBytes,
      decoded.data(),
      decoded.size());
  EXPECT_EQ(original, std::string(decoded.data(), decoded.size()));
}

TEST_F(CompressionManagerTest, AlgorithmsRejectInvalidCompressedPayloads) {
  const auto original = compressiblePayload(1024);
  std::vector<char> compressed(2048);
  std::vector<char> decoded(original.size());

  Lz4Options lz4;
  auto lz4Bytes = Lz4Compress(
      nullptr,
      lz4,
      original.data(),
      original.size(),
      compressed.data(),
      compressed.size());
  EXPECT_THROW(
      Lz4Decompress(
          nullptr,
          compressed.data(),
          lz4Bytes,
          decoded.data(),
          decoded.size() + 1),
      std::exception);

  ZstdOptions zstd;
  auto zstdBytes = ZstdCompress(
      nullptr,
      zstd,
      original.data(),
      original.size(),
      compressed.data(),
      compressed.size());
  EXPECT_THROW(
      ZstdDecompress(
          nullptr,
          compressed.data(),
          zstdBytes,
          decoded.data(),
          decoded.size() + 1),
      std::exception);

  EXPECT_THROW(
      SnappyDecompress(
          nullptr,
          "not-a-snappy-record",
          std::strlen("not-a-snappy-record"),
          decoded.data(),
          decoded.size()),
      std::exception);
}

TEST_F(CompressionManagerTest, Lz4RejectsOversizedInputs) {
  Lz4Options options;
  std::vector<char> oneByte(1);
  EXPECT_THROW(
      Lz4Compress(
          nullptr,
          options,
          oneByte.data(),
          static_cast<size_t>(std::numeric_limits<int>::max()) + 1,
          oneByte.data(),
          oneByte.size()),
      std::exception);
  EXPECT_THROW(
      Lz4Decompress(
          nullptr,
          oneByte.data(),
          static_cast<size_t>(std::numeric_limits<int>::max()) + 1,
          oneByte.data(),
          oneByte.size()),
      std::exception);
}

} // namespace
} // namespace bytedance::bolt::memory::bm::compress
