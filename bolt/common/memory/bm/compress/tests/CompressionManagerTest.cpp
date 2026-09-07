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

#include "bolt/common/memory/bm/compress/CompressionManager.h"
#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"
#include "bolt/common/memory/bm/compress/CompressionRecord.h"
#include "bolt/common/memory/bm/compress/OpenZlCompression.h"
#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <future>
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

BlockDescriptor mixedFixedRowDescriptor(uint32_t elementCount) {
  return BlockDescriptor{
      .schemaKind = BlockSchemaKind::kFixedRow,
      .elementCount = elementCount,
      .schema =
          FixedRowBlockSchema{
              .rowStride = 24,
              .fields =
                  {
                      {BlockFieldKind::kSignedInteger, 0, 4},
                      {BlockFieldKind::kUnsignedInteger, 6, 2},
                      {BlockFieldKind::kFloatingPoint, 8, 8},
                      {BlockFieldKind::kOpaque, 16, 4},
                  },
          },
  };
}

std::string mixedFixedRowPayload(uint32_t elementCount, size_t tailBytes) {
  constexpr size_t kRowStride = 24;
  std::string payload(elementCount * kRowStride + tailBytes, '\0');
  for (uint32_t row = 0; row < elementCount; ++row) {
    auto* rowData = payload.data() + row * kRowStride;
    for (size_t byte = 0; byte < kRowStride; ++byte) {
      rowData[byte] = static_cast<char>((row * 13 + byte * 7) & 0xff);
    }
    const int32_t signedValue = -static_cast<int32_t>(row % 19);
    const uint16_t unsignedValue = static_cast<uint16_t>(row % 23);
    const double floatingValue = static_cast<double>(row % 11) * 0.25;
    std::memcpy(rowData, &signedValue, sizeof(signedValue));
    std::memcpy(rowData + 6, &unsignedValue, sizeof(unsignedValue));
    std::memcpy(rowData + 8, &floatingValue, sizeof(floatingValue));
  }
  for (size_t byte = 0; byte < tailBytes; ++byte) {
    payload[elementCount * kRowStride + byte] =
        static_cast<char>((byte * 29) & 0xff);
  }
  return payload;
}

uint64_t recordStoredSize(const IoBuffer& record, size_t expectedRawSize) {
  return DecodeSpillRecordHeader(
             record.data(), record.length(), expectedRawSize)
      .storedSize;
}

TEST(CompressionConfigTest, DefaultsToZstdLevelThree) {
  CompressionConfig config;

  EXPECT_EQ(CompressionKind::kZstdFrame, config.kind);
  EXPECT_EQ(ZstdStrategy::kOneShot, config.zstd.strategy);
  EXPECT_EQ(3, config.zstd.compressionLevel);
  EXPECT_EQ(256, config.openZl.graphCacheCapacity);
  EXPECT_EQ(0, config.openZl.maxOutputBytes);
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

TEST_F(CompressionManagerTest, RawSpillRejectsOversizedSimdCopyInputs) {
  CompressionConfig config;
  config.kind = CompressionKind::kNone;
  CompressionManager manager(config);

  std::vector<char> oneByte(1);
  const auto oversized =
      static_cast<size_t>(std::numeric_limits<int32_t>::max()) + 1;
  EXPECT_THROW(
      manager.BuildSpillRecord(
          std::span<const char>(oneByte.data(), oversized)),
      std::exception);

  SpillRecordHeader header;
  header.compressionKind = static_cast<uint32_t>(CompressionKind::kNone);
  header.rawSize = oversized;
  header.storedSize = oversized;
  auto encoded = EncodeSpillRecordHeader(header);

  EXPECT_THROW(
      manager.DecodeSpillRecord(
          std::span<const char>(
              encoded.data(), sizeof(SpillRecordHeader) + oversized),
          oversized,
          nullptr,
          nullptr),
      std::exception);
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

TEST_F(
    CompressionManagerTest,
    OpenZlFallsBackToZstdWithoutSupportedDescriptor) {
  CompressionConfig config;
  config.kind = CompressionKind::kOpenZlFrame;
  config.minCompressBytes = 1;
  CompressionManager manager(config);
  const auto original = compressiblePayload(64 * 1024);
  auto payload = makePayload(original);
  const BlockDescriptor opaque{
      .schemaKind = BlockSchemaKind::kOpaque,
      .elementCount = 0,
      .schema = OpaqueBlockSchema{},
  };

  for (const BlockDescriptor* descriptor : {
           static_cast<const BlockDescriptor*>(nullptr),
           &opaque,
       }) {
    auto result = manager.BuildSpillRecord(
        std::span<const char>(payload.data(), payload.length()), descriptor);
    EXPECT_EQ(CompressionKind::kZstdFrame, result.storedKind);
    EXPECT_EQ(
        CompressionKind::kZstdFrame,
        recordKind(result.record, original.size()));
    auto decoded = decodeRecord(manager, result.record, original.size());
    EXPECT_EQ(original, readPayload(decoded, original.size()));
  }

  auto unsupported = BlockDescriptor{
      .schemaKind = BlockSchemaKind::kFixedRow,
      .elementCount = 128,
      .schema =
          FixedRowBlockSchema{
              .rowStride = 3,
              .fields = {{BlockFieldKind::kSignedInteger, 0, 3}},
          },
  };
  auto result = manager.BuildSpillRecord(
      std::span<const char>(payload.data(), payload.length()), &unsupported);
  EXPECT_EQ(CompressionKind::kZstdFrame, result.storedKind);
}

TEST_F(CompressionManagerTest, OpenZlFixedRowRoundTripsMixedFieldsGapsAndTail) {
  CompressionConfig config;
  config.kind = CompressionKind::kOpenZlFrame;
  config.minCompressBytes = 1;
  CompressionManager manager(config);
  constexpr uint32_t kElementCount = 2048;
  const auto original = mixedFixedRowPayload(kElementCount, 19);
  const auto descriptor = mixedFixedRowDescriptor(kElementCount);
  auto payload = makePayload(original);

  auto result = manager.BuildSpillRecord(
      std::span<const char>(payload.data(), payload.length()), &descriptor);

  ASSERT_TRUE(result.compressed);
  EXPECT_EQ(CompressionKind::kOpenZlFrame, result.storedKind);
  EXPECT_EQ(
      CompressionKind::kOpenZlFrame,
      recordKind(result.record, original.size()));
  auto decoded = decodeRecord(manager, result.record, original.size());
  EXPECT_EQ(original, readPayload(decoded, original.size()));
}

TEST_F(
    CompressionManagerTest,
    OpenZlReportsCapacityShortageAndNormalCapacityRoundTrips) {
  CompressionConfig config;
  config.kind = CompressionKind::kOpenZlFrame;
  config.minCompressBytes = 1;
  CompressionManager manager(config);
  constexpr uint32_t kRows = 64;
  const auto descriptor = mixedFixedRowDescriptor(kRows);
  const auto original = mixedFixedRowPayload(kRows, 5);
  auto payload = makePayload(original);

  const auto fingerprint =
      OpenZlDescriptorFingerprint(descriptor, original.size());
  const auto compressor =
      BuildOpenZlCompressor(descriptor, original.size(), fingerprint);
  OpenZlCompressionContext context;
  std::array<char, 1> undersizedOutput{};
  const auto firstAttempt = OpenZlCompress(
      context,
      *compressor,
      original.data(),
      original.size(),
      undersizedOutput.data(),
      undersizedOutput.size());
  EXPECT_TRUE(firstAttempt.capacityTooSmall);

  auto result = manager.BuildSpillRecord(
      std::span<const char>(payload.data(), payload.length()), &descriptor);

  EXPECT_EQ(CompressionKind::kOpenZlFrame, result.storedKind);
  auto decoded = decodeRecord(manager, result.record, original.size());
  EXPECT_EQ(original, readPayload(decoded, original.size()));
}

TEST_F(CompressionManagerTest, OpenZlRespectsConfiguredOutputLimit) {
  CompressionConfig config;
  config.kind = CompressionKind::kOpenZlFrame;
  config.minCompressBytes = 1;
  config.openZl.maxOutputBytes = 1;
  CompressionManager manager(config);
  constexpr uint32_t kRows = 64;
  const auto descriptor = mixedFixedRowDescriptor(kRows);
  const auto original = mixedFixedRowPayload(kRows, 5);
  auto payload = makePayload(original);

  try {
    (void)manager.BuildSpillRecord(
        std::span<const char>(payload.data(), payload.length()), &descriptor);
    FAIL() << "expected the configured OpenZL output limit to fail";
  } catch (const std::exception& error) {
    const std::string message = error.what();
    EXPECT_NE(
        message.find("openzl_error=dstCapacity_tooSmall"), std::string::npos);
    EXPECT_NE(message.find("schema=fixed-row:"), std::string::npos);
    EXPECT_NE(
        message.find("raw_size=" + std::to_string(original.size())),
        std::string::npos);
    EXPECT_NE(message.find("capacity=1"), std::string::npos);
    EXPECT_NE(message.find("max_output_bytes=1"), std::string::npos);
  }
}

TEST_F(CompressionManagerTest, OpenZlGraphCacheReusesAndEvictsByFullShape) {
  CompressionConfig config;
  config.kind = CompressionKind::kOpenZlFrame;
  config.minCompressBytes = 1;
  config.openZl.graphCacheCapacity = 1;
  CompressionManager manager(config);

  auto compress = [&](uint32_t rows, size_t tail) {
    const auto original = mixedFixedRowPayload(rows, tail);
    const auto descriptor = mixedFixedRowDescriptor(rows);
    auto payload = makePayload(original);
    auto result = manager.BuildSpillRecord(
        std::span<const char>(payload.data(), payload.length()), &descriptor);
    EXPECT_EQ(CompressionKind::kOpenZlFrame, result.storedKind);
    auto decoded = decodeRecord(manager, result.record, original.size());
    EXPECT_EQ(original, readPayload(decoded, original.size()));
  };

  compress(64, 5);
  compress(64, 5);
  compress(65, 5);
  compress(64, 5);
}

TEST_F(CompressionManagerTest, OpenZlZeroCapacityDisablesGraphCache) {
  CompressionConfig config;
  config.kind = CompressionKind::kOpenZlFrame;
  config.minCompressBytes = 1;
  config.openZl.graphCacheCapacity = 0;
  CompressionManager manager(config);
  const auto original = mixedFixedRowPayload(64, 0);
  const auto descriptor = mixedFixedRowDescriptor(64);
  auto payload = makePayload(original);

  for (size_t i = 0; i < 2; ++i) {
    auto result = manager.BuildSpillRecord(
        std::span<const char>(payload.data(), payload.length()), &descriptor);
    auto decoded = decodeRecord(manager, result.record, original.size());
    EXPECT_EQ(original, readPayload(decoded, original.size()));
  }
}

TEST_F(CompressionManagerTest, OpenZlGraphAndContextsSupportConcurrentUse) {
  CompressionConfig config;
  config.kind = CompressionKind::kOpenZlFrame;
  config.minCompressBytes = 1;
  CompressionManager manager(config);
  constexpr uint32_t kRows = 512;
  constexpr size_t kThreads = 8;
  constexpr size_t kIterations = 10;
  const auto original = mixedFixedRowPayload(kRows, 11);
  const auto descriptor = mixedFixedRowDescriptor(kRows);

  std::vector<std::future<bool>> futures;
  for (size_t thread = 0; thread < kThreads; ++thread) {
    futures.push_back(std::async(std::launch::async, [&] {
      for (size_t iteration = 0; iteration < kIterations; ++iteration) {
        auto payload = makePayload(original);
        auto result = manager.BuildSpillRecord(
            std::span<const char>(payload.data(), payload.length()),
            &descriptor);
        auto decoded = decodeRecord(manager, result.record, original.size());
        if (readPayload(decoded, original.size()) != original) {
          return false;
        }
      }
      return true;
    }));
  }
  for (auto& future : futures) {
    EXPECT_TRUE(future.get());
  }
}

TEST_F(
    CompressionManagerTest,
    OpenZlFrameUsesUniversalDecoderAndRejectsDamage) {
  CompressionConfig config;
  config.kind = CompressionKind::kOpenZlFrame;
  config.minCompressBytes = 1;
  const auto original = mixedFixedRowPayload(256, 7);
  const auto descriptor = mixedFixedRowDescriptor(256);
  auto payload = makePayload(original);
  IoBuffer record;
  {
    CompressionManager writer(config);
    record = writer
                 .BuildSpillRecord(
                     std::span<const char>(payload.data(), payload.length()),
                     &descriptor)
                 .record;
  }

  CompressionManager reader(CompressionConfig{});
  auto decoded = decodeRecord(reader, record, original.size());
  EXPECT_EQ(original, readPayload(decoded, original.size()));
  EXPECT_THROW(
      decodeRecord(reader, record, original.size() + 1), std::exception);

  const auto header =
      DecodeSpillRecordHeader(record.data(), record.length(), original.size());
  std::memset(
      SpillRecordBody(record), 0, std::min<uint64_t>(header.storedSize, 32));
  try {
    reader.DecodeSpillRecord(
        std::span<const char>(record.data(), record.length()),
        original.size(),
        nullptr,
        nullptr,
        123);
    FAIL() << "expected damaged OpenZL frame to fail";
  } catch (const std::exception& error) {
    EXPECT_NE(
        std::string(error.what()).find("block_id=123"), std::string::npos);
  }
}

TEST_F(
    CompressionManagerTest,
    OpenZlEvictedFramesSurviveWriterManagerDestruction) {
  CompressionConfig config;
  config.kind = CompressionKind::kOpenZlFrame;
  config.minCompressBytes = 1;
  config.openZl.graphCacheCapacity = 1;

  std::vector<std::string> originals;
  std::vector<IoBuffer> records;
  {
    CompressionManager writer(config);
    for (const uint32_t rows : {64, 65}) {
      originals.push_back(mixedFixedRowPayload(rows, 5));
      const auto descriptor = mixedFixedRowDescriptor(rows);
      auto payload = makePayload(originals.back());
      auto result = writer.BuildSpillRecord(
          std::span<const char>(payload.data(), payload.length()), &descriptor);
      ASSERT_EQ(CompressionKind::kOpenZlFrame, result.storedKind);
      records.push_back(std::move(result.record));
    }
  }

  CompressionManager reader(CompressionConfig{});
  ASSERT_EQ(originals.size(), records.size());
  for (size_t i = 0; i < records.size(); ++i) {
    auto decoded = decodeRecord(reader, records[i], originals[i].size());
    EXPECT_EQ(originals[i], readPayload(decoded, originals[i].size()));
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
