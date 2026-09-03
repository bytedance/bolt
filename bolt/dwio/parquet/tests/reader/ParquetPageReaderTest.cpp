/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/dwio/parquet/reader/PageReader.h"
#include "bolt/dwio/parquet/reader/ParquetData.h"
#include "bolt/dwio/parquet/reader/ParquetTypeWithId.h"
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"

#include <arrow/util/rle_encoding.h>
#include <snappy.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <zstd.h>
#include <atomic>
#include <limits>
#include <random>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/dwio/common/DirectBufferedInput.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;

namespace bytedance::bolt::parquet {
namespace {

void appendInt32(std::string& out, int32_t value) {
  out.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

std::string encodeLevels(const std::vector<int16_t>& levels, int bitWidth) {
  std::string encoded(
      ::arrow::util::RleEncoder::MaxBufferSize(bitWidth, levels.size()) +
          ::arrow::util::RleEncoder::MinBufferSize(bitWidth),
      '\0');
  ::arrow::util::RleEncoder encoder(
      reinterpret_cast<uint8_t*>(encoded.data()), encoded.size(), bitWidth);
  for (auto level : levels) {
    encoder.Put(level);
  }
  auto encodedSize = encoder.Flush();
  encoded.resize(encodedSize);
  return encoded;
}

std::string makeRepDefPrefix(std::string& expectedData) {
  auto repetitionLevels = encodeLevels({0, 1, 1, 0, 1, 0, 0, 1}, 1);
  auto definitionLevels = encodeLevels({2, 2, 1, 2, 2, 0, 1, 2}, 2);

  std::string prefix;
  appendInt32(prefix, repetitionLevels.size());
  prefix.append(repetitionLevels);
  appendInt32(prefix, definitionLevels.size());
  prefix.append(definitionLevels);

  expectedData = prefix;
  for (int32_t i = 0; i < 128; ++i) {
    appendInt32(expectedData, i);
  }
  return prefix;
}

std::string zstdCompress(const std::string& data) {
  std::string compressed(ZSTD_compressBound(data.size()), '\0');
  auto compressedSize = ZSTD_compress(
      compressed.data(), compressed.size(), data.data(), data.size(), 1);
  BOLT_CHECK(!ZSTD_isError(compressedSize), ZSTD_getErrorName(compressedSize));
  compressed.resize(compressedSize);
  return compressed;
}

std::string snappyCompress(const std::string& data) {
  std::string compressed;
  snappy::Compress(data.data(), data.size(), &compressed);
  return compressed;
}

thrift::PageHeader makePageHeader(
    int32_t compressedSize,
    int32_t uncompressedSize,
    int32_t numValues = 8) {
  thrift::DataPageHeader dataPageHeader;
  dataPageHeader.__set_num_values(numValues);
  dataPageHeader.__set_encoding(thrift::Encoding::PLAIN);
  dataPageHeader.__set_definition_level_encoding(thrift::Encoding::RLE);
  dataPageHeader.__set_repetition_level_encoding(thrift::Encoding::RLE);

  thrift::PageHeader pageHeader;
  pageHeader.__set_type(thrift::PageType::DATA_PAGE);
  pageHeader.__set_compressed_page_size(compressedSize);
  pageHeader.__set_uncompressed_page_size(uncompressedSize);
  pageHeader.__set_data_page_header(dataPageHeader);
  return pageHeader;
}

std::string serializePageHeader(const thrift::PageHeader& pageHeader) {
  auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
  apache::thrift::protocol::TCompactProtocolT<
      apache::thrift::transport::TMemoryBuffer>
      protocol(buffer);
  pageHeader.write(&protocol);

  uint8_t* data = nullptr;
  uint32_t size = 0;
  buffer->getBuffer(&data, &size);
  return std::string(reinterpret_cast<const char*>(data), size);
}

std::string makeDataPageV2(
    int32_t numValues,
    const std::string& repetitionLevels,
    const std::string& definitionLevels,
    const std::string& values,
    int32_t compressedValueSize,
    int32_t uncompressedValueSize) {
  thrift::DataPageHeaderV2 dataPageHeader;
  dataPageHeader.__set_num_values(numValues);
  dataPageHeader.__set_num_nulls(0);
  dataPageHeader.__set_num_rows(numValues);
  dataPageHeader.__set_encoding(thrift::Encoding::PLAIN);
  dataPageHeader.__set_definition_levels_byte_length(definitionLevels.size());
  dataPageHeader.__set_repetition_levels_byte_length(repetitionLevels.size());
  dataPageHeader.__set_is_compressed(true);

  thrift::PageHeader pageHeader;
  pageHeader.__set_type(thrift::PageType::DATA_PAGE_V2);
  pageHeader.__set_data_page_header_v2(dataPageHeader);
  pageHeader.__set_compressed_page_size(
      repetitionLevels.size() + definitionLevels.size() + compressedValueSize);
  pageHeader.__set_uncompressed_page_size(
      repetitionLevels.size() + definitionLevels.size() +
      uncompressedValueSize);
  return serializePageHeader(pageHeader) + repetitionLevels + definitionLevels +
      values;
}

struct NestedInt64Type {
  ParquetTypeWithIdPtr root;
  ParquetTypeWithIdPtr list;
  ParquetTypeWithIdPtr leaf;
};

NestedInt64Type makeNestedInt64Type() {
  auto leaf = std::make_shared<ParquetTypeWithId>(
      BIGINT(),
      std::vector<std::shared_ptr<const dwio::common::TypeWithId>>{},
      2,
      2,
      0,
      "element",
      thrift::Type::INT64,
      std::nullopt,
      std::nullopt,
      1,
      2,
      false,
      false);
  auto list = std::make_shared<ParquetTypeWithId>(
      ARRAY(BIGINT()),
      std::vector<std::shared_ptr<const TypeWithId>>{leaf},
      1,
      2,
      ParquetTypeWithId::kNonLeaf,
      "list",
      std::nullopt,
      std::nullopt,
      std::nullopt,
      1,
      1,
      true,
      true);
  auto root = std::make_shared<ParquetTypeWithId>(
      ROW({"list"}, {ARRAY(BIGINT())}),
      std::vector<std::shared_ptr<const TypeWithId>>{list},
      0,
      2,
      ParquetTypeWithId::kNonLeaf,
      "schema",
      std::nullopt,
      std::nullopt,
      std::nullopt,
      0,
      0,
      false,
      false);
  return {std::move(root), std::move(list), std::move(leaf)};
}

class CountingInMemoryReadFile final : public InMemoryReadFile {
 public:
  explicit CountingInMemoryReadFile(std::string data)
      : InMemoryReadFile(std::move(data)) {}

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer)
      const override {
    ++readCalls_;
    readBytes_ += length;
    return InMemoryReadFile::pread(offset, length, buffer);
  }

  uint64_t readCalls() const {
    return readCalls_;
  }

  uint64_t readBytes() const {
    return readBytes_;
  }

 private:
  mutable std::atomic<uint64_t> readCalls_{0};
  mutable std::atomic<uint64_t> readBytes_{0};
};

struct DataPageV1TestData {
  std::string body;
  std::string encoded;
  size_t encodedBodySize;
};

DataPageV1TestData makeDataPageV1(
    thrift::CompressionCodec::type codec,
    int32_t numRows,
    int32_t valuesPerRow,
    uint64_t seed,
    bool emptyLastRow = false) {
  const auto fullRows = numRows - (emptyLastRow ? 1 : 0);
  const auto numPresentValues = fullRows * valuesPerRow;
  const auto numLevels = numPresentValues + (emptyLastRow ? 1 : 0);
  std::vector<int16_t> repetitionLevels(numLevels, 1);
  for (int32_t i = 0; i < numPresentValues; i += valuesPerRow) {
    repetitionLevels[i] = 0;
  }
  std::vector<int16_t> definitionLevels(numLevels, 2);
  if (emptyLastRow) {
    repetitionLevels.back() = 0;
    definitionLevels.back() = 1;
  }

  std::string body;
  const auto encodedRepetitionLevels = encodeLevels(repetitionLevels, 1);
  appendInt32(body, encodedRepetitionLevels.size());
  body.append(encodedRepetitionLevels);
  const auto encodedDefinitionLevels = encodeLevels(definitionLevels, 2);
  appendInt32(body, encodedDefinitionLevels.size());
  body.append(encodedDefinitionLevels);

  std::mt19937_64 rng(seed);
  for (int32_t i = 0; i < numPresentValues; ++i) {
    const auto value = rng();
    body.append(reinterpret_cast<const char*>(&value), sizeof(value));
  }

  std::string encodedBody;
  if (codec == thrift::CompressionCodec::UNCOMPRESSED) {
    encodedBody = body;
  } else if (codec == thrift::CompressionCodec::ZSTD) {
    encodedBody = zstdCompress(body);
  } else {
    BOLT_CHECK_EQ(codec, thrift::CompressionCodec::SNAPPY);
    encodedBody = snappyCompress(body);
  }
  const auto encodedBodySize = encodedBody.size();
  auto encoded = serializePageHeader(
                     makePageHeader(encodedBodySize, body.size(), numLevels)) +
      encodedBody;
  return {std::move(body), std::move(encoded), encodedBodySize};
}

struct StreamingV1Reader {
  std::shared_ptr<memory::MemoryPool> pool;
  std::shared_ptr<CountingInMemoryReadFile> file;
  std::unique_ptr<DirectBufferedInput> input;
  NestedInt64Type nestedType;
  std::unique_ptr<PageReader> reader;
};

StreamingV1Reader makeStreamingV1Reader(
    const std::shared_ptr<memory::MemoryPool>& rootPool,
    const std::string& columnChunk,
    int32_t loadQuantum,
    int32_t memoryLimit,
    const std::string& poolName) {
  auto pool = rootPool->addLeafChild(poolName);
  auto file = std::make_shared<CountingInMemoryReadFile>(columnChunk);
  auto ioStats = std::make_shared<io::IoStatistics>();
  dwio::common::ReaderOptions options{pool.get()};
  options.setLoadQuantum(loadQuantum);
  auto input = std::make_unique<DirectBufferedInput>(
      file,
      MetricsLog::voidLog(),
      1,
      nullptr,
      1,
      ioStats,
      nullptr,
      options,
      nullptr);

  StreamIdentifier streamId(0);
  auto streams = input->enqueuePair(Region{0, columnChunk.size()}, &streamId);
  input->load(LogType::FILE);

  auto nestedType = makeNestedInt64Type();
  auto reader = std::make_unique<PageReader>(
      std::move(streams.first),
      *pool,
      nestedType.leaf,
      thrift::CompressionCodec::UNCOMPRESSED,
      columnChunk.size(),
      nullptr,
      std::move(streams.second));
  reader->setParquetRepDefMemoryLimit(memoryLimit);
  reader->setParquetRepDefStreamingWindowSize(2'048);

  return {
      std::move(pool),
      std::move(file),
      std::move(input),
      std::move(nestedType),
      std::move(reader)};
}

std::vector<int32_t> readListLengths(
    StreamingV1Reader& input,
    int32_t numRows) {
  arrow::LevelInfo listInfo;
  EXPECT_EQ(input.nestedType.list->makeLevelInfo(listInfo), LevelMode::kList);
  std::vector<int32_t> lengths(numRows);
  std::vector<uint64_t> nulls(bits::nwords(numRows));
  const auto [repDefBegin, repDefEnd] = input.reader->repDefRange();
  EXPECT_EQ(
      input.reader->getLengthsAndNulls(
          LevelMode::kList,
          listInfo,
          repDefBegin,
          repDefEnd,
          numRows,
          lengths.data(),
          nulls.data(),
          0),
      numRows);
  EXPECT_TRUE(bits::isAllSet(nulls.data(), 0, numRows, bits::kNotNull));
  return lengths;
}

} // namespace

class ParquetPageReaderTest : public ParquetTestBase {};

TEST_F(ParquetPageReaderTest, rowGroupRegionRejectsOverflow) {
  thrift::ColumnMetaData metadata;
  metadata.__set_data_page_offset(std::numeric_limits<int64_t>::max() - 4);
  metadata.__set_total_compressed_size(16);

  thrift::ColumnChunk column;
  column.__set_meta_data(metadata);
  thrift::RowGroup rowGroup;
  rowGroup.__set_columns({column});
  std::vector<thrift::RowGroup> rowGroups{rowGroup};

  std::vector<thrift::SchemaElement> schema;
  SchemaHelper schemaHelper(schema);
  auto nestedType = makeNestedInt64Type();
  ParquetData parquetData(
      nestedType.leaf,
      rowGroups,
      *leafPool_,
      nullptr,
      nullptr,
      schemaHelper,
      false,
      false,
      0,
      0,
      0);

  BOLT_ASSERT_THROW(
      parquetData.getRowGroupRegion(0),
      "Column chunk region overflows int64_t");

  rowGroups[0].columns[0].meta_data.__set_total_compressed_size(4);
  EXPECT_EQ(
      parquetData.getRowGroupRegion(0),
      std::make_pair(std::numeric_limits<int64_t>::max() - 4, int64_t{4}));
}

TEST_F(ParquetPageReaderTest, smallPage) {
  auto readFile =
      std::make_shared<LocalReadFile>(getExampleFilePath("small_page_header"));
  auto file = std::make_shared<ReadFileInputStream>(std::move(readFile));
  auto headerSize = file->getLength();
  auto inputStream = std::make_unique<SeekableFileInputStream>(
      std::move(file), 0, headerSize, *leafPool_, LogType::TEST);
  auto pageReader = std::make_unique<PageReader>(
      std::move(inputStream),
      *leafPool_,
      thrift::CompressionCodec::type::GZIP,
      headerSize);
  auto header = pageReader->readPageHeader();
  EXPECT_EQ(header.type, thrift::PageType::type::DATA_PAGE);
  EXPECT_EQ(header.uncompressed_page_size, 16950);
  EXPECT_EQ(header.compressed_page_size, 10759);
  EXPECT_EQ(header.data_page_header.num_values, 21738);

  // expectedMinValue: "aaaa...aaaa"
  std::string expectedMinValue(39, 'a');
  // expectedMaxValue: "zzzz...zzzz"
  std::string expectedMaxValue(49, 'z');
  auto minValue = header.data_page_header.statistics.min_value;
  auto maxValue = header.data_page_header.statistics.max_value;
  EXPECT_EQ(minValue, expectedMinValue);
  EXPECT_EQ(maxValue, expectedMaxValue);
}

TEST_F(ParquetPageReaderTest, largePage) {
  auto readFile =
      std::make_shared<LocalReadFile>(getExampleFilePath("large_page_header"));
  auto file = std::make_shared<ReadFileInputStream>(std::move(readFile));
  auto headerSize = file->getLength();
  auto inputStream = std::make_unique<SeekableFileInputStream>(
      std::move(file), 0, headerSize, *leafPool_, LogType::TEST);
  auto pageReader = std::make_unique<PageReader>(
      std::move(inputStream),
      *leafPool_,
      thrift::CompressionCodec::type::GZIP,
      headerSize);
  auto header = pageReader->readPageHeader();

  EXPECT_EQ(header.type, thrift::PageType::type::DATA_PAGE);
  EXPECT_EQ(header.uncompressed_page_size, 1050822);
  EXPECT_EQ(header.compressed_page_size, 66759);
  EXPECT_EQ(header.data_page_header.num_values, 970);

  // expectedMinValue: "aaaa...aaaa"
  std::string expectedMinValue(1295, 'a');
  // expectedMinValue: "zzzz...zzzz"
  std::string expectedMaxValue(2255, 'z');
  auto minValue = header.data_page_header.statistics.min_value;
  auto maxValue = header.data_page_header.statistics.max_value;
  EXPECT_EQ(minValue, expectedMinValue);
  EXPECT_EQ(maxValue, expectedMaxValue);
}

TEST_F(ParquetPageReaderTest, corruptedPageHeader) {
  auto readFile = std::make_shared<LocalReadFile>(
      getExampleFilePath("corrupted_page_header"));
  auto file = std::make_shared<ReadFileInputStream>(std::move(readFile));
  auto headerSize = file->getLength();
  auto inputStream = std::make_unique<SeekableFileInputStream>(
      std::move(file), 0, headerSize, *leafPool_, LogType::TEST);

  // In the corrupted_page_header, the min_value length is set incorrectly on
  // purpose. This is to simulate the situation where the Parquet Page Header is
  // corrupted. And an error is expected to be thrown.
  auto pageReader = std::make_unique<PageReader>(
      std::move(inputStream),
      *leafPool_,
      thrift::CompressionCodec::type::GZIP,
      headerSize);

  EXPECT_THROW(pageReader->readPageHeader(), BoltException);
}

TEST_F(
    ParquetPageReaderTest,
    streamingDataPageV2RepDefReadSkipsValueDecompression) {
  constexpr int32_t kNumValues = 4;
  const auto repetitionLevels = encodeLevels({0, 0, 0, 0}, 1);
  const auto definitionLevels = encodeLevels({2, 2, 2, 2}, 2);

  std::string invalidCompressedValues;
  const uint32_t zstdMagic = ZSTD_MAGICNUMBER;
  invalidCompressedValues.append(
      reinterpret_cast<const char*>(&zstdMagic), sizeof(zstdMagic));
  invalidCompressedValues.append(4, '\0');

  const std::string page = makeDataPageV2(
      kNumValues,
      repetitionLevels,
      definitionLevels,
      invalidCompressedValues,
      invalidCompressedValues.size(),
      1'024);
  const std::string pages = page + page;
  auto input =
      std::make_unique<SeekableArrayInputStream>(pages.data(), pages.size());
  auto repDefInput =
      std::make_unique<SeekableArrayInputStream>(pages.data(), pages.size());
  auto nestedType = makeNestedInt64Type();

  dwio::common::RuntimeStatistics stats;
  PageReader pageReader(
      std::move(input),
      *leafPool_,
      nestedType.leaf,
      thrift::CompressionCodec::ZSTD,
      pages.size(),
      &stats,
      std::move(repDefInput));

  EXPECT_NO_THROW(pageReader.decodeRepDefs(kNumValues - 1));
  arrow::LevelInfo listInfo;
  EXPECT_EQ(nestedType.list->makeLevelInfo(listInfo), LevelMode::kList);
  EXPECT_EQ(pageReader.repDefOutputSize(LevelMode::kList, listInfo), 3);
  EXPECT_NO_THROW(pageReader.decodeRepDefs(1));
  EXPECT_EQ(pageReader.repDefOutputSize(LevelMode::kList, listInfo), 1);
  EXPECT_NO_THROW(pageReader.decodeRepDefs(1));
  EXPECT_EQ(pageReader.repDefOutputSize(LevelMode::kList, listInfo), 1);
  EXPECT_EQ(stats.decompressDataTimeNs, 0);
}

TEST_F(ParquetPageReaderTest, zeroStreamingWindowUsesLegacyRepDefPath) {
  constexpr int32_t kNumValues = 4;
  const auto repetitionLevels = encodeLevels({0, 0, 0, 0}, 1);
  const auto definitionLevels = encodeLevels({2, 2, 2, 2}, 2);
  const auto page =
      makeDataPageV2(kNumValues, repetitionLevels, definitionLevels, "", 0, 0);
  auto nestedType = makeNestedInt64Type();

  dwio::common::RuntimeStatistics stats;
  PageReader pageReader(
      std::make_unique<SeekableArrayInputStream>(page.data(), page.size()),
      *leafPool_,
      nestedType.leaf,
      thrift::CompressionCodec::UNCOMPRESSED,
      page.size(),
      &stats,
      std::make_unique<SeekableArrayInputStream>(
          static_cast<const char*>(nullptr), 0));
  pageReader.setParquetRepDefStreamingWindowSize(0);

  EXPECT_NO_THROW(pageReader.decodeRepDefs(kNumValues));
  arrow::LevelInfo listInfo;
  EXPECT_EQ(nestedType.list->makeLevelInfo(listInfo), LevelMode::kList);
  EXPECT_EQ(
      pageReader.repDefOutputSize(LevelMode::kList, listInfo), kNumValues);
}

TEST_F(ParquetPageReaderTest, streamingRepDefsSkipEmptyDataPages) {
  auto emptyV1Header = makePageHeader(0, 0);
  emptyV1Header.data_page_header.__set_num_values(0);
  const auto emptyV1 = serializePageHeader(emptyV1Header);
  const auto emptyV2 = makeDataPageV2(0, "", "", "", 0, 0);
  const auto repetitionLevels = encodeLevels({0, 0}, 1);
  const auto definitionLevels = encodeLevels({1, 1}, 1);
  const auto dataV2 =
      makeDataPageV2(2, repetitionLevels, definitionLevels, "", 0, 0);
  const auto pages = emptyV1 + emptyV2 + dataV2;

  auto nestedType = makeNestedInt64Type();
  dwio::common::RuntimeStatistics stats;
  PageReader pageReader(
      std::make_unique<SeekableArrayInputStream>(pages.data(), pages.size()),
      *leafPool_,
      nestedType.leaf,
      thrift::CompressionCodec::UNCOMPRESSED,
      pages.size(),
      &stats,
      std::make_unique<SeekableArrayInputStream>(pages.data(), pages.size()));

  EXPECT_NO_THROW(pageReader.decodeRepDefs(1));
  arrow::LevelInfo listInfo;
  EXPECT_EQ(nestedType.list->makeLevelInfo(listInfo), LevelMode::kList);
  EXPECT_EQ(pageReader.repDefOutputSize(LevelMode::kList, listInfo), 1);
}

TEST_F(ParquetPageReaderTest, streamingDataPageV2RejectsTruncatedValueBody) {
  constexpr int32_t kNumValues = 4;
  constexpr int32_t kMissingValueBytes = 8;
  const auto repetitionLevels = encodeLevels({0, 0, 0, 0}, 1);
  const auto definitionLevels = encodeLevels({1, 1, 1, 1}, 1);
  const auto truncatedPage = makeDataPageV2(
      kNumValues,
      repetitionLevels,
      definitionLevels,
      "",
      kMissingValueBytes,
      kMissingValueBytes);
  const auto declaredChunkSize = truncatedPage.size() + kMissingValueBytes;

  auto nestedType = makeNestedInt64Type();
  dwio::common::RuntimeStatistics stats;
  PageReader pageReader(
      std::make_unique<SeekableArrayInputStream>(
          truncatedPage.data(), truncatedPage.size()),
      *leafPool_,
      nestedType.leaf,
      thrift::CompressionCodec::ZSTD,
      declaredChunkSize,
      &stats,
      std::make_unique<SeekableArrayInputStream>(
          truncatedPage.data(), truncatedPage.size()));

  EXPECT_NO_THROW(pageReader.decodeRepDefs(kNumValues - 1));
  EXPECT_THROW(pageReader.decodeRepDefs(1), BoltException);
}

TEST(CompressionOptionsTest, testCompressionOptions) {
  auto options = getParquetDecompressionOptions(
      bytedance::bolt::common::CompressionKind_ZLIB);
  EXPECT_EQ(
      options.format.zlib.windowBits,
      dwio::common::compression::Compressor::PARQUET_ZLIB_WINDOW_BITS);
}

TEST_F(ParquetPageReaderTest, zstdDataPageV1RepDefPrefix) {
  std::string pageBody;
  makeRepDefPrefix(pageBody);
  auto compressed = zstdCompress(pageBody);
  auto pageHeader = makePageHeader(compressed.size(), pageBody.size());
  std::string columnChunk;
  auto serializedHeader = serializePageHeader(pageHeader);
  columnChunk.append(serializedHeader);
  columnChunk.append(compressed);

  // First page is sampled and fully decodes rep/def levels. The second page is
  // kept raw and exercises the prefix-only preload path through public APIs.
  columnChunk.append(serializedHeader);
  columnChunk.append(compressed);

  auto stream = std::make_unique<SeekableArrayInputStream>(
      columnChunk.data(), columnChunk.size());
  auto nestedType = makeNestedInt64Type();
  PageReader reader(
      std::move(stream),
      *leafPool_,
      nestedType.leaf,
      thrift::CompressionCodec::ZSTD,
      columnChunk.size(),
      nullptr);
  reader.setDecodeRepDefPageCount(1);

  reader.decodeRepDefs(5);
  EXPECT_GT(reader.repDefRange().second, 8);
}

TEST_F(ParquetPageReaderTest, largeDataPageV1DirectInputSharesPhysicalReads) {
  constexpr int32_t kLoadQuantum = 8 << 10;
  constexpr int32_t kRows = 256;
  constexpr int32_t kValuesPerRow = 64;

  struct ScanStats {
    uint64_t readCalls;
    uint64_t readBytes;
    int64_t peakBytes;
    int64_t currentBytes;
    int32_t outputSize;
  };
  auto scan = [&](const std::string& columnChunk,
                  thrift::CompressionCodec::type codec,
                  int32_t streamingWindowSize) {
    auto readPool = rootPool_->addLeafChild(
        fmt::format("v1-direct-input-{}", streamingWindowSize));
    auto file = std::make_shared<CountingInMemoryReadFile>(columnChunk);
    auto ioStats = std::make_shared<io::IoStatistics>();
    dwio::common::ReaderOptions options{readPool.get()};
    options.setLoadQuantum(kLoadQuantum);
    auto input = std::make_unique<DirectBufferedInput>(
        file,
        MetricsLog::voidLog(),
        1,
        nullptr,
        1,
        ioStats,
        nullptr,
        options,
        nullptr);

    StreamIdentifier streamId(0);
    std::unique_ptr<SeekableInputStream> dataStream;
    std::unique_ptr<SeekableInputStream> repDefStream;
    const Region region{0, columnChunk.size()};
    if (streamingWindowSize == 0) {
      dataStream = input->enqueue(region, &streamId);
    } else {
      auto streams = input->enqueuePair(region, &streamId);
      dataStream = std::move(streams.first);
      repDefStream = std::move(streams.second);
    }
    input->load(LogType::FILE);

    auto nestedType = makeNestedInt64Type();
    PageReader reader(
        std::move(dataStream),
        *readPool,
        nestedType.leaf,
        codec,
        columnChunk.size(),
        nullptr,
        std::move(repDefStream));
    reader.setParquetRepDefStreamingWindowSize(streamingWindowSize);

    reader.decodeRepDefs(kRows);
    arrow::LevelInfo listInfo;
    EXPECT_EQ(nestedType.list->makeLevelInfo(listInfo), LevelMode::kList);
    std::vector<int32_t> lengths(kRows);
    std::vector<uint64_t> nulls(bits::nwords(kRows));
    const auto [repDefBegin, repDefEnd] = reader.repDefRange();
    const auto outputSize = reader.getLengthsAndNulls(
        LevelMode::kList,
        listInfo,
        repDefBegin,
        repDefEnd,
        kRows,
        lengths.data(),
        nulls.data(),
        0);
    EXPECT_TRUE(std::all_of(lengths.begin(), lengths.end(), [](auto length) {
      return length == kValuesPerRow;
    }));
    reader.seekToPage(0);
    reader.skip(1);
    return ScanStats{
        file->readCalls(),
        file->readBytes(),
        readPool->peakBytes(),
        readPool->currentBytes(),
        outputSize};
  };

  for (const auto codec :
       {thrift::CompressionCodec::ZSTD, thrift::CompressionCodec::SNAPPY}) {
    SCOPED_TRACE(fmt::format("codec={}", static_cast<int32_t>(codec)));
    const auto page = makeDataPageV1(codec, kRows, kValuesPerRow, 20260831);
    const auto& columnChunk = page.encoded;
    ASSERT_GE(columnChunk.size(), 4 * kLoadQuantum);

    const auto legacy = scan(columnChunk, codec, 0);
    const auto streaming = scan(columnChunk, codec, 2'048);

    EXPECT_EQ(legacy.outputSize, kRows);
    EXPECT_LT(streaming.readBytes, legacy.readBytes);
    EXPECT_LT(streaming.readCalls, legacy.readCalls);
    EXPECT_EQ(streaming.outputSize, kRows);
    EXPECT_LT(streaming.peakBytes, legacy.peakBytes);
    if (codec == thrift::CompressionCodec::SNAPPY) {
      const auto compressedAllocation = leafPool_->preferredSize(
          page.encodedBodySize + AlignedBuffer::kPaddedSize);
      EXPECT_LT(
          streaming.currentBytes + compressedAllocation, legacy.currentBytes);
    }
  }
}

TEST_F(
    ParquetPageReaderTest,
    streamingDataPageV1MultiPageHandoffReusesPhysicalReads) {
  constexpr int32_t kLoadQuantum = 8 << 10;
  constexpr int32_t kRowsPerPage = 128;
  constexpr int32_t kFirstPageValuesPerRow = 64;
  constexpr int32_t kSecondPageValuesPerRow = 96;

  for (const auto codec :
       {thrift::CompressionCodec::UNCOMPRESSED,
        thrift::CompressionCodec::ZSTD,
        thrift::CompressionCodec::SNAPPY}) {
    SCOPED_TRACE(fmt::format("codec={}", static_cast<int32_t>(codec)));
    const auto firstPage =
        makeDataPageV1(codec, kRowsPerPage, kFirstPageValuesPerRow, 2026083101);
    const auto secondPage = makeDataPageV1(
        codec, kRowsPerPage, kSecondPageValuesPerRow, 2026083102);
    EXPECT_GT(firstPage.encodedBodySize, 4 * kLoadQuantum);
    EXPECT_GT(secondPage.encodedBodySize, 4 * kLoadQuantum);
    const auto columnChunk = firstPage.encoded + secondPage.encoded;

    auto readPool = rootPool_->addLeafChild("v1-multi-page-handoff");
    auto file = std::make_shared<CountingInMemoryReadFile>(columnChunk);
    auto ioStats = std::make_shared<io::IoStatistics>();
    dwio::common::ReaderOptions options{readPool.get()};
    options.setLoadQuantum(kLoadQuantum);
    auto input = std::make_unique<DirectBufferedInput>(
        file,
        MetricsLog::voidLog(),
        1,
        nullptr,
        1,
        ioStats,
        nullptr,
        options,
        nullptr);

    StreamIdentifier streamId(0);
    auto streams = input->enqueuePair(Region{0, columnChunk.size()}, &streamId);
    input->load(LogType::FILE);

    auto nestedType = makeNestedInt64Type();
    PageReader reader(
        std::move(streams.first),
        *readPool,
        nestedType.leaf,
        codec,
        columnChunk.size(),
        nullptr,
        std::move(streams.second));
    reader.setParquetRepDefMemoryLimit(1 << 20);
    reader.setParquetRepDefStreamingWindowSize(2'048);

    const auto currentBytesBeforeRepDefs = readPool->currentBytes();
    reader.decodeRepDefs(2 * kRowsPerPage);
    EXPECT_LE(
        readPool->currentBytes() - currentBytesBeforeRepDefs,
        (1 << 20) + 2 * kLoadQuantum);
    arrow::LevelInfo listInfo;
    ASSERT_EQ(nestedType.list->makeLevelInfo(listInfo), LevelMode::kList);
    std::vector<int32_t> lengths(2 * kRowsPerPage);
    std::vector<uint64_t> nulls(bits::nwords(2 * kRowsPerPage));
    const auto [repDefBegin, repDefEnd] = reader.repDefRange();
    ASSERT_EQ(
        reader.getLengthsAndNulls(
            LevelMode::kList,
            listInfo,
            repDefBegin,
            repDefEnd,
            lengths.size(),
            lengths.data(),
            nulls.data(),
            0),
        lengths.size());
    EXPECT_TRUE(std::all_of(
        lengths.begin(), lengths.begin() + kRowsPerPage, [](auto length) {
          return length == kFirstPageValuesPerRow;
        }));
    EXPECT_TRUE(std::all_of(
        lengths.begin() + kRowsPerPage, lengths.end(), [](auto length) {
          return length == kSecondPageValuesPerRow;
        }));
    const auto readCallsAfterRepDefs = file->readCalls();
    const auto readBytesAfterRepDefs = file->readBytes();
    reader.seekToPage(0);
    reader.skip(kRowsPerPage * kFirstPageValuesPerRow);
    reader.skip(1);

    EXPECT_LE(file->readCalls() - readCallsAfterRepDefs, 2);
    EXPECT_LE(file->readBytes() - readBytesAfterRepDefs, 2 * kLoadQuantum);
  }
}

TEST_F(
    ParquetPageReaderTest,
    streamingDataPageV1HandoffBudgetUsesRetainedAllocationBytes) {
  constexpr int32_t kLoadQuantum = 8 << 10;
  constexpr int32_t kRowsPerPage = 128;
  constexpr int32_t kFirstPageValuesPerRow = 64;
  constexpr int32_t kSecondPageValuesPerRow = 96;
  const auto firstPage = makeDataPageV1(
      thrift::CompressionCodec::UNCOMPRESSED,
      kRowsPerPage,
      kFirstPageValuesPerRow,
      2026083103);
  const auto secondPage = makeDataPageV1(
      thrift::CompressionCodec::UNCOMPRESSED,
      kRowsPerPage,
      kSecondPageValuesPerRow,
      2026083104);
  const auto columnChunk = firstPage.encoded + secondPage.encoded;
  ASSERT_GT(firstPage.body.size(), 4 * kLoadQuantum);
  ASSERT_GT(secondPage.body.size(), 4 * kLoadQuantum);

  const auto logicalBudget = firstPage.body.size() + secondPage.body.size();
  const auto firstAllocation = leafPool_->preferredSize(
      firstPage.body.size() + AlignedBuffer::kPaddedSize);
  const auto secondAllocation = leafPool_->preferredSize(
      secondPage.body.size() + AlignedBuffer::kPaddedSize);
  ASSERT_LE(firstAllocation, logicalBudget);
  ASSERT_LT(logicalBudget, firstAllocation + secondAllocation);
  ASSERT_LE(
      firstAllocation + secondAllocation,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));

  auto scan = [&](int32_t memoryLimit, const std::string& poolName) {
    auto input = makeStreamingV1Reader(
        rootPool_, columnChunk, kLoadQuantum, memoryLimit, poolName);
    input.reader->decodeRepDefs(2 * kRowsPerPage);
    const auto lengths = readListLengths(input, 2 * kRowsPerPage);
    EXPECT_TRUE(std::all_of(
        lengths.begin(), lengths.begin() + kRowsPerPage, [](auto length) {
          return length == kFirstPageValuesPerRow;
        }));
    EXPECT_TRUE(std::all_of(
        lengths.begin() + kRowsPerPage, lengths.end(), [](auto length) {
          return length == kSecondPageValuesPerRow;
        }));

    const auto readsBeforeValues = input.file->readBytes();
    input.reader->seekToPage(0);
    input.reader->skip(kRowsPerPage * kFirstPageValuesPerRow);
    const auto readsBeforeSecondPage = input.file->readBytes();
    input.reader->skip(1);
    return std::make_pair(
        readsBeforeSecondPage - readsBeforeValues,
        input.file->readBytes() - readsBeforeSecondPage);
  };

  const auto boundaryBudgetReads =
      scan(logicalBudget, "v1-logical-byte-budget");
  const auto fullBudgetReads =
      scan(firstAllocation + secondAllocation, "v1-allocation-byte-budget");

  EXPECT_LE(boundaryBudgetReads.first, 2 * kLoadQuantum);
  EXPECT_LE(fullBudgetReads.first, 2 * kLoadQuantum);
  EXPECT_GT(boundaryBudgetReads.second, 4 * kLoadQuantum);
  EXPECT_LE(fullBudgetReads.second, 2 * kLoadQuantum);
}

TEST_F(
    ParquetPageReaderTest,
    streamingDataPageV1SeekEvictsStaleHandoffBeforeCachingNextPage) {
  constexpr int32_t kLoadQuantum = 8 << 10;
  constexpr int32_t kRowsPerPage = 128;
  constexpr int32_t kValuesPerRow = 96;
  const auto firstPage = makeDataPageV1(
      thrift::CompressionCodec::UNCOMPRESSED,
      kRowsPerPage,
      kValuesPerRow,
      2026083105,
      true);
  const auto secondPage = makeDataPageV1(
      thrift::CompressionCodec::UNCOMPRESSED,
      kRowsPerPage,
      kValuesPerRow,
      2026083106);
  const auto columnChunk = firstPage.encoded + secondPage.encoded;
  ASSERT_GT(firstPage.body.size(), 8 * kLoadQuantum);
  const auto firstPageAllocation = leafPool_->preferredSize(
      firstPage.body.size() + AlignedBuffer::kPaddedSize);
  const auto secondPageAllocation = leafPool_->preferredSize(
      secondPage.body.size() + AlignedBuffer::kPaddedSize);
  const auto onePageBudget =
      std::max(firstPageAllocation, secondPageAllocation);
  ASSERT_LT(onePageBudget, firstPageAllocation + secondPageAllocation);
  ASSERT_LE(
      onePageBudget,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));

  auto scan = [&](int32_t memoryLimit, const std::string& poolName) {
    auto input = makeStreamingV1Reader(
        rootPool_, columnChunk, kLoadQuantum, memoryLimit, poolName);
    input.reader->decodeRepDefs(kRowsPerPage - 1);
    const auto lengths = readListLengths(input, kRowsPerPage - 1);
    EXPECT_TRUE(std::all_of(lengths.begin(), lengths.end(), [](auto length) {
      return length == kValuesPerRow;
    }));

    const auto readsBeforeSeek = input.file->readBytes();
    input.reader->seekToPage((kRowsPerPage - 1) * kValuesPerRow);
    input.reader->decodeRepDefs(2);
    EXPECT_EQ(
        readListLengths(input, 2), (std::vector<int32_t>{0, kValuesPerRow}));
    input.reader->skip(1);
    return input.file->readBytes() - readsBeforeSeek;
  };

  const auto noHandoffReads = scan(0, "v1-stale-page-no-handoff");
  const auto onePageHandoffReads =
      scan(onePageBudget, "v1-stale-page-one-page-handoff");

  EXPECT_GT(noHandoffReads, onePageHandoffReads);
  EXPECT_GT(noHandoffReads - onePageHandoffReads, 4 * kLoadQuantum);
}

} // namespace bytedance::bolt::parquet
