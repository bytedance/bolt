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
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"

#include <arrow/util/rle_encoding.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <zstd.h>

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
  std::string encoded(levels.size() * sizeof(int16_t) + 64, '\0');
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

thrift::PageHeader makePageHeader(
    int32_t compressedSize,
    int32_t uncompressedSize) {
  thrift::DataPageHeader dataPageHeader;
  dataPageHeader.__set_num_values(8);
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

ParquetTypeWithIdPtr makeNestedInt64Type() {
  return std::make_shared<ParquetTypeWithId>(
      BIGINT(),
      std::vector<std::shared_ptr<const dwio::common::TypeWithId>>{},
      0,
      0,
      0,
      "value",
      thrift::Type::INT64,
      std::nullopt,
      std::nullopt,
      1,
      2,
      true,
      true);
}

} // namespace

class ParquetPageReaderTest : public ParquetTestBase {};

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
  PageReader reader(
      std::move(stream),
      *leafPool_,
      makeNestedInt64Type(),
      thrift::CompressionCodec::ZSTD,
      columnChunk.size(),
      nullptr);
  reader.setDecodeRepDefPageCount(1);

  reader.decodeRepDefs(5);
  EXPECT_GT(reader.repDefRange().second, 8);
}

} // namespace bytedance::bolt::parquet
