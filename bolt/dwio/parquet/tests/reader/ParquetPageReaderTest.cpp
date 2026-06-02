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

#include <cstring>
#include "bolt/dwio/parquet/reader/PageReader.h"
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;

class ParquetPageReaderTest : public ParquetTestBase {};

namespace bytedance::bolt::parquet {

class PageReaderTestPeer {
 public:
  static raw_vector<char> makeRepDefBatch(
      int32_t numRepDefs,
      int16_t repetitionLevel,
      int16_t definitionLevel,
      int32_t maxRepeat,
      int32_t maxDefine) {
    raw_vector<char> batch;
    append<int32_t>(batch, numRepDefs);
    appendLevels(batch, numRepDefs, repetitionLevel, maxRepeat);
    appendLevels(batch, numRepDefs, definitionLevel, maxDefine);
    return batch;
  }

  static void initializeForPendingRepDefs(PageReader& pageReader) {
    pageReader.hasChunkRepDefs_ = true;
    pageReader.pageIndex_ = 0;
    pageReader.numLeavesInPage_ = {1};
    pageReader.preloadedRepDefs_.push_back(makeRepDefBatch(
        1,
        0,
        pageReader.maxDefine_,
        pageReader.maxRepeat_,
        pageReader.maxDefine_));
  }

  static void setPageRowInfo(PageReader& pageReader) {
    pageReader.setPageRowInfo(false);
  }

  static int32_t pageIndex(const PageReader& pageReader) {
    return pageReader.pageIndex_;
  }

  static int32_t numRowsInPage(const PageReader& pageReader) {
    return pageReader.numRowsInPage_;
  }

  static size_t numKnownRepDefPages(const PageReader& pageReader) {
    return pageReader.numLeavesInPage_.size();
  }

  static size_t numPendingRepDefBatches(const PageReader& pageReader) {
    return pageReader.preloadedRepDefs_.size();
  }

 private:
  template <typename T>
  static void append(raw_vector<char>& out, T value) {
    const auto offset = out.size();
    out.resize(offset + sizeof(T));
    std::memcpy(out.data() + offset, &value, sizeof(T));
  }

  static void appendLevels(
      raw_vector<char>& out,
      int32_t numLevels,
      int16_t level,
      int32_t maxLevel) {
    const auto bitWidth = ::arrow::bit_util::NumRequiredBits(maxLevel);
    std::vector<uint8_t> encoded(
        ::arrow::util::RleEncoder::MaxBufferSize(bitWidth, numLevels) +
        ::arrow::util::RleEncoder::MinBufferSize(bitWidth));
    ::arrow::util::RleEncoder encoder(encoded.data(), encoded.size(), bitWidth);
    for (auto i = 0; i < numLevels; ++i) {
      encoder.Put(level);
    }
    const auto encodedSize = encoder.Flush();
    append<uint32_t>(out, encodedSize);
    const auto offset = out.size();
    out.resize(offset + encodedSize);
    std::memcpy(out.data() + offset, encoded.data(), encodedSize);
  }
};

} // namespace bytedance::bolt::parquet

TEST_F(ParquetPageReaderTest, loadsPendingRepDefsBeforePageRowInfoCheck) {
  auto fileType = std::make_shared<ParquetTypeWithId>(
      VARCHAR(),
      std::vector<std::shared_ptr<const dwio::common::TypeWithId>>{},
      0,
      0,
      0,
      "items.list.element",
      thrift::Type::BYTE_ARRAY,
      std::nullopt,
      thrift::ConvertedType::UTF8,
      1,
      2,
      true,
      false);
  PageReader pageReader(
      nullptr,
      *leafPool_,
      fileType,
      thrift::CompressionCodec::UNCOMPRESSED,
      0,
      nullptr);
  PageReaderTestPeer::initializeForPendingRepDefs(pageReader);

  ASSERT_NO_THROW(PageReaderTestPeer::setPageRowInfo(pageReader));
  EXPECT_EQ(PageReaderTestPeer::pageIndex(pageReader), 1);
  EXPECT_EQ(PageReaderTestPeer::numRowsInPage(pageReader), 1);
  EXPECT_EQ(PageReaderTestPeer::numKnownRepDefPages(pageReader), 2);
  EXPECT_EQ(PageReaderTestPeer::numPendingRepDefBatches(pageReader), 0);
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

TEST(CompressionOptionsTest, testCompressionOptions) {
  auto options = getParquetDecompressionOptions(
      bytedance::bolt::common::CompressionKind_ZLIB);
  EXPECT_EQ(
      options.format.zlib.windowBits,
      dwio::common::compression::Compressor::PARQUET_ZLIB_WINDOW_BITS);
}
