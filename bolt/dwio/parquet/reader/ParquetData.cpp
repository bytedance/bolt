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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include "bolt/dwio/parquet/reader/ParquetData.h"
#include "bolt/common/base/BloomFilter.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/dwio/parquet/reader/DictionaryFilter.h"
#include "bolt/dwio/parquet/reader/Statistics.h"
#include "bolt/type/filter/MapSubscriptFilter.h"
namespace bytedance::bolt::parquet {

std::unique_ptr<dwio::common::FormatData> ParquetParams::toFormatData(
    const std::shared_ptr<const dwio::common::TypeWithId>& type,
    const common::ScanSpec& scanSpec) {
  return std::make_unique<ParquetData>(
      type,
      metaData_.row_groups,
      pool(),
      scanSpec.getRuntimeStatistics(),
      schemaHelper_,
      enableDictionaryFilter_,
      decodeRepDefPageCount_,
      parquetRepDefMemoryLimit_);
}

void ParquetData::filterRowGroups(
    const common::ScanSpec& scanSpec,
    uint64_t /*rowsPerRowGroup*/,
    const dwio::common::StatsContext& /*writerContext*/,
    FilterRowGroupsResult& result,
    dwio::common::BufferedInput& input) {
  result.totalCount = std::max<int>(result.totalCount, rowGroups_.size());
  auto nwords = bits::nwords(result.totalCount);
  if (result.filterResult.size() < nwords) {
    result.filterResult.resize(nwords);
  }
  auto metadataFiltersStartIndex = result.metadataFilterResults.size();
  for (int i = 0; i < scanSpec.numMetadataFilters(); ++i) {
    result.metadataFilterResults.emplace_back(
        scanSpec.metadataFilterNodeAt(i), std::vector<uint64_t>(nwords));
  }
  for (auto i = 0; i < rowGroups_.size(); ++i) {
    if (scanSpec.filter() && !rowGroupMatches(i, scanSpec.filter(), input)) {
      bits::setBit(result.filterResult.data(), i);
      continue;
    }
    for (int j = 0; j < scanSpec.numMetadataFilters(); ++j) {
      auto* metadataFilter = scanSpec.metadataFilterAt(j);
      if (!rowGroupMatches(i, metadataFilter, input)) {
        bits::setBit(
            result.metadataFilterResults[metadataFiltersStartIndex + j]
                .second.data(),
            i);
      }
    }
  }
}

std::unique_ptr<BlockSplitBloomFilter> ParquetData::loadBlockBloomFilter(
    uint32_t rowGroupId,
    uint32_t columnId,
    dwio::common::BufferedInput& input) {
  constexpr uint64_t guessHeaderLen = 128;
  uint64_t readSize = 0;
  std::vector<char> copy(readSize);
  const char* bufferStart = nullptr;
  const char* bufferEnd = nullptr;
  auto fileLength = input.getReadFile()->size();

  auto& columnMetaData = rowGroups_[rowGroupId].columns[columnId].meta_data;
  if (columnMetaData.__isset.bloom_filter_offset &&
      columnMetaData.bloom_filter_offset > 0) {
    auto offset = columnMetaData.bloom_filter_offset;
    readSize = std::min(guessHeaderLen, fileLength - offset);
    auto filter_stream =
        input.read(offset, readSize, dwio::common::LogType::HEADER);
    copy.resize(readSize);
    bufferStart = nullptr;
    bufferEnd = nullptr;
    dwio::common::readBytes(
        readSize, filter_stream.get(), copy.data(), bufferStart, bufferEnd);
    auto thriftTransport = std::make_shared<thrift::ThriftBufferedTransport>(
        copy.data(), readSize);
    auto thriftProtocol =
        std::make_unique<apache::thrift::protocol::TCompactProtocolT<
            thrift::ThriftBufferedTransport>>(thriftTransport);
    thrift::BloomFilterHeader filterHeader;
    offset += filterHeader.read(thriftProtocol.get());
    if (filterHeader.numBytes > 0) {
      auto numBytes = filterHeader.numBytes;
      BOLT_CHECK_LE(numBytes, fileLength - offset);
      auto bloomfilter = std::make_unique<BlockSplitBloomFilter>(numBytes);
      auto bitmap_stream =
          input.read(offset, numBytes, dwio::common::LogType::HEADER);
      bufferStart = nullptr;
      bufferEnd = nullptr;
      dwio::common::readBytes(
          numBytes,
          bitmap_stream.get(),
          bloomfilter->getFilter().data(),
          bufferStart,
          bufferEnd);
      return bloomfilter;
    }
  }

  return nullptr;
}

std::vector<std::pair<
    std::unique_ptr<struct NgramTokenExtractor>,
    std::unique_ptr<NGramBloomFilter>>>
ParquetData::loadNGramBloomFilter(
    uint32_t rowGroupId,
    uint32_t columnId,
    dwio::common::BufferedInput& input) {
  constexpr uint64_t guessHeaderLen = 128;
  std::vector<char> copy(0);
  const char* bufferStart = nullptr;
  const char* bufferEnd = nullptr;
  std::vector<std::pair<
      std::unique_ptr<struct NgramTokenExtractor>,
      std::unique_ptr<NGramBloomFilter>>>
      token_bloom_filters;
  auto fileLength = input.getReadFile()->size();

  auto& columnMetaData = rowGroups_[rowGroupId].columns[columnId].meta_data;
  if (columnMetaData.__isset.token_bloom_filter_offset &&
      columnMetaData.token_bloom_filter_offset.size() > 0 &&
      columnMetaData.__isset.token_bloom_filter_length &&
      columnMetaData.token_bloom_filter_length.size() > 0) {
    for (int i = 0; i < columnMetaData.token_bloom_filter_offset.size(); ++i) {
      auto offset = columnMetaData.token_bloom_filter_offset[i];
      auto readSize = columnMetaData.token_bloom_filter_length[i];
      BOLT_CHECK_LE(readSize, fileLength - offset);
      auto filter_stream =
          input.read(offset, readSize, dwio::common::LogType::HEADER);
      copy.resize(readSize);
      bufferStart = nullptr;
      bufferEnd = nullptr;
      dwio::common::readBytes(
          readSize, filter_stream.get(), copy.data(), bufferStart, bufferEnd);
      auto thriftTransport = std::make_shared<thrift::ThriftBufferedTransport>(
          copy.data(), readSize);
      auto thriftProtocol =
          std::make_unique<apache::thrift::protocol::TCompactProtocolT<
              thrift::ThriftBufferedTransport>>(thriftTransport);
      thrift::TokenBloomFilterHeader filterHeader;
      auto headerSize = filterHeader.read(thriftProtocol.get());
      if (filterHeader.numBits > 0) {
        auto numBytes = (filterHeader.numBits + 7) / 8;
        BOLT_CHECK_LE(numBytes, fileLength - offset);
        auto extractor = std::make_unique<struct NgramTokenExtractor>(
            filterHeader.algorithm.NGRAM.nGram);
        BOLT_CHECK_EQ(headerSize + numBytes, readSize);
        auto bloomfilter = std::make_unique<NGramBloomFilter>(
            numBytes,
            filterHeader.algorithm.NGRAM.hashes,
            filterHeader.algorithm.NGRAM.seed,
            copy.data() + headerSize);
        token_bloom_filters.push_back(
            std::make_pair(std::move(extractor), std::move(bloomfilter)));
      }
    }
  }

  return token_bloom_filters;
}

bool ParquetData::rowGroupMatches(
    uint32_t rowGroupId,
    common::Filter* FOLLY_NULLABLE filter,
    dwio::common::BufferedInput& input) {
  if (!filter) {
    return true;
  }

  auto column = type_->column();
  auto type = type_->type();

  // Handle MapSubscriptFilter filters specially for Map types
  if (type->isMap()) {
    // Get key and value columns and types
    auto keyColumn = type_->childAt(0)->column();
    auto keyType = type_->childAt(0)->type();
    auto valueColumn = type_->childAt(1)->column();
    auto valueType = type_->childAt(1)->type();

    // Get the key column chunk
    const auto& keyColumnChunk = rowGroups_[rowGroupId].columns[keyColumn];
    // Get the value column chunk
    const auto& valueColumnChunk = rowGroups_[rowGroupId].columns[valueColumn];
    if (filter->kind() == common::FilterKind::kMapSubscript) {
      // For Map types, we need to check both key and value columns separately
      // Map has two children: key and value

      // Check if it's a MapSubscriptFilter to get the key value
      auto mapSubscriptFilter =
          dynamic_cast<const common::MapSubscriptFilter*>(filter);
      if (mapSubscriptFilter) {
        BOLT_CHECK_EQ(type_->size(), 2);
        // Get the key and value filter from the MapSubscriptFilter filter
        // Remove const qualifier as testFilter expects non-const Filter*
        auto valueFilter =
            const_cast<common::Filter*>(mapSubscriptFilter->valueFilter());
        auto keyFilter =
            const_cast<common::Filter*>(mapSubscriptFilter->keyFilter());

        // Check if key column has statistics
        if (!checkColumnFilter(
                keyColumnChunk, keyFilter, keyType, rowGroupId)) {
          return false;
        }

        if (!checkColumnFilter(
                valueColumnChunk, valueFilter, valueType, rowGroupId)) {
          return false;
        }
      }
    } else if (
        filter->kind() == common::FilterKind::kIsNull ||
        filter->kind() == common::FilterKind::kIsNotNull) {
      if (!checkColumnFilter(keyColumnChunk, filter, keyType, rowGroupId)) {
        return false;
      }
    }
    return true;
  }

  // Map columns are handled differently. Instead of a single column index,
  // the map's keys and values are stored as separate columns, each with their
  // own index. This check distinguishes map columns from other data types.
  const auto& rowGroup = rowGroups_[rowGroupId];
  const auto& columnChunk = rowGroup.columns[column];

  // Filter out rowgroup if there is statistics.
  if (columnChunk.__isset.meta_data &&
      columnChunk.meta_data.__isset.statistics) {
    // Prepare bloom filters
    std::unique_ptr<BlockSplitBloomFilter> blockBloomFilter;
    std::vector<std::pair<
        std::unique_ptr<struct NgramTokenExtractor>,
        std::unique_ptr<NGramBloomFilter>>>
        tokenBloomFilters;

    auto realFilterKind = filter->kind();
    switch (realFilterKind) {
      case common::FilterKind::kBytesValues:
      case common::FilterKind::kBigintValuesUsingHashTable:
      case common::FilterKind::kBigintRange:
        blockBloomFilter = loadBlockBloomFilter(rowGroupId, column, input);
        break;
      case common::FilterKind::kBytesLike:
        tokenBloomFilters = loadNGramBloomFilter(rowGroupId, column, input);
        break;
      default:
        break;
    }

    // Build column statistics
    auto columnStats = buildColumnStatisticsFromThrift(
        columnChunk.meta_data.statistics,
        std::move(blockBloomFilter),
        std::move(tokenBloomFilters),
        *type,
        rowGroup.num_rows);

    // Handle regular filter
    if (!testFilter(filter, columnStats.get(), rowGroup.num_rows, type)) {
      // We only need to check the first row group.
      return false;
    }
  }

  // Try dictionary filtering first - pass the whole columnChunk
  if (enableDictionaryFilter_) {
    DictionaryFilter dictFilter(
        columnChunk,
        schemaHelper_.getElementByLeafIndex(column),
        type_,
        input,
        pool_,
        filter,
        rowGroupId,
        rowGroup.num_rows);

    auto matchResult = dictFilter.tryMatch();
    return matchResult != DictionaryFilter::MatchResult::Drop;
  }

  return true;
}

void ParquetData::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto& chunk = rowGroups_[index].columns[type_->column()];
  streams_.resize(rowGroups_.size());
  BOLT_CHECK(
      chunk.__isset.meta_data,
      "ColumnMetaData does not exist for schema Id ",
      type_->column());
  auto& metaData = chunk.meta_data;

  uint64_t chunkReadOffset = metaData.data_page_offset;
  if (metaData.__isset.dictionary_page_offset &&
      metaData.dictionary_page_offset >= 4) {
    // this assumes the data pages follow the dict pages directly.
    chunkReadOffset = metaData.dictionary_page_offset;
  }
  BOLT_CHECK_GE(chunkReadOffset, 0);

  uint64_t readSize = (metaData.codec == thrift::CompressionCodec::UNCOMPRESSED)
      ? metaData.total_uncompressed_size
      : metaData.total_compressed_size;

  auto id = dwio::common::StreamIdentifier(type_->column());
  streams_[index] = input.enqueue({chunkReadOffset, readSize}, &id);
}

dwio::common::PositionProvider ParquetData::seekToRowGroup(int64_t index) {
  static std::vector<uint64_t> empty;
  BOLT_CHECK_LT(index, streams_.size());
  BOLT_CHECK(streams_[index], "Stream not enqueued for column");
  auto& metadata = rowGroups_[index].columns[type_->column()].meta_data;
  reader_ = std::make_unique<PageReader>(
      std::move(streams_[index]),
      pool_,
      type_,
      metadata.codec,
      metadata.total_compressed_size,
      statis_);
  reader_->setDecodeRepDefPageCount(decodeRepDefPageCount_);
  reader_->setParquetRepDefMemoryLimit(parquetRepDefMemoryLimit_);

  return dwio::common::PositionProvider(empty);
}

std::pair<int64_t, int64_t> ParquetData::getRowGroupRegion(
    uint32_t index) const {
  auto& rowGroup = rowGroups_[index];

  BOLT_CHECK_GT(rowGroup.columns.size(), 0);
  auto fileOffset = rowGroup.__isset.file_offset ? rowGroup.file_offset
      : rowGroup.columns[0].meta_data.__isset.dictionary_page_offset
      ? rowGroup.columns[0].meta_data.dictionary_page_offset
      : rowGroup.columns[0].meta_data.data_page_offset;
  BOLT_CHECK_GT(fileOffset, 0);

  auto length = rowGroup.__isset.total_compressed_size
      ? rowGroup.total_compressed_size
      : rowGroup.total_byte_size;

  return {fileOffset, length};
}

bool ParquetData::checkColumnFilter(
    const thrift::ColumnChunk& columnChunk,
    common::Filter* filter,
    const TypePtr& type,
    size_t rowGroupId) {
  if (columnChunk.__isset.meta_data &&
      columnChunk.meta_data.__isset.statistics) {
    // Create empty vectors for token bloom filters
    std::vector<std::pair<
        std::unique_ptr<struct NgramTokenExtractor>,
        std::unique_ptr<NGramBloomFilter>>>
        emptyTokenFilters;

    auto columnStats = buildColumnStatisticsFromThrift(
        columnChunk.meta_data.statistics,
        nullptr, // No bloom filter for key
        std::move(emptyTokenFilters), // No token bloom filters for key
        *type,
        rowGroups_[rowGroupId].num_rows);

    return testFilter(
        filter, columnStats.get(), rowGroups_[rowGroupId].num_rows, type);
  } else {
    // If no statistics available, conservatively pass through
    return true;
  }
}

} // namespace bytedance::bolt::parquet
