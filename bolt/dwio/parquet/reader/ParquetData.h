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

#pragma once

#include <thrift/protocol/TCompactProtocol.h> //@manual
#include "bolt/dwio/common/BufferUtil.h"
#include "bolt/dwio/parquet/reader/PageReader.h"
#include "bolt/dwio/parquet/reader/SchemaHelper.h"
#include "bolt/dwio/parquet/reader/Statistics.h"
#include "bolt/dwio/parquet/thrift/ThriftTransport.h"
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"

#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/dwio/parquet/encryption/InternalFileDecryptor.h"

namespace bytedance::bolt::common {
class ScanSpec;
} // namespace bytedance::bolt::common
namespace bytedance::bolt::dwio::common {
class BufferedInput;
} // namespace bytedance::bolt::dwio::common
namespace bytedance::bolt::parquet {

class ParquetParams : public dwio::common::FormatParams {
 public:
  ParquetParams(
      memory::MemoryPool& pool,
      dwio::common::ColumnReaderStatistics& stats,
      const thrift::FileMetaData& metaData,
      TimestampPrecision timestampPrecision,
      const std::shared_ptr<InternalFileDecryptor>& fileDecryptor,
      const SchemaHelper& schemaHelper,
      bool enableDictionaryFilter,
      int32_t decodeRepDefPageCount,
      int32_t parquetRepDefMemoryLimit)
      : FormatParams(pool, stats),
        metaData_(metaData),
        timestampPrecision_(timestampPrecision),
        fileDecryptor_(fileDecryptor),
        schemaHelper_(schemaHelper),
        enableDictionaryFilter_(enableDictionaryFilter),
        decodeRepDefPageCount_(decodeRepDefPageCount),
        parquetRepDefMemoryLimit_(parquetRepDefMemoryLimit) {}

  std::unique_ptr<dwio::common::FormatData> toFormatData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const common::ScanSpec& scanSpec) override;

  TimestampPrecision timestampPrecision() const {
    return timestampPrecision_;
  }

  bool enableDictionaryFilter() const {
    return enableDictionaryFilter_;
  }

 private:
  const thrift::FileMetaData& metaData_;
  const TimestampPrecision timestampPrecision_;
  const std::shared_ptr<InternalFileDecryptor>& fileDecryptor_;
  const SchemaHelper& schemaHelper_;
  const bool enableDictionaryFilter_;
  const int32_t decodeRepDefPageCount_;
  const int32_t parquetRepDefMemoryLimit_;
};

/// Format-specific data created for each leaf column of a Parquet rowgroup.
class ParquetData : public dwio::common::FormatData {
 public:
  ParquetData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const std::vector<thrift::RowGroup>& rowGroups,
      memory::MemoryPool& pool,
      const std::shared_ptr<InternalFileDecryptor>& fileDecryptor,
      dwio::common::RuntimeStatistics* statis,
      const SchemaHelper& schemaHelper,
      bool enableDictionaryFilter,
      int32_t decodeRepDefPageCount,
      int32_t parquetRepDefMemoryLimit)
      : pool_(pool),
        type_(std::static_pointer_cast<const ParquetTypeWithId>(type)),
        rowGroups_(rowGroups),
        maxDefine_(type_->maxDefine_),
        maxRepeat_(type_->maxRepeat_),
        rowsInRowGroup_(-1),
        fileDecryptor_(fileDecryptor),
        statis_(statis),
        schemaHelper_(schemaHelper),
        enableDictionaryFilter_(enableDictionaryFilter),
        decodeRepDefPageCount_(decodeRepDefPageCount),
        parquetRepDefMemoryLimit_(parquetRepDefMemoryLimit) {
    rowGroupOffsets_ = std::vector<int64_t>(rowGroups_.size());
    rowGroupOffsets_[0] = 0;
    for (int32_t i = 1; i < rowGroups_.size(); ++i) {
      rowGroupOffsets_[i] =
          rowGroupOffsets_[i - 1] + rowGroups_[i - 1].num_rows;
    }
  }

  /// Prepares to read data for 'index'th row group.
  void enqueueRowGroup(uint32_t index, dwio::common::BufferedInput& input);

  /// Positions 'this' at 'index'th row group. loadRowGroup must be called
  /// first. The returned PositionProvider is empty and should not be used.
  /// Other formats may use it.
  dwio::common::PositionProvider seekToRowGroup(int64_t index) override;

  void filterRowGroups(
      const common::ScanSpec& scanSpec,
      uint64_t rowsPerRowGroup,
      const dwio::common::StatsContext& writerContext,
      FilterRowGroupsResult&,
      dwio::common::BufferedInput& input) override;

  PageReader* FOLLY_NONNULL reader() const {
    return reader_.get();
  }

  // Reads null flags for 'numValues' next top level rows. The first 'numValues'
  // bits of 'nulls' are set and the reader is advanced by numValues'.
  void readNullsOnly(int32_t numValues, BufferPtr& nulls) {
    reader_->readNullsOnly(numValues, nulls);
  }

  bool hasNulls() const override {
    return maxDefine_ > 0;
  }

  /// Sets nulls to be returned by readNulls(). Nulls for non-leaf readers come
  /// from leaf repdefs which are gathered before descending the reader tree.
  void setNulls(BufferPtr& nulls, int32_t numValues) {
    if (nulls || numValues) {
      BOLT_CHECK_EQ(presetNullsConsumed_, presetNullsSize_);
    }
    presetNulls_ = nulls;
    presetNullsSize_ = numValues;
    presetNullsConsumed_ = 0;
  }

  int32_t presetNullsLeft() const {
    return presetNullsSize_ - presetNullsConsumed_;
  }

  void readNulls(
      vector_size_t numValues,
      const uint64_t* FOLLY_NULLABLE incomingNulls,
      BufferPtr& nulls,
      bool nullsOnly = false) override {
    // If the query accesses only nulls, read the nulls from the pages in range.
    // If nulls are preread, return those minus any skipped.
    if (presetNulls_) {
      BOLT_CHECK_LE(numValues, presetNullsSize_ - presetNullsConsumed_);
      if (!presetNullsConsumed_ && numValues == presetNullsSize_) {
        nulls = std::move(presetNulls_);
        presetNullsConsumed_ = numValues;
      } else {
        dwio::common::ensureCapacity<bool>(nulls, numValues, &pool_);
        auto bits = nulls->asMutable<uint64_t>();
        bits::copyBits(
            presetNulls_->as<uint64_t>(),
            presetNullsConsumed_,
            bits,
            0,
            numValues);
        presetNullsConsumed_ += numValues;
      }
      return;
    }
    if (nullsOnly) {
      readNullsOnly(numValues, nulls);
      return;
    }
    // There are no column-level nulls in Parquet, only page-level ones, so this
    // is always non-null.
    nulls = nullptr;
  }

  uint64_t skipNulls(uint64_t numValues, bool nullsOnly) override {
    // If we are seeking a column where nulls and data are read, the skip is
    // done in skip(). If we are reading nulls only, this is called with
    // 'nullsOnly' set and is responsible for reading however many nulls or
    // pages it takes to skip 'numValues' top level rows.
    if (nullsOnly) {
      reader_->skipNullsOnly(numValues);
    }
    if (presetNulls_) {
      BOLT_DCHECK_LE(numValues, presetNullsSize_ - presetNullsConsumed_);
      presetNullsConsumed_ += numValues;
    }
    return numValues;
  }

  uint64_t skip(uint64_t numRows) override {
    reader_->skip(numRows);
    return numRows;
  }

  /// Applies 'visitor' to the data in the column of 'this'. See
  /// PageReader::readWithVisitor().
  template <typename Visitor>
  void readWithVisitor(Visitor visitor) {
    reader_->readWithVisitor(visitor);
  }

  const VectorPtr& dictionaryValues(const TypePtr& type) {
    return reader_->dictionaryValues(type);
  }

  void clearDictionary() {
    reader_->clearDictionary();
  }

  bool hasDictionary() const {
    return reader_->isDictionary();
  }

  bool isDeltaBinaryPacked() const {
    return reader_->isDeltaBinaryPacked();
  }

  bool parentNullsInLeaves() const override {
    return true;
  }

  int64_t rowGroupOffset(size_t rowGroupIndex) const {
    return rowGroupOffsets_[rowGroupIndex];
  }

  // Returns the <offset, length> of the row group.
  std::pair<int64_t, int64_t> getRowGroupRegion(uint32_t index) const;

 private:
  // read block split bloom filter
  std::unique_ptr<BlockSplitBloomFilter> loadBlockBloomFilter(
      uint32_t rowGroupId,
      uint32_t columnId,
      dwio::common::BufferedInput& input);

  // read ngram bloom filter
  std::vector<std::pair<
      std::unique_ptr<struct NgramTokenExtractor>,
      std::unique_ptr<NGramBloomFilter>>>
  loadNGramBloomFilter(
      uint32_t rowGroupId,
      uint32_t columnId,
      dwio::common::BufferedInput& input);

  /// True if 'filter' may have hits for the column of 'this' according to the
  /// stats in 'rowGroup'.
  bool rowGroupMatches(
      uint32_t rowGroupId,
      common::Filter* filter,
      dwio::common::BufferedInput& input);

  bool checkColumnFilter(
      const thrift::ColumnChunk& columnChunk,
      common::Filter* filter,
      const TypePtr& type,
      size_t rowGroupId);

 protected:
  memory::MemoryPool& pool_;
  std::shared_ptr<const ParquetTypeWithId> type_;
  const std::vector<thrift::RowGroup>& rowGroups_;
  // Streams for this column in each of 'rowGroups_'. Will be created on or
  // ahead of first use, not at construction.
  std::vector<std::unique_ptr<dwio::common::SeekableInputStream>> streams_;
  std::vector<int64_t> rowGroupOffsets_;

  const uint32_t maxDefine_;
  const uint32_t maxRepeat_;
  int64_t rowsInRowGroup_;
  std::unique_ptr<PageReader> reader_;

  // Nulls derived from leaf repdefs for non-leaf readers.
  BufferPtr presetNulls_;

  // Number of valid bits in 'presetNulls_'.
  int32_t presetNullsSize_{0};

  // Count of leading skipped positions in 'presetNulls_'
  int32_t presetNullsConsumed_{0};

  const std::shared_ptr<InternalFileDecryptor> fileDecryptor_;

  dwio::common::RuntimeStatistics* statis_{nullptr};
  const SchemaHelper& schemaHelper_;
  const bool enableDictionaryFilter_;
  const int32_t decodeRepDefPageCount_;
  const int32_t parquetRepDefMemoryLimit_;
};

} // namespace bytedance::bolt::parquet
