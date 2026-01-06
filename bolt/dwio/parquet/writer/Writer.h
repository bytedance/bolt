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

#pragma once

#include <arrow/record_batch.h>
#include "bolt/common/compression/Compression.h"
#include "bolt/common/config/Config.h"
#include "bolt/dwio/common/DataBuffer.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/FlushPolicy.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/Writer.h"
#include "bolt/dwio/common/WriterFactory.h"
#include "bolt/dwio/parquet/arrow/Properties.h"
#include "bolt/dwio/parquet/arrow/Types.h"
#include "bolt/dwio/parquet/arrow/util/Compression.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/arrow/Bridge.h"
namespace bytedance::bolt::parquet {

namespace arrow {
class WriterProperties;
class ArrowWriterProperties;
} // namespace arrow

using bytedance::bolt::parquet::arrow::util::CodecOptions;

class ArrowDataBufferSink;

struct ArrowContext;

class DefaultFlushPolicy : public dwio::common::FlushPolicy {
 public:
  DefaultFlushPolicy()
      : rowsInRowGroup_(1'024 * 1'024), bytesInRowGroup_(128 * 1'024 * 1'024) {}
  DefaultFlushPolicy(uint64_t rowsInRowGroup, int64_t bytesInRowGroup)
      : rowsInRowGroup_(rowsInRowGroup), bytesInRowGroup_(bytesInRowGroup) {}

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    return stripeProgress.stripeRowCount >= rowsInRowGroup_ ||
        stripeProgress.stripeSizeEstimate >= bytesInRowGroup_;
  }

  void onClose() override {
    // No-op
  }

  uint64_t rowsInRowGroup() const {
    return rowsInRowGroup_;
  }

  int64_t bytesInRowGroup() const {
    return bytesInRowGroup_;
  }

  virtual void setBytesPerRow(int64_t bytes) {}

  virtual void setMemLimit(int64_t memLimit) {}

 private:
  const uint64_t rowsInRowGroup_;
  const int64_t bytesInRowGroup_;
};

class LambdaFlushPolicy : public DefaultFlushPolicy {
 public:
  explicit LambdaFlushPolicy(
      uint64_t rowsInRowGroup,
      int64_t bytesInRowGroup,
      std::function<bool()> lambda)
      : DefaultFlushPolicy(rowsInRowGroup, bytesInRowGroup) {
    lambda_ = std::move(lambda);
  }
  virtual ~LambdaFlushPolicy() override = default;

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    return lambda_() || DefaultFlushPolicy::shouldFlush(stripeProgress);
  }

 private:
  std::function<bool()> lambda_;
};

class SizeFlushPolicy : public DefaultFlushPolicy {
 public:
  int64_t KDefaultSizeMultiplier = 2;

  explicit SizeFlushPolicy(int64_t bytesInRowGroup)
      : DefaultFlushPolicy(
            std::numeric_limits<int64_t>::max(),
            bytesInRowGroup) {}

  virtual ~SizeFlushPolicy() override = default;

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    if (memLimit_ > 0 && stripeProgress.stripeSizeEstimate >= memLimit_) {
      return true;
    } else if (bytesPerRowCompressed_ > 0) {
      return stripeProgress.stripeRowCount * bytesPerRowCompressed_ >=
          bytesInRowGroup();
    } else {
      return stripeProgress.stripeSizeEstimate >=
          bytesInRowGroup() * KDefaultSizeMultiplier;
    }
  }

  void setBytesPerRow(int64_t bytes) override {
    bytesPerRowCompressed_ = bytes;
  }

  void setMemLimit(int64_t memLimit) override {
    memLimit_ = std::max(memLimit, bytesInRowGroup());
  }

 private:
  int64_t bytesPerRowCompressed_{0};
  int64_t memLimit_{0};
};

struct WriterOptions {
  bool enableDictionary = true;
  bool enableRowGroupAlignedWrite = false;
  bool enableFlushBasedOnBlockSize = false;
  int64_t parquet_block_size = -1;
  std::vector<int32_t> expectedRowsInEachBlock;
  int64_t dataPageSize = 1'024 * 1'024;
  int64_t dictionaryPageSizeLimit = 1'024 * 1'024;
  // Growth ratio passed to ArrowDataBufferSink. The default value is a
  // heuristic borrowed from
  // folly/FBVector(https://github.com/facebook/folly/blob/main/folly/docs/FBVector.md#memory-handling).
  double bufferGrowRatio = 1.5;
  double bufferReserveRatio = 0; // 0 denotes we don't pre-reserve buffer
  std::unordered_map<std::string, std::string> encryptionOptions;
  common::CompressionKind compression = common::CompressionKind_NONE;
  arrow::Encoding::type encoding = arrow::Encoding::PLAIN;
  bolt::memory::MemoryPool* memoryPool;
  // The default factory allows the writer to construct the default flush
  // policy with the configs in its ctor.
  std::function<std::unique_ptr<DefaultFlushPolicy>()> flushPolicyFactory;
  std::shared_ptr<CodecOptions> codecOptions;
  // columnPath to CompressionKind
  // for primitive type, columnPath is `column_name`
  // for struct type, columnPath is `struct_column_name.child_field`
  // for array type, columnPath is `array_column_name.list.element`
  // for map type, columnPath is `map_column_name.key_value.key` or
  // `map_column_name.key_value.value`
  std::unordered_map<std::string, common::CompressionKind>
      columnCompressionsMap;
  // columnPath to CodecOptions
  std::unordered_map<std::string, std::shared_ptr<CodecOptions>>
      columnCodecOptionsMap;
  // columnPath to enableDictionary
  std::unordered_map<std::string, bool> columnEnableDictionaryMap;
  // columnPath to dictionaryPageSizeLimit
  std::unordered_map<std::string, int64_t> columnDictionaryPageSizeLimitMap;
  // columnPath to dataPageSize
  std::unordered_map<std::string, int64_t> columnDataPageSizeMap;
  /// Timestamp unit for Parquet write through Arrow bridge.
  /// Default if not specified: TimestampUnit::kNano (9).
  std::optional<TimestampUnit> parquetWriteTimestampUnit;
  std::optional<std::string> parquetWriteTimestampTimeZone;
  bool writeInt96AsTimestamp = false;
  int64_t bufferFlushThresholdBytes = 10 * 1024 * 1024; // 10M
  arrow::ParquetVersion::type parquetVersion =
      arrow::ParquetVersion::PARQUET_2_6;
  arrow::ParquetDataPageVersion dataPageVersion =
      arrow::ParquetDataPageVersion::V1;
  bool storeDecimalAsInteger = true;
  uint64_t writeBatchBytes = 40 * 1024 * 1024; // 40M
  vector_size_t minBatchSize = 512;
  // Controls the number of threads to use for ParquetWriter.
  // If greater than 0, threading is enabled (`set_use_threads` is set to true)
  // and a global static thread pool with this number of threads is created.
  // If 0 or less, threading is disabled (`set_use_threads` is set to false).
  int32_t threadPoolSize = 0;

  std::shared_ptr<arrow::WriterProperties::Builder> getWriterPropertiesBuilder()
      const;
  std::shared_ptr<arrow::ArrowWriterProperties::Builder>
  getArrowWriterPropertiesBuilder() const;
};

// Writes Bolt vectors into  a DataSink using Arrow Parquet writer.
class Writer : public dwio::common::Writer {
 public:
  // Constructs a writer with output to 'sink'. A new row group is
  // started every 'rowsInRowGroup' top level rows. 'pool' is used for
  // temporary memory. 'properties' specifies Parquet-specific
  // options. 'schema' specifies the file's overall schema, and it is always
  // non-null.
  Writer(
      std::unique_ptr<dwio::common::FileSink> sink,
      const WriterOptions& options,
      std::shared_ptr<memory::MemoryPool> pool,
      arrow::MemoryPool* arrowPool,
      RowTypePtr schema,
      std::shared_ptr<::arrow::Schema> arrowSchema = nullptr);

  Writer(
      std::unique_ptr<dwio::common::FileSink> sink,
      const WriterOptions& options,
      RowTypePtr schema);

  ~Writer() override = default;

  static bool isCodecAvailable(common::CompressionKind compression);

  // Appends 'data' into the writer.
  void write(const VectorPtr& data) override;

  void write(const VectorPtr& data, int64_t memLimit);

  void flush() override;
  void flush(int64_t rowsInCurrentRowGroup);

  // Forces a row group boundary before the data added by next write().
  void newRowGroup(int32_t numRows = 0);

  // Closes 'this', After close, data can no longer be added and the completed
  // Parquet file is flushed into 'sink' provided at construction. 'sink' stays
  // live until destruction of 'this'.
  void close() override;

  void abort() override;

  // Used in data retention scenario. Write parquet with specific row numbers in
  // each row group.
  void rowGroupAlignedFlush(
      int32_t numRows,
      uint64_t bytes,
      std::shared_ptr<::arrow::RecordBatch>& recordBatch);

 private:
  void splitWriteRecordBatch(
      const VectorPtr& data,
      std::shared_ptr<::arrow::Schema> arraySchemaPtr);

  // Sets the memory reclaimers for all the memory pools used by this writer.
  void setMemoryReclaimers();

  void writeRecordBatch(std::shared_ptr<::arrow::RecordBatch>& recordBatch);

  void createFileWriterIfNotExist();

  void createEmptyFile();

  void parseColumnWidth();

  uint64_t estimateExportArrowSize(const RowVectorPtr& data);

  // Pool for 'stream_'.
  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<memory::MemoryPool> generalPool_;
  std::shared_ptr<memory::MemoryPool> exportPool_;
  arrow::MemoryPool* arrowPool_{nullptr};

  // Temporary Arrow stream for capturing the output.
  std::shared_ptr<ArrowDataBufferSink> stream_;

  std::shared_ptr<ArrowContext> arrowContext_;

  std::unique_ptr<DefaultFlushPolicy> flushPolicy_;

  const RowTypePtr schema_;
  const std::shared_ptr<::arrow::Schema> arrowSchemaFromHive_;

  ArrowOptions options_{
      .flattenDictionary = true,
      .flattenConstant = true,
      .useLargeString = true};

  const double bufferGrowRatio_;
  const double bufferReserveRatio_;
  const bool enableRowGroupAlignedWrite_;
  const std::vector<int32_t> expectedRowsInEachBlock_;
  const bool enableFlushBasedOnBlockSize_;

  int32_t currentRowGroup_ = 0;
  bool closed_{false};

  uint64_t writeBatchBytes_{0};
  vector_size_t minBatchSize_{0};
  int64_t memLimit_{0};
  // Excluding Varchar and Varbinary type
  uint64_t totalFixedColumnWidth_{0};
  std::vector<uint32_t> variableLengthColumnIndex_{};
};

class ParquetWriterFactory : public dwio::common::WriterFactory {
 public:
  ParquetWriterFactory() : WriterFactory(dwio::common::FileFormat::PARQUET) {}

  std::unique_ptr<dwio::common::Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      const dwio::common::WriterOptions& options) override;
};

} // namespace bytedance::bolt::parquet
