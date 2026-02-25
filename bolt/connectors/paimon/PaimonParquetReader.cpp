/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */


#include <memory>
#include "bolt/connectors/paimon/PaimonParquetReader.h"
#include "common/memory/Memory.h"
#include "common/memory/MemoryPool.h"
#include "connectors/paimon/BoltMemoryPool.h"
#include "paimon/format/reader_builder.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/fs/file_system.h" // for ::paimon::InputStream
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/common/file/File.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/vector/arrow/Abi.h"
#include <folly/io/IOBuf.h>

namespace bytedance::bolt::connector::paimon {

class PaimonParquetFileBatchReader : public ::paimon::FileBatchReader {
 public:
  PaimonParquetFileBatchReader(
      std::unique_ptr<parquet::ParquetReader> reader,
      int32_t batch_size,
      memory::MemoryPool* pool)
      : reader_(std::move(reader)), batch_size_(batch_size), pool_(pool) {
      // Create initial row reader with default options (read all)
      dwio::common::RowReaderOptions opts;
      // Ensure ScanSpec is set to avoid null deref in ParquetRowReader
      // Use root scan spec with no filters and full projection
      opts.setScanSpec(std::make_shared<bolt::common::ScanSpec>("<root>"));
      rowReader_ = reader_->createRowReader(opts);
  }

  ::paimon::Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
      auto schema = std::make_unique<::ArrowSchema>();

      // Create a dummy vector to export schema
      const auto rowType = reader_->rowType();
      auto dummyVector = BaseVector::create(rowType, 0, pool_);

      ArrowOptions opts;
      exportToArrow(dummyVector, *schema, opts);
      return std::move(schema);
  }

  ::paimon::Status SetReadSchema(::ArrowSchema* read_schema,
                                 const std::shared_ptr<::paimon::Predicate>& predicate,
                                 const std::optional<::paimon::RoaringBitmap32>& selection_bitmap) override {
      try {
          // 1. Convert ArrowSchema to Bolt Type
          auto type = importFromArrow(*read_schema);
          auto rowType = std::dynamic_pointer_cast<const RowType>(type);
          if (!rowType) {
              return ::paimon::Status::Invalid("Read schema must be a struct/row type");
          }

          // 2. Configure options - create column selector with desired columns
          dwio::common::RowReaderOptions opts;
          auto fileRowType = reader_->rowType();
          auto selector = std::make_shared<dwio::common::ColumnSelector>(fileRowType, rowType->names());
          opts.select(selector);

          // 3. Create new RowReader
          rowReader_ = reader_->createRowReader(opts);

          return ::paimon::Status::OK();
      } catch (const std::exception& e) {
          return ::paimon::Status::Invalid(std::string("Failed to set read schema: ") + e.what());
      }
  }

  ::paimon::Result<ReadBatch> NextBatch() override {
      try {
          // Preallocate an empty RowVector to be populated by RowReader
          VectorPtr result = BaseVector::create(reader_->rowType(), 0, pool_);
          bool hasData = rowReader_->next(batch_size_, result) != 0;

          if (!hasData) {
              return ::paimon::Status::Invalid("End of file reached");
          }

          auto arrowArray = std::make_unique<::ArrowArray>();
          auto arrowSchema = std::make_unique<::ArrowSchema>();

          ArrowOptions opts;
          exportToArrow(result, *arrowArray, pool_, opts);
          exportToArrow(result, *arrowSchema, opts);

          return std::make_pair(std::move(arrowArray), std::move(arrowSchema));
      } catch (const std::exception& e) {
          return ::paimon::Status::IOError(std::string("Failed to read batch: ") + e.what());
      }
  }

  std::shared_ptr<::paimon::Metrics> GetReaderMetrics() const override {
      return nullptr;
  }

  void Close() override {}

  uint64_t GetPreviousBatchFirstRowNumber() const override { return 0; }
  ::paimon::Result<uint64_t> GetNumberOfRows() const override {
      auto numRows = reader_->numberOfRows();
      if (numRows) {
          return *numRows;
      }
      return ::paimon::Status::Invalid("Number of rows not available");
  }
  bool SupportPreciseBitmapSelection() const override { return false; }

 private:
  std::unique_ptr<parquet::ParquetReader> reader_;
  std::unique_ptr<dwio::common::RowReader> rowReader_;
  int32_t batch_size_;
  memory::MemoryPool* pool_;
};

class PaimonParquetReaderBuilder : public ::paimon::ReaderBuilder {
 public:
  explicit PaimonParquetReaderBuilder(int32_t batch_size, const std::shared_ptr<memory::MemoryPool>& pool)
      : batch_size_(batch_size) {
    paimonPool_ = std::make_shared<BoltPaimonMemoryPool>(pool);
  }

  ::paimon::ReaderBuilder* WithMemoryPool(const std::shared_ptr<::paimon::MemoryPool>& pool) override {
    auto boltPool = std::dynamic_pointer_cast<BoltPaimonMemoryPool>(pool);
      if (boltPool != nullptr) {
        paimonPool_ = boltPool;
      }
      return this;
  }

  ::paimon::Result<std::unique_ptr<::paimon::FileBatchReader>> Build(
      const std::shared_ptr<::paimon::InputStream>& path) const override {
      try {
      // Choose Bolt memory pool to back DWIO
      memory::MemoryPool* boltPool = paimonPool_->getBoltPool();

          auto rf = std::make_shared<PaimonReadFile>(path);
          auto input = std::make_unique<dwio::common::BufferedInput>(
              std::make_shared<dwio::common::ReadFileInputStream>(rf), *boltPool);

          dwio::common::ReaderOptions readerOptions(boltPool);
          auto reader = std::make_unique<parquet::ParquetReader>(std::move(input), readerOptions);

          return std::make_unique<PaimonParquetFileBatchReader>(
              std::move(reader), batch_size_, boltPool);
      } catch (const std::exception& e) {
          return ::paimon::Status::IOError(std::string("Failed to build reader from InputStream: ") + e.what());
      }
  }

  ::paimon::Result<std::unique_ptr<::paimon::FileBatchReader>> Build(const std::string& path) const override {
      try {
          auto file = std::make_shared<LocalReadFile>(path);
      // Choose Bolt memory pool to back DWIO
      memory::MemoryPool* boltPool = paimonPool_->getBoltPool();

          auto input = std::make_unique<dwio::common::BufferedInput>(file, *boltPool);

          dwio::common::ReaderOptions readerOptions(boltPool);
          auto reader = std::make_unique<parquet::ParquetReader>(std::move(input), readerOptions);

          return std::make_unique<PaimonParquetFileBatchReader>(std::move(reader), batch_size_, boltPool);
      } catch (const std::exception& e) {
          return ::paimon::Status::IOError(std::string("Failed to open file: ") + e.what());
      }
  }

 private:
  int32_t batch_size_;
  std::shared_ptr<BoltPaimonMemoryPool> paimonPool_;
};

const std::string& PaimonParquetReader::Identifier() const {
  static const std::string kIdentifier = "parquet";
  return kIdentifier;
}

::paimon::Result<std::unique_ptr<::paimon::ReaderBuilder>> PaimonParquetReader::CreateReaderBuilder(
    int32_t batch_size) const {
  auto pool = memory::memoryManager()->addLeafPool("paimon-parquet-reader");
  return std::make_unique<PaimonParquetReaderBuilder>(batch_size, std::move(pool));
}

::paimon::Result<std::unique_ptr<::paimon::WriterBuilder>> PaimonParquetReader::CreateWriterBuilder(
    ::ArrowSchema* /* schema */, int32_t /* batch_size */) const {
  return ::paimon::Status::NotImplemented("Writer not supported yet");
}

::paimon::Result<std::unique_ptr<::paimon::FormatStatsExtractor>> PaimonParquetReader::CreateStatsExtractor(
    ::ArrowSchema* schema) const {
  return ::paimon::Status::NotImplemented("Stats extractor not supported yet");
}

} // namespace bytedance::bolt::connector::paimon
