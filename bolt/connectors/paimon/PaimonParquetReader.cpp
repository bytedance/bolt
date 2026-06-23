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

#include "bolt/connectors/paimon/PaimonParquetReader.h"
#include <folly/io/IOBuf.h>
#include <paimon/format/reader_builder.h>
#include <paimon/fs/file_system.h>
#include <paimon/reader/file_batch_reader.h>
#include <paimon/result.h>
#include <memory>
#include "bolt/common/file/File.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/connectors/paimon/PaimonFilterTranslator.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::connector::paimon {

namespace {

class PaimonParquetFileBatchReader : public ::paimon::FileBatchReader {
 private:
  static std::shared_ptr<bolt::common::ScanSpec> buildScanSpecFromRowType(
      const RowTypePtr& rowType) {
    auto scanSpec = std::make_shared<bolt::common::ScanSpec>("<root>");
    scanSpec->addAllChildFields(*rowType);
    return scanSpec;
  }

  dwio::common::RowReaderOptions makeRowReaderOpts(
      const RowTypePtr& rowType) const {
    dwio::common::RowReaderOptions opts;
    opts.setScanSpec(buildScanSpecFromRowType(rowType));
    opts.setTimestampPrecision(
        static_cast<TimestampPrecision>(timestampPrecision_));
    return opts;
  }

  void initializeRowReaderWithFullSchema() {
    // LOG(INFO) << "Initializing rowReader_ with full file schema: " <<
    // reader_->rowType()->toString();
    rowReader_ =
        reader_->createRowReader(makeRowReaderOpts(reader_->rowType()));
  }

 public:
  PaimonParquetFileBatchReader(
      std::unique_ptr<parquet::ParquetReader> reader,
      int32_t batch_size,
      memory::MemoryPool* const pool,
      uint8_t timestampPrecision)
      : reader_(std::move(reader)),
        batch_size_(batch_size),
        pool_(pool),
        timestampPrecision_(timestampPrecision),
        readType_(reader_->rowType()) {
    // LOG(INFO) << "PaimonParquetFileBatchReader created, reader_->rowType() =
    // " << reader_->rowType()->toString();
  }

  ::paimon::Result<std::unique_ptr<::ArrowSchema>> GetFileSchema()
      const override {
    auto schema = std::make_unique<::ArrowSchema>();

    const auto& fileRowType = reader_->rowType();

    auto dummyVector = BaseVector::create(fileRowType, 0, pool_);

    ArrowOptions opts;
    exportToArrow(dummyVector, *schema, opts);

    if (VLOG_IS_ON(1)) {
      VLOG(1) << "GetFileSchema exported ArrowSchema has " << schema->n_children
              << " children";
      for (int i = 0; i < schema->n_children; ++i) {
        VLOG(1) << "GetFileSchema child[" << i << "]: name="
                << (schema->children[i]->name ? schema->children[i]->name : "")
                << ", format="
                << (schema->children[i]->format ? schema->children[i]->format
                                                : "");
      }
    }
    return std::move(schema);
  }

  ::paimon::Status SetReadSchema(
      ::ArrowSchema* read_schema,
      const std::shared_ptr<::paimon::Predicate>& predicate,
      const std::optional<::paimon::RoaringBitmap32>& /*selection_bitmap*/)
      override {
    try {
      auto type = importFromArrow(*read_schema);
      auto rowType = std::dynamic_pointer_cast<const RowType>(type);
      if (!rowType) {
        return ::paimon::Status::Invalid(
            "Read schema must be a struct/row type");
      }

      std::vector<std::string> dataColumnNames;
      int startIndex = 0;
      for (int i = startIndex; i < rowType->size(); ++i) {
        dataColumnNames.push_back(rowType->nameOf(i));
      }

      dwio::common::RowReaderOptions opts = makeRowReaderOpts(rowType);
      auto fileRowType = reader_->rowType();

      auto scanSpec = buildScanSpecFromRowType(rowType);
      if (predicate) {
        // Convert: paimon::Predicate → TypedExprPtr → SubfieldFilters →
        // ScanSpec
        auto result = PaimonFilterTranslator::toTypedExpr(predicate, pool_);
        BOLT_CHECK(
            result.ok(),
            "expression {} not supported for filter pushdown by paimon connector",
            predicate->ToString());
        auto filters = PaimonFilterTranslator::toSubfieldFilters(result.value);
        if (filters.empty()) {
          LOG(INFO) << "[FilterPushdown] predicate translated successfully but "
                       "produced zero subfield filters — filter will not be "
                       "evaluated in the scan";
        } else {
          LOG(INFO) << "[FilterPushdown] pushed down " << filters.size()
                    << " subfield filter(s)";
        }
        for (const auto& [subfield, filter] : filters) {
          auto* fieldSpec = scanSpec->getOrCreateChild(subfield);
          fieldSpec->addFilter(*filter);
        }
      }
      opts.setScanSpec(scanSpec);
      auto selector = std::make_shared<dwio::common::ColumnSelector>(
          fileRowType, dataColumnNames);
      opts.select(selector);
      rowReader_ = reader_->createRowReader(opts);
      readType_ = rowType;

      return ::paimon::Status::OK();
    } catch (const std::exception& e) {
      LOG(ERROR) << "SetReadSchema: exception " << e.what();
      return ::paimon::Status::Invalid(
          std::string("Failed to set read schema: ") + e.what());
    }
  }

  ::paimon::Result<ReadBatch> NextBatch() override {
    try {
      if (!rowReader_) {
        LOG(INFO)
            << "NextBatch: rowReader_ not initialized, initializing with full schema";
        initializeRowReaderWithFullSchema();
      }

      // Record the absolute file row number where this batch starts.
      // nextRowNumber() returns the position relative to file start,
      // including rows that may be filtered/deleted — this is what
      // paimon-cpp needs for deletion vector offset computation.
      int64_t nextRow = rowReader_->nextRowNumber();
      if (nextRow == dwio::common::RowReader::kAtEnd) {
        LOG(INFO) << "NextBatch: End of file reached";
        return ::paimon::BatchReader::MakeEofBatch();
      }
      previousBatchFirstRowNumber_ = static_cast<uint64_t>(nextRow);

      VectorPtr result = BaseVector::create(readType_, batch_size_, pool_);
      uint64_t numRows = rowReader_->next(batch_size_, result);

      if (numRows == 0) {
        LOG(INFO) << "NextBatch: End of file reached";
        return ::paimon::BatchReader::MakeEofBatch();
      }

      // LOG(INFO) << "NextBatch: result has type " <<
      // result->type()->toString(); LOG(INFO) << "NextBatch: number of rows in
      // batch = " << result->size();

      auto arrowArray = std::make_unique<::ArrowArray>();
      auto arrowSchema = std::make_unique<::ArrowSchema>();

      ArrowOptions opts;
      opts.timestampUnit = static_cast<TimestampUnit>(timestampPrecision_);
      exportToArrow(result, *arrowArray, pool_, opts);
      exportToArrow(result, *arrowSchema, opts);

      // LOG(INFO) << "NextBatch: exported ArrowSchema has "
      //           << arrowSchema->n_children << " children";
      // for (int i = 0; i < arrowSchema->n_children; ++i) {
      //   LOG(INFO) << "NextBatch exported child[" << i << "]: name=" <<
      //   (arrowSchema->children[i]->name ? arrowSchema->children[i]->name :
      //   "")
      //             << ", format=" << (arrowSchema->children[i]->format ?
      //             arrowSchema->children[i]->format : "");
      // }
      // LOG(INFO) << "NextBatch: exported ArrowArray has length " <<
      // arrowArray->length;

      hasReadAnyBatch_ = true;
      return std::make_pair(std::move(arrowArray), std::move(arrowSchema));
    } catch (const std::exception& e) {
      LOG(ERROR) << "NextBatch: exception " << e.what();
      return ::paimon::Status::IOError(
          std::string("Failed to read batch: ") + e.what());
    }
  }

  std::shared_ptr<::paimon::Metrics> GetReaderMetrics() const override {
    return nullptr;
  }

  void Close() override {
    rowReader_.reset();
    reader_.reset();
  }

  ::paimon::Result<uint64_t> GetPreviousBatchFirstRowNumber() const override {
    return previousBatchFirstRowNumber_;
  }

  ::paimon::Result<uint64_t> GetNumberOfRows() const override {
    auto numRows = reader_->numberOfRows();
    if (numRows) {
      return *numRows;
    }
    return ::paimon::Status::Invalid("Number of rows not available");
  }
  bool SupportPreciseBitmapSelection() const override {
    return false;
  }

 private:
  std::unique_ptr<parquet::ParquetReader> reader_;
  std::unique_ptr<dwio::common::RowReader> rowReader_;
  int32_t batch_size_;
  memory::MemoryPool* const pool_;
  uint8_t timestampPrecision_;
  RowTypePtr readType_;
  uint64_t previousBatchFirstRowNumber_{0};
  bool hasReadAnyBatch_{false};
};

class PaimonParquetReaderBuilder : public ::paimon::ReaderBuilder {
 public:
  explicit PaimonParquetReaderBuilder(
      int32_t batch_size,
      const PaimonIoOptions& ioOptions,
      uint8_t timestampPrecision)
      : batch_size_(batch_size),
        ioOptions_(ioOptions),
        timestampPrecision_(timestampPrecision) {}

  ::paimon::ReaderBuilder* WithMemoryPool(
      const std::shared_ptr<::paimon::MemoryPool>& pool) override {
    auto boltPool = std::dynamic_pointer_cast<BoltPaimonMemoryPool>(pool);
    if (boltPool != nullptr) {
      paimonPool_ = boltPool;
    }
    return this;
  }

  ::paimon::Result<std::unique_ptr<::paimon::FileBatchReader>> Build(
      const std::shared_ptr<::paimon::InputStream>& path) const override {
    BOLT_CHECK_NOT_NULL(
        paimonPool_,
        "PaimonParquetReaderBuilder requires WithMemoryPool to be called before Build");
    try {
      auto rf = std::make_shared<PaimonReadFile>(path, ioOptions_);
      auto input = std::make_unique<dwio::common::BufferedInput>(
          std::make_shared<dwio::common::ReadFileInputStream>(rf),
          *paimonPool_->getBoltPool());

      dwio::common::ReaderOptions readerOptions(paimonPool_->getBoltPool());
      auto reader = std::make_unique<parquet::ParquetReader>(
          std::move(input), readerOptions);

      return std::make_unique<PaimonParquetFileBatchReader>(
          std::move(reader),
          batch_size_,
          paimonPool_->getBoltPool(),
          timestampPrecision_);
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(
          std::string("Failed to build reader from InputStream: ") + e.what());
    }
  }

  ::paimon::Result<std::unique_ptr<::paimon::FileBatchReader>> Build(
      const std::string& path) const override {
    BOLT_CHECK_NOT_NULL(
        paimonPool_,
        "PaimonParquetReaderBuilder requires WithMemoryPool to be called before Build");
    try {
      auto file = std::make_shared<LocalReadFile>(path);
      memory::MemoryPool* boltPool = paimonPool_->getBoltPool();

      auto input =
          std::make_unique<dwio::common::BufferedInput>(file, *boltPool);

      dwio::common::ReaderOptions readerOptions(boltPool);
      auto reader = std::make_unique<parquet::ParquetReader>(
          std::move(input), readerOptions);

      return std::make_unique<PaimonParquetFileBatchReader>(
          std::move(reader),
          batch_size_,
          paimonPool_->getBoltPool(),
          timestampPrecision_);
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(
          std::string("Failed to open file: ") + e.what());
    }
  }

 private:
  int32_t batch_size_;
  PaimonIoOptions ioOptions_;
  uint8_t timestampPrecision_;
  std::shared_ptr<BoltPaimonMemoryPool> paimonPool_;
};

} // namespace

PaimonParquetReader::PaimonParquetReader(
    const std::map<std::string, std::string>& options) {
  auto it = options.find(PaimonConfig::kNaturalReadSize);
  if (it != options.end()) {
    ioOptions_.naturalReadSize = std::stoull(it->second);
  }
  it = options.find(PaimonConfig::kCoalesceReads);
  if (it != options.end()) {
    ioOptions_.coalesceReads = it->second == "true";
  }
  it = options.find(PaimonConfig::kReadTimestampUnit);
  if (it != options.end()) {
    timestampPrecision_ = static_cast<uint8_t>(std::stoi(it->second));
  }
}

const std::string& PaimonParquetReader::Identifier() const {
  static const std::string kIdentifier = "parquet";
  return kIdentifier;
}

::paimon::Result<std::unique_ptr<::paimon::ReaderBuilder>>
PaimonParquetReader::CreateReaderBuilder(int32_t batch_size) const {
  return std::make_unique<PaimonParquetReaderBuilder>(
      batch_size, ioOptions_, timestampPrecision_);
}

::paimon::Result<std::unique_ptr<::paimon::WriterBuilder>>
PaimonParquetReader::CreateWriterBuilder(
    ::ArrowSchema* /* schema */,
    int32_t /* batch_size */) const {
  return ::paimon::Status::NotImplemented("Writer not supported yet");
}

::paimon::Result<std::unique_ptr<::paimon::FormatStatsExtractor>>
PaimonParquetReader::CreateStatsExtractor(::ArrowSchema* /*schema*/) const {
  return ::paimon::Status::NotImplemented("Stats extractor not supported yet");
}

void EnsurePaimonParquetFormatRegistered() {
  ::paimon::ensureParquetFormatFactoryRegistered();
}

} // namespace bytedance::bolt::connector::paimon

namespace paimon {

Result<std::unique_ptr<::paimon::FileFormat>> ParquetFileFormatFactory::Create(
    const std::map<std::string, std::string>& options) const {
  return std::make_unique<
      bytedance::bolt::connector::paimon::PaimonParquetReader>(options);
}

// Explicit registration function (called from
// EnsurePaimonParquetFormatRegistered in PaimonDataSource). Using an explicit
// call rather than REGISTER_PAIMON_FACTORY macro because the linker may strip
// static
// __attribute__((constructor)) functions from object files inside static
// archives when no symbol explicitly references them.
void ensureParquetFormatFactoryRegistered() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    LOG(INFO)
        << "[PAIMON] Registering bolt ParquetFileFormatFactory with identifier='"
        << ParquetFileFormatFactory::kIDENTIFIER << "'";
    auto* factory = new ParquetFileFormatFactory;
    ::paimon::FactoryCreator::GetInstance()->Register(
        factory->Identifier(), factory);
    LOG(INFO) << "[PAIMON] ParquetFileFormatFactory registration complete";
  });
}

} // namespace paimon
