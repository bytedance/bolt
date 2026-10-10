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
#include "bolt/dwio/common/Mutation.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::connector::paimon {

namespace {

// Input schemas are consumed even on failed resets. Partial output exports
// must likewise be released if exporting the other C ABI object fails.
template <typename T>
struct ArrowRelease {
  T* value;
  ~ArrowRelease() {
    if (value && value->release) {
      value->release(value);
    }
  }
};

class PaimonParquetFileBatchReader : public ::paimon::FileBatchReader {
 public:
  PaimonParquetFileBatchReader(
      std::shared_ptr<ReadFile> file,
      int32_t batchSize,
      memory::MemoryPool* pool,
      core::ExpressionEvaluator* expressionEvaluator,
      uint8_t timestampPrecision)
      : file_(std::move(file)),
        batchSize_(batchSize),
        pool_(pool),
        expressionEvaluator_(expressionEvaluator),
        timestampPrecision_(timestampPrecision) {
    BOLT_CHECK_GT(batchSize_, 0, "Parquet batch size must be positive");
    reader_ = createFileReader();
    readType_ = reader_->rowType();
  }

  ::paimon::Result<std::unique_ptr<::ArrowSchema>> GetFileSchema()
      const override {
    if (!reader_) {
      return ::paimon::Status::Invalid("Parquet reader is closed");
    }
    auto schema = std::make_unique<::ArrowSchema>();
    auto vector = BaseVector::create(reader_->rowType(), 0, pool_);
    exportToArrow(vector, *schema, {});
    return schema;
  }

  ::paimon::Status SetReadSchema(
      ::ArrowSchema* schema,
      const std::shared_ptr<::paimon::Predicate>& predicate,
      const std::optional<::paimon::RoaringBitmap32>& selection) override {
    ArrowRelease schemaGuard{schema};
    if (!reader_ || !schema) {
      return ::paimon::Status::Invalid("Parquet reader or read schema is null");
    }
    try {
      auto type =
          std::dynamic_pointer_cast<const RowType>(importFromArrow(*schema));
      if (!type) {
        return ::paimon::Status::Invalid(
            "Parquet read schema must be a row type");
      }
      // Commit a new read only after construction succeeds; a failed reset
      // leaves the native cursor and the previous row mapping intact.
      // Native Parquet row readers share buffered row-group state, so a
      // rewind needs an independent file reader over the same input file.
      auto replacement = rowReader_ ? createFileReader() : nullptr;
      bool appendRowNumbers;
      auto rowReader = createRowReader(
          replacement ? *replacement : *reader_,
          type,
          predicate,
          appendRowNumbers);
      rowReader_ = std::move(rowReader);
      if (replacement) {
        reader_ = std::move(replacement);
      }
      readType_ = std::move(type);
      appendRowNumbers_ = appendRowNumbers;
      selection_ = selection;
      positions_.clear();
      return ::paimon::Status::OK();
    } catch (const std::exception& e) {
      return ::paimon::Status::Invalid(
          std::string("Failed to set Parquet read schema: ") + e.what());
    }
  }

  ::paimon::Result<ReadBatch> NextBatch() override {
    positions_.clear();
    try {
      auto output = readBatch();
      if (!output) {
        return MakeEofBatch();
      }
      return exportBatch(output);
    } catch (const std::exception& e) {
      positions_.clear();
      return ::paimon::Status::IOError(
          std::string("Failed to read Parquet batch: ") + e.what());
    }
  }

  std::shared_ptr<::paimon::Metrics> GetReaderMetrics() const override {
    return nullptr;
  }

  void Close() override {
    positions_.clear();
    selection_.reset();
    rowReader_.reset();
    reader_.reset();
    file_.reset();
  }

  ::paimon::Result<uint64_t> GetPreviousBatchFileRowId(
      uint64_t index) const override {
    if (index >= positions_.size()) {
      return ::paimon::Status::Invalid("No row at this Parquet batch index");
    }
    return positions_[index];
  }

  ::paimon::Result<uint64_t> GetNumberOfRows() const override {
    if (reader_) {
      if (auto rows = reader_->numberOfRows()) {
        return *rows;
      }
    }
    return ::paimon::Status::Invalid("Number of rows not available");
  }

  bool SupportPreciseBitmapSelection() const override {
    return true;
  }

 private:
  std::unique_ptr<parquet::ParquetReader> createFileReader() const {
    return std::make_unique<parquet::ParquetReader>(
        std::make_unique<dwio::common::BufferedInput>(file_, *pool_),
        dwio::common::ReaderOptions(pool_));
  }

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      parquet::ParquetReader& reader,
      const RowTypePtr& type,
      const std::shared_ptr<::paimon::Predicate>& predicate,
      bool& appendRowNumbers) {
    auto scanSpec = std::make_shared<bolt::common::ScanSpec>("<root>");
    scanSpec->addAllChildFields(*type);
    if (predicate) {
      const auto translated =
          PaimonFilterTranslator::toTypedExpr(predicate, pool_);
      BOLT_CHECK(translated.ok(), "{}", translated.reason);
      auto filters = PaimonFilterTranslator::toSubfieldFilters(
          translated.value, expressionEvaluator_);
      for (const auto& [subfield, filter] : filters) {
        scanSpec->getOrCreateChild(subfield)->addFilter(*filter);
      }
    }
    // The native count/constant-only path has no column-reader outputRows().
    // It retains physical rows; synthesize positions and apply the bitmap in
    // this adapter instead. Predicate-only physical columns still need native
    // row numbers even when none of those columns are projected.
    appendRowNumbers = false;
    for (const auto& child : scanSpec->children()) {
      appendRowNumbers |= reader.rowType()->containsChild(child->fieldName());
    }
    dwio::common::RowReaderOptions opts;
    opts.setScanSpec(scanSpec);
    opts.setTimestampPrecision(
        static_cast<TimestampPrecision>(timestampPrecision_));
    opts.setAppendRowNumberColumn(appendRowNumbers);
    opts.select(std::make_shared<dwio::common::ColumnSelector>(
        reader.rowType(), type->names()));
    return reader.createRowReader(opts);
  }

  bool selected(int64_t position) const {
    return !selection_ ||
        (position <= ::paimon::RoaringBitmap32::MAX_VALUE &&
         selection_->Contains(position));
  }

  RowVectorPtr readBatch() {
    BOLT_CHECK_NOT_NULL(reader_, "Parquet reader is closed");
    if (selection_ && selection_->IsEmpty()) {
      return nullptr;
    }
    if (!rowReader_) {
      rowReader_ =
          createRowReader(*reader_, readType_, nullptr, appendRowNumbers_);
    }
    while (true) {
      const auto first = rowReader_->nextRowNumber();
      if (first == dwio::common::RowReader::kAtEnd) {
        return nullptr;
      }
      BufferPtr deletedRows;
      dwio::common::Mutation mutation;
      if (appendRowNumbers_ && selection_) {
        const auto size = rowReader_->nextReadSize(batchSize_);
        deletedRows =
            AlignedBuffer::allocate<uint64_t>(bits::nwords(size), pool_, 0);
        auto* raw = deletedRows->asMutable<uint64_t>();
        for (int64_t i = 0; i < size; ++i) {
          if (!selected(first + i)) {
            bits::setBit(raw, i);
          }
        }
        mutation.deletedRows = raw;
      }
      VectorPtr output = BaseVector::create(readType_, batchSize_, pool_);
      const auto scanned = rowReader_->next(batchSize_, output, &mutation);
      if (scanned == 0) {
        return nullptr;
      }
      // A fully filtered batch is not EOF. Native next() counts physical rows.
      if (output->size() == 0) {
        continue;
      }
      auto row = std::dynamic_pointer_cast<RowVector>(output);
      auto children = row->children();
      if (appendRowNumbers_) {
        auto* rowNumbers = children.back()->as<SimpleVector<int64_t>>();
        BOLT_CHECK_NOT_NULL(rowNumbers);
        for (vector_size_t i = 0; i < row->size(); ++i) {
          positions_.push_back(rowNumbers->valueAt(i));
        }
        children.pop_back();
      } else {
        BOLT_CHECK_EQ(row->size(), scanned);
        auto indices =
            AlignedBuffer::allocate<vector_size_t>(row->size(), pool_);
        auto* raw = indices->asMutable<vector_size_t>();
        for (vector_size_t i = 0; i < row->size(); ++i) {
          if (selected(first + i)) {
            raw[positions_.size()] = i;
            positions_.push_back(first + i);
          }
        }
        if (positions_.empty()) {
          continue;
        }
        if (positions_.size() != row->size()) {
          for (auto& child : children) {
            child = BaseVector::wrapInDictionary(
                nullptr, indices, positions_.size(), child);
          }
        }
      }
      for (auto& child : children) {
        child = BaseVector::loadedVectorShared(child);
      }
      return std::make_shared<RowVector>(
          pool_, readType_, nullptr, positions_.size(), std::move(children));
    }
  }

  ReadBatch exportBatch(const VectorPtr& vector) const {
    auto array = std::make_unique<::ArrowArray>();
    auto schema = std::make_unique<::ArrowSchema>();
    ArrowRelease arrayGuard{array.get()};
    ArrowRelease schemaGuard{schema.get()};
    ArrowOptions opts;
    opts.timestampUnit = static_cast<TimestampUnit>(timestampPrecision_);
    // Paimon's row merge accesses primitive Arrow buffers directly. Neither
    // selection dictionaries nor constant-null run-end encoding may escape.
    opts.flattenDictionary = true;
    opts.flattenConstant = true;
    exportToArrow(vector, *array, pool_, opts);
    exportToArrow(vector, *schema, opts);
    arrayGuard.value = nullptr;
    schemaGuard.value = nullptr;
    return std::make_pair(std::move(array), std::move(schema));
  }

  std::shared_ptr<ReadFile> file_;
  std::unique_ptr<parquet::ParquetReader> reader_;
  std::unique_ptr<dwio::common::RowReader> rowReader_;
  int32_t batchSize_;
  memory::MemoryPool* const pool_;
  core::ExpressionEvaluator* const expressionEvaluator_;
  uint8_t timestampPrecision_;
  RowTypePtr readType_;
  std::optional<::paimon::RoaringBitmap32> selection_;
  bool appendRowNumbers_{false};
  std::vector<int64_t> positions_;
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
      return std::make_unique<PaimonParquetFileBatchReader>(
          std::move(rf),
          batch_size_,
          paimonPool_->getBoltPool(),
          paimonPool_->getExpressionEvaluator(),
          timestampPrecision_);
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(
          std::string("Failed to build reader from InputStream: ") + e.what());
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
