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

#include "bolt/connectors/paimon/PaimonOrcReader.h"

#include <paimon/factories/factory_creator.h>
#include <paimon/format/file_format_factory.h>
#include <paimon/format/reader_builder.h>
#include <paimon/predicate/compound_predicate.h>
#include <paimon/predicate/leaf_predicate.h>
#include <paimon/reader/file_batch_reader.h>
#include <mutex>
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/orc/reader/OrcReader.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::connector::paimon {
namespace {

// Release consumed input schemas and partially exported C data. Successful
// output exports transfer their release callbacks to Paimon's caller.
template <typename T>
struct ArrowRelease {
  T* value;
  ~ArrowRelease() {
    if (value && value->release) {
      value->release(value);
    }
  }
};

bool containsTimestamp(const Type& type) {
  if (type.kind() == TypeKind::TIMESTAMP) {
    return true;
  }
  for (uint32_t i = 0; i < type.size(); ++i) {
    if (containsTimestamp(*type.childAt(i))) {
      return true;
    }
  }
  return false;
}

bool hasTimestampPredicate(
    const std::shared_ptr<::paimon::Predicate>& predicate) {
  if (!predicate) {
    return false;
  }
  if (auto leaf =
          std::dynamic_pointer_cast<::paimon::LeafPredicate>(predicate)) {
    return leaf->GetFieldType() == ::paimon::FieldType::TIMESTAMP;
  }
  auto compound =
      std::dynamic_pointer_cast<::paimon::CompoundPredicate>(predicate);
  BOLT_USER_CHECK(compound, "Unsupported Paimon ORC predicate");
  for (const auto& child : compound->Children()) {
    if (hasTimestampPredicate(child)) {
      return true;
    }
  }
  return false;
}

class PaimonOrcFileBatchReader final : public ::paimon::FileBatchReader {
 public:
  PaimonOrcFileBatchReader(
      std::unique_ptr<dwio::common::Reader> reader,
      int32_t batchSize,
      std::shared_ptr<BoltPaimonMemoryPool> pool,
      uint8_t timestampPrecision)
      : reader_(std::move(reader)),
        batchSize_(batchSize),
        pool_(std::move(pool)),
        timestampPrecision_(timestampPrecision),
        readType_(reader_->rowType()) {}

  ::paimon::Result<std::unique_ptr<::ArrowSchema>> GetFileSchema()
      const override {
    if (!reader_) {
      return ::paimon::Status::Invalid("ORC reader is closed");
    }
    try {
      auto schema = std::make_unique<::ArrowSchema>();
      ArrowRelease guard{schema.get()};
      exportToArrow(
          BaseVector::create(reader_->rowType(), 0, boltPool()),
          *schema,
          arrowOptions());
      guard.value = nullptr;
      return std::move(schema);
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(std::string("ORC schema: ") + e.what());
    }
  }

  ::paimon::Status SetReadSchema(
      ::ArrowSchema* schema,
      const std::shared_ptr<::paimon::Predicate>& predicate,
      const std::optional<::paimon::RoaringBitmap32>& /*selectionBitmap*/)
      override {
    // Paimon transfers the exported schema even when validation fails.
    ArrowRelease schemaGuard{schema};
    if (!reader_) {
      return ::paimon::Status::Invalid("ORC reader is closed");
    }
    // Paimon passes index/deletion selections even to conservative readers.
    // Since SupportPreciseBitmapSelection() is false, its bitmap wrapper
    // applies the selection to the physical positions in our batches.
    if (!schema) {
      return ::paimon::Status::Invalid("ORC read schema must not be null");
    }
    try {
      auto type =
          std::dynamic_pointer_cast<const RowType>(importFromArrow(*schema));
      if (!type) {
        return ::paimon::Status::Invalid("ORC read schema must be a row type");
      }
      auto rowReader = createRowReader(type, predicate);
      if (!rowReader.ok()) {
        return rowReader.status();
      }
      rowReader_ = std::move(rowReader).value();
      readType_ = std::move(type);
      previousBatchSize_ = 0;
      return ::paimon::Status::OK();
    } catch (const std::exception& e) {
      return ::paimon::Status::Invalid(
          std::string("ORC read schema: ") + e.what());
    }
  }

  ::paimon::Result<ReadBatch> NextBatch() override {
    previousBatchSize_ = 0;
    if (!reader_) {
      return ::paimon::Status::Invalid("ORC reader is closed");
    }
    try {
      if (!rowReader_) {
        auto rowReader = createRowReader(readType_, nullptr);
        if (!rowReader.ok()) {
          return rowReader.status();
        }
        rowReader_ = std::move(rowReader).value();
      }
      while (true) {
        const auto firstRow = rowReader_->nextRowNumber();
        if (firstRow == dwio::common::RowReader::kAtEnd) {
          return ::paimon::BatchReader::MakeEofBatch();
        }
        // Each export owns its vector; never reuse a vector held by a caller.
        VectorPtr result =
            BaseVector::create(readType_, batchSize_, boltPool());
        const auto scanned = rowReader_->next(batchSize_, result);
        if (scanned == 0) {
          return ::paimon::BatchReader::MakeEofBatch();
        }
        if (result->size() == 0) {
          continue;
        }
        auto array = std::make_unique<::ArrowArray>();
        auto schema = std::make_unique<::ArrowSchema>();
        ArrowRelease arrayGuard{array.get()};
        ArrowRelease schemaGuard{schema.get()};
        exportToArrow(result, *array, boltPool(), arrowOptions());
        exportToArrow(result, *schema, arrowOptions());
        arrayGuard.value = nullptr;
        schemaGuard.value = nullptr;
        previousBatchFirstRowNumber_ = firstRow;
        previousBatchSize_ = result->size();
        return std::make_pair(std::move(array), std::move(schema));
      }
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(std::string("ORC batch: ") + e.what());
    }
  }

  std::shared_ptr<::paimon::Metrics> GetReaderMetrics() const override {
    return nullptr;
  }

  void Close() override {
    rowReader_.reset();
    reader_.reset();
    previousBatchSize_ = 0;
  }

  ::paimon::Result<uint64_t> GetPreviousBatchFileRowId(
      uint64_t batchRowId) const override {
    if (batchRowId >= previousBatchSize_) {
      return ::paimon::Status::Invalid(
          "ORC batch row index is out of range or no batch is available");
    }
    return previousBatchFirstRowNumber_ + batchRowId;
  }

  ::paimon::Result<uint64_t> GetNumberOfRows() const override {
    if (!reader_) {
      return ::paimon::Status::Invalid("ORC reader is closed");
    }
    auto rows = reader_->numberOfRows();
    if (!rows) {
      return ::paimon::Status::Invalid("ORC row count is unavailable");
    }
    return *rows;
  }

  bool SupportPreciseBitmapSelection() const override {
    return false;
  }

 private:
  memory::MemoryPool* boltPool() const {
    return pool_->getBoltPool();
  }

  ArrowOptions arrowOptions() const {
    ArrowOptions options;
    options.timestampUnit = static_cast<TimestampUnit>(timestampPrecision_);
    // Paimon's row merge expects ordinary arrays, including all-null columns.
    options.flattenConstant = true;
    return options;
  }

  ::paimon::Result<std::unique_ptr<dwio::common::RowReader>> createRowReader(
      const RowTypePtr& type,
      const std::shared_ptr<::paimon::Predicate>& predicate) const {
    // The decoder does not interpret ORC writerTimezone. Guard every read path,
    // including default full-schema reads and predicates on non-output fields.
    if (containsTimestamp(*type) || hasTimestampPredicate(predicate)) {
      return ::paimon::Status::NotImplemented(
          "ORC timestamp reads require writer-timezone support");
    }
    auto scanSpec = std::make_shared<common::ScanSpec>("<root>");
    scanSpec->addAllChildFields(*type);
    // Each exported batch must cover contiguous physical file rows. Row-level
    // filters would compact the batch and invalidate the offsets used by
    // Paimon's bitmap wrapper. Leave exact filtering to Paimon/the datasource.
    dwio::common::RowReaderOptions options;
    options.setScanSpec(scanSpec);
    options.setTimestampPrecision(
        static_cast<TimestampPrecision>(timestampPrecision_));
    options.setUseColumnNamesForColumnMapping(true);
    // ScanSpec controls projected channels.
    // Keep file columns available to the selector even for zero-column output.
    options.select(
        std::make_shared<dwio::common::ColumnSelector>(reader_->rowType()));
    return reader_->createRowReader(options);
  }

  std::unique_ptr<dwio::common::Reader> reader_;
  std::unique_ptr<dwio::common::RowReader> rowReader_;
  const int32_t batchSize_;
  const std::shared_ptr<BoltPaimonMemoryPool> pool_;
  const uint8_t timestampPrecision_;
  RowTypePtr readType_;
  uint64_t previousBatchFirstRowNumber_{0};
  uint64_t previousBatchSize_{0};
};

class PaimonOrcReaderBuilder final : public ::paimon::ReaderBuilder {
 public:
  PaimonOrcReaderBuilder(
      int32_t batchSize,
      PaimonIoOptions ioOptions,
      uint8_t timestampPrecision)
      : batchSize_(batchSize),
        ioOptions_(ioOptions),
        timestampPrecision_(timestampPrecision) {}

  ::paimon::ReaderBuilder* WithMemoryPool(
      const std::shared_ptr<::paimon::MemoryPool>& pool) override {
    pool_ = std::dynamic_pointer_cast<BoltPaimonMemoryPool>(pool);
    return this;
  }

  ::paimon::Result<std::unique_ptr<::paimon::FileBatchReader>> Build(
      const std::shared_ptr<::paimon::InputStream>& stream) const override {
    if (!pool_ || !pool_->getBoltPool()) {
      return ::paimon::Status::Invalid(
          "ORC reader requires a BoltPaimonMemoryPool");
    }
    if (!stream) {
      return ::paimon::Status::Invalid("ORC InputStream must not be null");
    }
    try {
      auto file = std::make_shared<PaimonReadFile>(stream, ioOptions_);
      auto input = std::make_unique<dwio::common::BufferedInput>(
          std::make_shared<dwio::common::ReadFileInputStream>(file),
          *pool_->getBoltPool());
      dwio::common::ReaderOptions options(pool_->getBoltPool());
      options.setFileFormat(dwio::common::FileFormat::ORC);
      options.setUseColumnNamesForColumnMapping(true);
      orc::reader::OrcReaderFactory factory;
      return std::make_unique<PaimonOrcFileBatchReader>(
          factory.createReader(std::move(input), options),
          batchSize_,
          pool_,
          timestampPrecision_);
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(
          std::string("Failed to open ORC InputStream: ") + e.what());
    }
  }

 private:
  const int32_t batchSize_;
  const PaimonIoOptions ioOptions_;
  const uint8_t timestampPrecision_;
  std::shared_ptr<BoltPaimonMemoryPool> pool_;
};

class BoltOrcFileFormatFactory final : public ::paimon::FileFormatFactory {
 public:
  const char* Identifier() const override {
    return "orc";
  }

  ::paimon::Result<std::unique_ptr<::paimon::FileFormat>> Create(
      const std::map<std::string, std::string>& options) const override {
    return std::make_unique<PaimonOrcReader>(options);
  }
};
} // namespace

PaimonOrcReader::PaimonOrcReader(
    const std::map<std::string, std::string>& options)
    : options_(options) {
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
    auto precision = std::stoi(it->second);
    BOLT_USER_CHECK(
        precision == 3 || precision == 6 || precision == 9,
        "Unsupported ORC timestamp precision: {}",
        precision);
    timestampPrecision_ = precision;
  }
}

const std::string& PaimonOrcReader::Identifier() const {
  static const std::string identifier = "orc";
  return identifier;
}

::paimon::Result<std::unique_ptr<::paimon::ReaderBuilder>>
PaimonOrcReader::CreateReaderBuilder(int32_t batchSize) const {
  if (batchSize <= 0) {
    return ::paimon::Status::Invalid("ORC batch size must be positive");
  }
  return std::make_unique<PaimonOrcReaderBuilder>(
      batchSize, ioOptions_, timestampPrecision_);
}

::paimon::Result<std::unique_ptr<::paimon::WriterBuilder>>
PaimonOrcReader::CreateWriterBuilder(::ArrowSchema*, int32_t) const {
  return ::paimon::Status::NotImplemented(
      "ORC writing is not supported by the Bolt adapter");
}

::paimon::Result<std::unique_ptr<::paimon::FormatStatsExtractor>>
PaimonOrcReader::CreateStatsExtractor(::ArrowSchema*) const {
  return ::paimon::Status::NotImplemented(
      "ORC statistics extraction is not supported by the Bolt adapter");
}

void EnsurePaimonOrcFormatRegistered() {
  static std::once_flag flag;
  std::call_once(flag, [] {
    auto* factory = new BoltOrcFileFormatFactory;
    ::paimon::FactoryCreator::GetInstance()->Register(
        factory->Identifier(), factory);
  });
}
} // namespace bytedance::bolt::connector::paimon
