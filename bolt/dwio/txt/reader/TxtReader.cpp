/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/dwio/txt/reader/TxtReader.h"
#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/csv/api.h>
#include <arrow/io/memory.h>
#include "bolt/connectors/Connector.h"
#include "bolt/dwio/common/BufferUtil.h"
#include "bolt/dwio/common/StreamUtil.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::txt::reader {
class TxtRowReader::Impl {
 public:
  Impl(
      const std::shared_ptr<ReaderBase>& readerBase,
      const dwio::common::RowReaderOptions& options)
      : pool_(readerBase->getMemoryPool()),
        readerBase_(readerBase),
        options_(options) {
    startPositionInByte_ = options.getOffset();
    endPositionInByte_ =
        std::min(readerBase_->fileLength(), options.getLimit());
    // initial read size is 2MB
    nextReadSizeInByte_ = 2 * 1024 * 1024;
    rowsRead_ = 0;
  }

  int64_t nextReadSize(uint64_t size) {
    BOLT_CHECK_GT(size, 0);
    if (startPositionInByte_ >= readerBase_->fileLength()) {
      return kAtEnd;
    }
    return size;
  }

  int64_t nextReadByteSize(uint64_t size) {
    BOLT_CHECK_GT(size, 0);
    if (!readerBase_) {
      BOLT_FAIL("reader base not available");
    }

    if (startPositionInByte_ >= readerBase_->fileLength()) {
      return 0;
    }
    return std::min(size, readerBase_->fileLength() - startPositionInByte_);
  }

  std::shared_ptr<arrow::DataType> toArrowType(
      std::shared_ptr<const Type> type) {
    switch (type->kind()) {
      case TypeKind::BOOLEAN:
        return arrow::boolean();
      case TypeKind::TINYINT:
        return arrow::int8();
      case TypeKind::SMALLINT:
        return arrow::int16();
      case TypeKind::INTEGER:
        return arrow::int32();
      case TypeKind::BIGINT:
        return arrow::int64();
      case TypeKind::REAL:
        return arrow::float32();
      case TypeKind::DOUBLE:
        return arrow::float64();
      case TypeKind::VARCHAR:
        return arrow::utf8();
      case TypeKind::VARBINARY:
        return arrow::binary();
      case TypeKind::TIMESTAMP:
        return arrow::timestamp(arrow::TimeUnit::NANO);
      case TypeKind::ARRAY: {
        auto childType = toArrowType(type->childAt(0));
        return arrow::list(std::move(childType));
      }
      case TypeKind::MAP: {
        auto keyType = toArrowType(type->childAt(0));
        auto valueType = toArrowType(type->childAt(1));
        return arrow::map(std::move(keyType), std::move(valueType));
      }
      default:
        BOLT_FAIL(fmt::format(
            "type {} are not supported in txt table", type->toString()));
    }
    BOLT_UNREACHABLE();
  }

  uint64_t next(uint64_t size, bolt::VectorPtr& result) {
    if (size <= 0 || nextReadByteSize(nextReadSizeInByte_) <= 0) {
      return 0;
    }

    auto colNames = getColumnNamesAndTypes();
    auto buffer = AlignedBuffer::allocate<char>(
        nextReadByteSize(nextReadSizeInByte_), &pool_);
    auto totalBytesRead = readDataIntoBuffer(size, buffer);

    if (totalBytesRead == 0) {
      return 0;
    }

    auto [reader, NoSelectedCol] =
        createCSVReader(buffer, totalBytesRead, colNames);
    auto batch = parseCSVData(reader, NoSelectedCol);

    if (!batch) {
      BOLT_FAIL("Unable to parse CSV Data!");
    }

    convertArrowToBolt(batch, result);

    rowsRead_ += batch->num_rows();
    return batch->num_rows();
  }

  std::pair<
      std::vector<std::string>,
      std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>>
  getColumnNamesAndTypes() {
    auto schema = readerBase_->getReaderOptions().getFileSchema();
    std::vector<std::string> colNames(schema->size());
    std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> types;

    for (int i = 0; i < schema->size(); i++) {
      colNames[i] = schema->nameOf(i);
      types[colNames[i]] = toArrowType(schema->childAt(i));
    }

    return {colNames, types};
  }

  void findNextNewline(BufferPtr& buffer) {
    size_t discardBytesRead = 0;
    while (startPositionInByte_ < endPositionInByte_) {
      auto bytesToRead = nextReadByteSize(nextReadSizeInByte_);
      auto stream = readerBase_->bufferedInput().read(
          startPositionInByte_, bytesToRead, dwio::common::LogType::FILE);
      AlignedBuffer::reallocate<char>(&buffer, discardBytesRead + bytesToRead);
      dwio::common::ensureCapacity<char>(
          buffer, discardBytesRead + bytesToRead, &pool_);

      const char* bufferStart = nullptr;
      const char* bufferEnd = nullptr;
      dwio::common::readBytes(
          bytesToRead,
          stream.get(),
          buffer->asMutable<char>() + discardBytesRead,
          bufferStart,
          bufferEnd);

      for (int i = 0; i < bytesToRead; i++) {
        if (buffer->asMutable<char>()[i + discardBytesRead] == '\n') {
          startPositionInByte_ += i + 1;
          return;
        }
      }

      startPositionInByte_ += bytesToRead;
      discardBytesRead += bytesToRead;
    }
  }

  size_t readDataIntoBuffer(uint64_t size, BufferPtr& buffer) {
    size_t rowsInBuffer = 0;
    size_t totalBytesRead = 0;

    // if startPositionInByte_ of the first batch
    // in the split is not 0,
    // find the start of the next whole line first.
    if (rowsRead_ == 0 && startPositionInByte_ != 0) {
      findNextNewline(buffer);
    }
    if (startPositionInByte_ >= endPositionInByte_) {
      return 0;
    }

    while (startPositionInByte_ < readerBase_->fileLength() &&
           rowsInBuffer < size) {
      auto bytesToRead = nextReadByteSize(nextReadSizeInByte_);
      auto stream = readerBase_->bufferedInput().read(
          startPositionInByte_, bytesToRead, dwio::common::LogType::FILE);
      AlignedBuffer::reallocate<char>(&buffer, totalBytesRead + bytesToRead);
      dwio::common::ensureCapacity<char>(
          buffer, totalBytesRead + bytesToRead, &pool_);

      const char* bufferStart = nullptr;
      const char* bufferEnd = nullptr;
      dwio::common::readBytes(
          bytesToRead,
          stream.get(),
          buffer->asMutable<char>() + totalBytesRead,
          bufferStart,
          bufferEnd);

      for (int i = 0; i < bytesToRead; i++) {
        if (buffer->asMutable<char>()[i + totalBytesRead] == '\n') {
          // in case \n is the last char of the current split,
          // we need to go pass the end position and read an additional line.
          ++rowsInBuffer;
          if (rowsInBuffer >= size ||
              startPositionInByte_ + i + 1 >= endPositionInByte_) {
            startPositionInByte_ += i + 1;
            return totalBytesRead + i + 1;
          }
        }
      }

      totalBytesRead += bytesToRead;
      startPositionInByte_ += bytesToRead;
    }

    return totalBytesRead;
  }

  std::pair<std::shared_ptr<arrow::csv::StreamingReader>, bool> createCSVReader(
      BufferPtr& buffer,
      size_t totalBytesRead,
      const std::pair<
          std::vector<std::string>,
          std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>>&
          colData) {
    auto specs = options_.getScanSpec()->stableChildren();
    auto selectedColumns = std::vector<std::string>();
    for (auto& spec : specs) {
      // constant columns are partition keys and are not in the txt files.
      if (!spec->isConstant()) {
        selectedColumns.push_back(spec->fieldName());
      }
    }

    arrow::io::IOContext io_context = arrow::io::default_io_context();
    auto arrowBuffer =
        std::make_shared<arrow::Buffer>(buffer->as<uint8_t>(), totalBytesRead);
    auto input = std::make_shared<arrow::io::BufferReader>(arrowBuffer);

    auto read_options = arrow::csv::ReadOptions::Defaults();
    read_options.column_names = colData.first;
    read_options.block_size = totalBytesRead;
    read_options.skip_rows = options_.getSkipRows();

    auto serdeOptions = readerBase_->getReaderOptions().getSerDeOptions();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = serdeOptions.separators[0];
    parse_options.collection_delimiter = serdeOptions.separators[1];
    parse_options.map_key_delimiter = serdeOptions.separators[2];
    parse_options.escaping = serdeOptions.isEscaped;
    parse_options.escape_char = serdeOptions.escapeChar;

    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    convert_options.column_types = colData.second;
    convert_options.include_columns = selectedColumns;
    convert_options.null_values = {serdeOptions.nullString};
    convert_options.strings_can_be_null = true;

    auto maybe_reader = arrow::csv::StreamingReader::Make(
        io_context, input, read_options, parse_options, convert_options);
    if (!maybe_reader.ok()) {
      BOLT_FAIL(
          "txt reader could not be initialized: {}",
          maybe_reader.status().message());
    }

    return {*maybe_reader, selectedColumns.empty()};
  }

  std::shared_ptr<arrow::RecordBatch> parseCSVData(
      std::shared_ptr<arrow::csv::StreamingReader> reader,
      bool NoSelectedCol) {
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = reader->ReadNext(&batch);

    if (!status.ok()) {
      BOLT_FAIL("Error parsing csv file: {}", status.message());
    }

    int rowCnt = batch->num_rows();
    if (NoSelectedCol) {
      auto emptySchema = arrow::schema({});
      batch = arrow::RecordBatch::Make(
          emptySchema, rowCnt, std::vector<std::shared_ptr<arrow::Array>>());
    }

    // add partition keys and unknown columns
    auto specs = options_.getScanSpec()->stableChildren();
    for (auto& spec : specs) {
      if (spec->isConstant()) {
        ArrowArray arrowArray;
        auto constantVector =
            BaseVector::wrapInConstant(rowCnt, 0, spec->constantValue());
        exportToArrow(
            constantVector, arrowArray, &pool_, {.flattenConstant = true});
        auto importedUnchecked = ImportArray(
            &arrowArray, toArrowType(spec->constantValue()->type()));
        if (importedUnchecked.ok()) {
          auto imported = importedUnchecked.ValueOrDie();
          auto batchUnchecked = batch->AddColumn(
              batch->num_columns(), spec->fieldName(), imported);
          if (batchUnchecked.ok()) {
            batch = batchUnchecked.ValueOrDie();
          } else {
            BOLT_FAIL(
                "Error adding constant column to RecordBatch: {}",
                batchUnchecked.status().ToString());
          }
        } else {
          BOLT_FAIL(
              "Error importing constant column to Arrow: {}",
              importedUnchecked.status().ToString());
        }
      }
    }

    return batch;
  }

  void convertArrowToBolt(
      const std::shared_ptr<arrow::RecordBatch>& batch,
      bolt::VectorPtr& result) {
    ArrowSchema schema;
    ArrowArray array;

    auto status = arrow::ExportRecordBatch(*batch, &array, &schema);

    if (!status.ok()) {
      BOLT_FAIL("Error exporting RecordBatch: {}", status.ToString());
    }

    result = importFromArrowAsOwner(schema, array, {}, &pool_);
  }

  int64_t nextRowNumber() {
    return rowsRead_;
  }

 private:
  memory::MemoryPool& pool_;
  const std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::RowReaderOptions options_;
  uint64_t startPositionInByte_;
  uint64_t endPositionInByte_;
  uint64_t nextReadSizeInByte_;
  int64_t rowsRead_;
};

std::unique_ptr<dwio::common::Reader> TxtReaderFactory::createReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options) {
  return std::make_unique<TxtReader>(std::move(input), options);
}

TxtReader::TxtReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options)
    : readerBase_(std::make_shared<ReaderBase>(std::move(input), options)) {}

std::optional<uint64_t> TxtReader::numberOfRows() const {
  return readerBase_->fileLength() > 0 ? 1 : 0;
}

const bolt::RowTypePtr& TxtReader::rowType() const {
  return readerBase_->getReaderOptions().getFileSchema();
}

const std::shared_ptr<const dwio::common::TypeWithId>& TxtReader::typeWithId()
    const {
  BOLT_NYI("typeWithId is not implemented for txt files!");
}

std::unique_ptr<dwio::common::RowReader> TxtReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<TxtRowReader>(readerBase_, options);
}

ReaderBase::ReaderBase(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options)
    : pool_(options.getMemoryPool()),
      options_(options),
      input_(std::move(input)) {
  fileLength_ = input_->getReadFile()->size();
}

TxtRowReader::TxtRowReader(
    const std::shared_ptr<ReaderBase>& readerBase,
    const dwio::common::RowReaderOptions& options) {
  impl_ = std::make_unique<TxtRowReader::Impl>(readerBase, options);
}

TxtRowReader::~TxtRowReader() = default;

int64_t TxtRowReader::nextRowNumber() {
  return impl_->nextRowNumber();
}

int64_t TxtRowReader::nextReadSize(uint64_t size) {
  return impl_->nextReadSize(size);
}

uint64_t TxtRowReader::next(
    uint64_t size,
    VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  return impl_->next(size, result);
}

uint64_t TxtRowReader::skip(uint64_t skipSize) {
  LOG(INFO) << "txt reader currently does not support skip()";
  return 0;
}

void TxtRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  LOG(INFO) << "txt reader currently does not support updateRuntimeStats()";
}

void TxtRowReader::resetFilterCaches() {
  LOG(INFO) << "txt reader currently does not support resetFilterCaches()";
}

std::optional<size_t> TxtRowReader::estimatedRowSize() const {
  return bytedance::bolt::connector::DataSource::kUnknownRowSize;
}
} // namespace bytedance::bolt::txt::reader
