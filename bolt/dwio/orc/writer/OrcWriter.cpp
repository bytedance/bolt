/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include "bolt/dwio/orc/writer/OrcWriter.h"
#include <arrow/adapters/orc/options.h>
#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/io/interfaces.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/compression.h>
#include "bolt/dwio/common/DataBuffer.h"
#include "bolt/exec/MemoryReclaimer.h"
#include "bolt/vector/arrow/Bridge.h"

// Private modified arrow details.
#include "arrow/adapters/orc/adapter.h"
#include "arrow/util/type_fwd.h"

#ifndef ARROW_THROW_NOT_OK
#define ARROW_THROW_NOT_OK(status)             \
  do {                                         \
    ::arrow::Status _s = (status);             \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {       \
      throw std::runtime_error(_s.ToString()); \
    }                                          \
  } while (false)
#endif

#define ARROW_ASSIGN_OR_THROW(lhs, rhs)   \
  auto _result = (rhs);                   \
  if (!_result.ok()) {                    \
    ARROW_THROW_NOT_OK(_result.status()); \
  }                                       \
  lhs = std::move(_result).ValueOrDie();
namespace bytedance::bolt::orc::writer {
using ArrowOrcWriteOption = arrow::adapters::orc::WriteOptions;
using arrow::Compression;
using arrow::adapters::orc::ORCFileWriter;

// Utility for buffering Arrow output with a DataBuffer.
class ArrowDataBufferSink : public ::arrow::io::OutputStream {
 public:
  /// @param growRatio Growth factor used when invoking the reserve() method of
  /// DataSink, thereby helping to minimize frequent memcpy operations.
  ArrowDataBufferSink(
      std::unique_ptr<dwio::common::FileSink> sink,
      memory::MemoryPool& pool,
      double growRatio)
      : sink_(std::move(sink)), growRatio_(growRatio), buffer_(pool) {}

  ::arrow::Status Write(const std::shared_ptr<::arrow::Buffer>& data) override {
    auto requestCapacity = buffer_.size() + data->size();
    if (requestCapacity > buffer_.capacity()) {
      // LOG(INFO) << "request capacity: " << requestCapacity
      //           << ", buffer.capacity: " << buffer_.capacity();
      buffer_.reserve(growRatio_ * (requestCapacity));
    }
    buffer_.append(
        buffer_.size(),
        reinterpret_cast<const char*>(data->data()),
        data->size());
    return ::arrow::Status::OK();
  }

  ::arrow::Status Write(const void* data, int64_t nbytes) override {
    auto requestCapacity = buffer_.size() + nbytes;
    if (requestCapacity > buffer_.capacity()) {
      // LOG(INFO) << "request capacity: " << requestCapacity
      //           << ", buffer.capacity: " << buffer_.capacity();
      buffer_.reserve(growRatio_ * (requestCapacity));
    }
    buffer_.append(buffer_.size(), reinterpret_cast<const char*>(data), nbytes);
    return ::arrow::Status::OK();
  }

  ::arrow::Status Flush() override {
    if (buffer_.size() > 0) {
      bytesFlushed_ += buffer_.size();
      sink_->write(std::move(buffer_));
    }
    return ::arrow::Status::OK();
  }

  ::arrow::Result<int64_t> Tell() const override {
    return bytesFlushed_ + buffer_.size();
  }

  ::arrow::Status Close() override {
    ARROW_RETURN_NOT_OK(Flush());
    sink_->close();
    return ::arrow::Status::OK();
  }

  dwio::common::DataBuffer<char>& dataBuffer() {
    return buffer_;
  }

  bool closed() const override {
    return sink_->isClosed();
  }

  void abort() {
    sink_.reset();
    buffer_.clear();
  }

 private:
  std::unique_ptr<dwio::common::FileSink> sink_;
  const double growRatio_;
  dwio::common::DataBuffer<char> buffer_;
  int64_t bytesFlushed_ = 0;
};

struct ArrowOrcContext {
  std::unique_ptr<ORCFileWriter> writer;
  std::shared_ptr<::arrow::Schema> schema;
  std::shared_ptr<ArrowOrcWriteOption> arrowWriteOptions;
  std::vector<std::vector<std::shared_ptr<::arrow::Array>>> stagingChunks;
  uint64_t stagingRows = 0;
  int64_t stagingBytes = 0;
};

namespace {

Compression::type getArrowOrcCompression(common::CompressionKind compression) {
  if (compression == common::CompressionKind_SNAPPY) {
    return Compression::SNAPPY;
  } else if (
      compression == common::CompressionKind_GZIP ||
      compression == common::CompressionKind_ZLIB) {
    // Gzip and zlib compression are similar, and zlib is used in orc only.
    // Input gzip or zlib, and zlib will actually be used in orc.
    // ref: https://orc.apache.org/specification/ORCv1/
    return Compression::GZIP;
  } else if (compression == common::CompressionKind_ZSTD) {
    return Compression::ZSTD;
  } else if (compression == common::CompressionKind_NONE) {
    return Compression::UNCOMPRESSED;
  } else if (compression == common::CompressionKind_LZ4) {
    return Compression::LZ4;
  } else {
    BOLT_FAIL("Unsupported compression {}", compression);
  }
}

constexpr int64_t DEFAULT_PARQUET_BLOCK_SIZE = 128 * 1024 * 1024; // 128M

} // namespace

void validateSchemaRecursive(const RowTypePtr& schema) {
  // Check the schema's field names is not empty and unique.
  BOLT_USER_CHECK_NOT_NULL(schema, "Field schema must not be empty.");
  const auto& fieldNames = schema->names();

  folly::F14FastSet<std::string> uniqueNames;
  for (const auto& name : fieldNames) {
    BOLT_USER_CHECK(!name.empty(), "Field name must not be empty.")
    auto result = uniqueNames.insert(name);
    BOLT_USER_CHECK(
        result.second,
        "File schema should not have duplicate columns: {}",
        name);
  }

  for (auto i = 0; i < schema->size(); ++i) {
    if (auto childSchema =
            std::dynamic_pointer_cast<const RowType>(schema->childAt(i))) {
      validateSchemaRecursive(childSchema);
    }
  }
}

std::shared_ptr<ArrowOrcWriteOption> getArrowOrcWriteOptions(
    const BoltOrcWriteOption& options) {
  auto arrowOption = std::make_shared<ArrowOrcWriteOption>();

  if (!options.enableDictionary) {
    // Use 0 to disable dictionary encoding.
    arrowOption->dictionary_key_size_threshold = 0;
  }
  arrowOption->compression = getArrowOrcCompression(options.compression);
  arrowOption->stripe_size = options.dataPageSize;
  return arrowOption;
}

std::shared_ptr<::arrow::Field> updateFieldNameRecursive(
    const std::shared_ptr<::arrow::Field>& field,
    const Type& type,
    const std::string& name = "") {
  if (type.isRow()) {
    auto rowType = type.asRow();
    auto newField = field->WithName(name);
    auto structType =
        std::dynamic_pointer_cast<::arrow::StructType>(newField->type());
    auto childrenSize = rowType.size();
    std::vector<std::shared_ptr<::arrow::Field>> newFields;
    newFields.reserve(childrenSize);
    for (auto i = 0; i < childrenSize; i++) {
      newFields.push_back(updateFieldNameRecursive(
          structType->fields()[i], *rowType.childAt(i), rowType.nameOf(i)));
    }
    return newField->WithType(::arrow::struct_(newFields));
  } else if (type.isArray()) {
    auto newField = field->WithName(name);
    auto listType =
        std::dynamic_pointer_cast<::arrow::BaseListType>(newField->type());
    auto elementType = type.asArray().elementType();
    auto elementField = listType->value_field();
    return newField->WithType(
        ::arrow::list(updateFieldNameRecursive(elementField, *elementType)));
  } else if (type.isMap()) {
    auto mapType = type.asMap();
    auto newField = field->WithName(name);
    auto arrowMapType =
        std::dynamic_pointer_cast<::arrow::MapType>(newField->type());
    auto newKeyField =
        updateFieldNameRecursive(arrowMapType->key_field(), *mapType.keyType());
    auto newValueField = updateFieldNameRecursive(
        arrowMapType->item_field(), *mapType.valueType());
    return newField->WithType(
        ::arrow::map(newKeyField->type(), newValueField->type()));
  } else if (name != "") {
    return field->WithName(name);
  } else {
    return field;
  }
}

ArrowOrcWriter::ArrowOrcWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    const BoltOrcWriteOption& options,
    std::shared_ptr<memory::MemoryPool> pool,
    arrow::MemoryPool* arrowPool,
    RowTypePtr schema)
    : pool_(std::move(pool)),
      generalPool_{pool_->addLeafChild(".general")},
      arrowPool_(arrowPool),
      stream_(std::make_shared<ArrowDataBufferSink>(
          std::move(sink),
          *generalPool_,
          options.bufferGrowRatio)),
      arrowContext_(std::make_shared<ArrowOrcContext>()),
      schema_(std::move(schema)),
      bufferReserveRatio_(options.bufferReserveRatio),
      enableRowGroupAlignedWrite_(options.enableRowGroupAlignedWrite),
      expectedRowsInEachBlock_(options.expectedRowsInEachBlock),
      enableFlushBasedOnBlockSize_(options.enableFlushBasedOnBlockSize) {
  validateSchemaRecursive(schema_);
  flushPolicy_ = std::make_unique<DefaultFlushPolicy>();
  options_.timestampUnit =
      options.orcWriteTimestampUnit.value_or(TimestampUnit::kNano);

  // Don't change following line, unless we port the whole implementation
  // of arrow/adapters/orc to our codebase. The commiter hard coded the timezone
  // as UTC when invoking liborc.
  options_.timestampTimeZone = "UTC";
  arrowContext_->arrowWriteOptions = getArrowOrcWriteOptions(options);
  setMemoryReclaimers();
}

ArrowOrcWriter::ArrowOrcWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    const BoltOrcWriteOption& options,
    RowTypePtr schema)
    : ArrowOrcWriter{
          std::move(sink),
          options,
          options.memoryPool->addAggregateChild(fmt::format(
              "writer_node_{}",
              folly::to<std::string>(folly::Random::rand64()))),
          nullptr,
          std::move(schema)} {}

void ArrowOrcWriter::flush() {
  if (enableRowGroupAlignedWrite_) {
    BOLT_CHECK(
        expectedRowsInEachBlock_.size() > 0,
        "expectedRowsInEachBlock_ should be set when row group aligned write enabled!")
    int64_t rowCount = 0;
    if (currentRowGroup_ < expectedRowsInEachBlock_.size()) {
      rowCount = expectedRowsInEachBlock_[currentRowGroup_];
    }
    flush(rowCount);
  } else {
    flush(flushPolicy_->rowsInRowGroup());
  }
}

void ArrowOrcWriter::flush(int64_t rowsInCurrentRowGroup) {
  if (arrowContext_->stagingRows > 0) {
    if (!arrowContext_->writer) {
      ARROW_ASSIGN_OR_THROW(
          arrowContext_->writer,
          ORCFileWriter::Open(
              this->stream_.get(), *arrowContext_->arrowWriteOptions));
    }

    auto fields = arrowContext_->schema->fields();

    if (bufferReserveRatio_ > 0) {
      LOG(INFO) << "current row group: " << currentRowGroup_
                << ", stagingBytes :" << arrowContext_->stagingBytes
                << ", column num: " << fields.size() << ", reserve byte: "
                << bufferReserveRatio_ * arrowContext_->stagingBytes /
              fields.size();
      stream_->dataBuffer().reserve(
          bufferReserveRatio_ * arrowContext_->stagingBytes / fields.size());
    }

    std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunks;
    for (int colIdx = 0; colIdx < fields.size(); colIdx++) {
      auto dataType = fields.at(colIdx)->type();
      auto chunk =
          ::arrow::ChunkedArray::Make(
              std::move(arrowContext_->stagingChunks.at(colIdx)), dataType)
              .ValueOrDie();
      chunks.push_back(chunk);
    }
    auto table = ::arrow::Table::Make(
        arrowContext_->schema,
        std::move(chunks),
        static_cast<int64_t>(arrowContext_->stagingRows));
    ARROW_THROW_NOT_OK(arrowContext_->writer->Write(*table));
    ARROW_THROW_NOT_OK(stream_->Flush());
    for (auto& chunk : arrowContext_->stagingChunks) {
      chunk.clear();
    }
    arrowContext_->stagingRows = 0;
    arrowContext_->stagingBytes = 0;
  }
}

void ArrowOrcWriter::writeRecordBatch(
    std::shared_ptr<::arrow::RecordBatch>& recordBatch) {
  if (recordBatch->num_rows() <= 0) {
    return;
  }
  if (!arrowContext_->writer) {
    BOLT_CHECK_NOT_NULL(arrowPool_);
    ARROW_ASSIGN_OR_THROW(
        arrowContext_->writer,
        ORCFileWriter::Open(
            this->stream_.get(), *arrowContext_->arrowWriteOptions));
  }
  ARROW_THROW_NOT_OK(arrowContext_->writer->Write(*recordBatch));
  ARROW_THROW_NOT_OK(stream_->Flush());
}

dwio::common::StripeProgress getStripeProgress(
    uint64_t stagingRows,
    int64_t stagingBytes) {
  return dwio::common::StripeProgress{
      .stripeRowCount = stagingRows, .stripeSizeEstimate = stagingBytes};
}

/**
 * This method would cache input `ColumnarBatch` to make the size of row group
 * big. It would flush when:
 * - the cached numRows bigger than `rowsInRowGroup_`
 * - the cached bytes bigger than `bytesInRowGroup_`
 *
 * This method assumes each input `ColumnarBatch` have same schema.
 */
void ArrowOrcWriter::write(const VectorPtr& data) {
  BOLT_USER_CHECK(
      data->type()->equivalent(*schema_),
      "The file schema type should be equal with the input rowvector type.");

  ArrowArray array;
  ArrowSchema schema;
  exportToArrow(data, array, generalPool_.get(), options_);
  exportToArrow(data, schema, options_);

  // Convert the arrow schema to Schema and then update the column names based
  // on schema_.
  auto arrowSchema = ::arrow::ImportSchema(&schema).ValueOrDie();
  std::vector<std::shared_ptr<::arrow::Field>> newFields;
  auto childSize = schema_->size();
  for (auto i = 0; i < childSize; i++) {
    newFields.push_back(updateFieldNameRecursive(
        arrowSchema->fields()[i], *schema_->childAt(i), schema_->nameOf(i)));
  }

  ARROW_ASSIGN_OR_THROW(
      auto recordBatch,
      ::arrow::ImportRecordBatch(&array, ::arrow::schema(newFields)));
  if (!arrowContext_->schema) {
    arrowContext_->schema = recordBatch->schema();
    for (int colIdx = 0; colIdx < arrowContext_->schema->num_fields();
         colIdx++) {
      arrowContext_->stagingChunks.push_back(
          std::vector<std::shared_ptr<::arrow::Array>>());
    }
  }

  auto bytes = data->estimateFlatSize();
  auto numRows = data->size();
  if (enableRowGroupAlignedWrite_) {
    rowGroupAlignedFlush(numRows, bytes, recordBatch);
    return;
  } else if (enableFlushBasedOnBlockSize_) {
    writeRecordBatch(recordBatch);
    return;
  } else {
    if (flushPolicy_->shouldFlush(getStripeProgress(
            arrowContext_->stagingRows, arrowContext_->stagingBytes))) {
      flush();
    }
  }

  for (int colIdx = 0; colIdx < recordBatch->num_columns(); colIdx++) {
    arrowContext_->stagingChunks.at(colIdx).push_back(
        recordBatch->column(colIdx));
  }
  arrowContext_->stagingRows += numRows;
  arrowContext_->stagingBytes += bytes;
}

void ArrowOrcWriter::write(const VectorPtr& data, int64_t memLimit) {
  flushPolicy_->setMemLimit(memLimit);
  write(data);
}

bool ArrowOrcWriter::isCodecAvailable(common::CompressionKind compression) {
  return arrow::util::Codec::IsAvailable(getArrowOrcCompression(compression));
}

void ArrowOrcWriter::close() {
  flush();

  if (arrowContext_->writer) {
    ARROW_THROW_NOT_OK(arrowContext_->writer->Close());
    arrowContext_->writer.reset();
  }
  ARROW_THROW_NOT_OK(stream_->Close());

  arrowContext_->stagingChunks.clear();
}

void ArrowOrcWriter::abort() {
  stream_->abort();
  arrowContext_.reset();
}

BoltOrcWriteOption getOrcOptions(const dwio::common::WriterOptions& options) {
  BoltOrcWriteOption orcOptions;
  orcOptions.memoryPool = options.memoryPool;
  if (options.compressionKind.has_value()) {
    orcOptions.compression = options.compressionKind.value();
  }
  if (options.arrowBridgeTimestampUnit.has_value()) {
    orcOptions.orcWriteTimestampUnit =
        static_cast<TimestampUnit>(options.arrowBridgeTimestampUnit.value());
  }
  return orcOptions;
}

void ArrowOrcWriter::setMemoryReclaimers() {
  BOLT_CHECK(
      !pool_->isLeaf(),
      "The root memory pool for parquet writer can't be leaf: {}",
      pool_->name());
  BOLT_CHECK_NULL(pool_->reclaimer());

  if ((pool_->parent() == nullptr) ||
      (pool_->parent()->reclaimer() == nullptr)) {
    return;
  }

  pool_->setReclaimer(exec::MemoryReclaimer::create());
  generalPool_->setReclaimer(exec::MemoryReclaimer::create());
}

std::unique_ptr<dwio::common::Writer> OrcWriterFactory::createWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    const dwio::common::WriterOptions& options) {
  auto orcOptions = getOrcOptions(options);
  return std::make_unique<ArrowOrcWriter>(
      std::move(sink), orcOptions, asRowType(options.schema));
}

void ArrowOrcWriter::rowGroupAlignedFlush(
    int32_t numRows,
    uint64_t bytes,
    std::shared_ptr<::arrow::RecordBatch>& recordBatch) {
  BOLT_CHECK(
      expectedRowsInEachBlock_.size() > 0,
      "expectedRowsInEachBlock_ should be set when row group aligned write enabled.")
  BOLT_CHECK(
      currentRowGroup_ <= expectedRowsInEachBlock_.size(),
      "currentRowGroup_ should be less than the size of expectedRowsInEachBlock_.");
  BOLT_CHECK(
      arrowContext_->stagingRows < expectedRowsInEachBlock_[currentRowGroup_],
      "arrowContext_->stagingRows should be less than expected rows in current row group.");
  int32_t totalRows = numRows;
  int gapRow = 0;
  int sliceStart = 0;
  while (numRows > 0 &&
         (gapRow = expectedRowsInEachBlock_[currentRowGroup_] -
              arrowContext_->stagingRows) <= numRows) {
    LOG(INFO) << "current row group: " << currentRowGroup_
              << ", expect row: " << expectedRowsInEachBlock_[currentRowGroup_]
              << ", staging row: " << arrowContext_->stagingRows
              << ", gap row: " << gapRow << ", input row: " << numRows;
    for (int colIdx = 0; colIdx < recordBatch->num_columns(); colIdx++) {
      auto array = recordBatch->column(colIdx);
      if (sliceStart == 0 && gapRow == numRows) {
        arrowContext_->stagingChunks.at(colIdx).push_back(array);
      } else {
        auto sliceArray = array->Slice(sliceStart, gapRow);
        arrowContext_->stagingChunks.at(colIdx).push_back(sliceArray);
      }
    }
    arrowContext_->stagingRows += numRows;
    arrowContext_->stagingBytes += bytes * (double)numRows / totalRows;

    flush();

    ++currentRowGroup_;
    numRows -= gapRow;
    sliceStart += gapRow;
  }

  if (numRows > 0) {
    for (int colIdx = 0; colIdx < recordBatch->num_columns(); colIdx++) {
      auto array = recordBatch->column(colIdx);
      if (sliceStart == 0 && gapRow == numRows) {
        arrowContext_->stagingChunks.at(colIdx).push_back(array);
      } else {
        auto sliceArray = array->Slice(sliceStart, gapRow);
        arrowContext_->stagingChunks.at(colIdx).push_back(sliceArray);
      }
    }
    arrowContext_->stagingRows += numRows;
    arrowContext_->stagingBytes += bytes * (double)numRows / totalRows;
  }
}
} // namespace bytedance::bolt::orc::writer
