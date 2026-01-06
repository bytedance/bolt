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

#include "TxtWriter.h"
#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/csv/api.h>
#include "bolt/dwio/parquet/arrow/Exception.h"
#include "bolt/exec/MemoryReclaimer.h"

namespace bytedance::bolt::txt::writer {
class BoltTxtWriteOption;
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

  bytedance::bolt::dwio::common::DataBuffer<char>& dataBuffer() {
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
  std::unique_ptr<bytedance::bolt::dwio::common::FileSink> sink_;
  const double growRatio_;
  bytedance::bolt::dwio::common::DataBuffer<char> buffer_;
  int64_t bytesFlushed_ = 0;
};

struct ArrowTxtContext {
  std::shared_ptr<::arrow::Schema> schema;
  ::arrow::csv::WriteOptions arrowWriteOptions;
  std::vector<std::vector<std::shared_ptr<::arrow::Array>>> stagingChunks;
  uint64_t stagingRows = 0;
  int64_t stagingBytes = 0;
};

inline uint8_t parseDelimiter(const std::string& delim) {
  for (char const& ch : delim) {
    if (!std::isdigit(ch)) {
      return delim[0];
    }
  }
  return stoi(delim);
}

std::unique_ptr<dwio::common::SerDeOptions> parseSerdeParameters(
    const std::map<std::string, std::string>& serdeParameters) {
  auto fieldIt = serdeParameters.find(dwio::common::SerDeOptions::kFieldDelim);
  if (fieldIt == serdeParameters.end()) {
    fieldIt = serdeParameters.find("serialization.format");
  }
  auto collectionIt =
      serdeParameters.find(dwio::common::SerDeOptions::kCollectionDelim);
  if (collectionIt == serdeParameters.end()) {
    // For collection delimiter, Hive 1.x, 2.x uses "colelction.delim", but
    // Hive 3.x uses "collection.delim".
    // See: https://issues.apache.org/jira/browse/HIVE-16922)
    collectionIt = serdeParameters.find("colelction.delim");
  }
  auto mapKeyIt =
      serdeParameters.find(dwio::common::SerDeOptions::kMapKeyDelim);

  uint8_t fieldDelim = '\1';
  uint8_t collectionDelim = '\2';
  uint8_t mapKeyDelim = '\3';
  if (fieldIt != serdeParameters.end()) {
    fieldDelim = parseDelimiter(fieldIt->second);
  }
  if (collectionIt != serdeParameters.end()) {
    collectionDelim = parseDelimiter(collectionIt->second);
  }
  if (mapKeyIt != serdeParameters.end()) {
    mapKeyDelim = parseDelimiter(mapKeyIt->second);
  }
  auto serDeOptions = std::make_unique<dwio::common::SerDeOptions>(
      fieldDelim, collectionDelim, mapKeyDelim);
  return serDeOptions;
}

BoltTxtWriteOption parseSerdeOptions(
    const dwio::common::WriterOptions& options) {
  BoltTxtWriteOption txtOptions;

  txtOptions.memoryPool = options.memoryPool;
  txtOptions.arrowWriteOptions = ::arrow::csv::WriteOptions::Defaults();

  auto serdeOptions = parseSerdeParameters(options.serdeParameters);
  txtOptions.arrowWriteOptions.include_header = false;
  txtOptions.arrowWriteOptions.delimiter = serdeOptions->separators[0];
  txtOptions.arrowWriteOptions.null_string = serdeOptions->nullString;
  return txtOptions;
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

dwio::common::StripeProgress getStripeProgress(
    uint64_t stagingRows,
    int64_t stagingBytes) {
  return dwio::common::StripeProgress{
      .stripeRowCount = stagingRows, .stripeSizeEstimate = stagingBytes};
}

std::unique_ptr<dwio::common::Writer> TxtWriterFactory::createWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    const dwio::common::WriterOptions& options) {
  auto txtOptions = parseSerdeOptions(options);
  return std::make_unique<TxtWriter>(
      std::move(sink), txtOptions, asRowType(options.schema));
}

TxtWriter::TxtWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    BoltTxtWriteOption& options,
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
      arrowContext_(std::make_shared<ArrowTxtContext>()),
      enableFlushBasedOnBlockSize_(options.enableFlushBasedOnBlockSize),
      schema_(std::move(schema)),
      bufferReserveRatio_(options.bufferReserveRatio) {
  flushPolicy_ = std::make_unique<DefaultFlushPolicy>();
  exportOptions_.timestampUnit =
      options.txtWriteTimestampUnit.value_or(TimestampUnit::kNano);

  arrowContext_->arrowWriteOptions = std::move(options.arrowWriteOptions);
  arrowContext_->arrowWriteOptions.include_header = false;

  setMemoryReclaimers();
}

TxtWriter::TxtWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    BoltTxtWriteOption& options,
    RowTypePtr schema)
    : TxtWriter{
          std::move(sink),
          options,
          options.memoryPool->addAggregateChild(fmt::format(
              "writer_node_{}",
              folly::to<std::string>(folly::Random::rand64()))),
          nullptr,
          std::move(schema)} {}

void TxtWriter::write(const VectorPtr& data) {
  BOLT_USER_CHECK(
      data->type()->equivalent(*schema_),
      "The file schema type should be equal with the input rowvector type.");

  ArrowArray array;
  ArrowSchema schema;
  exportToArrow(data, array, generalPool_.get(), exportOptions_);
  exportToArrow(data, schema, exportOptions_);

  // Convert the arrow schema to Schema and then update the column names based
  // on schema_.
  auto arrowSchema = ::arrow::ImportSchema(&schema).ValueOrDie();
  std::vector<std::shared_ptr<::arrow::Field>> newFields;
  auto childSize = schema_->size();
  for (auto i = 0; i < childSize; i++) {
    newFields.push_back(updateFieldNameRecursive(
        arrowSchema->fields()[i], *schema_->childAt(i), schema_->nameOf(i)));
  }

  PARQUET_ASSIGN_OR_THROW(
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

  if (enableFlushBasedOnBlockSize_) {
    writeRecordBatch(recordBatch);
    return;
  } else if (flushPolicy_->shouldFlush(getStripeProgress(
                 arrowContext_->stagingRows, arrowContext_->stagingBytes))) {
    flush();
  }

  for (int colIdx = 0; colIdx < recordBatch->num_columns(); colIdx++) {
    arrowContext_->stagingChunks.at(colIdx).push_back(
        recordBatch->column(colIdx));
  }
  arrowContext_->stagingRows += numRows;
  arrowContext_->stagingBytes += bytes;
}

void TxtWriter::write(const VectorPtr& data, int64_t memLimit) {
  flushPolicy_->setMemLimit(memLimit);
  write(data);
}

void TxtWriter::flush() {
  if (arrowContext_->stagingRows > 0) {
    auto fields = arrowContext_->schema->fields();

    if (bufferReserveRatio_ > 0) {
      LOG(INFO) << ", stagingBytes :" << arrowContext_->stagingBytes
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
    PARQUET_THROW_NOT_OK(::arrow::csv::WriteCSV(
        *table, arrowContext_->arrowWriteOptions, stream_.get()));
    PARQUET_THROW_NOT_OK(stream_->Flush());
    for (auto& chunk : arrowContext_->stagingChunks) {
      chunk.clear();
    }
    arrowContext_->stagingRows = 0;
    arrowContext_->stagingBytes = 0;
  }
}

void TxtWriter::close() {
  flush();
  PARQUET_THROW_NOT_OK(stream_->Close());
  arrowContext_->stagingChunks.clear();
}

void TxtWriter::abort() {
  stream_->abort();
  arrowContext_.reset();
}

void TxtWriter::writeRecordBatch(
    std::shared_ptr<::arrow::RecordBatch>& recordBatch) {
  if (recordBatch->num_rows() <= 0) {
    return;
  }
  PARQUET_THROW_NOT_OK(::arrow::csv::WriteCSV(
      *(recordBatch.get()), arrowContext_->arrowWriteOptions, stream_.get()));
  PARQUET_THROW_NOT_OK(stream_->Flush());
}

void TxtWriter::setMemoryReclaimers() {
  BOLT_CHECK(
      !pool_->isLeaf(),
      "The root memory pool for txt writer can't be leaf: {}",
      pool_->name());
  BOLT_CHECK_NULL(pool_->reclaimer());

  if ((pool_->parent() == nullptr) ||
      (pool_->parent()->reclaimer() == nullptr)) {
    return;
  }

  // TODO https://github.com/bytedanceincubator/bolt/issues/8190
  pool_->setReclaimer(exec::MemoryReclaimer::create());
  generalPool_->setReclaimer(bytedance::bolt::exec::MemoryReclaimer::create());
}
} // namespace bytedance::bolt::txt::writer