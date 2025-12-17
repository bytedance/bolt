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

#include "bolt/dwio/parquet/writer/Writer.h"
#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/io/interfaces.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/util/thread_pool.h>
#include <dwio/parquet/writer/arrow/Types.h>
#include <parquet/arrow/writer.h> // @manual
#include <parquet/properties.h>
#include "bolt/common/config/Config.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/dwio/parquet/writer/ArrowDataBufferSink.h"
#include "bolt/dwio/parquet/writer/arrow/Properties.h"
#include "bolt/dwio/parquet/writer/arrow/Writer.h"
#include "bolt/exec/MemoryReclaimer.h"
#include "bolt/vector/arrow/Bridge.h"

#include <iostream>
namespace bytedance::bolt::parquet {

using bytedance::bolt::parquet::arrow::ArrowWriterProperties;
using bytedance::bolt::parquet::arrow::ColumnEncryptionProperties;
using bytedance::bolt::parquet::arrow::Compression;
using bytedance::bolt::parquet::arrow::FileEncryptionProperties;
using bytedance::bolt::parquet::arrow::ParquetCipher;
using bytedance::bolt::parquet::arrow::WriterProperties;
using bytedance::bolt::parquet::arrow::arrow::FileWriter;

struct ArrowContext {
  std::unique_ptr<FileWriter> writer;
  std::shared_ptr<::arrow::Schema> schema;
  std::shared_ptr<WriterProperties> properties;
  std::shared_ptr<ArrowWriterProperties> arrowWriterProperties;
  uint64_t stagingRows = 0;
  int64_t stagingBytes = 0;
  // columns, Arrays
  std::vector<std::vector<std::shared_ptr<::arrow::Array>>> stagingChunks;
};

Compression::type getArrowParquetCompression(
    common::CompressionKind compression) {
  if (compression == common::CompressionKind_SNAPPY) {
    return Compression::SNAPPY;
  } else if (compression == common::CompressionKind_GZIP) {
    return Compression::GZIP;
  } else if (compression == common::CompressionKind_ZSTD) {
    return Compression::ZSTD;
  } else if (compression == common::CompressionKind_NONE) {
    return Compression::UNCOMPRESSED;
  } else if (compression == common::CompressionKind_LZ4) {
    return Compression::LZ4_HADOOP;
  } else {
    BOLT_FAIL("Unsupported compression {}", compression);
  }
}

bool startsWith(const std::string& str, const std::string& prefix) {
  return str.size() >= prefix.size() && str.substr(0, prefix.size()) == prefix;
}

std::shared_ptr<FileEncryptionProperties> getEncryptionProperties(
    std::unordered_map<std::string, std::string> options) {
  if (options.find("footer_key") == options.end()) {
    return nullptr;
  }
  std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>>
      columns_info;
  const std::string column_encrypted_prefix = "column_encrypted";
  const std::string column_encrypted_footer_prefix = "column_footer";
  auto it = options.begin();
  while (it != options.end()) {
    auto key = it->first;
    auto value = it->second;
    if (startsWith(key, column_encrypted_prefix)) {
      std::string path = key.substr(column_encrypted_prefix.size());
      ColumnEncryptionProperties::Builder column_encrypted(path);
      if (options.find(column_encrypted_footer_prefix + path) !=
          options.end()) {
        auto column_key = options["column_key" + path];
        auto column_meta = options["column_meta" + path];
        if (column_meta.empty() || column_meta.empty()) {
          LOG(ERROR) << "failed to find encrypt info for " << path;
          throw "failed to find encrypt info";
        }
        column_encrypted.key(column_key);
        column_encrypted.key_metadata(column_meta);
        columns_info[path] = column_encrypted.build();
      } else {
        LOG(ERROR) << "failed to find encrypt info for " << path;
        throw "failed to find encrypt info";
      }
    }
    it++;
  }
  FileEncryptionProperties::Builder file_encryption_builder(
      options["footer_key"]);
  if (options["algorithm"] == "AES_GCM_V1") {
    file_encryption_builder.algorithm(ParquetCipher::AES_GCM_V1);
  } else if (options["algorithm"] == "AES_GCM_CTR_V1") {
    file_encryption_builder.algorithm(ParquetCipher::AES_GCM_CTR_V1);
  } else {
    LOG(ERROR) << "Encryption algorithm currently not supported! "
               << options["algorithm"];
    throw "Not support algorithm";
  }
  file_encryption_builder.footer_key_metadata(options["footer_key_metadata"]);
  file_encryption_builder.encrypted_columns((columns_info));
  return file_encryption_builder.build();
}

namespace {

constexpr int64_t DEFAULT_PARQUET_BLOCK_SIZE = 128 * 1024 * 1024; // 128M

std::shared_ptr<WriterProperties::Builder> getArrowParquetWriterOptionsBuilder(
    const parquet::WriterOptions& options,
    const std::unique_ptr<DefaultFlushPolicy>& flushPolicy) {
  auto builder = std::make_shared<WriterProperties::Builder>();
  WriterProperties::Builder* properties = builder.get();
  if (!options.enableDictionary) {
    properties = properties->disable_dictionary();
  }
  for (const auto& [path, enableDictionary] :
       options.columnEnableDictionaryMap) {
    if (!enableDictionary) {
      properties = properties->disable_dictionary(path);
    }
  }
  properties =
      properties->dictionary_pagesize_limit(options.dictionaryPageSizeLimit);
  for (const auto& [path, dictionaryPageSizeLimit] :
       options.columnDictionaryPageSizeLimitMap) {
    properties =
        properties->dictionary_pagesize_limit(path, dictionaryPageSizeLimit);
  }
  properties =
      properties->compression(getArrowParquetCompression(options.compression));
  for (const auto& columnCompressionValues : options.columnCompressionsMap) {
    properties->compression(
        columnCompressionValues.first,
        getArrowParquetCompression(columnCompressionValues.second));
  }
  properties = properties->codec_options(options.codecOptions);
  for (const auto& [path, option] : options.columnCodecOptionsMap) {
    properties = properties->codec_options(path, option);
  }
  properties = properties->encoding(options.encoding);
  properties = properties->data_pagesize(options.dataPageSize);
  for (const auto& [path, dataPageSize] : options.columnDataPageSizeMap) {
    properties = properties->data_pagesize(path, dataPageSize);
  }
  if (options.enableFlushBasedOnBlockSize) {
    auto size = options.parquet_block_size > 0 ? options.parquet_block_size
                                               : DEFAULT_PARQUET_BLOCK_SIZE;
    properties = properties->set_parquet_block_size(size);
    // Set max_row_group_length to a large value to avoid the number of rows in
    // the row group reaching the threshold first.
    properties = properties->max_row_group_length(
        static_cast<int64_t>(std::numeric_limits<uint32_t>::max()));
  } else {
    properties =
        properties->set_parquet_block_size(flushPolicy->bytesInRowGroup());
    properties = properties->max_row_group_length(
        static_cast<int64_t>(flushPolicy->rowsInRowGroup()));
  }

  properties = options.storeDecimalAsInteger
      ? properties->enable_store_decimal_as_integer()
      : properties->disable_store_decimal_as_integer();
  properties = properties->version(options.parquetVersion);
  properties = properties->data_page_version(options.dataPageVersion);
  return builder;
}

std::string WriterOptionsToString(const WriterOptions& options) {
  std::ostringstream oss;
  oss << "WriterOptions:" << std::endl;
  oss << "  enableDictionary: " << (options.enableDictionary ? "true" : "false")
      << std::endl;
  oss << "  columnEnableDictionaryMap: {";
  for (const auto& [key, value] : options.columnEnableDictionaryMap) {
    oss << key << ": " << (value ? "true" : "false") << ", ";
  }
  oss << "}" << std::endl;
  oss << "  enableRowGroupAlignedWrite: "
      << (options.enableRowGroupAlignedWrite ? "true" : "false") << std::endl;
  oss << "  enableFlushBasedOnBlockSize: "
      << (options.enableFlushBasedOnBlockSize ? "true" : "false") << std::endl;
  oss << "  parquet_block_size: " << options.parquet_block_size << std::endl;

  oss << "  expectedRowsInEachBlock: [";
  for (size_t i = 0; i < options.expectedRowsInEachBlock.size(); ++i) {
    oss << options.expectedRowsInEachBlock[i];
    if (i < options.expectedRowsInEachBlock.size() - 1) {
      oss << ", ";
    }
  }
  oss << "]" << std::endl;

  oss << "  dataPageSize: " << options.dataPageSize << std::endl;
  oss << "  columnDataPageSizeMap: {";
  for (const auto& [key, value] : options.columnDataPageSizeMap) {
    oss << key << ": " << value << ", ";
  }
  oss << "}" << std::endl;
  oss << "  dictionaryPageSizeLimit: " << options.dictionaryPageSizeLimit
      << std::endl;
  oss << "  columnDictionaryPageSizeLimitMap: {";
  for (const auto& [key, value] : options.columnDictionaryPageSizeLimitMap) {
    oss << key << ": " << value << ", ";
  }
  oss << "}" << std::endl;
  oss << "  bufferGrowRatio: " << options.bufferGrowRatio << std::endl;
  oss << "  bufferReserveRatio: " << options.bufferReserveRatio << std::endl;
  oss << "  compression: " << static_cast<int>(options.compression)
      << std::endl;
  oss << "  encoding: " << static_cast<int>(options.encoding) << std::endl;
  oss << "  memoryPool: " << options.memoryPool << std::endl;

  oss << "  columnCompressionsMap: {";
  for (const auto& [key, value] : options.columnCompressionsMap) {
    oss << key << ": " << static_cast<int>(value) << ", ";
  }
  oss << "}" << std::endl;

  oss << "  columnCodecOptionsMap: {";
  for (const auto& [key, value] : options.columnCodecOptionsMap) {
    oss << key << ": " << value->toString() << ", ";
  }
  oss << "}" << std::endl;

  oss << "  parquetWriteTimestampUnit: "
      << (options.parquetWriteTimestampUnit
              ? std::to_string(
                    static_cast<int>(*options.parquetWriteTimestampUnit))
              : "none")
      << std::endl;
  oss << "  parquetWriteTimestampTimeZone: "
      << (options.parquetWriteTimestampTimeZone
              ? *options.parquetWriteTimestampTimeZone
              : "none")
      << std::endl;
  oss << "  writeInt96AsTimestamp: "
      << (options.writeInt96AsTimestamp ? "true" : "false") << std::endl;
  oss << "  bufferFlushThresholdBytes: " << options.bufferFlushThresholdBytes
      << std::endl;
  oss << "  parquetVersion: " << options.parquetVersion << std::endl;
  oss << "  dataPageVersion: " << options.dataPageVersion << std::endl;
  oss << "  storeDecimalAsInteger: "
      << (options.storeDecimalAsInteger ? "true" : "false") << std::endl;
  oss << "  writeBatchBytes: " << options.writeBatchBytes << std::endl;
  oss << "  minBatchSize: " << options.minBatchSize << std::endl;
  oss << " write thread count: " << options.threadPoolSize << std::endl;

  return oss.str();
}

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

std::shared_ptr<::arrow::Field> updateFieldNameRecursive(
    const std::shared_ptr<::arrow::Field>& field,
    const Type& type,
    const std::shared_ptr<::arrow::Field>& fieldNullable,
    const std::string& name = "") {
  BOLT_DCHECK(
      field->type()->Equals(fieldNullable->type()),
      "field name: {}, type: {}; fieldNullable name: {}, type: {}.",
      name,
      field->type()->ToString(),
      fieldNullable->name(),
      fieldNullable->type()->ToString());
  if (type.isRow()) {
    auto rowType = type.asRow();
    auto newField =
        field->WithName(name)->WithNullable(fieldNullable->nullable());
    auto structType =
        std::dynamic_pointer_cast<::arrow::StructType>(newField->type());
    auto nullableStructType =
        std::dynamic_pointer_cast<::arrow::StructType>(fieldNullable->type());
    auto childrenSize = rowType.size();
    std::vector<std::shared_ptr<::arrow::Field>> newFields;
    newFields.reserve(childrenSize);
    for (auto i = 0; i < childrenSize; i++) {
      newFields.push_back(updateFieldNameRecursive(
          structType->fields()[i],
          *rowType.childAt(i),
          nullableStructType->fields()[i],
          rowType.nameOf(i)));
    }
    return newField->WithType(::arrow::struct_(newFields));
  } else if (type.isArray()) {
    auto newField =
        field->WithName(name)->WithNullable(fieldNullable->nullable());
    auto listType =
        std::dynamic_pointer_cast<::arrow::BaseListType>(newField->type());
    auto nullableListType =
        std::dynamic_pointer_cast<::arrow::BaseListType>(fieldNullable->type());
    auto elementType = type.asArray().elementType();
    auto elementField = listType->value_field();
    return newField->WithType(::arrow::list(updateFieldNameRecursive(
        elementField, *elementType, nullableListType->value_field())));
  } else if (type.isMap()) {
    auto mapType = type.asMap();
    auto newField =
        field->WithName(name)->WithNullable(fieldNullable->nullable());
    auto arrowMapType =
        std::dynamic_pointer_cast<::arrow::MapType>(newField->type());
    auto nullableMapType =
        std::dynamic_pointer_cast<::arrow::MapType>(fieldNullable->type());
    auto newKeyField = updateFieldNameRecursive(
        arrowMapType->key_field(),
        *mapType.keyType(),
        nullableMapType->key_field());
    auto newValueField = updateFieldNameRecursive(
        arrowMapType->item_field(),
        *mapType.valueType(),
        nullableMapType->item_field());
    return newField->WithType(
        ::arrow::map(newKeyField->type(), newValueField->type()));
  } else if (name != "") {
    return field->WithName(name)->WithNullable(fieldNullable->nullable());
  } else {
    return field->WithNullable(fieldNullable->nullable());
  }
}

} // namespace

std::shared_ptr<WriterProperties::Builder>
WriterOptions::getWriterPropertiesBuilder() const {
  auto defaultFlushPolicy = std::make_unique<DefaultFlushPolicy>();
  return getArrowParquetWriterOptionsBuilder(*this, defaultFlushPolicy);
}

std::shared_ptr<ArrowWriterProperties::Builder>
WriterOptions::getArrowWriterPropertiesBuilder() const {
  auto builderPtr = std::make_shared<ArrowWriterProperties::Builder>();
  auto& builder = *builderPtr;
  if (writeInt96AsTimestamp) {
    builder.enable_deprecated_int96_timestamps();
  }

  static std::shared_ptr<::arrow::internal::ThreadPool> threadPool = nullptr;
  static std::once_flag threadPoolInitFlag;

  // Default to no threading
  // This disables the use of threads for ParquetWriter by default.
  builder.set_use_threads(false);

  if (threadPoolSize > 0) {
    // Enable threading if threadPoolSize is greater than 0
    builder.set_use_threads(true);

    // Initialize the thread pool only once across all calls to this function
    std::call_once(threadPoolInitFlag, [this]() {
      // Attempt to create a thread pool with the specified size
      auto threadPoolResult =
          ::arrow::internal::ThreadPool::Make(threadPoolSize);
      if (threadPoolResult.ok()) {
        // If thread pool creation is successful, store it in the static
        // variable
        threadPool = threadPoolResult.ValueOrDie();
      } else {
        // Log an error if thread pool creation fails and fall back to
        // non-threaded mode
        LOG(ERROR) << "Failed to create thread pool: "
                   << threadPoolResult.status().ToString()
                   << ". Falling back to non-threaded mode.";
      }
    });

    // Use the thread pool if it was successfully created
    if (threadPool) {
      builder.set_executor(threadPool.get());
    }
  }

  return builderPtr;
}

Writer::Writer(
    std::unique_ptr<dwio::common::FileSink> sink,
    const WriterOptions& options,
    std::shared_ptr<memory::MemoryPool> pool,
    arrow::MemoryPool* arrowPool,
    RowTypePtr schema,
    std::shared_ptr<::arrow::Schema> arrowSchema)
    : pool_(std::move(pool)),
      generalPool_{pool_->addLeafChild(".general")},
      exportPool_{pool_->addLeafChild(".exportArrow")},
      arrowPool_(arrowPool),
      stream_(std::make_shared<ArrowDataBufferSink>(
          std::move(sink),
          *generalPool_,
          options.bufferFlushThresholdBytes,
          options.bufferGrowRatio)),
      arrowContext_(std::make_shared<ArrowContext>()),
      schema_(std::move(schema)),
      arrowSchemaFromHive_(std::move(arrowSchema)),
      bufferGrowRatio_(options.bufferGrowRatio),
      bufferReserveRatio_(options.bufferReserveRatio),
      enableRowGroupAlignedWrite_(options.enableRowGroupAlignedWrite),
      expectedRowsInEachBlock_(options.expectedRowsInEachBlock),
      enableFlushBasedOnBlockSize_(options.enableFlushBasedOnBlockSize),
      writeBatchBytes_(options.writeBatchBytes),
      minBatchSize_(options.minBatchSize) {
  validateSchemaRecursive(schema_);
  parseColumnWidth();
  if (options.flushPolicyFactory) {
    flushPolicy_ = options.flushPolicyFactory();
  } else {
    flushPolicy_ = std::make_unique<DefaultFlushPolicy>();
  }
  options_.timestampUnit =
      options.parquetWriteTimestampUnit.value_or(TimestampUnit::kNano);
  options_.timestampTimeZone =
      options.parquetWriteTimestampTimeZone.value_or("UTC");
  arrowContext_->properties =
      getArrowParquetWriterOptionsBuilder(options, flushPolicy_)->build();
  arrowContext_->arrowWriterProperties =
      options.getArrowWriterPropertiesBuilder()->build();
  setMemoryReclaimers();

  VLOG(1) << "WriteOption values of ParquetWriter: "
          << WriterOptionsToString(options);
}

Writer::Writer(
    std::unique_ptr<dwio::common::FileSink> sink,
    const WriterOptions& options,
    RowTypePtr schema)
    : Writer{
          std::move(sink),
          options,
          options.memoryPool->addAggregateChild(fmt::format(
              "writer_node_{}",
              folly::to<std::string>(folly::Random::rand64()))),
          ::arrow::default_memory_pool(),
          std::move(schema)} {}

void Writer::flush() {
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

void Writer::flush(int64_t rowsInCurrentRowGroup) {
  if (arrowContext_->stagingRows > 0) {
    createFileWriterIfNotExist();

    auto fields = arrowContext_->schema->fields();
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
    PARQUET_THROW_NOT_OK(
        arrowContext_->writer->WriteTable(*table, rowsInCurrentRowGroup));
    flushPolicy_->setBytesPerRow(
        arrowContext_->writer->getWrittenBytesPerRow());
    PARQUET_THROW_NOT_OK(stream_->Flush());
    for (auto& chunk : arrowContext_->stagingChunks) {
      chunk.clear();
    }
    arrowContext_->stagingRows = 0;
    arrowContext_->stagingBytes = 0;
  }
}

void Writer::writeRecordBatch(
    std::shared_ptr<::arrow::RecordBatch>& recordBatch) {
  if (recordBatch->num_rows() <= 0) {
    return;
  }

  createFileWriterIfNotExist();

  // Measure WriteRecordBatch
  auto writeStartTime = std::chrono::steady_clock::now();
  PARQUET_THROW_NOT_OK(arrowContext_->writer->WriteRecordBatch(*recordBatch));
  auto writeEndTime = std::chrono::steady_clock::now();
  auto writeTimeMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                         writeEndTime - writeStartTime)
                         .count();

  if (writeTimeMs > 3000) {
    LOG(WARNING) << "Slow WriteRecordBatch detected: " << writeTimeMs
                 << "ms for " << recordBatch->num_rows() << " rows";
  }
  PARQUET_THROW_NOT_OK(stream_->Flush());
}

void Writer::createFileWriterIfNotExist() {
  if (!arrowContext_->writer) {
    PARQUET_ASSIGN_OR_THROW(
        arrowContext_->writer,
        FileWriter::Open(
            *arrowContext_->schema.get(),
            arrowPool_,
            stream_,
            arrowContext_->properties,
            arrowContext_->arrowWriterProperties));
  }
}

dwio::common::StripeProgress getStripeProgress(
    uint64_t stagingRows,
    int64_t stagingBytes) {
  return dwio::common::StripeProgress{
      .stripeRowCount = stagingRows, .stripeSizeEstimate = stagingBytes};
}

void Writer::splitWriteRecordBatch(
    const VectorPtr& data,
    std::shared_ptr<::arrow::Schema> arraySchemaPtr) {
  vector_size_t rowNumPerBatch = data->size();
  if (rowNumPerBatch > minBatchSize_) {
    auto exportSize =
        estimateExportArrowSize(std::static_pointer_cast<RowVector>(data));
    auto writeBatchBytes =
        std::max<uint64_t>(writeBatchBytes_, (memLimit_ >> 2));
    if (writeBatchBytes > 0 && exportSize > writeBatchBytes * 1.5) {
      auto batchSize =
          checkedMultiply<uint64_t>(data->size(), writeBatchBytes, "uint64_t") /
          exportSize;
      rowNumPerBatch = std::max<vector_size_t>(minBatchSize_, batchSize);
    }
  }

  vector_size_t offset = 0;
  auto dataSize = data->size();
  while (offset < dataSize) {
    auto size = std::min<vector_size_t>(dataSize - offset, rowNumPerBatch);

    ArrowArray array;
    exportToArrow(
        offset == 0 && size == dataSize ? data : data->slice(offset, size),
        array,
        exportPool_.get(),
        options_);

    PARQUET_ASSIGN_OR_THROW(
        auto recordBatch, ::arrow::ImportRecordBatch(&array, arraySchemaPtr));
    if (!arrowContext_->schema) {
      arrowContext_->schema = recordBatch->schema();
    }
    writeRecordBatch(recordBatch);
    offset += size;
  }
}

/**
 * When enableFlushBasedOnBlockSize_ is enabled, current rowgroup will be
 * flushed after the written compressed size exceeds parquetBlockSize. This
 * parameter is configured by Spark on Bolt. Otherwise, this method would cache
 * input `ColumnarBatch` to make the size of row group big. It would flush when:
 * - the cached numRows bigger than `rowsInRowGroup_`
 * - the cached bytes bigger than `bytesInRowGroup_`
 *
 * This method assumes each input `ColumnarBatch` have same schema.
 */
void Writer::write(const VectorPtr& data) {
  BOLT_USER_CHECK(
      data->type()->equivalent(*schema_),
      "The file schema type should be equal with the input rowvector type.");

  ArrowSchema schema;
  exportToArrow(data, schema, options_, {}, exportPool_.get());

  // Convert the arrow schema to Schema and then update the column names based
  // on schema_.
  auto arrowSchema = ::arrow::ImportSchema(&schema).ValueOrDie();
  std::vector<std::shared_ptr<::arrow::Field>> newFields;
  auto childSize = schema_->size();
  for (auto i = 0; i < childSize; i++) {
    newFields.push_back(updateFieldNameRecursive(
        arrowSchema->fields()[i],
        *schema_->childAt(i),
        (arrowSchemaFromHive_ ? arrowSchemaFromHive_->fields()[i]
                              : arrowSchema->fields()[i]),
        schema_->nameOf(i)));
  }

  if (!enableRowGroupAlignedWrite_ && enableFlushBasedOnBlockSize_) {
    splitWriteRecordBatch(data, ::arrow::schema(newFields));
    return;
  }

  ArrowArray array;
  exportToArrow(data, array, exportPool_.get(), options_);
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
  if (enableRowGroupAlignedWrite_) {
    rowGroupAlignedFlush(numRows, bytes, recordBatch);
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

void Writer::write(const VectorPtr& data, int64_t memLimit) {
  memLimit_ = memLimit;
  write(data);
}

bool Writer::isCodecAvailable(common::CompressionKind compression) {
  return arrow::util::Codec::IsAvailable(
      getArrowParquetCompression(compression));
}

void Writer::newRowGroup(int32_t numRows) {
  if (enableFlushBasedOnBlockSize_) {
    PARQUET_THROW_NOT_OK(arrowContext_->writer->NewBufferedRowGroup());
  } else {
    PARQUET_THROW_NOT_OK(arrowContext_->writer->NewRowGroup(numRows));
  }
}

void Writer::close() {
  if (closed_) {
    return;
  }

  flush();
  if (!arrowContext_->writer) {
    createEmptyFile();
  }

  PARQUET_THROW_NOT_OK(arrowContext_->writer->Close());
  arrowContext_->writer.reset();
  PARQUET_THROW_NOT_OK(stream_->Close());
  arrowContext_->stagingChunks.clear();
  closed_ = true;
}

void Writer::createEmptyFile() {
  BOLT_CHECK_NULL(arrowContext_->schema);
  BOLT_CHECK_NULL(arrowContext_->writer);
  ArrowSchema arrowSchema;
  exportToArrow(
      BaseVector::create(
          std::static_pointer_cast<const Type>(schema_), 0, exportPool_.get()),
      arrowSchema,
      options_,
      {},
      exportPool_.get());
  arrowContext_->schema = ::arrow::ImportSchema(&arrowSchema).ValueOrDie();
  createFileWriterIfNotExist();
}

void Writer::abort() {
  stream_->abort();
  arrowContext_.reset();
}

parquet::WriterOptions getParquetOptions(
    const dwio::common::WriterOptions& options) {
  parquet::WriterOptions parquetOptions;
  parquetOptions.memoryPool = options.memoryPool;
  if (options.compressionKind.has_value()) {
    parquetOptions.compression = options.compressionKind.value();
  }
  if (options.arrowBridgeTimestampUnit.has_value()) {
    parquetOptions.parquetWriteTimestampUnit =
        static_cast<TimestampUnit>(options.arrowBridgeTimestampUnit.value());
  }
  return parquetOptions;
}

void Writer::setMemoryReclaimers() {
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
  exportPool_->setReclaimer(exec::MemoryReclaimer::create());
}

std::unique_ptr<dwio::common::Writer> ParquetWriterFactory::createWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    const dwio::common::WriterOptions& options) {
  auto parquetOptions = getParquetOptions(options);
  return std::make_unique<Writer>(
      std::move(sink), parquetOptions, asRowType(options.schema));
}

void Writer::rowGroupAlignedFlush(
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

void Writer::parseColumnWidth() {
  auto columnSize = schema_->children().size();
  for (int i = 0; i < columnSize; ++i) {
    const auto& childType = schema_->childAt(i);
    if (childType->isFixedWidth() && !childType->isVarchar() &&
        !childType->isVarbinary()) {
      totalFixedColumnWidth_ += getArrowElementSize(childType);
    } else {
      variableLengthColumnIndex_.push_back(i);
    }
  }
}

uint64_t Writer::estimateExportArrowSize(const RowVectorPtr& data) {
  uint64_t exportSize = totalFixedColumnWidth_ * data->size();
  for (auto index : variableLengthColumnIndex_) {
    exportSize += data->childAt(index)->estimateExportArrowSize();
  }
  return exportSize;
}

} // namespace bytedance::bolt::parquet
