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
 * 2026-03-30.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/connectors/hive/iceberg/IcebergDataSink.h"

#include <filesystem>
#include <unordered_map>
#include <unordered_set>

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include "bolt/common/encode/Base64.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/connectors/hive/iceberg/IcebergParquetStatsCollector.h"
#include "bolt/connectors/hive/iceberg/IcebergPartitionName.h"
#include "bolt/connectors/hive/iceberg/TransformEvaluator.h"
#include "bolt/connectors/hive/iceberg/TransformExprBuilder.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::connector::hive::iceberg {
namespace {

namespace fs = std::filesystem;

// Parquet (Arrow) writer reads field id from Arrow field metadata.
// Bolt's Parquet writer reads field ids from Arrow field metadata.
static constexpr char FIELD_ID_KEY[] = "PARQUET:field_id";

std::vector<std::shared_ptr<const HiveColumnHandle>> toHiveInputColumns(
    const std::vector<IcebergColumnHandlePtr>& inputColumns,
    const IcebergPartitionSpecPtr& partitionSpec) {
  std::unordered_set<std::string> partitionKeyNames;
  if (partitionSpec) {
    partitionKeyNames.reserve(partitionSpec->fields.size());
    for (const auto& field : partitionSpec->fields) {
      partitionKeyNames.emplace(field.name);
    }
  }

  std::vector<std::shared_ptr<const HiveColumnHandle>> columns;
  columns.reserve(inputColumns.size());
  for (const auto& column : inputColumns) {
    auto columnType = column->columnType();
    if (!partitionKeyNames.empty() &&
        partitionKeyNames.find(column->name()) != partitionKeyNames.end()) {
      columnType = HiveColumnHandle::ColumnType::kPartitionKey;
    }

    if (columnType == column->columnType()) {
      columns.emplace_back(column);
      continue;
    }

    columns.emplace_back(std::make_shared<const HiveColumnHandle>(
        column->name(),
        columnType,
        column->dataType(),
        column->hiveType(),
        std::vector<common::Subfield>{},
        column->fieldId()));
  }
  return columns;
}

std::shared_ptr<::arrow::Field> addFieldIdMetadata(
    const std::shared_ptr<::arrow::Field>& field,
    int32_t fieldId) {
  std::shared_ptr<::arrow::KeyValueMetadata> metadata;
  if (!field->metadata()) {
    metadata = ::arrow::key_value_metadata({}, {});
  } else {
    metadata = std::make_shared<::arrow::KeyValueMetadata>(
        field->metadata()->keys(), field->metadata()->values());
  }
  auto status = metadata->Set(FIELD_ID_KEY, folly::to<std::string>(fieldId));
  BOLT_CHECK(
      status.ok(),
      "Failed to set parquet field id metadata: {}",
      status.ToString());
  return field->WithMetadata(std::move(metadata));
}

std::shared_ptr<::arrow::Field> applyFieldIds(
    const std::shared_ptr<::arrow::Field>& field,
    const std::string& path,
    const FieldIdToColumnPathMap& fieldIdToColumnPath) {
  auto updated = field;
  for (const auto& [fieldId, columnPath] : fieldIdToColumnPath) {
    if (columnPath == path) {
      updated = addFieldIdMetadata(updated, fieldId);
      break;
    }
  }

  if (updated->type()->id() != ::arrow::Type::STRUCT) {
    return updated;
  }

  std::vector<std::shared_ptr<::arrow::Field>> children;
  children.reserve(updated->type()->num_fields());
  for (int i = 0; i < updated->type()->num_fields(); ++i) {
    const auto& child = updated->type()->field(i);
    children.push_back(applyFieldIds(
        child, fmt::format("{}.{}", path, child->name()), fieldIdToColumnPath));
  }
  return updated->WithType(::arrow::struct_(std::move(children)));
}

std::shared_ptr<::arrow::Field> applyTopLevelFieldId(
    const std::shared_ptr<::arrow::Field>& field,
    const std::vector<IcebergColumnHandlePtr>& inputColumns) {
  for (const auto& inputColumn : inputColumns) {
    if (inputColumn->name() == field->name()) {
      return addFieldIdMetadata(field, inputColumn->field().fieldId);
    }
  }
  return field;
}

template <TypeKind Kind>
folly::dynamic extractPartitionValue(
    const VectorPtr& child,
    vector_size_t row) {
  using T = typename TypeTraits<Kind>::NativeType;
  auto value = child->as<SimpleVector<T>>()->valueAt(row);
  if constexpr (Kind == TypeKind::BIGINT || Kind == TypeKind::HUGEINT) {
    if (child->type()->isDecimal()) {
      return DecimalUtil::toString(value, child->type());
    }
  }
  if constexpr (Kind == TypeKind::HUGEINT) {
    return std::to_string(value);
  }
  return value;
}

template <>
folly::dynamic extractPartitionValue<TypeKind::VARCHAR>(
    const VectorPtr& child,
    vector_size_t row) {
  return child->as<SimpleVector<StringView>>()->valueAt(row).str();
}

template <>
folly::dynamic extractPartitionValue<TypeKind::VARBINARY>(
    const VectorPtr& child,
    vector_size_t row) {
  return encoding::Base64::encode(
      child->as<SimpleVector<StringView>>()->valueAt(row));
}

template <>
folly::dynamic extractPartitionValue<TypeKind::TIMESTAMP>(
    const VectorPtr& child,
    vector_size_t row) {
  return child->as<SimpleVector<Timestamp>>()->valueAt(row).toMicros();
}

template <>
folly::dynamic extractPartitionValue<TypeKind::UNKNOWN>(
    const VectorPtr&,
    vector_size_t) {
  return nullptr;
}

// Builds an Arrow schema annotated with PARQUET:field_id metadata so Bolt's
// Parquet writer can emit Iceberg-compatible field ids while still going
// through the wrapped HiveDataSink write path.
//
// NOTE: This is a Bolt-specific adaptation for wiring Iceberg field ids into
// the existing HiveDataSink write pipeline via WriterOptions::arrowSchema.
// Semantics match Bolt's Parquet(Arrow) writer which consumes PARQUET:field_id
// metadata when producing the Parquet schema.
std::shared_ptr<::arrow::Schema> makeArrowSchemaWithFieldIds(
    const RowTypePtr& rowType,
    const std::vector<IcebergColumnHandlePtr>& inputColumns,
    const FieldIdToColumnPathMap& fieldIdToColumnPath,
    memory::MemoryPool* pool) {
  if (inputColumns.empty() && fieldIdToColumnPath.empty()) {
    return nullptr;
  }

  ArrowSchema cArrowSchema;
  exportToArrow(BaseVector::create(rowType, 0, pool), cArrowSchema, {});
  auto arrowSchema = ::arrow::ImportSchema(&cArrowSchema).ValueOrDie();

  std::vector<std::shared_ptr<::arrow::Field>> fields;
  fields.reserve(arrowSchema->num_fields());
  for (int i = 0; i < arrowSchema->num_fields(); ++i) {
    auto field = applyTopLevelFieldId(arrowSchema->field(i), inputColumns);
    fields.push_back(applyFieldIds(field, field->name(), fieldIdToColumnPath));
  }
  return ::arrow::schema(std::move(fields), arrowSchema->metadata());
}

// Creates partition channels by mapping partition spec fields to input column
// indices. For each field in the partition spec, finds the corresponding
// partition key column in the input columns and records its index.
std::vector<column_index_t> createPartitionChannels(
    const std::vector<std::shared_ptr<const HiveColumnHandle>>& inputColumns,
    const IcebergPartitionSpecPtr& partitionSpec) {
  std::vector<column_index_t> channels;
  if (!partitionSpec) {
    return channels;
  }

  // Build a map from partition key column names to their indices in the input.
  std::unordered_map<std::string, column_index_t> partitionKeyMap;
  for (auto i = 0; i < inputColumns.size(); ++i) {
    if (inputColumns[i]->isPartitionKey()) {
      partitionKeyMap[inputColumns[i]->name()] = i;
    }
  }

  // For each field in the partition spec, find its corresponding input column
  // index.
  channels.reserve(partitionSpec->fields.size());
  for (const auto& field : partitionSpec->fields) {
    if (auto it = partitionKeyMap.find(field.name);
        it != partitionKeyMap.end()) {
      channels.push_back(it->second);
    }
  }
  return channels;
}

std::vector<column_index_t> createDataChannels(
    const IcebergInsertTableHandlePtr& insertTableHandle) {
  std::vector<column_index_t> dataChannels(
      insertTableHandle->inputColumns().size());
  std::iota(dataChannels.begin(), dataChannels.end(), 0);
  return dataChannels;
}

RowTypePtr createPartitionRowType(
    const IcebergPartitionSpecPtr& partitionSpec) {
  if (!partitionSpec) {
    return nullptr;
  }

  std::vector<TypePtr> partitionKeyTypes;
  std::vector<std::string> partitionKeyNames;
  // Build column names and types for each partition field.
  // Identity transforms use the source column name directly.
  // Non-identity transforms use "columnName_transformName" format.
  for (const auto& field : partitionSpec->fields) {
    partitionKeyTypes.emplace_back(field.resultType());
    std::string key = field.transformType == TransformType::kIdentity
        ? field.name
        : fmt::format(
              "{}_{}",
              field.name,
              TransformTypeName::toName(field.transformType));
    partitionKeyNames.emplace_back(std::move(key));
  }

  return ROW(std::move(partitionKeyNames), std::move(partitionKeyTypes));
}

} // namespace

IcebergInsertTableHandle::IcebergInsertTableHandle(
    std::vector<IcebergColumnHandlePtr> inputColumns,
    LocationHandlePtr locationHandle,
    dwio::common::FileFormat tableStorageFormat,
    IcebergPartitionSpecPtr partitionSpec,
    std::optional<common::CompressionKind> compressionKind,
    const std::unordered_map<std::string, std::string>& serdeParameters,
    FieldIdToColumnPathMap fieldIdToColumnPath)
    : HiveInsertTableHandle(
          toHiveInputColumns(inputColumns, partitionSpec),
          std::move(locationHandle),
          tableStorageFormat,
          std::shared_ptr<const HiveBucketProperty>{},
          compressionKind,
          serdeParameters,
          nullptr,
          false,
          std::make_shared<const HiveInsertFileNameGenerator>()),
      icebergInputColumns_(std::move(inputColumns)),
      partitionSpec_(std::move(partitionSpec)),
      fieldIdToColumnPath_(std::move(fieldIdToColumnPath)) {
  BOLT_USER_CHECK(
      !icebergInputColumns_.empty(),
      "Input columns cannot be empty for Iceberg tables.");
  BOLT_USER_CHECK_NOT_NULL(
      this->locationHandle(),
      "Location handle is required for Iceberg tables.");
  BOLT_USER_CHECK(
      tableStorageFormat == dwio::common::FileFormat::PARQUET,
      "Only Parquet file format is supported when writing Iceberg tables.");
}

IcebergDataSink::IcebergDataSink(
    RowTypePtr inputType,
    IcebergInsertTableHandlePtr insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const std::shared_ptr<const IcebergConfig>& icebergConfig,
    const core::QueryConfig& queryConfig)
    : IcebergDataSink(
          inputType,
          insertTableHandle,
          connectorQueryCtx,
          commitStrategy,
          hiveConfig,
          icebergConfig,
          queryConfig,
          createPartitionChannels(
              insertTableHandle->inputColumns(),
              insertTableHandle->partitionSpec()),
          createDataChannels(insertTableHandle),
          createPartitionRowType(insertTableHandle->partitionSpec())) {}

IcebergDataSink::~IcebergDataSink() = default;

IcebergDataSink::IcebergDataSink(
    RowTypePtr inputType,
    IcebergInsertTableHandlePtr insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const std::shared_ptr<const IcebergConfig>& icebergConfig,
    const core::QueryConfig& queryConfig,
    const std::vector<column_index_t>& partitionChannels,
    const std::vector<column_index_t>& dataChannels,
    RowTypePtr partitionRowType)
    : HiveDataSink(
          inputType,
          insertTableHandle,
          connectorQueryCtx,
          commitStrategy,
          hiveConfig,
          queryConfig,
          0,
          nullptr,
          partitionChannels,
          dataChannels,
          partitionRowType != nullptr
              ? std::make_unique<PartitionIdGenerator>(
                    partitionRowType,
                    [&partitionChannels]() {
                      std::vector<column_index_t> transformedChannels(
                          partitionChannels.size());
                      std::iota(
                          transformedChannels.begin(),
                          transformedChannels.end(),
                          0);
                      return transformedChannels;
                    }(),
                    hiveConfig->maxPartitionsPerWriters(
                        connectorQueryCtx->sessionProperties()),
                    connectorQueryCtx->memoryPool(),
                    hiveConfig->isPartitionPathAsLowerCase(
                        connectorQueryCtx->sessionProperties()))
              : nullptr),
      partitionSpec_(insertTableHandle->partitionSpec()),
      transformEvaluator_(
          !partitionChannels.empty() ? std::make_unique<TransformEvaluator>(
                                           TransformExprBuilder::toExpressions(
                                               partitionSpec_,
                                               partitionChannels_,
                                               inputType_,
                                               icebergConfig->functionPrefix()),
                                           connectorQueryCtx_)
                                     : nullptr),
      partitionNameGenerator_(
          partitionSpec_ != nullptr
              ? std::make_unique<IcebergPartitionName>(partitionSpec_)
              : nullptr),
      partitionRowType_(std::move(partitionRowType)),
      parquetStatsCollector_(std::make_shared<IcebergParquetStatsCollector>(
          insertTableHandle->icebergInputColumns(),
          connectorQueryCtx->memoryPool())) {
  commitPartitionValue_.resize(maxOpenWriters_);
}

std::vector<std::string> IcebergDataSink::commitMessage() const {
  std::vector<std::string> commitTasks;
  commitTasks.reserve(writerInfo_.size());

  auto icebergInsertTableHandle =
      std::dynamic_pointer_cast<const IcebergInsertTableHandle>(
          insertTableHandle_);
  BOLT_CHECK_NOT_NULL(icebergInsertTableHandle);

  // Following metadata (json format) is consumed by Presto CommitTaskData.
  // It contains the minimal subset of metadata.
  // NOTE: Bolt collects full Iceberg metrics via IcebergParquetStatsCollector
  // and merges them into the "metrics" field.

  for (auto i = 0; i < writerInfo_.size(); ++i) {
    const auto& writerInfo = writerInfo_.at(i);
    BOLT_CHECK_NOT_NULL(writerInfo);
    for (const auto& fileInfo : writerInfo->writtenFiles) {
      const auto fullPath =
          (fs::path(writerInfo->writerParameters.targetDirectory()) /
           fileInfo.targetFileName)
              .string();

      folly::dynamic metrics =
          folly::dynamic::object("recordCount", fileInfo.numRows);
      if (parquetStatsCollector_) {
        auto fullMetrics =
            parquetStatsCollector_->aggregate(fullPath)->toJson();
        if (fullMetrics.isObject()) {
          for (const auto& [key, value] : fullMetrics.items()) {
            metrics[key] = value;
          }
        } else {
          metrics = std::move(fullMetrics);
        }
      }

      // clang-format off
      folly::dynamic commitData = folly::dynamic::object("path", fullPath)(
          "fileSizeInBytes", fileInfo.fileSize)(
          "metrics", metrics)(
          "partitionSpecJson",
          icebergInsertTableHandle->partitionSpec()
              ? icebergInsertTableHandle->partitionSpec()->specId
              : 0)("sortOrderId", 0)("fileFormat", "PARQUET")(
          "content", "DATA");
      // clang-format on
      if (!commitPartitionValue_.empty() &&
          !commitPartitionValue_[i].isNull()) {
        commitData["partitionDataJson"] = folly::toJson(folly::dynamic::object(
            "partitionValues", commitPartitionValue_[i]));
      }
      commitTasks.push_back(folly::toJson(commitData));
    }
  }
  return commitTasks;
}

void IcebergDataSink::computePartitionAndBucketIds(const RowVectorPtr& input) {
  BOLT_CHECK(isPartitioned());
  BOLT_CHECK_NOT_NULL(transformEvaluator_);
  BOLT_CHECK_NOT_NULL(partitionIdGenerator_);

  auto transformedColumns = transformEvaluator_->evaluate(input);
  auto transformedRowVector = std::make_shared<RowVector>(
      connectorQueryCtx_->memoryPool(),
      partitionRowType_,
      nullptr,
      input->size(),
      std::move(transformedColumns));
  partitionIdGenerator_->run(transformedRowVector, partitionIds_);
}

uint32_t IcebergDataSink::ensureWriter(const HiveWriterId& id) {
  auto writerId = HiveDataSink::ensureWriter(id);
  if (commitPartitionValue_[writerId].isNull()) {
    commitPartitionValue_[writerId] = makeCommitPartitionValue(writerId);
  }
  return writerId;
}

std::shared_ptr<dwio::common::WriterOptions>
IcebergDataSink::createWriterOptions(size_t writerIndex) const {
  auto options = HiveDataSink::createWriterOptions(writerIndex);
  // Per Iceberg specification (https://iceberg.apache.org/spec/#parquet):
  // - Timestamps must be stored with microsecond precision.
  // - Timestamps must NOT be adjusted to UTC timezone; they should be written
  //   as-is without timezone conversion (empty string disables conversion).
  //
  // These settings are passed via serdeParameters to avoid including
  // parquet-specific headers.
  options->serdeParameters["parquet.writer.timestamp.unit"] = "6";
  options->serdeParameters["parquet.writer.timestamp.timezone"] = "";

  auto icebergInsertTableHandle =
      std::dynamic_pointer_cast<const IcebergInsertTableHandle>(
          insertTableHandle_);
  BOLT_CHECK_NOT_NULL(icebergInsertTableHandle);

  // Bolt-specific: annotate Arrow schema with PARQUET:field_id so Parquet
  // writer can emit Iceberg-compatible field ids.
  options->arrowSchema = makeArrowSchemaWithFieldIds(
      inputType_,
      icebergInsertTableHandle->icebergInputColumns(),
      icebergInsertTableHandle->fieldIdToColumnPath(),
      connectorQueryCtx_->memoryPool());
  return options;
}

std::string IcebergDataSink::getPartitionName(uint32_t partitionId) const {
  BOLT_CHECK_NOT_NULL(partitionNameGenerator_);
  return partitionNameGenerator_->partitionName(
      partitionId,
      partitionIdGenerator_->partitionValues(),
      hiveConfig_->isPartitionPathAsLowerCase(
          connectorQueryCtx_->sessionProperties()));
}

folly::dynamic IcebergDataSink::makeCommitPartitionValue(
    uint32_t writerIndex) const {
  folly::dynamic partitionValues = folly::dynamic::array();
  const auto& transformedValues = partitionIdGenerator_->partitionValues();
  for (auto i = 0; i < partitionChannels_.size(); ++i) {
    const auto& child = transformedValues->childAt(i);
    if (child->isNullAt(writerIndex)) {
      partitionValues.push_back(nullptr);
    } else {
      partitionValues.push_back(BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          extractPartitionValue, child->typeKind(), child, writerIndex));
    }
  }
  return partitionValues;
}

} // namespace bytedance::bolt::connector::hive::iceberg
