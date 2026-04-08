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

#include "bolt/connectors/hive/iceberg/IcebergParquetStatsCollector.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <unordered_set>

#include "bolt/common/encode/Base64.h"
#include "bolt/common/file/File.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/ColumnSelector.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/TypeWithId.h"
#include "bolt/dwio/parquet/arrow/Statistics.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/functions/lib/string/StringImpl.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/SelectivityVector.h"

namespace bytedance::bolt::connector::hive::iceberg {

namespace {

struct LeafField {
  int32_t fieldId;
  TypePtr type;
  bool skipBounds;
};

void collectLeafFieldIds(
    const parquet::ParquetFieldId& field,
    std::vector<int32_t>& fieldIds) {
  if (field.children.empty()) {
    fieldIds.push_back(field.fieldId);
    return;
  }

  for (const auto& child : field.children) {
    collectLeafFieldIds(child, fieldIds);
  }
}

void addAllRecursive(
    const parquet::ParquetFieldId& field,
    const TypePtr& type,
    std::unordered_set<int32_t>& fieldIds) {
  fieldIds.insert(field.fieldId);

  BOLT_CHECK_EQ(field.children.size(), type->size());
  for (auto i = 0; i < type->size(); ++i) {
    addAllRecursive(field.children[i], type->childAt(i), fieldIds);
  }
}

// Recursively collects field IDs that should skip bounds collection.
// Repeated fields (e.g. MAP and ARRAY) are not currently supported by Iceberg.
// These fields, along with all their descendants, should skip bounds
// collection.
// @param field The Parquet field ID structure to process.
// @param type The Bolt type corresponding to this field.
// @param fieldIds Output set to populate with field IDs to skip.
void collectSkipBoundsFieldIds(
    const parquet::ParquetFieldId& field,
    const TypePtr& type,
    std::unordered_set<int32_t>& fieldIds) {
  BOLT_CHECK_NOT_NULL(type, "Input column type cannot be null");

  if (type->isMap() || type->isArray()) {
    addAllRecursive(field, type, fieldIds);
    return;
  }

  // Bolt still allows top-level-only field ids on write handles. In that case,
  // keep the collector usable and rely on file schema traversal in aggregate()
  // for repeated-field bounds skipping.
  if (field.children.empty() && type->size() > 0) {
    return;
  }

  BOLT_CHECK_EQ(field.children.size(), type->size());
  for (auto i = 0; i < type->size(); ++i) {
    collectSkipBoundsFieldIds(field.children[i], type->childAt(i), fieldIds);
  }
}

void collectLeafFields(
    const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId,
    std::vector<LeafField>& fields,
    bool skipBounds = false) {
  if (typeWithId->getChildren().empty()) {
    fields.push_back(LeafField{
        static_cast<int32_t>(typeWithId->id()),
        typeWithId->type(),
        skipBounds});
    return;
  }
  const bool childSkipBounds = skipBounds || typeWithId->type()->isArray() ||
      typeWithId->type()->isMap();
  for (const auto& child : typeWithId->getChildren()) {
    collectLeafFields(child, fields, childSkipBounds);
  }
}

// Builds a footer-only Parquet reader so the collector can aggregate file
// statistics after the wrapped HiveDataSink has finished writing the file.
std::unique_ptr<parquet::ParquetReader> makeReader(
    const std::string& filePath,
    memory::MemoryPool* pool) {
  dwio::common::ReaderOptions options{pool};
  return std::make_unique<parquet::ParquetReader>(
      std::make_unique<dwio::common::BufferedInput>(
          std::make_shared<LocalReadFile>(filePath), options.getMemoryPool()),
      options);
}

template <typename T>
int compareDecoded(std::string_view left, std::string_view right) {
  BOLT_CHECK_EQ(left.size(), sizeof(T));
  BOLT_CHECK_EQ(right.size(), sizeof(T));
  T leftValue;
  T rightValue;
  std::memcpy(&leftValue, left.data(), sizeof(T));
  std::memcpy(&rightValue, right.data(), sizeof(T));
  if (leftValue < rightValue) {
    return -1;
  }
  if (leftValue > rightValue) {
    return 1;
  }
  return 0;
}

int compareBinary(std::string_view left, std::string_view right) {
  const auto minSize = std::min(left.size(), right.size());
  for (size_t i = 0; i < minSize; ++i) {
    const auto leftByte = static_cast<uint8_t>(left[i]);
    const auto rightByte = static_cast<uint8_t>(right[i]);
    if (leftByte < rightByte) {
      return -1;
    }
    if (leftByte > rightByte) {
      return 1;
    }
  }
  if (left.size() < right.size()) {
    return -1;
  }
  if (left.size() > right.size()) {
    return 1;
  }
  return 0;
}

bool supportsBoundsAggregation(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
      return true;
    default:
      return type->isDate();
  }
}

template <typename T>
int64_t countNaNsInLeaf(
    const BaseVector& vector,
    const std::vector<uint8_t>& activeRows) {
  SelectivityVector rows(vector.size());
  DecodedVector decoded(vector, rows);

  int64_t nanCount = 0;
  for (auto i = 0; i < vector.size(); ++i) {
    if (!activeRows[i] || decoded.isNullAt(i)) {
      continue;
    }
    if (std::isnan(decoded.valueAt<T>(i))) {
      ++nanCount;
    }
  }
  return nanCount;
}

void collectNaNCounts(
    const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId,
    const VectorPtr& vector,
    const std::vector<uint8_t>& activeRows,
    folly::F14FastMap<int32_t, int64_t>& nanValueCounts) {
  const auto& loaded = BaseVector::loadedVectorShared(vector);
  const auto& type = typeWithId->type();

  if (type->isArray() || type->isMap()) {
    return;
  }

  switch (type->kind()) {
    case TypeKind::ROW: {
      auto rowVector = loaded->as<RowVector>();
      std::vector<uint8_t> childActiveRows = activeRows;
      if (loaded->mayHaveNulls()) {
        for (auto i = 0; i < loaded->size(); ++i) {
          if (childActiveRows[i] && loaded->isNullAt(i)) {
            childActiveRows[i] = false;
          }
        }
      }

      BOLT_CHECK_EQ(typeWithId->size(), rowVector->childrenSize());
      for (auto i = 0; i < typeWithId->size(); ++i) {
        collectNaNCounts(
            typeWithId->childAt(i),
            rowVector->childAt(i),
            childActiveRows,
            nanValueCounts);
      }
      return;
    }
    case TypeKind::REAL:
      nanValueCounts[typeWithId->id()] +=
          countNaNsInLeaf<float>(*loaded, activeRows);
      return;
    case TypeKind::DOUBLE:
      nanValueCounts[typeWithId->id()] +=
          countNaNsInLeaf<double>(*loaded, activeRows);
      return;
    default:
      return;
  }
}

void collectNaNCounts(
    const parquet::ParquetReader& reader,
    memory::MemoryPool* pool,
    IcebergDataFileStatistics& dataFileStats) {
  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      reader.rowType(), reader.rowType()->names()));
  auto scanSpec = std::make_shared<common::ScanSpec>("");
  scanSpec->addAllChildFields(*reader.rowType());
  rowReaderOptions.setScanSpec(scanSpec);

  auto rowReader = reader.createRowReader(rowReaderOptions);
  VectorPtr batch = BaseVector::create(reader.rowType(), 0, pool);
  folly::F14FastMap<int32_t, int64_t> nanValueCounts;

  while (rowReader->next(1024, batch)) {
    auto rowVector = batch->as<RowVector>();
    std::vector<uint8_t> activeRows(rowVector->size(), true);
    if (rowVector->mayHaveNulls()) {
      for (auto i = 0; i < rowVector->size(); ++i) {
        if (rowVector->isNullAt(i)) {
          activeRows[i] = false;
        }
      }
    }
    collectNaNCounts(reader.typeWithId(), batch, activeRows, nanValueCounts);
  }

  for (const auto& [fieldId, nanValueCount] : nanValueCounts) {
    dataFileStats.columnStats[fieldId].nanValueCount = nanValueCount;
  }
}

int compareEncodedBounds(
    const TypePtr& type,
    const std::string& left,
    const std::string& right) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return compareDecoded<uint8_t>(left, right);
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
      return compareDecoded<int32_t>(left, right);
    case TypeKind::BIGINT:
    case TypeKind::TIMESTAMP:
      return compareDecoded<int64_t>(left, right);
    case TypeKind::HUGEINT:
      return compareDecoded<int128_t>(left, right);
    case TypeKind::REAL:
      return compareDecoded<float>(left, right);
    case TypeKind::DOUBLE:
      return compareDecoded<double>(left, right);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return compareBinary(left, right);
    default:
      BOLT_CHECK(
          type->isDate(),
          "Unsupported bound aggregation type: {}",
          type->toString());
      return compareDecoded<int32_t>(left, right);
  }
}

std::string encodeIcebergLowerBound(
    const TypePtr& type,
    std::string_view value,
    int32_t truncateLength) {
  if (type->kind() == TypeKind::VARCHAR) {
    return std::string(
        functions::stringImpl::truncateUtf8(value, truncateLength));
  }
  return std::string(value);
}

std::optional<std::string> encodeIcebergUpperBound(
    const TypePtr& type,
    std::string_view value,
    int32_t truncateLength) {
  if (type->kind() == TypeKind::VARCHAR) {
    return functions::stringImpl::roundUpUtf8(value, truncateLength);
  }
  return std::string(value);
}

} // namespace

IcebergParquetStatsCollector::IcebergParquetStatsCollector(
    const std::vector<IcebergColumnHandlePtr>& inputColumns,
    memory::MemoryPool* pool)
    : pool_(pool) {
  parquetFieldIds_.children.reserve(inputColumns.size());
  for (const auto& columnHandle : inputColumns) {
    parquetFieldIds_.children.emplace_back(columnHandle->field());
    collectSkipBoundsFieldIds(
        columnHandle->field(), columnHandle->dataType(), skipBoundsFieldIds_);
  }
}

IcebergDataFileStatisticsPtr IcebergParquetStatsCollector::aggregate(
    const std::shared_ptr<parquet::arrow::FileMetaData>& fileMetaData) const {
  if (!fileMetaData) {
    return std::make_shared<IcebergDataFileStatistics>(
        IcebergDataFileStatistics::empty());
  }

  auto dataFileStats = std::make_shared<IcebergDataFileStatistics>();
  dataFileStats->numRecords = fileMetaData->num_rows();
  std::vector<int32_t> leafFieldIds;
  for (const auto& child : parquetFieldIds_.children) {
    collectLeafFieldIds(child, leafFieldIds);
  }
  std::vector<int32_t> resolvedFieldIdsByColumnIndex;

  for (int rowGroupIndex = 0; rowGroupIndex < fileMetaData->num_row_groups();
       ++rowGroupIndex) {
    const auto rowGroup = fileMetaData->RowGroup(rowGroupIndex);
    for (int columnIndex = 0; columnIndex < rowGroup->num_columns();
         ++columnIndex) {
      if (resolvedFieldIdsByColumnIndex.size() <= columnIndex) {
        resolvedFieldIdsByColumnIndex.resize(columnIndex + 1, -1);
      }
      const auto columnChunk = rowGroup->ColumnChunk(columnIndex);
      auto fieldId = columnChunk->field_id();
      if (fieldId < 0 && columnIndex < leafFieldIds.size()) {
        fieldId = leafFieldIds[columnIndex];
      }
      if (fieldId < 0) {
        continue;
      }
      resolvedFieldIdsByColumnIndex[columnIndex] = fieldId;
      auto& columnStats = dataFileStats->columnStats[fieldId];
      columnStats.columnSize += columnChunk->total_compressed_size();
      columnStats.valueCount += columnChunk->num_values();
      if (const auto stats = columnChunk->statistics()) {
        if (stats->HasNullCount()) {
          columnStats.nullValueCount += stats->null_count();
        }
      }
    }
  }

  for (size_t columnIndex = 0;
       columnIndex < resolvedFieldIdsByColumnIndex.size();
       ++columnIndex) {
    const auto fieldId = resolvedFieldIdsByColumnIndex[columnIndex];
    if (fieldId < 0) {
      continue;
    }
    auto [nanCount, hasNanCount] = fileMetaData->getNaNCount(fieldId);
    if (!hasNanCount) {
      std::tie(nanCount, hasNanCount) =
          fileMetaData->getNaNCountByColumnIndex(columnIndex);
    }
    if (hasNanCount) {
      dataFileStats->columnStats[fieldId].nanValueCount = nanCount;
    }
  }

  return dataFileStats;
}

IcebergDataFileStatisticsPtr IcebergParquetStatsCollector::aggregate(
    const std::string& filePath) const {
  auto reader = makeReader(filePath, pool_);
  auto dataFileStats = std::make_shared<IcebergDataFileStatistics>();
  const auto fileMetaData = reader->fileMetaData();
  dataFileStats->numRecords = fileMetaData.numRows();

  std::vector<LeafField> leafFields;
  collectLeafFields(reader->typeWithId(), leafFields);

  // Track global min/max statistics for each column across all row groups.
  // Key: Iceberg field ID.
  // Value: Encoded lower and upper bound values aggregated across row groups.
  folly::F14FastMap<int32_t, std::string> lowerBounds;
  folly::F14FastMap<int32_t, std::string> upperBounds;
  std::unordered_set<int32_t> incompleteBounds;

  for (int rowGroupIndex = 0; rowGroupIndex < fileMetaData.numRowGroups();
       ++rowGroupIndex) {
    const auto rowGroup = fileMetaData.rowGroup(rowGroupIndex);
    for (int columnIndex = 0; columnIndex < rowGroup.numColumns();
         ++columnIndex) {
      BOLT_CHECK_LT(columnIndex, leafFields.size());
      const auto& leafField = leafFields[columnIndex];
      auto& columnStats = dataFileStats->columnStats[leafField.fieldId];
      const auto columnChunk = rowGroup.columnChunk(columnIndex);
      columnStats.columnSize += columnChunk.totalCompressedSize();
      columnStats.valueCount += columnChunk.numValues();
      if (auto nullCount = columnChunk.nullCount()) {
        columnStats.nullValueCount += *nullCount;
      }

      if (leafField.skipBounds || !shouldStoreBounds(leafField.fieldId) ||
          !supportsBoundsAggregation(leafField.type) ||
          incompleteBounds.find(leafField.fieldId) != incompleteBounds.end()) {
        continue;
      }

      auto lowerBound = columnChunk.minValue();
      auto upperBound = columnChunk.maxValue();
      if (!lowerBound.has_value() || !upperBound.has_value()) {
        lowerBounds.erase(leafField.fieldId);
        upperBounds.erase(leafField.fieldId);
        incompleteBounds.insert(leafField.fieldId);
        continue;
      }

      auto [lowerIt, lowerInserted] =
          lowerBounds.try_emplace(leafField.fieldId, *lowerBound);
      if (!lowerInserted &&
          compareEncodedBounds(leafField.type, *lowerBound, lowerIt->second) <
              0) {
        lowerIt->second = *lowerBound;
      }

      auto [upperIt, upperInserted] =
          upperBounds.try_emplace(leafField.fieldId, *upperBound);
      if (!upperInserted &&
          compareEncodedBounds(leafField.type, *upperBound, upperIt->second) >
              0) {
        upperIt->second = *upperBound;
      }
    }
  }

  for (const auto& [fieldId, lowerBound] : lowerBounds) {
    auto upperIt = upperBounds.find(fieldId);
    if (upperIt == upperBounds.end()) {
      continue;
    }
    auto& columnStats = dataFileStats->columnStats[fieldId];
    const auto field = std::find_if(
        leafFields.begin(), leafFields.end(), [&](const LeafField& leafField) {
          return leafField.fieldId == fieldId;
        });
    BOLT_CHECK(field != leafFields.end(), "Missing leaf field {}", fieldId);
    columnStats.lowerBound = encoding::Base64::encode(encodeIcebergLowerBound(
        field->type, lowerBound, kDefaultTruncateLength));
    if (auto upperBound = encodeIcebergUpperBound(
            field->type, upperIt->second, kDefaultTruncateLength)) {
      columnStats.upperBound = encoding::Base64::encode(*upperBound);
    }
  }

  collectNaNCounts(*reader, pool_, *dataFileStats);

  return dataFileStats;
}

} // namespace bytedance::bolt::connector::hive::iceberg
