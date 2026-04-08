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

#pragma once

#include <unordered_set>

#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "bolt/connectors/hive/iceberg/IcebergDataFileStatistics.h"
#include "bolt/dwio/parquet/ParquetFieldId.h"
#include "bolt/dwio/parquet/arrow/Metadata.h"

namespace bytedance::bolt::connector::hive::iceberg {

class IcebergParquetStatsCollector {
 public:
  explicit IcebergParquetStatsCollector(
      const std::vector<IcebergColumnHandlePtr>& inputColumns,
      memory::MemoryPool* pool = nullptr);

  explicit IcebergParquetStatsCollector(memory::MemoryPool* pool)
      : pool_(pool) {}

  /// Aggregates Parquet file metadata into Iceberg data file statistics.
  /// Iterates through all row groups and columns to collect:
  /// - Record count, split offsets, value counts, column sizes, null counts.
  /// - Min/max bounds (base64-encoded). Currently not collected for MAP and
  ///   ARRAY types and all their descendants.
  IcebergDataFileStatisticsPtr aggregate(const std::string& filePath) const;

  IcebergDataFileStatisticsPtr aggregate(
      const std::shared_ptr<parquet::arrow::FileMetaData>& fileMetaData) const;

  /// Returns the Parquet field IDs for all input columns.
  /// The field IDs are written to the Parquet data file's column metadata.
  /// The return object describes a multi-column input.
  const parquet::ParquetFieldId& parquetFieldIds() const {
    return parquetFieldIds_;
  }

  /// TODO: Need to support this config property.
  /// 16 is default value. See DEFAULT_WRITE_METRICS_MODE_DEFAULT in
  /// org.apache.iceberg.TableProperties.
  constexpr static int32_t kDefaultTruncateLength{16};

 private:
  bool shouldStoreBounds(int32_t fieldId) const {
    return skipBoundsFieldIds_.find(fieldId) == skipBoundsFieldIds_.end();
  }

  memory::MemoryPool* const pool_;
  // Hierarchical Parquet field IDs for all input columns. A single
  // ParquetFieldId can describe all the columns including their nested
  // children.
  parquet::ParquetFieldId parquetFieldIds_;
  // Set of field IDs for which bounds collection should be skipped.
  // This includes MAP and ARRAY types and all their descendants.
  std::unordered_set<int32_t> skipBoundsFieldIds_;
};

} // namespace bytedance::bolt::connector::hive::iceberg
