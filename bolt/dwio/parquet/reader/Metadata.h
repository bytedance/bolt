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
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

// Adapted from Apache Arrow.

#pragma once

#include "bolt/dwio/common/compression/Compression.h"
namespace bytedance::bolt::parquet {
namespace thrift {
class PageEncodingStats;
}

/// ColumnChunkMetaDataPtr is a proxy around pointer to thrift::ColumnChunk.
class ColumnChunkMetaDataPtr {
 public:
  explicit ColumnChunkMetaDataPtr(const void* metadata);

  ~ColumnChunkMetaDataPtr();

  int64_t numValues() const;

  common::CompressionKind compression() const;

  int64_t totalCompressedSize() const;

  const std::vector<thrift::PageEncodingStats>& pageEncodingStats() const;

  std::string toString() const;

 private:
  const void* ptr_;
};

/// RowGroupMetaDataPtr is a proxy around pointer to thrift::RowGroup.
class RowGroupMetaDataPtr {
 public:
  explicit RowGroupMetaDataPtr(const void* metadata);

  ~RowGroupMetaDataPtr();

  /// The number of columns in this row group. The order must match the
  /// parent's column ordering.
  int numColumns() const;

  /// The sorting column, column index in this row group
  int32_t sortingColumnIdx(int i) const;

  /// If true, indicates this column is sorted in descending order.
  bool sortingColumnDescending(int i) const;

  /// If true, nulls will come before non-null values, otherwise, nulls go at
  /// the end
  bool sortingColumnNullsFirst(int i) const;

  /// Return the ColumnChunkMetaData pointer of the corresponding column index.
  ColumnChunkMetaDataPtr columnChunk(int index) const;

  /// Number of rows in this row group.
  int64_t numRows() const;

  /// Total byte size of all the uncompressed column data in this row
  /// group.
  int64_t totalByteSize() const;

  /// Total byte size of all the compressed (and potentially encrypted)
  /// column data in this row group.
  /// This information is optional and may be 0 if omitted.
  int64_t totalCompressedSize() const;

  /// Get the file offset of this Row Group
  int64_t fileOffset() const;

 private:
  const void* ptr_;
};

/// FileMetaData is a proxy around pointer to thrift::FileMetaData.
class FileMetaDataPtr {
 public:
  explicit FileMetaDataPtr(const void* metadata);

  ~FileMetaDataPtr();

  /// The total number of rows.
  int64_t numRows() const;

  /// The number of row groups in the file.
  int numRowGroups() const;

  /// Return the RowGroupMetaData pointer of the corresponding row group index.
  RowGroupMetaDataPtr rowGroup(int index) const;

  /// The key/value metadata size.
  int64_t keyValueMetadataSize() const;

  /// Returns True if the key/value metadata contains the key input.
  bool keyValueMetadataContains(const std::string_view key) const;

  /// Returns the value inside the key/value metadata if the key is present.
  std::string keyValueMetadataValue(const std::string_view key) const;

 private:
  const void* ptr_;
};

} // namespace bytedance::bolt::parquet
