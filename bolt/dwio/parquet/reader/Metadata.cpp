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

#include "bolt/dwio/parquet/reader/Metadata.h"
#include <vector>
#include "bolt/dwio/parquet/reader/ParquetReaderUtil.h"
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"
namespace bytedance::bolt::parquet {

ColumnChunkMetaDataPtr::ColumnChunkMetaDataPtr(const void* metadata)
    : ptr_(metadata) {}

ColumnChunkMetaDataPtr::~ColumnChunkMetaDataPtr() = default;

FOLLY_ALWAYS_INLINE const thrift::ColumnChunk* thriftColumnChunkPtr(
    const void* metadata) {
  return reinterpret_cast<const thrift::ColumnChunk*>(metadata);
}

int64_t ColumnChunkMetaDataPtr::numValues() const {
  return thriftColumnChunkPtr(ptr_)->meta_data.num_values;
}

common::CompressionKind ColumnChunkMetaDataPtr::compression() const {
  return thriftCodecToCompressionKind(
      thriftColumnChunkPtr(ptr_)->meta_data.codec);
}

int64_t ColumnChunkMetaDataPtr::totalCompressedSize() const {
  return thriftColumnChunkPtr(ptr_)->meta_data.total_compressed_size;
}

std::optional<int64_t> ColumnChunkMetaDataPtr::nullCount() const {
  const auto& columnMetaData = thriftColumnChunkPtr(ptr_)->meta_data;
  if (!columnMetaData.__isset.statistics ||
      !columnMetaData.statistics.__isset.null_count) {
    return std::nullopt;
  }
  return columnMetaData.statistics.null_count;
}

std::optional<std::string> ColumnChunkMetaDataPtr::minValue() const {
  const auto& columnMetaData = thriftColumnChunkPtr(ptr_)->meta_data;
  if (!columnMetaData.__isset.statistics) {
    return std::nullopt;
  }
  if (columnMetaData.statistics.__isset.min_value) {
    return columnMetaData.statistics.min_value;
  }
  if (columnMetaData.statistics.__isset.min) {
    return columnMetaData.statistics.min;
  }
  return std::nullopt;
}

std::optional<std::string> ColumnChunkMetaDataPtr::maxValue() const {
  const auto& columnMetaData = thriftColumnChunkPtr(ptr_)->meta_data;
  if (!columnMetaData.__isset.statistics) {
    return std::nullopt;
  }
  if (columnMetaData.statistics.__isset.max_value) {
    return columnMetaData.statistics.max_value;
  }
  if (columnMetaData.statistics.__isset.max) {
    return columnMetaData.statistics.max;
  }
  return std::nullopt;
}

const std::vector<thrift::PageEncodingStats>&
ColumnChunkMetaDataPtr::pageEncodingStats() const {
  return thriftColumnChunkPtr(ptr_)->meta_data.encoding_stats;
}

std::string ColumnChunkMetaDataPtr::toString() const {
  std::stringstream ss;
  thriftColumnChunkPtr(ptr_)->printTo(ss);
  return ss.str();
}

FOLLY_ALWAYS_INLINE const thrift::RowGroup* thriftRowGroupPtr(
    const void* metadata) {
  return reinterpret_cast<const thrift::RowGroup*>(metadata);
}

RowGroupMetaDataPtr::RowGroupMetaDataPtr(const void* metadata)
    : ptr_(metadata) {}

RowGroupMetaDataPtr::~RowGroupMetaDataPtr() = default;

int RowGroupMetaDataPtr::numColumns() const {
  return thriftRowGroupPtr(ptr_)->columns.size();
}

int64_t RowGroupMetaDataPtr::numRows() const {
  return thriftRowGroupPtr(ptr_)->num_rows;
}

int64_t RowGroupMetaDataPtr::totalByteSize() const {
  return thriftRowGroupPtr(ptr_)->total_byte_size;
}

int64_t RowGroupMetaDataPtr::totalCompressedSize() const {
  return thriftRowGroupPtr(ptr_)->total_compressed_size;
}

int64_t RowGroupMetaDataPtr::fileOffset() const {
  return thriftRowGroupPtr(ptr_)->file_offset;
};

ColumnChunkMetaDataPtr RowGroupMetaDataPtr::columnChunk(int i) const {
  return ColumnChunkMetaDataPtr(
      reinterpret_cast<const void*>(&thriftRowGroupPtr(ptr_)->columns[i]));
}

FOLLY_ALWAYS_INLINE const thrift::FileMetaData* thriftFileMetaDataPtr(
    const void* metadata) {
  return reinterpret_cast<const thrift::FileMetaData*>(metadata);
}

FileMetaDataPtr::FileMetaDataPtr(const void* metadata) : ptr_(metadata) {}

FileMetaDataPtr::~FileMetaDataPtr() = default;

RowGroupMetaDataPtr FileMetaDataPtr::rowGroup(int i) const {
  return RowGroupMetaDataPtr(reinterpret_cast<const void*>(
      &thriftFileMetaDataPtr(ptr_)->row_groups[i]));
}

int64_t FileMetaDataPtr::numRows() const {
  return thriftFileMetaDataPtr(ptr_)->num_rows;
}

int FileMetaDataPtr::numRowGroups() const {
  return thriftFileMetaDataPtr(ptr_)->row_groups.size();
}

} // namespace bytedance::bolt::parquet
