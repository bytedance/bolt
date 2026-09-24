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

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"
#include "bolt/dwio/lance/proto/lance_encodings_v2_0.pb.h"
#include "bolt/dwio/lance/proto/lance_encodings_v2_1.pb.h"
#include "bolt/dwio/lance/proto/lance_file.pb.h"
#include "bolt/dwio/lance/proto/lance_file_v2.pb.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::lance::reader {

/// Immutable file-level descriptors parsed before any column metadata.
class NativeLanceFileMetadata {
 public:
  static constexpr uint64_t kFooterSize = 40;

  struct Footer {
    uint64_t columnMetadataStart;
    uint64_t columnMetadataOffsetsStart;
    uint64_t globalBufferOffsetsStart;
    uint32_t numGlobalBuffers;
    uint32_t numColumns;
    uint16_t majorVersion;
    uint16_t minorVersion;
  };

  struct BufferDescriptor {
    uint64_t offset;
    uint64_t length;

    friend bool operator==(
        const BufferDescriptor& lhs,
        const BufferDescriptor& rhs) {
      return lhs.offset == rhs.offset && lhs.length == rhs.length;
    }
  };

  const Footer& footer() const {
    return footer_;
  }

  uint64_t numRows() const {
    return numRows_;
  }

  const std::vector<BufferDescriptor>& globalBuffers() const {
    return globalBuffers_;
  }

  const std::vector<BufferDescriptor>& columnMetadataLocations() const {
    return columnMetadataLocations_;
  }

 protected:
  Footer footer_{};
  uint64_t numRows_{0};
  std::vector<BufferDescriptor> globalBuffers_;
  std::vector<BufferDescriptor> columnMetadataLocations_;
};

/// Immutable logical-to-physical schema mapping built during file open.
class NativeLanceSchemaIndex {
 public:
  struct StructuralField {
    std::string name;
    std::string logicalType;
    TypePtr type;
    bool nullable{false};
    bool leaf{false};
    uint32_t physicalColumnIndex{0};
    uint32_t physicalColumnCount{0};
    uint64_t rowsPerParent{1};
    std::vector<StructuralField> children;
  };

  const RowTypePtr& rowType() const {
    return rowType_;
  }

  const std::vector<uint32_t>& physicalColumnIndices() const {
    return physicalColumnIndices_;
  }

  const StructuralField& structuralField(uint32_t columnIndex) const {
    return structuralFields_.at(columnIndex);
  }

 protected:
  explicit NativeLanceSchemaIndex(
      std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter)
      : typeAdapter_(std::move(typeAdapter)) {}

  std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter_;
  ::lance::file::FileDescriptor fileDescriptor_;
  RowTypePtr rowType_;
  std::vector<std::string> columnLogicalTypes_;
  std::vector<uint32_t> physicalColumnIndices_;
  std::vector<std::string> physicalColumnLogicalTypes_;
  std::vector<uint32_t> physicalColumnSpans_;
  std::vector<std::vector<std::string>> physicalColumnChildLogicalTypes_;
  std::vector<bool> rowAlignedPhysicalColumns_;
  std::unordered_map<uint32_t, uint32_t> leafPhysicalColumnIndices_;
  std::vector<StructuralField> structuralFields_;
};

/// Owns lazy column metadata and its synchronization. It never owns page
/// payload or decompressed values.
class NativeLanceColumnMetadataLoader {
 public:
  using BufferDescriptor = NativeLanceFileMetadata::BufferDescriptor;

  void loadPhysicalColumns(
      const std::vector<uint32_t>& physicalColumnIndices) const;

  const ::lance::file::v2::ColumnMetadata& column(
      uint32_t physicalColumnIndex) const;

  const std::vector<::lance::file::v2::ColumnMetadata>& columns() const;

  const ::lance::encodings::ArrayEncoding& pageEncoding(
      uint32_t physicalColumnIndex,
      int32_t pageIndex) const;

  const ::lance::encodings21::PageLayout& pageLayout(
      uint32_t physicalColumnIndex,
      int32_t pageIndex) const;

  const std::vector<uint64_t>& pageRowStarts(
      uint32_t physicalColumnIndex) const;

  bool isBlobColumn(uint32_t physicalColumnIndex) const;

  size_t loadedColumnMetadataCount() const;

  size_t numColumns() const {
    return columns_.size();
  }

 protected:
  NativeLanceColumnMetadataLoader(
      dwio::common::BufferedInput& input,
      memory::MemoryPool& pool)
      : input_(input), pool_(pool) {}

  void initialize(
      uint64_t fileSize,
      uint64_t columnMetadataStart,
      uint32_t numColumns,
      bool structuralEncoding,
      uint64_t fileRowCount,
      const std::vector<BufferDescriptor>& columnMetadataLocations);

  void setExpectedRows(uint32_t physicalColumnIndex, uint64_t rowCount);

  dwio::common::BufferedInput& input_;
  memory::MemoryPool& pool_;

 private:
  void parseColumnMetadata(
      uint32_t physicalColumnIndex,
      const char* data,
      size_t size) const;
  void validateColumnMetadata(uint32_t physicalColumnIndex) const;

  uint64_t fileSize_{0};
  uint64_t columnMetadataStart_{0};
  uint64_t fileRowCount_{0};
  bool structuralEncoding_{false};
  const std::vector<BufferDescriptor>* columnMetadataLocationIndex_{nullptr};
  std::vector<uint64_t> physicalColumnExpectedRows_;
  mutable std::vector<::lance::file::v2::ColumnMetadata> columns_;
  mutable std::vector<std::vector<::lance::encodings::ArrayEncoding>>
      pageEncodings_;
  mutable std::vector<std::vector<::lance::encodings21::PageLayout>>
      pageLayouts_;
  mutable std::vector<std::vector<uint64_t>> pageRowStarts_;
  mutable std::vector<bool> blobColumns_;
  mutable std::vector<std::atomic<bool>> columnMetadataLoaded_;
  mutable std::mutex columnMetadataMutex_;
};

} // namespace bytedance::bolt::lance::reader
