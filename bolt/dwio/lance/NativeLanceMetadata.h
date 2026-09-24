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

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bolt/dwio/lance/NativeLanceMetadataComponents.h"

namespace bytedance::bolt::lance::reader {

class NativeLanceFileOpenTask;

/// Metadata needed to plan native reads of Lance v2.0-v2.3 files.
///
/// The object deliberately retains offsets rather than page payloads. Payloads
/// are requested later through BufferedInput so Bolt remains in control of
/// coalescing, caching, prefetch, accounting, and cancellation.
class NativeLanceMetadata final : private NativeLanceFileMetadata,
                                  private NativeLanceSchemaIndex,
                                  private NativeLanceColumnMetadataLoader {
 public:
  static constexpr uint64_t kFooterSize = NativeLanceFileMetadata::kFooterSize;
  using Footer = NativeLanceFileMetadata::Footer;
  using BufferDescriptor = NativeLanceFileMetadata::BufferDescriptor;
  using StructuralField = NativeLanceSchemaIndex::StructuralField;

  NativeLanceMetadata(
      dwio::common::BufferedInput& input,
      memory::MemoryPool& pool,
      std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter =
          defaultNativeLanceTypeAdapter());

  const Footer& footer() const {
    return NativeLanceFileMetadata::footer();
  }

  uint64_t numRows() const {
    return NativeLanceFileMetadata::numRows();
  }

  const RowTypePtr& rowType() const {
    return NativeLanceSchemaIndex::rowType();
  }

  std::string_view columnLogicalType(uint32_t columnIndex) const {
    return columnLogicalTypes_.at(columnIndex);
  }

  uint32_t physicalColumnIndex(uint32_t columnIndex) const {
    return physicalColumnIndices_.at(columnIndex);
  }

  const std::vector<uint32_t>& physicalColumnIndices() const {
    return NativeLanceSchemaIndex::physicalColumnIndices();
  }

  std::string_view physicalColumnLogicalType(uint32_t columnIndex) const {
    return physicalColumnLogicalTypes_.at(columnIndex);
  }

  uint32_t physicalColumnSpan(uint32_t columnIndex) const {
    return physicalColumnSpans_.at(columnIndex);
  }

  const std::vector<std::string>& physicalColumnChildLogicalTypes(
      uint32_t columnIndex) const {
    return physicalColumnChildLogicalTypes_.at(columnIndex);
  }

  const std::vector<BufferDescriptor>& globalBuffers() const {
    return NativeLanceFileMetadata::globalBuffers();
  }

  BufferDescriptor resolveBuffer(
      const ::lance::encodings::Buffer& buffer,
      const ::lance::file::v2::ColumnMetadata& column,
      const ::lance::file::v2::ColumnMetadata::Page& page) const;

  uint32_t numPhysicalColumns() const {
    return footer_.numColumns;
  }

  const ::lance::file::v2::ColumnMetadata& column(
      uint32_t physicalColumnIndex) const;

  /// Loads all column metadata. Prefer loadLogicalColumns for scan paths.
  const std::vector<::lance::file::v2::ColumnMetadata>& columns() const;

  /// Loads the physical metadata required by the specified top-level columns.
  /// Missing columns are submitted together so BufferedInput can coalesce I/O.
  void loadLogicalColumns(const std::vector<uint32_t>& columnIndices) const;

  void loadPhysicalColumns(
      const std::vector<uint32_t>& physicalColumnIndices) const;

  size_t loadedColumnMetadataCount() const;

  const ::lance::encodings::ArrayEncoding& pageEncoding(
      uint32_t physicalColumnIndex,
      int32_t pageIndex) const;

  bool usesStructuralEncoding() const {
    return footer_.majorVersion == 2 && footer_.minorVersion >= 1;
  }

  const ::lance::encodings21::PageLayout& pageLayout(
      uint32_t physicalColumnIndex,
      int32_t pageIndex) const;

  /// Returns cumulative physical row offsets with one trailing end offset.
  const std::vector<uint64_t>& pageRowStarts(
      uint32_t physicalColumnIndex) const {
    return NativeLanceColumnMetadataLoader::pageRowStarts(physicalColumnIndex);
  }

  uint32_t leafPhysicalColumnIndex(uint32_t fieldId) const {
    return leafPhysicalColumnIndices_.at(fieldId);
  }

  const StructuralField& structuralField(uint32_t columnIndex) const {
    return NativeLanceSchemaIndex::structuralField(columnIndex);
  }

  const NativeLanceFileMetadata& fileMetadata() const {
    return *this;
  }

  const NativeLanceSchemaIndex& schemaIndex() const {
    return *this;
  }

  const NativeLanceColumnMetadataLoader& columnMetadataLoader() const {
    return *this;
  }

  bool isBlobColumn(uint32_t physicalColumnIndex) const {
    return NativeLanceColumnMetadataLoader::isBlobColumn(physicalColumnIndex);
  }

  /// Returns top-level row ranges owned by a byte range. A page belongs to
  /// the split containing the first byte of its first page buffer, matching
  /// the split ownership rule used by the other Bolt DWIO readers.
  std::vector<std::pair<uint64_t, uint64_t>> rowRangesForFileRange(
      uint64_t offset,
      uint64_t limit) const;

 private:
  struct DeferredOpenTag {};

  friend class NativeLanceFileOpenTask;
  BufferPtr read(uint64_t offset, uint64_t length) const;
  NativeLanceMetadata(
      dwio::common::BufferedInput& input,
      memory::MemoryPool& pool,
      std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter,
      DeferredOpenTag);
  void readFooter();
  void readGlobalBufferIndex();
  void readSchema();
  void readColumnMetadataIndex();
  void buildSchemaIndex();
  void validateOpenState() const;
};

} // namespace bytedance::bolt::lance::reader
