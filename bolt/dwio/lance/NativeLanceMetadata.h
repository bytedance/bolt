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
#include <functional>
#include <list>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"
#include "bolt/dwio/lance/proto/lance_encodings_v2_0.pb.h"
#include "bolt/dwio/lance/proto/lance_encodings_v2_1.pb.h"
#include "bolt/dwio/lance/proto/lance_file_v2.pb.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::lance::reader {

/// Metadata needed to plan native reads of Lance v2.0-v2.3 files.
///
/// The object deliberately retains offsets rather than page payloads. Payloads
/// are requested later through BufferedInput so Bolt remains in control of
/// coalescing, caching, prefetch, accounting, and cancellation.
class NativeLanceMetadata {
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
  };

  struct DebugStats {
    uint64_t decompressedCacheHits{0};
    uint64_t decompressedCacheMisses{0};
    uint64_t compressedBytesRead{0};
    uint64_t decompressedBytesProduced{0};
  };

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

  NativeLanceMetadata(
      dwio::common::BufferedInput& input,
      memory::MemoryPool& pool,
      std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter =
          defaultNativeLanceTypeAdapter());

  const Footer& footer() const {
    return footer_;
  }

  uint64_t numRows() const {
    return numRows_;
  }

  const RowTypePtr& rowType() const {
    return rowType_;
  }

  std::string_view columnLogicalType(uint32_t columnIndex) const {
    return columnLogicalTypes_.at(columnIndex);
  }

  uint32_t physicalColumnIndex(uint32_t columnIndex) const {
    return physicalColumnIndices_.at(columnIndex);
  }

  const std::vector<uint32_t>& physicalColumnIndices() const {
    return physicalColumnIndices_;
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
    return globalBuffers_;
  }

  const std::vector<::lance::file::v2::ColumnMetadata>& columns() const {
    return columns_;
  }

  const ::lance::encodings::ArrayEncoding& pageEncoding(
      uint32_t physicalColumnIndex,
      int32_t pageIndex) const {
    return pageEncodings_.at(physicalColumnIndex).at(pageIndex);
  }

  bool usesStructuralEncoding() const {
    return footer_.majorVersion == 2 && footer_.minorVersion >= 1;
  }

  const ::lance::encodings21::PageLayout& pageLayout(
      uint32_t physicalColumnIndex,
      int32_t pageIndex) const {
    return pageLayouts_.at(physicalColumnIndex).at(pageIndex);
  }

  uint32_t leafPhysicalColumnIndex(uint32_t fieldId) const {
    return leafPhysicalColumnIndices_.at(fieldId);
  }

  const StructuralField& structuralField(uint32_t columnIndex) const {
    return structuralFields_.at(columnIndex);
  }

  BufferPtr getCachedDecompressedBuffer(
      BufferDescriptor descriptor,
      const std::function<BufferPtr()>& load,
      bool* hit = nullptr) const;

  bool hasCachedDecompressedBuffer(BufferDescriptor descriptor) const;

  DebugStats debugStats() const {
    std::lock_guard<std::mutex> guard(decompressedBufferCacheMutex_);
    return debugStats_;
  }

  bool isBlobColumn(uint32_t physicalColumnIndex) const {
    return blobColumns_.at(physicalColumnIndex);
  }

  /// Returns top-level row ranges owned by a byte range. A page belongs to
  /// the split containing the first byte of its first page buffer, matching
  /// the split ownership rule used by the other Bolt DWIO readers.
  std::vector<std::pair<uint64_t, uint64_t>> rowRangesForFileRange(
      uint64_t offset,
      uint64_t limit) const;

 private:
  struct BufferDescriptorHash {
    size_t operator()(BufferDescriptor descriptor) const {
      return std::hash<uint64_t>{}(descriptor.offset) ^
          (std::hash<uint64_t>{}(descriptor.length) << 1);
    }
  };

  struct DecompressedBufferCacheEntry {
    BufferPtr buffer;
    std::list<BufferDescriptor>::iterator lruPosition;
  };

  friend bool operator==(
      const BufferDescriptor& lhs,
      const BufferDescriptor& rhs) {
    return lhs.offset == rhs.offset && lhs.length == rhs.length;
  }

  BufferPtr read(uint64_t offset, uint64_t length) const;
  void evictDecompressedBuffersLocked() const;

  dwio::common::BufferedInput& input_;
  memory::MemoryPool& pool_;
  std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter_;
  Footer footer_;
  uint64_t numRows_{0};
  RowTypePtr rowType_;
  std::vector<std::string> columnLogicalTypes_;
  std::vector<uint32_t> physicalColumnIndices_;
  std::vector<std::string> physicalColumnLogicalTypes_;
  std::vector<uint32_t> physicalColumnSpans_;
  std::vector<std::vector<std::string>> physicalColumnChildLogicalTypes_;
  std::vector<bool> rowAlignedPhysicalColumns_;
  std::vector<BufferDescriptor> globalBuffers_;
  std::vector<::lance::file::v2::ColumnMetadata> columns_;
  std::vector<std::vector<::lance::encodings::ArrayEncoding>> pageEncodings_;
  std::vector<std::vector<::lance::encodings21::PageLayout>> pageLayouts_;
  std::vector<bool> blobColumns_;
  std::unordered_map<uint32_t, uint32_t> leafPhysicalColumnIndices_;
  std::vector<StructuralField> structuralFields_;
  mutable std::mutex decompressedBufferCacheMutex_;
  mutable std::list<BufferDescriptor> decompressedBufferLru_;
  mutable std::unordered_map<
      BufferDescriptor,
      DecompressedBufferCacheEntry,
      BufferDescriptorHash>
      decompressedBufferCache_;
  mutable uint64_t decompressedBufferCacheBytes_{0};
  uint64_t decompressedBufferCacheMaxBytes_{0};
  mutable DebugStats debugStats_;
};

} // namespace bytedance::bolt::lance::reader
