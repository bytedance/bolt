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

#include <functional>
#include <memory>

#include "bolt/dwio/lance/NativeLanceBlobResolver.h"
#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

/// Immutable, page-local metadata used to map row ranges to structural page
/// payload ranges. The plan never owns encoded payload or decoded values.
class NativeLanceStructuralPagePlan {
 public:
  struct Chunk {
    uint64_t dataOffset;
    uint64_t bytes;
    uint64_t itemStart;
    uint64_t items;
    uint64_t rowStart;
    bool hasPreamble;
    bool hasTrailer;
  };

  struct ChunkRange {
    size_t firstChunk;
    size_t lastChunk;
    uint64_t dataOffset;
    uint64_t dataBytes;
    uint64_t itemStart;
    uint64_t decodedItems;
    uint64_t rowStart;
  };

  NativeLanceStructuralPagePlan(
      uint64_t dataBufferOffset,
      uint64_t dataBufferBytes,
      uint64_t pageRows,
      bool hasRepetition,
      std::vector<Chunk> chunks);

  ChunkRange select(uint64_t rowStart, uint64_t rowCount) const;

  std::vector<std::pair<uint64_t, uint64_t>> payloadRanges(
      uint64_t rowStart,
      uint64_t rowCount) const;

  const std::vector<Chunk>& chunks() const {
    return chunks_;
  }

  uint64_t pageRows() const {
    return pageRows_;
  }

 private:
  uint64_t dataBufferOffset_;
  uint64_t dataBufferBytes_;
  uint64_t pageRows_;
  bool hasRepetition_;
  std::vector<Chunk> chunks_;
};

/// Parses MiniBlock chunk metadata and the optional repetition index once.
/// The returned plan is small scan-local state and does not retain page data.
std::shared_ptr<const NativeLanceStructuralPagePlan>
prepareLanceStructuralPagePlan(
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const ::lance::encodings21::PageLayout& layout,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read);

/// Decodes a v2.1-v2.3 structural page. Range-addressable MiniBlock and
/// FullZip pages return only the requested rows; other layouts return the
/// complete page.
VectorPtr decodeLanceStructuralPage(
    const TypePtr& type,
    std::string_view leafLogicalType,
    const std::vector<uint32_t>& fixedSizeDimensions,
    const std::vector<std::string>& packedChildLogicalTypes,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const ::lance::encodings21::PageLayout& layout,
    uint64_t rowStart,
    uint64_t rowCount,
    memory::MemoryPool& pool,
    const std::shared_ptr<const NativeLanceBlobResolver>& blobResolver,
    std::string_view sourceDataFile,
    const std::function<
        void(const std::vector<std::pair<uint64_t, uint64_t>>&)>& prefetch,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read,
    const NativeLanceStructuralPagePlan* plan = nullptr);

bool lanceStructuralPageSupportsRangeRead(
    const TypePtr& type,
    const std::vector<uint32_t>& fixedSizeDimensions,
    const std::vector<std::string>& packedChildLogicalTypes,
    const ::lance::encodings21::PageLayout& layout);

bool lanceStructuralLayoutHasCompression(
    const ::lance::encodings21::PageLayout& layout);

} // namespace bytedance::bolt::lance::reader
