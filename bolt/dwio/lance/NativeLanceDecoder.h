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

#include <folly/Range.h>

#include <mutex>
#include <optional>

#include "bolt/dwio/lance/NativeLanceBlobResolver.h"
#include "bolt/dwio/lance/NativeLanceDecodedPageCache.h"
#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/dwio/lance/NativeLanceReadPlan.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

/// Decodes Lance v2.0-v2.3 page encodings directly into Bolt vectors.
///
/// Reads are issued as ranges against the original BufferedInput. No Rust or
/// Arrow objects participate in this path, and all output storage belongs to
/// the supplied Bolt memory pool.
class NativeLanceDecoder {
 public:
  NativeLanceDecoder(
      dwio::common::BufferedInput& input,
      const NativeLanceMetadata& metadata,
      memory::MemoryPool& pool,
      bool enableDecodedPageCache = false,
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver = nullptr,
      NativeLanceReadPlan::Options readPlanOptions = {},
      std::shared_ptr<NativeLanceDecodedPageCache> decodedPageCache = nullptr);

  ~NativeLanceDecoder();

  void cancelReadPlan();

  /// Schedules the first-stage ranges for all requested columns together.
  /// BufferedInput can coalesce these requests or dispatch them asynchronously.
  void prefetchColumns(
      const std::vector<uint32_t>& columnIndices,
      uint64_t rowStart,
      uint64_t rowCount) const;

  void planColumns(
      const std::vector<uint32_t>& columnIndices,
      uint64_t rowStart,
      uint64_t rowCount) const;

  void submitReadPlan() const;

  void materializeReadPlan() const;

  /// Returns true when independent columns can be decoded concurrently
  /// without sharing mutable asynchronous read-plan state.
  bool supportsConcurrentDecoding() const {
    return input_.supportSyncLoad();
  }

  /// Schedules one physical column and all fixed-row-count descendants. This
  /// is used for the second stage of list and map reads after their offsets
  /// reveal the child row range.
  void prefetchPhysicalColumn(
      const TypePtr& type,
      uint32_t physicalColumnIndex,
      uint64_t rowStart,
      uint64_t rowCount) const;

  void prefetchPhysicalColumns(
      const std::vector<std::pair<TypePtr, uint32_t>>& columns,
      uint64_t rowStart,
      uint64_t rowCount) const;

  void planPhysicalColumns(
      const std::vector<std::pair<TypePtr, uint32_t>>& columns,
      uint64_t rowStart,
      uint64_t rowCount) const;

  bool hasDecodedColumnPage(
      uint32_t columnIndex,
      uint64_t rowStart,
      uint64_t rowCount) const;

  bool hasCompressedColumn(
      uint32_t columnIndex,
      uint64_t rowStart,
      uint64_t rowCount) const;

  bool hasDecodedPhysicalPage(
      uint32_t physicalColumnIndex,
      uint64_t rowStart,
      uint64_t rowCount) const;

  VectorPtr decodeColumn(
      uint32_t columnIndex,
      uint64_t rowStart,
      uint64_t rowCount) const;

  /// Decodes sorted, batch-relative row numbers into a compact vector.
  /// Consecutive rows are coalesced into one range read.
  VectorPtr decodeSelectedRows(
      uint32_t columnIndex,
      uint64_t batchRowStart,
      folly::Range<const vector_size_t*> rows) const;

  VectorPtr decodePhysicalColumn(
      const TypePtr& type,
      std::string_view logicalType,
      uint32_t physicalColumnIndex,
      uint64_t rowStart,
      uint64_t rowCount,
      const std::vector<uint32_t>& arrayDimensions = {}) const;

 private:
  struct PageRange {
    int32_t pageIndex;
    uint64_t pageRowStart;
    uint64_t pageRowCount;
  };

  std::optional<PageRange> singlePageRange(
      uint32_t physicalColumnIndex,
      uint64_t rowStart,
      uint64_t rowCount) const;
  bool shouldCacheDecodedPage(
      const TypePtr& type,
      uint32_t physicalColumnIndex,
      const PageRange& page) const;
  VectorPtr decodePhysicalColumnNoCache(
      const TypePtr& type,
      std::string_view logicalType,
      uint32_t physicalColumnIndex,
      uint64_t rowStart,
      uint64_t rowCount,
      const std::vector<uint32_t>& arrayDimensions = {}) const;
  VectorPtr decodeStructuralField(
      const NativeLanceMetadata::StructuralField& field,
      uint64_t rowStart,
      uint64_t rowCount) const;
  VectorPtr getDecodedPage(
      const TypePtr& type,
      std::string_view logicalType,
      uint32_t physicalColumnIndex,
      const PageRange& page) const;
  void enqueuePhysicalColumn(
      const TypePtr& type,
      uint32_t physicalColumnIndex,
      uint64_t rowStart,
      uint64_t rowCount,
      const std::vector<uint32_t>& arrayDimensions = {}) const;
  void enqueueStructuralField(
      const NativeLanceMetadata::StructuralField& field,
      uint64_t rowStart,
      uint64_t rowCount) const;
  void scheduleRead(uint64_t offset, uint64_t length) const;
  BufferPtr read(uint64_t offset, uint64_t length) const;

  dwio::common::BufferedInput& input_;
  const NativeLanceMetadata& metadata_;
  memory::MemoryPool& pool_;
  const bool enableDecodedPageCache_;
  const std::shared_ptr<const NativeLanceBlobResolver> blobResolver_;
  mutable NativeLanceReadPlan readPlan_;
  const std::shared_ptr<NativeLanceDecodedPageCache> decodedPageCache_;
  // Offset-dependent columns may independently schedule their second-stage
  // payload reads from decoding workers. Keep each plan mutation atomic.
  mutable std::recursive_mutex readPlanMutex_;
};

} // namespace bytedance::bolt::lance::reader
