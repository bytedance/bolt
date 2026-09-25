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

#include <map>
#include <mutex>
#include <unordered_map>

#include "bolt/dwio/lance/NativeLanceBlobResolver.h"
#include "bolt/dwio/lance/NativeLanceColumnRequest.h"
#include "bolt/dwio/lance/NativeLanceLegacyPageReader.h"
#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/dwio/lance/NativeLancePageReader.h"
#include "bolt/dwio/lance/NativeLanceReadScheduler.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

/// Decodes Lance v2.0-v2.3 page encodings directly into Bolt vectors.
///
/// Reads are issued as ranges against the original BufferedInput. No Rust or
/// Arrow objects participate in this path, and all output storage belongs to
/// the supplied Bolt memory pool.
class NativeLancePageSource final {
 public:
  NativeLancePageSource(
      dwio::common::BufferedInput& input,
      const NativeLanceMetadata& metadata,
      memory::MemoryPool& pool,
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver = nullptr,
      NativeLanceReadScheduler::Options readSchedulerOptions = {});

  ~NativeLancePageSource();

  const NativeLanceMetadata& metadata() const {
    return metadata_;
  }

  memory::MemoryPool& pool() const {
    return pool_;
  }

  void cancel();

  /// Schedules the first-stage ranges for all requested columns together.
  /// BufferedInput can coalesce these requests or dispatch them asynchronously.
  void prefetchColumns(
      const std::vector<uint32_t>& columnIndices,
      uint64_t rowStart,
      uint64_t rowCount) const;

  void planColumns(
      const std::vector<uint32_t>& columnIndices,
      const NativeLanceColumnRequest& request) const;

  void planColumns(
      const std::vector<uint32_t>& columnIndices,
      uint64_t rowStart,
      uint64_t rowCount) const;

  void submitReadPlan() const;

  void materializeReadPlan() const;

  void finishBatch() const;

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

  bool hasCompressedColumn(
      uint32_t columnIndex,
      uint64_t rowStart,
      uint64_t rowCount) const;

  VectorPtr decodePhysicalColumn(
      const TypePtr& type,
      std::string_view logicalType,
      uint32_t physicalColumnIndex,
      uint64_t rowStart,
      uint64_t rowCount,
      const std::vector<uint32_t>& arrayDimensions = {}) const;

 private:
  struct StructuralPageRangeRequest {
    uint32_t physicalColumnIndex;
    int32_t pageIndex;
    uint64_t localRowStart;
    uint64_t rowCount;
  };

  void enqueueLogicalColumn(
      uint32_t columnIndex,
      uint64_t rowStart,
      uint64_t rowCount,
      std::vector<StructuralPageRangeRequest>* structuralRequests) const;

  void enqueuePhysicalColumn(
      const TypePtr& type,
      uint32_t physicalColumnIndex,
      uint64_t rowStart,
      uint64_t rowCount,
      const std::vector<uint32_t>& arrayDimensions,
      std::vector<StructuralPageRangeRequest>* structuralRequests) const;
  void enqueueStructuralField(
      const NativeLanceMetadata::StructuralField& field,
      uint64_t rowStart,
      uint64_t rowCount,
      std::vector<StructuralPageRangeRequest>* structuralRequests) const;
  void scheduleStructuralPayloads(
      const std::vector<StructuralPageRangeRequest>& requests) const;
  std::shared_ptr<const NativeLanceStructuralPagePlan>
  getOrCreateStructuralPagePlan(
      const StructuralPageRangeRequest& request) const;
  std::shared_ptr<const NativeLanceStructuralPagePlan> findStructuralPagePlan(
      NativeLancePageKey key) const;
  void releaseStructuralPagePlansBefore(NativeLancePageKey key) const;
  void releaseStructuralPagePlan(NativeLancePageKey key) const;
  void scheduleRead(uint64_t offset, uint64_t length) const;
  void scheduleCompressedRead(
      std::string_view scheme,
      uint64_t offset,
      uint64_t length) const;
  BufferPtr read(uint64_t offset, uint64_t length) const;
  BufferPtr readCompressedRange(
      NativeLancePageKey pageKey,
      std::string_view scheme,
      uint64_t compressedOffset,
      uint64_t compressedLength,
      uint64_t decodedOffset,
      uint64_t decodedLength) const;
  void releaseLegacyPageReadersBefore(NativeLancePageKey pageKey) const;

  struct CompressedBufferKey {
    uint64_t offset;
    uint64_t length;

    bool operator==(const CompressedBufferKey& other) const {
      return offset == other.offset && length == other.length;
    }
  };

  struct CompressedBufferKeyHash {
    size_t operator()(const CompressedBufferKey& key) const {
      return std::hash<uint64_t>{}(key.offset) ^
          (std::hash<uint64_t>{}(key.length) << 1);
    }
  };

  struct LegacyPageReaderEntry {
    NativeLancePageKey page;
    std::shared_ptr<NativeLanceLegacyPageReader> reader;
  };

  dwio::common::BufferedInput& input_;
  const NativeLanceMetadata& metadata_;
  memory::MemoryPool& pool_;
  const std::shared_ptr<const NativeLanceBlobResolver> blobResolver_;
  const uint64_t compressedStreamChunkBytes_;
  mutable NativeLanceReadScheduler readScheduler_;
  // Offset-dependent columns may independently schedule their second-stage
  // payload reads from decoding workers. Keep each plan mutation atomic.
  mutable std::recursive_mutex readPlanMutex_;
  mutable std::mutex legacyPageReadersMutex_;
  mutable std::unordered_map<
      CompressedBufferKey,
      LegacyPageReaderEntry,
      CompressedBufferKeyHash>
      legacyPageReaders_;
  mutable std::vector<std::map<int32_t, std::vector<CompressedBufferKey>>>
      legacyPageReaderKeys_;
  mutable std::mutex structuralPagePlansMutex_;
  mutable std::vector<
      std::map<int32_t, std::shared_ptr<const NativeLanceStructuralPagePlan>>>
      structuralPagePlans_;
};

} // namespace bytedance::bolt::lance::reader
