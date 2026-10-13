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

#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <utility>

namespace bytedance::bolt::exec::bm {

namespace {

uint64_t chunkKey(SegmentId segment, ChunkId chunk) {
  return (static_cast<uint64_t>(segment) << 32) | chunk;
}

bool chunkHasPinnedBlocks(const ChunkData& chunkData) {
  if (chunkData.rowBlock.handle.valid()) {
    return true;
  }
  for (const auto& block : chunkData.heapBlocks) {
    if (block.handle.valid()) {
      return true;
    }
  }
  return false;
}

uint64_t releaseLoadedBlock(BlockRef& block) {
  if (!block.handle.valid()) {
    return 0;
  }

  const auto bytes = block.size;
  block.handle = memory::bm::BufferHandle{};
  block.ptr = nullptr;
  return bytes;
}

uint64_t releaseLoadedChunk(ChunkData& chunkData) {
  uint64_t bytes = releaseLoadedBlock(chunkData.rowBlock);
  for (auto& block : chunkData.heapBlocks) {
    bytes += releaseLoadedBlock(block);
  }
  return bytes;
}

} // namespace

ScopedLoadedRows::~ScopedLoadedRows() {
  reset();
}

ScopedLoadedRows::ScopedLoadedRows(ScopedLoadedRows&& other) noexcept
    : session_(std::exchange(other.session_, nullptr)),
      rows_(std::move(other.rows_)),
      chunkKeys_(std::move(other.chunkKeys_)) {
  other.rows_.clear();
  other.chunkKeys_.clear();
}

ScopedLoadedRows& ScopedLoadedRows::operator=(
    ScopedLoadedRows&& other) noexcept {
  if (this != &other) {
    reset();
    session_ = std::exchange(other.session_, nullptr);
    rows_ = std::move(other.rows_);
    chunkKeys_ = std::move(other.chunkKeys_);
    other.rows_.clear();
    other.chunkKeys_.clear();
  }
  return *this;
}

ScopedLoadedRows ScopedLoadedRows::borrowed(std::vector<const char*> rows) {
  ScopedLoadedRows result;
  result.rows_ = std::move(rows);
  return result;
}

void ScopedLoadedRows::reset() noexcept {
  if (session_ != nullptr) {
    session_->releaseRowLeases(chunkKeys_);
    session_ = nullptr;
  }
  rows_.clear();
  chunkKeys_.clear();
}

ReadOnlyWindowReadSession::~ReadOnlyWindowReadSession() {
  CHECK(activeLeases_.empty())
      << "Scoped batches must outlive neither session nor container";
}

ReadOnlyWindowReadSession::ReadOnlyWindowReadSession(
    ReadOnlyWindowReadSession&& other) noexcept {
  *this = std::move(other);
}

ReadOnlyWindowReadSession& ReadOnlyWindowReadSession::operator=(
    ReadOnlyWindowReadSession&& other) noexcept {
  if (this != &other) {
    CHECK(activeLeases_.empty());
    CHECK(other.activeLeases_.empty());
    container_ = std::exchange(other.container_, nullptr);
    segmentOrder_ = std::move(other.segmentOrder_);
    segments_ = std::move(other.segments_);
    loadedChunks_ = std::move(other.loadedChunks_);
    loadedChunkKeys_ = std::move(other.loadedChunkKeys_);
  }
  return *this;
}

ReadOnlyWindowReadSession::ReadOnlyWindowReadSession(
    BmRowContainer* container,
    std::vector<SegmentId> segments)
    : container_(container),
      segmentOrder_(std::move(segments)),
      segments_(segmentOrder_.begin(), segmentOrder_.end()) {}

std::vector<RowId> ReadOnlyWindowReadSession::listRowIds() const {
  BOLT_CHECK_NOT_NULL(container_);
  return container_->listRowIdsForSegments(
      {segmentOrder_.data(), segmentOrder_.size()});
}

std::vector<ChunkData*> ReadOnlyWindowReadSession::chunksForRows(
    folly::Range<const RowId*> rows) const {
  BOLT_CHECK_NOT_NULL(container_);

  std::vector<ChunkData*> chunks;
  chunks.reserve(rows.size());
  std::unordered_set<uint64_t> seenChunks;
  for (const auto& row : rows) {
    BOLT_CHECK(
        segments_.count(row.segmentId) != 0,
        "Row segment {} is not covered by this read session",
        row.segmentId);
    auto& segment = container_->segments_.segmentData(row.segmentId);
    auto& chunk = container_->segments_.chunkForRow(segment, row.rowNumber);
    BOLT_CHECK(
        !chunk.consumed,
        "Cannot load consumed chunk {} in segment {}",
        chunk.meta.id,
        row.segmentId);
    const auto key = chunkKey(row.segmentId, chunk.meta.id);
    if (seenChunks.insert(key).second) {
      chunks.push_back(&chunk);
    }
  }
  return chunks;
}

ScopedLoadedRows ReadOnlyWindowReadSession::loadRowsScoped(
    folly::Range<const RowId*> rows) {
  auto chunks = chunksForRows(rows);
  ScopedLoadedRows result;
  result.rows_.resize(rows.size());
  result.chunkKeys_.reserve(chunks.size());
  result.session_ = chunks.empty() ? nullptr : this;
  for (auto* chunk : chunks) {
    const auto key = chunkKey(chunk->meta.segmentId, chunk->meta.id);
    const auto& segment =
        container_->segments_.segmentData(chunk->meta.segmentId);
    auto [it, inserted] = activeLeases_.try_emplace(
        key,
        ActiveLease{
            chunk,
            0,
            segment.meta.state != SegmentState::kActiveResident &&
                !chunkHasPinnedBlocks(*chunk)});
    ++it->second.count;
    // Capacity is reserved before acquiring leases, so recording a lease
    // cannot throw. The result unwinds all acquired leases if loading fails.
    result.chunkKeys_.push_back(key);
  }

  container_->ensureChunksLoaded({chunks.data(), chunks.size()});
  for (size_t i = 0; i < rows.size(); ++i) {
    result.rows_[i] = container_->segments_.rowPointer(rows[i]);
  }
  return result;
}

ScopedLoadedRows ReadOnlyWindowReadSession::loadRowsScoped(
    folly::Range<const SegmentRowRange*> ranges) {
  auto rows = expandRanges(ranges);
  return loadRowsScoped({rows.data(), rows.size()});
}

void ReadOnlyWindowReadSession::releaseRowLeases(
    const std::vector<uint64_t>& chunkKeys) noexcept {
  for (auto key : chunkKeys) {
    auto it = activeLeases_.find(key);
    CHECK(it != activeLeases_.end());
    CHECK_GT(it->second.count, 0);
    if (--it->second.count == 0) {
      if (it->second.releasePins) {
        releaseLoadedChunk(*it->second.chunk);
      }
      activeLeases_.erase(it);
    }
  }
}

std::vector<const char*> ReadOnlyWindowReadSession::loadRows(
    folly::Range<const RowId*> rows) {
  BOLT_CHECK(
      activeLeases_.empty(),
      "Cannot start raw reads with active scoped batches");
  auto chunks = chunksForRows(rows);
  for (auto* chunk : chunks) {
    const auto key = chunkKey(chunk->meta.segmentId, chunk->meta.id);
    if (loadedChunkKeys_.insert(key).second) {
      loadedChunks_.emplace_back(chunk->meta.segmentId, chunk->meta.id);
    }
  }

  container_->ensureChunksLoaded({chunks.data(), chunks.size()});

  std::vector<const char*> result;
  result.reserve(rows.size());
  for (const auto& row : rows) {
    result.push_back(container_->segments_.rowPointer(row));
  }
  return result;
}

std::vector<const char*> ReadOnlyWindowReadSession::loadRows(
    folly::Range<const SegmentRowRange*> ranges) {
  auto rows = expandRanges(ranges);
  return loadRows({rows.data(), rows.size()});
}

std::vector<RowId> ReadOnlyWindowReadSession::expandRanges(
    folly::Range<const SegmentRowRange*> ranges) const {
  BOLT_CHECK_NOT_NULL(container_);

  std::vector<RowId> rows;
  for (const auto& range : ranges) {
    BOLT_CHECK(
        segments_.count(range.segment) != 0,
        "Range segment {} is not covered by this read session",
        range.segment);
    if (range.count == 0) {
      continue;
    }
    auto& segment = container_->segments_.segmentData(range.segment);
    BOLT_CHECK_LE(
        static_cast<uint64_t>(range.begin) + range.count,
        segment.nextRowNumber,
        "Row range [{}, {}) exceeds segment {} row count {}",
        range.begin,
        range.begin + range.count,
        range.segment,
        segment.nextRowNumber);
    rows.reserve(rows.size() + range.count);
    for (RowNumber offset = 0; offset < range.count; ++offset) {
      rows.push_back(container_->segments_.rowIdForRowNumber(
          segment, range.begin + offset));
    }
  }
  return rows;
}

const char* ReadOnlyWindowReadSession::loadRow(const RowId& row) {
  auto rows = loadRows({&row, 1});
  BOLT_DCHECK_EQ(1, rows.size());
  return rows[0];
}

uint64_t ReadOnlyWindowReadSession::releaseLoadedChunks(uint64_t targetBytes) {
  BOLT_CHECK_NOT_NULL(container_);
  if (loadedChunks_.empty() || targetBytes == 0) {
    return 0;
  }

  uint64_t released = 0;
  for (const auto& [segment, chunk] : loadedChunks_) {
    if (released >= targetBytes) {
      break;
    }
    if (activeLeases_.count(chunkKey(segment, chunk)) != 0) {
      continue;
    }
    auto& segmentData = container_->segments_.segmentData(segment);
    if (segmentData.meta.state == SegmentState::kActiveResident) {
      continue;
    }
    BOLT_CHECK_LT(chunk, segmentData.chunks.size());
    auto& chunkData = *segmentData.chunks[chunk];
    BOLT_CHECK(
        !chunkData.consumed,
        "Cannot release consumed chunk {} in segment {}",
        chunk,
        segment);
    released += releaseLoadedChunk(chunkData);
  }

  std::vector<std::pair<SegmentId, ChunkId>> stillLoaded;
  stillLoaded.reserve(loadedChunks_.size());
  loadedChunkKeys_.clear();
  for (const auto& [segment, chunk] : loadedChunks_) {
    auto& segmentData = container_->segments_.segmentData(segment);
    BOLT_CHECK_LT(chunk, segmentData.chunks.size());
    if (chunkHasPinnedBlocks(*segmentData.chunks[chunk])) {
      stillLoaded.emplace_back(segment, chunk);
      loadedChunkKeys_.insert(chunkKey(segment, chunk));
    }
  }
  loadedChunks_ = std::move(stillLoaded);
  return released;
}

uint64_t ReadOnlyWindowReadSession::evictLoadedChunks(uint64_t targetBytes) {
  BOLT_CHECK_NOT_NULL(container_);
  if (loadedChunks_.empty() || targetBytes == 0) {
    return 0;
  }

  std::vector<std::pair<SegmentId, ChunkId>> evictable;
  evictable.reserve(loadedChunks_.size());
  for (const auto& chunk : loadedChunks_) {
    if (activeLeases_.count(chunkKey(chunk.first, chunk.second)) == 0) {
      evictable.push_back(chunk);
    }
  }
  const auto reclaimed = container_->evictReadOnlyLoadedChunks(
      {evictable.data(), evictable.size()}, targetBytes);

  std::vector<std::pair<SegmentId, ChunkId>> stillLoaded;
  stillLoaded.reserve(loadedChunks_.size());
  loadedChunkKeys_.clear();
  for (const auto& [segment, chunk] : loadedChunks_) {
    auto& segmentData = container_->segments_.segmentData(segment);
    BOLT_CHECK_LT(chunk, segmentData.chunks.size());
    if (chunkHasPinnedBlocks(*segmentData.chunks[chunk])) {
      stillLoaded.emplace_back(segment, chunk);
      loadedChunkKeys_.insert(chunkKey(segment, chunk));
    }
  }
  loadedChunks_ = std::move(stillLoaded);
  return reclaimed;
}

} // namespace bytedance::bolt::exec::bm
