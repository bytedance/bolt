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

#include "bolt/exec/bm/BmRowContainerPublicTypes.h"
#include "bolt/exec/bm/BmScopedLoadedRows.h"
#include "bolt/vector/TypeAliases.h"

#include <folly/Range.h>

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace bytedance::bolt::exec::bm {

class BmRowContainer;
struct ChunkData;

// Full segment reader. It keeps all bound segments loaded and does not provide
// loaded-block eviction. Use ReadOnlyWindowReadSession when the working set
// should be released between windows.
class BulkReadSession {
 public:
  BulkReadSession() = default;

  // Loads all live chunks in the bound segments without materializing a row
  // pointer vector. Consumed chunks are ignored because their rows are no
  // longer readable by callers.
  void load();

  std::vector<char*> loadRows();
  std::vector<const char*> loadRows(
      folly::Range<const SegmentRowRange*> ranges);

 private:
  BulkReadSession(BmRowContainer* container, std::vector<SegmentId> segments);

  BmRowContainer* container_{nullptr};
  std::vector<SegmentId> segmentOrder_;
  std::unordered_set<SegmentId> segments_;
  bool loaded_{false};

  friend class BmRowContainer;
};

// RowId-driven read-only reader for working sets that should not be fully kept
// pinned. Each load pins required chunks through the owning BmRowContainer.
// releaseLoadedChunks() drops the session's pins so BufferManager can reclaim
// the chunks later under memory pressure. evictLoadedChunks() remains available
// for explicit reclaim paths that want to release resident memory immediately.
// Both operations are intentionally chunk-granular: a chunk's row block and
// heap blocks move together so StringView rebase metadata can stay synchronized
// with the heap base currently referenced by row-block StringViews.
// Lease counts are intentionally session-local. Callers must not overlap raw
// or scoped reads from multiple ReadOnlyWindowReadSessions bound to the same
// BmRowContainer; cross-session pin ownership is not supported.
class ReadOnlyWindowReadSession {
 public:
  ReadOnlyWindowReadSession() = default;
  ~ReadOnlyWindowReadSession();
  // A session can only move when it has no outstanding scoped batches.
  ReadOnlyWindowReadSession(ReadOnlyWindowReadSession&& other) noexcept;
  ReadOnlyWindowReadSession& operator=(
      ReadOnlyWindowReadSession&& other) noexcept;
  ReadOnlyWindowReadSession(const ReadOnlyWindowReadSession&) = delete;
  ReadOnlyWindowReadSession& operator=(const ReadOnlyWindowReadSession&) =
      delete;

  std::vector<RowId> listRowIds() const;

  ScopedLoadedRows loadRowsScoped(folly::Range<const RowId*> rows);
  ScopedLoadedRows loadRowsScoped(folly::Range<const SegmentRowRange*> ranges);

  /// Loads the leading rows of 'range' that belong to one physical chunk.
  /// The returned ScopedLoadedRows owns the chunk lease. Its row pointers
  /// remain valid until the ScopedLoadedRows object is destroyed.
  ScopedLoadedRows loadChunkBoundedRowsScoped(const SegmentRowRange& range);

  // Legacy raw-pointer reads cannot start while a scoped batch is active.
  std::vector<const char*> loadRows(folly::Range<const RowId*> rows);
  std::vector<const char*> loadRows(
      folly::Range<const SegmentRowRange*> ranges);

  const char* loadRow(const RowId& row);

  uint64_t releaseLoadedChunks(uint64_t targetBytes = kUnlimitedBytes);

  uint64_t evictLoadedChunks(uint64_t targetBytes = kUnlimitedBytes);

 private:
  ReadOnlyWindowReadSession(
      BmRowContainer* container,
      std::vector<SegmentId> segments);

  std::vector<ChunkData*> chunksForRows(folly::Range<const RowId*> rows) const;
  std::vector<RowId> expandRanges(
      folly::Range<const SegmentRowRange*> ranges) const;
  void releaseRowLeases(const std::vector<uint64_t>& chunkKeys) noexcept;

  struct ActiveLease {
    ChunkData* chunk;
    uint64_t count{0};
    // Do not release pins borrowed from the resident or legacy reader owner.
    bool releasePins;
  };

  BmRowContainer* container_{nullptr};
  // Segment read order requested by the caller.
  std::vector<SegmentId> segmentOrder_;
  // Fast validation set for RowIds submitted to this session.
  std::unordered_set<SegmentId> segments_;
  std::vector<std::pair<SegmentId, ChunkId>> loadedChunks_;
  std::unordered_set<uint64_t> loadedChunkKeys_;
  std::unordered_map<uint64_t, ActiveLease> activeLeases_;

  friend class BmRowContainer;
  friend class ScopedLoadedRows;
};

// Read session for scanning multiple physically ordered segments.
//
// MergeReadSession owns cursor state, not block handles. It asks BmRowContainer
// to pin each current chunk and, by default, releases a chunk only after the
// next() call that returned its last row has completed and the following next()
// call begins. That keeps the previous batch's returned pointers valid while
// still avoiding future re-spill of already consumed chunks.
class MergeReadSession {
 public:
  MergeReadSession() = default;
  ~MergeReadSession();
  MergeReadSession(MergeReadSession&& other) noexcept;
  MergeReadSession& operator=(MergeReadSession&& other) noexcept;
  MergeReadSession(const MergeReadSession&) = delete;
  MergeReadSession& operator=(const MergeReadSession&) = delete;

  // Appends up to maxRows merged row pointers to rows. Returns false at EOF.
  // Returned pointers are valid until BmRowContainer spills or releases their
  // chunks. If releaseAfterRead is true, chunks exhausted by the previous
  // next() call are released at the beginning of the following next() call.
  bool next(std::vector<char*>& rows, vector_size_t maxRows);

 private:
  MergeReadSession(
      BmRowContainer* container,
      std::vector<SegmentId> segments,
      bool releaseAfterRead);

  struct Cursor {
    SegmentId segment{0};
    size_t chunkIndex{0};
    uint32_t rowIndexInChunk{0};
    char* currentRow{nullptr};
  };

  bool loadCursor(Cursor& cursor);
  void advanceCursor(Cursor& cursor);
  int32_t compareCursors(const Cursor& left, const Cursor& right) const;
  size_t bestCursor() const;
  void releasePendingChunks();

  BmRowContainer* container_{nullptr};
  std::vector<Cursor> cursors_;
  std::unordered_set<SegmentId> segments_;
  bool releaseAfterRead_{false};
  std::vector<std::pair<SegmentId, ChunkId>> pendingRelease_;

  friend class BmRowContainer;
};

} // namespace bytedance::bolt::exec::bm
