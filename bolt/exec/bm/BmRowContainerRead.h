#pragma once

#include "bolt/common/base/CompareFlags.h"
#include "bolt/common/memory/bm/BufferHandle.h"
#include "bolt/exec/bm/BmRowContainerTypes.h"
#include "bolt/vector/FlatVector.h"

#include <folly/Range.h>

#include <cstdint>
#include <unordered_set>
#include <vector>

namespace bytedance::bolt::exec::bm {

class BmRowContainer;

// RowId-driven reader for working sets that should not be fully loaded at once.
//
// WindowReadSession owns no BufferHandle. Each load delegates to the owning
// BmRowContainer, which pins required chunk blocks into BlockRef::handle and
// keeps the returned pointers valid until those chunks are spilled or released.
class WindowReadSession {
 public:
  WindowReadSession() = default;

  // Loads all chunks needed by rows and returns resident row pointers. The
  // pointers remain valid until the owning BmRowContainer spills or releases the
  // corresponding chunks/segments.
  std::vector<char*> loadRows(folly::Range<const RowId*> rows);

  // Explicit single-row slow path. Prefer loadRows() for operator hot paths.
  char* loadRow(const RowId& row);

 private:
  WindowReadSession(
      BmRowContainer* container,
      std::vector<SegmentId> segments);

  BmRowContainer* container_{nullptr};
  // Segment read order requested by the caller.
  std::vector<SegmentId> segmentOrder_;
  // Fast validation set for RowIds submitted to this session.
  std::unordered_set<SegmentId> segments_;

  friend class BmRowContainer;
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
  // chunks. If releaseAfterRead is true, chunks exhausted by the previous next()
  // call are released at the beginning of the following next() call.
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
