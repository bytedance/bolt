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
// It does not own memory. Every load delegates to BmRowContainer, which pins the
// required chunk blocks into BlockRef::handle and owns the resulting residency.
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

// Read session for scanning multiple physically ordered segments. It owns no
// block handles; all residency lives in BmRowContainer.
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
