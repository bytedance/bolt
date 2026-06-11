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

struct RowView {
  // Stable row identity that can be submitted again for a later window read.
  RowId id;
  // Resident row pointer. It is valid until the owning read session loads a new
  // window or is destroyed.
  char* ptr{nullptr};
};

struct RowWindow {
  // Views owned by BulkReadSession. Callers must copy the pointers/ids they need
  // before the next loadRows()/loadRow() call.
  folly::Range<const RowView*> rows;
};

// Reads finalized/flushed segments back through BufferManager.
//
// A session first tries to pin the complete working set with tryLoadAll(). If it
// succeeds, callers can use the returned char* rows directly. If it fails,
// callers keep the returned RowIds and submit access windows back through
// loadRows()/loadRow().
class BulkReadSession {
 public:
  BulkReadSession() = default;

  LoadAllResult tryLoadAll(
      std::vector<char*>& rows,
      std::vector<RowId>& rowIds);

  // Pins all chunks needed by rows and returns resident pointers for this
  // window. Replaces the previous window pins held by the session.
  RowWindow loadRows(folly::Range<const RowId*> rows);

  // Explicit single-row slow path. Prefer loadRows() for operator hot paths.
  char* loadRow(const RowId& row);

 private:
  BulkReadSession(
      BmRowContainer* container,
      std::vector<SegmentId> segments,
      ReadSessionOptions options);

  BmRowContainer* container_{nullptr};
  // Pins for the fully-resident tryLoadAll() result.
  std::vector<memory::bm::BufferHandle> pins_;
  // Pins for the most recent window read.
  std::vector<memory::bm::BufferHandle> windowPins_;
  // Backing storage for RowWindow::rows.
  std::vector<RowView> rowViews_;
  // Segment read order requested by the caller.
  std::vector<SegmentId> segmentOrder_;
  // Fast validation set for RowIds submitted to this session.
  std::unordered_set<SegmentId> segments_;
  ReadSessionOptions options_;

  friend class BmRowContainer;
};

// Cursor over one physically ordered segment. It pins the current chunk and
// advances sequentially inside that chunk before moving to the next chunk.
class SegmentCursor {
 public:
  SegmentCursor() = default;

  bool hasCurrent() const {
    return container_ != nullptr && currentRow_ != nullptr;
  }

  const char* currentRow() const {
    return currentRow_;
  }

  void advance();

 private:
  SegmentCursor(
      BmRowContainer* container,
      SegmentId segment,
      bool releaseAfterRead);

  void loadCurrentChunk();

  BmRowContainer* container_{nullptr};
  SegmentId segment_{0};
  size_t chunkIndex_{0};
  uint32_t rowIndexInChunk_{0};
  bool releaseAfterRead_{false};
  // Resident pointer for the current row. Valid until advance().
  char* currentRow_{nullptr};
  // Pins for the current row's chunk.
  std::vector<memory::bm::BufferHandle> pins_;

  friend class MergeReadSession;
};

// Read session for comparing/scanning multiple physically ordered segments.
class MergeReadSession {
 public:
  MergeReadSession() = default;

  SegmentCursor makeCursor(SegmentId segment);

  int32_t compareCurrentRows(
      const SegmentCursor& left,
      const SegmentCursor& right,
      const std::vector<CompareFlags>& flags);

 private:
  MergeReadSession(
      BmRowContainer* container,
      std::vector<SegmentId> segments,
      bool releaseAfterRead);

  BmRowContainer* container_{nullptr};
  std::unordered_set<SegmentId> segments_;
  bool releaseAfterRead_{false};

  friend class BmRowContainer;
};

} // namespace bytedance::bolt::exec::bm
