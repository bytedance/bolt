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
  RowId id;
  char* ptr{nullptr};
};

struct RowWindow {
  folly::Range<const RowView*> rows;
};

class BulkReadSession {
 public:
  BulkReadSession() = default;

  ReadMode mode() const {
    return mode_;
  }

  LoadAllResult tryLoadAll(
      std::vector<char*>& rows,
      std::vector<RowId>& rowIds);

  RowWindow loadRows(folly::Range<const RowId*> rows);

  char* loadRow(const RowId& row);

 private:
  BulkReadSession(
      BmRowContainer* container,
      ReadMode mode,
      std::vector<memory::bm::BufferHandle> pins,
      std::vector<SegmentId> segments,
      ReadSessionOptions options);

  BmRowContainer* container_{nullptr};
  ReadMode mode_{ReadMode::kFullyResident};
  std::vector<memory::bm::BufferHandle> pins_;
  std::vector<memory::bm::BufferHandle> windowPins_;
  std::vector<RowView> rowViews_;
  std::vector<SegmentId> segmentOrder_;
  std::unordered_set<SegmentId> segments_;
  ReadSessionOptions options_;

  friend class BmRowContainer;
};

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
      ReorderedRunId run,
      ReadSessionOptions options);

  void loadCurrent();

  BmRowContainer* container_{nullptr};
  ReorderedRunId run_{0};
  ReadSessionOptions options_;
  uint64_t index_{0};
  char* currentRow_{nullptr};
  std::vector<memory::bm::BufferHandle> pins_;

  friend class MergeReadSession;
};

class MergeReadSession {
 public:
  MergeReadSession() = default;

  SegmentCursor cursor(ReorderedRunId run);

  int32_t compareCurrentRows(
      const SegmentCursor& left,
      const SegmentCursor& right,
      const std::vector<CompareFlags>& flags);

 private:
  MergeReadSession(
      BmRowContainer* container,
      std::vector<ReorderedRunId> runs,
      ReadSessionOptions options);

  BmRowContainer* container_{nullptr};
  std::unordered_set<ReorderedRunId> runs_;
  ReadSessionOptions options_;

  friend class BmRowContainer;
};

} // namespace bytedance::bolt::exec::bm
