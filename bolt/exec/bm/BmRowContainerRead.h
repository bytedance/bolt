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

class BulkReadSession {
 public:
  BulkReadSession() = default;

  ReadMode mode() const {
    return mode_;
  }

  folly::Range<char* const*> resolveRows(
      folly::Range<const RowId*> rows,
      std::vector<char*>& result);

  void extractColumn(
      folly::Range<const RowId*> rows,
      int32_t column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false);

  void extractNulls(
      folly::Range<const RowId*> rows,
      int32_t column,
      const BufferPtr& result);

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

  RowId currentRowId() const {
    return currentRowId_;
  }

  const char* currentRow() const {
    return currentRow_;
  }

  void advance();

 private:
  SegmentCursor(
      BmRowContainer* container,
      SortedRunId run,
      ReadSessionOptions options);

  void loadCurrent();

  BmRowContainer* container_{nullptr};
  SortedRunId run_{0};
  ReadSessionOptions options_;
  uint64_t index_{0};
  RowId currentRowId_;
  char* currentRow_{nullptr};
  std::vector<memory::bm::BufferHandle> pins_;

  friend class MergeReadSession;
};

class MergeReadSession {
 public:
  MergeReadSession() = default;

  SegmentCursor cursor(SortedRunId run);

  int32_t compareCurrentRows(
      const SegmentCursor& left,
      const SegmentCursor& right,
      const std::vector<CompareFlags>& flags);

 private:
  MergeReadSession(
      BmRowContainer* container,
      std::vector<SortedRunId> runs,
      ReadSessionOptions options);

  BmRowContainer* container_{nullptr};
  std::unordered_set<SortedRunId> runs_;
  ReadSessionOptions options_;

  friend class BmRowContainer;
};

} // namespace bytedance::bolt::exec::bm
