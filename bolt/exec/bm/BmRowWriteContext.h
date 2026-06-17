#pragma once

#include "bolt/exec/bm/BmRowContainerPublicTypes.h"

#include <folly/Portability.h>

namespace bytedance::bolt::exec::bm {

class BmRowContainer;
struct BlockRef;
struct ChunkData;
struct SegmentData;

// Location token returned by BmRowContainer::appendRow(). It is only meant for
// immediately storing columns of that row; do not keep it after flush.
class RowWriteContext {
 public:
  RowWriteContext() = default;

  FOLLY_ALWAYS_INLINE char* row() const {
    return row_;
  }

  FOLLY_ALWAYS_INLINE SegmentData* segment() const {
    return segment_;
  }

  FOLLY_ALWAYS_INLINE ChunkData* chunk() const {
    return chunk_;
  }

 private:
  friend class BmRowContainer;

  RowWriteContext(
      SegmentData* segment,
      ChunkData* chunk,
      char* row)
      : segment_(segment),
        chunk_(chunk),
        row_(row) {}

  SegmentData* segment_{nullptr};
  ChunkData* chunk_{nullptr};
  BlockRef* currentHeap_{nullptr};
  BlockId recordedHeapBlock_{kNoBlock};
  char* row_{nullptr};
};

} // namespace bytedance::bolt::exec::bm
