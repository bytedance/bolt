#pragma once

#include "bolt/exec/bm/BmRowContainerTypes.h"

#include <folly/Portability.h>

namespace bytedance::bolt::exec::bm {

class BmRowContainer;

// Location token returned by BmRowContainer::appendRow(). It is only meant for
// immediately storing columns of that row; do not keep it after flush.
class RowWriteContext {
 public:
  RowWriteContext() = default;

  FOLLY_ALWAYS_INLINE char* row() const {
    return row_;
  }

 private:
  friend class BmRowContainer;

  RowWriteContext(
      SegmentId segment,
      ChunkId chunk,
      char* row)
      : segment_(segment),
        chunk_(chunk),
        row_(row) {}

  SegmentId segment_{0};
  ChunkId chunk_{kNoBlock};
  char* row_{nullptr};
};

} // namespace bytedance::bolt::exec::bm
