#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/bm/BmSegmentCollection.h"

#include <folly/Portability.h>

#include <vector>

namespace bytedance::bolt::exec::bm {

// Copies an already-resident row into another segment. Used by reordered
// segment materialization so merge cursors can scan rows in physical order.
class BmRowCopier {
 public:
  BmRowCopier(
      const std::vector<TypePtr>* types,
      const BmRowLayout* layout,
      BmSegmentCollection* segments);

  char* copyRowToSegment(SegmentData& segment, const char* source);

 private:
  FOLLY_ALWAYS_INLINE const std::vector<TypePtr>& types() const {
    BOLT_DCHECK_NOT_NULL(types_);
    return *types_;
  }

  FOLLY_ALWAYS_INLINE const BmRowLayout& layout() const {
    BOLT_DCHECK_NOT_NULL(layout_);
    return *layout_;
  }

  FOLLY_ALWAYS_INLINE BmSegmentCollection& segments() const {
    BOLT_DCHECK_NOT_NULL(segments_);
    return *segments_;
  }

  const std::vector<TypePtr>* types_{nullptr};
  const BmRowLayout* layout_{nullptr};
  BmSegmentCollection* segments_{nullptr};
};

} // namespace bytedance::bolt::exec::bm
