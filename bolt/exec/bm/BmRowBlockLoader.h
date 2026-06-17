#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/exec/bm/BmSegmentCollection.h"

#include <folly/Portability.h>
#include <folly/Range.h>

#include <memory>

namespace bytedance::bolt::exec::bm {

// Pins row/heap blocks through BufferManager and refreshes BlockRef pointers.
//
// BmRowBlockLoader is the only helper that performs bulk block pinning for row
// container read paths. It delegates StringView pointer repair after pinning so
// block residency management and row payload rewriting stay separate.
class BmRowBlockLoader {
 public:
  BmRowBlockLoader(
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      const BmRowLayout* layout,
      BmSegmentCollection* segments);

  void loadSegments(folly::Range<const SegmentId*> segments);

  void loadChunks(folly::Range<ChunkData* const*> chunks);

  void loadChunk(ChunkData& chunk);

 private:
  FOLLY_ALWAYS_INLINE const BmRowLayout& layout() const {
    BOLT_DCHECK_NOT_NULL(layout_);
    return *layout_;
  }

  FOLLY_ALWAYS_INLINE BmSegmentCollection& segments() const {
    BOLT_DCHECK_NOT_NULL(segments_);
    return *segments_;
  }

  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  const BmRowLayout* layout_{nullptr};
  BmSegmentCollection* segments_{nullptr};
};

} // namespace bytedance::bolt::exec::bm
