#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/exec/bm/BmSegmentCollection.h"

#include <folly/Portability.h>
#include <folly/Range.h>

#include <memory>
#include <unordered_map>
#include <vector>

namespace bytedance::bolt::exec::bm {

// Pins row/heap blocks through BufferManager and repairs raw pointers after
// blocks become resident. This is the only helper that should perform bulk
// block pinning and StringView rebasing for BmRowContainer.
class BmRowBlockLoader {
 public:
  BmRowBlockLoader(
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      const BmRowLayout* layout,
      BmSegmentCollection* storage);

  void loadSegments(
      folly::Range<const SegmentId*> segments,
      BulkLoadMetrics* metrics = nullptr);

  void loadChunks(
      folly::Range<ChunkData* const*> chunks,
      BulkLoadMetrics* metrics = nullptr);

  void loadChunk(ChunkData& chunk);

 private:
  void rebaseStringViews(
      SegmentData& segment,
      const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
          heapRebases,
      BulkLoadMetrics* metrics = nullptr);

  void rebaseChunk(
      ChunkData& chunk,
      const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
          heapRebases,
      BulkLoadMetrics* metrics = nullptr);

  FOLLY_ALWAYS_INLINE const BmRowLayout& layout() const {
    BOLT_DCHECK_NOT_NULL(layout_);
    return *layout_;
  }

  FOLLY_ALWAYS_INLINE BmSegmentCollection& storage() const {
    BOLT_DCHECK_NOT_NULL(storage_);
    return *storage_;
  }

  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  const BmRowLayout* layout_{nullptr};
  BmSegmentCollection* storage_{nullptr};
};

} // namespace bytedance::bolt::exec::bm
