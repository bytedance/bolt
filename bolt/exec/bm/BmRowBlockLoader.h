#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/exec/bm/BmRowStorage.h"

#include <folly/Portability.h>
#include <folly/Range.h>

#include <memory>
#include <unordered_map>
#include <vector>

namespace bytedance::bolt::exec::bm {

class BmRowBlockLoader {
 public:
  BmRowBlockLoader(
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      const BmRowLayout* layout,
      BmRowStorage* storage);

  std::vector<memory::bm::BufferHandle> pinSegments(
      folly::Range<const SegmentId*> segments,
      BulkLoadMetrics* metrics = nullptr);

  std::vector<memory::bm::BufferHandle> pinChunk(
      SegmentData& segment,
      const DataChunkMeta& chunk);

 private:
  void rebaseStringViews(
      SegmentData& segment,
      const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
          heapRebases,
      BulkLoadMetrics* metrics = nullptr);

  void rebaseChunk(
      SegmentData& segment,
      const DataChunkMeta& chunk,
      const std::unordered_map<BlockId, std::pair<uintptr_t, uintptr_t>>&
          heapRebases,
      BulkLoadMetrics* metrics = nullptr);

  FOLLY_ALWAYS_INLINE const BmRowLayout& layout() const {
    BOLT_CHECK_NOT_NULL(layout_);
    return *layout_;
  }

  FOLLY_ALWAYS_INLINE BmRowStorage& storage() const {
    BOLT_CHECK_NOT_NULL(storage_);
    return *storage_;
  }

  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  const BmRowLayout* layout_{nullptr};
  BmRowStorage* storage_{nullptr};
};

} // namespace bytedance::bolt::exec::bm
