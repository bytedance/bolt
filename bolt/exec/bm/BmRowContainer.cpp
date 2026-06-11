#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {

BmRowContainer::BmRowContainer(
    std::vector<TypePtr> types,
    std::vector<bool> nullable,
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize)
    : types_(std::move(types)),
      layout_(types_, nullable, rowBlockSize),
      bufferManager_(std::move(bufferManager)),
      segments_(
          bufferManager_,
          tag,
          &layout_,
          rowBlockSize,
          heapBlockSize),
      blockLoader_(bufferManager_, &layout_, &segments_),
      rowCopier_(&types_, &layout_, &segments_) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
}

RowWriteContext BmRowContainer::appendRow(PartitionId partition) {
  auto& segment = segments_.activeSegment(partition);
  auto* row = segments_.newRowInSegment(segment);
  auto& chunk = segments_.currentChunk(segment);
  return RowWriteContext(segment.meta.id, chunk.meta.id, row);
}

} // namespace bytedance::bolt::exec::bm
