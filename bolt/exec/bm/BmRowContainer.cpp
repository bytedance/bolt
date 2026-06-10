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
      storage_(
          bufferManager_,
          tag,
          &layout_,
          rowBlockSize,
          heapBlockSize),
      blockLoader_(bufferManager_, &layout_, &storage_),
      rowCopier_(&types_, &layout_, &storage_) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
}

BmRowContainer::RowWriteContext BmRowContainer::appendRow(
    PartitionId partition) {
  auto& segment = storage_.activeSegment(partition);
  auto* row = storage_.newRowInSegment(segment);
  auto& chunk = storage_.currentChunk(segment);
  return RowWriteContext(segment.meta.id, chunk.meta.id, row);
}

} // namespace bytedance::bolt::exec::bm
