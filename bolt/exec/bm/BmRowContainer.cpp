#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/AllocateSize.h"
#include "bolt/exec/bm/BmPressureAwareBlockArena.h"

#include <limits>
#include <stdexcept>

namespace bytedance::bolt::exec {
namespace {

std::shared_ptr<memory::bm::BufferManager> requireBufferManager(
    std::shared_ptr<memory::bm::BufferManager> bufferManager) {
  if (!bufferManager) {
    throw std::invalid_argument("BmRowContainer requires BufferManager");
  }
  return bufferManager;
}

} // namespace

BmRowContainer::BmRowContainer(
    std::vector<TypePtr> keyTypes,
    std::vector<TypePtr> dependentTypes,
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize)
    : layout_(std::move(keyTypes), std::move(dependentTypes)),
      tag_(tag),
      blocks_(std::make_unique<BmPressureAwareBlockArena>(
          requireBufferManager(std::move(bufferManager)),
          tag)),
      rowBlockSize_(rowBlockSize == 0
              ? memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)
              : rowBlockSize),
      heapBlockSize_(heapBlockSize == 0
              ? memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)
              : heapBlockSize) {
  BOLT_CHECK_GT(layout_.fixedRowSize(), 0);
  BOLT_CHECK_LE(layout_.fixedRowSize(), rowBlockSize_);
}

BmRowContainer::~BmRowContainer() {
  blocks_->clear();
}

uint64_t BmRowContainer::allocatedBytes() const {
  return blocks_->allocatedBytes();
}

uint64_t BmRowContainer::usedBytes() const {
  return blocks_->usedBytes();
}

uint64_t BmRowContainer::heapAllocatedBytes() const {
  uint64_t bytes = 0;
  for (auto blockId : heapBlockIds_) {
    bytes += blocks_->block(blockId).capacity;
  }
  return bytes;
}

std::optional<int64_t> BmRowContainer::estimateRowSize() const {
  if (numRows_ == 0) {
    return std::nullopt;
  }
  return static_cast<int64_t>(usedBytes() / numRows_);
}

void BmRowContainer::discardAllRows() {
  blocks_->clear();
  heapBlockIds_.clear();
  activeRowBlockId_ = std::numeric_limits<uint32_t>::max();
  activeHeapBlockId_ = std::numeric_limits<uint32_t>::max();
  numRows_ = 0;
}

void BmRowContainer::spillAllBlocks() {
  blocks_->spillReclaimableBlocks(0, [](uint32_t) { return true; });
  activeRowBlockId_ = std::numeric_limits<uint32_t>::max();
  activeHeapBlockId_ = std::numeric_limits<uint32_t>::max();
}

} // namespace bytedance::bolt::exec
