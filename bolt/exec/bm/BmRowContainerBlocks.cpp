#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/bm/BmPressureAwareBlockArena.h"

#include <limits>

namespace bytedance::bolt::exec {

void BmRowContainer::preload(std::vector<BlockId>& blockIds) {
  const auto protectedBlocks = protectedBlocksForRead(blockIds);
  blocks_->pinBlocks(blockIds, protectedBlocks);
}

BmBlockState& BmRowContainer::ensureWritableRowBlock() {
  if (activeRowBlockId_ == std::numeric_limits<uint32_t>::max() ||
      !hasRowCapacity(blocks_->block(activeRowBlockId_))) {
    activeRowBlockId_ = blocks_->allocateReservedBlock(rowBlockSize_);
  }
  return blocks_->block(activeRowBlockId_);
}

bool BmRowContainer::hasRowCapacity(const BmBlockState& block) const {
  const auto rowOffset = bits::roundUp(block.usedBytes, layout_.alignment());
  return rowOffset + layout_.fixedRowSize() <= rowBlockSize_;
}

char* BmRowContainer::mutableRow(RowId row) {
  auto& block = blocks_->block(row.blockId);
  BOLT_CHECK_LE(row.rowOffset + layout_.fixedRowSize(), block.usedBytes);
  return blocks_->activeData(row.blockId) + row.rowOffset;
}

const char* BmRowContainer::pinRow(RowId row) {
  auto& block = blocks_->block(row.blockId);
  BOLT_CHECK_LE(row.rowOffset + layout_.fixedRowSize(), block.usedBytes);
  return pinBlockForRead(row.blockId, "BmRowContainer cannot pin a row block") +
      row.rowOffset;
}

const char* BmRowContainer::pinBlockForRead(
    uint32_t blockId,
    const char* failureMessage) {
  const auto protectedBlocks =
      protectedBlocksForRead(std::span<const BlockId>(&blockId, 1));
  return blocks_->pinnedData(
      blockId,
      protectedBlocks,
      failureMessage);
}

std::vector<BlockId> BmRowContainer::protectedBlocksForRead(
    std::span<const BlockId> blockIds) const {
  std::vector<BlockId> protectedBlocks;
  protectedBlocks.reserve(blockIds.size() + 2);
  for (auto blockId : blockIds) {
    protectedBlocks.push_back(blockId);
  }
  if (activeRowBlockId_ != std::numeric_limits<uint32_t>::max()) {
    protectedBlocks.push_back(activeRowBlockId_);
  }
  if (activeHeapBlockId_ != std::numeric_limits<uint32_t>::max()) {
    protectedBlocks.push_back(activeHeapBlockId_);
  }
  return protectedBlocks;
}

} // namespace bytedance::bolt::exec
