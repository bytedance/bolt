#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/bm/BmPressureAwareBlockArena.h"

#include <limits>

namespace bytedance::bolt::exec {

void BmRowContainer::preload(std::vector<BlockId>& blockIds) {
  blocks_->pinBlocks(
      blockIds,
      [this](uint32_t candidateBlockId) {
        return canReclaimBlock(candidateBlockId);
      });
}

BmBlockState& BmRowContainer::ensureWritableRowBlock() {
  if (activeRowBlockId_ == std::numeric_limits<uint32_t>::max() ||
      !hasRowCapacity(blocks_->block(activeRowBlockId_))) {
    activeRowBlockId_ = allocateBlockAfterPressure(
        rowBlockSize_, "BmRowContainer cannot allocate a new row block");
  }
  return blocks_->block(activeRowBlockId_);
}

bool BmRowContainer::hasRowCapacity(const BmBlockState& block) const {
  const auto rowOffset = bits::roundUp(block.usedBytes, alignment_);
  return rowOffset + fixedRowSize_ <= rowBlockSize_;
}

char* BmRowContainer::mutableRow(RowId row) {
  auto& block = blocks_->block(row.blockId);
  BOLT_CHECK_LE(row.rowOffset + fixedRowSize_, block.usedBytes);
  return blocks_->activeData(row.blockId) + row.rowOffset;
}

const char* BmRowContainer::pinRow(RowId row) {
  auto& block = blocks_->block(row.blockId);
  BOLT_CHECK_LE(row.rowOffset + fixedRowSize_, block.usedBytes);
  return pinnedBlockDataAfterPressure(
             row.blockId, "BmRowContainer cannot pin a row block") +
      row.rowOffset;
}

uint32_t BmRowContainer::allocateBlockAfterPressure(
    uint32_t capacity,
    const char* failureMessage) {
  return blocks_->allocateBlock(
      capacity,
      [this](uint32_t candidateBlockId) {
        return canReclaimBlock(candidateBlockId);
      },
      failureMessage);
}

const char* BmRowContainer::pinnedBlockDataAfterPressure(
    uint32_t blockId,
    const char* failureMessage) {
  return blocks_->pinnedData(
      blockId,
      [this, blockId](uint32_t candidateBlockId) {
        return candidateBlockId != blockId && canReclaimBlock(candidateBlockId);
      },
      failureMessage);
}

bool BmRowContainer::canReclaimBlock(uint32_t blockId) const {
  return blockId != activeRowBlockId_ && blockId != activeHeapBlockId_;
}

} // namespace bytedance::bolt::exec
