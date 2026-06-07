#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/bm/BmPressureAwareBlockArena.h"

#include <limits>

#include <folly/container/F14Set.h>

namespace bytedance::bolt::exec {

void BmRowContainer::preloadRows(folly::Range<const RowId*> rows) {
  std::vector<BlockId> blockIds;
  blockIds.reserve(rows.size() * 2);
  folly::F14FastSet<BlockId> seen;
  seen.reserve(rows.size() * 2);

  const auto addBlock = [&](BlockId blockId) {
    if (seen.insert(blockId).second) {
      blockIds.push_back(blockId);
    }
  };

  for (const auto row : rows) {
    addBlock(row.rowBlockId());
    if (const auto heapBlockId = row.primaryHeapBlockId()) {
      addBlock(*heapBlockId);
    }
  }

  blocks_->tryPinBlocks(blockIds);
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
  auto& block = blocks_->block(row.rowBlockId());
  BOLT_CHECK_LE(row.rowOffset() + layout_.fixedRowSize(), block.usedBytes);
  return blocks_->activeData(row.rowBlockId()) + row.rowOffset();
}

const char* BmRowContainer::pinRow(RowId row) {
  auto& block = blocks_->block(row.rowBlockId());
  BOLT_CHECK_LE(row.rowOffset() + layout_.fixedRowSize(), block.usedBytes);
  return pinBlockForRead(
             row.rowBlockId(), "BmRowContainer cannot pin a row block") +
      row.rowOffset();
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
