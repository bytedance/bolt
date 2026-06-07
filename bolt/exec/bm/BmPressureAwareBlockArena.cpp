#include "bolt/exec/bm/BmPressureAwareBlockArena.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/bm/BmBlockReclaimPolicy.h"

#include <algorithm>
#include <stdexcept>

#include <folly/container/F14Set.h>

namespace bytedance::bolt::exec {

BmPressureAwareBlockArena::BmPressureAwareBlockArena(
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    std::unique_ptr<BmBlockReclaimPolicy> reclaimPolicy)
    : bufferManager_(std::move(bufferManager)),
      tag_(tag),
      reclaimPolicy_(std::move(reclaimPolicy)) {
  if (!bufferManager_) {
    throw std::invalid_argument("BmPressureAwareBlockArena requires BufferManager");
  }
  if (!reclaimPolicy_) {
    reclaimPolicy_ = std::make_unique<BmLruBlockReclaimPolicy>();
  }
}

BmPressureAwareBlockArena::~BmPressureAwareBlockArena() {
  clear();
}

BlockId BmPressureAwareBlockArena::allocateReservedBlock(
    uint32_t capacity) {
  auto handle = bufferManager_->Allocate(capacity, tag_);

  BmBlockState state;
  state.block = handle.block();
  state.pinnedHandle.emplace(std::move(handle));
  state.data = state.pinnedHandle->Ptr();
  state.capacity = capacity;
  touch(state);

  blocks_.push_back(std::move(state));
  return static_cast<uint32_t>(blocks_.size() - 1);
}

char* BmPressureAwareBlockArena::activeData(BlockId blockId) {
  auto& state = block(blockId);
  BOLT_CHECK(state.pinnedHandle.has_value());
  BOLT_CHECK_NOT_NULL(state.data);
  touch(state);
  return state.data;
}

const char* BmPressureAwareBlockArena::pinnedData(
    BlockId blockId,
    std::span<const BlockId> protectedBlocks,
    const char* failureMessage) {
  if (const auto* data = tryPinBlock(blockId)) {
    return data;
  }

  auto& state = block(blockId);
  std::vector<BlockId> readProtectedBlocks;
  readProtectedBlocks.reserve(protectedBlocks.size() + 1);
  readProtectedBlocks.push_back(blockId);
  readProtectedBlocks.insert(
      readProtectedBlocks.end(),
      protectedBlocks.begin(),
      protectedBlocks.end());
  ensureCapacityForPinnedRead(
      state.capacity,
      readProtectedBlocks,
      failureMessage);
  const auto* data = tryPinBlock(blockId);
  BOLT_CHECK_NOT_NULL(data, failureMessage);
  return data;
}

void BmPressureAwareBlockArena::pinBlocks(
    std::span<const BlockId> blockIds,
    std::span<const BlockId> protectedBlocks) {
  std::vector<BlockId> toPin;
  toPin.reserve(blockIds.size());
  folly::F14FastSet<BlockId> seen;
  seen.reserve(blockIds.size());
  uint64_t bytesToReserve = 0;
  for (auto blockId : blockIds) {
    auto& state = block(blockId);
    if (state.pinnedHandle.has_value()) {
      touch(state);
      continue;
    }
    if (!seen.insert(blockId).second) {
      continue;
    }
    toPin.push_back(blockId);
    bytesToReserve += state.capacity;
  }
  if (toPin.empty()) {
    return;
  }

  if (!bufferManager_->MaybeReserve(bytesToReserve)) {
    bufferManager_->ReleaseUnusedReservation();
    std::vector<BlockId> readProtectedBlocks;
    readProtectedBlocks.reserve(protectedBlocks.size() + toPin.size());
    readProtectedBlocks.insert(
        readProtectedBlocks.end(),
        protectedBlocks.begin(),
        protectedBlocks.end());
    readProtectedBlocks.insert(readProtectedBlocks.end(), toPin.begin(), toPin.end());
    for (auto blockId : toPin) {
      pinnedData(
          blockId,
          readProtectedBlocks,
          "BmPressureAwareBlockArena cannot reserve memory to pin blocks");
    }
    return;
  }

  std::vector<std::shared_ptr<memory::bm::BlockHandle>> handles;
  handles.reserve(toPin.size());
  for (auto blockId : toPin) {
    handles.push_back(block(blockId).block);
  }

  std::vector<memory::bm::BufferHandle> pinnedHandles;
  try {
    pinnedHandles = bufferManager_->BatchPin(handles);
  } catch (...) {
    bufferManager_->ReleaseUnusedReservation();
    throw;
  }
  BOLT_CHECK_EQ(pinnedHandles.size(), toPin.size());
  for (auto i = 0; i < toPin.size(); ++i) {
    auto& state = block(toPin[i]);
    state.pinnedHandle.emplace(std::move(pinnedHandles[i]));
    state.data = state.pinnedHandle->Ptr();
    BOLT_CHECK_NOT_NULL(state.data);
    touch(state);
  }
  bufferManager_->ReleaseUnusedReservation();
}

void BmPressureAwareBlockArena::tryPinBlocks(std::span<const BlockId> blockIds) {
  for (auto blockId : blockIds) {
    try {
      tryPinBlock(blockId);
    } catch (...) {
      bufferManager_->ReleaseUnusedReservation();
    }
  }
}

const char* BmPressureAwareBlockArena::tryPinBlock(BlockId blockId) {
  auto& state = block(blockId);
  if (!state.pinnedHandle.has_value()) {
    if (!bufferManager_->MaybeReserve(state.capacity)) {
      return nullptr;
    }
    state.pinnedHandle.emplace(bufferManager_->Pin(state.block));
    bufferManager_->ReleaseUnusedReservation();
    state.data = state.pinnedHandle->Ptr();
  }
  BOLT_CHECK_NOT_NULL(state.data);
  touch(state);
  return state.data;
}

BmBlockState& BmPressureAwareBlockArena::block(BlockId blockId) {
  BOLT_DCHECK_LT(blockId, blocks_.size());
  return blocks_[blockId];
}

const BmBlockState& BmPressureAwareBlockArena::block(BlockId blockId) const {
  BOLT_DCHECK_LT(blockId, blocks_.size());
  return blocks_[blockId];
}

uint64_t BmPressureAwareBlockArena::allocatedBytes() const {
  uint64_t bytes = 0;
  for (const auto& block : blocks_) {
    bytes += block.capacity;
  }
  return bytes;
}

uint64_t BmPressureAwareBlockArena::usedBytes() const {
  uint64_t bytes = 0;
  for (const auto& block : blocks_) {
    bytes += block.usedBytes;
  }
  return bytes;
}

uint32_t BmPressureAwareBlockArena::size() const {
  return static_cast<uint32_t>(blocks_.size());
}

bool BmPressureAwareBlockArena::empty() const {
  return blocks_.empty();
}

uint32_t BmPressureAwareBlockArena::spillReclaimableBlocks(
    uint64_t targetBytes,
    std::span<const BlockId> protectedBlocks) {
  const auto victims = selectVictims(targetBytes, protectedBlocks, false);
  releasePinnedVictims(victims);
  auto blocks = unpinnedVictimBlocks(victims);
  if (blocks.empty()) {
    return 0;
  }
  bufferManager_->SpillBlocks(blocks);
  return static_cast<uint32_t>(blocks.size());
}

void BmPressureAwareBlockArena::clear() {
  blocks_.clear();
  if (bufferManager_) {
    bufferManager_->ReleaseUnusedReservation();
  }
}

void BmPressureAwareBlockArena::ensureCapacityForPinnedRead(
    uint32_t capacity,
    std::span<const BlockId> protectedBlocks,
    const char* failureMessage) {
  if (bufferManager_->MaybeReserve(capacity)) {
    return;
  }

  const auto victims = selectVictims(capacity, protectedBlocks, false);
  releasePinnedVictims(victims);
  if (bufferManager_->MaybeReserve(capacity)) {
    return;
  }

  auto blocks = unpinnedVictimBlocks(victims);
  if (!blocks.empty()) {
    bufferManager_->SpillBlocks(blocks);
  }

  BOLT_CHECK(bufferManager_->MaybeReserve(capacity), failureMessage);
}

std::vector<BmBlockReclaimCandidate>
BmPressureAwareBlockArena::reclaimCandidates(
    std::span<const BlockId> protectedBlocks,
    bool pinnedOnly) const {
  std::vector<BmBlockReclaimCandidate> candidates;
  candidates.reserve(blocks_.size());
  for (BlockId i = 0; i < blocks_.size(); ++i) {
    const auto& state = blocks_[i];
    const auto pinned = state.pinnedHandle.has_value();
    if ((!pinnedOnly || pinned) && !containsProtectedBlock(protectedBlocks, i)) {
      candidates.push_back(BmBlockReclaimCandidate{
          .blockId = i,
          .capacity = state.capacity,
          .pinned = pinned,
          .lastAccess = state.lastAccess,
      });
    }
  }
  return candidates;
}

std::vector<BlockId> BmPressureAwareBlockArena::selectVictims(
    uint64_t targetBytes,
    std::span<const BlockId> protectedBlocks,
    bool pinnedOnly) const {
  const auto candidates = reclaimCandidates(protectedBlocks, pinnedOnly);
  auto victims = reclaimPolicy_->selectVictims(BmBlockReclaimContext{
      .candidates = candidates,
      .targetBytes = targetBytes,
  });
  for (auto victim : victims) {
    const auto selectedFromCandidates = std::any_of(
        candidates.begin(),
        candidates.end(),
        [&](const auto& candidate) { return candidate.blockId == victim; });
    BOLT_CHECK(
        selectedFromCandidates,
        "BmBlockReclaimPolicy selected a non-reclaimable block");
  }
  return victims;
}

bool BmPressureAwareBlockArena::containsProtectedBlock(
    std::span<const BlockId> protectedBlocks,
    BlockId blockId) const {
  return std::find(
             protectedBlocks.begin(),
             protectedBlocks.end(),
             blockId) != protectedBlocks.end();
}

uint64_t BmPressureAwareBlockArena::releasePinnedVictims(
    std::span<const BlockId> victims) {
  uint64_t released = 0;
  for (auto blockId : victims) {
    auto& state = block(blockId);
    if (!state.pinnedHandle.has_value()) {
      continue;
    }
    released += state.capacity;
    state.pinnedHandle.reset();
    state.data = nullptr;
  }
  return released;
}

std::vector<std::shared_ptr<memory::bm::BlockHandle>>
BmPressureAwareBlockArena::unpinnedVictimBlocks(
    std::span<const BlockId> victims) const {
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  blocks.reserve(victims.size());
  for (auto blockId : victims) {
    const auto& state = block(blockId);
    if (!state.pinnedHandle.has_value()) {
      blocks.push_back(state.block);
    }
  }
  return blocks;
}

void BmPressureAwareBlockArena::touch(BmBlockState& block) {
  block.lastAccess = ++accessCounter_;
}

} // namespace bytedance::bolt::exec
