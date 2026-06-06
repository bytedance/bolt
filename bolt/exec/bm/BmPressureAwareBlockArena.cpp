#include "bolt/exec/bm/BmPressureAwareBlockArena.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <stdexcept>

namespace bytedance::bolt::exec {

BmPressureAwareBlockArena::BmPressureAwareBlockArena(
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag)
    : bufferManager_(std::move(bufferManager)), tag_(tag) {
  if (!bufferManager_) {
    throw std::invalid_argument("BmPressureAwareBlockArena requires BufferManager");
  }
}

BmPressureAwareBlockArena::~BmPressureAwareBlockArena() {
  clear();
}

uint32_t BmPressureAwareBlockArena::allocateBlock(
    uint32_t capacity,
    const CanReclaimFn& canReclaim) {
  auto blockId = tryAllocateBlock(capacity);
  if (blockId.has_value()) {
    return blockId.value();
  }

  ensureMemoryForBlock(
      capacity, canReclaim, "BmPressureAwareBlockArena cannot reserve a new block");
  blockId = tryAllocateBlock(capacity);
  BOLT_CHECK(blockId.has_value(), "BmPressureAwareBlockArena cannot allocate a new block");
  return blockId.value();
}

std::optional<uint32_t> BmPressureAwareBlockArena::tryAllocateBlock(
    uint32_t capacity) {
  if (!bufferManager_->MaybeReserve(capacity)) {
    return std::nullopt;
  }
  auto handle = bufferManager_->Allocate(capacity, tag_);
  bufferManager_->ReleaseUnusedReservation();

  BmBlockState state;
  state.block = handle.block();
  state.pinnedHandle.emplace(std::move(handle));
  state.data = state.pinnedHandle->Ptr();
  state.capacity = capacity;
  touch(state);

  blocks_.push_back(std::move(state));
  return static_cast<uint32_t>(blocks_.size() - 1);
}

char* BmPressureAwareBlockArena::activeData(uint32_t blockId) {
  auto& state = block(blockId);
  BOLT_CHECK(state.pinnedHandle.has_value());
  BOLT_CHECK_NOT_NULL(state.data);
  touch(state);
  return state.data;
}

const char* BmPressureAwareBlockArena::pinnedData(
    uint32_t blockId,
    const CanReclaimFn& canReclaim) {
  if (const auto* data = tryPinnedData(blockId)) {
    return data;
  }

  auto& state = block(blockId);
  const auto canReclaimOthers = [&](uint32_t candidateBlockId) {
    return candidateBlockId != blockId && canReclaim(candidateBlockId);
  };
  ensureMemoryForBlock(
      state.capacity,
      canReclaimOthers,
      "BmPressureAwareBlockArena cannot reserve memory to pin a block");
  const auto* data = tryPinnedData(blockId);
  BOLT_CHECK_NOT_NULL(data);
  return data;
}

void BmPressureAwareBlockArena::pinBlocks(
    std::span<const uint32_t> blockIds,
    const CanReclaimFn& canReclaim) {
  std::vector<uint32_t> toPin;
  toPin.reserve(blockIds.size());
  uint64_t bytesToReserve = 0;
  for (auto blockId : blockIds) {
    auto& state = block(blockId);
    if (state.pinnedHandle.has_value()) {
      touch(state);
      continue;
    }
    if (std::find(toPin.begin(), toPin.end(), blockId) != toPin.end()) {
      continue;
    }
    toPin.push_back(blockId);
    bytesToReserve += state.capacity;
  }
  if (toPin.empty()) {
    return;
  }

  const auto canReclaimOthers = [&](uint32_t candidateBlockId) {
    return std::find(toPin.begin(), toPin.end(), candidateBlockId) ==
        toPin.end() && canReclaim(candidateBlockId);
  };
  ensureMemoryForBlock(
      bytesToReserve,
      canReclaimOthers,
      "BmPressureAwareBlockArena cannot reserve memory to pin blocks");

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

const char* BmPressureAwareBlockArena::tryPinnedData(uint32_t blockId) {
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

BmBlockState& BmPressureAwareBlockArena::block(uint32_t blockId) {
  BOLT_CHECK_LT(blockId, blocks_.size());
  return blocks_[blockId];
}

const BmBlockState& BmPressureAwareBlockArena::block(uint32_t blockId) const {
  BOLT_CHECK_LT(blockId, blocks_.size());
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

uint64_t BmPressureAwareBlockArena::makeBlocksReclaimable(
    uint64_t targetBytes,
    const CanReclaimFn& canReclaim) {
  std::vector<uint32_t> candidates;
  candidates.reserve(blocks_.size());
  for (uint32_t i = 0; i < blocks_.size(); ++i) {
    if (blocks_[i].pinnedHandle.has_value() && canReclaim(i)) {
      candidates.push_back(i);
    }
  }

  std::sort(candidates.begin(), candidates.end(), [&](auto left, auto right) {
    return blocks_[left].lastAccess < blocks_[right].lastAccess;
  });

  uint64_t released = 0;
  for (auto blockId : candidates) {
    auto& state = blocks_[blockId];
    released += state.capacity;
    state.pinnedHandle.reset();
    state.data = nullptr;
    if (targetBytes != 0 && released >= targetBytes) {
      break;
    }
  }
  return released;
}

std::vector<std::shared_ptr<memory::bm::BlockHandle>>
BmPressureAwareBlockArena::reclaimableBlocks(
    const CanReclaimFn& canReclaim) const {
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  blocks.reserve(blocks_.size());
  for (uint32_t i = 0; i < blocks_.size(); ++i) {
    const auto& state = blocks_[i];
    if (!state.pinnedHandle.has_value() && canReclaim(i)) {
      blocks.push_back(state.block);
    }
  }
  return blocks;
}

uint32_t BmPressureAwareBlockArena::spillReclaimableBlocks(
    uint64_t targetBytes,
    const CanReclaimFn& canReclaim) {
  makeBlocksReclaimable(targetBytes, canReclaim);
  auto blocks = reclaimableBlocks(canReclaim);
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

void BmPressureAwareBlockArena::ensureMemoryForBlock(
    uint32_t capacity,
    const CanReclaimFn& canReclaim,
    const char* failureMessage) {
  if (bufferManager_->MaybeReserve(capacity)) {
    return;
  }

  makeBlocksReclaimable(capacity, canReclaim);
  auto blocks = reclaimableBlocks(canReclaim);
  if (!blocks.empty()) {
    bufferManager_->SpillBlocks(blocks);
  }

  BOLT_CHECK(bufferManager_->MaybeReserve(capacity), failureMessage);
}

void BmPressureAwareBlockArena::touch(BmBlockState& block) {
  block.lastAccess = ++accessCounter_;
}

} // namespace bytedance::bolt::exec
