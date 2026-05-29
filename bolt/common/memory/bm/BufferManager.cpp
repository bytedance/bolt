#include "bolt/common/memory/bm/BufferManager.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BufferManagerReclaimer.h"
#include "bolt/common/memory/bm/EvictionQueue.h"
#include "bolt/common/memory/bm/SpillStore.h"

#include <glog/logging.h>

#include <utility>

namespace bytedance::bolt::memory::bm {
namespace {

[[noreturn]] void ThrowFileAllocateFailure(
    const FileAllocateResult& result,
    uint64_t blockId) {
  BOLT_FAIL(
      "BM file allocation failed, block_id={}, file_error={}, native_error={}",
      blockId,
      static_cast<int>(result.error),
      result.native_error_code);
}

[[noreturn]] void ThrowIoFailure(
    const char* operation,
    const IoResult& result,
    uint64_t blockId) {
  BOLT_FAIL(
      "BM {} IO failed, block_id={}, io_error={}, native_error={}, bytes={}",
      operation,
      blockId,
      static_cast<int>(result.error),
      result.nativeErrorCode,
      result.bytes);
}

} // namespace

std::shared_ptr<BufferManager> BufferManager::Create(
    MemoryPool& parent,
    BufferManagerConfig config) {
  BOLT_CHECK(!config.poolName.empty());
  auto manager =
      std::shared_ptr<BufferManager>(new BufferManager(std::move(config)));
  manager->Initialize(parent);
  return manager;
}

BufferManager::BufferManager(BufferManagerConfig config)
    : config_(std::move(config)),
      evictionQueue_(std::make_unique<EvictionQueue>()) {}

BufferManager::~BufferManager() = default;

void BufferManager::Initialize(MemoryPool& parent) {
  allocator_ = CreateFileBlockAllocator(config_.fileAllocatorConfig);
  BOLT_CHECK_NOT_NULL(allocator_);

  // v1 keeps the default thread-safe leaf pool. For strictly thread-local BM
  // instances this can be optimized to addLeafChild(name, false) later.
  pool_ = parent.addLeafChild(config_.poolName);
  BOLT_CHECK_NOT_NULL(pool_);
  spillStore_ = std::make_unique<SpillStore>(allocator_, pool_.get());
  pool_->setReclaimer(
      std::make_unique<BufferManagerReclaimer>(weak_from_this()));
}

BufferHandle BufferManager::Allocate(
    size_t size,
    MemoryTag tag,
    std::shared_ptr<BlockHandle>* block) {
  BOLT_CHECK_GT(size, 0);
  auto memory = std::make_shared<BlockMemory>(nextBlockId_++, size, tag);
  memory->payload = IoBuffer::allocateFromPool(pool_.get(), size);
  memory->pinCount = 1;
  auto handle = std::make_shared<BlockHandle>(std::move(memory));
  if (block != nullptr) {
    *block = handle;
  }
  return MakeHandle(handle);
}

bool BufferManager::MaybeReserve(size_t size) {
  return pool_->maybeReserve(size);
}

void BufferManager::ReleaseUnusedReservation() {
  pool_->release();
}

BufferHandle BufferManager::Pin(const std::shared_ptr<BlockHandle>& block) {
  BOLT_CHECK_NOT_NULL(block);
  auto& memory = *block->memory_;
  switch (memory.state) {
    case BlockMemoryState::kInMemory:
      return PinInMemory(block);
    case BlockMemoryState::kSpilled:
      return PinSpilled(block);
    case BlockMemoryState::kPrefetching:
      return PinPrefetching(block);
    case BlockMemoryState::kSpilling:
      BOLT_FAIL(
          "BM cannot pin a block while it is spilling, block_id={}", memory.id);
  }
  BOLT_FAIL("BM encountered unknown block state, block_id={}", memory.id);
}

std::vector<BufferHandle> BufferManager::BatchPin(
    std::span<const std::shared_ptr<BlockHandle>> blocks) {
  for (const auto& block : blocks) {
    BOLT_CHECK_NOT_NULL(block);
    auto& memory = *block->memory_;
    if (memory.state == BlockMemoryState::kSpilled) {
      SubmitRead(block, config_.readPriority);
    }
  }

  std::vector<BufferHandle> handles;
  handles.reserve(blocks.size());
  for (const auto& block : blocks) {
    handles.push_back(Pin(block));
  }
  return handles;
}

void BufferManager::Prefetch(
    std::span<const std::shared_ptr<BlockHandle>> blocks) noexcept {
  for (const auto& block : blocks) {
    if (!block) {
      ++prefetchSubmitFailures_;
      LOG(WARNING) << "BM Prefetch ignored null block";
      continue;
    }
    try {
      auto& memory = *block->memory_;
      if (memory.state == BlockMemoryState::kSpilled) {
        SubmitRead(block, config_.prefetchPriority);
      }
    } catch (const std::exception& e) {
      ++prefetchSubmitFailures_;
      LOG(WARNING) << "BM Prefetch failed for block_id=" << block->id() << ": "
                   << e.what();
    } catch (...) {
      ++prefetchSubmitFailures_;
      LOG(WARNING) << "BM Prefetch failed for block_id=" << block->id()
                   << " with unknown exception";
    }
  }
}

uint64_t BufferManager::Reclaim(uint64_t targetBytes) {
  uint64_t reclaimed = 0;
  while (targetBytes == 0 || reclaimed < targetBytes) {
    auto memory = evictionQueue_->PopEvictable();
    if (!memory) {
      break;
    }

    try {
      reclaimed += SpillBlock(memory);
    } catch (...) {
      if (memory->pinCount == 0 &&
          memory->state == BlockMemoryState::kInMemory &&
          memory->payload.has_value()) {
        evictionQueue_->Add(memory);
      }
      throw;
    }
  }

  reclaimedBytes_ += reclaimed;
  return reclaimed;
}

uint64_t BufferManager::reclaimableBytes() const {
  // The current BM threading contract serializes reclaimer calls with API
  // calls. If that changes, this field must become atomic or be protected.
  return unpinnedResidentBytes_;
}

BufferManagerStats BufferManager::stats() const {
  return BufferManagerStats{
      nextBlockId_ - 1,
      unpinnedResidentBytes_,
      reclaimedBytes_,
      prefetchSubmitFailures_,
      prefetchIoFailures_};
}

void BufferManager::Unpin(const std::shared_ptr<BlockHandle>& block) noexcept {
  if (!block || !block->memory_) {
    LOG(FATAL) << "BM Unpin received invalid block handle";
  }

  auto& memory = *block->memory_;
  if (memory.pinCount == 0) {
    LOG(FATAL) << "BM Unpin underflow, block_id=" << memory.id
               << ", tag=" << toString(memory.tag);
  }
  --memory.pinCount;
  if (memory.pinCount == 0 && memory.state == BlockMemoryState::kInMemory) {
    ++memory.evictionSequence;
    unpinnedResidentBytes_ += memory.size;
    evictionQueue_->Add(block->memory_);
  }
}

BufferHandle BufferManager::PinInMemory(
    const std::shared_ptr<BlockHandle>& block) {
  auto& memory = *block->memory_;
  BOLT_CHECK(memory.payload.has_value(), "resident BM block has no payload");
  if (memory.pinCount == 0) {
    BOLT_CHECK_GE(unpinnedResidentBytes_, memory.size);
    unpinnedResidentBytes_ -= memory.size;
  }
  ++memory.pinCount;
  ++memory.evictionSequence;
  return MakeHandle(block);
}

BufferHandle BufferManager::PinSpilled(
    const std::shared_ptr<BlockHandle>& block) {
  SubmitRead(block, config_.readPriority);
  return PinPrefetching(block);
}

BufferHandle BufferManager::PinPrefetching(
    const std::shared_ptr<BlockHandle>& block) {
  auto& memory = *block->memory_;
  BOLT_CHECK(memory.prefetchFuture.has_value());
  auto result = memory.prefetchFuture->get();
  memory.prefetchFuture.reset();
  if (!result.ok()) {
    ++prefetchIoFailures_;
    memory.state = BlockMemoryState::kSpilled;
    ThrowIoFailure("read", result, memory.id);
  }

  BOLT_CHECK(memory.extent.has_value());
  auto oldExtent = std::move(*memory.extent);
  memory.extent.reset();
  memory.payload = std::move(result.buffer);
  memory.state = BlockMemoryState::kInMemory;
  memory.pinCount = 1;
  ++memory.evictionSequence;
  oldExtent.FreeOrFatal("BufferManager::PinPrefetching");
  return MakeHandle(block);
}

void BufferManager::SubmitRead(
    const std::shared_ptr<BlockHandle>& block,
    IoPriority priority) {
  auto& memory = *block->memory_;
  BOLT_CHECK(
      memory.state == BlockMemoryState::kSpilled,
      "BM read submission expects a spilled block");
  BOLT_CHECK(memory.extent.has_value());

  memory.prefetchFuture =
      spillStore_->SubmitRead(*memory.extent, memory.size, priority);
  memory.state = BlockMemoryState::kPrefetching;
}

uint64_t BufferManager::SpillBlock(const std::shared_ptr<BlockMemory>& memory) {
  BOLT_CHECK_NOT_NULL(memory);
  BOLT_CHECK(memory->state == BlockMemoryState::kInMemory);
  BOLT_CHECK_EQ(memory->pinCount, 0);
  BOLT_CHECK(memory->payload.has_value());
  BOLT_CHECK_GE(unpinnedResidentBytes_, memory->size);

  auto allocation = spillStore_->AllocateExtent(memory->size);
  if (!allocation.ok()) {
    ThrowFileAllocateFailure(allocation, memory->id);
  }

  auto payload = std::move(*memory->payload);
  memory->payload.reset();
  memory->state = BlockMemoryState::kSpilling;
  unpinnedResidentBytes_ -= memory->size;

  IoResult result;
  try {
    result =
        spillStore_->Write(allocation.extent, payload, config_.writePriority);
  } catch (...) {
    auto freeResult = spillStore_->FreeExtent(allocation.extent);
    if (!freeResult.ok()) {
      LOG(FATAL)
          << "BM failed to free extent after spill write exception, block_id="
          << memory->id << ", file_error=" << static_cast<int>(freeResult.error)
          << ", native_error=" << freeResult.native_error_code;
    }
    if (!payload.valid()) {
      LOG(FATAL)
          << "BM spill write threw after payload ownership was moved, block_id="
          << memory->id;
    }
    memory->payload = std::move(payload);
    memory->state = BlockMemoryState::kInMemory;
    unpinnedResidentBytes_ += memory->size;
    throw;
  }

  if (!result.ok()) {
    auto freeResult = spillStore_->FreeExtent(allocation.extent);
    if (!freeResult.ok()) {
      LOG(FATAL)
          << "BM failed to free extent after failed spill write, block_id="
          << memory->id << ", file_error=" << static_cast<int>(freeResult.error)
          << ", native_error=" << freeResult.native_error_code;
    }
    memory->payload = std::move(result.buffer);
    memory->state = BlockMemoryState::kInMemory;
    unpinnedResidentBytes_ += memory->size;
    ThrowIoFailure("write", result, memory->id);
  }

  memory->extent = spillStore_->OwnExtent(allocation.extent);
  memory->state = BlockMemoryState::kSpilled;
  ++memory->evictionSequence;
  return memory->size;
}

BufferHandle BufferManager::MakeHandle(
    const std::shared_ptr<BlockHandle>& block) {
  auto& memory = *block->memory_;
  BOLT_CHECK(memory.payload.has_value());
  BOLT_CHECK(memory.payload->valid());
  return BufferHandle{weak_from_this(), block, memory.payload->data()};
}

} // namespace bytedance::bolt::memory::bm
