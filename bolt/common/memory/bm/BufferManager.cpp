#include "bolt/common/memory/bm/BufferManager.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BufferManagerReclaimer.h"
#include "bolt/common/memory/bm/EvictionQueue.h"
#include "bolt/common/memory/bm/SpillStore.h"

#include <glog/logging.h>

#include <array>
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

void SubtractOrFatal(
    uint64_t& value,
    uint64_t delta,
    const char* field,
    const BlockMemory& memory) noexcept {
  if (value < delta) {
    LOG(FATAL) << "BM observability counter underflow, field=" << field
               << ", value=" << value << ", delta=" << delta
               << ", block_id=" << memory.id
               << ", tag=" << toString(memory.tag)
               << ", size=" << memory.size
               << ", state=" << static_cast<int>(memory.state)
               << ", pin_count=" << memory.pinCount;
  }
  value -= delta;
}

} // namespace

constexpr std::array<MemoryTag, kMemoryTagCount> kMemoryTags{
    MemoryTag::kUnknown,
    MemoryTag::kHashBuild,
    MemoryTag::kAggregation,
    MemoryTag::kSort,
    MemoryTag::kWindow,
    MemoryTag::kExchange,
    MemoryTag::kTesting};

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
      evictionQueue_(std::make_unique<EvictionQueue>()) {
  for (size_t i = 0; i < kMemoryTags.size(); ++i) {
    tagStats_[i].tag = kMemoryTags[i];
  }
}

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
  memory->owner = weak_from_this();
  memory->payload = IoBuffer::allocateFromPool(pool_.get(), size);
  memory->pinCount = 1;
  ++stats_.allocatedBlocks;
  ++stats_.liveBlocks;
  stats_.pinnedResidentBytes += size;
  auto& tagStats = MutableTagStats(tag);
  ++tagStats.allocatedBlocks;
  ++tagStats.liveBlocks;
  tagStats.residentBytes += size;
  tagStats.pinnedResidentBytes += size;
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
  ++stats_.pinCount;
  ++MutableTagStats(block->tag()).pinCount;
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
  ++stats_.batchPinCount;
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
  ++stats_.prefetchCount;
  for (const auto& block : blocks) {
    if (!block) {
      ++stats_.prefetchSubmitFailures;
      LOG(WARNING) << "BM Prefetch ignored null block";
      continue;
    }
    try {
      auto& memory = *block->memory_;
      if (memory.state == BlockMemoryState::kSpilled) {
        SubmitRead(block, config_.prefetchPriority);
      }
    } catch (const std::exception& e) {
      ++stats_.prefetchSubmitFailures;
      LOG(WARNING) << "BM Prefetch failed for block_id=" << block->id() << ": "
                   << e.what();
    } catch (...) {
      ++stats_.prefetchSubmitFailures;
      LOG(WARNING) << "BM Prefetch failed for block_id=" << block->id()
                   << " with unknown exception";
    }
  }
}

uint64_t BufferManager::Reclaim(uint64_t targetBytes) {
  ++stats_.reclaimCount;
  uint64_t reclaimed = 0;
  while (targetBytes == 0 || reclaimed < targetBytes) {
    auto memory = evictionQueue_->PopEvictable();
    if (!memory) {
      break;
    }
    ++stats_.reclaimAttemptedBlocks;

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

  stats_.reclaimedBytes += reclaimed;
  return reclaimed;
}

uint64_t BufferManager::reclaimableBytes() const {
  // The current BM threading contract serializes reclaimer calls with API
  // calls. If that changes, this field must become atomic or be protected.
  return stats_.unpinnedResidentBytes;
}

BufferManagerStats BufferManager::stats() const {
  auto result = stats_;
  const auto queueStats = evictionQueue_->stats();
  result.evictionQueueSize = queueStats.size;
  result.evictionQueueStaleEntries = queueStats.staleEntries;
  result.reclaimSkippedBlocks = queueStats.staleEntries;
  return result;
}

std::vector<BufferManagerTagStats> BufferManager::tagStats() const {
  return nonEmptyTagStats(
      std::vector<BufferManagerTagStats>{tagStats_.begin(), tagStats_.end()});
}

std::string BufferManager::debugString() const {
  return toDebugString(
      stats(),
      std::vector<BufferManagerTagStats>{tagStats_.begin(), tagStats_.end()});
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
    SubtractOrFatal(
        stats_.pinnedResidentBytes,
        memory.size,
        "pinnedResidentBytes",
        memory);
    stats_.unpinnedResidentBytes += memory.size;
    auto& tagStats = MutableTagStats(memory.tag);
    SubtractOrFatal(
        tagStats.pinnedResidentBytes,
        memory.size,
        "tag.pinnedResidentBytes",
        memory);
    tagStats.unpinnedResidentBytes += memory.size;
    evictionQueue_->Add(block->memory_);
  }
}

BufferHandle BufferManager::PinInMemory(
    const std::shared_ptr<BlockHandle>& block) {
  auto& memory = *block->memory_;
  BOLT_CHECK(memory.payload.has_value(), "resident BM block has no payload");
  if (memory.pinCount == 0) {
    BOLT_CHECK_GE(stats_.unpinnedResidentBytes, memory.size);
    stats_.unpinnedResidentBytes -= memory.size;
    stats_.pinnedResidentBytes += memory.size;
    auto& tagStats = MutableTagStats(memory.tag);
    BOLT_CHECK_GE(tagStats.unpinnedResidentBytes, memory.size);
    tagStats.unpinnedResidentBytes -= memory.size;
    tagStats.pinnedResidentBytes += memory.size;
  }
  ++memory.pinCount;
  ++memory.evictionSequence;
  ++stats_.pinInMemoryCount;
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
  BOLT_CHECK_GE(stats_.prefetchingBytes, memory.size);
  stats_.prefetchingBytes -= memory.size;
  auto& tagStats = MutableTagStats(memory.tag);
  BOLT_CHECK_GE(tagStats.prefetchingBytes, memory.size);
  tagStats.prefetchingBytes -= memory.size;
  if (!result.ok()) {
    ++stats_.prefetchIoFailures;
    ++stats_.readIoFailures;
    memory.state = BlockMemoryState::kSpilled;
    ThrowIoFailure("read", result, memory.id);
  }

  BOLT_CHECK(memory.extent.has_value());
  auto oldExtent = std::move(*memory.extent);
  memory.extent.reset();
  memory.payload = std::move(result.buffer);
  memory.state = BlockMemoryState::kInMemory;
  memory.pinCount = 1;
  BOLT_CHECK_GE(stats_.spilledBytes, memory.size);
  stats_.spilledBytes -= memory.size;
  stats_.pinnedResidentBytes += memory.size;
  ++stats_.pinReadCount;
  ++stats_.spillReadCount;
  stats_.spillReadBytes += memory.size;
  BOLT_CHECK_GE(tagStats.spilledBytes, memory.size);
  tagStats.spilledBytes -= memory.size;
  tagStats.residentBytes += memory.size;
  tagStats.pinnedResidentBytes += memory.size;
  ++tagStats.spillReadCount;
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
  stats_.prefetchingBytes += memory.size;
  MutableTagStats(memory.tag).prefetchingBytes += memory.size;
}

uint64_t BufferManager::SpillBlock(const std::shared_ptr<BlockMemory>& memory) {
  BOLT_CHECK_NOT_NULL(memory);
  BOLT_CHECK(memory->state == BlockMemoryState::kInMemory);
  BOLT_CHECK_EQ(memory->pinCount, 0);
  BOLT_CHECK(memory->payload.has_value());
  BOLT_CHECK_GE(stats_.unpinnedResidentBytes, memory->size);

  auto allocation = spillStore_->AllocateExtent(memory->size);
  if (!allocation.ok()) {
    ++stats_.fileAllocateFailures;
    ThrowFileAllocateFailure(allocation, memory->id);
  }

  auto payload = std::move(*memory->payload);
  memory->payload.reset();
  memory->state = BlockMemoryState::kSpilling;
  stats_.unpinnedResidentBytes -= memory->size;
  stats_.spillingBytes += memory->size;
  auto& tagStats = MutableTagStats(memory->tag);
  BOLT_CHECK_GE(tagStats.residentBytes, memory->size);
  BOLT_CHECK_GE(tagStats.unpinnedResidentBytes, memory->size);
  tagStats.residentBytes -= memory->size;
  tagStats.unpinnedResidentBytes -= memory->size;
  tagStats.spillingBytes += memory->size;

  IoResult result;
  try {
    result =
        spillStore_->Write(allocation.extent, payload, config_.writePriority);
  } catch (...) {
    auto freeResult = spillStore_->FreeExtent(allocation.extent);
    if (!freeResult.ok()) {
      ++stats_.fileFreeFailures;
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
    stats_.spillingBytes -= memory->size;
    stats_.unpinnedResidentBytes += memory->size;
    tagStats.spillingBytes -= memory->size;
    tagStats.residentBytes += memory->size;
    tagStats.unpinnedResidentBytes += memory->size;
    throw;
  }

  if (!result.ok()) {
    auto freeResult = spillStore_->FreeExtent(allocation.extent);
    if (!freeResult.ok()) {
      ++stats_.fileFreeFailures;
      LOG(FATAL)
          << "BM failed to free extent after failed spill write, block_id="
          << memory->id << ", file_error=" << static_cast<int>(freeResult.error)
          << ", native_error=" << freeResult.native_error_code;
    }
    ++stats_.writeIoFailures;
    memory->payload = std::move(result.buffer);
    memory->state = BlockMemoryState::kInMemory;
    stats_.spillingBytes -= memory->size;
    stats_.unpinnedResidentBytes += memory->size;
    tagStats.spillingBytes -= memory->size;
    tagStats.residentBytes += memory->size;
    tagStats.unpinnedResidentBytes += memory->size;
    ThrowIoFailure("write", result, memory->id);
  }

  memory->extent = spillStore_->OwnExtent(allocation.extent);
  memory->state = BlockMemoryState::kSpilled;
  stats_.spillingBytes -= memory->size;
  stats_.spilledBytes += memory->size;
  ++stats_.spillWriteCount;
  stats_.spillWriteBytes += memory->size;
  tagStats.spillingBytes -= memory->size;
  tagStats.spilledBytes += memory->size;
  tagStats.reclaimedBytes += memory->size;
  ++tagStats.spillWriteCount;
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

BufferManagerTagStats& BufferManager::MutableTagStats(MemoryTag tag) {
  const auto index = static_cast<size_t>(tag);
  if (index < tagStats_.size() && tagStats_[index].tag == tag) {
    return tagStats_[index];
  }
  return tagStats_[static_cast<size_t>(MemoryTag::kUnknown)];
}

void BufferManager::OnBlockMemoryDestroy(const BlockMemory& memory) noexcept {
  SubtractOrFatal(stats_.liveBlocks, 1, "liveBlocks", memory);
  auto& tagStats = MutableTagStats(memory.tag);
  SubtractOrFatal(tagStats.liveBlocks, 1, "tag.liveBlocks", memory);

  switch (memory.state) {
    case BlockMemoryState::kInMemory:
      if (memory.pinCount == 0) {
        SubtractOrFatal(
            stats_.unpinnedResidentBytes,
            memory.size,
            "unpinnedResidentBytes",
            memory);
        SubtractOrFatal(
            tagStats.unpinnedResidentBytes,
            memory.size,
            "tag.unpinnedResidentBytes",
            memory);
      } else {
        SubtractOrFatal(
            stats_.pinnedResidentBytes,
            memory.size,
            "pinnedResidentBytes",
            memory);
        SubtractOrFatal(
            tagStats.pinnedResidentBytes,
            memory.size,
            "tag.pinnedResidentBytes",
            memory);
      }
      SubtractOrFatal(
          tagStats.residentBytes, memory.size, "tag.residentBytes", memory);
      break;
    case BlockMemoryState::kSpilled:
      SubtractOrFatal(stats_.spilledBytes, memory.size, "spilledBytes", memory);
      SubtractOrFatal(
          tagStats.spilledBytes, memory.size, "tag.spilledBytes", memory);
      break;
    case BlockMemoryState::kPrefetching:
      SubtractOrFatal(stats_.spilledBytes, memory.size, "spilledBytes", memory);
      SubtractOrFatal(
          stats_.prefetchingBytes, memory.size, "prefetchingBytes", memory);
      SubtractOrFatal(
          tagStats.spilledBytes, memory.size, "tag.spilledBytes", memory);
      SubtractOrFatal(
          tagStats.prefetchingBytes,
          memory.size,
          "tag.prefetchingBytes",
          memory);
      break;
    case BlockMemoryState::kSpilling:
      SubtractOrFatal(
          stats_.spillingBytes, memory.size, "spillingBytes", memory);
      SubtractOrFatal(
          tagStats.spillingBytes, memory.size, "tag.spillingBytes", memory);
      break;
  }
}

} // namespace bytedance::bolt::memory::bm
