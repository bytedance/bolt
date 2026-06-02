#include "bolt/common/memory/bm/EvictionQueue.h"
#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/io/IoBuffer.h"

#include <cstring>
#include <memory>

#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm {
namespace {

IoBuffer MakePayload(size_t size, char value) {
  auto payload = IoBuffer::allocateFromMalloc(size);
  std::memset(payload.data(), value, payload.length());
  return payload;
}

} // namespace

TEST(EvictionQueueTest, PopsOnlyCurrentUnpinnedResidentPayloadEntries) {
  auto current = std::make_shared<BlockMemory>(1, 4096, MemoryTag::kTesting);
  current->payload = MakePayload(current->size, 'x');

  auto pinned = std::make_shared<BlockMemory>(2, 4096, MemoryTag::kTesting);
  pinned->payload = MakePayload(pinned->size, 'y');
  pinned->pinCount = 1;

  auto spilled = std::make_shared<BlockMemory>(3, 4096, MemoryTag::kTesting);
  spilled->state = BlockMemoryState::kSpilled;

  EvictionQueue queue;
  queue.Add(pinned);
  queue.Add(spilled);
  queue.Add(current);

  EXPECT_EQ(current, queue.PopEvictable());
  EXPECT_EQ(nullptr, queue.PopEvictable());

  const auto stats = queue.stats();
  EXPECT_EQ(0, stats.size);
  EXPECT_EQ(2, stats.staleEntries);
  EXPECT_TRUE(queue.empty());
}

TEST(EvictionQueueTest, SkipsExpiredAndStaleSequenceEntries) {
  EvictionQueue queue;
  auto block = std::make_shared<BlockMemory>(4, 4096, MemoryTag::kTesting);
  block->payload = MakePayload(block->size, 'z');

  queue.Add(block);
  ++block->evictionSequence;
  queue.Add(block);
  EXPECT_EQ(block, queue.PopEvictable());

  auto expired = std::make_shared<BlockMemory>(5, 4096, MemoryTag::kTesting);
  expired->payload = MakePayload(expired->size, 'w');
  queue.Add(expired);
  expired.reset();
  EXPECT_EQ(nullptr, queue.PopEvictable());

  const auto stats = queue.stats();
  EXPECT_EQ(0, stats.size);
  EXPECT_EQ(2, stats.staleEntries);
}

} // namespace bytedance::bolt::memory::bm
