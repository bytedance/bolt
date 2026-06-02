#include "bolt/common/memory/bm/AllocateSize.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/MemoryTag.h"

#include <exception>

#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm {

TEST(BufferManagerValueMappingTest, AllMemoryTagsHaveStableNames) {
  EXPECT_STREQ("Unknown", toString(MemoryTag::kUnknown));
  EXPECT_STREQ("HashBuild", toString(MemoryTag::kHashBuild));
  EXPECT_STREQ("Aggregation", toString(MemoryTag::kAggregation));
  EXPECT_STREQ("Sort", toString(MemoryTag::kSort));
  EXPECT_STREQ("Window", toString(MemoryTag::kWindow));
  EXPECT_STREQ("Exchange", toString(MemoryTag::kExchange));
  EXPECT_STREQ("Testing", toString(MemoryTag::kTesting));
  EXPECT_STREQ("Unknown", toString(static_cast<MemoryTag>(123)));
}

TEST(BufferManagerValueMappingTest, AllAllocateSizesHaveStableNamesAndBytes) {
  EXPECT_EQ(256 * 1024, allocateSizeBytes(AllocateSize::kSmall));
  EXPECT_EQ(1024 * 1024, allocateSizeBytes(AllocateSize::kMedium));
  EXPECT_EQ(4 * 1024 * 1024, allocateSizeBytes(AllocateSize::kLarge));
  EXPECT_STREQ("small", toString(AllocateSize::kSmall));
  EXPECT_STREQ("medium", toString(AllocateSize::kMedium));
  EXPECT_STREQ("large", toString(AllocateSize::kLarge));
  EXPECT_THROW(
      (void)allocateSizeBytes(static_cast<AllocateSize>(99)), std::exception);
  EXPECT_THROW((void)toString(static_cast<AllocateSize>(99)), std::exception);
}

TEST(
    BufferManagerValueMappingTest,
    DefaultReclaimWriteInflightMatchesRingDepth) {
  BufferManagerConfig config;
  EXPECT_EQ(128, config.maxReclaimWriteInflight);
}

} // namespace bytedance::bolt::memory::bm
