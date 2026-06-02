#include "bolt/common/memory/bm/file/FileSegmentAllocator.h"
#include "bolt/common/memory/bm/file/ManagedFileSegment.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"

#include <filesystem>

#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm {

TEST(ManagedFileSegmentTest, MoveAndExplicitFree) {
  const auto directory = test::UniqueTempDir("bolt-bm-owned-file-segment");
  std::filesystem::remove_all(directory);
  auto allocator =
      CreateFileSegmentAllocator(test::ValidConfigWithDirectory(directory));
  ASSERT_NE(nullptr, allocator);

  auto first = allocator->Allocate(4096);
  ASSERT_TRUE(first.ok());
  ManagedFileSegment segment{first.segment, allocator};
  ASSERT_TRUE(segment.valid());
  EXPECT_EQ(first.segment.id, segment.segment().id);

  ManagedFileSegment moved{std::move(segment)};
  EXPECT_FALSE(segment.valid());
  ASSERT_TRUE(moved.valid());

  auto second = allocator->Allocate(4096);
  ASSERT_TRUE(second.ok());
  ManagedFileSegment assigned{second.segment, allocator};
  assigned = std::move(moved);
  EXPECT_FALSE(moved.valid());
  ASSERT_TRUE(assigned.valid());
  EXPECT_EQ(first.segment.id, assigned.segment().id);

  assigned.FreeOrFatal("ManagedFileSegmentMoveAndExplicitFree");
  EXPECT_FALSE(assigned.valid());
  std::filesystem::remove_all(directory);
}

} // namespace bytedance::bolt::memory::bm
