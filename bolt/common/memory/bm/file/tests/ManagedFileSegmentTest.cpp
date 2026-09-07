/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bolt/common/memory/bm/file/ManagedFileSegment.h"
#include "bolt/common/memory/bm/file/FileSegmentAllocator.h"
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
