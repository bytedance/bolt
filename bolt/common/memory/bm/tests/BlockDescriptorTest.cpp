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

#include "bolt/common/memory/bm/BlockDescriptor.h"

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"

#include <array>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm {
namespace {

BlockDescriptor fixedRowDescriptor(
    uint32_t elementCount,
    uint32_t rowStride,
    std::vector<BlockFieldSchema> fields) {
  return BlockDescriptor{
      .schemaKind = BlockSchemaKind::kFixedRow,
      .elementCount = elementCount,
      .schema =
          FixedRowBlockSchema{
              .rowStride = rowStride,
              .fields = std::move(fields),
          },
  };
}

TEST(BlockDescriptorTest, ValidatesAndNormalizesFixedRowSchema) {
  auto descriptor = fixedRowDescriptor(
      4,
      16,
      {
          {BlockFieldKind::kFloatingPoint, 8, 4},
          {BlockFieldKind::kSignedInteger, 0, 4},
      });

  EXPECT_NO_THROW(ValidateBlockDescriptor(descriptor, 80));
  const auto normalized =
      NormalizeFixedRowFields(std::get<FixedRowBlockSchema>(descriptor.schema));
  ASSERT_EQ(4, normalized.size());
  EXPECT_EQ(
      (BlockFieldSchema{BlockFieldKind::kSignedInteger, 0, 4}), normalized[0]);
  EXPECT_EQ((BlockFieldSchema{BlockFieldKind::kOpaque, 4, 4}), normalized[1]);
  EXPECT_EQ(
      (BlockFieldSchema{BlockFieldKind::kFloatingPoint, 8, 4}), normalized[2]);
  EXPECT_EQ((BlockFieldSchema{BlockFieldKind::kOpaque, 12, 4}), normalized[3]);
}

TEST(BlockDescriptorTest, RejectsSchemaKindVariantMismatch) {
  BlockDescriptor descriptor{
      .schemaKind = BlockSchemaKind::kFixedRow,
      .elementCount = 1,
      .schema = OpaqueBlockSchema{},
  };
  EXPECT_ANY_THROW(ValidateBlockDescriptor(descriptor, 64));
}

TEST(BlockDescriptorTest, RejectsInvalidFixedRowGeometry) {
  EXPECT_ANY_THROW(ValidateBlockDescriptor(fixedRowDescriptor(1, 0, {}), 64));
  EXPECT_ANY_THROW(ValidateBlockDescriptor(
      fixedRowDescriptor(1, 8, {{BlockFieldKind::kOpaque, 0, 0}}), 64));
  EXPECT_ANY_THROW(ValidateBlockDescriptor(
      fixedRowDescriptor(1, 8, {{BlockFieldKind::kOpaque, 7, 2}}), 64));
  EXPECT_ANY_THROW(ValidateBlockDescriptor(
      fixedRowDescriptor(
          1,
          8,
          {
              {BlockFieldKind::kOpaque, 0, 5},
              {BlockFieldKind::kOpaque, 4, 4},
          }),
      64));
}

TEST(BlockDescriptorTest, RejectsStructuredPrefixLargerThanBlock) {
  EXPECT_ANY_THROW(ValidateBlockDescriptor(fixedRowDescriptor(9, 8, {}), 64));
  EXPECT_ANY_THROW(ValidateBlockDescriptor(
      fixedRowDescriptor(
          std::numeric_limits<uint32_t>::max(),
          std::numeric_limits<uint32_t>::max(),
          {}),
      64));
}

TEST(BlockDescriptorTest, AcceptsOpaqueSchemaOnlyWithMatchingVariant) {
  BlockDescriptor opaque{
      .schemaKind = BlockSchemaKind::kOpaque,
      .elementCount = 0,
      .schema = OpaqueBlockSchema{},
  };
  EXPECT_NO_THROW(ValidateBlockDescriptor(opaque, 64));

  opaque.schema = FixedRowBlockSchema{.rowStride = 8, .fields = {}};
  EXPECT_ANY_THROW(ValidateBlockDescriptor(opaque, 64));
}

class BlockDescriptorBufferManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    root_ = manager_.addRootPool(
        fmt::format(
            "block-descriptor-root-{}",
            testing::UnitTest::GetInstance()->current_test_info()->name()),
        64 * 1024 * 1024,
        MemoryReclaimer::create());
  }

  std::shared_ptr<BufferManager> makeBufferManager(
      std::string_view suffix = "primary",
      compress::CompressionKind compressionKind =
          compress::CompressionKind::kZstdFrame,
      size_t minCompressBytes = 256 * 1024) {
    const auto directory =
        test::UniqueTempDir(fmt::format("bolt-bm-block-descriptor-{}", suffix));
    std::filesystem::remove_all(directory);
    BufferManagerConfig config;
    config.poolName = fmt::format("block-descriptor-{}", suffix);
    config.spillStoreConfig.fileAllocatorConfig =
        test::ValidConfigWithDirectory(directory);
    config.spillStoreConfig.compressionConfig.kind = compressionKind;
    config.spillStoreConfig.compressionConfig.minCompressBytes =
        minCompressBytes;
    return BufferManager::Create(*root_, std::move(config));
  }

  MemoryManager manager_;
  std::shared_ptr<MemoryPool> root_;
};

TEST_F(BlockDescriptorBufferManagerTest, DescriptorIsImmutableAfterFirstSet) {
  auto manager = makeBufferManager();
  auto handle = manager->Allocate(64, MemoryTag::kTesting);
  auto block = handle.block();
  auto descriptor =
      std::make_shared<const BlockDescriptor>(fixedRowDescriptor(4, 16, {}));

  EXPECT_NO_THROW(manager->SetBlockDescriptor(block, descriptor));
  EXPECT_ANY_THROW(manager->SetBlockDescriptor(block, descriptor));
  EXPECT_ANY_THROW(manager->SetBlockDescriptor(nullptr, descriptor));
  EXPECT_ANY_THROW(manager->SetBlockDescriptor(block, nullptr));
}

TEST_F(BlockDescriptorBufferManagerTest, DescriptorRequiresPinnedOwnerBlock) {
  auto owner = makeBufferManager("owner");
  auto other = makeBufferManager("other");
  auto descriptor =
      std::make_shared<const BlockDescriptor>(fixedRowDescriptor(4, 16, {}));

  std::shared_ptr<BlockHandle> unpinnedBlock;
  {
    auto handle = owner->Allocate(64, MemoryTag::kTesting);
    unpinnedBlock = handle.block();
  }
  EXPECT_ANY_THROW(owner->SetBlockDescriptor(unpinnedBlock, descriptor));

  auto pinned = owner->Allocate(64, MemoryTag::kTesting);
  EXPECT_ANY_THROW(other->SetBlockDescriptor(pinned.block(), descriptor));
}

TEST_F(
    BlockDescriptorBufferManagerTest,
    DescriptorPersistsAcrossDirtyAndSecondSpill) {
  auto manager = makeBufferManager(
      "dirty-respill", compress::CompressionKind::kOpenZlFrame, 1);
  constexpr size_t kBlockSize = 4096;
  constexpr uint32_t kRowStride = 16;
  constexpr uint32_t kRows = kBlockSize / kRowStride;
  auto descriptor = std::make_shared<const BlockDescriptor>(fixedRowDescriptor(
      kRows,
      kRowStride,
      {{BlockFieldKind::kSignedInteger, 0, sizeof(int32_t)}}));

  auto handle = manager->Allocate(kBlockSize, MemoryTag::kTesting);
  auto block = handle.block();
  manager->SetBlockDescriptor(block, descriptor);
  std::memset(handle.Ptr(), 7, kBlockSize);
  handle = BufferHandle{};

  const std::array<std::shared_ptr<BlockHandle>, 1> blocks{block};
  try {
    manager->SpillBlocks(blocks);
    auto repinned = manager->Pin(block);
    EXPECT_EQ(7, repinned.Ptr()[0]);
    manager->MarkDirty(block);
    std::memset(repinned.Ptr(), 11, kBlockSize);
    repinned = BufferHandle{};
    manager->SpillBlocks(blocks);
  } catch (const std::exception& error) {
    const std::string message = error.what();
    if (message.find("io_uring_queue_init failed") != std::string::npos) {
      GTEST_SKIP() << message;
    }
    throw;
  }

  auto repinned = manager->Pin(block);
  EXPECT_EQ(11, repinned.Ptr()[0]);
  EXPECT_EQ(11, repinned.Ptr()[kBlockSize - 1]);
  const auto stats = manager->stats();
  EXPECT_EQ(2, stats.spillWriteCount);
  EXPECT_EQ(2, stats.spillCompressedBlocks);
}

} // namespace
} // namespace bytedance::bolt::memory::bm
