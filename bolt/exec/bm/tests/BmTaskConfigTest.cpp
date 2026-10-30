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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/compress/CompressionRecord.h"
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

#include <cstring>
#include <string>
#include <vector>

#include <zstd.h>

namespace bytedance::bolt::exec::test {
namespace {

class BmTaskConfigTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    spillDirectory_ = TempDirectoryPath::create();
  }

  std::shared_ptr<Task> makeTask(
      std::unordered_map<std::string, std::string> configs) {
    configs[core::QueryConfig::kBufferManagerEnabled] = "true";
    auto plan = PlanBuilder()
                    .values({makeRowVector({makeFlatVector<int64_t>({1})})})
                    .planFragment();
    return Task::create(
        "bm-bucket-config",
        std::move(plan),
        0,
        core::QueryCtx::create(
            driverExecutor_.get(), core::QueryConfig{std::move(configs)}),
        Task::ExecutionMode::kParallel,
        Consumer{},
        0,
        common::SpillDiskOptions{
            .spillDirPath = spillDirectory_->path, .spillDirCreated = true});
  }

  std::shared_ptr<TempDirectoryPath> spillDirectory_;
};

TEST_F(BmTaskConfigTest, defaultsAndOverrides) {
  const core::QueryConfig defaults{{}};
  EXPECT_EQ(2LL << 30, defaults.bufferManagerSpillFileSize());
  EXPECT_EQ(64, defaults.bufferManagerSpillMaxFilesPerBucket());
  EXPECT_EQ("zstd", defaults.bufferManagerSpillCompressionKind());
  const core::QueryConfig overrides{
      {{core::QueryConfig::kBufferManagerSpillFileSize, "1073741824"},
       {core::QueryConfig::kBufferManagerSpillMaxFilesPerBucket, "128"}}};
  EXPECT_EQ(1LL << 30, overrides.bufferManagerSpillFileSize());
  EXPECT_EQ(128, overrides.bufferManagerSpillMaxFilesPerBucket());
  EXPECT_NE(nullptr, makeTask({})->bufferManager());
  EXPECT_NE(nullptr, makeTask(overrides.rawConfigsCopy())->bufferManager());
}

TEST_F(BmTaskConfigTest, rejectsInvalidBucketLimits) {
  for (const auto* size : {"-1", "0", "4190208", "4194305"}) {
    SCOPED_TRACE(size);
    BOLT_ASSERT_THROW(
        makeTask({{core::QueryConfig::kBufferManagerSpillFileSize, size}}),
        "buffer_manager_spill_file_size must be 4 KiB aligned and at least");
  }
  for (const auto* files : {"-1", "0", "4294967296"}) {
    SCOPED_TRACE(files);
    BOLT_ASSERT_THROW(
        makeTask(
            {{core::QueryConfig::kBufferManagerSpillMaxFilesPerBucket, files}}),
        "buffer_manager_spill_max_files_per_bucket must be between");
  }
}

TEST_F(BmTaskConfigTest, compressionLevelDefaultsAndValidation) {
  const core::QueryConfig defaults{{}};
  EXPECT_FALSE(defaults.bufferManagerSpillCompressionLevel().has_value());
  const core::QueryConfig selected{
      {{core::QueryConfig::kBufferManagerSpillCompressionLevel, "9"}}};
  ASSERT_TRUE(selected.bufferManagerSpillCompressionLevel().has_value());
  EXPECT_EQ(9, *selected.bufferManagerSpillCompressionLevel());

  for (const auto* codec : {"", "zstd", "openzl"}) {
    for (const auto* level : {"-2", "0", "9"}) {
      EXPECT_NE(
          nullptr,
          makeTask(
              {{core::QueryConfig::kBufferManagerSpillCompressionKind, codec},
               {core::QueryConfig::kBufferManagerSpillCompressionLevel, level}})
              ->bufferManager());
    }
  }
  // No compression means no level is applied, even when it is out of the
  // Zstd range. This also overrides the legacy spill codec.
  EXPECT_NE(
      nullptr,
      makeTask({{core::QueryConfig::kBufferManagerSpillCompressionKind, "none"},
                {core::QueryConfig::kSpillCompressionKind, "zstd"},
                {core::QueryConfig::kBufferManagerSpillCompressionLevel,
                 std::to_string(ZSTD_maxCLevel() + 1)}})
          ->bufferManager());
  for (const auto* codec : {"", "zstd"}) {
    for (const int level : {ZSTD_minCLevel() - 1, ZSTD_maxCLevel() + 1}) {
      BOLT_ASSERT_THROW(
          makeTask(
              {{core::QueryConfig::kBufferManagerSpillCompressionKind, codec},
               {core::QueryConfig::kBufferManagerSpillCompressionLevel,
                std::to_string(level)}}),
          "Invalid BM Zstd compression level");
    }
  }
  BOLT_ASSERT_THROW(
      makeTask(
          {{core::QueryConfig::kBufferManagerSpillCompressionKind, "lz4"}}),
      "Unsupported BufferManager spill compression kind");
}

TEST_F(BmTaskConfigTest, compressionLevelReachesSpillCodec) {
#ifndef IO_URING_SUPPORTED
  GTEST_SKIP() << "Requires IO_URING_SUPPORTED";
#endif
  try {
    memory::bm::diskIoScheduler().ensureReady();
  } catch (const std::exception& e) {
    if (std::string(e.what()).find("io_uring_queue_init failed") !=
        std::string::npos) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  std::vector<uint64_t> values(128 * 1024);
  uint64_t random = 0x123456789abcdefULL;
  for (auto& value : values) {
    random ^= random << 13;
    random ^= random >> 7;
    random ^= random << 17;
    value = random % 4096;
  }
  const auto size = values.size() * sizeof(uint64_t);
  std::vector<char> compressed(ZSTD_compressBound(size));
  const auto compressedSize = [&](int level) {
    const auto bytes = ZSTD_compress(
        compressed.data(), compressed.size(), values.data(), size, level);
    BOLT_CHECK(!ZSTD_isError(bytes));
    return bytes;
  };
  // Ensure this input can distinguish a wired level from the default.
  ASSERT_NE(compressedSize(1), compressedSize(9));
  ASSERT_NE(compressedSize(3), compressedSize(9));
  for (const auto* codec : {"default", "", "none", "zstd", "openzl"}) {
    for (const int level : {1, 9}) {
      SCOPED_TRACE(fmt::format("codec={}, level={}", codec, level));
      std::unordered_map<std::string, std::string> configs{
          {core::QueryConfig::kBufferManagerSpillCompressionLevel,
           std::to_string(level)},
          {core::QueryConfig::kSpillCompressionKind, "none"}};
      if (std::string(codec) != "default") {
        configs[core::QueryConfig::kBufferManagerSpillCompressionKind] = codec;
      }
      auto task = makeTask(std::move(configs));
      auto manager = task->bufferManager();
      auto handle = manager->Allocate(size, memory::bm::MemoryTag::kTesting);
      std::memcpy(handle.Ptr(), values.data(), size);
      auto block = handle.block();
      handle = {};
      manager->SpillBlocks(std::span{&block, 1});

      // These are opaque blocks. OpenZL falls back to Zstd with its own
      // default level, independently of the selected OpenZL level.
      const auto expectedBodySize = std::string(codec) == "none"
          ? size
          : compressedSize(std::string(codec) == "openzl" ? 3 : level);
      const auto stats = manager->stats();
      EXPECT_EQ(
          expectedBodySize + memory::bm::compress::SpillRecordHeaderSize(),
          stats.spillPhysicalWriteBytes);
      EXPECT_EQ(std::string(codec) == "none" ? 0 : 1, stats.spillZstdBlocks);
      auto pinned = manager->Pin(block);
      EXPECT_EQ(0, std::memcmp(pinned.Ptr(), values.data(), size));
    }
  }
}

TEST_F(BmTaskConfigTest, bucketCapacityUsesBothTaskConfigs) {
#ifndef IO_URING_SUPPORTED
  GTEST_SKIP() << "Requires IO_URING_SUPPORTED";
#endif
  try {
    memory::bm::diskIoScheduler().ensureReady();
  } catch (const std::exception& e) {
    if (std::string(e.what()).find("io_uring_queue_init failed") !=
        std::string::npos) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  // A 32 KiB payload plus the record header uses a 64 KiB bucket slot.
  // One 4 MiB file has 64 slots. The 65th record requires either a larger
  // file or a second file. Keep all blocks alive so slots cannot be recycled.
  for (const auto& [fileSize, files] :
       {std::pair{4LL << 20, 1}, {8LL << 20, 1}, {4LL << 20, 2}}) {
    SCOPED_TRACE(fmt::format("fileSize={}, files={}", fileSize, files));
    auto task = makeTask(
        {{core::QueryConfig::kBufferManagerSpillFileSize,
          std::to_string(fileSize)},
         {core::QueryConfig::kBufferManagerSpillMaxFilesPerBucket,
          std::to_string(files)}});
    auto manager = task->bufferManager();
    std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
    for (int i = 0; i < 65; ++i) {
      auto handle =
          manager->Allocate(32 * 1024, memory::bm::MemoryTag::kTesting);
      std::memset(handle.Ptr(), i, 32 * 1024);
      blocks.push_back(handle.block());
      handle = {};
      const auto block = std::span{&blocks.back(), 1};
      if (i == 64 && fileSize == (4LL << 20) && files == 1) {
        BOLT_ASSERT_THROW(
            manager->SpillBlocks(block), "BM file allocation failed");
      } else {
        ASSERT_NO_THROW(manager->SpillBlocks(block));
        EXPECT_TRUE(manager->IsSpilled(blocks.back()));
      }
    }
  }

  // Limits apply independently to each bucket in the same manager. Keep the
  // first bucket full while filling the second, so a manager-wide file or
  // byte limit cannot pass this check.
  auto task = makeTask(
      {{core::QueryConfig::kBufferManagerSpillFileSize, "4194304"},
       {core::QueryConfig::kBufferManagerSpillMaxFilesPerBucket, "1"},
       {core::QueryConfig::kBufferManagerSpillCompressionKind, "none"}});
  auto manager = task->bufferManager();
  std::vector<std::pair<std::shared_ptr<memory::bm::BlockHandle>, size_t>>
      retainedBlocks;
  for (const size_t payloadSize : {32 * 1024, 64 * 1024}) {
    // The record header moves these payloads into 64 KiB and 128 KiB slots.
    const auto slots = (4 * 1024 * 1024) / (2 * payloadSize);
    SCOPED_TRACE(fmt::format("payloadSize={}, slots={}", payloadSize, slots));
    for (size_t i = 0; i <= slots; ++i) {
      auto handle =
          manager->Allocate(payloadSize, memory::bm::MemoryTag::kTesting);
      std::memset(handle.Ptr(), 0x5a, payloadSize);
      auto block = handle.block();
      handle = {};
      if (i == slots) {
        BOLT_ASSERT_THROW(
            manager->SpillBlocks(std::span{&block, 1}),
            "BM file allocation failed");
      } else {
        ASSERT_NO_THROW(manager->SpillBlocks(std::span{&block, 1}));
        EXPECT_TRUE(manager->IsSpilled(block));
        retainedBlocks.emplace_back(std::move(block), payloadSize);
      }
    }
  }
  ASSERT_EQ(96, retainedBlocks.size());
  for (const auto& [block, payloadSize] : retainedBlocks) {
    auto pinned = manager->Pin(block);
    const std::vector<char> expected(payloadSize, 0x5a);
    EXPECT_EQ(0, std::memcmp(pinned.Ptr(), expected.data(), payloadSize));
  }
}

} // namespace
} // namespace bytedance::bolt::exec::test
