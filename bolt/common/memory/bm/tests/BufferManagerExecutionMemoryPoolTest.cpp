#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/common/memory/bm/tests/SparkListenableArbitratorContext.h"

#include <cstring>
#include <filesystem>
#include <memory>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

namespace bytedance::bolt::memory::bm {
namespace {

bool IsIoUringUnavailable(const std::exception& e) {
  return std::string(e.what()).find("io_uring_queue_init failed") !=
      std::string::npos;
}

std::shared_ptr<BufferManager> makeBufferManager(
    MemoryPool& root,
    const std::string& name) {
  const auto directory =
      test::UniqueTempDir(fmt::format("bolt-bm-execution-pool-{}", name));
  std::filesystem::remove_all(directory);

  BufferManagerConfig config;
  config.poolName = fmt::format("bm-execution-pool-{}", name);
  config.spillStoreConfig.fileAllocatorConfig =
      test::ValidConfigWithDirectory(directory);
  return BufferManager::Create(root, std::move(config));
}

} // namespace

TEST(BufferManagerExecutionMemoryPoolTest, AutomaticSpillerReclaimsBmBlocks) {
  constexpr int64_t kMemoryLimit = 1024 * 1024;
  constexpr size_t kBlockSize = 256 * 1024;

  SparkListenableArbitratorContext context{
      SparkListenableArbitratorContextOptions{
          .name = "bm-execution-memory-pool-spill-test",
          .memoryLimitBytes = kMemoryLimit,
          .sessionConf = {}}};
  context.installAutomaticReclaimSpill();

  auto root = context.rootPool();
  auto bm = makeBufferManager(*root, "automatic-spill");

  std::vector<std::shared_ptr<BlockHandle>> blocks;
  {
    auto handles = bm->BatchAllocate(3, kBlockSize, MemoryTag::kTesting);
    blocks.reserve(handles.size());
    for (size_t i = 0; i < handles.size(); ++i) {
      std::memset(handles[i].Ptr(), static_cast<int>(i + 1), kBlockSize);
      blocks.push_back(handles[i].block());
    }
  }
  ASSERT_EQ(3 * kBlockSize, bm->reclaimableBytes());

  auto pressurePool = root->addLeafChild("bm-execution-pool-pressure");
  void* pressure = nullptr;
  try {
    pressure = pressurePool->allocate(kBlockSize * 2);
  } catch (const std::exception& e) {
    if (IsIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  ASSERT_NE(nullptr, pressure);
  pressurePool->free(pressure, kBlockSize * 2);

  const auto contextStats = context.stats();
  EXPECT_GT(contextStats.automaticSpillTriggers, 0);
  EXPECT_GT(contextStats.automaticSpillReturnedBytes, 0);

  const auto bmStats = bm->stats();
  EXPECT_GT(bmStats.spillWriteBytes, 0);
  EXPECT_GT(bmStats.spilledBytes, 0);

  auto repin = bm->Pin(blocks.front());
  EXPECT_EQ(1, repin.Ptr()[0]);
}

} // namespace bytedance::bolt::memory::bm
