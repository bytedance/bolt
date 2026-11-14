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

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <random>
#include <string>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/PartitionBlockOutput.h"

namespace bytedance::bolt::shuffle::sparksql {
namespace {

/// Blocks of 16 KiB: a cap of 64 KiB holds four.
constexpr int64_t kBlock = 16 << 10;
constexpr int64_t kCap = 4 * kBlock;

class SpillMemoryTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    char pathTemplate[] = "/tmp/bolt_spill_memory_test_XXXXXX";
    ASSERT_NE(::mkdtemp(pathTemplate), nullptr);
    dir_ = pathTemplate;
    root_ = memory::memoryManager()->addRootPool();
    pool_ = root_->addLeafChild("spill");
  }

  void TearDown() override {
    // A block's maybeReserve() leaves the pool a minimum reservation until
    // release(), as the writers call it at their end; each test's
    // SpillMemory is gone by now.
    pool_->release();
    std::filesystem::remove_all(dir_);
  }

  /// Spill files sit next to this data file.
  PartitionWriterOptions options() const {
    PartitionWriterOptions options;
    options.dataFile = dir_ + "/data";
    return options;
  }

  /// Appends `bytes` random bytes to `file`, and to `expected`.
  void append(ShuffleSpillFile& file, std::string& expected, size_t bytes) {
    std::string data(bytes, '\0');
    for (auto& byte : data) {
      byte = static_cast<char>(rng_());
    }
    EXPECT_EQ(file.append(data.data(), data.size()), expected.size());
    expected += data;
  }

  /// Appends `bytes` to `file` in pieces of odd sizes, as runs come.
  void
  appendPieces(ShuffleSpillFile& file, std::string& expected, size_t bytes) {
    while (bytes > 0) {
      const size_t piece = std::min<size_t>(bytes, 1000 + rng_() % 9000);
      append(file, expected, piece);
      bytes -= piece;
    }
  }

  /// Reads `file` back whole, through read() and copyTo(), and a range
  /// across every block boundary.
  static void expectContents(
      ShuffleSpillFile& file,
      const std::string& expected) {
    file.flush();
    ASSERT_EQ(file.end(), expected.size());
    std::string read(expected.size(), '\0');
    file.read(0, read.data(), read.size());
    EXPECT_EQ(read, expected);
    std::string copied;
    file.copyTo(0, expected.size(), [&](const char* data, size_t bytes) {
      copied.append(data, bytes);
    });
    EXPECT_EQ(copied, expected);
    if (expected.size() > 2) {
      std::string middle(expected.size() - 2, '\0');
      file.read(1, middle.data(), middle.size());
      EXPECT_EQ(middle, expected.substr(1, middle.size()));
    }
  }

  std::string dir_;
  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::mt19937 rng_{91};
};

TEST_F(SpillMemoryTest, keepsTheNewestBytes) {
  // Past the cap, a file reuses its oldest block, written out first: the
  // memory keeps the newest bytes, at most the cap, and the disk the rest.
  SpillMemory memory(pool_.get(), kCap);
  ShuffleSpillFile file(options(), "newest", &memory);
  std::string expected;
  appendPieces(file, expected, 300 << 10);
  const auto onDisk = static_cast<size_t>(file.bytesWritten());
  EXPECT_GE(onDisk, expected.size() - kCap);
  EXPECT_LT(onDisk, expected.size());
  EXPECT_EQ(memory.flushedBytes(), onDisk);
  EXPECT_EQ(memory.directBytes(), uint64_t{0});
  EXPECT_EQ(memory.heldBytes(), kCap);
  EXPECT_EQ(memory.peakBytes(), kCap);
  expectContents(file, expected);
}

TEST_F(SpillMemoryTest, releaseWritesTheOldestOutAndEndsTheGrowth) {
  // Asked for a byte, release() writes the oldest block out and frees it,
  // and the memory grows no more: later bytes reuse the blocks left.
  SpillMemory memory(pool_.get(), kCap);
  ShuffleSpillFile file(options(), "release", &memory);
  std::string expected;
  append(file, expected, 2 * kBlock + kBlock / 2);
  EXPECT_EQ(memory.heldBytes(), 3 * kBlock);
  EXPECT_EQ(memory.release(1), kBlock);
  EXPECT_EQ(file.bytesWritten(), kBlock);
  EXPECT_EQ(memory.heldBytes(), 2 * kBlock);
  appendPieces(file, expected, 100 << 10);
  EXPECT_EQ(memory.heldBytes(), 2 * kBlock);
  EXPECT_EQ(memory.peakBytes(), 3 * kBlock);
  EXPECT_GE(
      static_cast<size_t>(file.bytesWritten()), expected.size() - 2 * kBlock);
  expectContents(file, expected);
  // Asked for nothing, it frees nothing.
  EXPECT_EQ(memory.release(0), 0);
  EXPECT_EQ(memory.heldBytes(), 2 * kBlock);
}

TEST_F(SpillMemoryTest, releaseAllEmptiesTheMemory) {
  // releaseAll() writes every block out, the last, partly filled one
  // included; with no block left anywhere, a file appends to disk.
  SpillMemory memory(pool_.get(), kCap);
  ShuffleSpillFile file(options(), "all", &memory);
  std::string expected;
  append(file, expected, 2 * kBlock + kBlock / 2);
  EXPECT_EQ(memory.releaseAll(), 3 * kBlock);
  EXPECT_EQ(memory.heldBytes(), 0);
  EXPECT_EQ(static_cast<size_t>(file.bytesWritten()), expected.size());
  append(file, expected, 10 << 10);
  EXPECT_EQ(memory.heldBytes(), 0);
  EXPECT_EQ(memory.directBytes(), uint64_t{10 << 10});
  EXPECT_EQ(static_cast<size_t>(file.bytesWritten()), expected.size());
  expectContents(file, expected);
}

TEST_F(SpillMemoryTest, offKeepsNothing) {
  // A cap below four machine pages keeps nothing in memory.
  SpillMemory memory(pool_.get(), 8 << 10);
  ShuffleSpillFile file(options(), "off", &memory);
  std::string expected;
  appendPieces(file, expected, 50 << 10);
  EXPECT_EQ(memory.peakBytes(), 0);
  EXPECT_EQ(static_cast<size_t>(file.bytesWritten()), expected.size());
  expectContents(file, expected);
}

TEST_F(SpillMemoryTest, aReleaseBetweenBlocksKeepsTheFileWhole) {
  // A block's reservation may reclaim the writer, whose reclaim writes
  // blocks of the asking file out: with its last block full, as when the
  // file asks. The file then goes on from what is left.
  SpillMemory memory(pool_.get(), kCap);
  ShuffleSpillFile file(options(), "between", &memory);
  std::string expected;
  append(file, expected, 2 * kBlock);
  EXPECT_EQ(memory.release(kBlock), kBlock);
  EXPECT_EQ(file.bytesWritten(), kBlock);
  // No growth after a release: the next bytes reuse the block left,
  // written out first.
  append(file, expected, kBlock / 4);
  EXPECT_EQ(file.bytesWritten(), 2 * kBlock);
  EXPECT_EQ(memory.heldBytes(), kBlock);
  EXPECT_EQ(memory.releaseAll(), kBlock);
  append(file, expected, kBlock / 4);
  EXPECT_EQ(static_cast<size_t>(file.bytesWritten()), expected.size());
  expectContents(file, expected);
}

TEST_F(SpillMemoryTest, filesShareTheMemory) {
  // Two files under one cap: past it, the one asking reuses the oldest
  // block of the file holding the most.
  SpillMemory memory(pool_.get(), kCap);
  ShuffleSpillFile first(options(), "first", &memory);
  ShuffleSpillFile second(options(), "second", &memory);
  std::string firstExpected;
  std::string secondExpected;
  append(first, firstExpected, kCap);
  append(second, secondExpected, kBlock + kBlock / 4);
  EXPECT_EQ(first.bytesWritten(), 2 * kBlock);
  EXPECT_EQ(second.bytesWritten(), 0);
  EXPECT_EQ(memory.heldBytes(), kCap);
  expectContents(first, firstExpected);
  expectContents(second, secondExpected);
}

TEST_F(SpillMemoryTest, aShortPoolEndsTheGrowth) {
  // A block the pool cannot reserve ends the growth as a release() does:
  // the memory keeps what it holds and reuses it. Blocks of 4 MiB under a
  // 64 MiB cap, in a pool of 16 MiB.
  auto root = memory::memoryManager()->addRootPool("", 16 << 20);
  auto pool = root->addLeafChild("spill");
  {
    SpillMemory memory(pool.get(), 64 << 20);
    ShuffleSpillFile file(options(), "short", &memory);
    std::string expected;
    appendPieces(file, expected, 40 << 20);
    EXPECT_GT(memory.peakBytes(), 0);
    EXPECT_LE(memory.peakBytes(), 16 << 20);
    EXPECT_EQ(memory.heldBytes(), memory.peakBytes());
    EXPECT_GT(file.bytesWritten(), 0);
    expectContents(file, expected);
  }
  pool->release();
}

TEST_F(SpillMemoryTest, patchesReachTheBytesOnDisk) {
  // A run's header is patched once its body is written: on disk by then,
  // or still in memory, the patch lands either way.
  SpillMemory memory(pool_.get(), kCap);
  ShuffleSpillFile file(options(), "patch", &memory);
  std::string expected;
  append(file, expected, 8);
  appendPieces(file, expected, 100 << 10);
  ASSERT_GT(file.bytesWritten(), 8);
  const uint64_t inMemory = expected.size() - kBlock / 2;
  const std::string header = "headerAB";
  file.patch(0, header.data(), header.size());
  expected.replace(0, header.size(), header);
  file.patch(inMemory, header.data(), header.size());
  expected.replace(inMemory, header.size(), header);
  expectContents(file, expected);
}

TEST_F(SpillMemoryTest, nothingIsReleasedDuringTheMerge) {
  // The merge reads the blocks: the writer cannot be reclaimed while it
  // stops, so a release() then is a bug.
  SpillMemory memory(pool_.get(), kCap);
  ShuffleSpillFile file(options(), "merge", &memory);
  std::string expected;
  append(file, expected, kBlock);
  file.flush();
  file.freeze();
  BOLT_ASSERT_THROW(memory.release(1), "released during the merge");
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql
