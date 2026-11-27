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
#include <fstream>
#include <random>
#include <string>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/base/PartitionBlockOutput.h"

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
    // ShuffleSpillFile is gone by now.
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
  ShuffleSpillFile file(options(), "newest", pool_.get(), kCap);
  std::string expected;
  appendPieces(file, expected, 300 << 10);
  const auto onDisk = static_cast<size_t>(file.bytesWritten());
  EXPECT_GE(onDisk, expected.size() - kCap);
  EXPECT_LT(onDisk, expected.size());
  EXPECT_EQ(file.flushedBytes(), onDisk);
  EXPECT_EQ(file.directBytes(), uint64_t{0});
  EXPECT_EQ(file.heldBytes(), kCap);
  EXPECT_EQ(file.peakBytes(), kCap);
  expectContents(file, expected);
}

TEST_F(SpillMemoryTest, releaseWritesTheOldestOutAndEndsTheGrowth) {
  // Asked for a byte, release() writes the oldest block out and frees it,
  // and the memory grows no more: later bytes reuse the blocks left.
  ShuffleSpillFile file(options(), "release", pool_.get(), kCap);
  std::string expected;
  append(file, expected, 2 * kBlock + kBlock / 2);
  EXPECT_EQ(file.heldBytes(), 3 * kBlock);
  EXPECT_EQ(file.release(1), kBlock);
  EXPECT_EQ(file.bytesWritten(), kBlock);
  EXPECT_EQ(file.heldBytes(), 2 * kBlock);
  appendPieces(file, expected, 100 << 10);
  EXPECT_EQ(file.heldBytes(), 2 * kBlock);
  EXPECT_EQ(file.peakBytes(), 3 * kBlock);
  EXPECT_GE(
      static_cast<size_t>(file.bytesWritten()), expected.size() - 2 * kBlock);
  expectContents(file, expected);
  // Asked for nothing, it frees nothing.
  EXPECT_EQ(file.release(0), 0);
  EXPECT_EQ(file.heldBytes(), 2 * kBlock);
}

TEST_F(SpillMemoryTest, releaseAllEmptiesTheMemory) {
  // releaseAll() writes every block out, the last, partly filled one
  // included; with no block left, the file appends directly to disk.
  ShuffleSpillFile file(options(), "all", pool_.get(), kCap);
  std::string expected;
  append(file, expected, 2 * kBlock + kBlock / 2);
  EXPECT_EQ(file.releaseAll(), 3 * kBlock);
  EXPECT_EQ(file.heldBytes(), 0);
  EXPECT_EQ(static_cast<size_t>(file.bytesWritten()), expected.size());
  append(file, expected, 10 << 10);
  EXPECT_EQ(file.heldBytes(), 0);
  EXPECT_EQ(file.directBytes(), uint64_t{10 << 10});
  EXPECT_EQ(static_cast<size_t>(file.bytesWritten()), expected.size());
  expectContents(file, expected);
}

TEST_F(SpillMemoryTest, offKeepsNothing) {
  // A cap below four machine pages keeps nothing in memory.
  ShuffleSpillFile file(options(), "off", pool_.get(), 8 << 10);
  std::string expected;
  appendPieces(file, expected, 50 << 10);
  EXPECT_EQ(file.peakBytes(), 0);
  EXPECT_EQ(static_cast<size_t>(file.bytesWritten()), expected.size());
  expectContents(file, expected);
}

TEST_F(SpillMemoryTest, aReleaseBetweenBlocksKeepsTheFileWhole) {
  // A block's reservation may reclaim the writer, whose reclaim writes
  // blocks of this file out: with its last block full, as when the file
  // asks. The file then goes on from what is left.
  ShuffleSpillFile file(options(), "between", pool_.get(), kCap);
  std::string expected;
  append(file, expected, 2 * kBlock);
  EXPECT_EQ(file.release(kBlock), kBlock);
  EXPECT_EQ(file.bytesWritten(), kBlock);
  // No growth after a release: the next bytes reuse the block left,
  // written out first.
  append(file, expected, kBlock / 4);
  EXPECT_EQ(file.bytesWritten(), 2 * kBlock);
  EXPECT_EQ(file.heldBytes(), kBlock);
  EXPECT_EQ(file.releaseAll(), kBlock);
  append(file, expected, kBlock / 4);
  EXPECT_EQ(static_cast<size_t>(file.bytesWritten()), expected.size());
  expectContents(file, expected);
}

TEST_F(SpillMemoryTest, sharedFileSerializesProducersAndRejectsOverlap) {
  // Two outputs sharing one ShuffleSpillFile share its memory budget when
  // writing sequentially, and reject nested or concurrent writes.
  auto opts = options();
  opts.numPartitions = 2;
  ShuffleSpillFile spill(opts, "shared", pool_.get(), kCap);
  LocalBlockOutput first(opts, spill);
  LocalBlockOutput second(opts, spill);

  const auto emitBytes = [&](LocalBlockOutput& output,
                             uint32_t targetPid,
                             size_t bytes,
                             char fill) {
    output.writeBlocks([&](uint32_t pid, BlockEmitter emit) {
      if (pid != targetPid) {
        return;
      }
      std::string data(bytes, fill);
      emit(data.data(), data.size());
    });
  };

  emitBytes(first, 0, kCap, 'a');
  emitBytes(second, 1, kBlock + kBlock / 4, 'b');
  EXPECT_EQ(spill.bytesWritten(), 2 * kBlock);
  EXPECT_EQ(spill.heldBytes(), kCap);

  // Attempting to write to `second` while `first` holds the shared write guard
  // must fail fast.
  BOLT_ASSERT_THROW(
      first.writeBlocks([&](uint32_t pid, BlockEmitter /*emit*/) {
        if (pid == 0) {
          emitBytes(second, 0, 16, 'c');
        }
      }),
      "concurrent or nested write to shared ShuffleSpillFile");

  // Freezing while a write guard is held must also fail.
  {
    auto guard = spill.scopedWrite(&first);
    BOLT_ASSERT_THROW(spill.freeze(), "frozen while a write is in progress");
  }

  ShuffleWriterMetrics metrics;
  PartialSpiller::mergeToDataFile(opts, spill, {&first, &second}, metrics);
  ASSERT_EQ(metrics.partitionLengths.size(), 2u);
  EXPECT_EQ(metrics.partitionLengths[0], kCap);
  EXPECT_EQ(metrics.partitionLengths[1], kBlock + kBlock / 4);
  EXPECT_EQ(metrics.totalBytesEvicted, 2 * kBlock);
  EXPECT_EQ(metrics.totalSpillFiles, 1);
}

TEST_F(SpillMemoryTest, aShortPoolEndsTheGrowth) {
  // A block the pool cannot reserve ends the growth as a release() does:
  // the memory keeps what it holds and reuses it. Blocks of 4 MiB under a
  // 64 MiB cap, in a pool of 16 MiB.
  auto root = memory::memoryManager()->addRootPool("", 16 << 20);
  auto pool = root->addLeafChild("spill");
  {
    ShuffleSpillFile file(options(), "short", pool.get(), 64 << 20);
    std::string expected;
    appendPieces(file, expected, 40 << 20);
    EXPECT_GT(file.peakBytes(), 0);
    EXPECT_LE(file.peakBytes(), 16 << 20);
    EXPECT_EQ(file.heldBytes(), file.peakBytes());
    EXPECT_GT(file.bytesWritten(), 0);
    expectContents(file, expected);
  }
  pool->release();
}

TEST_F(SpillMemoryTest, patchesReachTheBytesOnDisk) {
  // A run's header is patched once its body is written: on disk by then,
  // or still in memory, the patch lands either way.
  ShuffleSpillFile file(options(), "patch", pool_.get(), kCap);
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
  ShuffleSpillFile file(options(), "merge", pool_.get(), kCap);
  std::string expected;
  append(file, expected, kBlock);
  file.flush();
  file.freeze();
  BOLT_ASSERT_THROW(file.release(1), "released during the merge");
}

TEST_F(SpillMemoryTest, spillFilesCountCreatedPhysicalFiles) {
  // totalSpillFiles counts physical temporary spill files created on disk:
  // runs that stay wholly in memory create no file, while multiple runs
  // flushed into the same shared ShuffleSpillFile still create one file.
  const auto writeRun = [&](LocalBlockOutput& output, size_t bytes) {
    output.writeBlocks([&](uint32_t pid, BlockEmitter emit) {
      if (pid != 0 || bytes == 0) {
        return;
      }
      std::string data(bytes, 'a');
      emit(data.data(), data.size());
    });
  };
  const auto makeOptions = [&]() {
    auto opts = options();
    opts.numPartitions = 2;
    return opts;
  };

  // All runs stay in memory: no physical spill file is created.
  {
    const auto opts = makeOptions();
    ShuffleSpillFile spill(opts, "runs", pool_.get(), kCap);
    LocalBlockOutput output(opts, spill);
    writeRun(output, kBlock / 2);
    writeRun(output, 0);
    writeRun(output, kBlock / 2);
    ShuffleWriterMetrics metrics;
    output.finish(metrics);
    EXPECT_EQ(metrics.totalBytesEvicted, 0);
    EXPECT_EQ(metrics.totalSpillFiles, 0);
  }

  // Partial flush to disk creates one physical spill file.
  {
    const auto opts = makeOptions();
    ShuffleSpillFile spill(opts, "runs", pool_.get(), kCap);
    LocalBlockOutput output(opts, spill);
    writeRun(output, kBlock / 2);
    writeRun(output, kBlock);
    writeRun(output, kBlock);
    ASSERT_EQ(spill.release(1), kBlock);
    ShuffleWriterMetrics metrics;
    output.finish(metrics);
    EXPECT_EQ(metrics.totalBytesEvicted, kBlock);
    EXPECT_EQ(metrics.totalSpillFiles, 1);
  }

  // Multiple runs flushed to disk still share the single physical spill file.
  {
    const auto opts = makeOptions();
    ShuffleSpillFile spill(opts, "runs", pool_.get(), kCap);
    LocalBlockOutput output(opts, spill);
    writeRun(output, kBlock / 2);
    writeRun(output, 0);
    writeRun(output, kBlock);
    writeRun(output, kBlock / 2);
    ASSERT_GT(spill.releaseAll(), 0);
    ShuffleWriterMetrics metrics;
    output.finish(metrics);
    EXPECT_EQ(metrics.totalBytesEvicted, 2 * kBlock);
    EXPECT_EQ(metrics.totalSpillFiles, 1);
  }
}

TEST_F(SpillMemoryTest, zeroByteAppendCreatesNoSpillFile) {
  const auto opts = options();
  ShuffleSpillFile spill(opts, "zero");
  EXPECT_EQ(spill.append(nullptr, 0), 0u);
  EXPECT_EQ(spill.end(), 0u);
  EXPECT_EQ(spill.bytesWritten(), 0);
  EXPECT_EQ(spill.numFiles(), 0);
  EXPECT_FALSE(std::filesystem::exists(opts.dataFile + ".zero.spill"));
}

TEST_F(SpillMemoryTest, dataFileWriterPatchesAndCopyFromFastPaths) {
  // Exercises DataFileWriter's 512 KiB write buffer:
  // - in-buffer patch, overflow pwrite patch, and straddling patch
  // - copyFrom() on small on-disk (< 256 KiB), large on-disk (>= 256 KiB,
  //   copy_file_range), and straddling disk/memory ranges.
  const auto opts = options();
  ShuffleSpillFile spill(opts, "copy", pool_.get(), kCap);
  std::string spillExpected;
  // Write 400 KiB so the first > 336 KiB is on disk and the newest 64 KiB stays
  // in memory blocks.
  appendPieces(spill, spillExpected, 400 << 10);
  spill.flush();
  spill.freeze();
  ASSERT_GE(spill.bytesWritten(), 300 << 10);
  ASSERT_LT(static_cast<size_t>(spill.bytesWritten()), spillExpected.size());

  const std::string dataPath = dir_ + "/data_fast_copy";
  std::string expected;
  ShuffleWriterMetrics metrics;
  {
    DataFileWriter writer(dataPath, 3);
    // Partition 0: write 600 KiB in small pieces so the first 512 KiB flushes
    // to fd_, then patch an early flushed offset, a straddling offset, and an
    // in-buffer tail offset.
    writer.beginPartition(0);
    std::string p0(600 << 10, '\0');
    for (auto& ch : p0) {
      ch = static_cast<char>(rng_());
    }
    for (size_t off = 0; off < p0.size(); off += 12 << 10) {
      const size_t piece = std::min<size_t>(12 << 10, p0.size() - off);
      writer.write(p0.data() + off, piece);
    }
    const std::string earlyPatch = "EARLY_HDR";
    const std::string straddlePatch = "STRADDLE_BOUNDARY";
    const std::string tailPatch = "INBUF_HDR";
    const uint64_t straddlePos = (512 << 10) - 6;
    const uint64_t tailPos = (580 << 10);
    writer.patch(16, earlyPatch.data(), earlyPatch.size());
    p0.replace(16, earlyPatch.size(), earlyPatch);
    writer.patch(straddlePos, straddlePatch.data(), straddlePatch.size());
    p0.replace(straddlePos, straddlePatch.size(), straddlePatch);
    writer.patch(tailPos, tailPatch.data(), tailPatch.size());
    p0.replace(tailPos, tailPatch.size(), tailPatch);
    expected += p0;

    // Partition 1: small on-disk slice (< 256 KiB -> direct-to-buffer pread) +
    // straddling disk/memory tail.
    writer.beginPartition(1);
    const uint64_t smallSlice = 37 << 10;
    writer.copyFrom(spill, 0, smallSlice);
    expected += spillExpected.substr(0, smallSlice);
    const uint64_t straddleBegin =
        static_cast<uint64_t>(spill.bytesWritten()) - (10 << 10);
    const uint64_t straddleLen = spillExpected.size() - straddleBegin;
    writer.copyFrom(spill, straddleBegin, straddleLen);
    expected += spillExpected.substr(straddleBegin, straddleLen);

    // Partition 2: large on-disk + memory slice (>= 256 KiB -> copy_file_range
    // for the disk prefix, direct write for the in-memory suffix).
    writer.beginPartition(2);
    writer.copyFrom(spill, 0, spillExpected.size());
    expected += spillExpected;

    writer.close(metrics);
  }

  ASSERT_EQ(metrics.partitionLengths.size(), 3u);
  EXPECT_EQ(metrics.partitionLengths[0], 600 << 10);
  EXPECT_EQ(
      metrics.partitionLengths[1],
      static_cast<int64_t>(
          (37 << 10) + spillExpected.size() -
          (static_cast<size_t>(spill.bytesWritten()) - (10 << 10))));
  EXPECT_EQ(
      metrics.partitionLengths[2], static_cast<int64_t>(spillExpected.size()));
  EXPECT_EQ(metrics.totalBytesWritten, static_cast<int64_t>(expected.size()));

  std::ifstream in(dataPath, std::ios::binary);
  ASSERT_TRUE(in.good());
  std::string actual(
      (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  EXPECT_EQ(actual, expected);
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql
