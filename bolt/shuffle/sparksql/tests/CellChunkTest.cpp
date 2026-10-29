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

#include <algorithm>
#include <cstring>
#include <random>
#include <vector>

#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/cell/CellBuffer.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

constexpr uint32_t kChunkBytes = 16 << 10; // 16KB
constexpr uint32_t kCellBytes = 256;
constexpr uint32_t kCellsPerChunk = kChunkBytes / kCellBytes;

/// Bytes of a mixed column's cell array: a 32-byte cell and an 8-byte tail
/// per partition, and room to align the cells.
constexpr int64_t cellArrayBytes(uint32_t numPartitions) {
  return 32 + 40 * int64_t{numPartitions};
}

class CellChunkTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
  }

  /// Turns column col mixed before any row, as a batch holding both nulls
  /// and non-nulls does: setNull() then records the column's nulls.
  static void
  markMixed(NullCells& nulls, uint32_t numPartitions, uint32_t col) {
    const std::vector<uint32_t> noRows(numPartitions, 0);
    ASSERT_TRUE(nulls.observeBatch(col, true, false, noRows.data()));
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(CellChunkTest, appendCoversNewCellIdsBeforeLinking) {
  // The chain links (next_) must cover every held cell id before an append
  // links any of them: growing them while linking would allocate - and may
  // reclaim, spilling the chain - with part of the bytes copied. They grow
  // geometrically, a few times rather than once per chunk.
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  DataCells cells(pool_.get(), &alloc, 2, 1);
  std::vector<char> bytes(kCellBytes + 44, 'x');
  uint32_t growths = 0;
  uint32_t lastCapacity = cells.nextCapacity();
  for (int i = 0; i < 64 * static_cast<int>(kCellsPerChunk); ++i) {
    cells.append(i % 2, 0, bytes.data(), static_cast<uint32_t>(bytes.size()));
    ASSERT_GE(cells.nextCapacity(), alloc.cellIdCapacity());
    if (cells.nextCapacity() != lastCapacity) {
      ++growths;
      lastCapacity = cells.nextCapacity();
    }
  }
  // About 80 chunks' worth of ids; one growth per chunk would be ~80.
  EXPECT_GE(alloc.cellIdCapacity(), 64 * kCellsPerChunk);
  EXPECT_LE(growths, 10);
}

TEST_F(CellChunkTest, failedAppendLeavesTheChainUntouched) {
  // The chain links grow in an append's allocation phase, before a byte is
  // copied. When that allocation fails, the append changes nothing: no
  // bytes counted or copied, no cell linked or kept.
  auto root = memory::memoryManager()->addRootPool("cellLinks", 1 << 20);
  auto links = root->addLeafChild("links");
  ChunkAllocator alloc(pool_.get(), kChunkBytes, 64);
  DataCells cells(links.get(), &alloc, 2, 1);
  const std::vector<char> bytes(100, 'x');
  bool failed = false;
  for (int i = 0; i < (1 << 20) && !failed; ++i) {
    const uint32_t pid = i % 2;
    const uint64_t total = cells.totalBytes();
    const uint64_t chainBytes = cells.bytes(pid, 0);
    const int64_t used = alloc.usedBytes();
    try {
      cells.append(pid, 0, bytes.data(), static_cast<uint32_t>(bytes.size()));
    } catch (const std::exception&) {
      failed = true;
      EXPECT_EQ(cells.totalBytes(), total);
      EXPECT_EQ(cells.bytes(pid, 0), chainBytes);
      EXPECT_EQ(alloc.usedBytes(), used);
    }
  }
  EXPECT_TRUE(failed);
}

TEST_F(CellChunkTest, bumpAllocationAndAddressing) {
  int growCalls = 0;
  ChunkAllocator alloc(
      pool_.get(), kChunkBytes, kCellBytes, [&]() { ++growCalls; });
  EXPECT_EQ(alloc.allocatedBytes(), 0);
  EXPECT_EQ(alloc.usedCells(), 0);
  EXPECT_EQ(growCalls, 0);

  std::vector<uint32_t> ids;
  for (uint32_t i = 0; i < kCellsPerChunk; ++i) {
    ids.push_back(alloc.allocCell());
  }
  EXPECT_EQ(growCalls, 1); // only the very first cell needed a chunk
  EXPECT_EQ(alloc.allocatedBytes(), kChunkBytes);
  EXPECT_EQ(alloc.usedCells(), kCellsPerChunk);

  // Addresses are distinct, cell-aligned, and O(1)-recomputable.
  char* base = alloc.cellData(ids[0]);
  for (uint32_t i = 0; i < ids.size(); ++i) {
    char* data = alloc.cellData(ids[i]);
    EXPECT_EQ((data - base) % kCellBytes, 0);
    ::memset(data, static_cast<int>(i & 0xFF), kCellBytes);
  }
  for (uint32_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(
        static_cast<uint8_t>(*alloc.cellData(ids[i])),
        static_cast<uint8_t>(i & 0xFF));
  }

  // Next cell spills into a second chunk, announced via the callback.
  alloc.allocCell();
  EXPECT_EQ(growCalls, 2);
  EXPECT_EQ(alloc.allocatedBytes(), 2 * kChunkBytes);
}

TEST_F(CellChunkTest, recycleIsReusedBeforeGrowth) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  std::vector<uint32_t> ids;
  for (uint32_t i = 0; i < kCellsPerChunk; ++i) {
    ids.push_back(alloc.allocCell());
  }
  alloc.recycle(ids[3]);
  alloc.recycle(ids[7]);
  EXPECT_EQ(alloc.usedCells(), kCellsPerChunk - 2);

  // LIFO reuse, no new chunk.
  EXPECT_EQ(alloc.allocCell(), ids[7]);
  EXPECT_EQ(alloc.allocCell(), ids[3]);
  EXPECT_EQ(alloc.allocatedBytes(), kChunkBytes);
  EXPECT_EQ(alloc.usedCells(), kCellsPerChunk);
}

TEST_F(CellChunkTest, growCallbackMayRecycleInsteadOfGrowing) {
  std::vector<uint32_t> ids;
  bool recycleOnGrow = false;
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes, [&]() {
    if (recycleOnGrow) {
      alloc.recycle(ids[5]);
    }
  });
  for (uint32_t i = 0; i < kCellsPerChunk; ++i) {
    ids.push_back(alloc.allocCell());
  }
  // The callback simulates a spill: it frees a cell, so the allocator must
  // serve from the freelist and not grow.
  recycleOnGrow = true;
  EXPECT_EQ(alloc.allocCell(), ids[5]);
  EXPECT_EQ(alloc.allocatedBytes(), kChunkBytes);
}

TEST_F(CellChunkTest, resetAllRetainsChunksAndReusesThem) {
  int growCalls = 0;
  ChunkAllocator alloc(
      pool_.get(), kChunkBytes, kCellBytes, [&]() { ++growCalls; });
  for (uint32_t i = 0; i < kCellsPerChunk + 5; ++i) {
    alloc.allocCell();
  }
  EXPECT_EQ(alloc.allocatedBytes(), 2 * kChunkBytes);
  const auto capacityBefore = alloc.cellIdCapacity();

  alloc.resetAll();
  EXPECT_EQ(alloc.usedCells(), 0);
  EXPECT_EQ(alloc.allocatedBytes(), 2 * kChunkBytes); // retained

  growCalls = 0;
  for (uint32_t i = 0; i < 2 * kCellsPerChunk; ++i) {
    alloc.allocCell();
  }
  EXPECT_EQ(growCalls, 0); // fully served by retained chunks
  EXPECT_EQ(alloc.cellIdCapacity(), capacityBefore);
}

TEST_F(CellChunkTest, shrinkReleasesIdleChunksAndPurgesFreelist) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  std::vector<uint32_t> first;
  for (uint32_t i = 0; i < kCellsPerChunk; ++i) {
    first.push_back(alloc.allocCell());
  }
  const uint32_t second = alloc.allocCell();
  EXPECT_EQ(alloc.allocatedBytes(), 2 * kChunkBytes);

  // Chunk 0 becomes fully idle; its freelist entries must not survive shrink.
  for (const auto id : first) {
    alloc.recycle(id);
  }
  EXPECT_EQ(alloc.shrink(), kChunkBytes);
  EXPECT_EQ(alloc.allocatedBytes(), kChunkBytes);
  EXPECT_EQ(alloc.usedCells(), 1);

  // The live cell in chunk 1 is untouched.
  *alloc.cellData(second) = 42;
  EXPECT_EQ(*alloc.cellData(second), 42);

  // New growth reuses the hole slot; ids stay within capacity.
  for (uint32_t i = 0; i < 2 * kCellsPerChunk; ++i) {
    const auto id = alloc.allocCell();
    ASSERT_LT(id, alloc.cellIdCapacity());
    *alloc.cellData(id) = 7;
  }
  EXPECT_EQ(*alloc.cellData(second), 42);

  // After a reset, shrink returns everything.
  alloc.resetAll();
  const auto retained = alloc.allocatedBytes();
  EXPECT_EQ(alloc.shrink(), retained);
  EXPECT_EQ(alloc.allocatedBytes(), 0);
}

TEST_F(CellChunkTest, dataCellsAppendScanRoundtrip) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  constexpr uint32_t kPartitions = 8;
  constexpr uint32_t kStreams = 3;
  DataCells cells(pool_.get(), &alloc, kPartitions, kStreams);

  std::mt19937 rng(42);
  std::vector<std::vector<std::string>> expected(kPartitions * kStreams);
  // Interleave appends across (pid, stream) with sizes crossing cell
  // boundaries.
  for (int round = 0; round < 50; ++round) {
    const uint32_t pid = rng() % kPartitions;
    const uint32_t stream = rng() % kStreams;
    const uint32_t bytes = 1 + rng() % (3 * kCellBytes);
    std::string blob(bytes, '\0');
    for (auto& c : blob) {
      c = static_cast<char>(rng());
    }
    cells.append(pid, stream, blob.data(), bytes);
    expected[stream * kPartitions + pid].push_back(std::move(blob));
  }

  uint64_t expectedTotal = 0;
  for (uint32_t stream = 0; stream < kStreams; ++stream) {
    for (uint32_t pid = 0; pid < kPartitions; ++pid) {
      std::string want;
      for (const auto& blob : expected[stream * kPartitions + pid]) {
        want += blob;
      }
      expectedTotal += want.size();
      EXPECT_EQ(cells.bytes(pid, stream), want.size());
      std::string got;
      cells.scan(pid, stream, [&](const char* data, uint32_t len) {
        got.append(data, len);
      });
      EXPECT_EQ(got, want) << "pid " << pid << " stream " << stream;
    }
  }
  EXPECT_EQ(cells.totalBytes(), expectedTotal);
  EXPECT_EQ(alloc.usedBytes() >= static_cast<int64_t>(expectedTotal), true);

  cells.reset();
  alloc.resetAll();
  EXPECT_EQ(cells.totalBytes(), 0);
  EXPECT_EQ(cells.bytes(1, 1), 0);

  // Usable again after reset.
  cells.append(1, 1, "abc", 3);
  EXPECT_EQ(cells.bytes(1, 1), 3);
}

TEST_F(CellChunkTest, dataCellsReleasePartitionRecycles) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  DataCells cells(pool_.get(), &alloc, 4, 2);
  std::string blobA(1000, 'a');
  std::string blobB(1000, 'b');
  cells.append(0, 0, blobA.data(), blobA.size());
  cells.append(0, 1, blobA.data(), 500);
  cells.append(2, 0, blobB.data(), blobB.size());
  const auto usedBefore = alloc.usedCells();

  cells.releasePartition(0);
  EXPECT_EQ(cells.bytes(0, 0), 0);
  EXPECT_EQ(cells.bytes(0, 1), 0);
  EXPECT_EQ(cells.bytes(2, 0), 1000);
  EXPECT_LT(alloc.usedCells(), usedBefore);
  EXPECT_EQ(cells.totalBytes(), 1000);

  // Partition 2's data survives, byte for byte.
  std::string got;
  cells.scan(
      2, 0, [&](const char* data, uint32_t len) { got.append(data, len); });
  EXPECT_EQ(got, blobB);

  // Recycled cells feed later appends without growth.
  const auto bytesBefore = alloc.allocatedBytes();
  cells.append(3, 1, blobA.data(), blobA.size());
  EXPECT_EQ(alloc.allocatedBytes(), bytesBefore);
}

TEST_F(CellChunkTest, dataCellsCompactEmptiesChunksAfterPartialRelease) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  constexpr uint32_t kPartitions = 8;
  constexpr uint32_t kStreams = 2;
  DataCells cells(pool_.get(), &alloc, kPartitions, kStreams);

  // Round-robin appends spread every partition over every chunk.
  std::mt19937 rng(7);
  std::vector<std::string> expected(kPartitions * kStreams);
  for (int round = 0; round < 40; ++round) {
    for (uint32_t pid = 0; pid < kPartitions; ++pid) {
      for (uint32_t stream = 0; stream < kStreams; ++stream) {
        std::string blob(1 + rng() % kCellBytes, '\0');
        for (auto& c : blob) {
          c = static_cast<char>(rng());
        }
        cells.append(pid, stream, blob.data(), blob.size());
        expected[stream * kPartitions + pid] += blob;
      }
    }
  }
  const auto allocatedBefore = alloc.allocatedBytes();
  ASSERT_GE(allocatedBefore, 4 * kChunkBytes);

  // Releasing half the partitions frees cells in every chunk, no chunk.
  for (uint32_t pid = 0; pid < kPartitions; pid += 2) {
    cells.releasePartition(pid);
    expected[pid].clear();
    expected[kPartitions + pid].clear();
  }
  alloc.packFreelist();
  EXPECT_EQ(alloc.shrink(), 0);

  const uint64_t freed = cells.compact(allocatedBefore);
  EXPECT_GT(freed, 0);
  EXPECT_EQ(alloc.shrink(), static_cast<int64_t>(freed));
  EXPECT_EQ(alloc.allocatedBytes(), allocatedBefore - freed);
  EXPECT_LE(alloc.usedBytes(), alloc.allocatedBytes());

  // Moved chains read back intact and keep appending at the moved tail.
  for (uint32_t pid = 1; pid < kPartitions; pid += 2) {
    cells.append(pid, 1, "tail", 4);
    expected[kPartitions + pid] += "tail";
  }
  for (uint32_t stream = 0; stream < kStreams; ++stream) {
    for (uint32_t pid = 0; pid < kPartitions; ++pid) {
      const auto& want = expected[stream * kPartitions + pid];
      EXPECT_EQ(cells.bytes(pid, stream), want.size());
      std::string got;
      cells.scan(pid, stream, [&](const char* data, uint32_t len) {
        got.append(data, len);
      });
      EXPECT_EQ(got, want) << "pid " << pid << " stream " << stream;
    }
  }
}

TEST_F(CellChunkTest, dataCellsCompactLeavesChunksWithHeldCells) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  DataCells cells(pool_.get(), &alloc, 2, 1);
  const std::string blob(kCellBytes, 'x');
  // Alternating full cells: every chunk holds both partitions.
  for (uint32_t i = 0; i + 1 < 4 * kCellsPerChunk; ++i) {
    cells.append(i % 2, 0, blob.data(), blob.size());
  }
  // The last free slot, held unlinked as an append holds the cells it is
  // about to fill.
  const uint32_t held = alloc.allocCell();
  const uint32_t heldChunk = alloc.chunkOf(held);
  ::memset(alloc.cellData(held), 'h', kCellBytes);
  cells.releasePartition(0);

  cells.compact(4 * kChunkBytes);
  alloc.shrink();
  // The held cell's chunk stays, and so does what the append wrote there.
  EXPECT_TRUE(alloc.chunkRetained(heldChunk));
  EXPECT_EQ(
      std::string(alloc.cellData(held), kCellBytes),
      std::string(kCellBytes, 'h'));
  // Partition 1 took the odd appends: 2 * kCellsPerChunk - 1 full cells.
  std::string got;
  cells.scan(
      1, 0, [&](const char* data, uint32_t len) { got.append(data, len); });
  EXPECT_EQ(got, std::string((2 * kCellsPerChunk - 1) * kCellBytes, 'x'));
  alloc.recycle(held);
}

TEST_F(CellChunkTest, nullCellsDefaultIsNoNullAndCostsNothing) {
  NullCells nulls(pool_.get(), 16, 4);
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.tableBytes(), 0);
  const auto summary = nulls.summarize(3, 2, 1000);
  EXPECT_EQ(summary.tag, NullTag::kNoNull);
  EXPECT_EQ(summary.nonNullCount, 1000);
  const auto empty = nulls.summarize(0, 0, 0);
  EXPECT_EQ(empty.tag, NullTag::kNoNull);
  EXPECT_EQ(empty.nonNullCount, 0);
}

TEST_F(CellChunkTest, nullCellsRawBitmapAndMasking) {
  NullCells nulls(pool_.get(), 4, 2);
  markMixed(nulls, 4, 0);
  // 11 rows in pid 1, col 0: nulls at 0, 5, 10.
  nulls.setNull(1, 0, 0);
  nulls.setNull(1, 0, 5);
  nulls.setNull(1, 0, 10);
  const auto summary = nulls.summarize(1, 0, 11);
  EXPECT_EQ(summary.tag, NullTag::kRawNull);
  EXPECT_EQ(summary.nonNullCount, 8);

  uint8_t bitmap[2];
  nulls.emitBitmap(1, 0, 11, bitmap);
  // bit 1 = non-null: rows 1-4, 6-7 in byte 0 -> 0b1101_1110.
  EXPECT_EQ(bitmap[0], 0b11011110);
  // rows 8, 9 non-null, row 10 null; bits 3..7 past rowCount must be zero.
  EXPECT_EQ(bitmap[1], 0b00000011);

  // The sibling column of the same partition is untouched.
  EXPECT_EQ(nulls.summarize(1, 1, 11).tag, NullTag::kNoNull);
}

TEST_F(CellChunkTest, nullCellsAllNull) {
  NullCells nulls(pool_.get(), 2, 1);
  markMixed(nulls, 2, 0);
  for (uint32_t row = 0; row < 9; ++row) {
    nulls.setNull(0, 0, row);
  }
  const auto summary = nulls.summarize(0, 0, 9);
  EXPECT_EQ(summary.tag, NullTag::kAllNull);
  EXPECT_EQ(summary.nonNullCount, 0);
}

TEST_F(CellChunkTest, nullCellsLongGapsAndImplicitTail) {
  NullCells nulls(pool_.get(), 2, 3);
  markMixed(nulls, 2, 1);
  // A null after 5 non-null rows: a run of those and the null in the
  // bitmap, all in the partition's cell of the column's array.
  nulls.setNull(0, 1, 5);
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  // 4994 more non-null rows do not fit the bitmap: the cell moves to the
  // allocator, and the next one starts with their run.
  nulls.setNull(0, 1, 5000);
  EXPECT_EQ(nulls.allocatedBytes(), 32);

  const auto summary = nulls.summarize(0, 1, 6000);
  EXPECT_EQ(summary.tag, NullTag::kRawNull);
  // Rows after the last null are non-null; exactly two nulls exist.
  EXPECT_EQ(summary.nonNullCount, 5998);

  std::vector<uint8_t> bitmap((6000 + 7) / 8);
  nulls.emitBitmap(0, 1, 6000, bitmap.data());
  for (uint32_t row = 0; row < 6000; ++row) {
    ASSERT_EQ((bitmap[row / 8] >> (row % 8)) & 1, row != 5 && row != 5000)
        << "row " << row;
  }

  nulls.reset();
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.footprintBytes(), 0);
  EXPECT_EQ(nulls.summarize(0, 1, 6000).tag, NullTag::kNoNull);
}

TEST_F(CellChunkTest, nullCellsAllNullIsPureCounting) {
  NullCells nulls(pool_.get(), 4, 2);
  markMixed(nulls, 4, 0);
  markMixed(nulls, 4, 1);
  // A partition whose every row is null, in order: the cell's null run
  // absorbs it, and no cell moves.
  for (uint32_t row = 0; row < 500; ++row) {
    nulls.setNull(1, 0, row);
  }
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  const auto summary = nulls.summarize(1, 0, 500);
  EXPECT_EQ(summary.tag, NullTag::kAllNull);
  EXPECT_EQ(summary.nonNullCount, 0);

  // The bulk form does the same.
  nulls.setNullRun(2, 1, 0, 1000);
  nulls.setNullRun(2, 1, 1000, 500);
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.summarize(2, 1, 1500).tag, NullTag::kAllNull);
}

TEST_F(CellChunkTest, nullCellsAllocatePerColumn) {
  constexpr uint32_t kPartitions = 8;
  NullCells nulls(pool_.get(), kPartitions, 100);
  // Only a mixed column has cells, one per partition and contiguous, from
  // its first null: a batch classified as having nulls may hold none.
  markMixed(nulls, kPartitions, 42);
  markMixed(nulls, kPartitions, 41);
  EXPECT_EQ(nulls.tableBytes(), 0);
  // One sparse null: it sits in the partition's cell, nothing more.
  nulls.setNull(3, 42, 7);
  EXPECT_EQ(nulls.tableBytes(), cellArrayBytes(kPartitions));
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.summarize(3, 42, 100).tag, NullTag::kRawNull);
  EXPECT_EQ(nulls.summarize(3, 41, 100).tag, NullTag::kNoNull);
  EXPECT_EQ(nulls.summarize(2, 42, 100).tag, NullTag::kNoNull);

  // A partition null from its first row is a run.
  markMixed(nulls, kPartitions, 0);
  nulls.setNullRun(3, 0, 0, 1000);
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.summarize(3, 0, 1000).tag, NullTag::kAllNull);
  EXPECT_EQ(nulls.tableBytes(), 2 * cellArrayBytes(kPartitions));
}

TEST_F(CellChunkTest, nullCellsRecycleReleasedCells) {
  constexpr uint32_t kPartitions = 2;
  NullCells nulls(pool_.get(), kPartitions, 2);
  markMixed(nulls, kPartitions, 0);
  markMixed(nulls, kPartitions, 1);
  // Every other row null: a cell holds 201 rows, a run of one and 200 in
  // its bitmap, so 4000 rows take 20 cells, 19 of them moved.
  const auto everyOther = [&](uint32_t pid, uint32_t col) {
    for (uint32_t row = 0; row < 4000; row += 2) {
      nulls.setNull(pid, col, row);
    }
  };
  const auto checkEveryOther = [&](uint32_t pid, uint32_t col) {
    std::vector<uint8_t> bitmap(500);
    nulls.emitBitmap(pid, col, 4000, bitmap.data());
    for (uint32_t row = 0; row < 4000; ++row) {
      ASSERT_EQ((bitmap[row / 8] >> (row % 8)) & 1, row % 2) << "row " << row;
    }
    EXPECT_EQ(nulls.summarize(pid, col, 4000).nonNullCount, 2000);
  };
  everyOther(0, 0);
  EXPECT_EQ(nulls.allocatedBytes(), 19 * 32);
  checkEveryOther(0, 0);
  // Held from the pool beyond the arrays: one chunk of cells.
  EXPECT_EQ(nulls.footprintBytes() - nulls.tableBytes(), 64 << 10);

  // A released partition's cells go back to the allocator, and the next
  // partition needing cells reuses them.
  nulls.releasePartition(0);
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.summarize(0, 0, 4000).tag, NullTag::kNoNull);
  everyOther(1, 1);
  EXPECT_EQ(nulls.allocatedBytes(), 19 * 32);
  EXPECT_EQ(nulls.footprintBytes() - nulls.tableBytes(), 64 << 10);
  checkEveryOther(1, 1);

  nulls.reset();
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.tableBytes(), 0);
  EXPECT_EQ(nulls.footprintBytes(), 0);
}

TEST_F(CellChunkTest, nullCellsRunsSpanCells) {
  NullCells nulls(pool_.get(), 2, 1);
  markMixed(nulls, 2, 0);
  // A run holds at most 32767 rows: 100000 null rows take four cells.
  nulls.setNullRun(0, 0, 0, 100000);
  EXPECT_EQ(nulls.allocatedBytes(), 3 * 32);
  // As do 100000 non-null rows before the next null, which then starts the
  // last cell's bitmap.
  nulls.setNull(0, 0, 200000);
  EXPECT_EQ(nulls.allocatedBytes(), 7 * 32);
  const uint32_t rows = 200006;
  const auto summary = nulls.summarize(0, 0, rows);
  EXPECT_EQ(summary.tag, NullTag::kRawNull);
  EXPECT_EQ(summary.nonNullCount, rows - 100001);
  std::vector<uint8_t> bitmap((rows + 7) / 8);
  nulls.emitBitmap(0, 0, rows, bitmap.data());
  for (uint32_t row = 0; row < rows; ++row) {
    ASSERT_EQ(
        (bitmap[row / 8] >> (row % 8)) & 1, row >= 100000 && row != 200000)
        << "row " << row;
  }
  EXPECT_EQ(bitmap.back() >> (rows % 8), 0);

  // Null by null, the run fills and moves the same way.
  for (uint32_t row = 0; row < 70000; ++row) {
    nulls.setNull(1, 0, row);
  }
  EXPECT_EQ(nulls.allocatedBytes(), (7 + 2) * 32);
  EXPECT_EQ(nulls.summarize(1, 0, 70000).tag, NullTag::kAllNull);
}

TEST_F(CellChunkTest, nullCellsMatchAReferenceBitmap) {
  constexpr uint32_t kPartitions = 3;
  std::mt19937 rng(17);
  std::uniform_real_distribution<double> unit(0, 1);
  for (const double density : {0.001, 0.01, 0.1, 0.5, 0.9, 0.999}) {
    SCOPED_TRACE(density);
    NullCells nulls(pool_.get(), kPartitions, 1);
    // Per partition, true = null.
    std::vector<std::vector<bool>> expected(kPartitions);
    // Null throughout at first, up to past one run's capacity, then mixed:
    // each partition's rows so far become runs.
    std::vector<uint32_t> rows(kPartitions, 0);
    ASSERT_FALSE(nulls.observeBatch(0, true, true, rows.data()));
    for (uint32_t pid = 0; pid < kPartitions; ++pid) {
      rows[pid] = rng() % 50000;
      expected[pid].assign(rows[pid], true);
    }
    ASSERT_TRUE(nulls.observeBatch(0, true, false, rows.data()));
    for (int step = 0; step < 5000; ++step) {
      const uint32_t pid = rng() % kPartitions;
      auto& nullRows = expected[pid];
      const uint32_t row = nullRows.size();
      switch (rng() % 64) {
        case 0: { // An all-null batch's rows.
          const uint32_t count = 1 + rng() % 70000;
          nulls.setNullRun(pid, 0, row, count);
          nullRows.resize(row + count, true);
          break;
        }
        case 1: // Non-null rows: nothing to record.
          nullRows.resize(row + rng() % 70000, false);
          break;
        case 2: // The partition's payload closes, now and then.
          if (rng() % 8 == 0) {
            nulls.releasePartition(pid);
            nullRows.clear();
          }
          break;
        default: {
          const bool isNull = unit(rng) < density;
          if (isNull) {
            nulls.setNull(pid, 0, row);
          }
          nullRows.push_back(isNull);
          break;
        }
      }
    }
    for (uint32_t pid = 0; pid < kPartitions; ++pid) {
      const auto& nullRows = expected[pid];
      // Rows past the last recorded one, non-null.
      const uint32_t rowCount = nullRows.size() + rng() % 100;
      std::vector<uint8_t> want((rowCount + 7) / 8, 0);
      uint32_t nullCount = 0;
      for (uint32_t row = 0; row < rowCount; ++row) {
        const bool isNull = row < nullRows.size() && nullRows[row];
        nullCount += isNull;
        if (!isNull) {
          want[row / 8] |= 1 << (row % 8);
        }
      }
      const auto summary = nulls.summarize(pid, 0, rowCount);
      if (rowCount == 0 || nullCount == 0) {
        EXPECT_EQ(summary.tag, NullTag::kNoNull) << "partition " << pid;
      } else if (nullCount == rowCount) {
        EXPECT_EQ(summary.tag, NullTag::kAllNull) << "partition " << pid;
      } else {
        EXPECT_EQ(summary.tag, NullTag::kRawNull) << "partition " << pid;
        EXPECT_EQ(summary.nonNullCount, rowCount - nullCount);
      }
      std::vector<uint8_t> got(want.size());
      nulls.emitBitmap(pid, 0, rowCount, got.data());
      const auto diff = std::mismatch(want.begin(), want.end(), got.begin());
      EXPECT_TRUE(diff.first == want.end())
          << "partition " << pid << " of " << rowCount << " rows: byte "
          << (diff.first - want.begin());
    }
  }
}

TEST_F(CellChunkTest, nullCellsEmitColumnsTogether) {
  // More columns than are read in step at once, of every kind, emitted
  // together: each bitmap must hold its own column's nulls.
  constexpr uint32_t kColumns = 21;
  constexpr uint32_t kRows = 5003;
  NullCells nulls(pool_.get(), 2, kColumns);
  std::mt19937 rng(5);
  const std::vector<uint32_t> noRows(2, 0);
  std::vector<std::vector<bool>> expectNull(
      kColumns, std::vector<bool>(kRows, false));
  for (uint32_t col = 0; col < kColumns; ++col) {
    switch (col % 7) {
      case 0: // Null throughout.
        ASSERT_FALSE(nulls.observeBatch(col, true, true, noRows.data()));
        expectNull[col].assign(kRows, true);
        break;
      case 1: // Non-null throughout.
        ASSERT_FALSE(nulls.observeBatch(col, false, false, noRows.data()));
        break;
      case 2: // Mixed, but no null recorded.
        markMixed(nulls, 2, col);
        break;
      default: { // Mixed, at a density of its own.
        markMixed(nulls, 2, col);
        const uint32_t perMille[] = {5, 100, 500, 995};
        for (uint32_t row = 0; row < kRows; ++row) {
          if (rng() % 1000 < perMille[col % 7 - 3]) {
            nulls.setNull(1, col, row);
            expectNull[col][row] = true;
          }
        }
        break;
      }
    }
  }
  const uint32_t bytes = (kRows + 7) / 8;
  std::vector<uint32_t> cols(kColumns);
  std::vector<std::vector<uint8_t>> bitmaps(kColumns);
  std::vector<uint8_t*> outs(kColumns);
  for (uint32_t col = 0; col < kColumns; ++col) {
    cols[col] = col;
    bitmaps[col].assign(bytes, 0xAA);
    outs[col] = bitmaps[col].data();
  }
  nulls.emitBitmaps(1, cols.data(), kColumns, kRows, outs.data());
  for (uint32_t col = 0; col < kColumns; ++col) {
    std::vector<uint8_t> want(bytes, 0);
    for (uint32_t row = 0; row < kRows; ++row) {
      if (!expectNull[col][row]) {
        want[row / 8] |= 1 << (row % 8);
      }
    }
    EXPECT_EQ(bitmaps[col], want) << "column " << col;
  }
}

TEST_F(CellChunkTest, nullCellsPrefixThenMixed) {
  NullCells nulls(pool_.get(), 2, 1);
  markMixed(nulls, 2, 0);
  // Rows 0..9 null (a run), 10..19 non-null, 20 null again: the late null
  // and the rows before it go to the bitmap, after the run.
  for (uint32_t row = 0; row < 10; ++row) {
    nulls.setNull(0, 0, row);
  }
  nulls.setNull(0, 0, 20);
  EXPECT_EQ(nulls.allocatedBytes(), 0);

  const auto summary = nulls.summarize(0, 0, 30);
  EXPECT_EQ(summary.tag, NullTag::kRawNull);
  EXPECT_EQ(summary.nonNullCount, 30 - 11);

  std::vector<uint8_t> bitmap((30 + 7) / 8);
  nulls.emitBitmap(0, 0, 30, bitmap.data());
  for (uint32_t row = 0; row < 30; ++row) {
    const bool nonNull = (bitmap[row / 8] >> (row % 8)) & 1;
    const bool expectNull = row < 10 || row == 20;
    EXPECT_EQ(nonNull, !expectNull) << "row " << row;
  }

  // A run with no bitmap at all still synthesizes correctly.
  nulls.setNull(1, 0, 0);
  nulls.setNull(1, 0, 1);
  const auto runOnly = nulls.summarize(1, 0, 8);
  EXPECT_EQ(runOnly.tag, NullTag::kRawNull);
  EXPECT_EQ(runOnly.nonNullCount, 6);
  uint8_t byte;
  nulls.emitBitmap(1, 0, 8, &byte);
  EXPECT_EQ(byte, 0b11111100);
}

TEST_F(CellChunkTest, nullCellsUniformColumnsStoreNothing) {
  constexpr uint32_t kPartitions = 3;
  NullCells nulls(pool_.get(), kPartitions, 2);
  // Column 0 is null in every batch, column 1 in none: their states say it
  // all, and no batch records or allocates anything.
  std::vector<uint32_t> rows(kPartitions, 0);
  for (int batch = 0; batch < 3; ++batch) {
    EXPECT_FALSE(nulls.observeBatch(0, true, true, rows.data()));
    EXPECT_FALSE(nulls.observeBatch(1, false, false, rows.data()));
    for (auto& count : rows) {
      count += 5;
    }
  }
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.tableBytes(), 0);
  const auto allNull = nulls.summarize(2, 0, 15);
  EXPECT_EQ(allNull.tag, NullTag::kAllNull);
  EXPECT_EQ(allNull.nonNullCount, 0);
  const auto noNull = nulls.summarize(2, 1, 15);
  EXPECT_EQ(noNull.tag, NullTag::kNoNull);
  EXPECT_EQ(noNull.nonNullCount, 15);

  uint8_t bitmap[2];
  nulls.emitBitmap(1, 0, 11, bitmap);
  EXPECT_EQ(bitmap[0], 0);
  EXPECT_EQ(bitmap[1], 0);
  nulls.emitBitmap(1, 1, 11, bitmap);
  EXPECT_EQ(bitmap[0], 0xFF);
  EXPECT_EQ(bitmap[1], 0b00000111);

  // A released partition keeps the states for its rows to come; only the
  // window's close drops them.
  nulls.releasePartition(2);
  EXPECT_EQ(nulls.summarize(2, 0, 4).tag, NullTag::kAllNull);
  nulls.reset();
  EXPECT_EQ(nulls.summarize(2, 0, 4).tag, NullTag::kNoNull);
}

TEST_F(CellChunkTest, nullCellsAllNullColumnTurningMixedKeepsItsNulls) {
  constexpr uint32_t kPartitions = 3;
  NullCells nulls(pool_.get(), kPartitions, 1);
  std::vector<uint32_t> rows(kPartitions, 0);
  EXPECT_FALSE(nulls.observeBatch(0, true, true, rows.data()));
  // A batch with non-null rows after 5, 0 and 9 null ones: each
  // partition's rows so far become a null run of its own.
  rows = {5, 0, 9};
  EXPECT_TRUE(nulls.observeBatch(0, true, false, rows.data()));
  EXPECT_EQ(nulls.tableBytes(), cellArrayBytes(kPartitions));
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  // The batch: partition 0 gets rows 5 (null) and 6, partition 1 row 0,
  // partition 2 rows 9 and 10.
  nulls.setNull(0, 0, 5); // the run goes on

  const auto first = nulls.summarize(0, 0, 7);
  EXPECT_EQ(first.tag, NullTag::kRawNull);
  EXPECT_EQ(first.nonNullCount, 1);
  uint8_t byte;
  nulls.emitBitmap(0, 0, 7, &byte);
  EXPECT_EQ(byte, 0b01000000);
  EXPECT_EQ(nulls.summarize(1, 0, 1).tag, NullTag::kNoNull);
  const auto last = nulls.summarize(2, 0, 11);
  EXPECT_EQ(last.tag, NullTag::kRawNull);
  EXPECT_EQ(last.nonNullCount, 2);
  uint8_t bitmap[2];
  nulls.emitBitmap(2, 0, 11, bitmap);
  EXPECT_EQ(bitmap[0], 0);
  EXPECT_EQ(bitmap[1], 0b00000110);

  // Mixed for the rest of the window: a later all-null batch records its
  // rows per partition, here past a non-null row.
  rows = {7, 1, 11};
  EXPECT_TRUE(nulls.observeBatch(0, true, true, rows.data()));
  nulls.setNullRun(1, 0, 1, 3);
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.summarize(1, 0, 4).nonNullCount, 1);
}

TEST_F(CellChunkTest, nullCellsNoNullColumnTurningMixed) {
  constexpr uint32_t kPartitions = 2;
  NullCells nulls(pool_.get(), kPartitions, 1);
  std::vector<uint32_t> rows(kPartitions, 0);
  EXPECT_FALSE(nulls.observeBatch(0, false, false, rows.data()));
  // An all-null batch after 4 non-null rows of partition 0: those need
  // nothing, being what unrecorded rows are.
  rows = {4, 0};
  EXPECT_TRUE(nulls.observeBatch(0, true, true, rows.data()));
  EXPECT_EQ(nulls.tableBytes(), 0);
  nulls.setNullRun(0, 0, 4, 2);
  nulls.setNullRun(1, 0, 0, 3);
  EXPECT_EQ(nulls.tableBytes(), cellArrayBytes(kPartitions));
  const auto first = nulls.summarize(0, 0, 6);
  EXPECT_EQ(first.tag, NullTag::kRawNull);
  EXPECT_EQ(first.nonNullCount, 4);
  uint8_t byte;
  nulls.emitBitmap(0, 0, 6, &byte);
  EXPECT_EQ(byte, 0b00001111);
  // Partition 1's rows are all null: a run alone.
  EXPECT_EQ(nulls.summarize(1, 0, 3).tag, NullTag::kAllNull);
  EXPECT_EQ(nulls.allocatedBytes(), 0);

  // A released partition starts over within the still mixed column.
  nulls.releasePartition(0);
  EXPECT_EQ(nulls.summarize(0, 0, 6).tag, NullTag::kNoNull);
  nulls.setNull(0, 0, 0);
  EXPECT_EQ(nulls.summarize(0, 0, 2).nonNullCount, 1);
}

TEST_F(CellChunkTest, cellLayoutStreams) {
  auto rowType =
      ROW({"a", "b", "c", "d"}, {BIGINT(), VARCHAR(), REAL(), TINYINT()});
  ASSERT_TRUE(CellLayout::isSupportedRowType(rowType));
  const auto layout = CellLayout::create(rowType);
  EXPECT_EQ(layout.numColumns(), 4);
  EXPECT_EQ(layout.numStreams(), 5); // VARCHAR contributes two

  EXPECT_EQ(layout.stream(0).kind, StreamKind::kEncoded);
  EXPECT_EQ(layout.stream(0).sourceWidth, 8);
  EXPECT_EQ(layout.columnStream(1), 1);
  EXPECT_EQ(layout.stream(1).kind, StreamKind::kEncoded); // length stream
  EXPECT_EQ(layout.stream(1).sourceWidth, 8);
  EXPECT_EQ(layout.stream(2).kind, StreamKind::kStringData);
  EXPECT_EQ(layout.stream(3).kind, StreamKind::kRawFixed);
  EXPECT_EQ(layout.stream(3).sourceWidth, 4);
  EXPECT_EQ(layout.stream(4).kind, StreamKind::kRawFixed);
  EXPECT_EQ(layout.stream(4).sourceWidth, 1);
  EXPECT_TRUE(layout.isStringColumn(1));
  EXPECT_FALSE(layout.isStringColumn(0));

  EXPECT_TRUE(CellLayout::isSupportedRowType(ROW({"x"}, {BOOLEAN()})));
  EXPECT_TRUE(CellLayout::isSupportedRowType(ROW({"x"}, {TIMESTAMP()})));
  EXPECT_FALSE(CellLayout::isSupportedRowType(ROW({"x"}, {ARRAY(BIGINT())})));
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
