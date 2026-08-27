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

#include <random>

#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/cell/CellDirectory.h"
#include "bolt/shuffle/sparksql/cell/CellTypes.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

constexpr uint32_t kChunkBytes = 16 << 10; // 16KB
constexpr uint32_t kCellBytes = 256;
constexpr uint32_t kCellsPerChunk = kChunkBytes / kCellBytes;

class CellChunkTest : public testing::Test {
 protected:
  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(CellChunkTest, bumpAllocationAndAddressing) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  EXPECT_EQ(alloc.allocatedBytes(), 0);
  EXPECT_EQ(alloc.usedCells(), 0);

  int growCalls = 0;
  const auto grow = [&]() { ++growCalls; };

  std::vector<uint32_t> ids;
  for (uint32_t i = 0; i < kCellsPerChunk; ++i) {
    ids.push_back(alloc.allocCell(grow));
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
  alloc.allocCell(grow);
  EXPECT_EQ(growCalls, 2);
  EXPECT_EQ(alloc.allocatedBytes(), 2 * kChunkBytes);
}

TEST_F(CellChunkTest, recycleIsReusedBeforeGrowth) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  std::vector<uint32_t> ids;
  for (uint32_t i = 0; i < kCellsPerChunk; ++i) {
    ids.push_back(alloc.allocCell({}));
  }
  alloc.recycle(ids[3]);
  alloc.recycle(ids[7]);
  EXPECT_EQ(alloc.usedCells(), kCellsPerChunk - 2);

  // LIFO reuse, no new chunk.
  EXPECT_EQ(alloc.allocCell({}), ids[7]);
  EXPECT_EQ(alloc.allocCell({}), ids[3]);
  EXPECT_EQ(alloc.allocatedBytes(), kChunkBytes);
  EXPECT_EQ(alloc.usedCells(), kCellsPerChunk);
}

TEST_F(CellChunkTest, growCallbackMayRecycleInsteadOfGrowing) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  std::vector<uint32_t> ids;
  for (uint32_t i = 0; i < kCellsPerChunk; ++i) {
    ids.push_back(alloc.allocCell({}));
  }
  // The callback simulates a spill: it frees a cell, so the allocator must
  // serve from the freelist and not grow.
  const auto spillingGrow = [&]() { alloc.recycle(ids[5]); };
  EXPECT_EQ(alloc.allocCell(spillingGrow), ids[5]);
  EXPECT_EQ(alloc.allocatedBytes(), kChunkBytes);
}

TEST_F(CellChunkTest, resetAllRetainsChunksAndReusesThem) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  for (uint32_t i = 0; i < kCellsPerChunk + 5; ++i) {
    alloc.allocCell({});
  }
  EXPECT_EQ(alloc.allocatedBytes(), 2 * kChunkBytes);
  const auto capacityBefore = alloc.cellIdCapacity();

  alloc.resetAll();
  EXPECT_EQ(alloc.usedCells(), 0);
  EXPECT_EQ(alloc.allocatedBytes(), 2 * kChunkBytes); // retained

  int growCalls = 0;
  for (uint32_t i = 0; i < 2 * kCellsPerChunk; ++i) {
    alloc.allocCell([&]() { ++growCalls; });
  }
  EXPECT_EQ(growCalls, 0); // fully served by retained chunks
  EXPECT_EQ(alloc.cellIdCapacity(), capacityBefore);
}

TEST_F(CellChunkTest, shrinkReleasesIdleChunksAndPurgesFreelist) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  std::vector<uint32_t> first;
  for (uint32_t i = 0; i < kCellsPerChunk; ++i) {
    first.push_back(alloc.allocCell({}));
  }
  const uint32_t second = alloc.allocCell({});
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
    const auto id = alloc.allocCell({});
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
    cells.append(pid, stream, blob.data(), bytes, {});
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
  EXPECT_EQ(
      alloc.usedBytes() >= static_cast<int64_t>(expectedTotal), true);

  cells.reset();
  alloc.resetAll();
  EXPECT_EQ(cells.totalBytes(), 0);
  EXPECT_EQ(cells.bytes(1, 1), 0);

  // Usable again after reset.
  cells.append(1, 1, "abc", 3, {});
  EXPECT_EQ(cells.bytes(1, 1), 3);
}

TEST_F(CellChunkTest, dataCellsReleasePartitionRecycles) {
  ChunkAllocator alloc(pool_.get(), kChunkBytes, kCellBytes);
  DataCells cells(pool_.get(), &alloc, 4, 2);
  std::string blobA(1000, 'a');
  std::string blobB(1000, 'b');
  cells.append(0, 0, blobA.data(), blobA.size(), {});
  cells.append(0, 1, blobA.data(), 500, {});
  cells.append(2, 0, blobB.data(), blobB.size(), {});
  const auto usedBefore = alloc.usedCells();

  cells.releasePartition(0);
  EXPECT_EQ(cells.bytes(0, 0), 0);
  EXPECT_EQ(cells.bytes(0, 1), 0);
  EXPECT_EQ(cells.bytes(2, 0), 1000);
  EXPECT_LT(alloc.usedCells(), usedBefore);
  EXPECT_EQ(cells.totalBytes(), 1000);

  // Partition 2's data survives, byte for byte.
  std::string got;
  cells.scan(2, 0, [&](const char* data, uint32_t len) {
    got.append(data, len);
  });
  EXPECT_EQ(got, blobB);

  // Recycled cells feed later appends without growth.
  const auto bytesBefore = alloc.allocatedBytes();
  cells.append(3, 1, blobA.data(), blobA.size(), {});
  EXPECT_EQ(alloc.allocatedBytes(), bytesBefore);
}

TEST_F(CellChunkTest, nullCellsDefaultIsNoNullAndCostsNothing) {
  NullCells nulls(pool_.get(), 16, 4);
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  const auto summary = nulls.summarize(3, 2, 1000);
  EXPECT_EQ(summary.tag, NullTag::kNoNull);
  EXPECT_EQ(summary.nonNullCount, 1000);
  const auto empty = nulls.summarize(0, 0, 0);
  EXPECT_EQ(empty.tag, NullTag::kNoNull);
  EXPECT_EQ(empty.nonNullCount, 0);
}

TEST_F(CellChunkTest, nullCellsRawBitmapAndMasking) {
  NullCells nulls(pool_.get(), 4, 2);
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
  for (uint32_t row = 0; row < 9; ++row) {
    nulls.setNull(0, 0, row);
  }
  const auto summary = nulls.summarize(0, 0, 9);
  EXPECT_EQ(summary.tag, NullTag::kAllNull);
  EXPECT_EQ(summary.nonNullCount, 0);
}

TEST_F(CellChunkTest, nullCellsGrowthPreservesBitsAndImplicitTail) {
  NullCells nulls(pool_.get(), 2, 3);
  nulls.setNull(0, 1, 5);
  const auto small = nulls.allocatedBytes();
  EXPECT_GT(small, 0);
  nulls.setNull(0, 1, 5000); // forces growth well past the first 128 rows
  EXPECT_GT(nulls.allocatedBytes(), small);

  const auto summary = nulls.summarize(0, 1, 6000);
  EXPECT_EQ(summary.tag, NullTag::kRawNull);
  // Rows beyond capacity stay implicitly non-null; exactly two nulls exist.
  EXPECT_EQ(summary.nonNullCount, 5998);

  std::vector<uint8_t> bitmap((6000 + 7) / 8);
  nulls.emitBitmap(0, 1, 6000, bitmap.data());
  EXPECT_EQ(bitmap[0] & (1u << 5), 0u);
  EXPECT_EQ(bitmap[5000 / 8] & (1u << (5000 % 8)), 0u);
  EXPECT_NE(bitmap[3000 / 8] & (1u << (3000 % 8)), 0u);

  nulls.reset();
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.summarize(0, 1, 6000).tag, NullTag::kNoNull);
}

TEST_F(CellChunkTest, nullCellsAllNullIsPureCounting) {
  NullCells nulls(pool_.get(), 4, 2);
  // Every row of the window null, in order: the prefix counter absorbs it
  // and no bitmap storage is ever allocated.
  for (uint32_t row = 0; row < 500; ++row) {
    nulls.setNull(1, 0, row);
  }
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  const auto summary = nulls.summarize(1, 0, 500);
  EXPECT_EQ(summary.tag, NullTag::kAllNull);
  EXPECT_EQ(summary.nonNullCount, 0);

  // The bulk form does the same in O(1).
  nulls.setNullRun(2, 1, 0, 1000);
  nulls.setNullRun(2, 1, 1000, 500);
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  EXPECT_EQ(nulls.summarize(2, 1, 1500).tag, NullTag::kAllNull);
}

TEST_F(CellChunkTest, nullCellsPrefixThenMixed) {
  NullCells nulls(pool_.get(), 2, 1);
  // Rows 0..9 null (prefix), 10..19 non-null, 20 null again: the late null
  // must go to the bitmap while the prefix stays counted.
  for (uint32_t row = 0; row < 10; ++row) {
    nulls.setNull(0, 0, row);
  }
  EXPECT_EQ(nulls.allocatedBytes(), 0);
  nulls.setNull(0, 0, 20);
  EXPECT_GT(nulls.allocatedBytes(), 0);

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

  // A pure prefix with no bitmap at all still synthesizes correctly.
  nulls.setNull(1, 0, 0);
  nulls.setNull(1, 0, 1);
  const auto prefixOnly = nulls.summarize(1, 0, 8);
  EXPECT_EQ(prefixOnly.tag, NullTag::kRawNull);
  EXPECT_EQ(prefixOnly.nonNullCount, 6);
  uint8_t byte;
  nulls.emitBitmap(1, 0, 8, &byte);
  EXPECT_EQ(byte, 0b11111100);
}

TEST_F(CellChunkTest, cellLayoutStreams) {
  auto rowType = ROW(
      {"a", "b", "c", "d"}, {BIGINT(), VARCHAR(), REAL(), TINYINT()});
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

  EXPECT_FALSE(CellLayout::isSupportedRowType(ROW({"x"}, {BOOLEAN()})));
  EXPECT_FALSE(CellLayout::isSupportedRowType(ROW({"x"}, {TIMESTAMP()})));
  EXPECT_FALSE(
      CellLayout::isSupportedRowType(ROW({"x"}, {ARRAY(BIGINT())})));
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  bytedance::bolt::memory::MemoryManager::initialize({});
  return RUN_ALL_TESTS();
}
