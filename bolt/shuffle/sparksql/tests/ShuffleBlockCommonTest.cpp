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

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <folly/lang/Bits.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/base/CompositeShuffleReader.h"
#include "bolt/shuffle/sparksql/base/ShuffleBlockFormat.h"
#include "bolt/shuffle/sparksql/base/ShuffleBlockReader.h"
#include "bolt/shuffle/sparksql/base/ShuffleByteSource.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "bolt/shuffle/sparksql/tests/MemoryReaderStreamIterator.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql {
namespace {

std::vector<char> toCharVec(const BufferPtr& buf) {
  return std::vector<char>(buf->as<char>(), buf->as<char>() + buf->size());
}

/// Serializes `rows` as length-prefixed [int32_t size][bytes] into `storage`
/// and returns pointers to each framed row plus total framed bytes.
uint64_t packFramedRows(
    const std::vector<std::string>& rows,
    std::string& storage,
    std::vector<const uint8_t*>& ptrs) {
  storage.clear();
  ptrs.clear();
  std::vector<size_t> offsets;
  offsets.reserve(rows.size());
  for (const auto& row : rows) {
    offsets.push_back(storage.size());
    const auto len = static_cast<int32_t>(row.size());
    storage.append(reinterpret_cast<const char*>(&len), sizeof(len));
    storage.append(row);
  }
  ptrs.reserve(rows.size());
  for (size_t off : offsets) {
    ptrs.push_back(reinterpret_cast<const uint8_t*>(storage.data() + off));
  }
  return storage.size();
}

class ShuffleBlockCommonTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(ShuffleBlockCommonTest, rowBlockRoundTripUncompressedAndCompressed) {
  auto lz4 = createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});

  const std::vector<std::string> smallRows = {"a", "", "hello"};
  std::vector<std::string> largeRows;
  for (int i = 0; i < 64; ++i) {
    largeRows.push_back(std::string(32, static_cast<char>('A' + (i % 4))));
  }

  for (const bool useCodec : {false, true}) {
    for (const auto& inputRows : {smallRows, largeRows}) {
      std::string storage;
      std::vector<const uint8_t*> ptrs;
      const uint64_t rawBytes = packFramedRows(inputRows, storage, ptrs);

      BufferPtr block;
      BufferPtr compressed;
      uint64_t compressNs = 0;
      buildRowBlock(
          ShuffleBlockKind::kCompositeRows,
          ptrs.data(),
          ptrs.size(),
          rawBytes,
          useCodec ? lz4.get() : nullptr,
          block,
          compressed,
          pool_.get(),
          compressNs);

      ASSERT_GT(block->size(), kRowBlockHeaderBytes);
      EXPECT_EQ(
          static_cast<uint8_t>(block->as<char>()[0]),
          static_cast<uint8_t>(ShuffleBlockKind::kCompositeRows));

      MemoryByteSource src({block->as<uint8_t>() + 1, block->size() - 1});
      CodecDecompressor decompressor(lz4.get());
      RowBlockReader reader(useCodec ? &decompressor : nullptr, pool_.get());
      RowBlock decoded;
      reader.read(src, decoded, ShuffleBlockKind::kCompositeRows);

      EXPECT_TRUE(src.atEnd());
      ASSERT_EQ(decoded.buffers.size(), 1);
      ASSERT_EQ(decoded.rows.size(), inputRows.size());
      uint64_t expectedBytes = 0;
      for (size_t i = 0; i < inputRows.size(); ++i) {
        EXPECT_EQ(decoded.rows[i], inputRows[i]);
        expectedBytes += inputRows[i].size();
      }
      EXPECT_EQ(decoded.rowBytes, expectedBytes);
    }
  }
}

TEST_F(ShuffleBlockCommonTest, rowBlockAppendCombinesBlocksAcrossStreams) {
  auto lz4 = createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});

  const std::vector<std::string> firstRows(32, std::string(24, 'x'));
  const std::vector<std::string> secondRows(32, std::string(24, 'y'));

  auto makeBlock = [&](const std::vector<std::string>& rows) {
    std::string storage;
    std::vector<const uint8_t*> ptrs;
    const uint64_t rawBytes = packFramedRows(rows, storage, ptrs);
    BufferPtr block;
    BufferPtr compressed;
    uint64_t compressNs = 0;
    buildRowBlock(
        ShuffleBlockKind::kRows,
        ptrs.data(),
        ptrs.size(),
        rawBytes,
        lz4.get(),
        block,
        compressed,
        pool_.get(),
        compressNs);
    return toCharVec(block);
  };

  // Place an empty stream between two block-bearing streams to verify
  // ShuffleStreamCursor skips empty streams cleanly and RowBlockReader::append
  // combines small blocks across streams into one RowBlock.
  auto streams = std::make_shared<test::MemoryReaderStreamIterator>(
      std::vector<std::vector<char>>{
          {}, makeBlock(firstRows), {}, makeBlock(secondRows), {}});

  CodecDecompressor decompressor(lz4.get());
  ShuffleStreamCursor cursor(
      streams, arrow::default_memory_pool(), &decompressor);
  RowBlockReader reader(&decompressor, pool_.get());

  RowBlock combined;
  uint8_t kind = 0;
  while (true) {
    auto timer = cursor.timeRead();
    if (!cursor.nextBlockKind(kind)) {
      break;
    }
    ASSERT_EQ(kind, static_cast<uint8_t>(ShuffleBlockKind::kRows));
    reader.append(cursor.source(), combined, ShuffleBlockKind::kRows);
  }

  EXPECT_EQ(cursor.streamsOpened(), 5);
  ASSERT_EQ(combined.buffers.size(), 2);
  ASSERT_EQ(combined.rows.size(), firstRows.size() + secondRows.size());
  EXPECT_EQ(combined.rowBytes, 64 * 24);
  for (size_t i = 0; i < firstRows.size(); ++i) {
    EXPECT_EQ(combined.rows[i], firstRows[i]);
  }
  for (size_t i = 0; i < secondRows.size(); ++i) {
    EXPECT_EQ(combined.rows[firstRows.size() + i], secondRows[i]);
  }

  // Appending a malformed block leaves previously accumulated rows intact.
  const std::string truncated(4, '\0');
  MemoryByteSource badSrc(
      {reinterpret_cast<const uint8_t*>(truncated.data()), truncated.size()});
  BOLT_ASSERT_THROW(
      reader.append(badSrc, combined, ShuffleBlockKind::kRows),
      "malformed row shuffle block: truncated row block header");
  EXPECT_EQ(combined.buffers.size(), 2);
  EXPECT_EQ(combined.rows.size(), firstRows.size() + secondRows.size());
  EXPECT_EQ(combined.rowBytes, 64 * 24);
}

TEST_F(ShuffleBlockCommonTest, rowBlockReaderRejectsMalformedInput) {
  RowBlockReader reader(/*decompressor=*/nullptr, pool_.get());
  RowBlock out;

  // Truncated header.
  {
    const std::string bytes(4, '\0');
    MemoryByteSource src(
        {reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()});
    BOLT_ASSERT_THROW(
        reader.read(src, out, ShuffleBlockKind::kRows),
        "malformed row shuffle block: truncated row block header");
  }

  // Zero rowCount.
  {
    std::string header(kRowBlockHeaderBytes - 1, '\0');
    header[20] = static_cast<char>(RunLayout::kCombinedUncompressed);
    MemoryByteSource src(
        {reinterpret_cast<const uint8_t*>(header.data()), header.size()});
    BOLT_ASSERT_THROW(
        reader.read(src, out, ShuffleBlockKind::kRows),
        "malformed row shuffle block: row block row count out of range");
  }

  // Compressed layout without decompressor.
  {
    std::string bytes(kRowBlockHeaderBytes - 1 + 4, '\0');
    folly::storeUnaligned<uint32_t>(bytes.data(), 1);
    folly::storeUnaligned<uint64_t>(bytes.data() + 4, 4);
    folly::storeUnaligned<uint64_t>(bytes.data() + 12, 4);
    bytes[20] = static_cast<char>(RunLayout::kCombined);
    MemoryByteSource src(
        {reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()});
    BOLT_ASSERT_THROW(
        reader.read(src, out, ShuffleBlockKind::kRows),
        "malformed row shuffle block: unsupported row block layout");
  }

  // Stored size mismatch for uncompressed block.
  {
    std::string bytes(kRowBlockHeaderBytes - 1 + 8, '\0');
    folly::storeUnaligned<uint32_t>(bytes.data(), 1);
    folly::storeUnaligned<uint64_t>(bytes.data() + 4, 4);
    folly::storeUnaligned<uint64_t>(bytes.data() + 12, 5);
    bytes[20] = static_cast<char>(RunLayout::kCombinedUncompressed);
    MemoryByteSource src(
        {reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()});
    BOLT_ASSERT_THROW(
        reader.read(src, out, ShuffleBlockKind::kRows),
        "malformed row shuffle block: stored row block size mismatch");
  }

  // Compressed block with stored == 0 and huge decodedBytes must fail before
  // allocating decodedBytes.
  {
    auto lz4 = createCodec(
        arrow::Compression::LZ4_FRAME,
        CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
    CodecDecompressor decompressor(lz4.get());
    RowBlockReader compressedReader(&decompressor, pool_.get());
    std::string header(kRowBlockHeaderBytes - 1, '\0');
    folly::storeUnaligned<uint32_t>(header.data(), 1);
    folly::storeUnaligned<uint64_t>(header.data() + 4, uint64_t{1} << 40);
    folly::storeUnaligned<uint64_t>(header.data() + 12, 0);
    header[20] = static_cast<char>(RunLayout::kCombined);
    MemoryByteSource src(
        {reinterpret_cast<const uint8_t*>(header.data()), header.size()});
    BOLT_ASSERT_THROW(
        compressedReader.read(src, out, ShuffleBlockKind::kRows),
        "malformed row shuffle block: row block decompression failed");
  }
}

TEST_F(ShuffleBlockCommonTest, rowBlockSharedBufferIsNotOverwrittenOnReuse) {
  auto makeBlock = [&](const std::vector<std::string>& rows) {
    std::string storage;
    std::vector<const uint8_t*> ptrs;
    const uint64_t rawBytes = packFramedRows(rows, storage, ptrs);
    BufferPtr block;
    BufferPtr compressed;
    uint64_t compressNs = 0;
    buildRowBlock(
        ShuffleBlockKind::kCompositeRows,
        ptrs.data(),
        ptrs.size(),
        rawBytes,
        nullptr,
        block,
        compressed,
        pool_.get(),
        compressNs);
    return block;
  };

  const std::vector<std::string> firstRows = {"alpha-111", "beta-222"};
  const std::vector<std::string> secondRows = {"gamma", "delta"};
  auto block1 = makeBlock(firstRows);
  auto block2 = makeBlock(secondRows);

  RowBlockReader reader(/*decompressor=*/nullptr, pool_.get());
  RowBlock reusable;

  MemoryByteSource src1({block1->as<uint8_t>() + 1, block1->size() - 1});
  reader.read(src1, reusable, ShuffleBlockKind::kRows);
  ASSERT_EQ(reusable.buffers.size(), 1);
  // Retain a copy of the BufferPtr (making it non-unique) and its string_views.
  BufferPtr retainedBuffer = reusable.buffers[0];
  std::vector<std::string_view> savedViews = reusable.rows;

  // Reading a smaller or equal block into the same RowBlock must allocate a
  // fresh buffer instead of overwriting the shared buffer in place.
  MemoryByteSource src2({block2->as<uint8_t>() + 1, block2->size() - 1});
  reader.read(src2, reusable, ShuffleBlockKind::kRows);

  ASSERT_EQ(reusable.buffers.size(), 1);
  EXPECT_NE(retainedBuffer.get(), reusable.buffers[0].get());
  ASSERT_EQ(savedViews.size(), firstRows.size());
  for (size_t i = 0; i < firstRows.size(); ++i) {
    EXPECT_EQ(savedViews[i], firstRows[i]);
  }
  ASSERT_EQ(reusable.rows.size(), secondRows.size());
  for (size_t i = 0; i < secondRows.size(); ++i) {
    EXPECT_EQ(reusable.rows[i], secondRows[i]);
  }
}

TEST_F(ShuffleBlockCommonTest, buildRowBlockValidatesRowCountAndByteLength) {
  BufferPtr block;
  BufferPtr compressed;
  uint64_t compressNs = 0;

  BOLT_ASSERT_THROW(
      buildRowBlock(
          ShuffleBlockKind::kCompositeRows,
          nullptr,
          0,
          0,
          nullptr,
          block,
          compressed,
          pool_.get(),
          compressNs),
      "row block must contain at least one row");

  std::string storage;
  std::vector<const uint8_t*> ptrs;
  const uint64_t rawBytes = packFramedRows({"hello"}, storage, ptrs);

  BOLT_ASSERT_THROW(
      buildRowBlock(
          ShuffleBlockKind::kCompositeRows,
          ptrs.data(),
          ptrs.size(),
          rawBytes - 1,
          nullptr,
          block,
          compressed,
          pool_.get(),
          compressNs),
      "row block size overflow");

  BOLT_ASSERT_THROW(
      buildRowBlock(
          ShuffleBlockKind::kCompositeRows,
          ptrs.data(),
          ptrs.size(),
          rawBytes + 1,
          nullptr,
          block,
          compressed,
          pool_.get(),
          compressNs),
      "row block size mismatch");
}

TEST_F(ShuffleBlockCommonTest, streamByteSourcePartialReadsAndEof) {
  class ChunkedInputStream final : public arrow::io::InputStream {
   public:
    ChunkedInputStream(std::string data, size_t maxChunk)
        : data_(std::move(data)), maxChunk_(maxChunk) {}

    arrow::Status Close() override {
      closed_ = true;
      return arrow::Status::OK();
    }
    bool closed() const override {
      return closed_;
    }
    arrow::Result<int64_t> Tell() const override {
      return static_cast<int64_t>(pos_);
    }
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
      const size_t avail = data_.size() - pos_;
      const size_t take =
          std::min({static_cast<size_t>(nbytes), avail, maxChunk_});
      if (take > 0) {
        std::copy_n(data_.data() + pos_, take, static_cast<char*>(out));
        pos_ += take;
      }
      return static_cast<int64_t>(take);
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t) override {
      return arrow::Status::NotImplemented("unused");
    }

   private:
    std::string data_;
    size_t maxChunk_;
    size_t pos_{0};
    bool closed_{false};
  };

  StreamByteSource src(std::make_shared<ChunkedInputStream>("abcdef", 2));
  EXPECT_FALSE(src.atEnd());
  // Zero-byte read leaves peeked byte intact.
  EXPECT_TRUE(src.read(nullptr, 0));
  EXPECT_FALSE(src.atEnd());

  char buf[4] = {};
  EXPECT_TRUE(src.read(buf, 4));
  EXPECT_EQ(std::string_view(buf, 4), "abcd");

  EXPECT_FALSE(src.atEnd());
  // Requesting more than remaining returns false and latches EOF.
  EXPECT_FALSE(src.read(buf, 3));
  EXPECT_TRUE(src.atEnd());
  EXPECT_FALSE(src.read(buf, 1));
  EXPECT_TRUE(src.read(nullptr, 0));
}

TEST_F(
    ShuffleBlockCommonTest,
    compositeShuffleReaderInterleavesCompositeAndRows) {
  auto outputType = ROW({"v"}, {VARCHAR()});

  // Minimal non-Cell reader exercising CompositeShuffleReader + append().
  class TestRowBlockShuffleReader final : public CompositeShuffleReader {
   public:
    TestRowBlockShuffleReader(
        std::shared_ptr<ReaderStreamIterator> streams,
        const RowTypePtr& outputType,
        memory::MemoryPool* pool,
        int32_t batchSize)
        : CompositeShuffleReader(
              std::move(streams),
              outputType,
              /*codec=*/nullptr,
              arrow::default_memory_pool(),
              pool,
              batchSize,
              /*batchByteSize=*/0),
          outputType_(outputType) {}

    std::string_view name() const override {
      return "TestRowBlockShuffleReader";
    }

   private:
    RowVectorPtr nextNonComposite() override {
      RowBlock batch;
      const uint64_t start = currentTimeNs();
      while (static_cast<int32_t>(batch.size()) < batchSize()) {
        if (!readNextBlock([&](uint8_t kind, ShuffleByteSource& src) {
              BOLT_CHECK_EQ(
                  kind, static_cast<uint8_t>(ShuffleBlockKind::kRows));
              rowBlockReader().append(src, batch, ShuffleBlockKind::kRows);
              incrementPayloadsRead();
            })) {
          break;
        }
      }
      if (batch.empty()) {
        addDecodeTimeNs(currentTimeNs() - start);
        return nullptr;
      }
      auto col = BaseVector::create<FlatVector<StringView>>(
          VARCHAR(), batch.size(), pool());
      for (size_t i = 0; i < batch.size(); ++i) {
        col->set(i, StringView(batch.rows[i]));
      }
      auto rv = std::make_shared<RowVector>(
          pool(),
          outputType_,
          nullptr,
          batch.size(),
          std::vector<VectorPtr>{col});
      addDecodeTimeNs(currentTimeNs() - start);
      return rv;
    }

    RowTypePtr outputType_;
  };

  auto makeBlock = [&](ShuffleBlockKind kind,
                       const std::vector<std::string>& rows) {
    std::string storage;
    std::vector<const uint8_t*> ptrs;
    const uint64_t rawBytes = packFramedRows(rows, storage, ptrs);
    BufferPtr block;
    BufferPtr compressed;
    uint64_t compressNs = 0;
    buildRowBlock(
        kind,
        ptrs.data(),
        ptrs.size(),
        rawBytes,
        nullptr,
        block,
        compressed,
        pool_.get(),
        compressNs);
    return toCharVec(block);
  };

  // Stream 0: kRows ("r1", "r2")
  // Stream 1: kRows ("r3")
  // Stream 2: kCompositeRows ("c1", "c2", "c3")
  // Stream 3: kRows ("r4")
  auto streams = std::make_shared<test::MemoryReaderStreamIterator>(
      std::vector<std::vector<char>>{
          makeBlock(ShuffleBlockKind::kRows, {"r1", "r2"}),
          makeBlock(ShuffleBlockKind::kRows, {"r3"}),
          makeBlock(ShuffleBlockKind::kCompositeRows, {"c1", "c2", "c3"}),
          makeBlock(ShuffleBlockKind::kRows, {"r4"})});

  TestRowBlockShuffleReader reader(streams, outputType, pool_.get(), 2);

  // Batch 1: stream 0's 2 rows fill batchSize=2.
  auto b1 = reader.next();
  ASSERT_NE(b1, nullptr);
  EXPECT_EQ(dynamic_cast<CompositeRowVector*>(b1.get()), nullptr);
  ASSERT_EQ(b1->size(), 2);
  EXPECT_EQ(b1->childAt(0)->asFlatVector<StringView>()->valueAt(0), "r1");
  EXPECT_EQ(b1->childAt(0)->asFlatVector<StringView>()->valueAt(1), "r2");

  // Batch 2: stream 1's 1 row, then hits kCompositeRows in stream 2 and
  // flushes.
  auto b2 = reader.next();
  ASSERT_NE(b2, nullptr);
  EXPECT_EQ(dynamic_cast<CompositeRowVector*>(b2.get()), nullptr);
  ASSERT_EQ(b2->size(), 1);
  EXPECT_EQ(b2->childAt(0)->asFlatVector<StringView>()->valueAt(0), "r3");

  // Batches 3 & 4: CompositeRows sliced into batchSize=2 (2 rows, then 1 row).
  auto b3 = reader.next();
  ASSERT_NE(b3, nullptr);
  EXPECT_NE(dynamic_cast<CompositeRowVector*>(b3.get()), nullptr);
  EXPECT_EQ(b3->size(), 2);

  auto b4 = reader.next();
  ASSERT_NE(b4, nullptr);
  EXPECT_NE(dynamic_cast<CompositeRowVector*>(b4.get()), nullptr);
  EXPECT_EQ(b4->size(), 1);

  // Batch 5: trailing kRows in stream 3.
  auto b5 = reader.next();
  ASSERT_NE(b5, nullptr);
  EXPECT_EQ(dynamic_cast<CompositeRowVector*>(b5.get()), nullptr);
  ASSERT_EQ(b5->size(), 1);
  EXPECT_EQ(b5->childAt(0)->asFlatVector<StringView>()->valueAt(0), "r4");

  EXPECT_EQ(reader.next(), nullptr);
  EXPECT_EQ(reader.name(), "TestRowBlockShuffleReader");
  EXPECT_EQ(reader.rowsRead(), 7);
  EXPECT_EQ(reader.payloadsRead(), 3);
  EXPECT_EQ(reader.streamsRead(), 4);
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql
