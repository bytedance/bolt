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

#include <fstream>
#include <optional>
#include <random>

#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleWriter.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

/// Decompression seam over the engine codec, mirroring the reader node's
/// wiring.
class TestDecompressor : public cell::CellDecompressor {
 public:
  explicit TestDecompressor(Codec* codec) : codec_(codec) {}

  bool decompress(
      const uint8_t* data,
      size_t size,
      uint8_t* out,
      size_t decodedSize) override {
    return codec_->decompress(
               data,
               static_cast<int64_t>(size),
               out,
               static_cast<int64_t>(decodedSize)) ==
        static_cast<int64_t>(decodedSize);
  }

 private:
  Codec* const codec_;
};

class CellWriterTest : public testing::Test, public bolt::test::VectorTestBase {
 protected:
  void SetUp() override {
    char pathTemplate[] = "/tmp/bolt_cell_writer_test_XXXXXX";
    const int fd = ::mkstemp(pathTemplate);
    ASSERT_GE(fd, 0);
    ::close(fd);
    dataFile_ = pathTemplate;
  }

  void TearDown() override {
    ::unlink(dataFile_.c_str());
    ::unlink((dataFile_ + ".cellspill").c_str());
  }

  ShuffleWriterOptions makeOptions(int32_t numPartitions) {
    ShuffleWriterOptions options;
    options.partitioning = Partitioning::kHash;
    options.partitionWriterOptions.numPartitions = numPartitions;
    options.partitionWriterOptions.dataFile = dataFile_;
    return options;
  }

  /// Prepends an explicit pid column (values already in [0, P)).
  RowVectorPtr withPid(
      const std::vector<int32_t>& pids,
      const RowVectorPtr& data) {
    std::vector<std::string> names{"pid"};
    std::vector<TypePtr> types{INTEGER()};
    std::vector<VectorPtr> children{makeFlatVector<int32_t>(pids)};
    const auto& row = data->type()->asRow();
    for (uint32_t i = 0; i < row.size(); ++i) {
      names.push_back(row.nameOf(i));
      types.push_back(row.childAt(i));
      children.push_back(data->childAt(i));
    }
    return std::make_shared<RowVector>(
        pool(),
        ROW(std::move(names), std::move(types)),
        BufferPtr(nullptr),
        data->size(),
        std::move(children));
  }

  VectorPtr makeNullableStrings(int n, std::mt19937& rng) {
    std::vector<std::string> storage(n);
    std::vector<std::optional<StringView>> views(n);
    for (int i = 0; i < n; ++i) {
      if (rng() % 5 == 0) {
        views[i] = std::nullopt;
        continue;
      }
      storage[i].assign(rng() % 90, static_cast<char>('a' + rng() % 26));
      views[i] = StringView(storage[i]);
    }
    return makeNullableFlatVector<StringView>(views);
  }

  std::string readFile() {
    std::ifstream in(dataFile_, std::ios::binary);
    return std::string(
        std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  }

  /// Decodes every payload of one partition's byte range, in payload order.
  std::vector<RowVectorPtr> decodePartition(
      const CellLayout& layout,
      const std::string& file,
      uint64_t offset,
      uint64_t length) {
    std::vector<RowVectorPtr> payloads;
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(file.data()) + offset, length);
    // The writer compresses runs at the final merge with the default codec.
    auto codec = createCodec(
        arrow::Compression::LZ4_FRAME,
        CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
    TestDecompressor decompressor(codec.get());
    CellPayloadDecoder decoder(layout, &decompressor, pool());
    while (!source.atEnd()) {
      RowVectorPtr decoded;
      std::string error;
      EXPECT_TRUE(decoder.decode(source, decoded, error)) << error;
      if (decoded == nullptr) {
        break;
      }
      payloads.push_back(std::move(decoded));
    }
    return payloads;
  }

  /// Runs the writer over the batches and verifies, per partition, that the
  /// decoded rows equal the input rows routed to it, in order.
  void roundTrip(
      const ShuffleWriterOptions& options,
      const std::vector<std::vector<int32_t>>& pidsPerBatch,
      const std::vector<RowVectorPtr>& batches,
      CellShuffleWriter* preBuilt = nullptr,
      std::function<void(CellShuffleWriter&, uint32_t)> betweenBatches = {}) {
    const auto numPartitions = options.partitionWriterOptions.numPartitions;
    std::unique_ptr<CellShuffleWriter> owned;
    CellShuffleWriter* writer = preBuilt;
    if (writer == nullptr) {
      owned = std::make_unique<CellShuffleWriter>(
          options, pool(), arrow::default_memory_pool());
      writer = owned.get();
    }
    for (uint32_t i = 0; i < batches.size(); ++i) {
      ASSERT_TRUE(writer->split(withPid(pidsPerBatch[i], batches[i]), 0).ok());
      if (betweenBatches) {
        betweenBatches(*writer, i);
      }
    }
    ASSERT_TRUE(writer->stop().ok());

    const auto& metrics = writer->metrics();
    const auto file = readFile();
    ASSERT_EQ(metrics.partitionLengths.size(), numPartitions);
    int64_t sum = 0;
    for (const auto length : metrics.partitionLengths) {
      sum += length;
    }
    EXPECT_EQ(sum, metrics.totalBytesWritten);
    EXPECT_EQ(static_cast<int64_t>(file.size()), metrics.totalBytesWritten);

    // Expected rows per partition, in arrival order.
    const auto layout = CellLayout::create(asRowType(batches[0]->type()));
    std::vector<std::vector<std::pair<uint32_t, vector_size_t>>> expected(
        numPartitions);
    for (uint32_t i = 0; i < batches.size(); ++i) {
      for (vector_size_t row = 0; row < batches[i]->size(); ++row) {
        expected[pidsPerBatch[i][row]].emplace_back(i, row);
      }
    }
    uint64_t offset = 0;
    for (int32_t pid = 0; pid < numPartitions; ++pid) {
      const auto payloads = decodePartition(
          layout, file, offset, metrics.partitionLengths[pid]);
      size_t k = 0;
      for (const auto& decoded : payloads) {
        for (vector_size_t row = 0; row < decoded->size(); ++row, ++k) {
          ASSERT_LT(k, expected[pid].size()) << "partition " << pid;
          const auto [batchIdx, sourceRow] = expected[pid][k];
          EXPECT_TRUE(batches[batchIdx]->equalValueAt(
              decoded.get(), sourceRow, row))
              << "partition " << pid << " row " << k << ": expected "
              << batches[batchIdx]->toString(sourceRow) << ", got "
              << decoded->toString(row);
        }
      }
      EXPECT_EQ(k, expected[pid].size()) << "partition " << pid;
      offset += metrics.partitionLengths[pid];
    }
  }

  std::string dataFile_;
};

TEST_F(CellWriterTest, multiPartitionRoundTrip) {
  constexpr int32_t kPartitions = 8;
  std::mt19937 rng(7);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 5; ++batch) {
    const int n = 300 + batch * 57;
    std::vector<int32_t> batchPids(n);
    for (int i = 0; i < n; ++i) {
      batchPids[i] = rng() % kPartitions;
    }
    auto bigints = makeFlatVector<int64_t>(
        n, [&](auto row) { return row * 1'000'003 + batch; });
    std::vector<std::optional<int16_t>> shorts(n);
    for (int i = 0; i < n; ++i) {
      shorts[i] = (i % 7 == 0)
          ? std::nullopt
          : std::optional<int16_t>(static_cast<int16_t>(i % 100 - 50));
    }
    auto smallints = makeNullableFlatVector<int16_t>(shorts);
    auto reals =
        makeFlatVector<float>(n, [](auto row) { return row * 0.5f; });
    auto strings = makeNullableStrings(n, rng);
    batches.push_back(makeRowVector(
        {"a", "b", "c", "d"}, {bigints, smallints, reals, strings}));
    pids.push_back(std::move(batchPids));
  }
  roundTrip(makeOptions(kPartitions), pids, batches);
}

TEST_F(CellWriterTest, spillsAndCheckpointsStillRoundTrip) {
  constexpr int32_t kPartitions = 4;
  auto options = makeOptions(kPartitions);
  // Force frequent physical spills and window closes.
  options.cellOptions.cellMemoryCapBytes = 2 * options.cellOptions.chunkBytes;
  options.cellOptions.checkpointPartitionBytes = 16 << 10;
  options.cellOptions.nullMemLimitBytes = 4 << 10;

  std::mt19937 rng(11);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 8; ++batch) {
    const int n = 1024;
    std::vector<int32_t> batchPids(n);
    for (int i = 0; i < n; ++i) {
      batchPids[i] = rng() % kPartitions;
    }
    auto values = makeFlatVector<int64_t>(
        n, [&](auto /*row*/) { return static_cast<int64_t>(rng()); });
    auto strings = makeNullableStrings(n, rng);
    batches.push_back(makeRowVector({"v", "s"}, {values, strings}));
    pids.push_back(std::move(batchPids));
  }

  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  roundTrip(options, pids, batches, &writer);
  EXPECT_GT(writer.metrics().spillCount, 0) << "expected sealed windows";
  EXPECT_GT(writer.metrics().totalBytesEvicted, 0);
}

TEST_F(CellWriterTest, uncompressedSpillStillRoundTripsAndWritesMore) {
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(29);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 6; ++batch) {
    const int n = 2048;
    std::vector<int32_t> batchPids(n);
    for (int i = 0; i < n; ++i) {
      batchPids[i] = rng() % kPartitions;
    }
    auto values = makeFlatVector<int64_t>(
        n, [&](auto row) { return 5'000'000'000LL + row % 977; });
    std::vector<std::string> storage(n);
    std::vector<std::optional<StringView>> views(n);
    for (int i = 0; i < n; ++i) {
      storage[i].assign(256, static_cast<char>('a' + i % 4));
      views[i] = StringView(storage[i]);
    }
    auto strings = makeNullableFlatVector<StringView>(views);
    batches.push_back(makeRowVector({"v", "s"}, {values, strings}));
    pids.push_back(std::move(batchPids));
  }

  const auto runOnce = [&](bool compressSpill) {
    auto options = makeOptions(kPartitions);
    options.cellOptions.cellMemoryCapBytes =
        2 * options.cellOptions.chunkBytes; // force spills
    options.cellOptions.checkpointPartitionBytes = 64 << 10;
    options.cellOptions.compressSpill = compressSpill;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    EXPECT_GT(writer.metrics().spillCount, 0);
    return writer.metrics().totalBytesEvicted;
  };

  const auto compressedSpillBytes = runOnce(true);
  ::unlink(dataFile_.c_str());
  const auto rawSpillBytes = runOnce(false);
  // The whole point of spill compression: fewer bytes hit the disk on the
  // spill pass (the compressible strings dominate this data set).
  EXPECT_LT(compressedSpillBytes * 2, rawSpillBytes)
      << "compressed spill " << compressedSpillBytes << " vs raw "
      << rawSpillBytes;
}

TEST_F(CellWriterTest, reclaimMidStreamReleasesMemory) {
  constexpr int32_t kPartitions = 4;
  auto options = makeOptions(kPartitions);
  std::mt19937 rng(13);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  // Enough volume that the writer holds several chunks by the trigger
  // point; a tiny writer legitimately declines to reclaim (anti-churn
  // guard) and would make this test meaningless.
  for (int batch = 0; batch < 6; ++batch) {
    const int n = 8192;
    std::vector<int32_t> batchPids(n);
    for (int i = 0; i < n; ++i) {
      batchPids[i] = rng() % kPartitions;
    }
    auto values = makeFlatVector<int64_t>(
        n, [&](auto /*row*/) { return static_cast<int64_t>(rng()); });
    std::vector<std::string> storage(n);
    std::vector<std::optional<StringView>> views(n);
    for (int i = 0; i < n; ++i) {
      storage[i].assign(1024, static_cast<char>('a' + rng() % 26));
      views[i] = StringView(storage[i]);
    }
    auto strings = makeNullableFlatVector<StringView>(views);
    batches.push_back(makeRowVector({"v", "s"}, {values, strings}));
    pids.push_back(std::move(batchPids));
  }

  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  bool reclaimed = false;
  roundTrip(
      options,
      pids,
      batches,
      &writer,
      [&](CellShuffleWriter& w, uint32_t batch) {
        if (batch == 3) {
          int64_t actual = 0;
          ASSERT_TRUE(w.reclaimFixedSize(1 << 30, &actual).ok());
          reclaimed = actual > 0;
        }
      });
  EXPECT_TRUE(reclaimed) << "reclaim should have released chunk memory";
}

TEST_F(CellWriterTest, emptyPartitionsHaveZeroLength) {
  constexpr int32_t kPartitions = 8;
  const int n = 200;
  std::vector<int32_t> batchPids(n);
  for (int i = 0; i < n; ++i) {
    batchPids[i] = (i % 2) == 0 ? 2 : 5; // only partitions 2 and 5
  }
  auto values = makeFlatVector<int64_t>(n, [](auto row) { return row; });
  auto data = makeRowVector({"v"}, {values});
  CellShuffleWriter writer(
      makeOptions(kPartitions), pool(), arrow::default_memory_pool());
  roundTrip(makeOptions(kPartitions), {batchPids}, {data}, &writer);
  for (int32_t pid = 0; pid < kPartitions; ++pid) {
    if (pid != 2 && pid != 5) {
      EXPECT_EQ(writer.metrics().partitionLengths[pid], 0);
    } else {
      EXPECT_GT(writer.metrics().partitionLengths[pid], 0);
    }
  }
}

TEST_F(CellWriterTest, dictionaryInputAndLongStrings) {
  constexpr int32_t kPartitions = 4;
  const int n = 500;
  std::vector<int32_t> batchPids(n);
  for (int i = 0; i < n; ++i) {
    batchPids[i] = i % kPartitions;
  }
  // A dictionary-wrapped bigint column: the split must see through it
  // without flattening.
  auto base = makeFlatVector<int64_t>({10, 20, 30, 40, 50});
  auto indices = allocateIndices(n, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (int i = 0; i < n; ++i) {
    rawIndices[i] = i % 5;
  }
  auto dictionary = BaseVector::wrapInDictionary(nullptr, indices, n, base);
  // Strings crossing the 64-byte cache line exercise the direct-write path.
  std::vector<std::string> storage(n);
  std::vector<StringView> views(n);
  for (int i = 0; i < n; ++i) {
    storage[i].assign((i % 7) * 33 + 1, static_cast<char>('a' + i % 26));
    views[i] = StringView(storage[i]);
  }
  auto strings = makeFlatVector<StringView>(views);
  roundTrip(
      makeOptions(kPartitions),
      {batchPids},
      {makeRowVector({"d", "s"}, {dictionary, strings})});
}

TEST_F(CellWriterTest, staleNullsBufferAndFlatAllNull) {
  constexpr int32_t kPartitions = 4;
  const int n = 600;
  std::vector<int32_t> batchPids(n);
  for (int i = 0; i < n; ++i) {
    batchPids[i] = i % kPartitions;
  }
  // Column a: a defensively allocated, all-set nulls buffer - mayHaveNulls()
  // is stale and the batch scan must classify it as no-nulls.
  auto stale = makeFlatVector<int64_t>(n, [](auto row) { return row * 7; });
  auto allSet = AlignedBuffer::allocate<bool>(n, pool());
  ::memset(allSet->asMutable<uint8_t>(), 0xFF, (n + 7) / 8);
  stale->setNulls(allSet);
  ASSERT_TRUE(stale->mayHaveNulls());
  // Column b: a flat vector whose every row is null (values are garbage).
  auto flatNull = makeFlatVector<int32_t>(n, [](auto row) { return row; });
  auto allClear = AlignedBuffer::allocate<bool>(n, pool());
  ::memset(allClear->asMutable<uint8_t>(), 0, (n + 7) / 8);
  flatNull->setNulls(allClear);
  roundTrip(
      makeOptions(kPartitions),
      {batchPids},
      {makeRowVector({"a", "b"}, {stale, flatNull})});
}

TEST_F(CellWriterTest, constantNullColumnRoundTrip) {
  constexpr int32_t kPartitions = 4;
  const int n = 700;
  std::vector<int32_t> batchPids(n);
  for (int i = 0; i < n; ++i) {
    batchPids[i] = i % kPartitions;
  }
  auto values = makeFlatVector<int64_t>(n, [](auto row) { return row * 3; });
  auto allNull = BaseVector::createNullConstant(BIGINT(), n, pool());
  roundTrip(
      makeOptions(kPartitions),
      {batchPids},
      {makeRowVector({"v", "dead"}, {values, allNull})});
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
