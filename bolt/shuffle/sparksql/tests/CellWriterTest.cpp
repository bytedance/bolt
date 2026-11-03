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
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <optional>
#include <random>
#include <type_traits>

#include <fmt/format.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleWriter.h"
#include "bolt/shuffle/sparksql/tests/CellTestUtils.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

/// Flat schemas through the Cell shuffle, seen only from the outside: rows
/// into the writer the factory gives, every partition's bytes - from the
/// data file, or pushed to an RSS client - out through CellShuffleReader,
/// compared row by row with the input; and what the writer's metrics and
/// memory show of spills, seals, compression, reclaims and composite rows.
namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

using test::ShuffleInput;

/// For each bit width w a block of T packs values in: five blocks of values
/// within w bits, -2^(w-1) and 2^(w-1) - 1 among them, and five of a base
/// far from zero plus w-bit deltas, 0 and 2^w - 1 among them; then half a
/// block.
template <typename T>
std::vector<T> valuesOfEveryWidth(std::mt19937_64& rng) {
  constexpr uint32_t kPerBlock = kCacheLineBytes / sizeof(T);
  constexpr uint32_t kBits = 8 * sizeof(T);
  // The `i`-th of a block's values in [0, span): its extremes first.
  const auto offsetAt = [&](uint32_t i, uint64_t span) -> uint64_t {
    if (i == 0) {
      return 0;
    }
    if (i == 1) {
      return span - 1;
    }
    return rng() % span;
  };
  const int64_t base = std::numeric_limits<T>::min() / 2;
  std::vector<T> values;
  for (uint32_t width = 1; width < kBits; ++width) {
    const uint64_t span = uint64_t{1} << width;
    const int64_t low = -static_cast<int64_t>(span / 2);
    for (uint32_t block = 0; block < 5; ++block) {
      for (uint32_t i = 0; i < kPerBlock; ++i) {
        values.push_back(
            static_cast<T>(low + static_cast<int64_t>(offsetAt(i, span))));
      }
    }
    for (uint32_t block = 0; block < 5; ++block) {
      for (uint32_t i = 0; i < kPerBlock; ++i) {
        values.push_back(
            static_cast<T>(base + static_cast<int64_t>(offsetAt(i, span))));
      }
    }
  }
  for (uint32_t i = 0; i < kPerBlock / 2; ++i) {
    values.push_back(static_cast<T>(i));
  }
  return values;
}

class CellWriterTest : public testing::Test, public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    char pathTemplate[] = "/tmp/bolt_cell_writer_test_XXXXXX";
    ASSERT_NE(::mkdtemp(pathTemplate), nullptr);
    dir_ = pathTemplate;
    dataFile_ = dir_ + "/data";
  }

  void TearDown() override {
    std::filesystem::remove_all(dir_);
  }

  /// A hash shuffle of `numPartitions` partitions into the data file, its
  /// old files removed, or pushed to a RecordingRssClient when `rss`.
  ShuffleWriterOptions writerOptions(
      int32_t numPartitions,
      bool rss = false,
      arrow::Compression::type compression = arrow::Compression::LZ4_FRAME) {
    test::removeShuffleFiles(dataFile_);
    return test::shuffleOptions(dataFile_, numPartitions, compression, rss);
  }

  /// test::roundTrip() through this test's data file, at the default read
  /// shapes.
  void roundTrip(
      const ShuffleInput& input,
      int32_t numPartitions,
      const std::vector<test::ShuffleOutput>& outputs,
      const std::function<void(CellShuffleOptions&, bool rss)>& tune = nullptr,
      const std::function<void(const test::WrittenShuffle&, bool rss)>& check =
          nullptr) {
    test::roundTrip(
        input,
        numPartitions,
        dataFile_,
        pool(),
        tune,
        check,
        test::kReadShapes,
        outputs);
  }

  /// Row r of `rows` rows going to partition r % `numPartitions`.
  static std::vector<int32_t> roundRobinPids(
      vector_size_t rows,
      int32_t numPartitions) {
    std::vector<int32_t> pids(rows);
    for (vector_size_t row = 0; row < rows; ++row) {
      pids[row] = row % numPartitions;
    }
    return pids;
  }

  /// `data` as consecutive batches of at most `rows` rows, partitioned as
  /// `pids`. With partitionSealBytes = 1, every batch boundary then
  /// closes a window: one payload per batch and partition.
  ShuffleInput sliceBatches(
      const RowVectorPtr& data,
      const std::vector<int32_t>& pids,
      vector_size_t rows) {
    ShuffleInput input;
    for (vector_size_t begin = 0; begin < data->size(); begin += rows) {
      const auto length = std::min(rows, data->size() - begin);
      input.data.push_back(
          std::dynamic_pointer_cast<RowVector>(data->slice(begin, length)));
      input.pids.emplace_back(
          pids.begin() + begin, pids.begin() + begin + length);
    }
    return input;
  }

  /// `n` strings of up to 90 letters, a fifth of them null.
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

  /// `n` rows of 1KB single-letter strings beside a small bigint - the
  /// strings compress far below their size. Row 0 goes to the last
  /// partition with an 8-byte string, the rest spreads over the others.
  RowVectorPtr makeCompressibleBatch(
      int n,
      int32_t numPartitions,
      std::mt19937& rng,
      std::vector<int32_t>& pids) {
    pids.resize(n);
    std::vector<std::string> storage(n);
    std::vector<std::optional<StringView>> views(n);
    for (int i = 0; i < n; ++i) {
      pids[i] = i == 0 ? numPartitions - 1 : rng() % (numPartitions - 1);
      storage[i].assign(i == 0 ? 8 : 1024, static_cast<char>('a' + i % 4));
      views[i] = StringView(storage[i]);
    }
    return makeRowVector(
        {"v", "s"},
        {makeFlatVector<int64_t>(n, [](auto row) { return row % 977; }),
         makeNullableFlatVector<StringView>(views)});
  }

  std::string dir_;
  std::string dataFile_;
};

TEST_F(CellWriterTest, factoryPicksTheCellWriter) {
  // With forceShuffleWriterType Cell the factory gives a CellShuffleWriter,
  // whose rows read back. The legacy create() keeps its signature and return
  // type - integrations holding a shared_ptr<BoltShuffleWriter> keep
  // compiling - and refuses Cell. A VARIANT column, at the top level or
  // nested, fails the split as a schema the Cell shuffle does not carry,
  // instead of taking another writer or format.
  static_assert(std::is_same_v<
                decltype(BoltShuffleWriter::create(
                    std::declval<const ShuffleWriterOptions&>(),
                    0,
                    0,
                    0,
                    0,
                    nullptr,
                    nullptr)),
                std::shared_ptr<BoltShuffleWriter>>);
  auto options = writerOptions(2);
  options.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::Cell);
  EXPECT_THROW(
      BoltShuffleWriter::create(
          options, 1, 1, 0, 1 << 20, pool(), arrow::default_memory_pool()),
      BoltException);

  const auto input = test::roundRobin(
      {makeRowVector(
          {makeFlatVector<int64_t>(64, [](auto row) { return row * 7; })})},
      2);
  const auto& data = input.data[0];
  {
    auto writer = BoltShuffleWriter::createShuffleWriter(
        options,
        data->childrenSize(),
        data->size(),
        data->estimateFlatSize(),
        1 << 20,
        pool(),
        arrow::default_memory_pool());
    ASSERT_NE(dynamic_cast<CellShuffleWriter*>(writer.get()), nullptr);
    test::expectReadsBack(
        input,
        test::writeShuffle(*writer, options, input, pool()),
        arrow::Compression::LZ4_FRAME,
        pool());
  }

  for (const auto& type : std::vector<TypePtr>{
           VARIANT(), ARRAY(VARIANT()), ROW({"nested"}, {VARIANT()})}) {
    SCOPED_TRACE(type->toString());
    auto writer = BoltShuffleWriter::createShuffleWriter(
        options, 1, 1, 0, 1 << 20, pool(), arrow::default_memory_pool());
    ASSERT_NE(dynamic_cast<CellShuffleWriter*>(writer.get()), nullptr);
    const auto rows = makeRowVector({BaseVector::create(type, 1, pool())});
    BOLT_ASSERT_THROW(
        (void)writer->split(test::withPid({0}, rows, pool()), 0),
        "Unsupported Cell logical schema");
  }
}

TEST_F(CellWriterTest, preferCellShuffleOverridesForcedV1AndV2) {
  // preferCellShuffle takes priority over a forced V1 or V2, not over a
  // forced RowBased, and never applies without a pid. The readers decide
  // through writesCellShuffle(), which must agree with the writer the
  // factory returns.
  struct Case {
    Partitioning partitioning;
    ShuffleWriterType forced;
    bool prefer;
    bool cell;
  };
  const std::vector<Case> cases{
      {Partitioning::kHash, ShuffleWriterType::V1, true, true},
      {Partitioning::kHash, ShuffleWriterType::V2, true, true},
      {Partitioning::kHash, ShuffleWriterType::RowBased, true, false},
      {Partitioning::kHash, ShuffleWriterType::V1, false, false},
      {Partitioning::kHash, ShuffleWriterType::V2, false, false},
      {Partitioning::kHash, ShuffleWriterType::Adaptive, true, true},
      {Partitioning::kHash, ShuffleWriterType::Cell, false, true},
      {Partitioning::kSingle, ShuffleWriterType::Adaptive, true, false},
      {Partitioning::kSingle, ShuffleWriterType::V1, true, false},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(fmt::format(
        "partitioning={} forced={} prefer={}",
        static_cast<int>(c.partitioning),
        static_cast<int>(c.forced),
        c.prefer));
    auto options =
        writerOptions(c.partitioning == Partitioning::kSingle ? 1 : 2);
    options.partitioning = c.partitioning;
    options.forceShuffleWriterType = static_cast<int32_t>(c.forced);
    options.preferCellShuffle = c.prefer;
    auto writer = BoltShuffleWriter::createShuffleWriter(
        options, 1, 64, 64 * 8, 1 << 20, pool(), arrow::default_memory_pool());
    ASSERT_NE(writer, nullptr);
    EXPECT_EQ(
        dynamic_cast<CellShuffleWriter*>(writer.get()) != nullptr, c.cell);
    // The readers' mirror: a narrow schema, so the adaptive choice is not
    // the row-based writer.
    EXPECT_EQ(
        writesCellShuffle(
            c.partitioning,
            c.forced,
            c.prefer,
            /*isRowBased=*/c.forced == ShuffleWriterType::RowBased),
        c.cell);
  }
  // An adaptive choice of the row-based writer stays row-based.
  EXPECT_FALSE(writesCellShuffle(
      Partitioning::kHash, ShuffleWriterType::Adaptive, true, true));
}

TEST_F(CellWriterTest, valuesAtTheirLimits) {
  // Values at the edges of what the format carries read back from every
  // partition, locally and from an RSS client: integers of every bit width
  // a block packs them in, timestamps at their extremes and to the
  // nanosecond, strings from empty to 300 KiB, a null run longer than a
  // null record's, null densities from 0.1% to 99.9% across partition
  // seals, and the pid column flat, in a dictionary and constant.
  constexpr int32_t kPartitions = 3;
  const std::vector<test::ShuffleOutput> outputs{
      {false, arrow::Compression::LZ4_FRAME},
      {true, arrow::Compression::LZ4_FRAME}};
  std::mt19937_64 rng(53);
  {
    SCOPED_TRACE("every bit width");
    // Partition p takes value k at row 3k + p: each partition's stream
    // holds every width's blocks, and the vector path takes the full
    // blocks with data after them, the scalar one the stream's last.
    const auto smallints = valuesOfEveryWidth<int16_t>(rng);
    const auto integers = valuesOfEveryWidth<int32_t>(rng);
    const auto bigints = valuesOfEveryWidth<int64_t>(rng);
    const auto length =
        std::max({smallints.size(), integers.size(), bigints.size()});
    const auto valueAt = [&](const auto& values, vector_size_t row) {
      const size_t k = row / kPartitions;
      return k < values.size() ? values[k] : 0;
    };
    const auto rows = static_cast<vector_size_t>(kPartitions * length);
    roundTrip(
        test::roundRobin(
            {makeRowVector(
                {"s", "i", "b"},
                {makeFlatVector<int16_t>(
                     rows, [&](auto row) { return valueAt(smallints, row); }),
                 makeFlatVector<int32_t>(
                     rows, [&](auto row) { return valueAt(integers, row); }),
                 makeFlatVector<int64_t>(
                     rows, [&](auto row) { return valueAt(bigints, row); })})},
            kPartitions),
        kPartitions,
        outputs);
  }
  {
    SCOPED_TRACE("extreme timestamps and string lengths");
    constexpr vector_size_t kRows = 1500;
    const std::vector<Timestamp> extremes{
        Timestamp(Timestamp::kMinSeconds, 0),
        Timestamp(Timestamp::kMaxSeconds, Timestamp::kMaxNanos),
        Timestamp(-1, 1),
        Timestamp(0, 999'999'999)};
    const std::vector<size_t> lengths{0, 7, 8, 63, 64};
    const std::string large(300 << 10, 'q');
    std::vector<std::string> strings(kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      strings[row] = row % 500 == 499
          ? large
          : std::string(lengths[row % lengths.size()], 'a' + row % 26);
    }
    roundTrip(
        test::roundRobin(
            {makeRowVector(
                {"t", "s"},
                {makeFlatVector<Timestamp>(
                     kRows,
                     [&](auto row) {
                       if (row < extremes.size()) {
                         return extremes[row];
                       }
                       // Nanoseconds a microsecond encoding would lose.
                       return Timestamp(
                           1'700'000'000 + row * 37,
                           (row * 7919) % 1'000'000'000);
                     },
                     [](auto row) { return row % 11 == 5; }),
                 makeFlatVector<std::string>(
                     kRows, [&](auto row) { return strings[row]; })})},
            kPartitions),
        kPartitions,
        outputs);
  }
  {
    SCOPED_TRACE("a null run past a null record's");
    // Partition 0 takes 40000 null rows in a row, more than a null
    // record's run holds, in a batch whose other rows are not null.
    constexpr vector_size_t kNullRows = 40'000;
    constexpr vector_size_t kRows = kNullRows + 1000;
    ShuffleInput input;
    input.data.push_back(makeRowVector({makeFlatVector<int64_t>(
        kRows,
        [](auto row) { return row * 3; },
        [](auto row) { return row < kNullRows; })}));
    auto& pids = input.pids.emplace_back(kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      pids[row] = row < kNullRows ? 0 : row % kPartitions;
    }
    roundTrip(input, kPartitions, outputs);
  }
  {
    SCOPED_TRACE("every null density");
    // A batch at each density, every partition's payload sealed past
    // 64 KiB along the way.
    constexpr vector_size_t kRows = 4096;
    ShuffleInput input;
    for (const uint32_t perMille : {1, 10, 100, 500, 900, 990, 999, 500}) {
      std::vector<bool> nulls(kRows);
      for (auto&& null : nulls) {
        null = rng() % 1000 < perMille;
      }
      input.data.push_back(makeRowVector(
          {"v", "s"},
          {makeFlatVector<int32_t>(
               kRows,
               [&](auto row) { return static_cast<int32_t>(rng()); },
               [&](auto row) { return nulls[row]; }),
           makeFlatVector<std::string>(kRows, [&](auto row) {
             return std::string(24, 'a' + (row + perMille) % 26);
           })}));
      input.pids.push_back(roundRobinPids(kRows, kPartitions));
    }
    roundTrip(
        input,
        kPartitions,
        outputs,
        [](CellShuffleOptions& cell, bool) {
          cell.partitionSealBytes = 64 << 10;
        },
        [](const test::WrittenShuffle& written, bool) {
          EXPECT_GT(written.metrics.spillCount, 0);
        });
  }
  {
    SCOPED_TRACE("the pid column flat, in a dictionary and constant");
    constexpr vector_size_t kRows = 999;
    // Batch 0's rows go round robin, batch 1's the other way round, batch
    // 2's all to partition 1.
    const auto pidOf = [](int64_t batch, vector_size_t row) {
      if (batch == 0) {
        return row % 3;
      }
      return batch == 1 ? 2 - row % 3 : 1;
    };
    ShuffleInput input;
    for (int64_t batch = 0; batch < 3; ++batch) {
      input.data.push_back(makeRowVector({makeFlatVector<int64_t>(
          kRows, [&](auto row) { return row * 13 + batch; })}));
      auto& pids = input.pids.emplace_back(kRows);
      for (vector_size_t row = 0; row < kRows; ++row) {
        pids[row] = pidOf(batch, row);
      }
    }
    const auto batchOf = [&](size_t batch) -> RowVectorPtr {
      VectorPtr pid;
      if (batch == 0) {
        pid = makeFlatVector<int32_t>(input.pids[0]);
      } else if (batch == 1) {
        pid = BaseVector::wrapInDictionary(
            nullptr,
            makeIndices(kRows, [&](auto row) { return pidOf(1, row); }),
            kRows,
            makeFlatVector<int32_t>({0, 1, 2}));
      } else {
        pid = makeConstant<int32_t>(1, kRows);
      }
      return makeRowVector({"pid", "v"}, {pid, input.data[batch]->childAt(0)});
    };
    for (const bool rss : {false, true}) {
      SCOPED_TRACE(rss ? "rss" : "local");
      const auto options = writerOptions(kPartitions, rss);
      test::expectReadsBack(
          input,
          test::writeShuffle(options, input, pool(), batchOf),
          arrow::Compression::LZ4_FRAME,
          pool());
    }
  }
}

TEST_F(CellWriterTest, lazyInputIsSlicedByLoadedBytes) {
  // Lazy children estimate as zero bytes until loaded; the byte bound must
  // still split a batch whose loaded size exceeds it.
  constexpr vector_size_t kRows = 3300;
  constexpr int32_t kValueBytes = 64 << 10;
  auto chars = AlignedBuffer::allocate<char>(
      static_cast<size_t>(kRows) * kValueBytes, pool());
  ::memset(chars->asMutable<char>(), 'q', chars->size());
  auto views = AlignedBuffer::allocate<StringView>(kRows, pool());
  for (vector_size_t row = 0; row < kRows; ++row) {
    views->asMutable<StringView>()[row] = StringView(
        chars->as<char>() + static_cast<size_t>(row) * kValueBytes,
        kValueBytes);
  }
  auto strings = std::make_shared<FlatVector<StringView>>(
      pool(),
      VARBINARY(),
      nullptr,
      kRows,
      views,
      std::vector<BufferPtr>{chars});
  auto lazy = makeRowVector({wrapInLazyDictionary(strings)});
  ASSERT_GT(kRows * int64_t{kValueBytes}, kMaxShuffleWriterBatchBytes);
  CellShuffleWriter writer(
      writerOptions(1), pool(), arrow::default_memory_pool());
  ASSERT_TRUE(
      writer
          .split(test::withPid(std::vector<int32_t>(kRows, 0), lazy, pool()), 0)
          .ok());
  EXPECT_GE(writer.metrics().totalInputBatches, 2);
  ASSERT_TRUE(writer.stop().ok());
}

TEST_F(CellWriterTest, everyCodecShrinksSpillsAndOutput) {
  // Every codec, streaming and one-shot, shrinks what spills and what is
  // written, locally and pushed, to under half of the uncompressed
  // shuffle, despite a column of random numbers; the raw lengths match the
  // uncompressed sizes. A pool with no room to spare compresses alike.
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(37);
  std::mt19937_64 random(41);
  ShuffleInput input;
  for (int batch = 0; batch < 6; ++batch) {
    auto& pids = input.pids.emplace_back();
    const auto strings = makeCompressibleBatch(2048, kPartitions, rng, pids);
    input.data.push_back(makeRowVector(
        {"v", "s", "r"},
        {strings->childAt(0),
         strings->childAt(1),
         makeFlatVector<int64_t>(
             2048, [&](auto) { return static_cast<int64_t>(random()); })}));
  }
  const auto write =
      [&](bool rss, arrow::Compression::type compression, bool streaming) {
        auto options = writerOptions(kPartitions, rss, compression);
        options.cellOptions.cellMemorySpillBytes = 8 << 20;
        options.cellOptions.streamCompression = streaming;
        auto written = test::writeShuffle(options, input, pool());
        test::expectReadsBack(input, written, compression, pool());
        return written;
      };
  for (const bool rss : {false, true}) {
    const auto uncompressed =
        write(rss, arrow::Compression::UNCOMPRESSED, /*streaming=*/true);
    EXPECT_GT(uncompressed.metrics.totalBytesEvicted, 0);
    const auto codecs = rss
        ? std::vector<arrow::Compression::type>{arrow::Compression::LZ4_FRAME}
        : std::vector<arrow::Compression::type>{
              arrow::Compression::LZ4_FRAME,
              arrow::Compression::ZSTD,
              arrow::Compression::GZIP};
    for (const auto codec : codecs) {
      for (const bool streaming : {true, false}) {
        SCOPED_TRACE(fmt::format(
            "{} {}",
            test::ShuffleOutput{rss, codec}.toString(),
            streaming ? "streaming" : "one-shot"));
        const auto compressed = write(rss, codec, streaming);
        const auto& metrics = compressed.metrics;
        EXPECT_LT(
            metrics.totalBytesEvicted * 2,
            uncompressed.metrics.totalBytesEvicted);
        EXPECT_LT(
            metrics.totalBytesWritten * 2,
            uncompressed.metrics.totalBytesWritten);
        EXPECT_EQ(
            metrics.rawPartitionLengths, uncompressed.metrics.partitionLengths);
      }
    }
  }

  // 32 MB through a 24 MiB pool, raised after the last batch so that
  // stop() has room to write the output.
  ShuffleInput large;
  int64_t rawBytes = 0;
  for (int batch = 0; batch < 8; ++batch) {
    large.data.push_back(makeCompressibleBatch(
        4096, kPartitions, rng, large.pids.emplace_back()));
    rawBytes += large.data.back()->estimateFlatSize();
  }
  for (const bool streaming : {true, false}) {
    SCOPED_TRACE(streaming ? "streaming, squeezed" : "one-shot, squeezed");
    auto root = memory::memoryManager()->addRootPool(
        fmt::format("squeezedCodec{}", streaming), 24 << 20);
    auto leaf = root->addLeafChild("writer");
    auto options = writerOptions(kPartitions);
    options.cellOptions.partitionSealBytes = 4 << 20;
    options.cellOptions.streamCompression = streaming;
    CellShuffleWriter writer(options, leaf.get(), arrow::default_memory_pool());
    const auto written = test::writeShuffle(
        writer, options, large, pool(), nullptr, [&](size_t batch) {
          if (batch + 1 == large.data.size()) {
            static_cast<memory::MemoryPoolImpl*>(root.get())
                ->testingSetCapacity(1LL << 30);
          }
        });
    test::expectReadsBack(
        large, written, arrow::Compression::LZ4_FRAME, pool());
    EXPECT_GT(written.metrics.spillCount, 0);
    EXPECT_LT(written.metrics.totalBytesEvicted * 8, rawBytes);
    EXPECT_LT(written.metrics.totalBytesWritten * 8, rawBytes);
  }
}

TEST_F(CellWriterTest, spillsCompressInSealedAndResidualWindows) {
  // ~4MB per batch: with an 8MB chunk cap every window sees several
  // pressure spills. The last partition takes one short row per batch, so
  // its segments stay below the compression minimum and are stored as they
  // are. The closing small batch leaves resident cells
  // beside the last window's spilled segments. Sealed windows, the first
  // payload of a partition spanning several spill runs, then one window
  // only, whose payload carries its spilled segments and then the resident
  // cells. Spills are compressed: what hits the spill file is far smaller
  // than without a codec.
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(31);
  ShuffleInput input;
  for (int batch = 0; batch < 9; ++batch) {
    input.data.push_back(makeCompressibleBatch(
        batch < 8 ? 4096 : 256, kPartitions, rng, input.pids.emplace_back()));
  }
  for (const bool seal : {true, false}) {
    SCOPED_TRACE(seal ? "sealed windows" : "residual window");
    int64_t evicted[2] = {0, 0};
    for (const bool compressed : {true, false}) {
      const auto codec = compressed ? arrow::Compression::LZ4_FRAME
                                    : arrow::Compression::UNCOMPRESSED;
      auto options = writerOptions(kPartitions);
      options.partitionWriterOptions.compressionType = codec;
      options.cellOptions.cellMemorySpillBytes = 8 << 20;
      options.cellOptions.partitionSealBytes = seal ? 4 << 20 : 1LL << 30;
      const auto written = test::writeShuffle(options, input, pool());
      test::expectReadsBack(input, written, codec, pool());
      EXPECT_GT(written.metrics.totalBytesEvicted, 0);
      EXPECT_EQ(written.metrics.spillCount > 0, seal);
      evicted[compressed] = written.metrics.totalBytesEvicted;
    }
    EXPECT_LT(evicted[1] * 2, evicted[0]);
  }
}

TEST_F(CellWriterTest, dictionaryVocabularies) {
  // String columns probed into dictionary form, behind an UNKNOWN column
  // that has no wire column: the dictionary-form bit is the wire column's.
  // s shifts vocabulary mid-batch, so that a segment closes and a
  // successor chains; takes values too long for any entry and a
  // high-cardinality batch, which demote partitions to the fallback tail;
  // and returns to its vocabulary, re-engaging in a later window. l takes
  // vocabularies of eight-byte and of twelve-byte words, words that are
  // prefixes of each other, and a 70-byte value as the first of every
  // partition's window. Payloads close at 2 KiB, so every window starts
  // over; locally and from an RSS client every row reads back.
  constexpr int32_t kPartitions = 4;
  constexpr int kRows = 1500;
  const std::vector<std::string> vocabA = {
      "north", "south", "east!", "west!", "same1", "same2", "left-", "right"};
  const std::vector<std::string> vocabB = {
      "ocean", "river", "lake!", "pond!", "sea--", "bay--", "gulf-", "creek"};
  const std::vector<std::string> eightBytes = {
      "window-1", "window-2", "window-3", "window-4", "window-5", "window-6"};
  const std::vector<std::string> twelveBytes = {
      "partition-01", "partition-02", "partition-03", "partition-04"};
  // The longest first: it is a partition's first entry.
  const std::vector<std::string> prefixed = {
      "partition-key-0001-x", "partition-key-0001", "partition-key-0002"};
  const std::string longValue(70, 'L');
  std::mt19937 rng(7);
  const auto pick = [&](const std::vector<std::string>& words) {
    return words[rng() % words.size()];
  };
  ShuffleInput input;
  // `s(i)` and `l(i, first)` give row i's values, `first` when it is its
  // partition's first in the batch.
  const auto addBatch =
      [&](const std::function<std::optional<std::string>(int)>& s,
          const std::function<std::string(int, bool)>& l) {
        auto& pids = input.pids.emplace_back(kRows);
        std::vector<std::optional<std::string>> sValues(kRows);
        std::vector<std::string> lValues(kRows);
        std::vector<bool> seen(kPartitions, false);
        for (int i = 0; i < kRows; ++i) {
          pids[i] = rng() % kPartitions;
          sValues[i] = s(i);
          lValues[i] = l(i, !seen[pids[i]]);
          seen[pids[i]] = true;
        }
        input.data.push_back(makeRowVector(
            {"u", "s", "l"},
            {BaseVector::createNullConstant(UNKNOWN(), kRows, pool()),
             makeNullableFlatVector<std::string>(sValues),
             makeFlatVector<std::string>(lValues)}));
      };
  // The probe batch: both columns' vocabularies fit a dictionary.
  addBatch(
      [&](int) -> std::optional<std::string> {
        if (rng() % 9 == 0) {
          return std::nullopt;
        }
        return pick(vocabA);
      },
      [&](int, bool) { return pick(eightBytes); });
  addBatch(
      [&](int i) -> std::optional<std::string> {
        return i < kRows / 2 ? pick(vocabA) : pick(vocabB);
      },
      [&](int, bool) { return pick(twelveBytes); });
  addBatch(
      [&](int i) -> std::optional<std::string> {
        return i % 5 == 0 ? std::string(80, 'x') : pick(vocabB);
      },
      [&](int, bool first) { return first ? prefixed[0] : pick(prefixed); });
  addBatch(
      [&](int) -> std::optional<std::string> {
        return "uid-" + std::to_string(rng());
      },
      [&](int, bool first) { return first ? longValue : pick(eightBytes); });
  addBatch(
      [&](int) -> std::optional<std::string> { return pick(vocabA); },
      [&](int, bool) { return pick(twelveBytes); });
  roundTrip(
      input,
      kPartitions,
      {{false, arrow::Compression::LZ4_FRAME},
       {true, arrow::Compression::LZ4_FRAME}},
      [](CellShuffleOptions& cell, bool) { cell.partitionSealBytes = 2 << 10; },
      [](const test::WrittenShuffle& written, bool) {
        EXPECT_GT(written.metrics.dictionaryMatchedRows, 0);
        EXPECT_GT(written.metrics.dictionaryFallbackRows, 0);
        EXPECT_GT(written.metrics.spillCount, 1);
      });

  // A vocabulary that fits a dictionary takes every row, whatever the
  // lengths of its words.
  for (const auto& words : {eightBytes, twelveBytes, prefixed}) {
    SCOPED_TRACE(words[0]);
    constexpr vector_size_t kVocabularyRows = 2048;
    const auto vocabulary = test::roundRobin(
        {makeRowVector(
            {"u", "l"},
            {BaseVector::createNullConstant(UNKNOWN(), kVocabularyRows, pool()),
             makeFlatVector<std::string>(
                 kVocabularyRows,
                 [&](auto row) { return words[row % words.size()]; })})},
        kPartitions);
    const auto written =
        test::writeShuffle(writerOptions(kPartitions), vocabulary, pool());
    test::expectReadsBack(
        vocabulary, written, arrow::Compression::LZ4_FRAME, pool());
    EXPECT_EQ(written.metrics.dictionaryMatchedRows, kVocabularyRows);
    EXPECT_EQ(written.metrics.dictionaryFallbackRows, 0);
  }
}

TEST_F(CellWriterTest, readerStreamsPayloadsInBatches) {
  // One large payload per partition comes out over many batches of
  // batchSize rows; payloads near a batch's size straddle batches, and
  // many small ones fill one batch across their boundaries. Every column
  // kind, with nulls, inline and out-of-line strings, dictionary strings
  // and an all-null column.
  constexpr int32_t kPartitions = 2;
  constexpr vector_size_t kRows = 20'000;
  std::mt19937 rng(5);
  const auto data = makeRowVector(
      {makeFlatVector<int64_t>(
           kRows,
           [](auto row) { return row * 7919; },
           [](auto row) { return row % 13 == 0; }),
       makeFlatVector<int16_t>(kRows, [](auto row) { return row % 300; }),
       makeFlatVector<bool>(
           kRows,
           [](auto row) { return row % 3 == 0; },
           [](auto row) { return row % 7 == 0; }),
       makeFlatVector<double>(
           kRows,
           [](auto row) { return row * 0.5; },
           [](auto row) { return row % 5 == 0; }),
       makeFlatVector<Timestamp>(
           kRows,
           [](auto row) { return Timestamp(row, row * 1000); },
           [](auto row) { return row % 9 == 0; }),
       makeNullableStrings(kRows, rng),
       makeFlatVector<StringView>(
           kRows,
           [](auto row) {
             return StringView(
                 row % 3 == 0 ? "a dictionary value past inline" : "short");
           }),
       BaseVector::createNullConstant(BIGINT(), kRows, pool())});
  const auto type = asRowType(data->type());
  const auto pids = roundRobinPids(kRows, kPartitions);
  const auto codec = test::lz4Codec();
  for (const vector_size_t windowRows : {0, 2048, 7}) {
    SCOPED_TRACE(windowRows);
    auto options = writerOptions(kPartitions);
    auto input = test::roundRobin({data}, kPartitions);
    if (windowRows != 0) {
      options.cellOptions.partitionSealBytes = 1;
      input = sliceBatches(data, pids, windowRows);
    }
    const auto written = test::writeShuffle(options, input, pool());
    // The dictionary probe needs a first batch of 1024 rows.
    if (windowRows == 0 || windowRows >= 1024) {
      EXPECT_GT(written.metrics.dictionaryMatchedRows, 0);
    }
    const auto rows = input.rowsOf(kPartitions);
    for (int32_t pid = 0; pid < kPartitions; ++pid) {
      SCOPED_TRACE(fmt::format("partition {}", pid));
      const auto& bytes = written.partitions[pid];
      if (windowRows == 0) {
        // A window that never closes is one payload per partition.
        EXPECT_EQ(
            test::readerBatchSizes(bytes, type, codec.get(), pool(), kRows, 0),
            std::vector<vector_size_t>{kRows / kPartitions});
      }
      for (const int32_t batchSize : {333, 1000}) {
        SCOPED_TRACE(batchSize);
        const auto batches = test::readPartition(
            bytes, type, codec.get(), pool(), batchSize, 1 << 30);
        test::expectPartitionRows(rows[pid], batches, pid, batchSize);
        // Only the last batch of a partition may be short.
        for (size_t b = 0; b + 1 < batches.size(); ++b) {
          EXPECT_EQ(batches[b]->size(), batchSize);
        }
      }
      // A byte target at or below zero still reads every row, one append
      // per batch: at most one window's rows when the windows are small.
      for (const int64_t batchByteSize : {int64_t{0}, int64_t{-1}}) {
        SCOPED_TRACE(batchByteSize);
        const auto batches = test::readPartition(
            bytes, type, codec.get(), pool(), 1000, batchByteSize);
        test::expectPartitionRows(rows[pid], batches, pid, 1000);
        for (const auto& batch : batches) {
          EXPECT_LE(batch->size(), windowRows == 0 ? 1000 : windowRows);
        }
      }
    }
  }

  // A positive byte target closes a batch once its rows take that many
  // bytes, out-of-line strings counted: payloads of a few rows of 1 KiB
  // strings fill batches of 64 KiB, past it by at most a payload. A batch
  // size of 0 reads a row per batch.
  constexpr vector_size_t kStringRows = 2000;
  constexpr int64_t kBatchBytes = 64 << 10;
  const std::string text(1 << 10, 't');
  const auto strings = makeRowVector(
      {makeFlatVector<int64_t>(kStringRows, [](auto row) { return row; }),
       makeFlatVector<StringView>(
           kStringRows, [&](auto) { return StringView(text); })});
  auto options = writerOptions(kPartitions);
  options.cellOptions.partitionSealBytes = 1;
  const auto input =
      sliceBatches(strings, roundRobinPids(kStringRows, kPartitions), 7);
  const auto written = test::writeShuffle(options, input, pool());
  const auto rows = input.rowsOf(kPartitions);
  const auto stringsType = asRowType(strings->type());
  // A row's bytes: its two columns' and its string's.
  const int64_t rowBytes = 8 + 16 + static_cast<int64_t>(text.size());
  for (int32_t pid = 0; pid < kPartitions; ++pid) {
    SCOPED_TRACE(fmt::format("partition {}", pid));
    auto batches = test::readPartition(
        written.partitions[pid],
        stringsType,
        codec.get(),
        pool(),
        1000,
        kBatchBytes);
    test::expectPartitionRows(rows[pid], batches, pid, 1000);
    for (size_t b = 0; b < batches.size(); ++b) {
      if (b + 1 < batches.size()) {
        EXPECT_GE(batches[b]->size() * rowBytes, kBatchBytes);
      }
      EXPECT_LE(batches[b]->size() * rowBytes, kBatchBytes + 4 * rowBytes);
    }
    batches = test::readPartition(
        written.partitions[pid], stringsType, codec.get(), pool(), 0, 1 << 20);
    test::expectPartitionRows(rows[pid], batches, pid, 1);
  }
}

TEST_F(CellWriterTest, severalWritersShareAPartition) {
  // Map tasks write a partition each in their own way - one with string
  // dictionaries, one without, one with nulls besides - and its reader
  // takes their bytes as a stream each: the rows come back writer by
  // writer, in order. A flat schema, and a nested one whose strings at the
  // rows' positions take dictionaries as a flat column's do.
  constexpr int32_t kPartitions = 3;
  constexpr vector_size_t kRows = 2048;
  constexpr int kWriters = 3;
  const char* const kWords[] = {"red", "green", "blue", "cyan"};
  for (const bool nested : {false, true}) {
    SCOPED_TRACE(nested ? "nested" : "flat");
    std::vector<ShuffleInput> inputs;
    for (int writer = 0; writer < kWriters; ++writer) {
      const auto nulls = [&](int every) -> std::function<bool(vector_size_t)> {
        if (writer != 2) {
          return nullptr;
        }
        return nullEvery(every);
      };
      const auto words = [&](int shift) {
        return makeFlatVector<StringView>(
            kRows,
            [&, shift](auto row) {
              return StringView(kWords[(row + shift + writer) % 4]);
            },
            nulls(3));
      };
      std::vector<std::string> names{"s", "v"};
      std::vector<VectorPtr> columns{
          words(0),
          makeFlatVector<int64_t>(
              kRows, [&](auto row) { return writer * kRows + row; }, nulls(4))};
      if (nested) {
        names.insert(names.end(), {"a", "r"});
        columns.push_back(makeArrayVector<int64_t>(
            kRows,
            [](auto row) { return row % 3; },
            [&](auto idx) { return writer + idx; },
            nulls(5)));
        columns.push_back(makeRowVector(
            {"x", "t"},
            {makeFlatVector<int32_t>(kRows, [](auto row) { return row; }),
             words(1)},
            nulls(7)));
      }
      inputs.push_back(
          test::roundRobin({makeRowVector(names, columns)}, kPartitions));
    }
    std::vector<test::WrittenShuffle> written;
    for (int writer = 0; writer < kWriters; ++writer) {
      auto options = writerOptions(kPartitions);
      options.cellOptions.enableStringDictionary = writer != 1;
      written.push_back(test::writeShuffle(options, inputs[writer], pool()));
      EXPECT_EQ(written.back().metrics.dictionaryMatchedRows > 0, writer != 1);
    }
    const auto type = asRowType(inputs[0].data[0]->type());
    const auto codec = test::lz4Codec();
    for (int32_t pid = 0; pid < kPartitions; ++pid) {
      std::vector<test::RowRef> expected;
      std::vector<std::string> streams;
      for (int writer = 0; writer < kWriters; ++writer) {
        const auto rows = inputs[writer].rowsOf(kPartitions)[pid];
        expected.insert(expected.end(), rows.begin(), rows.end());
        streams.push_back(written[writer].partitions[pid]);
      }
      for (const auto& shape : test::kReadShapes) {
        SCOPED_TRACE(shape.batchSize);
        test::expectPartitionRows(
            expected,
            test::readPartition(
                streams,
                type,
                codec.get(),
                pool(),
                shape.batchSize,
                shape.batchByteSize),
            pid,
            shape.batchSize);
      }
    }
  }
}

TEST_F(CellWriterTest, writerFailsCleanlyWhenThePoolRunsOut) {
  // Under a pool too small for it, a writer either fits - spilling, or
  // pushing windows to an RSS client - and its rows read back, or fails
  // with an error: at its first chunk, or with chunks held, inside a batch
  // larger than the pool. Destroyed, it gives every byte back either way:
  // nothing kept, nothing freed twice. Locally and on an RSS client.
  constexpr int32_t kPartitions = 16;
  constexpr vector_size_t kRows = 4096;
  std::mt19937 rng(59);
  const std::string text(2000, 'p');
  ShuffleInput input;
  for (int batch = 0; batch < 2; ++batch) {
    input.data.push_back(makeRowVector(
        {"v", "s"},
        {makeFlatVector<int64_t>(
             kRows,
             [&](auto) { return static_cast<int64_t>(rng()); },
             nullEvery(5)),
         makeFlatVector<StringView>(
             kRows, [&](auto) { return StringView(text); }, nullEvery(7))}));
    input.pids.push_back(roundRobinPids(kRows, kPartitions));
  }
  int failures = 0;
  int fits = 0;
  for (const bool rss : {false, true}) {
    for (const int64_t cap : {4 << 20, 8 << 20, 12 << 20}) {
      SCOPED_TRACE(
          fmt::format("{} cap {} MiB", rss ? "rss" : "local", cap >> 20));
      auto root = memory::memoryManager()->addRootPool(
          fmt::format("cellWriterCap{}{}", rss, cap), cap);
      auto leaf = root->addLeafChild("writer");
      const auto options = writerOptions(kPartitions, rss);
      std::optional<test::WrittenShuffle> written;
      {
        CellShuffleWriter writer(
            options, leaf.get(), arrow::default_memory_pool());
        try {
          written = test::writeShuffle(writer, options, input, pool());
        } catch (const BoltException&) {
        }
      }
      EXPECT_EQ(leaf->usedBytes(), 0);
      EXPECT_EQ(root->usedBytes(), 0);
      if (written.has_value()) {
        ++fits;
        test::expectReadsBack(
            input, *written, arrow::Compression::LZ4_FRAME, pool());
      } else {
        ++failures;
      }
    }
  }
  EXPECT_GT(failures, 0);
  EXPECT_GT(fits, 0);
}

TEST_F(CellWriterTest, reclaimCompactsAndKeepsAppending) {
  // A reclaim asking for a quarter of the writer's memory spills the
  // largest partitions, whose cells lie in every chunk, and moves the
  // others' out of the emptiest chunks to give those back - writing far
  // less than the window. The partitions whose cells moved keep taking
  // rows, and a reclaim of three quarters of the memory then moves cells
  // again. Each frees what it asks for, and every row reads back.
  constexpr int32_t kPartitions = 8;
  constexpr vector_size_t kRows = 4096;
  std::mt19937 rng(61);
  ShuffleInput input;
  for (int batch = 0; batch < 14; ++batch) {
    std::vector<std::string> strings(kRows);
    for (auto& value : strings) {
      value.assign(1000, ' ');
      for (auto& c : value) {
        c = static_cast<char>('a' + rng() % 26);
      }
    }
    input.data.push_back(makeRowVector(
        {"v", "s"},
        {makeFlatVector<int64_t>(
             kRows, [&](auto) { return static_cast<int64_t>(rng()); }),
         makeFlatVector<std::string>(
             kRows, [&](auto row) { return strings[row]; })}));
    auto& pids = input.pids.emplace_back(kRows);
    for (auto& pid : pids) {
      pid = static_cast<int32_t>(rng() % kPartitions);
    }
  }
  // The writer's pool shows its memory alone.
  auto root = memory::memoryManager()->addRootPool("cellReclaimCompacts");
  auto leaf = root->addLeafChild("writer");
  const auto options =
      writerOptions(kPartitions, false, arrow::Compression::UNCOMPRESSED);
  CellShuffleWriter writer(options, leaf.get(), arrow::default_memory_pool());
  const auto reclaim = [&](int64_t size) {
    int64_t freed = 0;
    ASSERT_TRUE(writer.reclaimFixedSize(size, &freed).ok());
    EXPECT_GE(freed, size);
  };
  const auto written = test::writeShuffle(
      writer, options, input, pool(), nullptr, [&](size_t batch) {
        if (batch == 7) {
          const int64_t held = leaf->usedBytes();
          ASSERT_EQ(writer.metrics().totalBytesEvicted, 0);
          reclaim(held / 4);
          EXPECT_LT(writer.metrics().totalBytesEvicted, held / 2);
        } else if (batch == 11) {
          reclaim(leaf->usedBytes() * 3 / 4);
        }
      });
  test::expectReadsBack(
      input, written, arrow::Compression::UNCOMPRESSED, pool());
}

TEST_F(CellWriterTest, celebornReclaimPushesTheWindowBetweenBatches) {
  // Between batches the window is whole: a reclaim pushes it at once and
  // returns its chunks.
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(43);
  ShuffleInput input;
  for (int batch = 0; batch < 6; ++batch) {
    input.data.push_back(makeCompressibleBatch(
        4096, kPartitions, rng, input.pids.emplace_back()));
  }
  const auto options = writerOptions(kPartitions, true);
  const auto& client = test::rssClient(options);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  int64_t freed = 0;
  const auto written = test::writeShuffle(
      writer, options, input, pool(), nullptr, [&](size_t batch) {
        if (batch != 2) {
          return;
        }
        EXPECT_EQ(client.numPushes, 0);
        ASSERT_TRUE(writer.reclaimFixedSize(1LL << 30, &freed).ok());
        EXPECT_EQ(client.numPushes, kPartitions);
      });
  test::expectReadsBack(input, written, arrow::Compression::LZ4_FRAME, pool());
  EXPECT_GE(freed, 8 << 20);
  EXPECT_EQ(written.metrics.spillCount, 1);
  EXPECT_EQ(client.numPushes, 2 * kPartitions);
}

TEST_F(CellWriterTest, celebornEmptyMapperStillEndsTheAttempt) {
  const auto options = writerOptions(3, true);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  ASSERT_TRUE(writer.stop().ok());
  const auto& client = test::rssClient(options);
  EXPECT_EQ(client.stops, 1);
  EXPECT_EQ(client.numPushes, 0);
  EXPECT_EQ(writer.metrics().partitionLengths, std::vector<int64_t>(3, 0));
  EXPECT_FALSE(std::filesystem::exists(dataFile_));
}

TEST_F(CellWriterTest, compositeAndOrdinaryBatchesShareTheShuffle) {
  // Aggregation-state rows and ordinary rows alternate: every switch writes
  // the other side out first, and the reader hands back CompositeRowVectors
  // and ordinary batches, each with its rows intact and in order. The raw
  // lengths count both kinds.
  constexpr int32_t kPartitions = 3;
  const std::vector<bool> composite = {
      true, true, false, true, false, false, true};
  ShuffleInput input;
  int64_t base = 0;
  for (size_t batch = 0; batch < composite.size(); ++batch) {
    constexpr int n = 2500;
    auto& pids = input.pids.emplace_back(n);
    for (int i = 0; i < n; ++i) {
      pids[i] = (base + i * 7) % kPartitions;
    }
    input.data.push_back(makeRowVector(
        {"k", "s"},
        {makeFlatVector<int64_t>(n, [&](auto row) { return base + row; }),
         makeFlatVector<std::string>(n, [&](auto row) {
           return std::string((base + row) % 37, 'a' + row % 26);
         })}));
    base += n;
  }
  const auto rowType = asRowType(input.data[0]->type());
  // Per kind, ordinary first, and partition: the rows in order.
  std::vector<std::vector<test::RowRef>> expected[2];
  for (auto& rows : expected) {
    rows.resize(kPartitions);
  }
  for (size_t batch = 0; batch < composite.size(); ++batch) {
    for (vector_size_t row = 0; row < input.data[batch]->size(); ++row) {
      expected[composite[batch]][input.pids[batch][row]].emplace_back(
          input.data[batch].get(), row);
    }
  }
  const auto batchOf = [&](size_t batch) -> RowVectorPtr {
    if (composite[batch]) {
      return test::makeComposite(input.pids[batch], input.data[batch], pool());
    }
    return test::withPid(input.pids[batch], input.data[batch], pool());
  };
  for (const bool rss : {false, true}) {
    for (const auto compression :
         {arrow::Compression::LZ4_FRAME, arrow::Compression::UNCOMPRESSED}) {
      SCOPED_TRACE((test::ShuffleOutput{rss, compression}.toString()));
      const auto written = test::writeShuffle(
          writerOptions(kPartitions, rss, compression), input, pool(), batchOf);
      const auto& metrics = written.metrics;
      EXPECT_GE(metrics.spillCount, 1) << "an ordinary window was sealed";
      EXPECT_GT(metrics.convertTime, 0);
      // Every row and batch counts, composite or ordinary.
      EXPECT_EQ(metrics.totalInputRowNumber, base);
      EXPECT_EQ(
          metrics.totalInputBatches, static_cast<int64_t>(composite.size()));
      if (compression == arrow::Compression::UNCOMPRESSED) {
        EXPECT_EQ(metrics.rawPartitionLengths, metrics.partitionLengths);
      }
      const auto codec = test::codecOf(compression);
      for (int32_t pid = 0; pid < kPartitions; ++pid) {
        const auto reader = test::makeReader(
            test::partitionStreams(written, pid),
            rowType,
            codec.get(),
            pool(),
            1000,
            1 << 20);
        std::vector<RowVectorPtr> batches[2];
        while (auto batch = reader->next()) {
          if (RowVector::isComposite(batch)) {
            EXPECT_LE(batch->size(), 1000);
            batches[1].push_back(test::compositeRows(rowType, batch, pool()));
          } else {
            batches[0].push_back(std::move(batch));
          }
        }
        test::expectPartitionRows(expected[0][pid], batches[0], pid, 1000);
        SCOPED_TRACE("composite rows");
        test::expectPartitionRows(expected[1][pid], batches[1], pid, 1000);
      }
    }
  }
}

TEST_F(CellWriterTest, compositeReclaimWritesTheBufferedRows) {
  // While composite rows are held, a reclaim writes them out as blocks and
  // gives their memory back; only-composite output still reaches the data
  // file (local) or the pushes (Celeborn), and the attempt ends once.
  constexpr int32_t kPartitions = 2;
  const auto data = makeRowVector(
      {"k", "s"},
      {makeFlatVector<int64_t>(4000, [](auto row) { return row; }),
       makeFlatVector<std::string>(
           4000, [](auto row) { return std::string(500 + row % 1000, 'z'); })});
  const auto pids = roundRobinPids(data->size(), kPartitions);
  for (const bool rss : {false, true}) {
    SCOPED_TRACE(rss ? "rss" : "local");
    const auto options = writerOptions(kPartitions, rss);
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    ASSERT_TRUE(writer.split(test::makeComposite(pids, data, pool()), 0).ok());
    if (rss) {
      EXPECT_EQ(test::rssClient(options).numPushes, 0);
    }
    int64_t freed = 0;
    ASSERT_TRUE(writer.reclaimFixedSize(1LL << 30, &freed).ok());
    EXPECT_GT(freed, 0);
    if (rss) {
      EXPECT_EQ(test::rssClient(options).numPushes, kPartitions);
    }
    ASSERT_TRUE(writer.split(test::makeComposite(pids, data, pool()), 0).ok());
    ASSERT_TRUE(writer.stop().ok());
    const auto& metrics = writer.metrics();
    // Spark reports these as the stage's records written, which AQE reads
    // as the shuffle's row count: zero would make it an empty relation.
    EXPECT_EQ(metrics.totalInputRowNumber, 2 * data->size());
    EXPECT_EQ(metrics.totalInputBatches, 2);
    EXPECT_GT(metrics.totalBytesWritten, 0);
    EXPECT_GT(metrics.totalBytesEvicted, 0);
    EXPECT_GT(metrics.dataSize, 2 * data->size() * 4);
    int64_t lengthSum = 0;
    for (const auto length : metrics.partitionLengths) {
      lengthSum += length;
    }
    EXPECT_EQ(lengthSum, metrics.totalBytesWritten);
    if (rss) {
      EXPECT_EQ(test::rssClient(options).stops, 1);
      EXPECT_EQ(test::rssClient(options).numPushes, 2 * kPartitions);
    } else {
      EXPECT_EQ(
          static_cast<int64_t>(std::filesystem::file_size(dataFile_)),
          metrics.totalBytesWritten);
    }
  }
}

TEST_F(CellWriterTest, reclaimAfterStopIsHarmless) {
  CellShuffleWriter writer(
      writerOptions(2), pool(), arrow::default_memory_pool());
  auto data = makeRowVector(
      {makeFlatVector<int64_t>(4096, [](auto row) { return row; })});
  ASSERT_TRUE(
      writer
          .split(test::withPid(std::vector<int32_t>(4096, 1), data, pool()), 0)
          .ok());
  ASSERT_TRUE(writer.stop().ok());
  int64_t actual = -1;
  ASSERT_TRUE(writer.reclaimFixedSize(1 << 30, &actual).ok());
  EXPECT_GE(actual, 0);
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
