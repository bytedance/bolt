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
#include <chrono>
#include <cstring>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/shuffle/sparksql/ShuffleReaderNode.h"
#include "bolt/shuffle/sparksql/tests/CellTestUtils.h"
#include "bolt/shuffle/sparksql/tests/ShuffleTestBase.h"

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

/// Busy-waits until `delay` has passed by the steady clock, the clock of
/// the shuffle reader's timers.
void spinFor(std::chrono::nanoseconds delay) {
  const auto end = std::chrono::steady_clock::now() + delay;
  while (std::chrono::steady_clock::now() < end) {
  }
}

/// `bytes` as an input stream every Read() of which takes `delay` at least,
/// as a call into a shuffle fetch's Java stream takes its time; `calls`
/// counts them.
class SlowInputStream final : public arrow::io::InputStream {
 public:
  SlowInputStream(
      std::string bytes,
      std::chrono::nanoseconds delay,
      uint64_t& calls)
      : bytes_(std::move(bytes)), delay_(delay), calls_(calls) {}

  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }

  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Tell() const override {
    return position_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    ++calls_;
    spinFor(delay_);
    const int64_t n = std::min<int64_t>(
        nbytes, static_cast<int64_t>(bytes_.size()) - position_);
    std::memcpy(out, bytes_.data() + position_, n);
    position_ += n;
    return n;
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(
        const int64_t n, Read(nbytes, buffer->mutable_data()));
    RETURN_NOT_OK(buffer->Resize(n));
    return std::shared_ptr<arrow::Buffer>(std::move(buffer));
  }

 private:
  const std::string bytes_;
  const std::chrono::nanoseconds delay_;
  uint64_t& calls_;
  int64_t position_{0};
  bool closed_{false};
};

/// `streams` in turn as SlowInputStreams, each also opened after `delay`;
/// keeps the metrics the reader reports.
class SlowStreamIterator final : public ReaderStreamIterator {
 public:
  SlowStreamIterator(
      std::vector<std::string> streams,
      std::chrono::nanoseconds delay)
      : streams_(std::move(streams)), delay_(delay) {}

  std::shared_ptr<arrow::io::InputStream> nextStream(
      arrow::MemoryPool* /*pool*/) override {
    ++calls;
    spinFor(delay_);
    if (next_ == streams_.size()) {
      return nullptr;
    }
    return std::make_shared<SlowInputStream>(streams_[next_++], delay_, calls);
  }

  void close() override {}

  void updateMetrics(
      int64_t numRows,
      int64_t /*numBatches*/,
      int64_t decompressTime,
      int64_t deserializeTime,
      int64_t /*deserializerCreateTime*/,
      int64_t /*deserializerDestroyTime*/,
      int64_t /*mergeTime*/,
      int64_t totalReadTime) override {
    rows = numRows;
    decompressNs = decompressTime;
    deserializeNs = deserializeTime;
    totalReadNs = totalReadTime;
  }

  /// nextStream() calls, and Read() calls of the streams.
  uint64_t calls{0};
  int64_t rows{-1};
  int64_t decompressNs{0};
  int64_t deserializeNs{0};
  int64_t totalReadNs{0};

 private:
  const std::vector<std::string> streams_;
  const std::chrono::nanoseconds delay_;
  size_t next_{0};
};

} // namespace

class CellIntegrationTest : public ShuffleTestBase {};

TEST_F(CellIntegrationTest, arraysFlattenUnderLimitedMemory) {
  for (const auto writerType :
       {PartitionWriterType::kLocal, PartitionWriterType::kCeleborn}) {
    SCOPED_TRACE(fmt::format("{}", writerType));
    ShuffleTestParam param{
        "hash", 4, writerType, DataTypeGroup::kComplex, 4, 1};
    param.memoryLimit = 64 << 20;
    ShuffleInputData input;
    auto arrays = makeArrayVector<int64_t>(
        4096,
        [](auto) { return 10; },
        [](auto row) { return row; },
        [](auto row) { return row % 5 == 0; });
    input.inputsPerMapper = {{makeRowVector({arrays})}};
    ShuffleRunResult result;
    executeTestWithCustomInput(param, input, &result);
    EXPECT_GT(result.metrics.flattenTime, 0);
    EXPECT_EQ(result.metrics.convertTime, 0);
  }
}

TEST_F(CellIntegrationTest, unknownOnlyUsesHeaderPayloads) {
  for (const auto writerType :
       {PartitionWriterType::kLocal, PartitionWriterType::kCeleborn}) {
    SCOPED_TRACE(fmt::format("{}", writerType));
    ShuffleTestParam param{
        "hash", 4, writerType, DataTypeGroup::kHighNulls, 4, 2};
    ShuffleInputData input;
    input.inputsPerMapper = {{makeRowVector(
        {BaseVector::createNullConstant(UNKNOWN(), 2048, pool()),
         BaseVector::createNullConstant(UNKNOWN(), 2048, pool())})}};
    input.inputsPerMapper.push_back(input.inputsPerMapper.front());
    ShuffleRunResult result;
    executeTestWithCustomInput(param, input, &result);
    EXPECT_EQ(result.metrics.totalBytesWritten, 8 * 25);
  }
}

TEST_F(CellIntegrationTest, pidOnlyUsesHeaderPayloads) {
  // Column pruning leaves exchanges with no data column (e.g. a count over
  // a repartition): Cell must carry them end to end as row counts.
  for (const auto writerType :
       {PartitionWriterType::kLocal, PartitionWriterType::kCeleborn}) {
    SCOPED_TRACE(fmt::format("{}", writerType));
    ShuffleTestParam param{
        "hash", 4, writerType, DataTypeGroup::kHighNulls, 4, 2};
    ShuffleInputData input;
    input.inputsPerMapper = {{std::make_shared<RowVector>(
        pool(), ROW({}, {}), nullptr, 2048, std::vector<VectorPtr>{})}};
    input.inputsPerMapper.push_back(input.inputsPerMapper.front());
    ShuffleRunResult result;
    // The harness verifies every partition's row count and contents.
    executeTestWithCustomInput(param, input, &result);
    EXPECT_EQ(result.metrics.totalBytesWritten, 8 * 25);
  }
}

TEST_F(CellIntegrationTest, compositeOutputSharesTheShuffle) {
  // HashAggregation composite output: aggregation-state rows - alone, or
  // alternating with ordinary rows - go through the cell shuffle and come
  // back as CompositeRowVectors and ordinary batches with every row intact.
  for (const auto writerType :
       {PartitionWriterType::kLocal, PartitionWriterType::kCeleborn}) {
    for (const bool mixed : {false, true}) {
      SCOPED_TRACE(
          fmt::format("{}{}", writerType, mixed ? ", mixed" : ", composite"));
      ShuffleTestParam param{
          "hash", 4, writerType, DataTypeGroup::kString, 4, 2};
      const auto batch = [&](int32_t base) {
        return makeRowVector(
            {"c0", "c1"},
            {makeFlatVector<int64_t>(
                 3000, [base](auto row) { return base + row; }),
             makeFlatVector<std::string>(3000, [base](auto row) {
               return std::string((base + row) % 41, 'a' + row % 26);
             })});
      };
      ShuffleInputData input;
      for (int32_t mapper = 0; mapper < 2; ++mapper) {
        const int32_t base = mapper * 100'000;
        std::vector<RowVectorPtr> batches{
            createCompositeInput(batch(base), param.rowFormat)};
        if (mixed) {
          batches.push_back(batch(base + 10'000));
        }
        batches.push_back(
            createCompositeInput(batch(base + 20'000), param.rowFormat));
        if (mixed) {
          batches.push_back(batch(base + 30'000));
        }
        input.inputsPerMapper.push_back(std::move(batches));
      }
      ShuffleRunResult result;
      // The harness checks every partition's rows, composite ones decoded.
      executeTestWithCustomInput(param, input, &result);
      EXPECT_GT(result.metrics.convertTime, 0);
    }
  }
}

TEST_F(CellIntegrationTest, readerLeavesStreamReadsOutOfDeserializeTime) {
  // A Spark task reads its shuffle input through calls into the fetch's
  // Java streams, which the legacy reader counts in the total read time
  // alone. So does the Cell reader: the deserialize time it reports is its
  // decode time less decompression and the reads. With streams taking
  // their time on every call, the reads, decompression and deserialization
  // fit in the total read time together.
  constexpr int32_t kPartitions = 2;
  constexpr vector_size_t kBatchRows = 1000;
  constexpr std::chrono::microseconds kDelay{500};
  std::vector<RowVectorPtr> batches;
  for (int64_t batch = 0; batch < 3; ++batch) {
    batches.push_back(makeRowVector(
        {makeFlatVector<int64_t>(
             kBatchRows,
             [&](auto row) { return (batch * kBatchRows + row) * 7919; }),
         makeFlatVector<std::string>(kBatchRows, [](auto row) {
           return fmt::format("value {}", row % 97);
         })}));
  }
  const auto type = asRowType(batches[0]->type());
  const auto input = cell::test::roundRobin(batches, kPartitions);
  const auto dir = exec::test::TempDirectoryPath::create();
  auto writerOptions = cell::test::shuffleOptions(
      dir->path + "/data",
      kPartitions,
      arrow::Compression::LZ4_FRAME,
      /*rss=*/false);
  // A payload of each partition per batch.
  writerOptions.cellOptions.partitionSealBytes = 1;
  const auto written = cell::test::writeShuffle(writerOptions, input, pool());

  const auto streams =
      std::make_shared<SlowStreamIterator>(written.partitions, kDelay);
  ShuffleReaderOptions readerOptions;
  readerOptions.numPartitions = kPartitions;
  readerOptions.partitionShortName = "hash";
  readerOptions.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::Cell);
  exec::test::CursorParameters params;
  params.planNode = std::make_shared<SparkShuffleReaderNode>(
      "reader", type, readerOptions, streams);
  params.serialExecution = true;
  int64_t numRows = 0;
  {
    auto cursor = exec::test::TaskCursor::create(params);
    while (cursor->moveNext()) {
      numRows += cursor->current()->size();
    }
  }
  EXPECT_EQ(numRows, 3 * kBatchRows);
  EXPECT_EQ(streams->rows, 3 * kBatchRows);
  // Each call took kDelay at least, all of it read time.
  const auto readNs = static_cast<int64_t>(streams->calls) *
      std::chrono::nanoseconds(kDelay).count();
  EXPECT_LE(
      streams->deserializeNs + streams->decompressNs + readNs,
      streams->totalReadNs);
}

TEST_F(CellIntegrationTest, legacyReaderRejectsCellPayload) {
  // ShuffleBlockKind starts at 0x80 so a legacy V1/V2 reader fed Cell output
  // fails immediately instead of misinterpreting block bytes.
  constexpr int32_t kPartitions = 1;
  auto batch = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3, 4}),
       makeFlatVector<std::string>({"a", "b", "c", "d"})});
  const auto type = asRowType(batch->type());
  const auto input = cell::test::roundRobin({batch}, kPartitions);
  const auto dir = exec::test::TempDirectoryPath::create();
  auto writerOptions = cell::test::shuffleOptions(
      dir->path + "/data",
      kPartitions,
      arrow::Compression::LZ4_FRAME,
      /*rss=*/false);
  const auto written = cell::test::writeShuffle(writerOptions, input, pool());

  const auto streams = std::make_shared<SlowStreamIterator>(
      written.partitions, std::chrono::microseconds{0});
  ShuffleReaderOptions readerOptions;
  readerOptions.numPartitions = kPartitions;
  readerOptions.partitionShortName = "hash";
  readerOptions.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::V2);
  exec::test::CursorParameters params;
  params.planNode = std::make_shared<SparkShuffleReaderNode>(
      "reader", type, readerOptions, streams);
  params.serialExecution = true;
  auto cursor = exec::test::TaskCursor::create(params);
  BOLT_ASSERT_THROW(
      cursor->moveNext(),
      fmt::format(
          "unexpected payload type {}",
          static_cast<int>(ShuffleBlockKind::kCellPayload)));
}

TEST_F(
    CellIntegrationTest,
    optionsConstructorReadsFlatAndNestedWithAndWithoutCodec) {
  constexpr int32_t kPartitions = 3;
  constexpr vector_size_t kRows = 512;
  auto batch = makeRowVector(
      {"id", "s", "arr", "mp", "st"},
      {makeFlatVector<int64_t>(kRows, [](auto row) { return row * 13; }),
       makeFlatVector<std::string>(
           kRows, [](auto row) { return fmt::format("val-{}", row % 17); }),
       makeArrayVector<int32_t>(
           kRows,
           [](auto row) { return row % 4; },
           [](auto idx) { return static_cast<int32_t>(idx); }),
       makeMapVector<int32_t, std::string>(
           kRows,
           [](auto row) { return row % 3; },
           [](auto idx) { return static_cast<int32_t>(idx); },
           [](auto idx) { return fmt::format("m-{}", idx % 5); }),
       makeRowVector(
           {"a", "b"},
           {makeFlatVector<int32_t>(kRows, [](auto row) { return row; }),
            makeFlatVector<double>(
                kRows, [](auto row) { return row * 0.5; })})});
  const auto type = asRowType(batch->type());
  const auto input = cell::test::roundRobin({batch}, kPartitions);
  const auto expectedRows = input.rowsOf(kPartitions);

  for (const auto compression :
       {arrow::Compression::UNCOMPRESSED,
        arrow::Compression::LZ4_FRAME,
        arrow::Compression::ZSTD}) {
    for (const bool nativeNested : {true, false}) {
      for (const bool rss : {false, true}) {
        SCOPED_TRACE(fmt::format(
            "{} nativeNested={} rss={}",
            cell::test::codecName(compression),
            nativeNested,
            rss));
        const auto dir = exec::test::TempDirectoryPath::create();
        auto writerOptions = cell::test::shuffleOptions(
            dir->path + "/data", kPartitions, compression, rss);
        writerOptions.cellOptions.nativeNestedTypes = nativeNested;
        writerOptions.cellOptions.adaptiveCompression = false;
        const auto written =
            cell::test::writeShuffle(writerOptions, input, pool());

        ShuffleReaderOptions readerOptions;
        readerOptions.compressionType = compression;
        readerOptions.batchSize = 128;
        readerOptions.shuffleBatchByteSize = 1 << 20;
        readerOptions.cellNativeNestedTypes = nativeNested;
        readerOptions.forceShuffleWriterType =
            static_cast<int32_t>(ShuffleWriterType::Cell);

        for (int32_t pid = 0; pid < kPartitions; ++pid) {
          auto streams = std::make_shared<SlowStreamIterator>(
              cell::test::partitionStreams(written, pid),
              std::chrono::microseconds{0});
          cell::CellShuffleReader reader(
              streams,
              type,
              readerOptions,
              arrow::default_memory_pool(),
              pool());
          std::vector<RowVectorPtr> decoded;
          while (auto out = reader.next()) {
            out->validate({});
            decoded.push_back(std::move(out));
          }
          cell::test::expectPartitionRows(
              expectedRows[pid], decoded, pid, readerOptions.batchSize);

          const auto m = reader.metrics();
          EXPECT_EQ(m.rowsRead, expectedRows[pid].size());
          EXPECT_EQ(m.rowsRead, reader.rowsRead());
          EXPECT_GT(m.payloadsRead, 0);
          EXPECT_EQ(m.payloadsRead, reader.payloadsRead());
          EXPECT_GT(m.streamsRead, 0);
          EXPECT_EQ(m.streamsRead, reader.streamsRead());
          EXPECT_EQ(m.decodeTimeNs, reader.decodeTimeNs());
          EXPECT_EQ(m.decompressTimeNs, reader.decompressTimeNs());
          EXPECT_EQ(m.readTimeNs, reader.readTimeNs());
          EXPECT_EQ(m.deserializeTimeNs, reader.deserializeTimeNs());
          if (compression == arrow::Compression::UNCOMPRESSED) {
            EXPECT_EQ(m.decompressTimeNs, 0);
          } else {
            EXPECT_GT(m.decompressTimeNs, 0);
          }
          EXPECT_LE(
              m.decompressTimeNs + m.readTimeNs + m.deserializeTimeNs,
              m.decodeTimeNs);
        }
      }
    }
  }
}

TEST_F(
    CellIntegrationTest,
    optionsConstructorRejectsBadOptionsAndCodecMismatch) {
  auto batch = makeRowVector({makeFlatVector<std::string>(4096, [](auto row) {
    return fmt::format("compressible-value-{:04d}", row % 16);
  })});
  const auto type = asRowType(batch->type());
  const auto input = cell::test::roundRobin({batch}, /*numPartitions=*/1);
  const auto dir = exec::test::TempDirectoryPath::create();
  auto writerOptions = cell::test::shuffleOptions(
      dir->path + "/data",
      /*numPartitions=*/1,
      arrow::Compression::LZ4_FRAME,
      /*rss=*/false);
  writerOptions.cellOptions.adaptiveCompression = false;
  const auto written = cell::test::writeShuffle(writerOptions, input, pool());

  {
    ShuffleReaderOptions badOptions;
    badOptions.compressionType = arrow::Compression::UNCOMPRESSED;
    badOptions.shuffleBatchByteSize = 0;
    auto streams = std::make_shared<SlowStreamIterator>(
        written.partitions, std::chrono::microseconds{0});
    BOLT_ASSERT_THROW(
        cell::CellShuffleReader(
            streams, type, badOptions, arrow::default_memory_pool(), pool()),
        "maxBatchBytes must be positive");
  }

  {
    ShuffleReaderOptions uncompressedOptions;
    uncompressedOptions.compressionType = arrow::Compression::UNCOMPRESSED;
    auto streams = std::make_shared<SlowStreamIterator>(
        written.partitions, std::chrono::microseconds{0});
    cell::CellShuffleReader reader(
        streams,
        type,
        uncompressedOptions,
        arrow::default_memory_pool(),
        pool());
    BOLT_ASSERT_THROW(reader.next(), "compressed buffer without a codec");
  }
}

} // namespace bytedance::bolt::shuffle::sparksql::test
