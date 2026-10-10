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

#include <cstring>
#include <fstream>
#include <optional>
#include <random>
#include <stdexcept>
#include <type_traits>

#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleReader.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleWriter.h"
#include "bolt/shuffle/sparksql/cell/CellSplitter.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "bolt/shuffle/sparksql/tests/MemoryReaderStreamIterator.h"
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

/// Observes synchronous output calls and can simulate reclaim or failure.
class RecordingCellOutput final : public CellOutput {
 public:
  void spillRun(const CellWindowInput& in) override {
    ++spillCalls;
    if (onSpill) {
      onSpill(in);
    }
  }

  void sealWindow(const CellWindowInput& in) override {
    ++sealCalls;
    if (onSeal) {
      onSeal(in);
    }
  }

  void finalize(
      const CellWindowInput& in,
      bool windowHasData,
      ShuffleWriterMetrics&) override {
    ++finalizeCalls;
    if (onFinalize) {
      onFinalize(in, windowHasData);
    }
  }

  int64_t bytesEvicted() const override {
    return 0;
  }

  uint32_t spillCalls{0};
  uint32_t sealCalls{0};
  uint32_t finalizeCalls{0};
  std::function<void(const CellWindowInput&)> onSpill;
  std::function<void(const CellWindowInput&)> onSeal;
  std::function<void(const CellWindowInput&, bool)> onFinalize;
};

class CellWriterTest : public testing::Test, public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

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

  /// Builds the borrowed decoded batch for direct splitter lifecycle tests.
  void splitOnePartition(CellSplitter& splitter, const RowVectorPtr& rows) {
    const uint32_t numRows = rows->size();
    std::vector<DecodedVector> decoded(rows->childrenSize());
    std::vector<BatchNullClass> nullClass(decoded.size());
    for (uint32_t col = 0; col < decoded.size(); ++col) {
      decoded[col].decode(*rows->childAt(col));
      uint32_t nullCount = 0;
      for (uint32_t row = 0; row < numRows; ++row) {
        nullCount += decoded[col].isNullAt(row);
      }
      nullClass[col] = nullCount == 0 ? BatchNullClass::kNoNulls
          : nullCount == numRows      ? BatchNullClass::kAllNull
                                      : BatchNullClass::kSomeNulls;
    }
    std::vector<uint32_t> pids(numRows, 0);
    std::vector<uint32_t> rowIndex(numRows);
    for (uint32_t row = 0; row < numRows; ++row) {
      rowIndex[row] = row;
    }
    SplitBatch batch{
        decoded.data(),
        pids.data(),
        &numRows,
        nullClass.data(),
        numRows,
        rowIndex.data()};
    splitter.split(batch);
  }

  RowVectorPtr decodeComplex(
      const CellShuffleTypeAdapter& adapter,
      const RowVectorPtr& input) {
    vector_size_t offset = 0;
    auto result = adapter.decodeNext(input, offset, pool());
    EXPECT_EQ(offset, input->size());
    return result;
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
    // Compressed runs use LZ4; stored runs bypass the decompressor.
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

  /// The run count of every payload in one partition's byte range.
  std::vector<uint32_t> payloadRunCounts(
      const CellLayout& layout,
      const std::string& file,
      uint64_t offset,
      uint64_t length) {
    std::vector<uint32_t> runCounts;
    const uint64_t end = offset + length;
    const uint64_t tagBytes = (layout.numWireColumns() + 7) / 8;
    const uint64_t runHeaderBytes = 1 + 8 + 8ull * layout.numStreams();
    while (offset < end) {
      uint32_t runCount;
      uint32_t nullStored;
      ::memcpy(&runCount, file.data() + offset + 4, sizeof(runCount));
      ::memcpy(&nullStored, file.data() + offset + 16, sizeof(nullStored));
      offset += 24 + nullStored + tagBytes;
      for (uint32_t run = 0; run < runCount; ++run) {
        uint64_t stored;
        ::memcpy(&stored, file.data() + offset + 1, sizeof(stored));
        offset += runHeaderBytes + stored;
      }
      runCounts.push_back(runCount);
    }
    EXPECT_EQ(offset, end);
    return runCounts;
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
      const auto payloads =
          decodePartition(layout, file, offset, metrics.partitionLengths[pid]);
      size_t k = 0;
      for (const auto& decoded : payloads) {
        for (vector_size_t row = 0; row < decoded->size(); ++row, ++k) {
          ASSERT_LT(k, expected[pid].size()) << "partition " << pid;
          const auto [batchIdx, sourceRow] = expected[pid][k];
          EXPECT_TRUE(
              batches[batchIdx]->equalValueAt(decoded.get(), sourceRow, row))
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

TEST_F(CellWriterTest, schemaLessFactoryWritesCellPayloads) {
  constexpr int32_t kRows = 64;
  auto options = makeOptions(2);
  options.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::Cell);
  const auto rows = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto row) { return row * 7; })});
  auto writer = BoltShuffleWriter::createShuffleWriter(
      options,
      rows->childrenSize(),
      rows->size(),
      rows->estimateFlatSize(),
      1 << 20,
      pool(),
      arrow::default_memory_pool());
  auto* cellWriter = dynamic_cast<CellShuffleWriter*>(writer.get());
  ASSERT_NE(cellWriter, nullptr);
  std::vector<int32_t> pids(kRows);
  for (int32_t row = 0; row < kRows; ++row) {
    pids[row] = row % 2;
  }
  // Decode the factory-selected writer's output with the Cell payload reader.
  roundTrip(options, {pids}, {rows}, cellWriter);
}

TEST_F(CellWriterTest, legacyCreateKeepsItsInterfaceAndRejectsCell) {
  // The default factory keeps its original signature and return type, so
  // integrations holding a shared_ptr<BoltShuffleWriter> keep compiling.
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
  auto options = makeOptions(2);
  options.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::Cell);
  EXPECT_THROW(
      BoltShuffleWriter::create(
          options, 1, 1, 0, 1 << 20, pool(), arrow::default_memory_pool()),
      BoltException);
}

TEST_F(CellWriterTest, pidOnlyRoundTripsAsHeaderOnlyPayloads) {
  constexpr vector_size_t kRows = 101;
  auto options = makeOptions(3);
  options.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::Cell);
  auto writer = BoltShuffleWriter::createShuffleWriter(
      options, 0, kRows, 0, 1 << 20, pool(), arrow::default_memory_pool());
  auto* cellWriter = dynamic_cast<CellShuffleWriter*>(writer.get());
  ASSERT_NE(cellWriter, nullptr);
  auto data = std::make_shared<RowVector>(
      pool(), ROW({}, {}), nullptr, kRows, std::vector<VectorPtr>{});
  std::vector<int32_t> pids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    pids[row] = row % 2; // partition 2 stays empty
  }
  roundTrip(options, {pids}, {data}, cellWriter);
  // One 24-byte header per non-empty partition, carrying the row count.
  EXPECT_EQ(
      cellWriter->metrics().partitionLengths,
      (std::vector<int64_t>{24, 24, 0}));
  const auto bytes = readFile();
  EXPECT_EQ(static_cast<uint8_t>(bytes[0]), 51);
  EXPECT_EQ(static_cast<uint8_t>(bytes[24]), 50);
}

TEST_F(CellWriterTest, timestampKeepsAllSixteenBytes) {
  constexpr int32_t n = 1500;
  const std::vector<Timestamp> extremes = {
      Timestamp(Timestamp::kMinSeconds, 0),
      Timestamp(Timestamp::kMaxSeconds, Timestamp::kMaxNanos),
      Timestamp(-1, 1),
      Timestamp(0, 999'999'999)};
  auto timestamps = makeFlatVector<Timestamp>(
      n,
      [&](auto row) {
        return row < extremes.size()
            ? extremes[row]
            // Nanosecond precision that a microsecond encoding would lose.
            : Timestamp(1'700'000'000 + row * 37, (row * 7919) % 1'000'000'000);
      },
      [](auto row) { return row % 11 == 5; });
  auto data = makeRowVector({timestamps});
  std::vector<int32_t> pids(n);
  for (int32_t row = 0; row < n; ++row) {
    pids[row] = row % 4;
  }
  auto options = makeOptions(4);
  options.cellOptions.maxWindowRows = 256;
  for (const auto& input :
       {data,
        makeRowVector({BaseVector::wrapInDictionary(
            nullptr,
            makeIndices(n, [](auto row) { return n - row - 1; }),
            n,
            timestamps)}),
        makeRowVector({wrapInLazyDictionary(timestamps)}),
        makeRowVector({BaseVector::wrapInConstant(n, 1, timestamps)})}) {
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, {pids}, {input}, &writer);
  }

  // Spilled, merged and all-null batches: both streams stay in lockstep
  // across Run boundaries and window seals. High-entropy values keep the
  // encoded streams large enough to cross the chunk cap several times.
  constexpr vector_size_t kBig = 250'000;
  std::mt19937_64 rng(97);
  std::vector<RowVectorPtr> batches;
  std::vector<std::vector<int32_t>> bigPids;
  for (int b = 0; b < 4; ++b) {
    if (b == 1) {
      batches.push_back(makeRowVector(
          {BaseVector::createNullConstant(TIMESTAMP(), kBig, pool())}));
    } else {
      batches.push_back(makeRowVector({makeFlatVector<Timestamp>(
          kBig,
          [&](auto) {
            return Timestamp(
                static_cast<int64_t>(rng() % 4'000'000'000'000) -
                    2'000'000'000'000,
                rng() % 1'000'000'000);
          },
          [](auto row) { return row % 13 == 3; })}));
    }
    std::vector<int32_t> batchPids(kBig);
    for (auto& pid : batchPids) {
      pid = rng() % 4;
    }
    bigPids.push_back(std::move(batchPids));
  }
  auto spillOptions = makeOptions(4);
  // One chunk of cells at most: every further growth spills a Run first.
  spillOptions.cellOptions.cellMemoryCapBytes = 4 << 20;
  spillOptions.cellOptions.checkpointPartitionBytes = 1 << 20;
  CellShuffleWriter spilling(
      spillOptions, pool(), arrow::default_memory_pool());
  roundTrip(spillOptions, bigPids, batches, &spilling);
  EXPECT_GT(spilling.metrics().totalBytesEvicted, 0);
  EXPECT_GT(spilling.metrics().spillCount, 0);
}

TEST_F(CellWriterTest, rejectsOutOfRangeTimestamp) {
  const auto layout = CellLayout::create(ROW({TIMESTAMP()}));
  const auto payload = [](int64_t seconds, int64_t nanos) {
    std::string secondsStream;
    std::string nanosStream;
    encodeStream<int64_t>(&seconds, 1, secondsStream);
    encodeStream<int64_t>(&nanos, 1, nanosStream);
    std::string bytes(24, '\0');
    bytes[0] = 1; // row_count
    bytes[4] = 1; // run_count
    bytes[16] = 1; // null_stored_size: one tags byte
    bytes.push_back(0x01); // NO_NULL
    bytes.push_back(0x00); // encoding tags
    bytes.push_back(static_cast<char>(RunLayout::kCombinedStored));
    const auto appendU64 = [&](uint64_t value) {
      bytes.append(reinterpret_cast<const char*>(&value), 8);
    };
    appendU64(secondsStream.size() + nanosStream.size());
    appendU64(secondsStream.size());
    appendU64(nanosStream.size());
    return bytes + secondsStream + nanosStream;
  };
  CellPayloadDecoder decoder(layout, nullptr, pool());
  const auto decode = [&](const std::string& bytes, RowVectorPtr& output) {
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    std::string error;
    return decoder.decode(source, output, error);
  };
  RowVectorPtr output;
  ASSERT_TRUE(decode(payload(-5, 999'999'999), output));
  EXPECT_EQ(
      output->childAt(0)->as<FlatVector<Timestamp>>()->valueAt(0),
      Timestamp(-5, 999'999'999));
  EXPECT_FALSE(decode(payload(0, 1'000'000'000), output));
  EXPECT_FALSE(decode(payload(0, -1), output));
  EXPECT_FALSE(decode(payload(Timestamp::kMaxSeconds + 1, 0), output));
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
  auto options = makeOptions(1);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  ASSERT_TRUE(
      writer.split(withPid(std::vector<int32_t>(kRows, 0), lazy), 0).ok());
  EXPECT_GE(writer.metrics().totalInputBatches, 2);
  ASSERT_TRUE(writer.stop().ok());
}

TEST_F(CellWriterTest, variantSchemasFailWithoutChangingWriterType) {
  auto options = makeOptions(2);
  options.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::Cell);
  const std::vector<TypePtr> unsupported = {
      VARIANT(), ARRAY(VARIANT()), ROW({"nested"}, {VARIANT()})};
  for (const auto& type : unsupported) {
    SCOPED_TRACE(type->toString());
    const auto logicalType = ROW({"pid", "v"}, {INTEGER(), type});
    // The operator's logical-to-physical boundary must reject nested VARIANT
    // as well as a top-level VARIANT, before converting complex values.
    EXPECT_THROW(CellShuffleTypeAdapter{logicalType}, BoltException);

    auto writer = BoltShuffleWriter::createShuffleWriter(
        options, 1, 1, 0, 1 << 20, pool(), arrow::default_memory_pool());
    ASSERT_NE(dynamic_cast<CellShuffleWriter*>(writer.get()), nullptr);
    const auto rows = makeRowVector({BaseVector::create(type, 1, pool())});
    // The writer's own adapter rejects it instead of silently producing a
    // different payload format.
    EXPECT_THROW(writer->split(withPid({0}, rows), 0), BoltException);
  }
}

TEST_F(CellWriterTest, splitterCanSpillWhileFlushingCacheTails) {
  const auto rows = makeRowVector(
      {makeFlatVector<double>({12.5}), makeFlatVector<double>({6.25})});
  const auto layout = CellLayout::create(asRowType(rows->type()));
  for (const bool seal : {true, false}) {
    SCOPED_TRACE(seal ? "seal" : "finish");
    CellSplitter* activeSplitter = nullptr;
    bool inGrowCallback = false;
    uint32_t spillsFromGrow = 0;
    // Two cells fit a chunk. The second column's tail needs another cell
    // while append holds one unlinked, so reclaim must drain the first tail.
    ChunkAllocator allocator(pool(), 16 << 10, 8 << 10, [&]() {
      ASSERT_NE(activeSplitter, nullptr);
      EXPECT_FALSE(activeSplitter->isWritingOutput());
      inGrowCallback = true;
      activeSplitter->spillRun();
      inGrowCallback = false;
    });
    RecordingCellOutput output;
    CellSplitter splitter(layout, 1, allocator, output, *pool());
    activeSplitter = &splitter;
    std::vector<std::string> streams(layout.numStreams());
    const auto collect = [&](const CellWindowInput& in) {
      EXPECT_TRUE(splitter.isWritingOutput());
      for (uint32_t stream = 0; stream < streams.size(); ++stream) {
        in.cells->scan(0, stream, [&](const char* data, uint32_t bytes) {
          streams[stream].append(data, bytes);
        });
      }
    };
    output.onSpill = [&](const CellWindowInput& in) {
      spillsFromGrow += inGrowCallback;
      collect(in);
    };
    output.onSeal = [&](const CellWindowInput& in) {
      EXPECT_EQ(in.rowCounts[0], 1);
      EXPECT_EQ(in.cells->totalBytes(), 0);
    };
    output.onFinalize = [&](const CellWindowInput& in, bool windowHasData) {
      EXPECT_TRUE(windowHasData);
      EXPECT_EQ(in.rowCounts[0], 1);
      collect(in);
    };
    splitOnePartition(splitter, rows);
    EXPECT_EQ(output.spillCalls, 0);
    if (seal) {
      splitter.sealWindow();
    } else {
      ShuffleWriterMetrics metrics;
      splitter.finish(metrics);
    }
    EXPECT_GT(spillsFromGrow, 0);
    EXPECT_FALSE(splitter.isWritingOutput());
    EXPECT_EQ(output.sealCalls, seal ? 1 : 0);
    EXPECT_EQ(output.finalizeCalls, seal ? 0 : 1);
    for (uint32_t stream = 0; stream < streams.size(); ++stream) {
      ASSERT_EQ(streams[stream].size(), sizeof(double));
      double value;
      ::memcpy(&value, streams[stream].data(), sizeof(value));
      EXPECT_EQ(value, stream == 0 ? 12.5 : 6.25);
    }
  }
}

TEST_F(CellWriterTest, splitterOutputGuardRecoversAfterFailure) {
  const auto rows = makeRowVector(
      {makeFlatVector<double>(8, [](auto row) { return row + 0.5; })});
  const auto layout = CellLayout::create(asRowType(rows->type()));
  ChunkAllocator allocator(pool(), 16 << 10, 256);
  RecordingCellOutput output;
  CellSplitter splitter(layout, 1, allocator, output, *pool());
  std::vector<std::string> attempts;
  output.onSpill = [&](const CellWindowInput& in) {
    EXPECT_TRUE(splitter.isWritingOutput());
    std::string bytes;
    in.cells->scan(0, 0, [&](const char* data, uint32_t size) {
      bytes.append(data, size);
    });
    attempts.push_back(std::move(bytes));
    if (output.spillCalls == 1) {
      // Simulate reclaim during output allocation, then an output failure.
      splitter.spillRun();
      EXPECT_EQ(output.spillCalls, 1);
      throw std::runtime_error("injected output failure");
    }
  };
  splitOnePartition(splitter, rows);
  EXPECT_THROW(splitter.spillRun(), std::runtime_error);
  EXPECT_FALSE(splitter.isWritingOutput());
  EXPECT_TRUE(splitter.hasWindowRows());
  EXPECT_EQ(splitter.maxWindowRows(), 8);
  splitter.spillRun();
  ASSERT_EQ(attempts.size(), 2);
  EXPECT_EQ(attempts[0].size(), 8 * sizeof(double));
  EXPECT_EQ(attempts[1], attempts[0]);
  EXPECT_FALSE(splitter.isWritingOutput());
  splitter.spillRun();
  EXPECT_EQ(output.spillCalls, 2); // successful output released the cells
}

TEST_F(CellWriterTest, splitterSealsWindowAndFinishesResidentData) {
  const auto layout = CellLayout::create(ROW({"s"}, {VARCHAR()}));
  ChunkAllocator allocator(pool(), 16 << 10, 256);
  RecordingCellOutput output;
  CellSplitter splitter(layout, 1, allocator, output, *pool());
  splitter.enableDictionary(0);
  output.onSeal = [&](const CellWindowInput& in) {
    EXPECT_EQ(in.rowCounts[0], 3);
    EXPECT_EQ(in.variableBytes[0], 81);
    EXPECT_EQ(in.encodingTags[0], 1);
    const auto summary = in.nulls->summarize(0, 0, in.rowCounts[0]);
    EXPECT_EQ(summary.tag, NullTag::kRawNull);
    EXPECT_EQ(summary.nonNullCount, 2);
    uint8_t bitmap = 0;
    in.nulls->emitBitmap(0, 0, in.rowCounts[0], &bitmap);
    EXPECT_EQ(bitmap, 0b110);
  };
  splitOnePartition(
      splitter,
      makeRowVector({makeNullableFlatVector<StringView>(
          {std::nullopt, StringView("a")})}));
  const std::string longValue(80, 'x');
  splitOnePartition(
      splitter,
      makeRowVector({makeFlatVector<StringView>({StringView(longValue)})}));
  EXPECT_EQ(splitter.maxWindowRows(), 3);
  splitter.sealWindow();
  EXPECT_EQ(output.sealCalls, 1);
  EXPECT_FALSE(splitter.hasWindowRows());
  EXPECT_EQ(splitter.maxWindowRows(), 0);
  EXPECT_EQ(splitter.maxPartitionBytes(), 0);
  EXPECT_EQ(splitter.nullBytes(), 0);

  output.onFinalize = [&](const CellWindowInput& in, bool windowHasData) {
    EXPECT_TRUE(windowHasData);
    EXPECT_EQ(in.rowCounts[0], 2);
    EXPECT_EQ(in.variableBytes[0], 2);
    EXPECT_EQ(in.encodingTags[0], 1); // dictionary choice survives the seal
    const auto summary = in.nulls->summarize(0, 0, in.rowCounts[0]);
    EXPECT_EQ(summary.tag, NullTag::kNoNull);
    EXPECT_EQ(summary.nonNullCount, 2);
    std::string dictionary;
    in.cells->scan(0, 1, [&](const char* data, uint32_t size) {
      dictionary.append(data, size);
    });
    // The previous window demoted to fallback; this one starts a fresh
    // dictionary containing only "b", followed by its terminating row count.
    ASSERT_EQ(dictionary.size(), 7);
    EXPECT_EQ(dictionary[0], 1);
    EXPECT_EQ(dictionary[1], 'b');
    EXPECT_EQ(static_cast<uint8_t>(dictionary[2]), kDictLastMarker);
    uint32_t matchedRows;
    ::memcpy(&matchedRows, dictionary.data() + 3, sizeof(matchedRows));
    EXPECT_EQ(matchedRows, 2);
  };
  splitOnePartition(
      splitter,
      makeRowVector(
          {makeFlatVector<StringView>({StringView("b"), StringView("b")})}));
  const auto spillsBeforeFinish = output.spillCalls;
  const auto sealsBeforeFinish = output.sealCalls;
  ShuffleWriterMetrics metrics;
  splitter.finish(metrics);
  EXPECT_EQ(output.finalizeCalls, 1);
  EXPECT_EQ(output.spillCalls, spillsBeforeFinish);
  EXPECT_EQ(output.sealCalls, sealsBeforeFinish);
  EXPECT_EQ(metrics.dictionaryMatchedRows, 3);
  EXPECT_EQ(metrics.dictionaryFallbackRows, 1);
}

TEST_F(CellWriterTest, fullPrimitiveTypesAndUnknownWireSlots) {
  constexpr int32_t n = 2057;
  std::vector<VectorPtr> children;
  for (int i = 0; i < 10; ++i) {
    children.push_back(BaseVector::createNullConstant(UNKNOWN(), n, pool()));
    children.push_back(makeFlatVector<bool>(
        n,
        [i](auto row) { return (row + i) % 2; },
        [](auto row) { return row % 5 == 0; }));
  }
  children.push_back(makeFlatVector<Timestamp>(
      n,
      [](auto row) {
        return Timestamp::fromMicros((int64_t(row) - 128) * 1000001);
      },
      [](auto row) { return row % 7 == 0; }));
  children.push_back(makeFlatVector<int128_t>(
      n,
      [](auto row) { return (int128_t(row) - 128) * (int128_t(1) << 90); },
      [](auto row) { return row % 9 == 0; },
      DECIMAL(38, 4)));
  children.push_back(BaseVector::createNullConstant(UNKNOWN(), n, pool()));
  children.push_back(makeFlatVector<StringView>(
      n, [](auto) { return StringView("dictionary"); }));
  auto data = makeRowVector(children);
  auto options = makeOptions(4);
  options.cellOptions.maxWindowRows = 1024;
  std::vector<int32_t> pids(n);
  for (int i = 0; i < n; ++i) {
    pids[i] = i % 3;
  }
  const auto roundTripWithDictionary = [&](const RowVectorPtr& input) {
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, {pids}, {input}, &writer);
    EXPECT_GT(writer.metrics().dictionaryMatchedRows, 0);
  };
  roundTripWithDictionary(data);
  std::vector<VectorPtr> wrapped;
  auto indices = makeIndices(n, [](auto row) { return n - row - 1; });
  for (const auto& child : children) {
    wrapped.push_back(BaseVector::wrapInDictionary(nullptr, indices, n, child));
  }
  roundTripWithDictionary(makeRowVector(wrapped));
  wrapped.clear();
  for (const auto& child : children) {
    wrapped.push_back(wrapInLazyDictionary(child));
  }
  roundTripWithDictionary(makeRowVector(wrapped));
  wrapped.clear();
  for (const auto& child : children) {
    wrapped.push_back(BaseVector::wrapInConstant(n, 1, child));
  }
  roundTripWithDictionary(makeRowVector(wrapped));
}

TEST_F(CellWriterTest, unknownOnlyHeaderAndRowBoundaries) {
  auto data = makeRowVector(
      {BaseVector::createNullConstant(UNKNOWN(), 99, pool()),
       BaseVector::createNullConstant(UNKNOWN(), 99, pool())});
  auto options = makeOptions(3);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  roundTrip(options, {std::vector<int32_t>(99, 1)}, {data}, &writer);
  ASSERT_EQ(
      writer.metrics().partitionLengths, (std::vector<int64_t>{0, 24, 0}));
  auto bytes = readFile();
  EXPECT_EQ(static_cast<uint8_t>(bytes[0]), 99);
  EXPECT_EQ(bytes.substr(1), std::string(23, '\0'));

  options.cellOptions.maxWindowRows = 10;
  CellShuffleWriter bounded(options, pool(), arrow::default_memory_pool());
  roundTrip(options, {std::vector<int32_t>(99, 1)}, {data}, &bounded);
  EXPECT_EQ(bounded.metrics().partitionLengths[1], 10 * 24);

  auto layout = CellLayout::create(asRowType(data->type()));
  CellPayloadDecoder decoder(layout, nullptr, pool());
  for (const auto index : {4, 8, 16, 20}) {
    auto invalid = bytes;
    invalid[index] = 1;
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(invalid.data()), invalid.size());
    RowVectorPtr output;
    std::string error;
    EXPECT_FALSE(decoder.decode(source, output, error)) << index;
  }
}

TEST_F(CellWriterTest, groupedComplexAdapterAndCellRoundTrip) {
  auto arrays = makeArrayVector<int64_t>({{1, 2}, {}, {3}, {4, 5}});
  arrays->setNull(2, true);
  auto maps =
      makeMapVector<int64_t, int64_t>({{{1, 2}}, {}, {{3, 4}}, {{5, 6}}});
  maps->setNull(2, true);
  auto nested = makeRowVector({arrays, maps});
  nested->setNull(1, true);
  auto nativeBinary = makeFlatVector<StringView>(
      {StringView("ab"), StringView(""), StringView("cd"), StringView("ef")},
      VARBINARY());
  auto logical = makeRowVector(
      {arrays,
       nativeBinary,
       BaseVector::createNullConstant(UNKNOWN(), 4, pool()),
       maps,
       nested});
  CellShuffleTypeAdapter adapter(asRowType(logical->type()));
  ASSERT_EQ(adapter.physicalType()->size(), 3);
  EXPECT_EQ(adapter.physicalType()->childAt(0)->kind(), TypeKind::VARBINARY);
  EXPECT_EQ(adapter.physicalType()->childAt(1)->kind(), TypeKind::UNKNOWN);
  EXPECT_EQ(adapter.physicalType()->childAt(2)->kind(), TypeKind::VARBINARY);
  vector_size_t offset = 0;
  auto physical = adapter.encodeNext(logical, offset, pool());
  ASSERT_EQ(offset, 4);
  auto binary = physical->childAt(2)->as<FlatVector<StringView>>();
  for (int i = 0; i < 4; ++i) {
    EXPECT_FALSE(binary->isNullAt(i));
  }
  bolt::test::assertEqualVectors(logical, decodeComplex(adapter, physical));
  auto options = makeOptions(1);
  options.cellOptions.maxWindowRows = 2;
  roundTrip(options, {{0, 0, 0, 0}}, {physical});
  auto payloads = decodePartition(
      CellLayout::create(adapter.physicalType()),
      readFile(),
      0,
      readFile().size());
  vector_size_t at = 0;
  for (const auto& payload : payloads) {
    auto restored = decodeComplex(adapter, payload);
    bolt::test::assertEqualVectors(
        logical->slice(at, restored->size()), restored);
    at += restored->size();
  }
  ASSERT_EQ(at, 4);

  // CompactRow rejects a truncated binary.
  auto malformed =
      makeFlatVector<StringView>({StringView("\0", 1)}, VARBINARY());
  auto bad = std::make_shared<RowVector>(
      pool(),
      adapter.physicalType(),
      nullptr,
      1,
      std::vector<VectorPtr>{
          nativeBinary->slice(0, 1),
          BaseVector::createNullConstant(UNKNOWN(), 1, pool()),
          malformed});
  EXPECT_THROW(decodeComplex(adapter, bad), BoltException);
  malformed->setNull(0, true);
  EXPECT_THROW(decodeComplex(adapter, bad), BoltException);
}

TEST_F(CellWriterTest, complexAdapterSlicesAndSimpleFastPath) {
  auto simple = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  CellShuffleTypeAdapter identity(asRowType(simple->type()));
  vector_size_t offset = 0;
  EXPECT_EQ(identity.encodeNext(simple, offset, pool()).get(), simple.get());
  EXPECT_EQ(decodeComplex(identity, simple).get(), simple.get());
  auto arrays = makeArrayVector<int64_t>(
      2049, [](auto) { return 2; }, [](auto i) { return i; });
  auto input = makeRowVector({arrays});
  CellShuffleTypeAdapter adapter(asRowType(input->type()));
  offset = 0;
  int batches = 0;
  while (offset < input->size()) {
    const auto start = offset;
    auto physical = adapter.encodeNext(input, offset, pool());
    EXPECT_LE(physical->size(), 1024);
    bolt::test::assertEqualVectors(
        input->slice(start, offset - start), decodeComplex(adapter, physical));
    ++batches;
  }
  EXPECT_EQ(batches, 3);
}

TEST_F(CellWriterTest, nestedComplexEncodingsAndMalformedInput) {
  using Inner = std::vector<std::optional<int64_t>>;
  using Outer = std::vector<std::optional<Inner>>;
  auto arrays = makeNullableNestedArrayVector<int64_t>(
      {Outer{Inner{1, std::nullopt, 3}, std::nullopt, Inner{}},
       std::nullopt,
       Outer{Inner{4}, Inner{5, 6}}});
  auto nested = makeRowVector(
      {BaseVector::createNullConstant(UNKNOWN(), 3, pool()), arrays});
  auto logical = makeRowVector({arrays, nested});
  CellShuffleTypeAdapter adapter(asRowType(logical->type()));
  for (const bool lazy : {false, true}) {
    std::vector<VectorPtr> children;
    for (const auto& child : logical->children()) {
      children.push_back(lazy ? wrapInLazyDictionary(child) : child);
    }
    auto input = makeRowVector(children);
    vector_size_t offset = 0;
    auto physical = adapter.encodeNext(input, offset, pool());
    bolt::test::assertEqualVectors(logical, decodeComplex(adapter, physical));
    auto binary = physical->childAt(0)->as<FlatVector<StringView>>();
    auto value = binary->valueAt(0).str();
    // Match CompactRow's acceptance of trailing bytes.
    value.push_back('\0');
    binary->set(0, StringView(value));
    bolt::test::assertEqualVectors(logical, decodeComplex(adapter, physical));
    value.resize(value.size() - 2);
    binary->set(0, StringView(value));
    vector_size_t decodeOffset = 0;
    EXPECT_THROW(
        adapter.decodeNext(physical, decodeOffset, pool()), BoltException);
    EXPECT_EQ(decodeOffset, 0);
  }
  auto nullInput = makeRowVector(
      {BaseVector::createNullConstant(arrays->type(), 3, pool()),
       BaseVector::createNullConstant(nested->type(), 3, pool())});
  vector_size_t offset = 0;
  auto physical = adapter.encodeNext(nullInput, offset, pool());
  EXPECT_FALSE(physical->childAt(0)->isNullAt(0));
  bolt::test::assertEqualVectors(nullInput, decodeComplex(adapter, physical));
}

TEST_F(CellWriterTest, complexByteBounds) {
  std::string text(3 << 20, 'x');
  auto strings = makeFlatVector<StringView>(
      {StringView(text), StringView(text), StringView(text)});
  auto input = makeRowVector({makeRowVector({strings})});
  CellShuffleTypeAdapter adapter(asRowType(input->type()));
  vector_size_t offset = 0;
  auto physical = adapter.encodeNext(input, offset, pool());
  EXPECT_EQ(offset, 2);
  bolt::test::assertEqualVectors(
      input->slice(0, 2), decodeComplex(adapter, physical));
  physical = adapter.encodeNext(input, offset, pool());
  EXPECT_EQ(offset, 3);
  bolt::test::assertEqualVectors(
      input->slice(2, 1), decodeComplex(adapter, physical));

  auto repeated = std::make_shared<RowVector>(
      pool(),
      adapter.physicalType(),
      nullptr,
      3,
      std::vector<VectorPtr>{
          BaseVector::wrapInConstant(3, 0, physical->childAt(0))});
  vector_size_t decodeOffset = 0;
  auto first = adapter.decodeNext(repeated, decodeOffset, pool());
  EXPECT_EQ(decodeOffset, 2);
  bolt::test::assertEqualVectors(input->slice(0, 2), first);
  auto second = adapter.decodeNext(repeated, decodeOffset, pool());
  EXPECT_EQ(decodeOffset, 3);
  bolt::test::assertEqualVectors(input->slice(2, 1), second);

  // No per-row size cap: a row beyond the batch target forms its own batch.
  std::string oversized((64ULL << 20) + 1, 'z');
  auto large = makeRowVector({makeRowVector({makeFlatVector<StringView>(
      {StringView(oversized), StringView(text)})})});
  offset = 0;
  physical = adapter.encodeNext(large, offset, pool());
  EXPECT_EQ(offset, 1);
  bolt::test::assertEqualVectors(
      large->slice(0, 1), decodeComplex(adapter, physical));
  physical = adapter.encodeNext(large, offset, pool());
  EXPECT_EQ(offset, 2);
  bolt::test::assertEqualVectors(
      large->slice(1, 1), decodeComplex(adapter, physical));
}

TEST_F(CellWriterTest, rejectsInvalidBooleanAndMissingNullBody) {
  // One non-null BOOLEAN, one stored Run, one raw byte.
  std::string bytes(44, '\0');
  bytes[0] = 1;
  bytes[4] = 1;
  bytes[16] = 1;
  bytes[24] = 1;
  bytes[26] = 2;
  bytes[27] = 1;
  bytes[35] = 1;
  bytes[43] = 2;
  CellPayloadDecoder decoder(
      CellLayout::create(ROW({BOOLEAN()})), nullptr, pool());
  auto decode = [&](const std::string& payload) {
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
    RowVectorPtr output;
    std::string error;
    return decoder.decode(source, output, error);
  };
  EXPECT_FALSE(decode(bytes));
  bytes[43] = 1;
  EXPECT_TRUE(decode(bytes));
  bytes[16] = 0;
  EXPECT_FALSE(decode(bytes));
}

TEST_F(CellWriterTest, complexDecodeRespectsRowLimit) {
  constexpr vector_size_t elementsPerRow = 10000;
  auto offsets = allocateOffsets(1, pool());
  offsets->asMutable<vector_size_t>()[0] = 0;
  auto sizes = allocateSizes(1, pool());
  sizes->asMutable<vector_size_t>()[0] = elementsPerRow;
  auto array = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(UNKNOWN()),
      nullptr,
      1,
      offsets,
      sizes,
      BaseVector::createNullConstant(UNKNOWN(), elementsPerRow, pool()));
  auto input = makeRowVector({array});
  CellShuffleTypeAdapter adapter(asRowType(input->type()));
  vector_size_t offset = 0;
  auto encoded = adapter.encodeNext(input, offset, pool());
  // UNKNOWN elements remain a null constant in CompactRow. Batch by row count
  // without charging an artificial per-element workspace estimate.
  auto physical = std::make_shared<RowVector>(
      pool(),
      adapter.physicalType(),
      nullptr,
      2049,
      std::vector<VectorPtr>{
          BaseVector::wrapInConstant(2049, 0, encoded->childAt(0))});
  offset = 0;
  int batches = 0;
  while (offset < physical->size()) {
    const auto start = offset;
    auto part = adapter.decodeNext(physical, offset, pool());
    ASSERT_EQ(part->size(), std::min(1024, physical->size() - start));
    EXPECT_EQ(offset, start + part->size());
    const auto* restored = part->childAt(0)->as<ArrayVector>();
    EXPECT_EQ(restored->sizeAt(0), elementsPerRow);
    EXPECT_EQ(restored->sizeAt(part->size() - 1), elementsPerRow);
    EXPECT_TRUE(restored->elements()->isNullAt(0));
    ++batches;
  }
  EXPECT_EQ(batches, 3);
}

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
    auto reals = makeFlatVector<float>(n, [](auto row) { return row * 0.5f; });
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
  options.cellOptions.cellMemoryCapBytes = 8 << 20;
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

TEST_F(CellWriterTest, compressionCodecShrinksSpillsAndRoundTrips) {
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

  const auto runOnce = [&](arrow::Compression::type compressionType) {
    auto options = makeOptions(kPartitions);
    options.cellOptions.cellMemoryCapBytes = 8 << 20;
    options.cellOptions.checkpointPartitionBytes = 64 << 10;
    options.partitionWriterOptions.compressionType = compressionType;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    EXPECT_GT(writer.metrics().spillCount, 0);
    return writer.metrics().totalBytesEvicted;
  };

  const auto compressedSpillBytes = runOnce(arrow::Compression::LZ4_FRAME);
  ::unlink(dataFile_.c_str());
  const auto rawSpillBytes = runOnce(arrow::Compression::UNCOMPRESSED);
  // The whole point of spill compression: fewer bytes hit the disk on the
  // spill pass (the compressible strings dominate this data set).
  EXPECT_LT(compressedSpillBytes * 2, rawSpillBytes)
      << "compressed spill " << compressedSpillBytes << " vs raw "
      << rawSpillBytes;
}

TEST_F(CellWriterTest, compressSpillToggleAcrossMergeShapes) {
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(31);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  // ~4MB per batch: with an 8MB chunk cap every window sees several
  // pressure spills. The last partition takes one short row per batch, so
  // its segments stay below the compression minimum - stored at spill time
  // even with compressSpill, and final as they are. The closing small
  // batch leaves resident cells beside the last window's spilled segments.
  for (int batch = 0; batch < 9; ++batch) {
    pids.emplace_back();
    batches.push_back(makeCompressibleBatch(
        batch < 8 ? 4096 : 256, kPartitions, rng, pids.back()));
  }
  const auto layout = CellLayout::create(asRowType(batches[0]->type()));
  // Sealed windows (the first payload of a partition spans several spill
  // runs), then one window only: the residual payload gathers its spilled
  // segments together with the resident cells.
  for (const bool seal : {true, false}) {
    for (const bool coalesce : {true, false}) {
      SCOPED_TRACE(
          std::string(seal ? "sealed windows" : "residual window") + ", " +
          (coalesce ? "coalesced" : "run per segment"));
      int64_t evicted[2] = {0, 0};
      int64_t written[2] = {0, 0};
      for (const bool compressSpill : {true, false}) {
        auto options = makeOptions(kPartitions);
        options.partitionWriterOptions.compressionType =
            arrow::Compression::LZ4_FRAME;
        options.cellOptions.compressSpill = compressSpill;
        options.cellOptions.coalesceMergedRuns = coalesce;
        options.cellOptions.cellMemoryCapBytes = 8 << 20;
        options.cellOptions.checkpointPartitionBytes =
            seal ? 4 << 20 : 1LL << 30;
        CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
        roundTrip(options, pids, batches, &writer);
        const auto& metrics = writer.metrics();
        EXPECT_GT(metrics.totalBytesEvicted, 0);
        EXPECT_EQ(metrics.spillCount > 0, seal); // sealed windows
        const auto file = readFile();
        const auto runCounts =
            payloadRunCounts(layout, file, 0, metrics.partitionLengths[0]);
        ASSERT_FALSE(runCounts.empty());
        if (seal) {
          EXPECT_GE(runCounts.size(), 2);
        } else {
          EXPECT_EQ(runCounts.size(), 1);
        }
        if (coalesce) {
          EXPECT_EQ(runCounts[0], 1);
        } else {
          // Spilled segments, plus the resident cells when not sealed.
          EXPECT_GE(runCounts[0], 2);
        }
        evicted[compressSpill] = metrics.totalBytesEvicted;
        written[compressSpill] = metrics.totalBytesWritten;
        ::unlink(dataFile_.c_str());
      }
      EXPECT_LT(evicted[1] * 2, evicted[0]);
      // Spill compression changes what hits the spill file, never the
      // final file: verbatim and re-merged runs carry the same bytes.
      EXPECT_EQ(written[1], written[0]);
    }
  }
}

TEST_F(CellWriterTest, unfundedSpillCompressionIsMadeAtMerge) {
  // A pressure spill whose pool cannot fund the compression workspace
  // streams its runs out stored; those segments are not final, so the
  // merge makes the compression attempt for them.
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(37);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  int64_t rawBytes = 0;
  for (int batch = 0; batch < 8; ++batch) {
    pids.emplace_back();
    batches.push_back(
        makeCompressibleBatch(4096, kPartitions, rng, pids.back()));
    rawBytes += batches.back()->estimateFlatSize();
  }
  auto root = memory::memoryManager()->addRootPool(
      "unfundedSpillCompression", 24 << 20);
  auto leaf = root->addLeafChild("writer");
  const auto setCapacity = [&](int64_t bytes) {
    static_cast<memory::MemoryPoolImpl*>(root.get())->testingSetCapacity(bytes);
  };
  for (const bool coalesce : {true, false}) {
    SCOPED_TRACE(coalesce ? "coalesced" : "run per segment");
    auto options = makeOptions(kPartitions);
    options.partitionWriterOptions.compressionType =
        arrow::Compression::LZ4_FRAME;
    options.cellOptions.coalesceMergedRuns = coalesce;
    options.cellOptions.checkpointPartitionBytes = 4 << 20;
    setCapacity(24 << 20);
    CellShuffleWriter writer(options, leaf.get(), arrow::default_memory_pool());
    roundTrip(
        options, pids, batches, &writer, [&](CellShuffleWriter&, uint32_t i) {
          if (i + 1 == batches.size()) {
            setCapacity(1LL << 30); // the merge is funded
          }
        });
    const auto& metrics = writer.metrics();
    EXPECT_GT(metrics.spillCount, 0);
    // The spills could not compress: the spill file holds raw runs ...
    EXPECT_GT(metrics.totalBytesEvicted * 2, rawBytes);
    // ... and the merge compressed them all the same.
    EXPECT_LT(metrics.totalBytesWritten * 8, rawBytes);
    ::unlink(dataFile_.c_str());
  }
}

TEST_F(CellWriterTest, shrinkIsSafeWhileTheOutputReadsCells) {
  // Reclaim may shrink idle chunks while the output is reading the cells:
  // only chunks without a live cell may go, and a seal (which recycles
  // every cell first) leaves them all idle.
  const auto rows = makeRowVector(
      {makeFlatVector<int64_t>(4096, [](auto row) { return row * 31; })});
  const auto layout = CellLayout::create(asRowType(rows->type()));
  ChunkAllocator allocator(pool(), 16 << 10, 1 << 10, []() {});
  RecordingCellOutput output;
  CellSplitter splitter(layout, 1, allocator, output, *pool());
  std::string reference;
  std::string scanned;
  const auto scan = [&](const CellWindowInput& in, std::string& into) {
    in.cells->scan(0, 0, [&](const char* data, uint32_t bytes) {
      into.append(data, bytes);
    });
  };
  output.onSpill = [&](const CellWindowInput& in) {
    scan(in, reference);
    allocator.shrink(); // all chunks hold live cells: nothing may move
    scan(in, scanned);
  };
  int64_t releasedAtSeal = 0;
  output.onSeal = [&](const CellWindowInput&) {
    releasedAtSeal = allocator.shrink();
  };
  std::vector<uint32_t> row2Partition(rows->size(), 0);
  std::vector<uint32_t> partition2RowCount{static_cast<uint32_t>(rows->size())};
  std::vector<DecodedVector> decoded(1);
  decoded[0].decode(*rows->childAt(0));
  BatchNullClass nullClass = BatchNullClass::kNoNulls;
  SplitBatch batch;
  batch.decoded = decoded.data();
  batch.row2Partition = row2Partition.data();
  batch.partition2RowCount = partition2RowCount.data();
  batch.nullClass = &nullClass;
  batch.numRows = rows->size();
  batch.rowIndexInPid = nullptr;
  splitter.split(batch);
  splitter.sealWindow();
  EXPECT_FALSE(reference.empty());
  EXPECT_EQ(scanned, reference);
  EXPECT_GT(releasedAtSeal, 0);
}

TEST_F(CellWriterTest, reclaimAfterStopIsHarmless) {
  auto options = makeOptions(2);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  auto data = makeRowVector(
      {makeFlatVector<int64_t>(4096, [](auto row) { return row; })});
  ASSERT_TRUE(
      writer.split(withPid(std::vector<int32_t>(4096, 1), data), 0).ok());
  ASSERT_TRUE(writer.stop().ok());
  int64_t actual = -1;
  ASSERT_TRUE(writer.reclaimFixedSize(1 << 30, &actual).ok());
  EXPECT_GE(actual, 0);
}

TEST_F(CellWriterTest, complexColumnsRoundTripThroughWriterAndReader) {
  // Callers hand logical rows to the writer and get logical rows back from
  // the reader; the CompactRow packing stays inside the Cell classes.
  constexpr int32_t kPartitions = 3;
  constexpr vector_size_t kRows = 3000;
  auto data = makeRowVector(
      {"id", "tags", "attrs", "nested"},
      {makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
       makeArrayVector<int32_t>(
           kRows,
           [](auto row) { return row % 4; },
           [](auto row) { return row; },
           [](auto row) { return row % 7 == 0; }),
       makeMapVector<int32_t, int64_t>(
           kRows,
           [](auto row) { return row % 3; },
           [](auto row) { return row; },
           [](auto row) { return row * 2; }),
       makeRowVector(
           {makeFlatVector<std::string>(
                kRows, [](auto row) { return std::string(row % 20, 'n'); }),
            makeFlatVector<bool>(kRows, [](auto row) { return row % 2; })},
           [](auto row) { return row % 11 == 0; })});
  std::vector<int32_t> pids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    pids[row] = row % kPartitions;
  }
  auto options = makeOptions(kPartitions);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  ASSERT_TRUE(writer.split(withPid(pids, data), 0).ok());
  ASSERT_TRUE(writer.stop().ok());
  EXPECT_GT(writer.metrics().convertTime, 0);

  const auto file = readFile();
  auto codec = createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
  uint64_t offset = 0;
  for (int32_t pid = 0; pid < kPartitions; ++pid) {
    const auto length = writer.metrics().partitionLengths[pid];
    auto streams = std::make_shared<test::MemoryReaderStreamIterator>(
        std::vector<std::vector<char>>{std::vector<char>(
            file.begin() + offset, file.begin() + offset + length)});
    offset += length;
    CellShuffleReader reader(
        streams,
        asRowType(data->type()),
        codec.get(),
        arrow::default_memory_pool(),
        pool(),
        4096,
        1 << 20);
    vector_size_t next = pid;
    while (auto batch = reader.next()) {
      ASSERT_TRUE(batch->type()->equivalent(*data->type()));
      for (vector_size_t row = 0; row < batch->size(); ++row) {
        ASSERT_LT(next, kRows);
        EXPECT_TRUE(data->equalValueAt(batch.get(), next, row))
            << "partition " << pid << " row " << next;
        next += kPartitions;
      }
    }
    EXPECT_GE(next, kRows);
  }
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

TEST_F(CellWriterTest, stringDictionaryRoundTripAndShrinksBytes) {
  constexpr int32_t kPartitions = 8;
  constexpr int kRows = 2000;
  // 12 distinct values, 54 serialized bytes: fits one 64-byte dictionary,
  // so the probe turns the column on. The second string column is
  // high-cardinality and must stay raw.
  const std::vector<std::string> vocab = {
      "aaaa",
      "bbbb",
      "cccc",
      "dddd",
      "eeee",
      "ffff",
      "gggg",
      "hhhh",
      "iiii",
      "jjjj",
      "",
      "kk"};
  std::mt19937 rng(42);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < 4; ++b) {
    std::vector<int32_t> batchPids(kRows);
    std::vector<std::optional<StringView>> lowCard(kRows);
    std::vector<std::string> storage(kRows);
    std::vector<std::optional<StringView>> highCard(kRows);
    std::vector<int64_t> ids(kRows);
    for (int i = 0; i < kRows; ++i) {
      batchPids[i] = rng() % kPartitions;
      ids[i] = static_cast<int64_t>(rng());
      if (rng() % 7 == 0) {
        lowCard[i] = std::nullopt;
      } else {
        lowCard[i] = StringView(vocab[rng() % vocab.size()]);
      }
      storage[i] = "value-" + std::to_string(b) + "-" + std::to_string(rng());
      highCard[i] = StringView(storage[i]);
    }
    pids.push_back(std::move(batchPids));
    batches.push_back(makeRowVector(
        {"low", "id", "high"},
        {makeNullableFlatVector<StringView>(lowCard),
         makeFlatVector<int64_t>(ids),
         makeNullableFlatVector<StringView>(highCard)}));
  }

  // Uncompressed merge so the wire sizes compare byte for byte.
  int64_t bytesWith = 0;
  int64_t bytesWithout = 0;
  for (const bool enable : {true, false}) {
    auto options = makeOptions(kPartitions);
    options.partitionWriterOptions.compressionType =
        arrow::Compression::UNCOMPRESSED;
    options.cellOptions.enableStringDictionary = enable;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    (enable ? bytesWith : bytesWithout) = writer.metrics().totalBytesWritten;
    if (enable) {
      EXPECT_GT(writer.metrics().dictionaryMatchedRows, 0);
    } else {
      EXPECT_EQ(writer.metrics().dictionaryMatchedRows, 0);
    }
  }
  EXPECT_LT(bytesWith, bytesWithout);
}

TEST_F(CellWriterTest, dictionarySegmentsChainDemoteAndResetAcrossWindows) {
  constexpr int32_t kPartitions = 4;
  constexpr int kRows = 1500;
  // Both vocabularies serialize to 48 bytes (8 entries of 5 chars).
  // "same1"/"same2" share length and 4-byte prefix: the scan-key collision
  // path (prefix hit, suffix mismatch, rescan) is exercised.
  const std::vector<std::string> vocabA = {
      "north", "south", "east!", "west!", "same1", "same2", "left-", "right"};
  const std::vector<std::string> vocabB = {
      "ocean", "river", "lake!", "pond!", "sea--", "bay--", "gulf-", "creek"};
  std::mt19937 rng(7);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  const auto addBatch =
      [&](const std::function<std::optional<std::string>(int)>& gen) {
        std::vector<int32_t> batchPids(kRows);
        std::vector<std::optional<StringView>> col(kRows);
        std::vector<std::string> storage(kRows);
        for (int i = 0; i < kRows; ++i) {
          batchPids[i] = rng() % kPartitions;
          auto value = gen(i);
          if (!value.has_value()) {
            col[i] = std::nullopt;
          } else {
            storage[i] = std::move(*value);
            col[i] = StringView(storage[i]);
          }
        }
        pids.push_back(std::move(batchPids));
        batches.push_back(
            makeRowVector({"s"}, {makeNullableFlatVector<StringView>(col)}));
      };

  // Probe batch: vocabulary A with nulls -> dictionary on.
  addBatch([&](int) -> std::optional<std::string> {
    return rng() % 9 == 0 ? std::nullopt
                          : std::make_optional(vocabA[rng() % vocabA.size()]);
  });
  // Mid-batch vocabulary shift: the A segment closes with a high hit rate
  // and a successor segment chains (0xFE framing).
  addBatch([&](int i) -> std::optional<std::string> {
    return i < kRows / 2 ? vocabA[rng() % vocabA.size()]
                         : vocabB[rng() % vocabB.size()];
  });
  // Values too long for any entry force the per-partition fallback tail.
  addBatch([&](int i) -> std::optional<std::string> {
    return i % 5 == 0 ? std::string(80, 'x') : vocabB[rng() % vocabB.size()];
  });
  // High cardinality after a window reset: segments fill with no reuse and
  // the hit-rate rule demotes.
  addBatch([&](int) -> std::optional<std::string> {
    return "uid-" + std::to_string(rng());
  });
  // Back to a friendly vocabulary in a later window: dictionary re-engages.
  addBatch([&](int) -> std::optional<std::string> {
    return vocabA[rng() % vocabA.size()];
  });

  auto options = makeOptions(kPartitions);
  // Tiny window bound: several checkpoints, so per-window framing and the
  // dictionary state reset are exercised.
  options.cellOptions.checkpointPartitionBytes = 2 << 10;
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  roundTrip(options, pids, batches, &writer);
  // The vocabulary batches index through dictionaries; the long values
  // and the high-cardinality batch demote partitions into the tail.
  EXPECT_GT(writer.metrics().dictionaryMatchedRows, 0);
  EXPECT_GT(writer.metrics().dictionaryFallbackRows, 0);
}

TEST_F(CellWriterTest, coalescedMergeShrinksManyRunPayloads) {
  // Wide schema (many streams) plus a tiny memory cap: runs spill every
  // few batches, so without coalescing every payload carries many runs
  // whose per-run headers (1 + 8 + 8 x streams bytes) and tiny
  // compression contexts dominate the file.
  constexpr int32_t kPartitions = 64;
  constexpr int kColumns = 12;
  std::mt19937 rng(11);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 24; ++batch) {
    const int n = 1024;
    std::vector<int32_t> batchPids(n);
    for (int i = 0; i < n; ++i) {
      batchPids[i] = static_cast<int32_t>(rng() % kPartitions);
    }
    std::vector<std::string> names;
    std::vector<VectorPtr> columns;
    for (int c = 0; c < kColumns; ++c) {
      names.push_back("c" + std::to_string(c));
      columns.push_back(makeFlatVector<int64_t>(
          n, [&](auto /*row*/) { return static_cast<int64_t>(rng()); }));
    }
    batches.push_back(makeRowVector(names, columns));
    pids.push_back(std::move(batchPids));
  }

  int64_t bytesCoalesced = 0;
  int64_t bytesPerSpill = 0;
  for (const bool coalesce : {true, false}) {
    auto options = makeOptions(kPartitions);
    options.cellOptions.cellMemoryCapBytes = 8 << 20;
    options.cellOptions.coalesceMergedRuns = coalesce;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    EXPECT_GT(writer.metrics().totalBytesEvicted, 0) << "expected spills";
    (coalesce ? bytesCoalesced : bytesPerSpill) =
        writer.metrics().totalBytesWritten;
  }
  EXPECT_LT(bytesCoalesced, bytesPerSpill);
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
