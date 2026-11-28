/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/shuffle/sparksql/tests/ShuffleTestBase.h"

#include <arrow/io/memory.h>
#include <arrow/memory_pool.h>
#include <arrow/util/bit_util.h>
#include <cstring>
#include <numeric>

#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/row/CompactRow.h"
#include "bolt/row/dense/DenseRow.h"
#include "bolt/shuffle/sparksql/Payload.h"
#include "bolt/shuffle/sparksql/ShuffleColumnarToRowConverter.h"
#include "bolt/shuffle/sparksql/ShuffleReaderNode.h"
#include "bolt/shuffle/sparksql/ShuffleRowToColumnarConverter.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/tests/MemoryReaderStreamIterator.h"

namespace bytedance::bolt::shuffle::sparksql::test {

class ShuffleMiscTest : public ShuffleTestBase {};

TEST(ArrowBufferSliceTest, RetainsParentAndManager) {
  arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
  std::shared_ptr<arrow::ResizableBuffer> parent =
      arrow::AllocateResizableBuffer(16, &pool).ValueOrDie();
  std::iota(parent->mutable_data(), parent->mutable_data() + 16, uint8_t{0});
  auto manager = parent->memory_manager();
  std::weak_ptr<arrow::Buffer> weakParent = parent;
  auto* data = parent->data();

  auto slice = sliceBufferWithParentManager(parent, 4, 8);
  EXPECT_EQ(slice->data(), data + 4);
  EXPECT_EQ(slice->size(), 8);
  EXPECT_EQ(slice->memory_manager(), manager);
  EXPECT_EQ(slice->parent(), parent);

  parent.reset();
  EXPECT_FALSE(weakParent.expired());
  EXPECT_EQ(slice->data()[0], 4);
  EXPECT_EQ(slice->data()[7], 11);
  slice.reset();
  EXPECT_TRUE(weakParent.expired());
}

TEST(ArrowBufferSliceTest, PreservesWriterCapacityRange) {
  arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
  std::shared_ptr<arrow::ResizableBuffer> parent =
      arrow::AllocateResizableBuffer(4, &pool).ValueOrDie();
  ASSERT_TRUE(parent->Reserve(64).ok());
  std::iota(parent->mutable_data(), parent->mutable_data() + 32, uint8_t{0});

  auto slice = sliceBufferWithParentManager(parent, 8, 24);
  EXPECT_EQ(parent->size(), 4);
  EXPECT_EQ(slice->size(), 24);
  EXPECT_EQ(slice->data(), parent->data() + 8);
  EXPECT_EQ(slice->memory_manager(), parent->memory_manager());
  EXPECT_EQ(slice->data()[0], 8);
  EXPECT_EQ(slice->data()[23], 31);
}

namespace {

std::vector<int64_t> rowBytes(const RowVectorPtr& vector) {
  bolt::row::CompactRow compactRow(vector);
  std::vector<int64_t> bytes;
  bytes.reserve(vector->size());
  for (auto row = 0; row < vector->size(); ++row) {
    bytes.push_back(compactRow.rowSize(row) + kSizeOfRowHeader);
  }
  return bytes;
}

std::vector<int64_t> denseRowBytes(const RowVectorPtr& vector) {
  bolt::row::DenseRow denseRow(vector);
  std::vector<int64_t> bytes;
  bytes.reserve(vector->size());
  for (const auto rowSize : denseRow.rowSizes()) {
    bytes.push_back(static_cast<int64_t>(rowSize) + kSizeOfRowHeader);
  }
  return bytes;
}

RowTypePtr rowTypeOf(const RowVectorPtr& vector) {
  return std::dynamic_pointer_cast<const RowType>(vector->type());
}

std::vector<std::string_view> rowBodies(const std::vector<uint8_t*>& rows) {
  std::vector<std::string_view> bodies;
  bodies.reserve(rows.size());
  for (auto* row : rows) {
    const auto rowSize = *reinterpret_cast<const int32_t*>(row);
    bodies.emplace_back(
        reinterpret_cast<const char*>(row + kSizeOfRowHeader), rowSize);
  }
  return bodies;
}

class MetricsTrackingStreamIterator final : public MemoryReaderStreamIterator {
 public:
  using MemoryReaderStreamIterator::MemoryReaderStreamIterator;

  void updateMetrics(
      int64_t numRows,
      int64_t numBatches,
      int64_t decompressTime,
      int64_t deserializeTime,
      int64_t deserializerCreateTime,
      int64_t deserializerDestroyTime,
      int64_t mergeTime,
      int64_t totalReadTime) override {
    metricsUpdated = true;
    outputRows = numRows;
  }

  bool metricsUpdated{false};
  int64_t outputRows{-1};
};

std::vector<char> serializeBigIntStream(
    const std::vector<int64_t>& values,
    arrow::MemoryPool* pool) {
  const auto numRows = static_cast<uint32_t>(values.size());
  const auto validityBytes = arrow::bit_util::BytesForBits(numRows);
  auto validity =
      arrow::AllocateResizableBuffer(validityBytes, pool).ValueOrDie();
  std::memset(validity->mutable_data(), 0xFF, validityBytes);

  auto data = arrow::AllocateResizableBuffer(numRows * sizeof(int64_t), pool)
                  .ValueOrDie();
  std::memcpy(data->mutable_data(), values.data(), numRows * sizeof(int64_t));

  const std::vector<bool> isValidityBuffer = {true, false};
  auto payload = BlockPayload::fromBuffers(
                     Payload::Type::kUncompressed,
                     numRows,
                     {std::move(validity), std::move(data)},
                     &isValidityBuffer,
                     pool,
                     /*codec=*/nullptr,
                     Payload::Mode::kBuffer,
                     /*hasComplexType=*/false)
                     .ValueOrDie();

  auto out = arrow::io::BufferOutputStream::Create(1 << 10, pool).ValueOrDie();
  BOLT_CHECK(payload->serialize(out.get()).ok(), "serialize failed");
  auto buffer = out->Finish().ValueOrDie();
  return std::vector<char>(
      reinterpret_cast<const char*>(buffer->data()),
      reinterpret_cast<const char*>(buffer->data()) + buffer->size());
}

} // namespace

TEST_F(ShuffleMiscTest, TotalRowBytesIncludesHeadersAndUsesInt64) {
  int32_t rowSize = std::numeric_limits<int32_t>::max();
  std::vector<uint8_t*> rows(2, reinterpret_cast<uint8_t*>(&rowSize));

  EXPECT_EQ(
      getTotalRowBytes(rows),
      2LL * (std::numeric_limits<int32_t>::max() + kSizeOfRowHeader));
}

// End-to-end test: RoundRobin with Adaptive mode, >=8000 partitions and >=5
// columns should use V1 consistently on both writer and reader side.
// Before the fix, the writer chose V1 for RoundRobin (not in adaptive set
// when sort_before_repartition=false), but the reader incorrectly chose
// RowBased deserialization by checking partitioning name "rr" alone,
// causing a ZSTD decompression error on format mismatch.
TEST_F(ShuffleMiscTest, AdaptiveRoundRobinLargePartitions) {
  ShuffleTestParam param;
  param.partitioning = "rr";
  param.shuffleMode = 0; // Adaptive
  param.writerType = PartitionWriterType::kLocal;
  param.dataTypeGroup = DataTypeGroup::kInteger; // 5 columns
  param.numPartitions = 8000; // >= rowBasePartitionThreshold
  param.numMappers = 1;
  param.batchSize = 32;
  param.numBatches = 2;
  param.verifyOutput = true;
  executeTest(param);
}

// Same as above but with kMix (16 columns), well above the threshold.
TEST_F(ShuffleMiscTest, AdaptiveRoundRobinLargePartitionsMixTypes) {
  ShuffleTestParam param;
  param.partitioning = "rr";
  param.shuffleMode = 0; // Adaptive
  param.writerType = PartitionWriterType::kLocal;
  param.dataTypeGroup = DataTypeGroup::kMix; // 16 columns
  param.numPartitions = 8000;
  param.numMappers = 1;
  param.batchSize = 32;
  param.numBatches = 2;
  param.verifyOutput = true;
  executeTest(param);
}

// Test that shuffle writer correctly handles dictionary-encoded string columns
// with skewed entry sizes. After flatten, estimateFlatSize() should reflect
// actual string bytes (via StringViewStats), not the underestimated
// retainedSize from shared dictionary string buffers.
TEST_F(ShuffleMiscTest, SkewedDictionaryStringEstimateFlatSize) {
  // Create skewed data: 10 short strings + 990 copies of a 100KB string.
  // When dictionary-encoded, dict avg is ~1KB but most rows reference 100KB.
  constexpr int32_t kNumRows = 1000;
  constexpr int32_t kLongStringLen = 100 * 1024;
  constexpr int32_t kNumShortEntries = 10;

  std::string longStr(kLongStringLen, 'x');
  std::vector<std::string> shortStrs;
  for (int i = 0; i < kNumShortEntries; ++i) {
    shortStrs.push_back("s" + std::to_string(i));
  }

  // Build a FlatVector, then wrap in DictionaryVector to simulate Parquet
  // output
  auto flatValues =
      makeFlatVector<StringView>(kNumShortEntries + 1, [&](auto row) {
        if (row < kNumShortEntries) {
          return StringView(shortStrs[row]);
        }
        return StringView(longStr);
      });

  // Create indices: first 10 rows → short entries, rest → long entry
  auto indices = makeIndices(kNumRows, [&](auto row) {
    if (row < kNumShortEntries) {
      return static_cast<vector_size_t>(row);
    }
    return static_cast<vector_size_t>(kNumShortEntries); // long entry
  });
  auto dictVector = wrapInDictionary(indices, kNumRows, flatValues);

  // Verify DictionaryVector estimateFlatSize underestimates
  auto dictEstimate = dictVector->estimateFlatSize();
  uint64_t actualBytes = 0;
  uint64_t actualNonInlineBytes = 0;
  for (int i = 0; i < kNumRows; ++i) {
    auto sv = dictVector->as<SimpleVector<StringView>>()->valueAt(i);
    actualBytes += sv.size();
    if (!sv.isInline()) {
      actualNonInlineBytes += sv.size();
    }
  }
  // DictionaryVector default estimate uses dict avg, should be much smaller
  EXPECT_LT(dictEstimate, actualBytes / 2)
      << "DictionaryVector should underestimate before fix";

  // Flatten the DictionaryVector (simulates FilterProject or ShuffleWriter)
  VectorPtr flattened = dictVector;
  BaseVector::flattenVector(flattened);

  // After flatten, FlatVector should have StringViewStats set.
  // totalBytes only includes non-inline strings (>12B); inline strings are
  // stored in the StringView struct, covered by values.size().
  auto* flatVec = flattened->asFlatVector<StringView>();
  ASSERT_NE(flatVec, nullptr);
  ASSERT_TRUE(flatVec->stringStats().has_value())
      << "StringViewStats should be set after flattenVector";
  EXPECT_EQ(flatVec->stringStats()->totalBytes, actualNonInlineBytes);
  EXPECT_EQ(flatVec->stringStats()->maxLength, kLongStringLen);

  // estimateFlatSize should now be accurate
  auto flatEstimate = flattened->estimateFlatSize();
  EXPECT_GE(flatEstimate, actualBytes)
      << "Post-flatten estimateFlatSize should be >= actual string bytes";

  // Verify via RowVector (as ShuffleWriter would see it)
  auto rowVector = makeRowVector({"col0"}, {flattened});
  auto rowEstimate = rowVector->estimateFlatSize();
  EXPECT_GE(rowEstimate, actualBytes)
      << "RowVector estimateFlatSize should reflect StringViewStats";

  // Run through shuffle to verify data correctness
  ShuffleTestParam param;
  param.partitioning = "hash";
  param.shuffleMode = 2; // V2
  param.writerType = PartitionWriterType::kLocal;
  param.dataTypeGroup = DataTypeGroup::kLargeString;
  param.numPartitions = 10;
  param.numMappers = 1;
  param.batchSize = kNumRows;
  param.numBatches = 1;
  param.verifyOutput = true;

  // Use custom input with the skewed dictionary data
  auto pidColumn = makeFlatVector<int32_t>(
      kNumRows, [&](auto row) { return row % param.numPartitions; });
  auto inputWithPid = makeRowVector({pidColumn, dictVector});
  ShuffleInputData inputData;
  inputData.inputsPerMapper.push_back({inputWithPid});
  executeTestWithCustomInput(param, inputData);
}

TEST_F(ShuffleMiscTest, ColumnarToRowStatsSplitVariableWidthRows) {
  auto data = makeRowVector(
      {"c0"},
      {makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee"})});
  const auto bytes = rowBytes(data);
  const auto maxBatchSize = bytes[0] + bytes[1] + bytes[2];

  ShuffleColumnarToRowConverter converter(
      rowTypeOf(data), pool(), row::RowFormat::COMPACT);
  auto stats = converter.getWithStats(data, maxBatchSize);

  ASSERT_EQ(stats.ranges().size(), 2);
  EXPECT_EQ(stats.ranges()[0].offset, 0);
  EXPECT_EQ(stats.ranges()[0].length, 3);
  EXPECT_EQ(stats.ranges()[0].bytes, maxBatchSize);
  EXPECT_EQ(stats.ranges()[1].offset, 3);
  EXPECT_EQ(stats.ranges()[1].length, 2);
  EXPECT_EQ(stats.ranges()[1].bytes, bytes[3] + bytes[4]);
  EXPECT_EQ(
      stats.getTotalMemorySize(),
      std::accumulate(bytes.begin(), bytes.end(), int64_t{0}));
}

TEST_F(ShuffleMiscTest, ColumnarToRowStatsMergesSmallLastRange) {
  auto data = makeRowVector(
      {"c0"}, {makeFlatVector<std::string>({std::string(100, 'x'), "y"})});
  const auto bytes = rowBytes(data);
  const auto maxBatchSize = bytes[0];

  ASSERT_LT(bytes[1], maxBatchSize * 0.8);

  ShuffleColumnarToRowConverter converter(
      rowTypeOf(data), pool(), row::RowFormat::COMPACT);
  auto stats = converter.getWithStats(data, maxBatchSize);

  ASSERT_EQ(stats.ranges().size(), 1);
  EXPECT_EQ(stats.ranges()[0].offset, 0);
  EXPECT_EQ(stats.ranges()[0].length, data->size());
  EXPECT_EQ(
      stats.ranges()[0].bytes,
      std::accumulate(bytes.begin(), bytes.end(), int64_t{0}));
}

TEST_F(ShuffleMiscTest, ColumnarToDenseRowStatsSplitVariableWidthRows) {
  auto data = makeRowVector(
      {"c0"},
      {makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee"})});
  const auto bytes = denseRowBytes(data);
  const auto maxBatchSize = bytes[0] + bytes[1] + bytes[2];

  ShuffleColumnarToRowConverter converter(
      rowTypeOf(data), pool(), row::RowFormat::DENSE);
  auto stats = converter.getWithStats(data, maxBatchSize);

  ASSERT_EQ(stats.ranges().size(), 2);
  EXPECT_EQ(stats.ranges()[0].offset, 0);
  EXPECT_EQ(stats.ranges()[0].length, 3);
  EXPECT_EQ(stats.ranges()[0].bytes, maxBatchSize);
  EXPECT_EQ(stats.ranges()[1].offset, 3);
  EXPECT_EQ(stats.ranges()[1].length, 2);
  EXPECT_EQ(stats.ranges()[1].bytes, bytes[3] + bytes[4]);
  EXPECT_EQ(
      stats.getTotalMemorySize(),
      std::accumulate(bytes.begin(), bytes.end(), int64_t{0}));
}

TEST_F(ShuffleMiscTest, ColumnarToDenseRowSlicedStatsConvert) {
  auto data = makeRowVector(
      {"c0"},
      {makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee"})});
  const auto bytes = denseRowBytes(data);
  const auto maxBatchSize = bytes[0] + bytes[1] + bytes[2];

  ShuffleColumnarToRowConverter converter(
      rowTypeOf(data), pool(), row::RowFormat::DENSE);
  auto fullStats = converter.getWithStats(data, maxBatchSize);
  ASSERT_EQ(fullStats.ranges().size(), 2);

  auto slicedData = std::dynamic_pointer_cast<RowVector>(
      data->slice(fullStats.ranges()[1].offset, fullStats.ranges()[1].length));
  auto slicedStats = converter.sliceStats(fullStats, fullStats.ranges()[1]);

  std::vector<uint32_t> indexes(slicedData->size(), 0);
  std::vector<std::vector<uint8_t*>> sortedRows(1);
  std::vector<int64_t> partitionBytes(1, 0);
  converter.convert(slicedStats, indexes, sortedRows, partitionBytes);

  ASSERT_EQ(sortedRows[0].size(), slicedData->size());
  EXPECT_EQ(partitionBytes[0], bytes[3] + bytes[4]);
}

TEST_F(ShuffleMiscTest, ColumnarToRowSlicedStatsReusesOriginalCompactRow) {
  auto data = makeRowVector(
      {"c0"},
      {makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee"})});
  const auto bytes = rowBytes(data);
  const auto maxBatchSize = bytes[0] + bytes[1] + bytes[2];

  ShuffleColumnarToRowConverter converter(
      rowTypeOf(data), pool(), row::RowFormat::COMPACT);
  auto fullStats = converter.getWithStats(data, maxBatchSize);
  ASSERT_EQ(fullStats.ranges().size(), 2);

  auto slicedStats = converter.sliceStats(fullStats, fullStats.ranges()[1]);

  std::vector<uint32_t> indexes(fullStats.ranges()[1].length, 0);
  std::vector<std::vector<uint8_t*>> sortedRows(1);
  std::vector<int64_t> partitionBytes(1, 0);
  converter.convert(slicedStats, indexes, sortedRows, partitionBytes);

  auto expected = std::dynamic_pointer_cast<RowVector>(
      data->slice(fullStats.ranges()[1].offset, fullStats.ranges()[1].length));
  auto actual = row::CompactRow::deserialize(
      rowBodies(sortedRows[0]), rowTypeOf(data), pool());
  ASSERT_EQ(actual->size(), expected->size());
  for (auto row = 0; row < actual->size(); ++row) {
    EXPECT_TRUE(expected->equalValueAt(actual.get(), row, row));
  }
}

TEST_F(ShuffleMiscTest, ColumnarToRowSlicedStatsReusesOriginalDenseRow) {
  auto data = makeRowVector(
      {"c0"},
      {makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee"})});
  const auto bytes = denseRowBytes(data);
  const auto maxBatchSize = bytes[0] + bytes[1] + bytes[2];

  ShuffleColumnarToRowConverter converter(
      rowTypeOf(data), pool(), row::RowFormat::DENSE);
  auto fullStats = converter.getWithStats(data, maxBatchSize);
  ASSERT_EQ(fullStats.ranges().size(), 2);

  auto slicedStats = converter.sliceStats(fullStats, fullStats.ranges()[1]);

  std::vector<uint32_t> indexes(fullStats.ranges()[1].length, 0);
  std::vector<std::vector<uint8_t*>> sortedRows(1);
  std::vector<int64_t> partitionBytes(1, 0);
  converter.convert(slicedStats, indexes, sortedRows, partitionBytes);

  auto expected = std::dynamic_pointer_cast<RowVector>(
      data->slice(fullStats.ranges()[1].offset, fullStats.ranges()[1].length));
  auto actual = row::DenseRow::deserialize(
      rowBodies(sortedRows[0]), rowTypeOf(data), pool());
  ASSERT_EQ(actual->size(), expected->size());
  for (auto row = 0; row < actual->size(); ++row) {
    EXPECT_TRUE(expected->equalValueAt(actual.get(), row, row));
  }
}

TEST_F(ShuffleMiscTest, ColumnarToRowSlicedStatsReusesComplexDenseRow) {
  auto data = makeRowVector(
      {"c0", "c1"},
      {makeRowVector(
           {makeArrayVector<int64_t>(
                {{1}, {2, 3}, {4, 5, 6}, {7, 8, 9, 10}, {11, 12, 13, 14, 15}}),
            makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee"})}),
       makeArrayVector<int64_t>(
           {{10},
            {20, 30},
            {40, 50, 60},
            {70, 80, 90, 100},
            {110, 120, 130, 140, 150}})});
  const auto bytes = denseRowBytes(data);
  const auto maxBatchSize = bytes[0] + bytes[1] + bytes[2];

  ShuffleColumnarToRowConverter converter(
      rowTypeOf(data), pool(), row::RowFormat::DENSE);
  auto fullStats = converter.getWithStats(data, maxBatchSize);
  ASSERT_EQ(fullStats.ranges().size(), 2);

  auto slicedStats = converter.sliceStats(fullStats, fullStats.ranges()[1]);

  std::vector<uint32_t> indexes(fullStats.ranges()[1].length, 0);
  std::vector<std::vector<uint8_t*>> sortedRows(1);
  std::vector<int64_t> partitionBytes(1, 0);
  converter.convert(slicedStats, indexes, sortedRows, partitionBytes);

  auto expected = std::dynamic_pointer_cast<RowVector>(
      data->slice(fullStats.ranges()[1].offset, fullStats.ranges()[1].length));
  auto actual = row::DenseRow::deserialize(
      rowBodies(sortedRows[0]), rowTypeOf(data), pool());
  ASSERT_EQ(actual->size(), expected->size());
  for (auto row = 0; row < actual->size(); ++row) {
    EXPECT_TRUE(expected->equalValueAt(actual.get(), row, row));
  }
}

TEST_F(ShuffleMiscTest, DefaultRowFormatIsDense) {
  EXPECT_EQ(ShuffleWriterOptions{}.rowFormat, row::RowFormat::DENSE);
  EXPECT_EQ(ShuffleReaderOptions{}.rowFormat, row::RowFormat::DENSE);

  auto data = makeRowVector(
      {"c0"},
      {makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee"})});
  const auto bytes = denseRowBytes(data);
  ASSERT_NE(bytes, rowBytes(data));
  const auto totalBytes =
      std::accumulate(bytes.begin(), bytes.end(), int64_t{0});

  ShuffleColumnarToRowConverter converter(rowTypeOf(data), pool());
  auto stats = converter.getWithStats(data, totalBytes);
  ASSERT_EQ(stats.ranges().size(), 1);
  EXPECT_EQ(stats.getTotalMemorySize(), totalBytes);
  EXPECT_EQ(stats.ranges()[0].bytes, totalBytes);

  std::vector<uint32_t> indexes(data->size(), 0);
  std::vector<std::vector<uint8_t*>> sortedRows(1);
  std::vector<int64_t> partitionBytes(1, 0);
  converter.convert(stats, indexes, sortedRows, partitionBytes);

  ASSERT_EQ(sortedRows[0].size(), data->size());
  EXPECT_EQ(partitionBytes[0], totalBytes);
  for (vector_size_t row = 0; row < data->size(); ++row) {
    const auto rowSize = *reinterpret_cast<const int32_t*>(sortedRows[0][row]);
    EXPECT_EQ(static_cast<int64_t>(rowSize) + kSizeOfRowHeader, bytes[row]);
  }

  auto bodies = rowBodies(sortedRows[0]);
  auto denseActual =
      row::DenseRow::deserialize(bodies, rowTypeOf(data), pool());
  ASSERT_EQ(denseActual->size(), data->size());
  for (vector_size_t row = 0; row < denseActual->size(); ++row) {
    EXPECT_TRUE(data->equalValueAt(denseActual.get(), row, row));
  }

  ShuffleRowToColumnarConverter rowToCol(rowTypeOf(data), pool());
  auto roundTrip = rowToCol.convert(bodies);
  ASSERT_EQ(roundTrip->size(), data->size());
  for (vector_size_t row = 0; row < roundTrip->size(); ++row) {
    EXPECT_TRUE(data->equalValueAt(roundTrip.get(), row, row));
  }
}

TEST_F(ShuffleMiscTest, ReuseBufferedInputStreamEmptyPartition) {
  auto outputType = ROW({"c0"}, {BIGINT()});
  auto streamIter = std::make_shared<MetricsTrackingStreamIterator>(
      std::vector<std::vector<char>>{});

  ShuffleReaderOptions readerOptions;
  EXPECT_TRUE(readerOptions.reuseBufferedInputStream);
  readerOptions.numPartitions = 8;
  readerOptions.forceShuffleWriterType = 0; // Adaptive
  readerOptions.partitionShortName = "hash";

  auto readerNode = std::make_shared<SparkShuffleReaderNode>(
      core::PlanNodeId("reader_empty"), outputType, readerOptions, streamIter);

  exec::test::CursorParameters params;
  params.planNode = readerNode;
  params.serialExecution = true;

  auto cursor = exec::test::TaskCursor::create(params);
  EXPECT_FALSE(cursor->moveNext());
  EXPECT_TRUE(streamIter->metricsUpdated);
  EXPECT_EQ(streamIter->outputRows, 0);
}

TEST_F(ShuffleMiscTest, ReuseBufferedInputStreamMultipleStreams) {
  auto outputType = ROW({"c0"}, {BIGINT()});
  auto* arrowPool = arrow::default_memory_pool();
  auto stream0 = serializeBigIntStream({10, 20, 30}, arrowPool);
  auto stream1 = serializeBigIntStream({40, 50}, arrowPool);
  auto streamIter = std::make_shared<MetricsTrackingStreamIterator>(
      std::vector<std::vector<char>>{std::move(stream0), std::move(stream1)});

  ShuffleReaderOptions readerOptions;
  EXPECT_TRUE(readerOptions.reuseBufferedInputStream);
  readerOptions.numPartitions = 8;
  readerOptions.forceShuffleWriterType = 0; // Adaptive
  readerOptions.partitionShortName = "hash";

  auto readerNode = std::make_shared<SparkShuffleReaderNode>(
      core::PlanNodeId("reader_multi"), outputType, readerOptions, streamIter);

  exec::test::CursorParameters params;
  params.planNode = readerNode;
  params.serialExecution = true;

  std::vector<int64_t> actual;
  auto cursor = exec::test::TaskCursor::create(params);
  while (cursor->moveNext()) {
    auto batch = cursor->current();
    auto* col = batch->childAt(0)->asFlatVector<int64_t>();
    ASSERT_NE(col, nullptr);
    for (vector_size_t i = 0; i < batch->size(); ++i) {
      ASSERT_FALSE(col->isNullAt(i));
      actual.push_back(col->valueAt(i));
    }
    cursor->current().reset();
  }

  EXPECT_EQ(actual, (std::vector<int64_t>{10, 20, 30, 40, 50}));
  EXPECT_TRUE(streamIter->metricsUpdated);
  EXPECT_EQ(streamIter->outputRows, 5);
}

} // namespace bytedance::bolt::shuffle::sparksql::test
