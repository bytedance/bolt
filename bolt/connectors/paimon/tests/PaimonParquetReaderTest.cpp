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
#include <filesystem>
#include <memory>
#include <string>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/connectors/paimon/PaimonConfig.h"
#include "bolt/connectors/paimon/PaimonParquetReader.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
#include "paimon/result.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::paimon;
using namespace bytedance::bolt::parquet;
using namespace bytedance::bolt::dwio::common;

namespace {

class PaimonParquetReaderTest : public ::testing::Test,
                                public bytedance::bolt::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::Options options;
    options.allocatorCapacity = 8L << 30;
    memory::MemoryManager::testingSetInstance(options);
  }

  void SetUp() override {
    dwio::common::LocalFileSink::registerFactory();
    pool_ = memory::memoryManager()->addRootPool("PaimonParquetReaderTest");
    leafPool_ = pool_->addLeafChild("leaf");
    tempDir_ = std::filesystem::temp_directory_path();
  }

  std::string tempPath(const std::string& filename) const {
    return (tempDir_ / filename).string();
  }

  std::unique_ptr<parquet::Writer> createWriter(
      const std::string& parquetPath,
      const RowTypePtr& schema) {
    auto sink =
        dwio::common::FileSink::create(parquetPath, {.pool = pool_.get()});
    auto writerPool = pool_->addLeafChild("writer");
    parquet::WriterOptions opts;
    opts.memoryPool = writerPool.get();
    opts.enableFlushBasedOnBlockSize = true;
    return std::make_unique<parquet::Writer>(
        std::move(sink), opts, pool_, ::arrow::default_memory_pool(), schema);
  }

  /// Open via PaimonParquetReader, read all rows, and compare against expected.
  /// When \p extraOptions is provided, its entries are passed through to
  /// PaimonParquetReader (e.g. kReadTimestampUnit for timestamp truncation).
  static void validateRead(
      const std::string& parquetPath,
      int32_t batchSize,
      const RowVectorPtr& expectedData,
      memory::MemoryPool* pool,
      const std::map<std::string, std::string>& extraOptions = {}) {
    const auto& options = extraOptions;
    PaimonParquetReader format(options);
    auto rbRes = format.CreateReaderBuilder(batchSize);
    ASSERT_TRUE(rbRes.ok());
    std::unique_ptr<::paimon::ReaderBuilder> builder = std::move(rbRes).value();

    auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(pool);
    builder->WithMemoryPool(paimonPool);

    auto readerRes = builder->Build(parquetPath);
    ASSERT_TRUE(readerRes.ok());
    std::unique_ptr<::paimon::FileBatchReader> fileReader =
        std::move(readerRes).value();

    // Validate row count when we have expected data.
    auto expectedRows = expectedData->size();
    if (expectedRows > 0) {
      auto rowCountRes = fileReader->GetNumberOfRows();
      ASSERT_TRUE(rowCountRes.ok());
      ASSERT_EQ(rowCountRes.value(), static_cast<uint64_t>(expectedRows));
    }

    // Collect all batches.
    std::vector<RowVectorPtr> batches;
    uint32_t reads(0);
    while (true) {
      auto batchRes = fileReader->NextBatch();
      if (::paimon::BatchReader::IsEofBatch(batchRes.value())) {
        break;
      }
      BOLT_CHECK(
          batchRes.ok(),
          "NextBatch failed({}): {}",
          batchRes.status().CodeAsString(),
          batchRes.status().message());

      auto pair = std::move(batchRes).value();

      auto batch = importFromArrowAsOwner(
          *pair.second, *pair.first, {}, expectedData->pool());
      auto rowBatch = std::dynamic_pointer_cast<RowVector>(batch);
      ASSERT_TRUE(rowBatch != nullptr);
      batches.push_back(rowBatch);

      if (pair.first && pair.first->release) {
        pair.first->release(pair.first.get());
      }
      if (pair.second && pair.second->release) {
        pair.second->release(pair.second.get());
      }
      reads++;
    }
    fileReader->Close();

    if (expectedRows > 0) {
      ASSERT_GT(reads, 0);
    }

    if (!batches.empty()) {
      auto actualData =
          RowVector::createEmpty(expectedData->type(), expectedData->pool());
      for (const auto& batch : batches) {
        actualData->append(batch.get());
      }
      assertVectorsEqual(expectedData, actualData);
    }
  }

  static void assertVectorsEqual(
      const RowVectorPtr& expected,
      const RowVectorPtr& actual) {
    ASSERT_EQ(expected->size(), actual->size());
    ASSERT_EQ(*expected->type(), *actual->type());
    for (vector_size_t i = 0; i < expected->size(); ++i) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), i, i))
          << "Row " << i << " expected: " << expected->toString(i)
          << " got: " << actual->toString(i);
    }
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::filesystem::path tempDir_;
};

// ---- Existing tests
// ----------------------------------------------------------

TEST_F(PaimonParquetReaderTest, PrimitiveTypes) {
  auto schema = ROW({"c0", "c1", "c2"}, {INTEGER(), DOUBLE(), BIGINT()});
  const int64_t kRows = 1000;
  auto data = makeRowVector(std::vector<VectorPtr>{
      makeFlatVector<int32_t>(kRows, [](auto r) { return r + 1; }),
      makeFlatVector<double>(kRows, [](auto r) { return r * 1.5; }),
      makeFlatVector<int64_t>(kRows, [](auto r) { return r * 10; }),
  });

  auto path = tempPath("paimon_parquet_primitives.parquet");
  auto writer = createWriter(path, schema);
  writer->write(data);
  writer->close();

  validateRead(path, 256, data, leafPool_.get());
}

TEST_F(PaimonParquetReaderTest, ArraysOfInts) {
  const int64_t kRows = 5;
  auto arrType = ARRAY(INTEGER());
  auto schema = ROW({"arr"}, {arrType});

  std::vector<std::vector<int32_t>> arrays = {{1, 2, 3}, {}, {4}, {5, 6}, {7}};
  auto arrVec = makeArrayVector<int32_t>(arrays);
  auto row = makeRowVector({arrVec});

  auto path = tempPath("paimon_parquet_arrays.parquet");
  auto writer = createWriter(path, schema);
  writer->write(row);
  writer->close();

  validateRead(path, 256, row, leafPool_.get());
}

TEST_F(PaimonParquetReaderTest, MapsStringToInt) {
  using S = StringView;
  const int64_t kRows = 4;
  auto mapType = MAP(VARCHAR(), INTEGER());
  auto schema = ROW({"mp"}, {mapType});

  std::vector<std::vector<std::pair<S, std::optional<int32_t>>>> maps = {
      {{S{"a"}, 1}, {S{"b"}, 2}},
      {},
      {{S{"c"}, std::nullopt}, {S{"d"}, 4}},
      {{S{"e"}, 5}}};

  auto mapVec = makeMapVector<S, int32_t>(maps, mapType);
  auto row = makeRowVector({mapVec});

  auto path = tempPath("paimon_parquet_maps.parquet");
  auto writer = createWriter(path, schema);
  writer->write(row);
  writer->close();

  validateRead(path, 128, row, leafPool_.get());
}

TEST_F(PaimonParquetReaderTest, MixedArrayAndMap) {
  using S = StringView;
  const int64_t kRows = 6;
  auto schema =
      ROW({"arr", "mp"}, {ARRAY(INTEGER()), MAP(VARCHAR(), INTEGER())});

  std::vector<std::vector<int32_t>> arrays = {
      {1}, {}, {2, 3}, {4}, {}, {5, 6, 7}};
  auto arrVec = makeArrayVector<int32_t>(arrays);

  std::vector<std::vector<std::pair<S, std::optional<int32_t>>>> maps = {
      {{S{"a"}, 1}},
      {},
      {{S{"b"}, std::nullopt}},
      {{S{"c"}, 3}, {S{"d"}, 4}},
      {},
      {{S{"e"}, 5}}};
  auto mapVec = makeMapVector<S, int32_t>(maps);

  auto row = makeRowVector({arrVec, mapVec});

  auto path = tempPath("paimon_parquet_mixed.parquet");
  auto writer = createWriter(path, schema);
  writer->write(row);
  writer->close();

  validateRead(path, 256, row, leafPool_.get());
}

// ---- Timestamp precision test
// ------------------------------------------------

TEST_F(PaimonParquetReaderTest, TimestampPrecision) {
  // Verify kReadTimestampUnit config threads through PaimonDataSource →
  // PaimonParquetReader → RowReaderOptions.setTimestampPrecision().
  // Uses INT64-encoded timestamps (microsecond precision on disk).

  auto schema = ROW({"ts", "value"}, {TIMESTAMP(), BIGINT()});

  const auto parseTs = [](std::string_view s) {
    return util::fromTimestampString(s.data(), s.size(), nullptr);
  };

  // Timestamps with sub-millisecond precision to observe truncation.
  auto writtenData = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<Timestamp>({
          parseTs("2015-06-01 19:34:56.123456"),
          parseTs("2023-04-21 09:09:34.567890"),
          std::nullopt,
      }),
      makeFlatVector<int64_t>({10, 20, 30}),
  });

  // Expected at millisecond precision: sub-ms digits zeroed.
  auto expectedMilli = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<Timestamp>({
          parseTs("2015-06-01 19:34:56.123000"),
          parseTs("2023-04-21 09:09:34.567000"),
          std::nullopt,
      }),
      makeFlatVector<int64_t>({10, 20, 30}),
  });

  // Expected at microsecond precision: full INT64 native precision preserved.
  auto expectedMicro = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<Timestamp>({
          parseTs("2015-06-01 19:34:56.123456"),
          parseTs("2023-04-21 09:09:34.567890"),
          std::nullopt,
      }),
      makeFlatVector<int64_t>({10, 20, 30}),
  });

  // Write with default INT64 encoding (microsecond precision on disk).
  auto path = tempPath("ts_precision.parquet");
  {
    auto sink = dwio::common::FileSink::create(path, {.pool = pool_.get()});
    parquet::WriterOptions opts;
    opts.memoryPool = pool_->addLeafChild("writer").get();
    opts.enableFlushBasedOnBlockSize = true;
    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink), opts, pool_, ::arrow::default_memory_pool(), schema);
    writer->write(writtenData);
    writer->close();
  }

  // Millisecond precision (default): sub-ms truncated.
  validateRead(path, 256, expectedMilli, leafPool_.get());

  // Explicit milli option should give same result.
  validateRead(
      path,
      256,
      expectedMilli,
      leafPool_.get(),
      {{PaimonConfig::kReadTimestampUnit, "3"}});

  // Microsecond precision: preserves INT64's native micro precision.
  validateRead(
      path,
      256,
      expectedMicro,
      leafPool_.get(),
      {{PaimonConfig::kReadTimestampUnit, "6"}});

  // ---- INT96-encoded timestamps ----
  // INT96 stores nanosecond precision on disk (days + nanos).
  // Verify precision config truncates correctly for this encoding too.
  {
    auto int96Path = tempPath("ts_precision_int96.parquet");
    {
      auto sink =
          dwio::common::FileSink::create(int96Path, {.pool = pool_.get()});
      parquet::WriterOptions opts;
      opts.memoryPool = pool_->addLeafChild("int96_writer").get();
      opts.enableFlushBasedOnBlockSize = true;
      opts.writeInt96AsTimestamp = true;
      auto writer = std::make_unique<parquet::Writer>(
          std::move(sink), opts, pool_, ::arrow::default_memory_pool(), schema);
      writer->write(writtenData);
      writer->close();
    }

    // Millisecond precision: sub-ms digits zeroed.
    validateRead(
        int96Path,
        256,
        expectedMilli,
        leafPool_.get(),
        {{PaimonConfig::kReadTimestampUnit, "3"}});

    // Microsecond precision: sub-us digits zeroed.
    validateRead(
        int96Path,
        256,
        expectedMicro,
        leafPool_.get(),
        {{PaimonConfig::kReadTimestampUnit, "6"}});

    // Nanosecond precision: full INT96 native precision preserved.
    validateRead(
        int96Path,
        256,
        writtenData,
        leafPool_.get(),
        {{PaimonConfig::kReadTimestampUnit, "9"}});
  }
}

// ---- Full-schema fallback test (initializeRowReaderWithFullSchema)
// ----

TEST_F(PaimonParquetReaderTest, NextBatchWithoutSetReadSchema) {
  auto schema = ROW({"c0", "c1", "c2"}, {INTEGER(), DOUBLE(), VARCHAR()});
  const int64_t kRows = 100;
  std::vector<std::string> stringData(kRows);
  for (int64_t i = 0; i < kRows; ++i) {
    stringData[i] = "row_" + std::to_string(i);
  }
  auto data = makeRowVector(std::vector<VectorPtr>{
      makeFlatVector<int32_t>(kRows, [](auto r) { return r + 1; }),
      makeFlatVector<double>(kRows, [](auto r) { return r * 2.5; }),
      makeFlatVector<StringView>(
          kRows, [&](auto r) { return StringView(stringData[r]); }),
  });

  auto path = tempPath("paimon_parquet_full_schema_fallback.parquet");
  auto writer = createWriter(path, schema);
  writer->write(data);
  writer->close();

  // Open the file and call NextBatch directly WITHOUT SetReadSchema.
  PaimonParquetReader format({});
  auto rbRes = format.CreateReaderBuilder(256);
  ASSERT_TRUE(rbRes.ok());
  std::unique_ptr<::paimon::ReaderBuilder> builder = std::move(rbRes).value();

  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool_.get());
  builder->WithMemoryPool(paimonPool);

  auto readerRes = builder->Build(path);
  ASSERT_TRUE(readerRes.ok());
  std::unique_ptr<::paimon::FileBatchReader> fileReader =
      std::move(readerRes).value();

  // Call NextBatch directly — this should trigger the full-schema fallback.
  auto batchRes = fileReader->NextBatch();
  ASSERT_TRUE(batchRes.ok())
      << "NextBatch failed: " << batchRes.status().message();

  auto pair = std::move(batchRes).value();
  ASSERT_NE(pair.first, nullptr);
  ASSERT_NE(pair.second, nullptr);

  // Verify the returned schema matches the file schema (all 3 columns).
  auto batch =
      importFromArrowAsOwner(*pair.second, *pair.first, {}, leafPool_.get());
  auto rowBatch = std::dynamic_pointer_cast<RowVector>(batch);
  ASSERT_NE(rowBatch, nullptr);
  auto rowType = std::dynamic_pointer_cast<const RowType>(rowBatch->type());
  ASSERT_NE(rowType, nullptr);
  ASSERT_EQ(rowType->size(), 3)
      << "Full-schema fallback should return all columns";
  ASSERT_EQ(rowType->nameOf(0), "c0");
  ASSERT_EQ(rowType->nameOf(1), "c1");
  ASSERT_EQ(rowType->nameOf(2), "c2");

  // Verify the row count matches.
  int64_t totalRows = rowBatch->size();
  while (true) {
    auto nextRes = fileReader->NextBatch();
    if (::paimon::BatchReader::IsEofBatch(nextRes.value())) {
      break;
    }
    auto nextPair = std::move(nextRes).value();
    auto nextBatch = importFromArrowAsOwner(
        *nextPair.second, *nextPair.first, {}, leafPool_.get());
    auto nextRow = std::dynamic_pointer_cast<RowVector>(nextBatch);
    ASSERT_NE(nextRow, nullptr);
    totalRows += nextRow->size();
    if (nextPair.first && nextPair.first->release) {
      nextPair.first->release(nextPair.first.get());
    }
    if (nextPair.second && nextPair.second->release) {
      nextPair.second->release(nextPair.second.get());
    }
  }
  ASSERT_EQ(totalRows, kRows);

  fileReader->Close();
}

/// Helper: open a parquet file via PaimonParquetReader and return the
/// FileBatchReader. Does NOT call SetReadSchema — use for testing
/// GetPreviousBatchFirstRowNumber with full-schema fallback.
std::unique_ptr<::paimon::FileBatchReader> openReader(
    const std::string& parquetPath,
    int32_t batchSize,
    memory::MemoryPool* pool,
    const std::map<std::string, std::string>& extraOptions = {}) {
  PaimonParquetReader format(extraOptions);
  auto rbRes = format.CreateReaderBuilder(batchSize);
  EXPECT_TRUE(rbRes.ok()) << rbRes.status().ToString();
  std::unique_ptr<::paimon::ReaderBuilder> builder = std::move(rbRes).value();

  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(pool);
  builder->WithMemoryPool(paimonPool);

  auto readerRes = builder->Build(parquetPath);
  EXPECT_TRUE(readerRes.ok()) << readerRes.status().message();
  return std::move(readerRes).value();
}

// ---- GetPreviousBatchFirstRowNumber tests
// ----------------------------------------------------------

TEST_F(PaimonParquetReaderTest, PreviousBatchRowNumberSingleBatch) {
  // Single batch: all rows fit in one read, so first batch starts at row 0.
  const int64_t kRows = 100;
  auto schema = ROW({"id"}, {BIGINT()});
  auto data =
      makeRowVector({makeFlatVector<int64_t>(kRows, [](auto r) { return r; })});

  auto path = tempPath("row_number_single.parquet");
  auto writer = createWriter(path, schema);
  writer->write(data);
  writer->close();

  auto reader = openReader(path, kRows, leafPool_.get());

  // First batch should start at row 0.
  auto rowRes = reader->GetPreviousBatchFirstRowNumber();
  ASSERT_TRUE(rowRes.ok()) << rowRes.status().message();
  EXPECT_EQ(rowRes.value(), 0U);

  // Read the only batch.
  auto batchRes = reader->NextBatch();
  ASSERT_TRUE(batchRes.ok());
  EXPECT_FALSE(::paimon::BatchReader::IsEofBatch(batchRes.value()));

  // Release Arrow data to free underlying memory.
  {
    auto pair = std::move(batchRes).value();
    if (pair.first && pair.first->release) {
      pair.first->release(pair.first.get());
    }
    if (pair.second && pair.second->release) {
      pair.second->release(pair.second.get());
    }
  }

  // After reading, next call still returns the same batch's start (or EOF).
  reader->Close();
}

TEST_F(PaimonParquetReaderTest, PreviousBatchRowNumberMultipleBatches) {
  // Small batch size forces multiple reads from a larger file.
  const int64_t kRows = 100;
  const int32_t kBatchSize = 17; // doesn't divide evenly
  auto schema = ROW({"id"}, {BIGINT()});
  auto data = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto r) { return r * 10; })});

  auto path = tempPath("row_number_multi.parquet");
  auto writer = createWriter(path, schema);
  writer->write(data);
  writer->close();

  auto reader = openReader(path, kBatchSize, leafPool_.get());

  uint64_t totalRows = 0;
  uint64_t expectedStart = 0;
  while (true) {
    auto batchRes = reader->NextBatch();
    if (::paimon::BatchReader::IsEofBatch(batchRes.value())) {
      break;
    }
    ASSERT_TRUE(batchRes.ok()) << batchRes.status().message();

    // After NextBatch(), GetPreviousBatchFirstRowNumber() returns the start
    // row of the batch just read.
    auto rowRes = reader->GetPreviousBatchFirstRowNumber();
    ASSERT_TRUE(rowRes.ok()) << rowRes.status().message();
    EXPECT_EQ(rowRes.value(), expectedStart)
        << "Batch starting at wrong absolute row position";

    auto pair = std::move(batchRes).value();
    auto batch =
        importFromArrowAsOwner(*pair.second, *pair.first, {}, leafPool_.get());
    auto rowBatch = std::dynamic_pointer_cast<RowVector>(batch);
    ASSERT_NE(rowBatch, nullptr);
    uint64_t batchRows = rowBatch->size();
    totalRows += batchRows;

    // Release Arrow data to free underlying memory.
    if (pair.first && pair.first->release) {
      pair.first->release(pair.first.get());
    }
    if (pair.second && pair.second->release) {
      pair.second->release(pair.second.get());
    }

    expectedStart += batchRows;
  }

  EXPECT_EQ(totalRows, static_cast<uint64_t>(kRows));
  reader->Close();
}

TEST_F(
    PaimonParquetReaderTest,
    PreviousBatchRowNumberReturnsZeroBeforeAnyRead) {
  // Before any NextBatch() call, the method should return 0.
  const int64_t kRows = 50;
  auto schema = ROW({"id"}, {BIGINT()});
  auto data =
      makeRowVector({makeFlatVector<int64_t>(kRows, [](auto r) { return r; })});

  auto path = tempPath("row_number_before_read.parquet");
  auto writer = createWriter(path, schema);
  writer->write(data);
  writer->close();

  auto reader = openReader(path, 20, leafPool_.get());

  auto rowRes = reader->GetPreviousBatchFirstRowNumber();
  ASSERT_TRUE(rowRes.ok()) << rowRes.status().message();
  EXPECT_EQ(rowRes.value(), 0U);

  reader->Close();
}

} // namespace
