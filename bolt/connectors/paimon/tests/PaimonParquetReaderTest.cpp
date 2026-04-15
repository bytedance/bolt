/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <string>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/connectors/paimon/PaimonParquetReader.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/type/StringView.h"
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

class PaimonParquetReaderTest : public testing::Test,
                                public bytedance::bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    // Register local filesystem sink for writing to local paths
    bytedance::bolt::dwio::common::LocalFileSink::registerFactory();
  }

  void SetUp() override {
    rootPool_ = memory::memoryManager()->addRootPool("PaimonParquetReaderTest");
    leafPool_ = rootPool_->addLeafChild("Leaf");
    tempDir_ = std::filesystem::temp_directory_path();
  }

  std::string tempPath(const std::string& filename) const {
    auto path = (tempDir_ / filename).string();
    return path;
  }

  std::unique_ptr<bytedance::bolt::parquet::Writer> createWriter(
      const std::string& parquetPath,
      const RowTypePtr& schema) {
    auto sink =
        dwio::common::FileSink::create(parquetPath, {.pool = leafPool_.get()});
    bytedance::bolt::parquet::WriterOptions opts;
    opts.memoryPool = leafPool_.get();
    opts.enableFlushBasedOnBlockSize = true;
    return std::make_unique<bytedance::bolt::parquet::Writer>(
        std::move(sink),
        opts,
        rootPool_,
        ::arrow::default_memory_pool(),
        schema);
  }

  // Open via PaimonParquetReader and validate row count and batch iteration
  static void validateRead(
      const std::string& parquetPath,
      int32_t batchSize,
      const RowVectorPtr& expectedData,
      memory::MemoryPool* pool) {
    PaimonParquetReader format({});
    auto rbRes = format.CreateReaderBuilder(batchSize);
    ASSERT_TRUE(rbRes.ok());
    std::unique_ptr<::paimon::ReaderBuilder> builder = std::move(rbRes).value();

    auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(pool);
    builder->WithMemoryPool(paimonPool);

    auto readerRes = builder->Build(parquetPath);
    ASSERT_TRUE(readerRes.ok());
    std::unique_ptr<::paimon::FileBatchReader> fileReader =
        std::move(readerRes).value();

    // Read expected number of rows
    auto expectedRows = expectedData->size();
    auto rowCountRes = fileReader->GetNumberOfRows();
    if (expectedRows > 0) {
      ASSERT_TRUE(rowCountRes.ok());
      ASSERT_EQ(rowCountRes.value(), static_cast<uint64_t>(expectedRows));
    }

    // Iterate batches until end and collect data
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
      auto& arr = pair.first;
      auto& sch = pair.second;

      // Convert Arrow array to RowVectorPtr
      auto type = importFromArrow(*sch);
      auto rowType = std::dynamic_pointer_cast<const RowType>(type);
      ASSERT_TRUE(rowType != nullptr);

      auto batch = importFromArrowAsOwner(*sch, *arr, {}, expectedData->pool());
      auto rowBatch = std::dynamic_pointer_cast<RowVector>(batch);
      ASSERT_TRUE(rowBatch != nullptr);

      batches.push_back(rowBatch);
      reads++;

      if (arr && arr->release) {
        arr->release(arr.get());
      }
      if (sch && sch->release) {
        sch->release(sch.get());
      }
    }
    fileReader->Close();

    if (expectedRows > 0) {
      ASSERT_GT(reads, 0);
    }

    // Concatenate all batches into a single result vector
    if (!batches.empty()) {
      auto actualData =
          RowVector::createEmpty(expectedData->type(), expectedData->pool());
      for (const auto& batch : batches) {
        actualData->append(batch.get());
      }

      // Compare actual data with expected data
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

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::filesystem::path tempDir_;
};

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
  using namespace bytedance::bolt::test;
  const int64_t kRows = 5;
  auto arrType = ARRAY(INTEGER());
  auto schema = ROW({"arr"}, {arrType});

  // Create array column with varying sizes
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
  using S = bytedance::bolt::StringView;
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
  using S = bytedance::bolt::StringView;
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

} // namespace
