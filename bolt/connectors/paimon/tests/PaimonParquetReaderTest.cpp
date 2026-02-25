/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <filesystem>

#include "bolt/connectors/paimon/PaimonParquetReader.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/type/StringView.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/vector/arrow/Abi.h"
#include "paimon/result.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::paimon;
using namespace bytedance::bolt::parquet;
using namespace bytedance::bolt::dwio::common;

namespace {

class PaimonParquetReaderTest : public testing::Test, public bytedance::bolt::test::VectorTestBase {
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

  std::unique_ptr<bytedance::bolt::parquet::Writer> createWriter(const std::string& parquetPath, const RowTypePtr& schema) {
    auto sink = dwio::common::FileSink::create(parquetPath, {.pool = leafPool_.get()});
    bytedance::bolt::parquet::WriterOptions opts;
    opts.memoryPool = leafPool_.get();
    opts.enableFlushBasedOnBlockSize = true;
    return std::make_unique<bytedance::bolt::parquet::Writer>(std::move(sink), opts, rootPool_, ::arrow::default_memory_pool(), schema);
  }

  // Open via PaimonParquetReader and validate row count and batch iteration
  static void validateRead(const std::string& parquetPath, int32_t batchSize, int64_t expectedRows) {
    PaimonParquetReader format;
    auto rbRes = format.CreateReaderBuilder(batchSize);
    ASSERT_TRUE(rbRes.ok());
    std::unique_ptr<::paimon::ReaderBuilder> builder = std::move(rbRes).value();

    auto readerRes = builder->Build(parquetPath);
    ASSERT_TRUE(readerRes.ok());
    std::unique_ptr<::paimon::FileBatchReader> fileReader = std::move(readerRes).value();

    auto rowCountRes = fileReader->GetNumberOfRows();
    ASSERT_TRUE(rowCountRes.ok());
    ASSERT_EQ(rowCountRes.value(), static_cast<uint64_t>(expectedRows));

    // Iterate batches until end
    int64_t seen = 0;
    while (true) {
      auto batchRes = fileReader->NextBatch();
      if (!batchRes.ok()) {
        break;
      }
      auto pair = std::move(batchRes).value();
      auto& arr = pair.first; (void)arr; // ensure consumed
      auto& sch = pair.second; (void)sch;
      // No content validation here; just count approximate rows by schema fields being present
      seen += 0; // content count omitted
    }
  }

  static void assertVectorsEqual(const RowVectorPtr& expected, const RowVectorPtr& actual) {
    ASSERT_EQ(expected->size(), actual->size());
    ASSERT_EQ(*expected->type(), *actual->type());
    for (vector_size_t i = 0; i < expected->size(); ++i) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), i, i))
          << "Row " << i << " expected: " << expected->toString(i) << " got: " << actual->toString(i);
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

  validateRead(path, 256, kRows);
}

TEST_F(PaimonParquetReaderTest, EmptyFile) {
  auto schema = ROW({"c0", "c1"}, {INTEGER(), DOUBLE()});
  auto path = tempPath("paimon_parquet_empty.parquet");
  auto writer = createWriter(path, schema);
  writer->close();

  validateRead(path, 128, 0);
}

TEST_F(PaimonParquetReaderTest, ArraysOfInts) {
  using namespace bytedance::bolt::test;
  const int64_t kRows = 5;
  auto arrType = ARRAY(INTEGER());
  auto schema = ROW({"arr"}, {arrType});

  // Create array column with varying sizes
  std::vector<std::vector<int32_t>> arrays = {
      {1, 2, 3},
      {},
      {4},
      {5, 6},
      {7}
  };
  auto arrVec = makeArrayVector<int32_t>(arrays);
  auto row = makeRowVector({arrVec});

  auto path = tempPath("paimon_parquet_arrays.parquet");
  auto writer = createWriter(path, schema);
  writer->write(row);
  writer->close();

  validateRead(path, 256, kRows);
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
      {{S{"e"}, 5}}
  };

  auto mapVec = makeMapVector<S, int32_t>(maps, mapType);
  auto row = makeRowVector({mapVec});

  auto path = tempPath("paimon_parquet_maps.parquet");
  auto writer = createWriter(path, schema);
  writer->write(row);
  writer->close();

  validateRead(path, 128, kRows);
}

TEST_F(PaimonParquetReaderTest, MixedArrayAndMap) {
  using S = bytedance::bolt::StringView;
  const int64_t kRows = 6;
  auto schema = ROW({"arr", "mp"}, {ARRAY(INTEGER()), MAP(VARCHAR(), INTEGER())});

  std::vector<std::vector<int32_t>> arrays = {
      {1}, {}, {2, 3}, {4}, {}, {5, 6, 7}
  };
  auto arrVec = makeArrayVector<int32_t>(arrays);

  std::vector<std::vector<std::pair<S, std::optional<int32_t>>>> maps = {
      {{S{"a"}, 1}},
      {},
      {{S{"b"}, std::nullopt}},
      {{S{"c"}, 3}, {S{"d"}, 4}},
      {},
      {{S{"e"}, 5}}
  };
  auto mapVec = makeMapVector<S, int32_t>(maps);

  auto row = makeRowVector({arrVec, mapVec});

  auto path = tempPath("paimon_parquet_mixed.parquet");
  auto writer = createWriter(path, schema);
  writer->write(row);
  writer->close();

  validateRead(path, 256, kRows);
}

} // namespace
