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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>

#include <fstream>
#include <iterator>

#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/shuffle/sparksql/BoltShuffleReader.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/tests/MockRssClient.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

constexpr int32_t kCompressionThreshold = 100;
constexpr int32_t kNumColumns = 20;
constexpr int64_t kMemoryLimit = 1LL << 30;

struct TestParam {
  PartitionWriterType writerType;
  arrow::Compression::type codec;
  int32_t numRows;
  bool evict;
  bool multipleBatches;
};

class BoltShuffleWriterV2Test : public testing::TestWithParam<TestParam>,
                                public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void testRoundTrip() {
    const auto& param = GetParam();
    auto* arrowPool = arrow::default_memory_pool();
    auto rssClient = std::make_shared<MockRssClient>();
    auto tempDir = exec::test::TempDirectoryPath::create();
    const auto dataFile = tempDir->path + "/shuffle.bin";

    // The default row-vector optimization requires at least 20 simple columns.
    // Include strings and nulls to validate both lengths and validity buffers.
    std::vector<VectorPtr> children;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int32_t column = 0; column < kNumColumns; ++column) {
      const bool isString = column == kNumColumns - 1;
      if (isString) {
        children.push_back(
            makeFlatVector<std::string>(param.numRows, [](auto row) {
              return "shuffle-string-value-" + std::to_string(row);
            }));
      } else {
        children.push_back(makeFlatVector<int64_t>(
            param.numRows,
            [column](auto row) { return column * 10000 + row; }));
      }
      for (vector_size_t row = 0; row < param.numRows; ++row) {
        if ((row + column) % 13 == 0) {
          children.back()->setNull(row, true);
        }
      }
      fields.push_back(arrow::field(
          "c" + std::to_string(column),
          isString ? arrow::utf8() : arrow::int64()));
    }
    auto expected = makeRowVector(children);
    const auto rowType =
        std::dynamic_pointer_cast<const RowType>(expected->type());
    children.insert(
        children.begin(),
        makeFlatVector<int32_t>(param.numRows, [](auto) { return 0; }));
    auto input = makeRowVector(children);

    ShuffleWriterOptions options;
    options.partitioning = Partitioning::kRange;
    options.forceShuffleWriterType =
        static_cast<int32_t>(ShuffleWriterType::V2);
    options.enableVectorCombination = false;
    options.bufferSize = 4096;
    auto& partitionOptions = options.partitionWriterOptions;
    partitionOptions.numPartitions = 2; // The second partition stays empty.
    partitionOptions.partitionWriterType = param.writerType;
    partitionOptions.compressionType = param.codec;
    partitionOptions.compressionThreshold = kCompressionThreshold;
    partitionOptions.rssClient = rssClient;
    partitionOptions.dataFile = dataFile;
    partitionOptions.configuredDirs = {tempDir->path};
    partitionOptions.numSubDirs = 1;
    auto writer = BoltShuffleWriter::create(
        options,
        kNumColumns,
        param.numRows,
        input->estimateFlatSize(),
        kMemoryLimit,
        pool(),
        arrowPool);
    ASSERT_EQ(writer->toString(), "BoltShuffleWriterV2");

    auto firstInput = param.multipleBatches
        ? std::dynamic_pointer_cast<RowVector>(
              input->slice(0, param.numRows / 2))
        : input;
    auto status = writer->split(firstInput, kMemoryLimit);
    ASSERT_TRUE(status.ok()) << status.ToString();
    if (param.multipleBatches) {
      auto remainingInput = std::dynamic_pointer_cast<RowVector>(
          input->slice(param.numRows / 2, param.numRows - param.numRows / 2));
      status = writer->split(remainingInput, kMemoryLimit);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }

    const bool isCeleborn = param.writerType == PartitionWriterType::kCeleborn;
    if (isCeleborn) {
      ASSERT_TRUE(rssClient->getData().empty());
    }
    if (param.evict) {
      // Trigger tryEvict() before splitting another batch. Its 8 MiB minimum
      // reserve exceeds one quarter of this budget, causing kCacheNoMerge
      // eviction without requiring an external memory reclaimer.
      status = writer->split(input, 16LL << 20);
      ASSERT_TRUE(status.ok()) << status.ToString();
      if (isCeleborn) {
        ASSERT_FALSE(rssClient->getData().empty());
      }
    }
    status = writer->stop();
    ASSERT_TRUE(status.ok()) << status.ToString();
    const auto& metrics = writer->metrics();
    ASSERT_EQ(metrics.partitionLengths.size(), 2);
    EXPECT_EQ(metrics.partitionLengths[1], 0);

    // UNCOMPRESSED uses a non-null copy codec and supports row-vector framing.
    // Only Celeborn partitions below the threshold require buffer mode.
    const bool expectRowVector =
        !isCeleborn || param.numRows >= kCompressionThreshold;
    const auto numPayloads = param.evict ? 2 : 1;
    ASSERT_EQ(metrics.rowVectorModeCompress, expectRowVector ? numPayloads : 0);

    std::string data;
    if (isCeleborn) {
      ASSERT_EQ(rssClient->getData().size(), 1);
      const auto& bytes = rssClient->getData().at(0);
      data.assign(bytes.begin(), bytes.end());
    } else {
      std::ifstream stream(dataFile, std::ios::binary);
      ASSERT_TRUE(stream.is_open());
      data.assign(std::istreambuf_iterator<char>(stream), {});
    }
    ASSERT_FALSE(data.empty());
    std::shared_ptr<Codec> codec = createCodec(
        param.codec,
        CodecOptions{getCodecBackend("none"), kDefaultCompressionLevel});
    BoltColumnarBatchDeserializerFactory factory(
        arrow::schema(fields),
        codec,
        rowType,
        4096,
        1 << 20,
        arrowPool,
        pool());
    factory.setpartitioningShortName("range");
    factory.setNumPartitions(2);
    factory.setShuffleWriterType(static_cast<int32_t>(ShuffleWriterType::V2));
    auto reader =
        factory.createDeserializer(std::make_shared<arrow::io::BufferReader>(
            arrow::Buffer::FromString(std::move(data))));

    const auto expectedRows = param.numRows * numPayloads;
    vector_size_t totalRows = 0;
    while (auto batch = reader->next()) {
      ASSERT_EQ(batch->childrenSize(), kNumColumns);
      for (vector_size_t row = 0; row < batch->size(); ++row) {
        ASSERT_LT(totalRows, expectedRows);
        ASSERT_TRUE(
            batch->equalValueAt(expected.get(), row, totalRows % param.numRows))
            << "Round-trip mismatch at row " << totalRows;
        ++totalRows;
      }
    }
    EXPECT_EQ(totalRows, expectedRows);
  }
};

TEST_P(BoltShuffleWriterV2Test, compressionThresholdRoundTrip) {
  testRoundTrip();
}

std::vector<TestParam> testParams() {
  std::vector<TestParam> params;
  for (auto writerType :
       {PartitionWriterType::kCeleborn, PartitionWriterType::kLocal}) {
    for (auto codec :
         {arrow::Compression::LZ4_FRAME,
          arrow::Compression::ZSTD,
          arrow::Compression::UNCOMPRESSED}) {
      for (auto numRows :
           {kCompressionThreshold - 1,
            kCompressionThreshold,
            kCompressionThreshold + 1}) {
        for (bool evict : {false, true}) {
          for (bool multipleBatches : {false, true}) {
            params.push_back(
                {writerType, codec, numRows, evict, multipleBatches});
          }
        }
      }
    }
  }
  return params;
}

INSTANTIATE_TEST_SUITE_P(
    CompressionThreshold,
    BoltShuffleWriterV2Test,
    testing::ValuesIn(testParams()),
    [](const testing::TestParamInfo<TestParam>& info) {
      const auto& param = info.param;
      const auto codecName = param.codec == arrow::Compression::UNCOMPRESSED
          ? "Uncompressed"
          : (param.codec == arrow::Compression::ZSTD ? "Zstd" : "Lz4");
      return std::string(
                 param.writerType == PartitionWriterType::kCeleborn ? "Celeborn"
                                                                    : "Local") +
          (param.evict ? "Evict" : "Stop") +
          (param.multipleBatches ? "MultipleBatches" : "SingleBatch") +
          codecName + "Rows" + std::to_string(param.numRows);
    });

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::test
