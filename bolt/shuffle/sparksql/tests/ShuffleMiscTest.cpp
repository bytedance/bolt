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

#include <numeric>
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/partitioner/HashPartitioner.h"
#include "bolt/shuffle/sparksql/tests/ShuffleTestBase.h"

namespace bytedance::bolt::shuffle::sparksql::test {

namespace {

class TestBoltShuffleWriter : public BoltShuffleWriter {
 public:
  TestBoltShuffleWriter(
      ShuffleWriterOptions options,
      bytedance::bolt::memory::MemoryPool* boltPool,
      arrow::MemoryPool* pool)
      : BoltShuffleWriter(std::move(options), boltPool, pool) {}

  arrow::Status initialize() {
    return init();
  }

  arrow::Status buildPartitionRowsWithInjectedPid(
      const bytedance::bolt::RowVector& rv,
      uint32_t badPid) {
    auto status = initFromRowVector(rv);
    if (!status.ok()) {
      return status;
    }
    row2Partition_.assign(rv.size(), badPid);
    std::fill(partition2RowCount_.begin(), partition2RowCount_.end(), 0);
    partition2RowCount_[0] = rv.size();
    return buildPartition2Row(rv.size());
  }

  arrow::Status splitWithInjectedRowId(
      const bytedance::bolt::RowVector& rv,
      uint32_t badRowId) {
    auto status = initFromRowVector(rv);
    if (!status.ok()) {
      return status;
    }
    std::fill(partitionBufferBase_.begin(), partitionBufferBase_.end(), 0);
    std::fill(partitionBufferSize_.begin(), partitionBufferSize_.end(), 0);
    std::fill(partition2RowCount_.begin(), partition2RowCount_.end(), 0);
    partition2RowCount_[0] = 1;
    partitionUsed_ = {0};
    std::fill(
        partition2RowOffsetBase_.begin(), partition2RowOffsetBase_.end(), 0);
    partition2RowOffsetBase_[1] = 1;
    rowOffset2RowId_.assign(1, badRowId);
    status = updateInputHasNull(rv);
    if (!status.ok()) {
      return status;
    }
    status = preAllocPartitionBuffers(1);
    if (!status.ok()) {
      return status;
    }
    return splitRowVector(rv);
  }

  arrow::Status splitWithInjectedRowRange(
      const bytedance::bolt::RowVector& rv,
      uint32_t endOffset) {
    auto status = initFromRowVector(rv);
    if (!status.ok()) {
      return status;
    }
    std::fill(partitionBufferBase_.begin(), partitionBufferBase_.end(), 0);
    std::fill(partitionBufferSize_.begin(), partitionBufferSize_.end(), 0);
    std::fill(partition2RowCount_.begin(), partition2RowCount_.end(), 0);
    partition2RowCount_[0] = endOffset;
    partitionUsed_ = {0};
    std::fill(
        partition2RowOffsetBase_.begin(), partition2RowOffsetBase_.end(), 0);
    partition2RowOffsetBase_[1] = endOffset;
    rowOffset2RowId_.assign(1, 0);
    status = updateInputHasNull(rv);
    if (!status.ok()) {
      return status;
    }
    status = preAllocPartitionBuffers(endOffset);
    if (!status.ok()) {
      return status;
    }
    return splitRowVector(rv);
  }
};

ShuffleWriterOptions makeWriterOptions(const std::string& baseDir) {
  ShuffleWriterOptions options;
  options.partitioning = Partitioning::kHash;
  options.partitionWriterOptions.partitionWriterType =
      PartitionWriterType::kLocal;
  options.partitionWriterOptions.numPartitions = 2;
  options.partitionWriterOptions.dataFile = baseDir + "/shuffle.bin";
  options.partitionWriterOptions.configuredDirs = {baseDir};
  options.partitionWriterOptions.numSubDirs = 1;
  options.bufferSize = 4;
  return options;
}

} // namespace

class ShuffleMiscTest : public ShuffleTestBase {};

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

TEST_F(ShuffleMiscTest, HashPartitionerNormalizesNegativeHashes) {
  HashPartitioner partitioner(4);
  std::vector<uint32_t> row2Partition;
  std::vector<uint32_t> partition2RowCount(4, 0);
  const std::vector<int32_t> hashes = {-1, -2, -3, -4, -5, 0, 1, 5};

  ASSERT_TRUE(
      partitioner
          .compute(
              hashes.data(), hashes.size(), row2Partition, partition2RowCount)
          .ok());

  ASSERT_EQ(row2Partition.size(), hashes.size());
  for (size_t i = 0; i < hashes.size(); ++i) {
    const auto expected = ((hashes[i] % 4) + 4) % 4;
    EXPECT_EQ(row2Partition[i], expected) << "hash=" << hashes[i];
  }
  EXPECT_EQ(
      std::accumulate(
          partition2RowCount.begin(), partition2RowCount.end(), uint32_t{0}),
      hashes.size());
}

TEST_F(ShuffleMiscTest, BuildPartition2RowRejectsInvalidPartitionId) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  TestBoltShuffleWriter writer(
      makeWriterOptions(tempDir->path), pool(), arrow::default_memory_pool());
  ASSERT_TRUE(writer.initialize().ok());

  auto rv = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  auto status = writer.buildPartitionRowsWithInjectedPid(*rv, 2);

  ASSERT_FALSE(status.ok());
  EXPECT_NE(
      status.message().find("buildPartition2Row: invalid partition id"),
      std::string::npos);
}

TEST_F(ShuffleMiscTest, SplitRowVectorRejectsOutOfBoundsRowId) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  TestBoltShuffleWriter writer(
      makeWriterOptions(tempDir->path), pool(), arrow::default_memory_pool());
  ASSERT_TRUE(writer.initialize().ok());

  auto rv = makeRowVector({makeFlatVector<int64_t>({1})});
  auto status = writer.splitWithInjectedRowId(*rv, rv->size());

  ASSERT_FALSE(status.ok());
  EXPECT_NE(status.message().find("source rowId"), std::string::npos);
}

TEST_F(ShuffleMiscTest, SplitRowVectorRejectsInvalidPartitionRowRange) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  TestBoltShuffleWriter writer(
      makeWriterOptions(tempDir->path), pool(), arrow::default_memory_pool());
  ASSERT_TRUE(writer.initialize().ok());

  auto rv = makeRowVector({makeFlatVector<int64_t>({1})});
  auto status = writer.splitWithInjectedRowRange(*rv, 2);

  ASSERT_FALSE(status.ok());
  EXPECT_NE(status.message().find("invalid row range"), std::string::npos);
}

} // namespace bytedance::bolt::shuffle::sparksql::test
