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

#include <vector/ComplexVector.h>
#include <filesystem>
#include "bolt/common/caching/AsyncDataCache.h"
#include "bolt/common/memory/sparksql/tests/MemoryTestUtils.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/MemoryHogOperator.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/shuffle/sparksql/ShuffleWriterNode.h"
#include "bolt/shuffle/sparksql/partitioner/Partitioning.h"
#include "bolt/shuffle/sparksql/tests/ShuffleTestBase.h"

using namespace bytedance::bolt::common::testutil;
using namespace bytedance::bolt::memory::sparksql::test;

namespace bytedance::bolt::shuffle::sparksql::test {

using bytedance::bolt::exec::test::MemoryHogNode;

// A test suite for shuffle memory related tests
class ShuffleMemoryTest : public ShuffleTestBase {
 protected:
  static void SetUpTestCase() {
    ShuffleTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    ShuffleTestBase::TearDownTestCase();
  }

  // Runs a reclaimable MemoryHog (holding most of the task memory) feeding a
  // shuffle writer of 'writerType', over batches whose rows are identical
  // within a partition but incompressible across partitions. A writer that
  // makes large splits lets the per-partition repeats deduplicate (small
  // compressed output); one that fragments into tiny splits re-stores them.
  ShuffleWriterMetrics runReclaimableHogScenario(int32_t writerType);

  // Asserts the shuffle output compressed far below the raw size, i.e. the
  // writer made splits large enough for the per-partition repeats to dedup.
  static void expectWellCompressed(const ShuffleWriterMetrics& metrics) {
    int64_t rawTotal = 0;
    for (auto length : metrics.rawPartitionLengths) {
      rawTotal += length;
    }
    ASSERT_GT(metrics.totalBytesWritten, 0);
    ASSERT_GT(rawTotal, 0);
    EXPECT_LT(metrics.totalBytesWritten * 10, rawTotal)
        << "compressed shuffle output " << metrics.totalBytesWritten
        << " for raw " << rawTotal << " (ratio "
        << (rawTotal / metrics.totalBytesWritten)
        << ":1); splits are too small, hurting compression (issue #662)";
  }
};

TEST_F(ShuffleMemoryTest, testRowBasedShuffleEstimateLowerThanActual) {
  std::string str(10 * 1024, 'x');
  auto rowCount = 1024;
  auto baseVectorPtr = BaseVector::create(VARCHAR(), rowCount, pool());
  auto flatVector = baseVectorPtr->asFlatVector<StringView>();
  for (int i = 0; i < rowCount; ++i) {
    flatVector->set(i, StringView(str));
  }

  auto rowType = ROW({"c0"}, {VARCHAR()});
  auto rowVector = std::make_shared<MockRowVector>(
      pool(),
      rowType,
      nullptr,
      rowCount,
      std::vector<VectorPtr>{baseVectorPtr},
      100 /* fake small size */);

  std::string large(50 * 1024, 'x');
  baseVectorPtr = BaseVector::create(VARCHAR(), rowCount, pool());
  flatVector = baseVectorPtr->asFlatVector<StringView>();
  for (int i = 0; i < rowCount; ++i) {
    flatVector->set(i, StringView(large));
  }
  auto largeRowVector = std::make_shared<MockRowVector>(
      pool(),
      rowType,
      nullptr,
      rowCount,
      std::vector<VectorPtr>{baseVectorPtr},
      100 /* fake small size */);
  ShuffleTestParam param;
  param.partitioning = "hash";
  param.shuffleMode = 3; // RowBased
  param.writerType = PartitionWriterType::kLocal;
  param.dataTypeGroup = DataTypeGroup::kString;
  param.numPartitions = 1;
  param.numMappers = 1;
  param.memoryLimit = 128 * 1024 * 1024; // 128MB

  // 5 batches of 10MB then a 50MB batch with an under-estimated flat size; the
  // writer must handle the under-estimate without OOM.
  ShuffleInputData inputData;
  inputData.inputsPerMapper.emplace_back(5, rowVector);
  inputData.inputsPerMapper[0].push_back(largeRowVector);

  executeTestWithCustomInput(param, inputData);
}

TEST_F(ShuffleMemoryTest, testMinMemLimit) {
  std::string str(10 * 1024, 'x');
  auto rowCount = 1024;
  auto baseVectorPtr = BaseVector::create(VARCHAR(), rowCount, pool());
  auto flatVector = baseVectorPtr->asFlatVector<StringView>();
  for (int i = 0; i < rowCount; ++i) {
    flatVector->set(i, StringView(str));
  }

  auto rowType = ROW({"c0"}, {VARCHAR()});
  auto rowVector = std::make_shared<RowVector>(
      pool(),
      rowType,
      nullptr,
      rowCount,
      std::vector<VectorPtr>{baseVectorPtr});

  ShuffleTestParam param;
  param.partitioning = "hash";
  param.shuffleMode = 2;
  param.writerType = PartitionWriterType::kLocal;
  param.dataTypeGroup = DataTypeGroup::kString;
  param.numPartitions = 10;
  param.numMappers = 1;
  // kMinMemLimit (the writer's minimum budget) is 128MB, so the task needs
  // enough headroom to hold it; with only ~100MB the writer would OOM before
  // it could reach the minimum.
  param.memoryLimit = 512 * 1024 * 1024; // 512MB
  param.shuffleBufferSize = 40 * 1024 * 1024; // 40MB

  ShuffleInputData inputData;
  inputData.inputsPerMapper.emplace_back(20, rowVector);

  executeTestWithCustomInput(param, inputData);
}

TEST_F(ShuffleMemoryTest, testExtrameLargeRowVector) {
  std::string str(2 * 1024, '\0');
  auto rowCount = 1024;
  auto baseVectorPtr = BaseVector::create(VARCHAR(), rowCount, pool());
  auto flatVector = baseVectorPtr->asFlatVector<StringView>();
  for (int i = 0; i < rowCount; ++i) {
    flatVector->set(i, StringView(str));
  }

  auto rowType = ROW({"c0"}, {VARCHAR()});
  auto rowVector = std::make_shared<MockRowVector>(
      pool(),
      rowType,
      nullptr,
      rowCount,
      std::vector<VectorPtr>{baseVectorPtr},
      1 * 1024 * 1024 * 1024);

  ShuffleTestParam param;
  param.partitioning = "hash";
  param.shuffleMode = 1;
  param.writerType = PartitionWriterType::kLocal;
  param.dataTypeGroup = DataTypeGroup::kString;
  param.numPartitions = 1;
  param.numMappers = 1;
  param.memoryLimit = 1024 * 1024 * 1024; // 1GB
  param.shuffleBufferSize = 40 * 1024 * 1024; // 40MB

  ShuffleInputData inputData;
  inputData.inputsPerMapper.emplace_back(1, rowVector);

  SCOPED_TESTVALUE_SET(
      "BoltShuffleWriter::extremeLargeBatch",
      std::function<void(void*)>([&](void* batchCount) {
        // 1GB should split into 6 batches (200MB each)
        ASSERT_EQ(*(size_t*)batchCount, 6);
      }));

  executeTestWithCustomInput(param, inputData);
}

TEST_F(ShuffleMemoryTest, testCompositeRowEvictBeforeInit) {
  std::string str(25 * 1024, '\0');
  auto rowCount = 5 * 1024;

  auto rowType = ROW({"c1"}, {VARCHAR()});
  auto rowVector = createCompositeRowVectorWithPid(rowType, rowCount);

  size_t totalRowSize = str.size() * rowCount;
  rowVector->allocateRows(totalRowSize);
  {
    RowInfoTracker tracker(rowVector.get(), 0, rowCount);
    for (auto i = 0; i < rowCount; i++) {
      rowVector->store(i, rowVector->newRow());
      rowVector->advance(str.size());
    }
  }

  ShuffleTestParam param;
  param.partitioning = "hash";
  param.shuffleMode = 1;
  param.writerType = PartitionWriterType::kLocal;
  param.dataTypeGroup = DataTypeGroup::kString;
  param.numPartitions = 1;
  param.numMappers = 1;
  param.memoryLimit = 100 * 1024 * 1024; // 100MB
  param.shuffleBufferSize = 40 * 1024 * 1024; // 40MB
  param.verifyOutput = false;

  ShuffleInputData inputData;
  inputData.inputsPerMapper.emplace_back(1, rowVector);

  // expect OOM for composite row vector large than memory limit rather than
  // coredump
  EXPECT_THROW(executeTestWithCustomInput(param, inputData), BoltRuntimeError);
}

TEST_F(ShuffleMemoryTest, testRowBasedReclaimViaMemoryPressure) {
  using namespace bytedance::bolt::exec::test;

  constexpr int32_t kNumPartitions = 4;
  constexpr int32_t kRowCount = 1024;
  std::string str(8 * 1024, 'x');

  // Batches with pid as the first column (required by the writer). Feed enough
  // batches before the trigger so the writer buffers a sizable, reclaimable
  // amount (so reclaiming it frees enough for the hog's allocation to then
  // fit).
  constexpr int32_t kNumBatches = 8;
  constexpr int32_t kTriggerAt = 6;
  auto rowType = ROW({"pid", "c0"}, {INTEGER(), VARCHAR()});
  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < kNumBatches; ++b) {
    auto pidVector = makeFlatVector<int32_t>(
        kRowCount, [](auto row) { return row % kNumPartitions; });
    auto dataVector = makeFlatVector<StringView>(
        kRowCount, [&](auto /*row*/) { return StringView(str); });
    batches.push_back(makeRowVector({"pid", "c0"}, {pidVector, dataVector}));
  }

  // Enable the operator reclaim spiller (gluten's kSpill spiller analog) on the
  // test memory manager.
  const int64_t memoryLimit = 128 * 1024 * 1024;
  auto memoryManagerHolder = TestMemoryManagerHolder::create(
      memoryLimit, /*withOperatorReclaim=*/true);

  auto tempDir = TempDirectoryPath::create();
  std::string localDir = tempDir->path + "/local_dir";
  std::filesystem::create_directories(localDir);

  ShuffleWriterOptions writerOptions;
  writerOptions.partitioning = Partitioning::kHash;
  writerOptions.forceShuffleWriterType = 3; // RowBased
  writerOptions.partitionWriterOptions.numPartitions = kNumPartitions;
  writerOptions.partitionWriterOptions.partitionWriterType =
      PartitionWriterType::kLocal;
  writerOptions.partitionWriterOptions.dataFile =
      tempDir->path + "/shuffle_data.bin";
  writerOptions.partitionWriterOptions.configuredDirs = {localDir};
  writerOptions.partitionWriterOptions.numSubDirs = 1;
  writerOptions.taskAttemptId = memoryManagerHolder->taskAttemptId();

  // MemoryHog -> SparkShuffleWriter. Before emitting batch kTriggerAt (after
  // the writer has buffered the earlier batches and returned to kInit), the hog
  // allocates allocBytes. That exceeds the free capacity and forces a spill,
  // but is < the limit so it fits once the idle writer is reclaimed.
  auto sourceNode = std::make_shared<MemoryHogNode>(
      "source",
      rowType,
      batches,
      kTriggerAt,
      /*allocBytes=*/100 * 1024 * 1024);
  core::PlanNodeId writerId("writer");
  ShuffleWriterMetrics metrics;
  auto reportCallback = [&](const ShuffleWriterMetrics& m) { metrics = m; };
  auto writerNode = std::make_shared<SparkShuffleWriterNode>(
      writerId, writerOptions, reportCallback, sourceNode);

  CursorParameters params;
  params.planNode = writerNode;
  params.serialExecution = true;
  params.queryCtx = core::QueryCtx::create(
      nullptr,
      core::QueryConfig{{}},
      {},
      cache::AsyncDataCache::getInstance(),
      memoryManagerHolder->rootPool());

  auto cursor = TaskCursor::create(params);
  // Should not throw when the shuffle writer is reclaimed.
  EXPECT_NO_THROW({
    while (cursor->moveNext()) {
    }
  });
}

ShuffleWriterMetrics ShuffleMemoryTest::runReclaimableHogScenario(
    int32_t writerType) {
  using namespace bytedance::bolt::exec::test;

  constexpr int32_t kNumPartitions = 256;
  constexpr int32_t kRowCount = 256; // one row per partition per batch
  constexpr int32_t kNumBatches = 40;
  constexpr size_t kStrLen = 32 * 1024; // ~8MB per batch (256 rows * 32KB)

  std::vector<std::string> perPartition(kNumPartitions);
  for (int p = 0; p < kNumPartitions; ++p) {
    std::string s(kStrLen, '\0');
    uint32_t x = static_cast<uint32_t>(p) * 2654435761u + 12345u;
    for (auto& c : s) {
      x ^= x << 13;
      x ^= x >> 17;
      x ^= x << 5;
      c = static_cast<char>(x);
    }
    perPartition[p] = std::move(s);
  }

  auto rowType = ROW({"pid", "c0"}, {INTEGER(), VARCHAR()});
  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < kNumBatches; ++b) {
    auto pidVector = makeFlatVector<int32_t>(
        kRowCount, [](auto row) { return row % kNumPartitions; });
    auto dataVector = makeFlatVector<StringView>(kRowCount, [&](auto row) {
      return StringView(perPartition[row % kNumPartitions]);
    });
    batches.push_back(makeRowVector({"pid", "c0"}, {pidVector, dataVector}));
  }

  // 1GB task; the hog holds 979MB, leaving the writer ~45MB free (far below
  // kMinMemLimit). Demanding that minimum reclaims the held memory.
  const int64_t memoryLimit = 1024LL * 1024 * 1024;
  const int64_t kHeldBytes = 979LL * 1024 * 1024;
  auto memoryManagerHolder = TestMemoryManagerHolder::create(
      memoryLimit, /*withOperatorReclaim=*/true);

  auto tempDir = TempDirectoryPath::create();
  std::string localDir = tempDir->path + "/local_dir";
  std::filesystem::create_directories(localDir);

  ShuffleWriterOptions writerOptions;
  writerOptions.partitioning = Partitioning::kHash;
  writerOptions.forceShuffleWriterType = writerType;
  writerOptions.partitionWriterOptions.numPartitions = kNumPartitions;
  writerOptions.partitionWriterOptions.partitionWriterType =
      PartitionWriterType::kLocal;
  writerOptions.partitionWriterOptions.dataFile =
      tempDir->path + "/shuffle_data.bin";
  writerOptions.partitionWriterOptions.configuredDirs = {localDir};
  writerOptions.partitionWriterOptions.numSubDirs = 1;
  writerOptions.taskAttemptId = memoryManagerHolder->taskAttemptId();

  // MemoryHog -> SparkShuffleWriter: the hog holds kHeldBytes (reclaimable).
  auto sourceNode = std::make_shared<MemoryHogNode>(
      "source",
      rowType,
      batches,
      /*triggerAt=*/0,
      /*allocBytes=*/kHeldBytes,
      /*holdAllocation=*/true);
  core::PlanNodeId writerId("writer");
  ShuffleWriterMetrics metrics;
  auto reportCallback = [&](const ShuffleWriterMetrics& m) { metrics = m; };
  auto writerNode = std::make_shared<SparkShuffleWriterNode>(
      writerId, writerOptions, reportCallback, sourceNode);

  CursorParameters params;
  params.planNode = writerNode;
  params.serialExecution = true;
  params.queryCtx = core::QueryCtx::create(
      nullptr,
      core::QueryConfig{{}},
      {},
      cache::AsyncDataCache::getInstance(),
      memoryManagerHolder->rootPool());

  auto cursor = TaskCursor::create(params);
  EXPECT_NO_THROW({
    while (cursor->moveNext()) {
    }
  });
  return metrics;
}

// Issue #662: an upstream operator holding most of the memory leaves writer V2
// a tiny budget, so it spills every batch into small splits that compress
// poorly and bloat the shuffle output. Enforcing a minimum budget
// (kMinMemLimit) reclaims the upstream and yields large, well-compressed
// splits.
TEST_F(ShuffleMemoryTest, testMinMemLimitAvoidsSpillingEveryBatch) {
  expectWellCompressed(
      runReclaimableHogScenario(static_cast<int32_t>(ShuffleWriterType::V2)));
}

// The row-based writer spills via pool reservation failure (which triggers
// arbitration), so under the same pressure it reclaims the upstream instead of
// fragmenting into tiny splits. This guards that it keeps making large,
// well-compressed splits.
TEST_F(ShuffleMemoryTest, testRowBasedKeepsLargeSplitsUnderMemoryPressure) {
  expectWellCompressed(runReclaimableHogScenario(
      static_cast<int32_t>(ShuffleWriterType::RowBased)));
}

} // namespace bytedance::bolt::shuffle::sparksql::test
