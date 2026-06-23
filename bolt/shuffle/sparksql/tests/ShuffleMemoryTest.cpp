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
  param.memoryLimit = 100 * 1024 * 1024; // 100MB

  // first 5 batches with 10MB memory, then 50MB batch with under estimated flat
  // size, should trigger spilling and not OOM
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
  param.memoryLimit = 100 * 1024 * 1024; // 100MB
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

} // namespace bytedance::bolt::shuffle::sparksql::test
