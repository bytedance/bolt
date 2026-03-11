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
#include "bolt/common/testutil/TestValue.h"
#include "bolt/shuffle/sparksql/tests/ShuffleTestBase.h"

using namespace bytedance::bolt::common::testutil;

namespace bytedance::bolt::shuffle::sparksql::test {

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

} // namespace bytedance::bolt::shuffle::sparksql::test
