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

} // namespace bytedance::bolt::shuffle::sparksql::test
