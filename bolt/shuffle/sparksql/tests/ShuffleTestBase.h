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

#pragma once
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/shuffle/sparksql/Options.h"

namespace bytedance::bolt::shuffle::sparksql::test {

// Mock RowVector that allows setting a custom estimateFlatSize() value
class MockRowVector : public RowVector {
 public:
  MockRowVector(
      memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      BufferPtr nulls,
      size_t length,
      std::vector<VectorPtr> children,
      uint64_t flatSize);

  static bool isMockRowVector(const RowVectorPtr& rv);

  uint64_t estimateFlatSize() const override;

 private:
  uint64_t flatSize_;
};

enum class DataTypeGroup {
  kInteger,
  kFloat,
  kString,
  kLargeString,
  kDecimal,
  kDateTime,
  kComplex,
  kMix,
  kHighNulls,
  kEmpty
};

extern const std::vector<DataTypeGroup> dataGroups;

struct ShuffleTestParam {
  std::string partitioning;
  int32_t shuffleMode; // 0: Adaptive, 1: V1, 2: V2, 3: RowBased
  PartitionWriterType writerType;
  DataTypeGroup dataTypeGroup;
  int32_t numPartitions;
  int32_t numMappers;
  int64_t memoryLimit = 1024 * 1024 * 1024; // 1GB
  int32_t batchSize = 32;
  int32_t numBatches = 4;
  int32_t shuffleBufferSize = kDefaultShuffleWriterBufferSize;

  std::string toString() const;

  bool isSupported() const;
};

struct ShuffleInputData {
  std::vector<std::vector<RowVectorPtr>> inputsPerMapper;
};

struct ShuffleRunResult {
  ShuffleWriterMetrics metrics;
  std::vector<std::vector<RowVectorPtr>> partitionOutputs;
};

// A base class for shuffle tests
class ShuffleTestBase : public bytedance::bolt::exec::test::OperatorTestBase {
 protected:
  static void SetUpTestCase();

  static void TearDownTestCase();

  void SetUp() override;

  void TearDown() override;

  // execute test with param and custom input data, will first run shuffle
  // writer with custom input data and then shuffle reader and verify output
  void executeTestWithCustomInput(
      const ShuffleTestParam& param,
      ShuffleInputData& inputData);

  // execute test with param, generating input data internally
  void executeTest(const ShuffleTestParam& param);

 private:
  ShuffleInputData makeInputData(const ShuffleTestParam& param);

  RowVectorPtr prependPidColumn(
      const RowVectorPtr& input,
      int32_t numPartitions,
      std::mt19937& rng);

  std::vector<RowVectorPtr> prependPidBatches(
      const std::vector<RowVectorPtr>& batches,
      int32_t numPartitions);

  std::vector<std::vector<RowVectorPtr>> splitInputByPid(
      const std::vector<RowVectorPtr>& writerInput,
      int32_t numPartitions);

  ShuffleRunResult runShuffle(
      const std::vector<std::vector<RowVectorPtr>>& inputsPerMapper,
      const RowTypePtr& outputType,
      const ShuffleTestParam& param,
      const ShuffleInputData& inputData);
};

} // namespace bytedance::bolt::shuffle::sparksql::test
