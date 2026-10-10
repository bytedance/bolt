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

std::vector<ShuffleTestParam> buildShuffleParams() {
  std::vector<ShuffleTestParam> params;
  const std::vector<std::string> partitionings = {
      "single", "rr", "hash", "range"};
  const std::vector<int32_t> shuffleModes = {0, 1, 2, 3, 4};
  const std::vector<int32_t> partitionNumbers = {1, 4, 16};
  const std::vector<int32_t> mapperNumbers = {1, 4};

  const std::vector<PartitionWriterType> writerTypes = {
      PartitionWriterType::kLocal, PartitionWriterType::kCeleborn};

  for (auto partitioning : partitionings) {
    for (auto shuffleMode : shuffleModes) {
      for (auto writerType : writerTypes) {
        for (auto dataTypeGroup : dataGroups) {
          for (auto numPartitions : partitionNumbers) {
            for (auto numMappers : mapperNumbers) {
              auto param = ShuffleTestParam{
                  partitioning,
                  shuffleMode,
                  writerType,
                  dataTypeGroup,
                  numPartitions,
                  numMappers};
              if (!param.isSupported()) {
                continue;
              }
              if (shuffleMode == 3) {
                // RowBased: round-trip both on-wire row formats.
                param.rowFormat = bytedance::bolt::row::RowFormat::DENSE;
                params.push_back(param);
                param.rowFormat = bytedance::bolt::row::RowFormat::COMPACT;
                params.push_back(param);
              } else {
                params.push_back(param);
              }
            }
          }
        }
      }
    }
  }

  return params;
}

class CellTypeIntegrationTest : public ShuffleTestBase {};

TEST_F(CellTypeIntegrationTest, complexUsesAdapterUnderLimitedMemory) {
  for (const auto writerType :
       {PartitionWriterType::kLocal, PartitionWriterType::kCeleborn}) {
    SCOPED_TRACE(fmt::format("{}", writerType));
    ShuffleTestParam param{
        "hash", 4, writerType, DataTypeGroup::kComplex, 4, 1};
    param.memoryLimit = 64 << 20;
    ShuffleInputData input;
    auto arrays = makeArrayVector<int64_t>(
        4096,
        [](auto) { return 10; },
        [](auto row) { return row; },
        [](auto row) { return row % 5 == 0; });
    input.inputsPerMapper = {{makeRowVector({arrays})}};
    ShuffleRunResult result;
    executeTestWithCustomInput(param, input, &result);
    EXPECT_GT(result.metrics.convertTime, 0);
  }
}

TEST_F(CellTypeIntegrationTest, unknownOnlyUsesHeaderPayloads) {
  for (const auto writerType :
       {PartitionWriterType::kLocal, PartitionWriterType::kCeleborn}) {
    SCOPED_TRACE(fmt::format("{}", writerType));
    ShuffleTestParam param{
        "hash", 4, writerType, DataTypeGroup::kHighNulls, 4, 2};
    ShuffleInputData input;
    input.inputsPerMapper = {{makeRowVector(
        {BaseVector::createNullConstant(UNKNOWN(), 2048, pool()),
         BaseVector::createNullConstant(UNKNOWN(), 2048, pool())})}};
    input.inputsPerMapper.push_back(input.inputsPerMapper.front());
    ShuffleRunResult result;
    executeTestWithCustomInput(param, input, &result);
    EXPECT_EQ(result.metrics.totalBytesWritten, 8 * 25);
  }
}

TEST_F(CellTypeIntegrationTest, pidOnlyUsesHeaderPayloads) {
  // Column pruning leaves exchanges with no data column (e.g. a count over
  // a repartition): Cell must carry them end to end as row counts.
  for (const auto writerType :
       {PartitionWriterType::kLocal, PartitionWriterType::kCeleborn}) {
    SCOPED_TRACE(fmt::format("{}", writerType));
    ShuffleTestParam param{
        "hash", 4, writerType, DataTypeGroup::kHighNulls, 4, 2};
    ShuffleInputData input;
    input.inputsPerMapper = {{std::make_shared<RowVector>(
        pool(), ROW({}, {}), nullptr, 2048, std::vector<VectorPtr>{})}};
    input.inputsPerMapper.push_back(input.inputsPerMapper.front());
    ShuffleRunResult result;
    // The harness verifies every partition's row count and contents.
    executeTestWithCustomInput(param, input, &result);
    EXPECT_EQ(result.metrics.totalBytesWritten, 8 * 25);
  }
}

TEST_F(CellTypeIntegrationTest, compositeOutputSharesTheShuffle) {
  // HashAggregation composite output: aggregation-state rows - alone, or
  // alternating with ordinary rows - go through the cell shuffle and come
  // back as CompositeRowVectors and ordinary batches with every row intact.
  for (const auto writerType :
       {PartitionWriterType::kLocal, PartitionWriterType::kCeleborn}) {
    for (const bool mixed : {false, true}) {
      SCOPED_TRACE(
          fmt::format("{}{}", writerType, mixed ? ", mixed" : ", composite"));
      ShuffleTestParam param{
          "hash", 4, writerType, DataTypeGroup::kString, 4, 2};
      const auto batch = [&](int32_t base) {
        return makeRowVector(
            {"c0", "c1"},
            {makeFlatVector<int64_t>(
                 3000, [base](auto row) { return base + row; }),
             makeFlatVector<std::string>(3000, [base](auto row) {
               return std::string((base + row) % 41, 'a' + row % 26);
             })});
      };
      ShuffleInputData input;
      for (int32_t mapper = 0; mapper < 2; ++mapper) {
        const int32_t base = mapper * 100'000;
        std::vector<RowVectorPtr> batches{
            createCompositeInput(batch(base), param.rowFormat)};
        if (mixed) {
          batches.push_back(batch(base + 10'000));
        }
        batches.push_back(
            createCompositeInput(batch(base + 20'000), param.rowFormat));
        if (mixed) {
          batches.push_back(batch(base + 30'000));
        }
        input.inputsPerMapper.push_back(std::move(batches));
      }
      ShuffleRunResult result;
      // The harness checks every partition's rows, composite ones decoded.
      executeTestWithCustomInput(param, input, &result);
      EXPECT_GT(result.metrics.convertTime, 0);
    }
  }
}

// A test suite that runs shuffle tests with different parameters
class ShuffleMatrixTest : public ShuffleTestBase,
                          public testing::WithParamInterface<ShuffleTestParam> {
};

TEST_P(ShuffleMatrixTest, RoundTrip) {
  executeTest(GetParam());
}

INSTANTIATE_TEST_SUITE_P(
    ShuffleMatrix,
    ShuffleMatrixTest,
    testing::ValuesIn(buildShuffleParams()),
    [](const testing::TestParamInfo<ShuffleTestParam>& info) {
      return info.param.toString();
    });

} // namespace bytedance::bolt::shuffle::sparksql::test
