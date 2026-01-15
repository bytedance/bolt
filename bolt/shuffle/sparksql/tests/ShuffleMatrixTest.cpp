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
  const std::vector<int32_t> shuffleModes = {0, 1, 2, 3};
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
              if (param.isSupported()) {
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
