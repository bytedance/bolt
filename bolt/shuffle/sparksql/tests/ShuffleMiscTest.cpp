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

} // namespace bytedance::bolt::shuffle::sparksql::test
