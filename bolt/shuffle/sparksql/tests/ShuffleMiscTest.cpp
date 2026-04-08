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

// Test that shuffle writer correctly handles dictionary-encoded string columns
// with skewed entry sizes. After flatten, estimateFlatSize() should reflect
// actual string bytes (via StringViewStats), not the underestimated
// retainedSize from shared dictionary string buffers.
TEST_F(ShuffleMiscTest, SkewedDictionaryStringEstimateFlatSize) {
  // Create skewed data: 10 short strings + 990 copies of a 100KB string.
  // When dictionary-encoded, dict avg is ~1KB but most rows reference 100KB.
  constexpr int32_t kNumRows = 1000;
  constexpr int32_t kLongStringLen = 100 * 1024;
  constexpr int32_t kNumShortEntries = 10;

  std::string longStr(kLongStringLen, 'x');
  std::vector<std::string> shortStrs;
  for (int i = 0; i < kNumShortEntries; ++i) {
    shortStrs.push_back("s" + std::to_string(i));
  }

  // Build a FlatVector, then wrap in DictionaryVector to simulate Parquet
  // output
  auto flatValues =
      makeFlatVector<StringView>(kNumShortEntries + 1, [&](auto row) {
        if (row < kNumShortEntries) {
          return StringView(shortStrs[row]);
        }
        return StringView(longStr);
      });

  // Create indices: first 10 rows → short entries, rest → long entry
  auto indices = makeIndices(kNumRows, [&](auto row) {
    if (row < kNumShortEntries) {
      return static_cast<vector_size_t>(row);
    }
    return static_cast<vector_size_t>(kNumShortEntries); // long entry
  });
  auto dictVector = wrapInDictionary(indices, kNumRows, flatValues);

  // Verify DictionaryVector estimateFlatSize underestimates
  auto dictEstimate = dictVector->estimateFlatSize();
  uint64_t actualBytes = 0;
  uint64_t actualNonInlineBytes = 0;
  for (int i = 0; i < kNumRows; ++i) {
    auto sv = dictVector->as<SimpleVector<StringView>>()->valueAt(i);
    actualBytes += sv.size();
    if (!sv.isInline()) {
      actualNonInlineBytes += sv.size();
    }
  }
  // DictionaryVector default estimate uses dict avg, should be much smaller
  EXPECT_LT(dictEstimate, actualBytes / 2)
      << "DictionaryVector should underestimate before fix";

  // Flatten the DictionaryVector (simulates FilterProject or ShuffleWriter)
  VectorPtr flattened = dictVector;
  BaseVector::flattenVector(flattened);

  // After flatten, FlatVector should have StringViewStats set.
  // totalBytes only includes non-inline strings (>12B); inline strings are
  // stored in the StringView struct, covered by values.size().
  auto* flatVec = flattened->asFlatVector<StringView>();
  ASSERT_NE(flatVec, nullptr);
  ASSERT_TRUE(flatVec->stringStats().has_value())
      << "StringViewStats should be set after flattenVector";
  EXPECT_EQ(flatVec->stringStats()->totalBytes, actualNonInlineBytes);
  EXPECT_EQ(flatVec->stringStats()->maxLength, kLongStringLen);

  // estimateFlatSize should now be accurate
  auto flatEstimate = flattened->estimateFlatSize();
  EXPECT_GE(flatEstimate, actualBytes)
      << "Post-flatten estimateFlatSize should be >= actual string bytes";

  // Verify via RowVector (as ShuffleWriter would see it)
  auto rowVector = makeRowVector({"col0"}, {flattened});
  auto rowEstimate = rowVector->estimateFlatSize();
  EXPECT_GE(rowEstimate, actualBytes)
      << "RowVector estimateFlatSize should reflect StringViewStats";

  // Run through shuffle to verify data correctness
  ShuffleTestParam param;
  param.partitioning = "hash";
  param.shuffleMode = 2; // V2
  param.writerType = PartitionWriterType::kLocal;
  param.dataTypeGroup = DataTypeGroup::kLargeString;
  param.numPartitions = 10;
  param.numMappers = 1;
  param.batchSize = kNumRows;
  param.numBatches = 1;
  param.verifyOutput = true;

  // Use custom input with the skewed dictionary data
  auto pidColumn = makeFlatVector<int32_t>(
      kNumRows, [&](auto row) { return row % param.numPartitions; });
  auto inputWithPid = makeRowVector({pidColumn, dictVector});
  ShuffleInputData inputData;
  inputData.inputsPerMapper.push_back({inputWithPid});
  executeTestWithCustomInput(param, inputData);
}

} // namespace bytedance::bolt::shuffle::sparksql::test
