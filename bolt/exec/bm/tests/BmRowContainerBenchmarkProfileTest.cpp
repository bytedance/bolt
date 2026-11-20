/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

TEST(BmRowContainerBenchmarkProfileTest, DatasetNamesDescribeProfiles) {
  EXPECT_STREQ("fixed", datasetName(DatasetKind::kFixed));
  EXPECT_STREQ("variable_small", datasetName(DatasetKind::kVariableSmall));
  EXPECT_STREQ("variable_large", datasetName(DatasetKind::kVariableLarge));
  EXPECT_STREQ("bigint", datasetName(DatasetKind::kBigint));
  EXPECT_STREQ("integer", datasetName(DatasetKind::kInteger));
  EXPECT_STREQ("double", datasetName(DatasetKind::kDouble));
  EXPECT_STREQ("varchar_small", datasetName(DatasetKind::kVarcharSmall));
  EXPECT_STREQ("varchar_large", datasetName(DatasetKind::kVarcharLarge));
  EXPECT_STREQ("array", datasetName(DatasetKind::kArray));
  EXPECT_STREQ("map", datasetName(DatasetKind::kMap));
  EXPECT_STREQ("row", datasetName(DatasetKind::kRow));
}

TEST(BmRowContainerBenchmarkProfileTest, CodecDatasetsContainOneTargetType) {
  const std::vector<std::pair<DatasetKind, TypePtr>> expected{
      {DatasetKind::kBigint, BIGINT()},
      {DatasetKind::kInteger, INTEGER()},
      {DatasetKind::kDouble, DOUBLE()},
      {DatasetKind::kVarcharSmall, VARCHAR()},
      {DatasetKind::kVarcharLarge, VARCHAR()},
      {DatasetKind::kArray, ARRAY(BIGINT())},
      {DatasetKind::kMap, MAP(BIGINT(), VARCHAR())},
      {DatasetKind::kRow, ROW({BIGINT(), VARCHAR(), ARRAY(INTEGER())})},
  };

  for (const auto& [dataset, type] : expected) {
    const auto types = columnTypes(dataset);
    ASSERT_EQ(1, types.size()) << datasetName(dataset);
    EXPECT_TRUE(types.front()->equivalent(*type)) << datasetName(dataset);
  }
}

TEST(BmRowContainerBenchmarkProfileTest, RowTypeMatchesEveryDatasetSchema) {
  for (const auto dataset :
       {DatasetKind::kFixed,
        DatasetKind::kVariableSmall,
        DatasetKind::kVariableLarge,
        DatasetKind::kBigint,
        DatasetKind::kInteger,
        DatasetKind::kDouble,
        DatasetKind::kVarcharSmall,
        DatasetKind::kVarcharLarge,
        DatasetKind::kArray,
        DatasetKind::kMap,
        DatasetKind::kRow}) {
    const auto types = columnTypes(dataset);
    const auto type = rowType(dataset);
    ASSERT_EQ(types.size(), type->size()) << datasetName(dataset);
    for (size_t column = 0; column < types.size(); ++column) {
      EXPECT_EQ(types[column], type->childAt(column)) << datasetName(dataset);
    }
  }
}

TEST(BmRowContainerBenchmarkProfileTest, LogicalBytesIncludeTheCeilingRow) {
  const auto opts = options(DatasetKind::kArray, 65);
  EXPECT_EQ(2, rowCount(opts));
  EXPECT_EQ(128, logicalBytesProcessed(opts));
}

TEST(BmRowContainerBenchmarkProfileTest, OpenZlIsBmOnlyCompressionKind) {
  EXPECT_STREQ("openzl", spillCompressionName(SpillCompressionKind::kOpenZl));
  EXPECT_EQ(
      memory::bm::compress::CompressionKind::kOpenZlFrame,
      bmCompressionKind(SpillCompressionKind::kOpenZl));
  EXPECT_THROW(
      checkOldRowBasedSpillBenchmarkSupported(
          options(DatasetKind::kFixed, 1024, SpillCompressionKind::kOpenZl)),
      std::exception);
}

TEST(BmRowContainerBenchmarkProfileTest, VariableColumnPresence) {
  EXPECT_FALSE(hasVariableColumn(DatasetKind::kFixed));
  EXPECT_TRUE(hasVariableColumn(DatasetKind::kVariableSmall));
  EXPECT_TRUE(hasVariableColumn(DatasetKind::kVariableLarge));
}

TEST(BmRowContainerBenchmarkProfileTest, VariableProfileCoversOneToMax) {
  StringProfileOptions options;
  options.variableMaxStringLength = 64;

  uint64_t sum = 0;
  for (uint64_t row = 0; row < 64; ++row) {
    const auto length =
        stringLengthForRow(DatasetKind::kVariableSmall, row, options);
    EXPECT_GE(length, 1);
    EXPECT_LE(length, 64);
    sum += length;
  }

  EXPECT_EQ(2080, sum);
  EXPECT_EQ(
      33, estimatedStringBytesPerRow(DatasetKind::kVariableSmall, options));
}

TEST(BmRowContainerBenchmarkProfileTest, VariableLargeProfileUsesFixedLength) {
  StringProfileOptions options;
  options.largeStringLength = 1024;

  EXPECT_EQ(1024, stringLengthForRow(DatasetKind::kVariableLarge, 0, options));
  EXPECT_EQ(
      1024, stringLengthForRow(DatasetKind::kVariableLarge, 12345, options));
  EXPECT_EQ(
      1024, estimatedStringBytesPerRow(DatasetKind::kVariableLarge, options));
}

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
