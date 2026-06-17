#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkProfiles.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

TEST(BmRowContainerBenchmarkProfileTest, DatasetNamesDescribeProfiles) {
  EXPECT_STREQ("fixed", datasetName(DatasetKind::kFixed));
  EXPECT_STREQ("variable", datasetName(DatasetKind::kVariable));
  EXPECT_STREQ("variable_large", datasetName(DatasetKind::kVariableLarge));
}

TEST(BmRowContainerBenchmarkProfileTest, VariableColumnPresence) {
  EXPECT_FALSE(hasVariableColumn(DatasetKind::kFixed));
  EXPECT_TRUE(hasVariableColumn(DatasetKind::kVariable));
  EXPECT_TRUE(hasVariableColumn(DatasetKind::kVariableLarge));
}

TEST(BmRowContainerBenchmarkProfileTest, VariableProfileCoversOneToMax) {
  StringProfileOptions options;
  options.variableMaxStringLength = 64;

  uint64_t sum = 0;
  for (uint64_t row = 0; row < 64; ++row) {
    const auto length = stringLengthForRow(
        DatasetKind::kVariable, row, options);
    EXPECT_GE(length, 1);
    EXPECT_LE(length, 64);
    sum += length;
  }

  EXPECT_EQ(2080, sum);
  EXPECT_EQ(33, estimatedStringBytesPerRow(DatasetKind::kVariable, options));
}

TEST(BmRowContainerBenchmarkProfileTest, VariableLargeProfileUsesFixedLength) {
  StringProfileOptions options;
  options.largeStringLength = 1024;

  EXPECT_EQ(
      1024,
      stringLengthForRow(DatasetKind::kVariableLarge, 0, options));
  EXPECT_EQ(
      1024,
      stringLengthForRow(DatasetKind::kVariableLarge, 12345, options));
  EXPECT_EQ(
      1024,
      estimatedStringBytesPerRow(DatasetKind::kVariableLarge, options));
}

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
