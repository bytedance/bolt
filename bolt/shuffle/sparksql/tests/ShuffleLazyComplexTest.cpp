/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

// Regression coverage for SparkShuffleWriter + SparkShuffleReader with the
// lazy-complex codec active. The writer encodes complex columns, swaps them
// for their inner VARBINARY bytes on the wire; the reader re-wraps the
// bytes as LazyComplexVector of the original type. `ShuffleTestBase`
// transparently decodes lazy outputs before value-level comparison
// (`maybeDecodeLazyComplex` helper), so the same round-trip assertion
// exercised in the non-lazy matrix tests applies here.

#include "bolt/row/CompactRowLazyCodec.h"
#include "bolt/shuffle/sparksql/tests/ShuffleTestBase.h"
#include "bolt/vector/tests/utils/ScopedActiveLazyFormat.h"

namespace bytedance::bolt::shuffle::sparksql::test {

class ShuffleLazyComplexTest
    : public ShuffleTestBase,
      public testing::WithParamInterface<ShuffleTestParam> {
 protected:
  void SetUp() override {
    ShuffleTestBase::SetUp();
    lazyScope_ =
        std::make_unique<bolt::test::ScopedActiveLazyFormat>("compact_row");
  }

  void TearDown() override {
    lazyScope_.reset();
    ShuffleTestBase::TearDown();
  }

 private:
  std::unique_ptr<bolt::test::ScopedActiveLazyFormat> lazyScope_;
};

TEST_P(ShuffleLazyComplexTest, RoundTrip) {
  executeTest(GetParam());
}

namespace {
std::vector<ShuffleTestParam> buildLazyShuffleParams() {
  // Focused coverage: two complex-heavy type groups (kComplex, kMix) crossed
  // with the four partitioning modes and the four shuffle modes. Adaptive
  // mode auto-picks a writer; the explicit V1/V2/RowBased forcings exercise
  // each writer path (including the row-based path that uses the
  // ShuffleRowToColumnarConverter built from wireOutputType_).
  std::vector<ShuffleTestParam> params;
  const std::vector<std::string> partitionings = {
      "single", "rr", "hash", "range"};
  const std::vector<int32_t> shuffleModes = {0, 1, 2, 3};
  const std::vector<DataTypeGroup> types = {
      DataTypeGroup::kComplex, DataTypeGroup::kMix};
  for (const auto& partitioning : partitionings) {
    for (auto shuffleMode : shuffleModes) {
      for (auto dataGroup : types) {
        ShuffleTestParam p;
        p.partitioning = partitioning;
        p.shuffleMode = shuffleMode;
        p.writerType = PartitionWriterType::kLocal;
        p.dataTypeGroup = dataGroup;
        p.numPartitions = 4;
        p.numMappers = 1;
        if (p.isSupported()) {
          params.push_back(p);
        }
      }
    }
  }
  return params;
}
} // namespace

INSTANTIATE_TEST_SUITE_P(
    ShuffleLazyComplex,
    ShuffleLazyComplexTest,
    testing::ValuesIn(buildLazyShuffleParams()),
    [](const testing::TestParamInfo<ShuffleTestParam>& info) {
      return info.param.toString();
    });

} // namespace bytedance::bolt::shuffle::sparksql::test
