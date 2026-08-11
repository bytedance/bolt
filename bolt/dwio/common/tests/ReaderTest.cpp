/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/dwio/common/Reader.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/dwio/common/FormatData.h"
#include "bolt/dwio/common/SelectiveColumnReader.h"
#include "bolt/type/Subfield.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>
#include <array>
namespace bytedance::bolt::dwio::common {
namespace {
using namespace bytedance::bolt::common;

class ReaderTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

class TestFormatData : public FormatData {
 public:
  void readNulls(
      vector_size_t /*numValues*/,
      const uint64_t* FOLLY_NULLABLE /*incomingNulls*/,
      BufferPtr& nulls,
      bool /*nullsOnly*/ = false) override {
    nulls = nullptr;
  }

  uint64_t skipNulls(uint64_t numValues, bool /*nullsOnly*/ = false) override {
    return numValues;
  }

  uint64_t skip(uint64_t numValues) override {
    return numValues;
  }

  bool hasNulls() const override {
    return true;
  }

  PositionProvider seekToRowGroup(int64_t /*index*/) override {
    return PositionProvider(emptyPositions_);
  }

  void filterRowGroups(
      const common::ScanSpec& /*scanSpec*/,
      uint64_t /*rowsPerRowGroup*/,
      const StatsContext& /*writerContext*/,
      FilterRowGroupsResult& /*result*/,
      BufferedInput& /*input*/) override {}

 private:
  std::vector<uint64_t> emptyPositions_;
};

class TestFormatParams : public FormatParams {
 public:
  explicit TestFormatParams(
      memory::MemoryPool& pool,
      ColumnReaderStatistics& stats)
      : FormatParams(pool, stats) {}

  std::unique_ptr<FormatData> toFormatData(
      const std::shared_ptr<const TypeWithId>& /*type*/,
      const common::ScanSpec& /*scanSpec*/) override {
    return std::make_unique<TestFormatData>();
  }
};

class TestSelectiveColumnReader : public SelectiveColumnReader {
 public:
  TestSelectiveColumnReader(
      const TypePtr& requestedType,
      const std::shared_ptr<const TypeWithId>& fileType,
      FormatParams& params,
      common::ScanSpec& scanSpec)
      : SelectiveColumnReader(requestedType, fileType, params, scanSpec) {}

  void read(
      int64_t /*offset*/,
      const RowSet& /*rows*/,
      const uint64_t* /*incomingNulls*/) override {}

  void getValues(const RowSet& /*rows*/, VectorPtr* /*result*/) override {}

  void prepareOutputNullsForTest(
      const RowSet& rows,
      bool inputHasNulls,
      int32_t extraRows = 0) {
    prepareOutputNulls(rows, inputHasNulls, extraRows);
  }

  const BufferPtr& storedResultNullsForTest() const {
    return resultNulls_;
  }

  const uint64_t* rawResultNullsForTest() const {
    return rawResultNulls_;
  }

  bool returnReaderNullsForTest() const {
    return returnReaderNulls_;
  }
};

class TestValueHook : public ValueHook {
 public:
  void addValue(vector_size_t /*row*/, const void* /*value*/) override {}
};

enum class TestFilterKind { kNone, kIsNotNull, kIsNull, kBigintRange };

std::unique_ptr<common::Filter> makeTestFilter(TestFilterKind kind) {
  switch (kind) {
    case TestFilterKind::kNone:
      return nullptr;
    case TestFilterKind::kIsNotNull:
      return std::make_unique<common::IsNotNull>();
    case TestFilterKind::kIsNull:
      return std::make_unique<common::IsNull>();
    case TestFilterKind::kBigintRange:
      return std::make_unique<common::BigintRange>(0, 10, false);
  }
  BOLT_UNREACHABLE();
}

struct PrepareOutputNullsCase {
  const char* name;
  TypePtr requestedType;
  TypePtr fileType;
  TestFilterKind filterKind;
  bool projectOut{true};
  bool extractValues{false};
  bool hasValueHook{false};
  bool inputHasNulls{true};
  int32_t extraRows{0};
  bool expectAllocation{true};
};

PrepareOutputNullsCase prepareOutputNullsCase(
    const char* name,
    TypePtr requestedType,
    TypePtr fileType,
    TestFilterKind filterKind,
    bool expectAllocation,
    bool projectOut = true,
    bool extractValues = false,
    bool hasValueHook = false,
    bool inputHasNulls = true,
    int32_t extraRows = 0) {
  return PrepareOutputNullsCase{
      name,
      std::move(requestedType),
      std::move(fileType),
      filterKind,
      projectOut,
      extractValues,
      hasValueHook,
      inputHasNulls,
      extraRows,
      expectAllocation};
}

class PrepareOutputNullsTest
    : public ReaderTest,
      public testing::WithParamInterface<PrepareOutputNullsCase> {};

TEST_P(PrepareOutputNullsTest, allocation) {
  auto testCase = GetParam();
  common::ScanSpec scanSpec("c0");
  scanSpec.setProjectOut(testCase.projectOut);
  scanSpec.setExtractValues(testCase.extractValues);
  scanSpec.setFilter(makeTestFilter(testCase.filterKind));
  TestValueHook hook;
  if (testCase.hasValueHook) {
    scanSpec.setValueHook(&hook);
  }

  ColumnReaderStatistics stats;
  TestFormatParams params(*pool(), stats);
  auto fileTypeWithId = TypeWithId::create(testCase.fileType);
  TestSelectiveColumnReader reader(
      testCase.requestedType, fileTypeWithId, params, scanSpec);

  const std::array<vector_size_t, 4> rowNumbers = {0, 1, 2, 3};
  RowSet rows(rowNumbers.data(), rowNumbers.size());
  const auto allocsBefore = pool()->stats().numAllocs;
  reader.prepareOutputNullsForTest(
      rows, testCase.inputHasNulls, testCase.extraRows);
  const auto allocsAfter = pool()->stats().numAllocs;

  if (testCase.expectAllocation) {
    EXPECT_GT(allocsAfter, allocsBefore);
  } else {
    EXPECT_EQ(allocsAfter, allocsBefore);
  }
}

INSTANTIATE_TEST_SUITE_P(
    IsNotNullNullBufferElision,
    PrepareOutputNullsTest,
    testing::Values(
        prepareOutputNullsCase(
            "no_filter_may_need_output_nulls",
            BIGINT(),
            BIGINT(),
            TestFilterKind::kNone,
            true),
        prepareOutputNullsCase(
            "bigint_is_not_null_projected",
            BIGINT(),
            BIGINT(),
            TestFilterKind::kIsNotNull,
            false),
        prepareOutputNullsCase(
            "varchar_is_not_null_extracted",
            VARCHAR(),
            VARCHAR(),
            TestFilterKind::kIsNotNull,
            false,
            false,
            true),
        prepareOutputNullsCase(
            "varbinary_is_not_null_projected",
            VARBINARY(),
            VARBINARY(),
            TestFilterKind::kIsNotNull,
            false),
        prepareOutputNullsCase(
            "no_input_nulls",
            BIGINT(),
            BIGINT(),
            TestFilterKind::kBigintRange,
            false,
            true,
            false,
            false,
            false),
        prepareOutputNullsCase(
            "range_filter_may_need_output_nulls",
            BIGINT(),
            BIGINT(),
            TestFilterKind::kBigintRange,
            true),
        prepareOutputNullsCase(
            "is_null_filter_may_need_output_nulls",
            BIGINT(),
            BIGINT(),
            TestFilterKind::kIsNull,
            true),
        prepareOutputNullsCase(
            "is_not_null_filter_only",
            BIGINT(),
            BIGINT(),
            TestFilterKind::kIsNotNull,
            true,
            false,
            false),
        prepareOutputNullsCase(
            "is_not_null_with_value_hook",
            BIGINT(),
            BIGINT(),
            TestFilterKind::kIsNotNull,
            true,
            true,
            false,
            true),
        prepareOutputNullsCase(
            "is_not_null_cast_output",
            INTEGER(),
            BIGINT(),
            TestFilterKind::kIsNotNull,
            true),
        prepareOutputNullsCase(
            "is_not_null_complex_output",
            ARRAY(BIGINT()),
            ARRAY(BIGINT()),
            TestFilterKind::kIsNotNull,
            true),
        prepareOutputNullsCase(
            "is_not_null_with_extra_rows",
            BIGINT(),
            BIGINT(),
            TestFilterKind::kIsNotNull,
            false,
            true,
            false,
            false,
            true,
            7),
        prepareOutputNullsCase(
            "range_filter_with_extra_rows",
            BIGINT(),
            BIGINT(),
            TestFilterKind::kBigintRange,
            true,
            true,
            false,
            false,
            true,
            7)),
    [](const testing::TestParamInfo<PrepareOutputNullsCase>& info) {
      return info.param.name;
    });

TEST_F(ReaderTest, prepareOutputNullsClearsPreviouslyAllocatedNulls) {
  common::ScanSpec scanSpec("c0");
  scanSpec.setProjectOut(true);
  scanSpec.setFilter(makeTestFilter(TestFilterKind::kBigintRange));

  ColumnReaderStatistics stats;
  TestFormatParams params(*pool(), stats);
  auto fileTypeWithId = TypeWithId::create(BIGINT());
  TestSelectiveColumnReader reader(BIGINT(), fileTypeWithId, params, scanSpec);

  const std::array<vector_size_t, 4> rowNumbers = {0, 1, 2, 3};
  RowSet rows(rowNumbers.data(), rowNumbers.size());
  reader.prepareOutputNullsForTest(rows, true);
  ASSERT_NE(nullptr, reader.storedResultNullsForTest().get());
  ASSERT_NE(nullptr, reader.rawResultNullsForTest());

  scanSpec.setFilter(makeTestFilter(TestFilterKind::kIsNotNull));
  const auto allocsBefore = pool()->stats().numAllocs;
  reader.prepareOutputNullsForTest(rows, true);
  EXPECT_EQ(allocsBefore, pool()->stats().numAllocs);
  EXPECT_EQ(nullptr, reader.storedResultNullsForTest().get());
  EXPECT_EQ(nullptr, reader.rawResultNullsForTest());
}

TEST_F(ReaderTest, prepareOutputNullsClearsReturnedReaderNulls) {
  common::ScanSpec scanSpec("c0");
  scanSpec.setProjectOut(true);

  ColumnReaderStatistics stats;
  TestFormatParams params(*pool(), stats);
  auto fileTypeWithId = TypeWithId::create(BIGINT());
  TestSelectiveColumnReader reader(BIGINT(), fileTypeWithId, params, scanSpec);

  const std::array<vector_size_t, 4> rowNumbers = {0, 1, 2, 3};
  RowSet rows(rowNumbers.data(), rowNumbers.size());
  reader.nullsInReadRange() =
      AlignedBuffer::allocate<bool>(rows.size(), pool(), bits::kNotNull);

  reader.prepareOutputNullsForTest(rows, true);
  ASSERT_TRUE(reader.returnReaderNullsForTest());

  scanSpec.setFilter(makeTestFilter(TestFilterKind::kIsNotNull));
  reader.prepareOutputNullsForTest(rows, true);
  EXPECT_FALSE(reader.returnReaderNullsForTest());
  EXPECT_EQ(nullptr, reader.storedResultNullsForTest().get());
  EXPECT_EQ(nullptr, reader.rawResultNullsForTest());
}

TEST_F(ReaderTest, projectColumnsFilterStruct) {
  constexpr int kSize = 10;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kSize, folly::identity),
      makeRowVector({
          makeFlatVector<int64_t>(kSize, folly::identity),
      }),
  });
  common::ScanSpec spec("<root>");
  spec.addField("c0", 0);
  spec.getOrCreateChild(common::Subfield("c1.c0"))
      ->setFilter(common::createBigintValues({2, 4, 6}, false));
  auto actual = RowReader::projectColumns(input, spec);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({2, 4, 6}),
  });
  test::assertEqualVectors(expected, actual);
}

TEST_F(ReaderTest, projectColumnsFilterArray) {
  constexpr int kSize = 10;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kSize, folly::identity),
      makeArrayVector<int64_t>(
          kSize,
          [](auto) { return 1; },
          [](auto i) { return i; },
          [](auto i) { return i % 2 != 0; }),
  });
  common::ScanSpec spec("<root>");
  spec.addField("c0", 0);
  auto* c1 = spec.getOrCreateChild("c1");
  {
    SCOPED_TRACE("IS NULL");
    c1->setFilter(std::make_unique<common::IsNull>());
    auto actual = RowReader::projectColumns(input, spec);
    auto expected = makeRowVector({
        makeFlatVector<int64_t>({1, 3, 5, 7, 9}),
    });
    test::assertEqualVectors(expected, actual);
  }
  {
    SCOPED_TRACE("IS NOT NULL");
    c1->setFilter(std::make_unique<common::IsNotNull>());
    auto actual = RowReader::projectColumns(input, spec);
    auto expected = makeRowVector({
        makeFlatVector<int64_t>({0, 2, 4, 6, 8}),
    });
    test::assertEqualVectors(expected, actual);
  }
}

TEST_F(ReaderTest, getOrCreateChild) {
  constexpr int kSize = 5;
  auto input = makeRowVector(
      {"c.0", "c.1"},
      {
          makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
          makeFlatVector<int64_t>({2, 4, 6, 7, 8}),
      });

  common::ScanSpec spec("<root>");
  spec.addField("c.0", 0);
  // Create child from name.
  spec.getOrCreateChild("c.1")->setFilter(
      common::createBigintValues({2, 4, 6}, false));

  auto actual = RowReader::projectColumns(input, spec);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
  });
  test::assertEqualVectors(expected, actual);

  // Create child from subfield.
  spec.getOrCreateChild(common::Subfield("c.1"))
      ->setFilter(common::createBigintValues({2, 4, 6}, false));
  BOLT_ASSERT_USER_THROW(
      RowReader::projectColumns(input, spec),
      "Field not found: c. Available fields are: c.0, c.1.");
}

} // namespace
} // namespace bytedance::bolt::dwio::common
