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

#include <type/Subfield.h>
#include <type/Timestamp.h>
#include "bolt/common/testutil/TempFilePath.h"
#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/dwio/parquet/reader/DictionaryFilter.h"
#include "bolt/dwio/parquet/reader/ParquetData.h"
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"
#include "bolt/type/Type.h"
#include "bolt/type/filter/FilterUtil.h"
namespace bytedance::bolt::parquet {
using namespace bytedance::bolt::common;

class DictionaryFilterTest : public ParquetTestBase {
 protected:
  void SetUp() override {
    ParquetTestBase::SetUp();
    filePath_ = ::bytedance::bolt::test::TempFilePath::create()->path;
  }

  void createTestFile() {
    // Create test data with repeating values for dictionary encoding
    auto strings = makeRepeatingVector(kStringValues);
    auto int16s = makeRepeatingVector(kInt16Values);
    auto int32s = makeRepeatingVector(kInt32Values);
    auto int64s = makeRepeatingVector(kInt64Values);
    auto numbers = makeRepeatingVector(kIntValues);
    auto timestamps = makeRepeatingVector(kTimestampValues);

    auto rowVector = makeRowVector(
        {"strings", "int16s", "int32s", "int64s", "numbers", "timestamps"},
        {strings, int16s, int32s, int64s, numbers, timestamps});

    // Write using Parquet writer with dictionary encoding enabled
    bytedance::bolt::parquet::WriterOptions options;
    options.memoryPool = rootPool_.get();
    options.enableDictionary = true;
    options.parquet_block_size = 1000;
    options.enableFlushBasedOnBlockSize = true;
    options.encoding = arrow::Encoding::PLAIN;
    options.enableDictionary = true;
    options.compression = common::CompressionKind_NONE;
    options.dataPageSize = 1024;
    options.dictionaryPageSizeLimit = 128 * 1024;
    options.writeBatchBytes = 4 * 1024 * 1024;

    auto sink = createSink(filePath_);
    auto writer = std::make_unique<bytedance::bolt::parquet::Writer>(
        std::move(sink), options, asRowType(rowVector->type()));

    writer->write(rowVector);
    writer->close();
  }

  // Helper to create repeating vectors for good dictionary encoding
  template <typename T>
  VectorPtr makeRepeatingVector(const std::vector<T>& values) {
    std::vector<T> repeatedValues;
    repeatedValues.reserve(values.size() * kRepetitions);
    for (size_t i = 0; i < kRepetitions; i++) {
      repeatedValues.insert(repeatedValues.end(), values.begin(), values.end());
    }
    std::sort(repeatedValues.begin(), repeatedValues.end());
    return vectorMaker_.flatVector<T>(repeatedValues);
  }

  RowTypePtr makeRowType() const {
    return ROW(
        {"strings", "int16s", "int32s", "int64s", "numbers", "timestamps"},
        {VARCHAR(), SMALLINT(), INTEGER(), BIGINT(), BIGINT(), TIMESTAMP()});
  }

  void assertFilter(
      std::unique_ptr<const common::Filter> filter,
      const std::string& columnName,
      bool expectedMatch) {
    auto rowType = makeRowType();
    auto parentSpec = makeScanSpec(makeRowType());
    auto channelIndex = rowType->getChildIdx(columnName);
    auto childSpec = parentSpec->getOrCreateChild(Subfield(columnName));
    childSpec->setChannel(channelIndex);
    childSpec->setFilter(filter->clone());

    auto reader =
        createReader(filePath_, dwio::common::ReaderOptions{leafPool_.get()});
    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.setEnableDictionaryFilter(true);
    rowReaderOpts.setScanSpec(parentSpec);

    auto rowReader = reader->createRowReader(rowReaderOpts);

    int64_t totalRows = 0;
    VectorPtr batch = BaseVector::create(makeRowType(), 1, leafPool_.get());
    while (true) {
      auto rowsRead = rowReader->next(1000, batch);
      if (!batch || rowsRead == 0) {
        break;
      }
      totalRows += rowsRead;
    }

    if (expectedMatch) {
      EXPECT_GT(totalRows, 0)
          << "Expected at least one matching row, but got 0.";
    } else {
      EXPECT_EQ(totalRows, 0) << "Expected no rows, but got " << totalRows;
    }
  }

  std::string filePath_;
  static constexpr size_t kRepetitions = 200;

  // Test data for all types
  const std::vector<std::string> kStringValues =
      {"foo", "bar", "baz", "qux", "quux"};
  const std::vector<int16_t> kInt16Values = {-1000, 0, 1000, 2000, 3000};
  const std::vector<int32_t> kInt32Values =
      {-100000, 0, 100000, 200000, 300000};
  const std::vector<int64_t> kInt64Values =
      {-1000000, 0, 1000000, 2000000, 3000000};
  const std::vector<int64_t> kIntValues = {1, 5, 10, 15, 20};
  const std::vector<Timestamp> kTimestampValues = {
      Timestamp::fromNanos(1640995200000000), // 2022-01-01
      Timestamp::fromNanos(1643673600000000), // 2022-02-01
      Timestamp::fromNanos(1646092800000000), // 2022-03-01
      Timestamp::fromNanos(1648771200000000), // 2022-04-01
      Timestamp::fromNanos(1651363200000000) // 2022-05-01
  };
};

TEST_F(DictionaryFilterTest, TestStringFilter) {
  createTestFile();

  // Test exact match filter
  auto exactMatch = common::createBigintValues(
      {1}, // Match the first 'numbers' value = 1
      true /* nullAllowed */);
  assertFilter(std::move(exactMatch), "numbers", true);

  // Test range filter that should match (numbers in [0..10])
  auto rangeMatch =
      common::createBigintRange(0, 10, true /* lower */, true /* upper */);
  assertFilter(std::move(rangeMatch), "numbers", true);

  // Test range filter that shouldn't match (numbers in [100..200])
  auto rangeNoMatch =
      common::createBigintRange(100, 200, true /* lower */, true /* upper */);
  assertFilter(std::move(rangeNoMatch), "numbers", false);

  // Test string equals filter on "strings" column
  auto stringMatch = common::createBytesValues(
      std::vector<std::string>{"foo"}, true /* nullAllowed */);
  assertFilter(std::move(stringMatch), "strings", true);

  // Test string range filter
  auto stringRange = common::createBytesRange(
      "bar",
      true, // lower bound inclusive
      "qux",
      true, // upper bound inclusive
      true); // nullAllowed
  assertFilter(std::move(stringRange), "strings", true);

  // Test string filter that shouldn't match
  auto stringNoMatch = common::createBytesValues(
      std::vector<std::string>{"nonexistent"}, true /* nullAllowed */);
  assertFilter(std::move(stringNoMatch), "strings", false);
}

TEST_F(DictionaryFilterTest, TestInt16Filter) {
  createTestFile();

  // Test exact match filter
  auto exactMatch = common::createBigintValues(
      {0}, // Match value = 0
      true /* nullAllowed */);
  assertFilter(std::move(exactMatch), "int16s", true);

  // Test range filter that should match
  auto rangeMatch = common::createBigintRange(
      -1000, 1000, true /* lower */, true /* upper */);
  assertFilter(std::move(rangeMatch), "int16s", true);

  // Test range filter that shouldn't match
  auto rangeNoMatch =
      common::createBigintRange(4000, 5000, true /* lower */, true /* upper */);
  assertFilter(std::move(rangeNoMatch), "int16s", false);
}

TEST_F(DictionaryFilterTest, TestInt32Filter) {
  createTestFile();

  // Test exact match filter
  auto exactMatch = common::createBigintValues(
      {0}, // Match value = 0
      true /* nullAllowed */);
  assertFilter(std::move(exactMatch), "int32s", true);

  // Test range filter that should match
  auto rangeMatch = common::createBigintRange(
      -100000, 100000, true /* lower */, true /* upper */);
  assertFilter(std::move(rangeMatch), "int32s", true);

  // Test range filter that shouldn't match
  auto rangeNoMatch = common::createBigintRange(
      400000, 500000, true /* lower */, true /* upper */);
  assertFilter(std::move(rangeNoMatch), "int32s", false);
}

TEST_F(DictionaryFilterTest, TestInt64Filter) {
  createTestFile();

  // Test exact match filter
  auto exactMatch = common::createBigintValues(
      {0}, // Match value = 0
      true /* nullAllowed */);
  assertFilter(std::move(exactMatch), "int64s", true);

  // Test range filter that should match
  auto rangeMatch = common::createBigintRange(
      -1000000, 1000000, true /* lower */, true /* upper */);
  assertFilter(std::move(rangeMatch), "int64s", true);

  // Test range filter that shouldn't match
  auto rangeNoMatch = common::createBigintRange(
      4000000, 5000000, true /* lower */, true /* upper */);
  assertFilter(std::move(rangeNoMatch), "int64s", false);
}

TEST_F(DictionaryFilterTest, DISABLED_TestTimestampFilter) {
  createTestFile();

  // Test timestamp equals
  auto timestampMatch = common::createBigintValues(
      {1640995200000000}, // 2022-01-01
      true /* nullAllowed */);
  assertFilter(std::move(timestampMatch), "timestamps", true);

  // Test timestamp range that should match
  auto timestampRangeMatch = common::createBigintRange(
      1640995200000000, // 2022-01-01
      1643673600000000, // 2022-02-01
      true /* lower */,
      true /* upper */);
  assertFilter(std::move(timestampRangeMatch), "timestamps", true);

  // Test timestamp range that shouldn't match
  auto timestampRangeNoMatch = common::createBigintRange(
      1672531200000000, // 2023-01-01
      1675209600000000, // 2023-02-01
      true /* lower */,
      true /* upper */);
  assertFilter(std::move(timestampRangeNoMatch), "timestamps", false);
}

TEST_F(DictionaryFilterTest, TestNonDictionaryEncoded) {
  // Just 3 rows so it's obviously non-dictionary
  auto vector = makeRowVector(
      {"strings", "int16s", "int32s", "int64s", "numbers", "timestamps"},
      {vectorMaker_.flatVector<StringView>({"foo", "bar", "baz"}),
       vectorMaker_.flatVector<int16_t>({-1000, 0, 1000}),
       vectorMaker_.flatVector<int32_t>({-100000, 0, 100000}),
       vectorMaker_.flatVector<int64_t>({-1000000, 0, 1000000}),
       vectorMaker_.flatVector<int64_t>({1, 2, 3}),
       vectorMaker_.flatVector<bolt::Timestamp>({
           Timestamp::fromNanos(1640995200000000), // 2022-01-01
           Timestamp::fromNanos(1643673600000000), // 2022-02-01
           Timestamp::fromNanos(1646092800000000), // 2022-03-01
       })});

  auto writer = createWriter(
      createSink(filePath_),
      [&]() {
        return std::make_unique<LambdaFlushPolicy>(
            kRowsInRowGroup, kBytesInRowGroup, [&]() { return false; });
      },
      makeRowType());

  writer->write(vector);
  writer->close();

  // All filters should return true (may match) for non-dictionary encoded
  // columns
  auto filter = common::createBigintValues({1}, true);
  assertFilter(std::move(filter), "numbers", true);
}

TEST_F(DictionaryFilterTest, TestMultiColumnFilter) {
  createTestFile();
  auto reader =
      createReader(filePath_, dwio::common::ReaderOptions{leafPool_.get()});
  auto rowType = makeRowType();

  // Create parent scan spec
  auto parentSpec = makeScanSpec(rowType);

  // Add multiple filters
  auto numbersSpec = parentSpec->getOrCreateChild("numbers");
  numbersSpec->setChannel(rowType->getChildIdx("numbers"));
  numbersSpec->setFilter(common::createBigintValues({1, 5}, true));

  auto stringsSpec = parentSpec->getOrCreateChild("strings");
  stringsSpec->setChannel(rowType->getChildIdx("strings"));
  stringsSpec->setFilter(
      common::createBytesValues(std::vector<std::string>{"foo"}, true));

  auto int16sSpec = parentSpec->getOrCreateChild("int16s");
  // Continuing from previous TestMultiColumnFilter
  int16sSpec->setChannel(rowType->getChildIdx("int16s"));
  int16sSpec->setFilter(common::createBigintValues({0}, true));

  dwio::common::RowReaderOptions rowReaderOpts;
  rowReaderOpts.setEnableDictionaryFilter(true);
  rowReaderOpts.setScanSpec(parentSpec);

  auto rowReader = reader->createRowReader(rowReaderOpts);
  ASSERT_TRUE(rowReader != nullptr);

  VectorPtr batch = BaseVector::create(rowType, 1, leafPool_.get());
  rowReader->next(1000, batch);
  ASSERT_TRUE(batch != nullptr);

  auto rowVector = batch->as<RowVector>();

  // Validate number values
  auto numberVector = rowVector->childAt(rowType->getChildIdx("numbers"))
                          ->as<FlatVector<int64_t>>();
  ASSERT_NE(numberVector, nullptr);

  // Validate string values
  auto stringVector = rowVector->childAt(rowType->getChildIdx("strings"))
                          ->as<SimpleVector<StringView>>();
  ASSERT_NE(stringVector, nullptr);

  // Validate int16 values
  auto int16Vector = rowVector->childAt(rowType->getChildIdx("int16s"))
                         ->as<FlatVector<int16_t>>();
  ASSERT_NE(int16Vector, nullptr);

  for (size_t i = 0; i < batch->size(); ++i) {
    if (!numberVector->isNullAt(i)) {
      auto numValue = numberVector->valueAt(i);
      EXPECT_TRUE(numValue == 1 || numValue == 5);
    }
    if (!stringVector->isNullAt(i)) {
      EXPECT_EQ(stringVector->valueAt(i), "foo");
    }
    if (!int16Vector->isNullAt(i)) {
      EXPECT_EQ(int16Vector->valueAt(i), 0);
    }
  }
}

TEST_F(DictionaryFilterTest, TestIntegerBoundaries) {
  // Create test data with boundary values
  const std::vector<int16_t> boundaryInt16s = {
      std::numeric_limits<int16_t>::min(),
      -1,
      0,
      1,
      std::numeric_limits<int16_t>::max()};

  const std::vector<int32_t> boundaryInt32s = {
      std::numeric_limits<int32_t>::min(),
      -1,
      0,
      1,
      std::numeric_limits<int32_t>::max()};

  const std::vector<int64_t> boundaryInt64s = {
      std::numeric_limits<int64_t>::min(),
      -1,
      0,
      1,
      std::numeric_limits<int64_t>::max()};

  auto int16Vector = makeRepeatingVector(boundaryInt16s);
  auto int32Vector = makeRepeatingVector(boundaryInt32s);
  auto int64Vector = makeRepeatingVector(boundaryInt64s);
  auto strings = makeRepeatingVector(kStringValues);
  auto numbers = makeRepeatingVector(kIntValues);
  auto timestamps = makeRepeatingVector(kTimestampValues);

  auto rowVector = makeRowVector(
      {"strings", "int16s", "int32s", "int64s", "numbers", "timestamps"},
      {strings, int16Vector, int32Vector, int64Vector, numbers, timestamps});

  // Write file with dictionary encoding
  bytedance::bolt::parquet::WriterOptions options;
  options.memoryPool = rootPool_.get();
  options.enableDictionary = true;
  options.parquet_block_size = 1000;
  options.enableFlushBasedOnBlockSize = true;
  options.encoding = arrow::Encoding::PLAIN;
  options.compression = common::CompressionKind_NONE;
  options.dataPageSize = 1024;
  options.dictionaryPageSizeLimit = 128 * 1024;
  options.writeBatchBytes = 4 * 1024 * 1024;

  auto sink = createSink(filePath_);
  auto writer = std::make_unique<bytedance::bolt::parquet::Writer>(
      std::move(sink), options, asRowType(rowVector->type()));

  writer->write(rowVector);
  writer->close();

  // Test int16_t boundaries
  {
    // Test min value
    auto minMatch = common::createBigintValues(
        {std::numeric_limits<int16_t>::min()}, true /* nullAllowed */);
    assertFilter(std::move(minMatch), "int16s", true);

    // Test max value
    auto maxMatch = common::createBigintValues(
        {std::numeric_limits<int16_t>::max()}, true /* nullAllowed */);
    assertFilter(std::move(maxMatch), "int16s", true);

    // Test full range
    auto fullRange = common::createBigintRange(
        std::numeric_limits<int16_t>::min(),
        std::numeric_limits<int16_t>::max(),
        true /* lower */,
        true /* upper */);
    assertFilter(std::move(fullRange), "int16s", true);
  }

  // Test int32_t boundaries
  {
    // Test min value
    auto minMatch = common::createBigintValues(
        {std::numeric_limits<int32_t>::min()}, true /* nullAllowed */);
    assertFilter(std::move(minMatch), "int32s", true);

    // Test max value
    auto maxMatch = common::createBigintValues(
        {std::numeric_limits<int32_t>::max()}, true /* nullAllowed */);
    assertFilter(std::move(maxMatch), "int32s", true);

    // Test full range
    auto fullRange = common::createBigintRange(
        std::numeric_limits<int32_t>::min(),
        std::numeric_limits<int32_t>::max(),
        true /* lower */,
        true /* upper */);
    assertFilter(std::move(fullRange), "int32s", true);
  }

  // Test int64_t boundaries
  {
    // Test min value
    auto minMatch = common::createBigintValues(
        {std::numeric_limits<int64_t>::min()}, true /* nullAllowed */);
    assertFilter(std::move(minMatch), "int64s", true);

    // Test max value
    auto maxMatch = common::createBigintValues(
        {std::numeric_limits<int64_t>::max()}, true /* nullAllowed */);
    assertFilter(std::move(maxMatch), "int64s", true);

    // Test full range
    auto fullRange = common::createBigintRange(
        std::numeric_limits<int64_t>::min(),
        std::numeric_limits<int64_t>::max(),
        true /* lower */,
        true /* upper */);
    assertFilter(std::move(fullRange), "int64s", true);
  }
}

// Test filtering on multiple columns simultaneously
TEST_F(DictionaryFilterTest, TestMultiValueDictionaryFilter) {
  // Make sure we have the dictionary-encoded file on disk.
  createTestFile();

  // Create a reader for the parquet file.
  auto reader =
      createReader(filePath_, dwio::common::ReaderOptions{leafPool_.get()});

  // Row type is strings, numbers, timestamps.
  auto rowType = makeRowType();

  // We'll filter on the "strings" column for multiple dictionary values.
  // Let's pick "bar" and "quux" from kStringValues = {"foo", "bar", "baz",
  // "qux", "quux"}.
  auto stringFilter = bytedance::bolt::common::createBytesValues(
      std::vector<std::string>{"bar", "quux"},
      /* nullAllowed = */ false);

  // Setup a ScanSpec that has filter on "strings".
  auto parentSpec = std::make_shared<bolt::common::ScanSpec>("root");
  auto stringsSpec = parentSpec->getOrCreateChild("strings");
  stringsSpec->setFilter(stringFilter->clone());

  // Because rowType has strings at index 0, numbers at 1, timestamps at 2,
  // we find the correct channel or just call:
  auto stringChannel = rowType->getChildIdx("strings");
  stringsSpec->setChannel(stringChannel);
  parentSpec->addField("strings", stringChannel);

  dwio::common::RowReaderOptions rowReaderOpts;
  rowReaderOpts.setEnableDictionaryFilter(true);
  rowReaderOpts.setScanSpec(parentSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  // Read some rows and validate that we got only bar or quux.
  VectorPtr batch = BaseVector::create(rowType, 1, leafPool_.get());
  int64_t totalRows = 0;
  int64_t matchedRows = 0;

  while (true) {
    auto rowsRead = rowReader->next(1000, batch);
    if (!batch || rowsRead == 0) {
      break; // no more data
    }

    auto rowVector = batch->as<RowVector>();
    auto stringVector = rowVector->childAt(rowType->getChildIdx("strings"))
                            ->as<SimpleVector<StringView>>();

    ASSERT_NE(stringVector, nullptr);
    for (int i = 0; i < batch->size(); ++i) {
      ++totalRows;
      if (!stringVector->isNullAt(i)) {
        auto val = stringVector->valueAt(i);
        // We only expect "bar" or "quux" because of the filter.
        EXPECT_TRUE(val == "bar" || val == "quux")
            << "Unexpected string: " << val;
        ++matchedRows;
      } else {
        // Because nullAllowed was false, we do not expect nulls.
        FAIL() << "Encountered null but nulls not allowed in filter.";
      }
    }
  }

  // Check final counts
  EXPECT_EQ(totalRows, matchedRows)
      << "All returned rows must match the filter.";
  // kStringValues.size() = 5, repeated kRepetitions=200 => total 1000 rows.
  // The filter picks 2 out of the 5 possible strings => ~400 should match.
  EXPECT_EQ(matchedRows, 400);
}

TEST_F(DictionaryFilterTest, RowReaderOptionDefaultValues) {
  // Test that dictionary filtering is disabled by default
  dwio::common::RowReaderOptions options;
  EXPECT_FALSE(options.isDictionaryFilterEnabled());

  // Test disabling dictionary filtering
  options.setEnableDictionaryFilter(true);
  EXPECT_TRUE(options.isDictionaryFilterEnabled());
}

TEST_F(DictionaryFilterTest, NonMatchingFilterWithDictionaryFilteringEnabled) {
  // Create test file
  createTestFile();

  // Test with a filter that doesn't match any rows
  auto nonExistentStringFilter = common::createBytesValues(
      std::vector<std::string>{"nonexistent_value"}, false);

  // With dictionary filtering enabled:
  // - The dictionary filter should determine no rows match
  // - No rows should be returned without reading any actual data
  auto reader =
      createReader(filePath_, dwio::common::ReaderOptions{leafPool_.get()});
  dwio::common::RowReaderOptions options;
  options.setEnableDictionaryFilter(true);

  auto parentSpec = std::make_shared<common::ScanSpec>("root");
  auto stringsSpec = parentSpec->getOrCreateChild("strings");
  stringsSpec->setFilter(nonExistentStringFilter->clone());
  auto stringChannel = makeRowType()->getChildIdx("strings");
  stringsSpec->setChannel(stringChannel);
  parentSpec->addField("strings", stringChannel);
  options.setScanSpec(parentSpec);

  auto rowReader = reader->createRowReader(options);
  VectorPtr batch = BaseVector::create(makeRowType(), 1000, leafPool_.get());

  // With dictionary filtering enabled, we expect 0 rows because
  // the dictionary filter will exclude all rows based on the filter
  EXPECT_EQ(rowReader->next(1000, batch), 0);
}

TEST_F(DictionaryFilterTest, NonMatchingFilterWithDictionaryFilteringDisabled) {
  // Create test file
  createTestFile();

  // Test with a filter that doesn't match any rows
  auto nonExistentStringFilter = common::createBytesValues(
      std::vector<std::string>{"nonexistent_value"}, false);

  // With dictionary filtering disabled:
  // - We can't use dictionary filtering, so we'll need to read rows
  // - But the filter will still apply to actual data, so we still expect 0 rows
  // - The difference is whether we need to read any data at all
  auto reader =
      createReader(filePath_, dwio::common::ReaderOptions{leafPool_.get()});
  dwio::common::RowReaderOptions options;
  options.setEnableDictionaryFilter(false);

  auto parentSpec = std::make_shared<common::ScanSpec>("root");
  auto stringsSpec = parentSpec->getOrCreateChild("strings");
  stringsSpec->setFilter(nonExistentStringFilter->clone());
  auto stringChannel = makeRowType()->getChildIdx("strings");
  stringsSpec->setChannel(stringChannel);
  parentSpec->addField("strings", stringChannel);
  options.setScanSpec(parentSpec);

  auto rowReader = reader->createRowReader(options);
  VectorPtr batch = BaseVector::create(makeRowType(), 1000, leafPool_.get());

  // Even with dictionary filtering disabled, we expect the standard filtering
  // behavior
  EXPECT_EQ(rowReader->next(1000, batch), 1000);
  EXPECT_EQ(batch->size(), 0); // No rows after filtering
}

TEST_F(DictionaryFilterTest, MatchingFilterWithDictionaryFilteringEnabled) {
  // Create test file
  createTestFile();

  // Test with a filter that matches existing rows
  auto existingStringFilter =
      common::createBytesValues(std::vector<std::string>{"foo"}, false);

  // With dictionary filtering enabled
  auto reader =
      createReader(filePath_, dwio::common::ReaderOptions{leafPool_.get()});
  dwio::common::RowReaderOptions options;
  options.setEnableDictionaryFilter(true);

  auto parentSpec = std::make_shared<common::ScanSpec>("root");
  auto stringsSpec = parentSpec->getOrCreateChild("strings");
  stringsSpec->setFilter(existingStringFilter->clone());
  auto stringChannel = makeRowType()->getChildIdx("strings");
  stringsSpec->setChannel(stringChannel);
  parentSpec->addField("strings", stringChannel);
  options.setScanSpec(parentSpec);

  auto rowReader = reader->createRowReader(options);
  VectorPtr batch = BaseVector::create(makeRowType(), 1000, leafPool_.get());
  int64_t rowsRead = rowReader->next(1000, batch);
  EXPECT_GT(rowsRead, 0);

  // Verify the rows actually match our filter
  auto rowVector = batch->as<RowVector>();
  auto stringVector =
      rowVector->childAt(stringChannel)->as<SimpleVector<StringView>>();
  for (size_t i = 0; i < batch->size(); ++i) {
    if (!stringVector->isNullAt(i)) {
      EXPECT_EQ(stringVector->valueAt(i), "foo");
    }
  }
}

TEST_F(DictionaryFilterTest, MatchingFilterWithDictionaryFilteringDisabled) {
  // Create test file
  createTestFile();

  // Test with a filter that matches existing rows
  auto existingStringFilter =
      common::createBytesValues(std::vector<std::string>{"foo"}, false);

  // With dictionary filtering disabled
  auto reader =
      createReader(filePath_, dwio::common::ReaderOptions{leafPool_.get()});
  dwio::common::RowReaderOptions options;
  options.setEnableDictionaryFilter(false);

  auto parentSpec = std::make_shared<common::ScanSpec>("root");
  auto stringsSpec = parentSpec->getOrCreateChild("strings");
  stringsSpec->setFilter(existingStringFilter->clone());
  auto stringChannel = makeRowType()->getChildIdx("strings");
  stringsSpec->setChannel(stringChannel);
  parentSpec->addField("strings", stringChannel);
  options.setScanSpec(parentSpec);

  auto rowReader = reader->createRowReader(options);
  VectorPtr batch = BaseVector::create(makeRowType(), 1000, leafPool_.get());
  int64_t rowsRead = rowReader->next(1000, batch);
  EXPECT_GT(rowsRead, 0);

  // Verify the rows actually match our filter
  auto rowVector = batch->as<RowVector>();
  auto stringVector =
      rowVector->childAt(stringChannel)->as<SimpleVector<StringView>>();
  for (size_t i = 0; i < batch->size(); ++i) {
    if (!stringVector->isNullAt(i)) {
      EXPECT_EQ(stringVector->valueAt(i), "foo");
    }
  }
}
} // namespace bytedance::bolt::parquet
