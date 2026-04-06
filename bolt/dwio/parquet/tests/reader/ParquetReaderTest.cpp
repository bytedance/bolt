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

#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include <type/HugeInt.h>
#include <type/Type.h>
#include <cstdlib>
#include <filesystem>
#include "bolt/core/QueryCtx.h"
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/exec/tests/utils/TempFilePath.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/expression/StringWriter.h"
#include "bolt/expression/UdfTypeResolver.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/sparksql/VariantEncoding.h"
#include "bolt/functions/sparksql/VariantFunctions.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/VariantVector.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

#include "bolt/dwio/parquet/encryption/KmsClient.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;

using bytedance::bolt::test::emptyArray;

namespace {
std::string getVariantFixturePath(const std::string& fileName) {
  const std::filesystem::path cwd = std::filesystem::current_path();
  const std::filesystem::path sourceDir =
      std::filesystem::path(__FILE__).parent_path();

  const std::filesystem::path candidates[] = {
      // Standard test layout in build tree.
      cwd / "../examples" / fileName,
      // Some builds may copy fixtures next to the test binary.
      cwd / fileName,
      // Fallback to source tree (works when build tree doesn't copy the file).
      sourceDir / "../examples" / fileName,
  };

  for (const auto& p : candidates) {
    std::error_code ec;
    if (std::filesystem::exists(p, ec) && !ec) {
      return std::filesystem::absolute(p).string();
    }
  }

  BOLT_CHECK(
      false,
      "VARIANT fixture not found: {}, cwd: {}, source: {}",
      fileName,
      cwd.string(),
      sourceDir.string());
}
} // namespace

class ParquetReaderTest : public ParquetTestBase {
 public:
  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const std::string& fileName,
      const RowTypePtr& rowType) {
    const std::string sample(getExampleFilePath(fileName));

    bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
    auto reader = createReader(sample, readerOptions);

    RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<bytedance::bolt::dwio::common::ColumnSelector>(
            rowType, rowType->names()));
    rowReaderOpts.setScanSpec(makeScanSpec(rowType));
    auto rowReader = reader->createRowReader(rowReaderOpts);
    return rowReader;
  }

  void assertReadWithExpected(
      const std::string& fileName,
      const RowTypePtr& rowType,
      const RowVectorPtr& expected) {
    auto rowReader = createRowReader(fileName, rowType);
    assertReadWithReaderAndExpected(rowType, *rowReader, expected, *pool_);
  }

  void assertReadWithFilters(
      const std::string& fileName,
      const RowTypePtr& fileSchema,
      FilterMap filters,
      const RowVectorPtr& expected) {
    const auto filePath(getExampleFilePath(fileName));
    bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    auto reader = createReader(filePath, readerOpts);
    assertReadWithReaderAndFilters(
        std::move(reader), fileName, fileSchema, std::move(filters), expected);
  }
};

TEST_F(ParquetReaderTest, parseDecimal) {
  // decimal_int96.parquet holds one column (s: Decimal(28, 10))
  const std::string sample(getExampleFilePath("decimal_int96.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  auto schema = ROW({"s"}, {DECIMAL(28, 10)});
  auto rowReaderOpts = getReaderOpts(schema);
  auto scanSpec = makeScanSpec(schema);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(schema, 1, leafPool_.get());
  rowReader->next(6, result);
  auto decimals = result->as<RowVector>();
  auto a = decimals->childAt(0)->asFlatVector<int128_t>()->rawValues();
  EXPECT_EQ(a[0], 64830000);
}

TEST_F(ParquetReaderTest, varcharFilterAppliedOnlyForStringLogicalType) {
  const std::string sample(getExampleFilePath("nation.parquet"));
  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  auto schema = ROW({"nationkey", "name"}, {BIGINT(), VARCHAR()});
  auto rowReaderOpts = getReaderOpts(schema);
  auto scanSpec = makeScanSpec(schema);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto nameSpec = scanSpec->childByName("name");
  ASSERT_TRUE(nameSpec != nullptr);
  EXPECT_EQ(nameSpec->logicalTypeName(), "STRING");
}
TEST_F(ParquetReaderTest, varcharSkipFilterWhenLogicalTypeMissing) {
  const std::string sample(getExampleFilePath("sample.parquet"));
  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  auto schema = sampleSchema();
  auto rowReaderOpts = getReaderOpts(schema);
  auto scanSpec = makeScanSpec(schema);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto a = scanSpec->childByName("a");
  auto b = scanSpec->childByName("b");
  ASSERT_TRUE(a != nullptr);
  ASSERT_TRUE(b != nullptr);
  EXPECT_TRUE(a->logicalTypeName().empty());
  EXPECT_TRUE(b->logicalTypeName().empty());
}
TEST_F(ParquetReaderTest, varcharFilterAppliedForStringLogicalType) {
  const std::string filename("nation.parquet");
  const std::string path(getExampleFilePath(filename));
  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(path, readerOptions);
  auto schema = ROW({"name"}, {VARCHAR()});
  auto rowReaderOpts = getReaderOpts(schema);
  auto scanSpec = makeScanSpec(schema);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  // Filter on s: the logical type should be STRING, allowing pruning
  FilterMap filters;
  filters["name"] = std::make_unique<BytesRange>(
      "x",
      false /* lowerUnbounded */,
      false /* lowerExclusive */,
      "x",
      false /* upperUnbounded */,
      false /* upperExclusive */,
      false /* nullAllowed */);
  auto scanSpec2 = makeScanSpec(schema);
  for (auto&& [col, f] : filters) {
    scanSpec2->getOrCreateChild(Subfield(col))->setFilter(std::move(f));
  }
  rowReaderOpts.setScanSpec(scanSpec2);
  auto rowReader2 = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(schema, 0, leafPool_.get());
  auto n = rowReader2->next(5, result);
  EXPECT_GE(n, 0);
}

TEST_F(ParquetReaderTest, parseSample) {
  // sample.parquet holds two columns (a: BIGINT, b: DOUBLE) and
  // 20 rows (10 rows per group). Group offsets are 153 and 614.
  // Data is in plain uncompressed format:
  //   a: [1..20]
  //   b: [1.0..20.0]
  const std::string sample(getExampleFilePath("sample.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 20ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::DOUBLE);
  EXPECT_EQ(type->childByName("a"), col0);
  EXPECT_EQ(type->childByName("b"), col1);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
      makeFlatVector<double>(20, [](auto row) { return row + 1; }),
  });

  assertReadWithReaderAndExpected(
      sampleSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, parseUnannotatedList) {
  // unannotated_list.parquet has the following the schema
  // the list is defined without the middle layer
  // message ParquetSchema {
  //   optional group self (LIST) {
  //     repeated group self_tuple {
  //       optional int64 a;
  //       optional boolean b;
  //       required binary c (STRING);
  //     }
  //   }
  // }
  const std::string sample(getExampleFilePath("unannotated_list.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  EXPECT_EQ(reader->numberOfRows(), 22ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::ARRAY);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0)->name_, "self");

  EXPECT_EQ(col0->size(), 3ULL);
  EXPECT_EQ(col0->childAt(0)->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0->childAt(0))
          ->name_,
      "a");

  EXPECT_EQ(col0->childAt(1)->type()->kind(), TypeKind::BOOLEAN);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0->childAt(1))
          ->name_,
      "b");

  EXPECT_EQ(col0->childAt(2)->type()->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0->childAt(2))
          ->name_,
      "c");
}

TEST_F(ParquetReaderTest, parseUnannotatedMap) {
  // unannotated_map.parquet has the following the schema
  // the map is defined with a MAP_KEY_VALUE node
  // message hive_schema {
  // optional group test (MAP) {
  //   repeated group key_value (MAP_KEY_VALUE) {
  //       required binary key (STRING);
  //       optional int64 value;
  //   }
  //  }
  //}
  const std::string filename("unnotated_map.parquet");
  const std::string sample(getExampleFilePath(filename));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  auto numRows = reader->numberOfRows();

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::MAP);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0)->name_, "test");

  EXPECT_EQ(col0->size(), 2ULL);
  EXPECT_EQ(col0->childAt(0)->type()->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0->childAt(0))
          ->name_,
      "key");

  EXPECT_EQ(col0->childAt(1)->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0->childAt(1))
          ->name_,
      "value");
}

TEST_F(ParquetReaderTest, parseSampleRange1) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  rowReaderOpts.range(0, 200);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(10, [](auto row) { return row + 1; }),
      makeFlatVector<double>(10, [](auto row) { return row + 1; }),
  });
  assertReadWithReaderAndExpected(
      sampleSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, parseSampleRange2) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  rowReaderOpts.range(200, 500);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(10, [](auto row) { return row + 11; }),
      makeFlatVector<double>(10, [](auto row) { return row + 11; }),
  });
  assertReadWithReaderAndExpected(
      sampleSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, parseSampleEmptyRange) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  rowReaderOpts.range(300, 10);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(1000, result), 0);
}

TEST_F(ParquetReaderTest, parseReadAsLowerCase) {
  // upper.parquet holds two columns (A: BIGINT, b: BIGINT) and
  // 2 rows.
  const std::string upper(getExampleFilePath("upper.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  readerOptions.setFileColumnNamesReadAsLowerCase(true);
  auto reader = createReader(upper, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 2ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(type->childByName("a"), col0);
  EXPECT_EQ(type->childByName("b"), col1);
}

TEST_F(ParquetReaderTest, parseRowMapArrayReadAsLowerCase) {
  // upper_complex.parquet holds one row of type
  // root
  //  |-- Cc: struct (nullable = true)
  //  |    |-- CcLong0: long (nullable = true)
  //  |    |-- CcMap1: map (nullable = true)
  //  |    |    |-- key: string
  //  |    |    |-- value: struct (valueContainsNull = true)
  //  |    |    |    |-- CcArray2: array (nullable = true)
  //  |    |    |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |    |    |-- CcInt3: integer (nullable = true)
  // data
  // +-----------------------+
  // |Cc                     |
  // +-----------------------+
  // |{120, {key -> {[{1}]}}}|
  // +-----------------------+
  const std::string upper(getExampleFilePath("upper_complex.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  readerOptions.setFileColumnNamesReadAsLowerCase(true);
  auto reader = createReader(upper, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 1ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);

  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(type->childByName("cc"), col0);

  auto col0_0 = col0->childAt(0);
  EXPECT_EQ(col0_0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(col0->childByName("cclong0"), col0_0);

  auto col0_1 = col0->childAt(1);
  EXPECT_EQ(col0_1->type()->kind(), TypeKind::MAP);
  EXPECT_EQ(col0->childByName("ccmap1"), col0_1);

  auto col0_1_0 = col0_1->childAt(0);
  EXPECT_EQ(col0_1_0->type()->kind(), TypeKind::VARCHAR);

  auto col0_1_1 = col0_1->childAt(1);
  EXPECT_EQ(col0_1_1->type()->kind(), TypeKind::ROW);

  auto col0_1_1_0 = col0_1_1->childAt(0);
  EXPECT_EQ(col0_1_1_0->type()->kind(), TypeKind::ARRAY);
  EXPECT_EQ(col0_1_1->childByName("ccarray2"), col0_1_1_0);

  auto col0_1_1_0_0 = col0_1_1_0->childAt(0);
  EXPECT_EQ(col0_1_1_0_0->type()->kind(), TypeKind::ROW);
  auto col0_1_1_0_0_0 = col0_1_1_0_0->childAt(0);
  EXPECT_EQ(col0_1_1_0_0_0->type()->kind(), TypeKind::INTEGER);
  EXPECT_EQ(col0_1_1_0_0->childByName("ccint3"), col0_1_1_0_0_0);
}

TEST_F(ParquetReaderTest, parseEmpty) {
  // empty.parquet holds two columns (a: BIGINT, b: DOUBLE) and
  // 0 rows.
  const std::string empty(getExampleFilePath("empty.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(empty, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 0ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::DOUBLE);
  EXPECT_EQ(type->childByName("a"), col0);
  EXPECT_EQ(type->childByName("b"), col1);
}

TEST_F(ParquetReaderTest, parseInt) {
  // int.parquet holds integer columns (int: INTEGER, bigint: BIGINT)
  // and 10 rows.
  // Data is in plain uncompressed format:
  //   int: [100 .. 109]
  //   bigint: [1000 .. 1009]
  const std::string sample(getExampleFilePath("int.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  EXPECT_EQ(reader->numberOfRows(), 10ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::INTEGER);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::BIGINT);

  auto rowReaderOpts = getReaderOpts(intSchema());
  auto scanSpec = makeScanSpec(intSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(10, [](auto row) { return row + 100; }),
      makeFlatVector<int64_t>(10, [](auto row) { return row + 1000; }),
  });
  assertReadWithReaderAndExpected(
      intSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, parseUnsignedInt1) {
  // uint.parquet holds unsigned integer columns (uint8: TINYINT, uint16:
  // SMALLINT, uint32: INTEGER, uint64: BIGINT) and 3 rows. Data is in plain
  // uncompressed format:
  //   uint8: [255, 3, 3]
  //   uint16: [65535, 2000, 3000]
  //   uint32: [4294967295, 2000000000, 3000000000]
  //   uint64: [18446744073709551615, 2000000000000000000, 3000000000000000000]
  const std::string sample(getExampleFilePath("uint.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 3ULL);
  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 4ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::TINYINT);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::SMALLINT);
  auto col2 = type->childAt(2);
  EXPECT_EQ(col2->type()->kind(), TypeKind::INTEGER);
  auto col3 = type->childAt(3);
  EXPECT_EQ(col3->type()->kind(), TypeKind::BIGINT);

  auto rowType =
      ROW({"uint8", "uint16", "uint32", "uint64"},
          {TINYINT(), SMALLINT(), INTEGER(), BIGINT()});

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(
      std::make_shared<bytedance::bolt::dwio::common::ColumnSelector>(
          rowType, rowType->names()));
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector(
      {makeFlatVector<uint8_t>({255, 2, 3}),
       makeFlatVector<uint16_t>({65535, 2000, 3000}),
       makeFlatVector<uint32_t>({4294967295, 2000000000, 3000000000}),
       makeFlatVector<uint64_t>(
           {18446744073709551615ULL,
            2000000000000000000ULL,
            3000000000000000000ULL})});
  assertReadWithReaderAndExpected(rowType, *rowReader, expected, *pool_);
}

TEST_F(ParquetReaderTest, parseUnsignedInt2) {
  auto rowType =
      ROW({"uint8", "uint16", "uint32", "uint64"},
          {SMALLINT(), SMALLINT(), INTEGER(), BIGINT()});
  auto expected = makeRowVector(
      {makeFlatVector<uint16_t>({255, 2, 3}),
       makeFlatVector<uint16_t>({65535, 2000, 3000}),
       makeFlatVector<uint32_t>({4294967295, 2000000000, 3000000000}),
       makeFlatVector<uint64_t>(
           {18446744073709551615ULL,
            2000000000000000000ULL,
            3000000000000000000ULL})});
  assertReadWithExpected("uint.parquet", rowType, expected);
}

TEST_F(ParquetReaderTest, parseUnsignedInt3) {
  auto rowType =
      ROW({"uint8", "uint16", "uint32", "uint64"},
          {SMALLINT(), INTEGER(), INTEGER(), BIGINT()});
  auto expected = makeRowVector(
      {makeFlatVector<uint16_t>({255, 2, 3}),
       makeFlatVector<uint32_t>({65535, 2000, 3000}),
       makeFlatVector<uint32_t>({4294967295, 2000000000, 3000000000}),
       makeFlatVector<uint64_t>(
           {18446744073709551615ULL,
            2000000000000000000ULL,
            3000000000000000000ULL})});
  assertReadWithExpected("uint.parquet", rowType, expected);
}

TEST_F(ParquetReaderTest, parseUnsignedInt4) {
  auto rowType =
      ROW({"uint8", "uint16", "uint32", "uint64"},
          {SMALLINT(), INTEGER(), INTEGER(), DECIMAL(20, 0)});
  auto expected = makeRowVector(
      {makeFlatVector<uint16_t>({255, 2, 3}),
       makeFlatVector<uint32_t>({65535, 2000, 3000}),
       makeFlatVector<uint32_t>({4294967295, 2000000000, 3000000000}),
       makeFlatVector<uint128_t>(
           {18446744073709551615ULL,
            2000000000000000000ULL,
            3000000000000000000ULL})});
  assertReadWithExpected("uint.parquet", rowType, expected);
}

TEST_F(ParquetReaderTest, parseUnsignedInt5) {
  auto rowType =
      ROW({"uint8", "uint16", "uint32", "uint64"},
          {SMALLINT(), INTEGER(), BIGINT(), DECIMAL(20, 0)});
  auto expected = makeRowVector(
      {makeFlatVector<uint16_t>({255, 2, 3}),
       makeFlatVector<uint32_t>({65535, 2000, 3000}),
       makeFlatVector<uint64_t>({4294967295, 2000000000, 3000000000}),
       makeFlatVector<uint128_t>(
           {18446744073709551615ULL,
            2000000000000000000ULL,
            3000000000000000000ULL})});
  assertReadWithExpected("uint.parquet", rowType, expected);
}

TEST_F(ParquetReaderTest, parseDate) {
  // date.parquet holds a single column (date: DATE) and
  // 25 rows.
  // Data is in plain uncompressed format:
  //   date: [1969-12-27 .. 1970-01-20]
  const std::string sample(getExampleFilePath("date.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 25ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type(), DATE());
  EXPECT_EQ(type->childByName("date"), col0);

  auto rowReaderOpts = getReaderOpts(dateSchema());
  auto scanSpec = makeScanSpec(dateSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto expected = makeRowVector({
      makeFlatVector<int32_t>(25, [](auto row) { return row - 5; }),
  });
  assertReadWithReaderAndExpected(
      dateSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, parseRowMapArray) {
  // sample.parquet holds one row of type (ROW(BIGINT c0, MAP(VARCHAR,
  // ARRAY(INTEGER)) c1) c)
  const std::string sample(getExampleFilePath("row_map_array.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 1ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);

  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(type->childByName("c"), col0);

  auto col0_0 = col0->childAt(0);
  EXPECT_EQ(col0_0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(col0->childByName("c0"), col0_0);

  auto col0_1 = col0->childAt(1);
  EXPECT_EQ(col0_1->type()->kind(), TypeKind::MAP);
  EXPECT_EQ(col0->childByName("c1"), col0_1);

  auto col0_1_0 = col0_1->childAt(0);
  EXPECT_EQ(col0_1_0->type()->kind(), TypeKind::VARCHAR);

  auto col0_1_1 = col0_1->childAt(1);
  EXPECT_EQ(col0_1_1->type()->kind(), TypeKind::ARRAY);

  auto col0_1_1_0 = col0_1_1->childAt(0);
  EXPECT_EQ(col0_1_1_0->type()->kind(), TypeKind::INTEGER);
}

TEST_F(ParquetReaderTest, projectNoColumns) {
  // This is the case for count(*).
  auto rowType = ROW({}, {});
  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(getExampleFilePath("sample.parquet"), readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(rowType, 1, leafPool_.get());
  constexpr int kBatchSize = 100;
  ASSERT_TRUE(rowReader->next(kBatchSize, result));
  EXPECT_EQ(result->size(), 10);
  ASSERT_TRUE(rowReader->next(kBatchSize, result));
  EXPECT_EQ(result->size(), 10);
  ASSERT_FALSE(rowReader->next(kBatchSize, result));
}

TEST_F(ParquetReaderTest, parseIntDecimal) {
  // decimal_dict.parquet two columns (a: DECIMAL(7,2), b: DECIMAL(14,2)) and
  // 6 rows.
  // The physical type of the decimal columns:
  //   a: int32
  //   b: int64
  // Data is in dictionary encoding:
  //   a: [11.11, 11.11, 22.22, 22.22, 33.33, 33.33]
  //   b: [11.11, 11.11, 22.22, 22.22, 33.33, 33.33]
  auto rowType = ROW({"a", "b"}, {DECIMAL(7, 2), DECIMAL(14, 2)});
  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  const std::string decimal_dict(getExampleFilePath("decimal_dict.parquet"));

  auto reader = createReader(decimal_dict, readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  EXPECT_EQ(reader->numberOfRows(), 6ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(col1->type()->kind(), TypeKind::BIGINT);

  int64_t expectValues[3] = {1111, 2222, 3333};
  auto result = BaseVector::create(rowType, 1, leafPool_.get());
  rowReader->next(6, result);
  EXPECT_EQ(result->size(), 6ULL);
  auto decimals = result->as<RowVector>();
  auto a = decimals->childAt(0)->asFlatVector<int64_t>()->rawValues();
  auto b = decimals->childAt(1)->asFlatVector<int64_t>()->rawValues();
  for (int i = 0; i < 3; i++) {
    int index = 2 * i;
    EXPECT_EQ(a[index], expectValues[i]);
    EXPECT_EQ(a[index + 1], expectValues[i]);
    EXPECT_EQ(b[index], expectValues[i]);
    EXPECT_EQ(b[index + 1], expectValues[i]);
  }
}

TEST_F(ParquetReaderTest, parseMapKeyValueAsMap) {
  // map_key_value.parquet holds a single map column (key: VARCHAR, b: BIGINT)
  // and 1 row that contains 8 map entries. It is with older version of Parquet
  // and uses MAP_KEY_VALUE instead of MAP as the map SchemaElement
  // converted_type. It has 5 SchemaElements in the schema, in the format of
  // schemaIdx: <repetition> <type> name (<converted type>):
  //
  // 0: REQUIRED BOOLEAN hive_schema (UTF8)
  // 1:   OPTIONAL BOOLEAN test (MAP_KEY_VALUE)
  // 2:     REPEATED BOOLEAN map (UTF8)
  // 3:       REQUIRED BYTE_ARRAY key (UTF8)
  // 4:       OPTIONAL INT64 value (UTF8)

  const std::string sample(getExampleFilePath("map_key_value.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 1ULL);

  auto rowType = reader->typeWithId();
  EXPECT_EQ(rowType->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->size(), 1ULL);

  auto mapColumnType = rowType->childAt(0);
  EXPECT_EQ(mapColumnType->type()->kind(), TypeKind::MAP);

  auto mapKeyType = mapColumnType->childAt(0);
  EXPECT_EQ(mapKeyType->type()->kind(), TypeKind::VARCHAR);

  auto mapValueType = mapColumnType->childAt(1);
  EXPECT_EQ(mapValueType->type()->kind(), TypeKind::BIGINT);

  auto fileSchema =
      ROW({"test"}, {createType<TypeKind::MAP>({VARCHAR(), BIGINT()})});
  auto rowReaderOpts = getReaderOpts(fileSchema);
  auto scanSpec = makeScanSpec(fileSchema);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({vectorMaker_.mapVector<std::string, int64_t>(
      {{{"0", 0},
        {"1", 1},
        {"2", 2},
        {"3", 3},
        {"4", 4},
        {"5", 5},
        {"6", 6},
        {"7", 7}}})});

  assertReadWithReaderAndExpected(fileSchema, *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, readSampleBigintRangeFilter) {
  // Read sample.parquet with the int filter "a BETWEEN 16 AND 20".
  FilterMap filters;
  filters.insert({"a", exec::between(16, 20)});
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(5, [](auto row) { return row + 16; }),
      makeFlatVector<double>(5, [](auto row) { return row + 16; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, readSampleBigintValuesUsingBitmaskFilter) {
  // Read sample.parquet with the int filter "a in 16, 17, 18, 19, 20".
  std::vector<int64_t> values{16, 17, 18, 19, 20};
  auto bigintBitmaskFilter =
      std::make_unique<bytedance::bolt::common::BigintValuesUsingBitmask>(
          16, 20, std::move(values), false);
  FilterMap filters;
  filters.insert({"a", std::move(bigintBitmaskFilter)});
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(5, [](auto row) { return row + 16; }),
      makeFlatVector<double>(5, [](auto row) { return row + 16; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, readSampleEqualFilter) {
  // Read sample.parquet with the int filter "a = 16".
  FilterMap filters;
  filters.insert({"a", exec::equal(16)});

  auto expected = makeRowVector({
      makeFlatVector<int64_t>(1, [](auto row) { return row + 16; }),
      makeFlatVector<double>(1, [](auto row) { return row + 16; }),
  });

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, dateFilters) {
  // Read date.parquet with the date filter "date BETWEEN 5 AND 14".
  FilterMap filters;
  filters.insert({"date", exec::between(5, 14)});

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(10, [](auto row) { return row + 5; }),
  });

  assertReadWithFilters(
      "date.parquet", dateSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, intMultipleFilters) {
  // Filter int BETWEEN 102 AND 120 AND bigint BETWEEN 900 AND 1006.
  FilterMap filters;
  filters.insert({"int", exec::between(102, 120)});
  filters.insert({"bigint", exec::between(900, 1006)});

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(5, [](auto row) { return row + 102; }),
      makeFlatVector<int64_t>(5, [](auto row) { return row + 1002; }),
  });

  assertReadWithFilters(
      "int.parquet", intSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, doubleFilters) {
  // Read sample.parquet with the double filter "b < 10.0".
  FilterMap filters;
  filters.insert({"b", exec::lessThanDouble(10.0)});

  auto expected = makeRowVector({
      makeFlatVector<int64_t>(9, [](auto row) { return row + 1; }),
      makeFlatVector<double>(9, [](auto row) { return row + 1; }),
  });

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
  filters.clear();

  // Test "b <= 10.0".
  filters.insert({"b", exec::lessThanOrEqualDouble(10.0)});
  expected = makeRowVector({
      makeFlatVector<int64_t>(10, [](auto row) { return row + 1; }),
      makeFlatVector<double>(10, [](auto row) { return row + 1; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
  filters.clear();

  // Test "b between 10.0 and 14.0".
  filters.insert({"b", exec::betweenDouble(10.0, 14.0)});
  expected = makeRowVector({
      makeFlatVector<int64_t>(5, [](auto row) { return row + 10; }),
      makeFlatVector<double>(5, [](auto row) { return row + 10; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
  filters.clear();

  // Test "b > 14.0".
  filters.insert({"b", exec::greaterThanDouble(14.0)});
  expected = makeRowVector({
      makeFlatVector<int64_t>(6, [](auto row) { return row + 15; }),
      makeFlatVector<double>(6, [](auto row) { return row + 15; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
  filters.clear();

  // Test "b >= 14.0".
  filters.insert({"b", exec::greaterThanOrEqualDouble(14.0)});
  expected = makeRowVector({
      makeFlatVector<int64_t>(7, [](auto row) { return row + 14; }),
      makeFlatVector<double>(7, [](auto row) { return row + 14; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
  filters.clear();
}

TEST_F(ParquetReaderTest, varcharFilters) {
  // Test "name < 'CANADA'".
  FilterMap filters;
  filters.insert({"name", exec::lessThan("CANADA")});

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2}),
      makeFlatVector<std::string>({"ALGERIA", "ARGENTINA", "BRAZIL"}),
      makeFlatVector<int64_t>({0, 1, 1}),
  });

  auto rowType =
      ROW({"nationkey", "name", "regionkey"}, {BIGINT(), VARCHAR(), BIGINT()});

  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name <= 'CANADA'".
  filters.insert({"name", exec::lessThanOrEqual("CANADA")});
  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3}),
      makeFlatVector<std::string>({"ALGERIA", "ARGENTINA", "BRAZIL", "CANADA"}),
      makeFlatVector<int64_t>({0, 1, 1, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name > UNITED KINGDOM".
  filters.insert({"name", exec::greaterThan("UNITED KINGDOM")});
  expected = makeRowVector({
      makeFlatVector<int64_t>({21, 24}),
      makeFlatVector<std::string>({"VIETNAM", "UNITED STATES"}),
      makeFlatVector<int64_t>({2, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name >= 'UNITED KINGDOM'".
  filters.insert({"name", exec::greaterThanOrEqual("UNITED KINGDOM")});
  expected = makeRowVector({
      makeFlatVector<int64_t>({21, 23, 24}),
      makeFlatVector<std::string>(
          {"VIETNAM", "UNITED KINGDOM", "UNITED STATES"}),
      makeFlatVector<int64_t>({2, 3, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name = 'CANADA'".
  filters.insert({"name", exec::equal("CANADA")});
  expected = makeRowVector({
      makeFlatVector<int64_t>(1, [](auto row) { return row + 3; }),
      makeFlatVector<std::string>({"CANADA"}),
      makeFlatVector<int64_t>(1, [](auto row) { return row + 1; }),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name IN ('CANADA', 'UNITED KINGDOM')".
  filters.insert({"name", exec::in({std::string("CANADA"), "UNITED KINGDOM"})});
  expected = makeRowVector({
      makeFlatVector<int64_t>({3, 23}),
      makeFlatVector<std::string>({"CANADA", "UNITED KINGDOM"}),
      makeFlatVector<int64_t>({1, 3}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name IN ('UNITED STATES', 'CANADA', 'INDIA', 'RUSSIA')".
  filters.insert(
      {"name",
       exec::in({std::string("UNITED STATES"), "INDIA", "CANADA", "RUSSIA"})});
  expected = makeRowVector({
      makeFlatVector<int64_t>({3, 8, 22, 24}),
      makeFlatVector<std::string>(
          {"CANADA", "INDIA", "RUSSIA", "UNITED STATES"}),
      makeFlatVector<int64_t>({1, 2, 3, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);
}

// This test is to verify filterRowGroups() doesn't throw the fileOffset Bolt
// check failure
TEST_F(ParquetReaderTest, filterRowGroups) {
  // decimal_no_ColumnMetadata.parquet has one columns a: DECIMAL(9,1). It
  // doesn't have ColumnMetaData, and rowGroups_[0].columns[0].file_offset is 0.
  auto rowType = ROW({"_c0"}, {DECIMAL(9, 1)});
  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  const std::string decimal_dict(
      getExampleFilePath("decimal_no_ColumnMetadata.parquet"));

  auto reader = createReader(decimal_dict, readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  EXPECT_EQ(reader->numberOfRows(), 10ULL);
}

TEST_F(ParquetReaderTest, parseLongTagged) {
  // This is a case for long with annotation read
  const std::string sample(getExampleFilePath("tagged_long.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 4ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(type->childByName("_c0"), col0);
}

TEST_F(ParquetReaderTest, preloadSmallFile) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  auto file = std::make_shared<LocalReadFile>(sample);
  auto input = std::make_unique<BufferedInput>(file, *leafPool_);

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader =
      std::make_unique<ParquetReader>(std::move(input), readerOptions);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  // Ensure the input is small parquet file.
  const auto fileSize = file->size();
  ASSERT_TRUE(
      fileSize <= bytedance::bolt::dwio::common::ReaderOptions::
                      kDefaultFilePreloadThreshold ||
      fileSize <= bytedance::bolt::dwio::common::ReaderOptions::
                      kDefaultFooterEstimatedSize);

  // Check the whole file already loaded.
  ASSERT_EQ(file->bytesRead(), fileSize);

  // Reset bytes read to check for duplicate reads.
  file->resetBytesRead();

  constexpr int kBatchSize = 10;
  auto result = BaseVector::create(sampleSchema(), 1, leafPool_.get());
  while (rowReader->next(kBatchSize, result)) {
    // Check no duplicate reads.
    ASSERT_EQ(file->bytesRead(), 0);
  }
}

TEST_F(ParquetReaderTest, prefetchRowGroups) {
  auto rowType = ROW({"id"}, {BIGINT()});
  const std::string sample(getExampleFilePath("multiple_row_groups.parquet"));
  const int numRowGroups = 4;

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  // Disable preload of file.
  readerOptions.setFilePreloadThreshold(0);

  // Test different number of prefetch row groups.
  // 2: Less than total number of row groups.
  // 4: Exactly as total number of row groups.
  // 10: More than total number of row groups.
  const std::vector<int> numPrefetchRowGroups{
      bytedance::bolt::dwio::common::ReaderOptions::kDefaultPrefetchRowGroups,
      2,
      4,
      10};
  for (auto numPrefetch : numPrefetchRowGroups) {
    readerOptions.setPrefetchRowGroups(numPrefetch);

    auto reader = createReader(sample, readerOptions);
    EXPECT_EQ(reader->fileMetaData().numRowGroups(), numRowGroups);

    RowReaderOptions rowReaderOpts;
    rowReaderOpts.setScanSpec(makeScanSpec(rowType));
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto parquetRowReader = dynamic_cast<ParquetRowReader*>(rowReader.get());

    constexpr int kBatchSize = 1000;
    auto result = BaseVector::create(rowType, kBatchSize, pool_.get());

    for (int i = 0; i < numRowGroups; i++) {
      if (i > 0) {
        // If it's not the first row group, check if the previous row group has
        // been evicted.
        EXPECT_FALSE(parquetRowReader->isRowGroupBuffered(i - 1));
      }
      EXPECT_TRUE(parquetRowReader->isRowGroupBuffered(i));
      if (i < numRowGroups - 1) {
        // If it's not the last row group, check if the configured number of
        // row groups have been prefetched.
        for (int j = 1; j <= numPrefetch && i + j < numRowGroups; j++) {
          EXPECT_TRUE(parquetRowReader->isRowGroupBuffered(i + j));
        }
      }

      // Read current row group.
      auto actualRows = parquetRowReader->next(kBatchSize, result);
      // kBatchSize should be large enough to hold the entire row group.
      EXPECT_LE(actualRows, kBatchSize);
      // Advance to the next row group.
      parquetRowReader->nextRowNumber();
    }
  }
}

TEST_F(ParquetReaderTest, testEmptyRowGroups) {
  // empty_row_groups.parquet contains empty row groups
  const std::string sample(getExampleFilePath("empty_row_groups.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 5ULL);

  auto rowType = reader->typeWithId();
  EXPECT_EQ(rowType->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->size(), 1ULL);

  auto integerType = rowType->childAt(0);
  EXPECT_EQ(integerType->type()->kind(), TypeKind::INTEGER);

  auto fileSchema = ROW({"a"}, {INTEGER()});
  auto rowReaderOpts = getReaderOpts(fileSchema);
  auto scanSpec = makeScanSpec(fileSchema);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({makeFlatVector<int32_t>({0, 3, 3, 3, 3})});

  assertReadWithReaderAndExpected(fileSchema, *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, arrayWithEmptyEntry) {
  // empty_array.parquet holds one column (gpslocation : array<string>) and 7
  // rows Data is in plain uncompressed format:
  // ["a","a","a"]
  // ["b","b","b"]
  // ["b","b","b"]
  // []
  // []
  // ["c","c","c"]
  // ["d","d","d"]
  const std::string sample(getExampleFilePath("empty_array.parquet"));
  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 7ULL);
  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::ARRAY);
  EXPECT_EQ(type->childByName("gpslocation"), col0);

  auto rowReaderOpts = getReaderOpts(arrayEmptySchema());
  auto scanSpec = makeScanSpec(arrayEmptySchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto arrayVector = vectorMaker_.arrayVector<StringView>(
      {{"a", "a", "a"},
       {"b", "b", "b"},
       {"b", "b", "b"},
       {},
       {},
       {"c", "c", "c"},
       {"d", "d", "d"}});
  auto expected = vectorMaker_.rowVector({arrayVector});

  auto result = BaseVector::create(arrayEmptySchema(), 7, leafPool_.get());
  rowReader->next(7, result);
  assertEqualVectorPart(expected, result, 0);
}

TEST_F(ParquetReaderTest, readEncryptedParquet) {
  auto rowType = ROW({"id", "name", "salary"}, {BIGINT(), VARCHAR(), BIGINT()});

  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};

  std::string encryptionData = getExampleFilePath("encrypted_sample.parquet");

  auto reader = createReader(encryptionData, readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto numRows = reader->numberOfRows();
  ASSERT_TRUE(numRows.has_value());
  EXPECT_EQ(numRows.value(), 3);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), rowType->size());
  EXPECT_EQ(type->type()->kind(), TypeKind::ROW);

  auto result = BaseVector::create(rowType, 3, leafPool_.get());
  auto rowsRead = rowReader->next(3, result);
  EXPECT_EQ(rowsRead, 3);
  EXPECT_EQ(result->size(), 3);

  auto ids = result->as<RowVector>()->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(ids->valueAt(0), 1);
}

TEST_F(ParquetReaderTest, readEncryptedParquetAllValues) {
  auto rowType = ROW({"id", "name", "salary"}, {BIGINT(), VARCHAR(), BIGINT()});

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<std::string>({"Alice", "Bob", "Charlie"}),
      makeFlatVector<int64_t>({100000, 90000, 110000}),
  });

  assertReadWithExpected("encrypted_sample.parquet", rowType, expected);
}

TEST_F(ParquetReaderTest, readEncryptedParquetWithProjection) {
  auto projectedType = ROW({"name", "salary"}, {VARCHAR(), BIGINT()});

  const std::string sample(getExampleFilePath("encrypted_sample.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  readerOptions.setFileSchema(projectedType);
  auto reader = createReader(sample, readerOptions);

  auto rowReaderOpts = getReaderOpts(projectedType);
  auto scanSpec = makeScanSpec(projectedType);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({
      makeFlatVector<std::string>({"Alice", "Bob", "Charlie"}),
      makeFlatVector<int64_t>({100000, 90000, 110000}),
  });

  auto result = BaseVector::create(projectedType, 3, leafPool_.get());
  auto rowsRead = rowReader->next(3, result);
  EXPECT_EQ(rowsRead, 3);
  EXPECT_EQ(result->size(), 3);
  assertEqualVectorPart(expected, result, 0);
}

TEST_F(ParquetReaderTest, readEncryptedParquetWithFilters) {
  auto rowType = ROW({"id", "name", "salary"}, {BIGINT(), VARCHAR(), BIGINT()});

  FilterMap filters;
  filters.insert({"salary", exec::greaterThan(int64_t(100000))});

  auto expected = makeRowVector({
      makeFlatVector<int64_t>(1, [](auto row) { return 3; }),
      makeFlatVector<std::string>({"Charlie"}),
      makeFlatVector<int64_t>(1, [](auto row) { return 110000; }),
  });

  assertReadWithFilters(
      "encrypted_sample.parquet", rowType, std::move(filters), expected);
}

TEST_F(ParquetReaderTest, testEnumType) {
  // enum_type.parquet contains 1 column (ENUM) with 3 rows.
  const std::string sample(getExampleFilePath("enum_type.parquet"));

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 3ULL);

  auto rowType = reader->typeWithId();
  EXPECT_EQ(rowType->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->size(), 1ULL);

  EXPECT_EQ(rowType->childAt(0)->type()->kind(), TypeKind::VARCHAR);

  auto fileSchema = ROW({"test"}, {VARCHAR()});
  auto rowReaderOpts = getReaderOpts(fileSchema);
  rowReaderOpts.setScanSpec(makeScanSpec(fileSchema));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected =
      makeRowVector({makeFlatVector<StringView>({"FOO", "BAR", "FOO"})});

  assertReadWithReaderAndExpected(fileSchema, *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, readBinaryAsStringFromNation) {
  const std::string filename("nation.parquet");
  const std::string sample(getExampleFilePath(filename));

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto outputRowType =
      ROW({"nationkey", "name", "regionkey", "comment"},
          {BIGINT(), VARCHAR(), BIGINT(), VARCHAR()});

  readerOptions.setFileSchema(outputRowType);
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 25ULL);
  auto rowType = reader->typeWithId();
  EXPECT_EQ(rowType->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->size(), 4ULL);
  EXPECT_EQ(rowType->childAt(1)->type()->kind(), TypeKind::VARCHAR);

  auto rowReaderOpts = getReaderOpts(outputRowType);
  rowReaderOpts.setScanSpec(makeScanSpec(outputRowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = std::string("ALGERIA");
  VectorPtr result = BaseVector::create(outputRowType, 0, &(*leafPool_));
  rowReader->next(1, result);
  EXPECT_EQ(
      expected,
      result->as<RowVector>()->childAt(1)->asFlatVector<StringView>()->valueAt(
          0));
}

TEST_F(ParquetReaderTest, readComplexType) {
  const std::string filename("complex_with_varchar_varbinary.parquet");
  const std::string sample(getExampleFilePath(filename));

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto outputRowType =
      ROW({"a", "b", "c", "d"},
          {ARRAY(VARCHAR()),
           ARRAY(VARBINARY()),
           MAP(VARCHAR(), BIGINT()),
           MAP(VARBINARY(), BIGINT())});

  readerOptions.setFileSchema(outputRowType);
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 1);
  auto rowType = reader->rowType();
  EXPECT_EQ(rowType->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->size(), 4);
  EXPECT_EQ(*rowType, *outputRowType);

  auto rowReaderOpts = getReaderOpts(outputRowType);
  rowReaderOpts.setScanSpec(makeScanSpec(outputRowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  VectorPtr result = BaseVector::create(outputRowType, 0, &(*leafPool_));
  rowReader->next(1, result);
  auto aColVector = result->as<RowVector>()
                        ->childAt(0)
                        ->loadedVector()
                        ->as<ArrayVector>()
                        ->elements();
  EXPECT_EQ(aColVector->size(), 3);
  EXPECT_EQ(aColVector->encoding(), VectorEncoding::Simple::DICTIONARY);
  EXPECT_EQ(
      aColVector->asUnchecked<DictionaryVector<StringView>>()->valueAt(0).str(),
      "AAAA");

  auto cColVector =
      result->as<RowVector>()->childAt(2)->loadedVector()->as<MapVector>();
  auto mapKeys = cColVector->mapKeys();
  EXPECT_EQ(mapKeys->size(), 2);
  EXPECT_EQ(mapKeys->encoding(), VectorEncoding::Simple::DICTIONARY);
  EXPECT_EQ(
      mapKeys->asUnchecked<DictionaryVector<StringView>>()->valueAt(0).str(),
      "foo");
}

TEST_F(ParquetReaderTest, readFixedLenBinaryAsStringFromUuid) {
  const std::string filename("uuid.parquet");
  const std::string sample(getExampleFilePath(filename));

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto outputRowType = ROW({"uuid_field"}, {VARCHAR()});

  readerOptions.setFileSchema(outputRowType);
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 3ULL);
  auto rowType = reader->typeWithId();
  EXPECT_EQ(rowType->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->size(), 1ULL);
  EXPECT_EQ(rowType->childAt(0)->type()->kind(), TypeKind::VARCHAR);

  auto rowReaderOpts = getReaderOpts(outputRowType);
  rowReaderOpts.setScanSpec(makeScanSpec(outputRowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = std::string("5468454a-363f-ccc8-7d0b-76072a75dfaa");
  VectorPtr result = BaseVector::create(outputRowType, 0, &(*leafPool_));
  rowReader->next(1, result);
  EXPECT_EQ(
      expected,
      result->as<RowVector>()->childAt(0)->asFlatVector<StringView>()->valueAt(
          0));
}

TEST_F(ParquetReaderTest, skip) {
  auto parquetData = makeRowVector(
      {"c0", "c1", "c2", "c3", "c4"},
      {makeNullableFlatVector<int32_t>({std::nullopt, 5, 63, std::nullopt}),
       makeNullableFlatVector<double>({15.9, std::nullopt, 20.1, 58.3}),
       makeNullableMapVector<std::string, uint64_t>(
           {std::nullopt,
            {{{"beijing", std::nullopt}}},
            {},
            {{{"shanghai", 1},
              {"guangdongguangzhou", 2},
              {"guangdongshenzhen", 3}}}}),
       makeNullableArrayVector<int32_t>(
           {{{1, 2, std::nullopt, 4}},
            emptyArray,
            std::nullopt,
            {{15, 26, std::nullopt}}}),
       makeRowVector(
           {"c10", "c11"},
           {makeNullableFlatVector<std::string>(
                {std::nullopt, "abc", "guangdongguangzhou", ""}),
            makeNullableFlatVector<double>(
                {15.9, std::nullopt, 20.1, 58.3})})});
  auto rowType = std::static_pointer_cast<const RowType>(parquetData->type());

  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  readerOpts.setFileSchema(rowType);
  readerOpts.setFileFormat(dwio::common::FileFormat::PARQUET);
  auto reader =
      createReader(getExampleFilePath("parquetSkip.parquet"), readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  VectorPtr result = BaseVector::create(rowType, 0, leafPool_.get());
  rowReader->skip(1);
  auto readRowNum = rowReader->next(1, result);
  EXPECT_EQ(1, readRowNum);
  EXPECT_EQ(1, result->size());
  assertEqualVectorPart(parquetData, result, 1);
  rowReader->skip(1);
  readRowNum = rowReader->next(1, result);
  EXPECT_EQ(1, readRowNum);
  EXPECT_EQ(1, result->size());
  assertEqualVectorPart(parquetData, result, 3);
  EXPECT_EQ(0, rowReader->next(1000, result));
}

TEST_F(ParquetReaderTest, readVarbinaryFromFLBA) {
  const std::string filename("varbinary_flba.parquet");
  const std::string sample(getExampleFilePath(filename));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 8ULL);
  auto flbaCol =
      std::static_pointer_cast<const ParquetTypeWithId>(type->childAt(6));
  EXPECT_EQ(flbaCol->name_, "flba_field");
  EXPECT_EQ(flbaCol->parquetType_, thrift::Type::FIXED_LEN_BYTE_ARRAY);

  auto selectedType = ROW({"flba_field"}, {VARBINARY()});
  auto rowReaderOpts = getReaderOpts(selectedType);
  rowReaderOpts.setScanSpec(makeScanSpec(selectedType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = std::string(1024, '*');
  VectorPtr result = BaseVector::create(selectedType, 0, &(*leafPool_));
  rowReader->next(1, result);
  EXPECT_EQ(
      expected,
      result->as<RowVector>()->childAt(0)->asFlatVector<StringView>()->valueAt(
          0));
}

TEST_F(ParquetReaderTest, arrayOfMapOfIntKeyArrayValue) {
  //  The Schema is of type
  //  message hive_schema {
  //    optional group test (LIST) {
  //      repeated group array (MAP) {
  //        repeated group key_value (MAP_KEY_VALUE) {
  //          required binary key (UTF8);
  //          optional group value (LIST) {
  //            repeated int32 array;
  //          }
  //        }
  //      }
  //    }
  //  }
  const std::string expectedBoltType =
      "ROW<test:ARRAY<MAP<VARCHAR,ARRAY<INTEGER>>>>";
  const std::string sample(
      getExampleFilePath("array_of_map_of_int_key_array_value.parquet"));
  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->rowType()->toString(), expectedBoltType);
  auto numRows = reader->numberOfRows();
  auto type = reader->typeWithId();
  RowReaderOptions rowReaderOpts;
  auto rowType = ROW({"test"}, {ARRAY(MAP(VARCHAR(), ARRAY(INTEGER())))});
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(rowType, 10, leafPool_.get());
  constexpr int kBatchSize = 1000;
  while (rowReader->next(kBatchSize, result)) {
  }
}

TEST_F(ParquetReaderTest, arrayOfMapOfIntKeyStructValue) {
  //  The Schema is of type
  //   message hive_schema {
  //    optional group test (LIST) {
  //      repeated group array (MAP) {
  //        repeated group key_value (MAP_KEY_VALUE) {
  //          required int32 key;
  //          optional group value {
  //            optional binary stringfield (UTF8);
  //            optional int64 longfield;
  //          }
  //        }
  //      }
  //    }
  //  }
  const std::string expectedBoltType =
      "ROW<test:ARRAY<MAP<INTEGER,ROW<stringfield:VARCHAR,longfield:BIGINT>>>>";
  const std::string sample(
      getExampleFilePath("array_of_map_of_int_key_struct_value.parquet"));
  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->rowType()->toString(), expectedBoltType);
  auto numRows = reader->numberOfRows();
  auto type = reader->typeWithId();
  RowReaderOptions rowReaderOpts;
  auto rowType = reader->rowType();
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(rowType, 10, leafPool_.get());
  constexpr int kBatchSize = 1000;
  while (rowReader->next(kBatchSize, result)) {
  }
}

TEST_F(ParquetReaderTest, struct_of_array_of_array) {
  //  The Schema is of type
  //  message hive_schema {
  //    optional group test {
  //      optional group stringarrayfield (LIST) {
  //        repeated group array (LIST) {
  //          repeated binary array (UTF8);
  //        }
  //      }
  //      optional group intarrayfield (LIST) {
  //        repeated group array (LIST) {
  //          repeated int32 array;
  //        }
  //      }
  //    }
  //  }
  const std::string expectedBoltType =
      "ROW<test:ROW<stringarrayfield:ARRAY<ARRAY<VARCHAR>>,intarrayfield:ARRAY<ARRAY<INTEGER>>>>";
  const std::string sample(
      getExampleFilePath("struct_of_array_of_array.parquet"));
  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  auto numRows = reader->numberOfRows();
  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  EXPECT_EQ(reader->rowType()->toString(), expectedBoltType);

  auto test_column = type->childAt(0);
  EXPECT_EQ(test_column->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(type->childByName("test"), test_column);

  // test_column has 2 children
  EXPECT_EQ(test_column->size(), 2ULL);
  // explore 1st child of test_column
  auto stringarrayfield_column = test_column->childAt(0);
  EXPECT_EQ(stringarrayfield_column->type()->kind(), TypeKind::ARRAY);

  // stringarrayfield_column column has 1 child
  EXPECT_EQ(stringarrayfield_column->size(), 1ULL);
  // explore 1st child of stringarrayfield_column
  auto array_column = stringarrayfield_column->childAt(0);
  EXPECT_EQ(array_column->type()->kind(), TypeKind::ARRAY);

  // array_column column has 1 child
  EXPECT_EQ(array_column->size(), 1ULL);
  // explore 1st child of array_column
  auto array_leaf_column = array_column->childAt(0);
  EXPECT_EQ(array_leaf_column->type()->kind(), TypeKind::VARCHAR);

  // explore 2nd child of test_column
  auto intarrayfield_column = test_column->childAt(1);
  EXPECT_EQ(intarrayfield_column->type()->kind(), TypeKind::ARRAY);
  EXPECT_EQ(test_column->childByName("intarrayfield"), intarrayfield_column);

  // intarrayfield_column column has 1 child
  EXPECT_EQ(intarrayfield_column->size(), 1ULL);
  // explore 1st child of intarrayfield_column
  auto array_column_for_intarrayfield = intarrayfield_column->childAt(0);
  EXPECT_EQ(array_column_for_intarrayfield->type()->kind(), TypeKind::ARRAY);

  // array_column_for_intarrayfield column has 1 child
  EXPECT_EQ(array_column_for_intarrayfield->size(), 1ULL);
  // explore 1st child
  auto array_leaf_column_for_intarrayfield =
      array_column_for_intarrayfield->childAt(0);
  EXPECT_EQ(
      array_leaf_column_for_intarrayfield->type()->kind(), TypeKind::INTEGER);

  RowReaderOptions rowReaderOpts;
  auto rowType =
      ROW({"test"},
          {ROW(
              {"stringarrayfield", "intarrayfield"},
              {ARRAY(ARRAY(VARCHAR())), ARRAY(ARRAY(INTEGER()))})});
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(rowType, 10, leafPool_.get());
  constexpr int kBatchSize = 1000;
  while (rowReader->next(kBatchSize, result)) {
  }
}

TEST_F(ParquetReaderTest, readDisputedNoLogicalType) {
  const std::string sample(getExampleFilePath("no_logical_type.parquet"));
  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  readerOptions.setFileSchema(ROW({"disputed"}, {VARCHAR()}));
  auto reader = createReader(sample, readerOptions);
  auto rowType = ROW({"disputed"}, {VARCHAR()});
  auto rowReaderOpts = getReaderOpts(rowType);
  auto scanSpec = makeScanSpec(rowType);
  scanSpec->getOrCreateChild(Subfield("disputed"))
      ->setFilter(exec::equal("Yes"));
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(rowType, 0, leafPool_.get());
  uint64_t total = 0;
  for (;;) {
    auto n = rowReader->next(1024, result);
    if (n == 0)
      break;
    total += n;
  }
  EXPECT_GT(total, 0);
}

// Verify that when LogicalType is absent but legacy ConvertedType (e.g., UTF8)
// is present on a BYTE_ARRAY column, the reader annotates ScanSpec with the
// converted type. This enables VARCHAR pruning to rely on ConvertedType as a
// fallback.
TEST_F(ParquetReaderTest, varcharConvertedTypePropagatedWhenLogicalMissing) {
  // Use column-only fixture extracted from real data; verifies ConvertedType
  // propagation used by VARCHAR pruning.
  const std::string sample(
      getExampleFilePath("varchar_converted_type_fallback_stat_type.parquet"));
  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  // Request VARCHAR for the 'stat_type' column.
  auto rowType = ROW({"stat_type"}, {VARCHAR()});
  auto rowReaderOpts = getReaderOpts(rowType);
  auto scanSpec = makeScanSpec(rowType);
  rowReaderOpts.setScanSpec(scanSpec);

  auto reader = createReader(sample, readerOptions);
  // Annotation happens during row reader creation.
  auto rowReader = reader->createRowReader(rowReaderOpts);
  (void)rowReader;

  auto spec = scanSpec->childByName("stat_type");
  ASSERT_NE(spec, nullptr);
  // Converted type should be annotated from schema (UTF8 for textual byte
  // data).
  EXPECT_EQ(spec->convertedTypeName(), "UTF8");
}

TEST_F(ParquetReaderTest, dcMapSimple) {
  // dcmapSimple.parquet stores all the values inside dynamic columns.
  const std::string sample(getExampleFilePath("dcmapSimple.parquet"));
  // scanSpec we get from HMS will be map(varchar, varchar)
  // so DCMap reader should be able to handle mismatched types.
  auto rowType =
      ROW({"name", "age", "accounts"},
          {VARCHAR(), BIGINT(), MAP(VARCHAR(), VARCHAR())});
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({
      makeFlatVector<StringView>({"xeonliu", "wukong"}),
      makeFlatVector<int64_t>({18, 500}),
      makeMapVector<StringView, StringView>({
          {{"baidu", "2020-01-01"},
           {"douyin", "2012-04-05"},
           {"tencent", "2011-02-03"}},
          {{"baidu", "2015-01-01"}, {"toutiao", "2013-02-04"}},
      }),
  });

  assertReadWithReaderAndExpected(rowType, *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, integerToVarcharSchemaMismatchCast) {
  // Register functions needed by the cast expression evaluator.
  functions::prestosql::registerAllScalarFunctions();

  // 1. Write a parquet file with an INTEGER column.
  auto fileSchema = ROW({"col"}, {INTEGER()});
  auto data =
      makeRowVector({"col"}, {makeFlatVector<int32_t>({1, 2, 3, 42, -100})});

  auto tempFile = exec::test::TempFilePath::create();
  {
    auto writeFile =
        std::make_unique<LocalWriteFile>(tempFile->getPath(), true, false);
    auto sink = std::make_unique<dwio::common::WriteFileSink>(
        std::move(writeFile), tempFile->getPath());
    bytedance::bolt::parquet::WriterOptions writerOptions;
    writerOptions.memoryPool = rootPool_.get();
    auto writer = std::make_unique<bytedance::bolt::parquet::Writer>(
        std::move(sink), writerOptions, fileSchema);
    writer->write(data);
    writer->close();
  }

  // 2. Read the file back requesting VARCHAR type for the INTEGER column.
  //    This triggers IntegerColumnReader::makeCastExpr() which needs
  //    scanSpec->getExpressionEvaluator() to be non-null.
  auto readSchema = ROW({"col"}, {VARCHAR()});

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(tempFile->getPath(), readerOptions);

  // set expressionEvaluator on root FIRST, then add child fields.
  auto queryCtx = core::QueryCtx::create();
  exec::SimpleExpressionEvaluator evaluator(queryCtx.get(), leafPool_.get());
  auto scanSpec = std::make_shared<ScanSpec>("");
  scanSpec->setExpressionEvaluator(&evaluator);
  scanSpec->addAllChildFields(*readSchema);

  auto rowReaderOpts = getReaderOpts(readSchema);
  rowReaderOpts.setScanSpec(scanSpec);

  // IntegerColumnReader's constructor calls makeCastExpr(), which
  // accesses scanSpec_->getExpressionEvaluator() on a child ScanSpec.
  auto rowReader = reader->createRowReader(rowReaderOpts);

  // 3. Actually read and verify the cast results.
  VectorPtr result = BaseVector::create(readSchema, 0, leafPool_.get());
  auto numRows = rowReader->next(10, result);
  ASSERT_EQ(numRows, 5);

  auto rowResult = result->as<RowVector>();
  auto colResult = rowResult->childAt(0)->asFlatVector<StringView>();
  ASSERT_NE(colResult, nullptr);
  EXPECT_EQ(colResult->valueAt(0).str(), "1");
  EXPECT_EQ(colResult->valueAt(1).str(), "2");
  EXPECT_EQ(colResult->valueAt(2).str(), "3");
  EXPECT_EQ(colResult->valueAt(3).str(), "42");
  EXPECT_EQ(colResult->valueAt(4).str(), "-100");
}

// Same regression test but in the reverse direction: reading a Parquet
// VARCHAR/STRING column as BIGINT, which triggers
// StringColumnReader::makeCastExpr().
TEST_F(ParquetReaderTest, varcharToBigintSchemaMismatchCast) {
  functions::prestosql::registerAllScalarFunctions();

  // 1. Write a parquet file with a VARCHAR column containing numeric strings.
  auto fileSchema = ROW({"col"}, {VARCHAR()});
  auto data = makeRowVector(
      {"col"}, {makeFlatVector<StringView>({"100", "200", "300", "-42", "0"})});

  auto tempFile = exec::test::TempFilePath::create();
  {
    auto writeFile =
        std::make_unique<LocalWriteFile>(tempFile->getPath(), true, false);
    auto sink = std::make_unique<dwio::common::WriteFileSink>(
        std::move(writeFile), tempFile->getPath());
    bytedance::bolt::parquet::WriterOptions writerOptions;
    writerOptions.memoryPool = rootPool_.get();
    auto writer = std::make_unique<bytedance::bolt::parquet::Writer>(
        std::move(sink), writerOptions, fileSchema);
    writer->write(data);
    writer->close();
  }

  // 2. Read the file back requesting BIGINT type for the VARCHAR column.
  auto readSchema = ROW({"col"}, {BIGINT()});

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(tempFile->getPath(), readerOptions);

  auto queryCtx = core::QueryCtx::create();
  exec::SimpleExpressionEvaluator evaluator(queryCtx.get(), leafPool_.get());
  auto scanSpec = std::make_shared<ScanSpec>("");
  scanSpec->setExpressionEvaluator(&evaluator);
  scanSpec->addAllChildFields(*readSchema);

  auto rowReaderOpts = getReaderOpts(readSchema);
  rowReaderOpts.setScanSpec(scanSpec);

  // In non-SPARK builds this is rejected by ParquetColumnReader::matchType.
#ifndef SPARK_COMPATIBLE
  EXPECT_THROW(reader->createRowReader(rowReaderOpts), BoltRuntimeError);
  return;
#endif

  // In SPARK-compatible builds, schema mismatch is allowed and cast is applied.
  auto rowReader = reader->createRowReader(rowReaderOpts);

  VectorPtr result = BaseVector::create(readSchema, 0, leafPool_.get());
  auto numRows = rowReader->next(10, result);
  ASSERT_EQ(numRows, 5);

  auto rowResult = result->as<RowVector>();
  auto colVector = rowResult->childAt(0);
  ASSERT_NE(colVector, nullptr);
  // The result may be dictionary-encoded after cast, so decode it.
  DecodedVector decoded(*colVector, SelectivityVector(numRows));
  for (int i = 0; i < numRows; ++i) {
    ASSERT_FALSE(decoded.isNullAt(i));
  }
  EXPECT_EQ(decoded.valueAt<int64_t>(0), 100);
  EXPECT_EQ(decoded.valueAt<int64_t>(1), 200);
  EXPECT_EQ(decoded.valueAt<int64_t>(2), 300);
  EXPECT_EQ(decoded.valueAt<int64_t>(3), -42);
  EXPECT_EQ(decoded.valueAt<int64_t>(4), 0);
}

TEST_F(ParquetReaderTest, readVariantParquet) {
  const std::string sample(getVariantFixturePath("variant_sample.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 4ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col0->type()->kind(), TypeKind::INTEGER);
  EXPECT_EQ(col1->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(col1->type()->size(), 2);
  EXPECT_EQ(col1->type()->childAt(0)->kind(), TypeKind::VARBINARY);
  EXPECT_EQ(col1->type()->childAt(1)->kind(), TypeKind::VARBINARY);

  auto rowType = ROW({"id", "v"}, {INTEGER(), VARIANT()});
  auto rowReaderOpts = getReaderOpts(rowType);
  auto scanSpec = std::make_shared<bytedance::bolt::common::ScanSpec>("");
  scanSpec->addFieldRecursively("id", *rowType->childAt(0), 0);
  scanSpec->addFieldRecursively("v", *VARIANT(), 1);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto result = BaseVector::create(rowType, 4, leafPool_.get());
  auto rowsRead = rowReader->next(10, result);
  EXPECT_EQ(rowsRead, 4);
  EXPECT_EQ(result->size(), 4);

  auto row = result->as<RowVector>();
  auto ids = row->childAt(0)->asFlatVector<int32_t>();
  EXPECT_EQ(ids->valueAt(0), 1);
  EXPECT_EQ(ids->valueAt(1), 2);
  EXPECT_EQ(ids->valueAt(2), 3);
  EXPECT_EQ(ids->valueAt(3), 4);

  auto variants = row->childAt(1)->as<VariantVector>();
  std::vector<std::string> expectedJson = {
      "{\"a\":1,\"b\":[true,\"x\"],\"c\":{\"d\":3.14}}",
      "[1,2,3]",
      "\"hello\"",
      "null"};
  auto decodeValue = [&](const VariantValue& value) -> std::string {
    if (value.value.empty()) {
      return "null";
    }
    if (value.metadata.empty()) {
      if (simdjson::validate_utf8(value.value.data(), value.value.size())) {
        return std::string(value.value.data(), value.value.size());
      }
      return "null";
    }
    auto decoded = bytedance::bolt::functions::sparksql::variant::
        SparkVariantReader::decode(value.value, value.metadata);
    return decoded.value_or("null");
  };
  simdjson::dom::parser parser;
  for (int i = 0; i < 3; ++i) {
    auto value = variants->valueAt(i);
    EXPECT_FALSE(variants->isNullAt(i));
    EXPECT_FALSE(value.metadata.empty());
    EXPECT_EQ(
        static_cast<uint8_t>(value.metadata.data()[0]),
        bytedance::bolt::functions::sparksql::variant::VERSION);
    auto decoded = decodeValue(value);
    EXPECT_EQ(decoded, expectedJson[i]);
    simdjson::dom::element doc;
    EXPECT_EQ(parser.parse(decoded).get(doc), simdjson::SUCCESS);
  }
  auto value3 = variants->valueAt(3);
  auto decoded3 = decodeValue(value3);
  EXPECT_EQ(decoded3, expectedJson[3]);

  auto variantGet = [&](const VariantValue& value, const StringView& path) {
    auto out = makeFlatVector<StringView>(1);
    exec::StringWriter<> writer(out.get(), 0);
    bytedance::bolt::functions::sparksql::VariantGetFunction<exec::VectorExec>
        func;
    auto ok = func.call(writer, value, path);
    if (ok) {
      writer.finalize();
      return std::make_pair(ok, std::string(out->valueAt(0)));
    }
    return std::make_pair(ok, std::string());
  };

  auto [ok0, v0] = variantGet(variants->valueAt(0), "$.c.d");
  EXPECT_TRUE(ok0);
  EXPECT_EQ(v0, "3.14");
  auto [ok1, v1] = variantGet(variants->valueAt(0), "$.b[1]");
  EXPECT_TRUE(ok1);
  EXPECT_EQ(v1, "x");
  auto [ok2, v2] = variantGet(variants->valueAt(1), "$[2]");
  EXPECT_TRUE(ok2);
  EXPECT_EQ(v2, "3");
  auto [ok3, v3] = variantGet(variants->valueAt(2), "$");
  EXPECT_TRUE(ok3);
  EXPECT_EQ(v3, "hello");
  auto [ok4, v4] = variantGet(variants->valueAt(3), "$");
  // A VARIANT encoding of JSON null correctly decodes to "null".
  // With the std::optional decode fix, this is no longer confused with
  // a decode failure.
  EXPECT_TRUE(ok4);
  EXPECT_EQ(v4, "null");
}

TEST_F(ParquetReaderTest, readVariantParquetScanSpecOrderMismatch) {
  const std::string sample(getVariantFixturePath("variant_sample.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  auto rowType = ROW({"id", "v"}, {INTEGER(), VARIANT()});
  auto rowReaderOpts = getReaderOpts(rowType);

  // Intentionally add VARIANT children in reverse order.
  auto scanSpec = std::make_shared<bytedance::bolt::common::ScanSpec>("");
  scanSpec->addFieldRecursively("id", *rowType->childAt(0), 0);
  auto vSpec = scanSpec->addField("v", 1);
  vSpec->addFieldRecursively(
      "metadata",
      *rowType->childAt(1)->childAt(1),
      bytedance::bolt::common::ScanSpec::kNoChannel);
  vSpec->addFieldRecursively(
      "value",
      *rowType->childAt(1)->childAt(0),
      bytedance::bolt::common::ScanSpec::kNoChannel);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto result = BaseVector::create(rowType, 4, leafPool_.get());
  auto rowsRead = rowReader->next(10, result);
  EXPECT_EQ(rowsRead, 4);

  auto row = result->as<RowVector>();
  auto variants = row->childAt(1)->as<VariantVector>();

  auto variantGet = [&](const VariantValue& value, const StringView& path) {
    auto out = makeFlatVector<StringView>(1);
    exec::StringWriter<> writer(out.get(), 0);
    bytedance::bolt::functions::sparksql::VariantGetFunction<exec::VectorExec>
        func;
    auto ok = func.call(writer, value, path);
    if (ok) {
      writer.finalize();
      return std::make_pair(ok, std::string(out->valueAt(0)));
    }
    return std::make_pair(ok, std::string());
  };

  // If the reader fails to correct the ordering mismatch, this lookup is
  // expected to fail.
  auto [ok0, v0] = variantGet(variants->valueAt(0), "$.c.d");
  EXPECT_TRUE(ok0);
  EXPECT_EQ(v0, "3.14");
}

TEST_F(ParquetReaderTest, readVariantParquetPrimitivesSpark) {
  const std::string sample(getVariantFixturePath("variant_primitives.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 12ULL);

  auto rowType = ROW({"id", "v"}, {INTEGER(), VARIANT()});
  auto rowReaderOpts = getReaderOpts(rowType);
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto result = BaseVector::create(rowType, 12, leafPool_.get());
  auto rowsRead = rowReader->next(100, result);
  EXPECT_EQ(rowsRead, 12);

  auto row = result->as<RowVector>();
  auto ids = row->childAt(0)->asFlatVector<int32_t>();
  auto variants = row->childAt(1)->as<VariantVector>();

  auto decodeValue = [&](const VariantValue& value) -> std::string {
    if (value.value.empty()) {
      return "null";
    }
    auto decoded = bytedance::bolt::functions::sparksql::variant::
        SparkVariantReader::decode(value.value, value.metadata);
    return decoded.value_or("null");
  };

  auto variantGet = [&](const VariantValue& value, const StringView& path) {
    auto out = makeFlatVector<StringView>(1);
    exec::StringWriter<> writer(out.get(), 0);
    bytedance::bolt::functions::sparksql::VariantGetFunction<exec::VectorExec>
        func;
    auto ok = func.call(writer, value, path);
    if (ok) {
      writer.finalize();
      return std::make_pair(ok, std::string(out->valueAt(0)));
    }
    return std::make_pair(ok, std::string());
  };

  simdjson::dom::parser parser;
  for (int i = 0; i < 12; ++i) {
    EXPECT_EQ(ids->valueAt(i), i + 1);
    auto decoded = decodeValue(variants->valueAt(i));
    if (decoded != "null") {
      simdjson::dom::element doc;
      EXPECT_EQ(parser.parse(decoded).get(doc), simdjson::SUCCESS);
    }
  }

  // Spot-check path extraction across different shapes.
  auto [okObj, vObj] = variantGet(variants->valueAt(0), "$.c.d");
  EXPECT_TRUE(okObj);
  EXPECT_EQ(vObj, "3.14");

  auto [okArr, vArr] = variantGet(variants->valueAt(1), "$[4].k");
  EXPECT_TRUE(okArr);
  EXPECT_EQ(vArr, "v");

  auto [okNum, vNum] = variantGet(variants->valueAt(6), "$");
  EXPECT_TRUE(okNum);
  EXPECT_EQ(vNum, "2147483648");

  // Validate that Spark metadata dictionary is parseable for non-trivial keys.
  auto dictForComplexKeys = bytedance::bolt::functions::sparksql::variant::
      SparkVariantReader::parseDictionary(variants->valueAt(8).metadata);
  EXPECT_FALSE(dictForComplexKeys.empty());
  bool hasSpaceKey = false;
  bool hasQuoteKey = false;
  for (const auto& k : dictForComplexKeys) {
    if (k == "space_key") {
      hasSpaceKey = true;
    }
    if (k == "quote_key") {
      hasQuoteKey = true;
    }
  }
  EXPECT_TRUE(hasSpaceKey);
  EXPECT_TRUE(hasQuoteKey);

  // Ensure the Spark payload can be decoded.
  auto decodedComplex = decodeValue(variants->valueAt(8));
  EXPECT_NE(decodedComplex, "null");

  auto [okSpace, vSpace] = variantGet(variants->valueAt(8), "$.space_key");
  EXPECT_TRUE(okSpace);
  EXPECT_EQ(vSpace, "ok");

  auto [okQuote, vQuote] = variantGet(variants->valueAt(8), "$.quote_key");
  EXPECT_TRUE(okQuote);
  EXPECT_EQ(vQuote, "2");

  auto [okNull, vNull] = variantGet(variants->valueAt(11), "$");
  // JSON null is now correctly returned instead of being confused with
  // a decode failure (fix #5/N6).
  EXPECT_TRUE(okNull);
  EXPECT_EQ(vNull, "null");
}

TEST_F(ParquetReaderTest, readVariantParquetNestedSpark) {
  const std::string sample(getVariantFixturePath("variant_nested.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 2ULL);

  auto stType = ROW({"v1", "v2"}, {VARIANT(), VARIANT()});
  auto rowType =
      ROW({"id", "arr", "m", "st"},
          {INTEGER(), ARRAY(VARIANT()), MAP(VARCHAR(), VARIANT()), stType});
  auto rowReaderOpts = getReaderOpts(rowType);
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto result = BaseVector::create(rowType, 2, leafPool_.get());
  auto rowsRead = rowReader->next(10, result);
  EXPECT_EQ(rowsRead, 2);

  auto row = result->as<RowVector>();
  auto arr = row->childAt(1)->as<ArrayVector>();
  auto map = row->childAt(2)->as<MapVector>();
  auto st = row->childAt(3)->as<RowVector>();

  auto variantGet = [&](const VariantValue& value, const StringView& path) {
    auto out = makeFlatVector<StringView>(1);
    exec::StringWriter<> writer(out.get(), 0);
    bytedance::bolt::functions::sparksql::VariantGetFunction<exec::VectorExec>
        func;
    auto ok = func.call(writer, value, path);
    if (ok) {
      writer.finalize();
      return std::make_pair(ok, std::string(out->valueAt(0)));
    }
    return std::make_pair(ok, std::string());
  };

  // ARRAY<VARIANT>
  EXPECT_EQ(arr->sizeAt(0), 3);
  auto arrElements = arr->elements()->as<VariantVector>();
  auto offset0 = arr->offsetAt(0);
  auto [okY, vY] = variantGet(arrElements->valueAt(offset0), "$.y[1]");
  EXPECT_TRUE(okY);
  EXPECT_EQ(vY, "2");

  // MAP<VARCHAR, VARIANT>
  EXPECT_EQ(map->sizeAt(0), 2);
  auto keyVec = map->mapKeys()->as<SimpleVector<StringView>>();
  auto valVec = map->mapValues()->as<VariantVector>();
  auto mapOffset0 = map->offsetAt(0);
  std::string gotK2;
  for (int i = 0; i < 2; ++i) {
    if (keyVec->valueAt(mapOffset0 + i) == StringView("k2")) {
      auto [ok, v] = variantGet(valVec->valueAt(mapOffset0 + i), "$");
      EXPECT_TRUE(ok);
      gotK2 = v;
    }
  }
  EXPECT_EQ(gotK2, "s");

  // STRUCT<VARIANT, VARIANT>
  auto v1 = st->childAt(0)->as<VariantVector>()->valueAt(0);
  auto v2 = st->childAt(1)->as<VariantVector>()->valueAt(0);
  auto [okA, a] = variantGet(v1, "$.a");
  EXPECT_TRUE(okA);
  EXPECT_EQ(a, "1");
  auto [okEmptyArr, emptyArr] = variantGet(v2, "$");
  EXPECT_TRUE(okEmptyArr);
  EXPECT_EQ(emptyArr, "[]");
}

TEST_F(ParquetReaderTest, readVariantParquetRawJsonStruct) {
  const std::string sample(getVariantFixturePath("variant_rawjson.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 4ULL);

  auto rowType = ROW({"id", "v"}, {INTEGER(), VARIANT()});
  auto rowReaderOpts = getReaderOpts(rowType);
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto result = BaseVector::create(rowType, 4, leafPool_.get());
  auto rowsRead = rowReader->next(10, result);
  EXPECT_EQ(rowsRead, 4);

  auto row = result->as<RowVector>();
  auto variants = row->childAt(1)->as<VariantVector>();

  // Raw JSON payloads are expected to have empty metadata.
  EXPECT_TRUE(variants->valueAt(0).metadata.empty());
  EXPECT_TRUE(variants->valueAt(1).metadata.empty());
  EXPECT_TRUE(variants->valueAt(2).metadata.empty());
  EXPECT_TRUE(variants->valueAt(3).metadata.empty());

  auto variantGet = [&](const VariantValue& value, const StringView& path) {
    auto out = makeFlatVector<StringView>(1);
    exec::StringWriter<> writer(out.get(), 0);
    bytedance::bolt::functions::sparksql::VariantGetFunction<exec::VectorExec>
        func;
    auto ok = func.call(writer, value, path);
    if (ok) {
      writer.finalize();
      return std::make_pair(ok, std::string(out->valueAt(0)));
    }
    return std::make_pair(ok, std::string());
  };

  auto [okA, a] = variantGet(variants->valueAt(0), "$.a");
  EXPECT_TRUE(okA);
  EXPECT_EQ(a, "1");

  auto [okStr, s] = variantGet(variants->valueAt(2), "$");
  EXPECT_TRUE(okStr);
  EXPECT_EQ(s, "hello");

  auto [okNull, vNull] = variantGet(variants->valueAt(3), "$");
  EXPECT_FALSE(okNull);
  EXPECT_TRUE(vNull.empty());
}

TEST_F(ParquetReaderTest, readVariantParquetRawParts) {
  const std::string sample(getVariantFixturePath("variant_sample.parquet"));

  bytedance::bolt::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  auto rowType = ROW({"id", "v"}, {INTEGER(), VARIANT()});
  auto rowReaderOpts = getReaderOpts(rowType);
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto result = BaseVector::create(rowType, 4, leafPool_.get());
  auto rowsRead = rowReader->next(10, result);
  EXPECT_EQ(rowsRead, 4);
  EXPECT_EQ(result->size(), 4);

  auto row = result->as<RowVector>();
  auto variants = row->childAt(1)->as<VariantVector>();

  for (int i = 0; i < 4; ++i) {
    if (variants->isNullAt(i)) {
      continue;
    }
    auto value = variants->valueAt(i);
    EXPECT_GT(value.value.size(), 0);
    if (!value.metadata.empty()) {
      auto decoded = bytedance::bolt::functions::sparksql::variant::
          SparkVariantReader::decode(value.value, value.metadata);
      EXPECT_TRUE(decoded.has_value());
      EXPECT_FALSE(decoded->empty());
    }
  }
}

TEST_F(ParquetReaderTest, dcMapNested) {
  const std::string sample(getExampleFilePath("dcmapNested.parquet"));
  auto rowType =
      ROW({"name", "age", "contact"},
          {VARCHAR(),
           BIGINT(),
           ROW({"city", "phone"},
               {VARCHAR(),
                ROW({"key_value", "dynamic_column"},
                    {MAP(VARCHAR(), VARCHAR()),
                     ROW({"value_0",
                          "value_1",
                          "value_2",
                          "value_3",
                          "value_4",
                          "value_5"},
                         {VARCHAR(),
                          VARCHAR(),
                          VARCHAR(),
                          VARCHAR(),
                          VARCHAR(),
                          VARCHAR()})})})});

  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({
      makeFlatVector<StringView>({"xeonliu", "wukong"}),
      makeFlatVector<int64_t>({18, 500}),
      makeRowVector(
          {makeFlatVector<StringView>({"Shanghai", "Beijing"}),
           makeMapVector<StringView, StringView>(
               {{{"tangchenyipin", "021-88880001"},
                 {"zhongliangyihao", "021-88880002"},
                 {"hepingfandian", "021-88880003"}},
                {{"erhuan", "010-66660001"},
                 {"sanhuan", "010-66660002"},
                 {"sihuan", "010-66660003"}}})}),
  });

  assertReadWithReaderAndExpected(rowType, *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, dcMapContainsMap) {
  // dcmapContainsMap.parquet stores values of "tencent" and "toutiao"
  // in dynamic columns and other kv pairs in MAP part of DCMAP.
  const std::string sample(getExampleFilePath("dcmapContainsMap.parquet"));
  // scanSpec we get from HMS will be map(varchar, varchar)
  // so DCMap reader should be able to handle mismatched types.
  auto rowType =
      ROW({"name", "age", "accounts"},
          {VARCHAR(), BIGINT(), MAP(VARCHAR(), VARCHAR())});
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({
      makeFlatVector<StringView>({"xeon_liu", "wukong"}),
      makeFlatVector<int64_t>({18, 500}),
      makeMapVector<StringView, StringView>({
          {{"douyin", "2012-04-05"},
           {"baidu", "2010-01-01"},
           {"tencent", "2011-02-03"}},
          {{"toutiao", "2013-02-04"}, {"baidu", "2015-01-01"}},
      }),
  });

  assertReadWithReaderAndExpected(rowType, *rowReader, expected, *leafPool_);
}
