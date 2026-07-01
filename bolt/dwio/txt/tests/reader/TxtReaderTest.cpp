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

#include <type/HugeInt.h>
#include <type/Type.h>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/dwio/txt/tests/TxtTestBase.h"
#include "bolt/exec/tests/utils/ConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::txt;
using namespace bytedance::bolt::txt::reader;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

class TxtReaderTest : public bytedance::bolt::txt::TxtTestBase {
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

TEST_F(TxtReaderTest, txtSimple) {
  const std::string sample(getExampleFilePath("simple.txt"));
  auto rowType =
      ROW({"int", "double", "var"}, {INTEGER(), DOUBLE(), VARCHAR()});
  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  SerDeOptions serDe(uint8_t(','));
  readerOpts.setSerDeOptions(serDe);
  readerOpts.setFileSchema(rowType);
  auto reader = createReader(sample, readerOpts);

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeFlatVector<double>({7.2, 0.7}),
      makeFlatVector<StringView>({"abc", "def"}),
  });

  assertReadWithReaderAndExpected(rowType, *rowReader, expected, *leafPool_);
}

TEST_F(TxtReaderTest, txtArray) {
  const std::string sample(getExampleFilePath("array.txt"));
  auto rowType = ROW(
      {
          "int",
          "arrayBool",
          "arrayInt8",
          "arrayInt16",
          "arrayInt32",
          "arrayInt64",
          "arrayReal",
          "arrayDouble",
          // decimal arrow types are omitted for now
          //   "arrayShortDecimal",
          //   "arrayLongDecimal",
          "arrayVarChar",
          "arrayTimestamp",
      },
      {
          INTEGER(),
          ARRAY(BOOLEAN()),
          ARRAY(TINYINT()),
          ARRAY(SMALLINT()),
          ARRAY(INTEGER()),
          ARRAY(BIGINT()),
          ARRAY(REAL()),
          ARRAY(DOUBLE()),
          //   ARRAY(DECIMAL(10, 2)),
          //   ARRAY(DECIMAL(38, 10)),
          ARRAY(VARCHAR()),
          ARRAY(TIMESTAMP()),
      });
  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  SerDeOptions serDe(uint8_t(','), uint8_t(';'));
  readerOpts.setSerDeOptions(serDe);
  readerOpts.setFileSchema(rowType);
  auto reader = createReader(sample, readerOpts);

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  std::unique_ptr<bytedance::bolt::test::VectorMaker> vectorMaker_{
      std::make_unique<bytedance::bolt::test::VectorMaker>(pool_.get())};

  auto intVector = vectorMaker_->flatVector<int32_t>({1, 5, 7});
  auto arrayBooleanVector = makeArrayVector<bool>(
      {{true, false, true}, {false}, {true, true, false, false}}, BOOLEAN());
  auto arrayInt8Vector =
      makeArrayVector<int8_t>({{2, 3, 4}, {6}, {8, 9, 10, 11}}, TINYINT());
  auto arrayInt16Vector =
      makeArrayVector<int16_t>({{2, 3, 4}, {6}, {8, 9, 10, 11}}, SMALLINT());
  auto arrayInt32Vector =
      makeArrayVector<int32_t>({{2, 3, 4}, {6}, {8, 9, 10, 11}}, INTEGER());
  auto arrayInt64Vector =
      makeArrayVector<int64_t>({{2, 3, 4}, {6}, {8, 9, 10, 11}}, BIGINT());
  auto arrayRealVector =
      makeArrayVector<float>({{3.4, 4.5}, {4, 5.6, 0.2}, {6.3}}, REAL());
  auto arrayDoubleVector =
      makeArrayVector<double>({{3.4, 4.5}, {4, 5.6, 0.2}, {6.3}}, DOUBLE());
  //   auto arrayShortDecimalVector = makeArrayVector<int64_t>(
  //       {{1111, 2222}, {3333, 4444, 5555}, {6666}}, DECIMAL(10, 2));
  //   auto arrayLongDecimalVector = makeArrayVector<int128_t>(
  //       {{922337203685471, 922337203685472},
  //        {922337203685473, 922337203685474, 922337203685475},
  //        {922337203685476}},
  //       DECIMAL(38, 10));
  auto arrayVarCharVector = makeArrayVector<StringView>(
      {{"Short", "LoooongString"}, {"c"}, {"Man", "y", "str", "ing"}},
      VARCHAR());
  auto arrayTimestampVector = makeArrayVector<Timestamp>(
      {{bytedance::bolt::util::fromTimestampString("2025-01-09", nullptr)},
       {bytedance::bolt::util::fromTimestampString("2025-01-10", nullptr)},
       {bytedance::bolt::util::fromTimestampString("2025-01-11", nullptr)}});

  auto expected = makeRowVector(
      {intVector,
       arrayBooleanVector,
       arrayInt8Vector,
       arrayInt16Vector,
       arrayInt32Vector,
       arrayInt64Vector,
       arrayRealVector,
       arrayDoubleVector,
       //   arrayShortDecimalVector,
       //   arrayLongDecimalVector,
       arrayVarCharVector,
       arrayTimestampVector});

  assertReadWithReaderAndExpected(rowType, *rowReader, expected, *leafPool_);
}

TEST_F(TxtReaderTest, txtMap) {
  const std::string sample(getExampleFilePath("map.txt"));
  auto rowType =
      ROW({"mapInt32Double",
           "mapDoubleInT64",
           "mapInt8Int16",
           "mapBoolFloat",
           // decimals are omitted
           "mapStringTimeStamp"},
          {
              MAP(INTEGER(), DOUBLE()),
              MAP(DOUBLE(), BIGINT()),
              MAP(TINYINT(), SMALLINT()),
              MAP(BOOLEAN(), REAL()),
              // decimals are omitted
              MAP(VARCHAR(), TIMESTAMP()),
          });
  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  SerDeOptions serDe(uint8_t(','), uint8_t(';'), uint8_t(':'));
  readerOpts.setSerDeOptions(serDe);
  readerOpts.setFileSchema(rowType);
  auto reader = createReader(sample, readerOpts);

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  std::unique_ptr<bytedance::bolt::test::VectorMaker> vectorMaker_{
      std::make_unique<bytedance::bolt::test::VectorMaker>(pool_.get())};

  auto mapIntDouble = makeMapVector<int32_t, double>({{{1, 2.0}}});
  auto mapDoubleInt64 = makeMapVector<double, int64_t>({{{3.0, 4}}});
  auto mapInt8Int16 = makeMapVector<int8_t, int16_t>({{{5, 6}}});
  auto mapBoolFloat = makeMapVector<bool, float>({{{true, 7.0}}});
  auto mapStringTimeStamp = makeMapVector<StringView, Timestamp>(
      {{{"Hello",
         bytedance::bolt::util::fromTimestampString("2025-01-15", nullptr)}}});

  auto expected = makeRowVector(
      {mapIntDouble,
       mapDoubleInt64,
       mapInt8Int16,
       mapBoolFloat,
       mapStringTimeStamp});

  assertReadWithReaderAndExpected(rowType, *rowReader, expected, *leafPool_);
}

TEST_F(TxtReaderTest, txtPartitionKey) {
  const std::string sample(getExampleFilePath("simple.txt"));
  auto rowType =
      ROW({"int", "double", "var"}, {INTEGER(), DOUBLE(), VARCHAR()});
  bytedance::bolt::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  SerDeOptions serDe(uint8_t(','));
  readerOpts.setSerDeOptions(serDe);
  readerOpts.setFileSchema(rowType);
  auto reader = createReader(sample, readerOpts);

  RowReaderOptions rowReaderOpts;
  auto partitionKey = ROW({"part"}, {INTEGER()});
  auto scanSpec = makeScanSpec(partitionKey);
  scanSpec->children()[0]->setConstantValue(
      BaseVector::createConstant(INTEGER(), 1, 1, leafPool_.get()));
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({makeFlatVector<int32_t>({1, 1})});

  assertReadWithReaderAndExpected(rowType, *rowReader, expected, *leafPool_);
}
