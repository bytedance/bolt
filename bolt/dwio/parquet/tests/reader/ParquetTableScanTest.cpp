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

#include <folly/init/Init.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/type/tests/SubfieldFiltersBuilder.h"

#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/dwio/parquet/writer/Writer.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::parquet;

class ParquetTableScanTest : public HiveConnectorTestBase {
 protected:
  using OperatorTestBase::assertQuery;

  void SetUp() {
    registerParquetReaderFactory();

    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);
  }

  void assertSelect(
      std::vector<std::string>&& outputColumnNames,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));

    auto plan = PlanBuilder().tableScan(rowType).planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithDataColumns(
      std::vector<std::string>&& outputColumnNames,
      const RowTypePtr& dataColumns,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));
    auto plan =
        PlanBuilder().tableScan(rowType, {}, "", dataColumns).planNode();
    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithFilter(
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& subfieldFilters,
      const std::string& remainingFilter,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));
    parse::ParseOptions options;
    options.parseDecimalAsDouble = false;

    auto plan = PlanBuilder(pool_.get())
                    .setParseOptions(options)
                    .tableScan(rowType, subfieldFilters, remainingFilter)
                    .planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithFilter(
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& subfieldFilters,
      const std::string& remainingFilter,
      const std::string& sql,
      bool isFilterPushdownEnabled) {
    auto rowType = getRowType(std::move(outputColumnNames));
    parse::ParseOptions options;
    options.parseDecimalAsDouble = false;

    auto plan = PlanBuilder(pool_.get())
                    .setParseOptions(options)
                    // Function extractFiltersFromRemainingFilter will extract
                    // filters to subfield filters, but for some types, filter
                    // pushdown is not supported.
                    .tableScan(
                        "hive_table",
                        rowType,
                        {},
                        subfieldFilters,
                        remainingFilter,
                        nullptr,
                        isFilterPushdownEnabled)
                    .planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithAgg(
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& groupingKeys,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));

    auto plan = PlanBuilder()
                    .tableScan(rowType)
                    .singleAggregation(groupingKeys, aggregates)
                    .planNode();

    assertQuery(plan, splits_, sql);
  }

  void assertSelectWithFilterAndAgg(
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& filters,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& groupingKeys,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));

    auto plan = PlanBuilder()
                    .tableScan(rowType, filters)
                    .singleAggregation(groupingKeys, aggregates)
                    .planNode();

    assertQuery(plan, splits_, sql);
  }

  void
  loadData(const std::string& filePath, RowTypePtr rowType, RowVectorPtr data) {
    splits_ = {makeSplit(filePath)};
    rowType_ = rowType;
    createDuckDbTable({data});
  }

  void loadDataWithRowType(const std::string& filePath, RowVectorPtr data) {
    splits_ = {makeSplit(filePath)};
    auto pool = bytedance::bolt::memory::memoryManager()->addLeafPool();
    dwio::common::ReaderOptions readerOpts{pool.get()};
    auto reader = std::make_unique<ParquetReader>(
        std::make_unique<bytedance::bolt::dwio::common::BufferedInput>(
            std::make_shared<LocalReadFile>(filePath),
            readerOpts.getMemoryPool()),
        readerOpts);
    rowType_ = reader->rowType();
    createDuckDbTable({data});
  }

  std::string getExampleFilePath(const std::string& fileName) {
    return bytedance::bolt::test::getDataFilePath("../examples/" + fileName);
  }

  std::shared_ptr<connector::hive::HiveConnectorSplit> makeSplit(
      const std::string& filePath) {
    return makeHiveConnectorSplits(
        filePath, 1, dwio::common::FileFormat::PARQUET)[0];
  }

  const std::vector<std::shared_ptr<connector::ConnectorSplit>>& splits()
      const {
    return splits_;
  }

  // Write data to a parquet file on specified path.
  // @param writeInt96AsTimestamp Write timestamp as Int96 if enabled.
  void writeToParquetFile(
      const std::string& path,
      const std::vector<RowVectorPtr>& data,
      WriterOptions options) {
    BOLT_CHECK_GT(data.size(), 0);

    auto writeFile = std::make_unique<LocalWriteFile>(path, true, false);
    auto sink = std::make_unique<dwio::common::WriteFileSink>(
        std::move(writeFile), path);
    auto childPool =
        rootPool_->addAggregateChild("ParquetTableScanTest.Writer");
    options.memoryPool = childPool.get();

    auto writer = std::make_unique<Writer>(
        std::move(sink), options, asRowType(data[0]->type()));

    for (const auto& vector : data) {
      writer->write(vector);
    }
    writer->close();
  }

  void testTimestampRead(const WriterOptions& options) {
    auto stringToTimestamp = [](std::string_view view) {
      return util::fromTimestampString(view.data(), view.size(), nullptr);
    };
    std::vector<std::string_view> views = {
        "2015-06-01 19:34:56.007",
        "2015-06-02 19:34:56.123",
        "2001-02-03 03:34:06.056",
        "1998-03-01 08:01:06.996",
        "2022-12-23 03:56:01",
        "1980-01-24 00:23:07",
        "1999-12-08 13:39:26.123",
        "2023-04-21 09:09:34.5",
        "2000-09-12 22:36:29",
        "2007-12-12 04:27:56.999",
    };
    std::vector<Timestamp> values;
    values.reserve(views.size());
    for (auto view : views) {
      values.emplace_back(stringToTimestamp(view));
    }

    auto vector = makeRowVector(
        {"t"},
        {
            makeFlatVector<Timestamp>(values),
        });
    auto schema = asRowType(vector->type());
    auto file = TempFilePath::create();
    writeToParquetFile(file->getPath(), {vector}, options);
    loadData(file->getPath(), schema, vector);

    assertSelectWithFilter({"t"}, {}, "", "SELECT t from tmp", false);
    assertSelectWithFilter(
        {"t"},
        {},
        "t < TIMESTAMP '2000-09-12 22:36:29'",
        "SELECT t from tmp where t < TIMESTAMP '2000-09-12 22:36:29'",
        false);
    assertSelectWithFilter(
        {"t"},
        {},
        "t <= TIMESTAMP '2000-09-12 22:36:29'",
        "SELECT t from tmp where t <= TIMESTAMP '2000-09-12 22:36:29'",
        false);
    assertSelectWithFilter(
        {"t"},
        {},
        "t > TIMESTAMP '1980-01-24 00:23:07'",
        "SELECT t from tmp where t > TIMESTAMP '1980-01-24 00:23:07'",
        false);
    assertSelectWithFilter(
        {"t"},
        {},
        "t >= TIMESTAMP '1980-01-24 00:23:07'",
        "SELECT t from tmp where t >= TIMESTAMP '1980-01-24 00:23:07'",
        false);
    assertSelectWithFilter(
        {"t"},
        {},
        "t == TIMESTAMP '2022-12-23 03:56:01'",
        "SELECT t from tmp where t == TIMESTAMP '2022-12-23 03:56:01'",
        false);
  }

 private:
  RowTypePtr getRowType(std::vector<std::string>&& outputColumnNames) const {
    std::vector<TypePtr> types;
    for (auto colName : outputColumnNames) {
      types.push_back(rowType_->findChild(colName));
    }

    return ROW(std::move(outputColumnNames), std::move(types));
  }

  RowTypePtr rowType_;
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits_;
};

TEST_F(ParquetTableScanTest, basic) {
  loadData(
      getExampleFilePath("sample.parquet"),
      ROW({"a", "b"}, {BIGINT(), DOUBLE()}),
      makeRowVector(
          {"a", "b"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
          }));

  // Plain select.
  assertSelect({"a"}, "SELECT a FROM tmp");
  assertSelect({"b"}, "SELECT b FROM tmp");
  assertSelect({"a", "b"}, "SELECT a, b FROM tmp");
  assertSelect({"b", "a"}, "SELECT b, a FROM tmp");

  // With filters.
  assertSelectWithFilter({"a"}, {"a < 3"}, "", "SELECT a FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {"a", "b"}, {"a < 3"}, "", "SELECT a, b FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {"b", "a"}, {"a < 3"}, "", "SELECT b, a FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {"a", "b"}, {"a < 0"}, "", "SELECT a, b FROM tmp WHERE a < 0");

  assertSelectWithFilter(
      {"b"}, {"b < DOUBLE '2.0'"}, "", "SELECT b FROM tmp WHERE b < 2.0");
  assertSelectWithFilter(
      {"a", "b"},
      {"b >= DOUBLE '2.0'"},
      "",
      "SELECT a, b FROM tmp WHERE b >= 2.0");
  assertSelectWithFilter(
      {"b", "a"},
      {"b <= DOUBLE '2.0'"},
      "",
      "SELECT b, a FROM tmp WHERE b <= 2.0");
  assertSelectWithFilter(
      {"a", "b"},
      {"b < DOUBLE '0.0'"},
      "",
      "SELECT a, b FROM tmp WHERE b < 0.0");

  // With aggregations.
  assertSelectWithAgg({"a"}, {"sum(a)"}, {}, "SELECT sum(a) FROM tmp");
  assertSelectWithAgg({"b"}, {"max(b)"}, {}, "SELECT max(b) FROM tmp");
  assertSelectWithAgg(
      {"a", "b"}, {"min(a)", "max(b)"}, {}, "SELECT min(a), max(b) FROM tmp");
  assertSelectWithAgg(
      {"b", "a"}, {"max(b)"}, {"a"}, "SELECT max(b), a FROM tmp GROUP BY a");
  assertSelectWithAgg(
      {"a", "b"}, {"max(a)"}, {"b"}, "SELECT max(a), b FROM tmp GROUP BY b");

  // With filter and aggregation.
  assertSelectWithFilterAndAgg(
      {"a"}, {"a < 3"}, {"sum(a)"}, {}, "SELECT sum(a) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {"a", "b"},
      {"a < 3"},
      {"sum(b)"},
      {},
      "SELECT sum(b) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {"a", "b"},
      {"a < 3"},
      {"min(a)", "max(b)"},
      {},
      "SELECT min(a), max(b) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {"b", "a"},
      {"a < 3"},
      {"max(b)"},
      {"a"},
      "SELECT max(b), a FROM tmp WHERE a < 3 GROUP BY a");
}

TEST_F(ParquetTableScanTest, countStar) {
  // sample.parquet holds two columns (a: BIGINT, b: DOUBLE) and
  // 20 rows.
  auto filePath = getExampleFilePath("sample.parquet");
  auto split = makeSplit(filePath);

  // Output type does not have any columns.
  auto rowType = ROW({}, {});
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .singleAggregation({}, {"count(0)"})
                  .planNode();

  assertQuery(plan, {split}, "SELECT 20");
}

TEST_F(ParquetTableScanTest, decimalSubfieldFilter) {
  // decimal.parquet holds two columns (a: DECIMAL(5, 2), b: DECIMAL(20, 5)) and
  // 20 rows (10 rows per group). Data is in plain uncompressed format:
  //   a: [100.01 .. 100.20]
  //   b: [100000000000000.00001 .. 100000000000000.00020]
  std::vector<int64_t> unscaledShortValues(20);
  std::iota(unscaledShortValues.begin(), unscaledShortValues.end(), 10001);
  loadData(
      getExampleFilePath("decimal.parquet"),
      ROW({"a"}, {DECIMAL(5, 2)}),
      makeRowVector(
          {"a"},
          {
              makeFlatVector(unscaledShortValues, DECIMAL(5, 2)),
          }));

  assertSelectWithFilter(
      {"a"}, {"a < 100.07"}, "", "SELECT a FROM tmp WHERE a < 100.07");
  assertSelectWithFilter(
      {"a"}, {"a <= 100.07"}, "", "SELECT a FROM tmp WHERE a <= 100.07");
  assertSelectWithFilter(
      {"a"}, {"a > 100.07"}, "", "SELECT a FROM tmp WHERE a > 100.07");
  assertSelectWithFilter(
      {"a"}, {"a >= 100.07"}, "", "SELECT a FROM tmp WHERE a >= 100.07");
  assertSelectWithFilter(
      {"a"}, {"a = 100.07"}, "", "SELECT a FROM tmp WHERE a = 100.07");
  assertSelectWithFilter(
      {"a"},
      {"a BETWEEN 100.07 AND 100.12"},
      "",
      "SELECT a FROM tmp WHERE a BETWEEN 100.07 AND 100.12");

  BOLT_ASSERT_THROW(
      assertSelectWithFilter(
          {"a"}, {"a < 1000.7"}, "", "SELECT a FROM tmp WHERE a < 1000.7"),
      "Scalar function signature is not supported: lt(DECIMAL(5, 2), DECIMAL(5, 1))");
  BOLT_ASSERT_THROW(
      assertSelectWithFilter(
          {"a"}, {"a = 1000.7"}, "", "SELECT a FROM tmp WHERE a = 1000.7"),
      "Scalar function signature is not supported: eq(DECIMAL(5, 2), DECIMAL(5, 1))");
}

// Core dump is fixed.
TEST_F(ParquetTableScanTest, map) {
  auto vector = makeMapVector<StringView, StringView>({{{"name", "gluten"}}});

  loadData(
      getExampleFilePath("types.parquet"),
      ROW({"map"}, {MAP(VARCHAR(), VARCHAR())}),
      makeRowVector(
          {"map"},
          {
              vector,
          }));

  assertSelectWithFilter({"map"}, {}, "", "SELECT map FROM tmp");
}

TEST_F(ParquetTableScanTest, nullMap) {
  auto path = getExampleFilePath("null_map.parquet");
  loadData(
      path,
      ROW({"i", "c"}, {VARCHAR(), MAP(VARCHAR(), VARCHAR())}),
      makeRowVector(
          {"i", "c"},
          {makeConstant<std::string>("1", 1),
           makeNullableMapVector<std::string, std::string>({std::nullopt})}));

  assertSelectWithFilter({"i", "c"}, {}, "", "SELECT i, c FROM tmp");
}

// Core dump is fixed.
TEST_F(ParquetTableScanTest, singleRowStruct) {
  auto vector = makeArrayVector<int32_t>({{}});
  loadData(
      getExampleFilePath("single_row_struct.parquet"),
      ROW({"s"}, {ROW({"a", "b"}, {BIGINT(), BIGINT()})}),
      makeRowVector(
          {"s"},
          {
              vector,
          }));

  assertSelectWithFilter({"s"}, {}, "", "SELECT (0, 1)");
}

// Core dump and incorrect result are fixed.
TEST_F(ParquetTableScanTest, DISABLED_array) {
  auto vector = makeArrayVector<int32_t>({});
  loadData(
      getExampleFilePath("old_repeated_int.parquet"),
      ROW({"repeatedInt"}, {ARRAY(INTEGER())}),
      makeRowVector(
          {"repeatedInt"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"repeatedInt"}, {}, "", "SELECT UNNEST(array[array[1,2,3]])");
}

// Optional array with required elements.
TEST_F(ParquetTableScanTest, optArrayReqEle) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      getExampleFilePath("array_0.parquet"),
      ROW({"_1"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[array['a', 'b'], array['c', 'd'], array['e', 'f'], array[], null])");
}

// Required array with required elements.
TEST_F(ParquetTableScanTest, reqArrayReqEle) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      getExampleFilePath("array_1.parquet"),
      ROW({"_1"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[array['a', 'b'], array['c', 'd'], array[]])");
}

// Required array with optional elements.
TEST_F(ParquetTableScanTest, reqArrayOptEle) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      getExampleFilePath("array_2.parquet"),
      ROW({"_1"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[array['a', null], array[], array[null, 'b']])");
}

TEST_F(ParquetTableScanTest, arrayOfArrayTest) {
  auto vector = makeArrayVector<StringView>({});

  loadDataWithRowType(
      getExampleFilePath("array_of_array1.parquet"),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[null, array[array['g', 'h'], null]])");
}

// Required array with legacy format.
TEST_F(ParquetTableScanTest, reqArrayLegacy) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      getExampleFilePath("array_3.parquet"),
      ROW({"element"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"element"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {"element"},
      {},
      "",
      "SELECT UNNEST(array[array['a', 'b'], array[], array['c', 'd']])");
}

TEST_F(ParquetTableScanTest, readAsLowerCase) {
  auto plan = PlanBuilder(pool_.get())
                  .tableScan(ROW({"a"}, {BIGINT()}), {}, "")
                  .planNode();
  CursorParameters params;
  std::shared_ptr<folly::Executor> executor =
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency());
  std::shared_ptr<core::QueryCtx> queryCtx =
      core::QueryCtx::create(executor.get());
  std::unordered_map<std::string, std::string> session = {
      {std::string(
           connector::hive::HiveConfig::kFileColumnNamesReadAsLowerCaseSession),
       "true"}};
  queryCtx->setConnectorSessionOverridesUnsafe(
      kHiveConnectorId, std::move(session));
  params.queryCtx = queryCtx;
  params.planNode = plan;
  const int numSplitsPerFile = 1;

  bool noMoreSplits = false;
  auto addSplits = [&](exec::Task* task) {
    if (!noMoreSplits) {
      auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
          {getExampleFilePath("upper.parquet")},
          numSplitsPerFile,
          dwio::common::FileFormat::PARQUET);
      for (const auto& split : splits) {
        task->addSplit("0", exec::Split(split));
      }
      task->noMoreSplits("0");
    }
    noMoreSplits = true;
  };
  auto result = readCursor(params, addSplits);
  ASSERT_TRUE(waitForTaskCompletion(result.first->task().get()));
  assertEqualResults(
      result.second, {makeRowVector({"a"}, {makeFlatVector<int64_t>({0, 1})})});
}

TEST_F(ParquetTableScanTest, DISABLED_structSelection) {
  auto vector = makeArrayVector<StringView>({{}});
  loadData(
      getExampleFilePath("contacts.parquet"),
      ROW({"name"}, {ROW({"first", "last"}, {VARCHAR(), VARCHAR()})}),
      makeRowVector(
          {"t"},
          {
              vector,
          }));
  assertSelectWithFilter({"name"}, {}, "", "SELECT ('Janet', 'Jones')");

  loadData(
      getExampleFilePath("contacts.parquet"),
      ROW({"name"},
          {ROW(
              {"first", "middle", "last"}, {VARCHAR(), VARCHAR(), VARCHAR()})}),
      makeRowVector(
          {"t"},
          {
              vector,
          }));
  assertSelectWithFilter({"name"}, {}, "", "SELECT ('Janet', null, 'Jones')");

  loadData(
      getExampleFilePath("contacts.parquet"),
      ROW({"name"}, {ROW({"first", "middle"}, {VARCHAR(), VARCHAR()})}),
      makeRowVector(
          {"t"},
          {
              vector,
          }));
  assertSelectWithFilter({"name"}, {}, "", "SELECT ('Janet', null)");

  loadData(
      getExampleFilePath("contacts.parquet"),
      ROW({"name"}, {ROW({"middle", "last"}, {VARCHAR(), VARCHAR()})}),
      makeRowVector(
          {"t"},
          {
              vector,
          }));
  assertSelectWithFilter({"name"}, {}, "", "SELECT (null, 'Jones')");

  loadData(
      getExampleFilePath("contacts.parquet"),
      ROW({"name"}, {ROW({"middle"}, {VARCHAR()})}),
      makeRowVector(
          {"t"},
          {
              vector,
          }));
  assertSelectWithFilter({"name"}, {}, "", "SELECT row(null)");

  loadData(
      getExampleFilePath("contacts.parquet"),
      ROW({"name"}, {ROW({"middle", "info"}, {VARCHAR(), VARCHAR()})}),
      makeRowVector(
          {"t"},
          {
              vector,
          }));
  assertSelectWithFilter({"name"}, {}, "", "SELECT NULL");

  loadData(
      getExampleFilePath("contacts.parquet"),
      ROW({"name"}, {ROW({}, {})}),
      makeRowVector(
          {"t"},
          {
              vector,
          }));

  assertSelectWithFilter({"name"}, {}, "", "SELECT t from tmp");
}

TEST_F(ParquetTableScanTest, timestampInt96Dictionary) {
  WriterOptions options;
  options.writeInt96AsTimestamp = true;
  options.enableDictionary = true;
  testTimestampRead(options);
}

TEST_F(ParquetTableScanTest, timestampInt96Plain) {
  WriterOptions options;
  options.writeInt96AsTimestamp = true;
  options.enableDictionary = false;
  testTimestampRead(options);
}

TEST_F(ParquetTableScanTest, timestampINT96) {
  auto a = makeFlatVector<Timestamp>({Timestamp(1, 0), Timestamp(2, 0)});
  auto expected = makeRowVector({"time"}, {a});
  createDuckDbTable("expected", {expected});

  auto vector = makeArrayVector<Timestamp>({{}});
  loadData(
      getExampleFilePath("timestamp_dict_int96.parquet"),
      ROW({"time"}, {TIMESTAMP()}),
      makeRowVector(
          {"time"},
          {
              vector,
          }));
  assertSelect({"time"}, "SELECT time from expected");

  loadData(
      getExampleFilePath("timestamp_plain_int96.parquet"),
      ROW({"time"}, {TIMESTAMP()}),
      makeRowVector(
          {"time"},
          {
              vector,
          }));
  assertSelect({"time"}, "SELECT time from expected");
}

TEST_F(ParquetTableScanTest, timestampInt64Dictionary) {
  WriterOptions options;
  options.writeInt96AsTimestamp = false;
  options.enableDictionary = true;
  options.parquetWriteTimestampUnit = TimestampUnit::kMicro;
  testTimestampRead(options);
}

TEST_F(ParquetTableScanTest, timestampInt64Plain) {
  WriterOptions options;
  options.writeInt96AsTimestamp = false;
  options.enableDictionary = false;
  options.parquetWriteTimestampUnit = TimestampUnit::kMicro;
  testTimestampRead(options);
}

TEST_F(ParquetTableScanTest, timestampConvertedType) {
  auto stringToTimestamp = [](std::string_view view) {
    return util::fromTimestampString(view.data(), view.size(), nullptr);
  };
  std::vector<std::string_view> expected = {
      "1970-01-01 00:00:00.010",
      "1970-01-01 00:00:00.010",
      "1970-01-01 00:00:00.010",
  };
  std::vector<Timestamp> values;
  values.reserve(expected.size());
  for (auto view : expected) {
    values.emplace_back(stringToTimestamp(view));
  }

  const auto vector = makeRowVector(
      {"time"},
      {
          makeFlatVector<Timestamp>(values),
      });
  const auto schema = asRowType(vector->type());
  const auto path = getExampleFilePath("tmmillis_i64.parquet");
  loadData(path, schema, vector);

  assertSelectWithFilter({"time"}, {}, "", "SELECT time from tmp");
}

TEST_F(ParquetTableScanTest, timestampPrecisionMicrosecond) {
  // Write timestamp data into parquet.
  constexpr int kSize = 10;
  auto vector = makeRowVector({
      makeFlatVector<Timestamp>(
          kSize, [](auto i) { return Timestamp(i, i * 1'001'001); }),
  });
  auto schema = asRowType(vector->type());
  auto file = TempFilePath::create();
  WriterOptions options;
  options.writeInt96AsTimestamp = true;
  writeToParquetFile(file->getPath(), {vector}, options);
  auto plan = PlanBuilder().tableScan(schema).planNode();

  // Read timestamp data from parquet with microsecond precision.
  CursorParameters params;
  std::shared_ptr<folly::Executor> executor =
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());
  std::unordered_map<std::string, std::string> session = {
      {std::string(connector::hive::HiveConfig::kReadTimestampUnitSession),
       "6"}};
  queryCtx->setConnectorSessionOverridesUnsafe(
      kHiveConnectorId, std::move(session));
  params.queryCtx = queryCtx;
  params.planNode = plan;
  const int numSplitsPerFile = 1;

  bool noMoreSplits = false;
  auto addSplits = [&](exec::Task* task) {
    if (!noMoreSplits) {
      auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
          {file->path}, numSplitsPerFile, dwio::common::FileFormat::PARQUET);
      for (const auto& split : splits) {
        task->addSplit("0", exec::Split(split));
      }
      task->noMoreSplits("0");
    }
    noMoreSplits = true;
  };
  auto result = readCursor(params, addSplits);
  ASSERT_TRUE(waitForTaskCompletion(result.first->task().get()));
  auto expected = makeRowVector({
      makeFlatVector<Timestamp>(
          kSize, [](auto i) { return Timestamp(i, i * 1'001'000); }),
  });
  assertEqualResults({expected}, result.second);
}

TEST_F(ParquetTableScanTest, structMatchByName) {
  const auto assertSelectUseColumnNames =
      [this](
          const RowTypePtr& outputType,
          const std::string& sql,
          const std::string& remainingFilter = "") {
        auto builder = PlanBuilder().tableScan(outputType, {}, remainingFilter);
        if (!remainingFilter.empty()) {
          builder.filter(remainingFilter);
        }
        const auto plan = builder.planNode();
        auto query = AssertQueryBuilder(plan, duckDbQueryRunner_);
        query.connectorSessionProperty(
            kHiveConnectorId,
            connector::hive::HiveConfig::kParquetUseColumnNamesSession,
            "true");
        query.splits(splits());
        if (remainingFilter.empty()) {
          query.assertResults(sql);
        } else {
          auto result = query.copyResults(pool());
          ASSERT_EQ(0, result->size());
        }
      };

  std::vector<int64_t> values = {2};
  const auto id = makeFlatVector<int64_t>(values);
  const auto name = makeRowVector(
      {"first", "last"},
      {
          makeFlatVector<std::string>({"Janet"}),
          makeFlatVector<std::string>({"Jones"}),
      });
  const auto address = makeFlatVector<std::string>({"567 Maple Drive"});
  auto vector = makeRowVector({"id", "name", "address"}, {id, name, address});

  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, {});

  loadData(file->getPath(), asRowType(vector->type()), vector);
  assertSelect({"id", "name", "address"}, "SELECT id, name, address from tmp");

  auto rowType =
      ROW({"id", "name", "email"},
          {BIGINT(),
           ROW({"first", "middle", "last"}, {VARCHAR(), VARCHAR(), VARCHAR()}),
           VARCHAR()});
  loadData(file->getPath(), rowType, vector);
  assertSelectUseColumnNames(
      rowType, "SELECT 2, ('Janet', null, 'Jones'), null");

  assertSelectUseColumnNames(
      rowType, "SELECT * from tmp where false", "not(is_null(name.middle))");

  rowType =
      ROW({"id", "name", "address"},
          {BIGINT(), ROW({"a", "b"}, {VARCHAR(), VARCHAR()}), VARCHAR()});
  loadData(file->getPath(), rowType, vector);
  assertSelectUseColumnNames(rowType, "SELECT 2, null, '567 Maple Drive'");

  assertSelectUseColumnNames(
      rowType, "SELECT * from tmp where false", "not(is_null(name))");

  rowType =
      ROW({"id", "name", "address"},
          {BIGINT(), ROW({"full"}, {VARCHAR()}), VARCHAR()});
  loadData(file->getPath(), rowType, vector);
  assertSelectUseColumnNames(rowType, "SELECT 2, row(null), '567 Maple Drive'");

  assertSelectUseColumnNames(
      rowType, "SELECT * from tmp where false", "not(is_null(name.full))");

  rowType = ROW({"id", "name", "address"}, {BIGINT(), ROW({}, {}), VARCHAR()});
  const auto plan = PlanBuilder()
                        .startTableScan()
                        .outputType(rowType)
                        .dataColumns(rowType)
                        .endTableScan()
                        .planNode();
  const auto split = makeSplit(file->getPath());
  const auto result =
      AssertQueryBuilder(plan)
          .connectorSessionProperty(
              kHiveConnectorId,
              connector::hive::HiveConfig::kParquetUseColumnNamesSession,
              "true")
          .split(split)
          .copyResults(pool());
  const auto rows = result->as<RowVector>();
  const auto expected = makeRowVector(ROW({}, {}), 1);
  bytedance::bolt::test::assertEqualVectors(expected, rows->childAt(1));

  vector = makeRowVector(
      {"id", "name", "address"},
      {id,
       makeRowVector(
           {"FIRST", "LAST"},
           {
               makeFlatVector<std::string>({"Janet"}),
               makeFlatVector<std::string>({"Jones"}),
           }),
       address});
  file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, {});

  rowType =
      ROW({"id", "name", "address"},
          {BIGINT(),
           ROW({"first", "middle", "last"}, {VARCHAR(), VARCHAR(), VARCHAR()}),
           VARCHAR()});
  loadData(file->getPath(), rowType, vector);
  assertSelectUseColumnNames(rowType, "SELECT 2, null, '567 Maple Drive'");

  auto lowerCasePlan =
      PlanBuilder().tableScan(rowType, {}, "", rowType).planNode();
  AssertQueryBuilder(lowerCasePlan, duckDbQueryRunner_)
      .connectorSessionProperty(
          kHiveConnectorId,
          connector::hive::HiveConfig::kParquetUseColumnNamesSession,
          "true")
      .connectorSessionProperty(
          kHiveConnectorId,
          connector::hive::HiveConfig::kFileColumnNamesReadAsLowerCaseSession,
          "true")
      .splits(splits())
      .assertResults("SELECT 2, ('Janet', null, 'Jones'), '567 Maple Drive'");
}

TEST_F(ParquetTableScanTest, testColumnNotExists) {
  auto rowType =
      ROW({"a", "b", "not_exists", "not_exists_array", "not_exists_map"},
          {BIGINT(),
           DOUBLE(),
           BIGINT(),
           ARRAY(VARBINARY()),
           MAP(VARCHAR(), BIGINT())});
  // message schema {
  //  optional int64 a;
  //  optional double b;
  // }
  loadData(
      getExampleFilePath("sample.parquet"),
      rowType,
      makeRowVector(
          {"a", "b"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
          }));

  assertSelectWithDataColumns(
      {"a", "b", "not_exists", "not_exists_array", "not_exists_map"},
      rowType,
      "SELECT a, b, NULL, NULL, NULL FROM tmp");
}

TEST_F(ParquetTableScanTest, dcMapContainsDifferentDynamicColumns) {
  // Scan three files that have different dynamic columns to mimic
  // the scenario that different splits under the same table contains
  // different dynamic columns.
  auto inputType = ROW({"event_tag"}, {MAP(VARCHAR(), VARCHAR())});

  auto plan =
      PlanBuilder(pool_.get())
          .tableScan(inputType)
          .project(
              {"CAST(event_tag['is_product_show'] AS BIGINT) AS is_product_show"})
          .planNode();

  CursorParameters params;
  params.planNode = plan;

  const int numSplitsPerFile = 1;
  const std::vector<std::string> files = {
      getExampleFilePath("dcmapDifferentDynamicColumns1.parquet"),
      getExampleFilePath("dcmapDifferentDynamicColumns2.parquet"),
      getExampleFilePath("dcmapDifferentDynamicColumns3.parquet"),
  };

  bool noMoreSplits = false;
  auto addSplits = [&](exec::Task* task) {
    if (!noMoreSplits) {
      for (const auto& file : files) {
        auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
            file, numSplitsPerFile, dwio::common::FileFormat::PARQUET);
        for (const auto& split : splits) {
          task->addSplit("0", exec::Split(split));
        }
      }
      task->noMoreSplits("0");
      noMoreSplits = true;
    }
  };

  auto result = readCursor(params, addSplits);
  ASSERT_TRUE(waitForTaskCompletion(result.first->task().get()));

  std::vector<int64_t> actual;
  for (const auto& batch : result.second) {
    auto values = batch->childAt(0)->asFlatVector<int64_t>();
    auto size = batch->size();
    for (auto i = 0; i < size; ++i) {
      actual.push_back(values->valueAt(i));
    }
  }

  ASSERT_EQ(3, actual.size());
  EXPECT_EQ(1, actual[0]);
  EXPECT_EQ(0, actual[1]);
  EXPECT_EQ(1, actual[2]);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
