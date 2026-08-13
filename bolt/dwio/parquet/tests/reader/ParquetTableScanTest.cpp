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
#include <simdjson.h>
#include <thrift/protocol/TCompactProtocol.h> //@manual
#include <thrift/transport/TBufferTransports.h>
#include <fstream>
#include <iterator>
#include <limits>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/functions/sparksql/VariantEncoding.h"
#include "bolt/functions/sparksql/registration/Register.h"
#include "bolt/type/filter/FilterUtil.h"
#include "bolt/type/filter/MapSubscriptFilter.h"
#include "bolt/type/tests/SubfieldFiltersBuilder.h"

#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/dwio/parquet/writer/Writer.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::parquet;

namespace {

std::pair<std::string, std::string> encodeVariantJson(std::string_view json) {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  auto error = parser.parse(json.data(), json.size()).get(doc);
  BOLT_CHECK(!error, "Failed to parse JSON for VARIANT test data: {}", json);

  functions::sparksql::variant::StringDictionary dict;
  functions::sparksql::variant::SparkVariantEncoder::collectKeys(doc, dict);
  dict.finalize();

  std::string value;
  functions::sparksql::variant::SparkVariantEncoder::encode(doc, dict, value);
  return {std::move(value), dict.serialize()};
}

int64_t loadedToValueHook(const std::shared_ptr<Task>& task) {
  int64_t sum = 0;
  for (const auto& pipelineStats : task->taskStats().pipelineStats) {
    for (const auto& operatorStats : pipelineStats.operatorStats) {
      auto it = operatorStats.runtimeStats.find("loadedToValueHook");
      if (it != operatorStats.runtimeStats.end()) {
        sum += it->second.sum;
      }
    }
  }
  return sum;
}

RowVectorPtr makeVariantParquetBatch(
    memory::MemoryPool* pool,
    const std::vector<int64_t>& groups,
    const std::vector<std::string>& jsons) {
  BOLT_CHECK_EQ(groups.size(), jsons.size());

  auto groupVector = BaseVector::create(BIGINT(), groups.size(), pool);
  auto valueVector = BaseVector::create(VARBINARY(), jsons.size(), pool);
  auto metadataVector = BaseVector::create(VARBINARY(), jsons.size(), pool);
  auto* flatGroupVector = groupVector->asUnchecked<FlatVector<int64_t>>();
  auto* flatValueVector = valueVector->asUnchecked<FlatVector<StringView>>();
  auto* flatMetadataVector =
      metadataVector->asUnchecked<FlatVector<StringView>>();

  std::vector<std::pair<std::string, std::string>> encoded;
  encoded.reserve(jsons.size());
  for (const auto& json : jsons) {
    encoded.push_back(encodeVariantJson(json));
  }

  for (auto i = 0; i < groups.size(); ++i) {
    flatGroupVector->set(i, groups[i]);
    flatValueVector->set(i, StringView(encoded[i].first));
    flatMetadataVector->set(i, StringView(encoded[i].second));
  }

  auto variantStorageType =
      ROW({"value", "metadata"}, {VARBINARY(), VARBINARY()});
  auto variantStorageVector = std::make_shared<RowVector>(
      pool,
      variantStorageType,
      nullptr,
      jsons.size(),
      std::vector<VectorPtr>{valueVector, metadataVector});

  return std::make_shared<RowVector>(
      pool,
      ROW({"g", "v"}, {BIGINT(), variantStorageType}),
      nullptr,
      groups.size(),
      std::vector<VectorPtr>{groupVector, variantStorageVector});
}

uint32_t readUint32LE(const char* data) {
  return static_cast<uint8_t>(data[0]) | (static_cast<uint8_t>(data[1]) << 8) |
      (static_cast<uint8_t>(data[2]) << 16) |
      (static_cast<uint8_t>(data[3]) << 24);
}

void appendUint32LE(std::string& out, uint32_t value) {
  out.push_back(static_cast<char>(value & 0xff));
  out.push_back(static_cast<char>((value >> 8) & 0xff));
  out.push_back(static_cast<char>((value >> 16) & 0xff));
  out.push_back(static_cast<char>((value >> 24) & 0xff));
}

void rewriteDateConvertedTypeToLogicalTypeOnly(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  BOLT_CHECK(in.good(), "Failed to open Parquet file for read: {}", path);
  std::string file(
      (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  BOLT_CHECK_GE(file.size(), 12);
  BOLT_CHECK_EQ(file.substr(file.size() - 4), "PAR1");

  const auto footerLength = readUint32LE(file.data() + file.size() - 8);
  BOLT_CHECK_LE(footerLength + 8, file.size());
  const auto footerOffset = file.size() - 8 - footerLength;

  thrift::FileMetaData metadata;
  auto inputBuffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
      reinterpret_cast<uint8_t*>(file.data() + footerOffset),
      footerLength,
      apache::thrift::transport::TMemoryBuffer::OBSERVE);
  apache::thrift::protocol::TCompactProtocol inputProtocol(inputBuffer);
  metadata.read(&inputProtocol);

  bool removedDateConvertedType = false;
  for (auto& schemaElement : metadata.schema) {
    if (schemaElement.__isset.converted_type &&
        schemaElement.converted_type == thrift::ConvertedType::DATE &&
        schemaElement.__isset.logicalType &&
        schemaElement.logicalType.__isset.DATE) {
      schemaElement.__isset.converted_type = false;
      removedDateConvertedType = true;
    }
  }
  BOLT_CHECK(
      removedDateConvertedType,
      "Failed to find DATE converted_type in Parquet footer: {}",
      path);

  auto outputBuffer =
      std::make_shared<apache::thrift::transport::TMemoryBuffer>();
  apache::thrift::protocol::TCompactProtocol outputProtocol(outputBuffer);
  metadata.write(&outputProtocol);
  uint8_t* serializedFooter{nullptr};
  uint32_t serializedFooterLength{0};
  outputBuffer->getBuffer(&serializedFooter, &serializedFooterLength);

  file.resize(footerOffset);
  file.append(
      reinterpret_cast<const char*>(serializedFooter), serializedFooterLength);
  appendUint32LE(file, serializedFooterLength);
  file.append("PAR1", 4);

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  BOLT_CHECK(out.good(), "Failed to open Parquet file for write: {}", path);
  out.write(file.data(), file.size());
}

class MapSubscriptMetadataFilterParser final
    : public exec::PrestoExprToSubfieldFilterParser {
 public:
  explicit MapSubscriptMetadataFilterParser(
      std::unique_ptr<common::Filter> valueFilter =
          common::createBytesRange("1.25", true, "1.25", true, false))
      : valueFilter_(std::move(valueFilter)) {}

  std::optional<std::pair<common::Subfield, std::unique_ptr<common::Filter>>>
  leafCallToSubfieldFilter(
      const core::CallTypedExpr& call,
      core::ExpressionEvaluator* evaluator,
      bool negated) override {
    if (!negated && call.name() == "eq" && call.inputs().size() == 2) {
      auto mapAccess = std::dynamic_pointer_cast<const core::CallTypedExpr>(
          call.inputs()[0]);
      if (mapAccess && mapAccess->name() == "element_at") {
        std::shared_ptr<const common::Filter> keyFilter =
            common::createBytesRange("key", true, "key", true, false);
        return std::make_pair(
            common::Subfield("c0"),
            common::createMapSubscriptFilter(
                "key", valueFilter_->clone(), std::move(keyFilter)));
      }
    }
    return PrestoExprToSubfieldFilterParser::leafCallToSubfieldFilter(
        call, evaluator, negated);
  }

 private:
  const std::shared_ptr<const common::Filter> valueFilter_;
};

class ScopedExprToSubfieldFilterParser {
 public:
  explicit ScopedExprToSubfieldFilterParser(
      std::shared_ptr<exec::ExprToSubfieldFilterParser> parser)
      : previous_(exec::ExprToSubfieldFilterParser::getInstance()) {
    exec::ExprToSubfieldFilterParser::registerParser(std::move(parser));
  }

  ~ScopedExprToSubfieldFilterParser() {
    exec::ExprToSubfieldFilterParser::registerParser(std::move(previous_));
  }

 private:
  std::shared_ptr<exec::ExprToSubfieldFilterParser> previous_;
};

} // namespace

class ParquetTableScanTest : public HiveConnectorTestBase {
 protected:
  using OperatorTestBase::assertQuery;

  void SetUp() {
    registerParquetReaderFactory();
    functions::sparksql::registerFunctions("");

    auto hiveConnector =
        connector::getConnectorFactory(connector::kHiveConnectorName)
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

  void assertSelectWithAssignments(
      std::vector<std::string>&& outputColumnNames,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& assignments,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));
    auto tableHandle = makeTableHandle({}, nullptr, "hive_table", rowType);
    auto plan =
        PlanBuilder().tableScan(rowType, tableHandle, assignments).planNode();
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

  std::shared_ptr<Task> runSelectWithFilter(
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& subfieldFilters,
      const std::string& remainingFilter,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));
    parse::ParseOptions options;
    options.parseDecimalAsDouble = false;

    auto plan =
        PlanBuilder(pool_.get())
            .setParseOptions(options)
            .tableScan(rowType, subfieldFilters, remainingFilter, rowType_)
            .planNode();

    return assertQuery(plan, splits_, sql);
  }

  static std::unordered_map<std::string, RuntimeMetric>
  getTableScanRuntimeStats(const std::shared_ptr<Task>& task) {
    return task->taskStats().pipelineStats[0].operatorStats[0].runtimeStats;
  }

  static int64_t getRuntimeStat(
      const std::shared_ptr<Task>& task,
      const std::string& name) {
    return getTableScanRuntimeStats(task).at(name).sum;
  }

  void loadColumnIndexTestData(const RowTypePtr& rowType, RowVectorPtr data) {
    loadData(getExampleFilePath("column_index.parquet"), rowType, data);
  }

  void loadColumnIndexSingleBigintColumn() {
    loadColumnIndexTestData(
        ROW({"_1"}, {BIGINT()}),
        makeRowVector({"_1"}, {makeFlatVector<int64_t>(2000, [](auto row) {
                        return row;
                      })}));
  }

  void loadColumnIndexTwoBigintColumns() {
    loadColumnIndexTestData(
        ROW({"_1", "_5"}, {BIGINT(), BIGINT()}),
        makeRowVector(
            {"_1", "_5"},
            {
                makeFlatVector<int64_t>(2000, [](auto row) { return row; }),
                makeFlatVector<int64_t>(2000, [](auto row) { return row; }),
            }));
  }

  void loadColumnIndexThreeProjectedColumns() {
    loadColumnIndexTestData(
        ROW({"_1", "_2", "_5"}, {BIGINT(), VARCHAR(), BIGINT()}),
        makeRowVector(
            {"_1", "_2", "_5"},
            {
                makeFlatVector<int64_t>(2000, [](auto row) { return row; }),
                makeFlatVector<std::string>(
                    2000, [](auto row) { return fmt::format("{}", row); }),
                makeFlatVector<int64_t>(2000, [](auto row) { return row; }),
            }));
  }

  void assertFilteredOutPagesMetrics(
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& subfieldFilters,
      const std::string& remainingFilter,
      const std::string& sql,
      int64_t expectedTotalPages,
      int64_t expectedFilteredOutPages) {
    std::shared_ptr<Task> task;
    if (subfieldFilters.empty() && remainingFilter.empty()) {
      auto rowType = getRowType(std::move(outputColumnNames));
      task = assertQuery(
          PlanBuilder(pool_.get()).tableScan(rowType).planNode(), splits_, sql);
    } else {
      task = runSelectWithFilter(
          std::move(outputColumnNames), subfieldFilters, remainingFilter, sql);
    }

    EXPECT_EQ(getRuntimeStat(task, "totalPages"), expectedTotalPages);
    EXPECT_EQ(
        getRuntimeStat(task, "filteredOutPages"), expectedFilteredOutPages);
  }

  void loadData(
      const std::string& filePath,
      RowTypePtr rowType,
      RowVectorPtr data,
      const std::optional<
          std::unordered_map<std::string, std::optional<std::string>>>&
          partitionKeys = std::nullopt,
      const std::optional<std::unordered_map<std::string, std::string>>&
          infoColumns = std::nullopt) {
    splits_ = {makeSplit(filePath, partitionKeys, infoColumns)};
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
      const std::string& filePath,
      const std::optional<
          std::unordered_map<std::string, std::optional<std::string>>>&
          partitionKeys = std::nullopt,
      const std::optional<std::unordered_map<std::string, std::string>>&
          infoColumns = std::nullopt) {
    return makeHiveConnectorSplits(
        filePath,
        1,
        dwio::common::FileFormat::PARQUET,
        partitionKeys,
        infoColumns)[0];
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

  std::unique_ptr<TaskCursor> makeCursorWithoutCopy(
      const core::PlanNodePtr& plan,
      const std::string& filePath) {
    CursorParameters params;
    params.copyResult = false;
    params.serialExecution = true;
    params.planNode = plan;
    auto cursor = TaskCursor::create(params);
    cursor->task()->addSplit("0", exec::Split(makeSplit(filePath)));
    cursor->task()->noMoreSplits("0");
    return cursor;
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

TEST_F(ParquetTableScanTest, tableScanPreservesLazyVectors) {
  constexpr vector_size_t kSize = 20;
  auto data = makeRowVector(
      {"a", "b"},
      {
          makeFlatVector<int64_t>(kSize, [](auto row) { return row + 1; }),
          makeFlatVector<double>(kSize, [](auto row) { return row + 1; }),
      });
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, {});
  auto rowType = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  auto plan = PlanBuilder().tableScan(rowType).planNode();

  auto cursor = makeCursorWithoutCopy(plan, file->getPath());
  vector_size_t rowOffset = 0;
  while (cursor->moveNext()) {
    auto result = cursor->current()->asUnchecked<RowVector>();
    ASSERT_GT(result->size(), 0);
    ASSERT_EQ(2, result->childrenSize());
    ASSERT_TRUE(isLazyNotLoaded(*result->childAt(0)));
    ASSERT_TRUE(isLazyNotLoaded(*result->childAt(1)));

    auto a = result->childAt(0)->loadedVector()->asFlatVector<int64_t>();
    auto b = result->childAt(1)->loadedVector()->asFlatVector<double>();
    for (auto i = 0; i < result->size(); ++i) {
      EXPECT_EQ(rowOffset + i + 1, a->valueAt(i));
      EXPECT_EQ(rowOffset + i + 1, b->valueAt(i));
    }
    rowOffset += result->size();
  }
  ASSERT_EQ(20, rowOffset);
  ASSERT_TRUE(waitForTaskCompletion(cursor->task().get()));
}

TEST_F(ParquetTableScanTest, tableScanPreservesComplexLazyVectors) {
  constexpr vector_size_t kSize = 8;
  auto data = makeRowVector(
      {"id", "items", "payload"},
      {
          makeFlatVector<int64_t>(kSize, [](auto row) { return row; }),
          makeArrayVector<int32_t>(
              kSize,
              [](auto row) { return row % 3; },
              [](auto row) { return row * 7; }),
          makeRowVector(
              {"name", "score"},
              {
                  makeFlatVector<std::string>(
                      kSize, [](auto row) { return fmt::format("n{}", row); }),
                  makeFlatVector<double>(
                      kSize, [](auto row) { return row * 2.5; }),
              }),
      });
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, {});
  auto rowType = asRowType(data->type());
  auto plan = PlanBuilder().tableScan(rowType).planNode();

  auto cursor = makeCursorWithoutCopy(plan, file->getPath());
  ASSERT_TRUE(cursor->moveNext());
  auto result = cursor->current();
  ASSERT_EQ(kSize, result->size());
  for (auto i = 0; i < result->childrenSize(); ++i) {
    ASSERT_TRUE(isLazyNotLoaded(*result->childAt(i))) << i;
    result->childAt(i)->loadedVector();
  }
  bytedance::bolt::test::assertEqualVectors(data, result);
  ASSERT_FALSE(cursor->moveNext());
  ASSERT_TRUE(waitForTaskCompletion(cursor->task().get()));
}

TEST_F(ParquetTableScanTest, filterColumnsAreEagerButProjectedColumnsAreLazy) {
  constexpr vector_size_t kSize = 10;
  auto data = makeRowVector(
      {"filter_col", "payload", "items"},
      {
          makeFlatVector<int64_t>(kSize, [](auto row) { return row; }),
          makeFlatVector<double>(kSize, [](auto row) { return row + 0.25; }),
          makeArrayVector<int32_t>(
              kSize,
              [](auto row) { return row % 2; },
              [](auto row) { return row + 100; }),
      });
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, {});
  auto rowType = asRowType(data->type());
  auto plan = PlanBuilder().tableScan(rowType, {"filter_col >= 3"}).planNode();

  auto cursor = makeCursorWithoutCopy(plan, file->getPath());
  ASSERT_TRUE(cursor->moveNext());
  auto result = cursor->current();
  ASSERT_EQ(7, result->size());
  ASSERT_FALSE(isLazyNotLoaded(*result->childAt(0)));
  ASSERT_TRUE(isLazyNotLoaded(*result->childAt(1)));
  ASSERT_TRUE(isLazyNotLoaded(*result->childAt(2)));

  auto filterCol = result->childAt(0)->asFlatVector<int64_t>();
  auto payload = result->childAt(1)->loadedVector()->asFlatVector<double>();
  result->childAt(2)->loadedVector();
  for (auto i = 0; i < result->size(); ++i) {
    EXPECT_EQ(i + 3, filterCol->valueAt(i));
    EXPECT_EQ(i + 3.25, payload->valueAt(i));
  }
  ASSERT_FALSE(cursor->moveNext());
  ASSERT_TRUE(waitForTaskCompletion(cursor->task().get()));
}

TEST_F(ParquetTableScanTest, remainingFilterPreservesLazyOutputWrapper) {
  constexpr vector_size_t kSize = 12;
  auto data = makeRowVector(
      {"a", "b", "payload"},
      {
          makeFlatVector<int64_t>(kSize, [](auto row) { return row; }),
          makeFlatVector<int64_t>(kSize, [](auto row) { return row % 3; }),
          makeFlatVector<std::string>(
              kSize, [](auto row) { return fmt::format("payload{}", row); }),
      });
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, {});
  auto rowType = asRowType(data->type());
  auto plan = PlanBuilder().tableScan(rowType, {}, "b = 1").planNode();

  auto cursor = makeCursorWithoutCopy(plan, file->getPath());
  ASSERT_TRUE(cursor->moveNext());
  auto result = cursor->current();
  ASSERT_EQ(4, result->size());
  ASSERT_TRUE(isLazyNotLoaded(*result->childAt(0)));
  ASSERT_FALSE(isLazyNotLoaded(*result->childAt(1)));
  ASSERT_TRUE(isLazyNotLoaded(*result->childAt(2)));

  DecodedVector a(*result->childAt(0), SelectivityVector(result->size()));
  DecodedVector b(*result->childAt(1), SelectivityVector(result->size()));
  DecodedVector payload(*result->childAt(2), SelectivityVector(result->size()));
  for (auto i = 0; i < result->size(); ++i) {
    const auto sourceRow = 1 + 3 * i;
    EXPECT_EQ(sourceRow, a.valueAt<int64_t>(i));
    EXPECT_EQ(1, b.valueAt<int64_t>(i));
    EXPECT_EQ(
        fmt::format("payload{}", sourceRow),
        payload.valueAt<StringView>(i).str());
  }
  ASSERT_FALSE(cursor->moveNext());
  ASSERT_TRUE(waitForTaskCompletion(cursor->task().get()));
}

TEST_F(ParquetTableScanTest, aggregatePushdownToSmallPages) {
  const std::vector<std::string> columnNames = {"a", "b", "c"};
  const auto expectedRowVector = makeRowVector(
      {makeFlatVector<int16_t>({1, 2, 4}),
       makeFlatVector<int64_t>({7, 9, 13})});
  const auto outputType =
      ROW(std::vector<std::string>(columnNames),
          {SMALLINT(), SMALLINT(), VARCHAR()});
  std::vector<RowVectorPtr> data;
  for (auto row = 0; row < 10; ++row) {
    data.emplace_back(makeRowVector(
        columnNames,
        {
            makeFlatVector<int16_t>(
                std::vector<int16_t>{static_cast<int16_t>(row % 5)}),
            makeFlatVector<int16_t>(
                std::vector<int16_t>{static_cast<int16_t>(row)}),
            makeFlatVector<std::string>({std::to_string(row)}),
        }));
  }
  const auto filePath = TempFilePath::create();
  WriterOptions options;
  options.dataPageSize = 1;
  writeToParquetFile(filePath->getPath(), data, options);
  const auto plan =
      PlanBuilder(pool())
          .tableScan(
              outputType,
              {},
              "c <> '' AND a in (1::smallint, 2::smallint, 4::smallint)")
          .singleAggregation({"a"}, {"sum(b) as s"})
          .planNode();
  AssertQueryBuilder(plan)
      .split(makeSplit(filePath->getPath()))
      .assertResults(expectedRowVector);

  std::shared_ptr<Task> task;
  AssertQueryBuilder(plan)
      .split(makeSplit(filePath->getPath()))
      .copyResults(pool(), task);
  EXPECT_GT(loadedToValueHook(task), 0);
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

TEST_F(ParquetTableScanTest, variantE2EProjectAndAggregation) {
  auto file = TempFilePath::create();
  WriterOptions writerOptions;
  auto data = makeVariantParquetBatch(
      pool(),
      {1, 1, 2},
      {R"({"a":1,"name":"x"})",
       R"({"a":2,"name":"y"})",
       R"({"a":3,"name":"z"})"});
  writeToParquetFile(file->getPath(), {data}, writerOptions);

  auto logicalRowType = ROW({"g", "v"}, {BIGINT(), VARIANT()});
  auto plan = PlanBuilder(pool())
                  .tableScan(logicalRowType)
                  .project({"g", "cast(variant_get(v, '$.a') as bigint) AS a"})
                  .singleAggregation({"g"}, {"sum(a) AS sum_a"})
                  .orderBy({"g"}, false)
                  .planNode();

  auto results = AssertQueryBuilder(plan)
                     .split(makeSplit(file->getPath()))
                     .copyResults(pool());

  auto expected = makeRowVector(
      {"g", "sum_a"},
      {makeFlatVector<int64_t>({1, 2}), makeFlatVector<int64_t>({3, 3})});
  ASSERT_TRUE(assertEqualResults({expected}, {results}));
}

TEST_F(ParquetTableScanTest, variantReadsAfterReaderOutputReuse) {
  auto file = TempFilePath::create();
  WriterOptions writerOptions;
  auto data = makeVariantParquetBatch(
      pool(),
      {1, 2, 3, 4, 5},
      {R"({"a":10,"name":"aa"})",
       R"({"a":20,"name":"bb"})",
       R"({"a":30,"name":"cc"})",
       R"({"a":40,"name":"dd"})",
       R"({"a":50,"name":"ee"})"});
  writeToParquetFile(file->getPath(), {data}, writerOptions);

  auto logicalRowType = ROW({"g", "v"}, {BIGINT(), VARIANT()});
  auto plan = PlanBuilder(pool())
                  .tableScan(logicalRowType)
                  .project(
                      {"g",
                       "cast(variant_get(v, '$.a') as bigint) AS a",
                       "variant_get(v, '$.name') AS name"})
                  .planNode();

  auto expected = makeRowVector(
      {"g", "a", "name"},
      {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
       makeFlatVector<std::string>({"aa", "bb", "cc", "dd", "ee"})});

  AssertQueryBuilder(plan)
      .split(makeSplit(file->getPath()))
      .config(core::QueryConfig::kMaxOutputBatchRows, "2")
      .assertResults(expected);
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

TEST_F(ParquetTableScanTest, rowIndex) {
  static const char* kPath = "file_path";
  // case 1: file not have `_tmp_metadata_row_index`, scan generate it for user.
  auto filePath = getExampleFilePath("sample.parquet");
  loadData(
      filePath,
      ROW({"a", "b", "_tmp_metadata_row_index", kPath},
          {BIGINT(), DOUBLE(), BIGINT(), VARCHAR()}),
      makeRowVector(
          {"a", "b", "_tmp_metadata_row_index", kPath},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
              makeFlatVector<int64_t>(20, [](auto row) { return row; }),
              makeFlatVector<std::string>(
                  20, [filePath](auto /*row*/) { return filePath; }),
          }),
      std::nullopt,
      std::unordered_map<std::string, std::string>{{kPath, filePath}});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  assignments["a"] = std::make_shared<connector::hive::HiveColumnHandle>(
      "a",
      connector::hive::HiveColumnHandle::ColumnType::kRegular,
      BIGINT(),
      BIGINT());
  assignments["b"] = std::make_shared<connector::hive::HiveColumnHandle>(
      "b",
      connector::hive::HiveColumnHandle::ColumnType::kRegular,
      DOUBLE(),
      DOUBLE());
  assignments[kPath] = synthesizedColumn(kPath, VARCHAR());
  assignments["_tmp_metadata_row_index"] =
      std::make_shared<connector::hive::HiveColumnHandle>(
          "_tmp_metadata_row_index",
          connector::hive::HiveColumnHandle::ColumnType::kRowIndex,
          BIGINT(),
          BIGINT());

  assertSelectWithAssignments({"a"}, assignments, "SELECT a FROM tmp");
  assertSelectWithAssignments(
      {"a", "_tmp_metadata_row_index"},
      assignments,
      "SELECT a, _tmp_metadata_row_index FROM tmp");
  assertSelectWithAssignments(
      {"_tmp_metadata_row_index", "a"},
      assignments,
      "SELECT _tmp_metadata_row_index, a FROM tmp");
  assertSelectWithAssignments(
      {kPath, "_tmp_metadata_row_index"},
      assignments,
      fmt::format("SELECT {}, _tmp_metadata_row_index FROM tmp", kPath));

  // case 2: file has `_tmp_metadata_row_index` column, then use user data
  // insteads of generating it.
  loadData(
      getExampleFilePath("sample_with_rowindex.parquet"),
      ROW({"a", "b", "_tmp_metadata_row_index"},
          {BIGINT(), DOUBLE(), BIGINT()}),
      makeRowVector(
          {"a", "b", "_tmp_metadata_row_index"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
          }));

  assignments.erase(kPath);
  assertSelectWithAssignments({"a"}, assignments, "SELECT a FROM tmp");
  assertSelectWithAssignments(
      {"a", "_tmp_metadata_row_index"},
      assignments,
      "SELECT a, _tmp_metadata_row_index FROM tmp");
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

TEST_F(ParquetTableScanTest, deltaByteArray) {
  auto a = makeFlatVector<StringView>({"axis", "axle", "babble", "babyhood"});
  auto expected = makeRowVector({"a"}, {a});
  createDuckDbTable("expected", {expected});

  auto vector = makeFlatVector<StringView>({{}});
  loadData(
      getExampleFilePath("delta_byte_array.parquet"),
      ROW({"a"}, {VARCHAR()}),
      makeRowVector({"a"}, {vector}));
  assertSelect({"a"}, "SELECT a from expected");
}

TEST_F(ParquetTableScanTest, filteredOutPagesMetricsNoFilter) {
  loadColumnIndexSingleBigintColumn();
  assertFilteredOutPagesMetrics({"_1"}, {}, "", "SELECT _1 FROM tmp", 21, 0);
}

TEST_F(ParquetTableScanTest, filteredOutPagesMetricsSingleFilterColumn) {
  loadColumnIndexSingleBigintColumn();
  assertFilteredOutPagesMetrics(
      {"_1"}, {"_1 < 20"}, "", "SELECT _1 FROM tmp WHERE _1 < 20", 19, 18);
}

TEST_F(ParquetTableScanTest, filteredOutPagesMetricsMultipleOutputColumns) {
  loadColumnIndexTwoBigintColumns();
  assertFilteredOutPagesMetrics(
      {"_1", "_5"},
      {"_1 < 20"},
      "",
      "SELECT _1, _5 FROM tmp WHERE _1 < 20",
      20,
      18);
}

TEST_F(ParquetTableScanTest, filteredOutPagesMetricsAllPagesFilteredOut) {
  loadColumnIndexSingleBigintColumn();
  assertFilteredOutPagesMetrics(
      {"_1"}, {"_1 < 0"}, "", "SELECT _1 FROM tmp WHERE _1 < 0", 0, 0);
}

TEST_F(ParquetTableScanTest, filteredOutPagesMetricsSingleTailPageHit) {
  loadColumnIndexSingleBigintColumn();
  assertFilteredOutPagesMetrics(
      {"_1"}, {"_1 > 1900"}, "", "SELECT _1 FROM tmp WHERE _1 > 1900", 2, 0);
}

TEST_F(ParquetTableScanTest, filteredOutPagesMetricsFilterKeepsAllPages) {
  loadColumnIndexSingleBigintColumn();
  assertFilteredOutPagesMetrics(
      {"_1"}, {"_1 >= 0"}, "", "SELECT _1 FROM tmp WHERE _1 >= 0", 21, 0);
}

TEST_F(ParquetTableScanTest, filteredOutPagesMetricsFilterOnUnprojectedColumn) {
  loadColumnIndexTwoBigintColumns();
  assertFilteredOutPagesMetrics(
      {"_5"}, {"_1 < 20"}, "", "SELECT _5 FROM tmp WHERE _1 < 20", 20, 18);
}

TEST_F(
    ParquetTableScanTest,
    filteredOutPagesMetricsNoFilterThreeProjectedColumns) {
  loadColumnIndexThreeProjectedColumns();
  assertFilteredOutPagesMetrics(
      {"_1", "_2", "_5"}, {}, "", "SELECT _1, _2, _5 FROM tmp", 63, 0);
}

TEST_F(ParquetTableScanTest, filteredOutPagesMetricsThreeProjectedColumns) {
  loadColumnIndexThreeProjectedColumns();
  assertFilteredOutPagesMetrics(
      {"_1", "_2", "_5"},
      {"_1 < 20"},
      "",
      "SELECT _1, _2, _5 FROM tmp WHERE _1 < 20",
      21,
      18);
}

// Plan-level matrix coverage for the strict convertType policy.
//
// Each case writes a 3-row parquet file using `fileType`, then runs a
// TableScan plan declaring `declaredType` (propagated to HiveConnector
// via dataColumns, which becomes ReaderOptions::fileSchema). We assert:
//   - shouldThrow=true: AssertQueryBuilder.copyResults raises an error
//     whose message contains Case::errMsg (defaults to the BoltUserError
//     from convertType; non-Spark builds also see matchType rejections).
//   - shouldThrow=false: copyResults returns 3 rows of declaredType
//     and (when `expectedValues` is supplied) the values match.
TEST_F(ParquetTableScanTest, convertTypePolicyMatrix) {
  struct Case {
    const char* name;
    TypePtr fileType;
    TypePtr declaredType;
    bool shouldThrow;
    // Substring to match against the thrown error. Defaults to the
    // BoltUserError raised by ReaderBase::convertType via
    // BOLT_SCHEMA_MISMATCH_ERROR with a Parquet-specific stable prefix.
    // Override when a case is rejected by a later layer with a
    // different message (e.g. ParquetColumnReader::matchType in
    // non-Spark builds).
    const char* errMsg = kParquetTypeMappingErrorPrefix;
  };

  // clang-format off
  const std::vector<Case> cases = {
    // ---- Reject: structural mismatch (the original SIGSEGV bug) ----
    {"array_declared_vs_varchar_file", VARCHAR(),       ARRAY(VARCHAR()),   true},
    {"array_declared_vs_bigint_file",  BIGINT(),        ARRAY(BIGINT()),    true},

    // ---- Reject: INT64 narrowing (both flavours; Spark INT64 reader
    //               also rejects Byte/Short/Integer requests). ----
    {"bigint_to_integer",              BIGINT(),        INTEGER(),          true},
    {"bigint_to_smallint",             BIGINT(),        SMALLINT(),         true},
    {"bigint_to_tinyint",              BIGINT(),        TINYINT(),          true},

    // ---- Accept: INT32 narrowing. File INT32 -> requested Byte/Short
    //      is silently truncated at read time by IntegerColumnReader,
    //      matching Spark's vectorized reader (ByteUpdater/ShortUpdater)
    //      and covering HIVE-14294 / SPARK-16632 where Hive 1.x writes
    //      TINYINT/SMALLINT as unannotated INT32. See the
    //      integer-narrowing block in convertTypePolicyValueChecks
    //      for end-to-end values. ----
    {"integer_to_smallint",            INTEGER(),       SMALLINT(),         false},
    {"integer_to_tinyint",             INTEGER(),       TINYINT(),          false},

    // ---- Accept: INT32 -> DOUBLE. INT32 is exactly representable as DOUBLE. ----
    {"integer_to_double",              INTEGER(),       DOUBLE(),           false},

    // ---- Reject: cross-family with no safe column-reader auto-cast ----
    {"bigint_to_double",               BIGINT(),        DOUBLE(),           true},
    {"integer_to_real",                INTEGER(),       REAL(),             true},

    // ---- Accept: INT32 -> BOOLEAN for Spark/Hive compatibility. ----
    {"integer_to_boolean",             INTEGER(),       BOOLEAN(),          false},

    // ---- Accept: DATE annotation read as raw epoch-day INT or VARCHAR. ----
    {"date_to_integer",                DATE(),          INTEGER(),          false},
    {"date_to_varchar",                DATE(),          VARCHAR(),          false},

    // ---- Reject: DATE annotation should not be treated as wider int family. ----
    {"date_to_bigint",                 DATE(),          BIGINT(),           true,
     "From Kind: INT32(DATE), To Kind: BIGINT"},

    // ---- Accept: column-reader auto-cast floating point -> VARCHAR. ----
    {"real_to_varchar",                REAL(),          VARCHAR(),          false},
    {"double_to_varchar",              DOUBLE(),        VARCHAR(),          false},

    // ---- Hive SerDe-compatible floating point -> BIGINT cast. ----
#ifdef SPARK_COMPATIBLE
    {"double_to_bigint",               DOUBLE(),        BIGINT(),           false},
#else
    {"double_to_bigint",               DOUBLE(),        BIGINT(),           true},
#endif

    // ---- Reject: float narrowing ----
    {"double_to_real",                 DOUBLE(),        REAL(),             true},

    // ---- Reject: BOOLEAN cross-family ----
    {"boolean_to_integer",             BOOLEAN(),       INTEGER(),          true},
    {"boolean_to_varchar",             BOOLEAN(),       VARCHAR(),          true},

    // ---- Reject: DECIMAL widening violations ----
    {"decimal_scale_shrink",           DECIMAL(10, 2),  DECIMAL(10, 0),     true},
    {"decimal_scale_grew_prec_same",   DECIMAL(10, 2),  DECIMAL(10, 5),     true},
    {"decimal_both_shrink",            DECIMAL(38, 18), DECIMAL(10, 2),     true},

    // ---- Reject: DECIMAL cross-family ----
    {"decimal_to_bigint",              DECIMAL(10, 2),  BIGINT(),           true},
    {"decimal_to_varchar",             DECIMAL(10, 2),  VARCHAR(),          true},

    // ---- Accept: identity ----
    {"bigint_to_bigint",   BIGINT(),  BIGINT(),   false},
    {"varchar_to_varchar", VARCHAR(), VARCHAR(),  false},
    {"double_to_double",   DOUBLE(),  DOUBLE(),   false},
    {"boolean_to_boolean", BOOLEAN(), BOOLEAN(),  false},

    // ---- Accept: integer widening within int family ----
    {"tinyint_to_smallint", TINYINT(), SMALLINT(), false},
    {"tinyint_to_integer",  TINYINT(), INTEGER(),  false},
    {"tinyint_to_bigint",   TINYINT(), BIGINT(),   false},
    {"smallint_to_integer", SMALLINT(),INTEGER(),  false},
    {"smallint_to_bigint",  SMALLINT(),BIGINT(),   false},
    {"integer_to_bigint",   INTEGER(), BIGINT(),   false},

    // ---- Accept: float widening (REAL -> DOUBLE, lossless) ----
    {"real_to_double",      REAL(),    DOUBLE(),   false},

    // ---- Accept: DECIMAL widening ----
    {"decimal_identity",              DECIMAL(10, 2),  DECIMAL(10, 2),     false},
    {"decimal_widen_precision_only",  DECIMAL(10, 2),  DECIMAL(15, 2),     false},
    {"decimal_widen_both",            DECIMAL(10, 2),  DECIMAL(20, 5),     false},
    {"decimal_widen_to_long_decimal", DECIMAL(10, 2),  DECIMAL(28, 5),     false},

    // ---- Accept: column-reader auto-cast int family -> VARCHAR ----
    //  (IntegerColumnReader::makeCastExpr; the cast path compiles in
    //  both build flavours and matchType() lets INT fileType fall
    //  through to its default branch, so this works in non-Spark too.)
    {"tinyint_to_varchar", TINYINT(), VARCHAR(), false},
    {"smallint_to_varchar",SMALLINT(),VARCHAR(), false},
    {"integer_to_varchar", INTEGER(), VARCHAR(), false},
    {"bigint_to_varchar",  BIGINT(),  VARCHAR(), false},

    // ---- Column-reader auto-cast VARCHAR -> int family ----
    //  StringColumnReader::makeCastExpr handles the cast in both build
    //  flavours; convertType accepts the pair. In non-Spark builds an
    //  extra matchType() check at ParquetColumnReader.cpp:95 then
    //  rejects VARCHAR-file -> INT-requested with its own message, so
    //  the case expectation flips per flavour.
#ifdef SPARK_COMPATIBLE
    {"varchar_to_tinyint", VARCHAR(), TINYINT(), false},
    {"varchar_to_smallint",VARCHAR(), SMALLINT(),false},
    {"varchar_to_integer", VARCHAR(), INTEGER(), false},
    {"varchar_to_bigint",  VARCHAR(), BIGINT(),  false},
#else
    {"varchar_to_bigint",  VARCHAR(), BIGINT(),  true, "file schema type"},
#endif
  };
  // clang-format on

  // 3-row sample data builder. DECIMAL is INT64-backed and gets the
  // type override; the rest use their native C++ types.
  auto makeSampleData = [&](const TypePtr& fileType) -> RowVectorPtr {
    if (fileType->isDecimal()) {
      return makeRowVector(
          {"c0"}, {makeFlatVector<int64_t>({100, 200, 300}, fileType)});
    }
    if (fileType->isDate()) {
      return makeRowVector(
          {"c0"}, {makeFlatVector<int32_t>({1, 2, 3}, DATE())});
    }
    switch (fileType->kind()) {
      case TypeKind::TINYINT:
        return makeRowVector({"c0"}, {makeFlatVector<int8_t>({1, 2, 3})});
      case TypeKind::SMALLINT:
        return makeRowVector({"c0"}, {makeFlatVector<int16_t>({1, 2, 3})});
      case TypeKind::INTEGER:
        return makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3})});
      case TypeKind::BIGINT:
        return makeRowVector({"c0"}, {makeFlatVector<int64_t>({1, 2, 3})});
      case TypeKind::REAL:
        return makeRowVector(
            {"c0"}, {makeFlatVector<float>({1.0f, 2.0f, 3.0f})});
      case TypeKind::DOUBLE:
        return makeRowVector({"c0"}, {makeFlatVector<double>({1.0, 2.0, 3.0})});
      case TypeKind::BOOLEAN:
        return makeRowVector(
            {"c0"}, {makeFlatVector<bool>({true, false, true})});
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        return makeRowVector(
            {"c0"}, {makeFlatVector<StringView>({"100", "200", "300"})});
      default:
        BOLT_FAIL("unsupported test fileType: {}", fileType->toString());
    }
  };

  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);

    auto data = makeSampleData(c.fileType);
    auto file = exec::test::TempFilePath::create();
    WriterOptions writerOptions;
    writeToParquetFile(file->getPath(), {data}, writerOptions);

    auto declaredRowType = ROW({"c0"}, {c.declaredType});
    auto plan = PlanBuilder(pool())
                    .tableScan(declaredRowType, {}, "", declaredRowType)
                    .planNode();

    if (c.shouldThrow) {
      BOLT_ASSERT_THROW(
          AssertQueryBuilder(plan)
              .split(makeSplit(file->getPath()))
              .copyResults(pool()),
          c.errMsg);
    } else {
      auto result = AssertQueryBuilder(plan)
                        .split(makeSplit(file->getPath()))
                        .copyResults(pool());
      ASSERT_NE(result, nullptr);
      EXPECT_EQ(result->size(), 3) << "expected 3 rows, got " << result->size();
      EXPECT_TRUE(result->type()->equivalent(*declaredRowType))
          << "expected " << declaredRowType->toString() << ", got "
          << result->type()->toString();
    }
  }
}

// Targeted value-validation companion for the matrix above. The cases
// here exercise the data path (not just convertType), so we explicitly
// check the column-reader-level cast and the integer-widening path
// produce the right values.
TEST_F(ParquetTableScanTest, convertTypePolicyValueChecks) {
  // 1. INT widening: file INT32 [1,2,3] read as BIGINT -> [1,2,3].
  {
    auto data = makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3})});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {BIGINT()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected = makeRowVector({"c0"}, {makeFlatVector<int64_t>({1, 2, 3})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }

  // 2. INT32 -> DOUBLE widening: file INT32 [1,2,3] read as DOUBLE.
  {
    auto data = makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3})});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {DOUBLE()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected =
        makeRowVector({"c0"}, {makeFlatVector<double>({1.0, 2.0, 3.0})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }

  // 3. INT32 -> BOOLEAN compatibility: zero maps to false, non-zero maps to
  //    true, and nulls are preserved.
  {
    auto data = makeRowVector(
        {"c0"}, {makeNullableFlatVector<int32_t>({0, 1, -7, std::nullopt})});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {BOOLEAN()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected = makeRowVector(
        {"c0"},
        {makeNullableFlatVector<bool>({false, true, true, std::nullopt})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }

  // INT32 -> BOOLEAN with all-null input uses the same null-only result shape
  // as the generic fixed-width path.
  {
    auto data = makeRowVector({"c0"}, {makeAllNullFlatVector<int32_t>(3)});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {BOOLEAN()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected = makeRowVector({"c0"}, {makeAllNullFlatVector<bool>(3)});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }

  // 4. Float widening: REAL [1.5, 2.5, 3.5] read as DOUBLE.
  {
    auto data =
        makeRowVector({"c0"}, {makeFlatVector<float>({1.5f, 2.5f, 3.5f})});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {DOUBLE()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected =
        makeRowVector({"c0"}, {makeFlatVector<double>({1.5, 2.5, 3.5})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }

#ifdef SPARK_COMPATIBLE
  // 4. Auto-cast VARCHAR -> BIGINT: ["100","200","300"] -> [100,200,300].
  //    Skipped in non-Spark builds because matchType() at
  //    ParquetColumnReader.cpp:95 rejects VARCHAR-file -> INT-requested
  //    before the column-reader cast can run.
  {
    auto data = makeRowVector(
        {"c0"}, {makeFlatVector<StringView>({"100", "200", "300"})});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {BIGINT()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected =
        makeRowVector({"c0"}, {makeFlatVector<int64_t>({100, 200, 300})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }
#endif

  // DATE annotation read as INTEGER: file INT32(DATE) [1,2,3]
  // returns raw epoch-day integers when the Hive/Spark schema says INT.
  {
    auto data =
        makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3}, DATE())});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {INTEGER()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected = makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }

  // Same behavior for files that carry only the newer logicalType.DATE
  // annotation without the deprecated converted_type field.
  {
    auto data =
        makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3}, DATE())});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});
    rewriteDateConvertedTypeToLogicalTypeOnly(file->getPath());

    auto declared = ROW({"c0"}, {INTEGER()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected = makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }

  {
    auto data =
        makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3}, DATE())});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});
    rewriteDateConvertedTypeToLogicalTypeOnly(file->getPath());

    auto declared = ROW({"c0"}, {BIGINT()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    BOLT_ASSERT_THROW(
        AssertQueryBuilder(plan)
            .split(makeSplit(file->getPath()))
            .copyResults(pool()),
        "From Kind: INT32(DATE), To Kind: BIGINT");
  }

  // DATE annotation read as VARCHAR: match Spark's INT32 physical read path
  // and stringify the raw epoch-day integer, not the formatted DATE value.
  {
    auto data =
        makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3}, DATE())});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {VARCHAR()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected =
        makeRowVector({"c0"}, {makeFlatVector<StringView>({"1", "2", "3"})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }

  // 3a/3b. INT32 narrowing: file INT32 read back as TINYINT and SMALLINT.
  //        Models HIVE-14294 / SPARK-16632 where Hive 1.x writes TINYINT/
  //        SMALLINT as unannotated INT32. The matching matrix cases only
  //        assert no-throw; these check the data path actually narrows
  //        correctly instead of returning garbage. IntegerColumnReader
  //        does the silent truncation, matching Spark / Trino / parquet-mr.
  {
    auto data = makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3})});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    {
      auto declared = ROW({"c0"}, {TINYINT()});
      auto plan =
          PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
      auto result = AssertQueryBuilder(plan)
                        .split(makeSplit(file->getPath()))
                        .copyResults(pool());
      auto expected =
          makeRowVector({"c0"}, {makeFlatVector<int8_t>({1, 2, 3})});
      EXPECT_TRUE(assertEqualResults({expected}, {result}));
    }
    {
      auto declared = ROW({"c0"}, {SMALLINT()});
      auto plan =
          PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
      auto result = AssertQueryBuilder(plan)
                        .split(makeSplit(file->getPath()))
                        .copyResults(pool());
      auto expected =
          makeRowVector({"c0"}, {makeFlatVector<int16_t>({1, 2, 3})});
      EXPECT_TRUE(assertEqualResults({expected}, {result}));
    }
  }

  // 4. Auto-cast INTEGER -> VARCHAR: [1,2,3] -> ["1","2","3"].
  //    Works in both build flavours: matchType() lets INT fileType fall
  //    through, and IntegerColumnReader::makeCastExpr handles the cast.
  {
    auto data = makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3})});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {VARCHAR()});
    auto plan =
        PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected =
        makeRowVector({"c0"}, {makeFlatVector<StringView>({"1", "2", "3"})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }
}

TEST_F(ParquetTableScanTest, floatingPointToVarcharValueFilters) {
  // Floating point -> VARCHAR follows Spark's vectorized Parquet reader
  // conversion. Value filters cannot run against the physical floating-point
  // values; residual predicates run after conversion.
  auto data = makeRowVector(
      {"c0"},
      {makeNullableFlatVector<double>(
          {1.25,
           2.5,
           0.00012,
           std::numeric_limits<double>::quiet_NaN(),
           std::nullopt})});
  auto file = exec::test::TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, WriterOptions{});

  auto declared = ROW({"c0"}, {VARCHAR()});
  auto plan =
      PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
  auto result = AssertQueryBuilder(plan)
                    .split(makeSplit(file->getPath()))
                    .copyResults(pool());
  auto expected = makeRowVector(
      {"c0"},
      {makeNullableFlatVector<StringView>(
          {"1.25", "2.5", "1.2E-4", "NaN", std::nullopt})});
  EXPECT_TRUE(assertEqualResults({expected}, {result}));

  // A logical VARCHAR value filter cannot be evaluated against physical
  // DOUBLE values.
  auto pushdownOnlyPlan =
      PlanBuilder(pool())
          .tableScan(declared, {"c0 = '1.25'"}, "", declared)
          .planNode();
  BOLT_ASSERT_THROW(
      AssertQueryBuilder(pushdownOnlyPlan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply VARCHAR filter to physical DOUBLE Parquet column c0");

  auto extractedFilterPlan =
      PlanBuilder(pool())
          .tableScan(declared, {}, "c0 = '1.25'", declared)
          .planNode();
  BOLT_ASSERT_THROW(
      AssertQueryBuilder(extractedFilterPlan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply VARCHAR filter to physical DOUBLE Parquet column c0");

  auto isNullPlan = PlanBuilder(pool())
                        .tableScan(declared, {"c0 IS NULL"}, "", declared)
                        .planNode();
  auto isNullResult = AssertQueryBuilder(isNullPlan)
                          .split(makeSplit(file->getPath()))
                          .copyResults(pool());
  auto nullExpected = makeRowVector(
      {"c0"}, {makeNullableFlatVector<StringView>({std::nullopt})});
  EXPECT_TRUE(assertEqualResults({nullExpected}, {isNullResult}));

  // Merging mutually exclusive VARCHAR filters produces an AlwaysFalse
  // filter, which is safe to evaluate against the physical DOUBLE values.
  auto alwaysFalsePlan =
      PlanBuilder(pool())
          .tableScan(declared, {"c0 < '2'"}, "c0 >= '2'", declared)
          .planNode();
  auto alwaysFalseResult = AssertQueryBuilder(alwaysFalsePlan)
                               .split(makeSplit(file->getPath()))
                               .copyResults(pool());
  EXPECT_EQ(alwaysFalseResult->size(), 0);
}

#ifdef SPARK_COMPATIBLE
TEST_F(ParquetTableScanTest, doubleToBigintValueChecks) {
  auto data = makeRowVector(
      {"c0"},
      {makeNullableFlatVector<double>({1.0, 1.2, -1.8, 0.0, std::nullopt})});
  auto file = exec::test::TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, WriterOptions{});

  auto declared = ROW({"c0"}, {BIGINT()});
  auto plan =
      PlanBuilder(pool()).tableScan(declared, {}, "", declared).planNode();
  auto result = AssertQueryBuilder(plan)
                    .split(makeSplit(file->getPath()))
                    .copyResults(pool());
  auto expected = makeRowVector(
      {"c0"}, {makeNullableFlatVector<int64_t>({1, 1, -1, 0, std::nullopt})});
  EXPECT_TRUE(assertEqualResults({expected}, {result}));

  auto isNullPlan = PlanBuilder(pool())
                        .tableScan(declared, {"c0 IS NULL"}, "", declared)
                        .planNode();
  auto isNullResult = AssertQueryBuilder(isNullPlan)
                          .split(makeSplit(file->getPath()))
                          .copyResults(pool());
  auto nullExpected =
      makeRowVector({"c0"}, {makeNullableFlatVector<int64_t>({std::nullopt})});
  EXPECT_TRUE(assertEqualResults({nullExpected}, {isNullResult}));

  auto pushdownOnlyPlan = PlanBuilder(pool())
                              .tableScan(declared, {"c0 = 1"}, "", declared)
                              .planNode();
  BOLT_ASSERT_THROW(
      AssertQueryBuilder(pushdownOnlyPlan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply BIGINT filter to physical DOUBLE Parquet column c0");
}
#endif

TEST_F(ParquetTableScanTest, integerToDoubleValueFilters) {
  auto declared = ROW({"c0"}, {DOUBLE()});
  auto test = [&](const RowVectorPtr& data, const std::string& physicalType) {
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto pushdownOnlyPlan = PlanBuilder(pool())
                                .tableScan(declared, {"c0 > 1.0"}, "", declared)
                                .planNode();
    BOLT_ASSERT_THROW(
        AssertQueryBuilder(pushdownOnlyPlan)
            .split(makeSplit(file->getPath()))
            .copyResults(pool()),
        fmt::format(
            "Cannot apply DOUBLE filter to physical {} Parquet column c0",
            physicalType));
  };

  test(
      makeRowVector(
          {"c0"}, {makeNullableFlatVector<int8_t>({1, 2, std::nullopt})}),
      "TINYINT");
  test(
      makeRowVector(
          {"c0"}, {makeNullableFlatVector<int16_t>({1, 2, std::nullopt})}),
      "SMALLINT");

  auto data = makeRowVector(
      {"c0"}, {makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt})});
  test(data, "INTEGER");

  auto file = exec::test::TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, WriterOptions{});
  auto extractedFilterPlan = PlanBuilder(pool())
                                 .tableScan(declared, {}, "c0 > 1.0", declared)
                                 .planNode();
  BOLT_ASSERT_THROW(
      AssertQueryBuilder(extractedFilterPlan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply DOUBLE filter to physical INTEGER Parquet column c0");

  auto isNullPlan = PlanBuilder(pool())
                        .tableScan(declared, {"c0 IS NULL"}, "", declared)
                        .planNode();
  auto isNullResult = AssertQueryBuilder(isNullPlan)
                          .split(makeSplit(file->getPath()))
                          .copyResults(pool());
  auto expected =
      makeRowVector({"c0"}, {makeNullableFlatVector<double>({std::nullopt})});
  EXPECT_TRUE(assertEqualResults({expected}, {isNullResult}));
}

TEST_F(ParquetTableScanTest, dateToVarcharValueFilters) {
  auto data = makeRowVector(
      {"c0"},
      {makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt}, DATE())});
  auto file = exec::test::TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, WriterOptions{});

  auto declared = ROW({"c0"}, {VARCHAR()});
  auto pushdownOnlyPlan = PlanBuilder(pool())
                              .tableScan(declared, {"c0 = '1'"}, "", declared)
                              .planNode();
  BOLT_ASSERT_THROW(
      AssertQueryBuilder(pushdownOnlyPlan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply VARCHAR filter to physical INTEGER Parquet column c0");

  auto extractedFilterPlan = PlanBuilder(pool())
                                 .tableScan(declared, {}, "c0 = '1'", declared)
                                 .planNode();
  BOLT_ASSERT_THROW(
      AssertQueryBuilder(extractedFilterPlan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply VARCHAR filter to physical INTEGER Parquet column c0");

  auto isNullPlan = PlanBuilder(pool())
                        .tableScan(declared, {"c0 IS NULL"}, "", declared)
                        .planNode();
  auto isNullResult = AssertQueryBuilder(isNullPlan)
                          .split(makeSplit(file->getPath()))
                          .copyResults(pool());
  auto expected = makeRowVector(
      {"c0"}, {makeNullableFlatVector<StringView>({std::nullopt})});
  EXPECT_TRUE(assertEqualResults({expected}, {isNullResult}));
}

TEST_F(ParquetTableScanTest, floatingPointToVarcharFilterOnlyColumn) {
  auto data = makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<double>({1.25, 2.5}), makeFlatVector<int64_t>({10, 20})});
  auto file = exec::test::TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, WriterOptions{});

  auto outputType = ROW({"c1"}, {BIGINT()});
  auto dataColumns = ROW({"c0", "c1"}, {VARCHAR(), BIGINT()});
  auto plan = PlanBuilder(pool())
                  .tableScan(outputType, {"c0 = '1.25'"}, "", dataColumns)
                  .planNode();
  BOLT_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply VARCHAR filter to physical DOUBLE Parquet column c0");
}

TEST_F(ParquetTableScanTest, integerToDoubleFilterOnlyColumn) {
  auto data = makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<int32_t>({1, 2}), makeFlatVector<int64_t>({10, 20})});
  auto file = exec::test::TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, WriterOptions{});

  auto outputType = ROW({"c1"}, {BIGINT()});
  auto dataColumns = ROW({"c0", "c1"}, {DOUBLE(), BIGINT()});
  auto plan = PlanBuilder(pool())
                  .tableScan(outputType, {"c0 > 1.0"}, "", dataColumns)
                  .planNode();
  BOLT_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply DOUBLE filter to physical INTEGER Parquet column c0");
}

TEST_F(ParquetTableScanTest, floatingPointToVarcharMapValueFilter) {
  auto data = makeRowVector(
      {"c0"},
      {makeMapVector<StringView, double>({{{"key", 1.25}}, {{"key", 2.5}}})});
  auto file = exec::test::TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, WriterOptions{});

  auto declared = ROW({"c0"}, {MAP(VARCHAR(), VARCHAR())});
  auto filters =
      common::test::SubfieldFiltersBuilder()
          .add(
              "c0",
              common::createMapSubscriptFilter(
                  "key",
                  common::createBytesRange("1.25", true, "1.25", true, false),
                  common::createBytesRange("key", true, "key", true, false)))
          .build();
  auto tableHandle =
      makeTableHandle(std::move(filters), nullptr, "hive_table", declared);
  auto plan = PlanBuilder(pool())
                  .tableScan(declared, tableHandle, allRegularColumns(declared))
                  .planNode();

  BOLT_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply VARCHAR filter to physical DOUBLE Parquet column c0.values");
}

TEST_F(ParquetTableScanTest, integerToDoubleMapValueFilter) {
  auto data = makeRowVector(
      {"c0"},
      {makeMapVector<StringView, int32_t>({{{"key", 1}}, {{"key", 2}}})});
  auto file = exec::test::TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, WriterOptions{});

  auto declared = ROW({"c0"}, {MAP(VARCHAR(), DOUBLE())});
  auto filters =
      common::test::SubfieldFiltersBuilder()
          .add(
              "c0",
              common::createMapSubscriptFilter(
                  "key",
                  std::make_unique<common::DoubleRange>(
                      1.0, false, false, 1.0, false, false, false),
                  common::createBytesRange("key", true, "key", true, false)))
          .build();
  auto tableHandle =
      makeTableHandle(std::move(filters), nullptr, "hive_table", declared);
  auto plan = PlanBuilder(pool())
                  .tableScan(declared, tableHandle, allRegularColumns(declared))
                  .planNode();

  BOLT_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply DOUBLE filter to physical INTEGER Parquet column c0.values");
}

TEST_F(ParquetTableScanTest, dateToVarcharMapValueFilter) {
  auto map = makeMapVector<StringView, int32_t>(
      {{{"key", 1}}, {{"key", 2}}}, MAP(VARCHAR(), DATE()));
  auto data = makeRowVector({"c0"}, {map});
  auto file = exec::test::TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, WriterOptions{});

  auto declared = ROW({"c0"}, {MAP(VARCHAR(), VARCHAR())});
  auto filters =
      common::test::SubfieldFiltersBuilder()
          .add(
              "c0",
              common::createMapSubscriptFilter(
                  "key",
                  common::createBytesRange("1", true, "1", true, false),
                  common::createBytesRange("key", true, "key", true, false)))
          .build();
  auto tableHandle =
      makeTableHandle(std::move(filters), nullptr, "hive_table", declared);
  auto plan = PlanBuilder(pool())
                  .tableScan(declared, tableHandle, allRegularColumns(declared))
                  .planNode();

  BOLT_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .split(makeSplit(file->getPath()))
          .copyResults(pool()),
      "Cannot apply VARCHAR filter to physical INTEGER Parquet column c0.values");
}

TEST_F(ParquetTableScanTest, integerReaderCastMapMetadataFilters) {
  auto test = [&](const RowVectorPtr& data,
                  const RowTypePtr& declared,
                  const std::string& filter,
                  std::unique_ptr<common::Filter> valueFilter,
                  const RowVectorPtr& expected) {
    auto file = exec::test::TempFilePath::create();
    WriterOptions writerOptions;
    writerOptions.flushPolicyFactory = [] {
      return std::make_unique<DefaultFlushPolicy>(
          2, std::numeric_limits<int64_t>::max());
    };
    writeToParquetFile(file->getPath(), {data}, writerOptions);

    ScopedExprToSubfieldFilterParser parser(
        std::make_shared<MapSubscriptMetadataFilterParser>(
            std::move(valueFilter)));
    core::PlanNodeId scanNodeId;
    auto plan =
        PlanBuilder(pool())
            .tableScan("hive_table", declared, {}, {}, filter, declared, false)
            .capturePlanNodeId(scanNodeId)
            .planNode();
    std::shared_ptr<exec::Task> task;
    auto result =
        AssertQueryBuilder(plan)
            .config(core::QueryConfig::kMapSubscriptFilterPushdown, "true")
            .split(makeSplit(file->getPath()))
            .copyResults(pool(), task);
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
    EXPECT_EQ(
        exec::toPlanStats(task->taskStats())
            .at(scanNodeId)
            .customStats.at("skippedStrides")
            .sum,
        1);
  };

  test(
      makeRowVector(
          {"c0", "c1"},
          {makeMapVector<StringView, int32_t>(
               {{{"key", 1}}, {{"key", 2}}, {{"key", 1}}, {{"key", 2}}}),
           makeFlatVector<int64_t>({0, 0, 100, 100})}),
      ROW({"c0", "c1"}, {MAP(VARCHAR(), DOUBLE()), BIGINT()}),
      "element_at(c0, 'key') = 1.0 AND c1 >= 100",
      std::make_unique<common::DoubleRange>(
          1.0, false, false, 1.0, false, false, false),
      makeRowVector(
          {"c0", "c1"},
          {makeMapVector<StringView, double>({{{"key", 1.0}}}),
           makeFlatVector<int64_t>({100})}));

  test(
      makeRowVector(
          {"c0", "c1"},
          {makeMapVector<StringView, int32_t>(
               {{{"key", 1}}, {{"key", 2}}, {{"key", 1}}, {{"key", 2}}},
               MAP(VARCHAR(), DATE())),
           makeFlatVector<int64_t>({0, 0, 100, 100})}),
      ROW({"c0", "c1"}, {MAP(VARCHAR(), VARCHAR()), BIGINT()}),
      "element_at(c0, 'key') = '1' AND c1 >= 100",
      common::createBytesRange("1", true, "1", true, false),
      makeRowVector(
          {"c0", "c1"},
          {makeMapVector<StringView, StringView>({{{"key", "1"}}}),
           makeFlatVector<int64_t>({100})}));
}

TEST_F(ParquetTableScanTest, floatingPointToVarcharMapMetadataFilter) {
  auto data = makeRowVector(
      {"c0", "c1"},
      {makeMapVector<StringView, double>(
           {{{"key", 1.25}}, {{"key", 2.5}}, {{"key", 1.25}}, {{"key", 2.5}}}),
       makeFlatVector<int64_t>({0, 0, 100, 100})});
  auto file = exec::test::TempFilePath::create();
  WriterOptions writerOptions;
  writerOptions.flushPolicyFactory = [] {
    return std::make_unique<DefaultFlushPolicy>(
        2, std::numeric_limits<int64_t>::max());
  };
  writeToParquetFile(file->getPath(), {data}, writerOptions);

  ScopedExprToSubfieldFilterParser parser(
      std::make_shared<MapSubscriptMetadataFilterParser>());
  auto declared = ROW({"c0", "c1"}, {MAP(VARCHAR(), VARCHAR()), BIGINT()});
  core::PlanNodeId scanNodeId;
  auto plan = PlanBuilder(pool())
                  .tableScan(
                      "hive_table",
                      declared,
                      {},
                      {},
                      "element_at(c0, 'key') = '1.25' AND c1 >= 100",
                      declared,
                      false)
                  .capturePlanNodeId(scanNodeId)
                  .planNode();
  std::shared_ptr<exec::Task> task;
  auto result =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kMapSubscriptFilterPushdown, "true")
          .split(makeSplit(file->getPath()))
          .copyResults(pool(), task);
  auto expected = makeRowVector(
      {"c0", "c1"},
      {makeMapVector<StringView, StringView>({{{"key", "1.25"}}}),
       makeFlatVector<int64_t>({100})});
  EXPECT_TRUE(assertEqualResults({expected}, {result}));
  EXPECT_EQ(
      exec::toPlanStats(task->taskStats())
          .at(scanNodeId)
          .customStats.at("skippedStrides")
          .sum,
      1);
}

#ifdef SPARK_COMPATIBLE
TEST_F(ParquetTableScanTest, doubleToBigintMapMetadataFilter) {
  auto data = makeRowVector(
      {"c0", "c1"},
      {makeMapVector<StringView, double>(
           {{{"key", 1.2}}, {{"key", 2.5}}, {{"key", 1.8}}, {{"key", 2.5}}}),
       makeFlatVector<int64_t>({0, 0, 100, 100})});
  auto file = exec::test::TempFilePath::create();
  WriterOptions writerOptions;
  writerOptions.flushPolicyFactory = [] {
    return std::make_unique<DefaultFlushPolicy>(
        2, std::numeric_limits<int64_t>::max());
  };
  writeToParquetFile(file->getPath(), {data}, writerOptions);

  ScopedExprToSubfieldFilterParser parser(
      std::make_shared<MapSubscriptMetadataFilterParser>(
          common::createBigintRange(1, 1, false, false)));
  auto declared = ROW({"c0", "c1"}, {MAP(VARCHAR(), BIGINT()), BIGINT()});
  core::PlanNodeId scanNodeId;
  auto plan = PlanBuilder(pool())
                  .tableScan(
                      "hive_table",
                      declared,
                      {},
                      {},
                      "element_at(c0, 'key') = 1 AND c1 >= 100",
                      declared,
                      false)
                  .capturePlanNodeId(scanNodeId)
                  .planNode();
  std::shared_ptr<exec::Task> task;
  auto result =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kMapSubscriptFilterPushdown, "true")
          .split(makeSplit(file->getPath()))
          .copyResults(pool(), task);
  auto expected = makeRowVector(
      {"c0", "c1"},
      {makeMapVector<StringView, int64_t>({{{"key", 1}}}),
       makeFlatVector<int64_t>({100})});
  EXPECT_TRUE(assertEqualResults({expected}, {result}));
  EXPECT_EQ(
      exec::toPlanStats(task->taskStats())
          .at(scanNodeId)
          .customStats.at("skippedStrides")
          .sum,
      1);
}
#endif

TEST_F(ParquetTableScanTest, integerReaderCastMetadataFilters) {
  auto test = [&](const RowVectorPtr& data,
                  const RowTypePtr& declared,
                  const std::string& filter,
                  const RowVectorPtr& expected) {
    auto file = exec::test::TempFilePath::create();
    WriterOptions writerOptions;
    writerOptions.flushPolicyFactory = [] {
      return std::make_unique<DefaultFlushPolicy>(
          2, std::numeric_limits<int64_t>::max());
    };
    writeToParquetFile(file->getPath(), {data}, writerOptions);

    core::PlanNodeId scanNodeId;
    auto plan =
        PlanBuilder(pool())
            .tableScan("hive_table", declared, {}, {}, filter, declared, false)
            .capturePlanNodeId(scanNodeId)
            .planNode();
    std::shared_ptr<exec::Task> task;
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool(), task);
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
    EXPECT_EQ(
        exec::toPlanStats(task->taskStats())
            .at(scanNodeId)
            .customStats.at("skippedStrides")
            .sum,
        1);
  };

  test(
      makeRowVector(
          {"c0", "c1"},
          {makeFlatVector<int32_t>({1, 2, 1, 2}),
           makeFlatVector<int64_t>({0, 0, 100, 100})}),
      ROW({"c0", "c1"}, {DOUBLE(), BIGINT()}),
      "c0 = 1.0 AND c1 >= 100",
      makeRowVector(
          {"c0", "c1"},
          {makeFlatVector<double>({1.0}), makeFlatVector<int64_t>({100})}));

  test(
      makeRowVector(
          {"c0", "c1"},
          {makeFlatVector<int32_t>({1, 2, 1, 2}, DATE()),
           makeFlatVector<int64_t>({0, 0, 100, 100})}),
      ROW({"c0", "c1"}, {VARCHAR(), BIGINT()}),
      "c0 = '1' AND c1 >= 100",
      makeRowVector(
          {"c0", "c1"},
          {makeFlatVector<StringView>({"1"}), makeFlatVector<int64_t>({100})}));
}

TEST_F(ParquetTableScanTest, floatingPointToVarcharMetadataFilter) {
  // Without filter extraction, the predicate remains a residual expression.
  // The physical DOUBLE statistics are present, but its metadata filter must
  // be ignored and the predicate evaluated after conversion.
  {
    auto data = makeRowVector({"c0"}, {makeFlatVector<double>({1.25, 2.5})});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {VARCHAR()});
    auto plan =
        PlanBuilder(pool())
            .tableScan(
                "hive_table", declared, {}, {}, "c0 = '1.25'", declared, false)
            .planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected =
        makeRowVector({"c0"}, {makeFlatVector<StringView>({"1.25"})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }

  {
    auto data =
        makeRowVector({"c0"}, {makeFlatVector<float>({1.25F, 0.00012F})});
    auto file = exec::test::TempFilePath::create();
    writeToParquetFile(file->getPath(), {data}, WriterOptions{});

    auto declared = ROW({"c0"}, {VARCHAR()});
    auto plan = PlanBuilder(pool())
                    .tableScan(
                        "hive_table",
                        declared,
                        {},
                        {},
                        "c0 = '1.2E-4'",
                        declared,
                        false)
                    .planNode();
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool());
    auto expected =
        makeRowVector({"c0"}, {makeFlatVector<StringView>({"1.2E-4"})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
  }

  // A type mismatch on one metadata-filter leaf must not disable row-group
  // pruning for compatible leaves in the same AND expression.
  {
    auto data = makeRowVector(
        {"c0", "c1"},
        {makeFlatVector<double>({1.25, 2.5, 1.25, 2.5}),
         makeFlatVector<int64_t>({0, 0, 100, 100})});
    auto file = exec::test::TempFilePath::create();
    WriterOptions writerOptions;
    writerOptions.flushPolicyFactory = [] {
      return std::make_unique<DefaultFlushPolicy>(
          2, std::numeric_limits<int64_t>::max());
    };
    writeToParquetFile(file->getPath(), {data}, writerOptions);

    auto declared = ROW({"c0", "c1"}, {VARCHAR(), BIGINT()});
    core::PlanNodeId scanNodeId;
    auto plan = PlanBuilder(pool())
                    .tableScan(
                        "hive_table",
                        declared,
                        {},
                        {},
                        "c0 = '1.25' AND c1 >= 100",
                        declared,
                        false)
                    .capturePlanNodeId(scanNodeId)
                    .planNode();
    std::shared_ptr<exec::Task> task;
    auto result = AssertQueryBuilder(plan)
                      .split(makeSplit(file->getPath()))
                      .copyResults(pool(), task);
    auto expected = makeRowVector(
        {"c0", "c1"},
        {makeFlatVector<StringView>({"1.25"}), makeFlatVector<int64_t>({100})});
    EXPECT_TRUE(assertEqualResults({expected}, {result}));
    EXPECT_EQ(
        exec::toPlanStats(task->taskStats())
            .at(scanNodeId)
            .customStats.at("skippedStrides")
            .sum,
        1);
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
