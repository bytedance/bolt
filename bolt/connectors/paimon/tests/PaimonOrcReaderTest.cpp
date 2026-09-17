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

#include "bolt/connectors/paimon/PaimonOrcReader.h"
#include <arrow/adapters/orc/adapter.h>
#include <arrow/c/bridge.h>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <folly/Subprocess.h>
#include <gtest/gtest.h>
#include <paimon/format/file_format_factory.h>
#include <paimon/predicate/literal.h>
#include <paimon/predicate/predicate_builder.h>
#include <cstdlib>
#include <fstream>
#include <limits>
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/connectors/paimon/PaimonBoltFileSystem.h"
#include "bolt/connectors/paimon/PaimonConnector.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::paimon;

namespace {
// Count consumption while retaining the real exported schema's release
// callback.
class SchemaReleaseCounter {
 public:
  explicit SchemaReleaseCounter(ArrowSchema& schema)
      : schema_(schema),
        release_(schema.release),
        privateData_(schema.private_data) {
    schema_.private_data = this;
    schema_.release = [](ArrowSchema* value) {
      auto* counter = static_cast<SchemaReleaseCounter*>(value->private_data);
      ++counter->calls;
      value->private_data = counter->privateData_;
      value->release = counter->release_;
      value->release(value);
    };
  }

  ~SchemaReleaseCounter() {
    // Clean up after a failed expectation without hiding the unconsumed schema.
    if (schema_.release) {
      schema_.release(&schema_);
    }
  }

  int calls{0};

 private:
  ArrowSchema& schema_;
  void (*release_)(ArrowSchema*);
  void* privateData_;
};

class PaimonOrcReaderTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
    filesystems::registerLocalFileSystem();
  }

  void SetUp() override {
    PaimonConnectorFactory factory;
    directory_ = exec::test::TempDirectoryPath::create();
  }

  std::string write(const RowVectorPtr& data) {
    auto path = directory_->getPath() + "/data.orc";
    auto output = ::arrow::io::FileOutputStream::Open(path).ValueOrDie();
    auto writer = ::arrow::adapters::orc::ORCFileWriter::Open(output.get(), {})
                      .ValueOrDie();
    ArrowArray array{};
    ArrowSchema schema{};
    ArrowOptions options;
    options.timestampUnit = TimestampUnit::kMicro;
    exportToArrow(data, array, pool(), options);
    exportToArrow(data, schema, options);
    auto batch = ::arrow::ImportRecordBatch(&array, &schema).ValueOrDie();
    auto table = ::arrow::Table::FromRecordBatches({batch}).ValueOrDie();
    BOLT_CHECK(writer->Write(*table).ok());
    BOLT_CHECK(writer->Close().ok());
    BOLT_CHECK(output->Close().ok());
    return path;
  }

  RowVectorPtr data() {
    return makeRowVector(
        {"id", "text"},
        {makeFlatVector<int32_t>(12, [](auto i) { return i; }),
         makeNullableFlatVector<std::string>(
             {"zero",
              "one",
              std::nullopt,
              "three",
              "four",
              "five",
              "six",
              "seven",
              "eight",
              "nine",
              "ten",
              "eleven"})});
  }

  std::unique_ptr<::paimon::FileBatchReader> open(
      const std::string& path,
      int32_t batchSize = 3,
      const std::map<std::string, std::string>& options = {}) {
    auto format = ::paimon::FileFormatFactory::Get("orc", options);
    BOLT_CHECK(format.ok(), "{}", format.status().ToString());
    auto builder = format.value()->CreateReaderBuilder(batchSize);
    BOLT_CHECK(builder.ok(), "{}", builder.status().ToString());
    builder.value()->WithMemoryPool(
        std::make_shared<BoltPaimonMemoryPool>(pool()));
    PaimonBoltFileSystem fs(options);
    auto stream = fs.Open(path);
    BOLT_CHECK(stream.ok(), "{}", stream.status().ToString());
    auto reader = builder.value()->Build(
        std::shared_ptr<::paimon::InputStream>(std::move(stream).value()));
    BOLT_CHECK(reader.ok(), "{}", reader.status().ToString());
    return std::move(reader).value();
  }

  ::paimon::Status reset(
      ::paimon::FileBatchReader& reader,
      const TypePtr& type,
      std::shared_ptr<::paimon::Predicate> predicate = nullptr,
      const std::optional<::paimon::RoaringBitmap32>& bitmap = std::nullopt) {
    ArrowSchema schema{};
    exportToArrow(BaseVector::create(type, 0, pool()), schema, {});
    auto status = reader.SetReadSchema(&schema, predicate, bitmap);
    EXPECT_EQ(schema.release, nullptr);
    if (schema.release) {
      schema.release(&schema);
    }
    return status;
  }

  void expectTimestampReadRejected(::paimon::FileBatchReader& reader) {
    auto result = reader.NextBatch();
    if (result.ok()) {
      auto batch = std::move(result).value();
      if (!::paimon::BatchReader::IsEofBatch(batch)) {
        auto owned =
            importFromArrowAsOwner(*batch.second, *batch.first, {}, pool());
      }
      ADD_FAILURE() << "ORC timestamp read was accepted";
      return;
    }
    EXPECT_TRUE(result.status().IsNotImplemented())
        << result.status().ToString();
    EXPECT_NE(result.status().ToString().find("timestamp"), std::string::npos);
  }

  RowVectorPtr next(::paimon::FileBatchReader& reader) {
    auto result = reader.NextBatch();
    BOLT_CHECK(result.ok(), "{}", result.status().ToString());
    auto batch = std::move(result).value();
    if (::paimon::BatchReader::IsEofBatch(batch)) {
      EXPECT_TRUE(reader.GetPreviousBatchFileRowId(0).status().IsInvalid());
      return nullptr;
    }
    auto vector =
        importFromArrowAsOwner(*batch.second, *batch.first, {}, pool());
    return std::dynamic_pointer_cast<RowVector>(vector);
  }

  RowVectorPtr readAll(::paimon::FileBatchReader& reader, const TypePtr& type) {
    auto all = RowVector::createEmpty(type, pool());
    while (auto batch = next(reader)) {
      BOLT_CHECK_GT(batch->size(), 0);
      all->append(batch.get());
    }
    return all;
  }

  std::shared_ptr<exec::test::TempDirectoryPath> directory_;
};

TEST_F(PaimonOrcReaderTest, FactoryRegistration) {
  auto format = ::paimon::FileFormatFactory::Get("orc", {});
  ASSERT_TRUE(format.ok()) << format.status().ToString();
  EXPECT_EQ(format.value()->Identifier(), "orc");
  EXPECT_NE(dynamic_cast<PaimonOrcReader*>(format.value().get()), nullptr);
  EXPECT_FALSE(format.value()->CreateWriterBuilder(nullptr, 3).ok());
  EXPECT_FALSE(format.value()->CreateStatsExtractor(nullptr).ok());
}

TEST_F(PaimonOrcReaderTest, InputStreamPathAndFileUri) {
  auto expected = data();
  auto path = write(expected);
  for (const auto& uri : {path, "file:" + path, "file://" + path}) {
    auto reader = open(uri);
    EXPECT_EQ(reader->GetNumberOfRows().value(), 12);
    auto schema = reader->GetFileSchema();
    ASSERT_TRUE(schema.ok());
    EXPECT_EQ(*importFromArrow(*schema.value()), *expected->type());
    schema.value()->release(schema.value().get());
    test::assertEqualVectors(expected, readAll(*reader, expected->type()));
    EXPECT_EQ(next(*reader), nullptr);
    EXPECT_EQ(next(*reader), nullptr);
    EXPECT_EQ(reader->GetReaderMetrics(), nullptr);
  }
}

TEST_F(PaimonOrcReaderTest, PhysicalRowIdsAndBatchState) {
  auto expected = data();
  auto reader = open(write(expected), 5);
  EXPECT_TRUE(reader->GetPreviousBatchFileRowId(0).status().IsInvalid());
  for (int firstRow : {0, 5, 10}) {
    auto batch = next(*reader);
    ASSERT_NE(batch, nullptr);
    for (uint64_t row = 0; row < batch->size(); ++row) {
      auto fileRowId = reader->GetPreviousBatchFileRowId(row);
      ASSERT_TRUE(fileRowId.ok()) << fileRowId.status().ToString();
      EXPECT_EQ(fileRowId.value(), firstRow + row);
    }
    EXPECT_TRUE(
        reader->GetPreviousBatchFileRowId(batch->size()).status().IsInvalid());
    EXPECT_TRUE(
        reader->GetPreviousBatchFileRowId(std::numeric_limits<uint64_t>::max())
            .status()
            .IsInvalid());
    test::assertEqualVectors(expected->slice(firstRow, batch->size()), batch);
  }
  EXPECT_EQ(next(*reader), nullptr);
  EXPECT_EQ(next(*reader), nullptr);
  ASSERT_TRUE(reset(*reader, expected->type()).ok());
  EXPECT_TRUE(reader->GetPreviousBatchFileRowId(0).status().IsInvalid());
  test::assertEqualVectors(expected->slice(0, 5), next(*reader));
  EXPECT_EQ(reader->GetPreviousBatchFileRowId(4).value(), 4);
  reader->Close();
  EXPECT_TRUE(reader->GetPreviousBatchFileRowId(0).status().IsInvalid());
}

TEST_F(PaimonOrcReaderTest, AllNullColumnsExportPrimitiveArrowArrays) {
  auto expected = makeRowVector(
      {"id", "age", "name"},
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeNullableFlatVector<int32_t>(
           {std::nullopt, std::nullopt, std::nullopt}),
       makeNullableFlatVector<StringView>(
           {std::nullopt, std::nullopt, std::nullopt})});
  auto reader = open(write(expected));
  auto result = reader->NextBatch();
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  auto batch = std::move(result).value();
  EXPECT_STREQ(batch.second->children[1]->format, "i");
  EXPECT_STREQ(batch.second->children[2]->format, "u");
  if (HasFailure()) {
    batch.first->release(batch.first.get());
    batch.second->release(batch.second.get());
    return;
  }
  auto actual = importFromArrowAsOwner(*batch.second, *batch.first, {}, pool());
  test::assertEqualVectors(expected, actual);
}

TEST_F(PaimonOrcReaderTest, RetainedBatchesResetAndClose) {
  auto expected = data();
  auto reader = open(write(expected));
  EXPECT_TRUE(reader->GetPreviousBatchFileRowId(0).status().IsInvalid());
  // Retain Arrow exports themselves across further reads, reset and close.
  std::vector<::paimon::BatchReader::ReadBatch> retained;
  for (int i = 0; i < 4; ++i) {
    auto result = reader->NextBatch();
    ASSERT_TRUE(result.ok());
    retained.push_back(std::move(result).value());
    EXPECT_EQ(reader->GetPreviousBatchFileRowId(0).value(), i * 3);
  }
  ASSERT_TRUE(reset(*reader, expected->type()).ok());
  EXPECT_TRUE(reader->GetPreviousBatchFileRowId(0).status().IsInvalid());
  auto first = next(*reader);
  reader->Close();
  reader->Close();
  EXPECT_FALSE(reader->NextBatch().ok());
  EXPECT_TRUE(reader->GetPreviousBatchFileRowId(0).status().IsInvalid());
  EXPECT_FALSE(reader->GetFileSchema().ok());
  EXPECT_FALSE(reader->GetNumberOfRows().ok());
  EXPECT_FALSE(reset(*reader, expected->type()).ok());
  auto actual = RowVector::createEmpty(expected->type(), pool());
  for (auto& batch : retained) {
    auto vector =
        importFromArrowAsOwner(*batch.second, *batch.first, {}, pool());
    actual->append(vector->as<RowVector>());
  }
  test::assertEqualVectors(expected, actual);
  test::assertEqualVectors(expected->slice(0, 3), first);
}

TEST_F(PaimonOrcReaderTest, ProjectionResetAndZeroColumns) {
  auto expected = data();
  auto reader = open(write(expected));
  auto reversed = ROW({"text", "id"}, {VARCHAR(), INTEGER()});
  ASSERT_TRUE(reset(*reader, reversed).ok());
  auto projected = makeRowVector(
      {"text", "id"}, {expected->childAt(1), expected->childAt(0)});
  test::assertEqualVectors(projected, readAll(*reader, reversed));
  ASSERT_TRUE(reset(*reader, ROW({}, {})).ok());
  auto empty = readAll(*reader, ROW({}, {}));
  EXPECT_EQ(empty->size(), 12);
  EXPECT_EQ(empty->childrenSize(), 0);
  ASSERT_TRUE(reset(*reader, expected->type()).ok());
  test::assertEqualVectors(expected, readAll(*reader, expected->type()));
}

TEST_F(PaimonOrcReaderTest, PredicatesPreserveContiguousPhysicalBatches) {
  auto expected = data();
  auto reader = open(write(expected));
  auto predicate = ::paimon::PredicateBuilder::GreaterOrEqual(
      0, "id", ::paimon::FieldType::INT, ::paimon::Literal(int32_t{7}));
  ASSERT_TRUE(reset(*reader, expected->type(), predicate).ok());
  // Paimon's bitmap wrapper maps batch positions to physical file rows.
  // Compacting rows here would apply deletions to the wrong records.
  for (int firstRow : {0, 3, 6, 9}) {
    auto batch = next(*reader);
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(reader->GetPreviousBatchFileRowId(0).value(), firstRow);
    test::assertEqualVectors(expected->slice(firstRow, 3), batch);
  }
  EXPECT_EQ(next(*reader), nullptr);
  ASSERT_TRUE(reset(*reader, expected->type()).ok());
  test::assertEqualVectors(expected, readAll(*reader, expected->type()));
}

TEST_F(PaimonOrcReaderTest, FilterOnlyColumnRetainsCandidates) {
  auto expected = data();
  auto reader = open(write(expected));
  auto type = ROW({"text"}, {VARCHAR()});
  for (const auto& predicate :
       {::paimon::PredicateBuilder::GreaterOrEqual(
            0, "id", ::paimon::FieldType::INT, ::paimon::Literal(int32_t{7})),
        ::paimon::PredicateBuilder::LessThan(
            0,
            "id",
            ::paimon::FieldType::INT,
            ::paimon::Literal(int32_t{0}))}) {
    ASSERT_TRUE(reset(*reader, type, predicate).ok());
    test::assertEqualVectors(
        makeRowVector({"text"}, {expected->childAt(1)}),
        readAll(*reader, type));
    EXPECT_EQ(next(*reader), nullptr);
  }
}

TEST_F(PaimonOrcReaderTest, BitmapFallbackPreservesPhysicalPositionsAndReset) {
  auto expected = data();
  auto reader = open(write(expected));
  auto predicate = ::paimon::PredicateBuilder::GreaterOrEqual(
      0, "id", ::paimon::FieldType::INT, ::paimon::Literal(int32_t{7}));
  ::paimon::RoaringBitmap32 selection;
  for (int row : {1, 4, 7, 8, 10}) {
    selection.Add(row);
  }
  EXPECT_FALSE(reader->SupportPreciseBitmapSelection());
  for (const auto& bitmap : {selection, ::paimon::RoaringBitmap32()}) {
    auto status = reset(*reader, expected->type(), predicate, bitmap);
    ASSERT_TRUE(status.ok()) << status.ToString();
    for (int firstRow : {0, 3, 6, 9}) {
      auto batch = next(*reader);
      ASSERT_NE(batch, nullptr);
      EXPECT_EQ(reader->GetPreviousBatchFileRowId(0).value(), firstRow);
      test::assertEqualVectors(expected->slice(firstRow, 3), batch);
    }
    EXPECT_EQ(next(*reader), nullptr);
  }
  ASSERT_TRUE(reset(*reader, expected->type()).ok());
  test::assertEqualVectors(expected, readAll(*reader, expected->type()));
}

TEST_F(PaimonOrcReaderTest, FailedResetPreservesReadPosition) {
  auto expected = data();
  auto reader = open(write(expected));
  test::assertEqualVectors(expected->slice(0, 3), next(*reader));
  EXPECT_FALSE(reset(*reader, INTEGER()).ok());
  EXPECT_EQ(reader->GetPreviousBatchFileRowId(0).value(), 0);
  test::assertEqualVectors(expected->slice(3, 3), next(*reader));
  EXPECT_EQ(reader->GetPreviousBatchFileRowId(0).value(), 3);
  ASSERT_TRUE(reset(*reader, expected->type()).ok());
  test::assertEqualVectors(expected->slice(0, 3), next(*reader));
}

TEST_F(PaimonOrcReaderTest, ConservativePredicatePushdown) {
  auto expected = data();
  auto reader = open(write(expected));
  // Cross-column OR cannot be represented by independent ScanSpec filters.
  // It must leave all candidates to Paimon's complete residual evaluation.
  auto predicate =
      ::paimon::PredicateBuilder::Or(
          {::paimon::PredicateBuilder::Equal(
               0,
               "id",
               ::paimon::FieldType::INT,
               ::paimon::Literal(int32_t{11})),
           ::paimon::PredicateBuilder::Equal(
               1,
               "text",
               ::paimon::FieldType::STRING,
               ::paimon::Literal(::paimon::FieldType::STRING, "zero", 4))})
          .value();
  ASSERT_TRUE(reset(*reader, expected->type(), predicate).ok());
  test::assertEqualVectors(expected, readAll(*reader, expected->type()));
  // The file adapter also accepts no predicate (predicate filtering disabled).
  ASSERT_TRUE(reset(*reader, expected->type()).ok());
  test::assertEqualVectors(expected, readAll(*reader, expected->type()));
}

TEST_F(PaimonOrcReaderTest, ReadSchemaOwnershipSuccessAndFailures) {
  auto expected = data();
  auto reader = open(write(expected));
  const auto check =
      [&](const TypePtr& type,
          bool success,
          bool malformed = false,
          const std::optional<::paimon::RoaringBitmap32>& bitmap =
              std::nullopt) {
        ArrowSchema schema{};
        exportToArrow(BaseVector::create(type, 0, pool()), schema, {});
        SchemaReleaseCounter counter(schema);
        if (malformed) {
          schema.format = "invalid-arrow-format";
        }
        const auto status = reader->SetReadSchema(&schema, nullptr, bitmap);
        EXPECT_EQ(status.ok(), success) << status.ToString();
        EXPECT_EQ(counter.calls, 1);
        EXPECT_EQ(schema.release, nullptr);
      };
  check(expected->type(), true);
  check(expected->type(), true);
  check(INTEGER(), false);
  check(expected->type(), false, true);
  check(expected->type(), true, false, ::paimon::RoaringBitmap32());
  EXPECT_FALSE(reader->SetReadSchema(nullptr, nullptr, std::nullopt).ok());
  test::assertEqualVectors(expected, readAll(*reader, expected->type()));
  reader->Close();
  check(expected->type(), false);
}

TEST_F(PaimonOrcReaderTest, NullableDecimalAndNestedTypes) {
  auto decimals = makeNullableFlatVector<int128_t>(
      {HugeInt::build(123, 456), -HugeInt::build(123, 456), std::nullopt, 0},
      DECIMAL(38, 18));
  auto expected = makeRowVector(
      {"id", "decimal", "array", "map", "struct"},
      {makeNullableFlatVector<int32_t>({1, std::nullopt, -3, 4}),
       decimals,
       makeArrayVector<int32_t>({{1, 2}, {}, {3}, {4, 5}}),
       makeMapVector<int32_t, int64_t>({{{1, 10}}, {}, {{2, 20}}, {{3, 30}}}),
       makeRowVector(
           {"nested"},
           {makeNullableFlatVector<std::string>(
               {"a", std::nullopt, "c", "d"})})});
  auto reader = open(write(expected), 2);
  test::assertEqualVectors(expected, readAll(*reader, expected->type()));
}

TEST_F(PaimonOrcReaderTest, TimestampProjectionAndFilterOnlyFieldsAreRejected) {
  auto rows = makeRowVector(
      {"id", "ts"},
      {makeFlatVector<int32_t>({1, 2}),
       makeFlatVector<Timestamp>(
           {Timestamp(-1, 999999000), Timestamp(1, 123456000)})});
  const auto path = write(rows);
  const auto projected = ROW({"id"}, {INTEGER()});
  const auto expected = makeRowVector({"id"}, {rows->childAt(0)});
  const auto timestampPredicate = ::paimon::PredicateBuilder::IsNotNull(
      1, "ts", ::paimon::FieldType::TIMESTAMP);
  const auto idPredicate = ::paimon::PredicateBuilder::GreaterOrEqual(
      0, "id", ::paimon::FieldType::INT, ::paimon::Literal(int32_t{1}));
  for (const auto& precision : {"3", "6", "9"}) {
    SCOPED_TRACE(precision);
    auto reader =
        open(path, 2, {{PaimonConfig::kReadTimestampUnit, precision}});
    // A caller may omit SetReadSchema entirely; no timestamp batch may escape.
    expectTimestampReadRejected(*reader);
    auto status = reset(*reader, rows->type());
    EXPECT_TRUE(status.IsNotImplemented()) << status.ToString();
    status = reset(*reader, projected, timestampPredicate);
    EXPECT_TRUE(status.IsNotImplemented()) << status.ToString();
    const auto compound =
        ::paimon::PredicateBuilder::Or({idPredicate, timestampPredicate})
            .value();
    status = reset(*reader, projected, compound);
    EXPECT_TRUE(status.IsNotImplemented()) << status.ToString();
    // A timestamp column elsewhere in the file must not prevent safe
    // projection.
    ASSERT_TRUE(reset(*reader, projected, idPredicate).ok());
    test::assertEqualVectors(expected, readAll(*reader, projected));
    ASSERT_TRUE(reset(*reader, ROW({}, {})).ok());
    EXPECT_EQ(readAll(*reader, ROW({}, {}))->size(), 2);
  }
}

TEST_F(PaimonOrcReaderTest, NestedTimestampFieldsAreRejected) {
  const Timestamp timestamp(1, 123456000);
  const std::vector<VectorPtr> nested = {
      makeRowVector({"ts"}, {makeFlatVector<Timestamp>({timestamp})}),
      makeArrayVector<Timestamp>({{timestamp}}),
      makeMapVector<int32_t, Timestamp>({{{1, timestamp}}})};
  for (const auto& value : nested) {
    auto rows = makeRowVector({"nested"}, {value});
    SCOPED_TRACE(rows->type()->toString());
    auto reader = open(write(rows));
    const auto status = reset(*reader, rows->type());
    EXPECT_TRUE(status.IsNotImplemented()) << status.ToString();
    expectTimestampReadRejected(*reader);
  }
}

TEST_F(PaimonOrcReaderTest, IndependentTimestampFilesAreRejected) {
  // Arrow 15's C++ ORC writer truncates negative fractional seconds before
  // calculating nanos. Use the Python fixture writer's independent oracle.
  const auto path = directory_->getPath() + "/timestamps.orc";
  const char* python = std::getenv("PAIMON_TEST_PYTHON");
  const char* scripts = std::getenv("PAIMON_TEST_SCRIPT_DIR");
  const std::string scriptDirectory = scripts && scripts[0] != '\0'
      ? scripts
      : "./bolt/connectors/paimon/tests";
  folly::Subprocess process(
      {python && python[0] != '\0' ? python : "python3",
       scriptDirectory + "/create_orc_reader_fixture.py",
       path},
      folly::Subprocess::Options().usePath());
  const auto status = process.wait();
  ASSERT_TRUE(status.exited() && status.exitStatus() == 0)
      << "Timestamp fixture generator failed: " << status.str();
  for (const auto& file : {path, path + ".utc.orc"}) {
    auto reader = open(file, 2);
    expectTimestampReadRejected(*reader);
    const auto status = reset(*reader, ROW({"ts"}, {TIMESTAMP()}));
    EXPECT_TRUE(status.IsNotImplemented()) << status.ToString();
  }
}

TEST_F(PaimonOrcReaderTest, CloseReleasesInputStream) {
  auto path = write(data());
  PaimonOrcReader format({});
  auto builder = format.CreateReaderBuilder(3).value();
  builder->WithMemoryPool(std::make_shared<BoltPaimonMemoryPool>(pool()));
  PaimonBoltFileSystem fs({});
  std::shared_ptr<::paimon::InputStream> stream =
      std::move(fs.Open(path)).value();
  std::weak_ptr<::paimon::InputStream> weakStream = stream;
  auto reader = builder->Build(stream).value();
  stream.reset();
  auto retained = next(*reader);
  EXPECT_FALSE(weakStream.expired());
  reader->Close();
  EXPECT_TRUE(weakStream.expired());
  test::assertEqualVectors(data()->slice(0, 3), retained);
}

TEST_F(PaimonOrcReaderTest, EmptyAndDamagedFiles) {
  auto empty = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(ROW({"id"}, {INTEGER()}), 0, pool()));
  auto reader = open(write(empty));
  EXPECT_EQ(reader->GetNumberOfRows().value(), 0);
  EXPECT_EQ(next(*reader), nullptr);
  EXPECT_EQ(next(*reader), nullptr);
  auto path = directory_->getPath() + "/damaged.orc";
  std::ofstream(path) << "not an ORC file";
  auto format = ::paimon::FileFormatFactory::Get("orc", {}).value();
  auto builder = format->CreateReaderBuilder(3).value();
  builder->WithMemoryPool(std::make_shared<BoltPaimonMemoryPool>(pool()));
  PaimonBoltFileSystem fs({});
  auto stream = fs.Open(path);
  ASSERT_TRUE(stream.ok()) << stream.status().ToString();
  auto result = builder->Build(
      std::shared_ptr<::paimon::InputStream>(std::move(stream).value()));
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.status().ToString().find("ORC"), std::string::npos);
  EXPECT_FALSE(format->CreateReaderBuilder(0).ok());
}
} // namespace
