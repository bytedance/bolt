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

#include <gtest/gtest.h>
#include <lance_file.h>

#include <fmt/format.h>
#include <folly/Conv.h>

#include <cstdlib>

#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/tests/utils/BatchMaker.h"
#include "bolt/dwio/lance/LanceWriter.h"
#include "bolt/dwio/lance/NativeLanceReader.h"
#include "bolt/dwio/lance/tests/RustLanceReaderAdapter.inc"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/type/fbhive/HiveTypeParser.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::lance::reader::test {
namespace {

class NativeLanceDifferentialTest : public testing::Test,
                                    protected bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
    dwio::common::LocalFileSink::registerFactory();
  }

  std::string writeWithRust(
      const RowVectorPtr& data,
      std::string_view version = "stable") {
    const auto* requestedPath = std::getenv("BOLT_LANCE_DIFFERENTIAL_OUTPUT");
    const auto path = requestedPath == nullptr
        ? tempDirectory_->getPath() + "/differential.lance"
        : std::string(requestedPath);
    dwio::common::WriterOptions options;
    options.memoryPool = rootPool_.get();
    options.serdeParameters["lance_file_version"] = version;
    auto context = std::shared_ptr<lce_ctx_common>(
        lce_alloc_ctx_common(), lce_free_ctx_common);
    auto writer =
        std::make_unique<::bytedance::bolt::lance::writer::LanceWriter>(
            dwio::common::FileSink::create(
                fmt::format("file:{}", path), {.pool = rootPool_.get()}),
            options,
            context);
    writer->write(data);
    writer->close();
    return path;
  }

  RowVectorPtr readNative(const std::string& path) {
    dwio::common::ReaderOptions options(pool());
    NativeLanceReader reader(
        std::make_unique<dwio::common::BufferedInput>(
            std::make_shared<LocalReadFile>(path), *pool()),
        options);
    auto rowReader = reader.createRowReader({});
    VectorPtr firstResult;
    const auto scanned = rowReader->next(10'000, firstResult);
    EXPECT_TRUE(reader.numberOfRows().has_value());
    if (!reader.numberOfRows().has_value()) {
      return nullptr;
    }
    EXPECT_EQ(scanned, reader.numberOfRows().value());
    VectorPtr endResult;
    EXPECT_EQ(rowReader->next(1, endResult), 0);
    return std::static_pointer_cast<RowVector>(firstResult);
  }

  RowVectorPtr readRust(const std::string& path, const RowTypePtr& type) {
    RustLanceReaderAdapter reader(path, *pool(), type->names());
    EXPECT_EQ(*reader.rowType(), *type);
    reader.open(10'000);
    VectorPtr firstResult;
    EXPECT_TRUE(reader.next(firstResult));
    VectorPtr endResult;
    EXPECT_FALSE(reader.next(endResult));
    return std::static_pointer_cast<RowVector>(firstResult);
  }

  RowVectorPtr makeInput() {
    constexpr vector_size_t kRows = 1'024;
    std::vector<std::optional<int64_t>> ids(kRows);
    std::vector<std::optional<std::string>> names(kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      if (row % 13 != 0) {
        ids[row] = row * 7;
      }
      if (row % 17 != 0) {
        names[row] = folly::to<std::string>("value-", row);
      }
    }
    return makeRowVector(
        {"id", "score", "name", "values"},
        {makeNullableFlatVector<int64_t>(ids),
         makeFlatVector<double>(kRows, [](auto row) { return row * 0.5; }),
         makeNullableFlatVector<std::string>(names),
         makeArrayVector<int32_t>(
             kRows,
             [](auto row) { return row % 4; },
             [](auto index) { return index * 3; },
             [](auto row) { return row % 19 == 0; })});
  }

  RowVectorPtr makeFsstInput() {
    constexpr vector_size_t kRows = 4'096;
    std::vector<std::optional<std::string>> values(kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      switch (row % 19) {
        case 0:
          values[row] = std::nullopt;
          break;
        case 1:
          values[row] = std::string{};
          break;
        default:
          values[row] = fmt::format(
              "tenant-{:02}/repeated-lance-fsst-value-{:03}/suffix-{:02}",
              row % 17,
              row % 127,
              row % 31);
      }
    }
    return makeRowVector(
        {"value"}, {makeNullableFlatVector<std::string>(values)});
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tempDirectory_{
      exec::test::TempDirectoryPath::create()};
};

TEST_F(NativeLanceDifferentialTest, rustWriterRustAndNativeReadersAgree) {
  auto data = makeInput();
  const auto path = writeWithRust(data);
  const auto type = std::static_pointer_cast<const RowType>(data->type());
  const auto rust = readRust(path, type);
  const auto native = readNative(path);
  bolt::test::assertEqualVectors(data, rust);
  bolt::test::assertEqualVectors(data, native);
  bolt::test::assertEqualVectors(rust, native);
}

TEST_F(NativeLanceDifferentialTest, rustProductionTypeMatrixAgrees) {
  constexpr vector_size_t kRows = 4'096;
  type::fbhive::HiveTypeParser parser;
  const auto type = std::static_pointer_cast<const RowType>(
      parser.parse("struct<"
                   "bool_val:boolean,"
                   "byte_val:tinyint,"
                   "short_val:smallint,"
                   "int_val:int,"
                   "long_val:bigint,"
                   "float_val:float,"
                   "double_val:double,"
                   "string_val:string,"
                   "binary_val:binary,"
                   "timestamp_val:timestamp,"
                   "date_val:date,"
                   "array_val:array<float>,"
                   "nested_array_val:array<array<int>>"
                   ">"));
  const auto data =
      std::static_pointer_cast<RowVector>(bolt::test::BatchMaker::createBatch(
          type, kRows, *pool(), [](auto row) { return row % 23 == 0; }));
  const auto path = writeWithRust(data);

  const auto rust = readRust(path, type);
  const auto native = readNative(path);
  bolt::test::assertEqualVectors(data, rust);
  bolt::test::assertEqualVectors(data, native);
  bolt::test::assertEqualVectors(rust, native);
}

TEST_F(NativeLanceDifferentialTest, structuralVersionsAgree) {
  const auto data = makeInput();
  const auto type = std::static_pointer_cast<const RowType>(data->type());
  for (const auto version : {"2.1", "2.2"}) {
    const auto path = writeWithRust(data, version);
    const auto rust = readRust(path, type);
    const auto native = readNative(path);
    bolt::test::assertEqualVectors(data, rust);
    bolt::test::assertEqualVectors(data, native);
    bolt::test::assertEqualVectors(rust, native);
  }
}

TEST_F(NativeLanceDifferentialTest, structuralNestedTypesAgree) {
  constexpr vector_size_t kRows = 2'049;
  type::fbhive::HiveTypeParser parser;
  const auto type = std::static_pointer_cast<const RowType>(
      parser.parse("struct<nested:struct<a:int,b:string>,"
                   "nested_array_val:array<array<int>>>"));
  const auto data =
      std::static_pointer_cast<RowVector>(bolt::test::BatchMaker::createBatch(
          type, kRows, *pool(), [](auto row) { return row % 23 == 0; }));

  for (const auto version : {"2.1", "2.2"}) {
    const auto path = writeWithRust(data, version);
    const auto rust = readRust(path, type);
    const auto native = readNative(path);
    bolt::test::assertEqualVectors(data, rust);
    bolt::test::assertEqualVectors(data, native);
    bolt::test::assertEqualVectors(rust, native);
  }
}

TEST_F(NativeLanceDifferentialTest, rust037FsstFixtureAgrees) {
  const std::string path = "examples/fsst_v2_0.lance";
  const auto expected = makeFsstInput();
  const auto type = std::static_pointer_cast<const RowType>(expected->type());
  const auto rust = readRust(path, type);
  const auto native = readNative(path);
  bolt::test::assertEqualVectors(expected, rust);
  bolt::test::assertEqualVectors(expected, native);
  bolt::test::assertEqualVectors(rust, native);
}

} // namespace
} // namespace bytedance::bolt::lance::reader::test
