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

#include "bolt/dwio/common/tests/E2EFilterTestBase.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/reader/ParquetTypeWithId.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <limits>
#include <utility>
using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;

using dwio::common::MemorySink;

class E2EFilterTest : public E2EFilterTestBase, public test::VectorTestBase {
 protected:
  void SetUp() override {
    E2EFilterTestBase::SetUp();
  }

  void testWithTypes(
      const std::string& columns,
      std::function<void()> customize,
      bool wrapInStruct,
      const std::vector<std::string>& filterable,
      int32_t numCombinations) {
    testScenario(columns, customize, wrapInStruct, filterable, numCombinations);

    // Always test no null case.
    auto newCustomize = [&]() {
      if (customize) {
        customize();
      }
      makeNotNull(0);
    };
    testScenario(
        columns, newCustomize, wrapInStruct, filterable, numCombinations);
  }

  void writeToMemory(
      const TypePtr& type,
      const std::vector<RowVectorPtr>& batches,
      bool forRowGroupSkip = false) override {
    auto sink = std::make_unique<MemorySink>(
        200 * 1024 * 1024, FileSink::Options{.pool = leafPool_.get()});
    sinkPtr_ = sink.get();
    options_.memoryPool = E2EFilterTestBase::rootPool_.get();
    int32_t flushCounter = 0;
    options_.flushPolicyFactory = [&]() {
      return std::make_unique<LambdaFlushPolicy>(
          rowsInRowGroup_, bytesInRowGroup_, [&]() {
            return forRowGroupSkip
                ? false
                : (++flushCounter % flushEveryNBatches_ == 0);
          });
    };

    writer_ = std::make_unique<bytedance::bolt::parquet::Writer>(
        std::move(sink), options_, asRowType(type));
    for (auto& batch : batches) {
      writer_->write(batch);
    }
    writer_->flush();
    writer_->close();
  }

  std::unique_ptr<dwio::common::Reader> makeReader(
      const dwio::common::ReaderOptions& opts,
      std::unique_ptr<dwio::common::BufferedInput> input) override {
    return std::make_unique<ParquetReader>(std::move(input), opts);
  }

  std::pair<VectorPtr, uint64_t> readWithSpec(
      const RowVectorPtr& data,
      const std::shared_ptr<common::ScanSpec>& spec,
      uint64_t size,
      const RowTypePtr& outputType = nullptr) {
    auto rowReader = makeRowReader(data, spec);
    auto result = BaseVector::create(
        outputType ? outputType : data->type(), 0, leafPool_.get());
    auto rowsScanned = rowReader->next(size, result);
    result->loadedVector();
    return {result, rowsScanned};
  }

  std::unique_ptr<RowReader> makeRowReader(
      const RowVectorPtr& data,
      const std::shared_ptr<common::ScanSpec>& spec) {
    writeToMemory(data->type(), {data}, false);

    dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.setScanSpec(spec);
    std::string_view serializedData(sinkPtr_->data(), sinkPtr_->size());
    auto input = std::make_unique<BufferedInput>(
        std::make_shared<InMemoryReadFile>(serializedData),
        readerOpts.getMemoryPool());
    auto reader = makeReader(readerOpts, std::move(input));
    return reader->createRowReader(rowReaderOpts);
  }

  void assertNoNullBuffer(const VectorPtr& vector) {
    ASSERT_FALSE(vector->isLazy());
    EXPECT_EQ(nullptr, vector->nulls().get());
    EXPECT_EQ(nullptr, vector->rawNulls());
    EXPECT_FALSE(vector->mayHaveNulls());
  }

  void assertHasNullBuffer(const VectorPtr& vector) {
    ASSERT_FALSE(vector->isLazy());
    EXPECT_NE(nullptr, vector->nulls().get());
    EXPECT_NE(nullptr, vector->rawNulls());
    EXPECT_TRUE(vector->mayHaveNulls());
  }

  std::unique_ptr<bytedance::bolt::parquet::Writer> writer_;
  bytedance::bolt::parquet::WriterOptions options_;
  uint64_t rowsInRowGroup_ = 10'000;
  int64_t bytesInRowGroup_ = 128 * 1'024 * 1'024;
};

TEST_F(E2EFilterTest, writerMagic) {
  rowType_ = ROW({"c0"}, {INTEGER()});
  std::vector<RowVectorPtr> batches;
  batches.push_back(std::static_pointer_cast<RowVector>(
      test::BatchMaker::createBatch(rowType_, 20000, *leafPool_, nullptr, 0)));
  writeToMemory(rowType_, batches, false);
  auto data = sinkPtr_->data();
  auto size = sinkPtr_->size();
  EXPECT_EQ("PAR1", std::string(data, 4));
  EXPECT_EQ("PAR1", std::string(data + size - 4, 4));
}

TEST_F(E2EFilterTest, boolean) {
  testWithTypes(
      "boolean_val:boolean,"
      "boolean_null:boolean",
      [&]() { makeAllNulls("boolean_null"); },
      true,
      {"boolean_val"},
      20);
}

TEST_F(E2EFilterTest, integerDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "long_null:bigint",
      [&]() { makeAllNulls("long_null"); },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

TEST_F(E2EFilterTest, integerDeltaBinaryPack) {
  options_.enableDictionary = false;
  options_.encoding =
      bytedance::bolt::parquet::arrow::Encoding::DELTA_BINARY_PACKED;

  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "long_null:bigint",
      [&]() { makeAllNulls("long_null"); },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

TEST_F(E2EFilterTest, compression) {
  for (const auto compression :
       {common::CompressionKind_SNAPPY,
        common::CompressionKind_ZSTD,
        common::CompressionKind_GZIP,
        common::CompressionKind_NONE,
        common::CompressionKind_LZ4}) {
    if (!bytedance::bolt::parquet::Writer::isCodecAvailable(compression)) {
      continue;
    }

    options_.dataPageSize = 4 * 1024;
    options_.compression = compression;

    testWithTypes(
        "tinyint_val:tinyint,"
        "short_val:smallint,"
        "int_val:int,"
        "long_val:bigint",
        [&]() {
          makeIntDistribution<int64_t>(
              "long_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -9999, // rareMin
              10000000000, // rareMax
              true); // keepNulls

          makeIntDistribution<int32_t>(
              "int_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -9999, // rareMin
              100000000, // rareMax
              false); // keepNulls

          makeIntDistribution<int16_t>(
              "short_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -999, // rareMin
              30000, // rareMax
              true); // keepNulls

          makeIntDistribution<int8_t>(
              "tinyint_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -99, // rareMin
              3000, // rareMax
              true); // keepNulls
        },
        true,
        {"tinyint_val", "short_val", "int_val", "long_val"},
        3);
  }
}

TEST_F(E2EFilterTest, integerDictionary) {
  options_.dataPageSize = 4 * 1024;

  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntDistribution<int64_t>(
            "long_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            10000000000, // rareMax
            true); // keepNulls

        makeIntDistribution<int32_t>(
            "int_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            100000000, // rareMax
            false); // keepNulls

        makeIntDistribution<int16_t>(
            "short_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -999, // rareMin
            30000, // rareMax
            true); // keepNulls
      },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

TEST_F(E2EFilterTest, timestampDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;
  options_.writeInt96AsTimestamp = true;
  timestampPrecision_ = TimestampPrecision::kNanoseconds;

  testWithTypes(
      "timestamp_val_0:timestamp,"
      "timestamp_val_1:timestamp",
      [&]() {},
      true,
      {"timestamp_val_0", "timestamp_val_1"},
      20);
}

TEST_F(E2EFilterTest, timestampDictionary) {
  options_.dataPageSize = 4 * 1024;
  options_.writeInt96AsTimestamp = true;
  timestampPrecision_ = TimestampPrecision::kNanoseconds;

  testWithTypes(
      "timestamp_val_0:timestamp,"
      "timestamp_val_1:timestamp",
      [&]() {},
      true,
      {"timestamp_val_0", "timestamp_val_1"},
      20);
}

TEST_F(E2EFilterTest, floatAndDoubleDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  testWithTypes(
      "float_val:float,"
      "double_val:double,"
      "float_val2:float,"
      "double_val2:double,"
      "long_val:bigint,"
      "float_null:float",
      [&]() {
        makeAllNulls("float_null");
        makeQuantizedFloat<float>("float_val2", 200, true);
        makeQuantizedFloat<double>("double_val2", 522, true);
      },
      true,
      {"float_val", "double_val", "float_val2", "double_val2", "float_null"},
      20);
}

TEST_F(E2EFilterTest, floatAndDouble) {
  // float_val and double_val may be direct since the
  // values are random.float_val2 and double_val2 are expected to be
  // dictionaries since the values are quantized.
  testWithTypes(
      "float_val:float,"
      "double_val:double,"
      "float_val2:float,"
      "double_val2:double,"

      "long_val:bigint,"
      "float_null:float",
      [&]() {
        makeAllNulls("float_null");
        makeQuantizedFloat<float>("float_val2", 200, true);
        makeQuantizedFloat<double>("double_val2", 522, true);
        // Make sure there are RLE's.
        makeReapeatingValues<float>("float_val2", 0, 100, 200, 10.1);
        makeReapeatingValues<double>("double_val2", 0, 100, 200, 100.8);
      },
      true,
      {"float_val", "double_val", "float_val2", "double_val2", "float_null"},
      20);
}

TEST_F(E2EFilterTest, shortDecimalDictionary) {
  // decimal(8, 5) maps to 4 bytes FLBA in Parquet.
  // decimal(10, 5) maps to 5 bytes FLBA in Parquet.
  // decimal(17, 5) maps to 8 bytes FLBA in Parquet.
  for (const auto& type : {
           "shortdecimal_val:decimal(8, 5)",
           "shortdecimal_val:decimal(10, 5)",
           "shortdecimal_val:decimal(17, 5)",
       }) {
    testWithTypes(
        type,
        [&]() {
          makeIntDistribution<int64_t>(
              "shortdecimal_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -999, // rareMin
              30000, // rareMax
              true);
        },
        false,
        {"shortdecimal_val"},
        20);
  }
}

TEST_F(E2EFilterTest, shortDecimalDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  // decimal(8, 5) maps to 4 bytes FLBA in Parquet.
  // decimal(10, 5) maps to 5 bytes FLBA in Parquet.
  // decimal(17, 5) maps to 8 bytes FLBA in Parquet.
  for (const auto& type : {
           "shortdecimal_val:decimal(8, 5)",
           "shortdecimal_val:decimal(10, 5)",
           "shortdecimal_val:decimal(17, 5)",
       }) {
    testWithTypes(
        type,
        [&]() {
          makeIntDistribution<int64_t>(
              "shortdecimal_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -999, // rareMin
              30000, // rareMax
              true);
        },
        false,
        {"shortdecimal_val"},
        20);
  }

  testWithTypes(
      "shortdecimal_val:decimal(10, 5)",
      [&]() {
        useSuppliedValues<int64_t>("shortdecimal_val", 0, {-479, 40000000});
      },
      false,
      {"shortdecimal_val"},
      20);
}

TEST_F(E2EFilterTest, longDecimalDictionary) {
  // decimal(30, 10) maps to 13 bytes FLBA in Parquet.
  // decimal(37, 15) maps to 16 bytes FLBA in Parquet.
  for (const auto& type : {
           "longdecimal_val:decimal(30, 10)",
           "longdecimal_val:decimal(37, 15)",
       }) {
    testWithTypes(
        type,
        [&]() {
          makeIntDistribution<int128_t>(
              "longdecimal_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -999, // rareMin
              30000, // rareMax
              true);
        },
        true,
        {},
        20);
  }
}

TEST_F(E2EFilterTest, longDecimalDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  // decimal(30, 10) maps to 13 bytes FLBA in Parquet.
  // decimal(37, 15) maps to 16 bytes FLBA in Parquet.
  for (const auto& type : {
           "longdecimal_val:decimal(30, 10)",
           "longdecimal_val:decimal(37, 15)",
       }) {
    testWithTypes(
        type,
        [&]() {
          makeIntDistribution<int128_t>(
              "longdecimal_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -999, // rareMin
              30000, // rareMax
              true);
        },
        true,
        {},
        20);
  }

  testWithTypes(
      "longdecimal_val:decimal(30, 10)",
      [&]() {
        useSuppliedValues<int128_t>(
            "longdecimal_val",
            0,
            {-479, HugeInt::build(1546093991, 4054979645)});
      },
      false,
      {},
      20);
}

TEST_F(E2EFilterTest, stringDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  testWithTypes(
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringUnique("string_val");
        makeStringUnique("string_val_2");
      },
      true,
      {"string_val", "string_val_2"},
      20);
}

TEST_F(E2EFilterTest, stringDictionary) {
  testWithTypes(
      "string_val:string,"
      "string_val_2:string,"
      "string_const: string",
      [&]() {
        makeStringDistribution("string_val", 100, true, false);
        makeStringDistribution("string_val_2", 170, false, true);
        makeStringDistribution("string_const", 1, true, false);
      },
      true,
      {"string_val", "string_val_2"},
      20);
}

TEST_F(E2EFilterTest, stringDeltaByteArray) {
  options_.enableDictionary = false;
  options_.encoding =
      bytedance::bolt::parquet::arrow::Encoding::DELTA_BYTE_ARRAY;

  testWithTypes(
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringUnique("string_val");
        makeStringUnique("string_val_2");
      },
      true,
      {"string_val", "string_val_2"},
      20);
}

TEST_F(E2EFilterTest, dedictionarize) {
  rowsInRowGroup_ = 10'000;
  options_.dictionaryPageSizeLimit = 20'000;

  testWithTypes(
      "long_val: bigint,"
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringDistribution("string_val", 10000000, true, false);
        makeStringDistribution("string_val_2", 1700000, false, true);
      },
      true,
      {"long_val", "string_val", "string_val_2"},
      20);
}

TEST_F(E2EFilterTest, filterStruct) {
  // The data has a struct member with one second level struct
  // column. Both structs have a column that gets filtered 'nestedxxx'
  // and one that does not 'dataxxx'.
  testWithTypes(
      "long_val:bigint,"
      "outer_struct: struct<nested1:bigint, "
      "  data1: string, "
      "  inner_struct: struct<nested2: bigint, data2: array<smallint>>>",
      [&]() {},
      false,
      {"long_val",
       "outer_struct.inner_struct",
       "outer_struct.nested1",
       "outer_struct.inner_struct.nested2"},
      40);
}

TEST_F(E2EFilterTest, list) {
  // Break up the leaf data in small pages to cover coalescing repdefs.
  options_.dataPageSize = 4 * 1024;

  batchCount_ = 2;
  batchSize_ = 12000;
  testWithTypes(
      "long_val:bigint, array_val:array<int>,"
      "struct_array: struct<a: array<struct<k:int, v:int, va: array<smallint>>>>",
      nullptr,
      false,
      {"long_val", "array_val"},
      10);
}

TEST_F(E2EFilterTest, metadataFilter) {
  // Follow the batch size in `E2EFiltersTestBase`,
  // so that each batch can produce a row group.
  rowsInRowGroup_ = 10;
  testMetadataFilter();
}

TEST_F(E2EFilterTest, subfieldsPruning) {
  testSubfieldsPruning();
}

TEST_F(E2EFilterTest, mutationCornerCases) {
  testMutationCornerCases();
}

TEST_F(E2EFilterTest, map) {
  // Break up the leaf data in small pages to cover coalescing repdefs.
  options_.dataPageSize = 4 * 1024;

  batchCount_ = 2;
  batchSize_ = 12000;
  testWithTypes(
      "long_val:bigint,"
      "map_val:map<int, int>,"
      "nested_map:map<int, map<int, bigint>>,"
      "struct_map: struct<m: map<int, struct<k:int, v:int, vm: map<bigint, smallint>>>>",
      nullptr,
      false,
      {"long_val", "map_val"},
      10);
}

TEST_F(E2EFilterTest, varbinaryDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  testWithTypes(
      "varbinary_val:varbinary,"
      "varbinary_val_2:varbinary",
      [&]() {
        makeStringUnique("varbinary_val");
        makeStringUnique("varbinary_val_2");
      },
      true,
      {"varbinary_val", "varbinary_val_2"},
      20);
}

TEST_F(E2EFilterTest, varbinaryDictionary) {
  testWithTypes(
      "varbinary_val:varbinary,"
      "varbinary_val_2:varbinary,"
      "varbinary_const:varbinary",
      [&]() {
        makeStringDistribution("varbinary_val", 100, true, false);
        makeStringDistribution("varbinary_val_2", 170, false, true);
        makeStringDistribution("varbinary_const", 1, true, false);
      },
      true,
      {"varbinary_val", "varbinary_val_2"},
      20);
}

TEST_F(E2EFilterTest, largeMetadata) {
  rowsInRowGroup_ = 1;

  rowType_ = ROW({"c0"}, {INTEGER()});
  std::vector<RowVectorPtr> batches;
  batches.push_back(std::static_pointer_cast<RowVector>(
      test::BatchMaker::createBatch(rowType_, 1000, *leafPool_, nullptr, 0)));
  writeToMemory(rowType_, batches, false);
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  readerOpts.setFooterEstimatedSize(1024);
  readerOpts.setFilePreloadThreshold(1024 * 8);
  dwio::common::RowReaderOptions rowReaderOpts;
  std::string_view data(sinkPtr_->data(), sinkPtr_->size());
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(data), readerOpts.getMemoryPool());
  auto reader = makeReader(readerOpts, std::move(input));
  EXPECT_EQ(1000, reader->numberOfRows());
}

TEST_F(E2EFilterTest, isNotNullOutputHasNoNullsAfterCompaction) {
  auto data = makeRowVector(
      {"a", "b"},
      {makeNullableFlatVector<int64_t>(
           {10, std::nullopt, 30, 40, std::nullopt, 60}),
       makeFlatVector<int64_t>({5, 7, 12, 3, 8, 20})});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("a", 0)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("b", 1)->setFilter(std::make_unique<common::BigintRange>(
      std::numeric_limits<int64_t>::min(), 9, false));

  auto [result, rowsScanned] = readWithSpec(data, spec, 6);
  EXPECT_EQ(6, rowsScanned);
  auto row = result->asUnchecked<RowVector>();
  ASSERT_EQ(2, row->size());
  test::assertEqualVectors(
      makeRowVector(
          {"a", "b"},
          {makeFlatVector<int64_t>({10, 40}), makeFlatVector<int64_t>({5, 3})}),
      result);

  assertNoNullBuffer(row->childAt(0));
}

TEST_F(E2EFilterTest, isNotNullOutputOnlyElidesProvenNonNullColumn) {
  auto data = makeRowVector(
      {"a", "b", "guard"},
      {makeNullableFlatVector<int64_t>(
           {10, std::nullopt, 30, 40, 50, std::nullopt}),
       makeNullableFlatVector<int64_t>(
           {std::nullopt, 21, 22, std::nullopt, 24, 25}),
       makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5})});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("a", 0)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("b", 1);
  spec->addField("guard", 2)
      ->setFilter(std::make_unique<common::BigintRange>(0, 4, false));

  auto [result, rowsScanned] = readWithSpec(data, spec, 6);
  EXPECT_EQ(6, rowsScanned);
  test::assertEqualVectors(
      makeRowVector(
          {"a", "b", "guard"},
          {makeFlatVector<int64_t>({10, 30, 40, 50}),
           makeNullableFlatVector<int64_t>(
               {std::nullopt, 22, std::nullopt, 24}),
           makeFlatVector<int64_t>({0, 2, 3, 4})}),
      result);

  auto row = result->asUnchecked<RowVector>();
  assertNoNullBuffer(row->childAt(0));
  assertHasNullBuffer(row->childAt(1));
}

TEST_F(E2EFilterTest, isNotNullOutputHasNoNullsAfterDictionaryFallback) {
  options_.dictionaryPageSizeLimit = 128;

  constexpr vector_size_t kSize = 2048;
  auto data = makeRowVector(
      {"s", "guard"},
      {makeFlatVector<std::string>(
           kSize,
           [](auto row) { return fmt::format("value_{}", row); },
           [](auto row) { return row % 7 == 0; }),
       makeFlatVector<int64_t>(kSize, [](auto row) { return row % 13; })});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("s", 0)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("guard", 1)
      ->setFilter(std::make_unique<common::BigintRange>(0, 5, false));

  auto [result, rowsScanned] = readWithSpec(data, spec, kSize);
  EXPECT_EQ(kSize, rowsScanned);
  auto row = result->asUnchecked<RowVector>();
  auto s = row->childAt(0)->asUnchecked<FlatVector<StringView>>();
  auto guard = row->childAt(1)->asUnchecked<FlatVector<int64_t>>();
  assertNoNullBuffer(row->childAt(0));
  for (auto i = 0; i < row->size(); ++i) {
    auto sourceRow = std::stoll(s->valueAt(i).getString().substr(6));
    EXPECT_NE(0, sourceRow % 7);
    EXPECT_LT(guard->valueAt(i), 6);
  }
}

TEST_F(E2EFilterTest, isNotNullOutputHandlesAllRowsFiltered) {
  auto data = makeRowVector(
      {"a", "b"},
      {makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt, 3}),
       makeFlatVector<int64_t>({10, 11, 12})});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("a", 0)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("b", 1)->setFilter(
      std::make_unique<common::BigintRange>(0, 1, false));

  auto [result, rowsScanned] = readWithSpec(data, spec, 3);
  (void)rowsScanned;
  EXPECT_EQ(0, result->size());
}

TEST_F(E2EFilterTest, isNotNullOutputHasNoNullsWithResultReuse) {
  constexpr vector_size_t kSize = 32;
  auto data = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>(
           kSize,
           [](auto row) { return row; },
           [](auto row) { return row % 4 == 0; }),
       makeFlatVector<int64_t>(kSize, [](auto row) { return row % 3; })});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("a", 0)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("b", 1)->setFilter(
      std::make_unique<common::BigintRange>(0, 1, false));

  auto rowReader = makeRowReader(data, spec);
  auto result = BaseVector::create(data->type(), 0, leafPool_.get());
  uint64_t totalScanned = 0;
  for (;;) {
    auto scanned = rowReader->next(7, result);
    if (!scanned) {
      break;
    }
    totalScanned += scanned;
    if (!result->size()) {
      continue;
    }
    auto row = result->asUnchecked<RowVector>();
    assertNoNullBuffer(row->childAt(0));
    auto a = row->childAt(0)->asUnchecked<FlatVector<int64_t>>();
    auto b = row->childAt(1)->asUnchecked<FlatVector<int64_t>>();
    for (auto i = 0; i < row->size(); ++i) {
      EXPECT_NE(0, a->valueAt(i) % 4);
      EXPECT_LT(b->valueAt(i), 2);
    }
  }
  EXPECT_EQ(kSize, totalScanned);
}

TEST_F(E2EFilterTest, isNotNullOutputHandlesEmptyBatchReuse) {
  constexpr vector_size_t kSize = 12;
  auto data = makeRowVector(
      {"a", "guard"},
      {makeFlatVector<int64_t>(
           kSize,
           [](auto row) { return row; },
           [](auto row) { return row == 1 || row == 9; }),
       makeFlatVector<int64_t>(
           kSize, [](auto row) { return row >= 4 && row < 8 ? 9 : 0; })});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("a", 0)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("guard", 1)
      ->setFilter(std::make_unique<common::BigintRange>(0, 0, false));

  auto rowReader = makeRowReader(data, spec);
  auto result = BaseVector::create(data->type(), 0, leafPool_.get());

  ASSERT_EQ(4, rowReader->next(4, result));
  test::assertEqualVectors(
      makeRowVector(
          {"a", "guard"},
          {makeFlatVector<int64_t>({0, 2, 3}),
           makeFlatVector<int64_t>({0, 0, 0})}),
      result);
  assertNoNullBuffer(result->asUnchecked<RowVector>()->childAt(0));

  ASSERT_EQ(4, rowReader->next(4, result));
  EXPECT_EQ(0, result->size());

  ASSERT_EQ(4, rowReader->next(4, result));
  test::assertEqualVectors(
      makeRowVector(
          {"a", "guard"},
          {makeFlatVector<int64_t>({8, 10, 11}),
           makeFlatVector<int64_t>({0, 0, 0})}),
      result);
  assertNoNullBuffer(result->asUnchecked<RowVector>()->childAt(0));
  ASSERT_FALSE(rowReader->next(4, result));
}

TEST_F(E2EFilterTest, isNotNullOutputHasNoNullsAcrossPages) {
  options_.enableDictionary = false;
  options_.dataPageSize = 128;

  constexpr vector_size_t kSize = 4096;
  auto data = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>(
           kSize,
           [](auto row) { return row; },
           [](auto row) { return row % 5 == 0; }),
       makeFlatVector<int64_t>(kSize, [](auto row) { return row % 11; })});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("a", 0)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("b", 1)->setFilter(
      std::make_unique<common::BigintRange>(0, 4, false));

  auto [result, rowsScanned] = readWithSpec(data, spec, kSize);
  EXPECT_EQ(kSize, rowsScanned);
  auto row = result->asUnchecked<RowVector>();
  auto a = row->childAt(0)->asUnchecked<FlatVector<int64_t>>();
  auto b = row->childAt(1)->asUnchecked<FlatVector<int64_t>>();
  ASSERT_EQ(a->size(), b->size());
  assertNoNullBuffer(row->childAt(0));
  for (auto i = 0; i < row->size(); ++i) {
    EXPECT_NE(0, a->valueAt(i) % 5);
    EXPECT_LT(b->valueAt(i), 5);
  }
}

TEST_F(E2EFilterTest, isNotNullOutputHasNoNullsAcrossRowGroups) {
  rowsInRowGroup_ = 4;
  constexpr vector_size_t kSize = 12;
  auto data = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>(
           kSize,
           [](auto row) { return row; },
           [](auto row) { return row < 4 || row == 8; }),
       makeFlatVector<int64_t>(kSize, [](auto row) { return row % 5; })});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("a", 0)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("b", 1)->setFilter(
      std::make_unique<common::BigintRange>(0, 3, false));

  auto rowReader = makeRowReader(data, spec);
  auto result = BaseVector::create(data->type(), 0, leafPool_.get());
  std::vector<int64_t> values;
  for (;;) {
    auto scanned = rowReader->next(4, result);
    if (!scanned) {
      break;
    }
    if (!result->size()) {
      continue;
    }
    auto row = result->asUnchecked<RowVector>();
    auto a = row->childAt(0)->asUnchecked<FlatVector<int64_t>>();
    auto b = row->childAt(1)->asUnchecked<FlatVector<int64_t>>();
    assertNoNullBuffer(row->childAt(0));
    for (auto i = 0; i < row->size(); ++i) {
      EXPECT_FALSE(a->valueAt(i) < 4 || a->valueAt(i) == 8);
      EXPECT_LT(b->valueAt(i), 4);
      values.push_back(a->valueAt(i));
    }
  }
  EXPECT_EQ((std::vector<int64_t>{5, 6, 7, 10, 11}), values);
}

TEST_F(E2EFilterTest, isNotNullOutputHasNoNullsForScalarTypes) {
  auto data = makeRowVector(
      {"s", "vb", "d", "guard"},
      {makeNullableFlatVector<std::string>(
           {"apple", std::nullopt, "banana", "cherry", std::nullopt, "date"}),
       makeNullableFlatVector<std::string>(
           {"aa", "bb", std::nullopt, "cc", "dd", std::nullopt}, VARBINARY()),
       makeFlatVector<double>(
           6,
           [](auto row) { return row + 0.5; },
           [](auto row) { return row == 4; }),
       makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("s", 0)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("vb", 1)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("d", 2)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("guard", 3)
      ->setFilter(std::make_unique<common::BigintRange>(2, 4, false));

  auto [result, rowsScanned] = readWithSpec(data, spec, 6);
  EXPECT_EQ(6, rowsScanned);
  auto row = result->asUnchecked<RowVector>();
  ASSERT_EQ(1, row->size());
  test::assertEqualVectors(
      makeRowVector(
          {"s", "vb", "d", "guard"},
          {makeFlatVector<std::string>({"cherry"}),
           makeFlatVector<std::string>({"cc"}, VARBINARY()),
           makeFlatVector<double>({3.5}),
           makeFlatVector<int64_t>({4})}),
      result);
  assertNoNullBuffer(row->childAt(0));
  assertNoNullBuffer(row->childAt(1));
  assertNoNullBuffer(row->childAt(2));
}

TEST_F(E2EFilterTest, isNotNullOutputHasNoNullsForAdditionalScalarTypes) {
  auto data = makeRowVector(
      {"flag", "ts", "dec", "guard"},
      {makeFlatVector<bool>(
           6,
           [](auto row) { return row % 2 == 0; },
           [](auto row) { return row == 1; }),
       makeFlatVector<Timestamp>(
           6,
           [](auto row) { return Timestamp(row, row * 1000); },
           [](auto row) { return row == 2; }),
       makeNullableFlatVector<int64_t>(
           {100, std::nullopt, 300, 400, std::nullopt, 600}, DECIMAL(10, 2)),
       makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5})});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("flag", 0)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("ts", 1)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("dec", 2)->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("guard", 3)
      ->setFilter(std::make_unique<common::BigintRange>(0, 4, false));

  auto [result, rowsScanned] = readWithSpec(data, spec, 6);
  EXPECT_EQ(6, rowsScanned);
  auto row = result->asUnchecked<RowVector>();
  ASSERT_EQ(2, row->size());
  test::assertEqualVectors(
      makeRowVector(
          {"flag", "ts", "dec", "guard"},
          {makeFlatVector<bool>({true, false}),
           makeFlatVector<Timestamp>({Timestamp(0, 0), Timestamp(3, 0)}),
           makeFlatVector<int64_t>({100, 400}, DECIMAL(10, 2)),
           makeFlatVector<int64_t>({0, 3})}),
      result);
  assertNoNullBuffer(row->childAt(0));
  assertNoNullBuffer(row->childAt(1));
  assertNoNullBuffer(row->childAt(2));
}

TEST_F(E2EFilterTest, complexIsNotNullOutputKeepsCorrectNulls) {
  auto data = makeRowVector(
      {"arr", "guard"},
      {makeArrayVector<int64_t>(
           5,
           [](auto) { return 2; },
           [](auto row, auto index) { return row * 10 + index; },
           [](auto row) { return row == 1 || row == 4; }),
       makeFlatVector<int64_t>({0, 1, 2, 3, 4})});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addFieldRecursively("arr", *ARRAY(BIGINT()), 0)
      ->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("guard", 1)
      ->setFilter(std::make_unique<common::BigintRange>(0, 3, false));

  auto [result, rowsScanned] = readWithSpec(data, spec, 5);
  EXPECT_EQ(5, rowsScanned);
  auto row = result->asUnchecked<RowVector>();
  ASSERT_EQ(3, row->size());
  auto arr = row->childAt(0)->asUnchecked<ArrayVector>();
  ASSERT_FALSE(arr->isNullAt(0));
  ASSERT_FALSE(arr->isNullAt(1));
  ASSERT_FALSE(arr->isNullAt(2));
  EXPECT_EQ(2, arr->sizeAt(0));
  EXPECT_EQ(2, arr->sizeAt(1));
  EXPECT_EQ(2, arr->sizeAt(2));
  test::assertEqualVectors(
      makeRowVector(
          {"arr", "guard"},
          {makeArrayVector<int64_t>({{0, 1}, {20, 21}, {30, 31}}),
           makeFlatVector<int64_t>({0, 2, 3})}),
      result);
}

TEST_F(E2EFilterTest, mapIsNotNullOutputKeepsCorrectNulls) {
  auto data = makeRowVector(
      {"m", "guard"},
      {makeNullableMapVector<int64_t, int64_t>(
           {std::vector<std::pair<int64_t, std::optional<int64_t>>>{
                {1, 10}, {2, std::nullopt}},
            std::nullopt,
            std::vector<std::pair<int64_t, std::optional<int64_t>>>{
                {4, 40}, {5, 50}},
            std::vector<std::pair<int64_t, std::optional<int64_t>>>{{6, 60}},
            std::nullopt},
           MAP(BIGINT(), BIGINT())),
       makeFlatVector<int64_t>({0, 1, 2, 3, 4})});

  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addFieldRecursively("m", *MAP(BIGINT(), BIGINT()), 0)
      ->setFilter(std::make_unique<common::IsNotNull>());
  spec->addField("guard", 1)
      ->setFilter(std::make_unique<common::BigintRange>(0, 3, false));

  auto [result, rowsScanned] = readWithSpec(data, spec, 5);
  EXPECT_EQ(5, rowsScanned);
  auto row = result->asUnchecked<RowVector>();
  ASSERT_EQ(3, row->size());
  auto map = row->childAt(0)->asUnchecked<MapVector>();
  ASSERT_FALSE(map->isNullAt(0));
  ASSERT_FALSE(map->isNullAt(1));
  ASSERT_FALSE(map->isNullAt(2));
  EXPECT_EQ(2, map->sizeAt(0));
  EXPECT_EQ(2, map->sizeAt(1));
  EXPECT_EQ(1, map->sizeAt(2));
}

TEST_F(E2EFilterTest, siblingFilterPreservesNullableOutputAcrossPages) {
  options_.enableDictionary = false;
  options_.dataPageSize = 128;

  constexpr vector_size_t kSize = 4096;
  auto data = makeRowVector(
      {"a", "guard"},
      {makeFlatVector<int64_t>(
           kSize,
           [](auto row) { return row; },
           [](auto row) { return row < 64; }),
       makeFlatVector<int64_t>(kSize, [](auto row) { return row % 5; })});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("a", 0);
  spec->addField("guard", 1)
      ->setFilter(std::make_unique<common::BigintRange>(0, 3, false));

  std::vector<std::optional<int64_t>> expectedA;
  std::vector<int64_t> expectedGuard;
  for (auto row = 0; row < kSize; ++row) {
    const auto guard = row % 5;
    if (guard > 3) {
      continue;
    }
    expectedA.push_back(row < 64 ? std::nullopt : std::make_optional(row));
    expectedGuard.push_back(guard);
  }

  auto [result, rowsScanned] = readWithSpec(data, spec, kSize);
  EXPECT_EQ(kSize, rowsScanned);
  test::assertEqualVectors(
      makeRowVector(
          {"a", "guard"},
          {makeNullableFlatVector<int64_t>(expectedA),
           makeFlatVector<int64_t>(expectedGuard)}),
      result);

  auto row = result->asUnchecked<RowVector>();
  assertHasNullBuffer(row->childAt(0));
}

TEST_F(E2EFilterTest, isNotNullFilterOnlyFiltersRows) {
  auto data = makeRowVector(
      {"a", "b"},
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 3, 4}),
       makeFlatVector<int64_t>({10, 20, 30, 40})});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  auto* aSpec = spec->getOrCreateChild("a");
  aSpec->setFilter(std::make_unique<common::IsNotNull>());
  aSpec->setProjectOut(false);
  spec->addField("b", 0);

  auto [result, rowsScanned] =
      readWithSpec(data, spec, 4, ROW({"b"}, {BIGINT()}));
  EXPECT_EQ(4, rowsScanned);
  test::assertEqualVectors(
      makeRowVector({"b"}, {makeFlatVector<int64_t>({10, 30, 40})}), result);
}

TEST_F(E2EFilterTest, nonIsNotNullFilterDoesNotUseOutputElision) {
  auto data = makeRowVector(
      {"a"},
      {makeFlatVector<int64_t>(
          6, [](auto row) { return row; }, [](auto row) { return row == 4; })});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("a", 0)->setFilter(
      std::make_unique<common::BigintRange>(0, 3, false));

  auto [result, rowsScanned] = readWithSpec(data, spec, 6);
  EXPECT_EQ(6, rowsScanned);
  test::assertEqualVectors(
      makeRowVector({"a"}, {makeFlatVector<int64_t>({0, 1, 2, 3})}), result);
}

TEST_F(E2EFilterTest, DISABLED_date) {
  testWithTypes(
      "date_val:date",
      [&]() {
        makeIntDistribution<int32_t>(
            "date_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -999, // rareMin
            30000, // rareMax
            true); // keepNulls
      },
      false,
      {"date_val"},
      20);
}

TEST_F(E2EFilterTest, combineRowGroup) {
  rowsInRowGroup_ = 5;
  rowType_ = ROW({"c0"}, {INTEGER()});
  std::vector<RowVectorPtr> batches;
  for (int i = 0; i < 5; i++) {
    batches.push_back(std::static_pointer_cast<RowVector>(
        test::BatchMaker::createBatch(rowType_, 1, *leafPool_, nullptr, 0)));
  }
  writeToMemory(rowType_, batches, false);
  std::string_view data(sinkPtr_->data(), sinkPtr_->size());
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(data), readerOpts.getMemoryPool());
  auto reader = makeReader(readerOpts, std::move(input));
  auto parquetReader = dynamic_cast<ParquetReader&>(*reader.get());
  EXPECT_EQ(parquetReader.fileMetaData().numRowGroups(), 1);
  EXPECT_EQ(parquetReader.numberOfRows(), 5);
}

TEST_F(E2EFilterTest, writeDecimalAsInteger) {
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>({1, 2}, DECIMAL(8, 2)),
       makeFlatVector<int64_t>({1, 2}, DECIMAL(10, 2)),
       makeFlatVector<int64_t>({1, 2}, DECIMAL(19, 2))});
  writeToMemory(rowVector->type(), {rowVector}, false);
  std::string_view data(sinkPtr_->data(), sinkPtr_->size());
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(data), readerOpts.getMemoryPool());
  auto reader = makeReader(readerOpts, std::move(input));
  auto parquetReader = dynamic_cast<ParquetReader&>(*reader.get());

  auto types = parquetReader.typeWithId()->getChildren();
  auto c0 = std::dynamic_pointer_cast<const ParquetTypeWithId>(types[0]);
  EXPECT_EQ(c0->parquetType_.value(), thrift::Type::type::INT32);
  auto c1 = std::dynamic_pointer_cast<const ParquetTypeWithId>(types[1]);
  EXPECT_EQ(c1->parquetType_.value(), thrift::Type::type::INT64);
  auto c2 = std::dynamic_pointer_cast<const ParquetTypeWithId>(types[2]);
  EXPECT_EQ(c2->parquetType_.value(), thrift::Type::type::FIXED_LEN_BYTE_ARRAY);
}

TEST_F(E2EFilterTest, configurableWriteSchema) {
  auto test = [&](auto& type, auto& newType) {
    std::vector<RowVectorPtr> batches;
    for (auto i = 0; i < 5; i++) {
      auto vector = BaseVector::create(type, 100, pool());
      auto rowVector = std::dynamic_pointer_cast<RowVector>(vector);
      batches.push_back(rowVector);
    }

    writeToMemory(newType, batches, false);
    std::string_view data(sinkPtr_->data(), sinkPtr_->size());
    dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    auto input = std::make_unique<BufferedInput>(
        std::make_shared<InMemoryReadFile>(data), readerOpts.getMemoryPool());
    auto reader = makeReader(readerOpts, std::move(input));
    auto parquetReader = dynamic_cast<ParquetReader&>(*reader.get());

    EXPECT_EQ(parquetReader.rowType()->toString(), newType->toString());
  };

  // ROW(ROW(ROW))
  auto type =
      ROW({"a", "b"}, {INTEGER(), ROW({"c"}, {ROW({"d"}, {INTEGER()})})});
  auto newType =
      ROW({"aa", "bb"}, {INTEGER(), ROW({"cc"}, {ROW({"dd"}, {INTEGER()})})});
  test(type, newType);

  // ARRAY(ROW)
  type =
      ROW({"a", "b"}, {ARRAY(ROW({"c", "d"}, {BIGINT(), BIGINT()})), BIGINT()});
  newType = ROW(
      {"aa", "bb"}, {ARRAY(ROW({"cc", "dd"}, {BIGINT(), BIGINT()})), BIGINT()});
  test(type, newType);

  // // MAP(ROW)
  type =
      ROW({"a", "b"},
          {MAP(ROW({"c", "d"}, {BIGINT(), BIGINT()}),
               ROW({"e", "f"}, {BIGINT(), BIGINT()})),
           BIGINT()});
  newType =
      ROW({"aa", "bb"},
          {MAP(ROW({"cc", "dd"}, {BIGINT(), BIGINT()}),
               ROW({"ee", "ff"}, {BIGINT(), BIGINT()})),
           BIGINT()});
  test(type, newType);
}
