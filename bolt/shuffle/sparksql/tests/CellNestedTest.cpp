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

#include <unistd.h>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <random>

#include <arrow/io/memory.h>
#include <fmt/format.h>

#include "bolt/shuffle/sparksql/BoltShuffleReader.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/tests/CellNestedTestUtils.h"
#include "bolt/shuffle/sparksql/tests/CellTestUtils.h"
#include "bolt/vector/LazyVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

/// ARRAY, MAP and ROW columns through the Cell shuffle, seen only from the
/// outside: rows into CellShuffleWriter, every partition's bytes out
/// through CellShuffleReader, compared with the input, and fuzzed schemas
/// with V1's output as well. Every partition reads back its rows, in order
/// and with every map's entries in their order, whatever the nesting, the
/// nulls at each level, the input's encodings, the partition count, codec,
/// output, spills, seals and read batch sizes. Besides the rows, the tests
/// check what the shuffle shows of itself: which way a schema takes - a
/// nested layout, or the adapter's binary column - the batches the reader
/// returns, the writer's and the reader's metrics, and the memory the
/// writer holds.
namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

using test::Encoding;
using test::NestedVectorMaker;
using test::ReadShape;
using test::ShuffleInput;

/// "P=<partitions> nulls a% b% ..." for a list of per-level null rates.
std::string nullTrace(
    int32_t numPartitions,
    const std::vector<uint32_t>& rates) {
  std::string trace = fmt::format("P={} nulls", numPartitions);
  for (const auto rate : rates) {
    trace += fmt::format(" {}%", rate);
  }
  return trace;
}

class CellNestedTest : public testing::Test, public bolt::test::VectorTestBase {
 protected:
  /// Whole batches and a row per append.
  static inline const std::vector<ReadShape> kFewerReadShapes{
      {4096, 1 << 20},
      {100, 1}};

  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    char pathTemplate[] = "/tmp/bolt_cell_nested_test_XXXXXX";
    ASSERT_NE(::mkdtemp(pathTemplate), nullptr);
    dir_ = pathTemplate;
    dataFile_ = dir_ + "/data";
  }

  void TearDown() override {
    std::filesystem::remove_all(dir_);
  }

  void removeFiles() {
    test::removeShuffleFiles(dataFile_);
  }

  /// A hash shuffle of `numPartitions` partitions into the data file, its
  /// old files removed, or pushed to a RecordingRssClient when `rss`.
  ShuffleWriterOptions writerOptions(
      int32_t numPartitions,
      bool rss = false,
      arrow::Compression::type compression = arrow::Compression::UNCOMPRESSED) {
    removeFiles();
    return test::shuffleOptions(dataFile_, numPartitions, compression, rss);
  }

  /// What V1 reads back of `input`, per partition: the reference that the
  /// Cell output must match besides the input.
  std::vector<std::vector<RowVectorPtr>> v1Partitions(
      const ShuffleInput& input,
      int32_t numPartitions) {
    constexpr int64_t kMemLimit = int64_t{1} << 30;
    removeFiles();
    ShuffleWriterOptions options;
    options.partitioning = Partitioning::kHash;
    options.forceShuffleWriterType =
        static_cast<int32_t>(ShuffleWriterType::V1);
    options.partitionWriterOptions.numPartitions = numPartitions;
    options.partitionWriterOptions.dataFile = dataFile_;
    options.partitionWriterOptions.configuredDirs = {dir_};
    options.partitionWriterOptions.numSubDirs = 1;
    options.partitionWriterOptions.compressionType =
        arrow::Compression::LZ4_FRAME;
    const auto rowType = asRowType(input.data[0]->type());
    std::vector<int64_t> lengths;
    {
      auto writer = BoltShuffleWriter::createShuffleWriter(
          options,
          rowType->size(),
          input.data[0]->size(),
          input.data[0]->estimateFlatSize(),
          kMemLimit,
          pool(),
          arrow::default_memory_pool());
      for (size_t batch = 0; batch < input.data.size(); ++batch) {
        const auto& data = input.data[batch];
        if (data->size() == 0) {
          continue;
        }
        // V1 flattens its input in place, nested vectors included: it gets
        // a flat copy, and the input keeps its encodings.
        std::vector<VectorPtr> children;
        for (const auto& child : data->children()) {
          children.push_back(
              BaseVector::copy(*BaseVector::loadedVectorShared(child)));
        }
        const auto status = writer->split(
            test::withPid(
                input.pids[batch],
                std::make_shared<RowVector>(
                    pool(), rowType, nullptr, data->size(), children),
                pool()),
            kMemLimit);
        BOLT_CHECK(status.ok(), "{}", status.ToString());
      }
      const auto status = writer->stop();
      BOLT_CHECK(status.ok(), "{}", status.ToString());
      lengths = writer->metrics().partitionLengths;
    }
    std::ifstream in(dataFile_, std::ios::binary);
    const std::string file(
        (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    BoltColumnarBatchDeserializerFactory factory(
        boltTypeToArrowSchema(rowType, pool()),
        test::lz4Codec(),
        rowType,
        4096,
        1 << 20,
        arrow::default_memory_pool(),
        pool());
    factory.setShuffleWriterType(static_cast<int32_t>(ShuffleWriterType::V1));
    factory.setNumPartitions(numPartitions);
    factory.setpartitioningShortName("hash");
    std::vector<std::vector<RowVectorPtr>> partitions(numPartitions);
    int64_t offset = 0;
    for (int32_t pid = 0; pid < numPartitions; ++pid) {
      if (lengths[pid] == 0) {
        continue;
      }
      BOLT_CHECK_LE(static_cast<size_t>(offset + lengths[pid]), file.size());
      auto deserializer =
          factory.createDeserializer(std::make_shared<arrow::io::BufferReader>(
              reinterpret_cast<const uint8_t*>(file.data()) + offset,
              lengths[pid]));
      offset += lengths[pid];
      while (auto batch = deserializer->next()) {
        partitions[pid].push_back(std::move(batch));
      }
    }
    return partitions;
  }

  /// test::roundTrip() of `input` through this test's data file.
  void roundTrip(
      const ShuffleInput& input,
      int32_t numPartitions,
      const std::function<void(CellShuffleOptions&, bool rss)>& tune = nullptr,
      const std::function<void(const test::WrittenShuffle&, bool rss)>& check =
          nullptr,
      const std::vector<ReadShape>& readShapes = test::kReadShapes) {
    test::roundTrip(
        input, numPartitions, dataFile_, pool(), tune, check, readShapes);
  }

  /// Holds the data file and, should V1 spill, its spill files.
  std::string dir_;
  std::string dataFile_;
};

TEST_F(CellNestedTest, nestedRoundTrips) {
  // Each case's shuffle, written to both outputs, reads back every
  // partition's rows at both read shapes, or at the case's own.
  struct Case {
    std::string name;
    int32_t numPartitions;
    std::function<ShuffleInput()> input;
    std::vector<ReadShape> readShapes{test::kReadShapes};
  };
  std::vector<Case> cases;
  // A case per list of per-level null rates of `rates`: batches of `rows`
  // rows of `type` from `options`, the i-th from seed `seed` + i.
  const auto atNullRates = [&](const std::string& name,
                               const RowTypePtr& type,
                               NestedVectorMaker::Options options,
                               const std::vector<std::vector<uint32_t>>& rates,
                               const std::vector<vector_size_t>& rows,
                               int32_t numPartitions,
                               uint64_t seed) {
    for (size_t i = 0; i < rates.size(); ++i) {
      options.nullPercent = rates[i];
      cases.push_back(
          {fmt::format(
               "{}, {}: {}",
               name,
               nullTrace(numPartitions, rates[i]),
               type->toString()),
           numPartitions,
           [=, this] {
             return test::makeInput(
                 type, options, rows, numPartitions, seed + i, pool());
           }});
    }
  };

  // Every leaf type in an array, as the values of a map and as a struct
  // field, beside flat columns, and without the struct; no, some, half and
  // every row null at every level.
  for (const bool withRow : {true, false}) {
    const auto leaves = test::leafTypes();
    std::vector<std::string> names{"id", "s"};
    std::vector<TypePtr> types{BIGINT(), VARCHAR()};
    std::vector<std::string> fieldNames;
    for (uint32_t i = 0; i < leaves.size(); ++i) {
      names.push_back(fmt::format("a{}", i));
      types.push_back(ARRAY(leaves[i]));
      names.push_back(fmt::format("m{}", i));
      types.push_back(MAP(INTEGER(), leaves[i]));
      fieldNames.push_back(fmt::format("f{}", i));
    }
    if (withRow) {
      names.push_back("r");
      types.push_back(ROW(std::move(fieldNames), std::vector<TypePtr>(leaves)));
    }
    atNullRates(
        "every leaf type",
        ROW(std::move(names), std::move(types)),
        {},
        {{0}, {10}, {50}, {100}},
        {600, 1, 0, 900},
        7,
        0);
  }

  // Arrays, maps and structs four deep, alone and mixed, and arrays and
  // maps alone, at per-level null rates from the top: at one partition, at
  // 7 and at a few rows in each of 1024.
  NestedVectorMaker::Options shallow;
  shallow.maxLength = 3;
  for (const auto& type : {test::deepRowType(), test::deepNestedType()}) {
    atNullRates(
        "four levels",
        type,
        shallow,
        {{10}, {0, 100}, {50, 0, 10, 0, 50}},
        {800, 3, 1200},
        7,
        100);
    atNullRates(
        "four levels",
        type,
        shallow,
        {{50, 0, 10, 0, 50}},
        {800, 3, 1200},
        1,
        200);
    atNullRates(
        "four levels", type, shallow, {{10}}, {800, 3, 1200}, 1024, 200);
  }

  // Containers all empty; every null container keeping its elements;
  // containers all null, or full of null elements; nine in ten empty, the
  // rest with nulls inside.
  NestedVectorMaker::Options empty;
  empty.maxLength = 0;
  NestedVectorMaker::Options sizedNulls;
  sizedNulls.nullPercent = {50};
  sizedNulls.sizedNullPercent = 100;
  sizedNulls.emptyPercent = 0;
  NestedVectorMaker::Options allNull;
  allNull.nullPercent = {100};
  allNull.sizedNullPercent = 100;
  NestedVectorMaker::Options nullElements;
  nullElements.nullPercent = {0, 100};
  nullElements.emptyPercent = 0;
  NestedVectorMaker::Options sparse;
  sparse.nullPercent = {10, 50};
  sparse.emptyPercent = 90;
  for (const auto& type : {test::deepRowType(), test::deepNestedType()}) {
    for (const auto& [name, options] :
         std::vector<std::pair<const char*, NestedVectorMaker::Options>>{
             {"empty", empty},
             {"sized nulls", sizedNulls},
             {"all null", allNull},
             {"null elements", nullElements},
             {"sparse", sparse}}) {
      cases.push_back(
          {fmt::format("{} containers: {}", name, type->toString()),
           7,
           [=, this] {
             return test::makeInput(type, options, {500, 700}, 7, 3, pool());
           }});
    }
  }

  // Top-level columns flat, in dictionaries with nulls of their own,
  // constant, constant null, sliced and lazy, a different one per batch and
  // column; inside, half the vectors wrapped and half the containers
  // overlapping and out of order.
  NestedVectorMaker::Options wrapped;
  wrapped.nullPercent = {10, 20, 10};
  wrapped.wrapPercent = 50;
  wrapped.scatteredPercent = 50;
  const auto everyEncoding = [](uint32_t batch, uint32_t col) {
    return static_cast<Encoding>(
        (batch + col) % static_cast<uint32_t>(Encoding::kCount));
  };
  for (const auto& type : {test::deepRowType(), test::deepNestedType()}) {
    for (const int32_t numPartitions : {1, 7}) {
      cases.push_back(
          {fmt::format(
               "wrapped and overlapping, P={}: {}",
               numPartitions,
               type->toString()),
           numPartitions,
           [=, this] {
             return test::makeInput(
                 type,
                 wrapped,
                 {300, 500, 1, 700, 400, 600, 800},
                 numPartitions,
                 21,
                 pool(),
                 everyEncoding);
           }});
    }
  }

  // Rows of tens of thousands of elements among small ones, read back one
  // row and one append at a time as well.
  cases.push_back(
      {"large containers",
       7,
       [this] {
         const auto type =
             ROW({"id", "values", "map"},
                 {BIGINT(), ARRAY(BIGINT()), MAP(VARCHAR(), INTEGER())});
         NestedVectorMaker::Options small;
         small.nullPercent = {5};
         small.maxLength = 2;
         auto input = test::makeInput(type, small, {100, 100}, 7, 77, pool());
         NestedVectorMaker::Options large;
         large.nullPercent = {0, 5};
         large.maxLength = 30'000;
         large.emptyPercent = 0;
         const auto big = test::makeInput(type, large, {6}, 7, 78, pool());
         input.data.insert(input.data.begin() + 1, big.data[0]);
         input.pids.insert(input.pids.begin() + 1, big.pids[0]);
         return input;
       },
       {{4096, 1 << 20}, {1, 1 << 20}, {1000, 1}}});

  // Containers of up to 150000 elements among small ones: an entry's
  // elements go into several of the writer's chunks, a chunk holds the
  // elements of many entries, and a batch of a space more positions than
  // one append takes. Partition 0 takes both large rows. Read back in
  // batches of every row, of one row, of one append and with no byte
  // target.
  cases.push_back(
      {"elements past a chunk",
       7,
       [this] {
         const auto type =
             ROW({"id", "values", "map", "nested"},
                 {BIGINT(),
                  ARRAY(BIGINT()),
                  MAP(INTEGER(), BIGINT()),
                  ARRAY(ARRAY(SMALLINT()))});
         NestedVectorMaker::Options small;
         small.nullPercent = {5, 5, 5};
         small.maxLength = 3;
         auto input = test::makeInput(type, small, {300, 300}, 7, 81, pool());
         const std::vector<vector_size_t> sizes{150'000, 70'000, 1};
         // The nested column's rows hold 100000 arrays of one element each.
         const auto inner = makeArrayVector<int16_t>(
             300'000,
             [](auto) { return 1; },
             [](auto idx) { return static_cast<int16_t>(idx % 30'011); },
             nullptr,
             [](auto idx) { return idx % 13 == 0; });
         const auto big = makeRowVector(
             {"id", "values", "map", "nested"},
             {makeFlatVector<int64_t>(3, [](auto row) { return row * 101; }),
              makeArrayVector<int64_t>(
                  3,
                  [&](auto row) { return sizes[row]; },
                  [](auto idx) { return idx * 7919; },
                  nullptr,
                  [](auto idx) { return idx % 11 == 0; }),
              makeMapVector<int32_t, int64_t>(
                  3,
                  [](auto row) { return row == 0 ? 80'000 : 1; },
                  [](auto idx) { return static_cast<int32_t>(idx); },
                  [](auto idx) { return idx * 3; }),
              makeArrayVector({0, 100'000, 200'000}, inner)});
         input.data.insert(input.data.begin() + 1, big);
         input.pids.insert(input.pids.begin() + 1, {0, 0, 1});
         return input;
       },
       {{4096, 1 << 30}, {1, 1 << 20}, {1000, 1}, {1000, 0}}});

  // Every nanosecond of a timestamp, at any depth, as a flat column keeps
  // it: in arrays and in the arrays of maps' values, and at the top level
  // of a struct, in a struct below it and in structs in an array.
  cases.push_back(
      {"nanoseconds in containers", 3, [this] {
         const auto values = test::nanoTimestamps(9);
         const auto n = static_cast<vector_size_t>(values.size());
         auto flat = makeFlatVector<Timestamp>(
             n, [&](auto row) { return values[row]; }, nullEvery(7));
         const auto rows = n / 4;
         return test::roundRobin(
             {makeRowVector(
                 {"arr", "map"},
                 {makeArrayVector<Timestamp>(
                      rows,
                      [](auto) { return 4; },
                      [&](auto idx) { return values[idx]; },
                      nullEvery(9)),
                  makeMapVector(
                      test::evenOffsets(rows, 2),
                      makeFlatVector<int32_t>(
                          rows * 2, [](auto idx) { return idx; }),
                      makeArrayVector(test::evenOffsets(rows * 2, 2), flat))})},
             3);
       }});
  cases.push_back(
      {"nanoseconds in structs", 3, [this] {
         const auto values = test::nanoTimestamps(11);
         const auto rows = static_cast<vector_size_t>(values.size());
         const auto stamps = [&](vector_size_t size, vector_size_t shift) {
           return makeFlatVector<Timestamp>(
               size,
               [&, shift](auto row) { return values[(row + shift) % rows]; },
               nullEvery(9));
         };
         return test::roundRobin(
             {makeRowVector(
                 {"r", "ar"},
                 {makeRowVector(
                      {"t", "n"},
                      {stamps(rows, 0),
                       makeRowVector({"u"}, {stamps(rows, 1)}, nullEvery(5))},
                      nullEvery(3)),
                  makeArrayVector(
                      test::evenOffsets(rows, 2),
                      makeRowVector(
                          {"t"}, {stamps(2 * rows, 2)}, nullEvery(4)))})},
             3);
       }});

  // Structs null in no, a tenth, half and every row against fields null at
  // the same rates, fields of every kind - a number, a string, an array, a
  // map and a struct - at the top level and as an array's elements: a null
  // struct reads back null whatever its fields hold, and a container below
  // it holds no element.
  const auto fields =
      ROW({"a", "s", "l", "m", "n"},
          {INTEGER(),
           VARCHAR(),
           ARRAY(BIGINT()),
           MAP(INTEGER(), VARCHAR()),
           ROW({"x", "t"}, {DOUBLE(), VARCHAR()})});
  for (const bool inArray : {false, true}) {
    const auto type = inArray ? ROW({"id", "ar"}, {BIGINT(), ARRAY(fields)})
                              : ROW({"id", "r"}, {BIGINT(), fields});
    for (const uint32_t structNulls : {0, 10, 50, 100}) {
      for (const uint32_t fieldNulls : {0, 10, 50, 100}) {
        NestedVectorMaker::Options options;
        // From the top: the structs' level, then their fields'.
        options.nullPercent = inArray
            ? std::vector<uint32_t>{0, structNulls, fieldNulls}
            : std::vector<uint32_t>{structNulls, fieldNulls};
        options.maxLength = 3;
        cases.push_back(
            {fmt::format(
                 "structs {}% null, fields {}%{}",
                 structNulls,
                 fieldNulls,
                 inArray ? ", in an array" : ""),
             7,
             [=, this] {
               return test::makeInput(
                   type,
                   options,
                   {250, 350},
                   7,
                   500 + 7 * structNulls + fieldNulls,
                   pool());
             }});
      }
    }
  }

  // Structs in structs eight levels deep, a number and a string beside each
  // level's struct and an array at the bottom: a struct null at any level
  // nulls every column below it, whatever each level's null rate.
  TypePtr chain = ROW({"v", "a"}, {INTEGER(), ARRAY(BIGINT())});
  for (int level = 6; level >= 0; --level) {
    chain =
        ROW({fmt::format("v{}", level), fmt::format("s{}", level), "next"},
            {BIGINT(), VARCHAR(), chain});
  }
  atNullRates(
      "eight levels",
      ROW({"id", "chain"}, {BIGINT(), chain}),
      shallow,
      {{0, 0, 0, 0, 0, 0, 0, 100}, {0, 10, 0, 10, 0, 10, 0, 10, 50}},
      {600, 900},
      7,
      600);

  // Arrays of structs, a struct of an array and a map, maps of structs
  // holding arrays of structs, and maps whose keys and values are both
  // structs: a struct below a container has the positions of its elements,
  // a container below a struct those of the struct. Plain, and with half
  // the vectors inside wrapped and half the containers overlapping and out
  // of order.
  const auto around = ROW(
      {"ar", "ra", "mr", "rk"},
      {ARRAY(ROW({"a", "s"}, {INTEGER(), VARCHAR()})),
       ROW({"l", "m"}, {ARRAY(BIGINT()), MAP(INTEGER(), ARRAY(VARCHAR()))}),
       MAP(VARCHAR(),
           ROW({"x", "y"}, {DOUBLE(), ARRAY(ROW({"z"}, {SMALLINT()}))})),
       MAP(ROW({"k", "t"}, {INTEGER(), VARCHAR()}), ROW({"v"}, {BIGINT()}))});
  for (const bool wrap : {false, true}) {
    NestedVectorMaker::Options options;
    options.maxLength = 4;
    if (wrap) {
      options.wrapPercent = 50;
      options.scatteredPercent = 50;
    }
    atNullRates(
        wrap ? "structs and containers, wrapped" : "structs and containers",
        around,
        options,
        {{10}, {50, 10, 50, 10}, {0, 100}},
        {600, 1, 900},
        7,
        700 + 10 * wrap);
  }

  // An UNKNOWN field is a column of the layout with none on the wire, so
  // the layout's and the wire's column numbers part after it: at the top
  // level, as an array's elements and as a map's values; a struct of
  // UNKNOWN fields alone carries only its presence.
  atNullRates(
      "unknown fields",
      ROW({"r", "ar", "m", "o"},
          {ROW({"u", "a"}, {UNKNOWN(), INTEGER()}),
           ARRAY(ROW({"u", "a", "s"}, {UNKNOWN(), INTEGER(), VARCHAR()})),
           MAP(INTEGER(), ROW({"a", "u"}, {BIGINT(), UNKNOWN()})),
           ROW({"u", "w"}, {UNKNOWN(), UNKNOWN()})}),
      {},
      {{10}, {50, 50}},
      {500, 800},
      7,
      800);

  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    roundTrip(c.input(), c.numPartitions, nullptr, nullptr, c.readShapes);
    if (HasFatalFailure()) {
      return;
    }
  }
}

TEST_F(CellNestedTest, adapterWhenNestedTypesAreNotNative) {
  // With nativeNestedTypes off on both sides, every nested schema goes
  // through the adapter's binary column, converted and not shredded, and
  // reads back as it does in a nested layout - batches of 3000 rows and
  // batches of large rows, a struct of three 3 MiB strings and a row of
  // 64 MiB + 1 beside one of 3 MiB, alike.
  const auto notNative = [](CellShuffleOptions& cell, bool) {
    cell.nativeNestedTypes = false;
  };
  const auto converted = [](const test::WrittenShuffle& written, bool) {
    EXPECT_GT(written.metrics.convertTime, 0);
    EXPECT_EQ(written.metrics.flattenTime, 0);
  };
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20, 10};
  options.wrapPercent = 50;
  options.scatteredPercent = 50;
  for (const auto& type : {test::deepRowType(), test::deepNestedType()}) {
    SCOPED_TRACE(type->toString());
    ASSERT_TRUE(CellNestedLayout::supports(type));
    const auto input =
        test::makeInput(type, options, {300, 1, 700, 3000}, 7, 23, pool());
    roundTrip(input, 7, notNative, converted);
  }
  // Every row in one partition: its batches hold the large rows together.
  const std::string text(3 << 20, 'x');
  const std::string oversized((64 << 20) + 1, 'z');
  const auto structsOf = [&](const std::vector<StringView>& strings) {
    return makeRowVector(
        {"r"}, {makeRowVector({"s"}, {makeFlatVector<StringView>(strings)})});
  };
  roundTrip(
      test::roundRobin(
          {structsOf({StringView(text), StringView(text), StringView(text)}),
           structsOf({StringView(oversized), StringView(text)})},
          1),
      1,
      notNative,
      converted);
}

TEST_F(CellNestedTest, spillsAndSeals) {
  // Enough rows for the writer to seal payloads past a small size: of every
  // partition, of partition 0 alone, and together with spills under a
  // small cap and windows sealed past a small null memory; on an RSS
  // client a seal pushes its payloads. Half the rows go to partition 0,
  // whose payload passes a bound well before the others' do: past a bound
  // only it reaches, its payloads close while the others' stay open, which
  // the reader shows as more payloads - a batch each at a byte target of
  // zero - in partition 0 than in some other partition.
  const auto withRow =
      ROW({"id", "s", "tags", "attrs", "st"},
          {BIGINT(),
           VARCHAR(),
           ARRAY(VARCHAR()),
           MAP(INTEGER(), ARRAY(BIGINT())),
           ROW({"a", "b", "c"}, {INTEGER(), VARCHAR(), ARRAY(DOUBLE())})});
  // Structs, null at a tenth of the rows and a fifth of the elements, around
  // and inside containers.
  const auto structs = ROW(
      {"id", "r", "items"},
      {BIGINT(),
       ROW({"x", "m"}, {VARCHAR(), MAP(INTEGER(), ROW({"y"}, {DOUBLE()}))}),
       ARRAY(ROW({"a", "s", "l"}, {INTEGER(), VARCHAR(), ARRAY(BIGINT())}))});
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20};
  options.maxLength = 6;
  const auto lz4 = test::lz4Codec();
  for (const auto& type : {withRow, structs}) {
    SCOPED_TRACE(type->toString());
    auto input = test::makeInput(
        type, options, std::vector<vector_size_t>(12, 2048), 7, 41, pool());
    for (auto& pids : input.pids) {
      for (size_t row = 0; row < pids.size(); row += 2) {
        pids[row] = 0;
      }
    }
    struct Limit {
      const char* name;
      std::function<void(CellShuffleOptions&, bool rss)> tune;
      /// Partition 0's payloads close while some other partition's stay
      /// open, locally and on an RSS client.
      bool partial{false};
    };
    const std::vector<Limit> limits = {
        {"partition seals",
         [](CellShuffleOptions& cell, bool) {
           cell.checkpointPartitionBytes = 1;
         }},
        {"seals of partition 0",
         [](CellShuffleOptions& cell, bool rss) {
           cell.checkpointPartitionBytes = rss ? 128 << 10 : 1 << 20;
         },
         true},
        // Every limit at once: locally growth spills and a null seal per
        // batch, on RSS a push at the cap per batch, each closing the
        // window before partition 0's payload passes its bound.
        {"all", [](CellShuffleOptions& cell, bool rss) {
           cell.cellMemoryCapBytes = 4 << 20;
           cell.checkpointPartitionBytes = rss ? 128 << 10 : 1 << 20;
           cell.nullMemLimitBytes = 4 << 10;
         }}};
    for (const auto& limit : limits) {
      SCOPED_TRACE(limit.name);
      roundTrip(
          input,
          7,
          limit.tune,
          [&](const test::WrittenShuffle& written, bool rss) {
            const auto& metrics = written.metrics;
            EXPECT_GT(metrics.spillCount, 0);
            if (!rss) {
              EXPECT_GT(metrics.totalBytesEvicted, 0);
            }
            if (!limit.partial) {
              return;
            }
            std::vector<size_t> payloads;
            for (const auto& bytes : written.partitions) {
              payloads.push_back(test::readerBatchSizes(
                                     bytes,
                                     type,
                                     rss ? lz4.get() : nullptr,
                                     pool(),
                                     32 << 10,
                                     0)
                                     .size());
            }
            EXPECT_GT(payloads[0], 1);
            EXPECT_LT(
                *std::min_element(payloads.begin(), payloads.end()),
                payloads[0]);
          },
          {{4096, 1 << 20}, {333, 1}});
    }
  }
}

TEST_F(CellNestedTest, readsAtEveryByteBudget) {
  // As for flat payloads, a byte target at or below zero reads one append
  // per batch, and a large one every row up to the batch size, across
  // payloads. A positive one also bounds each append, by the payload's
  // bytes per row, every space's together: a tiny one reads a row per
  // batch, one between them cuts batches by their bytes.
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20};
  roundTrip(
      test::makeInput(
          test::deepNestedType(), options, {700, 900, 300}, 7, 91, pool()),
      7,
      nullptr,
      nullptr,
      {{1000, 0}, {1000, -1}, {64, 1}, {4096, 3000}, {5000, 1 << 30}});

  // Three payloads of 1000 rows of an id and 10 elements each: a row
  // decodes into 8 + 4 bytes at the rows' positions and 10 * 8 at the
  // elements', 92 in all.
  std::vector<RowVectorPtr> batches;
  for (int64_t batch = 0; batch < 3; ++batch) {
    batches.push_back(makeRowVector(
        {"id", "a"},
        {makeFlatVector<int64_t>(
             1000, [&](auto row) { return batch * 1000 + row; }),
         makeArrayVector<int64_t>(
             1000,
             [](auto) { return 10; },
             [&](auto idx) { return batch * 10'000 + idx; })}));
  }
  const auto uniform = test::roundRobin(std::move(batches), 1);
  const auto type = asRowType(uniform.data[0]->type());
  // A payload per batch.
  auto sealEveryBatch = writerOptions(1);
  sealEveryBatch.cellOptions.checkpointPartitionBytes = 1;
  const auto bytes =
      test::writeShuffle(sealEveryBatch, uniform, pool()).partitions[0];
  ASSERT_EQ(
      test::readerBatchSizes(bytes, type, nullptr, pool(), 4096, 0),
      (std::vector<vector_size_t>(3, 1000)));
  const auto expected = uniform.rowsOf(1)[0];
  const auto batchSizes = [&](int32_t batchSize, int64_t budget) {
    const auto batches =
        test::readPartition(bytes, type, nullptr, pool(), batchSize, budget);
    test::expectPartitionRows(expected, batches, 0, batchSize);
    std::vector<vector_size_t> sizes;
    for (const auto& batch : batches) {
      sizes.push_back(batch->size());
    }
    return sizes;
  };
  for (const int64_t budget : {0, -1}) {
    SCOPED_TRACE(budget);
    // One append a batch, within its payload.
    std::vector<vector_size_t> appends;
    for (int payload = 0; payload < 3; ++payload) {
      appends.insert(appends.end(), {300, 300, 300, 100});
    }
    EXPECT_EQ(batchSizes(300, budget), appends);
  }
  // Full batches, across payloads.
  EXPECT_EQ(batchSizes(300, 1 << 30), std::vector<vector_size_t>(10, 300));
  // 920 bytes are about 10 rows, every space's bytes counted: between 2
  // and 20, but for the last batch; 1 byte, one row.
  const auto cut = batchSizes(4096, 920);
  ASSERT_FALSE(cut.empty());
  for (size_t i = 0; i < cut.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_LE(cut[i], 20);
    if (i + 1 < cut.size()) {
      EXPECT_GE(cut[i], 2);
    }
  }
  EXPECT_EQ(batchSizes(4096, 1), std::vector<vector_size_t>(3000, 1));
}

TEST_F(CellNestedTest, compositeBlocksBetweenPayloads) {
  // Aggregation state rows between ordinary batches of a nested layout: a
  // composite block ends the batch being assembled and comes out on its
  // own, and the payload after it starts the next batch. Each kind's rows
  // read back in order, locally and from an RSS client, at every read
  // shape.
  const auto type =
      ROW({"k", "a", "m"},
          {BIGINT(), ARRAY(BIGINT()), MAP(INTEGER(), ARRAY(VARCHAR()))});
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20, 10};
  options.maxLength = 5;
  constexpr int32_t kPartitions = 3;
  // Whether each batch goes in as composite rows.
  const std::vector<bool> composite{
      false, true, false, false, true, true, false};
  std::vector<vector_size_t> sizes;
  for (size_t batch = 0; batch < composite.size(); ++batch) {
    sizes.push_back(300 + 100 * static_cast<vector_size_t>(batch));
  }
  const auto input =
      test::makeInput(type, options, sizes, kPartitions, 5, pool());
  // Per kind, ordinary first, and partition: the rows in order.
  std::vector<std::vector<test::RowRef>> expected[2];
  for (auto& rows : expected) {
    rows.resize(kPartitions);
  }
  for (size_t batch = 0; batch < composite.size(); ++batch) {
    for (vector_size_t row = 0; row < input.data[batch]->size(); ++row) {
      expected[composite[batch]][input.pids[batch][row]].emplace_back(
          input.data[batch].get(), row);
    }
  }
  const auto batchOf = [&](size_t batch) -> RowVectorPtr {
    if (composite[batch]) {
      return test::makeComposite(input.pids[batch], input.data[batch], pool());
    }
    return test::withPid(input.pids[batch], input.data[batch], pool());
  };
  for (const bool rss : {false, true}) {
    SCOPED_TRACE(rss ? "rss" : "local");
    const auto written = test::writeShuffle(
        writerOptions(kPartitions, rss), input, pool(), batchOf);
    EXPECT_GT(written.metrics.flattenTime, 0);
    for (const auto& shape : std::vector<ReadShape>{
             {4096, 1 << 30}, {1000, 0}, {7, 1}, {250, 2000}}) {
      SCOPED_TRACE(fmt::format(
          "batches of {} rows, {} bytes",
          shape.batchSize,
          shape.batchByteSize));
      for (int32_t pid = 0; pid < kPartitions; ++pid) {
        const auto reader = test::makeReader(
            written.partitions[pid],
            type,
            nullptr,
            pool(),
            shape.batchSize,
            shape.batchByteSize);
        std::vector<RowVectorPtr> batches[2];
        while (auto batch = reader->next()) {
          if (RowVector::isComposite(batch)) {
            ASSERT_LE(batch->size(), shape.batchSize);
            batches[1].push_back(test::compositeRows(type, batch, pool()));
          } else {
            batch->validate({});
            batches[0].push_back(std::move(batch));
          }
        }
        test::expectPartitionRows(
            expected[0][pid], batches[0], pid, shape.batchSize);
        {
          SCOPED_TRACE("composite rows");
          test::expectPartitionRows(
              expected[1][pid], batches[1], pid, shape.batchSize);
        }
        if (HasFatalFailure()) {
          return;
        }
      }
    }
  }
}

TEST_F(CellNestedTest, dictionaryProbeTakesTheRowsStrings) {
  // A string column at the rows' positions goes into dictionary form as in
  // a flat schema, and so do the string fields of top-level structs, at any
  // depth, null where their struct is; the strings at positions of their
  // own - in arrays, and in the structs in an array - are never probed:
  // every dictionary row is one of the rows'.
  constexpr vector_size_t kRows = 4096;
  const char* const kWords[] = {"red", "green", "blue", "cyan"};
  const auto words = [&](vector_size_t size, int shift) {
    return makeFlatVector<StringView>(size, [&, shift](auto row) {
      return StringView(kWords[(row + shift) % 4]);
    });
  };
  // Each batch, and the rows of it in dictionary form.
  const std::vector<std::pair<RowVectorPtr, int64_t>> batches{
      {makeRowVector(
           {"s", "tags"},
           {words(kRows, 0),
            makeArrayVector<StringView>(
                kRows,
                [](auto row) { return 1 + row % 3; },
                [&](auto idx) { return StringView(kWords[idx % 4]); })}),
       kRows},
      // The two strings of each struct but the null ones.
      {makeRowVector(
           {"r", "ar"},
           {makeRowVector(
                {"s", "n"},
                {words(kRows, 0), makeRowVector({"t"}, {words(kRows, 1)})},
                nullEvery(7)),
            makeArrayVector(
                test::evenOffsets(kRows, 2),
                makeRowVector({"u"}, {words(2 * kRows, 2)}))}),
       2 * (kRows - (kRows + 6) / 7)}};
  for (const auto& [data, matched] : batches) {
    SCOPED_TRACE(data->type()->toString());
    roundTrip(
        test::roundRobin({data}, 5),
        5,
        nullptr,
        [&](const test::WrittenShuffle& written, bool) {
          EXPECT_EQ(written.metrics.dictionaryMatchedRows, matched);
          EXPECT_EQ(written.metrics.dictionaryFallbackRows, 0);
        },
        {{4096, 1 << 20}});
  }
}

TEST_F(CellNestedTest, elementsPastTheRowLimit) {
  // Rows of 2^30 elements: arrays and maps of UNKNOWN, which carry lengths
  // only and cost nothing to write or read, and arrays of null BOOLEANs,
  // whose space has a column on the wire, 2^30 null bits a row to read
  // back. Two such rows in one partition hold more positions than a
  // payload may. Coming one by one, the writer closes a window before each,
  // however few bytes they take; and a batch holding a payload's elements
  // takes no next payload whose elements would take its space past
  // kMaxPositions, so it ends there. Coming in one batch, they fail the
  // write: no payload carries the batch.
  constexpr vector_size_t kLength = 1 << 30;
  const std::vector<vector_size_t> lengths{kLength, 5, kLength, kLength};
  const auto rows = static_cast<vector_size_t>(lengths.size());
  const auto unknowns =
      BaseVector::createNullConstant(UNKNOWN(), kLength, pool());
  const std::vector<std::pair<const char*, VectorPtr>> columns{
      {"array", test::containersOf(lengths, unknowns, pool())},
      {"map", test::containersOf(lengths, unknowns, pool(), true)},
      {"null BOOLEANs",
       test::containersOf(
           lengths,
           BaseVector::createNullConstant(BOOLEAN(), kLength, pool()),
           pool())}};
  const auto lz4 = test::lz4Codec();
  for (const auto& [name, column] : columns) {
    const auto data = makeRowVector(
        {"id", "c"},
        {makeFlatVector<int64_t>(rows, [](auto row) { return row; }), column});
    const auto type = asRowType(data->type());
    std::vector<RowVectorPtr> rowByRow;
    for (vector_size_t row = 0; row < rows; ++row) {
      rowByRow.push_back(
          std::dynamic_pointer_cast<RowVector>(data->slice(row, 1)));
    }
    for (const auto& split : {std::vector<RowVectorPtr>{data}, rowByRow}) {
      for (const bool rss : {false, true}) {
        SCOPED_TRACE(fmt::format(
            "{} {} {}",
            name,
            split.size() == 1 ? "one batch" : "a batch per row",
            rss ? "rss" : "local"));
        if (split.size() == 1) {
          EXPECT_THROW(
              (void)test::writeShuffle(
                  writerOptions(1, rss, arrow::Compression::LZ4_FRAME),
                  test::roundRobin(split, 1),
                  pool()),
              BoltUserError);
          continue;
        }
        const auto written = test::writeShuffle(
            writerOptions(1, rss, arrow::Compression::LZ4_FRAME),
            test::roundRobin(split, 1),
            pool());
        EXPECT_GT(written.metrics.flattenTime, 0);
        // A payload per window - rows 0 and 1, row 2, row 3 - and, with no
        // byte target, a batch per payload.
        const auto batches = test::readPartition(
            written.partitions[0],
            type,
            lz4.get(),
            pool(),
            4096,
            std::numeric_limits<int64_t>::max());
        ASSERT_EQ(batches.size(), 3);
        const std::vector<vector_size_t> batchRows{2, 1, 1};
        vector_size_t next = 0;
        for (size_t b = 0; b < batches.size(); ++b) {
          ASSERT_EQ(batches[b]->size(), batchRows[b]);
          const auto* ids = batches[b]->childAt(0)->asFlatVector<int64_t>();
          const auto* containers =
              batches[b]->childAt(1)->as<ArrayVectorBase>();
          ASSERT_NE(containers, nullptr);
          uint64_t total = 0;
          for (vector_size_t row = 0; row < batches[b]->size(); ++row, ++next) {
            EXPECT_EQ(ids->valueAt(row), next);
            EXPECT_FALSE(containers->isNullAt(row));
            EXPECT_EQ(containers->sizeAt(row), lengths[next]);
            total += containers->sizeAt(row);
          }
          const auto& values = column->typeKind() == TypeKind::MAP
              ? batches[b]->childAt(1)->as<MapVector>()->mapValues()
              : batches[b]->childAt(1)->as<ArrayVector>()->elements();
          ASSERT_EQ(values->size(), total);
          if (values->typeKind() == TypeKind::BOOLEAN) {
            ASSERT_NE(values->rawNulls(), nullptr);
            EXPECT_EQ(
                BaseVector::countNulls(values->nulls(), 0, values->size()),
                values->size());
          } else {
            EXPECT_TRUE(values->isNullAt(0));
          }
        }
      }
    }
  }
}

TEST_F(CellNestedTest, positionsUpToTheLimit) {
  // A container's space holds kMaxPositions positions in a payload, the
  // most whose null bits one vector holds in whole bytes: a row of that
  // many elements is written and read back whole, a row of one more fails.
  // Two payloads whose elements add up to exactly that share a batch; with
  // one element more, the second starts the next batch.
  constexpr auto kMax =
      static_cast<vector_size_t>(CellNestedLayout::kMaxPositions);
  const auto arrayOf = [&](vector_size_t length) {
    return makeRowVector(
        {"c"},
        {test::containersOf(
            {length},
            BaseVector::createNullConstant(UNKNOWN(), length, pool()),
            pool())});
  };
  const auto type = asRowType(arrayOf(1)->type());
  // One row's partition bytes.
  const auto bytesOf = [&](vector_size_t length) {
    return test::writeShuffle(
               writerOptions(1), test::roundRobin({arrayOf(length)}, 1), pool())
        .partitions[0];
  };
  {
    const auto batches =
        test::readPartition(bytesOf(kMax), type, nullptr, pool(), 4096, 0);
    ASSERT_EQ(batches.size(), 1);
    const auto* arrays = batches[0]->childAt(0)->as<ArrayVector>();
    EXPECT_EQ(arrays->sizeAt(0), kMax);
    EXPECT_EQ(arrays->elements()->size(), kMax);
  }
  {
    CellShuffleWriter writer(
        writerOptions(1), pool(), arrow::default_memory_pool());
    EXPECT_THROW(
        (void)writer.split(test::withPid({0}, arrayOf(kMax + 1), pool()), 0),
        BoltUserError);
  }
  for (const vector_size_t extra : {0, 1}) {
    SCOPED_TRACE(fmt::format("{} past the limit", extra));
    const vector_size_t first = 1 << 30;
    const auto batches = test::readPartition(
        bytesOf(first) + bytesOf(kMax - first + extra),
        type,
        nullptr,
        pool(),
        4096,
        1 << 20);
    ASSERT_EQ(batches.size(), extra == 0 ? 1 : 2);
    EXPECT_EQ(batches[0]->size(), extra == 0 ? 2 : 1);
  }
}

TEST_F(CellNestedTest, constantArraysPastThePositionLimit) {
  // A constant array repeats its elements wherever it is referenced: four
  // references to one of 2^30 elements are 2^32 positions in one row, none
  // in 32 bits; 2^20 references to an array of 2^20 references to one of
  // 2^10 elements are 2^50. The writer counts in 64 bits, stops once past
  // the limit and fails the row, having split nothing of it.
  const auto repeat = [&](vector_size_t times, const VectorPtr& vector) {
    return test::containersOf(
        {times}, BaseVector::wrapInConstant(times, 0, vector), pool());
  };
  const auto leaf = BaseVector::wrapInConstant(
      1 << 30, 0, makeFlatVector<int64_t>(std::vector<int64_t>{7}));
  const std::vector<VectorPtr> columns{
      repeat(4, test::containersOf({1 << 30}, leaf, pool())),
      repeat(
          1 << 20,
          repeat(1 << 20, test::containersOf({1 << 10}, leaf, pool())))};
  for (const auto& column : columns) {
    SCOPED_TRACE(column->type()->toString());
    CellShuffleWriter writer(
        writerOptions(2, false, arrow::Compression::LZ4_FRAME),
        pool(),
        arrow::default_memory_pool());
    const auto data = makeRowVector({"a"}, {column});
    EXPECT_THROW(
        (void)writer.split(test::withPid({1}, data, pool()), 0), BoltUserError);
  }
}

/// Loads `values`, first calling `onLoad`: memory pressure arriving while
/// the writer decodes a batch.
class CallbackLoader final : public VectorLoader {
 public:
  CallbackLoader(VectorPtr values, std::function<void()> onLoad)
      : values_(std::move(values)), onLoad_(std::move(onLoad)) {}

 protected:
  void loadInternal(
      RowSet /*rows*/,
      ValueHook* /*hook*/,
      vector_size_t /*resultSize*/,
      VectorPtr* result) override {
    onLoad_();
    *result = values_;
  }

 private:
  VectorPtr values_;
  std::function<void()> onLoad_;
};

TEST_F(CellNestedTest, pushWhileABatchIsDecoded) {
  // On a remote shuffle, partition 0's window holds kMaxPositions - 100
  // UNKNOWN elements, and the next row brings 200: the window must close
  // before it. A reclaim pushes it before the row comes, or while the
  // writer decodes the row - loading a lazy vector inside a container, the
  // split not begun - and either way the row then goes into the next
  // window, which it fits.
  constexpr auto kBig =
      static_cast<vector_size_t>(CellNestedLayout::kMaxPositions) - 100;
  constexpr vector_size_t kRows = 2000;
  // `rows` arrays of `elements`, the first holding `length` of them.
  const auto arrays =
      [&](vector_size_t rows, vector_size_t length, VectorPtr elements) {
        std::vector<vector_size_t> lengths(rows, 0);
        lengths[0] = length;
        return test::containersOf(lengths, elements, pool());
      };
  for (const bool whileDecoding : {false, true}) {
    SCOPED_TRACE(whileDecoding ? "while decoding the row" : "before the row");
    const auto options = writerOptions(1, true);
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    // 16 MB of strings: a window worth a push.
    const std::string text(8 << 10, 'q');
    const auto first = makeRowVector(
        {"s", "a", "b"},
        {makeFlatVector<StringView>(
             kRows, [&](auto) { return StringView(text); }),
         arrays(
             kRows,
             kBig,
             BaseVector::createNullConstant(UNKNOWN(), kBig, pool())),
         arrays(kRows, 0, makeFlatVector<int64_t>(std::vector<int64_t>{}))});
    ASSERT_TRUE(
        writer
            .split(
                test::withPid(std::vector<int32_t>(kRows, 0), first, pool()), 0)
            .ok());
    int reclaims = 0;
    const auto reclaim = [&] {
      int64_t freed = 0;
      ASSERT_TRUE(writer.reclaimFixedSize(int64_t{1} << 30, &freed).ok());
      ++reclaims;
    };
    VectorPtr values = makeFlatVector<int64_t>({7, 8, 9});
    if (whileDecoding) {
      values = std::make_shared<LazyVector>(
          pool(),
          BIGINT(),
          3,
          std::make_unique<CallbackLoader>(values, reclaim));
    } else {
      reclaim();
    }
    const auto row = makeRowVector(
        {"s", "a", "b"},
        {makeFlatVector<StringView>({"x"}),
         arrays(1, 200, BaseVector::createNullConstant(UNKNOWN(), 200, pool())),
         arrays(1, 3, values)});
    ASSERT_TRUE(writer.split(test::withPid({0}, row, pool()), 0).ok());
    EXPECT_EQ(reclaims, 1);
    ASSERT_TRUE(writer.stop().ok());
    const auto bytes =
        test::partitionBytes(options, writer.metrics().partitionLengths)[0];
    const auto type = asRowType(row->type());
    // The pushed window, then the row's: a batch each at a byte target of
    // zero.
    const auto batches =
        test::readPartition(bytes, type, nullptr, pool(), 4096, 0);
    ASSERT_EQ(batches.size(), 2);
    EXPECT_EQ(batches[0]->size(), kRows);
    ASSERT_EQ(batches[1]->size(), 1);
    EXPECT_EQ(batches[0]->childAt(1)->as<ArrayVector>()->sizeAt(0), kBig);
    const auto& last = batches[1];
    EXPECT_EQ(last->childAt(1)->as<ArrayVector>()->sizeAt(0), 200);
    const auto* b = last->childAt(2)->as<ArrayVector>();
    ASSERT_EQ(b->sizeAt(0), 3);
    const auto* elements = b->elements()->asFlatVector<int64_t>();
    const auto offset = b->offsetAt(0);
    EXPECT_EQ(elements->valueAt(offset), 7);
    EXPECT_EQ(elements->valueAt(offset + 2), 9);
  }
}

TEST_F(CellNestedTest, fuzzedSchemasMatchV1) {
  // Random arrays and maps over every leaf type V1 carries, up to three
  // deep, and then with structs among them, at the top level and at any
  // depth below, from VectorFuzzer in the encodings it makes, lazy ones
  // included: what V1 reads back, the Cell shuffle must.
  std::vector<TypePtr> leaves;
  for (const auto& leaf : test::leafTypes()) {
    // VectorFuzzer makes no UNKNOWN values.
    if (test::v1Carries(leaf) && leaf->kind() != TypeKind::UNKNOWN) {
      leaves.push_back(leaf);
    }
  }
  const std::vector<TypePtr> keys{INTEGER(), BIGINT(), VARCHAR()};
  for (const bool withStructs : {false, true}) {
    std::mt19937_64 rng(withStructs ? 2027 : 2026);
    // A leaf, an array, a map or, `withStructs`, a struct, of `depth` levels
    // at most below it.
    std::function<TypePtr(uint32_t)> randomType =
        [&](uint32_t depth) -> TypePtr {
      const auto pick = rng() % (withStructs ? 5 : 4);
      if (depth == 0 || pick >= (withStructs ? 3 : 2)) {
        return leaves[rng() % leaves.size()];
      }
      if (pick == 0) {
        return ARRAY(randomType(depth - 1));
      }
      if (pick == 1) {
        return MAP(keys[rng() % keys.size()], randomType(depth - 1));
      }
      std::vector<std::string> names;
      std::vector<TypePtr> types;
      const auto numFields = 1 + rng() % 3;
      for (uint32_t f = 0; f < numFields; ++f) {
        names.push_back(fmt::format("f{}", f));
        types.push_back(randomType(depth - 1));
      }
      return ROW(std::move(names), std::move(types));
    };
    for (uint32_t iteration = 0; iteration < 10; ++iteration) {
      std::vector<std::string> names;
      std::vector<TypePtr> types;
      const auto numColumns = 1 + rng() % 4;
      for (uint32_t col = 0; col < numColumns; ++col) {
        names.push_back(fmt::format("c{}", col));
        if (col > 0) {
          types.push_back(randomType(3));
        } else if (withStructs) {
          types.push_back(ROW({"a", "b"}, {randomType(2), randomType(2)}));
        } else {
          types.push_back(ARRAY(randomType(2)));
        }
      }
      const auto type = ROW(std::move(names), std::move(types));
      SCOPED_TRACE(
          fmt::format("iteration {}: {}", iteration, type->toString()));
      ASSERT_TRUE(CellNestedLayout::supports(type));
      VectorFuzzer::Options fuzzerOptions;
      fuzzerOptions.vectorSize = 300;
      fuzzerOptions.nullRatio = 0.05 * (iteration % 5);
      fuzzerOptions.containerLength = 6;
      fuzzerOptions.complexElementsMaxSize = 3000;
      fuzzerOptions.stringLength = 20;
      fuzzerOptions.stringVariableLength = true;
      fuzzerOptions.allowLazyVector = iteration % 2 == 1;
      // What V1 keeps of a timestamp.
      fuzzerOptions.timestampPrecision =
          VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
      fuzzerOptions.timestampFormat = {
          VectorFuzzer::Options::TimestampFormat::kStandard};
      VectorFuzzer fuzzer(
          fuzzerOptions, pool(), (withStructs ? 17 : 7) + iteration);
      ShuffleInput input;
      for (uint32_t batch = 0; batch < 3; ++batch) {
        input.data.push_back(fuzzer.fuzzInputRow(type));
        auto& pids = input.pids.emplace_back(input.data.back()->size());
        for (auto& pid : pids) {
          pid = static_cast<int32_t>(rng() % 7);
        }
      }
      roundTrip(input, 7, nullptr, nullptr, {{4096, 1 << 20}, {7, 100}});
      const auto v1 = v1Partitions(input, 7);
      const auto rows = input.rowsOf(7);
      for (int32_t pid = 0; pid < 7; ++pid) {
        SCOPED_TRACE("V1");
        test::expectPartitionRows(
            rows[pid], v1[pid], pid, std::numeric_limits<int32_t>::max());
      }
      if (HasFatalFailure()) {
        return;
      }
    }
  }
}

TEST_F(CellNestedTest, structNullsFirstInALaterBatch) {
  // Structs without a null in the first two batches and with some in the
  // third: their fields, with no null of their own, turn mixed only there,
  // and so does a field null throughout until then, at the top level and as
  // an array's elements. At P=1024 a column turning mixed fills in the
  // positions every partition held before. Four batches more hold structs
  // null only past their first 64 or only among their first 100, flat and
  // then in a dictionary: a batch's struct nulls count wherever they lie.
  constexpr vector_size_t kRows = 2000;
  const std::vector<std::string> names{"a", "s", "l", "late"};
  using IsNull = std::function<bool(vector_size_t)>;
  const IsNull none = [](auto) { return false; };
  const IsNull past64 = [](auto row) { return row == 100 || row == 700; };
  const IsNull below100 = [](auto row) { return row < 100; };
  // Per batch, where its structs are null and whether in a dictionary.
  const std::vector<std::pair<IsNull, bool>> batches = {
      {none, false},
      {none, false},
      {[](auto row) { return row % 7 == 3; }, false},
      {none, false},
      {past64, false},
      {below100, false},
      {past64, true},
      {below100, true}};
  const auto batchOf = [&](int64_t batch,
                           const IsNull& isNull,
                           bool dictionary,
                           bool late) {
    // `size` structs, values from `from` on.
    const auto structs = [&](vector_size_t size, int64_t from) -> VectorPtr {
      auto values = makeRowVector(
          names,
          {makeFlatVector<int64_t>(size, [&](auto row) { return from + row; }),
           makeFlatVector<std::string>(
               size,
               [&](auto row) {
                 return std::string((from + row) % 13, 'a' + row % 26);
               }),
           makeArrayVector<int32_t>(
               size,
               [](auto row) { return row % 3; },
               [&](auto idx) { return static_cast<int32_t>(from + idx); }),
           makeFlatVector<int32_t>(
               size,
               [](auto row) { return row; },
               [&](auto) { return !late; })},
          dictionary ? none : isNull);
      if (!dictionary) {
        return values;
      }
      // In reverse, the dictionary's nulls where the structs' are.
      return BaseVector::wrapInDictionary(
          makeNulls(size, isNull),
          makeIndicesInReverse(size),
          size,
          std::move(values));
    };
    return makeRowVector(
        {"id", "r", "ar"},
        {makeFlatVector<int64_t>(
             kRows, [&](auto row) { return batch * kRows + row; }),
         structs(kRows, batch * kRows),
         makeArrayVector(
             test::evenOffsets(kRows, 2),
             structs(2 * kRows, 2 * batch * kRows))});
  };
  for (const int32_t numPartitions : {7, 1024}) {
    SCOPED_TRACE(fmt::format("P={}", numPartitions));
    ShuffleInput input;
    std::mt19937_64 rng(numPartitions);
    for (size_t batch = 0; batch < batches.size(); ++batch) {
      input.data.push_back(batchOf(
          batch, batches[batch].first, batches[batch].second, batch >= 2));
      auto& pids = input.pids.emplace_back(kRows);
      for (auto& pid : pids) {
        pid = static_cast<int32_t>(rng() % numPartitions);
      }
    }
    roundTrip(input, numPartitions, nullptr, nullptr, kFewerReadShapes);
    if (HasFatalFailure()) {
      return;
    }
  }
}

TEST_F(CellNestedTest, wrappedStructsAndFields) {
  // Structs flat, in dictionaries with nulls of their own, constant,
  // constant null, sliced and lazy at the top level, and all those ways but
  // lazy as an array's elements; their fields held in each of those ways
  // too, lazy only in a flat struct at the top level, as a scan makes them:
  // a different way per batch, struct and field. In the second round the
  // fields' ways shift, so that each way of a struct meets two sets of them
  // and a flat struct at the top level the lazy ones. A wrapped struct's
  // fields, and the wrapped fields of a flat struct with nulls, take the
  // structs' nulls through a dictionary.
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20};
  NestedVectorMaker maker(pool(), 23, options);
  auto& rng = maker.rng();
  const auto fieldsType =
      ROW({"a", "s", "l", "n"},
          {INTEGER(), VARCHAR(), ARRAY(BIGINT()), ROW({"x"}, {DOUBLE()})});
  constexpr auto kEncodings = static_cast<uint32_t>(Encoding::kCount);
  const auto isLazy = [](Encoding encoding) {
    return encoding == Encoding::kLazy || encoding == Encoding::kLazyDictionary;
  };
  // `size` structs held as `encoding`, a tenth of them null, field f as
  // encoding `first` + f.
  uint32_t numLazyFields = 0;
  const auto structs = [&](vector_size_t size,
                           Encoding encoding,
                           uint32_t first,
                           bool elements) {
    const bool lazyFields = !elements && encoding == Encoding::kFlat;
    const auto encodingOf = [&](uint32_t i) {
      const auto e = static_cast<Encoding>((first + i) % kEncodings);
      if (!isLazy(e)) {
        return e;
      }
      numLazyFields += lazyFields;
      return lazyFields ? e : Encoding::kFlat;
    };
    return maker.column(fieldsType, size, encoding, [&](vector_size_t n) {
      std::vector<VectorPtr> fields;
      for (uint32_t f = 0; f < fieldsType->size(); ++f) {
        fields.push_back(
            maker.column(fieldsType->childAt(f), n, encodingOf(f)));
      }
      auto nulls = AlignedBuffer::allocate<bool>(n, pool(), bits::kNotNull);
      for (vector_size_t row = 0; row < n; ++row) {
        if (rng() % 10 == 0) {
          bits::setNull(nulls->asMutable<uint64_t>(), row);
        }
      }
      return std::make_shared<RowVector>(
          pool(), fieldsType, std::move(nulls), n, std::move(fields));
    });
  };
  constexpr int32_t kPartitions = 7;
  ShuffleInput input;
  for (uint32_t batch = 0; batch < 2 * kEncodings; ++batch) {
    const vector_size_t rows = 300 + 50 * static_cast<vector_size_t>(batch);
    const auto top = static_cast<Encoding>(batch % kEncodings);
    auto elementsEncoding = static_cast<Encoding>((batch + 3) % kEncodings);
    if (isLazy(elementsEncoding)) {
      elementsEncoding = Encoding::kDictionary;
    }
    input.data.push_back(makeRowVector(
        {"id", "r", "ar"},
        {makeFlatVector<int64_t>(rows, [](auto row) { return row; }),
         structs(rows, top, batch + 4 * (batch / kEncodings), false),
         makeArrayVector(
             test::evenOffsets(rows, 2),
             structs(2 * rows, elementsEncoding, batch + 1, true))}));
    auto& pids = input.pids.emplace_back(rows);
    for (auto& pid : pids) {
      pid = static_cast<int32_t>(rng() % kPartitions);
    }
  }
  // Some flat struct got lazy fields.
  ASSERT_GT(numLazyFields, 0);
  roundTrip(input, kPartitions);
}

TEST_F(CellNestedTest, fieldsShorterThanTheirStructs) {
  // A struct's fields may stop short of it where every struct from there on
  // is null, as RowVector::validate() allows: numbers, a string, an array, a
  // map and a struct whose own fields stop shorter still. Structs with
  // fields far shorter, one row shorter, in a dictionary, constant and all
  // null, at the top level and as an array's elements: nothing past a
  // field's end is read, and the input's buffers keep their sizes.
  const auto innerType = ROW({"x", "t"}, {DOUBLE(), VARCHAR()});
  const auto fieldsType =
      ROW({"a", "b", "s", "l", "m", "n"},
          {BIGINT(),
           INTEGER(),
           VARCHAR(),
           ARRAY(BIGINT()),
           MAP(INTEGER(), BIGINT()),
           innerType});
  const auto text = [](auto row) { return std::string(row % 23, 't'); };
  // `size` structs, null at every fourth row and from `length` on, whose
  // fields hold `length` rows; n null at every seventh row and from `inner`
  // on, its fields holding `inner` rows.
  const auto structs = [&](vector_size_t size,
                           vector_size_t length,
                           vector_size_t inner) {
    auto n = std::make_shared<RowVector>(
        pool(),
        innerType,
        makeNulls(
            length, [&](auto row) { return row % 7 == 0 || row >= inner; }),
        length,
        std::vector<VectorPtr>{
            makeFlatVector<double>(inner, [](auto row) { return row * 0.5; }),
            makeFlatVector<std::string>(inner, text)});
    return std::make_shared<RowVector>(
        pool(),
        fieldsType,
        makeNulls(
            size, [&](auto row) { return row % 4 == 0 || row >= length; }),
        size,
        std::vector<VectorPtr>{
            makeFlatVector<int64_t>(
                length, [](auto row) { return row * 3; }, nullEvery(5)),
            makeFlatVector<int32_t>(length, [](auto row) { return row; }),
            makeFlatVector<std::string>(length, text),
            makeArrayVector<int64_t>(
                length,
                [](auto row) { return row % 3; },
                [](auto idx) { return idx; }),
            makeMapVector<int32_t, int64_t>(
                length,
                [](auto row) { return row % 4; },
                [](auto idx) { return static_cast<int32_t>(idx); },
                [](auto idx) { return idx * 7; }),
            std::move(n)});
  };
  std::mt19937_64 rng(31);
  // `size` rows of `base` in no order, a tenth of them null.
  const auto shuffled = [&](vector_size_t size, const VectorPtr& base) {
    auto indices = allocateIndices(size, pool());
    auto* raw = indices->asMutable<vector_size_t>();
    for (vector_size_t row = 0; row < size; ++row) {
      raw[row] = static_cast<vector_size_t>(rng() % base->size());
    }
    return BaseVector::wrapInDictionary(
        makeNulls(size, [&](auto) { return rng() % 10 == 0; }),
        std::move(indices),
        size,
        base);
  };
  // Structs at the top level and, four to an array, as elements.
  constexpr int32_t kPartitions = 7;
  ShuffleInput input;
  const auto add = [&](const VectorPtr& top, const VectorPtr& elements) {
    const vector_size_t rows = top->size();
    BOLT_CHECK_EQ(elements->size(), 4 * rows);
    input.data.push_back(makeRowVector(
        {"id", "r", "ar"},
        {makeFlatVector<int64_t>(rows, [](auto row) { return row; }),
         top,
         makeArrayVector(test::evenOffsets(rows, 4), elements)}));
    auto& pids = input.pids.emplace_back(rows);
    for (auto& pid : pids) {
      pid = static_cast<int32_t>(rng() % kPartitions);
    }
  };
  add(structs(2000, 300, 100), structs(8000, 500, 200));
  // A row short at both levels, where the fields' buffers have room for
  // every struct: a view as long as the structs would resize them, which
  // only their sizes show.
  const auto top = structs(1000, 999, 998);
  const auto elements = structs(4000, 3999, 3998);
  add(top, elements);
  add(shuffled(1000, structs(600, 200, 50)),
      shuffled(4000, structs(2400, 700, 300)));
  add(BaseVector::wrapInConstant(300, 1, structs(4, 2, 1)),
      BaseVector::wrapInConstant(1200, 1, structs(4, 2, 1)));
  add(structs(500, 0, 0), structs(2000, 0, 0));
  // The values buffers of the flat fields a, b, s, x and t.
  const auto valueSizes = [&] {
    std::vector<uint64_t> sizes;
    for (const auto& vector : {top, elements}) {
      const auto* n = vector->childAt(5)->as<RowVector>();
      for (const auto* field :
           {vector->childAt(0).get(),
            vector->childAt(1).get(),
            vector->childAt(2).get(),
            n->childAt(0).get(),
            n->childAt(1).get()}) {
        sizes.push_back(field->values()->size());
      }
    }
    return sizes;
  };
  const auto before = valueSizes();
  roundTrip(input, kPartitions, nullptr, nullptr, kFewerReadShapes);
  EXPECT_EQ(valueSizes(), before);
}

TEST_F(CellNestedTest, structsPastTheColumnLimitTakeTheAdapter) {
  // A struct of 65535 fields shreds into 65536 columns, one more than a
  // nested layout holds: it goes through the adapter's binary column,
  // converted and not shredded, and reads back whole. A field fewer takes
  // the nested layout.
  constexpr uint32_t kFields = 65'535;
  for (const uint32_t numFields : {kFields, kFields - 1}) {
    SCOPED_TRACE(fmt::format("{} fields", numFields));
    std::vector<std::string> names;
    std::vector<VectorPtr> fields;
    for (uint32_t f = 0; f < numFields; ++f) {
      names.push_back(fmt::format("f{}", f));
      fields.push_back(makeFlatVector<int64_t>(
          3,
          [f](auto row) { return int64_t{f} * 3 + row; },
          [f](auto row) { return row == 1 && f % 2 == 1; }));
    }
    const auto input = test::roundRobin(
        {makeRowVector(
            {"r"},
            {makeRowVector(names, fields, [](auto row) { return row == 2; })})},
        2);
    const auto written = test::writeShuffle(writerOptions(2), input, pool());
    if (numFields == kFields) {
      EXPECT_GT(written.metrics.convertTime, 0);
      EXPECT_EQ(written.metrics.flattenTime, 0);
    } else {
      EXPECT_GT(written.metrics.flattenTime, 0);
      EXPECT_EQ(written.metrics.convertTime, 0);
    }
    test::expectReadsBack(
        input, written, arrow::Compression::UNCOMPRESSED, pool());
  }
}

TEST_F(CellNestedTest, nestedShuffleMetrics) {
  // A nested layout's batches are shredded, not converted: flattenTime
  // counts the shredding and splitTime the rest of each split, as V1
  // counts them, so that together they fit in the time split() took; the
  // batches and their rows count as the writer's input, as flat ones do.
  // Reading the payloads back - decompressing them, counting their
  // positions and assembling the batches - is the reader's decode time,
  // decompression a share of it.
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20};
  options.wrapPercent = 50;
  options.scatteredPercent = 30;
  constexpr int32_t kPartitions = 7;
  const auto input = test::makeInput(
      test::deepNestedType(),
      options,
      {2000, 2000, 2000},
      kPartitions,
      111,
      pool(),
      [](uint32_t batch, uint32_t col) {
        return (batch + col) % 2 == 0 ? Encoding::kDictionary : Encoding::kFlat;
      });
  const auto type = asRowType(input.data[0]->type());
  const auto rows = input.rowsOf(kPartitions);
  const auto lz4 = test::lz4Codec();
  for (const bool rss : {false, true}) {
    SCOPED_TRACE(rss ? "rss" : "local");
    const auto written = test::writeShuffle(
        writerOptions(kPartitions, rss, arrow::Compression::LZ4_FRAME),
        input,
        pool());
    const auto& metrics = written.metrics;
    EXPECT_GT(metrics.flattenTime, 0);
    EXPECT_GT(metrics.splitTime, 0);
    EXPECT_EQ(metrics.convertTime, 0);
    EXPECT_LE(metrics.splitTime + metrics.flattenTime, written.splitWallNs);
    EXPECT_EQ(metrics.totalInputRowNumber, 6000);
    EXPECT_EQ(metrics.totalInputBatches, 3);
    uint64_t decompressTimeNs = 0;
    for (int32_t pid = 0; pid < kPartitions; ++pid) {
      SCOPED_TRACE(pid);
      const auto reader = test::makeReader(
          test::partitionStreams(written, pid),
          type,
          lz4.get(),
          pool(),
          100,
          1 << 20);
      size_t numRows = 0;
      while (auto batch = reader->next()) {
        numRows += batch->size();
      }
      EXPECT_EQ(numRows, rows[pid].size());
      EXPECT_GT(reader->decodeTimeNs(), 0);
      EXPECT_LE(reader->decompressTimeNs(), reader->decodeTimeNs());
      decompressTimeNs += reader->decompressTimeNs();
    }
    EXPECT_GT(decompressTimeNs, 0);
  }
}

TEST_F(CellNestedTest, writerReturnsScratchAfterEachBatch) {
  // Structs below a container have an entry per element, far more than a
  // batch has rows. The scratch the writer takes per element - a
  // dictionary's indices and nulls, the presences' values, a field's nulls
  // with its structs' - goes back with the batch: after one of 2^22 structs
  // that take little else of the writer, it holds less than a byte per
  // struct. A reclaim gives back the scratch it keeps between batches, and
  // may come in the middle of a batch, while the numbers in a struct's
  // array load and before the struct's next field takes the structs'
  // indices: it then gives back nothing the batch still uses. On an RSS
  // client the window goes out as each batch ends, and its cells' memory
  // with it, so that the writer's own pool shows its scratch and state
  // alone. Every partition reads back its rows.
  constexpr int32_t kPartitions = 3;
  constexpr vector_size_t kRows = 100;
  constexpr vector_size_t kPerRow = 2500;
  constexpr vector_size_t kElements = kRows * kPerRow;
  constexpr vector_size_t kManyStructs = 1 << 22;
  const auto numbers = [&](vector_size_t size) {
    return makeFlatVector<int64_t>(
        size, [](auto row) { return row * 3; }, nullEvery(5));
  };
  // `rows` rows of arrays of `perRow` structs each: ROW<a BIGINT, s
  // VARCHAR>, and in a dictionary with nulls of its own ROW<l
  // ARRAY<BIGINT>, a BIGINT>, l holding a number at every other struct, the
  // numbers `listed`.
  const auto batchOf = [&](vector_size_t rows,
                           vector_size_t perRow,
                           VectorPtr listed) {
    const vector_size_t size = rows * perRow;
    BOLT_CHECK_EQ(listed->size(), size / 2);
    std::vector<vector_size_t> listOffsets;
    for (vector_size_t row = 0; row < size; ++row) {
      listOffsets.push_back(row / 2);
    }
    const auto flat = makeRowVector(
        {"a", "s"},
        {numbers(size),
         makeFlatVector<std::string>(
             size, [](auto row) { return std::string(row % 17, 'w'); })},
        nullEvery(7));
    const auto wrapped = BaseVector::wrapInDictionary(
        makeNulls(size, nullEvery(9)),
        makeIndicesInReverse(size),
        size,
        makeRowVector(
            {"l", "a"},
            {makeArrayVector(listOffsets, std::move(listed)), numbers(size)},
            nullEvery(7)));
    const auto offsets = test::evenOffsets(rows, perRow);
    return makeRowVector(
        {"ar", "ad"},
        {makeArrayVector(offsets, flat), makeArrayVector(offsets, wrapped)});
  };
  // 2^22 of the second structs, in a dictionary over 1000 whose fields are
  // null, and none of the first.
  const auto manyStructs = [&] {
    constexpr vector_size_t kRowsOfMany = 1024;
    const auto base = makeRowVector(
        {"l", "a"},
        {BaseVector::createNullConstant(ARRAY(BIGINT()), 1000, pool()),
         BaseVector::createNullConstant(BIGINT(), 1000, pool())});
    const auto structs = BaseVector::wrapInDictionary(
        makeNulls(kManyStructs, nullEvery(9)),
        makeIndices(kManyStructs, [](auto row) { return row % 1000; }),
        kManyStructs,
        base);
    return makeRowVector(
        {"ar", "ad"},
        {makeArrayVector(
             std::vector<vector_size_t>(kRowsOfMany, 0),
             BaseVector::create(
                 ROW({"a", "s"}, {BIGINT(), VARCHAR()}), 0, pool())),
         makeArrayVector(
             test::evenOffsets(kRowsOfMany, kManyStructs / kRowsOfMany),
             structs)});
  };
  // A pool of the writer's own, and a cap that every window passes at its
  // first chunk: on an RSS client, the window goes out as the batch ends.
  auto root = memory::memoryManager()->addRootPool("cellNestedScratch");
  auto writerPool = root->addLeafChild("writer");
  auto options = writerOptions(kPartitions, true);
  options.cellOptions.cellMemoryCapBytes = 1;
  CellShuffleWriter writer(
      options, writerPool.get(), arrow::default_memory_pool());
  const auto reclaim = [&] {
    int64_t freed = 0;
    ASSERT_TRUE(writer.reclaimFixedSize(int64_t{1} << 30, &freed).ok());
  };
  int loads = 0;
  const auto input = test::roundRobin(
      {manyStructs(),
       batchOf(
           kRows,
           kPerRow,
           std::make_shared<LazyVector>(
               pool(),
               BIGINT(),
               kElements / 2,
               std::make_unique<CallbackLoader>(
                   numbers(kElements / 2),
                   [&] {
                     ++loads;
                     reclaim();
                   }))),
       batchOf(10, 3, numbers(15))},
      kPartitions);
  const auto written = test::writeShuffle(
      writer, options, input, pool(), nullptr, [&](size_t batch) {
        if (batch == 0) {
          EXPECT_LT(writerPool->usedBytes(), kManyStructs);
        } else if (batch == 1) {
          reclaim();
          EXPECT_LT(writerPool->usedBytes(), 1 << 20);
        }
      });
  EXPECT_EQ(loads, 1);
  test::expectReadsBack(
      input, written, arrow::Compression::UNCOMPRESSED, pool());
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
