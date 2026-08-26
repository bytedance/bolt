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

#include <gtest/gtest.h>

#include <algorithm>
#include <bit>
#include <filesystem>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/radixsort/RadixSortBuffer.h"
#include "bolt/exec/tests/utils/QueryAssertions.h"
#include "bolt/exec/tests/utils/RadixSortComparatorOracle.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/prestosql/types/HyperLogLogType.h"
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "bolt/type/HugeInt.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/SimpleVector.h"
#include "bolt/vector/VariantVector.h"

namespace bytedance::bolt::exec::radixsort::test {
namespace {

class RadixSortBufferTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    filesystems::registerLocalFileSystem();
  }

 protected:
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("radix-sort-buffer-test")};

  static CompareFlags flags(bool ascending, bool nullsFirst) {
    return CompareFlags{
        .nullsFirst = nullsFirst,
        .ascending = ascending,
        .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
  }

  template <typename T, typename U = T>
  FlatVectorPtr<T> makeVector(
      const TypePtr& type,
      const std::vector<std::optional<U>>& values) {
    auto vector =
        BaseVector::create<FlatVector<T>>(type, values.size(), pool());
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        vector->set(row, T(*values[row]));
      } else {
        vector->setNull(row, true);
      }
    }
    return vector;
  }

  FlatVectorPtr<StringView> makeStringVector(
      const TypePtr& type,
      const std::vector<std::optional<std::string>>& values) {
    return makeVector<StringView, std::string>(type, values);
  }

  RowVectorPtr makeRows(
      std::vector<std::string> names,
      const std::vector<VectorPtr>& children) {
    std::vector<TypePtr> types;
    types.reserve(children.size());
    for (const auto& child : children) {
      types.push_back(child->type());
    }
    return std::make_shared<RowVector>(
        pool(),
        ROW(std::move(names), std::move(types)),
        nullptr,
        children.empty() ? 0 : children.front()->size(),
        children);
  }

  RowVectorPtr
  slice(const RowVector& input, vector_size_t offset, vector_size_t count) {
    std::vector<VectorPtr> children;
    children.reserve(input.childrenSize());
    for (uint32_t column = 0; column < input.childrenSize(); ++column) {
      auto child =
          BaseVector::create(input.childAt(column)->type(), count, pool());
      child->copy(input.childAt(column).get(), 0, offset, count);
      children.push_back(std::move(child));
    }
    return std::make_shared<RowVector>(
        pool(), input.type(), nullptr, count, std::move(children));
  }

  RowVectorPtr concatenateBatches(
      const std::vector<RowVectorPtr>& batches,
      vector_size_t total) {
    const auto inputType = std::static_pointer_cast<const RowType>(
        batches.empty() ? inputType_ : batches.front()->type());
    std::vector<VectorPtr> children;
    for (const auto& type : inputType->children()) {
      children.push_back(BaseVector::create(type, total, pool()));
    }
    vector_size_t offset = 0;
    for (const auto& batch : batches) {
      for (uint32_t column = 0; column < children.size(); ++column) {
        children[column]->copy(
            batch->childAt(column).get(), offset, 0, batch->size());
      }
      offset += batch->size();
    }
    EXPECT_EQ(offset, total);
    return std::make_shared<RowVector>(
        pool(), inputType, nullptr, total, std::move(children));
  }

  RowVectorPtr collect(
      RadixSortBuffer& buffer,
      vector_size_t batchSize,
      const RowVectorPtr& prefix = nullptr) {
    const auto total = static_cast<vector_size_t>(buffer.numInputRows());
    if (prefix == nullptr && total > 0 && batchSize >= total) {
      auto output = buffer.getOutput(batchSize);
      EXPECT_NE(output, nullptr);
      EXPECT_EQ(buffer.getOutput(batchSize), nullptr);
      return output;
    }
    std::vector<RowVectorPtr> batches;
    if (prefix != nullptr) {
      batches.push_back(prefix);
    }
    while (auto batch = buffer.getOutput(batchSize)) {
      batches.push_back(std::move(batch));
    }
    return concatenateBatches(batches, total);
  }

  void collectAndVerify(
      RadixSortBuffer& buffer,
      const RowVectorPtr& input,
      vector_size_t batchSize,
      column_index_t idChannel,
      column_index_t keyChannel = 0,
      int64_t idBase = 0,
      const RowVectorPtr& prefix = nullptr,
      const CompareFlags& keyFlags = flags(true, true)) {
    auto output = collect(buffer, batchSize, prefix);
    expectRowsMatchById(*input, *output, idChannel, std::nullopt, idBase);
    expectSorted(*output, {keyChannel}, {keyFlags});
    EXPECT_EQ(buffer.numOutputRows(), input->size());
  }

  RowVectorPtr sortAndCollect(
      const RowVectorPtr& input,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<CompareFlags>& keyFlags,
      vector_size_t batchSize = 3,
      bool multipleInputs = true) {
    RadixSortBuffer buffer(
        std::static_pointer_cast<const RowType>(input->type()),
        keyChannels,
        keyFlags,
        pool());
    if (multipleInputs && input->size() >= 3) {
      const auto first = input->size() / 3;
      const auto second = input->size() / 3;
      buffer.addInput(slice(*input, 0, first));
      buffer.addInput(slice(*input, first, second));
      buffer.addInput(
          slice(*input, first + second, input->size() - first - second));
    } else {
      buffer.addInput(input);
    }
    EXPECT_EQ(buffer.numInputRows(), input->size());
    buffer.noMoreInput();
    auto output = collect(buffer, batchSize);
    EXPECT_EQ(buffer.numOutputRows(), input->size());
    EXPECT_EQ(buffer.getOutput(batchSize), nullptr);
    return output;
  }

  void sortAndVerify(
      const RowVectorPtr& input,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<CompareFlags>& keyFlags,
      column_index_t idChannel,
      vector_size_t batchSize = 2,
      bool multipleInputs = true) {
    auto output =
        sortAndCollect(input, keyChannels, keyFlags, batchSize, multipleInputs);
    expectRowsMatchById(*input, *output, idChannel);
    expectSorted(*output, keyChannels, keyFlags);
  }

  void sortAndVerifyWithDuckDb(
      const RowVectorPtr& input,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<CompareFlags>& keyFlags,
      std::string_view duckDbSql,
      vector_size_t batchSize = 2,
      bool multipleInputs = true) {
    ::bytedance::bolt::exec::test::DuckDbQueryRunner duckDbQueryRunner;
    duckDbQueryRunner.createTable("tmp", {input});
    const auto output =
        sortAndCollect(input, keyChannels, keyFlags, batchSize, multipleInputs);
    ::bytedance::bolt::exec::test::assertResultsOrdered(
        {output},
        std::static_pointer_cast<const RowType>(output->type()),
        std::string(duckDbSql),
        duckDbQueryRunner,
        keyChannels);
  }

  void sortIdsAndVerifyWithDuckDb(
      const RowVectorPtr& input,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<CompareFlags>& keyFlags,
      std::string_view duckDbSql,
      vector_size_t batchSize = 2) {
    ::bytedance::bolt::exec::test::DuckDbQueryRunner duckDbQueryRunner;
    duckDbQueryRunner.createTable("tmp", {input});
    auto output = sortAndCollect(input, keyChannels, keyFlags, batchSize);
    auto ids = BaseVector::create(BIGINT(), output->size(), pool());
    ids->copy(output->childAt("id").get(), 0, 0, output->size());
    const auto idRows = makeRows({"id"}, {ids});
    ::bytedance::bolt::exec::test::assertResultsOrdered(
        {idRows},
        std::static_pointer_cast<const RowType>(idRows->type()),
        std::string(duckDbSql),
        duckDbQueryRunner,
        {0});
  }

  static int64_t
  idAt(const RowVector& rows, vector_size_t row, column_index_t idChannel) {
    return rows.childAt(idChannel)
        ->asUnchecked<SimpleVector<int64_t>>()
        ->valueAt(row);
  }

  static void expectRowsMatchById(
      const RowVector& input,
      const RowVector& output,
      column_index_t idChannel,
      std::optional<column_index_t> directlyCheckedColumn = std::nullopt,
      int64_t idBase = 0) {
    ASSERT_EQ(output.size(), input.size());
    std::vector<bool> seen(input.size(), false);
    for (vector_size_t row = 0; row < output.size(); ++row) {
      const auto id = idAt(output, row, idChannel);
      const auto inputRow = id - idBase;
      ASSERT_GE(inputRow, 0);
      ASSERT_LT(inputRow, input.size());
      const auto inputIndex = static_cast<vector_size_t>(inputRow);
      EXPECT_FALSE(seen[inputIndex]);
      seen[inputIndex] = true;
      for (uint32_t column = 0; column < input.childrenSize(); ++column) {
        if (directlyCheckedColumn == column) {
          continue;
        }
        EXPECT_EQ(
            SortComparatorOracle::compare(
                *input.childAt(column),
                inputIndex,
                *output.childAt(column),
                row,
                flags(true, true)),
            0)
            << "row=" << row << ", id=" << id << ", column=" << column;
      }
    }
    EXPECT_TRUE(std::all_of(
        seen.begin(), seen.end(), [](bool value) { return value; }));
  }

  static void expectSorted(
      const RowVector& output,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<CompareFlags>& keyFlags) {
    for (vector_size_t row = 1; row < output.size(); ++row) {
      EXPECT_LE(
          SortComparatorOracle::compareRows(
              output, row - 1, output, row, keyChannels, keyFlags),
          0)
          << "row=" << row;
    }
  }

  void spillRemainingOutputAndCheckStats(
      RadixSortBuffer& buffer,
      uint64_t expectedRows) {
    EXPECT_TRUE(buffer.canReclaim());
    const auto statsBefore =
        buffer.spilledStats().value_or(common::SpillStats{});
    buffer.spill();
    EXPECT_FALSE(buffer.canReclaim());
    ASSERT_TRUE(buffer.spilledStats());
    EXPECT_EQ(
        buffer.spilledStats()->spilledRows,
        statsBefore.spilledRows + expectedRows);
    EXPECT_GT(buffer.spilledStats()->spilledBytes, statsBefore.spilledBytes);
    const auto statsAfter = *buffer.spilledStats();
    buffer.spill();
    EXPECT_EQ(buffer.spilledStats()->spilledRows, statsAfter.spilledRows);
    EXPECT_EQ(buffer.spilledStats()->spilledBytes, statsAfter.spilledBytes);
  }

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  common::SpillConfig spillConfig(
      const std::string& directory,
      const std::string& compressionKind = "none",
      common::UpdateAndCheckSpillLimitCB spillLimit = [](uint64_t) {}) const {
    return common::SpillConfig(
        [directory]() -> const std::string& { return directory; },
        std::move(spillLimit),
        "radix-sort-buffer-spill",
        0,
        false,
        0,
        nullptr,
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        0,
        compressionKind);
  }

  struct InputRun {
    vector_size_t size;
    bool spillAfter;
  };

  struct SpillContext {
    SpillContext(
        RadixSortBufferTest& test,
        const RowVectorPtr& input,
        const std::string& compression = "none",
        uint64_t spillMemoryThreshold = 0,
        column_index_t keyChannel = 0)
        : directory(exec::test::TempDirectoryPath::create()),
          config(test.spillConfig(directory->path, compression)),
          buffer(
              std::static_pointer_cast<const RowType>(input->type()),
              {keyChannel},
              {flags(true, true)},
              test.pool(),
              &config,
              spillMemoryThreshold) {
      test.inputType_ = std::static_pointer_cast<const RowType>(input->type());
    }

    std::shared_ptr<exec::test::TempDirectoryPath> directory;
    common::SpillConfig config;
    RadixSortBuffer buffer;
  };

  std::pair<uint64_t, size_t> addInputRuns(
      RadixSortBuffer& buffer,
      const RowVectorPtr& input,
      const std::vector<InputRun>& runs) {
    vector_size_t offset = 0;
    uint64_t currentRunRows = 0;
    uint64_t spilledRows = 0;
    size_t spilledRuns = 0;
    for (const auto& run : runs) {
      buffer.addInput(slice(*input, offset, run.size));
      offset += run.size;
      currentRunRows += run.size;
      if (run.spillAfter) {
        buffer.spill();
        spilledRows += currentRunRows;
        currentRunRows = 0;
        ++spilledRuns;
      }
    }
    EXPECT_EQ(offset, input->size());
    return {spilledRows, spilledRuns};
  }

  template <typename T, typename F>
  std::vector<T> generate(vector_size_t size, F valueAt) {
    std::vector<T> values;
    values.reserve(size);
    for (vector_size_t row = 0; row < size; ++row) {
      values.push_back(valueAt(row));
    }
    return values;
  }

  template <typename T, typename F>
  FlatVectorPtr<T>
  generateVector(const TypePtr& type, vector_size_t size, F valueAt) {
    return makeVector<T>(type, generate<std::optional<T>>(size, valueAt));
  }

  template <typename F>
  FlatVectorPtr<StringView> generateStringVector(
      vector_size_t size,
      F valueAt) {
    return makeStringVector(
        VARCHAR(), generate<std::optional<std::string>>(size, valueAt));
  }

  RowVectorPtr makeKeyPayloadIdRows(
      const std::vector<std::optional<int64_t>>& keys,
      const std::vector<std::optional<std::string>>& payloads,
      int64_t idBase = 0) {
    auto ids = generate<std::optional<int64_t>>(
        keys.size(), [idBase](vector_size_t row) { return idBase + row; });
    return makeRows(
        {"key", "payload", "id"},
        {makeVector<int64_t>(BIGINT(), keys),
         makeStringVector(VARCHAR(), payloads),
         makeVector<int64_t>(BIGINT(), ids)});
  }

  RowVectorPtr makeKeyPayloadIdRows(
      const std::vector<std::optional<int64_t>>& keys,
      int64_t idBase = 0) {
    auto payloads = generate<std::optional<std::string>>(
        keys.size(), [&](vector_size_t row) -> std::optional<std::string> {
          if (row % 7 == 3) {
            return std::nullopt;
          }
          if (row % 3 == 0) {
            return std::string(
                96 + row % 31, static_cast<char>('a' + row % 26));
          }
          return "payload_" + std::to_string(idBase + row) + "_" +
              std::to_string(row % 5);
        });
    return makeKeyPayloadIdRows(keys, payloads, idBase);
  }

  common::SpillStats runSpillPlanAndVerify(
      const RowVectorPtr& input,
      const std::vector<InputRun>& runs,
      vector_size_t outputBatchSize,
      column_index_t idChannel,
      column_index_t keyChannel = 0,
      int64_t idBase = 0,
      std::string_view compression = "none",
      std::optional<vector_size_t> prefixRows = std::nullopt) {
    SpillContext spill(*this, input, std::string(compression), 0, keyChannel);
    auto& buffer = spill.buffer;
    const auto [spilledRows, spilledRuns] = addInputRuns(buffer, input, runs);
    buffer.noMoreInput();

    RowVectorPtr prefix = nullptr;
    if (prefixRows) {
      prefix = buffer.getOutput(*prefixRows);
      EXPECT_NE(prefix, nullptr);
      if (prefix == nullptr) {
        return {};
      }
      EXPECT_EQ(prefix->size(), *prefixRows);
      spillRemainingOutputAndCheckStats(
          buffer, buffer.numInputRows() - prefix->size());
    }
    collectAndVerify(
        buffer, input, outputBatchSize, idChannel, keyChannel, idBase, prefix);
    EXPECT_TRUE(buffer.spillReadStats());
    const auto stats = buffer.spilledStats();
    EXPECT_TRUE(stats);
    if (!stats) {
      return {};
    }
    if (!prefixRows) {
      EXPECT_EQ(stats->spillRuns, spilledRuns);
      EXPECT_EQ(stats->spilledRows, spilledRows);
      EXPECT_GE(stats->spilledFiles, spilledRuns);
    }
    return *stats;
  }

  auto spillAccounting(const common::SpillStats& stats) {
    return std::tie(
        stats.spilledInputBytes,
        stats.spilledRows,
        stats.spilledPartitions,
        stats.spillFillTimeUs,
        stats.spillSortTimeUs,
        stats.spillSerializationTimeUs,
        stats.spillTotalTimeUs);
  }

  auto spillFileWriteAccounting(const common::SpillStats& stats) {
    return std::tie(
        stats.spilledBytes,
        stats.spilledFiles,
        stats.spillWrites,
        stats.spillFlushTimeUs,
        stats.spillWriteTimeUs);
  }

  ArrayVectorPtr makeIntegerArrays() {
    auto arrays = std::make_shared<ArrayVector>(
        pool(),
        ARRAY(INTEGER()),
        nullptr,
        7,
        makeBuffer<vector_size_t>({0, 0, 1, 3, 5, 7, 8}),
        makeBuffer<vector_size_t>({0, 1, 2, 2, 2, 1, 0}),
        makeVector<int32_t>(INTEGER(), {1, 1, 2, 1, 3, 1, std::nullopt, 2}));
    arrays->setNull(6, true);
    return arrays;
  }

  MapVectorPtr makeIntegerStringMaps() {
    auto maps = std::make_shared<MapVector>(
        pool(),
        MAP(INTEGER(), VARCHAR()),
        nullptr,
        7,
        makeBuffer<vector_size_t>({0, 0, 1, 3, 5, 7, 8}),
        makeBuffer<vector_size_t>({0, 1, 2, 2, 2, 1, 0}),
        makeVector<int32_t>(INTEGER(), {1, 2, 1, 1, 2, 2, 1, 1}),
        makeStringVector(VARCHAR(), {"a", "b", "a", "a", "b", "c", "a", "z"}));
    maps->setNull(6, true);
    return maps;
  }

  MapVectorPtr makeStringBigintMaps() {
    auto maps = std::make_shared<MapVector>(
        pool(),
        MAP(VARCHAR(), BIGINT()),
        nullptr,
        9,
        makeBuffer<vector_size_t>({0, 2, 4, 6, 6, 6, 8, 9, 12}),
        makeBuffer<vector_size_t>({2, 2, 2, 0, 0, 2, 1, 3, 1}),
        makeStringVector(
            VARCHAR(),
            {"b",
             "a",
             "a",
             "b",
             "a",
             "b",
             "a",
             "d",
             "aa",
             "a",
             "aa",
             "c",
             "z"}),
        makeVector<int64_t>(
            BIGINT(), {2, 1, 1, 4, 1, 3, 7, 2, 10, 1, 10, 11, 0}));
    maps->setNull(4, true);
    return maps;
  }

  RowVectorPtr makeStringBigintMapKeyRows() {
    return makeRows(
        {"c0", "payload", "id"},
        {makeStringBigintMaps(),
         makeStringVector(
             VARCHAR(),
             {"payload_0",
              "payload_1",
              "payload_2",
              "payload_3",
              "payload_4",
              "payload_5",
              "payload_6",
              "payload_7",
              "payload_8"}),
         generateVector<int64_t>(
             BIGINT(), 9, [](vector_size_t row) { return row; })});
  }

  RowVectorPtr makeNestedComplexKeyRows() {
    auto mapElements = std::make_shared<MapVector>(
        pool(),
        MAP(VARCHAR(), BIGINT()),
        nullptr,
        7,
        makeBuffer<vector_size_t>({0, 2, 3, 5, 7, 8, 10}),
        makeBuffer<vector_size_t>({2, 1, 2, 2, 1, 2, 2}),
        makeStringVector(
            VARCHAR(),
            {"b", "a", "a", "a", "b", "aa", "a", "z", "a", "d", "a", "b"}),
        makeVector<int64_t>(BIGINT(), {2, 1, 2, 1, 3, 10, 1, 0, 1, 2, 1, 3}));
    auto maps = std::make_shared<ArrayVector>(
        pool(),
        ARRAY(MAP(VARCHAR(), BIGINT())),
        nullptr,
        6,
        makeBuffer<vector_size_t>({0, 2, 2, 3, 5, 6}),
        makeBuffer<vector_size_t>({2, 0, 1, 2, 1, 1}),
        mapElements);
    maps->setNull(1, true);
    auto rowArrayValues = std::make_shared<ArrayVector>(
        pool(),
        ARRAY(INTEGER()),
        nullptr,
        6,
        makeBuffer<vector_size_t>({0, 2, 2, 3, 5, 6}),
        makeBuffer<vector_size_t>({2, 0, 1, 2, 1, 2}),
        makeVector<int32_t>(INTEGER(), {2, 1, 1, 1, 2, 3, 1, 4}));
    auto rows = std::make_shared<RowVector>(
        pool(),
        ROW({"a", "b"}, {INTEGER(), ARRAY(INTEGER())}),
        nullptr,
        6,
        std::vector<VectorPtr>{
            makeVector<int32_t>(INTEGER(), {2, 0, 1, 1, 3, 1}),
            rowArrayValues});
    rows->setNull(5, true);
    return makeRows(
        {"array_map", "row_key", "id"},
        {maps,
         rows,
         generateVector<int64_t>(
             BIGINT(), 6, [](vector_size_t row) { return row; })});
  }

  RowVectorPtr makeArrayRowMapIdRows() {
    auto rows = std::make_shared<RowVector>(
        pool(),
        ROW({"number", "text"}, {INTEGER(), VARCHAR()}),
        nullptr,
        7,
        std::vector<VectorPtr>{
            makeVector<int32_t>(INTEGER(), {1, 1, 2, 1, 1, std::nullopt, 1}),
            makeStringVector(
                VARCHAR(), {"a", "b", "a", std::nullopt, "a", "a", "a"})});
    rows->setNull(6, true);
    return makeRows(
        {"array", "row", "map", "id"},
        {makeIntegerArrays(),
         rows,
         makeIntegerStringMaps(),
         generateVector<int64_t>(
             BIGINT(), 7, [](vector_size_t row) { return row; })});
  }

  MapVectorPtr makeLargeStringStringMaps(
      vector_size_t rows,
      const std::string& marker = "") {
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<std::string>> values;
    std::vector<vector_size_t> offsets;
    std::vector<vector_size_t> sizes;
    vector_size_t offset = 0;
    for (vector_size_t row = 0; row < rows; ++row) {
      offsets.push_back(offset);
      const vector_size_t entries = row % 13 == 0 ? 0
          : row % 7 == 0                          ? 32
          : row % 5 == 0                          ? 24
          : row % 3 == 0                          ? 12
                                                  : 5;
      sizes.push_back(entries);
      for (vector_size_t entry = 0; entry < entries; ++entry) {
        keys.push_back(
            marker + "param_" + std::to_string(entry % 31) + "_" +
            std::to_string(row % 17));
        values.push_back(
            entry % 9 == 0 ? marker +
                    std::string(48 + (row + entry) % 41,
                                static_cast<char>('a' + entry % 26))
                           : marker + "v_" + std::to_string(row) + "_" +
                    std::to_string(entry));
      }
      offset += entries;
    }

    auto maps = std::make_shared<MapVector>(
        pool(),
        MAP(VARCHAR(), VARCHAR()),
        nullptr,
        rows,
        makeBuffer<vector_size_t>(offsets),
        makeBuffer<vector_size_t>(sizes),
        makeStringVector(VARCHAR(), keys),
        makeStringVector(VARCHAR(), values));
    for (vector_size_t row = 11; row < rows; row += 37) {
      maps->setNull(row, true);
    }
    return maps;
  }

  RowVectorPtr makeEventMapPayloadRows(
      vector_size_t rows,
      const std::string& marker = "") {
    static constexpr std::array<const char*, 8> kEvents{
        "video_play",
        "video_play_pause",
        "like",
        "follow",
        "share",
        "comment",
        "enter_homepage",
        "click_music"};
    return makeRows(
        {"device_id",
         "user_id",
         "group_id",
         "local_time_ms",
         "enter_from",
         "relation_tag",
         "author_id",
         "params",
         "hour",
         "event"},
        {generateVector<int64_t>(
             BIGINT(), rows, [](vector_size_t row) { return 10'000 + row; }),
         generateVector<int64_t>(
             BIGINT(),
             rows,
             [](vector_size_t row) { return 20'000 + row * 3; }),
         generateVector<int64_t>(
             BIGINT(),
             rows,
             [](vector_size_t row) {
               return row % 3 == 0 ? std::optional<int64_t>{}
                                   : std::optional<int64_t>(row % 97);
             }),
         generateVector<int64_t>(
             BIGINT(),
             rows,
             [](vector_size_t row) { return 1'787'000'000'000 + row * 1000; }),
         generateStringVector(
             rows,
             [&](vector_size_t row) {
               return row % 4 == 0
                   ? std::optional<std::string>{}
                   : marker + "enter_" + std::to_string(row % 11);
             }),
         generateStringVector(
             rows,
             [&](vector_size_t row) {
               return row % 2 == 0
                   ? std::optional<std::string>{}
                   : marker + "relation_" + std::to_string(row % 5);
             }),
         generateStringVector(
             rows,
             [&](vector_size_t row) {
               return row % 7 == 0
                   ? std::optional<std::string>{}
                   : marker + "author_" + std::to_string(row % 101);
             }),
         makeLargeStringStringMaps(rows, marker),
         generateStringVector(
             rows,
             [&](vector_size_t row) {
               return marker + (row % 24 < 10 ? "0" : "") +
                   std::to_string(row % 24);
             }),
         generateStringVector(rows, [&](vector_size_t row) {
           const auto eventIndex =
               row % 10 < 6 ? row % 3 : row % kEvents.size();
           return marker + kEvents[eventIndex];
         })});
  }

  template <typename T>
  BufferPtr makeBuffer(std::initializer_list<T> values) {
    auto buffer = AlignedBuffer::allocate<T>(values.size(), pool());
    std::copy(values.begin(), values.end(), buffer->template asMutable<T>());
    return buffer;
  }

  template <typename T>
  BufferPtr makeBuffer(const std::vector<T>& values) {
    auto buffer = AlignedBuffer::allocate<T>(values.size(), pool());
    std::copy(values.begin(), values.end(), buffer->template asMutable<T>());
    return buffer;
  }

  RowTypePtr inputType_;
};

TEST_F(RadixSortBufferTest, singleRunLifecycleAndMultipleKeys) {
  constexpr vector_size_t kRows = 257;
  std::vector<std::optional<int64_t>> first(kRows);
  std::vector<std::optional<int32_t>> second(kRows);
  std::vector<std::optional<std::string>> payload(kRows);
  std::vector<std::optional<int64_t>> ids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    if (row % 11 != 0) {
      first[row] = (row * 37) % 97;
    }
    if (row % 13 != 0) {
      second[row] = (row * 17) % 53;
    }
    payload[row] = row % 5 == 0
        ? std::string(80, static_cast<char>('a' + row % 20))
        : "value-" + std::to_string(row);
    ids[row] = row;
  }
  auto input = makeRows(
      {"first", "payload", "second", "id"},
      {makeVector<int64_t>(BIGINT(), first),
       makeStringVector(VARCHAR(), payload),
       makeVector<int32_t>(INTEGER(), second),
       makeVector<int64_t>(BIGINT(), ids)});
  const std::vector<column_index_t> keyChannels{0, 2};
  const std::vector<CompareFlags> keyFlags{
      flags(true, false), flags(false, true)};

  for (const auto batchSize : {1, 17, 2048}) {
    sortAndVerify(input, keyChannels, keyFlags, 3, batchSize);
  }
}

TEST_F(RadixSortBufferTest, estimateOutputRowSizeMatchesLegacyForComplexRows) {
  constexpr vector_size_t kRows = 128;
  auto input = makeEventMapPayloadRows(kRows);
  auto inputType = std::static_pointer_cast<const RowType>(input->type());
  const std::vector<column_index_t> keyChannels{9};
  const std::vector<CompareFlags> keyFlags{flags(true, true)};

  tsan_atomic<bool> nonReclaimableSection{false};
  SortBuffer legacy(
      inputType, keyChannels, keyFlags, pool(), &nonReclaimableSection);
  RadixSortBuffer radix(inputType, keyChannels, keyFlags, pool());

  legacy.addInput(input);
  radix.addInput(input);
  legacy.noMoreInput();
  radix.noMoreInput();

  ASSERT_TRUE(legacy.estimateOutputRowSize());
  ASSERT_TRUE(radix.estimateOutputRowSize());
  const auto legacySize = *legacy.estimateOutputRowSize();
  const auto radixSize = *radix.estimateOutputRowSize();
  const auto inputFlatSizePerRow = input->estimateFlatSize() / input->size();

  EXPECT_GT(inputFlatSizePerRow, legacySize * 11 / 10);
  const auto diff =
      legacySize > radixSize ? legacySize - radixSize : radixSize - legacySize;
  EXPECT_LE(diff * 10, legacySize);
}

TEST_F(RadixSortBufferTest, allOrderDirectionsAndNullPlacements) {
  auto input = makeRows(
      {"key", "id"},
      {makeVector<int32_t>(
           INTEGER(), {2, std::nullopt, 1, 2, std::nullopt, -1, 1}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5, 6})});
  for (const auto ascending : {false, true}) {
    for (const auto nullsFirst : {false, true}) {
      const std::vector<CompareFlags> keyFlags{flags(ascending, nullsFirst)};
      sortAndVerify(input, {0}, keyFlags, 1);
    }
  }
}

TEST_F(RadixSortBufferTest, scalarAndCustomOrderKeyTypes) {
  const std::vector<std::optional<int64_t>> ids{0, 1, 2, 3, 4, 5, 6};
  const auto idVector = makeVector<int64_t>(BIGINT(), ids);
  const auto payload = makeStringVector(
      VARCHAR(), {"zero", "one", "two", "three", "four", "five", "six"});
  const auto int128Min = HugeInt::build(uint64_t{1} << 63, 0);
  const auto int128Max = HugeInt::build(
      (uint64_t{1} << 63) - 1, std::numeric_limits<uint64_t>::max());

  std::vector<VectorPtr> keys{
      makeVector<bool>(
          BOOLEAN(), {true, false, std::nullopt, true, false, true, false}),
      makeVector<int8_t>(TINYINT(), {3, -1, std::nullopt, 0, 2, -4, 1}),
      makeVector<int16_t>(SMALLINT(), {300, -1, std::nullopt, 0, 2, -400, 1}),
      makeVector<int32_t>(
          INTEGER(), {30000, -1, std::nullopt, 0, 2, -40000, 1}),
      makeVector<int64_t>(BIGINT(), {30000, -1, std::nullopt, 0, 2, -40000, 1}),
      makeVector<int128_t>(
          HUGEINT(),
          {int128Max,
           static_cast<int128_t>(-1),
           std::nullopt,
           static_cast<int128_t>(0),
           static_cast<int128_t>(2),
           int128Min,
           static_cast<int128_t>(1)}),
      makeVector<float>(
          REAL(), {3.5F, -1.0F, std::nullopt, 0.0F, 2.0F, -4.0F, 1.0F}),
      makeVector<double>(
          DOUBLE(), {3.5, -1.0, std::nullopt, 0.0, 2.0, -4.0, 1.0}),
      makeVector<int64_t>(
          DECIMAL(18, 4), {30000, -1, std::nullopt, 0, 2, -40000, 1}),
      makeVector<int128_t>(
          DECIMAL(38, 18),
          {int128Max,
           static_cast<int128_t>(-1),
           std::nullopt,
           static_cast<int128_t>(0),
           static_cast<int128_t>(2),
           int128Min,
           static_cast<int128_t>(1)}),
      makeVector<int32_t>(DATE(), {20000, -1, std::nullopt, 0, 2, -40000, 1}),
      makeVector<int64_t>(
          INTERVAL_DAY_TIME(), {30000, -1, std::nullopt, 0, 2, -40000, 1}),
      makeVector<int32_t>(
          INTERVAL_YEAR_MONTH(), {300, -1, std::nullopt, 0, 2, -400, 1}),
      makeVector<Timestamp>(
          TIMESTAMP(),
          {Timestamp(3, 5),
           Timestamp(-1, 999999999),
           std::nullopt,
           Timestamp(0, 0),
           Timestamp(2, 0),
           Timestamp(-4, 0),
           Timestamp(1, 0)}),
      makeStringVector(
          VARCHAR(), {"three", "", std::nullopt, "zero", "two", "aa", "one"}),
      makeStringVector(
          VARBINARY(),
          {std::string("\xff", 1),
           std::string(),
           std::nullopt,
           std::string("\x00", 1),
           std::string("\x02", 1),
           std::string("\x80", 1),
           std::string("\x01", 1)}),
      makeStringVector(JSON(), {"3", "-1", std::nullopt, "0", "2", "-4", "1"}),
      makeStringVector(
          HYPERLOGLOG(),
          {"three", "", std::nullopt, "zero", "two", "aa", "one"}),
      BaseVector::createNullConstant(UNKNOWN(), ids.size(), pool())};

  auto timestampWithTimeZone = std::make_shared<RowVector>(
      pool(),
      TIMESTAMP_WITH_TIME_ZONE(),
      nullptr,
      ids.size(),
      std::vector<VectorPtr>{
          makeVector<int64_t>(
              BIGINT(), {30000, -1, std::nullopt, 0, 2, -40000, 1}),
          makeVector<int16_t>(SMALLINT(), {3, 2, std::nullopt, 0, 2, 1, 1})});
  keys.push_back(timestampWithTimeZone);

  for (const auto& key : keys) {
    SCOPED_TRACE(key->type()->toString());
    auto input = makeRows({"key", "payload", "id"}, {key, payload, idVector});
    const std::vector<column_index_t> keyChannels{0};
    const std::vector<CompareFlags> keyFlags{flags(true, false)};
    sortAndVerify(input, keyChannels, keyFlags, 2, 2, false);
  }
}

TEST_F(RadixSortBufferTest, arrayAndRowKeysWithComplexPayload) {
  auto input = makeArrayRowMapIdRows();
  const std::vector<column_index_t> keyChannels{0, 1};
  const std::vector<CompareFlags> keyFlags{
      flags(true, false), flags(false, true)};
  sortAndVerify(input, keyChannels, keyFlags, 3);
}

TEST_F(RadixSortBufferTest, complexOrderKeysMatchDuckDb) {
  const auto rows = makeArrayRowMapIdRows();
  auto input = makeRows(
      {"c0", "c1", "c2", "id"},
      {rows->childAt(0), rows->childAt(1), rows->childAt(2), rows->childAt(3)});
  sortAndVerifyWithDuckDb(
      input,
      {0, 1, 2},
      {flags(true, false), flags(false, true), flags(true, true)},
      "SELECT * FROM tmp ORDER BY "
      "c0 ASC NULLS LAST, "
      "c1 DESC NULLS FIRST, "
      "list_transform(list_sort(map_entries(c2)), x -> x.key) "
      "ASC NULLS FIRST, "
      "list_transform(list_sort(map_entries(c2)), x -> x.value) "
      "ASC NULLS FIRST",
      2);
}

TEST_F(RadixSortBufferTest, mapVarcharBigintOrderKey) {
  auto input = makeStringBigintMapKeyRows();
  sortAndVerifyWithDuckDb(
      input,
      {0},
      {flags(true, true)},
      "SELECT * FROM tmp ORDER BY list_transform(list_sort(map_entries(c0)), "
      "x -> x.key) ASC NULLS FIRST, "
      "list_transform(list_sort(map_entries(c0)), x -> x.value) ASC NULLS FIRST",
      2);
  sortAndVerifyWithDuckDb(
      input,
      {0},
      {flags(false, false)},
      "SELECT * FROM tmp ORDER BY list_transform(list_sort(map_entries(c0)), "
      "x -> x.key) DESC NULLS LAST, "
      "list_transform(list_sort(map_entries(c0)), x -> x.value) DESC NULLS LAST",
      3,
      false);
}

TEST_F(RadixSortBufferTest, nestedComplexOrderKeys) {
  auto input = makeNestedComplexKeyRows();
  const std::vector<column_index_t> keyChannels{0, 1};
  const std::vector<CompareFlags> keyFlags{
      flags(true, true), flags(false, false)};
  sortIdsAndVerifyWithDuckDb(
      input,
      keyChannels,
      keyFlags,
      "SELECT id FROM tmp ORDER BY "
      "list_transform(array_map, m -> struct_pack("
      "k := list_transform(list_sort(map_entries(m)), x -> x.key), "
      "v := list_transform(list_sort(map_entries(m)), x -> x.value))) "
      "ASC NULLS FIRST, "
      "row_key DESC NULLS LAST",
      3);
}

TEST_F(RadixSortBufferTest, unknownAndNestedUnknownRemainSupported) {
  auto unknown = BaseVector::createNullConstant(UNKNOWN(), 7, pool());
  auto unknownArray = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(UNKNOWN()),
      nullptr,
      7,
      makeBuffer<vector_size_t>({0, 0, 0, 0, 0, 0, 0}),
      makeBuffer<vector_size_t>({0, 0, 0, 0, 0, 0, 0}),
      BaseVector::createNullConstant(UNKNOWN(), 0, pool()));
  auto nested = std::make_shared<RowVector>(
      pool(),
      ROW({"unknown", "value"}, {UNKNOWN(), INTEGER()}),
      nullptr,
      7,
      std::vector<VectorPtr>{
          unknown,
          makeVector<int32_t>(INTEGER(), {3, -1, std::nullopt, 0, 2, -4, 1})});
  auto input = makeRows(
      {"unknown_array", "row_key", "id"},
      {unknownArray,
       nested,
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5, 6})});
  const std::vector<column_index_t> keyChannels{0, 1};
  const std::vector<CompareFlags> keyFlags{
      flags(true, false), flags(true, false)};
  sortAndVerify(input, keyChannels, keyFlags, 2);
}

TEST_F(RadixSortBufferTest, allSupportedPayloadTypesRoundTrip) {
  constexpr vector_size_t kRows = 3;
  auto unknown = BaseVector::createNullConstant(UNKNOWN(), kRows, pool());
  auto variants = VariantVector::create(pool(), VARIANT(), kRows);
  auto* variantValues =
      variants->valueChildVector()->asUnchecked<FlatVector<StringView>>();
  auto* variantMetadata =
      variants->metadataChildVector()->asUnchecked<FlatVector<StringView>>();
  const std::string longVariantValue(80, 'v');
  const std::string longVariantMetadata(80, 'm');
  variantValues->set(0, StringView("one"));
  variantMetadata->set(0, StringView("meta-one"));
  variantValues->set(1, StringView(longVariantValue));
  variantMetadata->set(1, StringView(longVariantMetadata));
  variants->setNull(2, true);
  auto variantArrays = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(VARIANT()),
      nullptr,
      kRows,
      makeBuffer<vector_size_t>({0, 1, 2}),
      makeBuffer<vector_size_t>({1, 1, 1}),
      variants);
  auto arrays = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(INTEGER()),
      nullptr,
      kRows,
      makeBuffer<vector_size_t>({0, 1, 3}),
      makeBuffer<vector_size_t>({1, 2, 1}),
      makeVector<int32_t>(INTEGER(), {10, 20, 21, 30}));
  auto maps = std::make_shared<MapVector>(
      pool(),
      MAP(INTEGER(), VARCHAR()),
      nullptr,
      kRows,
      makeBuffer<vector_size_t>({0, 1, 2}),
      makeBuffer<vector_size_t>({1, 1, 1}),
      makeVector<int32_t>(INTEGER(), {1, 2, 3}),
      makeStringVector(VARCHAR(), {"one", "two", "three"}));
  auto rows = std::make_shared<RowVector>(
      pool(),
      ROW({"number", "text"}, {INTEGER(), VARCHAR()}),
      nullptr,
      kRows,
      std::vector<VectorPtr>{
          makeVector<int32_t>(INTEGER(), {10, 20, 30}),
          makeStringVector(VARCHAR(), {"ten", "twenty", "thirty"})});
  auto timestampWithTimeZone = std::make_shared<RowVector>(
      pool(),
      TIMESTAMP_WITH_TIME_ZONE(),
      nullptr,
      kRows,
      std::vector<VectorPtr>{
          makeVector<int64_t>(BIGINT(), {1000, 2000, 3000}),
          makeVector<int16_t>(SMALLINT(), {1, 2, 3})});

  auto input = makeRows(
      {"key",
       "boolean",
       "tinyint",
       "smallint",
       "integer",
       "bigint",
       "hugeint",
       "real",
       "double",
       "short_decimal",
       "long_decimal",
       "date",
       "interval_day_time",
       "interval_year_month",
       "timestamp",
       "varchar",
       "varbinary",
       "json",
       "hyperloglog",
       "unknown",
       "variant_array",
       "array",
       "map",
       "row",
       "timestamp_with_time_zone",
       "id"},
      {makeVector<int64_t>(BIGINT(), {30, 10, 20}),
       makeVector<bool>(BOOLEAN(), {true, false, std::nullopt}),
       makeVector<int8_t>(TINYINT(), {1, -2, std::nullopt}),
       makeVector<int16_t>(SMALLINT(), {10, -20, std::nullopt}),
       makeVector<int32_t>(INTEGER(), {100, -200, std::nullopt}),
       makeVector<int64_t>(BIGINT(), {1000, -2000, std::nullopt}),
       makeVector<int128_t>(
           HUGEINT(),
           {static_cast<int128_t>(1), static_cast<int128_t>(-2), std::nullopt}),
       makeVector<float>(REAL(), {1.5F, -2.5F, std::nullopt}),
       makeVector<double>(DOUBLE(), {1.5, -2.5, std::nullopt}),
       makeVector<int64_t>(DECIMAL(18, 4), {100, -200, std::nullopt}),
       makeVector<int128_t>(
           DECIMAL(38, 18),
           {static_cast<int128_t>(100),
            static_cast<int128_t>(-200),
            std::nullopt}),
       makeVector<int32_t>(DATE(), {100, -200, std::nullopt}),
       makeVector<int64_t>(INTERVAL_DAY_TIME(), {100, -200, std::nullopt}),
       makeVector<int32_t>(INTERVAL_YEAR_MONTH(), {100, -200, std::nullopt}),
       makeVector<Timestamp>(
           TIMESTAMP(), {Timestamp(1, 2), Timestamp(-2, 3), std::nullopt}),
       makeStringVector(VARCHAR(), {"one", std::string(80, 'x'), std::nullopt}),
       makeStringVector(
           VARBINARY(),
           {std::string("\x00", 1), std::string("\xff", 1), std::nullopt}),
       makeStringVector(JSON(), {"1", "{\"a\":2}", std::nullopt}),
       makeStringVector(
           HYPERLOGLOG(), {"one", std::string(80, 'h'), std::nullopt}),
       unknown,
       variantArrays,
       arrays,
       maps,
       rows,
       timestampWithTimeZone,
       makeVector<int64_t>(BIGINT(), {0, 1, 2})});
  auto output = sortAndCollect(input, {0}, {flags(true, true)}, kRows, false);
  const auto* outputVariantArrays =
      output->childAt(20)->asUnchecked<ArrayVector>();
  const auto* outputVariants =
      outputVariantArrays->elements()->asUnchecked<VariantVector>();
  for (vector_size_t row = 0; row < output->size(); ++row) {
    const auto id = idAt(*output, row, 25);
    EXPECT_EQ(variantArrays->isNullAt(id), outputVariantArrays->isNullAt(row))
        << "row=" << row << ", id=" << id;
    EXPECT_EQ(variantArrays->sizeAt(id), outputVariantArrays->sizeAt(row))
        << "row=" << row << ", id=" << id;
    const auto expectedOffset = variantArrays->offsetAt(id);
    const auto actualOffset = outputVariantArrays->offsetAt(row);
    EXPECT_EQ(
        variants->isNullAt(expectedOffset),
        outputVariants->isNullAt(actualOffset))
        << "row=" << row << ", id=" << id;
    if (!variants->isNullAt(expectedOffset)) {
      const auto expected = variants->valueAt(expectedOffset);
      const auto actual = outputVariants->valueAt(actualOffset);
      EXPECT_EQ(expected.value, actual.value) << "row=" << row << ", id=" << id;
      EXPECT_EQ(expected.metadata, actual.metadata)
          << "row=" << row << ", id=" << id;
    }
  }
  expectRowsMatchById(*input, *output, 25, 20);
  expectSorted(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortBufferTest, wrappedFloatingPointKeyOutputUsesDecodedKey) {
  const std::vector<uint64_t> bits{
      0x0000000000000000ULL,
      0x8000000000000000ULL,
      0x7ff8000000000001ULL,
      0x7ff8000000000011ULL,
      0x7ff0000000000000ULL,
      0xfff0000000000000ULL,
      0x3ff0000000000000ULL,
      0xbff0000000000000ULL};
  std::vector<std::optional<double>> values;
  std::vector<std::optional<int64_t>> ids;
  for (uint64_t row = 0; row < bits.size(); ++row) {
    values.push_back(std::bit_cast<double>(bits[row]));
    ids.push_back(row);
  }
  auto flatKeys = makeVector<double>(DOUBLE(), values);
  auto indices = makeBuffer<vector_size_t>({7, 2, 5, 0, 4, 1, 6, 3});
  auto dictionary =
      BaseVector::wrapInDictionary(nullptr, indices, bits.size(), flatKeys);
  auto constant = BaseVector::wrapInConstant(
      bits.size(), 0, makeStringVector(VARCHAR(), {"constant"}));
  auto input = makeRows(
      {"key", "constant", "id"},
      {dictionary, constant, makeVector<int64_t>(BIGINT(), ids)});
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  RadixSortBuffer buffer(inputType_, {0}, {flags(true, true)}, pool());
  buffer.addInput(input);
  buffer.noMoreInput();
  auto output = collect(buffer, 3);

  expectRowsMatchById(*input, *output, 2);
  expectSorted(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortBufferTest, spillPlansMergeCorrectly) {
  struct SpillPlan {
    const char* name;
    std::vector<std::optional<int64_t>> keys;
    std::vector<InputRun> runs;
    vector_size_t outputBatchSize;
  };
  for (const auto& plan : std::vector<SpillPlan>{
           {"single disk and memory",
            {40, 10, 30, 20, 50, 0},
            {{3, true}, {3, false}},
            2},
           {"multiple disks and memory",
            {70, std::nullopt, 60, 20, 50, std::nullopt, 40, 10, 30},
            {{3, true}, {3, true}, {3, false}},
            2},
           {"duplicate keys",
            {2, 1, 2, std::nullopt, 3, 2, 3, 1, std::nullopt, 2},
            {{2, true}, {2, true}, {2, true}, {4, false}},
            3},
           {"multiple disks only",
            {9, 1, 5, 8, 2, 6},
            {{3, true}, {3, true}},
            4}}) {
    SCOPED_TRACE(plan.name);
    runSpillPlanAndVerify(
        makeKeyPayloadIdRows(plan.keys), plan.runs, plan.outputBatchSize, 2);
  }
}

TEST_F(RadixSortBufferTest, variableKeyHeapOffsetSpillMerge) {
  constexpr vector_size_t kRows = 48;
  auto input = makeRows(
      {"first", "second", "text", "id"},
      {generateVector<int32_t>(
           INTEGER(),
           kRows,
           [](vector_size_t row) {
             return row % 11 == 0
                 ? std::optional<int32_t>{}
                 : std::optional<int32_t>{static_cast<int32_t>(100 - row % 17)};
           }),
       generateVector<int64_t>(
           BIGINT(),
           kRows,
           [](vector_size_t row) {
             return row % 7 == 0 ? std::optional<int64_t>{}
                                 : std::optional<int64_t>{row % 13};
           }),
       generateStringVector(
           kRows,
           [](vector_size_t row) {
             return std::string(32 + row % 9, static_cast<char>('a' + row % 5));
           }),
       generateVector<int64_t>(
           BIGINT(), kRows, [](vector_size_t row) { return row; })});
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  auto directory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(directory->path);
  RadixSortBuffer buffer(
      inputType_,
      {0, 1, 2},
      {flags(true, true), flags(true, false), flags(false, false)},
      pool(),
      &config);

  buffer.addInput(slice(*input, 0, 16));
  buffer.spill();
  buffer.addInput(slice(*input, 16, 16));
  buffer.spill();
  buffer.addInput(slice(*input, 32, 16));
  buffer.noMoreInput();

  auto output = collect(buffer, 5);
  expectRowsMatchById(*input, *output, 3);
  expectSorted(
      *output,
      {0, 1, 2},
      {flags(true, true), flags(true, false), flags(false, false)});
}

TEST_F(RadixSortBufferTest, longSpilledKeyKeepsHeapOutputForShortFinalRun) {
  auto input = makeRows(
      {"key", "id"},
      {makeStringVector(
           VARCHAR(), {std::string(40, 'z'), "k3", "k1", "k2", "k0"}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4})});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  buffer.addInput(slice(*input, 0, 1));
  buffer.spill();
  buffer.addInput(slice(*input, 1, 4));
  buffer.noMoreInput();
  collectAndVerify(buffer, input, 2, 1);
}

TEST_F(RadixSortBufferTest, spillMergeNullFreeOutputResetsNullBuffers) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  auto input = makeRows(
      {"text_key",
       "nullable_key",
       "fixed_payload",
       "string_payload",
       "nullable_payload",
       "id"},
      {makeStringVector(VARCHAR(), {"k0", "k1", "k4", "k3", "k5", "k2"}),
       makeVector<int64_t>(BIGINT(), {std::nullopt, 2, 1, 3, 0, 4}),
       makeVector<int64_t>(BIGINT(), {60, 10, 40, 30, 50, 20}),
       makeStringVector(
           VARCHAR(),
           {std::string(72, 'f'),
            std::string(72, 'a'),
            std::string(72, 'd'),
            std::string(72, 'c'),
            std::string(72, 'e'),
            std::string(72, 'b')}),
       makeVector<int64_t>(BIGINT(), {std::nullopt, 200, 100, 300, 500, 400}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5})});
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  RadixSortBuffer buffer(
      inputType_,
      {0, 1},
      {flags(true, true), flags(true, true)},
      pool(),
      &config);
  buffer.addInput(slice(*input, 0, 3));
  buffer.spill();
  ASSERT_TRUE(buffer.spilledStats());
  buffer.addInput(slice(*input, 3, 3));
  buffer.noMoreInput();

  auto firstOutput = buffer.getOutput(3);
  ASSERT_NE(firstOutput, nullptr);
  ASSERT_NE(firstOutput->childAt(1)->rawNulls(), nullptr);
  ASSERT_NE(firstOutput->childAt(4)->rawNulls(), nullptr);
  auto firstOutputCopy = slice(*firstOutput, 0, firstOutput->size());
  for (const auto column : {0, 2, 3, 5}) {
    ASSERT_EQ(firstOutput->childAt(column)->rawNulls(), nullptr) << column;
    firstOutput->childAt(column)->mutableRawNulls();
    ASSERT_NE(firstOutput->childAt(column)->rawNulls(), nullptr) << column;
  }
  firstOutput.reset();

  auto secondOutput = buffer.getOutput(3);
  ASSERT_NE(secondOutput, nullptr);
  EXPECT_NE(secondOutput->childAt(1)->rawNulls(), nullptr);
  EXPECT_TRUE(secondOutput->childAt(1)->mayHaveNulls());
  EXPECT_NE(secondOutput->childAt(4)->rawNulls(), nullptr);
  EXPECT_TRUE(secondOutput->childAt(4)->mayHaveNulls());
  for (const auto column : {0, 2, 3, 5}) {
    EXPECT_EQ(secondOutput->childAt(column)->rawNulls(), nullptr) << column;
    EXPECT_FALSE(secondOutput->childAt(column)->mayHaveNulls()) << column;
  }
  ASSERT_EQ(buffer.getOutput(3), nullptr);

  auto output = concatenateBatches({firstOutputCopy, secondOutput}, 6);
  expectRowsMatchById(*input, *output, 5);
  expectSorted(*output, {0, 1}, {flags(true, true), flags(true, true)});
  EXPECT_EQ(buffer.numOutputRows(), 6);
  EXPECT_TRUE(buffer.spillReadStats());
}

TEST_F(RadixSortBufferTest, spillConfigOwnsDirectoryPath) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  EXPECT_EQ(config.getSpillDirPathCb(), spillDirectory->path);
}

TEST_F(RadixSortBufferTest, keyOnlySpillOutput) {
  auto first = makeRows({"key"}, {makeVector<int64_t>(BIGINT(), {4, 1, 3})});
  SpillContext spill(*this, first);
  auto& buffer = spill.buffer;
  buffer.addInput(first);
  buffer.spill();
  buffer.addInput(makeRows({"key"}, {makeVector<int64_t>(BIGINT(), {2, 5})}));
  buffer.noMoreInput();
  auto output = collect(buffer, 2);
  ASSERT_EQ(output->size(), 5);
  const auto* values = output->childAt(0)->asUnchecked<SimpleVector<int64_t>>();
  for (vector_size_t row = 0; row < output->size(); ++row) {
    EXPECT_EQ(values->valueAt(row), row + 1);
  }
}

TEST_F(RadixSortBufferTest, spillMemoryThresholdTriggersBeforeNextInput) {
  auto input = makeKeyPayloadIdRows({3, 1, 2, 4});
  SpillContext spill(*this, input, "none", 1);
  auto& buffer = spill.buffer;
  buffer.addInput(slice(*input, 0, 2));
  buffer.addInput(slice(*input, 2, 2));
  buffer.noMoreInput();
  ASSERT_TRUE(buffer.spilledStats());
  collectAndVerify(buffer, input, 2, 2);
}

TEST_F(RadixSortBufferTest, estimateOutputRowSizeIncludesSpilledAndMemoryRuns) {
  auto input = makeKeyPayloadIdRows(
      {3, 1, 2, 4},
      {std::string(80, 'c'),
       std::string(80, 'a'),
       std::string(160, 'b'),
       std::string(160, 'd')});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  addInputRuns(buffer, slice(*input, 0, 2), {{2, true}});
  const auto spilledOnlyEstimate = buffer.estimateOutputRowSize();
  ASSERT_TRUE(spilledOnlyEstimate);
  buffer.addInput(slice(*input, 2, 2));
  const auto mixedEstimate = buffer.estimateOutputRowSize();
  ASSERT_TRUE(mixedEstimate);
  EXPECT_GT(*mixedEstimate, *spilledOnlyEstimate);

  buffer.noMoreInput();
  collectAndVerify(buffer, input, 2, 2);
}

TEST_F(RadixSortBufferTest, reservationFailureTriggersSpillWithZeroThreshold) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  std::vector<std::string> names{"key"};
  std::vector<TypePtr> types{BIGINT()};
  constexpr int32_t kPayloadColumns = 48;
  for (int32_t column = 0; column < kPayloadColumns; ++column) {
    names.push_back(fmt::format("payload_{}", column));
    types.push_back(BIGINT());
  }
  names.push_back("id");
  types.push_back(BIGINT());
  auto inputType = ROW(std::move(names), std::move(types));
  inputType_ = inputType;
  auto rootPool = memory::memoryManager()->addRootPool(
      "radix-sort-buffer-reservation-failure-root", 2 << 20);
  auto sortPool =
      rootPool->addLeafChild("radix-sort-buffer-reservation-failure");
  RadixSortBuffer buffer(
      inputType, {0}, {flags(true, true)}, sortPool.get(), &config);

  auto makeWideInput = [&](const std::vector<int64_t>& keys,
                           const std::vector<int64_t>& ids) {
    BOLT_CHECK_EQ(keys.size(), ids.size());
    std::vector<VectorPtr> children;
    children.push_back(makeVector<int64_t>(
        BIGINT(),
        std::vector<std::optional<int64_t>>(keys.begin(), keys.end())));
    for (int32_t column = 0; column < kPayloadColumns; ++column) {
      children.push_back(
          generateVector<int64_t>(BIGINT(), ids.size(), [&](vector_size_t row) {
            return 1000 + column + ids[row] * kPayloadColumns;
          }));
    }
    children.push_back(makeVector<int64_t>(
        BIGINT(), std::vector<std::optional<int64_t>>(ids.begin(), ids.end())));
    return makeRows(inputType->names(), children);
  };
  const std::vector<int64_t> firstKeys{3, 1};
  const std::vector<int64_t> firstIds{0, 1};
  buffer.addInput(makeWideInput(firstKeys, firstIds));

  constexpr vector_size_t kSecondRows = 64;
  auto secondKeys = generate<int64_t>(
      kSecondRows, [](vector_size_t row) { return 100 - row; });
  auto secondIds = generate<int64_t>(
      kSecondRows, [&](vector_size_t row) { return row + firstIds.size(); });
  auto secondInput = makeWideInput(secondKeys, secondIds);
  buffer.addInput(secondInput);

  buffer.noMoreInput();
  auto output = collect(buffer, 2);
  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spilledRows, 2);
  auto expectedInput = concatenateBatches(
      {makeWideInput(firstKeys, firstIds),
       makeWideInput(secondKeys, secondIds)},
      firstKeys.size() + secondKeys.size());
  expectRowsMatchById(*expectedInput, *output, inputType->size() - 1);
  expectSorted(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortBufferTest, spillStatsCoverInputAndOutputStageMetrics) {
  const auto globalStatsBefore = common::globalSpillStats();
  auto input = makeKeyPayloadIdRows({9, 1, 8, 2, 7, 3, 6, 4, 5});
  SpillContext spill(*this, input, "lz4");
  auto& buffer = spill.buffer;
  addInputRuns(buffer, slice(*input, 0, 3), {{3, true}});
  auto inputStageStats = buffer.spilledStats();
  ASSERT_TRUE(inputStageStats);
  EXPECT_EQ(inputStageStats->spillRuns, 1);
  EXPECT_EQ(inputStageStats->spilledRows, 3);
  EXPECT_EQ(inputStageStats->spilledPartitions, 1);
  EXPECT_GT(inputStageStats->spilledInputBytes, 0);
  EXPECT_GT(inputStageStats->spilledBytes, 0);
  EXPECT_GT(inputStageStats->spilledFiles, 0);
  EXPECT_GT(inputStageStats->spillWrites, 0);
  const auto globalInputStats = common::globalSpillStats() - globalStatsBefore;
  EXPECT_EQ(globalInputStats.spillRuns, 0);
  EXPECT_EQ(
      spillAccounting(globalInputStats), spillAccounting(*inputStageStats));
  EXPECT_EQ(
      spillFileWriteAccounting(globalInputStats),
      spillFileWriteAccounting(*inputStageStats));

  buffer.addInput(slice(*input, 3, 3));
  buffer.addInput(slice(*input, 6, 3));
  buffer.noMoreInput();
  auto prefix = buffer.getOutput(4);
  ASSERT_NE(prefix, nullptr);
  const auto readStatsBeforeOutputSpill = buffer.spillReadStats();
  const auto globalStatsBeforeOutputSpill = common::globalSpillStats();
  spillRemainingOutputAndCheckStats(buffer, input->size() - prefix->size());
  const auto allStats = buffer.spilledStats();
  ASSERT_TRUE(allStats);
  const auto outputStats = *allStats - *inputStageStats;
  EXPECT_EQ(allStats->spillRuns, 2);
  EXPECT_EQ(outputStats.spillRuns, 1);
  EXPECT_EQ(outputStats.spilledRows, input->size() - prefix->size());
  EXPECT_GT(outputStats.spilledInputBytes, 0);
  EXPECT_EQ(outputStats.spilledPartitions, 0);
  EXPECT_EQ(outputStats.spillFillTimeUs, 0);
  EXPECT_EQ(outputStats.spillSortTimeUs, 0);
  EXPECT_EQ(allStats->spilledPartitions, 1);
  EXPECT_GE(
      allStats->spillSerializationTimeUs,
      inputStageStats->spillSerializationTimeUs);
  EXPECT_GE(allStats->spillTotalTimeUs, inputStageStats->spillTotalTimeUs);
  EXPECT_GE(allStats->spilledFiles, inputStageStats->spilledFiles);
  EXPECT_GE(allStats->spillWrites, inputStageStats->spillWrites);
  EXPECT_GE(allStats->spillFlushTimeUs, inputStageStats->spillFlushTimeUs);
  EXPECT_GE(allStats->spillWriteTimeUs, inputStageStats->spillWriteTimeUs);
  EXPECT_LE(
      allStats->spillSerializationTimeUs + allStats->spillFlushTimeUs +
          allStats->spillWriteTimeUs,
      allStats->spillTotalTimeUs);
  const auto globalOutputStats =
      common::globalSpillStats() - globalStatsBeforeOutputSpill;
  EXPECT_EQ(globalOutputStats.spillRuns, 0);
  EXPECT_EQ(spillAccounting(globalOutputStats), spillAccounting(outputStats));
  EXPECT_EQ(
      spillFileWriteAccounting(globalOutputStats),
      spillFileWriteAccounting(outputStats));
  const auto readStatsAfterOutputSpill = buffer.spillReadStats();
  ASSERT_TRUE(readStatsAfterOutputSpill);
  if (readStatsBeforeOutputSpill) {
    EXPECT_GE(
        readStatsAfterOutputSpill->spillReadTimeUs,
        readStatsBeforeOutputSpill->spillReadTimeUs);
    EXPECT_GE(
        readStatsAfterOutputSpill->spillDecompressTimeUs,
        readStatsBeforeOutputSpill->spillDecompressTimeUs);
    EXPECT_GE(
        readStatsAfterOutputSpill->spillReadIOTimeUs,
        readStatsBeforeOutputSpill->spillReadIOTimeUs);
  }

  collectAndVerify(buffer, input, 2, 2, 0, 0, prefix);
  ASSERT_TRUE(buffer.spillReadStats());
}

TEST_F(RadixSortBufferTest, spillBatchOutputSizes) {
  auto input = makeKeyPayloadIdRows({8, 1, 7, 2, 6, 3, 5, 4});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  addInputRuns(buffer, input, {{3, true}, {3, true}, {2, false}});
  buffer.noMoreInput();

  std::vector<vector_size_t> batchSizes;
  std::vector<RowVectorPtr> batches;
  while (auto batch = buffer.getOutput(3)) {
    batchSizes.push_back(batch->size());
    batches.push_back(std::move(batch));
  }
  EXPECT_EQ(batchSizes, std::vector<vector_size_t>({3, 3, 2}));
  auto output = concatenateBatches(batches, input->size());
  expectRowsMatchById(*input, *output, 2);
  expectSorted(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortBufferTest, spillDisabledAndEmptySpill) {
  inputType_ = ROW({"key", "id"}, {BIGINT(), BIGINT()});
  RadixSortBuffer disabled(inputType_, {0}, {flags(true, true)}, pool());
  EXPECT_FALSE(disabled.canSpill());
  EXPECT_THROW(disabled.spill(), BoltException);

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  RadixSortBuffer empty(inputType_, {0}, {flags(true, true)}, pool(), &config);
  EXPECT_TRUE(empty.canSpill());
  empty.spill();
  empty.noMoreInput();
  EXPECT_FALSE(empty.spilledStats());
  EXPECT_EQ(empty.getOutput(1), nullptr);

  RadixSortBuffer postSpillInput(
      inputType_, {0}, {flags(true, true)}, pool(), &config);
  postSpillInput.spill();
  postSpillInput.addInput(makeRows(
      {"key", "id"},
      {makeVector<int64_t>(BIGINT(), {2, 1}),
       makeVector<int64_t>(BIGINT(), {0, 1})}));
  postSpillInput.noMoreInput();
  EXPECT_FALSE(postSpillInput.spilledStats());
  auto output = collect(postSpillInput, 1);
  expectSorted(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortBufferTest, spillOrderDirectionsAndNullPlacements) {
  auto input = makeRows(
      {"key", "id"},
      {makeVector<int32_t>(
           INTEGER(), {2, std::nullopt, 1, 2, std::nullopt, -1, 1}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5, 6})});
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  for (const auto ascending : {false, true}) {
    for (const auto nullsFirst : {false, true}) {
      const std::vector<CompareFlags> keyFlags{flags(ascending, nullsFirst)};
      SpillContext spill(*this, input);
      RadixSortBuffer buffer(inputType_, {0}, keyFlags, pool(), &spill.config);
      addInputRuns(buffer, input, {{3, true}, {input->size() - 3, false}});
      buffer.noMoreInput();
      collectAndVerify(buffer, input, 2, 1, 0, 0, nullptr, keyFlags.front());
      ASSERT_TRUE(buffer.spilledStats());
    }
  }
}

TEST_F(RadixSortBufferTest, compressedSpillOutput) {
  auto input = makeKeyPayloadIdRows({5, 1, 4, 2, 3});
  for (const auto& compression : {std::string("lz4"), std::string("zstd")}) {
    runSpillPlanAndVerify(
        input, {{3, true}, {2, false}}, 2, 2, 0, 0, compression);
  }
}

TEST_F(RadixSortBufferTest, outputStageSpillAfterPartialOutput) {
  auto input = makeKeyPayloadIdRows({8, 1, 7, 2, 6, 3, 5, 4});
  runSpillPlanAndVerify(input, {{input->size(), false}}, 2, 2, 0, 0, "lz4", 3);
}

TEST_F(RadixSortBufferTest, outputStageSpillAfterInputSpill) {
  auto input = makeKeyPayloadIdRows({9, 1, 8, 2, 7, 3, 6, 4, 5});
  runSpillPlanAndVerify(
      input, {{3, true}, {3, true}, {3, false}}, 2, 2, 0, 0, "none", 4);
}

TEST_F(RadixSortBufferTest, mixedEncodingsAcrossInputAndOutputSpills) {
  auto input =
      makeKeyPayloadIdRows({9, 1, 8, 4, 7, 3, 6, 2, 5, 0, std::nullopt, 10});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;

  auto first = slice(*input, 0, 4);
  buffer.addInput(first);
  buffer.spill();
  auto indices = makeBuffer<vector_size_t>({6, 4, 7, 5});
  auto nulls = allocateNulls(4, pool());
  bits::setNull(nulls->asMutable<uint64_t>(), 1);
  auto wrapped = makeRows(
      inputType_->names(),
      {BaseVector::wrapInDictionary(nulls, indices, 4, input->childAt(0)),
       BaseVector::wrapInConstant(4, 6, input->childAt(1)),
       makeVector<int64_t>(BIGINT(), {4, 5, 6, 7})});
  buffer.addInput(wrapped);
  buffer.spill();
  auto last = slice(*input, 8, 4);
  buffer.addInput(last);
  auto expected = concatenateBatches(
      {first, slice(*wrapped, 0, wrapped->size()), last}, input->size());
  buffer.noMoreInput();
  auto prefix = buffer.getOutput(5);
  ASSERT_NE(prefix, nullptr);
  ASSERT_EQ(prefix->size(), 5);
  spillRemainingOutputAndCheckStats(buffer, input->size() - prefix->size());

  collectAndVerify(buffer, expected, 3, 2, 0, 0, prefix);
  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, 3);
}

TEST_F(RadixSortBufferTest, prepareMergeFailureCleansAllFiles) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  {
    auto config = spillConfig(spillDirectory->path);
    auto input = makeKeyPayloadIdRows({6, 1, 5, 2, 4, 3});
    inputType_ = std::static_pointer_cast<const RowType>(input->type());
    RadixSortBuffer buffer(
        inputType_, {0}, {flags(true, true)}, pool(), &config);
    addInputRuns(buffer, slice(*input, 0, 3), {{3, true}});
    auto files = std::filesystem::directory_iterator(spillDirectory->path);
    ASSERT_NE(files, std::filesystem::directory_iterator{});
    const auto firstFile = files->path();
    addInputRuns(buffer, slice(*input, 3, 3), {{3, true}});
    files = std::filesystem::directory_iterator(spillDirectory->path);
    ASSERT_NE(files, std::filesystem::directory_iterator{});
    const auto first = files->path();
    ++files;
    ASSERT_NE(files, std::filesystem::directory_iterator{});
    std::filesystem::resize_file(first == firstFile ? files->path() : first, 1);

    EXPECT_THROW(buffer.noMoreInput(), BoltException);
  }
  EXPECT_TRUE(std::filesystem::is_empty(spillDirectory->path));
}

TEST_F(RadixSortBufferTest, outputStageSpillWithoutPayload) {
  auto input = makeRows(
      {"key"}, {makeVector<int64_t>(BIGINT(), {4, 1, std::nullopt, 3, 2})});
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  RadixSortBuffer buffer(
      inputType_, {0}, {flags(true, false)}, pool(), &config);
  buffer.addInput(input);
  buffer.noMoreInput();

  spillRemainingOutputAndCheckStats(buffer, input->size());
  auto output = collect(buffer, 2);
  expectSorted(*output, {0}, {flags(true, false)});
  auto* keys = output->childAt(0)->asUnchecked<SimpleVector<int64_t>>();
  ASSERT_EQ(output->size(), input->size());
  EXPECT_EQ(keys->valueAt(0), 1);
  EXPECT_EQ(keys->valueAt(1), 2);
  EXPECT_EQ(keys->valueAt(2), 3);
  EXPECT_EQ(keys->valueAt(3), 4);
  EXPECT_TRUE(keys->isNullAt(4));
  EXPECT_EQ(buffer.numOutputRows(), input->size());
  EXPECT_TRUE(buffer.spillReadStats());
}

TEST_F(RadixSortBufferTest, complexKeyAndPayloadSpillOutput) {
  auto input = makeArrayRowMapIdRows();
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  SpillContext spill(*this, input);
  const std::vector<column_index_t> keyChannels{0, 1};
  const std::vector<CompareFlags> keyFlags{
      flags(true, false), flags(false, true)};
  RadixSortBuffer buffer(
      inputType_, keyChannels, keyFlags, pool(), &spill.config);
  buffer.addInput(slice(*input, 0, 3));
  buffer.spill();
  buffer.addInput(slice(*input, 3, 4));
  buffer.noMoreInput();
  auto output = collect(buffer, 2);
  expectRowsMatchById(*input, *output, 3);
  expectSorted(*output, keyChannels, keyFlags);
}

TEST_F(RadixSortBufferTest, mapVarcharBigintOrderKeySpillOutput) {
  auto input = makeStringBigintMapKeyRows();
  const auto stats =
      runSpillPlanAndVerify(input, {{3, true}, {3, true}, {3, false}}, 2, 2);
  EXPECT_GT(stats.spilledRows, 0);
  EXPECT_GT(stats.spilledBytes, 0);
}

TEST_F(RadixSortBufferTest, eventStringKeyWideMapPayloadSpill) {
  struct Plan {
    const char* name;
    vector_size_t rows;
    std::vector<InputRun> runs;
    vector_size_t outputBatchSize;
  };
  for (const auto& plan : std::vector<Plan>{
           {"multiple spilled runs",
            96,
            {{33, true}, {31, true}, {32, false}},
            17},
           {"multiple inputs in one spilled run",
            72,
            {{25, false}, {23, true}, {24, false}},
            16}}) {
    SCOPED_TRACE(plan.name);
    auto input = makeEventMapPayloadRows(plan.rows);
    const auto stats = runSpillPlanAndVerify(
        input, plan.runs, plan.outputBatchSize, 0, 9, 10'000, "zstd");
    EXPECT_GT(stats.spilledRows, plan.rows / 2);
    EXPECT_GT(stats.spilledBytes, 0);
  }
}

TEST_F(RadixSortBufferTest, mixedWrappedVectorInputWithMapPayloadSpill) {
  constexpr vector_size_t kRows = 48;
  auto eventBase = makeStringVector(
      VARCHAR(),
      {"video_play", "like", "follow", "share", "comment", "enter_homepage"});
  auto eventIndices = generate<vector_size_t>(kRows, [&](vector_size_t row) {
    return row % 10 < 6 ? row % 3 : row % eventBase->size();
  });
  auto event = BaseVector::wrapInDictionary(
      nullptr, makeBuffer<vector_size_t>(eventIndices), kRows, eventBase);

  auto device = BaseVector::wrapInDictionary(
      nullptr,
      makeBuffer<vector_size_t>(generate<vector_size_t>(
          kRows, [](vector_size_t row) { return (row * 5) % 6; })),
      kRows,
      makeVector<int64_t>(BIGINT(), {1000, 1001, 1002, 1003, 1004, 1005}));
  auto hour =
      BaseVector::wrapInConstant(kRows, 0, makeStringVector(VARCHAR(), {"12"}));

  auto params = BaseVector::wrapInDictionary(
      nullptr,
      makeBuffer<vector_size_t>(generate<vector_size_t>(
          kRows, [&](vector_size_t row) { return (row * 7) % kRows; })),
      kRows,
      makeLargeStringStringMaps(kRows));

  auto relationTags = generate<std::optional<std::string>>(
      kRows, [](vector_size_t row) -> std::optional<std::string> {
        return row % 4 == 0 ? std::nullopt
                            : std::optional("tag_" + std::to_string(row % 5));
      });
  auto ids = generate<std::optional<int64_t>>(
      kRows, [](vector_size_t row) { return row; });
  auto input = makeRows(
      {"event", "device_id", "relation_tag", "hour", "params", "id"},
      {event,
       device,
       makeStringVector(VARCHAR(), relationTags),
       hour,
       params,
       makeVector<int64_t>(BIGINT(), ids)});
  const auto stats = runSpillPlanAndVerify(
      input,
      {{17, true}, {16, true}, {kRows - 33, false}},
      11,
      5,
      0,
      0,
      "zstd",
      7);
  EXPECT_GT(stats.spilledRows, kRows / 2);
  EXPECT_GT(stats.spilledFiles, 0);
  EXPECT_GT(stats.spillWrites, 0);
}

TEST_F(RadixSortBufferTest, outputStageSpillWithMapPayload) {
  constexpr vector_size_t kRows = 80;
  auto input = makeEventMapPayloadRows(kRows);
  runSpillPlanAndVerify(
      input,
      {{27, false}, {25, false}, {kRows - 52, false}},
      23,
      0,
      9,
      10'000,
      "lz4",
      19);
}

TEST_F(
    RadixSortBufferTest,
    compressedSpillOutputOwnsMapPayloadAfterBufferDestruction) {
  constexpr vector_size_t kRows = 48;
  std::vector<RowVectorPtr> batches;
  {
    auto input = makeEventMapPayloadRows(kRows);
    SpillContext spill(*this, input, "zstd", 0, 9);
    auto& buffer = spill.buffer;
    addInputRuns(buffer, input, {{17, true}, {16, true}, {kRows - 33, true}});
    buffer.noMoreInput();
    while (auto batch = buffer.getOutput(19)) {
      batches.push_back(std::move(batch));
    }
  }

  auto churn = makeEventMapPayloadRows(kRows, "churn_");
  (void)churn;
  auto output = concatenateBatches(batches, kRows);
  expectRowsMatchById(
      *makeEventMapPayloadRows(kRows), *output, 0, std::nullopt, 10'000);
  expectSorted(*output, {9}, {flags(true, true)});
}

TEST_F(RadixSortBufferTest, outputOwnsVariableDataAfterBufferDestruction) {
  RowVectorPtr output;
  {
    auto input = makeRows(
        {"key", "payload"},
        {makeVector<int64_t>(BIGINT(), {3, 1, 2}),
         makeStringVector(
             VARCHAR(),
             {std::string(128, 'c'),
              std::string(128, 'a'),
              std::string(128, 'b')})});
    auto inputType = std::static_pointer_cast<const RowType>(input->type());
    RadixSortBuffer buffer(inputType, {0}, {flags(true, true)}, pool());
    buffer.addInput(input);
    buffer.noMoreInput();
    output = buffer.getOutput(3);
    ASSERT_NE(output, nullptr);
  }
  ASSERT_EQ(output->size(), 3);
  EXPECT_EQ(
      output->childAt(1)
          ->asUnchecked<SimpleVector<StringView>>()
          ->valueAt(0)
          .getString(),
      std::string(128, 'a'));
}

TEST_F(RadixSortBufferTest, stateStatsAndEmptyInput) {
  auto inputType = ROW({"key"}, {BIGINT()});
  RadixSortBuffer buffer(inputType, {0}, {flags(true, true)}, pool());
  EXPECT_THROW(buffer.getOutput(1), BoltException);

  auto empty = makeRows({"key"}, {BaseVector::create(BIGINT(), 0, pool())});
  buffer.addInput(empty);
  EXPECT_EQ(buffer.numInputRows(), 0);
  buffer.noMoreInput();
  EXPECT_EQ(buffer.getOutput(1), nullptr);
  EXPECT_EQ(buffer.numOutputRows(), 0);
  ASSERT_TRUE(buffer.sortStats());
  EXPECT_THROW(buffer.addInput(empty), BoltException);

  RadixSortBuffer nonEmpty(inputType, {0}, {flags(true, true)}, pool());
  nonEmpty.addInput(makeRows({"key"}, {makeVector<int64_t>(BIGINT(), {1})}));
  nonEmpty.noMoreInput();
}

TEST_F(RadixSortBufferTest, rejectsUnsupportedOrderKeysAndPayload) {
  const auto compareFlags = std::vector<CompareFlags>{flags(true, true)};
  for (const auto& keyType : std::vector<TypePtr>{
           VARIANT(),
           ARRAY(VARIANT()),
           ROW({INTEGER(), VARIANT()}),
           OPAQUE<int32_t>(),
           FUNCTION({BIGINT()}, BOOLEAN()),
           ARRAY(OPAQUE<int32_t>()),
           ROW({INTEGER(), FUNCTION({BIGINT()}, BOOLEAN())}),
           MAP(VARCHAR(), OPAQUE<int32_t>())}) {
    SCOPED_TRACE(keyType->toString());
    EXPECT_THROW(
        RadixSortBuffer(ROW({"key"}, {keyType}), {0}, compareFlags, pool()),
        BoltException);
  }

  EXPECT_THROW(
      RadixSortBuffer(
          ROW({"key", "payload"}, {BIGINT(), OPAQUE<int32_t>()}),
          {0},
          compareFlags,
          pool()),
      BoltException);
}

TEST_F(RadixSortBufferTest, validatesConstructionAndInputContracts) {
  auto inputType = ROW({"key", "value"}, {BIGINT(), VARCHAR()});
  EXPECT_THROW(
      RadixSortBuffer(
          inputType, {inputType->size()}, {flags(true, true)}, pool()),
      BoltException);

  auto invalidFlags = flags(true, true);
  invalidFlags.equalsOnly = true;
  EXPECT_THROW(
      RadixSortBuffer(inputType, {0}, {invalidFlags}, pool()), BoltException);
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
