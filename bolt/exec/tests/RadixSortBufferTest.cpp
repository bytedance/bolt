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
#include <array>
#include <atomic>
#include <bit>
#include <filesystem>
#include <limits>
#include <numeric>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/Spill.h"
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

class RadixSortBufferTestHelper {
 public:
  struct AdmissionEstimate {
    uint64_t outputGrowth;
    uint64_t scratchGrowth;
    uint64_t total;
  };

  static AdmissionEstimate outputAdmissionEstimate(
      RadixSortBuffer& buffer,
      vector_size_t rows) {
    const auto estimate = buffer.outputAdmissionEstimate(rows);
    return {estimate.outputGrowth, estimate.scratchGrowth, estimate.total()};
  }

  static void dropMergePointerScratch(RadixSortBuffer& buffer) {
    buffer.mergeKeyRows_.reset();
    buffer.mergePayloadRows_.reset();
  }

  static void ensureOutputFits(RadixSortBuffer& buffer, vector_size_t rows) {
    buffer.ensureOutputFits(rows);
  }

  static void shareOutputChild(RadixSortBuffer& buffer, VectorPtr& child) {
    BOLT_CHECK_NOT_NULL(buffer.output_);
    child = buffer.output_->childAt(0);
  }
};

namespace {

class RadixSortBufferTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    filesystems::registerLocalFileSystem();
    BOLT_TEST_VALUE_ENABLE();
  }

 protected:
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("radix-sort-buffer-test")};
  tsan_atomic<bool> nonReclaimableSection_{true};

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

  static void expectRowsEqual(
      const RowVector& expected,
      const RowVector& actual) {
    ASSERT_EQ(expected.size(), actual.size());
    ASSERT_EQ(expected.childrenSize(), actual.childrenSize());
    for (uint32_t column = 0; column < expected.childrenSize(); ++column) {
      for (vector_size_t row = 0; row < expected.size(); ++row) {
        EXPECT_TRUE(expected.childAt(column)->equalValueAt(
            actual.childAt(column).get(), row, row))
            << "column=" << column << ", row=" << row;
      }
    }
  }

  static void expectSpillReadStatsEqual(
      const std::optional<common::SpillReadStats>& expected,
      const std::optional<common::SpillReadStats>& actual) {
    ASSERT_EQ(expected.has_value(), actual.has_value());
    if (!expected.has_value()) {
      return;
    }
    EXPECT_EQ(expected->spillReadTimeUs, actual->spillReadTimeUs);
    EXPECT_EQ(expected->spillDecompressTimeUs, actual->spillDecompressTimeUs);
    EXPECT_EQ(expected->spillReadIOTimeUs, actual->spillReadIOTimeUs);
  }

  static void expectSpillReadStatsNotDecreased(
      const std::optional<common::SpillReadStats>& before,
      const std::optional<common::SpillReadStats>& after) {
    ASSERT_EQ(before.has_value(), after.has_value());
    if (!before.has_value()) {
      return;
    }
    EXPECT_GE(after->spillReadTimeUs, before->spillReadTimeUs);
    EXPECT_GE(after->spillDecompressTimeUs, before->spillDecompressTimeUs);
    EXPECT_GE(after->spillReadIOTimeUs, before->spillReadIOTimeUs);
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
      const CompareFlags& keyFlags =
          SortComparatorOracle::makeSortFlags(true, true)) {
    auto output = collect(buffer, batchSize, prefix);
    SortComparatorOracle::expectRowsMatchById(
        *input, *output, idChannel, {.idBase = idBase});
    SortComparatorOracle::expectSorted(*output, {keyChannel}, {keyFlags});
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
    SortComparatorOracle::expectRowsMatchById(*input, *output, idChannel);
    SortComparatorOracle::expectSorted(*output, keyChannels, keyFlags);
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

  void spillMemoryRunAndCheckStats(
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
    EXPECT_LE(buffer.spilledStats()->spilledRows, buffer.numInputRows());
    EXPECT_GT(buffer.spilledStats()->spilledBytes, statsBefore.spilledBytes);
    const auto statsAfter = *buffer.spilledStats();
    buffer.spill();
    EXPECT_EQ(*buffer.spilledStats(), statsAfter);
  }

  struct SpillMemoryRunOptions {
    vector_size_t prefixRows;
    size_t expectedSpillRuns;
    size_t expectedMergeStreams;
    uint64_t expectedSpilledRows;
  };

  void spillMemoryRunAndVerifyReplacement(
      RadixSortBuffer& buffer,
      const RowVectorPtr& input,
      SpillMemoryRunOptions options) {
    auto prefix = buffer.getOutput(options.prefixRows);
    ASSERT_NE(prefix, nullptr);
    ASSERT_EQ(prefix->size(), options.prefixRows);

    spillMemoryRunAndCheckStats(buffer, options.expectedSpilledRows);
    EXPECT_EQ(buffer.testingSpilledRunCount(), 0);
    EXPECT_EQ(buffer.testingMergeStreamCount(), options.expectedMergeStreams);
    ASSERT_TRUE(buffer.spilledStats());
    EXPECT_EQ(buffer.spilledStats()->spillRuns, options.expectedSpillRuns);
    EXPECT_GT(buffer.spilledStats()->spilledFiles, 1);

    collectAndVerify(
        buffer,
        input,
        /*batchSize=*/257,
        /*idChannel=*/1,
        /*keyChannel=*/0,
        /*idBase=*/0,
        prefix);
  }

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  common::SpillConfig spillConfig(
      const std::string& directory,
      const std::string& compressionKind = "none",
      common::UpdateAndCheckSpillLimitCB spillLimit = [](uint64_t) {},
      uint64_t maxFileSize = 0,
      uint64_t writeBufferSize = 0) const {
    return common::SpillConfig(
        [directory]() -> const std::string& { return directory; },
        std::move(spillLimit),
        "radix-sort-buffer-spill",
        maxFileSize,
        false,
        writeBufferSize,
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
        column_index_t keyChannel = 0,
        uint64_t maxFileSize = 0,
        uint64_t writeBufferSize = 0)
        : directory(exec::test::TempDirectoryPath::create()),
          config(test.spillConfig(
              directory->path,
              compression,
              [](uint64_t) {},
              maxFileSize,
              writeBufferSize)),
          buffer(
              std::static_pointer_cast<const RowType>(input->type()),
              {keyChannel},
              {SortComparatorOracle::makeSortFlags(true, true)},
              test.pool(),
              &config,
              spillMemoryThreshold,
              nullptr,
              &test.nonReclaimableSection_) {
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

  RowVectorPtr makeLargeStringKeyIdRows(
      const std::vector<int64_t>& keyRanks,
      int64_t idBase = 0) {
    std::vector<std::optional<std::string>> keys;
    keys.reserve(keyRanks.size());
    for (vector_size_t row = 0; row < keyRanks.size(); ++row) {
      auto key = fmt::format("{:08d}_", keyRanks[row]);
      key.append(700 * 1024, static_cast<char>('a' + ((idBase + row) % 26)));
      keys.push_back(std::move(key));
    }
    auto ids = generate<std::optional<int64_t>>(
        keyRanks.size(), [idBase](vector_size_t row) { return idBase + row; });
    return makeRows(
        {"key", "id"},
        {makeStringVector(VARCHAR(), keys),
         makeVector<int64_t>(BIGINT(), ids)});
  }

  common::SpillStats runSpillPlanAndVerify(
      const RowVectorPtr& input,
      const std::vector<InputRun>& runs,
      vector_size_t outputBatchSize,
      column_index_t idChannel,
      column_index_t keyChannel = 0,
      int64_t idBase = 0,
      std::string_view compression = "none",
      std::optional<vector_size_t> prefixRows = std::nullopt,
      std::optional<uint64_t> expectedOutputSpillRows = std::nullopt) {
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
      spillMemoryRunAndCheckStats(
          buffer,
          expectedOutputSpillRows.value_or(
              buffer.numInputRows() - prefix->size()));
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
      SortComparatorOracle::makeSortFlags(true, false),
      SortComparatorOracle::makeSortFlags(false, true)};

  for (const auto batchSize : {1, 17, 2048}) {
    sortAndVerify(input, keyChannels, keyFlags, 3, batchSize);
  }
}

TEST_F(RadixSortBufferTest, estimateOutputRowSizeMatchesLegacyForComplexRows) {
  constexpr vector_size_t kRows = 128;
  auto input = makeEventMapPayloadRows(kRows);
  auto inputType = std::static_pointer_cast<const RowType>(input->type());
  const std::vector<column_index_t> keyChannels{9};
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, true)};

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
      const std::vector<CompareFlags> keyFlags{
          SortComparatorOracle::makeSortFlags(ascending, nullsFirst)};
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
    const std::vector<CompareFlags> keyFlags{
        SortComparatorOracle::makeSortFlags(true, false)};
    sortAndVerify(input, keyChannels, keyFlags, 2, 2, false);
  }
}

TEST_F(RadixSortBufferTest, arrayAndRowKeysWithComplexPayload) {
  auto input = makeArrayRowMapIdRows();
  const std::vector<column_index_t> keyChannels{0, 1};
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, false),
      SortComparatorOracle::makeSortFlags(false, true)};
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
      {SortComparatorOracle::makeSortFlags(true, false),
       SortComparatorOracle::makeSortFlags(false, true),
       SortComparatorOracle::makeSortFlags(true, true)},
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
      {SortComparatorOracle::makeSortFlags(true, true)},
      "SELECT * FROM tmp ORDER BY list_transform(list_sort(map_entries(c0)), "
      "x -> x.key) ASC NULLS FIRST, "
      "list_transform(list_sort(map_entries(c0)), x -> x.value) ASC NULLS FIRST",
      2);
  sortAndVerifyWithDuckDb(
      input,
      {0},
      {SortComparatorOracle::makeSortFlags(false, false)},
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
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(false, false)};
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
      SortComparatorOracle::makeSortFlags(true, false),
      SortComparatorOracle::makeSortFlags(true, false)};
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
  auto output = sortAndCollect(
      input,
      {0},
      {SortComparatorOracle::makeSortFlags(true, true)},
      kRows,
      false);
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
  SortComparatorOracle::expectRowsMatchById(
      *input, *output, 25, {.directlyCheckedColumn = 20});
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
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
  RadixSortBuffer buffer(
      inputType_,
      {0},
      {SortComparatorOracle::makeSortFlags(true, true)},
      pool());
  buffer.addInput(input);
  buffer.noMoreInput();
  auto output = collect(buffer, 3);

  SortComparatorOracle::expectRowsMatchById(*input, *output, 2);
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
}

TEST_F(RadixSortBufferTest, outputVectorReuse) {
  auto verify = [&](RadixSortBuffer& buffer,
                    const char* mode,
                    bool expectChildReuse) {
    SCOPED_TRACE(mode);
    auto first = buffer.getOutput(2);
    ASSERT_NE(first, nullptr);
    ASSERT_EQ(first->size(), 2);
    const auto firstOutput = first.get();
    const std::vector<int64_t> firstIds{idAt(*first, 0, 2), idAt(*first, 1, 2)};
    const auto* firstPayloads =
        first->childAt(1)->asUnchecked<SimpleVector<StringView>>();
    std::vector<std::optional<std::string>> firstPayloadValues;
    for (vector_size_t row = 0; row < first->size(); ++row) {
      firstPayloadValues.push_back(
          firstPayloads->isNullAt(row)
              ? std::nullopt
              : std::optional(firstPayloads->valueAt(row).getString()));
    }

    auto second = buffer.getOutput(2);
    ASSERT_NE(second, nullptr);
    ASSERT_EQ(second->size(), 2);
    EXPECT_NE(second.get(), firstOutput);
    EXPECT_EQ(idAt(*first, 0, 2), firstIds[0]);
    EXPECT_EQ(idAt(*first, 1, 2), firstIds[1]);
    for (vector_size_t row = 0; row < first->size(); ++row) {
      EXPECT_EQ(firstPayloads->isNullAt(row), !firstPayloadValues[row]);
      if (firstPayloadValues[row]) {
        EXPECT_EQ(
            firstPayloads->valueAt(row).getString(), *firstPayloadValues[row]);
      }
    }

    const auto secondOutput = second.get();
    std::vector<const BaseVector*> secondChildren;
    secondChildren.reserve(second->childrenSize());
    for (const auto& child : second->children()) {
      secondChildren.push_back(child.get());
    }
    std::vector<int64_t> ids = firstIds;
    ids.push_back(idAt(*second, 0, 2));
    ids.push_back(idAt(*second, 1, 2));
    first.reset();
    second.reset();

    auto third = buffer.getOutput(2);
    ASSERT_NE(third, nullptr);
    ASSERT_EQ(third->size(), 2);
    EXPECT_EQ(third.get(), secondOutput);
    if (expectChildReuse) {
      ASSERT_EQ(third->childrenSize(), secondChildren.size());
      for (uint32_t column = 0; column < third->childrenSize(); ++column) {
        EXPECT_EQ(third->childAt(column).get(), secondChildren[column])
            << "column=" << column;
      }
    }
    ids.push_back(idAt(*third, 0, 2));
    ids.push_back(idAt(*third, 1, 2));

    EXPECT_EQ(buffer.getOutput(2), nullptr);
    EXPECT_EQ(ids, (std::vector<int64_t>{1, 3, 5, 4, 2, 0}));
  };

  auto input = makeKeyPayloadIdRows({6, 1, 5, 2, 4, 3});
  const auto inputType = std::static_pointer_cast<const RowType>(input->type());
  {
    RadixSortBuffer buffer(
        inputType,
        {0},
        {SortComparatorOracle::makeSortFlags(true, true)},
        pool());
    buffer.addInput(input);
    buffer.noMoreInput();
    verify(buffer, "in-memory", false);
  }

  {
    SpillContext spill(*this, input);
    auto& buffer = spill.buffer;
    buffer.addInput(input);
    buffer.spill();
    buffer.noMoreInput();
    verify(buffer, "spill-merge", true);
  }
}

TEST_F(RadixSortBufferTest, directOutputReusesMaterialization) {
  auto input = makeRows(
      {"key", "payload"},
      {makeVector<int64_t>(BIGINT(), {6, 5, 4, 3, 2, 1}),
       makeVector<int64_t>(BIGINT(), {60, 50, 40, 30, 20, 10})});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  buffer.addInput(input);
  buffer.noMoreInput();
  const auto expectBatch = [](const RowVectorPtr& batch, int64_t firstKey) {
    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->size(), 2);
    const auto* keys = batch->childAt(0)->asUnchecked<SimpleVector<int64_t>>();
    const auto* payloads =
        batch->childAt(1)->asUnchecked<SimpleVector<int64_t>>();
    for (vector_size_t row = 0; row < 2; ++row) {
      EXPECT_EQ(keys->valueAt(row), firstKey + row);
      EXPECT_EQ(payloads->valueAt(row), (firstKey + row) * 10);
    }
  };

  auto first = buffer.getOutput(2);
  expectBatch(first, 1);
  std::vector<const BaseVector*> firstChildren;
  for (const auto& child : first->children()) {
    firstChildren.push_back(child.get());
  }
  first.reset();

  auto second = buffer.getOutput(2);
  expectBatch(second, 3);
  ASSERT_EQ(second->childrenSize(), firstChildren.size());
  for (uint32_t channel = 0; channel < second->childrenSize(); ++channel) {
    EXPECT_EQ(second->childAt(channel).get(), firstChildren[channel]);
  }
  second.reset();
  const auto reused =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, 2);
  EXPECT_EQ(reused.outputGrowth, 0);
  EXPECT_EQ(reused.scratchGrowth, 0);
  EXPECT_EQ(reused.total, 0);
  auto third = buffer.getOutput(2);
  expectBatch(third, 5);
  EXPECT_EQ(buffer.getOutput(2), nullptr);
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

TEST_F(RadixSortBufferTest, equalAndNullVariableKeysAcrossDiskStreams) {
  const std::string equalKey(80, 'e');
  auto input = makeRows(
      {"key", "id"},
      {makeStringVector(
           VARCHAR(),
           {equalKey,
            std::nullopt,
            std::string(80, 'z'),
            std::nullopt,
            equalKey,
            std::string(80, 'a'),
            equalKey,
            std::nullopt,
            std::string(80, 'm')}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5, 6, 7, 8})});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  addInputRuns(buffer, input, {{3, true}, {3, true}, {3, false}});

  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, 2);
  EXPECT_EQ(buffer.spilledStats()->spilledRows, 6);
  buffer.noMoreInput();
  EXPECT_EQ(buffer.testingMergeStreamCount(), 3);
  collectAndVerify(buffer, input, 1, 1);
}

TEST_F(RadixSortBufferTest, splitSpillFilesMergeAsSingleLogicalRun) {
  constexpr vector_size_t kRows = 4;
  constexpr uint64_t kMaxFileSize = 512 * 1024;
  auto input = makeLargeStringKeyIdRows(
      generate<int64_t>(kRows, [](vector_size_t row) { return kRows - row; }));
  SpillContext spill(
      *this,
      input,
      "none",
      /*spillMemoryThreshold=*/0,
      /*keyChannel=*/0,
      kMaxFileSize);
  auto& buffer = spill.buffer;
  buffer.addInput(input);
  buffer.spill();

  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, 1);
  EXPECT_EQ(buffer.testingSpilledRunCount(), 1);
  ASSERT_GT(buffer.spilledStats()->spilledFiles, 1);

  buffer.noMoreInput();
  EXPECT_EQ(buffer.testingMergeStreamCount(), 1);
  collectAndVerify(buffer, input, 257, 1);
}

TEST_F(RadixSortBufferTest, splitSpillFilesPreserveRunMergeFanIn) {
  constexpr vector_size_t kRowsPerRun = 4;
  constexpr uint64_t kMaxFileSize = 512 * 1024;
  std::vector<int64_t> keys;
  keys.reserve(kRowsPerRun * 2);
  for (vector_size_t row = 0; row < kRowsPerRun; ++row) {
    keys.push_back(2 * row + 1);
  }
  for (vector_size_t row = 0; row < kRowsPerRun; ++row) {
    keys.push_back(2 * row);
  }
  auto input = makeLargeStringKeyIdRows(keys);
  SpillContext spill(
      *this,
      input,
      "none",
      /*spillMemoryThreshold=*/0,
      /*keyChannel=*/0,
      kMaxFileSize);
  auto& buffer = spill.buffer;
  buffer.addInput(slice(*input, 0, kRowsPerRun));
  buffer.spill();
  buffer.addInput(slice(*input, kRowsPerRun, kRowsPerRun));
  buffer.spill();

  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, 2);
  EXPECT_EQ(buffer.testingSpilledRunCount(), 2);
  ASSERT_GT(buffer.spilledStats()->spilledFiles, 2);

  buffer.noMoreInput();
  EXPECT_EQ(buffer.testingMergeStreamCount(), 2);
  collectAndVerify(buffer, input, 1024, 1);
}

TEST_F(RadixSortBufferTest, spillMergeReadersUseOperatorPool) {
  auto verify = [&](const RowVectorPtr& input,
                    uint64_t maxFileSize,
                    bool expectSplitFiles,
                    column_index_t idChannel) {
    SpillContext spill(
        *this,
        input,
        "none",
        /*spillMemoryThreshold=*/0,
        /*keyChannel=*/0,
        maxFileSize);
    auto& buffer = spill.buffer;
    buffer.addInput(input);
    buffer.spill();

    ASSERT_TRUE(buffer.spilledStats());
    if (expectSplitFiles) {
      ASSERT_GT(buffer.spilledStats()->spilledFiles, 1);
    } else {
      ASSERT_EQ(buffer.spilledStats()->spilledFiles, 1);
    }

    const auto spillPoolBytesBefore =
        memory::spillMemoryPool()->stats().currentBytes;
    const auto operatorPoolBytesBefore = pool()->currentBytes();
    buffer.noMoreInput();

    EXPECT_EQ(
        memory::spillMemoryPool()->stats().currentBytes, spillPoolBytesBefore);
    EXPECT_GT(pool()->currentBytes(), operatorPoolBytesBefore);
    EXPECT_EQ(buffer.testingMergeStreamCount(), 1);

    collectAndVerify(
        buffer,
        input,
        /*batchSize=*/257,
        idChannel,
        /*keyChannel=*/0);
  };

  verify(makeKeyPayloadIdRows({4, 1, 3, 2}), 0, false, 2);

  constexpr vector_size_t kRows = 4;
  verify(
      makeLargeStringKeyIdRows(generate<int64_t>(
          kRows, [](vector_size_t row) { return kRows - row; })),
      512 * 1024,
      true,
      1);
}

TEST_F(RadixSortBufferTest, spillMergeWidePayloadStaysWithinOperatorCap) {
  constexpr vector_size_t kRowsPerRun = 64;
  constexpr vector_size_t kRows = 2 * kRowsPerRun;
  constexpr size_t kPayloadBytes = 100 * 1024;
  constexpr int64_t kSortCapacity = 24 << 20;

  std::vector<std::optional<int64_t>> keys;
  std::vector<std::optional<std::string>> payloads;
  keys.reserve(kRows);
  payloads.reserve(kRows);
  for (vector_size_t run = 0; run < 2; ++run) {
    for (vector_size_t row = 0; row < kRowsPerRun; ++row) {
      keys.push_back(2 * row + 1 - run);
      payloads.push_back(
          fmt::format("payload_{:03d}_", 2 * row + 1 - run) +
          std::string(
              kPayloadBytes - 12,
              static_cast<char>('a' + (run * kRowsPerRun + row) % 26)));
    }
  }
  auto input = makeKeyPayloadIdRows(keys, payloads);
  auto directory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(directory->path, "none");
  auto sortRoot = memory::memoryManager()->addRootPool(
      "radix-sort-buffer-wide-payload-cap-root", kSortCapacity);
  auto sortPool = sortRoot->addLeafChild("radix-sort-buffer-wide-payload-cap");
  RadixSortBuffer buffer(
      std::static_pointer_cast<const RowType>(input->type()),
      {0},
      {SortComparatorOracle::makeSortFlags(true, true)},
      sortPool.get(),
      &config,
      /*spillMemoryThreshold=*/0,
      /*operatorCtx=*/nullptr,
      &nonReclaimableSection_);

  buffer.addInput(slice(*input, 0, kRowsPerRun));
  buffer.spill();
  buffer.addInput(slice(*input, kRowsPerRun, kRowsPerRun));
  buffer.spill();
  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, 2);
  EXPECT_EQ(buffer.spilledStats()->spilledRows, kRows);
  EXPECT_GE(buffer.spilledStats()->spillWrites, 12);

  buffer.noMoreInput();
  EXPECT_EQ(buffer.testingMergeStreamCount(), 2);
  sortPool->release();
  auto output = buffer.getOutput(kRows);

  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), kRows);
  EXPECT_EQ(buffer.getOutput(kRows), nullptr);
  SortComparatorOracle::expectRowsMatchById(*input, *output, 2);
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
  EXPECT_EQ(buffer.numOutputRows(), kRows);
  EXPECT_LE(sortRoot->peakBytes(), kSortCapacity);
}

TEST_F(RadixSortBufferTest, outputAdmissionUsesIncrementalEstimate) {
  constexpr vector_size_t kRows = 8;
  auto input = makeKeyPayloadIdRows(
      generate<std::optional<int64_t>>(
          kRows, [](vector_size_t row) { return kRows - row; }),
      generate<std::optional<std::string>>(kRows, [](vector_size_t row) {
        return std::string(64 + row, static_cast<char>('a' + row));
      }));
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  buffer.addInput(input);
  buffer.noMoreInput();

  const auto before =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, 4);
  EXPECT_GT(before.outputGrowth, 0);
  EXPECT_EQ(
      before.total,
      before.outputGrowth + before.outputGrowth / 5 + before.scratchGrowth);
  EXPECT_LT(before.total, before.outputGrowth * 2);

  auto first = buffer.getOutput(4);
  ASSERT_NE(first, nullptr);
  first.reset();
  const auto after =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, 4);
  // String backing capacity is data-dependent, so the cheap estimator
  // conservatively reserves output growth even when the RowVector is writable.
  EXPECT_EQ(after.outputGrowth, before.outputGrowth);
  EXPECT_EQ(
      after.total,
      after.outputGrowth + after.outputGrowth / 5 + after.scratchGrowth);
  EXPECT_LT(after.scratchGrowth, before.scratchGrowth);
}

TEST_F(RadixSortBufferTest, reusableFixedOutputStillAccountsForScratch) {
  auto input = makeRows(
      {"key", "payload"},
      {makeVector<int64_t>(BIGINT(), {4, 3, 2, 1}),
       makeVector<int64_t>(BIGINT(), {40, 30, 20, 10})});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  buffer.addInput(slice(*input, 0, 2));
  buffer.spill();
  buffer.addInput(slice(*input, 2, 2));
  buffer.spill();
  buffer.noMoreInput();

  auto first = buffer.getOutput(2);
  ASSERT_NE(first, nullptr);
  const auto reusedOutputSize = first->size();
  std::vector<uint64_t> childValueCapacities;
  childValueCapacities.reserve(first->childrenSize());
  for (const auto& child : first->children()) {
    ASSERT_NE(child, nullptr);
    ASSERT_NE(child->values(), nullptr);
    childValueCapacities.push_back(child->values()->capacity());
  }
  first.reset();
  const auto warmEstimate =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, 2);
  EXPECT_EQ(warmEstimate.outputGrowth, 0);
  EXPECT_EQ(warmEstimate.scratchGrowth, 0);
  EXPECT_EQ(warmEstimate.total, 0);
  ASSERT_EQ(reusedOutputSize, 2);
  for (const auto capacity : childValueCapacities) {
    EXPECT_GE(capacity, 3 * sizeof(int64_t));
  }
  const auto largerBatch =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, 3);
  EXPECT_EQ(largerBatch.outputGrowth, 0);
  EXPECT_EQ(largerBatch.total, 0);

  RadixSortBufferTestHelper::dropMergePointerScratch(buffer);
  const auto missingScratch =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, 2);
  EXPECT_EQ(missingScratch.outputGrowth, 0);
  EXPECT_GT(missingScratch.scratchGrowth, 0);
  EXPECT_EQ(missingScratch.total, missingScratch.scratchGrowth);
  auto second = buffer.getOutput(2);
  ASSERT_NE(second, nullptr);
  const auto* keys = second->childAt(0)->asUnchecked<SimpleVector<int64_t>>();
  const auto* payloads =
      second->childAt(1)->asUnchecked<SimpleVector<int64_t>>();
  EXPECT_EQ(keys->valueAt(0), 3);
  EXPECT_EQ(keys->valueAt(1), 4);
  EXPECT_EQ(payloads->valueAt(0), 30);
  EXPECT_EQ(payloads->valueAt(1), 40);
  EXPECT_EQ(buffer.getOutput(2), nullptr);
}

TEST_F(RadixSortBufferTest, sharedOutputChildRequiresOutputAdmission) {
  auto input = makeRows(
      {"key", "payload"},
      {makeVector<int64_t>(BIGINT(), {4, 3, 2, 1}),
       makeVector<int64_t>(BIGINT(), {40, 30, 20, 10})});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  buffer.addInput(input);
  buffer.noMoreInput();

  auto first = buffer.getOutput(2);
  ASSERT_NE(first, nullptr);
  VectorPtr heldChild;
  RadixSortBufferTestHelper::shareOutputChild(buffer, heldChild);
  first.reset();
  const auto estimate =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, 2);
  EXPECT_GT(estimate.outputGrowth, 0);
  EXPECT_EQ(
      estimate.total,
      estimate.outputGrowth + estimate.outputGrowth / 5 +
          estimate.scratchGrowth);
  auto second = buffer.getOutput(2);
  ASSERT_NE(second, nullptr);
  const auto* heldKeys = heldChild->asUnchecked<SimpleVector<int64_t>>();
  EXPECT_EQ(heldKeys->valueAt(0), 1);
  EXPECT_EQ(heldKeys->valueAt(1), 2);
  EXPECT_EQ(
      second->childAt(0)->asUnchecked<SimpleVector<int64_t>>()->valueAt(0), 3);
  EXPECT_EQ(
      second->childAt(0)->asUnchecked<SimpleVector<int64_t>>()->valueAt(1), 4);
  EXPECT_EQ(buffer.getOutput(2), nullptr);
}

TEST_F(RadixSortBufferTest, nullableFixedOutputRequiresOutputAdmission) {
  auto input = makeRows(
      {"key", "payload"},
      {makeVector<int64_t>(BIGINT(), {1, 2, 3, 4}),
       makeVector<int64_t>(BIGINT(), {10, 20, std::nullopt, 40})});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  addInputRuns(buffer, input, {{2, true}, {2, true}});
  buffer.noMoreInput();

  auto first = buffer.getOutput(2);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(first->childAt(1)->nulls(), nullptr);
  EXPECT_FALSE(first->childAt(1)->isNullAt(0));
  EXPECT_FALSE(first->childAt(1)->isNullAt(1));
  first.reset();
  const auto estimate =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, 2);
  EXPECT_GT(estimate.outputGrowth, 0);
  EXPECT_EQ(
      estimate.total,
      estimate.outputGrowth + estimate.outputGrowth / 5 +
          estimate.scratchGrowth);
  auto second = buffer.getOutput(2);
  ASSERT_NE(second, nullptr);
  const auto* payloads =
      second->childAt(1)->asUnchecked<SimpleVector<int64_t>>();
  EXPECT_TRUE(payloads->isNullAt(0));
  EXPECT_FALSE(payloads->isNullAt(1));
  EXPECT_EQ(payloads->valueAt(1), 40);
  EXPECT_EQ(buffer.getOutput(2), nullptr);
}

TEST_F(RadixSortBufferTest, nullableFixedOutputReusesCachedNullBitmap) {
  auto input = makeRows(
      {"key", "payload"},
      {makeVector<int64_t>(BIGINT(), {1, 2, 3, 4}),
       makeVector<int64_t>(BIGINT(), {10, std::nullopt, 30, 40})});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  addInputRuns(buffer, input, {{2, true}, {2, true}});
  buffer.noMoreInput();

  auto first = buffer.getOutput(2);
  ASSERT_NE(first, nullptr);
  EXPECT_TRUE(first->childAt(1)->isNullAt(1));
  first->childAt(1)->setNullCount(1);
  first.reset();
  const auto estimate =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, 2);
  EXPECT_EQ(estimate.outputGrowth, 0);
  EXPECT_EQ(estimate.total, estimate.scratchGrowth);
  auto second = buffer.getOutput(2);
  ASSERT_NE(second, nullptr);
  const auto* payloads =
      second->childAt(1)->asUnchecked<SimpleVector<int64_t>>();
  EXPECT_FALSE(payloads->isNullAt(0));
  EXPECT_FALSE(payloads->isNullAt(1));
  EXPECT_EQ(payloads->valueAt(0), 30);
  EXPECT_EQ(payloads->valueAt(1), 40);
  EXPECT_EQ(buffer.getOutput(2), nullptr);
}

DEBUG_ONLY_TEST_F(
    RadixSortBufferTest,
    outputAdmissionWithEnoughReservationSkipsMaybeReserve) {
  constexpr vector_size_t kBatchRows = 80;
  auto input = makeRows(
      {"key"},
      {generateVector<int64_t>(BIGINT(), kBatchRows, [](vector_size_t row) {
        return kBatchRows - row;
      })});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  buffer.addInput(input);
  buffer.noMoreInput();

  const auto estimate =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, kBatchRows);
  const auto need = estimate.total;
  ASSERT_GT(need, 0);
  ASSERT_TRUE(pool()->maybeReserve(need));
  ASSERT_GE(pool()->availableReservation(), need);
  const auto alignment = static_cast<uint64_t>(pool()->alignment());
  const auto targetReservation =
      ((need + alignment - 1) / alignment) * alignment;
  ASSERT_EQ(targetReservation, need);
  ASSERT_GE(pool()->availableReservation(), targetReservation);
  const auto excessReservation =
      static_cast<uint64_t>(pool()->availableReservation() - targetReservation);
  void* reservationPadding = nullptr;
  if (excessReservation > 0) {
    ASSERT_EQ(excessReservation % alignment, 0);
    reservationPadding = pool()->allocate(excessReservation);
  }
  ASSERT_EQ(pool()->availableReservation(), targetReservation);
  ASSERT_GE(pool()->availableReservation(), need);

  uint32_t maybeReserveCalls = 0;
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          [&](memory::MemoryPoolImpl* candidate) {
            if (candidate == pool()) {
              ++maybeReserveCalls;
            }
          }));
  RadixSortBufferTestHelper::ensureOutputFits(buffer, kBatchRows);
  EXPECT_EQ(maybeReserveCalls, 0);

  if (reservationPadding != nullptr) {
    pool()->free(reservationPadding, excessReservation);
  }
}

DEBUG_ONLY_TEST_F(
    RadixSortBufferTest,
    outputAdmissionBelowLegacyMultiplierDoesNotReclaim) {
  constexpr vector_size_t kBatchRows = 80;
  auto input = makeRows(
      {"key"},
      {generateVector<int64_t>(BIGINT(), kBatchRows, [](vector_size_t row) {
        return kBatchRows - row;
      })});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  buffer.addInput(input);
  buffer.noMoreInput();

  const auto estimate =
      RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, kBatchRows);
  const auto need = estimate.total;
  const auto outputBytes = estimate.outputGrowth;
  const auto legacyEstimate = outputBytes * 2 + (outputBytes * 2) / 5;
  ASSERT_GT(outputBytes, 0);
  ASSERT_LT(need, legacyEstimate);
  ASSERT_TRUE(pool()->maybeReserve(legacyEstimate));
  const auto alignment = static_cast<uint64_t>(pool()->alignment());
  const auto targetReservation =
      need + ((legacyEstimate - need) / 2 / alignment) * alignment;
  ASSERT_GE(targetReservation, need);
  ASSERT_LT(targetReservation, legacyEstimate);
  ASSERT_GE(pool()->availableReservation(), targetReservation);
  const auto excessReservation =
      static_cast<uint64_t>(pool()->availableReservation()) - targetReservation;
  void* reservationPadding = nullptr;
  if (excessReservation > 0) {
    ASSERT_EQ(excessReservation % alignment, 0);
    reservationPadding = pool()->allocate(excessReservation);
  }
  ASSERT_EQ(pool()->availableReservation(), targetReservation);

  uint32_t maybeReserveCalls = 0;
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          [&](memory::MemoryPoolImpl* candidate) {
            if (candidate == pool()) {
              ++maybeReserveCalls;
            }
          }));
  RadixSortBufferTestHelper::ensureOutputFits(buffer, kBatchRows);
  EXPECT_EQ(maybeReserveCalls, 0);
  EXPECT_FALSE(buffer.spilledStats());
  EXPECT_TRUE(buffer.canReclaim());

  if (reservationPadding != nullptr) {
    pool()->free(reservationPadding, excessReservation);
  }
}

DEBUG_ONLY_TEST_F(
    RadixSortBufferTest,
    outputAdmissionReestimatesAfterReclaimChangesToMerge) {
  constexpr vector_size_t kAdmissionRows = 1'100'000;
  auto input = makeRows({"key"}, {makeVector<int64_t>(BIGINT(), {3, 1, 2})});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  buffer.addInput(input);
  buffer.noMoreInput();
  pool()->release();

  const auto directEstimate =
      RadixSortBufferTestHelper::outputAdmissionEstimate(
          buffer, kAdmissionRows);
  const auto directNeed = directEstimate.total;
  ASSERT_GT(directNeed, 0);
  ASSERT_LT(directNeed, 24UL << 20);

  uint32_t maybeReserveCalls = 0;
  std::atomic<bool> spilling{false};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          [&](memory::MemoryPoolImpl* candidate) {
            if (candidate != pool() || spilling) {
              return;
            }
            ++maybeReserveCalls;
            if (maybeReserveCalls == 1) {
              spilling = true;
              buffer.spill();
              spilling = false;
            }
          }));

  RadixSortBufferTestHelper::ensureOutputFits(buffer, kAdmissionRows);
  EXPECT_EQ(maybeReserveCalls, 2);
  EXPECT_EQ(buffer.testingMergeStreamCount(), 1);
  const auto mergeEstimate = RadixSortBufferTestHelper::outputAdmissionEstimate(
      buffer, kAdmissionRows);
  const auto mergeNeed = mergeEstimate.total;
  EXPECT_GT(mergeNeed, directNeed);
  EXPECT_GT(mergeNeed, 24UL << 20);
}

TEST_F(RadixSortBufferTest, outputAdmissionCoversRepresentativeShapes) {
  constexpr vector_size_t kRows = 8;
  const auto estimate = [&](const RowVectorPtr& input,
                            std::vector<column_index_t> keyChannels) {
    auto directory = exec::test::TempDirectoryPath::create();
    auto config = spillConfig(directory->path);
    std::vector<CompareFlags> flags(
        keyChannels.size(), SortComparatorOracle::makeSortFlags(true, true));
    RadixSortBuffer buffer(
        std::static_pointer_cast<const RowType>(input->type()),
        keyChannels,
        flags,
        pool(),
        &config,
        /*spillMemoryThreshold=*/0,
        /*operatorCtx=*/nullptr,
        &nonReclaimableSection_);
    buffer.addInput(input);
    buffer.noMoreInput();
    return RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, kRows);
  };

  const auto keyOnly = estimate(
      makeRows(
          {"key"},
          {generateVector<int64_t>(
              BIGINT(), kRows, [](vector_size_t row) { return row; })}),
      {0});
  EXPECT_GT(keyOnly.outputGrowth, 0);
  EXPECT_EQ(keyOnly.scratchGrowth, 0);

  const auto fixedMultiKey = estimate(
      makeRows(
          {"first", "second"},
          {generateVector<int64_t>(
               BIGINT(), kRows, [](vector_size_t row) { return row; }),
           generateVector<int64_t>(
               BIGINT(),
               kRows,
               [](vector_size_t row) { return kRows - row; })}),
      {0, 1});
  EXPECT_GT(fixedMultiKey.scratchGrowth, keyOnly.scratchGrowth);
  const auto complex = estimate(makeEventMapPayloadRows(kRows), {9});
  EXPECT_GT(complex.outputGrowth, fixedMultiKey.outputGrowth);
  EXPECT_EQ(
      complex.total,
      complex.outputGrowth + complex.outputGrowth / 5 + complex.scratchGrowth);
}

DEBUG_ONLY_TEST_F(RadixSortBufferTest, ensureMergeFitsCanTriggerReclaimSpill) {
  auto input = makeKeyPayloadIdRows({9, 1, 8, 2, 7, 3});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  buffer.addInput(slice(*input, 0, 3));
  buffer.spill();
  buffer.addInput(slice(*input, 3, 3));

  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, 1);
  EXPECT_EQ(buffer.testingSpilledRunCount(), 1);
  pool()->release();

  std::atomic<bool> injected{false};
  std::atomic<bool> spilling{false};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            if (pool != this->pool() || injected.exchange(true) || spilling) {
              return;
            }
            EXPECT_FALSE(nonReclaimableSection_);
            spilling = true;
            buffer.spill();
            spilling = false;
          })));

  buffer.noMoreInput();

  EXPECT_TRUE(injected);
  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, 2);
  EXPECT_EQ(buffer.spilledStats()->spilledRows, input->size());
  EXPECT_EQ(buffer.testingSpilledRunCount(), 0);
  EXPECT_EQ(buffer.testingMergeStreamCount(), 2);
  collectAndVerify(buffer, input, 2, 2);
}

DEBUG_ONLY_TEST_F(RadixSortBufferTest, ensureOutputFitsCanTriggerReclaimSpill) {
  auto input = makeLargeStringKeyIdRows({9, 1, 8, 2, 7, 3});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  addInputRuns(buffer, input, {{3, true}, {3, false}});
  buffer.noMoreInput();
  EXPECT_EQ(buffer.testingMergeStreamCount(), 2);
  pool()->release();

  std::atomic<bool> injected{false};
  std::atomic<bool> spilling{false};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            if (pool != this->pool() || injected.exchange(true) || spilling) {
              return;
            }
            EXPECT_FALSE(nonReclaimableSection_);
            spilling = true;
            buffer.spill();
            spilling = false;
          })));

  auto output = collect(buffer, 2);

  EXPECT_TRUE(injected);
  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, 2);
  // The reclaimed memory suffix replaces its slot with one disk stream.
  // Existing disk streams are preserved, so fan-in stays unchanged.
  EXPECT_EQ(buffer.testingMergeStreamCount(), 2);
  SortComparatorOracle::expectRowsMatchById(*input, *output, 1);
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
  EXPECT_EQ(buffer.numOutputRows(), input->size());
}

TEST_F(RadixSortBufferTest, splitOutputStageSpillFilesMergeAsSingleRun) {
  constexpr vector_size_t kRows = 4;
  constexpr uint64_t kMaxFileSize = 512 * 1024;
  auto input = makeLargeStringKeyIdRows(
      generate<int64_t>(kRows, [](vector_size_t row) { return kRows - row; }));
  SpillContext spill(
      *this,
      input,
      "none",
      /*spillMemoryThreshold=*/0,
      /*keyChannel=*/0,
      kMaxFileSize);
  auto& buffer = spill.buffer;
  buffer.addInput(input);
  buffer.noMoreInput();
  EXPECT_FALSE(buffer.spilledStats());
  EXPECT_EQ(buffer.testingMergeStreamCount(), 0);

  spillMemoryRunAndVerifyReplacement(
      buffer,
      input,
      {.prefixRows = 1,
       .expectedSpillRuns = 1,
       .expectedMergeStreams = 1,
       .expectedSpilledRows = kRows - 1});
}

TEST_F(RadixSortBufferTest, keyOnlyConcatOutputReplacement) {
  constexpr vector_size_t kRows = 6;
  constexpr uint64_t kMaxFileSize = 512 * 1024;
  auto keyAndId = makeLargeStringKeyIdRows({1, 3, 5, 2, 4, 6});
  auto input = makeRows({"key"}, {keyAndId->childAt(0)});
  SpillContext spill(
      *this,
      input,
      "none",
      /*spillMemoryThreshold=*/0,
      /*keyChannel=*/0,
      kMaxFileSize);
  auto& buffer = spill.buffer;
  buffer.addInput(slice(*input, 0, 3));
  buffer.spill();
  buffer.addInput(slice(*input, 3, 3));
  buffer.noMoreInput();

  auto prefix = buffer.getOutput(2);
  ASSERT_NE(prefix, nullptr);
  ASSERT_EQ(prefix->size(), 2);
  const auto filesBefore = buffer.spilledStats()->spilledFiles;
  spillMemoryRunAndCheckStats(buffer, 2);
  EXPECT_GT(buffer.spilledStats()->spilledFiles - filesBefore, 1);
  EXPECT_EQ(buffer.testingMergeStreamCount(), 2);

  auto output = collect(buffer, 2, prefix);
  ASSERT_EQ(output->size(), kRows);
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
  const auto* inputKeys =
      input->childAt(0)->asUnchecked<SimpleVector<StringView>>();
  const auto* outputKeys =
      output->childAt(0)->asUnchecked<SimpleVector<StringView>>();
  constexpr std::array<vector_size_t, kRows> expectedRows{0, 3, 1, 4, 2, 5};
  for (vector_size_t row = 0; row < kRows; ++row) {
    EXPECT_EQ(
        outputKeys->valueAt(row).getString(),
        inputKeys->valueAt(expectedRows[row]).getString());
  }
}

TEST_F(RadixSortBufferTest, outputStageSpillReplacesMultiStreamMerge) {
  constexpr vector_size_t kRowsPerRun = 4;
  constexpr vector_size_t kInputSpillRuns = 2;
  constexpr vector_size_t kRows = (kInputSpillRuns + 1) * kRowsPerRun;
  constexpr uint64_t kMaxFileSize = 512 * 1024;
  auto input = makeLargeStringKeyIdRows(
      generate<int64_t>(kRows, [](vector_size_t row) { return kRows - row; }));
  SpillContext spill(
      *this,
      input,
      "none",
      /*spillMemoryThreshold=*/0,
      /*keyChannel=*/0,
      kMaxFileSize);
  auto& buffer = spill.buffer;
  addInputRuns(
      buffer,
      input,
      {{kRowsPerRun, true}, {kRowsPerRun, true}, {kRowsPerRun, false}});

  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, kInputSpillRuns);
  EXPECT_EQ(buffer.testingSpilledRunCount(), kInputSpillRuns);
  ASSERT_GT(buffer.spilledStats()->spilledFiles, kInputSpillRuns);

  buffer.noMoreInput();
  EXPECT_EQ(buffer.testingSpilledRunCount(), 0);
  EXPECT_EQ(buffer.testingMergeStreamCount(), kInputSpillRuns + 1);

  spillMemoryRunAndVerifyReplacement(
      buffer,
      input,
      {.prefixRows = 3,
       .expectedSpillRuns = kInputSpillRuns + 1,
       .expectedMergeStreams = kInputSpillRuns + 1,
       .expectedSpilledRows = 1});
}

TEST_F(RadixSortBufferTest, outputStageSpillUsesMemoryCursor) {
  struct TestCase {
    const char* name;
    std::vector<std::optional<int64_t>> diskKeys;
    std::vector<std::optional<int64_t>> memoryKeys;
    vector_size_t prefixRows;
    uint64_t expectedMemoryPosition;
  };

  for (const auto& test : std::vector<TestCase>{
           {"memory exhausted", {10, 11, 12}, {1, 2, 3}, 3, 3},
           {"memory keys last", {1, 2, 3}, {10, 11, 12}, 2, 0},
           {"interleaved keys", {1, 3, 5}, {2, 4, 6}, 3, 1},
       }) {
    SCOPED_TRACE(test.name);
    std::vector<std::optional<int64_t>> keys = test.diskKeys;
    keys.insert(keys.end(), test.memoryKeys.begin(), test.memoryKeys.end());
    auto input = makeKeyPayloadIdRows(keys);
    SpillContext spill(*this, input);
    auto& buffer = spill.buffer;
    buffer.addInput(slice(*input, 0, test.diskKeys.size()));
    buffer.spill();
    buffer.addInput(
        slice(*input, test.diskKeys.size(), test.memoryKeys.size()));
    buffer.noMoreInput();

    ASSERT_EQ(buffer.testingMergeStreamCount(), 2);
    auto prefix = buffer.getOutput(test.prefixRows);
    ASSERT_NE(prefix, nullptr);
    ASSERT_EQ(prefix->size(), test.prefixRows);
    const auto statsBefore = *buffer.spilledStats();
    const auto readStatsBefore = buffer.spillReadStats();
    const auto remainingMemoryRows =
        test.memoryKeys.size() - test.expectedMemoryPosition;

    if (remainingMemoryRows == 0) {
      EXPECT_FALSE(buffer.canReclaim());
      buffer.spill();
      EXPECT_EQ(*buffer.spilledStats(), statsBefore);
      EXPECT_EQ(buffer.testingMergeStreamCount(), 1);
      expectSpillReadStatsEqual(readStatsBefore, buffer.spillReadStats());
    } else {
      spillMemoryRunAndCheckStats(buffer, remainingMemoryRows);
      ASSERT_TRUE(buffer.spilledStats());
      EXPECT_EQ(buffer.spilledStats()->spillRuns, statsBefore.spillRuns + 1);
      EXPECT_EQ(buffer.testingMergeStreamCount(), 2);
      // Constructing the replacement stream eagerly loads its first block,
      // so aggregate read counters can legitimately increase here.
      expectSpillReadStatsNotDecreased(
          readStatsBefore, buffer.spillReadStats());
    }

    collectAndVerify(buffer, input, 2, 2, 0, 0, prefix);
    ASSERT_TRUE(buffer.spilledStats());
    EXPECT_LE(buffer.spilledStats()->spilledRows, input->size());
  }
}

TEST_F(RadixSortBufferTest, unknownColumnsSurviveMixedOutputReclaim) {
  constexpr vector_size_t kRows = 6;
  auto unknownKey = BaseVector::createNullConstant(UNKNOWN(), kRows, pool());
  auto unknownPayload =
      BaseVector::createNullConstant(UNKNOWN(), kRows, pool());
  auto input = makeRows(
      {"unknown_key", "key", "unknown_payload", "id"},
      {unknownKey,
       makeVector<int64_t>(BIGINT(), {1, 3, 5, 2, 4, 6}),
       unknownPayload,
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5})});
  auto directory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(directory->path);
  const std::vector<column_index_t> keyChannels{0, 1};
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, true)};
  RadixSortBuffer buffer(
      std::static_pointer_cast<const RowType>(input->type()),
      keyChannels,
      keyFlags,
      pool(),
      &config);
  buffer.addInput(slice(*input, 0, 3));
  buffer.spill();
  buffer.addInput(slice(*input, 3, 3));
  buffer.noMoreInput();

  auto prefix = buffer.getOutput(2);
  ASSERT_NE(prefix, nullptr);
  ASSERT_EQ(prefix->size(), 2);
  spillMemoryRunAndCheckStats(buffer, 2);

  auto output = collect(buffer, 2, prefix);
  SortComparatorOracle::expectRowsMatchById(*input, *output, 3);
  SortComparatorOracle::expectSorted(*output, keyChannels, keyFlags);
  for (const auto channel : {0, 2}) {
    for (vector_size_t row = 0; row < output->size(); ++row) {
      EXPECT_TRUE(output->childAt(channel)->isNullAt(row));
    }
  }
}

TEST_F(RadixSortBufferTest, diskOnlyMergeIsNotReclaimable) {
  auto input = makeKeyPayloadIdRows({1, 3, 5, 2, 4, 6});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  addInputRuns(buffer, input, {{3, true}, {3, true}});
  buffer.noMoreInput();

  ASSERT_EQ(buffer.testingMergeStreamCount(), 2);
  ASSERT_TRUE(buffer.spilledStats());
  const auto statsBefore = *buffer.spilledStats();
  const auto readStatsBefore = buffer.spillReadStats();
  EXPECT_FALSE(buffer.canReclaim());
  buffer.spill();
  EXPECT_EQ(*buffer.spilledStats(), statsBefore);
  expectSpillReadStatsEqual(readStatsBefore, buffer.spillReadStats());
  EXPECT_EQ(buffer.testingMergeStreamCount(), 2);
  collectAndVerify(buffer, input, 2, 2);
}

TEST_F(RadixSortBufferTest, outputStageSpillPreservesSuffixKeys) {
  constexpr vector_size_t kRowsPerRun = 4;
  constexpr vector_size_t kRows = kRowsPerRun * 3;
  auto input = makeLargeStringKeyIdRows(
      generate<int64_t>(kRows, [](vector_size_t row) { return kRows - row; }));
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  addInputRuns(
      buffer,
      input,
      {{kRowsPerRun, true}, {kRowsPerRun, true}, {kRowsPerRun, false}});
  buffer.noMoreInput();

  auto prefix = buffer.getOutput(1);
  ASSERT_NE(prefix, nullptr);
  ASSERT_EQ(prefix->size(), 1);
  ASSERT_TRUE(buffer.spilledStats());
  const auto spillWritesBefore = buffer.spilledStats()->spillWrites;
  spillMemoryRunAndCheckStats(buffer, kRowsPerRun - 1);
  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_GT(buffer.spilledStats()->spillWrites - spillWritesBefore, 1);

  collectAndVerify(
      buffer,
      input,
      /*batchSize=*/2,
      /*idChannel=*/1,
      /*keyChannel=*/0,
      /*idBase=*/0,
      prefix);
}

TEST_F(RadixSortBufferTest, outputStageSpillPreservesSuffixPayload) {
  constexpr vector_size_t kRowsPerRun = 4;
  constexpr vector_size_t kRows = kRowsPerRun * 3;
  constexpr size_t kPayloadBytes = 600 * 1024;
  std::vector<std::optional<int64_t>> keys;
  std::vector<std::optional<std::string>> payloads;
  keys.reserve(kRows);
  payloads.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    keys.push_back(kRows - row);
    payloads.push_back(
        "payload_" + std::to_string(row) + "_" +
        std::string(
            kPayloadBytes + row * 7, static_cast<char>('a' + row % 26)));
  }
  auto input = makeKeyPayloadIdRows(keys, payloads);
  SpillContext spill(*this, input, "lz4");
  auto& buffer = spill.buffer;
  addInputRuns(
      buffer,
      input,
      {{kRowsPerRun, true}, {kRowsPerRun, true}, {kRowsPerRun, false}});
  buffer.noMoreInput();

  auto prefix = buffer.getOutput(1);
  ASSERT_NE(prefix, nullptr);
  ASSERT_EQ(prefix->size(), 1);
  ASSERT_TRUE(buffer.spilledStats());
  const auto spillWritesBefore = buffer.spilledStats()->spillWrites;
  spillMemoryRunAndCheckStats(buffer, kRowsPerRun - 1);
  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_GT(buffer.spilledStats()->spillWrites - spillWritesBefore, 1);

  collectAndVerify(
      buffer,
      input,
      /*batchSize=*/2,
      /*idChannel=*/2,
      /*keyChannel=*/0,
      /*idBase=*/0,
      prefix);
}

TEST_F(RadixSortBufferTest, returnedOutputSurvivesMemoryStreamReplacement) {
  constexpr vector_size_t kRows = 9;
  std::vector<std::optional<int64_t>> keys{1, 4, 7, 2, 5, 8, 3, 6, 9};
  std::vector<std::optional<std::string>> payloads;
  payloads.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    payloads.push_back(
        fmt::format("payload_{:02d}_", row) +
        std::string(96 + row, static_cast<char>('a' + row)));
  }
  auto input = makeKeyPayloadIdRows(keys, payloads);
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  addInputRuns(buffer, input, {{3, true}, {3, true}, {3, false}});
  buffer.noMoreInput();

  auto heldOutput = buffer.getOutput(4);
  ASSERT_NE(heldOutput, nullptr);
  auto expectedHeldOutput = slice(*heldOutput, 0, heldOutput->size());
  spillMemoryRunAndCheckStats(buffer, 2);
  pool()->release();
  auto churn = makeStringVector(
      VARCHAR(),
      generate<std::optional<std::string>>(kRows, [](vector_size_t row) {
        return std::string(256 + row, 'x');
      }));
  (void)churn;
  expectRowsEqual(*expectedHeldOutput, *heldOutput);

  collectAndVerify(buffer, input, 2, 2, 0, 0, heldOutput);
}

TEST_F(
    RadixSortBufferTest,
    returnedComplexOutputSurvivesMemoryStreamReplacement) {
  auto input = makeEventMapPayloadRows(12);
  SpillContext spill(*this, input, "lz4", 0, 9);
  auto& buffer = spill.buffer;
  addInputRuns(buffer, input, {{4, true}, {4, true}, {4, false}});
  buffer.noMoreInput();

  auto heldOutput = buffer.getOutput(3);
  ASSERT_NE(heldOutput, nullptr);
  auto expectedHeldOutput = slice(*heldOutput, 0, heldOutput->size());
  spillMemoryRunAndCheckStats(buffer, 4);
  pool()->release();
  auto churn = makeEventMapPayloadRows(12, "churn_");
  (void)churn;
  expectRowsEqual(*expectedHeldOutput, *heldOutput);

  collectAndVerify(buffer, input, 2, 0, 9, 10'000, heldOutput);
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
      {SortComparatorOracle::makeSortFlags(true, true),
       SortComparatorOracle::makeSortFlags(true, false),
       SortComparatorOracle::makeSortFlags(false, false)},
      pool(),
      &config);

  buffer.addInput(slice(*input, 0, 16));
  buffer.spill();
  buffer.addInput(slice(*input, 16, 16));
  buffer.spill();
  buffer.addInput(slice(*input, 32, 16));
  buffer.noMoreInput();

  auto output = collect(buffer, 5);
  SortComparatorOracle::expectRowsMatchById(*input, *output, 3);
  SortComparatorOracle::expectSorted(
      *output,
      {0, 1, 2},
      {SortComparatorOracle::makeSortFlags(true, true),
       SortComparatorOracle::makeSortFlags(true, false),
       SortComparatorOracle::makeSortFlags(false, false)});
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
      {SortComparatorOracle::makeSortFlags(true, true),
       SortComparatorOracle::makeSortFlags(true, true)},
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
  SortComparatorOracle::expectRowsMatchById(*input, *output, 5);
  SortComparatorOracle::expectSorted(
      *output,
      {0, 1},
      {SortComparatorOracle::makeSortFlags(true, true),
       SortComparatorOracle::makeSortFlags(true, true)});
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

TEST_F(RadixSortBufferTest, testSpillInjectionTriggersBeforeNextInput) {
  auto input = makeKeyPayloadIdRows({6, 1, 5, 2, 4, 3});
  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  buffer.addInput(slice(*input, 0, 3));
  {
    TestScopedSpillInjection spillInjection(100, 1);
    buffer.addInput(slice(*input, 3, 3));
  }

  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, 1);
  EXPECT_EQ(buffer.spilledStats()->spilledRows, 3);
  EXPECT_EQ(buffer.testingSpilledRunCount(), 1);
  buffer.noMoreInput();
  EXPECT_EQ(buffer.testingMergeStreamCount(), 2);
  collectAndVerify(buffer, input, 1, 2);
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
      "radix-sort-buffer-reservation-failure-root", 4 << 20);
  auto sortPool =
      rootPool->addLeafChild("radix-sort-buffer-reservation-failure");
  RadixSortBuffer buffer(
      inputType,
      {0},
      {SortComparatorOracle::makeSortFlags(true, true)},
      sortPool.get(),
      &config);

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
  SortComparatorOracle::expectRowsMatchById(
      *expectedInput, *output, inputType->size() - 1);
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
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
  spillMemoryRunAndCheckStats(buffer, 3);
  const auto allStats = buffer.spilledStats();
  ASSERT_TRUE(allStats);
  const auto outputStats = *allStats - *inputStageStats;
  EXPECT_EQ(allStats->spillRuns, 2);
  EXPECT_EQ(outputStats.spillRuns, 1);
  EXPECT_EQ(outputStats.spilledRows, 3);
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

TEST_F(RadixSortBufferTest, spillBatchOutputSizesAcrossBlockBoundaries) {
  const std::vector<int64_t> keyRanks{8, 1, 7, 2, 6, 3, 5, 4};
  auto inputWithPayload = makeLargeStringKeyIdRows(keyRanks);
  const auto verify = [&](const RowVectorPtr& input,
                          std::string_view compression,
                          std::optional<column_index_t> idChannel) {
    SCOPED_TRACE(compression);
    SpillContext spill(
        *this,
        input,
        std::string(compression),
        /*spillMemoryThreshold=*/0,
        /*keyChannel=*/0);
    auto& buffer = spill.buffer;
    addInputRuns(buffer, input, {{3, true}, {3, true}, {2, false}});
    ASSERT_TRUE(buffer.spilledStats());
    EXPECT_EQ(buffer.spilledStats()->spillRuns, 2);
    EXPECT_GT(
        buffer.spilledStats()->spillWrites, buffer.spilledStats()->spillRuns);
    buffer.noMoreInput();
    EXPECT_EQ(buffer.testingMergeStreamCount(), 3);

    std::vector<vector_size_t> batchSizes;
    std::vector<RowVectorPtr> batches;
    while (auto batch = buffer.getOutput(3)) {
      batchSizes.push_back(batch->size());
      batches.push_back(std::move(batch));
    }
    EXPECT_EQ(batchSizes, std::vector<vector_size_t>({3, 3, 2}));
    auto output = concatenateBatches(batches, input->size());
    if (idChannel) {
      SortComparatorOracle::expectRowsMatchById(*input, *output, *idChannel);
    } else {
      std::vector<vector_size_t> expectedRows(keyRanks.size());
      std::iota(expectedRows.begin(), expectedRows.end(), 0);
      std::sort(
          expectedRows.begin(),
          expectedRows.end(),
          [&](vector_size_t left, vector_size_t right) {
            return keyRanks[left] < keyRanks[right];
          });
      const auto* inputKeys =
          input->childAt(0)->asUnchecked<SimpleVector<StringView>>();
      const auto* outputKeys =
          output->childAt(0)->asUnchecked<SimpleVector<StringView>>();
      for (vector_size_t row = 0; row < output->size(); ++row) {
        EXPECT_EQ(
            outputKeys->valueAt(row).getString(),
            inputKeys->valueAt(expectedRows[row]).getString());
      }
    }
    SortComparatorOracle::expectSorted(
        *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
    EXPECT_EQ(buffer.numOutputRows(), input->size());
    EXPECT_TRUE(buffer.spillReadStats());
  };

  verify(
      makeRows({"key"}, {inputWithPayload->childAt(0)}), "none", std::nullopt);
  verify(inputWithPayload, "lz4", 1);
}

TEST_F(RadixSortBufferTest, fixedWidthSpillOutputCrossesBlockBoundaries) {
  constexpr vector_size_t kRowsPerRun = 1024;
  constexpr vector_size_t kRows = 2 * kRowsPerRun;
  constexpr uint32_t kPayloadColumns = 128;

  std::vector<std::string> names{"key"};
  std::vector<VectorPtr> children;
  names.reserve(kPayloadColumns + 2);
  children.reserve(kPayloadColumns + 2);

  std::vector<std::optional<int64_t>> keys;
  keys.reserve(kRows);
  for (vector_size_t run = 0; run < 2; ++run) {
    for (vector_size_t row = 0; row < kRowsPerRun; ++row) {
      keys.push_back(2 * row + 1 - run);
    }
  }
  children.push_back(makeVector<int64_t>(BIGINT(), keys));
  for (uint32_t column = 0; column < kPayloadColumns; ++column) {
    names.push_back("payload_" + std::to_string(column));
    children.push_back(
        generateVector<int64_t>(BIGINT(), kRows, [column](vector_size_t row) {
          return static_cast<int64_t>(row) * 10'000 + column;
        }));
  }
  names.push_back("id");
  children.push_back(generateVector<int64_t>(
      BIGINT(), kRows, [](vector_size_t row) { return row; }));
  auto input = makeRows(std::move(names), children);

  SpillContext spill(*this, input);
  auto& buffer = spill.buffer;
  addInputRuns(buffer, input, {{kRowsPerRun, true}, {kRowsPerRun, true}});
  ASSERT_TRUE(buffer.spilledStats());
  EXPECT_EQ(buffer.spilledStats()->spillRuns, 2);
  EXPECT_GE(buffer.spilledStats()->spillWrites, 4);

  buffer.noMoreInput();
  EXPECT_EQ(buffer.testingMergeStreamCount(), 2);
  auto output = buffer.getOutput(kRows);

  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), kRows);
  EXPECT_EQ(buffer.getOutput(kRows), nullptr);
  SortComparatorOracle::expectRowsMatchById(
      *input, *output, kPayloadColumns + 1);
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
  EXPECT_EQ(buffer.numOutputRows(), kRows);
}

TEST_F(RadixSortBufferTest, spillDisabledAndEmptySpill) {
  inputType_ = ROW({"key", "id"}, {BIGINT(), BIGINT()});
  RadixSortBuffer disabled(
      inputType_,
      {0},
      {SortComparatorOracle::makeSortFlags(true, true)},
      pool());
  EXPECT_FALSE(disabled.canSpill());
  EXPECT_THROW(disabled.spill(), BoltException);

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  RadixSortBuffer empty(
      inputType_,
      {0},
      {SortComparatorOracle::makeSortFlags(true, true)},
      pool(),
      &config);
  EXPECT_TRUE(empty.canSpill());
  empty.spill();
  empty.noMoreInput();
  EXPECT_FALSE(empty.spilledStats());
  EXPECT_EQ(empty.getOutput(1), nullptr);

  RadixSortBuffer postSpillInput(
      inputType_,
      {0},
      {SortComparatorOracle::makeSortFlags(true, true)},
      pool(),
      &config);
  postSpillInput.spill();
  postSpillInput.addInput(makeRows(
      {"key", "id"},
      {makeVector<int64_t>(BIGINT(), {2, 1}),
       makeVector<int64_t>(BIGINT(), {0, 1})}));
  postSpillInput.noMoreInput();
  EXPECT_FALSE(postSpillInput.spilledStats());
  auto output = collect(postSpillInput, 1);
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
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
      const std::vector<CompareFlags> keyFlags{
          SortComparatorOracle::makeSortFlags(ascending, nullsFirst)};
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
      input, {{3, true}, {3, true}, {3, false}}, 2, 2, 0, 0, "none", 4, 2);
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
  spillMemoryRunAndCheckStats(buffer, 2);

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
        inputType_,
        {0},
        {SortComparatorOracle::makeSortFlags(true, true)},
        pool(),
        &config);
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

TEST_F(RadixSortBufferTest, spillWriteFailureCleansFiles) {
  auto input = makeKeyPayloadIdRows({4, 1, 3, 2});
  for (const auto outputStage : {false, true}) {
    SCOPED_TRACE(outputStage ? "output stage" : "input stage");
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    {
      uint64_t callbackBytes = 0;
      auto config =
          spillConfig(spillDirectory->path, "none", [&](uint64_t bytes) {
            callbackBytes += bytes;
            BOLT_FAIL("injected spill limit failure");
          });
      RadixSortBuffer buffer(
          std::static_pointer_cast<const RowType>(input->type()),
          {0},
          {SortComparatorOracle::makeSortFlags(true, true)},
          pool(),
          &config);
      buffer.addInput(input);
      if (outputStage) {
        buffer.noMoreInput();
      }
      EXPECT_THROW(buffer.spill(), BoltException);
      EXPECT_GT(callbackBytes, 0);
      EXPECT_TRUE(std::filesystem::is_empty(spillDirectory->path));
    }
    EXPECT_TRUE(std::filesystem::is_empty(spillDirectory->path));
  }
}

TEST_F(
    RadixSortBufferTest,
    outputStageWriterFailurePropagatesAndCleansNewFiles) {
  constexpr vector_size_t kRowsPerRun = 15;
  constexpr size_t kPayloadBytes = 128 * 1024;
  std::vector<std::optional<int64_t>> keys;
  std::vector<std::optional<std::string>> payloads;
  keys.reserve(3 * kRowsPerRun);
  payloads.reserve(3 * kRowsPerRun);
  for (vector_size_t run = 0; run < 3; ++run) {
    for (vector_size_t row = 0; row < kRowsPerRun; ++row) {
      keys.push_back(
          run == 2 ? 100 + row : 2 * row + static_cast<int64_t>(run));
      payloads.push_back(std::string(
          kPayloadBytes, static_cast<char>('a' + (run + row) % 26)));
    }
  }
  auto input = makeKeyPayloadIdRows(keys, payloads);
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  bool outputStage = false;
  uint32_t outputWrites = 0;
  auto config = spillConfig(
      spillDirectory->path,
      "none",
      [&](uint64_t) {
        if (!outputStage) {
          return;
        }
        ++outputWrites;
        if (outputWrites == 2) {
          BOLT_FAIL("injected second output spill write failure");
        }
      },
      /*maxFileSize=*/0,
      /*writeBufferSize=*/256 << 10);

  std::vector<std::filesystem::path> sourceFiles;
  {
    RadixSortBuffer buffer(
        std::static_pointer_cast<const RowType>(input->type()),
        {0},
        {SortComparatorOracle::makeSortFlags(true, true)},
        pool(),
        &config);
    addInputRuns(
        buffer,
        input,
        {{kRowsPerRun, true}, {kRowsPerRun, true}, {kRowsPerRun, false}});
    ASSERT_TRUE(buffer.spilledStats());
    EXPECT_EQ(buffer.spilledStats()->spillRuns, 2);
    EXPECT_GT(buffer.spilledStats()->spillWrites, 0);
    EXPECT_EQ(buffer.testingSpilledRunCount(), 2);
    for (const auto& file :
         std::filesystem::directory_iterator(spillDirectory->path)) {
      sourceFiles.push_back(file.path());
    }
    ASSERT_EQ(sourceFiles.size(), 2);

    buffer.noMoreInput();
    ASSERT_EQ(buffer.testingMergeStreamCount(), 3);
    auto prefix = buffer.getOutput(1);
    ASSERT_NE(prefix, nullptr);
    ASSERT_EQ(prefix->size(), 1);
    EXPECT_EQ(idAt(*prefix, 0, 2), 0);
    outputStage = true;
    EXPECT_THROW(buffer.spill(), BoltException);
    EXPECT_EQ(outputWrites, 2);
    const auto filesAfterFailure = [&]() {
      std::vector<std::filesystem::path> paths;
      for (const auto& file :
           std::filesystem::directory_iterator(spillDirectory->path)) {
        paths.push_back(file.path());
      }
      return paths;
    }();
    EXPECT_EQ(filesAfterFailure.size(), sourceFiles.size());
    for (const auto& file : sourceFiles) {
      EXPECT_TRUE(std::filesystem::exists(file));
    }
  }

  EXPECT_TRUE(std::filesystem::is_empty(spillDirectory->path));
}

TEST_F(
    RadixSortBufferTest,
    outputStageReplacementReaderFailurePropagatesAndCleansNewFile) {
  auto input = makeKeyPayloadIdRows({1, 3, 5, 2, 4, 6});
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  bool corruptOutputFile = false;
  std::filesystem::path outputFile;
  auto config = spillConfig(
      spillDirectory->path,
      "none",
      [&](uint64_t) {
        if (!corruptOutputFile || !outputFile.empty()) {
          return;
        }
        for (const auto& file :
             std::filesystem::directory_iterator(spillDirectory->path)) {
          if (file.path().filename().string().find("-output") !=
              std::string::npos) {
            outputFile = file.path();
            std::filesystem::resize_file(outputFile, 1);
            return;
          }
        }
      },
      /*maxFileSize=*/0,
      /*writeBufferSize=*/64);

  std::vector<std::filesystem::path> diskFiles;
  {
    RadixSortBuffer buffer(
        std::static_pointer_cast<const RowType>(input->type()),
        {0},
        {SortComparatorOracle::makeSortFlags(true, true)},
        pool(),
        &config);
    addInputRuns(buffer, input, {{3, true}, {3, false}});
    ASSERT_TRUE(buffer.spilledStats());
    for (const auto& file :
         std::filesystem::directory_iterator(spillDirectory->path)) {
      diskFiles.push_back(file.path());
    }
    ASSERT_EQ(diskFiles.size(), 1);
    buffer.noMoreInput();
    ASSERT_EQ(buffer.testingMergeStreamCount(), 2);

    corruptOutputFile = true;
    EXPECT_THROW(buffer.spill(), BoltException);
    EXPECT_FALSE(outputFile.empty());
    EXPECT_FALSE(std::filesystem::exists(outputFile));
    EXPECT_TRUE(std::filesystem::exists(diskFiles.front()));
  }
  EXPECT_TRUE(std::filesystem::is_empty(spillDirectory->path));
}

TEST_F(RadixSortBufferTest, destructionCleansOwnedAndMergeStreamFiles) {
  auto input = makeKeyPayloadIdRows({6, 1, 5, 2, 4, 3});
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);

  {
    RadixSortBuffer buffer(
        std::static_pointer_cast<const RowType>(input->type()),
        {0},
        {SortComparatorOracle::makeSortFlags(true, true)},
        pool(),
        &config);
    buffer.addInput(input);
    buffer.spill();
    EXPECT_EQ(buffer.testingSpilledRunCount(), 1);
    EXPECT_FALSE(std::filesystem::is_empty(spillDirectory->path));
  }
  EXPECT_TRUE(std::filesystem::is_empty(spillDirectory->path));

  {
    RadixSortBuffer buffer(
        std::static_pointer_cast<const RowType>(input->type()),
        {0},
        {SortComparatorOracle::makeSortFlags(true, true)},
        pool(),
        &config);
    buffer.addInput(input);
    buffer.spill();
    buffer.noMoreInput();
    EXPECT_EQ(buffer.testingSpilledRunCount(), 0);
    EXPECT_EQ(buffer.testingMergeStreamCount(), 1);
    EXPECT_FALSE(std::filesystem::is_empty(spillDirectory->path));
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
      inputType_,
      {0},
      {SortComparatorOracle::makeSortFlags(true, false)},
      pool(),
      &config);
  buffer.addInput(input);
  buffer.noMoreInput();

  spillMemoryRunAndCheckStats(buffer, input->size());
  auto output = collect(buffer, 2);
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, false)});
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
      SortComparatorOracle::makeSortFlags(true, false),
      SortComparatorOracle::makeSortFlags(false, true)};
  RadixSortBuffer buffer(
      inputType_, keyChannels, keyFlags, pool(), &spill.config);
  buffer.addInput(slice(*input, 0, 3));
  buffer.spill();
  buffer.addInput(slice(*input, 3, 4));
  buffer.noMoreInput();
  auto output = collect(buffer, 2);
  SortComparatorOracle::expectRowsMatchById(*input, *output, 3);
  SortComparatorOracle::expectSorted(*output, keyChannels, keyFlags);
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
      7,
      kRows - 36);
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

  pool()->release();
  auto churn = makeEventMapPayloadRows(kRows, "churn_");
  (void)churn;
  auto output = concatenateBatches(batches, kRows);
  SortComparatorOracle::expectRowsMatchById(
      *makeEventMapPayloadRows(kRows), *output, 0, {.idBase = 10'000});
  SortComparatorOracle::expectSorted(
      *output, {9}, {SortComparatorOracle::makeSortFlags(true, true)});
}

TEST_F(
    RadixSortBufferTest,
    outputOwnsVariableDataAfterBufferDestructionAndPoolChurn) {
  constexpr size_t kPayloadBytes = 600 * 1024;
  const std::vector<std::optional<std::string>> payloads{
      std::string(kPayloadBytes, 'c'),
      std::string(kPayloadBytes, 'a'),
      std::string(kPayloadBytes, 'b')};
  for (const auto spillOutput : {false, true}) {
    SCOPED_TRACE(spillOutput ? "spill" : "in-memory");
    RowVectorPtr output;
    {
      auto input = makeRows(
          {"key", "payload"},
          {makeVector<int64_t>(BIGINT(), {3, 1, 2}),
           makeStringVector(VARCHAR(), payloads)});
      SpillContext spill(*this, input);
      auto& buffer = spill.buffer;
      buffer.addInput(input);
      if (spillOutput) {
        buffer.spill();
        ASSERT_TRUE(buffer.spilledStats());
        EXPECT_GT(buffer.spilledStats()->spillWrites, 1);
      }
      buffer.noMoreInput();
      output = buffer.getOutput(3);
      ASSERT_NE(output, nullptr);
    }

    pool()->release();
    auto churn = makeStringVector(
        VARCHAR(),
        {std::string(kPayloadBytes, 'x'),
         std::string(kPayloadBytes, 'y'),
         std::string(kPayloadBytes, 'z')});
    (void)churn;

    ASSERT_EQ(output->size(), 3);
    const auto* keys = output->childAt(0)->asUnchecked<SimpleVector<int64_t>>();
    const auto* outputPayloads =
        output->childAt(1)->asUnchecked<SimpleVector<StringView>>();
    for (vector_size_t row = 0; row < output->size(); ++row) {
      EXPECT_EQ(keys->valueAt(row), row + 1);
      EXPECT_EQ(
          outputPayloads->valueAt(row).getString(),
          std::string(kPayloadBytes, static_cast<char>('a' + row)));
    }
  }
}

TEST_F(RadixSortBufferTest, pointerFreeSpillMergeLayoutAndTopologyMatrix) {
  enum class Topology {
    kSingleSpill,
    kMemorySpill,
    kTwoSpills,
    kLoserTree,
  };
  struct TopologyCase {
    const char* name;
    Topology topology;
    size_t expectedStreams;
  };

  constexpr vector_size_t kRows = 16;
  const std::array<int64_t, kRows> ranks{
      15, 3, 11, 7, 14, 2, 10, 6, 13, 1, 9, 5, 12, 0, 8, 4};
  const std::string commonPrefix(80, 'p');
  const std::array<TopologyCase, 4> topologies{{
      {"single spill", Topology::kSingleSpill, 1},
      {"memory-spill", Topology::kMemorySpill, 2},
      {"spill-spill two-way", Topology::kTwoSpills, 2},
      {"spill-spill loser tree", Topology::kLoserTree, 3},
  }};

  const auto makeInput = [&](bool variableKey, bool hasPayload) {
    VectorPtr keys;
    if (variableKey) {
      std::vector<std::optional<std::string>> values;
      values.reserve(kRows);
      for (const auto rank : ranks) {
        values.push_back(commonPrefix + fmt::format("{:04d}", rank));
      }
      keys = makeStringVector(VARCHAR(), values);
    } else {
      keys = makeVector<int64_t>(
          BIGINT(),
          std::vector<std::optional<int64_t>>(ranks.begin(), ranks.end()));
    }

    std::vector<std::string> names{"key"};
    std::vector<VectorPtr> children{std::move(keys)};
    if (hasPayload) {
      names.insert(names.end(), {"payload", "id"});
      children.push_back(makeStringVector(
          VARCHAR(),
          generate<std::optional<std::string>>(
              kRows, [](vector_size_t row) -> std::optional<std::string> {
                switch (row % 5) {
                  case 0:
                    return std::nullopt;
                  case 1:
                    return std::string{};
                  case 2:
                    return std::string(12, 'i');
                  case 3:
                    return std::string(13, 'b');
                  default:
                    return std::string(80 + row, 'l');
                }
              })));
      children.push_back(generateVector<int64_t>(
          BIGINT(), kRows, [](vector_size_t row) { return row; }));
    }
    return makeRows(std::move(names), children);
  };

  const auto addRuns = [&](RadixSortBuffer& buffer,
                           const RowVectorPtr& input,
                           Topology topology) {
    switch (topology) {
      case Topology::kSingleSpill:
        buffer.addInput(input);
        buffer.spill();
        return;
      case Topology::kMemorySpill:
        addInputRuns(buffer, input, {{8, true}, {8, false}});
        return;
      case Topology::kTwoSpills:
        addInputRuns(buffer, input, {{8, true}, {8, true}});
        return;
      case Topology::kLoserTree:
        addInputRuns(buffer, input, {{5, true}, {5, true}, {6, true}});
        return;
    }
  };

  for (const auto variableKey : {false, true}) {
    for (const auto hasPayload : {false, true}) {
      for (const auto& topology : topologies) {
        SCOPED_TRACE(fmt::format(
            "key={}, payload={}, topology={}",
            variableKey ? "variable" : "fixed",
            hasPayload,
            topology.name));
        auto input = makeInput(variableKey, hasPayload);
        auto directory = exec::test::TempDirectoryPath::create();
        auto config = spillConfig(directory->path);
        RadixSortBuffer buffer(
            std::static_pointer_cast<const RowType>(input->type()),
            {0},
            {SortComparatorOracle::makeSortFlags(true, true)},
            pool(),
            &config);
        addRuns(buffer, input, topology.topology);
        buffer.noMoreInput();
        EXPECT_EQ(buffer.testingMergeStreamCount(), topology.expectedStreams);

        auto output = collect(buffer, 3);
        ASSERT_NE(output, nullptr);
        ASSERT_EQ(output->size(), kRows);
        SortComparatorOracle::expectSorted(
            *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
        if (hasPayload) {
          SortComparatorOracle::expectRowsMatchById(*input, *output, 2);
        } else if (variableKey) {
          const auto* values =
              output->childAt(0)->asUnchecked<SimpleVector<StringView>>();
          for (vector_size_t row = 0; row < kRows; ++row) {
            EXPECT_EQ(
                values->valueAt(row).getString(),
                commonPrefix + fmt::format("{:04d}", row));
          }
        } else {
          const auto* values =
              output->childAt(0)->asUnchecked<SimpleVector<int64_t>>();
          for (vector_size_t row = 0; row < kRows; ++row) {
            EXPECT_EQ(values->valueAt(row), row);
          }
        }
        EXPECT_EQ(buffer.numOutputRows(), kRows);
      }
    }
  }
}

TEST_F(RadixSortBufferTest, variableMergeCommonPrefixEqualAndNullOrdering) {
  const std::string commonPrefix(96, 'c');
  const std::string equalKey = commonPrefix + "equal";
  auto input = makeRows(
      {"key", "payload", "id"},
      {makeStringVector(
           VARCHAR(),
           {equalKey,
            std::nullopt,
            commonPrefix + "z",
            commonPrefix + "a",
            equalKey,
            commonPrefix + "b",
            equalKey,
            std::nullopt,
            commonPrefix + "y",
            commonPrefix + "c",
            std::nullopt,
            commonPrefix + "d",
            equalKey,
            commonPrefix + "x",
            commonPrefix + "a"}),
       makeStringVector(
           VARCHAR(),
           {std::nullopt,
            "",
            std::string(12, 'a'),
            std::string(13, 'b'),
            std::string(80, 'c'),
            "p5",
            std::nullopt,
            "p7",
            std::string(64, 'd'),
            "p9",
            std::string(13, 'e'),
            std::string(12, 'f'),
            "p12",
            std::nullopt,
            std::string(96, 'g')}),
       generateVector<int64_t>(
           BIGINT(), 15, [](vector_size_t row) { return row; })});
  struct Plan {
    const char* name;
    std::vector<InputRun> runs;
    size_t expectedStreams;
  };
  const std::array<Plan, 4> plans{{
      {"memory-spill two-way", {{8, true}, {7, false}}, 2},
      {"spill-spill two-way", {{8, true}, {7, true}}, 2},
      {"memory-spill loser tree", {{5, true}, {5, true}, {5, false}}, 3},
      {"spill-spill loser tree", {{5, true}, {5, true}, {5, true}}, 3},
  }};

  for (const auto ascending : {false, true}) {
    for (const auto nullsFirst : {false, true}) {
      const auto flags =
          SortComparatorOracle::makeSortFlags(ascending, nullsFirst);
      for (const auto& plan : plans) {
        SCOPED_TRACE(fmt::format(
            "topology={}, ascending={}, nullsFirst={}",
            plan.name,
            ascending,
            nullsFirst));
        auto directory = exec::test::TempDirectoryPath::create();
        auto config = spillConfig(directory->path);
        RadixSortBuffer buffer(
            std::static_pointer_cast<const RowType>(input->type()),
            {0},
            {flags},
            pool(),
            &config);
        addInputRuns(buffer, input, plan.runs);
        buffer.noMoreInput();
        EXPECT_EQ(buffer.testingMergeStreamCount(), plan.expectedStreams);
        auto output = collect(buffer, 1);
        SortComparatorOracle::expectRowsMatchById(*input, *output, 2);
        SortComparatorOracle::expectSorted(*output, {0}, {flags});
      }
    }
  }
}

TEST_F(RadixSortBufferTest, variableMergeOutputProjectionModes) {
  constexpr vector_size_t kRows = 18;
  const std::array<int64_t, kRows> ranks{
      17, 5, 11, 2, 14, 8, 16, 4, 10, 1, 13, 7, 15, 3, 9, 0, 12, 6};
  const std::string commonPrefix(96, 's');
  auto input = makeRows(
      {"prefix", "suffix", "id"},
      {generateVector<int64_t>(
           BIGINT(), kRows, [](vector_size_t row) { return row % 3; }),
       makeStringVector(
           VARCHAR(),
           generate<std::optional<std::string>>(
               kRows,
               [&](vector_size_t row) {
                 return commonPrefix + fmt::format("{:04d}", ranks[row]);
               })),
       generateVector<int64_t>(
           BIGINT(), kRows, [](vector_size_t row) { return row; })});

  const auto runMode = [&](const char* name,
                           const std::vector<column_index_t>& keyChannels,
                           uint64_t& scratchGrowth) {
    SCOPED_TRACE(name);
    auto directory = exec::test::TempDirectoryPath::create();
    auto config = spillConfig(directory->path);
    std::vector<CompareFlags> keyFlags(
        keyChannels.size(), SortComparatorOracle::makeSortFlags(true, true));
    RadixSortBuffer buffer(
        std::static_pointer_cast<const RowType>(input->type()),
        keyChannels,
        keyFlags,
        pool(),
        &config);
    addInputRuns(buffer, input, {{6, true}, {6, true}, {6, false}});
    buffer.noMoreInput();
    ASSERT_EQ(buffer.testingMergeStreamCount(), 3);
    scratchGrowth =
        RadixSortBufferTestHelper::outputAdmissionEstimate(buffer, 5)
            .scratchGrowth;

    std::vector<RowVectorPtr> batches;
    while (auto batch = buffer.getOutput(5)) {
      EXPECT_LE(batch->size(), 5);
      batches.push_back(std::move(batch));
    }
    EXPECT_EQ(batches.size(), 4);
    auto output = concatenateBatches(batches, kRows);
    SortComparatorOracle::expectRowsMatchById(*input, *output, 2);
    SortComparatorOracle::expectSorted(*output, keyChannels, keyFlags);
  };

  uint64_t noDecodedKeyScratch = 0;
  uint64_t prefixOnlyScratch = 0;
  uint64_t prefixPayloadSuffixDecodedScratch = 0;
  uint64_t externalSuffixScratch = 0;
  // Duplicating every key channel makes all output columns payload-backed.
  runMode("no decoded key", {0, 0, 1, 1}, noDecodedKeyScratch);
  // The fixed prefix remains decoded, while the duplicated suffix is payload.
  runMode("fixed prefix only", {0, 1, 1}, prefixOnlyScratch);
  // Duplicating only the fixed prefix leaves the variable suffix decoded.
  runMode(
      "prefix payload-backed, suffix decoded",
      {0, 0, 1},
      prefixPayloadSuffixDecodedScratch);
  runMode("external suffix", {0, 1}, externalSuffixScratch);

  // The payload-backed modes reserve merge key/payload pointer arrays. When the
  // variable suffix still has to be decoded, merge output also needs the same
  // selected-view and decode scratch as the external-suffix path.
  EXPECT_GT(noDecodedKeyScratch, 0);
  EXPECT_EQ(prefixOnlyScratch, noDecodedKeyScratch);
  EXPECT_EQ(prefixPayloadSuffixDecodedScratch, externalSuffixScratch);
  EXPECT_GT(prefixPayloadSuffixDecodedScratch, prefixOnlyScratch);
  EXPECT_GT(externalSuffixScratch, prefixOnlyScratch);
}

TEST_F(
    RadixSortBufferTest,
    externalSuffixOutputSurvivesBlockRetirementAndFileRollover) {
  constexpr vector_size_t kRowsPerRun = 5'000;
  constexpr vector_size_t kRows = 2 * kRowsPerRun;
  constexpr size_t kSuffixPaddingBytes = 400;
  const std::string commonPrefix(220, 'v');
  std::vector<std::optional<std::string>> keys;
  keys.reserve(kRows);
  for (vector_size_t row = 0; row < kRowsPerRun; ++row) {
    const auto rank = 2 * (kRowsPerRun - row - 1);
    keys.push_back(
        commonPrefix + fmt::format("{:010d}_", rank) +
        std::string(kSuffixPaddingBytes, static_cast<char>('a' + rank % 26)));
  }
  for (vector_size_t row = 0; row < kRowsPerRun; ++row) {
    const auto rank = 2 * (kRowsPerRun - row - 1) + 1;
    keys.push_back(
        commonPrefix + fmt::format("{:010d}_", rank) +
        std::string(kSuffixPaddingBytes, static_cast<char>('a' + rank % 26)));
  }
  RowVectorPtr input = makeRows({"key"}, {makeStringVector(VARCHAR(), keys)});
  std::vector<RowVectorPtr> batches;
  {
    SpillContext spill(
        *this,
        input,
        "none",
        /*spillMemoryThreshold=*/0,
        /*keyChannel=*/0,
        /*maxFileSize=*/1,
        /*writeBufferSize=*/0);
    auto& buffer = spill.buffer;
    buffer.addInput(slice(*input, 0, kRowsPerRun));
    buffer.spill();
    buffer.addInput(slice(*input, kRowsPerRun, kRowsPerRun));
    buffer.spill();
    ASSERT_TRUE(buffer.spilledStats());
    EXPECT_EQ(buffer.spilledStats()->spillRuns, 2);
    EXPECT_GT(
        buffer.spilledStats()->spilledFiles, buffer.spilledStats()->spillRuns);
    EXPECT_GE(
        buffer.spilledStats()->spillWrites,
        buffer.spilledStats()->spilledFiles);

    buffer.noMoreInput();
    EXPECT_EQ(buffer.testingMergeStreamCount(), 2);
    while (auto batch = buffer.getOutput(257)) {
      batches.push_back(std::move(batch));
    }
    EXPECT_GT(batches.size(), 2);
    EXPECT_EQ(buffer.numOutputRows(), kRows);
  }

  input.reset();
  pool()->release();
  auto churn = generateStringVector(kRows, [&](vector_size_t row) {
    return std::string(300 + row % 17, static_cast<char>('a' + row % 26));
  });
  (void)churn;

  auto output = concatenateBatches(batches, kRows);
  const auto* outputKeys =
      output->childAt(0)->asUnchecked<SimpleVector<StringView>>();
  for (vector_size_t row = 0; row < kRows; ++row) {
    EXPECT_EQ(
        outputKeys->valueAt(row).getString(),
        commonPrefix + fmt::format("{:010d}_", row) +
            std::string(
                kSuffixPaddingBytes, static_cast<char>('a' + row % 26)));
  }
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
}

TEST_F(RadixSortBufferTest, stateStatsAndEmptyInput) {
  auto inputType = ROW({"key"}, {BIGINT()});
  RadixSortBuffer buffer(
      inputType,
      {0},
      {SortComparatorOracle::makeSortFlags(true, true)},
      pool());
  EXPECT_FALSE(buffer.canSpill());
  EXPECT_FALSE(buffer.canReclaim());
  EXPECT_FALSE(buffer.spilledStats());
  EXPECT_FALSE(buffer.spillReadStats());
  EXPECT_THROW(buffer.getOutput(1), BoltException);

  auto empty = makeRows({"key"}, {BaseVector::create(BIGINT(), 0, pool())});
  buffer.addInput(empty);
  EXPECT_EQ(buffer.numInputRows(), 0);
  buffer.noMoreInput();
  EXPECT_EQ(buffer.getOutput(1), nullptr);
  EXPECT_EQ(buffer.numOutputRows(), 0);
  ASSERT_TRUE(buffer.sortStats());
  EXPECT_THROW(buffer.addInput(empty), BoltException);

  RadixSortBuffer nonEmpty(
      inputType,
      {0},
      {SortComparatorOracle::makeSortFlags(true, true)},
      pool());
  nonEmpty.addInput(makeRows({"key"}, {makeVector<int64_t>(BIGINT(), {1})}));
  EXPECT_EQ(nonEmpty.numInputRows(), 1);
  EXPECT_FALSE(nonEmpty.canReclaim());
  EXPECT_FALSE(nonEmpty.spilledStats());
  EXPECT_FALSE(nonEmpty.spillReadStats());
  nonEmpty.noMoreInput();
  ASSERT_NE(nonEmpty.getOutput(1), nullptr);
  EXPECT_EQ(nonEmpty.numOutputRows(), 1);
  EXPECT_EQ(nonEmpty.getOutput(1), nullptr);
  EXPECT_FALSE(nonEmpty.spilledStats());
  EXPECT_FALSE(nonEmpty.spillReadStats());
  EXPECT_TRUE(nonEmpty.sortStats());
}

TEST_F(RadixSortBufferTest, rejectsNonPositiveOutputBatchSize) {
  auto input = makeRows({"key"}, {makeVector<int64_t>(BIGINT(), {1})});
  for (const auto maxOutputRows : {0, -1}) {
    RadixSortBuffer buffer(
        std::static_pointer_cast<const RowType>(input->type()),
        {0},
        {SortComparatorOracle::makeSortFlags(true, true)},
        pool());
    buffer.addInput(input);
    buffer.noMoreInput();
    EXPECT_THROW(buffer.getOutput(maxOutputRows), BoltException);
  }
}

TEST_F(RadixSortBufferTest, rejectsUnsupportedOrderKeysAndPayload) {
  const auto compareFlags = std::vector<CompareFlags>{
      SortComparatorOracle::makeSortFlags(true, true)};
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
          inputType,
          {inputType->size()},
          {SortComparatorOracle::makeSortFlags(true, true)},
          pool()),
      BoltException);

  auto invalidFlags = SortComparatorOracle::makeSortFlags(true, true);
  invalidFlags.equalsOnly = true;
  EXPECT_THROW(
      RadixSortBuffer(inputType, {0}, {invalidFlags}, pool()), BoltException);
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
