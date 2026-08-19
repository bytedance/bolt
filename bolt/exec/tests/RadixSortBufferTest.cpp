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
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/radixsort/RadixSortBuffer.h"
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

  template <typename T>
  FlatVectorPtr<T> makeVector(
      const TypePtr& type,
      const std::vector<std::optional<T>>& values) {
    auto vector =
        BaseVector::create<FlatVector<T>>(type, values.size(), pool());
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        vector->set(row, *values[row]);
      } else {
        vector->setNull(row, true);
      }
    }
    return vector;
  }

  FlatVectorPtr<StringView> makeStringVector(
      const TypePtr& type,
      const std::vector<std::optional<std::string>>& values) {
    auto vector =
        BaseVector::create<FlatVector<StringView>>(type, values.size(), pool());
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        vector->set(row, StringView(*values[row]));
      } else {
        vector->setNull(row, true);
      }
    }
    return vector;
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

  RowVectorPtr collect(
      RadixSortBuffer& buffer,
      vector_size_t batchSize,
      const RowVectorPtr& prefix = nullptr) {
    const auto total = static_cast<vector_size_t>(buffer.numInputRows());
    if (prefix == nullptr && total > 0 && batchSize >= total) {
      auto result = buffer.getOutput(batchSize);
      EXPECT_NE(result, nullptr);
      EXPECT_EQ(buffer.getOutput(batchSize), nullptr);
      return result;
    }
    std::vector<VectorPtr> children;
    children.reserve(buffer.numInputRows() == 0 ? 0 : inputType_->size());
    for (const auto& type : inputType_->children()) {
      children.push_back(BaseVector::create(type, total, pool()));
    }

    vector_size_t offset = 0;
    if (prefix != nullptr) {
      if (prefix->size() > total) {
        ADD_FAILURE() << "Prefix contains more rows than the sort buffer";
        return nullptr;
      }
      for (uint32_t column = 0; column < children.size(); ++column) {
        children[column]->copy(
            prefix->childAt(column).get(), offset, 0, prefix->size());
      }
      offset += prefix->size();
    }
    while (true) {
      auto batch = buffer.getOutput(batchSize);
      if (batch == nullptr) {
        break;
      }
      if (offset + batch->size() > total) {
        ADD_FAILURE() << "RadixSortBuffer returned more rows than it accepted";
        return nullptr;
      }
      for (uint32_t column = 0; column < children.size(); ++column) {
        children[column]->copy(
            batch->childAt(column).get(), offset, 0, batch->size());
      }
      offset += batch->size();
    }
    EXPECT_EQ(offset, total);
    return std::make_shared<RowVector>(
        pool(), inputType_, nullptr, total, std::move(children));
  }

  RowVectorPtr sortAndCollect(
      const RowVectorPtr& input,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<CompareFlags>& keyFlags,
      vector_size_t batchSize = 3,
      bool multipleInputs = true) {
    inputType_ = std::static_pointer_cast<const RowType>(input->type());
    RadixSortBuffer buffer(inputType_, keyChannels, keyFlags, pool());
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
    const auto statsBefore = buffer.spilledStats();
    const auto rowsBefore =
        statsBefore.has_value() ? statsBefore->spilledRows : 0;
    const auto bytesBefore =
        statsBefore.has_value() ? statsBefore->spilledBytes : 0;
    buffer.spill();
    EXPECT_FALSE(buffer.canReclaim());
    ASSERT_TRUE(buffer.spilledStats().has_value());
    EXPECT_EQ(buffer.spilledStats()->spilledRows, rowsBefore + expectedRows);
    EXPECT_GT(buffer.spilledStats()->spilledBytes, bytesBefore);
    const auto rowsAfter = buffer.spilledStats()->spilledRows;
    const auto bytesAfter = buffer.spilledStats()->spilledBytes;
    buffer.spill();
    EXPECT_EQ(buffer.spilledStats()->spilledRows, rowsAfter);
    EXPECT_EQ(buffer.spilledStats()->spilledBytes, bytesAfter);
  }

  ArrayVectorPtr makeIntegerArrays() {
    auto elements =
        makeVector<int32_t>(INTEGER(), {1, 1, 2, 1, 3, 1, std::nullopt, 2});
    auto arrays = std::make_shared<ArrayVector>(
        pool(),
        ARRAY(INTEGER()),
        nullptr,
        7,
        makeBuffer<vector_size_t>({0, 0, 1, 3, 5, 7, 8}),
        makeBuffer<vector_size_t>({0, 1, 2, 2, 2, 1, 0}),
        elements);
    arrays->setNull(6, true);
    return arrays;
  }

  MapVectorPtr makeIntegerStringMaps() {
    auto keys = makeVector<int32_t>(INTEGER(), {1, 2, 1, 1, 2, 2, 1, 1});
    auto values =
        makeStringVector(VARCHAR(), {"a", "b", "a", "a", "b", "c", "a", "z"});
    auto maps = std::make_shared<MapVector>(
        pool(),
        MAP(INTEGER(), VARCHAR()),
        nullptr,
        7,
        makeBuffer<vector_size_t>({0, 0, 1, 3, 5, 7, 8}),
        makeBuffer<vector_size_t>({0, 1, 2, 2, 2, 1, 0}),
        keys,
        values);
    maps->setNull(6, true);
    return maps;
  }

  MapVectorPtr makeStringStringMapsWithUnusedValues(vector_size_t rows) {
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<std::string>> values;
    keys.reserve(rows * 2 + 32);
    values.reserve(rows * 2 + 32);
    std::vector<vector_size_t> offsets;
    std::vector<vector_size_t> sizes;
    offsets.reserve(rows);
    sizes.reserve(rows);
    vector_size_t offset = 0;
    for (vector_size_t row = 0; row < rows; ++row) {
      offsets.push_back(offset);
      const auto entries = row % 5 == 0 ? 0 : 2;
      sizes.push_back(entries);
      for (vector_size_t entry = 0; entry < entries; ++entry) {
        keys.push_back(
            "k" + std::to_string(row % 11) + "_" + std::to_string(entry));
        values.push_back(
            "v" + std::to_string(row % 17) + "_" + std::to_string(entry));
      }
      offset += entries;
    }
    for (uint32_t unused = 0; unused < 32; ++unused) {
      keys.push_back(std::string(256, 'u'));
      values.push_back(std::string(256, 'x'));
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
    if (rows > 3) {
      maps->setNull(3, true);
    }
    return maps;
  }

  MapVectorPtr makeLargeStringStringMaps(vector_size_t rows) {
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<std::string>> values;
    std::vector<vector_size_t> offsets;
    std::vector<vector_size_t> sizes;
    keys.reserve(rows * 40);
    values.reserve(rows * 40);
    offsets.reserve(rows);
    sizes.reserve(rows);
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
            "param_" + std::to_string(entry % 31) + "_" +
            std::to_string(row % 17));
        values.push_back(
            entry % 9 == 0
                ? std::string(
                      48 + (row + entry) % 41,
                      static_cast<char>('a' + entry % 26))
                : "v_" + std::to_string(row) + "_" + std::to_string(entry));
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
    if (rows > 11) {
      for (vector_size_t row = 11; row < rows; row += 37) {
        maps->setNull(row, true);
      }
    }
    return maps;
  }

  RowVectorPtr makeEventMapPayloadRows(vector_size_t rows) {
    std::vector<std::optional<int64_t>> deviceIds;
    std::vector<std::optional<int64_t>> userIds;
    std::vector<std::optional<int64_t>> groupIds;
    std::vector<std::optional<int64_t>> localTimes;
    std::vector<std::optional<std::string>> enterFrom;
    std::vector<std::optional<std::string>> relationTag;
    std::vector<std::optional<std::string>> authorId;
    std::vector<std::optional<std::string>> hour;
    std::vector<std::optional<std::string>> event;
    deviceIds.reserve(rows);
    userIds.reserve(rows);
    groupIds.reserve(rows);
    localTimes.reserve(rows);
    enterFrom.reserve(rows);
    relationTag.reserve(rows);
    authorId.reserve(rows);
    hour.reserve(rows);
    event.reserve(rows);
    static constexpr std::array<const char*, 8> kEvents{
        "video_play",
        "video_play_pause",
        "like",
        "follow",
        "share",
        "comment",
        "enter_homepage",
        "click_music"};
    for (vector_size_t row = 0; row < rows; ++row) {
      deviceIds.push_back(10'000 + row);
      userIds.push_back(20'000 + row * 3);
      groupIds.push_back(
          row % 3 == 0 ? std::optional<int64_t>{}
                       : std::optional<int64_t>(row % 97));
      localTimes.push_back(1'787'000'000'000 + row * 1000);
      enterFrom.push_back(
          row % 4 == 0 ? std::optional<std::string>{}
                       : std::optional<std::string>(
                             "enter_" + std::to_string(row % 11)));
      relationTag.push_back(
          row % 2 == 0 ? std::optional<std::string>{}
                       : std::optional<std::string>(
                             "relation_" + std::to_string(row % 5)));
      authorId.push_back(
          row % 7 == 0 ? std::optional<std::string>{}
                       : std::optional<std::string>(
                             "author_" + std::to_string(row % 101)));
      hour.push_back((row % 24 < 10 ? "0" : "") + std::to_string(row % 24));
      const auto eventIndex = row % 10 < 6 ? row % 3 : row % kEvents.size();
      event.push_back(kEvents[eventIndex]);
    }

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
        {makeVector<int64_t>(BIGINT(), deviceIds),
         makeVector<int64_t>(BIGINT(), userIds),
         makeVector<int64_t>(BIGINT(), groupIds),
         makeVector<int64_t>(BIGINT(), localTimes),
         makeStringVector(VARCHAR(), enterFrom),
         makeStringVector(VARCHAR(), relationTag),
         makeStringVector(VARCHAR(), authorId),
         makeLargeStringStringMaps(rows),
         makeStringVector(VARCHAR(), hour),
         makeStringVector(VARCHAR(), event)});
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

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  common::SpillConfig spillConfig(
      const std::string& directory,
      const std::string& compressionKind = "none") const {
    return common::SpillConfig(
        [directory]() -> const std::string& { return directory; },
        [&](uint64_t) {},
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
    auto output = sortAndCollect(input, keyChannels, keyFlags, batchSize, true);
    expectRowsMatchById(*input, *output, 3);
    expectSorted(*output, keyChannels, keyFlags);
  }
}

TEST_F(RadixSortBufferTest, estimateOutputRowSizeMatchesLegacyForComplexRows) {
  constexpr vector_size_t kRows = 128;
  std::vector<std::optional<int64_t>> ids;
  std::vector<std::optional<int64_t>> groupIds;
  std::vector<std::optional<int64_t>> localTimes;
  std::vector<std::optional<int64_t>> authorIds;
  std::vector<std::optional<int64_t>> hours;
  std::vector<std::optional<std::string>> users;
  std::vector<std::optional<std::string>> enterFrom;
  std::vector<std::optional<std::string>> relationTags;
  std::vector<std::optional<std::string>> events;
  ids.reserve(kRows);
  groupIds.reserve(kRows);
  localTimes.reserve(kRows);
  authorIds.reserve(kRows);
  hours.reserve(kRows);
  users.reserve(kRows);
  enterFrom.reserve(kRows);
  relationTags.reserve(kRows);
  events.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    ids.push_back(row);
    groupIds.push_back(row % 13);
    localTimes.push_back(1'700'000'000'000 + row);
    authorIds.push_back(row % 19);
    hours.push_back(row % 24);
    users.push_back("user_" + std::to_string(row % 31));
    enterFrom.push_back("enter_" + std::to_string(row % 7));
    relationTags.push_back(
        row % 9 == 0 ? std::optional<std::string>{}
                     : "tag_" + std::to_string(row % 5));
    events.push_back("event_" + std::to_string(row % 4));
  }

  auto input = makeRows(
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
      {makeVector<int64_t>(BIGINT(), ids),
       makeStringVector(VARCHAR(), users),
       makeVector<int64_t>(BIGINT(), groupIds),
       makeVector<int64_t>(BIGINT(), localTimes),
       makeStringVector(VARCHAR(), enterFrom),
       makeStringVector(VARCHAR(), relationTags),
       makeVector<int64_t>(BIGINT(), authorIds),
       makeStringStringMapsWithUnusedValues(kRows),
       makeVector<int64_t>(BIGINT(), hours),
       makeStringVector(VARCHAR(), events)});
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

  ASSERT_TRUE(legacy.estimateOutputRowSize().has_value());
  ASSERT_TRUE(radix.estimateOutputRowSize().has_value());
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
      auto output = sortAndCollect(input, {0}, keyFlags, 2);
      expectRowsMatchById(*input, *output, 1);
      expectSorted(*output, {0}, keyFlags);
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
    auto output = sortAndCollect(input, keyChannels, keyFlags, 2, false);
    expectRowsMatchById(*input, *output, 2);
    expectSorted(*output, keyChannels, keyFlags);
  }
}

TEST_F(RadixSortBufferTest, arrayAndRowKeysWithComplexPayload) {
  auto arrays = makeIntegerArrays();
  auto nestedRows = std::make_shared<RowVector>(
      pool(),
      ROW({"number", "text"}, {INTEGER(), VARCHAR()}),
      nullptr,
      7,
      std::vector<VectorPtr>{
          makeVector<int32_t>(INTEGER(), {1, 1, 2, 1, 1, std::nullopt, 1}),
          makeStringVector(
              VARCHAR(), {"a", "b", "a", std::nullopt, "a", "a", "a"})});
  nestedRows->setNull(6, true);
  auto maps = makeIntegerStringMaps();
  auto input = makeRows(
      {"array", "row", "map", "id"},
      {arrays,
       nestedRows,
       maps,
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5, 6})});
  const std::vector<column_index_t> keyChannels{0, 1};
  const std::vector<CompareFlags> keyFlags{
      flags(true, false), flags(false, true)};
  auto output = sortAndCollect(input, keyChannels, keyFlags, 2);
  expectRowsMatchById(*input, *output, 3);
  expectSorted(*output, keyChannels, keyFlags);
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
  auto output = sortAndCollect(input, keyChannels, keyFlags, 2);
  expectRowsMatchById(*input, *output, 2);
  expectSorted(*output, keyChannels, keyFlags);
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

TEST_F(RadixSortBufferTest, wrappedVectorsAndBitExactFloatOutput) {
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

  for (vector_size_t row = 0; row < output->size(); ++row) {
    const auto id = idAt(*output, row, 2);
    const auto inputIndex = indices->as<vector_size_t>()[id];
    const auto value =
        output->childAt(0)->asUnchecked<SimpleVector<double>>()->valueAt(row);
    EXPECT_EQ(std::bit_cast<uint64_t>(value), bits[inputIndex]);
    EXPECT_EQ(
        output->childAt(1)
            ->asUnchecked<SimpleVector<StringView>>()
            ->valueAt(row)
            .getString(),
        "constant");
  }
  expectSorted(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortBufferTest, explicitSpillMergesDiskAndMemoryRuns) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  auto first = makeRows(
      {"key", "payload", "id"},
      {makeVector<int64_t>(BIGINT(), {40, 10, 30}),
       makeStringVector(
           VARCHAR(),
           {std::string(80, 'd'), std::string(80, 'a'), std::string(80, 'c')}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2})});
  auto second = makeRows(
      {"key", "payload", "id"},
      {makeVector<int64_t>(BIGINT(), {20, 50, 0}),
       makeStringVector(
           VARCHAR(),
           {std::string(80, 'b'), std::string(80, 'e'), std::string(80, 'z')}),
       makeVector<int64_t>(BIGINT(), {3, 4, 5})});
  inputType_ = std::static_pointer_cast<const RowType>(first->type());
  RadixSortBuffer buffer(inputType_, {0}, {flags(true, true)}, pool(), &config);
  buffer.addInput(first);
  buffer.spill();
  ASSERT_TRUE(buffer.spilledStats().has_value());
  EXPECT_GT(buffer.spilledStats()->spilledRows, 0);
  buffer.addInput(second);
  buffer.noMoreInput();
  auto output = collect(buffer, 2);

  auto combined = makeRows(
      {"key", "payload", "id"},
      {makeVector<int64_t>(BIGINT(), {40, 10, 30, 20, 50, 0}),
       makeStringVector(
           VARCHAR(),
           {std::string(80, 'd'),
            std::string(80, 'a'),
            std::string(80, 'c'),
            std::string(80, 'b'),
            std::string(80, 'e'),
            std::string(80, 'z')}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5})});
  expectRowsMatchById(*combined, *output, 2);
  expectSorted(*output, {0}, {flags(true, true)});
  EXPECT_EQ(buffer.numOutputRows(), 6);
  EXPECT_TRUE(buffer.spillReadStats().has_value());
}

TEST_F(RadixSortBufferTest, spillMergeNullFreeOutputResetsNullBuffers) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  auto first = makeRows(
      {"text_key",
       "nullable_key",
       "fixed_payload",
       "string_payload",
       "nullable_payload",
       "id"},
      {makeStringVector(
           VARCHAR(),
           {std::string("k0"), std::string("k1"), std::string("k4")}),
       makeVector<int64_t>(BIGINT(), {std::nullopt, 2, 1}),
       makeVector<int64_t>(BIGINT(), {60, 10, 40}),
       makeStringVector(
           VARCHAR(),
           {std::string(72, 'f'), std::string(72, 'a'), std::string(72, 'd')}),
       makeVector<int64_t>(BIGINT(), {std::nullopt, 200, 100}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2})});
  auto second = makeRows(
      {"text_key",
       "nullable_key",
       "fixed_payload",
       "string_payload",
       "nullable_payload",
       "id"},
      {makeStringVector(
           VARCHAR(),
           {std::string("k3"), std::string("k5"), std::string("k2")}),
       makeVector<int64_t>(BIGINT(), {3, 0, 4}),
       makeVector<int64_t>(BIGINT(), {30, 50, 20}),
       makeStringVector(
           VARCHAR(),
           {std::string(72, 'c'), std::string(72, 'e'), std::string(72, 'b')}),
       makeVector<int64_t>(BIGINT(), {300, 500, 400}),
       makeVector<int64_t>(BIGINT(), {3, 4, 5})});
  inputType_ = std::static_pointer_cast<const RowType>(first->type());
  RadixSortBuffer buffer(
      inputType_,
      {0, 1},
      {flags(true, true), flags(true, true)},
      pool(),
      &config);
  buffer.addInput(first);
  buffer.spill();
  ASSERT_TRUE(buffer.spilledStats().has_value());
  buffer.addInput(second);
  buffer.noMoreInput();

  auto firstOutput = buffer.getOutput(3);
  ASSERT_NE(firstOutput, nullptr);
  ASSERT_NE(firstOutput->childAt(1)->rawNulls(), nullptr);
  ASSERT_NE(firstOutput->childAt(4)->rawNulls(), nullptr);
  std::vector<VectorPtr> children;
  children.reserve(inputType_->size());
  for (const auto& type : inputType_->children()) {
    children.push_back(BaseVector::create(type, 6, pool()));
  }
  for (uint32_t column = 0; column < children.size(); ++column) {
    children[column]->copy(
        firstOutput->childAt(column).get(), 0, 0, firstOutput->size());
  }
  vector_size_t offset = firstOutput->size();
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
  for (uint32_t column = 0; column < children.size(); ++column) {
    children[column]->copy(
        secondOutput->childAt(column).get(), offset, 0, secondOutput->size());
  }
  offset += secondOutput->size();
  ASSERT_EQ(buffer.getOutput(3), nullptr);

  auto output = std::make_shared<RowVector>(
      pool(), inputType_, nullptr, offset, std::move(children));
  auto combined = makeRows(
      {"text_key",
       "nullable_key",
       "fixed_payload",
       "string_payload",
       "nullable_payload",
       "id"},
      {makeStringVector(
           VARCHAR(),
           {std::string("k0"),
            std::string("k1"),
            std::string("k4"),
            std::string("k3"),
            std::string("k5"),
            std::string("k2")}),
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
  expectRowsMatchById(*combined, *output, 5);
  expectSorted(*output, {0, 1}, {flags(true, true), flags(true, true)});
  EXPECT_EQ(buffer.numOutputRows(), 6);
  EXPECT_TRUE(buffer.spillReadStats().has_value());
}

TEST_F(RadixSortBufferTest, spillConfigOwnsDirectoryPath) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  EXPECT_EQ(config.getSpillDirPathCb(), spillDirectory->path);
}

TEST_F(RadixSortBufferTest, multipleSpilledRunsWithoutMemoryRun) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  auto inputType =
      ROW({"key", "payload", "id"}, {BIGINT(), VARCHAR(), BIGINT()});
  inputType_ = inputType;
  RadixSortBuffer buffer(inputType, {0}, {flags(true, true)}, pool(), &config);

  auto first = makeRows(
      {"key", "payload", "id"},
      {makeVector<int64_t>(BIGINT(), {9, 1, 5}),
       makeStringVector(
           VARCHAR(),
           {std::string(64, 'i'), std::string(64, 'a'), std::string(64, 'e')}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2})});
  auto second = makeRows(
      {"key", "payload", "id"},
      {makeVector<int64_t>(BIGINT(), {8, 2, 6}),
       makeStringVector(
           VARCHAR(),
           {std::string(64, 'h'), std::string(64, 'b'), std::string(64, 'f')}),
       makeVector<int64_t>(BIGINT(), {3, 4, 5})});
  buffer.addInput(first);
  buffer.spill();
  buffer.addInput(second);
  buffer.spill();
  buffer.noMoreInput();
  auto output = collect(buffer, 4);

  auto combined = makeRows(
      {"key", "payload", "id"},
      {makeVector<int64_t>(BIGINT(), {9, 1, 5, 8, 2, 6}),
       makeStringVector(
           VARCHAR(),
           {std::string(64, 'i'),
            std::string(64, 'a'),
            std::string(64, 'e'),
            std::string(64, 'h'),
            std::string(64, 'b'),
            std::string(64, 'f')}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5})});
  expectRowsMatchById(*combined, *output, 2);
  expectSorted(*output, {0}, {flags(true, true)});
  ASSERT_TRUE(buffer.spilledStats().has_value());
  EXPECT_EQ(buffer.spilledStats()->spilledRows, 6);
}

TEST_F(RadixSortBufferTest, keyOnlySpillOutput) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  auto inputType = ROW({"key"}, {BIGINT()});
  inputType_ = inputType;
  RadixSortBuffer buffer(inputType, {0}, {flags(true, true)}, pool(), &config);
  buffer.addInput(
      makeRows({"key"}, {makeVector<int64_t>(BIGINT(), {4, 1, 3})}));
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
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  auto inputType = ROW({"key", "id"}, {BIGINT(), BIGINT()});
  inputType_ = inputType;
  RadixSortBuffer buffer(
      inputType, {0}, {flags(true, true)}, pool(), &config, 1);
  buffer.addInput(makeRows(
      {"key", "id"},
      {makeVector<int64_t>(BIGINT(), {3, 1}),
       makeVector<int64_t>(BIGINT(), {0, 1})}));
  buffer.addInput(makeRows(
      {"key", "id"},
      {makeVector<int64_t>(BIGINT(), {2, 4}),
       makeVector<int64_t>(BIGINT(), {2, 3})}));
  buffer.noMoreInput();
  auto output = collect(buffer, 2);
  ASSERT_TRUE(buffer.spilledStats().has_value());
  expectSorted(*output, {0}, {flags(true, true)});
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

  auto makeWideInput = [&](std::vector<int64_t> keys,
                           std::vector<int64_t> ids) {
    std::vector<VectorPtr> children;
    children.push_back(makeVector<int64_t>(
        BIGINT(),
        {std::optional<int64_t>(keys[0]), std::optional<int64_t>(keys[1])}));
    for (int32_t column = 0; column < kPayloadColumns; ++column) {
      children.push_back(makeVector<int64_t>(
          BIGINT(),
          {1000 + column + ids[0] * kPayloadColumns,
           1000 + column + ids[1] * kPayloadColumns}));
    }
    children.push_back(makeVector<int64_t>(
        BIGINT(),
        {std::optional<int64_t>(ids[0]), std::optional<int64_t>(ids[1])}));
    return makeRows(inputType->names(), children);
  };
  buffer.addInput(makeWideInput({3, 1}, {0, 1}));

  auto secondInput = makeWideInput({2, 4}, {2, 3});
  buffer.addInput(secondInput);

  buffer.noMoreInput();
  auto output = collect(buffer, 2);
  ASSERT_TRUE(buffer.spilledStats().has_value());
  EXPECT_EQ(buffer.spilledStats()->spilledRows, 2);
  expectSorted(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortBufferTest, spillBatchOutputSizes) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  auto input = makeRows(
      {"key", "payload", "id"},
      {makeVector<int64_t>(BIGINT(), {8, 1, 7, 2, 6, 3, 5, 4}),
       makeStringVector(
           VARCHAR(),
           {std::string(40, 'h'),
            std::string(40, 'a'),
            std::string(40, 'g'),
            std::string(40, 'b'),
            std::string(40, 'f'),
            std::string(40, 'c'),
            std::string(40, 'e'),
            std::string(40, 'd')}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5, 6, 7})});
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  RadixSortBuffer buffer(inputType_, {0}, {flags(true, true)}, pool(), &config);
  buffer.addInput(slice(*input, 0, 3));
  buffer.spill();
  buffer.addInput(slice(*input, 3, 3));
  buffer.spill();
  buffer.addInput(slice(*input, 6, 2));
  buffer.noMoreInput();

  std::vector<vector_size_t> batchSizes;
  std::vector<VectorPtr> children;
  for (const auto& type : inputType_->children()) {
    children.push_back(BaseVector::create(type, input->size(), pool()));
  }
  vector_size_t offset = 0;
  while (auto batch = buffer.getOutput(3)) {
    batchSizes.push_back(batch->size());
    for (uint32_t column = 0; column < children.size(); ++column) {
      children[column]->copy(
          batch->childAt(column).get(), offset, 0, batch->size());
    }
    offset += batch->size();
  }
  EXPECT_EQ(batchSizes, std::vector<vector_size_t>({3, 3, 2}));
  auto output = std::make_shared<RowVector>(
      pool(), inputType_, nullptr, offset, std::move(children));
  expectRowsMatchById(*input, *output, 2);
  expectSorted(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortBufferTest, spillDisabledAndEmptySpill) {
  auto inputType = ROW({"key", "id"}, {BIGINT(), BIGINT()});
  inputType_ = inputType;
  RadixSortBuffer disabled(inputType, {0}, {flags(true, true)}, pool());
  EXPECT_FALSE(disabled.canSpill());
  EXPECT_THROW(disabled.spill(), BoltException);

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  RadixSortBuffer empty(inputType, {0}, {flags(true, true)}, pool(), &config);
  EXPECT_TRUE(empty.canSpill());
  empty.spill();
  empty.noMoreInput();
  EXPECT_FALSE(empty.spilledStats().has_value());
  EXPECT_EQ(empty.getOutput(1), nullptr);

  RadixSortBuffer postSpillInput(
      inputType, {0}, {flags(true, true)}, pool(), &config);
  postSpillInput.spill();
  postSpillInput.addInput(makeRows(
      {"key", "id"},
      {makeVector<int64_t>(BIGINT(), {2, 1}),
       makeVector<int64_t>(BIGINT(), {0, 1})}));
  postSpillInput.noMoreInput();
  EXPECT_FALSE(postSpillInput.spilledStats().has_value());
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
      auto spillDirectory = exec::test::TempDirectoryPath::create();
      auto config = spillConfig(spillDirectory->path);
      const std::vector<CompareFlags> keyFlags{flags(ascending, nullsFirst)};
      RadixSortBuffer buffer(inputType_, {0}, keyFlags, pool(), &config);
      buffer.addInput(slice(*input, 0, 3));
      buffer.spill();
      buffer.addInput(slice(*input, 3, input->size() - 3));
      buffer.noMoreInput();
      auto output = collect(buffer, 2);
      expectRowsMatchById(*input, *output, 1);
      expectSorted(*output, {0}, keyFlags);
      ASSERT_TRUE(buffer.spilledStats().has_value());
    }
  }
}

TEST_F(RadixSortBufferTest, compressedSpillOutput) {
  auto input = makeRows(
      {"key", "payload", "id"},
      {makeVector<int64_t>(BIGINT(), {5, 1, 4, 2, 3}),
       makeStringVector(
           VARCHAR(),
           {std::string(256, 'e'),
            std::string(256, 'a'),
            std::string(256, 'd'),
            std::string(256, 'b'),
            std::string(256, 'c')}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4})});
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  for (const auto& compression : {std::string("lz4"), std::string("zstd")}) {
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto config = spillConfig(spillDirectory->path, compression);
    RadixSortBuffer buffer(
        inputType_, {0}, {flags(true, true)}, pool(), &config);
    buffer.addInput(slice(*input, 0, 3));
    buffer.spill();
    buffer.addInput(slice(*input, 3, 2));
    buffer.noMoreInput();
    auto output = collect(buffer, 2);
    expectRowsMatchById(*input, *output, 2);
    expectSorted(*output, {0}, {flags(true, true)});
    ASSERT_TRUE(buffer.spilledStats().has_value());
    ASSERT_TRUE(buffer.spillReadStats().has_value());
    EXPECT_GT(buffer.spillReadStats()->spillReadTimeUs, 0);
  }
}

TEST_F(RadixSortBufferTest, outputStageSpillAfterPartialOutput) {
  auto input = makeRows(
      {"key", "payload", "id"},
      {makeVector<int64_t>(BIGINT(), {8, 1, 7, 2, 6, 3, 5, 4}),
       makeStringVector(
           VARCHAR(),
           {std::string(128, 'h'),
            std::string(128, 'a'),
            std::string(128, 'g'),
            std::string(128, 'b'),
            std::string(128, 'f'),
            std::string(128, 'c'),
            std::string(128, 'e'),
            std::string(128, 'd')}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5, 6, 7})});
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path, "lz4");
  RadixSortBuffer buffer(inputType_, {0}, {flags(true, true)}, pool(), &config);
  buffer.addInput(input);
  buffer.noMoreInput();

  auto prefix = buffer.getOutput(3);
  ASSERT_NE(prefix, nullptr);
  EXPECT_EQ(prefix->size(), 3);
  spillRemainingOutputAndCheckStats(buffer, 5);
  auto output = collect(buffer, 2, prefix);
  expectRowsMatchById(*input, *output, 2);
  expectSorted(*output, {0}, {flags(true, true)});
  EXPECT_EQ(buffer.numOutputRows(), input->size());
  EXPECT_TRUE(buffer.spillReadStats().has_value());
}

TEST_F(RadixSortBufferTest, outputStageSpillAfterInputSpill) {
  auto input = makeRows(
      {"key", "payload", "id"},
      {makeVector<int64_t>(BIGINT(), {9, 1, 8, 2, 7, 3, 6, 4, 5}),
       makeStringVector(
           VARCHAR(),
           {std::string(96, 'i'),
            std::string(96, 'a'),
            std::string(96, 'h'),
            std::string(96, 'b'),
            std::string(96, 'g'),
            std::string(96, 'c'),
            std::string(96, 'f'),
            std::string(96, 'd'),
            std::string(96, 'e')}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5, 6, 7, 8})});
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  RadixSortBuffer buffer(inputType_, {0}, {flags(true, true)}, pool(), &config);
  buffer.addInput(slice(*input, 0, 3));
  buffer.spill();
  buffer.addInput(slice(*input, 3, 3));
  buffer.spill();
  buffer.addInput(slice(*input, 6, 3));
  buffer.noMoreInput();

  auto prefix = buffer.getOutput(4);
  ASSERT_NE(prefix, nullptr);
  EXPECT_EQ(prefix->size(), 4);
  spillRemainingOutputAndCheckStats(buffer, 5);
  auto output = collect(buffer, 2, prefix);
  expectRowsMatchById(*input, *output, 2);
  expectSorted(*output, {0}, {flags(true, true)});
  EXPECT_EQ(buffer.numOutputRows(), input->size());
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
  EXPECT_TRUE(buffer.spillReadStats().has_value());
}

TEST_F(RadixSortBufferTest, complexKeyAndPayloadSpillOutput) {
  auto arrays = makeIntegerArrays();
  auto maps = makeIntegerStringMaps();
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
  auto input = makeRows(
      {"array_key", "row_key", "map_payload", "id"},
      {arrays,
       rows,
       maps,
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5, 6})});
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path);
  const std::vector<column_index_t> keyChannels{0, 1};
  const std::vector<CompareFlags> keyFlags{
      flags(true, false), flags(false, true)};
  RadixSortBuffer buffer(inputType_, keyChannels, keyFlags, pool(), &config);
  buffer.addInput(slice(*input, 0, 3));
  buffer.spill();
  buffer.addInput(slice(*input, 3, 4));
  buffer.noMoreInput();
  auto output = collect(buffer, 2);
  expectRowsMatchById(*input, *output, 3);
  expectSorted(*output, keyChannels, keyFlags);
}

TEST_F(RadixSortBufferTest, eventStringKeyWideMapPayloadSpill) {
  constexpr vector_size_t kRows = 96;
  auto input = makeEventMapPayloadRows(kRows);
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path, "zstd");
  const std::vector<column_index_t> keyChannels{9};
  const std::vector<CompareFlags> keyFlags{flags(true, true)};
  RadixSortBuffer buffer(inputType_, keyChannels, keyFlags, pool(), &config);
  buffer.addInput(slice(*input, 0, 33));
  buffer.spill();
  buffer.addInput(slice(*input, 33, 31));
  buffer.spill();
  buffer.addInput(slice(*input, 64, kRows - 64));
  buffer.noMoreInput();

  auto output = collect(buffer, 17);
  expectRowsMatchById(*input, *output, 0, std::nullopt, 10'000);
  expectSorted(*output, keyChannels, keyFlags);
  ASSERT_TRUE(buffer.spilledStats().has_value());
  ASSERT_TRUE(buffer.spillReadStats().has_value());
  EXPECT_GT(buffer.spilledStats()->spilledRows, kRows / 2);
  EXPECT_GT(buffer.spilledStats()->spilledBytes, 0);
  EXPECT_GT(buffer.spillReadStats()->spillReadTimeUs, 0);
}

TEST_F(RadixSortBufferTest, compressedZstdSpillWithMapPayload) {
  constexpr vector_size_t kRows = 72;
  auto input = makeEventMapPayloadRows(kRows);
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path, "zstd");
  const std::vector<column_index_t> keyChannels{9};
  const std::vector<CompareFlags> keyFlags{flags(true, true)};
  RadixSortBuffer buffer(inputType_, keyChannels, keyFlags, pool(), &config);
  buffer.addInput(slice(*input, 0, 25));
  buffer.addInput(slice(*input, 25, 23));
  buffer.spill();
  buffer.addInput(slice(*input, 48, kRows - 48));
  buffer.noMoreInput();

  auto output = collect(buffer, 16);
  expectRowsMatchById(*input, *output, 0, std::nullopt, 10'000);
  expectSorted(*output, keyChannels, keyFlags);
  ASSERT_TRUE(buffer.spilledStats().has_value());
  ASSERT_TRUE(buffer.spillReadStats().has_value());
  EXPECT_GT(buffer.spilledStats()->spilledBytes, 0);
  EXPECT_GT(buffer.spillReadStats()->spillDecompressTimeUs, 0);
}

TEST_F(RadixSortBufferTest, outputStageSpillWithMapPayload) {
  constexpr vector_size_t kRows = 80;
  auto input = makeEventMapPayloadRows(kRows);
  inputType_ = std::static_pointer_cast<const RowType>(input->type());
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto config = spillConfig(spillDirectory->path, "lz4");
  const std::vector<column_index_t> keyChannels{9};
  const std::vector<CompareFlags> keyFlags{flags(true, true)};
  RadixSortBuffer buffer(inputType_, keyChannels, keyFlags, pool(), &config);
  buffer.addInput(slice(*input, 0, 27));
  buffer.addInput(slice(*input, 27, 25));
  buffer.addInput(slice(*input, 52, kRows - 52));
  buffer.noMoreInput();

  auto prefix = buffer.getOutput(19);
  ASSERT_NE(prefix, nullptr);
  ASSERT_EQ(prefix->size(), 19);
  spillRemainingOutputAndCheckStats(buffer, kRows - prefix->size());
  auto output = collect(buffer, 23, prefix);
  expectRowsMatchById(*input, *output, 0, std::nullopt, 10'000);
  expectSorted(*output, keyChannels, keyFlags);
  EXPECT_EQ(buffer.numOutputRows(), input->size());
  ASSERT_TRUE(buffer.spillReadStats().has_value());
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

  auto empty = std::make_shared<RowVector>(
      pool(),
      inputType,
      nullptr,
      0,
      std::vector<VectorPtr>{BaseVector::create(BIGINT(), 0, pool())});
  buffer.addInput(empty);
  EXPECT_EQ(buffer.numInputRows(), 0);
  buffer.noMoreInput();
  EXPECT_EQ(buffer.getOutput(1), nullptr);
  EXPECT_EQ(buffer.numOutputRows(), 0);
  ASSERT_TRUE(buffer.sortStats().has_value());
  EXPECT_THROW(buffer.addInput(empty), BoltException);
  EXPECT_THROW(buffer.noMoreInput(), BoltException);
  EXPECT_EQ(buffer.getOutput(0), nullptr);

  RadixSortBuffer nonEmpty(inputType, {0}, {flags(true, true)}, pool());
  nonEmpty.addInput(makeRows({"key"}, {makeVector<int64_t>(BIGINT(), {1})}));
  nonEmpty.noMoreInput();
  EXPECT_THROW(nonEmpty.getOutput(0), BoltException);
}

TEST_F(RadixSortBufferTest, rejectsUnsupportedOrderKeysAndPayload) {
  const auto compareFlags = std::vector<CompareFlags>{flags(true, true)};
  for (const auto& keyType : std::vector<TypePtr>{
           MAP(INTEGER(), BIGINT()),
           ARRAY(MAP(INTEGER(), BIGINT())),
           ROW({INTEGER(), MAP(INTEGER(), BIGINT())}),
           VARIANT(),
           ARRAY(VARIANT()),
           ROW({INTEGER(), VARIANT()}),
           OPAQUE<int32_t>(),
           FUNCTION({BIGINT()}, BOOLEAN())}) {
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
  EXPECT_THROW(RadixSortBuffer(inputType, {}, {}, pool()), BoltException);
  EXPECT_THROW(RadixSortBuffer(inputType, {0}, {}, pool()), BoltException);
  EXPECT_THROW(
      RadixSortBuffer(
          inputType, {inputType->size()}, {flags(true, true)}, pool()),
      BoltException);
  EXPECT_THROW(
      RadixSortBuffer(inputType, {0}, {flags(true, true)}, nullptr),
      BoltException);

  auto invalidFlags = flags(true, true);
  invalidFlags.equalsOnly = true;
  EXPECT_THROW(
      RadixSortBuffer(inputType, {0}, {invalidFlags}, pool()), BoltException);

  RadixSortBuffer buffer(inputType, {0}, {flags(true, true)}, pool());
  auto wrongType = makeRows(
      {"key", "value"},
      {makeVector<int32_t>(INTEGER(), {1}),
       makeStringVector(VARCHAR(), {"one"})});
  EXPECT_THROW(buffer.addInput(wrongType), BoltException);
  EXPECT_EQ(buffer.numInputRows(), 0);
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
