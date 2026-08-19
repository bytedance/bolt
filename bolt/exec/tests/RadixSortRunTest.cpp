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
#include <cmath>
#include <cstring>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "bolt/exec/radixsort/RadixSortRun.h"
#include "bolt/exec/tests/utils/RadixSortComparatorOracle.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/SimpleVector.h"

namespace bytedance::bolt::exec::radixsort::test {
namespace {

class RadixSortRunTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

 protected:
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> runPool_{
      rootPool_->addLeafChild("radix-sort-run-test")};
  std::shared_ptr<memory::MemoryPool> outputPool_{
      rootPool_->addLeafChild("radix-sort-run-output-test")};

  static CompareFlags flags(bool ascending, bool nullsFirst) {
    return CompareFlags{
        .nullsFirst = nullsFirst,
        .ascending = ascending,
        .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
  }

  template <typename T>
  FlatVectorPtr<T> makeVector(
      memory::MemoryPool* pool,
      const TypePtr& type,
      const std::vector<std::optional<T>>& values) {
    auto vector = BaseVector::create<FlatVector<T>>(type, values.size(), pool);
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
      memory::MemoryPool* pool,
      const TypePtr& type,
      const std::vector<std::optional<std::string>>& values) {
    auto vector =
        BaseVector::create<FlatVector<StringView>>(type, values.size(), pool);
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        vector->set(row, StringView(*values[row]));
      } else {
        vector->setNull(row, true);
      }
    }
    return vector;
  }

  template <typename T>
  BufferPtr makeBuffer(memory::MemoryPool* pool, const std::vector<T>& values) {
    auto buffer = AlignedBuffer::allocate<T>(values.size(), pool);
    std::copy(values.begin(), values.end(), buffer->template asMutable<T>());
    return buffer;
  }

  ArrayVectorPtr makeNonNullIntegerArrays(vector_size_t size) {
    std::vector<vector_size_t> offsets(size);
    std::vector<vector_size_t> sizes(size, 1);
    std::vector<std::optional<int32_t>> elements(size);
    for (vector_size_t row = 0; row < size; ++row) {
      offsets[row] = row;
      elements[row] = row * 10 + 1;
    }
    return std::make_shared<ArrayVector>(
        runPool_.get(),
        ARRAY(INTEGER()),
        nullptr,
        size,
        makeBuffer(runPool_.get(), offsets),
        makeBuffer(runPool_.get(), sizes),
        makeVector<int32_t>(runPool_.get(), INTEGER(), elements));
  }

  ArrayVectorPtr makeIntegerArrays() {
    const std::array<vector_size_t, 7> offsets{0, 0, 1, 3, 5, 7, 8};
    const std::array<vector_size_t, 7> sizes{0, 1, 2, 2, 2, 1, 0};
    auto offsetBuffer =
        AlignedBuffer::allocate<vector_size_t>(offsets.size(), runPool_.get());
    auto sizeBuffer =
        AlignedBuffer::allocate<vector_size_t>(sizes.size(), runPool_.get());
    std::memcpy(
        offsetBuffer->asMutable<vector_size_t>(),
        offsets.data(),
        sizeof(offsets));
    std::memcpy(
        sizeBuffer->asMutable<vector_size_t>(), sizes.data(), sizeof(sizes));
    auto elements = makeVector<int32_t>(
        runPool_.get(), INTEGER(), {1, 1, 2, 1, 3, 1, std::nullopt, 2});
    auto arrays = std::make_shared<ArrayVector>(
        runPool_.get(),
        ARRAY(INTEGER()),
        nullptr,
        offsets.size(),
        std::move(offsetBuffer),
        std::move(sizeBuffer),
        elements);
    arrays->setNull(6, true);
    return arrays;
  }

  RowVectorPtr makeNestedRows() {
    auto rows = std::make_shared<RowVector>(
        runPool_.get(),
        ROW({"number", "text"}, {INTEGER(), VARCHAR()}),
        nullptr,
        7,
        std::vector<VectorPtr>{
            makeVector<int32_t>(
                runPool_.get(), INTEGER(), {1, 1, 2, 1, 1, std::nullopt, 1}),
            makeStringVector(
                runPool_.get(),
                VARCHAR(),
                {std::string("a"),
                 std::string("b"),
                 std::string("a"),
                 std::nullopt,
                 std::string("a"),
                 std::string("a"),
                 std::string("a")})});
    rows->setNull(6, true);
    return rows;
  }

  MapVectorPtr makeIntegerStringMaps() {
    const std::array<vector_size_t, 7> offsets{0, 0, 1, 3, 5, 7, 8};
    const std::array<vector_size_t, 7> sizes{0, 1, 2, 2, 2, 1, 0};
    auto offsetBuffer =
        AlignedBuffer::allocate<vector_size_t>(offsets.size(), runPool_.get());
    auto sizeBuffer =
        AlignedBuffer::allocate<vector_size_t>(sizes.size(), runPool_.get());
    std::memcpy(
        offsetBuffer->asMutable<vector_size_t>(),
        offsets.data(),
        sizeof(offsets));
    std::memcpy(
        sizeBuffer->asMutable<vector_size_t>(), sizes.data(), sizeof(sizes));
    auto keys = makeVector<int32_t>(
        runPool_.get(), INTEGER(), {1, 2, 1, 1, 2, 2, 1, 1});
    auto values = makeStringVector(
        runPool_.get(),
        VARCHAR(),
        {std::string("a"),
         std::string("b"),
         std::string("a"),
         std::string("a"),
         std::string("b"),
         std::string("c"),
         std::string("a"),
         std::string("z")});
    auto maps = std::make_shared<MapVector>(
        runPool_.get(),
        MAP(INTEGER(), VARCHAR()),
        nullptr,
        offsets.size(),
        std::move(offsetBuffer),
        std::move(sizeBuffer),
        keys,
        values);
    maps->setNull(6, true);
    return maps;
  }

  MapVectorPtr makeStringStringMaps(
      memory::MemoryPool* pool,
      vector_size_t rows) {
    std::vector<vector_size_t> offsets;
    std::vector<vector_size_t> sizes;
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<std::string>> values;
    offsets.reserve(rows);
    sizes.reserve(rows);
    vector_size_t offset = 0;
    for (vector_size_t row = 0; row < rows; ++row) {
      offsets.push_back(offset);
      const vector_size_t entries = row % 7 == 0 ? 0
          : row % 5 == 0                         ? 16
          : row % 3 == 0                         ? 8
                                                 : 3;
      sizes.push_back(entries);
      for (vector_size_t entry = 0; entry < entries; ++entry) {
        keys.push_back(
            "param_" + std::to_string(entry % 11) + "_" +
            std::to_string(row % 5));
        values.push_back(
            entry % 6 == 0
                ? std::string(32 + row % 9, static_cast<char>('a' + entry))
                : "value_" + std::to_string(row) + "_" + std::to_string(entry));
      }
      offset += entries;
    }
    auto maps = std::make_shared<MapVector>(
        pool,
        MAP(VARCHAR(), VARCHAR()),
        nullptr,
        rows,
        makeBuffer(pool, offsets),
        makeBuffer(pool, sizes),
        makeStringVector(pool, VARCHAR(), keys),
        makeStringVector(pool, VARCHAR(), values));
    if (rows > 9) {
      maps->setNull(9, true);
    }
    return maps;
  }

  MapVectorPtr makeStringStringMaps(vector_size_t rows) {
    return makeStringStringMaps(runPool_.get(), rows);
  }

  RowVectorPtr makeRows(
      memory::MemoryPool* pool,
      std::vector<std::string> names,
      const std::vector<VectorPtr>& children) {
    std::vector<TypePtr> types;
    types.reserve(children.size());
    for (const auto& child : children) {
      types.push_back(child->type());
    }
    return std::make_shared<RowVector>(
        pool,
        ROW(std::move(names), std::move(types)),
        nullptr,
        children.empty() ? 0 : children.front()->size(),
        children);
  }

  static RowTypePtr rowTypeOf(const RowVector& rows) {
    return std::static_pointer_cast<const RowType>(rows.type());
  }

  RowVectorPtr
  slice(const RowVector& input, vector_size_t offset, vector_size_t count) {
    std::vector<VectorPtr> children;
    children.reserve(input.childrenSize());
    for (uint32_t column = 0; column < input.childrenSize(); ++column) {
      auto child = BaseVector::create(
          input.childAt(column)->type(), count, runPool_.get());
      child->copy(input.childAt(column).get(), 0, offset, count);
      children.push_back(std::move(child));
    }
    return std::make_shared<RowVector>(
        runPool_.get(), input.type(), nullptr, count, std::move(children));
  }

  std::unique_ptr<RadixSortRun> createRun(
      const RowTypePtr& outputType,
      const RowTypePtr& keyType,
      const std::vector<CompareFlags>& keyFlags,
      const std::vector<column_index_t>& directKeyChannels,
      const std::vector<bool>& bitExactRequired = {},
      RadixSortRunOptions options = {}) {
    auto run = RadixSortRun::create(
        runPool_.get(),
        outputType,
        keyType,
        keyFlags,
        directKeyChannels,
        bitExactRequired,
        std::move(options));
    EXPECT_NE(run, nullptr);
    return run;
  }

  RowVectorPtr collect(RadixSortRun& run, vector_size_t batchSize) {
    const auto total = static_cast<vector_size_t>(run.metrics().inputRows);
    std::vector<VectorPtr> children;
    children.reserve(run.projection().outputType()->size());
    for (const auto& type : run.projection().outputType()->children()) {
      children.push_back(BaseVector::create(type, total, outputPool_.get()));
    }

    vector_size_t offset = 0;
    while (true) {
      auto batch = run.getOutput(batchSize, outputPool_.get());
      if (batch == nullptr) {
        break;
      }
      if (offset + batch->size() > total) {
        ADD_FAILURE() << "RadixSortRun returned more rows than it accepted";
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
        outputPool_.get(),
        run.projection().outputType(),
        nullptr,
        total,
        std::move(children));
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
      column_index_t idChannel) {
    ASSERT_EQ(output.size(), input.size());
    std::vector<bool> seen(input.size(), false);
    const auto compareFlags = flags(true, true);
    for (vector_size_t row = 0; row < output.size(); ++row) {
      const auto id = idAt(output, row, idChannel);
      ASSERT_GE(id, 0);
      ASSERT_LT(id, input.size());
      EXPECT_FALSE(seen[id]);
      seen[id] = true;
      for (uint32_t column = 0; column < input.childrenSize(); ++column) {
        EXPECT_EQ(
            SortComparatorOracle::compare(
                *input.childAt(column),
                id,
                *output.childAt(column),
                row,
                compareFlags),
            0)
            << "row=" << row << ", id=" << id << ", column=" << column;
      }
    }
    EXPECT_TRUE(std::all_of(
        seen.begin(), seen.end(), [](bool value) { return value; }));
  }

  static void expectSortedByOutput(
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
};

TEST_F(RadixSortRunTest, directProjectionMultipleKeysAndBatchSizes) {
  constexpr vector_size_t kRows = 257;
  std::vector<std::optional<int64_t>> firstKey(kRows);
  std::vector<std::optional<int32_t>> secondKey(kRows);
  std::vector<std::optional<std::string>> payload(kRows);
  std::vector<std::optional<int64_t>> ids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    if (row % 11 != 0) {
      firstKey[row] = (row * 37) % 97;
    }
    if (row % 13 != 0) {
      secondKey[row] = (row * 17) % 53;
    }
    payload[row] = row % 5 == 0
        ? std::string(80, static_cast<char>('a' + row % 20))
        : "value-" + std::to_string(row);
    ids[row] = row;
  }
  auto input = makeRows(
      runPool_.get(),
      {"first", "payload", "second", "id"},
      {makeVector<int64_t>(runPool_.get(), BIGINT(), firstKey),
       makeStringVector(runPool_.get(), VARCHAR(), payload),
       makeVector<int32_t>(runPool_.get(), INTEGER(), secondKey),
       makeVector<int64_t>(runPool_.get(), BIGINT(), ids)});
  const std::vector<CompareFlags> keyFlags{
      flags(true, false), flags(false, true)};

  for (const auto outputBatchSize : {1, 17, 2048}) {
    auto run = createRun(
        rowTypeOf(*input),
        ROW({"first", "second"}, {BIGINT(), INTEGER()}),
        keyFlags,
        {0, 2});
    run->append(*slice(*input, 0, 73));
    run->append(*slice(*input, 73, 91));
    run->append(*slice(*input, 164, kRows - 164));
    EXPECT_EQ(run->state(), RadixSortRunState::kBuilding);
    run->finalize();
    EXPECT_EQ(run->state(), RadixSortRunState::kSortedInMemory);

    auto output = collect(*run, outputBatchSize);
    EXPECT_EQ(run->state(), RadixSortRunState::kConsumed);
    EXPECT_EQ(run->metrics().inputRows, kRows);
    EXPECT_EQ(run->metrics().outputRows, kRows);
    expectRowsMatchById(*input, *output, 3);
    expectSortedByOutput(*output, {0, 2}, keyFlags);
    ASSERT_EQ(run->projection().columns().size(), 4);
    EXPECT_EQ(
        run->projection().columns()[0].source,
        RadixSortOutputSource::kDecodedKey);
    EXPECT_EQ(
        run->projection().columns()[1].source, RadixSortOutputSource::kPayload);
    EXPECT_EQ(
        run->projection().columns()[2].source,
        RadixSortOutputSource::kDecodedKey);
  }
}

TEST_F(RadixSortRunTest, keyOnlyEmptyAndOneRow) {
  auto rowType = ROW({"key"}, {BIGINT()});
  {
    auto run = createRun(rowType, rowType, {flags(true, true)}, {0});
    EXPECT_FALSE(run->projection().hasPayload());
    EXPECT_EQ(run->storage()->payloadLayout(), nullptr);
    run->finalize();
    auto output = run->getOutput(10, outputPool_.get());
    EXPECT_EQ(output, nullptr);
    EXPECT_EQ(run->state(), RadixSortRunState::kConsumed);
  }

  auto input = makeRows(
      runPool_.get(),
      {"key"},
      {makeVector<int64_t>(runPool_.get(), BIGINT(), {42})});
  auto run = createRun(rowType, rowType, {flags(true, true)}, {0});
  run->append(*input);
  EXPECT_EQ(run->storage()->payloadSize(), 0);
  run->finalize();
  auto output = collect(*run, 1);
  ASSERT_EQ(output->size(), 1);
  EXPECT_EQ(
      output->childAt(0)->asUnchecked<SimpleVector<int64_t>>()->valueAt(0), 42);
}

TEST_F(RadixSortRunTest, appendUpdatesKeyNullabilityStats) {
  auto input = makeRows(
      runPool_.get(),
      {"key", "id"},
      {makeVector<int64_t>(runPool_.get(), BIGINT(), {2, std::nullopt, 1, 3}),
       makeVector<int64_t>(runPool_.get(), BIGINT(), {0, 1, 2, 3})});
  auto run = createRun(
      rowTypeOf(*input),
      ROW({"key"}, {BIGINT()}),
      {flags(true, false)},
      {0},
      {});
  run->append(*input);
  EXPECT_EQ(run->keyMayHaveNulls(), (std::vector<uint8_t>{1}));
  run->finalize();

  auto output = collect(*run, 2);
  expectRowsMatchById(*input, *output, 1);
  expectSortedByOutput(*output, {0}, {flags(true, false)});
}

TEST_F(RadixSortRunTest, duplicateDirectKeyStaysInPayload) {
  constexpr vector_size_t kRows = 64;
  std::vector<std::optional<int64_t>> keys(kRows);
  std::vector<std::optional<int64_t>> ids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    keys[row] = row % 7;
    ids[row] = row;
  }
  auto input = makeRows(
      runPool_.get(),
      {"key", "id"},
      {makeVector<int64_t>(runPool_.get(), BIGINT(), keys),
       makeVector<int64_t>(runPool_.get(), BIGINT(), ids)});
  auto run = createRun(
      rowTypeOf(*input),
      ROW({"key0", "key1"}, {BIGINT(), BIGINT()}),
      {flags(true, true), flags(true, true)},
      {0, 0});
  EXPECT_EQ(
      run->projection().columns()[0].source, RadixSortOutputSource::kPayload);
  run->append(*input);
  run->finalize();
  auto output = collect(*run, 9);
  expectRowsMatchById(*input, *output, 1);
  expectSortedByOutput(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortRunTest, bitExactFloatSpecialValues) {
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
  values.reserve(bits.size());
  ids.reserve(bits.size());
  for (uint64_t index = 0; index < bits.size(); ++index) {
    values.push_back(std::bit_cast<double>(bits[index]));
    ids.push_back(index);
  }
  auto input = makeRows(
      runPool_.get(),
      {"value", "id"},
      {makeVector<double>(runPool_.get(), DOUBLE(), values),
       makeVector<int64_t>(runPool_.get(), BIGINT(), ids)});
  auto run = createRun(
      rowTypeOf(*input),
      ROW({"value"}, {DOUBLE()}),
      {flags(true, true)},
      {0},
      {true, false});
  EXPECT_EQ(
      run->projection().columns()[0].source, RadixSortOutputSource::kPayload);
  run->append(*input);
  run->finalize();
  auto output = collect(*run, 3);
  for (vector_size_t row = 0; row < output->size(); ++row) {
    const auto id = idAt(*output, row, 1);
    const auto value =
        output->childAt(0)->asUnchecked<SimpleVector<double>>()->valueAt(row);
    EXPECT_EQ(std::bit_cast<uint64_t>(value), bits[id]);
  }
  expectSortedByOutput(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortRunTest, selectiveDecodeKeepsBitExactPayloadKey) {
  const std::vector<uint64_t> bits{
      0x0000000000000000ULL,
      0x8000000000000000ULL,
      0x7ff8000000000001ULL,
      0x7ff8000000000011ULL,
      0x7ff0000000000000ULL,
      0xfff0000000000000ULL,
      0x3ff0000000000000ULL,
      0xbff0000000000000ULL};
  std::vector<std::optional<int64_t>> groups;
  std::vector<std::optional<double>> values;
  std::vector<std::optional<int64_t>> ids;
  groups.reserve(bits.size());
  values.reserve(bits.size());
  ids.reserve(bits.size());
  for (uint64_t index = 0; index < bits.size(); ++index) {
    groups.push_back(index % 2);
    values.push_back(std::bit_cast<double>(bits[index]));
    ids.push_back(index);
  }
  auto input = makeRows(
      runPool_.get(),
      {"group", "value", "id"},
      {makeVector<int64_t>(runPool_.get(), BIGINT(), groups),
       makeVector<double>(runPool_.get(), DOUBLE(), values),
       makeVector<int64_t>(runPool_.get(), BIGINT(), ids)});
  const std::vector<CompareFlags> keyFlags{
      flags(true, true), flags(true, true)};
  auto run = createRun(
      rowTypeOf(*input),
      ROW({"group", "value"}, {BIGINT(), DOUBLE()}),
      keyFlags,
      {0, 1},
      {false, true, false});
  EXPECT_EQ(run->projection().decodedKeyMask(), (std::vector<uint8_t>{1, 0}));
  EXPECT_EQ(
      run->projection().columns()[0].source,
      RadixSortOutputSource::kDecodedKey);
  EXPECT_EQ(
      run->projection().columns()[1].source, RadixSortOutputSource::kPayload);

  run->append(*input);
  run->finalize();
  auto output = collect(*run, 3);
  for (vector_size_t row = 0; row < output->size(); ++row) {
    const auto id = idAt(*output, row, 2);
    const auto value =
        output->childAt(1)->asUnchecked<SimpleVector<double>>()->valueAt(row);
    EXPECT_EQ(std::bit_cast<uint64_t>(value), bits[id]);
  }
  expectSortedByOutput(*output, {0, 1}, keyFlags);
}

TEST_F(RadixSortRunTest, timestampNanosAndInvalidUtf8Keys) {
  const std::vector<std::optional<Timestamp>> timestamps{
      Timestamp(0, 1),
      Timestamp(-1, 999999999),
      Timestamp(0, 0),
      Timestamp(123, 456789123),
      std::nullopt};
  const std::vector<std::optional<std::string>> strings{
      std::string("\xc3\x28\xff", 3),
      std::string("\x00\xff", 2),
      std::string(40, 'z'),
      std::string(),
      std::string("null-ts")};
  std::vector<std::optional<int64_t>> ids(timestamps.size());
  for (uint64_t row = 0; row < ids.size(); ++row) {
    ids[row] = row;
  }
  auto input = makeRows(
      runPool_.get(),
      {"timestamp", "string", "id"},
      {makeVector<Timestamp>(runPool_.get(), TIMESTAMP(), timestamps),
       makeStringVector(runPool_.get(), VARCHAR(), strings),
       makeVector<int64_t>(runPool_.get(), BIGINT(), ids)});
  const std::vector<CompareFlags> keyFlags{
      flags(true, false), flags(false, true)};
  auto run = createRun(
      rowTypeOf(*input),
      ROW({"timestamp", "string"}, {TIMESTAMP(), VARCHAR()}),
      keyFlags,
      {0, 1});
  run->append(*input);
  run->finalize();
  auto output = collect(*run, 2);
  expectRowsMatchById(*input, *output, 2);
  expectSortedByOutput(*output, {0, 1}, keyFlags);
}

TEST_F(RadixSortRunTest, complexDirectKeysRoundTripAndSort) {
  auto arrays = makeIntegerArrays();
  auto rows = makeNestedRows();
  auto maps = makeIntegerStringMaps();
  auto input = makeRows(
      runPool_.get(),
      {"array", "row", "map", "id"},
      {arrays,
       rows,
       maps,
       makeVector<int64_t>(runPool_.get(), BIGINT(), {0, 1, 2, 3, 4, 5, 6})});
  const std::vector<CompareFlags> keyFlags{
      flags(true, false), flags(false, true), flags(true, true)};
  auto run = createRun(
      rowTypeOf(*input),
      ROW({"array", "row", "map"},
          {arrays->type(), rows->type(), maps->type()}),
      keyFlags,
      {0, 1, 2});
  EXPECT_EQ(
      run->projection().payloadChannels(), (std::vector<column_index_t>{3}));
  run->append(*input);
  run->finalize();
  auto output = collect(*run, 2);
  expectRowsMatchById(*input, *output, 3);
  expectSortedByOutput(*output, {0, 1, 2}, keyFlags);
}

TEST_F(RadixSortRunTest, complexPayloadRoundTripAndSort) {
  auto arrays = makeIntegerArrays();
  auto rows = makeNestedRows();
  auto maps = makeIntegerStringMaps();
  auto keys =
      makeVector<int64_t>(runPool_.get(), BIGINT(), {6, 5, 4, 3, 2, 1, 0});
  auto ids =
      makeVector<int64_t>(runPool_.get(), BIGINT(), {0, 1, 2, 3, 4, 5, 6});
  auto input = makeRows(
      runPool_.get(),
      {"array", "row", "map", "key", "id"},
      {arrays, rows, maps, keys, ids});
  auto run = createRun(
      rowTypeOf(*input), ROW({"key"}, {BIGINT()}), {flags(true, true)}, {3});
  EXPECT_EQ(
      run->projection().payloadChannels(),
      (std::vector<column_index_t>{0, 1, 2, 4}));
  run->append(*input);
  run->finalize();
  auto output = collect(*run, 2);
  expectRowsMatchById(*input, *output, 4);
  expectSortedByOutput(*output, {3}, {flags(true, true)});
}

TEST_F(RadixSortRunTest, eventKeyMapPayloadMultipleBatches) {
  constexpr vector_size_t kRows = 64;
  std::vector<std::optional<std::string>> events;
  std::vector<std::optional<int64_t>> ids;
  std::vector<std::optional<std::string>> enterFrom;
  std::vector<std::optional<std::string>> hours;
  events.reserve(kRows);
  ids.reserve(kRows);
  enterFrom.reserve(kRows);
  hours.reserve(kRows);
  static constexpr std::array<const char*, 6> kEvents{
      "video_play", "video_play_pause", "like", "follow", "share", "comment"};
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto eventIndex = row % 10 < 6 ? row % 3 : row % kEvents.size();
    events.push_back(kEvents[eventIndex]);
    ids.push_back(row);
    enterFrom.push_back(
        row % 4 == 0
            ? std::optional<std::string>{}
            : std::optional<std::string>("enter_" + std::to_string(row % 7)));
    hours.push_back((row % 24 < 10 ? "0" : "") + std::to_string(row % 24));
  }
  auto params = makeStringStringMaps(kRows);
  auto input = makeRows(
      runPool_.get(),
      {"event", "id", "enter_from", "params", "hour"},
      {makeStringVector(runPool_.get(), VARCHAR(), events),
       makeVector<int64_t>(runPool_.get(), BIGINT(), ids),
       makeStringVector(runPool_.get(), VARCHAR(), enterFrom),
       params,
       makeStringVector(runPool_.get(), VARCHAR(), hours)});

  auto run = createRun(
      rowTypeOf(*input), ROW({"event"}, {VARCHAR()}), {flags(true, true)}, {0});
  EXPECT_EQ(
      run->projection().payloadChannels(),
      (std::vector<column_index_t>{1, 2, 3, 4}));
  run->append(*slice(*input, 0, 17));
  run->append(*slice(*input, 17, 23));
  run->append(*slice(*input, 40, kRows - 40));
  run->finalize();

  auto output = collect(*run, 13);
  expectRowsMatchById(*input, *output, 1);
  expectSortedByOutput(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortRunTest, mapPayloadOutputOwnsDataAfterRunClear) {
  constexpr vector_size_t kRows = 48;
  auto sourcePool = rootPool_->addLeafChild("radix-sort-run-map-source-test");
  std::vector<std::optional<std::string>> events;
  std::vector<std::optional<int64_t>> ids;
  events.reserve(kRows);
  ids.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    events.push_back("event_" + std::to_string(row % 5));
    ids.push_back(row);
  }
  auto input = makeRows(
      sourcePool.get(),
      {"event", "params", "id"},
      {makeStringVector(sourcePool.get(), VARCHAR(), events),
       makeStringStringMaps(sourcePool.get(), kRows),
       makeVector<int64_t>(sourcePool.get(), BIGINT(), ids)});
  auto run = createRun(
      rowTypeOf(*input), ROW({"event"}, {VARCHAR()}), {flags(true, true)}, {0});
  run->append(*input);
  run->finalize();

  auto output = collect(*run, 11);
  EXPECT_EQ(run->state(), RadixSortRunState::kConsumed);
  EXPECT_EQ(run->storage(), nullptr);
  EXPECT_EQ(runPool_->currentBytes(), 0);
  expectRowsMatchById(*input, *output, 2);
  expectSortedByOutput(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortRunTest, directKeyOutputDoesNotDuplicatePayloadForEvent) {
  auto input = makeRows(
      runPool_.get(),
      {"event", "id"},
      {makeStringVector(
           runPool_.get(),
           VARCHAR(),
           {"play", "play", "click", "share", "click"}),
       makeVector<int64_t>(runPool_.get(), BIGINT(), {0, 1, 2, 3, 4})});
  auto run = createRun(
      rowTypeOf(*input), ROW({"event"}, {VARCHAR()}), {flags(true, true)}, {0});
  ASSERT_EQ(run->projection().columns().size(), 2);
  EXPECT_EQ(
      run->projection().columns()[0].source,
      RadixSortOutputSource::kDecodedKey);
  EXPECT_EQ(
      run->projection().payloadChannels(), (std::vector<column_index_t>{1}));
  run->append(*input);
  run->finalize();
  auto output = collect(*run, 2);
  expectRowsMatchById(*input, *output, 1);
  expectSortedByOutput(*output, {0}, {flags(true, true)});
}

TEST_F(RadixSortRunTest, nullFreeDecodedKeysAndPayloadResetNullBuffers) {
  constexpr vector_size_t kRows = 12;
  std::vector<std::optional<std::string>> textKeys(kRows);
  std::vector<std::optional<int64_t>> nullableKeys(kRows);
  std::vector<std::optional<int32_t>> fixedKeys(kRows);
  std::vector<std::optional<bool>> boolKeys(kRows);
  std::vector<std::optional<int64_t>> fixedPayload(kRows);
  std::vector<std::optional<bool>> boolPayload(kRows);
  std::vector<std::optional<std::string>> firstStrings(kRows);
  std::vector<std::optional<std::string>> secondStrings(kRows);
  std::vector<std::optional<int64_t>> nullablePayload(kRows);
  std::vector<std::optional<int64_t>> ids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    textKeys[row] = "key-" + std::to_string(static_cast<int32_t>(kRows - row));
    if (row % 5 != 0) {
      nullableKeys[row] = row % 4;
    }
    fixedKeys[row] = row % 3;
    boolKeys[row] = row % 2 == 0;
    fixedPayload[row] = 100 + row;
    boolPayload[row] = row % 3 == 0;
    firstStrings[row] = std::string(40 + row, static_cast<char>('a' + row));
    secondStrings[row] = "payload-" + std::to_string(row);
    if (row % 4 != 1) {
      nullablePayload[row] = 1000 + row;
    }
    ids[row] = row;
  }
  auto arrays = makeNonNullIntegerArrays(kRows);
  auto input = makeRows(
      runPool_.get(),
      {"text_key",
       "nullable_key",
       "fixed_key",
       "bool_key",
       "fixed_payload",
       "bool_payload",
       "first_string",
       "second_string",
       "array_payload",
       "nullable_payload",
       "id"},
      {makeStringVector(runPool_.get(), VARCHAR(), textKeys),
       makeVector<int64_t>(runPool_.get(), BIGINT(), nullableKeys),
       makeVector<int32_t>(runPool_.get(), INTEGER(), fixedKeys),
       makeVector<bool>(runPool_.get(), BOOLEAN(), boolKeys),
       makeVector<int64_t>(runPool_.get(), BIGINT(), fixedPayload),
       makeVector<bool>(runPool_.get(), BOOLEAN(), boolPayload),
       makeStringVector(runPool_.get(), VARCHAR(), firstStrings),
       makeStringVector(runPool_.get(), VARCHAR(), secondStrings),
       arrays,
       makeVector<int64_t>(runPool_.get(), BIGINT(), nullablePayload),
       makeVector<int64_t>(runPool_.get(), BIGINT(), ids)});
  const std::vector<CompareFlags> keyFlags{
      flags(true, true),
      flags(true, true),
      flags(true, true),
      flags(true, true)};
  auto run = createRun(
      rowTypeOf(*input),
      ROW({"text_key", "nullable_key", "fixed_key", "bool_key"},
          {VARCHAR(), BIGINT(), INTEGER(), BOOLEAN()}),
      keyFlags,
      {0, 1, 2, 3});
  run->append(*input);
  EXPECT_EQ(run->keyMayHaveNulls(), (std::vector<uint8_t>{0, 1, 0, 0}));
  EXPECT_EQ(
      run->payloadMayHaveNulls(), (std::vector<uint8_t>{0, 0, 0, 0, 0, 1, 0}));
  run->finalize();

  auto first = run->getOutput(4, outputPool_.get());
  ASSERT_NE(first, nullptr);
  std::vector<VectorPtr> children;
  children.reserve(input->childrenSize());
  for (const auto& type : input->type()->as<TypeKind::ROW>().children()) {
    children.push_back(BaseVector::create(type, kRows, outputPool_.get()));
  }
  for (uint32_t column = 0; column < children.size(); ++column) {
    children[column]->copy(first->childAt(column).get(), 0, 0, first->size());
  }
  vector_size_t offset = first->size();
  for (const auto column : {0, 2, 3, 4, 5, 6, 7, 8, 10}) {
    ASSERT_EQ(first->childAt(column)->rawNulls(), nullptr) << column;
    first->childAt(column)->mutableRawNulls();
    ASSERT_NE(first->childAt(column)->rawNulls(), nullptr) << column;
  }
  first.reset();

  auto second = run->getOutput(4, outputPool_.get());
  ASSERT_NE(second, nullptr);
  for (const auto column : {0, 2, 3, 4, 5, 6, 7, 8, 10}) {
    EXPECT_EQ(second->childAt(column)->rawNulls(), nullptr) << column;
    EXPECT_FALSE(second->childAt(column)->mayHaveNullsRecursive()) << column;
  }

  for (uint32_t column = 0; column < children.size(); ++column) {
    children[column]->copy(
        second->childAt(column).get(), offset, 0, second->size());
  }
  offset += second->size();
  while (auto batch = run->getOutput(4, outputPool_.get())) {
    for (uint32_t column = 0; column < children.size(); ++column) {
      children[column]->copy(
          batch->childAt(column).get(), offset, 0, batch->size());
    }
    offset += batch->size();
  }
  ASSERT_EQ(offset, kRows);
  auto output = std::make_shared<RowVector>(
      outputPool_.get(), input->type(), nullptr, kRows, std::move(children));
  expectRowsMatchById(*input, *output, 10);
  expectSortedByOutput(*output, {0, 1, 2, 3}, keyFlags);
}

TEST_F(RadixSortRunTest, singleStringPayloadNullFreeReuseResetsNullBuffer) {
  constexpr vector_size_t kRows = 8;
  std::vector<std::optional<int64_t>> keys(kRows);
  std::vector<std::optional<std::string>> strings(kRows);
  std::vector<std::optional<int64_t>> ids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    keys[row] = kRows - row;
    strings[row] = std::string(64 + row, static_cast<char>('a' + row));
    ids[row] = row;
  }
  auto input = makeRows(
      runPool_.get(),
      {"key", "string", "id"},
      {makeVector<int64_t>(runPool_.get(), BIGINT(), keys),
       makeStringVector(runPool_.get(), VARCHAR(), strings),
       makeVector<int64_t>(runPool_.get(), BIGINT(), ids)});
  auto run = createRun(
      rowTypeOf(*input), ROW({"key"}, {BIGINT()}), {flags(true, true)}, {0});
  run->append(*input);
  EXPECT_EQ(run->payloadMayHaveNulls(), (std::vector<uint8_t>{0, 0}));
  run->finalize();

  auto first = run->getOutput(3, outputPool_.get());
  ASSERT_NE(first, nullptr);
  ASSERT_EQ(first->childAt(1)->rawNulls(), nullptr);
  first->childAt(1)->mutableRawNulls();
  ASSERT_NE(first->childAt(1)->rawNulls(), nullptr);
  first.reset();

  auto second = run->getOutput(3, outputPool_.get());
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(second->childAt(1)->rawNulls(), nullptr);
  EXPECT_FALSE(second->childAt(1)->mayHaveNulls());
}

TEST_F(RadixSortRunTest, runOwnedAllocationPoolCoversPersistentData) {
  constexpr vector_size_t kRows = 7;
  std::vector<std::optional<std::string>> keys;
  std::vector<std::optional<std::string>> strings;
  std::vector<std::optional<int64_t>> ids;
  keys.reserve(kRows);
  strings.reserve(kRows);
  ids.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    keys.emplace_back(std::string(96 + row, static_cast<char>('a' + row)));
    strings.emplace_back(std::string(80 + row, static_cast<char>('k' + row)));
    ids.emplace_back(row);
  }
  auto arrays = makeIntegerArrays();
  auto input = makeRows(
      runPool_.get(),
      {"key", "string", "array", "id"},
      {makeStringVector(runPool_.get(), VARCHAR(), keys),
       makeStringVector(runPool_.get(), VARCHAR(), strings),
       arrays,
       makeVector<int64_t>(runPool_.get(), BIGINT(), ids)});
  auto run = createRun(
      rowTypeOf(*input), ROW({"key"}, {VARCHAR()}), {flags(true, true)}, {0});
  run->append(*input);

  const auto* arena = run->storage();
  ASSERT_NE(arena, nullptr);
  EXPECT_FALSE(arena->keyBlocks().empty());
  EXPECT_FALSE(arena->keyHeapGroups().empty());
  EXPECT_FALSE(arena->payloadFixedBlocks().empty());
  EXPECT_FALSE(arena->payloadHeapGroups().empty());
  ASSERT_NE(run->payloadLayout(), nullptr);
  ASSERT_EQ(run->payloadLayout()->columns().size(), 3);

  const auto* keyBlockBegin = arena->keyBlocks()[0].base;
  const auto* keyBlockEnd =
      keyBlockBegin + arena->keyBlocks()[0].count * arena->layout().width();
  for (vector_size_t row = 0; row < kRows; ++row) {
    EXPECT_GE(arena->keyDataAt(row), keyBlockBegin);
    EXPECT_LT(arena->keyDataAt(row), keyBlockEnd);
    const auto key = arena->keyAt(row);
    ASSERT_NE(key.fullKeyData(), nullptr);

    const auto* payload = key.payload();
    ASSERT_NE(payload, nullptr);

    const auto longString = loadUnaligned<StringView>(
        payload + run->payloadLayout()->columns()[0].offset);
    ASSERT_FALSE(longString.isInline());
    EXPECT_NE(longString.data(), nullptr);

    if (row != kRows - 1) {
      const auto complex = loadUnaligned<PayloadVarlenRef>(
          payload + run->payloadLayout()->columns()[1].offset);
      ASSERT_GT(complex.size, 0);
      ASSERT_NE(complex.data, nullptr);
    }
  }
}

TEST_F(RadixSortRunTest, outputDoesNotMutateOrReferenceRun) {
  constexpr vector_size_t kRows = 40;
  auto sourcePool = rootPool_->addLeafChild("radix-sort-run-source-test");
  std::vector<std::optional<int64_t>> keys(kRows);
  std::vector<std::optional<std::string>> strings(kRows);
  std::vector<std::optional<int64_t>> ids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    keys[row] = kRows - row;
    strings[row] = std::string(80, static_cast<char>('a' + row % 20));
    ids[row] = row;
  }
  auto input = makeRows(
      sourcePool.get(),
      {"key", "string", "id"},
      {makeVector<int64_t>(sourcePool.get(), BIGINT(), keys),
       makeStringVector(sourcePool.get(), VARCHAR(), strings),
       makeVector<int64_t>(sourcePool.get(), BIGINT(), ids)});
  auto run = createRun(
      rowTypeOf(*input), ROW({"key"}, {BIGINT()}), {flags(true, true)}, {0});
  run->append(*input);
  run->finalize();

  const auto width = run->storage()->layout().width();
  std::vector<RadixSortInlineKeyBuffer> records(run->size());
  std::vector<char*> payloadPointers(run->size());
  for (uint64_t row = 0; row < run->size(); ++row) {
    std::memcpy(records[row].data(), run->storage()->keyDataAt(row), width);
    payloadPointers[row] = run->storage()->keyAt(row).payload();
  }

  auto firstBatch = run->getOutput(7, outputPool_.get());
  ASSERT_NE(firstBatch, nullptr);
  EXPECT_EQ(run->state(), RadixSortRunState::kSortedInMemory);
  for (uint64_t row = 0; row < run->size(); ++row) {
    EXPECT_EQ(
        std::memcmp(records[row].data(), run->storage()->keyDataAt(row), width),
        0);
    EXPECT_EQ(run->storage()->keyAt(row).payload(), payloadPointers[row]);
  }

  uint64_t drainedRows = firstBatch->size();
  while (true) {
    auto batch = run->getOutput(9, outputPool_.get());
    if (batch == nullptr) {
      break;
    }
    drainedRows += batch->size();
  }
  EXPECT_EQ(run->state(), RadixSortRunState::kConsumed);
  EXPECT_EQ(run->storage(), nullptr);
  EXPECT_EQ(run->retainedBytes(), 0);
  EXPECT_EQ(runPool_->currentBytes(), 0);
  EXPECT_EQ(drainedRows, kRows);
  ASSERT_GT(firstBatch->size(), 0);
  EXPECT_EQ(
      firstBatch->childAt(1)
          ->asUnchecked<SimpleVector<StringView>>()
          ->valueAt(0)
          .size(),
      80);
}

TEST_F(RadixSortRunTest, outputReusesBuffersWithCopyOnWrite) {
  constexpr vector_size_t kRows = 16;
  std::vector<std::optional<int64_t>> keys(kRows);
  std::vector<std::optional<std::string>> strings(kRows);
  std::vector<std::optional<int64_t>> ids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    keys[row] = kRows - row;
    strings[row] = std::string(48, static_cast<char>('a' + row));
    ids[row] = row;
  }
  auto input = makeRows(
      runPool_.get(),
      {"key", "string", "id"},
      {makeVector<int64_t>(runPool_.get(), BIGINT(), keys),
       makeStringVector(runPool_.get(), VARCHAR(), strings),
       makeVector<int64_t>(runPool_.get(), BIGINT(), ids)});
  auto run = createRun(
      rowTypeOf(*input), ROW({"key"}, {BIGINT()}), {flags(true, true)}, {0});
  run->append(*input);
  run->finalize();

  auto first = run->getOutput(4, outputPool_.get());
  ASSERT_NE(first, nullptr);
  auto* firstKey = first->childAt(0)->asUnchecked<FlatVector<int64_t>>();
  auto* firstString = first->childAt(1)->asUnchecked<FlatVector<StringView>>();
  const auto* keyValues = firstKey->values().get();
  const auto* stringValues = firstString->values().get();
  ASSERT_EQ(firstString->stringBuffers().size(), 1);
  const auto* stringBuffer = firstString->stringBuffers().front().get();
  first.reset();

  auto second = run->getOutput(4, outputPool_.get());
  ASSERT_NE(second, nullptr);
  auto* secondKey = second->childAt(0)->asUnchecked<FlatVector<int64_t>>();
  auto* secondString =
      second->childAt(1)->asUnchecked<FlatVector<StringView>>();
  EXPECT_EQ(secondKey->values().get(), keyValues);
  EXPECT_EQ(secondString->values().get(), stringValues);
  ASSERT_EQ(secondString->stringBuffers().size(), 1);
  EXPECT_EQ(secondString->stringBuffers().front().get(), stringBuffer);

  const auto retainedKey = secondKey->valueAt(0);
  const auto retainedString = secondString->valueAt(0).getString();
  const auto* secondKeyValues = secondKey->values().get();
  const auto* secondStringValues = secondString->values().get();
  const auto* secondStringBuffer = secondString->stringBuffers().front().get();

  auto third = run->getOutput(4, outputPool_.get());
  ASSERT_NE(third, nullptr);
  auto* thirdKey = third->childAt(0)->asUnchecked<FlatVector<int64_t>>();
  auto* thirdString = third->childAt(1)->asUnchecked<FlatVector<StringView>>();
  EXPECT_NE(thirdKey->values().get(), secondKeyValues);
  EXPECT_NE(thirdString->values().get(), secondStringValues);
  ASSERT_EQ(thirdString->stringBuffers().size(), 1);
  EXPECT_NE(thirdString->stringBuffers().front().get(), secondStringBuffer);
  EXPECT_EQ(secondKey->valueAt(0), retainedKey);
  EXPECT_EQ(secondString->valueAt(0).getString(), retainedString);
}

TEST_F(RadixSortRunTest, stateAndCapabilityValidation) {
  auto rowType = ROW({"key"}, {BIGINT()});
  auto input = makeRows(
      runPool_.get(),
      {"key"},
      {makeVector<int64_t>(runPool_.get(), BIGINT(), {1, 2})});
  auto run = createRun(rowType, rowType, {flags(true, true)}, {0});
  RowVectorPtr output;
  EXPECT_THROW(run->getOutput(1, outputPool_.get()), BoltException);
  run->append(*input);
  run->finalize();
  EXPECT_THROW(run->append(*input), BoltException);
  EXPECT_THROW(run->finalize(), BoltException);
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
