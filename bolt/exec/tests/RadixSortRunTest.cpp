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
#include "bolt/exec/radixsort/RadixSortSpill.h"
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
  static constexpr std::array<uint64_t, 8> kDoubleBits{
      0x0000000000000000ULL,
      0x8000000000000000ULL,
      0x7ff8000000000001ULL,
      0x7ff8000000000011ULL,
      0x7ff0000000000000ULL,
      0xfff0000000000000ULL,
      0x3ff0000000000000ULL,
      0xbff0000000000000ULL};
  FlatVectorPtr<double> specialDoubles() {
    return makeVector<double>(
        DOUBLE(), makeValues<double>(kDoubleBits.size(), [](vector_size_t row) {
          return std::bit_cast<double>(kDoubleBits[row]);
        }));
  }

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> runPool_{
      rootPool_->addLeafChild("radix-sort-run-test")};
  std::shared_ptr<memory::MemoryPool> outputPool_{
      rootPool_->addLeafChild("radix-sort-run-output-test")};

  template <typename T, typename Input = T>
  FlatVectorPtr<T> makeVector(
      const TypePtr& type,
      const std::vector<std::optional<Input>>& values,
      memory::MemoryPool* pool = nullptr) {
    pool = pool == nullptr ? runPool_.get() : pool;
    auto vector = BaseVector::create<FlatVector<T>>(type, values.size(), pool);
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        vector->set(row, T(*values[row]));
      } else {
        vector->setNull(row, true);
      }
    }
    return vector;
  }

  template <typename T, typename F>
  static std::vector<std::optional<T>> makeValues(
      vector_size_t size,
      F valueAt) {
    std::vector<std::optional<T>> values;
    values.reserve(size);
    for (vector_size_t row = 0; row < size; ++row) {
      values.push_back(valueAt(row));
    }
    return values;
  }

  FlatVectorPtr<StringView> makeStringVector(
      const std::vector<std::optional<std::string>>& values,
      memory::MemoryPool* pool = nullptr) {
    return makeVector<StringView, std::string>(VARCHAR(), values, pool);
  }

  FlatVectorPtr<int64_t> makeIds(
      vector_size_t size,
      memory::MemoryPool* pool = nullptr) {
    return makeVector<int64_t>(
        BIGINT(),
        makeValues<int64_t>(size, [](vector_size_t row) { return row; }),
        pool);
  }

  template <typename Container>
  BufferPtr makeBuffer(
      const Container& values,
      memory::MemoryPool* pool = nullptr) {
    pool = pool == nullptr ? runPool_.get() : pool;
    using T = typename Container::value_type;
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
        makeBuffer(offsets),
        makeBuffer(sizes),
        makeVector<int32_t>(INTEGER(), elements));
  }

  ArrayVectorPtr makeIntegerArrays() {
    const std::array<vector_size_t, 7> offsets{0, 0, 1, 3, 5, 7, 8};
    const std::array<vector_size_t, 7> sizes{0, 1, 2, 2, 2, 1, 0};
    auto arrays = std::make_shared<ArrayVector>(
        runPool_.get(),
        ARRAY(INTEGER()),
        nullptr,
        offsets.size(),
        makeBuffer(offsets),
        makeBuffer(sizes),
        makeVector<int32_t>(INTEGER(), {1, 1, 2, 1, 3, 1, std::nullopt, 2}));
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
            makeVector<int32_t>(INTEGER(), {1, 1, 2, 1, 1, std::nullopt, 1}),
            makeStringVector(
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
    auto values = makeStringVector(
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
        makeBuffer(offsets),
        makeBuffer(sizes),
        makeVector<int32_t>(INTEGER(), {1, 2, 1, 1, 2, 2, 1, 1}),
        values);
    maps->setNull(6, true);
    return maps;
  }

  MapVectorPtr makeStringStringMaps(
      vector_size_t rows,
      memory::MemoryPool* pool = nullptr) {
    pool = pool == nullptr ? runPool_.get() : pool;
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
        makeBuffer(offsets, pool),
        makeBuffer(sizes, pool),
        makeStringVector(keys, pool),
        makeStringVector(values, pool));
    if (rows > 9) {
      maps->setNull(9, true);
    }
    return maps;
  }

  RowVectorPtr makeRows(
      std::vector<std::string> names,
      const std::vector<VectorPtr>& children,
      memory::MemoryPool* pool = nullptr) {
    pool = pool == nullptr ? runPool_.get() : pool;
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

  RowVectorPtr makeKeyStringRows(
      const std::vector<std::optional<int64_t>>& keys,
      const std::vector<std::optional<std::string>>& strings,
      std::string stringName = "string",
      memory::MemoryPool* pool = nullptr) {
    pool = pool == nullptr ? runPool_.get() : pool;
    return makeRows(
        {"key", std::move(stringName), "id"},
        {makeVector<int64_t>(BIGINT(), keys, pool),
         makeStringVector(strings, pool),
         makeIds(keys.size(), pool)},
        pool);
  }

  RowVectorPtr makeDescendingKeyStringRows(
      vector_size_t size,
      uint32_t stringSize,
      memory::MemoryPool* pool = nullptr) {
    return makeKeyStringRows(
        makeValues<int64_t>(
            size, [size](vector_size_t row) { return size - row; }),
        makeValues<std::string>(
            size,
            [stringSize](vector_size_t row) {
              return std::string(stringSize, static_cast<char>('a' + row % 20));
            }),
        "string",
        pool);
  }

  static RowTypePtr rowTypeOf(const RowVector& rows) {
    return std::static_pointer_cast<const RowType>(rows.type());
  }
  RowVectorPtr
  slice(const RowVector& input, vector_size_t offset, vector_size_t count) {
    BOLT_CHECK_GE(offset, 0);
    BOLT_CHECK_GE(count, 0);
    BOLT_CHECK_LE(offset, input.size());
    BOLT_CHECK_LE(count, input.size() - offset);
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

  struct RunDescriptor {
    const RowVector& input;
    std::vector<column_index_t> keyChannels;
    std::vector<CompareFlags> keyFlags;
  };
  std::unique_ptr<RadixSortRun> createRun(
      const RunDescriptor& desc,
      RadixSortRunOptions options = {}) {
    const auto inputType = rowTypeOf(desc.input);
    BOLT_CHECK_EQ(desc.keyChannels.size(), desc.keyFlags.size());
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (const auto channel : desc.keyChannels) {
      BOLT_CHECK_LT(channel, inputType->size());
      names.push_back(inputType->nameOf(channel));
      types.push_back(inputType->childAt(channel));
    }
    auto run = RadixSortRun::create(
        runPool_.get(),
        inputType,
        ROW(std::move(names), std::move(types)),
        desc.keyFlags,
        desc.keyChannels,
        std::move(options));
    BOLT_CHECK_NOT_NULL(run);
    return run;
  }

  std::unique_ptr<RadixSortRun> finalizedRun(
      const RunDescriptor& desc,
      RadixSortRunOptions options = {}) {
    auto run = createRun(desc, std::move(options));
    run->append(desc.input);
    run->finalize();
    return run;
  }

  RowVectorPtr finalizeAndCollect(
      const RunDescriptor& desc,
      RadixSortKeyLayoutKind layout) {
    auto run = createRun(desc);
    run->append(desc.input);
    EXPECT_EQ(run->storage()->layout().kind(), layout);
    run->finalize();
    return collect(*run, 2);
  }

  RowVectorPtr collect(RadixSortRun& run, vector_size_t batchSize) {
    BOLT_CHECK_GT(batchSize, 0);
    const auto total = static_cast<vector_size_t>(run.metrics().inputRows);
    std::vector<VectorPtr> children;
    children.reserve(run.projection().outputType()->size());
    for (const auto& type : run.projection().outputType()->children()) {
      children.push_back(BaseVector::create(type, total, outputPool_.get()));
    }

    vector_size_t offset = 0;
    while (auto batch = run.getOutput(batchSize, outputPool_.get())) {
      if (offset + batch->size() > total) {
        ADD_FAILURE() << "RadixSortRun returned more rows than it accepted";
        return nullptr;
      }
      copyBatch(*batch, children, offset);
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

  RowVectorPtr collectAndVerify(
      RadixSortRun& run,
      const RowVector& input,
      vector_size_t batchSize,
      column_index_t idChannel,
      const std::vector<column_index_t>& keyChannels =
          std::vector<column_index_t>{0},
      const std::vector<CompareFlags>& keyFlags = std::vector<CompareFlags>{
          SortComparatorOracle::makeSortFlags(true, true)}) {
    auto output = collect(run, batchSize);
    SortComparatorOracle::expectRowsMatchById(input, *output, idChannel);
    SortComparatorOracle::expectSorted(*output, keyChannels, keyFlags);
    return output;
  }

  static void copyBatch(
      const RowVector& batch,
      std::vector<VectorPtr>& children,
      vector_size_t offset) {
    for (uint32_t column = 0; column < children.size(); ++column) {
      children[column]->copy(
          batch.childAt(column).get(), offset, 0, batch.size());
    }
  }

  static int64_t
  idAt(const RowVector& rows, vector_size_t row, column_index_t idChannel) {
    return rows.childAt(idChannel)
        ->asUnchecked<SimpleVector<int64_t>>()
        ->valueAt(row);
  }

  RowVectorPtr concatenate(const std::vector<RowVectorPtr>& batches) {
    BOLT_CHECK(!batches.empty());
    BOLT_CHECK_NOT_NULL(batches.front());
    const auto outputType = rowTypeOf(*batches.front());
    vector_size_t totalRows = 0;
    for (const auto& batch : batches) {
      BOLT_CHECK_NOT_NULL(batch);
      BOLT_CHECK(batch->type()->equivalent(*outputType));
      totalRows += batch->size();
    }
    std::vector<VectorPtr> children;
    children.reserve(outputType->size());
    for (const auto& type : outputType->children()) {
      children.push_back(
          BaseVector::create(type, totalRows, outputPool_.get()));
    }
    vector_size_t offset = 0;
    for (const auto& batch : batches) {
      copyBatch(*batch, children, offset);
      offset += batch->size();
    }
    return std::make_shared<RowVector>(
        outputPool_.get(), outputType, nullptr, totalRows, std::move(children));
  }

  static void expectSortedValues(
      const RowVector& input,
      const RowVector& output,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<CompareFlags>& keyFlags) {
    ASSERT_EQ(output.size(), input.size());
    std::vector<vector_size_t> expectedRows(input.size());
    std::iota(expectedRows.begin(), expectedRows.end(), 0);
    std::sort(
        expectedRows.begin(),
        expectedRows.end(),
        [&](vector_size_t left, vector_size_t right) {
          return SortComparatorOracle::compareRows(
                     input, left, input, right, keyChannels, keyFlags) < 0;
        });
    for (vector_size_t row = 0; row < output.size(); ++row) {
      EXPECT_TRUE(input.equalValueAt(&output, expectedRows[row], row))
          << "row=" << row;
    }
  }

  using OutputBuffers = std::array<const Buffer*, 3>;
  static OutputBuffers outputBuffers(
      const RowVector& output,
      bool includeString = false) {
    BOLT_CHECK_GT(output.childrenSize(), 0);
    BOLT_CHECK_NOT_NULL(output.childAt(0));
    const auto* key = output.childAt(0)->asUnchecked<FlatVector<int64_t>>();
    if (!includeString) {
      return {key->values().get(), nullptr, nullptr};
    }
    BOLT_CHECK_GE(output.childrenSize(), 2);
    BOLT_CHECK_NOT_NULL(output.childAt(1));
    const auto* string =
        output.childAt(1)->asUnchecked<FlatVector<StringView>>();
    BOLT_CHECK_EQ(string->stringBuffers().size(), 1);
    return {
        key->values().get(),
        string->values().get(),
        string->stringBuffers().front().get()};
  }

  void expectCleared(RadixSortRun& run) {
    EXPECT_EQ(run.state(), RadixSortRunState::kConsumed);
    EXPECT_EQ(run.storage(), nullptr);
    EXPECT_EQ(run.retainedBytes(), 0);
    EXPECT_EQ(run.getOutput(1, outputPool_.get()), nullptr);
    std::array<const char*, 1> keys{};
    std::array<char*, 1> payloads{};
    EXPECT_EQ(run.collectRemainingRows(1, keys.data(), payloads.data()), 0);
  }
};

TEST_F(RadixSortRunTest, directProjectionMultipleKeysAndBatchSizes) {
  constexpr vector_size_t kRows = 257;
  std::vector<std::optional<int64_t>> firstKey(kRows);
  std::vector<std::optional<int32_t>> secondKey(kRows);
  std::vector<std::optional<std::string>> payload(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    firstKey[row] =
        row % 11 == 0 ? std::nullopt : std::optional<int64_t>{(row * 37) % 97};
    secondKey[row] = row % 13 == 0
        ? std::nullopt
        : std::optional<int32_t>{static_cast<int32_t>((row * 17) % 53)};
    payload[row] = row % 5 == 0
        ? std::string(80, static_cast<char>('a' + row % 20))
        : "value-" + std::to_string(row);
  }
  auto input = makeRows(
      {"first", "payload", "second", "id"},
      {makeVector<int64_t>(BIGINT(), firstKey),
       makeStringVector(payload),
       makeVector<int32_t>(INTEGER(), secondKey),
       makeIds(kRows)});
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, false),
      SortComparatorOracle::makeSortFlags(false, true)};

  for (const auto outputBatchSize : {1, 17, 2048}) {
    auto run = createRun({*input, {0, 2}, keyFlags});
    run->append(*slice(*input, 0, 73));
    run->append(*slice(*input, 73, 91));
    run->append(*slice(*input, 164, kRows - 164));
    EXPECT_EQ(run->state(), RadixSortRunState::kBuilding);
    run->finalize();
    EXPECT_EQ(run->state(), RadixSortRunState::kSortedInMemory);

    collectAndVerify(*run, *input, outputBatchSize, 3, {0, 2}, keyFlags);
    EXPECT_EQ(run->state(), RadixSortRunState::kConsumed);
    EXPECT_EQ(run->metrics().inputRows, kRows);
    EXPECT_EQ(run->metrics().outputRows, kRows);
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

TEST_F(RadixSortRunTest, variableKeyHeapOffsetSortsAndDecodes) {
  auto input = makeRows(
      {"i", "s", "id"},
      {makeVector<int32_t>(INTEGER(), {3, std::nullopt, 1, 3, 2, std::nullopt}),
       makeStringVector(
           {std::string(),
            std::string(80, 'a'),
            std::string("b"),
            std::string(96, 'a'),
            std::nullopt,
            std::string(40, 'z')}),
       makeIds(6)});
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(false, false)};
  auto run = finalizedRun({*input, {0, 1}, keyFlags});
  ASSERT_TRUE(run->keyLayout().isVariable());
  EXPECT_EQ(run->keyLayout().heapKeyOffset(), 5);
  ASSERT_NE(run->storage(), nullptr);
  for (uint64_t row = 0; row < run->storage()->size(); ++row) {
    const auto key = run->storage()->keyAt(row);
    EXPECT_EQ(key.heapKey().size(), key.heapSize());
  }
  collectAndVerify(*run, *input, 2, 2, {0, 1}, keyFlags);
}

TEST_F(RadixSortRunTest, variableKeyHeapOffsetStopsAtColumnBoundary) {
  auto input = makeRows(
      {"first", "second", "text"},
      {makeVector<int64_t>(BIGINT(), {4, 1, 4, std::nullopt, 2}),
       makeVector<int64_t>(BIGINT(), {3, std::nullopt, 1, 2, 9}),
       makeStringVector(
           {std::string(64, 'd'),
            std::string(80, 'a'),
            std::string(80, 'b'),
            std::string(24, 'c'),
            std::string(96, 'e')})});
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, false),
      SortComparatorOracle::makeSortFlags(true, true)};
  auto run = finalizedRun({*input, {0, 1, 2}, keyFlags});
  ASSERT_TRUE(run->keyLayout().isVariable());
  EXPECT_EQ(run->keyLayout().inlineCapacity(), 18);
  EXPECT_EQ(run->keyLayout().heapKeyOffset(), 18);
  auto output = collect(*run, 2);
  expectSortedValues(*input, *output, {0, 1, 2}, keyFlags);

  auto inputWithPayload = makeRows(
      {"first", "second", "text", "id"},
      {input->childAt(0), input->childAt(1), input->childAt(2), makeIds(5)});
  auto payloadRun = finalizedRun({*inputWithPayload, {0, 1, 2}, keyFlags});
  ASSERT_TRUE(payloadRun->keyLayout().isVariable());
  EXPECT_EQ(payloadRun->keyLayout().inlineCapacity(), 12);
  EXPECT_EQ(payloadRun->keyLayout().heapKeyOffset(), 9);
  collectAndVerify(*payloadRun, *inputWithPayload, 2, 3, {0, 1, 2}, keyFlags);
}

TEST_F(RadixSortRunTest, variableKeyEncodesDirectlyIntoRecordAndHeap) {
  auto input = makeRows(
      {"first", "second", "text"},
      {makeVector<int32_t>(INTEGER(), {1, std::nullopt, 3}),
       makeVector<int32_t>(INTEGER(), {4, 5, std::nullopt}),
       makeStringVector(
           {std::string(), std::string("abc"), std::string(40, 'x')})});
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, true)};

  std::array<EncodedKeyBatch, 3> encodedColumns;
  for (uint32_t column = 0; column < encodedColumns.size(); ++column) {
    std::unique_ptr<RadixSortKeyCodec> codec;
    RadixSortKeyCodec::bind(
        {input->childAt(column)->type()}, {keyFlags[column]}, codec);
    auto columnInput = makeRows({"key"}, {input->childAt(column)});
    codec->encode(*columnInput, runPool_.get(), encodedColumns[column]);
  }
  const auto encodedFixedColumnAt = [&](uint32_t column, vector_size_t row) {
    auto word = encodedColumns[column].fixedKeyAt(row);
    if constexpr (std::endian::native == std::endian::little) {
      word = byteSwap(word);
    }
    std::string encoded(sizeof(word), '\0');
    storeUnaligned(encoded.data(), word);
    encoded.resize(5);
    return encoded;
  };

  auto run = createRun({*input, {0, 1, 2}, keyFlags});
  ASSERT_EQ(
      run->keyLayout().kind(), RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  ASSERT_EQ(run->keyLayout().inlineCapacity(), 18);
  ASSERT_EQ(run->keyLayout().heapKeyOffset(), 10);
  ASSERT_EQ(run->keyLayout().radixWidth(), 18);
  run->append(*slice(*input, 0, 1));
  run->append(*slice(*input, 1, 2));

  const auto* storage = run->storage();
  ASSERT_NE(storage, nullptr);
  uint64_t expectedHeapBytes = 0;
  for (vector_size_t row = 0; row < input->size(); ++row) {
    const auto first = encodedFixedColumnAt(0, row);
    const auto second = encodedFixedColumnAt(1, row);
    const auto suffix = encodedColumns[2].variableKeyAt(row);
    const auto* record = storage->keyDataAt(row);
    const auto key = storage->keyAt(row);

    EXPECT_EQ(std::string_view(record, 5), first);
    EXPECT_EQ(std::string_view(record + 5, 5), second);
    EXPECT_EQ(key.heapKey(), suffix);
    EXPECT_EQ(
        std::string_view(record + 10, std::min<size_t>(suffix.size(), 8)),
        suffix.substr(0, 8));
    if (suffix.size() < 8) {
      EXPECT_EQ(
          std::string_view(record + 10 + suffix.size(), 8 - suffix.size()),
          std::string(8 - suffix.size(), '\0'));
    }
    expectedHeapBytes += suffix.size();
  }
  uint64_t actualHeapBytes = 0;
  for (const auto& group : storage->keyHeapGroups()) {
    actualHeapBytes += group.used;
  }
  EXPECT_EQ(actualHeapBytes, expectedHeapBytes);

  run->finalize();
  auto output = collect(*run, 2);
  expectSortedValues(*input, *output, {0, 1, 2}, keyFlags);
}

TEST_F(RadixSortRunTest, variableKeyHeapOffsetIsZeroWhenFirstKeyIsVariable) {
  auto input = makeRows(
      {"text", "i", "id"},
      {makeStringVector(
           {std::string(64, 'c'), std::string(80, 'a'), std::string(24, 'b')}),
       makeVector<int32_t>(INTEGER(), {3, 1, 2}),
       makeIds(3)});
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, true)};
  auto run = finalizedRun({*input, {0, 1}, keyFlags});
  ASSERT_TRUE(run->keyLayout().isVariable());
  EXPECT_EQ(run->keyLayout().heapKeyOffset(), 0);
  collectAndVerify(*run, *input, 2, 2, {0, 1}, keyFlags);
}

TEST_F(RadixSortRunTest, variableKeyOutputSourceIsSelectedAtFinalize) {
  auto shortInput = makeRows(
      {"text", "id"},
      {makeStringVector(
           {"k3", "k1", std::nullopt, std::string("\0", 1), "", "k0"}),
       makeIds(6)});
  auto shortRun = createRun(
      {*shortInput, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});
  shortRun->append(*slice(*shortInput, 0, 2));
  shortRun->append(*slice(*shortInput, 2, 4));
  EXPECT_EQ(shortRun->keyLayout().inlineCapacity(), 12);
  EXPECT_EQ(shortRun->maximumEncodedKeySize(), 4);
  EXPECT_FALSE(shortRun->decodesVariableKeysFromInline());
  shortRun->finalize();
  ASSERT_TRUE(shortRun->decodesVariableKeysFromInline());
  for (uint64_t row = 0; row < shortRun->storage()->size(); ++row) {
    auto key = shortRun->storage()->keyAt(row);
    std::memset(key.heapKeyData(), 'x', key.heapSize());
  }
  collectAndVerify(*shortRun, *shortInput, 2, 1);

  auto mixedInput = makeRows(
      {"text", "id"},
      {makeStringVector({"k3", std::string(32, 'a'), "k2", "k1", "k0"}),
       makeIds(5)});
  auto mixedRun = createRun(
      {*mixedInput, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});
  mixedRun->append(*slice(*mixedInput, 0, 1));
  mixedRun->append(*slice(*mixedInput, 1, 4));
  EXPECT_EQ(mixedRun->maximumEncodedKeySize(), 34);
  mixedRun->finalize();
  ASSERT_FALSE(mixedRun->decodesVariableKeysFromInline());
  for (uint64_t row = 0; row < mixedRun->storage()->size(); ++row) {
    std::memset(
        const_cast<char*>(mixedRun->storage()->keyDataAt(row)),
        'x',
        mixedRun->keyLayout().inlineCapacity());
  }
  collectAndVerify(*mixedRun, *mixedInput, 2, 1);
}

TEST_F(RadixSortRunTest, inheritedVariableKeySizeSelectsHeapOutput) {
  auto input = makeRows(
      {"text", "id"}, {makeStringVector({"k3", "k1", "k2", "k0"}), makeIds(4)});
  RadixSortRunOptions options;
  options.initialVariableKeysFitRadixPrefix = false;
  auto run = createRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}},
      options);
  run->append(*input);
  EXPECT_FALSE(run->variableKeysFitRadixPrefix());
  run->finalize();
  ASSERT_FALSE(run->decodesVariableKeysFromInline());
  for (uint64_t row = 0; row < run->storage()->size(); ++row) {
    std::memset(
        const_cast<char*>(run->storage()->keyDataAt(row)),
        'x',
        run->keyLayout().inlineCapacity());
  }
  collectAndVerify(*run, *input, 2, 1);
}

TEST_F(RadixSortRunTest, fixedPrefixAndVariableSuffixDecodeFromRecord) {
  auto input = makeRows(
      {"first", "second", "text"},
      {makeVector<int32_t>(INTEGER(), {3, 1, 2, 1}),
       makeVector<int32_t>(INTEGER(), {2, 3, 1, 1}),
       makeStringVector({"c", "a", "b", ""})});
  const std::vector<CompareFlags> keyFlags(
      3, SortComparatorOracle::makeSortFlags(true, true));
  auto run = createRun({*input, {0, 1, 2}, keyFlags});
  run->append(*input);
  run->finalize();
  ASSERT_EQ(run->keyLayout().heapKeyOffset(), 10);
  ASSERT_TRUE(run->decodesVariableKeysFromInline());
  for (uint64_t row = 0; row < run->storage()->size(); ++row) {
    auto key = run->storage()->keyAt(row);
    std::memset(key.heapKeyData(), 'x', key.heapSize());
  }
  auto output = collect(*run, 2);
  expectSortedValues(*input, *output, {0, 1, 2}, keyFlags);
}

TEST_F(RadixSortRunTest, keyLayoutBoundariesKeepSortedOutput) {
  auto ids = makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5});
  auto payload = makeStringVector(
      {"payload_d",
       std::string(80, 'a'),
       std::nullopt,
       std::string(),
       std::string(33, 'z'),
       "payload_b"});
  auto textKeys = makeStringVector(
      {std::string(40, 'd'),
       "a",
       std::nullopt,
       std::string(48, 'c'),
       "b",
       std::string("prefix_") + std::string(40, 'x')});
  std::array<VectorPtr, 6> fixedKeys{
      makeVector<int32_t>(INTEGER(), {4, 1, std::nullopt, 2, 4, -3}),
      makeVector<int32_t>(INTEGER(), {0, 3, 1, std::nullopt, 2, 3}),
      makeVector<int32_t>(INTEGER(), {8, 5, 7, 6, 5, std::nullopt}),
      makeVector<int32_t>(INTEGER(), {9, 6, 8, 7, std::nullopt, 6}),
      makeVector<int32_t>(INTEGER(), {13, 10, 12, 11, 10, 12}),
      makeVector<int32_t>(INTEGER(), {17, 14, std::nullopt, 15, 14, 16})};
  std::array<VectorPtr, 3> variableTies{
      makeVector<int64_t>(BIGINT(), {0, 3, 1, std::nullopt, 2, 3}),
      makeVector<int64_t>(BIGINT(), {8, 5, 7, 6, 5, std::nullopt}),
      makeVector<int64_t>(BIGINT(), {9, 6, 8, 7, std::nullopt, 6})};
  auto tinyKey = makeVector<int8_t>(TINYINT(), {4, 1, std::nullopt, 2, 4, -3});

  struct LayoutCase {
    std::vector<VectorPtr> keys;
    RadixSortKeyLayoutKind withPayloadKind;
    RadixSortKeyLayoutKind keyOnlyKind;
  };
  const std::vector<LayoutCase> cases{
      {{tinyKey},
       RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
       RadixSortKeyLayoutKind::kKeyOnlyFixed8},
      {{fixedKeys.begin(), fixedKeys.begin() + 2},
       RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
       RadixSortKeyLayoutKind::kKeyOnlyFixed16},
      {{fixedKeys.begin(), fixedKeys.begin() + 4},
       RadixSortKeyLayoutKind::kKeyWithPayloadFixed32,
       RadixSortKeyLayoutKind::kKeyOnlyFixed24},
      {{fixedKeys.begin(), fixedKeys.end()},
       RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
       RadixSortKeyLayoutKind::kKeyOnlyFixed32},
      {{textKeys},
       RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
       RadixSortKeyLayoutKind::kKeyOnlyVariable32},
      {{textKeys, variableTies[0]},
       RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
       RadixSortKeyLayoutKind::kKeyOnlyVariable32},
      {{textKeys, variableTies[0], variableTies[1]},
       RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
       RadixSortKeyLayoutKind::kKeyOnlyVariable32},
      {{textKeys, variableTies[0], variableTies[1], variableTies[2]},
       RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
       RadixSortKeyLayoutKind::kKeyOnlyVariable32}};

  for (const auto& testCase : cases) {
    std::vector<std::string> keyNames;
    std::vector<TypePtr> keyTypes;
    for (uint32_t key = 0; key < testCase.keys.size(); ++key) {
      keyNames.push_back("k" + std::to_string(key));
      keyTypes.push_back(testCase.keys[key]->type());
    }
    auto keyType = ROW(std::vector<std::string>(keyNames), std::move(keyTypes));
    std::vector<column_index_t> keyChannels(testCase.keys.size());
    std::iota(keyChannels.begin(), keyChannels.end(), 0);
    keyNames.insert(keyNames.end(), {"payload", "id"});
    auto children = testCase.keys;
    children.insert(children.end(), {payload, ids});
    auto input = makeRows(std::move(keyNames), children);

    SCOPED_TRACE(keyType->toString());
    const std::vector<CompareFlags> keyFlags(
        keyChannels.size(), SortComparatorOracle::makeSortFlags(true, true));
    auto output = finalizeAndCollect(
        {*input, keyChannels, keyFlags}, testCase.withPayloadKind);
    SortComparatorOracle::expectRowsMatchById(
        *input, *output, input->childrenSize() - 1);
    SortComparatorOracle::expectSorted(*output, keyChannels, keyFlags);

    auto keyOnlyInput = std::make_shared<RowVector>(
        runPool_.get(), keyType, nullptr, input->size(), testCase.keys);
    auto keyOnlyOutput = finalizeAndCollect(
        {*keyOnlyInput, keyChannels, keyFlags}, testCase.keyOnlyKind);
    expectSortedValues(*keyOnlyInput, *keyOnlyOutput, keyChannels, keyFlags);
  }
}

TEST_F(RadixSortRunTest, keyOnlyEmptyAndOneRow) {
  auto rowType = ROW({"key"}, {BIGINT()});
  auto emptyRun = RadixSortRun::create(
      runPool_.get(),
      rowType,
      rowType,
      {SortComparatorOracle::makeSortFlags(true, true)},
      {0},
      {});
  ASSERT_NE(emptyRun, nullptr);
  EXPECT_FALSE(emptyRun->projection().hasPayload());
  EXPECT_EQ(emptyRun->storage()->payloadLayout(), nullptr);
  emptyRun->finalize();
  EXPECT_EQ(emptyRun->getOutput(10, outputPool_.get()), nullptr);
  EXPECT_EQ(emptyRun->state(), RadixSortRunState::kConsumed);

  auto input = makeRows({"key"}, {makeVector<int64_t>(BIGINT(), {42})});
  auto run = createRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});
  run->append(*input);
  EXPECT_EQ(run->storage()->payloadSize(), 0);
  run->finalize();
  auto output = collect(*run, 1);
  ASSERT_EQ(output->size(), 1);
  EXPECT_EQ(
      output->childAt(0)->asUnchecked<SimpleVector<int64_t>>()->valueAt(0), 42);
}

TEST_F(RadixSortRunTest, lifecycleAndOutputValidation) {
  auto input =
      makeKeyStringRows({2, 1}, {std::string(32, 'b'), std::string(32, 'a')});
  auto run = createRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});
  std::array<const char*, 2> keys{};
  std::array<char*, 2> payloads{};

  EXPECT_THROW(run->getOutput(1, outputPool_.get()), BoltException);
  EXPECT_THROW(
      run->collectRemainingRows(1, keys.data(), payloads.data()),
      BoltException);
  EXPECT_EQ(run->getOutput({}, {}, nullptr), nullptr);

  run->append(*input);
  run->finalize();
  EXPECT_EQ(run->getOutput({}, {}, nullptr), nullptr);
  EXPECT_THROW(run->append(*input), BoltException);
  EXPECT_THROW(run->finalize(), BoltException);
  EXPECT_EQ(run->collectRemainingRows(0, keys.data(), payloads.data()), 0);

  run->clear();
  run->clear();
  expectCleared(*run);
  EXPECT_EQ(run->getOutput({}, {}, nullptr), nullptr);
  EXPECT_THROW(run->append(*input), BoltException);
  EXPECT_THROW(run->finalize(), BoltException);
}

TEST_F(RadixSortRunTest, collectRemainingRowsAcrossCalls) {
  constexpr vector_size_t kRows = 9;
  auto input = makeKeyStringRows(
      makeValues<int64_t>(kRows, [](vector_size_t row) { return kRows - row; }),
      makeValues<std::string>(kRows, [](vector_size_t row) {
        return "payload-" + std::to_string(row) +
            std::string(24 + row, static_cast<char>('a' + row));
      }));
  RadixSortRunOptions options;
  options.keysPerBlock = 2;
  options.payloadRowsPerBlock = 2;
  auto run = finalizedRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}},
      options);

  std::array<const char*, kRows> keys{};
  std::array<char*, kRows> payloads{};
  std::vector<RowVectorPtr> batches;
  vector_size_t collectedRows = 0;
  for (const auto requested : {2, 3, 10}) {
    const auto count =
        run->collectRemainingRows(requested, keys.data(), payloads.data());
    ASSERT_EQ(count, std::min<vector_size_t>(requested, kRows - collectedRows));
    auto batch = run->getOutput(
        std::span<const char* const>(keys.data(), count),
        std::span<char* const>(payloads.data(), count),
        outputPool_.get());
    ASSERT_NE(batch, nullptr);
    batches.push_back(std::move(batch));
    collectedRows += count;
  }
  EXPECT_EQ(run->collectRemainingRows(1, keys.data(), payloads.data()), 0);
  EXPECT_EQ(run->metrics().outputRows, kRows);
  EXPECT_EQ(run->state(), RadixSortRunState::kSortedInMemory);
  auto output = concatenate(batches);
  SortComparatorOracle::expectRowsMatchById(*input, *output, 2);
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
  run->clear();
  expectCleared(*run);

  auto keyOnlyInput =
      makeRows({"key"}, {makeVector<int64_t>(BIGINT(), {7, 6, 5, 4, 3, 2, 1})});
  options = {};
  options.keysPerBlock = 2;
  auto keyOnlyRun = finalizedRun(
      {*keyOnlyInput, {0}, {SortComparatorOracle::makeSortFlags(true, true)}},
      options);
  vector_size_t expected = 1;
  for (const auto requested : {1, 2, 8}) {
    const auto count =
        keyOnlyRun->collectRemainingRows(requested, keys.data(), nullptr);
    ASSERT_GT(count, 0);
    auto batch = keyOnlyRun->getOutput(
        std::span<const char* const>(keys.data(), count),
        {},
        outputPool_.get());
    ASSERT_NE(batch, nullptr);
    for (vector_size_t row = 0; row < count; ++row) {
      EXPECT_EQ(
          batch->childAt(0)->asUnchecked<SimpleVector<int64_t>>()->valueAt(row),
          expected++);
    }
  }
  EXPECT_EQ(expected, 8);
  EXPECT_EQ(keyOnlyRun->collectRemainingRows(1, keys.data(), nullptr), 0);
  EXPECT_EQ(keyOnlyRun->metrics().outputRows, 7);
  keyOnlyRun->clear();
  expectCleared(*keyOnlyRun);
}

TEST_F(RadixSortRunTest, outputScratchGrowsAndShrinksAcrossCalls) {
  constexpr vector_size_t kRows = 433;
  auto input = makeRows(
      {"first", "second", "payload", "id"},
      {makeVector<int64_t>(
           BIGINT(),
           makeValues<int64_t>(
               kRows, [](vector_size_t row) { return (row * 11) % kRows; })),
       makeStringVector(makeValues<std::string>(
           kRows,
           [](vector_size_t row) {
             return std::string(40 + row, static_cast<char>('a' + row));
           })),
       makeStringVector(makeValues<std::string>(
           kRows,
           [](vector_size_t row) {
             return "payload-" + std::to_string(row) + std::string(row, 'x');
           })),
       makeIds(kRows)});
  const std::vector<column_index_t> keyChannels{0, 1};
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(false, false)};
  auto run = finalizedRun({*input, keyChannels, keyFlags});

  std::vector<VectorPtr> outputChildren;
  outputChildren.reserve(input->childrenSize());
  for (const auto& type : input->type()->as<TypeKind::ROW>().children()) {
    outputChildren.push_back(
        BaseVector::create(type, kRows, outputPool_.get()));
  }
  vector_size_t offset = 0;
  for (const auto batchSize : {1, 129, 2, 301}) {
    auto batch = run->getOutput(batchSize, outputPool_.get());
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->size(), batchSize);
    copyBatch(*batch, outputChildren, offset);
    offset += batch->size();
  }
  ASSERT_EQ(offset, kRows);
  EXPECT_EQ(run->getOutput(1, outputPool_.get()), nullptr);
  auto output = std::make_shared<RowVector>(
      outputPool_.get(),
      input->type(),
      nullptr,
      kRows,
      std::move(outputChildren));
  SortComparatorOracle::expectRowsMatchById(*input, *output, 3);
  SortComparatorOracle::expectSorted(*output, keyChannels, keyFlags);
}

TEST_F(RadixSortRunTest, appendUpdatesKeyNullabilityStats) {
  auto input = makeRows(
      {"key", "id"},
      {makeVector<int64_t>(BIGINT(), {2, std::nullopt, 1, 3}), makeIds(4)});
  auto run = createRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, false)}});
  run->append(*input);
  EXPECT_EQ(run->keyMayHaveNulls(), (std::vector<uint8_t>{1}));
  run->finalize();
  collectAndVerify(
      *run,
      *input,
      2,
      1,
      {0},
      {SortComparatorOracle::makeSortFlags(true, false)});
}

TEST_F(RadixSortRunTest, duplicateDirectKeyStaysInPayload) {
  constexpr vector_size_t kRows = 64;
  auto input = makeRows(
      {"key", "id"},
      {makeVector<int64_t>(
           BIGINT(),
           makeValues<int64_t>(
               kRows, [](vector_size_t row) { return row % 7; })),
       makeIds(kRows)});
  auto run = finalizedRun(
      {*input,
       {0, 0},
       {SortComparatorOracle::makeSortFlags(true, true),
        SortComparatorOracle::makeSortFlags(true, true)}});
  EXPECT_EQ(
      run->projection().columns()[0].source, RadixSortOutputSource::kPayload);
  collectAndVerify(*run, *input, 9, 1);
}

TEST_F(RadixSortRunTest, floatingPointKeyOutputUsesDecodedKey) {
  auto input = makeRows(
      {"value", "id"}, {specialDoubles(), makeIds(kDoubleBits.size())});
  auto run = finalizedRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});
  EXPECT_EQ(
      run->projection().columns()[0].source,
      RadixSortOutputSource::kDecodedKey);
  EXPECT_EQ(
      run->projection().payloadChannels(), (std::vector<column_index_t>{1}));
  auto output = collect(*run, 3);
  SortComparatorOracle::expectRowsMatchById(*input, *output, 1);
  SortComparatorOracle::expectSorted(
      *output, {0}, {SortComparatorOracle::makeSortFlags(true, true)});
}

TEST_F(RadixSortRunTest, directFixedTailLayoutsRoundTripWithPayload) {
  struct Case {
    VectorPtr key;
    RadixSortKeyLayoutKind layout;
  };
  const std::vector<Case> cases{
      {makeVector<int64_t>(BIGINT(), {7, std::nullopt, -4, 0, 9}),
       RadixSortKeyLayoutKind::kKeyWithPayloadFixed16},
      {makeVector<double>(DOUBLE(), {3.5, std::nullopt, -2.0, 0.0, 17.25}),
       RadixSortKeyLayoutKind::kKeyWithPayloadFixed16},
      {makeVector<Timestamp>(
           TIMESTAMP(),
           {Timestamp(3, 7),
            std::nullopt,
            Timestamp(-2, 999999999),
            Timestamp(0, 0),
            Timestamp(1, 42)}),
       RadixSortKeyLayoutKind::kKeyWithPayloadFixed24}};

  for (const auto& testCase : cases) {
    for (const auto keyFlags :
         {SortComparatorOracle::makeSortFlags(true, true),
          SortComparatorOracle::makeSortFlags(true, false),
          SortComparatorOracle::makeSortFlags(false, true),
          SortComparatorOracle::makeSortFlags(false, false)}) {
      SCOPED_TRACE(
          testCase.key->type()->toString() +
          (keyFlags.ascending ? " ASC" : " DESC"));
      auto input = makeRows(
          {"key", "payload", "id"},
          {testCase.key,
           makeStringVector({"p0", "p1", std::string(40, 'x'), "p3", "p4"}),
           makeIds(testCase.key->size())});
      auto run = finalizedRun({*input, {0}, {keyFlags}});
      EXPECT_EQ(run->keyLayout().kind(), testCase.layout);
      auto output = collect(*run, 2);
      SortComparatorOracle::expectRowsMatchById(*input, *output, 2);
      SortComparatorOracle::expectSorted(*output, {0}, {keyFlags});
    }
  }
}

TEST_F(RadixSortRunTest, selectiveDecodeIncludesFloatingPointKey) {
  auto input = makeRows(
      {"group", "value", "id"},
      {makeVector<int64_t>(
           BIGINT(),
           makeValues<int64_t>(
               kDoubleBits.size(), [](vector_size_t row) { return row % 2; })),
       specialDoubles(),
       makeIds(kDoubleBits.size())});
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, true)};
  auto run = finalizedRun({*input, {0, 1}, keyFlags});
  EXPECT_EQ(run->projection().decodedKeyMask(), (std::vector<uint8_t>{1, 1}));
  EXPECT_EQ(
      run->projection().columns()[0].source,
      RadixSortOutputSource::kDecodedKey);
  EXPECT_EQ(
      run->projection().columns()[1].source,
      RadixSortOutputSource::kDecodedKey);
  EXPECT_EQ(
      run->projection().payloadChannels(), (std::vector<column_index_t>{2}));

  auto output = collect(*run, 3);
  SortComparatorOracle::expectRowsMatchById(*input, *output, 2);
  SortComparatorOracle::expectSorted(*output, {0, 1}, keyFlags);
}

TEST_F(RadixSortRunTest, mixedScalarStringFloatingKeysDoNotUsePayload) {
  auto input = makeRows(
      {"uid", "ut", "data_type", "feature_id", "score"},
      {makeVector<int64_t>(BIGINT(), {4, 1, 1, 3, 2, std::nullopt}),
       makeVector<int64_t>(BIGINT(), {2, 1, 1, 3, 2, 5}),
       makeVector<int32_t>(INTEGER(), {1, 2, 1, 3, 2, 5}),
       makeStringVector({"b", "c", "a", std::string(40, 'x'), "b", "z"}),
       makeVector<float>(REAL(), {2.5F, 1.5F, 3.5F, -4.0F, 0.5F, 9.0F})});
  const std::vector<column_index_t> keyChannels{0, 1, 2, 3, 4};
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(false, false),
      SortComparatorOracle::makeSortFlags(false, false)};

  auto run = createRun({*input, keyChannels, keyFlags});
  EXPECT_FALSE(run->projection().hasPayload());
  EXPECT_TRUE(run->projection().payloadChannels().empty());
  EXPECT_EQ(run->payloadLayout(), nullptr);
  EXPECT_EQ(
      run->keyLayout().kind(), RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  EXPECT_EQ(run->keyLayout().heapKeyOffset(), 18);

  run->append(*input);
  run->finalize();
  auto output = collect(*run, 2);
  expectSortedValues(*input, *output, keyChannels, keyFlags);
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
  auto input = makeRows(
      {"timestamp", "string", "id"},
      {makeVector<Timestamp>(TIMESTAMP(), timestamps),
       makeStringVector(strings),
       makeIds(timestamps.size())});
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, false),
      SortComparatorOracle::makeSortFlags(false, true)};
  auto run = finalizedRun({*input, {0, 1}, keyFlags});
  collectAndVerify(*run, *input, 2, 2, {0, 1}, keyFlags);
}

TEST_F(RadixSortRunTest, complexDirectKeysRoundTripAndSort) {
  auto arrays = makeIntegerArrays();
  auto rows = makeNestedRows();
  auto maps = makeIntegerStringMaps();
  auto input = makeRows(
      {"array", "row", "map", "id"},
      {arrays, rows, maps, makeIds(arrays->size())});
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, false),
      SortComparatorOracle::makeSortFlags(false, true),
      SortComparatorOracle::makeSortFlags(true, true)};
  auto run = finalizedRun({*input, {0, 1, 2}, keyFlags});
  EXPECT_EQ(
      run->projection().payloadChannels(), (std::vector<column_index_t>{3}));
  collectAndVerify(*run, *input, 2, 3, {0, 1, 2}, keyFlags);
}

TEST_F(RadixSortRunTest, complexPayloadRoundTripAndSort) {
  auto arrays = makeIntegerArrays();
  auto rows = makeNestedRows();
  auto maps = makeIntegerStringMaps();
  auto keys = makeVector<int64_t>(BIGINT(), {6, 5, 4, 3, 2, 1, 0});
  auto input = makeRows(
      {"array", "row", "map", "key", "id"},
      {arrays, rows, maps, keys, makeIds(arrays->size())});
  auto run = finalizedRun(
      {*input, {3}, {SortComparatorOracle::makeSortFlags(true, true)}});
  EXPECT_EQ(
      run->projection().payloadChannels(),
      (std::vector<column_index_t>{0, 1, 2, 4}));
  collectAndVerify(*run, *input, 2, 4, {3});
}

TEST_F(RadixSortRunTest, eventKeyMapPayloadMultipleBatches) {
  constexpr vector_size_t kRows = 64;
  std::vector<std::optional<std::string>> events;
  std::vector<std::optional<std::string>> enterFrom;
  std::vector<std::optional<std::string>> hours;
  events.reserve(kRows);
  enterFrom.reserve(kRows);
  hours.reserve(kRows);
  static constexpr std::array<const char*, 6> kEvents{
      "video_play", "video_play_pause", "like", "follow", "share", "comment"};
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto eventIndex = row % 10 < 6 ? row % 3 : row % kEvents.size();
    events.push_back(kEvents[eventIndex]);
    enterFrom.push_back(
        row % 4 == 0
            ? std::optional<std::string>{}
            : std::optional<std::string>("enter_" + std::to_string(row % 7)));
    hours.push_back((row % 24 < 10 ? "0" : "") + std::to_string(row % 24));
  }
  auto params = makeStringStringMaps(kRows);
  auto input = makeRows(
      {"event", "id", "enter_from", "params", "hour"},
      {makeStringVector(events),
       makeIds(kRows),
       makeStringVector(enterFrom),
       params,
       makeStringVector(hours)});

  auto run = createRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});
  EXPECT_EQ(
      run->projection().payloadChannels(),
      (std::vector<column_index_t>{1, 2, 3, 4}));
  run->append(*slice(*input, 0, 17));
  run->append(*slice(*input, 17, 23));
  run->append(*slice(*input, 40, kRows - 40));
  run->finalize();

  collectAndVerify(*run, *input, 13, 1);
}

TEST_F(RadixSortRunTest, mapPayloadOutputOwnsDataAfterRunClear) {
  constexpr vector_size_t kRows = 48;
  auto sourcePool = rootPool_->addLeafChild("radix-sort-run-map-source-test");
  auto input = makeRows(
      {"event", "params", "id"},
      {makeStringVector(
           makeValues<std::string>(
               kRows,
               [](vector_size_t row) {
                 return "event_" + std::to_string(row % 5);
               }),
           sourcePool.get()),
       makeStringStringMaps(kRows, sourcePool.get()),
       makeIds(kRows, sourcePool.get())},
      sourcePool.get());
  auto run = finalizedRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});

  auto output = collectAndVerify(*run, *input, 11, 2);
  EXPECT_EQ(run->state(), RadixSortRunState::kConsumed);
  EXPECT_EQ(run->storage(), nullptr);
  EXPECT_EQ(runPool_->currentBytes(), 0);
}

TEST_F(RadixSortRunTest, directKeyOutputDoesNotDuplicatePayloadForEvent) {
  auto input = makeRows(
      {"event", "id"},
      {makeStringVector({"play", "play", "click", "share", "click"}),
       makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4})});
  auto run = finalizedRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});
  ASSERT_EQ(run->projection().columns().size(), 2);
  EXPECT_EQ(
      run->projection().columns()[0].source,
      RadixSortOutputSource::kDecodedKey);
  EXPECT_EQ(
      run->projection().payloadChannels(), (std::vector<column_index_t>{1}));
  collectAndVerify(*run, *input, 2, 1);
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
  }
  auto arrays = makeNonNullIntegerArrays(kRows);
  auto input = makeRows(
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
      {makeStringVector(textKeys),
       makeVector<int64_t>(BIGINT(), nullableKeys),
       makeVector<int32_t>(INTEGER(), fixedKeys),
       makeVector<bool>(BOOLEAN(), boolKeys),
       makeVector<int64_t>(BIGINT(), fixedPayload),
       makeVector<bool>(BOOLEAN(), boolPayload),
       makeStringVector(firstStrings),
       makeStringVector(secondStrings),
       arrays,
       makeVector<int64_t>(BIGINT(), nullablePayload),
       makeIds(kRows)});
  const std::vector<CompareFlags> keyFlags{
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(true, true)};
  auto run = createRun({*input, {0, 1, 2, 3}, keyFlags});
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
  copyBatch(*first, children, 0);
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

  copyBatch(*second, children, offset);
  offset += second->size();
  while (auto batch = run->getOutput(4, outputPool_.get())) {
    copyBatch(*batch, children, offset);
    offset += batch->size();
  }
  ASSERT_EQ(offset, kRows);
  auto output = std::make_shared<RowVector>(
      outputPool_.get(), input->type(), nullptr, kRows, std::move(children));
  SortComparatorOracle::expectRowsMatchById(*input, *output, 10);
  SortComparatorOracle::expectSorted(*output, {0, 1, 2, 3}, keyFlags);
}

TEST_F(RadixSortRunTest, singleStringPayloadNullFreeReuseResetsNullBuffer) {
  constexpr vector_size_t kRows = 8;
  auto input = makeKeyStringRows(
      makeValues<int64_t>(kRows, [](vector_size_t row) { return kRows - row; }),
      makeValues<std::string>(kRows, [](vector_size_t row) {
        return std::string(64 + row, static_cast<char>('a' + row));
      }));
  auto run = createRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});
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
  auto keys = makeValues<std::string>(kRows, [](vector_size_t row) {
    return std::string(96 + row, static_cast<char>('a' + row));
  });
  auto strings = makeValues<std::string>(kRows, [](vector_size_t row) {
    return std::string(80 + row, static_cast<char>('k' + row));
  });
  auto arrays = makeIntegerArrays();
  auto input = makeRows(
      {"key", "string", "array", "id"},
      {makeStringVector(keys),
       makeStringVector(strings),
       arrays,
       makeIds(kRows)});
  auto run = createRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});
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
    ASSERT_NE(key.heapKeyData(), nullptr);
    EXPECT_EQ(key.heapSize(), key.heapKey().size());
    EXPECT_EQ(key.heapKeyData(), key.heapKey().data());

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
  auto input = makeDescendingKeyStringRows(kRows, 80, sourcePool.get());
  auto run = finalizedRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});

  const auto width = run->storage()->layout().width();
  std::vector<std::array<char, 32>> records(run->size());
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
  while (auto batch = run->getOutput(9, outputPool_.get())) {
    drainedRows += batch->size();
  }
  EXPECT_EQ(run->state(), RadixSortRunState::kConsumed);
  EXPECT_EQ(run->storage(), nullptr);
  EXPECT_EQ(run->retainedBytes(), 0);
  EXPECT_EQ(runPool_->currentBytes(), 0);
  EXPECT_EQ(drainedRows, kRows);
  ASSERT_GT(firstBatch->size(), 0);
  for (vector_size_t row = 0; row < firstBatch->size(); ++row) {
    const auto id = idAt(*firstBatch, row, 2);
    EXPECT_EQ(
        firstBatch->childAt(0)->asUnchecked<SimpleVector<int64_t>>()->valueAt(
            row),
        kRows - id);
    EXPECT_EQ(
        firstBatch->childAt(1)
            ->asUnchecked<SimpleVector<StringView>>()
            ->valueAt(row)
            .str(),
        std::string(80, static_cast<char>('a' + id % 20)));
  }
}

TEST_F(
    RadixSortRunTest,
    wideVariableKeyRoundTripAcrossBlocksBatchesAndPointerMerge) {
  constexpr vector_size_t kRows = 41;
  constexpr uint32_t kKeys = 8;
  std::vector<VectorPtr> children;
  std::vector<std::string> names;
  std::vector<TypePtr> keyTypes(kKeys, BIGINT());
  std::vector<column_index_t> keyChannels(kKeys);
  std::vector<CompareFlags> keyFlags(kKeys);
  children.reserve(kKeys + 2);
  for (uint32_t key = 0; key < kKeys - 1; ++key) {
    names.push_back("key" + std::to_string(key));
    children.push_back(makeVector<int64_t>(
        BIGINT(), makeValues<int64_t>(kRows, [key](vector_size_t row) {
          if (row % 18 == 0 ||
              (row % 9 == 0 ? key >= 4 : (row + key * 3) % 11 == 0)) {
            return std::optional<int64_t>{};
          }
          return std::optional<int64_t>{
              key < 4 ? static_cast<int64_t>((row + key) % 3)
                      : static_cast<int64_t>(
                            ((row * (37 + key * 2)) ^ (key * 101)) % 29 - 14)};
        })));
    keyChannels[key] = key;
    keyFlags[key] =
        SortComparatorOracle::makeSortFlags(key % 3 != 1, key % 2 == 0);
  }
  keyTypes.back() = VARCHAR();
  keyChannels.back() = kKeys - 1;
  keyFlags.back() = SortComparatorOracle::makeSortFlags(false, false);
  names.insert(names.end(), {"key7", "payload", "id"});
  children.push_back(makeStringVector(makeValues<std::string>(
      kRows, [](vector_size_t row) -> std::optional<std::string> {
        return row % 18 == 0 ? std::nullopt
                             : std::optional{std::string(
                                   row % 5 == 0 ? 64 : 3 + row % 7,
                                   static_cast<char>('a' + row % 17))};
      })));
  children.push_back(makeStringVector(makeValues<std::string>(
      kRows, [](vector_size_t row) -> std::optional<std::string> {
        return row % 7 == 0
            ? std::nullopt
            : std::optional{
                  std::string(48 + row % 9, static_cast<char>('a' + row % 17))};
      })));
  children.push_back(makeIds(kRows));
  auto keyType =
      ROW(std::vector<std::string>(names.begin(), names.begin() + kKeys),
          std::move(keyTypes));
  auto input = makeRows(std::move(names), children);

  const auto makeRun = [&](const RowVector& rows) {
    RadixSortRunOptions options;
    options.keysPerBlock = 3;
    options.preferredKeyHeapGroupBytes = 29;
    options.payloadRowsPerBlock = 2;
    auto run = createRun({*input, keyChannels, keyFlags}, options);
    const auto middle = (rows.size() - 6) / 2;
    run->append(*slice(rows, 0, 5));
    run->append(*slice(rows, 5, 1));
    run->append(*slice(rows, 6, middle));
    run->append(*slice(rows, 6 + middle, rows.size() - 6 - middle));
    EXPECT_EQ(
        run->keyLayout().kind(),
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
    EXPECT_GT(run->storage()->keyBlocks().size(), 1);
    EXPECT_GT(run->storage()->keyHeapGroups().size(), 1);
    EXPECT_GT(run->storage()->payloadFixedBlocks().size(), 1);
    run->finalize();
    return run;
  };

  auto inMemoryRun = makeRun(*input);
  auto first = inMemoryRun->getOutput(6, outputPool_.get());
  ASSERT_NE(first, nullptr);
  auto firstCopy = slice(*first, 0, first->size());
  const auto reusableBuffers = outputBuffers(*first);
  first.reset();
  auto second = inMemoryRun->getOutput(6, outputPool_.get());
  ASSERT_NE(second, nullptr);
  const auto retainedBuffers = outputBuffers(*second);
  EXPECT_EQ(retainedBuffers[0], reusableBuffers[0]);
  const auto retainedId = idAt(*second, 0, kKeys + 1);
  const auto retainedPayload = second->childAt(kKeys)
                                   ->asUnchecked<SimpleVector<StringView>>()
                                   ->valueAt(0)
                                   .str();
  auto third = inMemoryRun->getOutput(6, outputPool_.get());
  ASSERT_NE(third, nullptr);
  EXPECT_NE(outputBuffers(*third)[0], retainedBuffers[0]);
  EXPECT_EQ(idAt(*second, 0, kKeys + 1), retainedId);
  EXPECT_EQ(
      second->childAt(kKeys)
          ->asUnchecked<SimpleVector<StringView>>()
          ->valueAt(0)
          .str(),
      retainedPayload);
  auto alternateOutputPool =
      rootPool_->addLeafChild("radix-sort-run-alternate-output-test");
  auto alternate = inMemoryRun->getOutput(6, alternateOutputPool.get());
  ASSERT_NE(alternate, nullptr);
  EXPECT_EQ(alternate->pool(), alternateOutputPool.get());
  for (uint32_t column = 0; column < alternate->childrenSize(); ++column) {
    EXPECT_EQ(alternate->childAt(column)->pool(), alternateOutputPool.get());
  }
  std::vector<RowVectorPtr> inMemoryBatches{
      firstCopy, second, third, alternate};
  while (auto batch = inMemoryRun->getOutput(7, outputPool_.get())) {
    inMemoryBatches.push_back(std::move(batch));
  }
  auto inMemoryOutput = concatenate(inMemoryBatches);
  SortComparatorOracle::expectRowsMatchById(*input, *inMemoryOutput, kKeys + 1);
  SortComparatorOracle::expectSorted(*inMemoryOutput, keyChannels, keyFlags);

  auto leftInput = slice(*input, 0, 20);
  auto rightInput = slice(*input, 20, kRows - 20);
  auto mergeRun = makeRun(*leftInput);
  auto secondMergeRun = makeRun(*rightInput);
  std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
  streams.push_back(
      std::make_unique<RadixSortMemoryRunMergeStream>(*mergeRun->storage()));
  streams.push_back(std::make_unique<RadixSortMemoryRunMergeStream>(
      *secondMergeRun->storage()));
  RadixSortMerger merger(mergeRun->keyLayout(), std::move(streams));
  std::vector<RowVectorPtr> mergeBatches;
  std::array<const char*, 9> keys{};
  std::array<char*, 9> payloadRows{};
  vector_size_t mergedRows = 0;
  while (mergedRows < kRows) {
    const auto requested = std::min<vector_size_t>(
        keys.size(), static_cast<vector_size_t>(kRows - mergedRows));
    const auto count =
        merger.collectRows(requested, keys.data(), payloadRows.data());
    ASSERT_EQ(count, requested);
    mergeBatches.push_back(mergeRun->getOutput(
        std::span<const char* const>(keys.data(), count),
        std::span<char* const>(payloadRows.data(), count),
        outputPool_.get()));
    mergedRows += count;
  }
  ASSERT_GT(mergeBatches.size(), 2);
  auto mergeOutput = concatenate(mergeBatches);
  SortComparatorOracle::expectRowsMatchById(*input, *mergeOutput, kKeys + 1);
  SortComparatorOracle::expectSorted(*mergeOutput, keyChannels, keyFlags);
}

TEST_F(RadixSortRunTest, outputReusesBuffersWithCopyOnWrite) {
  constexpr vector_size_t kRows = 16;
  auto input = makeDescendingKeyStringRows(kRows, 48);
  auto run = finalizedRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});

  auto first = run->getOutput(4, outputPool_.get());
  ASSERT_NE(first, nullptr);
  const auto reusableBuffers = outputBuffers(*first, true);
  ASSERT_NE(reusableBuffers[2], nullptr);
  first.reset();

  auto second = run->getOutput(4, outputPool_.get());
  ASSERT_NE(second, nullptr);
  auto* secondKey = second->childAt(0)->asUnchecked<FlatVector<int64_t>>();
  auto* secondString =
      second->childAt(1)->asUnchecked<FlatVector<StringView>>();
  const auto retainedBuffers = outputBuffers(*second, true);
  ASSERT_NE(retainedBuffers[2], nullptr);
  EXPECT_EQ(retainedBuffers[0], reusableBuffers[0]);
  EXPECT_EQ(retainedBuffers[1], reusableBuffers[1]);
  EXPECT_EQ(retainedBuffers[2], reusableBuffers[2]);

  const auto retainedKey = secondKey->valueAt(0);
  const auto retainedString = secondString->valueAt(0).getString();

  auto third = run->getOutput(4, outputPool_.get());
  ASSERT_NE(third, nullptr);
  const auto thirdBuffers = outputBuffers(*third, true);
  ASSERT_NE(thirdBuffers[2], nullptr);
  EXPECT_NE(thirdBuffers[0], retainedBuffers[0]);
  EXPECT_NE(thirdBuffers[1], retainedBuffers[1]);
  EXPECT_NE(thirdBuffers[2], retainedBuffers[2]);
  EXPECT_EQ(secondKey->valueAt(0), retainedKey);
  EXPECT_EQ(secondString->valueAt(0).getString(), retainedString);
  std::vector<bool> seen(kRows, false);
  SortComparatorOracle::expectRowsMatchById(
      *input, *second, 2, {.seen = &seen});
}

TEST_F(RadixSortRunTest, outputSurvivesExplicitRunClear) {
  auto input = makeRows(
      {"key", "payload"},
      {makeVector<int64_t>(BIGINT(), {1, 2}),
       makeStringVector({std::string(4096, 'x'), "y"})});
  auto run = createRun(
      {*input, {0}, {SortComparatorOracle::makeSortFlags(true, true)}});
  run->append(*input);
  run->finalize();
  auto output = run->getOutput(1, outputPool_.get());
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), 1);
  run->clear();
  expectCleared(*run);
  EXPECT_EQ(
      output->childAt(0)->asUnchecked<SimpleVector<int64_t>>()->valueAt(0), 1);
  EXPECT_EQ(
      output->childAt(1)
          ->asUnchecked<SimpleVector<StringView>>()
          ->valueAt(0)
          .str(),
      std::string(4096, 'x'));
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
