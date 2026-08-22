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

#include <zstd.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/radixsort/PayloadRow.h"
#include "bolt/exec/radixsort/RadixSortKeyCodec.h"
#include "bolt/exec/radixsort/RadixSortSpill.h"
#include "bolt/exec/radixsort/RadixSortSpillRow.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::exec::radixsort::test {
namespace {

constexpr std::array kLayouts{
    RadixSortKeyLayoutKind::kKeyOnlyFixed8,
    RadixSortKeyLayoutKind::kKeyOnlyFixed16,
    RadixSortKeyLayoutKind::kKeyOnlyFixed24,
    RadixSortKeyLayoutKind::kKeyOnlyFixed32,
    RadixSortKeyLayoutKind::kKeyOnlyVariable32,
    RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
    RadixSortKeyLayoutKind::kKeyWithPayloadFixed24,
    RadixSortKeyLayoutKind::kKeyWithPayloadFixed32,
    RadixSortKeyLayoutKind::kKeyWithPayloadVariable32};

class RadixSortSpillRowTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    filesystems::registerLocalFileSystem();
  }

 protected:
  static constexpr uint64_t kBlockHeaderSize = 2 * sizeof(uint32_t);
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("radix-sort-spill-row-test")};

  template <typename T, typename U = T>
  FlatVectorPtr<T> makeVector(
      const TypePtr& type,
      const std::vector<std::optional<U>>& values) {
    auto result =
        BaseVector::create<FlatVector<T>>(type, values.size(), pool_.get());
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        result->set(row, T(*values[row]));
      } else {
        result->setNull(row, true);
      }
    }
    return result;
  }

  FlatVectorPtr<StringView> makeStringVector(
      const std::vector<std::optional<std::string>>& values) {
    return makeVector<StringView>(VARCHAR(), values);
  }

  RowVectorPtr makeRows(
      std::vector<std::string> names,
      const std::vector<VectorPtr>& children) {
    std::vector<TypePtr> types;
    for (const auto& child : children) {
      types.push_back(child->type());
    }
    return std::make_shared<RowVector>(
        pool_.get(),
        ROW(std::move(names), std::move(types)),
        nullptr,
        children.empty() ? 0 : children.front()->size(),
        children);
  }

  template <typename T>
  BufferPtr makeBuffer(const std::vector<T>& values) {
    auto buffer = AlignedBuffer::allocate<T>(values.size(), pool_.get());
    std::copy(values.begin(), values.end(), buffer->template asMutable<T>());
    return buffer;
  }

  MapVectorPtr makeLargeStringStringMaps(vector_size_t rows) {
    std::vector<vector_size_t> offsets;
    std::vector<vector_size_t> sizes;
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<std::string>> values;
    vector_size_t offset = 0;
    for (vector_size_t row = 0; row < rows; ++row) {
      offsets.push_back(offset);
      const vector_size_t entries = row % 9 == 0 ? 0
          : row % 4 == 0                         ? 64
          : row % 3 == 0                         ? 24
                                                 : 6;
      sizes.push_back(entries);
      for (vector_size_t entry = 0; entry < entries; ++entry) {
        keys.push_back(
            "param_" + std::to_string(entry % 17) + "_" +
            std::to_string(row % 5));
        values.push_back(
            entry % 8 == 0
                ? std::string(
                      64 + (row + entry) % 19,
                      static_cast<char>('a' + entry % 26))
                : "value_" + std::to_string(row) + "_" + std::to_string(entry));
      }
      offset += entries;
    }
    auto maps = std::make_shared<MapVector>(
        pool_.get(),
        MAP(VARCHAR(), VARCHAR()),
        nullptr,
        rows,
        makeBuffer(offsets),
        makeBuffer(sizes),
        makeStringVector(keys),
        makeStringVector(values));
    if (rows > 5) {
      maps->setNull(5, true);
    }
    return maps;
  }

  static void expectEquivalent(
      const RowVector& expected,
      const RowVector& actual) {
    const CompareFlags flags{
        .nullsFirst = true,
        .ascending = true,
        .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
    ASSERT_EQ(expected.size(), actual.size());
    ASSERT_EQ(expected.childrenSize(), actual.childrenSize());
    for (uint32_t column = 0; column < expected.childrenSize(); ++column) {
      for (vector_size_t row = 0; row < expected.size(); ++row) {
        const auto result = expected.childAt(column)->compare(
            actual.childAt(column).get(), row, row, flags);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, 0) << "column=" << column << ", row=" << row;
      }
    }
  }

  void expectStringPayload(
      const PayloadRowLayout& layout,
      const std::vector<char*>& payloads,
      const std::string& expected) {
    RowVectorPtr output;
    PayloadRowReader::gather(layout, payloads, pool_.get(), output);
    EXPECT_EQ(
        output->childAt(0)->as<FlatVector<StringView>>()->valueAt(0),
        StringView(expected));
  }

  common::SpillConfig spillConfig(
      const std::string& directory,
      common::CompressionKind compression,
      uint64_t writeBufferSize) const {
    common::SpillConfig config;
    config.getSpillDirPathCb = [directory]() -> const std::string& {
      return directory;
    };
    config.updateAndCheckSpillLimitCb = [&](uint64_t) {};
    config.fileNamePrefix = "radix-sort-spill-test";
    config.maxFileSize =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    config.spillUringEnabled = false;
    config.writeBufferSize = writeBufferSize;
    config.compressionKind = compression;
    return config;
  }

  struct SpilledRun {
    std::shared_ptr<exec::test::TempDirectoryPath> directory;
    RadixSortSpillFile file;
    RadixSortSpillRunMeta meta;
  };

  template <typename T>
  void overwriteSpillValue(
      const std::string& path,
      uint64_t offset,
      const T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    BOLT_CHECK(file.good(), path);
    file.seekp(offset);
    BOLT_CHECK(file.good(), "offset={}", offset);
    file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    BOLT_CHECK(file.good(), "offset={}", offset);
  }

  template <typename T>
  T readSpillValue(const std::string& path, uint64_t offset) {
    std::ifstream file(path, std::ios::binary);
    T value;
    file.seekg(offset);
    file.read(reinterpret_cast<char*>(&value), sizeof(value));
    EXPECT_TRUE(file.good()) << "offset=" << offset;
    return value;
  }

  SpilledRun spillSingleRun(
      const RadixSortRunStorage& storage,
      const PayloadRowLayout* payloadLayout,
      common::CompressionKind compression = common::CompressionKind_NONE,
      uint64_t writeBufferSize = 1 << 20,
      bool outputStage = false) {
    auto directory = exec::test::TempDirectoryPath::create();
    folly::Synchronized<common::SpillStats> stats;
    RadixSortSpillWriter writer(
        directory->path + "/spill",
        spillConfig(directory->path, compression, writeBufferSize),
        pool_.get(),
        &stats);
    std::vector<RadixSortSpillFile> files;
    if (outputStage) {
      std::vector<const char*> keys(storage.size());
      std::vector<char*> payloads(storage.size());
      for (uint64_t row = 0; row < storage.size(); ++row) {
        keys[row] = storage.keyDataAt(row);
        payloads[row] = RadixSortKey(storage.layout(), keys[row]).payload();
      }
      writer.writeRows(
          storage.layout(),
          payloadLayout,
          keys.data(),
          payloads.data(),
          storage.size());
      files = writer.finishRows();
    } else {
      files = writer.writeRun(storage, payloadLayout);
    }
    BOLT_CHECK_EQ(files.size(), 1);
    auto file = std::move(files.front());
    EXPECT_EQ(file.rowCount, storage.size());
    EXPECT_EQ(file.compressionKind, compression);
    EXPECT_GT(file.size, 0);
    const auto written = stats.copy();
    EXPECT_EQ(written.spilledBytes, file.size);
    EXPECT_GT(written.spillWrites, 0);
    EXPECT_TRUE(std::filesystem::exists(file.path));
    return {
        std::move(directory),
        std::move(file),
        {storage.layout(),
         static_cast<uint32_t>(payloadLayout ? payloadLayout->rowWidth() : 0)}};
  }

  struct ReadRows {
    SpilledRun spill;
    std::unique_ptr<RadixSortSpillReader> reader;
    std::vector<char*> keys;
    std::vector<char*> payloads;
  };
  ReadRows readAll(SpilledRun spill, const PayloadRowLayout* payloadLayout) {
    ReadRows result{std::move(spill), nullptr, {}, {}};
    result.reader = std::make_unique<RadixSortSpillReader>(
        result.spill.file,
        result.spill.meta,
        payloadLayout,
        pool_.get(),
        false);
    std::vector<char*> keys, payloads;
    while (result.reader->nextBatch(keys, payloads)) {
      result.keys.insert(result.keys.end(), keys.begin(), keys.end());
      result.payloads.insert(
          result.payloads.end(), payloads.begin(), payloads.end());
    }
    return result;
  }

  void appendAndVerify(
      RadixSortKeyLayoutKind kind,
      const std::vector<std::string>& keys,
      const RowVectorPtr& payload,
      const std::shared_ptr<const PayloadRowLayout>& payloadLayout,
      common::CompressionKind compression = common::CompressionKind_NONE,
      uint64_t writeBufferSize = 1 << 20,
      bool outputStage = false) {
    auto keyLayout = RadixSortKeyLayout::fromKind(kind);
    const auto layout = keyLayout.hasPayload() ? payloadLayout : nullptr;
    RadixSortRunStorage storage(pool_.get(), keyLayout, 4, 64, layout, 4, 1024);
    PayloadRowBatch rows;
    if (layout) {
      PayloadRowWriter payloadWriter;
      payloadWriter.append(*payload, storage, rows);
    }
    for (uint32_t row = 0; row < keys.size(); ++row) {
      storage.append(keys[row], layout ? rows.rowAt(row) : nullptr);
    }
    auto read = readAll(
        spillSingleRun(
            storage, layout.get(), compression, writeBufferSize, outputStage),
        layout.get());
    ASSERT_EQ(read.keys.size(), storage.size());
    for (vector_size_t row = 0; row < storage.size(); ++row) {
      EXPECT_EQ(
          RadixSortKey(keyLayout, read.keys[row])
              .compare(RadixSortKey(keyLayout, storage.keyDataAt(row))),
          0)
          << "row=" << row;
    }
    if (layout) {
      ASSERT_EQ(read.payloads.size(), storage.size());
      RowVectorPtr output;
      PayloadRowReader::gather(*layout, read.payloads, pool_.get(), output);
      expectEquivalent(*payload, *output);
    }
  }

  void expectOversizedRoundTrip(
      RadixSortKeyLayoutKind kind,
      const std::shared_ptr<const PayloadRowLayout>& payloadLayout,
      char* payload) {
    auto keyLayout = RadixSortKeyLayout::fromKind(kind);
    RadixSortRunStorage storage(
        pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 64);
    storage.append(
        std::string(
            keyLayout.isVariable() ? keyLayout.inlineCapacity() : 8, 'a'),
        payload);
    auto read = readAll(
        spillSingleRun(storage, payloadLayout.get()), payloadLayout.get());
    ASSERT_EQ(read.keys.size(), 1);
    ASSERT_EQ(read.payloads.size(), 1);
    EXPECT_EQ(
        RadixSortKey(keyLayout, read.keys[0])
            .compare(RadixSortKey(keyLayout, storage.keyDataAt(0))),
        0);
    EXPECT_EQ(
        std::memcmp(read.payloads[0], payload, payloadLayout->rowWidth()), 0);
  }

  void verifyMerge(
      const RadixSortKeyLayout& layout,
      const std::vector<int64_t>& values,
      bool includeEmpty = false) {
    std::vector<std::unique_ptr<RadixSortRunStorage>> runs;
    std::vector<std::unique_ptr<int64_t>> payloadValues;
    for (const auto value : values) {
      auto storage =
          std::make_unique<RadixSortRunStorage>(pool_.get(), layout, 2, 64);
      payloadValues.push_back(std::make_unique<int64_t>(value));
      std::string key(sizeof(uint64_t), '\0');
      key.back() = static_cast<char>(value);
      storage->append(key, reinterpret_cast<char*>(payloadValues.back().get()));
      runs.push_back(std::move(storage));
    }
    if (includeEmpty) {
      runs.push_back(
          std::make_unique<RadixSortRunStorage>(pool_.get(), layout, 2, 64));
    }
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    for (const auto& run : runs) {
      streams.push_back(std::make_unique<RadixSortMemoryRunMergeStream>(*run));
    }
    RadixSortMerger merger(layout, std::move(streams));
    std::vector<const char*> keys(values.size());
    std::vector<char*> payloads(values.size());
    const auto count =
        merger.collectRows(keys.size(), keys.data(), payloads.data());
    ASSERT_EQ(count, values.size());
    for (vector_size_t row = 0; row < count; ++row) {
      EXPECT_EQ(*reinterpret_cast<int64_t*>(payloads[row]), values[row])
          << "row=" << row;
    }
  }

  SpilledRun writeInlineKeySpill(
      RadixSortKeyLayoutKind kind,
      uint32_t rowCount) {
    auto keyLayout = RadixSortKeyLayout::fromKind(kind);
    RadixSortRunStorage storage(pool_.get(), keyLayout, 4, 64);
    const auto keySize =
        keyLayout.isVariable() ? keyLayout.inlineCapacity() : 8;
    for (uint32_t row = 0; row < rowCount; ++row) {
      storage.append(std::string(keySize, static_cast<char>(row + 1)));
    }
    return spillSingleRun(storage, nullptr);
  }

  SpilledRun writeLargePayloadSpill(
      const std::string& value,
      std::shared_ptr<const PayloadRowLayout>& payloadLayout) {
    auto payload =
        makeRows({"payload_string"}, {makeStringVector({value, value, value})});
    payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
    RadixSortRunStorage storage(
        pool_.get(),
        RadixSortKeyLayout::fromKind(
            RadixSortKeyLayoutKind::kKeyWithPayloadVariable32),
        4,
        64,
        payloadLayout,
        4,
        2 << 20);
    PayloadRowBatch rows;
    PayloadRowWriter payloadWriter;
    payloadWriter.append(*payload, storage, rows);
    for (vector_size_t row = 0; row < payload->size(); ++row) {
      storage.append("key_" + std::to_string(row), rows.rowAt(row));
    }
    return spillSingleRun(storage, payloadLayout.get());
  }
};

TEST_F(RadixSortSpillRowTest, fixedPayloadRoundTrip) {
  auto payload = makeRows(
      {"payload_bigint", "payload_double"},
      {makeVector<int64_t>(BIGINT(), {7, std::nullopt, 1}),
       makeVector<double>(DOUBLE(), {1.5, 2.5, std::nullopt})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage arena(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 64);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, arena, payloadBatch);
  arena.append(
      std::string_view("\x10\x00\x00\x00\x00\x00\x00\x01", 8),
      payloadBatch.rowAt(0));

  const auto* key = arena.keyDataAt(0);
  const auto serializedSize =
      RadixSortSpillRow::sizeForSerialize(keyLayout, payloadLayout.get(), key);
  const auto rowSize = serializedSize.totalSize;
  std::vector<char> buffer(rowSize);
  RadixSortSpillRow::serialize(
      keyLayout, payloadLayout.get(), key, buffer.data());
  auto row = RadixSortSpillRow(buffer.data());
  RadixSortSpillRunMeta meta{
      keyLayout, static_cast<uint32_t>(payloadLayout->rowWidth())};
  row.validate(meta);
  EXPECT_EQ(row.header().totalSize, rowSize);
  EXPECT_EQ(row.trustedKeySize(meta), *keyLayout.payloadOffset());
  EXPECT_EQ(row.trustedPayloadHeapSize(meta), 0);
  row.trustedRestoreKeyDataPointer(meta);
  EXPECT_EQ(
      row.trustedPayloadFixed(meta),
      buffer.data() + RadixSortSpillRow::kHeaderSize +
          row.trustedKeySize(meta));

  std::array<char*, 1> rows{row.trustedPayloadFixed(meta)};
  RowVectorPtr output;
  PayloadRowReader::gather(
      *payloadLayout, std::span<char* const>(rows), pool_.get(), output);
  auto expected = makeRows(
      {"payload_bigint", "payload_double"},
      {makeVector<int64_t>(BIGINT(), {7}),
       makeVector<double>(DOUBLE(), {1.5})});
  expectEquivalent(*expected, *output);
}

TEST_F(RadixSortSpillRowTest, variableKeyAndPayloadRoundTrip) {
  const std::string longKey(96, 'k');
  const std::string longText(80, 'x');
  auto payload = makeRows(
      {"payload_string", "payload_row"},
      {makeStringVector({longText, std::nullopt}),
       makeRows(
           {"nested_bigint", "nested_string"},
           {makeVector<int64_t>(BIGINT(), {11, 22}),
            makeStringVector({std::string(40, 'n'), std::string("short")})})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  RadixSortRunStorage arena(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, arena, payloadBatch);
  arena.append(longKey, payloadBatch.rowAt(0));

  const auto* key = arena.keyDataAt(0);
  const auto serializedSize =
      RadixSortSpillRow::sizeForSerialize(keyLayout, payloadLayout.get(), key);
  const auto rowSize = serializedSize.totalSize;
  std::vector<char> buffer(rowSize);
  RadixSortSpillRow::serialize(
      keyLayout, payloadLayout.get(), key, buffer.data());
  auto row = RadixSortSpillRow(buffer.data());
  RadixSortSpillRunMeta meta{
      keyLayout, static_cast<uint32_t>(payloadLayout->rowWidth())};
  row.validate(meta);
  const auto serializedKeyFixedSize =
      *keyLayout.dataOffset() + sizeof(uint64_t);
  EXPECT_EQ(serializedSize.keyHeapSize, longKey.size());
  EXPECT_EQ(serializedSize.keySize, serializedKeyFixedSize + longKey.size());
  EXPECT_EQ(
      rowSize,
      RadixSortSpillRow::kHeaderSize + serializedKeyFixedSize + longKey.size() +
          payloadLayout->rowWidth() + payloadBatch.heapSizeAt(0));
  EXPECT_EQ(row.trustedKeySize(meta), serializedKeyFixedSize + longKey.size());
  EXPECT_EQ(row.trustedPayloadHeapSize(meta), payloadBatch.heapSizeAt(0));

  const auto* keyRecord = row.trustedKeyBytes(meta).data();
  const auto keyOffset =
      loadUnaligned<uint64_t>(keyRecord + *keyLayout.dataOffset());
  EXPECT_EQ(keyOffset, RadixSortSpillRow::kHeaderSize + serializedKeyFixedSize);
  EXPECT_EQ(
      std::string_view(buffer.data() + keyOffset, longKey.size()), longKey);

  auto* fixed = row.trustedPayloadFixed(meta);
  EXPECT_EQ(
      fixed,
      buffer.data() + RadixSortSpillRow::kHeaderSize + serializedKeyFixedSize +
          longKey.size());
  const auto stringValue =
      loadUnaligned<StringView>(fixed + payloadLayout->columns()[0].offset);
  ASSERT_FALSE(stringValue.isInline());
  EXPECT_LT(
      reinterpret_cast<uintptr_t>(loadUnaligned<const char*>(
          reinterpret_cast<const char*>(&stringValue) + sizeof(uint64_t))),
      rowSize);
  const auto nestedValue = loadUnaligned<PayloadVarlenRef>(
      fixed + payloadLayout->columns()[1].offset);
  EXPECT_LT(reinterpret_cast<uintptr_t>(nestedValue.data), rowSize);

  row.trustedRestoreKeyDataPointer(meta);
  const auto restoredKey =
      RadixSortKey(keyLayout, row.trustedKeyBytes(meta).data());
  EXPECT_EQ(
      std::string_view(restoredKey.fullKeyData(), longKey.size()), longKey);
  EXPECT_EQ(restoredKey.fullKeyData(), buffer.data() + keyOffset);
  row.trustedRestorePayloadPointers(meta, *payloadLayout);
  const auto restoredString =
      loadUnaligned<StringView>(fixed + payloadLayout->columns()[0].offset);
  EXPECT_EQ(
      std::string(restoredString.data(), restoredString.size()), longText);
  const auto restoredNested = loadUnaligned<PayloadVarlenRef>(
      fixed + payloadLayout->columns()[1].offset);
  EXPECT_GE(restoredNested.data, buffer.data());
  EXPECT_LT(restoredNested.data, buffer.data() + buffer.size());

  std::array<char*, 1> rows{fixed};
  RowVectorPtr output;
  PayloadRowReader::gather(
      *payloadLayout, std::span<char* const>(rows), pool_.get(), output);
  auto expected = makeRows(
      {"payload_string", "payload_row"},
      {makeStringVector({longText}),
       makeRows(
           {"nested_bigint", "nested_string"},
           {makeVector<int64_t>(BIGINT(), {11}),
            makeStringVector({std::string(40, 'n')})})});
  expectEquivalent(*expected, *output);
}

TEST_F(RadixSortSpillRowTest, writerReaderRoundTripWithoutCompression) {
  auto payload = makeRows(
      {"payload_string", "payload_bigint"},
      {makeStringVector(
           {std::string(80, 'a'),
            std::string("short"),
            std::nullopt,
            std::string(96, 'b')}),
       makeVector<int64_t>(BIGINT(), {3, 1, std::nullopt, 2})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  appendAndVerify(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
      {std::string(48, 'd'), "alpha", std::string(80, 'c'), "omega"},
      payload,
      payloadLayout,
      common::CompressionKind_NONE,
      96);
}

TEST_F(RadixSortSpillRowTest, writerReaderRoundTripWithCompression) {
  for (const auto compression :
       {common::CompressionKind_LZ4, common::CompressionKind_ZSTD}) {
    SCOPED_TRACE(compression);
    auto payload = makeRows(
        {"payload_string"},
        {makeStringVector(
            {std::string(256, 'x'),
             std::string(256, 'y'),
             std::string(256, 'z')})});
    auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
    std::vector<std::string> keys;
    for (uint32_t row = 0; row < payload->size(); ++row) {
      std::array<char, sizeof(uint64_t)> key{};
      key.back() = static_cast<char>(row + 1);
      keys.emplace_back(key.data(), key.size());
    }
    appendAndVerify(
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
        keys,
        payload,
        payloadLayout,
        compression);
  }
}

TEST_F(RadixSortSpillRowTest, writerReaderRoundTripWithLargeMapPayloadAndZstd) {
  constexpr vector_size_t kRows = 96;
  std::vector<std::optional<int64_t>> deviceIds;
  std::vector<std::optional<std::string>> events;
  std::vector<std::string> keys;
  for (vector_size_t row = 0; row < kRows; ++row) {
    deviceIds.push_back(10'000 + row);
    events.push_back("event_" + std::to_string(row % 8));
    keys.push_back(*events.back() + "_" + std::to_string(row));
  }
  auto payload = makeRows(
      {"device_id", "params", "event"},
      {makeVector<int64_t>(BIGINT(), deviceIds),
       makeLargeStringStringMaps(kRows),
       makeStringVector(events)});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  appendAndVerify(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
      keys,
      payload,
      payloadLayout,
      common::CompressionKind_ZSTD);
}

TEST_F(RadixSortSpillRowTest, rejectsBlockSizesBeforeRowAllocation) {
  for (const auto& [name, header] :
       std::array<std::pair<const char*, std::array<int32_t, 2>>, 2>{{
           {"negative uncompressed size", {INT32_MIN, 1}},
           {"negative stored size", {1, INT32_MIN}},
       }}) {
    SCOPED_TRACE(name);
    auto spill = writeInlineKeySpill(RadixSortKeyLayoutKind::kKeyOnlyFixed8, 1);
    overwriteSpillValue(spill.file.path, 0, header);
    RadixSortSpillReader reader(
        spill.file, spill.meta, nullptr, pool_.get(), false);
    const auto allocations = pool_->stats().numAllocs;
    std::vector<char*> keys, payloads;
    EXPECT_THROW(reader.nextBatch(keys, payloads), BoltException);
    EXPECT_EQ(pool_->stats().numAllocs, allocations);
  }
}

TEST_F(
    RadixSortSpillRowTest,
    rejectsCompressedSizeAboveCodecBoundBeforeAllocation) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 4, 64);
  storage.append(std::string(8, 'k'));
  auto spill = spillSingleRun(storage, nullptr, common::CompressionKind_ZSTD);
  const auto uncompressedSize = readSpillValue<int32_t>(spill.file.path, 0);
  const auto invalidStoredSize =
      static_cast<int32_t>(ZSTD_compressBound(uncompressedSize) + 1);
  overwriteSpillValue(spill.file.path, sizeof(int32_t), invalidStoredSize);
  spill.file.size = kBlockHeaderSize + invalidStoredSize;
  std::filesystem::resize_file(spill.file.path, spill.file.size);

  RadixSortSpillReader reader(
      spill.file, spill.meta, nullptr, pool_.get(), false);
  const auto allocations = pool_->stats().numAllocs;
  std::vector<char*> keys, payloads;
  EXPECT_THROW(reader.nextBatch(keys, payloads), BoltException);
  EXPECT_EQ(pool_->stats().numAllocs, allocations);
}

TEST_F(RadixSortSpillRowTest, writeRowsOutputStageRoundTripWithPayload) {
  auto payload = makeRows(
      {"payload_string", "payload_bigint"},
      {makeStringVector(
           {std::string(80, 'a'),
            std::string(96, 'b'),
            std::string("short"),
            std::string(112, 'c')}),
       makeVector<int64_t>(BIGINT(), {10, 20, 30, 40})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  appendAndVerify(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
      {"k3", "k1", "k4", "k2"},
      payload,
      payloadLayout,
      common::CompressionKind_LZ4,
      128,
      true);
}

TEST_F(RadixSortSpillRowTest, fixedAndInlineVariableFastPathRoundTrip) {
  const auto payload = makeRows(
      {"payload_bigint", "payload_double"},
      {makeVector<int64_t>(BIGINT(), {10, std::nullopt, 30}),
       makeVector<double>(DOUBLE(), {1.5, -0.0, std::nullopt})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  for (const auto kind : kLayouts) {
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    const auto keySize = RadixSortKeyLayout::fromKind(kind).inlineCapacity();
    appendAndVerify(
        kind,
        {
            std::string(keySize - 1, 'a'),
            std::string(keySize, '\0'),
            std::string(keySize, 'z'),
        },
        payload,
        payloadLayout);
  }
}

TEST_F(
    RadixSortSpillRowTest,
    oversizedFixedAndInlineVariableFastPathRowsRoundTrip) {
  constexpr uint32_t kColumns = 132'000;
  std::vector<TypePtr> payloadTypes(kColumns, BIGINT());
  auto payloadLayout = PayloadRowLayout::create(ROW(std::move(payloadTypes)));
  ASSERT_GT(payloadLayout->rowWidth(), 1 << 20);
  auto payload =
      AlignedBuffer::allocate<char>(payloadLayout->rowWidth(), pool_.get());
  auto* rawPayload = payload->asMutable<char>();
  std::memset(rawPayload, 0xff, payloadLayout->nullBytes());
  for (uint32_t column = 0; column < payloadLayout->columns().size();
       ++column) {
    storeUnaligned<int64_t>(
        rawPayload + payloadLayout->columns()[column].offset,
        static_cast<int64_t>(column));
  }

  for (const auto kind :
       {RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32}) {
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    expectOversizedRoundTrip(kind, payloadLayout, rawPayload);
  }
}

TEST_F(RadixSortSpillRowTest, mixedInlineAndHeapVariableBlockRoundTrip) {
  const auto payload = makeRows(
      {"payload_bigint", "payload_double"},
      {makeVector<int64_t>(BIGINT(), {10, std::nullopt, 30}),
       makeVector<double>(DOUBLE(), {1.5, -0.0, std::nullopt})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  for (const auto kind : kLayouts) {
    auto keyLayout = RadixSortKeyLayout::fromKind(kind);
    if (!keyLayout.isVariable()) {
      continue;
    }
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    const auto capacity = keyLayout.inlineCapacity();
    appendAndVerify(
        kind,
        {std::string(capacity == 0 ? 0 : capacity - 1, 'a'),
         std::string(capacity, 'b'),
         std::string(capacity + 1, 'c')},
        payload,
        payloadLayout);
  }
}

TEST_F(
    RadixSortSpillRowTest,
    readerRetainsPreviousMultiBlockPayloadsUntilReleased) {
  const std::string value((1 << 20) + 256, 'r');
  std::shared_ptr<const PayloadRowLayout> payloadLayout;
  auto spill = writeLargePayloadSpill(value, payloadLayout);
  RadixSortSpillReader reader(
      spill.file, spill.meta, payloadLayout.get(), pool_.get(), false);
  std::vector<char*> keys;
  std::vector<char*> firstPayloads;
  ASSERT_TRUE(reader.nextBatch(keys, firstPayloads));

  std::vector<char*> nextPayloads;
  ASSERT_TRUE(reader.nextBatch(keys, nextPayloads));

  expectStringPayload(*payloadLayout, firstPayloads, value);
  reader.releaseRetainedBuffers(false);
}

TEST_F(RadixSortSpillRowTest, readerReusesReleasedRowBuffer) {
  const std::string value((1 << 20) - 1, 'q');
  std::shared_ptr<const PayloadRowLayout> payloadLayout;
  auto spill = writeLargePayloadSpill(value, payloadLayout);
  RadixSortSpillReadBufferCache bufferCache;
  RadixSortSpillReader reader(
      spill.file,
      spill.meta,
      payloadLayout.get(),
      pool_.get(),
      false,
      &bufferCache);
  std::vector<char*> keys;
  std::vector<char*> firstPayloads;
  ASSERT_TRUE(reader.nextBatch(keys, firstPayloads));

  std::vector<char*> secondPayloads;
  ASSERT_TRUE(reader.nextBatch(keys, secondPayloads));
  reader.releaseRetainedBuffers(false);

  const auto allocationsBeforeThird = pool_->stats().numAllocs;
  std::vector<char*> thirdPayloads;
  ASSERT_TRUE(reader.nextBatch(keys, thirdPayloads));
  EXPECT_EQ(pool_->stats().numAllocs, allocationsBeforeThird);

  expectStringPayload(*payloadLayout, secondPayloads, value);

  reader.releaseRetainedBuffers(false);
  expectStringPayload(*payloadLayout, thirdPayloads, value);

  RadixSortSpillReader secondReader(
      spill.file,
      spill.meta,
      payloadLayout.get(),
      pool_.get(),
      false,
      &bufferCache);
  std::vector<char*> sharedPayloads;
  const auto allocationsBeforeSharedRead = pool_->stats().numAllocs;
  ASSERT_TRUE(secondReader.nextBatch(keys, sharedPayloads));
  EXPECT_EQ(pool_->stats().numAllocs, allocationsBeforeSharedRead);
  EXPECT_EQ(bufferCache.rowBuffer, nullptr);
}

TEST_F(RadixSortSpillRowTest, mergerHandlesStreamCountsAndExhaustion) {
  auto layout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  const std::vector<std::vector<int64_t>> cases{
      {1}, {1, 2}, {1, 2, 3}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5, 6}};
  for (const auto& values : cases) {
    SCOPED_TRACE(values.size());
    verifyMerge(layout, values, true);
  }
  verifyMerge(layout, {7, 7, 7, 7, 7, 7});
}

TEST_F(RadixSortSpillRowTest, fileMergeStreamRemovesOwnedFile) {
  enum class Failure { kNone, kMissing };
  for (const auto& [name, failure] : std::array{
           std::pair{"on destruction", Failure::kNone},
           std::pair{"reader cannot open file", Failure::kMissing}}) {
    SCOPED_TRACE(name);
    auto spill = writeInlineKeySpill(RadixSortKeyLayoutKind::kKeyOnlyFixed8, 1);
    const auto spillPath = spill.file.path;
    const auto missingPath = spillPath + ".missing";
    ASSERT_TRUE(std::filesystem::exists(spillPath));
    if (failure == Failure::kMissing) {
      std::filesystem::rename(spillPath, missingPath);
    }
    if (failure == Failure::kNone) {
      {
        RadixSortSpillFileMergeStream stream(
            spill.file, spill.meta, nullptr, pool_.get(), false);
        ASSERT_TRUE(stream.hasData());
        EXPECT_TRUE(std::filesystem::exists(spillPath));
      }
    } else {
      EXPECT_THROW(
          RadixSortSpillFileMergeStream(
              spill.file, spill.meta, nullptr, pool_.get(), false),
          BoltException);
    }
    EXPECT_FALSE(std::filesystem::exists(spillPath));
    if (failure == Failure::kMissing) {
      std::filesystem::rename(missingPath, spillPath);
    }
  }
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
