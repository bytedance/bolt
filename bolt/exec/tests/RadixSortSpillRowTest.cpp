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
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/radixsort/PayloadRowReader.h"
#include "bolt/exec/radixsort/PayloadRowWriter.h"
#include "bolt/exec/radixsort/RadixSortKeyCodec.h"
#include "bolt/exec/radixsort/RadixSortSpill.h"
#include "bolt/exec/radixsort/RadixSortSpillRow.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::exec::radixsort::test {
namespace {

class RadixSortSpillRowTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    filesystems::registerLocalFileSystem();
  }

 protected:
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("radix-sort-spill-row-test")};

  template <typename T>
  FlatVectorPtr<T> makeVector(
      const TypePtr& type,
      const std::vector<std::optional<T>>& values) {
    auto result =
        BaseVector::create<FlatVector<T>>(type, values.size(), pool_.get());
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        result->set(row, *values[row]);
      } else {
        result->setNull(row, true);
      }
    }
    return result;
  }

  FlatVectorPtr<StringView> makeStringVector(
      const std::vector<std::optional<std::string>>& values) {
    auto result = BaseVector::create<FlatVector<StringView>>(
        VARCHAR(), values.size(), pool_.get());
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        result->set(row, StringView(*values[row]));
      } else {
        result->setNull(row, true);
      }
    }
    return result;
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
    offsets.reserve(rows);
    sizes.reserve(rows);
    keys.reserve(rows * 24);
    values.reserve(rows * 24);
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

  static CompareFlags flags() {
    return CompareFlags{
        .nullsFirst = true,
        .ascending = true,
        .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
  }

  static void expectEquivalent(
      const RowVector& expected,
      const RowVector& actual) {
    ASSERT_EQ(expected.size(), actual.size());
    ASSERT_EQ(expected.childrenSize(), actual.childrenSize());
    for (uint32_t column = 0; column < expected.childrenSize(); ++column) {
      for (vector_size_t row = 0; row < expected.size(); ++row) {
        const auto result = expected.childAt(column)->compare(
            actual.childAt(column).get(), row, row, flags());
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, 0) << "column=" << column << ", row=" << row;
      }
    }
  }

  static const char* stringPointer(const StringView& value) {
    return loadUnaligned<const char*>(
        reinterpret_cast<const char*>(&value) + sizeof(uint64_t));
  }

  common::SpillConfig::SpillIOConfig spillIOConfig(
      const std::string& directory,
      common::CompressionKind compression,
      uint64_t writeBufferSize) const {
    return common::SpillConfig::SpillIOConfig{
        [directory]() -> const std::string& { return directory; },
        [&](uint64_t) {},
        "radix-sort-spill-test",
        0,
        false,
        writeBufferSize,
        compression,
        "",
        std::nullopt};
  }

  std::vector<char*> readSpilledRows(
      const std::vector<RadixSortSpillFile>& files,
      const RadixSortSpillRunMeta& meta,
      const PayloadRowLayout* layout) {
    std::vector<char*> rows;
    for (const auto& file : files) {
      RadixSortSpillReader reader(file, meta, layout, pool_.get(), false);
      std::vector<char*> keys;
      std::vector<char*> payloads;
      while (reader.nextBatch(keys, payloads)) {
        for (auto* payload : payloads) {
          rows.push_back(payload);
        }
      }
    }
    return rows;
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
  PayloadRowWriter::append(*payload, arena, payloadBatch);
  arena.append(
      std::string_view("\x10\x00\x00\x00\x00\x00\x00\x01", 8),
      payloadBatch.rowAt(0));

  const auto* key = arena.keyDataAt(0);
  const auto rowSize =
      RadixSortSpillRow::sizeForSerialize(keyLayout, payloadLayout.get(), key)
          .totalSize;
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
  PayloadRowWriter::append(*payload, arena, payloadBatch);
  arena.append(longKey, payloadBatch.rowAt(0));

  const auto* key = arena.keyDataAt(0);
  const auto rowSize =
      RadixSortSpillRow::sizeForSerialize(keyLayout, payloadLayout.get(), key)
          .totalSize;
  std::vector<char> buffer(rowSize);
  RadixSortSpillRow::serialize(
      keyLayout, payloadLayout.get(), key, buffer.data());
  auto row = RadixSortSpillRow(buffer.data());
  RadixSortSpillRunMeta meta{
      keyLayout, static_cast<uint32_t>(payloadLayout->rowWidth())};
  row.validate(meta);
  EXPECT_EQ(
      row.trustedKeySize(meta), *keyLayout.payloadOffset() + longKey.size());
  EXPECT_EQ(row.trustedPayloadHeapSize(meta), payloadBatch.heapSizeAt(0));

  const auto* keyRecord = row.trustedKeyBytes(meta).data();
  const auto keyOffset =
      loadUnaligned<uint64_t>(keyRecord + *keyLayout.dataOffset());
  EXPECT_GE(keyOffset, RadixSortSpillRow::kHeaderSize);
  EXPECT_LT(keyOffset, row.header().totalSize);
  EXPECT_NE(
      reinterpret_cast<const char*>(keyOffset), key + *keyLayout.dataOffset());

  auto* fixed = row.trustedPayloadFixed(meta);
  const auto stringValue =
      loadUnaligned<StringView>(fixed + payloadLayout->columns()[0].offset);
  ASSERT_FALSE(stringValue.isInline());
  EXPECT_LT(reinterpret_cast<uintptr_t>(stringPointer(stringValue)), rowSize);
  const auto nestedValue = loadUnaligned<PayloadVarlenRef>(
      fixed + payloadLayout->columns()[1].offset);
  EXPECT_LT(reinterpret_cast<uintptr_t>(nestedValue.data), rowSize);

  row.trustedRestoreKeyDataPointer(meta);
  const auto restoredKey =
      RadixSortKey(keyLayout, row.trustedKeyBytes(meta).data());
  EXPECT_EQ(
      std::string_view(restoredKey.fullKeyData(), longKey.size()), longKey);
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

TEST_F(RadixSortSpillRowTest, rejectsCorruptHeaders) {
  RadixSortSpillRowHeader header{
      static_cast<uint32_t>(RadixSortSpillRow::kHeaderSize - 1)};
  RadixSortSpillRunMeta meta{
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8), 0};
  std::vector<char> buffer(sizeof(header) + 8);
  storeUnaligned<RadixSortSpillRowHeader>(buffer.data(), header);
  EXPECT_THROW(RadixSortSpillRow(buffer.data()).validate(meta), BoltException);

  header.totalSize = 16;
  storeUnaligned<RadixSortSpillRowHeader>(buffer.data(), header);
  EXPECT_THROW(RadixSortSpillRow(buffer.data()).validate(meta), BoltException);

  header.totalSize = 13;
  storeUnaligned<RadixSortSpillRowHeader>(buffer.data(), header);
  EXPECT_THROW(RadixSortSpillRow(buffer.data()).validate(meta), BoltException);
}

TEST_F(RadixSortSpillRowTest, writerReaderRoundTripWithoutCompression) {
  auto directory = exec::test::TempDirectoryPath::create();
  auto payload = makeRows(
      {"payload_string", "payload_bigint"},
      {makeStringVector(
           {std::string(80, 'a'),
            std::string("short"),
            std::nullopt,
            std::string(96, 'b')}),
       makeVector<int64_t>(BIGINT(), {3, 1, std::nullopt, 2})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter::append(*payload, storage, payloadBatch);
  const std::array<std::string, 4> keys{
      std::string(48, 'd'),
      std::string("alpha"),
      std::string(80, 'c'),
      std::string("omega")};
  for (uint32_t row = 0; row < keys.size(); ++row) {
    storage.append(keys[row], payloadBatch.rowAt(row));
  }

  folly::Synchronized<common::SpillStats> stats;
  RadixSortSpillWriter writer(
      directory->path + "/spill",
      spillIOConfig(directory->path, common::CompressionKind_NONE, 96),
      pool_.get(),
      &stats);
  auto files = writer.writeRun(storage, payloadLayout.get());
  ASSERT_EQ(files.size(), 1);
  uint64_t spilledRows = 0;
  for (const auto& file : files) {
    spilledRows += file.rowCount;
    EXPECT_GT(file.size, 0);
  }
  EXPECT_EQ(spilledRows, storage.size());
  EXPECT_GT(stats.copy().spilledBytes, 0);

  RadixSortSpillRunMeta meta{
      keyLayout, static_cast<uint32_t>(payloadLayout->rowWidth())};
  for (const auto& file : files) {
    RadixSortSpillReader reader(
        file, meta, payloadLayout.get(), pool_.get(), false);
    std::vector<char*> keyRows;
    std::vector<char*> payloadRows;
    std::vector<char*> batchKeys;
    std::vector<char*> batchPayloads;
    while (reader.nextBatch(batchKeys, batchPayloads)) {
      keyRows.insert(keyRows.end(), batchKeys.begin(), batchKeys.end());
      payloadRows.insert(
          payloadRows.end(), batchPayloads.begin(), batchPayloads.end());
    }
    ASSERT_EQ(keyRows.size(), storage.size());
    ASSERT_EQ(payloadRows.size(), storage.size());
    for (uint32_t row = 0; row < keyRows.size(); ++row) {
      EXPECT_EQ(
          RadixSortKey(keyLayout, keyRows[row])
              .compare(RadixSortKey(keyLayout, storage.keyDataAt(row))),
          0);
    }
    RowVectorPtr output;
    PayloadRowReader::gather(
        *payloadLayout,
        std::span<char* const>(payloadRows.data(), payloadRows.size()),
        pool_.get(),
        output);
    expectEquivalent(*payload, *output);
  }
}

TEST_F(RadixSortSpillRowTest, mixedVariableKeyBlockFallsBackToGenericRead) {
  auto directory = exec::test::TempDirectoryPath::create();
  auto payload = makeRows(
      {"payload_bigint"}, {makeVector<int64_t>(BIGINT(), {0, 1, 2, 3, 4, 5})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable56);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 16, 256, payloadLayout, 16, 256);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter::append(*payload, storage, payloadBatch);

  const std::array<std::string, 6> keys{
      std::string(32, 'a'),
      std::string(32, 'b'),
      std::string(32, 'c'),
      std::string(45, 'd'),
      std::string(32, 'e'),
      std::string(32, 'f')};
  for (uint32_t row = 0; row < keys.size(); ++row) {
    storage.append(keys[row], payloadBatch.rowAt(row));
  }

  folly::Synchronized<common::SpillStats> stats;
  RadixSortSpillWriter writer(
      directory->path + "/spill",
      spillIOConfig(directory->path, common::CompressionKind_NONE, 4096),
      pool_.get(),
      &stats);
  auto files = writer.writeRun(storage, payloadLayout.get());
  ASSERT_EQ(files.size(), 1);

  RadixSortSpillRunMeta meta{
      keyLayout, static_cast<uint32_t>(payloadLayout->rowWidth())};
  RadixSortSpillReader reader(
      files[0], meta, payloadLayout.get(), pool_.get(), false);
  std::vector<char*> batchKeys;
  std::vector<char*> batchPayloads;
  ASSERT_TRUE(reader.nextBatch(batchKeys, batchPayloads));
  ASSERT_EQ(batchKeys.size(), keys.size());
  ASSERT_EQ(batchPayloads.size(), keys.size());
  for (uint32_t row = 0; row < keys.size(); ++row) {
    EXPECT_EQ(
        RadixSortKey(keyLayout, batchKeys[row])
            .compare(RadixSortKey(keyLayout, storage.keyDataAt(row))),
        0)
        << "row=" << row;
  }
  EXPECT_FALSE(reader.nextBatch(batchKeys, batchPayloads));
}

TEST_F(RadixSortSpillRowTest, writerReaderRoundTripWithCompression) {
  for (const auto compression :
       {common::CompressionKind_LZ4, common::CompressionKind_ZSTD}) {
    auto directory = exec::test::TempDirectoryPath::create();
    auto payload = makeRows(
        {"payload_string"},
        {makeStringVector(
            {std::string(256, 'x'),
             std::string(256, 'y'),
             std::string(256, 'z')})});
    auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
    auto keyLayout = RadixSortKeyLayout::fromKind(
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
    RadixSortRunStorage storage(
        pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
    PayloadRowBatch payloadBatch;
    PayloadRowWriter::append(*payload, storage, payloadBatch);
    storage.append(
        std::string_view("\x00\x00\x00\x00\x00\x00\x00\x01", 8),
        payloadBatch.rowAt(0));
    storage.append(
        std::string_view("\x00\x00\x00\x00\x00\x00\x00\x02", 8),
        payloadBatch.rowAt(1));
    storage.append(
        std::string_view("\x00\x00\x00\x00\x00\x00\x00\x03", 8),
        payloadBatch.rowAt(2));

    folly::Synchronized<common::SpillStats> stats;
    RadixSortSpillWriter writer(
        directory->path + "/spill",
        spillIOConfig(directory->path, compression, 1 << 20),
        pool_.get(),
        &stats);
    auto files = writer.writeRun(storage, payloadLayout.get());
    ASSERT_EQ(files.size(), 1);
    RadixSortSpillRunMeta meta{
        keyLayout, static_cast<uint32_t>(payloadLayout->rowWidth())};
    for (const auto& file : files) {
      RadixSortSpillReader reader(
          file, meta, payloadLayout.get(), pool_.get(), false);
      std::vector<char*> payloadRows;
      std::vector<char*> keys;
      std::vector<char*> payloads;
      while (reader.nextBatch(keys, payloads)) {
        payloadRows.insert(payloadRows.end(), payloads.begin(), payloads.end());
      }
      RowVectorPtr output;
      PayloadRowReader::gather(
          *payloadLayout,
          std::span<char* const>(payloadRows.data(), payloadRows.size()),
          pool_.get(),
          output);
      expectEquivalent(*payload, *output);
    }
  }
}

TEST_F(RadixSortSpillRowTest, writerReaderRoundTripWithLargeMapPayloadAndZstd) {
  constexpr vector_size_t kRows = 96;
  auto payload = makeRows(
      {"device_id", "params", "event"},
      {makeVector<int64_t>(
           BIGINT(),
           [&]() {
             std::vector<std::optional<int64_t>> values;
             values.reserve(kRows);
             for (vector_size_t row = 0; row < kRows; ++row) {
               values.push_back(10'000 + row);
             }
             return values;
           }()),
       makeLargeStringStringMaps(kRows),
       makeStringVector([&]() {
         std::vector<std::optional<std::string>> values;
         values.reserve(kRows);
         for (vector_size_t row = 0; row < kRows; ++row) {
           values.push_back("event_" + std::to_string(row % 8));
         }
         return values;
       }())});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 32, 4096, payloadLayout, 32, 128 * 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter::append(*payload, storage, payloadBatch);
  for (vector_size_t row = 0; row < kRows; ++row) {
    storage.append(
        "event_" + std::to_string(row % 8) + "_" + std::to_string(row),
        payloadBatch.rowAt(row));
  }

  auto directory = exec::test::TempDirectoryPath::create();
  folly::Synchronized<common::SpillStats> stats;
  RadixSortSpillWriter writer(
      directory->path + "/spill",
      spillIOConfig(directory->path, common::CompressionKind_ZSTD, 1 << 20),
      pool_.get(),
      &stats);
  auto files = writer.writeRun(storage, payloadLayout.get());
  ASSERT_EQ(files.size(), 1);
  EXPECT_EQ(files[0].compressionKind, common::CompressionKind_ZSTD);
  EXPECT_EQ(files[0].rowCount, kRows);
  EXPECT_GT(stats.copy().spilledBytes, 0);
  EXPECT_GT(stats.copy().spillFlushTimeUs, 0);

  RadixSortSpillRunMeta meta{
      keyLayout, static_cast<uint32_t>(payloadLayout->rowWidth())};
  for (const auto& file : files) {
    RadixSortSpillReader reader(
        file, meta, payloadLayout.get(), pool_.get(), false);
    std::vector<char*> keys;
    std::vector<char*> payloadRows;
    std::vector<char*> batchPayloads;
    while (reader.nextBatch(keys, batchPayloads)) {
      payloadRows.insert(
          payloadRows.end(), batchPayloads.begin(), batchPayloads.end());
    }
    ASSERT_EQ(payloadRows.size(), kRows);
    RowVectorPtr output;
    PayloadRowReader::gather(
        *payloadLayout,
        std::span<char* const>(payloadRows.data(), payloadRows.size()),
        pool_.get(),
        output);
    expectEquivalent(*payload, *output);
  }
}

TEST_F(RadixSortSpillRowTest, rejectsTruncatedCompressedBlock) {
  auto payload = makeRows(
      {"payload_string"},
      {makeStringVector({std::string(256, 'x'), std::string(256, 'y')})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter::append(*payload, storage, payloadBatch);
  storage.append(
      std::string_view("\x00\x00\x00\x00\x00\x00\x00\x01", 8),
      payloadBatch.rowAt(0));
  storage.append(
      std::string_view("\x00\x00\x00\x00\x00\x00\x00\x02", 8),
      payloadBatch.rowAt(1));

  auto directory = exec::test::TempDirectoryPath::create();
  folly::Synchronized<common::SpillStats> stats;
  RadixSortSpillWriter writer(
      directory->path + "/spill",
      spillIOConfig(directory->path, common::CompressionKind_ZSTD, 1 << 20),
      pool_.get(),
      &stats);
  auto files = writer.writeRun(storage, payloadLayout.get());
  ASSERT_EQ(files.size(), 1);
  ASSERT_GT(files[0].size, 0);
  std::filesystem::resize_file(files[0].path, files[0].size - 1);
  files[0].size -= 1;

  RadixSortSpillRunMeta meta{
      keyLayout, static_cast<uint32_t>(payloadLayout->rowWidth())};
  RadixSortSpillReader reader(
      files[0], meta, payloadLayout.get(), pool_.get(), false);
  std::vector<char*> keys;
  std::vector<char*> payloads;
  EXPECT_THROW(reader.nextBatch(keys, payloads), BoltException);
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
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter::append(*payload, storage, payloadBatch);
  const std::array<std::string, 4> keys{
      std::string("k3"),
      std::string("k1"),
      std::string("k4"),
      std::string("k2")};
  std::array<const char*, 4> keyRows{};
  std::array<char*, 4> payloadRows{};
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    storage.append(keys[row], payloadBatch.rowAt(row));
    keyRows[row] = storage.keyDataAt(row);
    payloadRows[row] = payloadBatch.rowAt(row);
  }

  auto directory = exec::test::TempDirectoryPath::create();
  folly::Synchronized<common::SpillStats> stats;
  RadixSortSpillWriter writer(
      directory->path + "/spill",
      spillIOConfig(directory->path, common::CompressionKind_LZ4, 128),
      pool_.get(),
      &stats);
  writer.writeRows(
      keyLayout,
      payloadLayout.get(),
      keyRows.data(),
      payloadRows.data(),
      keyRows.size());
  auto files = writer.finishRows();
  ASSERT_EQ(files.size(), 1);

  RadixSortSpillRunMeta meta{
      keyLayout, static_cast<uint32_t>(payloadLayout->rowWidth())};
  RadixSortSpillReader reader(
      files[0], meta, payloadLayout.get(), pool_.get(), false);
  std::vector<char*> readKeys;
  std::vector<char*> readPayloads;
  ASSERT_TRUE(reader.nextBatch(readKeys, readPayloads));
  ASSERT_EQ(readKeys.size(), keys.size());
  ASSERT_EQ(readPayloads.size(), keys.size());

  RowVectorPtr output;
  PayloadRowReader::gather(
      *payloadLayout,
      std::span<char* const>(readPayloads.data(), readPayloads.size()),
      pool_.get(),
      output);
  expectEquivalent(*payload, *output);

  std::vector<char*> emptyKeys;
  std::vector<char*> emptyPayloads;
  EXPECT_FALSE(reader.nextBatch(emptyKeys, emptyPayloads));
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
