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
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/SpillFile.h"
#include "bolt/exec/radixsort/PayloadRow.h"
#include "bolt/exec/radixsort/RadixSortKeyCodec.h"
#include "bolt/exec/radixsort/RadixSortSpill.h"
#include "bolt/exec/radixsort/RadixSortSpillSections.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::exec::radixsort::test {
namespace {

struct TestRadixSortSpillBlockHeader {
  int32_t uncompressedSize;
  int32_t storedSize;
  uint32_t rowCount;
  uint32_t reserved;
  uint64_t keyRecordBytes;
  uint64_t keyHeapBytes;
  uint64_t payloadFixedBytes;
  uint64_t payloadHeapBytes;
};

static_assert(sizeof(TestRadixSortSpillBlockHeader) == 48);

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

class RadixSortSpillSectionsTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    filesystems::registerLocalFileSystem();
  }

 protected:
  static constexpr uint64_t kBlockHeaderSize =
      sizeof(TestRadixSortSpillBlockHeader);
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("radix-sort-spill-sections-test")};
  std::vector<std::shared_ptr<exec::test::TempDirectoryPath>> spillDirectories_;

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

  static const char* stringPointerBytes(const char* slot) {
    return loadUnaligned<const char*>(slot + sizeof(uint64_t));
  }

  static std::string_view slotBytes(
      const char* row,
      const PayloadRowColumnLayout& column) {
    return std::string_view(row + column.offset, column.width);
  }

  static std::vector<char> expectedDiskKeyRecord(
      const RadixSortSpillSectionMeta& meta,
      const char* sourceKey) {
    std::vector<char> expected(meta.runtimeKeyRecordSize);
    std::memcpy(expected.data(), sourceKey, expected.size());
    if (meta.hasKeyHeap) {
      storeCompactPointer(expected.data() + meta.keyDataOffset, nullptr);
    }
    if (meta.hasPayload) {
      storeCompactPointer(expected.data() + meta.keyPayloadOffset, nullptr);
    }
    return expected;
  }

  static void expectDiskKeyRecord(
      const RadixSortSpillSectionMeta& meta,
      const char* diskKey,
      const char* sourceKey) {
    const auto expected = expectedDiskKeyRecord(meta, sourceKey);
    EXPECT_EQ(std::memcmp(diskKey, expected.data(), expected.size()), 0);
    if (meta.hasKeyHeap) {
      EXPECT_EQ(loadCompactPointer(diskKey + meta.keyDataOffset), nullptr);
    }
    if (meta.hasPayload) {
      EXPECT_EQ(loadCompactPointer(diskKey + meta.keyPayloadOffset), nullptr);
    }
  }

  static void expectPayloadVariablePointersCleared(
      const RadixSortSpillSectionMeta& meta,
      const char* payloadFixed,
      const char* sourceFixed,
      const PayloadRowLayout& payloadLayout) {
    EXPECT_EQ(
        std::memcmp(payloadFixed, sourceFixed, payloadLayout.nullBytes()), 0);
    for (const auto& op : meta.payloadVariableOps) {
      const auto* slot = payloadFixed + op.offset;
      const auto* sourceSlot = sourceFixed + op.offset;
      const bool isNull =
          (static_cast<uint8_t>(sourceFixed[op.nullByte]) & op.nullMask) == 0;
      if (isNull) {
        EXPECT_TRUE(std::all_of(
            slot, slot + op.width, [](char value) { return value == 0; }));
        continue;
      }
      if (op.kind == RadixSortSpillPayloadVariableKind::kString) {
        const auto sourceValue = loadUnaligned<StringView>(sourceSlot);
        const auto diskValue = loadUnaligned<StringView>(slot);
        EXPECT_EQ(diskValue.size(), sourceValue.size());
        if (sourceValue.isInline()) {
          EXPECT_EQ(
              std::string_view(slot, op.width),
              std::string_view(sourceSlot, op.width));
        } else {
          EXPECT_EQ(
              std::memcmp(
                  diskValue.prefix(),
                  sourceValue.prefix(),
                  StringView::kPrefixSize),
              0);
          EXPECT_EQ(stringPointerBytes(slot), nullptr);
        }
        continue;
      }
      const auto sourceValue = loadUnaligned<PayloadVarlenRef>(sourceSlot);
      const auto diskValue = loadUnaligned<PayloadVarlenRef>(slot);
      EXPECT_EQ(diskValue.size, sourceValue.size);
      EXPECT_EQ(diskValue.data, nullptr);
    }
  }

  static void expectPayloadVariablePointersInSection(
      const RadixSortSpillSectionMeta& meta,
      const char* payloadFixed,
      const char* payloadHeap,
      uint64_t payloadHeapBytes) {
    auto* heapCursor = const_cast<char*>(payloadHeap);
    const auto* const heapEnd = payloadHeap + payloadHeapBytes;
    for (const auto& op : meta.payloadVariableOps) {
      auto* slot = const_cast<char*>(payloadFixed + op.offset);
      const bool isNull =
          (static_cast<uint8_t>(payloadFixed[op.nullByte]) & op.nullMask) == 0;
      if (isNull) {
        continue;
      }
      if (op.kind == RadixSortSpillPayloadVariableKind::kString) {
        const auto value = loadUnaligned<StringView>(slot);
        if (value.isInline()) {
          continue;
        }
        EXPECT_EQ(value.data(), heapCursor);
        heapCursor += value.size();
        continue;
      }
      const auto value = loadUnaligned<PayloadVarlenRef>(slot);
      if (value.size == 0) {
        EXPECT_EQ(value.data, nullptr);
        continue;
      }
      EXPECT_EQ(value.data, heapCursor);
      heapCursor += value.size;
    }
    EXPECT_EQ(heapCursor, heapEnd);
  }

  static std::vector<char> materializeSectionBlock(
      const RadixSortSpillSectionMeta& meta,
      const char* keyBase,
      vector_size_t rowCount,
      std::vector<RadixSortSpillSectionSize>& rowSizes) {
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta,
        keyBase,
        rowCount,
        std::numeric_limits<uint64_t>::max(),
        rowSizes);
    EXPECT_EQ(batchSize.rowCount, rowCount);

    std::vector<char> block(
        batchSize.keyRecordBytes + batchSize.keyHeapBytes +
        batchSize.payloadFixedBytes + batchSize.payloadHeapBytes);
    auto* keyRecords = block.data();
    auto* keyHeap = keyRecords + batchSize.keyRecordBytes;
    auto* payloadFixed = keyHeap + batchSize.keyHeapBytes;
    auto* payloadHeap = payloadFixed + batchSize.payloadFixedBytes;
    auto* keyHeapCursor = keyHeap;
    auto* payloadHeapCursor = payloadHeap;
    RadixSortSpillSections::copyRowsToSections(
        meta,
        keyBase,
        rowSizes.data(),
        rowCount,
        batchSize.keyHeapBytes,
        batchSize.payloadHeapBytes,
        keyRecords,
        keyHeapCursor,
        payloadFixed,
        payloadHeapCursor);
    EXPECT_EQ(keyHeapCursor, keyHeap + batchSize.keyHeapBytes);
    EXPECT_EQ(payloadHeapCursor, payloadHeap + batchSize.payloadHeapBytes);
    return block;
  }

  static RadixSortSpillSectionSize sizeForSingleRow(
      const RadixSortSpillSectionMeta& meta,
      const char* key) {
    std::vector<RadixSortSpillSectionSize> rowSizes;
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta, key, 1, std::numeric_limits<uint64_t>::max(), rowSizes);
    EXPECT_EQ(batchSize.rowCount, 1);
    RadixSortSpillSectionSize size{
        batchSize.totalBytes(),
        batchSize.keyHeapBytes,
        batchSize.payloadHeapBytes};
    if (!rowSizes.empty()) {
      EXPECT_EQ(rowSizes.size(), 1);
      size = rowSizes.front();
    }
    return size;
  }

  common::SpillConfig spillConfig(
      const std::string& directory,
      common::CompressionKind compression,
      uint64_t writeBufferSize,
      uint64_t maxFileSize =
          static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) const {
    common::SpillConfig config;
    config.getSpillDirPathCb = [directory]() -> const std::string& {
      return directory;
    };
    config.updateAndCheckSpillLimitCb = [&](uint64_t) {};
    config.fileNamePrefix = "radix-sort-spill-test";
    config.maxFileSize = maxFileSize;
    config.spillUringEnabled = false;
    config.writeBufferSize = writeBufferSize;
    config.compressionKind = compression;
    return config;
  }

  struct SpilledRun {
    std::shared_ptr<exec::test::TempDirectoryPath> directory;
    RadixSortSpillFile file;
    RadixSortSpillSectionMeta meta;
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

  static std::vector<char> readSpillBytes(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    BOLT_CHECK(file.good(), path);
    const auto size = std::filesystem::file_size(path);
    std::vector<char> bytes(size);
    file.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    BOLT_CHECK(file.good(), path);
    return bytes;
  }

  std::unique_ptr<SpillInputStream> makeSpillInputStream(
      const std::string& path) {
    auto fs = filesystems::getFileSystem(path, nullptr);
    std::vector<BufferPtr> readBuffers;
    readBuffers.push_back(AlignedBuffer::allocate<char>(
        (1 << 20) - AlignedBuffer::kPaddedSize, pool_.get()));
    return std::make_unique<SpillInputStream>(
        fs->openFileForRead(path), std::move(readBuffers), false);
  }

  static std::vector<std::string> directoryFiles(const std::string& path) {
    std::vector<std::string> files;
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      files.push_back(entry.path().string());
    }
    std::sort(files.begin(), files.end());
    return files;
  }

  struct UncompressedSpillBlock {
    TestRadixSortSpillBlockHeader header;
    std::vector<char> body;
  };

  UncompressedSpillBlock readUncompressedBlock(
      const RadixSortSpillFile& fileInfo) {
    std::ifstream file(fileInfo.path, std::ios::binary);
    EXPECT_TRUE(file.good());
    UncompressedSpillBlock result;
    file.read(reinterpret_cast<char*>(&result.header), sizeof(result.header));
    EXPECT_TRUE(file.good());
    EXPECT_EQ(result.header.storedSize, result.header.uncompressedSize);
    result.body.resize(result.header.uncompressedSize);
    file.read(result.body.data(), result.body.size());
    EXPECT_TRUE(file.good());
    return result;
  }

  static std::vector<char*> payloadsFromKeys(
      const RadixSortKeyLayout& keyLayout,
      const std::vector<const char*>& keys) {
    std::vector<char*> payloads;
    payloads.reserve(keys.size());
    for (const auto* key : keys) {
      payloads.push_back(
          keyLayout.hasPayload() ? RadixSortKey(keyLayout, key).payload()
                                 : nullptr);
    }
    return payloads;
  }

  static std::string fixed8Key(uint8_t value) {
    std::string key(sizeof(uint64_t), '\0');
    key.back() = static_cast<char>(value);
    return key;
  }

  SpilledRun spillSingleRun(
      const RadixSortRunStorage& storage,
      const PayloadRowLayout* payloadLayout,
      common::CompressionKind compression = common::CompressionKind_NONE,
      uint64_t writeBufferSize = 1 << 20,
      bool outputStage = false) {
    auto directory = exec::test::TempDirectoryPath::create();
    folly::Synchronized<common::SpillStats> stats;
    const auto globalStatsBefore = common::globalSpillStats();
    RadixSortSpillWriter writer(
        directory->path + "/spill",
        spillConfig(directory->path, compression, writeBufferSize),
        pool_.get(),
        &stats);
    std::vector<RadixSortSpillFile> files;
    if (outputStage) {
      std::vector<char> keyRecords(storage.size() * storage.layout().width());
      for (uint64_t row = 0; row < storage.size(); ++row) {
        std::memcpy(
            keyRecords.data() + row * storage.layout().width(),
            storage.keyDataAt(row),
            storage.layout().width());
      }
      writer.writeKeyRange(
          storage.layout(), payloadLayout, keyRecords.data(), storage.size());
      files = writer.finish();
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
    EXPECT_EQ(written.spilledRows, 0);
    EXPECT_EQ(written.spilledFiles, 1);
    EXPECT_EQ(written.spillSerializationTimeUs, 0);
    EXPECT_GT(written.spillWrites, 0);
    const auto globalDelta = common::globalSpillStats() - globalStatsBefore;
    EXPECT_EQ(globalDelta.spilledBytes, written.spilledBytes);
    EXPECT_EQ(globalDelta.spilledRows, 0);
    EXPECT_EQ(globalDelta.spilledFiles, written.spilledFiles);
    EXPECT_EQ(globalDelta.spillSerializationTimeUs, 0);
    EXPECT_EQ(globalDelta.spillWrites, written.spillWrites);
    EXPECT_TRUE(std::filesystem::exists(file.path));
    return {
        std::move(directory),
        std::move(file),
        RadixSortSpillSectionMeta::create(storage.layout(), payloadLayout)};
  }

  struct ReadRows {
    SpilledRun spill;
    std::unique_ptr<RadixSortSpillReader> reader;
    std::vector<const char*> keys;
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
    std::vector<const char*> keys;
    while (result.reader->nextBatch(keys)) {
      result.keys.insert(result.keys.end(), keys.begin(), keys.end());
      if (result.spill.meta.hasPayload) {
        result.payloads.reserve(result.payloads.size() + keys.size());
        for (const auto* key : keys) {
          result.payloads.push_back(
              RadixSortKey(result.spill.meta.keyLayout, key).payload());
        }
      }
    }
    return result;
  }

  std::vector<RadixSortSpillFile> spillRunFiles(
      const RadixSortRunStorage& storage,
      const PayloadRowLayout* payloadLayout,
      uint64_t maxFileSize,
      bool outputStage) {
    auto directory = exec::test::TempDirectoryPath::create();
    spillDirectories_.push_back(std::move(directory));
    folly::Synchronized<common::SpillStats> stats;
    const auto globalStatsBefore = common::globalSpillStats();
    RadixSortSpillWriter writer(
        spillDirectories_.back()->path + "/spill",
        spillConfig(
            spillDirectories_.back()->path,
            common::CompressionKind_NONE,
            1 << 20,
            maxFileSize),
        pool_.get(),
        &stats);

    std::vector<RadixSortSpillFile> files;
    if (outputStage) {
      std::vector<char> keyRecords(storage.size() * storage.layout().width());
      for (uint64_t row = 0; row < storage.size(); ++row) {
        std::memcpy(
            keyRecords.data() + row * storage.layout().width(),
            storage.keyDataAt(row),
            storage.layout().width());
      }
      writer.writeKeyRange(
          storage.layout(), payloadLayout, keyRecords.data(), storage.size());
      files = writer.finish();
    } else {
      files = writer.writeRun(storage, payloadLayout);
    }

    uint64_t totalSize = 0;
    uint64_t totalRows = 0;
    for (const auto& file : files) {
      EXPECT_TRUE(std::filesystem::exists(file.path));
      EXPECT_GT(file.size, 0);
      EXPECT_GT(file.rowCount, 0);
      EXPECT_EQ(file.compressionKind, common::CompressionKind_NONE);
      totalSize += file.size;
      totalRows += file.rowCount;
    }
    const auto written = stats.copy();
    EXPECT_EQ(written.spilledBytes, totalSize);
    EXPECT_EQ(written.spilledRows, 0);
    EXPECT_EQ(written.spilledFiles, files.size());
    EXPECT_EQ(written.spillSerializationTimeUs, 0);
    EXPECT_EQ(totalRows, storage.size());
    EXPECT_GT(written.spillWrites, 0);
    const auto globalDelta = common::globalSpillStats() - globalStatsBefore;
    EXPECT_EQ(globalDelta.spilledBytes, written.spilledBytes);
    EXPECT_EQ(globalDelta.spilledRows, 0);
    EXPECT_EQ(globalDelta.spilledFiles, written.spilledFiles);
    EXPECT_EQ(globalDelta.spillSerializationTimeUs, 0);
    EXPECT_EQ(globalDelta.spillWrites, written.spillWrites);
    return files;
  }

  void verifySpillFilesRoundTrip(
      const std::vector<RadixSortSpillFile>& files,
      const RadixSortRunStorage& storage,
      const PayloadRowLayout& payloadLayout) {
    auto meta =
        RadixSortSpillSectionMeta::create(storage.layout(), &payloadLayout);
    uint64_t row = 0;
    for (const auto& file : files) {
      RadixSortSpillReader reader(
          file, meta, &payloadLayout, pool_.get(), false);
      std::vector<const char*> keys;
      uint64_t fileRows = 0;
      while (reader.nextBatch(keys)) {
        for (vector_size_t i = 0; i < keys.size(); ++i) {
          ASSERT_LT(row, storage.size());
          EXPECT_EQ(
              RadixSortKey(storage.layout(), keys[i])
                  .compare(
                      RadixSortKey(storage.layout(), storage.keyDataAt(row))),
              0)
              << "row=" << row;
          auto* const payload =
              RadixSortKey(storage.layout(), keys[i]).payload();
          ASSERT_NE(payload, nullptr);
          EXPECT_EQ(
              std::memcmp(
                  payload,
                  RadixSortKey(storage.layout(), storage.keyDataAt(row))
                      .payload(),
                  payloadLayout.rowWidth()),
              0)
              << "row=" << row;
          ++row;
          ++fileRows;
        }
      }
      EXPECT_EQ(fileRows, file.rowCount);
    }
    EXPECT_EQ(row, storage.size());
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

TEST_F(
    RadixSortSpillSectionsTest,
    serdeMetadataPrecomputesKeyRecordAndPayloadVariableColumns) {
  auto payloadLayout = PayloadRowLayout::create(
      ROW({"fixed", "text", "items", "score"},
          {BIGINT(), VARCHAR(), ARRAY(INTEGER()), DOUBLE()}));
  ASSERT_EQ(payloadLayout->columns().size(), 4);
  ASSERT_EQ(payloadLayout->variableColumns().size(), 2);

  for (const auto kind : kLayouts) {
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    const auto keyLayout = RadixSortKeyLayout::fromKind(kind);
    const auto* layout = keyLayout.hasPayload() ? payloadLayout.get() : nullptr;
    const auto meta = RadixSortSpillSectionMeta::create(keyLayout, layout);

    EXPECT_EQ(meta.runtimeKeyRecordSize, keyLayout.width());
    if (keyLayout.isVariable()) {
      EXPECT_EQ(meta.keyHeapOffset, keyLayout.heapKeyOffset());
      EXPECT_EQ(meta.keySizeOffset, *keyLayout.sizeOffset());
      EXPECT_EQ(meta.keyDataOffset, *keyLayout.dataOffset());
      EXPECT_TRUE(meta.hasKeyHeap);
    } else {
      EXPECT_FALSE(meta.hasKeyHeap);
    }

    if (keyLayout.hasPayload()) {
      EXPECT_TRUE(meta.hasPayload);
      EXPECT_EQ(meta.keyPayloadOffset, *keyLayout.payloadOffset());
      EXPECT_EQ(meta.payloadFixedSize, payloadLayout->rowWidth());
      ASSERT_EQ(meta.payloadVariableOps.size(), 2);
      EXPECT_EQ(
          meta.payloadVariableOps[0].offset,
          payloadLayout->columns()[1].offset);
      EXPECT_EQ(
          meta.payloadVariableOps[0].kind,
          RadixSortSpillPayloadVariableKind::kString);
      EXPECT_EQ(
          meta.payloadVariableOps[1].offset,
          payloadLayout->columns()[2].offset);
      EXPECT_EQ(
          meta.payloadVariableOps[1].kind,
          RadixSortSpillPayloadVariableKind::kComplex);
    } else {
      EXPECT_FALSE(meta.hasPayload);
      EXPECT_EQ(meta.payloadFixedSize, 0);
      EXPECT_TRUE(meta.payloadVariableOps.empty());

      const auto ignoredPayloadMeta =
          RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
      EXPECT_FALSE(ignoredPayloadMeta.hasPayload);
      EXPECT_EQ(ignoredPayloadMeta.payloadFixedSize, 0);
      EXPECT_TRUE(ignoredPayloadMeta.payloadVariableOps.empty());
    }
  }
}

TEST_F(RadixSortSpillSectionsTest, batchSizeFixedKeyOnlyUsesFixedWidth) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed16);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 8, 64);
  for (uint32_t row = 0; row < 5; ++row) {
    std::array<char, sizeof(uint64_t)> key{};
    key.back() = static_cast<char>(row + 1);
    storage.append(std::string(key.data(), key.size()));
  }

  const auto meta = RadixSortSpillSectionMeta::create(keyLayout, nullptr);
  std::vector<RadixSortSpillSectionSize> rowSizes;
  const auto fixedRowBytes = static_cast<uint64_t>(meta.runtimeKeyRecordSize);
  auto expectSize =
      [&](uint64_t maxRowCount, uint64_t maxBytes, uint64_t expectedRows) {
        const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
            meta, storage.keyDataAt(0), maxRowCount, maxBytes, rowSizes);
        EXPECT_EQ(batchSize.rowCount, expectedRows);
        EXPECT_EQ(batchSize.keyRecordBytes, expectedRows * fixedRowBytes);
        EXPECT_EQ(batchSize.keyHeapBytes, 0);
        EXPECT_EQ(batchSize.payloadFixedBytes, 0);
        EXPECT_EQ(batchSize.payloadHeapBytes, 0);
        EXPECT_EQ(batchSize.totalBytes(), expectedRows * fixedRowBytes);
        EXPECT_TRUE(rowSizes.empty());
      };

  expectSize(storage.size(), 0, 0);
  expectSize(storage.size(), fixedRowBytes - 1, 0);
  expectSize(storage.size(), fixedRowBytes, 1);
  expectSize(storage.size(), fixedRowBytes * 3 - 1, 2);
  expectSize(storage.size(), fixedRowBytes * 3, 3);
  expectSize(0, fixedRowBytes * storage.size(), 0);
  expectSize(storage.size(), fixedRowBytes * storage.size(), storage.size());
}

TEST_F(RadixSortSpillSectionsTest, batchSizeFixedPayloadUsesFixedWidth) {
  auto payload = makeRows(
      {"payload_bigint", "payload_double"},
      {makeVector<int64_t>(BIGINT(), {1, 2, 3, 4}),
       makeVector<double>(DOUBLE(), {1.5, 2.5, 3.5, 4.5})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 8, 64, payloadLayout, 8, 64);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, storage, payloadBatch);
  for (vector_size_t row = 0; row < payload->size(); ++row) {
    std::array<char, sizeof(uint64_t)> key{};
    key.back() = static_cast<char>(row + 1);
    storage.append(
        std::string(key.data(), key.size()), payloadBatch.rowAt(row));
  }

  const auto meta =
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  std::vector<RadixSortSpillSectionSize> rowSizes;
  const auto fixedRowBytes =
      static_cast<uint64_t>(meta.runtimeKeyRecordSize) + meta.payloadFixedSize;
  auto expectSize = [&](uint64_t maxBytes, uint64_t expectedRows) {
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta, storage.keyDataAt(0), storage.size(), maxBytes, rowSizes);
    EXPECT_EQ(batchSize.rowCount, expectedRows);
    EXPECT_EQ(
        batchSize.keyRecordBytes,
        expectedRows * static_cast<uint64_t>(meta.runtimeKeyRecordSize));
    EXPECT_EQ(batchSize.keyHeapBytes, 0);
    EXPECT_EQ(
        batchSize.payloadFixedBytes, expectedRows * meta.payloadFixedSize);
    EXPECT_EQ(batchSize.payloadHeapBytes, 0);
    EXPECT_EQ(batchSize.totalBytes(), expectedRows * fixedRowBytes);
    EXPECT_TRUE(rowSizes.empty());
  };

  expectSize(0, 0);
  expectSize(fixedRowBytes - 1, 0);
  expectSize(fixedRowBytes, 1);
  expectSize(fixedRowBytes * 2 - 1, 1);
  expectSize(fixedRowBytes * 2, 2);
  expectSize(fixedRowBytes * storage.size(), storage.size());
}

TEST_F(RadixSortSpillSectionsTest, batchSizeVariableKeyStopsAtByteBoundary) {
  auto keyLayout = RadixSortKeyLayout::select(std::nullopt, false, 7);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 8, 64);
  const std::vector<std::string> keys{
      std::string("prefix_") + std::string(13, 'a'),
      std::string("prefix_") + std::string(18, 'b'),
      std::string("prefix_") + std::string(23, 'c')};
  for (const auto& key : keys) {
    storage.append(key);
  }

  const auto meta = RadixSortSpillSectionMeta::create(keyLayout, nullptr);
  std::vector<RadixSortSpillSectionSize> rowSizes;
  std::vector<RadixSortSpillSectionSize> expected;
  for (const auto& key : keys) {
    expected.push_back(RadixSortSpillSectionSize{
        meta.runtimeKeyRecordSize + key.size() - keyLayout.heapKeyOffset(),
        key.size() - keyLayout.heapKeyOffset(),
        0});
  }
  auto expectSize = [&](uint64_t maxBytes, uint64_t expectedRows) {
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta, storage.keyDataAt(0), storage.size(), maxBytes, rowSizes);
    EXPECT_EQ(batchSize.rowCount, expectedRows);
    ASSERT_EQ(rowSizes.size(), expectedRows);
    uint64_t expectedKeyHeapBytes = 0;
    uint64_t expectedTotalBytes = 0;
    for (uint64_t row = 0; row < expectedRows; ++row) {
      EXPECT_EQ(rowSizes[row].totalSize, expected[row].totalSize);
      EXPECT_EQ(rowSizes[row].keyHeapSize, expected[row].keyHeapSize);
      EXPECT_EQ(rowSizes[row].payloadHeapSize, 0);
      expectedKeyHeapBytes += expected[row].keyHeapSize;
      expectedTotalBytes += expected[row].totalSize;
    }
    EXPECT_EQ(
        batchSize.keyRecordBytes,
        expectedRows * static_cast<uint64_t>(meta.runtimeKeyRecordSize));
    EXPECT_EQ(batchSize.keyHeapBytes, expectedKeyHeapBytes);
    EXPECT_EQ(batchSize.payloadFixedBytes, 0);
    EXPECT_EQ(batchSize.payloadHeapBytes, 0);
    EXPECT_EQ(batchSize.totalBytes(), expectedTotalBytes);
  };

  expectSize(expected[0].totalSize - 1, 0);
  expectSize(expected[0].totalSize, 1);
  expectSize(expected[0].totalSize + expected[1].totalSize - 1, 1);
  expectSize(expected[0].totalSize + expected[1].totalSize, 2);
  expectSize(
      expected[0].totalSize + expected[1].totalSize + expected[2].totalSize, 3);
}

TEST_F(RadixSortSpillSectionsTest, batchSizeVariablePayloadWithNoHeapBytes) {
  auto payload = makeRows(
      {"payload_string"},
      {makeStringVector({"a", std::nullopt, std::string("short")})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  ASSERT_TRUE(payloadLayout->hasVariableFields());
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 8, 64, payloadLayout, 8, 64);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, storage, payloadBatch);
  for (vector_size_t row = 0; row < payload->size(); ++row) {
    std::array<char, sizeof(uint64_t)> key{};
    key.back() = static_cast<char>(row + 1);
    storage.append(
        std::string(key.data(), key.size()), payloadBatch.rowAt(row));
    ASSERT_EQ(payloadBatch.heapSizeAt(row), 0);
  }

  const auto meta =
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  std::vector<RadixSortSpillSectionSize> rowSizes;
  const auto fixedRowBytes =
      static_cast<uint64_t>(meta.runtimeKeyRecordSize) + meta.payloadFixedSize;
  const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
      meta,
      storage.keyDataAt(0),
      storage.size(),
      fixedRowBytes * storage.size(),
      rowSizes);
  EXPECT_EQ(batchSize.rowCount, storage.size());
  EXPECT_EQ(batchSize.keyHeapBytes, 0);
  EXPECT_EQ(batchSize.payloadHeapBytes, 0);
  EXPECT_EQ(batchSize.totalBytes(), fixedRowBytes * storage.size());
  ASSERT_EQ(rowSizes.size(), storage.size());
  for (const auto& rowSize : rowSizes) {
    EXPECT_EQ(rowSize.totalSize, fixedRowBytes);
    EXPECT_EQ(rowSize.keyHeapSize, 0);
    EXPECT_EQ(rowSize.payloadHeapSize, 0);
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    batchSizeVariableKeyAndPayloadStopsAtCombinedBoundary) {
  const std::vector<std::string> keys{
      std::string("pre") + std::string(64, 'a'),
      std::string("pre") + std::string(72, 'b')};
  const std::vector<std::string> values{
      std::string(80, 'x'), std::string(96, 'y')};
  auto payload =
      makeRows({"payload_string"}, {makeStringVector({values[0], values[1]})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::select(std::nullopt, true, 3);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 8, 64, payloadLayout, 8, 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, storage, payloadBatch);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    storage.append(keys[row], payloadBatch.rowAt(row));
  }

  const auto meta =
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  const std::array expected{
      sizeForSingleRow(meta, storage.keyDataAt(0)),
      sizeForSingleRow(meta, storage.keyDataAt(1))};
  std::vector<RadixSortSpillSectionSize> rowSizes;
  auto expectSize = [&](uint64_t maxBytes, uint64_t expectedRows) {
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta, storage.keyDataAt(0), storage.size(), maxBytes, rowSizes);
    EXPECT_EQ(batchSize.rowCount, expectedRows);
    ASSERT_EQ(rowSizes.size(), expectedRows);
    uint64_t expectedKeyHeapBytes = 0;
    uint64_t expectedPayloadHeapBytes = 0;
    uint64_t expectedTotalBytes = 0;
    for (uint64_t row = 0; row < expectedRows; ++row) {
      EXPECT_EQ(rowSizes[row].totalSize, expected[row].totalSize);
      EXPECT_EQ(rowSizes[row].keyHeapSize, expected[row].keyHeapSize);
      EXPECT_EQ(rowSizes[row].payloadHeapSize, expected[row].payloadHeapSize);
      expectedKeyHeapBytes += expected[row].keyHeapSize;
      expectedPayloadHeapBytes += expected[row].payloadHeapSize;
      expectedTotalBytes += expected[row].totalSize;
    }
    EXPECT_EQ(
        batchSize.keyRecordBytes,
        expectedRows * static_cast<uint64_t>(meta.runtimeKeyRecordSize));
    EXPECT_EQ(
        batchSize.payloadFixedBytes, expectedRows * meta.payloadFixedSize);
    EXPECT_EQ(batchSize.keyHeapBytes, expectedKeyHeapBytes);
    EXPECT_EQ(batchSize.payloadHeapBytes, expectedPayloadHeapBytes);
    EXPECT_EQ(batchSize.totalBytes(), expectedTotalBytes);
  };

  expectSize(expected[0].totalSize - 1, 0);
  expectSize(expected[0].totalSize, 1);
  expectSize(expected[0].totalSize + expected[1].totalSize - 1, 1);
  expectSize(expected[0].totalSize + expected[1].totalSize, 2);
}

TEST_F(RadixSortSpillSectionsTest, fixedKeyOnlyRowsHaveNoHeaderOrPointers) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed16);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 4, 64);
  storage.append(std::string_view("\x01\x02\x03\x04\x05\x06\x07\x08", 8));
  storage.append(std::string_view("\x10\x20\x30\x40\x50\x60\x70\x80", 8));

  auto spill = spillSingleRun(storage, nullptr);
  const auto expectedBodySize = storage.size() * keyLayout.width();
  ASSERT_EQ(spill.file.size, kBlockHeaderSize + expectedBodySize);
  EXPECT_EQ(spill.file.rowCount, storage.size());

  const auto block = readUncompressedBlock(spill.file);
  const auto& header = block.header;
  EXPECT_EQ(header.uncompressedSize, expectedBodySize);
  EXPECT_EQ(header.storedSize, expectedBodySize);
  EXPECT_EQ(header.rowCount, storage.size());
  EXPECT_EQ(header.reserved, 0);
  EXPECT_EQ(header.keyRecordBytes, expectedBodySize);
  EXPECT_EQ(header.keyHeapBytes, 0);
  EXPECT_EQ(header.payloadFixedBytes, 0);
  EXPECT_EQ(header.payloadHeapBytes, 0);
  EXPECT_EQ(
      header.uncompressedSize,
      header.keyRecordBytes + header.keyHeapBytes + header.payloadFixedBytes +
          header.payloadHeapBytes);
  ASSERT_EQ(block.body.size(), expectedBodySize);
  EXPECT_EQ(
      std::memcmp(block.body.data(), storage.keyDataAt(0), keyLayout.width()),
      0);
  EXPECT_EQ(
      std::memcmp(
          block.body.data() + keyLayout.width(),
          storage.keyDataAt(1),
          keyLayout.width()),
      0);

  RadixSortSpillReader reader(
      spill.file, spill.meta, nullptr, pool_.get(), false);
  std::vector<const char*> keys;
  ASSERT_TRUE(reader.nextBatch(keys));
  ASSERT_EQ(keys.size(), storage.size());
  EXPECT_EQ(keys[1], keys[0] + keyLayout.width());
  EXPECT_EQ(RadixSortKey(keyLayout, keys[0]).payload(), nullptr);
  EXPECT_EQ(RadixSortKey(keyLayout, keys[1]).payload(), nullptr);
  EXPECT_EQ(
      RadixSortKey(keyLayout, keys[0])
          .compare(RadixSortKey(keyLayout, storage.keyDataAt(0))),
      0);
  EXPECT_EQ(
      RadixSortKey(keyLayout, keys[1])
          .compare(RadixSortKey(keyLayout, storage.keyDataAt(1))),
      0);
}

TEST_F(RadixSortSpillSectionsTest, fixedPayloadRoundTrip) {
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
  auto meta = RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  const auto serializedSize = sizeForSingleRow(meta, key);
  EXPECT_EQ(serializedSize.keyHeapSize, 0);
  EXPECT_EQ(serializedSize.payloadHeapSize, 0);
  EXPECT_EQ(
      serializedSize.totalSize,
      meta.runtimeKeyRecordSize + meta.payloadFixedSize);

  std::vector<RadixSortSpillSectionSize> rowSizes;
  auto block = materializeSectionBlock(meta, key, 1, rowSizes);
  ASSERT_TRUE(rowSizes.empty());
  auto* keyRecords = block.data();
  auto* keyHeap = keyRecords + meta.runtimeKeyRecordSize;
  auto* payloadFixed = keyHeap + serializedSize.keyHeapSize;
  auto* payloadHeap = payloadFixed + meta.payloadFixedSize;

  expectDiskKeyRecord(meta, keyRecords, key);
  EXPECT_EQ(
      std::memcmp(
          payloadFixed, payloadBatch.rowAt(0), payloadLayout->rowWidth()),
      0);
  EXPECT_EQ(payloadHeap, block.data() + block.size());

  char* keyHeapCursor = keyHeap;
  char* payloadHeapCursor = payloadHeap;
  std::vector<const char*> restoredKeys;
  const auto restored = RadixSortSpillSections::restorePointersInSectionRows(
      meta,
      keyRecords,
      1,
      keyHeapCursor,
      keyHeap,
      payloadFixed,
      payloadHeapCursor,
      payloadHeap,
      restoredKeys);
  ASSERT_TRUE(restored);
  ASSERT_EQ(restoredKeys.size(), 1);
  EXPECT_EQ(restoredKeys[0], keyRecords);
  EXPECT_EQ(keyHeapCursor, keyHeap);
  EXPECT_EQ(payloadHeapCursor, payloadHeap);
  EXPECT_EQ(
      RadixSortKey(keyLayout, keyRecords).compare(RadixSortKey(keyLayout, key)),
      0);
  auto* restoredPayload = RadixSortKey(keyLayout, keyRecords).payload();
  EXPECT_EQ(restoredPayload, payloadFixed);

  std::array<char*, 1> rows{restoredPayload};
  RowVectorPtr output;
  PayloadRowReader::gather(
      *payloadLayout, std::span<char* const>(rows), pool_.get(), output);
  auto expected = makeRows(
      {"payload_bigint", "payload_double"},
      {makeVector<int64_t>(BIGINT(), {7}),
       makeVector<double>(DOUBLE(), {1.5})});
  expectEquivalent(*expected, *output);
}

TEST_F(
    RadixSortSpillSectionsTest,
    variableKeyOnlyUsesExistingSizeWithoutHeader) {
  auto keyLayout = RadixSortKeyLayout::select(std::nullopt, false, 7);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 4, 64);
  const std::string key = std::string("prefix_") + std::string(64, 'k');
  storage.append(key);

  const auto* storedKey = storage.keyDataAt(0);
  const auto meta = RadixSortSpillSectionMeta::create(keyLayout, nullptr);
  const auto size = sizeForSingleRow(meta, storedKey);
  EXPECT_EQ(size.keyHeapSize, key.size() - keyLayout.heapKeyOffset());
  EXPECT_EQ(size.payloadHeapSize, 0);
  EXPECT_EQ(size.totalSize, meta.runtimeKeyRecordSize + size.keyHeapSize);

  std::vector<RadixSortSpillSectionSize> rowSizes;
  auto block = materializeSectionBlock(meta, storedKey, 1, rowSizes);
  ASSERT_EQ(rowSizes.size(), 1);
  auto* keyRecords = block.data();
  auto* keyHeap = keyRecords + meta.runtimeKeyRecordSize;
  auto* keyHeapCursor = keyHeap + size.keyHeapSize;

  expectDiskKeyRecord(meta, keyRecords, storedKey);
  EXPECT_EQ(
      loadUnaligned<uint64_t>(keyRecords + *keyLayout.sizeOffset()),
      key.size());
  EXPECT_EQ(
      std::string_view(keyHeap, size.keyHeapSize),
      std::string_view(key).substr(keyLayout.heapKeyOffset()));
  EXPECT_EQ(keyHeapCursor, keyHeap + size.keyHeapSize);

  keyHeapCursor = keyHeap;
  char* payloadHeapCursor = keyHeap + size.keyHeapSize;
  std::vector<const char*> restoredKeys;
  const auto restoredSize =
      RadixSortSpillSections::restorePointersInSectionRows(
          meta,
          keyRecords,
          1,
          keyHeapCursor,
          keyHeap + size.keyHeapSize,
          nullptr,
          payloadHeapCursor,
          payloadHeapCursor,
          restoredKeys);
  ASSERT_TRUE(restoredSize);
  ASSERT_EQ(restoredKeys.size(), 1);
  EXPECT_EQ(restoredKeys[0], keyRecords);
  const auto restored = RadixSortKey(keyLayout, keyRecords);
  std::string restoredKey(
      std::string_view(keyRecords, keyLayout.heapKeyOffset()));
  restoredKey.append(restored.heapKey());
  EXPECT_EQ(restoredKey, key);
  EXPECT_EQ(restored.heapKeyData(), keyHeap);
  EXPECT_EQ(restored.payload(), nullptr);
}

TEST_F(RadixSortSpillSectionsTest, variableKeyAndPayloadRoundTrip) {
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
  auto meta = RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  std::vector<RadixSortSpillSectionSize> rowSizes;
  auto block = materializeSectionBlock(meta, key, 1, rowSizes);
  ASSERT_EQ(rowSizes.size(), 1);
  const auto& serializedSize = rowSizes[0];
  EXPECT_EQ(serializedSize.keyHeapSize, longKey.size());
  EXPECT_EQ(serializedSize.payloadHeapSize, payloadBatch.heapSizeAt(0));
  EXPECT_EQ(
      serializedSize.totalSize,
      meta.runtimeKeyRecordSize + serializedSize.keyHeapSize +
          meta.payloadFixedSize + serializedSize.payloadHeapSize);

  auto* keyRecord = block.data();
  auto* keyHeap = keyRecord + meta.runtimeKeyRecordSize;
  auto* fixed = keyHeap + serializedSize.keyHeapSize;
  auto* payloadHeap = fixed + meta.payloadFixedSize;
  expectDiskKeyRecord(meta, keyRecord, key);
  EXPECT_EQ(
      loadUnaligned<uint64_t>(keyRecord + *keyLayout.sizeOffset()),
      longKey.size());
  EXPECT_EQ(
      std::string_view(keyHeap, serializedSize.keyHeapSize),
      std::string_view(longKey));

  EXPECT_EQ(
      std::memcmp(fixed, payloadBatch.rowAt(0), payloadLayout->nullBytes()), 0);
  const auto stringValue =
      loadUnaligned<StringView>(fixed + payloadLayout->columns()[0].offset);
  ASSERT_FALSE(stringValue.isInline());
  EXPECT_EQ(stringValue.size(), longText.size());
  EXPECT_EQ(
      std::memcmp(
          stringValue.prefix(), longText.data(), StringView::kPrefixSize),
      0);
  EXPECT_EQ(
      stringPointerBytes(fixed + payloadLayout->columns()[0].offset), nullptr);
  const auto nestedValue = loadUnaligned<PayloadVarlenRef>(
      fixed + payloadLayout->columns()[1].offset);
  EXPECT_EQ(
      nestedValue.size,
      loadUnaligned<PayloadVarlenRef>(
          payloadBatch.rowAt(0) + payloadLayout->columns()[1].offset)
          .size);
  EXPECT_EQ(nestedValue.data, nullptr);

  EXPECT_EQ(
      std::memcmp(
          payloadHeap, payloadBatch.heapAt(0), payloadBatch.heapSizeAt(0)),
      0);

  char* keyHeapCursor = keyHeap;
  char* payloadHeapCursor = payloadHeap;
  std::vector<const char*> restoredKeys;
  const auto restored = RadixSortSpillSections::restorePointersInSectionRows(
      meta,
      keyRecord,
      1,
      keyHeapCursor,
      keyHeap + serializedSize.keyHeapSize,
      fixed,
      payloadHeapCursor,
      payloadHeap + serializedSize.payloadHeapSize,
      restoredKeys);
  ASSERT_TRUE(restored);
  ASSERT_EQ(restoredKeys.size(), 1);
  EXPECT_EQ(restoredKeys[0], keyRecord);
  EXPECT_EQ(keyHeapCursor, keyHeap + serializedSize.keyHeapSize);
  EXPECT_EQ(payloadHeapCursor, payloadHeap + serializedSize.payloadHeapSize);

  const auto restoredKey = RadixSortKey(keyLayout, keyRecord);
  EXPECT_EQ(restoredKey.heapKey(), longKey);
  EXPECT_EQ(restoredKey.heapKeyData(), keyHeap);
  EXPECT_EQ(restoredKey.payload(), fixed);
  const auto restoredString = loadUnaligned<StringView>(
      restoredKey.payload() + payloadLayout->columns()[0].offset);
  EXPECT_EQ(
      std::string(restoredString.data(), restoredString.size()), longText);
  const auto restoredNested = loadUnaligned<PayloadVarlenRef>(
      restoredKey.payload() + payloadLayout->columns()[1].offset);
  EXPECT_EQ(restoredNested.data, payloadHeap + longText.size());
  EXPECT_EQ(
      restoredNested.size,
      loadUnaligned<PayloadVarlenRef>(
          payloadBatch.rowAt(0) + payloadLayout->columns()[1].offset)
          .size);

  std::array<char*, 1> rows{restoredKey.payload()};
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

TEST_F(RadixSortSpillSectionsTest, writerUsesBatchSectionBlockLayout) {
  const std::vector<std::string> keys{
      std::string("key_a_") + std::string(80, 'a'),
      std::string("key_b_") + std::string(96, 'b')};
  const std::vector<std::string> values{
      std::string(72, 'x'), std::string(88, 'y')};
  auto payload =
      makeRows({"payload_string"}, {makeStringVector({values[0], values[1]})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, storage, payloadBatch);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    storage.append(keys[row], payloadBatch.rowAt(row));
  }

  auto spill = spillSingleRun(storage, payloadLayout.get());
  const auto meta =
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  const auto keyRecordBytes =
      static_cast<uint64_t>(keys.size()) * meta.runtimeKeyRecordSize;
  const auto payloadFixedBytes =
      static_cast<uint64_t>(keys.size()) * meta.payloadFixedSize;
  uint64_t keyHeapBytes = 0;
  uint64_t payloadHeapBytes = 0;
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    keyHeapBytes += sizeForSingleRow(meta, storage.keyDataAt(row)).keyHeapSize;
    payloadHeapBytes += payloadBatch.heapSizeAt(row);
  }
  const auto expectedBodySize =
      keyRecordBytes + keyHeapBytes + payloadFixedBytes + payloadHeapBytes;

  const auto block = readUncompressedBlock(spill.file);
  const auto& header = block.header;
  EXPECT_EQ(header.uncompressedSize, expectedBodySize);
  EXPECT_EQ(header.storedSize, expectedBodySize);
  EXPECT_EQ(header.rowCount, keys.size());
  EXPECT_EQ(header.reserved, 0);
  EXPECT_EQ(header.keyRecordBytes, keyRecordBytes);
  EXPECT_EQ(header.keyHeapBytes, keyHeapBytes);
  EXPECT_EQ(header.payloadFixedBytes, payloadFixedBytes);
  EXPECT_EQ(header.payloadHeapBytes, payloadHeapBytes);
  EXPECT_EQ(
      header.uncompressedSize,
      header.keyRecordBytes + header.keyHeapBytes + header.payloadFixedBytes +
          header.payloadHeapBytes);
  ASSERT_EQ(block.body.size(), expectedBodySize);
  const char* keyRecords = block.body.data();
  const char* keyHeap = keyRecords + keyRecordBytes;
  const char* payloadFixed = keyHeap + keyHeapBytes;
  const char* payloadHeap = payloadFixed + payloadFixedBytes;

  const char* keyHeapCursor = keyHeap;
  const char* payloadHeapCursor = payloadHeap;
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    SCOPED_TRACE(row);
    expectDiskKeyRecord(
        meta,
        keyRecords + static_cast<uint64_t>(row) * meta.runtimeKeyRecordSize,
        storage.keyDataAt(row));

    const auto keyView = RadixSortKey(keyLayout, storage.keyDataAt(row));
    EXPECT_EQ(
        std::string_view(keyHeapCursor, keyView.heapSize()), keyView.heapKey());
    keyHeapCursor += keyView.heapSize();

    const auto* fixedRow =
        payloadFixed + static_cast<uint64_t>(row) * meta.payloadFixedSize;
    EXPECT_EQ(
        std::memcmp(
            fixedRow, payloadBatch.rowAt(row), payloadLayout->nullBytes()),
        0);
    const auto value = loadUnaligned<StringView>(
        fixedRow + payloadLayout->columns()[0].offset);
    ASSERT_FALSE(value.isInline());
    EXPECT_EQ(value.size(), values[row].size());
    EXPECT_EQ(
        std::memcmp(
            value.prefix(), values[row].data(), StringView::kPrefixSize),
        0);
    EXPECT_EQ(
        stringPointerBytes(fixedRow + payloadLayout->columns()[0].offset),
        nullptr);

    EXPECT_EQ(
        std::memcmp(
            payloadHeapCursor,
            payloadBatch.heapAt(row),
            payloadBatch.heapSizeAt(row)),
        0);
    payloadHeapCursor += payloadBatch.heapSizeAt(row);
  }
  EXPECT_EQ(keyHeapCursor, keyHeap + keyHeapBytes);
  EXPECT_EQ(payloadHeapCursor, payloadHeap + payloadHeapBytes);
}

TEST_F(
    RadixSortSpillSectionsTest,
    readerRestoresPointersInsideSerializedSections) {
  const std::vector<std::string> keys{
      std::string("left_") + std::string(80, 'a'),
      std::string("right_") + std::string(96, 'b')};
  const std::vector<std::string> texts{
      std::string(72, 'x'), std::string(88, 'y')};
  auto arrays = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(INTEGER()),
      nullptr,
      2,
      makeBuffer<vector_size_t>({0, 3}),
      makeBuffer<vector_size_t>({3, 2}),
      makeVector<int32_t>(INTEGER(), {1, std::nullopt, 3, 4, 5}));
  auto payload = makeRows(
      {"payload_string", "payload_array"},
      {makeStringVector({texts[0], texts[1]}), arrays});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, storage, payloadBatch);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    storage.append(keys[row], payloadBatch.rowAt(row));
  }

  auto spill = spillSingleRun(storage, payloadLayout.get());
  const auto header = readUncompressedBlock(spill.file).header;
  const auto meta =
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  ASSERT_EQ(header.keyRecordBytes, keys.size() * meta.runtimeKeyRecordSize);
  ASSERT_EQ(header.payloadFixedBytes, keys.size() * meta.payloadFixedSize);

  RadixSortSpillReader reader(
      spill.file, spill.meta, payloadLayout.get(), pool_.get(), false);
  std::vector<const char*> restoredKeys;
  ASSERT_TRUE(reader.nextBatch(restoredKeys));
  ASSERT_EQ(restoredKeys.size(), keys.size());

  const auto* const keyRecords = restoredKeys.front();
  const auto* const keyHeap = keyRecords + header.keyRecordBytes;
  const auto* const keyHeapEnd = keyHeap + header.keyHeapBytes;
  const auto* const payloadFixed = keyHeapEnd;
  const auto* const payloadHeap = payloadFixed + header.payloadFixedBytes;
  const auto* const payloadHeapEnd = payloadHeap + header.payloadHeapBytes;

  auto* keyHeapCursor = const_cast<char*>(keyHeap);
  auto* payloadHeapCursor = const_cast<char*>(payloadHeap);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    SCOPED_TRACE(row);
    const auto* const key =
        keyRecords + static_cast<uint64_t>(row) * meta.runtimeKeyRecordSize;
    EXPECT_EQ(restoredKeys[row], key);

    const auto size = sizeForSingleRow(meta, storage.keyDataAt(row));
    const auto restoredKey = RadixSortKey(keyLayout, key);
    EXPECT_EQ(restoredKey.heapKeyData(), keyHeapCursor);
    EXPECT_EQ(restoredKey.heapKey(), std::string_view(keys[row]));
    keyHeapCursor += size.keyHeapSize;

    auto* const restoredPayload = restoredKey.payload();
    auto* const expectedPayload = const_cast<char*>(payloadFixed) +
        static_cast<uint64_t>(row) * meta.payloadFixedSize;
    EXPECT_EQ(restoredPayload, expectedPayload);
    expectPayloadVariablePointersInSection(
        meta, restoredPayload, payloadHeapCursor, size.payloadHeapSize);
    payloadHeapCursor += size.payloadHeapSize;
  }
  EXPECT_EQ(keyHeapCursor, keyHeapEnd);
  EXPECT_EQ(payloadHeapCursor, payloadHeapEnd);
  ASSERT_FALSE(reader.nextBatch(restoredKeys));
}

TEST_F(
    RadixSortSpillSectionsTest,
    variablePayloadSlotsAreCanonicalAndHeapIsColumnOrdered) {
  const std::string inlineText = "short";
  const std::string longText(80, 'x');
  auto array = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(INTEGER()),
      nullptr,
      1,
      makeBuffer<vector_size_t>({0}),
      makeBuffer<vector_size_t>({3}),
      makeVector<int32_t>(INTEGER(), {1, std::nullopt, 3}));
  auto emptyRow = std::make_shared<RowVector>(
      pool_.get(),
      ROW(std::vector<std::string>{}, std::vector<TypePtr>{}),
      nullptr,
      1,
      std::vector<VectorPtr>{});
  auto payload = makeRows(
      {"null_string",
       "inline_string",
       "heap_string",
       "array_payload",
       "empty_row_payload",
       "fixed_payload"},
      {makeStringVector({std::nullopt}),
       makeStringVector({inlineText}),
       makeStringVector({longText}),
       array,
       emptyRow,
       makeVector<int64_t>(BIGINT(), {123})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  ASSERT_EQ(payloadLayout->variableColumns().size(), 5);

  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, storage, payloadBatch);
  storage.append(
      std::string_view("\x10\x00\x00\x00\x00\x00\x00\x01", 8),
      payloadBatch.rowAt(0));

  const auto* key = storage.keyDataAt(0);
  const auto meta =
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  const auto size = sizeForSingleRow(meta, key);
  ASSERT_EQ(size.keyHeapSize, 0);
  std::vector<RadixSortSpillSectionSize> rowSizes;
  auto block = materializeSectionBlock(meta, key, 1, rowSizes);
  ASSERT_EQ(rowSizes.size(), 1);
  EXPECT_EQ(rowSizes[0].totalSize, size.totalSize);
  EXPECT_EQ(rowSizes[0].keyHeapSize, size.keyHeapSize);
  EXPECT_EQ(rowSizes[0].payloadHeapSize, size.payloadHeapSize);

  auto* keyRecord = block.data();
  auto* keyHeap = keyRecord + meta.runtimeKeyRecordSize;
  const auto* sourceFixed = payloadBatch.rowAt(0);
  auto* diskFixed = keyHeap;
  auto* diskHeap = diskFixed + meta.payloadFixedSize;
  expectDiskKeyRecord(meta, keyRecord, key);
  EXPECT_EQ(std::memcmp(diskFixed, sourceFixed, payloadLayout->nullBytes()), 0);

  const auto& nullString = payloadLayout->columns()[0];
  EXPECT_TRUE(std::all_of(
      diskFixed + nullString.offset,
      diskFixed + nullString.offset + nullString.width,
      [](char value) { return value == 0; }));

  const auto& inlineString = payloadLayout->columns()[1];
  EXPECT_EQ(
      slotBytes(diskFixed, inlineString), slotBytes(sourceFixed, inlineString));
  const auto inlineValue =
      loadUnaligned<StringView>(diskFixed + inlineString.offset);
  ASSERT_TRUE(inlineValue.isInline());
  EXPECT_EQ(inlineValue, StringView(inlineText));

  const auto& heapString = payloadLayout->columns()[2];
  const auto diskString =
      loadUnaligned<StringView>(diskFixed + heapString.offset);
  EXPECT_FALSE(diskString.isInline());
  EXPECT_EQ(diskString.size(), longText.size());
  EXPECT_EQ(
      std::memcmp(
          diskString.prefix(), longText.data(), StringView::kPrefixSize),
      0);
  EXPECT_EQ(stringPointerBytes(diskFixed + heapString.offset), nullptr);

  const auto& arrayColumn = payloadLayout->columns()[3];
  const auto sourceArrayRef =
      loadUnaligned<PayloadVarlenRef>(sourceFixed + arrayColumn.offset);
  const auto diskArrayRef =
      loadUnaligned<PayloadVarlenRef>(diskFixed + arrayColumn.offset);
  EXPECT_GT(sourceArrayRef.size, 0);
  EXPECT_EQ(diskArrayRef.size, sourceArrayRef.size);
  EXPECT_EQ(diskArrayRef.data, nullptr);

  const auto& emptyRowColumn = payloadLayout->columns()[4];
  const auto diskEmptyRowRef =
      loadUnaligned<PayloadVarlenRef>(diskFixed + emptyRowColumn.offset);
  EXPECT_EQ(diskEmptyRowRef.size, 0);
  EXPECT_EQ(diskEmptyRowRef.data, nullptr);

  const auto& fixedColumn = payloadLayout->columns()[5];
  EXPECT_EQ(
      slotBytes(diskFixed, fixedColumn), slotBytes(sourceFixed, fixedColumn));
  EXPECT_EQ(size.payloadHeapSize, longText.size() + sourceArrayRef.size);
  EXPECT_EQ(
      std::memcmp(diskHeap, payloadBatch.heapAt(0), size.payloadHeapSize), 0);

  char* keyHeapCursor = keyHeap;
  char* payloadHeapCursor = diskHeap;
  std::vector<const char*> restoredKeys;
  const auto restored = RadixSortSpillSections::restorePointersInSectionRows(
      meta,
      keyRecord,
      1,
      keyHeapCursor,
      keyHeap,
      diskFixed,
      payloadHeapCursor,
      diskHeap + size.payloadHeapSize,
      restoredKeys);
  ASSERT_TRUE(restored);
  ASSERT_EQ(restoredKeys.size(), 1);
  EXPECT_EQ(restoredKeys[0], keyRecord);
  EXPECT_EQ(keyHeapCursor, keyHeap);
  EXPECT_EQ(payloadHeapCursor, diskHeap + size.payloadHeapSize);
  auto* restoredPayload = RadixSortKey(keyLayout, keyRecord).payload();
  ASSERT_EQ(restoredPayload, diskFixed);
  const auto* restoredHeap = diskHeap;
  const auto restoredString =
      loadUnaligned<StringView>(restoredPayload + heapString.offset);
  EXPECT_EQ(restoredString.data(), restoredHeap);
  EXPECT_EQ(
      std::string(restoredString.data(), restoredString.size()), longText);
  const auto restoredArrayRef =
      loadUnaligned<PayloadVarlenRef>(restoredPayload + arrayColumn.offset);
  EXPECT_EQ(restoredArrayRef.data, restoredHeap + longText.size());
  EXPECT_EQ(restoredArrayRef.size, sourceArrayRef.size);
  const auto restoredEmptyRowRef =
      loadUnaligned<PayloadVarlenRef>(restoredPayload + emptyRowColumn.offset);
  EXPECT_EQ(restoredEmptyRowRef.size, 0);
  EXPECT_EQ(restoredEmptyRowRef.data, nullptr);
}

TEST_F(RadixSortSpillSectionsTest, variableKeyHeapOffsetRoundTrip) {
  auto keyLayout = RadixSortKeyLayout::select(std::nullopt, true, 5);
  auto payloadLayout = PayloadRowLayout::create(ROW({"payload"}, {BIGINT()}));
  RadixSortRunStorage arena(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 64);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  auto payload = makeRows({"payload"}, {makeVector<int64_t>(BIGINT(), {7})});
  payloadWriter.append(*payload, arena, payloadBatch);

  const std::string key = std::string("abcde") + std::string(48, 'x');
  arena.append(key, payloadBatch.rowAt(0));
  const auto* storedKey = arena.keyDataAt(0);
  ASSERT_NE(arena.keyAt(0).heapKeyData(), nullptr);
  EXPECT_EQ(arena.keyAt(0).heapSize(), key.size() - keyLayout.heapKeyOffset());

  auto meta = RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  const auto serializedSize = sizeForSingleRow(meta, storedKey);
  EXPECT_EQ(serializedSize.keyHeapSize, key.size() - keyLayout.heapKeyOffset());
  EXPECT_EQ(
      serializedSize.totalSize,
      meta.runtimeKeyRecordSize + serializedSize.keyHeapSize +
          meta.payloadFixedSize);

  std::vector<RadixSortSpillSectionSize> rowSizes;
  auto block = materializeSectionBlock(meta, storedKey, 1, rowSizes);
  ASSERT_EQ(rowSizes.size(), 1);
  EXPECT_EQ(rowSizes[0].totalSize, serializedSize.totalSize);
  EXPECT_EQ(rowSizes[0].keyHeapSize, serializedSize.keyHeapSize);
  EXPECT_EQ(rowSizes[0].payloadHeapSize, 0);

  auto* keyRecord = block.data();
  auto* diskKeyHeap = keyRecord + meta.runtimeKeyRecordSize;
  auto* payloadFixed = diskKeyHeap + serializedSize.keyHeapSize;
  auto* payloadHeap = payloadFixed + meta.payloadFixedSize;
  expectDiskKeyRecord(meta, keyRecord, storedKey);
  EXPECT_EQ(
      loadUnaligned<uint64_t>(keyRecord + *keyLayout.sizeOffset()), key.size());
  EXPECT_EQ(
      std::string_view(diskKeyHeap, key.size() - keyLayout.heapKeyOffset()),
      std::string_view(key).substr(keyLayout.heapKeyOffset()));
  EXPECT_EQ(
      std::memcmp(payloadFixed, payloadBatch.rowAt(0), meta.payloadFixedSize),
      0);

  char* keyHeapCursor = diskKeyHeap;
  char* payloadHeapCursor = payloadHeap;
  std::vector<const char*> restoredKeys;
  const auto restored = RadixSortSpillSections::restorePointersInSectionRows(
      meta,
      keyRecord,
      1,
      keyHeapCursor,
      diskKeyHeap + serializedSize.keyHeapSize,
      payloadFixed,
      payloadHeapCursor,
      payloadHeap,
      restoredKeys);
  ASSERT_TRUE(restored);
  ASSERT_EQ(restoredKeys.size(), 1);
  EXPECT_EQ(restoredKeys[0], keyRecord);
  EXPECT_EQ(keyHeapCursor, diskKeyHeap + serializedSize.keyHeapSize);
  EXPECT_EQ(payloadHeapCursor, payloadHeap);

  const auto restoredKey = RadixSortKey(keyLayout, keyRecord);
  EXPECT_EQ(
      std::string(keyRecord, keyLayout.heapKeyOffset()) +
          std::string(restoredKey.heapKey()),
      key);
  EXPECT_EQ(restoredKey.heapKeyData(), diskKeyHeap);
  EXPECT_EQ(restoredKey.payload(), payloadFixed);
}

TEST_F(RadixSortSpillSectionsTest, compactPointerAtRecordEndDoesNotOverwrite) {
  const auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  RadixSortRunStorage arena(pool_.get(), keyLayout, 1, 64);
  const std::string key(64, 'k');
  arena.append(key);
  const auto* storedKey = arena.keyDataAt(0);
  auto meta = RadixSortSpillSectionMeta::create(keyLayout, nullptr);
  const auto size = sizeForSingleRow(meta, storedKey);

  constexpr uint8_t kSentinel = 0xa5;
  std::vector<RadixSortSpillSectionSize> rowSizes;
  const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
      meta, storedKey, 1, std::numeric_limits<uint64_t>::max(), rowSizes);
  ASSERT_EQ(batchSize.rowCount, 1);
  ASSERT_EQ(rowSizes.size(), 1);
  std::vector<uint8_t> storage(batchSize.totalBytes() + 2, kSentinel);
  auto* destination = reinterpret_cast<char*>(storage.data() + 1);
  auto* keyRecord = destination;
  auto* keyHeap = keyRecord + meta.runtimeKeyRecordSize;
  auto* keyHeapCursor = keyHeap;
  auto* payloadFixed = keyHeap + batchSize.keyHeapBytes;
  auto* payloadHeap = payloadFixed + batchSize.payloadFixedBytes;
  auto* payloadHeapCursor = payloadHeap;
  RadixSortSpillSections::copyRowsToSections(
      meta,
      storedKey,
      rowSizes.data(),
      1,
      batchSize.keyHeapBytes,
      batchSize.payloadHeapBytes,
      keyRecord,
      keyHeapCursor,
      payloadFixed,
      payloadHeapCursor);
  EXPECT_EQ(keyHeapCursor, keyHeap + size.keyHeapSize);
  EXPECT_EQ(payloadHeapCursor, payloadHeap);

  EXPECT_EQ(storage.front(), kSentinel);
  EXPECT_EQ(storage.back(), kSentinel);
  EXPECT_EQ(size.totalSize, meta.runtimeKeyRecordSize + key.size());
  EXPECT_EQ(
      std::string_view(
          destination + meta.runtimeKeyRecordSize,
          key.size() - keyLayout.heapKeyOffset()),
      std::string_view(key).substr(keyLayout.heapKeyOffset()));

  keyHeapCursor = keyHeap;
  std::vector<const char*> restoredKeys;
  const auto restoredSize =
      RadixSortSpillSections::restorePointersInSectionRows(
          meta,
          keyRecord,
          1,
          keyHeapCursor,
          keyHeap + size.keyHeapSize,
          nullptr,
          payloadHeapCursor,
          payloadHeapCursor,
          restoredKeys);
  ASSERT_TRUE(restoredSize);
  ASSERT_EQ(restoredKeys.size(), 1);
  EXPECT_EQ(restoredKeys[0], keyRecord);
  EXPECT_EQ(keyHeapCursor, keyHeap + size.keyHeapSize);
  const auto restored = RadixSortKey(keyLayout, keyRecord);
  EXPECT_EQ(restored.heapKey(), std::string_view(key));
  EXPECT_EQ(restored.heapKeyData(), keyHeap);
  EXPECT_EQ(storage.front(), kSentinel);
  EXPECT_EQ(storage.back(), kSentinel);
}

TEST_F(RadixSortSpillSectionsTest, restoreRejectsTruncatedSectionHeaps) {
  const std::string key = std::string("prefix") + std::string(64, 'k');
  const std::string payloadText(96, 'p');
  auto payload =
      makeRows({"payload_string"}, {makeStringVector({payloadText})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::select(std::nullopt, true, 3);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, storage, payloadBatch);
  storage.append(key, payloadBatch.rowAt(0));

  const auto* storedKey = storage.keyDataAt(0);
  const auto meta =
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  const auto size = sizeForSingleRow(meta, storedKey);
  ASSERT_GT(size.keyHeapSize, 0);
  ASSERT_GT(meta.payloadFixedSize, 0);
  ASSERT_GT(size.payloadHeapSize, 0);
  std::vector<RadixSortSpillSectionSize> rowSizes;
  const auto block = materializeSectionBlock(meta, storedKey, 1, rowSizes);
  ASSERT_EQ(rowSizes.size(), 1);

  auto restoreWithLimits = [&](uint64_t keyHeapBytes,
                               uint64_t payloadHeapBytes) {
    auto copy = block;
    auto* keyRecord = copy.data();
    auto* keyHeap = keyRecord + meta.runtimeKeyRecordSize;
    auto* payloadFixed = keyHeap + size.keyHeapSize;
    auto* payloadHeap = payloadFixed + meta.payloadFixedSize;
    auto* keyHeapCursor = keyHeap;
    auto* payloadHeapCursor = payloadHeap;
    std::vector<const char*> restoredKeys;
    return RadixSortSpillSections::restorePointersInSectionRows(
        meta,
        keyRecord,
        1,
        keyHeapCursor,
        keyHeap + keyHeapBytes,
        payloadFixed,
        payloadHeapCursor,
        payloadHeap + payloadHeapBytes,
        restoredKeys);
  };

  EXPECT_FALSE(restoreWithLimits(size.keyHeapSize - 1, size.payloadHeapSize));
  EXPECT_FALSE(restoreWithLimits(size.keyHeapSize, size.payloadHeapSize - 1));
  EXPECT_TRUE(restoreWithLimits(size.keyHeapSize, size.payloadHeapSize));
}

TEST_F(RadixSortSpillSectionsTest, writerReaderRoundTripWithoutCompression) {
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

TEST_F(RadixSortSpillSectionsTest, writerReaderRoundTripWithCompression) {
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

TEST_F(
    RadixSortSpillSectionsTest,
    writerReaderRoundTripWithLargeMapPayloadAndZstd) {
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

TEST_F(RadixSortSpillSectionsTest, encodedBlockWriterPreservesHeaderAndStats) {
  struct TestHeader {
    int32_t uncompressedSize;
    int32_t storedSize;
    uint32_t rowCount;
    uint32_t marker;
    uint64_t sectionBytes;
  };
  static_assert(sizeof(TestHeader) == 24);

  for (const auto compression :
       {common::CompressionKind_NONE,
        common::CompressionKind_LZ4,
        common::CompressionKind_ZSTD}) {
    SCOPED_TRACE(compression);
    auto directory = exec::test::TempDirectoryPath::create();
    folly::Synchronized<common::SpillStats> stats;
    uint64_t limitBytes = 0;
    auto config = spillConfig(directory->path, compression, 1 << 20);
    config.updateAndCheckSpillLimitCb = [&](uint64_t bytes) {
      limitBytes += bytes;
    };
    SpillWriter writer(
        directory->path + "/encoded",
        std::numeric_limits<uint64_t>::max(),
        config.spillIOConfig(1),
        pool_.get(),
        &stats);

    constexpr int32_t kBodySize = 4096;
    constexpr uint64_t kRows = 17;
    TestHeader header{
        -1,
        -1,
        static_cast<uint32_t>(kRows),
        0xabcdefu,
        static_cast<uint64_t>(kBodySize)};
    std::vector<char> frame(sizeof(TestHeader) + kBodySize);
    std::memcpy(frame.data(), &header, sizeof(header));
    for (int32_t i = 0; i < kBodySize; ++i) {
      frame[sizeof(TestHeader) + i] = static_cast<char>('a' + (i % 3));
    }
    const std::string expectedBody(
        frame.data() + sizeof(TestHeader), kBodySize);

    const auto globalStatsBefore = common::globalSpillStats();
    const auto writtenBytes = writer.writeEncodedBlock(
        frame.data(), sizeof(TestHeader), kBodySize, kRows);
    const auto statsAfterWrite = stats.copy();
    EXPECT_EQ(statsAfterWrite.spilledRows, 0);
    EXPECT_EQ(statsAfterWrite.spillSerializationTimeUs, 0);
    EXPECT_EQ(statsAfterWrite.spilledFiles, 0);
    EXPECT_EQ(statsAfterWrite.spilledBytes, writtenBytes);
    EXPECT_EQ(statsAfterWrite.spillWrites, 1);
    EXPECT_EQ(limitBytes, writtenBytes);

    TestHeader frameHeader;
    std::memcpy(&frameHeader, frame.data(), sizeof(frameHeader));
    EXPECT_EQ(frameHeader.uncompressedSize, kBodySize);
    EXPECT_GT(frameHeader.storedSize, 0);
    EXPECT_EQ(frameHeader.rowCount, kRows);
    EXPECT_EQ(frameHeader.marker, header.marker);
    EXPECT_EQ(frameHeader.sectionBytes, header.sectionBytes);
    if (compression == common::CompressionKind_NONE) {
      EXPECT_EQ(frameHeader.storedSize, kBodySize);
    } else {
      EXPECT_LE(
          frameHeader.storedSize,
          spillCompressionBound(compression, kBodySize));
    }

    auto files = writer.finish();
    ASSERT_EQ(files.size(), 1);
    EXPECT_EQ(files[0].rowCount, kRows);
    EXPECT_EQ(files[0].compressionKind, compression);
    EXPECT_EQ(files[0].size, writtenBytes);
    EXPECT_TRUE(std::filesystem::exists(files[0].path));

    const auto finalStats = stats.copy();
    EXPECT_EQ(finalStats.spilledRows, 0);
    EXPECT_EQ(finalStats.spillSerializationTimeUs, 0);
    EXPECT_EQ(finalStats.spilledFiles, 1);
    EXPECT_EQ(finalStats.spilledBytes, writtenBytes);
    EXPECT_EQ(finalStats.spillWrites, 1);
    const auto globalDelta = common::globalSpillStats() - globalStatsBefore;
    EXPECT_EQ(globalDelta.spilledRows, 0);
    EXPECT_EQ(globalDelta.spillSerializationTimeUs, 0);
    EXPECT_EQ(globalDelta.spilledFiles, finalStats.spilledFiles);
    EXPECT_EQ(globalDelta.spilledBytes, finalStats.spilledBytes);
    EXPECT_EQ(globalDelta.spillWrites, finalStats.spillWrites);
    EXPECT_EQ(globalDelta.spillFlushTimeUs, finalStats.spillFlushTimeUs);
    EXPECT_EQ(globalDelta.spillWriteTimeUs, finalStats.spillWriteTimeUs);

    const auto bytes = readSpillBytes(files[0].path);
    ASSERT_EQ(bytes.size(), writtenBytes);
    TestHeader diskHeader;
    std::memcpy(&diskHeader, bytes.data(), sizeof(diskHeader));
    EXPECT_EQ(diskHeader.uncompressedSize, frameHeader.uncompressedSize);
    EXPECT_EQ(diskHeader.storedSize, frameHeader.storedSize);
    EXPECT_EQ(diskHeader.rowCount, frameHeader.rowCount);
    EXPECT_EQ(diskHeader.marker, frameHeader.marker);
    EXPECT_EQ(diskHeader.sectionBytes, frameHeader.sectionBytes);

    auto input = makeSpillInputStream(files[0].path);
    TestHeader streamHeader;
    input->readBytes(
        reinterpret_cast<char*>(&streamHeader), sizeof(streamHeader));
    EXPECT_EQ(streamHeader.uncompressedSize, diskHeader.uncompressedSize);
    EXPECT_EQ(streamHeader.storedSize, diskHeader.storedSize);
    EXPECT_EQ(streamHeader.rowCount, diskHeader.rowCount);
    EXPECT_EQ(streamHeader.marker, diskHeader.marker);
    EXPECT_EQ(streamHeader.sectionBytes, diskHeader.sectionBytes);
    std::vector<char> decodedBody(kBodySize);
    BufferPtr compressedBuffer;
    uint64_t decompressTimeUs = 0;
    readSpillBlockBody(
        *input,
        compression,
        streamHeader.uncompressedSize,
        streamHeader.storedSize,
        decodedBody.data(),
        compressedBuffer,
        pool_.get(),
        decompressTimeUs);
    EXPECT_TRUE(input->atEnd());
    if (compression == common::CompressionKind_NONE) {
      EXPECT_EQ(compressedBuffer, nullptr);
      EXPECT_EQ(decompressTimeUs, 0);
    } else {
      EXPECT_NE(compressedBuffer, nullptr);
    }
    EXPECT_EQ(
        std::string_view(decodedBody.data(), decodedBody.size()),
        std::string_view(expectedBody.data(), expectedBody.size()));
  }
}

TEST_F(RadixSortSpillSectionsTest, encodedBlockWriterValidatesWriteLifecycle) {
  auto directory = exec::test::TempDirectoryPath::create();
  folly::Synchronized<common::SpillStats> stats;
  auto config = spillConfig(
      directory->path, common::CompressionKind_NONE, /*writeBufferSize=*/1024);
  SpillWriter writer(
      directory->path + "/encoded",
      std::numeric_limits<uint64_t>::max(),
      config.spillIOConfig(1),
      pool_.get(),
      &stats);

  std::array<char, 16> frame{};
  EXPECT_THROW(writer.writeEncodedBlock(frame.data(), 4, 8, 1), BoltException);
  EXPECT_THROW(writer.writeEncodedBlock(frame.data(), 8, 0, 1), BoltException);

  auto files = writer.finish();
  EXPECT_TRUE(files.empty());
  EXPECT_THROW(writer.writeEncodedBlock(frame.data(), 8, 8, 1), BoltException);
}

TEST_F(RadixSortSpillSectionsTest, encodedBlockWriterTracksRolledFileRows) {
  auto directory = exec::test::TempDirectoryPath::create();
  folly::Synchronized<common::SpillStats> stats;
  uint64_t limitBytes = 0;
  common::SpillConfig config = spillConfig(
      directory->path,
      common::CompressionKind_NONE,
      1 << 20,
      /*maxFileSize=*/150);
  config.updateAndCheckSpillLimitCb = [&](uint64_t bytes) {
    limitBytes += bytes;
  };
  SpillWriter writer(
      directory->path + "/encoded",
      config.maxFileSize,
      config.spillIOConfig(1),
      pool_.get(),
      &stats);

  struct TestHeader {
    int32_t uncompressedSize;
    int32_t storedSize;
  };
  auto writeBlock = [&](char fill, uint64_t rows) {
    constexpr int32_t kBodySize = 96;
    std::vector<char> frame(sizeof(TestHeader) + kBodySize, fill);
    return writer.writeEncodedBlock(
        frame.data(), sizeof(TestHeader), kBodySize, rows);
  };

  const auto globalStatsBefore = common::globalSpillStats();
  const auto firstBytes = writeBlock('x', 3);
  const auto secondBytes = writeBlock('y', 5);
  const auto thirdBytes = writeBlock('z', 7);
  auto files = writer.finish();

  ASSERT_EQ(files.size(), 2);
  EXPECT_EQ(files[0].id, 0);
  EXPECT_EQ(files[1].id, 1);
  EXPECT_EQ(files[0].rowCount, 8);
  EXPECT_EQ(files[1].rowCount, 7);
  EXPECT_EQ(files[0].size, firstBytes + secondBytes);
  EXPECT_EQ(files[1].size, thirdBytes);
  EXPECT_EQ(limitBytes, firstBytes + secondBytes + thirdBytes);
  for (const auto& file : files) {
    EXPECT_TRUE(std::filesystem::exists(file.path));
  }

  const auto finalStats = stats.copy();
  EXPECT_EQ(finalStats.spilledRows, 0);
  EXPECT_EQ(finalStats.spillSerializationTimeUs, 0);
  EXPECT_EQ(finalStats.spilledBytes, firstBytes + secondBytes + thirdBytes);
  EXPECT_EQ(finalStats.spillWrites, 3);
  EXPECT_EQ(finalStats.spilledFiles, files.size());
  const auto globalDelta = common::globalSpillStats() - globalStatsBefore;
  EXPECT_EQ(globalDelta.spilledRows, 0);
  EXPECT_EQ(globalDelta.spillSerializationTimeUs, 0);
  EXPECT_EQ(globalDelta.spilledBytes, finalStats.spilledBytes);
  EXPECT_EQ(globalDelta.spillWrites, finalStats.spillWrites);
  EXPECT_EQ(globalDelta.spilledFiles, finalStats.spilledFiles);
}

TEST_F(RadixSortSpillSectionsTest, writerTransfersOwnershipOnlyAfterFinish) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 4, 64);
  storage.append(std::string_view("\x00\x00\x00\x00\x00\x00\x00\x01", 8));

  {
    auto directory = exec::test::TempDirectoryPath::create();
    {
      folly::Synchronized<common::SpillStats> stats;
      RadixSortSpillWriter writer(
          directory->path + "/spill",
          spillConfig(directory->path, common::CompressionKind_NONE, 1 << 20),
          pool_.get(),
          &stats);
      writer.writeKeyRange(keyLayout, nullptr, storage.keyDataAt(0), 1);
      EXPECT_EQ(directoryFiles(directory->path).size(), 1);
    }
    EXPECT_TRUE(directoryFiles(directory->path).empty());
  }

  {
    auto directory = exec::test::TempDirectoryPath::create();
    std::vector<RadixSortSpillFile> files;
    {
      folly::Synchronized<common::SpillStats> stats;
      RadixSortSpillWriter writer(
          directory->path + "/spill",
          spillConfig(directory->path, common::CompressionKind_NONE, 1 << 20),
          pool_.get(),
          &stats);
      files = writer.writeRun(storage, nullptr);
      ASSERT_EQ(files.size(), 1);
      EXPECT_TRUE(std::filesystem::exists(files[0].path));
    }
    ASSERT_EQ(files.size(), 1);
    EXPECT_TRUE(std::filesystem::exists(files[0].path));
  }
}

TEST_F(RadixSortSpillSectionsTest, writerRollsFilesAtConfiguredMaxSize) {
  constexpr vector_size_t kRows = 7;
  constexpr uint32_t kPayloadColumns = 40'000;
  constexpr uint64_t kMaxFileSize = 512 * 1024;

  std::vector<TypePtr> payloadTypes(kPayloadColumns, BIGINT());
  auto payloadLayout = PayloadRowLayout::create(ROW(std::move(payloadTypes)));
  ASSERT_GT(payloadLayout->rowWidth(), kMaxFileSize / 2);
  ASSERT_LT(payloadLayout->rowWidth(), 1 << 20);

  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 64);
  std::vector<BufferPtr> payloadBuffers;
  payloadBuffers.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    auto payload = AlignedBuffer::allocate<char>(
        payloadLayout->rowWidth(), pool_.get(), 0);
    auto* rawPayload = payload->asMutable<char>();
    std::memset(rawPayload, 0xff, payloadLayout->nullBytes());
    std::memset(
        rawPayload + payloadLayout->nullBytes(),
        static_cast<int>(row + 1),
        payloadLayout->rowWidth() - payloadLayout->nullBytes());
    payloadBuffers.push_back(std::move(payload));

    std::array<char, sizeof(uint64_t)> key{};
    key.back() = static_cast<char>(row + 1);
    storage.append(
        std::string(key.data(), key.size()),
        payloadBuffers.back()->asMutable<char>());
  }

  for (const bool outputStage : {false, true}) {
    SCOPED_TRACE(outputStage ? "output-stage spill" : "run spill");
    auto files =
        spillRunFiles(storage, payloadLayout.get(), kMaxFileSize, outputStage);
    ASSERT_GT(files.size(), 1);
    for (size_t i = 0; i + 1 < files.size(); ++i) {
      EXPECT_GT(files[i].size, kMaxFileSize);
    }
    verifySpillFilesRoundTrip(files, storage, *payloadLayout);
  }
}

TEST_F(RadixSortSpillSectionsTest, rejectsBlockSizesBeforeRowAllocation) {
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
    std::vector<const char*> keys;
    EXPECT_THROW(reader.nextBatch(keys), BoltException);
    EXPECT_EQ(pool_->stats().numAllocs, allocations);
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    rejectsInvalidHeaderSectionSizesBeforeAllocation) {
  auto payload =
      makeRows({"payload_bigint"}, {makeVector<int64_t>(BIGINT(), {7})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 64);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, storage, payloadBatch);
  storage.append(
      std::string_view("\x10\x00\x00\x00\x00\x00\x00\x01", 8),
      payloadBatch.rowAt(0));

  struct Mutation {
    const char* name;
    uint64_t offset;
    uint64_t delta;
  };
  for (const auto mutation : std::array<Mutation, 3>{{
           {"key record bytes",
            offsetof(TestRadixSortSpillBlockHeader, keyRecordBytes),
            1},
           {"payload fixed bytes",
            offsetof(TestRadixSortSpillBlockHeader, payloadFixedBytes),
            1},
           {"uncompressed section sum",
            offsetof(TestRadixSortSpillBlockHeader, payloadHeapBytes),
            8},
       }}) {
    SCOPED_TRACE(mutation.name);
    auto spill = spillSingleRun(storage, payloadLayout.get());
    const auto original =
        readSpillValue<uint64_t>(spill.file.path, mutation.offset);
    overwriteSpillValue<uint64_t>(
        spill.file.path, mutation.offset, original + mutation.delta);

    RadixSortSpillReader reader(
        spill.file, spill.meta, payloadLayout.get(), pool_.get(), false);
    const auto allocations = pool_->stats().numAllocs;
    std::vector<const char*> keys;
    EXPECT_THROW(reader.nextBatch(keys), BoltException);
    EXPECT_EQ(pool_->stats().numAllocs, allocations);
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
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
  std::vector<const char*> keys;
  EXPECT_THROW(reader.nextBatch(keys), BoltException);
  EXPECT_EQ(pool_->stats().numAllocs, allocations);
}

TEST_F(RadixSortSpillSectionsTest, outputStageKeyRangeRoundTripWithPayload) {
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

TEST_F(RadixSortSpillSectionsTest, fixedAndInlineVariableFastPathRoundTrip) {
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
    RadixSortSpillSectionsTest,
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

TEST_F(RadixSortSpillSectionsTest, mixedInlineAndHeapVariableBlockRoundTrip) {
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
    RadixSortSpillSectionsTest,
    readerRetainsPreviousMultiBlockPayloadsUntilReleased) {
  const std::string value((1 << 20) + 256, 'r');
  std::shared_ptr<const PayloadRowLayout> payloadLayout;
  auto spill = writeLargePayloadSpill(value, payloadLayout);
  RadixSortSpillReader reader(
      spill.file, spill.meta, payloadLayout.get(), pool_.get(), false);
  std::vector<const char*> keys;
  ASSERT_TRUE(reader.nextBatch(keys));
  auto firstPayloads = payloadsFromKeys(spill.meta.keyLayout, keys);

  ASSERT_TRUE(reader.nextBatch(keys));
  auto nextPayloads = payloadsFromKeys(spill.meta.keyLayout, keys);
  ASSERT_FALSE(nextPayloads.empty());

  expectStringPayload(*payloadLayout, firstPayloads, value);
  reader.releaseRetainedBuffers(false);
}

TEST_F(RadixSortSpillSectionsTest, readerReusesReleasedSerializedBuffer) {
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
  std::vector<const char*> keys;
  ASSERT_TRUE(reader.nextBatch(keys));

  ASSERT_TRUE(reader.nextBatch(keys));
  auto secondPayloads = payloadsFromKeys(spill.meta.keyLayout, keys);
  reader.releaseRetainedBuffers(false);

  const auto allocationsBeforeThird = pool_->stats().numAllocs;
  ASSERT_TRUE(reader.nextBatch(keys));
  auto thirdPayloads = payloadsFromKeys(spill.meta.keyLayout, keys);
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
  const auto allocationsBeforeSharedRead = pool_->stats().numAllocs;
  ASSERT_TRUE(secondReader.nextBatch(keys));
  auto sharedPayloads = payloadsFromKeys(spill.meta.keyLayout, keys);
  EXPECT_EQ(pool_->stats().numAllocs, allocationsBeforeSharedRead);
  EXPECT_FALSE(sharedPayloads.empty());
  EXPECT_EQ(bufferCache.serializedBuffer, nullptr);
}

TEST_F(RadixSortSpillSectionsTest, mergerHandlesStreamCountsAndExhaustion) {
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

TEST_F(RadixSortSpillSectionsTest, fileMergeStreamRemovesOwnedFile) {
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

TEST_F(RadixSortSpillSectionsTest, concatFileMergeStreamReadsSplitRunInOrder) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 1, 64);
  for (uint8_t value = 1; value <= 4; ++value) {
    storage.append(fixed8Key(value));
  }

  auto files =
      spillRunFiles(storage, nullptr, /*maxFileSize=*/1, /*outputStage=*/false);
  ASSERT_GT(files.size(), 1);
  std::vector<std::string> filePaths;
  filePaths.reserve(files.size());
  for (const auto& file : files) {
    filePaths.push_back(file.path);
  }

  {
    RadixSortConcatFilesSpillMergeStream stream(
        std::move(files),
        RadixSortSpillSectionMeta::create(keyLayout, nullptr),
        nullptr,
        pool_.get(),
        false);
    ASSERT_TRUE(stream.hasData());
    uint64_t row = 0;
    while (stream.hasData()) {
      ASSERT_LT(row, storage.size());
      EXPECT_EQ(
          RadixSortKey(keyLayout, stream.key())
              .compare(RadixSortKey(keyLayout, storage.keyDataAt(row))),
          0)
          << "row=" << row;
      stream.pop();
      stream.releaseRetainedBuffers();
      ++row;
    }
    EXPECT_EQ(row, storage.size());
  }
  for (const auto& path : filePaths) {
    EXPECT_FALSE(std::filesystem::exists(path));
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    concatFileMergeStreamRetainsExhaustedFileBuffersUntilReleased) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 1, 64);
  for (uint8_t value = 1; value <= 2; ++value) {
    storage.append(fixed8Key(value));
  }

  auto files =
      spillRunFiles(storage, nullptr, /*maxFileSize=*/1, /*outputStage=*/false);
  ASSERT_EQ(files.size(), 2);
  RadixSortConcatFilesSpillMergeStream stream(
      std::move(files),
      RadixSortSpillSectionMeta::create(keyLayout, nullptr),
      nullptr,
      pool_.get(),
      false);
  ASSERT_TRUE(stream.hasData());
  const auto* firstKey = stream.key();
  EXPECT_EQ(
      RadixSortKey(keyLayout, firstKey)
          .compare(RadixSortKey(keyLayout, storage.keyDataAt(0))),
      0);

  stream.pop();
  ASSERT_TRUE(stream.hasData());
  EXPECT_EQ(
      RadixSortKey(keyLayout, stream.key())
          .compare(RadixSortKey(keyLayout, storage.keyDataAt(1))),
      0);
  EXPECT_EQ(
      RadixSortKey(keyLayout, firstKey)
          .compare(RadixSortKey(keyLayout, storage.keyDataAt(0))),
      0);
  stream.releaseRetainedBuffers();
}

TEST_F(RadixSortSpillSectionsTest, concatFileMergeStreamsMergeByLogicalRun) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage oddStorage(pool_.get(), keyLayout, 1, 64);
  RadixSortRunStorage evenStorage(pool_.get(), keyLayout, 1, 64);
  for (const auto value : {uint8_t{1}, uint8_t{3}, uint8_t{5}}) {
    oddStorage.append(fixed8Key(value));
  }
  for (const auto value : {uint8_t{2}, uint8_t{4}, uint8_t{6}}) {
    evenStorage.append(fixed8Key(value));
  }

  auto oddFiles = spillRunFiles(
      oddStorage, nullptr, /*maxFileSize=*/1, /*outputStage=*/false);
  auto evenFiles = spillRunFiles(
      evenStorage, nullptr, /*maxFileSize=*/1, /*outputStage=*/false);
  ASSERT_GT(oddFiles.size(), 1);
  ASSERT_GT(evenFiles.size(), 1);

  std::vector<std::string> filePaths;
  for (const auto& file : oddFiles) {
    filePaths.push_back(file.path);
  }
  for (const auto& file : evenFiles) {
    filePaths.push_back(file.path);
  }
  RadixSortRunStorage expectedStorage(pool_.get(), keyLayout, 1, 64);
  for (uint8_t value = 1; value <= 6; ++value) {
    expectedStorage.append(fixed8Key(value));
  }

  std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
  streams.push_back(std::make_unique<RadixSortConcatFilesSpillMergeStream>(
      std::move(oddFiles),
      RadixSortSpillSectionMeta::create(keyLayout, nullptr),
      nullptr,
      pool_.get(),
      false));
  streams.push_back(std::make_unique<RadixSortConcatFilesSpillMergeStream>(
      std::move(evenFiles),
      RadixSortSpillSectionMeta::create(keyLayout, nullptr),
      nullptr,
      pool_.get(),
      false));
  RadixSortMerger merger(keyLayout, std::move(streams));
  EXPECT_EQ(merger.testingNumStreams(), 2);

  std::array<const char*, 6> keys;
  std::array<char*, 6> payloads;
  const auto count =
      merger.collectRows(keys.size(), keys.data(), payloads.data());
  ASSERT_EQ(count, keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    EXPECT_EQ(
        RadixSortKey(keyLayout, keys[i])
            .compare(RadixSortKey(keyLayout, expectedStorage.keyDataAt(i))),
        0)
        << "row=" << i;
  }
  merger.releaseRetainedBuffers();
  for (const auto& path : filePaths) {
    EXPECT_FALSE(std::filesystem::exists(path));
  }
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
