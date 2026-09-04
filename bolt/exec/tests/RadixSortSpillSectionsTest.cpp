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
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "bolt/common/base/tests/GTestUtils.h"
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
static_assert(!std::is_copy_constructible_v<RadixSortSpillReader>);
static_assert(!std::is_copy_assignable_v<RadixSortSpillReader>);
static_assert(!std::is_move_constructible_v<RadixSortSpillReader>);
static_assert(!std::is_move_assignable_v<RadixSortSpillReader>);

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

constexpr std::array<uint32_t, kLayouts.size()>
    kRuntimeKeyRecordWidths{8, 16, 24, 32, 32, 16, 24, 32, 32};
constexpr std::array<uint32_t, kLayouts.size()>
    kWireKeyRecordWidths{8, 16, 24, 32, 26, 10, 18, 26, 20};

class BoundaryTestMergeStream final : public RadixSortMergeStream {
 public:
  BoundaryTestMergeStream(
      const RadixSortKeyLayout& layout,
      std::vector<uint8_t> keys,
      std::vector<size_t> boundaryRows,
      int64_t payloadBase = 0)
      : RadixSortMergeStream(layout), boundaryRows_(std::move(boundaryRows)) {
    keyStorage_.reserve(keys.size());
    payloadStorage_.reserve(keys.size());
    for (size_t row = 0; row < keys.size(); ++row) {
      keyStorage_.push_back(fixedKey(layout.width(), keys[row]));
      payloadStorage_.push_back(payloadBase + keys[row]);
      if (layout.hasPayload()) {
        storeCompactPointer(
            keyStorage_.back().data() + *layout.payloadOffset(),
            reinterpret_cast<char*>(&payloadStorage_.back()));
      }
    }
    loadCurrent();
  }

  bool hasData() const override {
    return key_ != nullptr;
  }

  bool tryAdvance() override {
    if (isBoundary()) {
      return false;
    }
    ++ordinaryAdvances_;
    ++index_;
    loadCurrent();
    return true;
  }

  void advanceAfterFlush() override {
    EXPECT_TRUE(isBoundary());
    ++safeAdvances_;
    ++index_;
    loadCurrent();
  }

  size_t index() const {
    return index_;
  }

  size_t safeAdvances() const {
    return safeAdvances_;
  }

  size_t ordinaryAdvances() const {
    return ordinaryAdvances_;
  }

 private:
  bool isBoundary() const {
    return key_ != nullptr &&
        std::find(boundaryRows_.begin(), boundaryRows_.end(), index_) !=
        boundaryRows_.end();
  }

  static std::string fixedKey(uint32_t width, uint8_t value) {
    std::string key(width, '\0');
    key[sizeof(uint64_t) - 1] = static_cast<char>(value);
    return key;
  }

  void loadCurrent() {
    if (index_ == keyStorage_.size()) {
      key_ = nullptr;
      payload_ = nullptr;
      return;
    }
    key_ = keyStorage_[index_].data();
    payload_ = hasPayload_ ? reinterpret_cast<char*>(&payloadStorage_[index_])
                           : nullptr;
  }

  std::vector<std::string> keyStorage_;
  std::vector<int64_t> payloadStorage_;
  std::vector<size_t> boundaryRows_;
  size_t index_{0};
  size_t safeAdvances_{0};
  size_t ordinaryAdvances_{0};
};

class TrackingStorageMergeStream final : public RadixSortMergeStream {
 public:
  explicit TrackingStorageMergeStream(const RadixSortRunStorage& storage)
      : RadixSortMergeStream(storage.layout()), storage_(storage) {
    loadCurrent();
  }

  bool hasData() const override {
    return key_ != nullptr;
  }

  bool tryAdvance() override {
    ++position_;
    loadCurrent();
    return true;
  }

  uint64_t position() const {
    return position_;
  }

 private:
  void loadCurrent() {
    if (position_ == storage_.size()) {
      key_ = nullptr;
      payload_ = nullptr;
      return;
    }
    key_ = storage_.keyDataAt(position_);
    payload_ =
        hasPayload_ ? RadixSortKey(storage_.layout(), key_).payload() : nullptr;
  }

  const RadixSortRunStorage& storage_;
  uint64_t position_{0};
};

class InspectableVariableMemoryMergeStream final
    : public RadixSortVariableMergeStream {
 public:
  explicit InspectableVariableMemoryMergeStream(
      const RadixSortRunStorage& storage)
      : RadixSortVariableMergeStream(
            storage.layout(),
            storage.layout().width()),
        storage_(storage) {
    BOLT_CHECK(storage.layout().isVariable());
    loadCurrent();
  }

  bool hasData() const override {
    return key_ != nullptr;
  }

  bool tryAdvance() override {
    ++position_;
    loadCurrent();
    return true;
  }

  bool suffixMaterialized() const {
    return encodedSuffix_.bytes.data() != nullptr;
  }

 private:
  void loadCurrent() {
    if (position_ == storage_.size()) {
      key_ = nullptr;
      payload_ = nullptr;
      encodedSuffix_ = {};
      return;
    }
    key_ = storage_.keyDataAt(position_);
    payload_ = nullptr;
    encodedSuffix_ = {};
  }

  const RadixSortRunStorage& storage_;
  uint64_t position_{0};
};

class RadixSortSpillSectionsTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    filesystems::registerLocalFileSystem();
  }

 protected:
  static constexpr uint64_t kBlockHeaderSize =
      sizeof(TestRadixSortSpillBlockHeader);
  static constexpr uint64_t kFixedWriteBufferSize =
      (1UL << 20) - AlignedBuffer::kPaddedSize;
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
      vector_size_t expectedOffset,
      const RowVector& actual) {
    const CompareFlags flags{
        .nullsFirst = true,
        .ascending = true,
        .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
    ASSERT_LE(expectedOffset + actual.size(), expected.size());
    ASSERT_EQ(expected.childrenSize(), actual.childrenSize());
    for (uint32_t column = 0; column < expected.childrenSize(); ++column) {
      for (vector_size_t row = 0; row < actual.size(); ++row) {
        const auto result = expected.childAt(column)->compare(
            actual.childAt(column).get(), expectedOffset + row, row, flags);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, 0) << "column=" << column << ", row=" << row;
      }
    }
  }

  static void expectEquivalent(
      const RowVector& expected,
      const RowVector& actual) {
    ASSERT_EQ(expected.size(), actual.size());
    expectEquivalent(expected, 0, actual);
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

  std::vector<std::string> readStringPayloads(
      const PayloadRowLayout& layout,
      const std::vector<char*>& payloads) {
    RowVectorPtr output;
    PayloadRowReader::gather(layout, payloads, pool_.get(), output);
    const auto* strings =
        output->childAt(0)->asUnchecked<FlatVector<StringView>>();
    std::vector<std::string> result;
    result.reserve(payloads.size());
    for (vector_size_t row = 0; row < output->size(); ++row) {
      result.push_back(strings->valueAt(row).str());
    }
    return result;
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
    std::vector<char> expected(meta.wireKeyRecordSize);
    std::memcpy(expected.data(), sourceKey, expected.size());
    return expected;
  }

  static void expectDiskKeyRecord(
      const RadixSortSpillSectionMeta& meta,
      const char* diskKey,
      const char* sourceKey) {
    const auto expected = expectedDiskKeyRecord(meta, sourceKey);
    EXPECT_EQ(std::memcmp(diskKey, expected.data(), expected.size()), 0);
    if (meta.hasKeyHeap) {
      EXPECT_EQ(meta.wireKeyRecordSize, *meta.keyLayout.dataOffset());
    }
    if (meta.hasPayload) {
      EXPECT_LE(meta.wireKeyRecordSize, meta.keyPayloadOffset);
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
        EXPECT_TRUE(
            std::all_of(slot, slot + sizeof(StringView), [](char value) {
              return value == 0;
            }));
        continue;
      }
      if (op.kind == RadixSortSpillPayloadVariableKind::kString) {
        const auto sourceValue = loadUnaligned<StringView>(sourceSlot);
        const auto diskValue = loadUnaligned<StringView>(slot);
        EXPECT_EQ(diskValue.size(), sourceValue.size());
        if (sourceValue.isInline()) {
          EXPECT_EQ(
              std::string_view(slot, sizeof(StringView)),
              std::string_view(sourceSlot, sizeof(StringView)));
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
      vector_size_t rowCount) {
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta, keyBase, rowCount, std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(batchSize.rowCount, rowCount);

    const auto keyRecordBytes = batchSize.rowCount * meta.wireKeyRecordSize;
    const auto payloadFixedBytes = batchSize.rowCount * meta.payloadFixedSize;
    std::vector<char> block(batchSize.totalBytes(meta.fixedWireBytesPerRow()));
    auto* keyRecords = block.data();
    auto* keyHeap = keyRecords + keyRecordBytes;
    auto* payloadFixed = keyHeap + batchSize.keyHeapBytes;
    auto* payloadHeap = payloadFixed + payloadFixedBytes;
    auto* keyHeapCursor = keyHeap;
    auto* payloadHeapCursor = payloadHeap;
    RadixSortSpillSections::copyRowsToSections(
        meta,
        keyBase,
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

  static RadixSortSpillSectionBatchSize batchSizeForSingleRow(
      const RadixSortSpillSectionMeta& meta,
      const char* key) {
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta, key, 1, std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(batchSize.rowCount, 1);
    return batchSize;
  }

  static uint64_t keyHeapBytesForRow(
      const RadixSortSpillSectionMeta& meta,
      const char* key) {
    if (!meta.hasKeyHeap) {
      return 0;
    }
    return loadUnaligned<uint64_t>(key + meta.keySizeOffset) -
        meta.keyHeapOffset;
  }

  static uint64_t payloadHeapBytesForRow(
      const RadixSortSpillSectionMeta& meta,
      const char* key) {
    return batchSizeForSingleRow(meta, key).payloadHeapBytes;
  }

  static uint64_t totalSizeForRow(
      const RadixSortSpillSectionMeta& meta,
      uint64_t keyHeapBytes,
      uint64_t payloadHeapBytes = 0) {
    return meta.fixedWireBytesPerRow() + keyHeapBytes + payloadHeapBytes;
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

  static uint64_t variableKeySizeDiskOffset(
      uint64_t blockOffset,
      uint64_t row,
      const RadixSortSpillSectionMeta& meta) {
    EXPECT_TRUE(meta.keyLayout.isVariable());
    return blockOffset + kBlockHeaderSize +
        row * static_cast<uint64_t>(meta.wireKeyRecordSize) +
        meta.keySizeOffset;
  }

  void overwriteSpillBytes(
      const std::string& path,
      uint64_t offset,
      const std::vector<char>& bytes) {
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    BOLT_CHECK(file.good(), path);
    file.seekp(offset);
    BOLT_CHECK(file.good(), "offset={}", offset);
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
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

  std::vector<UncompressedSpillBlock> readUncompressedBlocks(
      const RadixSortSpillFile& fileInfo) {
    std::ifstream file(fileInfo.path, std::ios::binary);
    EXPECT_TRUE(file.good());
    std::vector<UncompressedSpillBlock> blocks;
    uint64_t offset = 0;
    while (offset < fileInfo.size) {
      UncompressedSpillBlock block;
      file.read(reinterpret_cast<char*>(&block.header), sizeof(block.header));
      EXPECT_TRUE(file.good()) << "offset=" << offset;
      EXPECT_EQ(block.header.storedSize, block.header.uncompressedSize);
      block.body.resize(block.header.uncompressedSize);
      file.read(block.body.data(), block.body.size());
      EXPECT_TRUE(file.good()) << "offset=" << offset;
      offset += kBlockHeaderSize + block.header.storedSize;
      blocks.push_back(std::move(block));
    }
    EXPECT_EQ(offset, fileInfo.size);
    return blocks;
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

  static void expectSpillStreamKey(
      const RadixSortKeyLayout& keyLayout,
      const RadixSortMergeStream& stream,
      const char* expectedKey) {
    if (!keyLayout.isVariable()) {
      EXPECT_EQ(
          RadixSortKey(keyLayout, stream.key())
              .compare(RadixSortKey(keyLayout, expectedKey)),
          0);
      return;
    }
    ASSERT_NE(stream.key(), nullptr);
    ASSERT_TRUE(keyLayout.dataOffset().has_value());
    EXPECT_EQ(
        std::memcmp(stream.key(), expectedKey, *keyLayout.dataOffset()), 0);
    const auto* variable =
        dynamic_cast<const RadixSortVariableMergeStream*>(&stream);
    ASSERT_NE(variable, nullptr);
    EXPECT_EQ(
        variable->encodedSuffix().bytes,
        RadixSortKey(keyLayout, expectedKey).heapKey());
  }

  static std::string fixed8Key(uint8_t value) {
    std::string key(sizeof(uint64_t), '\0');
    key.back() = static_cast<char>(value);
    return key;
  }

  static uint64_t fixed8RowsPerBlock() {
    const auto meta = RadixSortSpillSectionMeta::create(
        RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8),
        nullptr);
    return (kFixedWriteBufferSize - kBlockHeaderSize) /
        meta.fixedWireBytesPerRow();
  }

  static std::string orderedFixed8EncodedKey(uint64_t value) {
    std::string key(sizeof(uint64_t), '\0');
    storeUnaligned<uint64_t>(key.data(), folly::Endian::big(value));
    return key;
  }

  static std::string orderedFixed8Key(uint64_t value) {
    std::string key(sizeof(uint64_t), '\0');
    storeUnaligned<uint64_t>(key.data(), value);
    return key;
  }

  std::vector<RadixSortSpillFile> spillLargeFixedKeyRunFiles(
      uint64_t rowCount = fixed8RowsPerBlock() + 17,
      uint64_t maxFileSize = 1) {
    const auto keyLayout =
        RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
    RadixSortRunStorage storage(pool_.get(), keyLayout, 1'024, 64);
    for (uint64_t row = 0; row < rowCount; ++row) {
      storage.append(orderedFixed8EncodedKey(row + 1));
    }
    auto files = spillRunFiles(storage, nullptr, maxFileSize);
    EXPECT_GT(files.size(), 1);
    uint64_t totalRows = 0;
    bool sawBoundary = false;
    for (const auto& file : files) {
      totalRows += file.rowCount;
      sawBoundary |= file.rowCount < rowCount;
    }
    EXPECT_EQ(totalRows, rowCount);
    EXPECT_TRUE(sawBoundary);
    return files;
  }

  SpilledRun spillSingleRun(
      const RadixSortRunStorage& storage,
      const PayloadRowLayout* payloadLayout,
      common::CompressionKind compression = common::CompressionKind_NONE,
      uint64_t writeBufferSize = 1 << 20) {
    auto directory = exec::test::TempDirectoryPath::create();
    folly::Synchronized<common::SpillStats> stats;
    const auto globalStatsBefore = common::globalSpillStats();
    RadixSortSpillWriter writer(
        directory->path + "/spill",
        spillConfig(directory->path, compression, writeBufferSize),
        pool_.get(),
        &stats);
    auto files = writer.writeRun(storage, payloadLayout);
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

  std::vector<RadixSortSpillFile> spillRunFiles(
      const RadixSortRunStorage& storage,
      const PayloadRowLayout* payloadLayout,
      uint64_t maxFileSize,
      uint64_t writeBufferSize = 1 << 20) {
    auto directory = exec::test::TempDirectoryPath::create();
    spillDirectories_.push_back(std::move(directory));
    folly::Synchronized<common::SpillStats> stats;
    const auto globalStatsBefore = common::globalSpillStats();

    RadixSortSpillWriter writer(
        spillDirectories_.back()->path + "/spill",
        spillConfig(
            spillDirectories_.back()->path,
            common::CompressionKind_NONE,
            writeBufferSize,
            maxFileSize),
        pool_.get(),
        &stats);
    auto files = writer.writeRun(storage, payloadLayout);

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
    auto stream = makeRadixSortSpillMergeStream(
        RadixSortSpillRun{files}, meta, pool_.get(), false);
    uint64_t row = 0;
    while (stream->hasData()) {
      ASSERT_LT(row, storage.size());
      expectSpillStreamKey(storage.layout(), *stream, storage.keyDataAt(row));
      ASSERT_NE(stream->payload(), nullptr);
      EXPECT_EQ(
          std::memcmp(
              stream->payload(),
              RadixSortKey(storage.layout(), storage.keyDataAt(row)).payload(),
              payloadLayout.rowWidth()),
          0)
          << "row=" << row;
      if (!stream->tryAdvance()) {
        stream->advanceAfterFlush();
      }
      ++row;
    }
    EXPECT_EQ(row, storage.size());
  }

  void appendAndVerify(
      RadixSortKeyLayoutKind kind,
      const std::vector<std::string>& keys,
      const RowVectorPtr& payload,
      const std::shared_ptr<const PayloadRowLayout>& payloadLayout,
      common::CompressionKind compression = common::CompressionKind_NONE,
      uint64_t writeBufferSize = 1 << 20) {
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
    auto spill =
        spillSingleRun(storage, layout.get(), compression, writeBufferSize);
    auto stream = makeRadixSortSpillMergeStream(
        RadixSortSpillRun{{spill.file}}, spill.meta, pool_.get(), false);
    vector_size_t outputOffset = 0;
    while (stream->hasData()) {
      ASSERT_LT(outputOffset, storage.size());
      expectSpillStreamKey(keyLayout, *stream, storage.keyDataAt(outputOffset));
      if (layout) {
        RowVectorPtr output;
        std::array<char*, 1> restoredPayloads{stream->payload()};
        PayloadRowReader::gather(
            *layout, restoredPayloads, pool_.get(), output);
        expectEquivalent(*payload, outputOffset, *output);
      }
      if (!stream->tryAdvance()) {
        stream->advanceAfterFlush();
      }
      ++outputOffset;
    }
    EXPECT_EQ(outputOffset, storage.size());
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
    auto spill = spillSingleRun(storage, payloadLayout.get());
    auto stream = makeRadixSortSpillMergeStream(
        RadixSortSpillRun{{spill.file}}, spill.meta, pool_.get(), false);
    ASSERT_TRUE(stream->hasData());
    expectSpillStreamKey(keyLayout, *stream, storage.keyDataAt(0));
    EXPECT_EQ(
        std::memcmp(stream->payload(), payload, payloadLayout->rowWidth()), 0);
    EXPECT_FALSE(stream->tryAdvance());
    stream->advanceAfterFlush();
    EXPECT_FALSE(stream->hasData());
  }

  void verifyMerge(
      const RadixSortKeyLayout& layout,
      const std::vector<std::vector<uint8_t>>& streamKeys) {
    std::vector<std::unique_ptr<RadixSortRunStorage>> runs;
    std::vector<std::vector<std::unique_ptr<int64_t>>> payloadValues;
    std::vector<std::pair<uint8_t, int64_t>> expected;
    payloadValues.resize(streamKeys.size());
    for (size_t stream = 0; stream < streamKeys.size(); ++stream) {
      auto storage =
          std::make_unique<RadixSortRunStorage>(pool_.get(), layout, 2, 64);
      for (size_t row = 0; row < streamKeys[stream].size(); ++row) {
        const auto key = streamKeys[stream][row];
        const auto payloadId =
            static_cast<int64_t>(stream * 1'000 + row * 10 + key);
        payloadValues[stream].push_back(std::make_unique<int64_t>(payloadId));
        storage->append(
            fixed8Key(key),
            reinterpret_cast<char*>(payloadValues[stream].back().get()));
        expected.emplace_back(key, payloadId);
      }
      runs.push_back(std::move(storage));
    }
    std::sort(expected.begin(), expected.end());
    RadixSortRunStorage expectedStorage(
        pool_.get(), layout, std::max<size_t>(expected.size(), 1), 64);
    for (const auto& [key, payloadId] : expected) {
      expectedStorage.append(fixed8Key(key), nullptr);
    }
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    for (const auto& run : runs) {
      streams.push_back(std::make_unique<RadixSortMemoryRunMergeStream>(*run));
    }
    RadixSortMerger merger(layout, std::move(streams));
    std::vector<const char*> keys(expected.size());
    std::vector<char*> payloads(expected.size());
    vector_size_t outputOffset = 0;
    const auto count = merger.collectRows(
        keys.size(),
        keys.data(),
        payloads.data(),
        std::span<EncodedKeyView>{},
        [&](vector_size_t size) {
          for (vector_size_t row = 0; row < size; ++row) {
            EXPECT_EQ(
                RadixSortKey(layout, keys[row])
                    .compare(RadixSortKey(
                        layout, expectedStorage.keyDataAt(outputOffset))),
                0)
                << "row=" << outputOffset;
            EXPECT_EQ(
                *reinterpret_cast<int64_t*>(payloads[row]),
                expected[outputOffset].second)
                << "row=" << outputOffset;
            ++outputOffset;
          }
        });
    ASSERT_EQ(count, expected.size());
    EXPECT_EQ(outputOffset, expected.size());
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
      const std::vector<std::string>& values,
      std::shared_ptr<const PayloadRowLayout>& payloadLayout) {
    std::vector<std::optional<std::string>> optionalValues;
    optionalValues.reserve(values.size());
    for (const auto& value : values) {
      optionalValues.push_back(value);
    }
    auto payload =
        makeRows({"payload_string"}, {makeStringVector(optionalValues)});
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

  struct VariableSpillFixture {
    SpilledRun spill;
    std::shared_ptr<const PayloadRowLayout> payloadLayout;
  };

  VariableSpillFixture writeVariableKeySpill(
      RadixSortKeyLayoutKind kind,
      const std::vector<std::string>& keys) {
    auto keyLayout = RadixSortKeyLayout::fromKind(kind);
    BOLT_CHECK(keyLayout.isVariable());
    std::shared_ptr<const PayloadRowLayout> payloadLayout;
    RowVectorPtr payload;
    PayloadRowBatch payloadBatch;
    if (keyLayout.hasPayload()) {
      std::vector<std::optional<int64_t>> values;
      values.reserve(keys.size());
      for (size_t row = 0; row < keys.size(); ++row) {
        values.push_back(static_cast<int64_t>(row + 1));
      }
      payload = makeRows({"payload"}, {makeVector<int64_t>(BIGINT(), values)});
      payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
    }
    RadixSortRunStorage storage(
        pool_.get(), keyLayout, 8, 1 << 20, payloadLayout, 8, 1024);
    if (payloadLayout) {
      PayloadRowWriter{}.append(*payload, storage, payloadBatch);
    }
    for (size_t row = 0; row < keys.size(); ++row) {
      storage.append(
          keys[row], payloadLayout ? payloadBatch.rowAt(row) : nullptr);
    }
    return {spillSingleRun(storage, payloadLayout.get()), payloadLayout};
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

  for (size_t i = 0; i < kLayouts.size(); ++i) {
    const auto kind = kLayouts[i];
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    const auto keyLayout = RadixSortKeyLayout::fromKind(kind);
    const auto* layout = keyLayout.hasPayload() ? payloadLayout.get() : nullptr;
    const auto meta = RadixSortSpillSectionMeta::create(keyLayout, layout);

    EXPECT_EQ(meta.runtimeKeyRecordSize, keyLayout.width());
    EXPECT_EQ(meta.runtimeKeyRecordSize, kRuntimeKeyRecordWidths[i]);
    EXPECT_EQ(meta.wireKeyRecordSize, kWireKeyRecordWidths[i]);
    EXPECT_EQ(
        meta.fixedWireBytesPerRow(),
        kWireKeyRecordWidths[i] + meta.payloadFixedSize);
    if (keyLayout.isVariable()) {
      EXPECT_EQ(meta.keyHeapOffset, keyLayout.heapKeyOffset());
      EXPECT_EQ(meta.keySizeOffset, *keyLayout.sizeOffset());
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

TEST_F(
    RadixSortSpillSectionsTest,
    copyRowsUsesRuntimeSourceAndCompactWireStridesForEveryLayout) {
  auto payload =
      makeRows({"payload"}, {makeVector<int64_t>(BIGINT(), {101, 202, 303})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  constexpr uint8_t kSentinel = 0xa5;

  for (size_t layoutIndex = 0; layoutIndex < kLayouts.size(); ++layoutIndex) {
    const auto kind = kLayouts[layoutIndex];
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    const auto keyLayout = RadixSortKeyLayout::fromKind(kind);
    std::shared_ptr<const PayloadRowLayout> layout =
        keyLayout.hasPayload() ? payloadLayout : nullptr;
    RadixSortRunStorage storage(
        pool_.get(), keyLayout, 4, 1024, layout, 4, 1024);
    PayloadRowBatch payloadRows;
    if (layout) {
      PayloadRowWriter{}.append(*payload, storage, payloadRows);
    }
    for (vector_size_t row = 0; row < 3; ++row) {
      const auto keySize = keyLayout.isVariable()
          ? keyLayout.inlineCapacity() + 20 + row
          : keyLayout.inlineCapacity();
      std::string key(keySize, static_cast<char>('a' + row));
      key.back() = static_cast<char>('x' + row);
      storage.append(key, layout ? payloadRows.rowAt(row) : nullptr);
    }

    const auto meta =
        RadixSortSpillSectionMeta::create(keyLayout, layout.get());
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta,
        storage.keyDataAt(0),
        storage.size(),
        std::numeric_limits<uint64_t>::max());
    ASSERT_EQ(batchSize.rowCount, storage.size());
    const auto keyRecordBytes = batchSize.rowCount * meta.wireKeyRecordSize;
    const auto payloadFixedBytes = batchSize.rowCount * meta.payloadFixedSize;

    std::vector<uint8_t> guarded(
        batchSize.totalBytes(meta.fixedWireBytesPerRow()) + 2, kSentinel);
    auto* const keyRecords = reinterpret_cast<char*>(guarded.data() + 1);
    auto* const keyHeap = keyRecords + keyRecordBytes;
    auto* const payloadFixed = keyHeap + batchSize.keyHeapBytes;
    auto* const payloadHeap = payloadFixed + payloadFixedBytes;
    auto* keyHeapCursor = keyHeap;
    auto* payloadHeapCursor = payloadHeap;
    RadixSortSpillSections::copyRowsToSections(
        meta,
        storage.keyDataAt(0),
        storage.size(),
        batchSize.keyHeapBytes,
        batchSize.payloadHeapBytes,
        keyRecords,
        keyHeapCursor,
        payloadFixed,
        payloadHeapCursor);

    EXPECT_EQ(guarded.front(), kSentinel);
    EXPECT_EQ(guarded.back(), kSentinel);
    const char* expectedKeyHeap = keyHeap;
    for (vector_size_t row = 0; row < storage.size(); ++row) {
      const auto* source = storage.keyDataAt(row);
      const auto* wire =
          keyRecords + static_cast<uint64_t>(row) * meta.wireKeyRecordSize;
      EXPECT_EQ(std::memcmp(wire, source, meta.wireKeyRecordSize), 0)
          << "row=" << row;
      if (keyLayout.isVariable()) {
        const auto sourceKey = RadixSortKey(keyLayout, source);
        EXPECT_EQ(
            std::string_view(expectedKeyHeap, sourceKey.heapSize()),
            sourceKey.heapKey());
        expectedKeyHeap += sourceKey.heapSize();
      }
      if (layout) {
        EXPECT_EQ(
            std::memcmp(
                payloadFixed +
                    static_cast<uint64_t>(row) * meta.payloadFixedSize,
                RadixSortKey(keyLayout, source).payload(),
                meta.payloadFixedSize),
            0);
      }
    }
    EXPECT_EQ(expectedKeyHeap, keyHeap + batchSize.keyHeapBytes);
    EXPECT_EQ(keyHeapCursor, keyHeap + batchSize.keyHeapBytes);
    EXPECT_EQ(payloadHeapCursor, payloadHeap + batchSize.payloadHeapBytes);
    EXPECT_EQ(meta.runtimeKeyRecordSize, kRuntimeKeyRecordWidths[layoutIndex]);
    EXPECT_EQ(meta.wireKeyRecordSize, kWireKeyRecordWidths[layoutIndex]);
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    pointerBearingLayoutsOmitKeyRecordPointersFromWire) {
  constexpr std::array kPointerLayouts{
      RadixSortKeyLayoutKind::kKeyOnlyVariable32,
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed24,
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed32,
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32};
  constexpr uint8_t kUnwritten = 0xa5;

  auto payload = makeRows({"payload"}, {makeVector<int64_t>(BIGINT(), {123})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));

  for (const auto kind : kPointerLayouts) {
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    const auto keyLayout = RadixSortKeyLayout::fromKind(kind);
    const auto layout = keyLayout.hasPayload() ? payloadLayout : nullptr;
    RadixSortRunStorage storage(
        pool_.get(), keyLayout, 1, 1024, layout, 1, 1024);
    PayloadRowBatch payloadBatch;
    if (layout) {
      PayloadRowWriter{}.append(*payload, storage, payloadBatch);
    }

    const auto keySize = keyLayout.isVariable()
        ? keyLayout.inlineCapacity() + 32
        : keyLayout.inlineCapacity();
    storage.append(
        std::string(keySize, 'k'), layout ? payloadBatch.rowAt(0) : nullptr);

    auto* const runtimeKey = storage.keyDataAt(0);
    const auto meta =
        RadixSortSpillSectionMeta::create(keyLayout, layout.get());
    ASSERT_LT(meta.wireKeyRecordSize, meta.runtimeKeyRecordSize);
    const auto removedPointerCount =
        static_cast<uint32_t>(keyLayout.isVariable()) +
        static_cast<uint32_t>(keyLayout.hasPayload());
    EXPECT_EQ(
        meta.runtimeKeyRecordSize - meta.wireKeyRecordSize,
        removedPointerCount * kCompactPointerBytes);

    std::vector<char> poisonedKeyHeap;
    std::vector<char> poisonedPayloadFixed;
    if (keyLayout.isVariable()) {
      auto* const originalKeyHeap =
          loadCompactPointer(runtimeKey + *keyLayout.dataOffset());
      poisonedKeyHeap.resize(
          loadUnaligned<uint64_t>(runtimeKey + *keyLayout.sizeOffset()) -
              keyLayout.heapKeyOffset(),
          'd');
      storeCompactPointer(
          runtimeKey + *keyLayout.dataOffset(), poisonedKeyHeap.data());
      EXPECT_NE(poisonedKeyHeap.data(), originalKeyHeap);
      EXPECT_EQ(
          loadCompactPointer(runtimeKey + *keyLayout.dataOffset()),
          poisonedKeyHeap.data());
    }
    if (keyLayout.hasPayload()) {
      auto* const originalPayload =
          loadCompactPointer(runtimeKey + *keyLayout.payloadOffset());
      poisonedPayloadFixed.resize(meta.payloadFixedSize, 'p');
      storeCompactPointer(
          runtimeKey + *keyLayout.payloadOffset(), poisonedPayloadFixed.data());
      EXPECT_NE(poisonedPayloadFixed.data(), originalPayload);
      EXPECT_EQ(
          loadCompactPointer(runtimeKey + *keyLayout.payloadOffset()),
          poisonedPayloadFixed.data());
    }

    const auto batchSize = batchSizeForSingleRow(meta, runtimeKey);
    std::vector<uint8_t> guardedKeyRecord(
        meta.runtimeKeyRecordSize, kUnwritten);
    std::vector<char> keyHeap(std::max<uint64_t>(1, batchSize.keyHeapBytes));
    std::vector<char> payloadFixed(
        std::max<uint64_t>(1, meta.payloadFixedSize));
    std::vector<char> payloadHeap(
        std::max<uint64_t>(1, batchSize.payloadHeapBytes));
    auto* keyHeapCursor = keyHeap.data();
    auto* payloadHeapCursor = payloadHeap.data();
    RadixSortSpillSections::copyRowsToSections(
        meta,
        runtimeKey,
        1,
        batchSize.keyHeapBytes,
        batchSize.payloadHeapBytes,
        reinterpret_cast<char*>(guardedKeyRecord.data()),
        keyHeapCursor,
        payloadFixed.data(),
        payloadHeapCursor);

    EXPECT_EQ(
        std::memcmp(
            guardedKeyRecord.data(), runtimeKey, meta.wireKeyRecordSize),
        0);
    EXPECT_TRUE(std::all_of(
        guardedKeyRecord.begin() + meta.wireKeyRecordSize,
        guardedKeyRecord.end(),
        [](uint8_t value) { return value == kUnwritten; }));
    if (keyLayout.isVariable()) {
      EXPECT_EQ(
          std::string_view(keyHeap.data(), batchSize.keyHeapBytes),
          std::string_view(poisonedKeyHeap.data(), poisonedKeyHeap.size()));
    }
    if (keyLayout.hasPayload()) {
      EXPECT_EQ(
          std::string_view(payloadFixed.data(), meta.payloadFixedSize),
          std::string_view(
              poisonedPayloadFixed.data(), poisonedPayloadFixed.size()));
    }
    if (keyLayout.isVariable()) {
      EXPECT_EQ(
          loadCompactPointer(runtimeKey + *keyLayout.dataOffset()),
          poisonedKeyHeap.data());
    }
    if (keyLayout.hasPayload()) {
      EXPECT_EQ(
          loadCompactPointer(runtimeKey + *keyLayout.payloadOffset()),
          poisonedPayloadFixed.data());
    }

    if (kind != RadixSortKeyLayoutKind::kKeyWithPayloadVariable32) {
      continue;
    }

    // One end-to-end case is sufficient in addition to the five-layout
    // direct-copy matrix above. This layout exercises both omitted pointers.
    auto spill = spillSingleRun(storage, layout.get());
    const auto block = readUncompressedBlock(spill.file);
    ASSERT_EQ(block.header.rowCount, 1);
    ASSERT_EQ(block.header.keyRecordBytes, meta.wireKeyRecordSize);
    EXPECT_EQ(
        std::memcmp(block.body.data(), runtimeKey, meta.wireKeyRecordSize), 0);
    auto stream = makeRadixSortSpillMergeStream(
        RadixSortSpillRun{{spill.file}}, spill.meta, pool_.get(), false);
    ASSERT_TRUE(stream->hasData());
    EXPECT_EQ(
        std::memcmp(stream->key(), runtimeKey, meta.wireKeyRecordSize), 0);
    const auto* variable =
        dynamic_cast<const RadixSortVariableMergeStream*>(stream.get());
    ASSERT_NE(variable, nullptr);
    EXPECT_EQ(
        variable->encodedSuffix().bytes,
        std::string_view(poisonedKeyHeap.data(), poisonedKeyHeap.size()));
    ASSERT_NE(stream->payload(), nullptr);
    EXPECT_EQ(
        std::string_view(stream->payload(), meta.payloadFixedSize),
        std::string_view(
            poisonedPayloadFixed.data(), poisonedPayloadFixed.size()));
    EXPECT_FALSE(stream->tryAdvance());
    stream->advanceAfterFlush();
    EXPECT_FALSE(stream->hasData());
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
  const auto fixedRowBytes = static_cast<uint64_t>(meta.wireKeyRecordSize);
  auto expectSize =
      [&](uint64_t maxRowCount, uint64_t maxBytes, uint64_t expectedRows) {
        const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
            meta, storage.keyDataAt(0), maxRowCount, maxBytes);
        EXPECT_EQ(batchSize.rowCount, expectedRows);
        EXPECT_EQ(batchSize.keyHeapBytes, 0);
        EXPECT_EQ(batchSize.payloadHeapBytes, 0);
        EXPECT_EQ(
            batchSize.totalBytes(meta.fixedWireBytesPerRow()),
            expectedRows * fixedRowBytes);
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
  const auto fixedRowBytes = meta.fixedWireBytesPerRow();
  auto expectSize = [&](uint64_t maxBytes, uint64_t expectedRows) {
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta, storage.keyDataAt(0), storage.size(), maxBytes);
    EXPECT_EQ(batchSize.rowCount, expectedRows);
    EXPECT_EQ(batchSize.keyHeapBytes, 0);
    EXPECT_EQ(batchSize.payloadHeapBytes, 0);
    EXPECT_EQ(
        batchSize.totalBytes(meta.fixedWireBytesPerRow()),
        expectedRows * fixedRowBytes);
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
  std::vector<uint64_t> perRowKeyHeapBytes;
  for (const auto& key : keys) {
    perRowKeyHeapBytes.push_back(key.size() - keyLayout.heapKeyOffset());
  }
  auto expectSize = [&](uint64_t maxBytes, uint64_t expectedRows) {
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta, storage.keyDataAt(0), storage.size(), maxBytes);
    EXPECT_EQ(batchSize.rowCount, expectedRows);
    uint64_t expectedKeyHeapBytes = 0;
    for (uint64_t row = 0; row < expectedRows; ++row) {
      expectedKeyHeapBytes += keyHeapBytesForRow(meta, storage.keyDataAt(row));
    }
    const auto expectedTotalBytes =
        expectedRows * meta.fixedWireBytesPerRow() + expectedKeyHeapBytes;
    EXPECT_EQ(batchSize.keyHeapBytes, expectedKeyHeapBytes);
    EXPECT_EQ(batchSize.payloadHeapBytes, 0);
    EXPECT_EQ(
        batchSize.totalBytes(meta.fixedWireBytesPerRow()), expectedTotalBytes);
  };

  EXPECT_EQ(
      keyHeapBytesForRow(meta, storage.keyDataAt(0)), perRowKeyHeapBytes[0]);
  EXPECT_EQ(
      keyHeapBytesForRow(meta, storage.keyDataAt(1)), perRowKeyHeapBytes[1]);
  EXPECT_EQ(
      keyHeapBytesForRow(meta, storage.keyDataAt(2)), perRowKeyHeapBytes[2]);
  const auto firstRowBytes = totalSizeForRow(meta, perRowKeyHeapBytes[0]);
  const auto secondRowBytes = totalSizeForRow(meta, perRowKeyHeapBytes[1]);
  const auto thirdRowBytes = totalSizeForRow(meta, perRowKeyHeapBytes[2]);
  expectSize(firstRowBytes - 1, 0);
  expectSize(firstRowBytes, 1);
  expectSize(firstRowBytes + secondRowBytes - 1, 1);
  expectSize(firstRowBytes + secondRowBytes, 2);
  expectSize(firstRowBytes + secondRowBytes + thirdRowBytes, 3);
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
  const auto fixedRowBytes = meta.fixedWireBytesPerRow();
  const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
      meta,
      storage.keyDataAt(0),
      storage.size(),
      fixedRowBytes * storage.size());
  EXPECT_EQ(batchSize.rowCount, storage.size());
  EXPECT_EQ(batchSize.keyHeapBytes, 0);
  EXPECT_EQ(batchSize.payloadHeapBytes, 0);
  EXPECT_EQ(
      batchSize.totalBytes(meta.fixedWireBytesPerRow()),
      fixedRowBytes * storage.size());
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
      batchSizeForSingleRow(meta, storage.keyDataAt(0)),
      batchSizeForSingleRow(meta, storage.keyDataAt(1))};
  auto expectSize = [&](uint64_t maxBytes, uint64_t expectedRows) {
    const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta, storage.keyDataAt(0), storage.size(), maxBytes);
    EXPECT_EQ(batchSize.rowCount, expectedRows);
    uint64_t expectedKeyHeapBytes = 0;
    uint64_t expectedPayloadHeapTotal = 0;
    uint64_t expectedTotalBytes = 0;
    for (uint64_t row = 0; row < expectedRows; ++row) {
      expectedKeyHeapBytes += expected[row].keyHeapBytes;
      expectedPayloadHeapTotal += expected[row].payloadHeapBytes;
      expectedTotalBytes +=
          expected[row].totalBytes(meta.fixedWireBytesPerRow());
    }
    EXPECT_EQ(batchSize.keyHeapBytes, expectedKeyHeapBytes);
    EXPECT_EQ(batchSize.payloadHeapBytes, expectedPayloadHeapTotal);
    EXPECT_EQ(
        batchSize.totalBytes(meta.fixedWireBytesPerRow()), expectedTotalBytes);
  };

  EXPECT_EQ(
      expected[0].keyHeapBytes, keyHeapBytesForRow(meta, storage.keyDataAt(0)));
  EXPECT_EQ(
      expected[1].keyHeapBytes, keyHeapBytesForRow(meta, storage.keyDataAt(1)));
  const auto firstRowBytes =
      expected[0].totalBytes(meta.fixedWireBytesPerRow());
  const auto secondRowBytes =
      expected[1].totalBytes(meta.fixedWireBytesPerRow());
  expectSize(firstRowBytes - 1, 0);
  expectSize(firstRowBytes, 1);
  expectSize(firstRowBytes + secondRowBytes - 1, 1);
  expectSize(firstRowBytes + secondRowBytes, 2);
}

TEST_F(
    RadixSortSpillSectionsTest,
    batchSizeVariablePayloadOverflowReturnsZeroRows) {
  const auto payloadType = ROW(
      {"array_col", "row_col"}, {ARRAY(INTEGER()), ROW({"v"}, {VARCHAR()})});
  auto payloadLayout = PayloadRowLayout::create(payloadType);
  ASSERT_EQ(payloadLayout->columns().size(), 2);
  ASSERT_TRUE(payloadLayout->columns()[0].complex);
  ASSERT_TRUE(payloadLayout->columns()[1].complex);

  const auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  const auto meta =
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  ASSERT_EQ(meta.payloadVariableOps.size(), 2);

  auto payloadFixed =
      AlignedBuffer::allocate<char>(payloadLayout->rowWidth(), pool_.get(), 0);
  auto* const row = payloadFixed->asMutable<char>();
  std::memset(row, 0xff, payloadLayout->nullBytes());
  storeUnaligned<PayloadVarlenRef>(
      row + payloadLayout->columns()[0].offset,
      PayloadVarlenRef{std::numeric_limits<uint64_t>::max() - 8, nullptr});
  storeUnaligned<PayloadVarlenRef>(
      row + payloadLayout->columns()[1].offset, PayloadVarlenRef{16, nullptr});

  std::array<char, 16> key{};
  key[sizeof(uint64_t) - 1] = 1;
  storeCompactPointer(key.data() + *keyLayout.payloadOffset(), row);

  const auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
      meta, key.data(), 1, std::numeric_limits<uint64_t>::max());
  EXPECT_EQ(batchSize.rowCount, 0);
  EXPECT_EQ(batchSize.keyHeapBytes, 0);
  EXPECT_EQ(batchSize.payloadHeapBytes, 0);
  EXPECT_EQ(batchSize.totalBytes(meta.fixedWireBytesPerRow()), 0);
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
  EXPECT_EQ(header.reserved, kCurrentRadixSortSpillFormat);
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

  RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
  const auto view = reader.nextBatch();
  ASSERT_TRUE(view.has_value());
  EXPECT_EQ(
      view->keyHeapBegin,
      view->keyRecordsBegin + storage.size() * spill.meta.wireKeyRecordSize);
  EXPECT_EQ(view->payloadFixedBegin, view->keyHeapBegin);
  EXPECT_EQ(
      RadixSortKey(keyLayout, view->keyRecordsBegin)
          .compare(RadixSortKey(keyLayout, storage.keyDataAt(0))),
      0);
  EXPECT_EQ(
      RadixSortKey(
          keyLayout, view->keyRecordsBegin + spill.meta.wireKeyRecordSize)
          .compare(RadixSortKey(keyLayout, storage.keyDataAt(1))),
      0);
  EXPECT_FALSE(reader.nextBatch().has_value());
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
  const auto serializedSize = batchSizeForSingleRow(meta, key);
  EXPECT_EQ(serializedSize.keyHeapBytes, 0);
  EXPECT_EQ(
      serializedSize.totalBytes(meta.fixedWireBytesPerRow()),
      meta.wireKeyRecordSize + meta.payloadFixedSize);

  auto block = materializeSectionBlock(meta, key, 1);
  auto* keyRecords = block.data();
  auto* keyHeap = keyRecords + meta.wireKeyRecordSize;
  auto* payloadFixed = keyHeap + serializedSize.keyHeapBytes;
  auto* payloadHeap = payloadFixed + meta.payloadFixedSize;

  expectDiskKeyRecord(meta, keyRecords, key);
  EXPECT_EQ(
      std::memcmp(
          payloadFixed, payloadBatch.rowAt(0), payloadLayout->rowWidth()),
      0);
  EXPECT_EQ(payloadHeap, block.data() + block.size());

  char* payloadHeapCursor = payloadHeap;
  const auto restored =
      RadixSortSpillSections::restorePayloadPointersInSectionRows(
          meta, 1, payloadFixed, payloadHeapCursor, payloadHeap);
  ASSERT_TRUE(restored);
  EXPECT_EQ(payloadHeapCursor, payloadHeap);

  std::array<char*, 1> rows{payloadFixed};
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
  const auto size = batchSizeForSingleRow(meta, storedKey);
  EXPECT_EQ(size.keyHeapBytes, key.size() - keyLayout.heapKeyOffset());
  EXPECT_EQ(
      size.totalBytes(meta.fixedWireBytesPerRow()),
      meta.wireKeyRecordSize + size.keyHeapBytes);

  auto block = materializeSectionBlock(meta, storedKey, 1);
  auto* keyRecords = block.data();
  auto* keyHeap = keyRecords + meta.wireKeyRecordSize;

  expectDiskKeyRecord(meta, keyRecords, storedKey);
  EXPECT_EQ(
      loadUnaligned<uint64_t>(keyRecords + *keyLayout.sizeOffset()),
      key.size());
  EXPECT_EQ(
      std::string_view(keyHeap, size.keyHeapBytes),
      std::string_view(key).substr(keyLayout.heapKeyOffset()));
  std::string restoredKey(
      std::string_view(keyRecords, keyLayout.heapKeyOffset()));
  restoredKey.append(keyHeap, size.keyHeapBytes);
  EXPECT_EQ(restoredKey, key);
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
  const auto serializedSize = batchSizeForSingleRow(meta, key);
  auto block = materializeSectionBlock(meta, key, 1);
  EXPECT_EQ(serializedSize.keyHeapBytes, longKey.size());
  EXPECT_EQ(serializedSize.payloadHeapBytes, payloadBatch.heapSizeAt(0));
  EXPECT_EQ(
      serializedSize.totalBytes(meta.fixedWireBytesPerRow()),
      meta.wireKeyRecordSize + serializedSize.keyHeapBytes +
          meta.payloadFixedSize + serializedSize.payloadHeapBytes);

  auto* keyRecord = block.data();
  auto* keyHeap = keyRecord + meta.wireKeyRecordSize;
  auto* fixed = keyHeap + serializedSize.keyHeapBytes;
  auto* payloadHeap = fixed + meta.payloadFixedSize;
  expectDiskKeyRecord(meta, keyRecord, key);
  EXPECT_EQ(
      loadUnaligned<uint64_t>(keyRecord + *keyLayout.sizeOffset()),
      longKey.size());
  EXPECT_EQ(
      std::string_view(keyHeap, serializedSize.keyHeapBytes),
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

  char* payloadHeapCursor = payloadHeap;
  const auto restored =
      RadixSortSpillSections::restorePayloadPointersInSectionRows(
          meta,
          1,
          fixed,
          payloadHeapCursor,
          payloadHeap + serializedSize.payloadHeapBytes);
  ASSERT_TRUE(restored);
  EXPECT_EQ(payloadHeapCursor, payloadHeap + serializedSize.payloadHeapBytes);

  EXPECT_EQ(
      std::string(keyRecord, keyLayout.heapKeyOffset()) +
          std::string(keyHeap, serializedSize.keyHeapBytes),
      longKey);
  const auto restoredString =
      loadUnaligned<StringView>(fixed + payloadLayout->columns()[0].offset);
  EXPECT_EQ(
      std::string(restoredString.data(), restoredString.size()), longText);
  const auto restoredNested = loadUnaligned<PayloadVarlenRef>(
      fixed + payloadLayout->columns()[1].offset);
  EXPECT_EQ(restoredNested.data, payloadHeap + longText.size());
  EXPECT_EQ(
      restoredNested.size,
      loadUnaligned<PayloadVarlenRef>(
          payloadBatch.rowAt(0) + payloadLayout->columns()[1].offset)
          .size);

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
      static_cast<uint64_t>(keys.size()) * meta.wireKeyRecordSize;
  const auto payloadFixedBytes =
      static_cast<uint64_t>(keys.size()) * meta.payloadFixedSize;
  uint64_t keyHeapBytes = 0;
  uint64_t payloadHeapBytes = 0;
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    keyHeapBytes +=
        batchSizeForSingleRow(meta, storage.keyDataAt(row)).keyHeapBytes;
    payloadHeapBytes += payloadBatch.heapSizeAt(row);
  }
  const auto expectedBodySize =
      keyRecordBytes + keyHeapBytes + payloadFixedBytes + payloadHeapBytes;

  const auto block = readUncompressedBlock(spill.file);
  const auto& header = block.header;
  EXPECT_EQ(header.uncompressedSize, expectedBodySize);
  EXPECT_EQ(header.storedSize, expectedBodySize);
  EXPECT_EQ(header.rowCount, keys.size());
  EXPECT_EQ(header.reserved, kCurrentRadixSortSpillFormat);
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
        keyRecords + static_cast<uint64_t>(row) * meta.wireKeyRecordSize,
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

TEST_F(RadixSortSpillSectionsTest, writerDoesNotFlushAtStorageBlockBoundary) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  constexpr uint32_t kRowsPerBlock = RadixSortRunStorage::kTestingRowsPerBlock;
  RadixSortRunStorage storage(pool_.get(), keyLayout, kRowsPerBlock, 64);
  for (uint32_t row = 0; row <= kRowsPerBlock; ++row) {
    storage.append(fixed8Key(static_cast<uint8_t>(row + 1)));
  }
  ASSERT_EQ(storage.keyBlocks().size(), 2);

  auto spill = spillSingleRun(
      storage,
      nullptr,
      common::CompressionKind_NONE,
      /*writeBufferSize=*/kBlockHeaderSize +
          storage.size() * keyLayout.width());
  const auto blocks = readUncompressedBlocks(spill.file);
  ASSERT_EQ(blocks.size(), 1);
  EXPECT_EQ(blocks[0].header.rowCount, storage.size());
  EXPECT_EQ(
      blocks[0].header.keyRecordBytes, storage.size() * keyLayout.width());
}

TEST_F(
    RadixSortSpillSectionsTest,
    writerPacksVariableKeyAndPayloadRangesIntoOneSpillBlock) {
  const std::vector<std::string> keys{
      "key_0|" + std::string(80, 'a'),
      "key_1|" + std::string(88, 'b'),
      "key_2|" + std::string(96, 'c'),
      "key_3|" + std::string(104, 'd'),
      "key_4|" + std::string(112, 'e')};
  const std::vector<std::string> values{
      "payload_0|" + std::string(72, 'u'),
      "payload_1|" + std::string(80, 'v'),
      "payload_2|" + std::string(88, 'w'),
      "payload_3|" + std::string(96, 'x'),
      "payload_4|" + std::string(104, 'y')};
  const std::vector<std::optional<std::string>> payloadValues(
      values.begin(), values.end());
  auto payload = makeRows({"payload"}, {makeStringVector(payloadValues)});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  const auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  RadixSortRunStorage storage(
      pool_.get(),
      keyLayout,
      /*keysPerBlock=*/2,
      /*preferredHeapGroupBytes=*/64,
      payloadLayout,
      /*payloadRowsPerBlock=*/3,
      /*preferredPayloadHeapGroupBytes=*/64);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter{}.append(*payload, storage, payloadBatch);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    storage.append(keys[row], payloadBatch.rowAt(row));
  }

  ASSERT_EQ(storage.keyBlocks().size(), 3);
  ASSERT_EQ(storage.keyBlocks()[0].count, 2);
  ASSERT_EQ(storage.keyBlocks()[1].count, 2);
  ASSERT_EQ(storage.keyBlocks()[2].count, 1);
  ASSERT_GT(storage.keyHeapGroups().size(), 1);
  ASSERT_EQ(storage.payloadFixedBlocks().size(), 2);
  ASSERT_EQ(storage.payloadFixedBlocks()[0].count, 3);
  ASSERT_EQ(storage.payloadFixedBlocks()[1].count, 2);
  ASSERT_GT(storage.payloadHeapGroups().size(), 1);

  auto spill = spillSingleRun(storage, payloadLayout.get());
  const auto blocks = readUncompressedBlocks(spill.file);
  ASSERT_EQ(blocks.size(), 1);
  const auto& block = blocks.front();
  const auto& header = block.header;
  const auto& meta = spill.meta;
  ASSERT_EQ(header.rowCount, keys.size());
  ASSERT_EQ(
      header.keyRecordBytes,
      keys.size() * static_cast<uint64_t>(meta.wireKeyRecordSize));
  ASSERT_EQ(
      header.payloadFixedBytes,
      keys.size() * static_cast<uint64_t>(meta.payloadFixedSize));

  uint64_t expectedKeyHeapBytes = 0;
  uint64_t expectedPayloadHeapBytes = 0;
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    expectedKeyHeapBytes += keys[row].size() - keyLayout.heapKeyOffset();
    expectedPayloadHeapBytes += values[row].size();
  }
  ASSERT_EQ(header.keyHeapBytes, expectedKeyHeapBytes);
  ASSERT_EQ(header.payloadHeapBytes, expectedPayloadHeapBytes);

  const auto* const keyRecords = block.body.data();
  const auto* keyHeapCursor = keyRecords + header.keyRecordBytes;
  const auto* const payloadFixed = keyHeapCursor + header.keyHeapBytes;
  const auto* payloadHeapCursor = payloadFixed + header.payloadFixedBytes;
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    SCOPED_TRACE(row);
    const auto* const wireKey =
        keyRecords + static_cast<uint64_t>(row) * meta.wireKeyRecordSize;
    expectDiskKeyRecord(meta, wireKey, storage.keyDataAt(row));
    const auto keyHeapBytes = keys[row].size() - keyLayout.heapKeyOffset();
    EXPECT_EQ(
        std::string_view(keyHeapCursor, keyHeapBytes),
        std::string_view(keys[row]).substr(keyLayout.heapKeyOffset()));
    keyHeapCursor += keyHeapBytes;

    const auto* const fixed =
        payloadFixed + static_cast<uint64_t>(row) * meta.payloadFixedSize;
    const auto serializedValue =
        loadUnaligned<StringView>(fixed + payloadLayout->columns()[0].offset);
    ASSERT_FALSE(serializedValue.isInline());
    EXPECT_EQ(serializedValue.size(), values[row].size());
    EXPECT_EQ(
        stringPointerBytes(fixed + payloadLayout->columns()[0].offset),
        nullptr);
    EXPECT_EQ(
        std::string_view(payloadHeapCursor, values[row].size()), values[row]);
    payloadHeapCursor += values[row].size();
  }
  EXPECT_EQ(keyHeapCursor, payloadFixed);
  EXPECT_EQ(
      payloadHeapCursor,
      block.body.data() + static_cast<uint64_t>(header.uncompressedSize));

  auto stream = makeRadixSortSpillMergeStream(
      RadixSortSpillRun{{spill.file}}, meta, pool_.get(), false);
  ASSERT_TRUE(stream->hasData());
  const auto* const streamKeyRecords = stream->key();
  const auto* expectedStreamKeyHeap = streamKeyRecords + header.keyRecordBytes;
  const auto* const streamPayloadFixed =
      expectedStreamKeyHeap + header.keyHeapBytes;
  auto* expectedStreamPayloadFixed = const_cast<char*>(streamPayloadFixed);
  const auto* const streamPayloadHeap =
      streamPayloadFixed + header.payloadFixedBytes;
  const auto* expectedStreamPayloadHeap = streamPayloadHeap;
  const auto* const expectedStreamPayloadHeapEnd =
      expectedStreamPayloadHeap + header.payloadHeapBytes;
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    SCOPED_TRACE(row);
    ASSERT_TRUE(stream->hasData());
    EXPECT_EQ(
        stream->key(),
        streamKeyRecords + static_cast<uint64_t>(row) * meta.wireKeyRecordSize);
    const auto* variable =
        dynamic_cast<const RadixSortVariableMergeStream*>(stream.get());
    ASSERT_NE(variable, nullptr);
    const auto suffix =
        std::string_view(keys[row]).substr(keyLayout.heapKeyOffset());
    EXPECT_EQ(variable->encodedSuffix().bytes, suffix);
    EXPECT_EQ(variable->encodedSuffix().bytes.data(), expectedStreamKeyHeap);
    expectedStreamKeyHeap += suffix.size();

    EXPECT_EQ(stream->payload(), expectedStreamPayloadFixed);
    const auto restoredValue = loadUnaligned<StringView>(
        stream->payload() + payloadLayout->columns()[0].offset);
    EXPECT_EQ(restoredValue, StringView(values[row]));
    EXPECT_EQ(restoredValue.data(), expectedStreamPayloadHeap);
    expectedStreamPayloadFixed += meta.payloadFixedSize;
    expectedStreamPayloadHeap += values[row].size();
    if (row + 1 == keys.size()) {
      EXPECT_EQ(expectedStreamKeyHeap, streamPayloadFixed);
      EXPECT_EQ(expectedStreamPayloadFixed, streamPayloadHeap);
      EXPECT_EQ(expectedStreamPayloadHeap, expectedStreamPayloadHeapEnd);
    }

    if (!stream->tryAdvance()) {
      stream->advanceAfterFlush();
    }
  }
  EXPECT_FALSE(stream->hasData());
}

TEST_F(RadixSortSpillSectionsTest, writerIgnoresSmallWriteBufferSize) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 1024, 64);
  for (uint8_t row = 1; row <= 5; ++row) {
    storage.append(fixed8Key(row));
  }

  auto spill = spillSingleRun(
      storage,
      nullptr,
      common::CompressionKind_NONE,
      /*writeBufferSize=*/kBlockHeaderSize + 2 * keyLayout.width());
  const auto blocks = readUncompressedBlocks(spill.file);
  ASSERT_EQ(blocks.size(), 1);
  EXPECT_EQ(blocks[0].header.rowCount, storage.size());
}

TEST_F(RadixSortSpillSectionsTest, writerRollsOverOneRowPastExactBodyCapacity) {
  const auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  const auto bodyCapacity = kFixedWriteBufferSize - kBlockHeaderSize;
  ASSERT_EQ(bodyCapacity % keyLayout.width(), 0);
  const auto rowsAtCapacity = bodyCapacity / keyLayout.width();
  RadixSortRunStorage storage(pool_.get(), keyLayout, 4'096, 64);
  for (uint64_t row = 0; row < rowsAtCapacity; ++row) {
    storage.append(fixed8Key(static_cast<uint8_t>(row)));
  }

  auto exact = spillSingleRun(storage, nullptr);
  const auto exactBlocks = readUncompressedBlocks(exact.file);
  ASSERT_EQ(exactBlocks.size(), 1);
  EXPECT_EQ(exactBlocks[0].header.rowCount, rowsAtCapacity);
  EXPECT_EQ(exactBlocks[0].header.uncompressedSize, bodyCapacity);

  storage.append(fixed8Key(0));
  auto overflow = spillSingleRun(storage, nullptr);
  const auto overflowBlocks = readUncompressedBlocks(overflow.file);
  ASSERT_EQ(overflowBlocks.size(), 2);
  EXPECT_EQ(overflowBlocks[0].header.rowCount, rowsAtCapacity);
  EXPECT_EQ(overflowBlocks[0].header.uncompressedSize, bodyCapacity);
  EXPECT_EQ(overflowBlocks[1].header.rowCount, 1);
  EXPECT_EQ(
      overflowBlocks[1].header.uncompressedSize,
      static_cast<int32_t>(keyLayout.width()));
}

TEST_F(
    RadixSortSpillSectionsTest,
    writerVariableRowsFillMultipleBlocksAtExactBodyCapacity) {
  const auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  const auto meta = RadixSortSpillSectionMeta::create(keyLayout, nullptr);
  const auto bodyCapacity = kFixedWriteBufferSize - kBlockHeaderSize;
  constexpr uint64_t kSmallRowBytes = 96;
  const auto largeRowBytes = bodyCapacity - kSmallRowBytes;
  auto keyForSerializedBytes = [&](uint64_t serializedBytes, char value) {
    BOLT_CHECK_GT(serializedBytes, meta.wireKeyRecordSize);
    return std::string(
        serializedBytes - meta.wireKeyRecordSize + keyLayout.heapKeyOffset(),
        value);
  };
  const std::vector<std::string> keys{
      keyForSerializedBytes(largeRowBytes, 'a'),
      keyForSerializedBytes(kSmallRowBytes, 'b'),
      keyForSerializedBytes(largeRowBytes, 'c'),
      keyForSerializedBytes(kSmallRowBytes, 'd')};
  RadixSortRunStorage storage(pool_.get(), keyLayout, 4, 2 << 20);
  for (const auto& key : keys) {
    storage.append(key);
  }

  auto spill = spillSingleRun(storage, nullptr);
  const auto blocks = readUncompressedBlocks(spill.file);
  ASSERT_EQ(blocks.size(), 2);
  for (const auto& block : blocks) {
    EXPECT_EQ(block.header.rowCount, 2);
    EXPECT_EQ(block.header.uncompressedSize, bodyCapacity);
    EXPECT_EQ(block.header.keyRecordBytes, 2 * meta.wireKeyRecordSize);
    EXPECT_EQ(
        block.header.keyHeapBytes, bodyCapacity - 2 * meta.wireKeyRecordSize);
  }

  auto stream = makeRadixSortSpillMergeStream(
      RadixSortSpillRun{{spill.file}}, spill.meta, pool_.get(), false);
  for (vector_size_t row = 0; row < storage.size(); ++row) {
    ASSERT_TRUE(stream->hasData());
    expectSpillStreamKey(keyLayout, *stream, storage.keyDataAt(row));
    if (!stream->tryAdvance()) {
      stream->advanceAfterFlush();
    }
  }
  EXPECT_FALSE(stream->hasData());
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
  ASSERT_EQ(header.keyRecordBytes, keys.size() * meta.wireKeyRecordSize);
  ASSERT_EQ(header.payloadFixedBytes, keys.size() * meta.payloadFixedSize);

  RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
  const auto view = reader.nextBatch();
  ASSERT_TRUE(view.has_value());

  const auto* const keyRecords = view->keyRecordsBegin;
  const auto* const keyHeap = view->keyHeapBegin;
  const auto* const keyHeapEnd = keyHeap + header.keyHeapBytes;
  EXPECT_EQ(keyHeapEnd, view->payloadFixedBegin);
  const auto* const payloadFixed = view->payloadFixedBegin;
  const auto* const payloadHeap = payloadFixed + header.payloadFixedBytes;
  const auto* const payloadHeapEnd = payloadHeap + header.payloadHeapBytes;

  auto* keyHeapCursor = const_cast<char*>(keyHeap);
  auto* payloadHeapCursor = const_cast<char*>(payloadHeap);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    SCOPED_TRACE(row);
    const auto* const key =
        keyRecords + static_cast<uint64_t>(row) * meta.wireKeyRecordSize;

    const auto size = batchSizeForSingleRow(meta, storage.keyDataAt(row));
    EXPECT_EQ(
        std::string(key, keyLayout.heapKeyOffset()) +
            std::string(keyHeapCursor, size.keyHeapBytes),
        keys[row]);
    keyHeapCursor += size.keyHeapBytes;

    auto* const expectedPayload = const_cast<char*>(payloadFixed) +
        static_cast<uint64_t>(row) * meta.payloadFixedSize;
    expectPayloadVariablePointersInSection(
        meta, expectedPayload, payloadHeapCursor, size.payloadHeapBytes);
    payloadHeapCursor += size.payloadHeapBytes;
  }
  EXPECT_EQ(keyHeapCursor, keyHeapEnd);
  EXPECT_EQ(payloadHeapCursor, payloadHeapEnd);
  ASSERT_FALSE(reader.nextBatch().has_value());
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
  const auto size = batchSizeForSingleRow(meta, key);
  ASSERT_EQ(size.keyHeapBytes, 0);
  auto block = materializeSectionBlock(meta, key, 1);

  auto* keyRecord = block.data();
  auto* keyHeap = keyRecord + meta.wireKeyRecordSize;
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
  const auto payloadHeapBytes = payloadHeapBytesForRow(meta, key);
  EXPECT_EQ(payloadHeapBytes, longText.size() + sourceArrayRef.size);
  EXPECT_EQ(std::memcmp(diskHeap, payloadBatch.heapAt(0), payloadHeapBytes), 0);

  char* payloadHeapCursor = diskHeap;
  const auto restored =
      RadixSortSpillSections::restorePayloadPointersInSectionRows(
          meta, 1, diskFixed, payloadHeapCursor, diskHeap + payloadHeapBytes);
  ASSERT_TRUE(restored);
  EXPECT_EQ(payloadHeapCursor, diskHeap + payloadHeapBytes);
  const auto* restoredHeap = diskHeap;
  const auto restoredString =
      loadUnaligned<StringView>(diskFixed + heapString.offset);
  EXPECT_EQ(restoredString.data(), restoredHeap);
  EXPECT_EQ(
      std::string(restoredString.data(), restoredString.size()), longText);
  const auto restoredArrayRef =
      loadUnaligned<PayloadVarlenRef>(diskFixed + arrayColumn.offset);
  EXPECT_EQ(restoredArrayRef.data, restoredHeap + longText.size());
  EXPECT_EQ(restoredArrayRef.size, sourceArrayRef.size);
  const auto restoredEmptyRowRef =
      loadUnaligned<PayloadVarlenRef>(diskFixed + emptyRowColumn.offset);
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
  const auto serializedSize = batchSizeForSingleRow(meta, storedKey);
  EXPECT_EQ(
      serializedSize.keyHeapBytes, key.size() - keyLayout.heapKeyOffset());
  EXPECT_EQ(
      serializedSize.totalBytes(meta.fixedWireBytesPerRow()),
      meta.wireKeyRecordSize + serializedSize.keyHeapBytes +
          meta.payloadFixedSize);

  auto block = materializeSectionBlock(meta, storedKey, 1);

  auto* keyRecord = block.data();
  auto* diskKeyHeap = keyRecord + meta.wireKeyRecordSize;
  auto* payloadFixed = diskKeyHeap + serializedSize.keyHeapBytes;
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

  char* payloadHeapCursor = payloadHeap;
  const auto restored =
      RadixSortSpillSections::restorePayloadPointersInSectionRows(
          meta, 1, payloadFixed, payloadHeapCursor, payloadHeap);
  ASSERT_TRUE(restored);
  EXPECT_EQ(payloadHeapCursor, payloadHeap);

  EXPECT_EQ(
      std::string(keyRecord, keyLayout.heapKeyOffset()) +
          std::string(diskKeyHeap, serializedSize.keyHeapBytes),
      key);
}

TEST_F(
    RadixSortSpillSectionsTest,
    zeroHeapVariablePayloadCopyRestoreRoundTrip) {
  auto emptyRows = std::make_shared<RowVector>(
      pool_.get(),
      ROW(std::vector<std::string>{}, std::vector<TypePtr>{}),
      nullptr,
      2,
      std::vector<VectorPtr>{});
  emptyRows->setNull(0, true);
  auto payload = makeRows(
      {"inline_string", "empty_row"},
      {makeStringVector({std::nullopt, std::string("short")}), emptyRows});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 64);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, storage, payloadBatch);

  const auto& stringColumn = payloadLayout->columns()[0];
  const auto& complexColumn = payloadLayout->columns()[1];
  ASSERT_FALSE(stringColumn.complex);
  ASSERT_TRUE(complexColumn.complex);
  ASSERT_EQ(payloadLayout->variableColumns().size(), 2);

  std::array<char, 1> pointerPoison{};
  auto* const poisonData = pointerPoison.data();
  auto* const row0 = payloadBatch.rowAt(0);
  auto* const row1 = payloadBatch.rowAt(1);
  storeUnaligned<const char*>(
      row0 + stringColumn.offset + sizeof(uint64_t), poisonData);
  storeUnaligned<PayloadVarlenRef>(
      row0 + complexColumn.offset, PayloadVarlenRef{0, poisonData});
  storeUnaligned<PayloadVarlenRef>(
      row1 + complexColumn.offset, PayloadVarlenRef{0, poisonData});
  EXPECT_EQ(
      stringPointerBytes(row0 + stringColumn.offset),
      static_cast<const char*>(poisonData));
  EXPECT_EQ(
      loadUnaligned<PayloadVarlenRef>(row0 + complexColumn.offset).data,
      poisonData);
  EXPECT_EQ(
      loadUnaligned<PayloadVarlenRef>(row1 + complexColumn.offset).data,
      poisonData);

  for (vector_size_t row = 0; row < payload->size(); ++row) {
    ASSERT_EQ(payloadBatch.heapSizeAt(row), 0);
    storage.append(fixed8Key(row + 1), payloadBatch.rowAt(row));
  }

  const auto meta =
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  auto block =
      materializeSectionBlock(meta, storage.keyDataAt(0), storage.size());
  auto* keyRecords = block.data();
  auto* payloadFixed = keyRecords + storage.size() * meta.wireKeyRecordSize;
  auto* payloadHeap = payloadFixed + storage.size() * meta.payloadFixedSize;
  ASSERT_EQ(payloadHeap, block.data() + block.size());

  uint64_t nullVariableSlots = 0;
  for (vector_size_t row = 0; row < storage.size(); ++row) {
    const auto* const sourceFixed = payloadBatch.rowAt(row);
    const auto* const serializedFixed =
        payloadFixed + static_cast<uint64_t>(row) * meta.payloadFixedSize;
    for (const auto& op : meta.payloadVariableOps) {
      const bool isNull =
          (static_cast<uint8_t>(sourceFixed[op.nullByte]) & op.nullMask) == 0;
      if (!isNull) {
        continue;
      }
      ++nullVariableSlots;
      EXPECT_TRUE(std::all_of(
          serializedFixed + op.offset,
          serializedFixed + op.offset + sizeof(StringView),
          [](char value) { return value == 0; }))
          << "row=" << row << ", offset=" << op.offset;
    }
  }
  EXPECT_EQ(nullVariableSlots, 2);

  const auto serializedEmptyComplex = loadUnaligned<PayloadVarlenRef>(
      payloadFixed + meta.payloadFixedSize + complexColumn.offset);
  EXPECT_EQ(serializedEmptyComplex.size, 0);
  EXPECT_EQ(serializedEmptyComplex.data, nullptr);

  auto* payloadHeapCursor = payloadHeap;
  ASSERT_TRUE(RadixSortSpillSections::restorePayloadPointersInSectionRows(
      meta, storage.size(), payloadFixed, payloadHeapCursor, payloadHeap));
  EXPECT_EQ(payloadHeapCursor, payloadHeap);
  const auto restoredEmptyComplex = loadUnaligned<PayloadVarlenRef>(
      payloadFixed + meta.payloadFixedSize + complexColumn.offset);
  EXPECT_EQ(restoredEmptyComplex.size, 0);
  EXPECT_EQ(restoredEmptyComplex.data, nullptr);
  std::vector<char*> restoredPayloads;
  for (vector_size_t row = 0; row < storage.size(); ++row) {
    restoredPayloads.push_back(
        payloadFixed + static_cast<uint64_t>(row) * meta.payloadFixedSize);
  }
  RowVectorPtr output;
  PayloadRowReader::gather(
      *payloadLayout, restoredPayloads, pool_.get(), output);
  expectEquivalent(*payload, *output);
}

TEST_F(
    RadixSortSpillSectionsTest,
    readerValidatesVariablePayloadSlotsWhenHeaderHeapIsEmpty) {
  auto emptyRow = std::make_shared<RowVector>(
      pool_.get(),
      ROW(std::vector<std::string>{}, std::vector<TypePtr>{}),
      nullptr,
      1,
      std::vector<VectorPtr>{});
  auto payload = makeRows(
      {"null_string", "inline_string", "empty_row"},
      {makeStringVector({std::nullopt}),
       makeStringVector({std::string("short")}),
       emptyRow});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  const auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 64);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter{}.append(*payload, storage, payloadBatch);
  ASSERT_EQ(payloadBatch.heapSizeAt(0), 0);
  storage.append(fixed8Key(1), payloadBatch.rowAt(0));

  const auto readPayloadFixedOffset = [&](const SpilledRun& spill) {
    const auto header =
        readSpillValue<TestRadixSortSpillBlockHeader>(spill.file.path, 0);
    EXPECT_EQ(header.rowCount, 1);
    EXPECT_EQ(header.keyHeapBytes, 0);
    EXPECT_EQ(header.payloadHeapBytes, 0);
    return kBlockHeaderSize + header.keyRecordBytes + header.keyHeapBytes;
  };

  {
    auto spill = spillSingleRun(storage, payloadLayout.get());
    readPayloadFixedOffset(spill);
    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    const auto block = reader.nextBatch();
    ASSERT_TRUE(block.has_value());
    RowVectorPtr output;
    std::array<char*, 1> rows{block->payloadFixedBegin};
    PayloadRowReader::gather(*payloadLayout, rows, pool_.get(), output);
    expectEquivalent(*payload, *output);
    EXPECT_FALSE(reader.nextBatch().has_value());
  }

  {
    auto spill = spillSingleRun(storage, payloadLayout.get());
    const auto payloadFixedOffset = readPayloadFixedOffset(spill);
    const auto& stringColumn = payloadLayout->columns()[1];
    const uint32_t nonInlineSize = StringView::kInlineSize + 1;
    overwriteSpillValue<uint32_t>(
        spill.file.path,
        payloadFixedOffset + stringColumn.offset,
        nonInlineSize);
    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    BOLT_ASSERT_THROW(
        reader.nextBatch(), "Invalid radix sort spill payload heap");
  }

  {
    auto spill = spillSingleRun(storage, payloadLayout.get());
    const auto payloadFixedOffset = readPayloadFixedOffset(spill);
    const auto& complexColumn = payloadLayout->columns()[2];
    overwriteSpillValue<uint64_t>(
        spill.file.path, payloadFixedOffset + complexColumn.offset, 1);
    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    BOLT_ASSERT_THROW(
        reader.nextBatch(), "Invalid radix sort spill payload heap");
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    restoreRejectsTruncatedOrTrailingPayloadHeap) {
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
  const auto size = batchSizeForSingleRow(meta, storedKey);
  ASSERT_GT(size.keyHeapBytes, 0);
  ASSERT_GT(meta.payloadFixedSize, 0);
  ASSERT_GT(size.payloadHeapBytes, 0);
  const auto block = materializeSectionBlock(meta, storedKey, 1);

  auto restoreWithLimit = [&](uint64_t payloadHeapBytes) {
    auto copy = block;
    auto* keyRecord = copy.data();
    auto* keyHeap = keyRecord + meta.wireKeyRecordSize;
    auto* payloadFixed = keyHeap + size.keyHeapBytes;
    auto* payloadHeap = payloadFixed + meta.payloadFixedSize;
    auto* payloadHeapCursor = payloadHeap;
    return RadixSortSpillSections::restorePayloadPointersInSectionRows(
        meta,
        1,
        payloadFixed,
        payloadHeapCursor,
        payloadHeap + payloadHeapBytes);
  };

  EXPECT_FALSE(restoreWithLimit(size.payloadHeapBytes - 1));
  EXPECT_TRUE(restoreWithLimit(size.payloadHeapBytes));

  auto blockWithTrailingByte = block;
  blockWithTrailingByte.push_back('x');
  auto* keyRecord = blockWithTrailingByte.data();
  auto* keyHeap = keyRecord + meta.wireKeyRecordSize;
  auto* payloadFixed = keyHeap + size.keyHeapBytes;
  auto* payloadHeap = payloadFixed + meta.payloadFixedSize;
  auto* payloadHeapCursor = payloadHeap;
  EXPECT_FALSE(RadixSortSpillSections::restorePayloadPointersInSectionRows(
      meta,
      1,
      payloadFixed,
      payloadHeapCursor,
      payloadHeap + size.payloadHeapBytes + 1));
}

TEST_F(RadixSortSpillSectionsTest, readerRejectsTrailingPayloadHeapByte) {
  auto payload =
      makeRows({"payload_string"}, {makeStringVector({std::string(96, 'p')})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  const auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter{}.append(*payload, storage, payloadBatch);
  storage.append(fixed8Key(1), payloadBatch.rowAt(0));

  auto spill = spillSingleRun(storage, payloadLayout.get());
  auto header =
      readSpillValue<TestRadixSortSpillBlockHeader>(spill.file.path, 0);
  ASSERT_GT(header.payloadHeapBytes, 0);
  ++header.uncompressedSize;
  ++header.storedSize;
  ++header.payloadHeapBytes;
  overwriteSpillValue(spill.file.path, 0, header);
  ++spill.file.size;
  std::filesystem::resize_file(spill.file.path, spill.file.size);

  RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
  BOLT_ASSERT_THROW(
      reader.nextBatch(), "Invalid radix sort spill payload heap");
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

TEST_F(
    RadixSortSpillSectionsTest,
    writerRejectsSingleRowFallbackWhenEncodedSizeIsInvalid) {
  auto directory = exec::test::TempDirectoryPath::create();
  folly::Synchronized<common::SpillStats> stats;
  auto config = spillConfig(
      directory->path, common::CompressionKind_NONE, /*writeBufferSize=*/1024);

  {
    auto keyLayout = RadixSortKeyLayout::select(std::nullopt, false, 7);
    RadixSortRunStorage storage(pool_.get(), keyLayout, 1, 64);
    storage.append(std::string(keyLayout.heapKeyOffset() + 1, 'k'));
    auto* key = storage.keyDataAt(0);
    storeUnaligned<uint64_t>(key + *keyLayout.sizeOffset(), 6);

    RadixSortSpillWriter writer(
        directory->path + "/invalid-key", config, pool_.get(), &stats);
    BOLT_ASSERT_THROW(
        writer.writeRun(storage, nullptr),
        "Radix sort spill failed to size a single row");
  }
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

TEST_F(RadixSortSpillSectionsTest, writerReturnedFilesOutliveWriter) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 4, 64);
  storage.append(std::string_view("\x00\x00\x00\x00\x00\x00\x00\x01", 8));

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

  auto files = spillRunFiles(storage, payloadLayout.get(), kMaxFileSize);
  ASSERT_GT(files.size(), 1);
  for (size_t i = 0; i + 1 < files.size(); ++i) {
    EXPECT_GT(files[i].size, kMaxFileSize);
  }
  verifySpillFilesRoundTrip(files, storage, *payloadLayout);
}

TEST_F(
    RadixSortSpillSectionsTest,
    rejectsUnsupportedFormatBeforeInvalidSectionsAndAllocation) {
  for (const auto unsupportedFormat :
       {uint32_t{0}, uint32_t{1}, kCurrentRadixSortSpillFormat + 1}) {
    SCOPED_TRACE(unsupportedFormat);
    auto fixture = writeVariableKeySpill(
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
        {std::string(32, 'k')});
    auto header = readSpillValue<TestRadixSortSpillBlockHeader>(
        fixture.spill.file.path, 0);
    header.reserved = unsupportedFormat;
    header.keyHeapBytes = std::numeric_limits<uint64_t>::max();
    header.payloadHeapBytes = std::numeric_limits<uint64_t>::max();
    overwriteSpillValue(fixture.spill.file.path, 0, header);

    RadixSortSpillReader reader(
        fixture.spill.file, fixture.spill.meta, pool_.get(), false);
    const auto allocations = pool_->stats().numAllocs;
    BOLT_ASSERT_THROW(
        reader.nextBatch(), "Unsupported radix sort spill format");
    EXPECT_EQ(pool_->stats().numAllocs, allocations);
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    rejectsVariableKeyAndPayloadSectionOverflowBeforeAllocation) {
  auto payload = makeRows(
      {"value"},
      {makeStringVector({std::string(StringView::kInlineSize + 1, 'p')})});
  auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
  const auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 64);
  PayloadRowBatch payloadBatch;
  PayloadRowWriter{}.append(*payload, storage, payloadBatch);
  storage.append(
      std::string(keyLayout.inlineCapacity() + 1, 'k'), payloadBatch.rowAt(0));

  for (const auto sectionOffset :
       {offsetof(TestRadixSortSpillBlockHeader, keyHeapBytes),
        offsetof(TestRadixSortSpillBlockHeader, payloadHeapBytes)}) {
    SCOPED_TRACE(sectionOffset);
    auto spill = spillSingleRun(storage, payloadLayout.get());
    overwriteSpillValue<uint64_t>(
        spill.file.path, sectionOffset, std::numeric_limits<uint64_t>::max());
    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    const auto allocations = pool_->stats().numAllocs;
    BOLT_ASSERT_THROW(
        reader.nextBatch(), "Radix sort spill block size overflows");
    EXPECT_EQ(pool_->stats().numAllocs, allocations);
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    rejectsSchemaImpossibleSectionsBeforeBodyAllocation) {
  struct Mutation {
    const char* name;
    uint64_t offset;
  };
  for (const auto mutation : std::array<Mutation, 3>{{
           {"fixed key heap",
            offsetof(TestRadixSortSpillBlockHeader, keyHeapBytes)},
           {"key-only payload fixed",
            offsetof(TestRadixSortSpillBlockHeader, payloadFixedBytes)},
           {"key-only payload heap",
            offsetof(TestRadixSortSpillBlockHeader, payloadHeapBytes)},
       }}) {
    SCOPED_TRACE(mutation.name);
    auto spill = writeInlineKeySpill(RadixSortKeyLayoutKind::kKeyOnlyFixed8, 1);
    overwriteSpillValue<uint64_t>(spill.file.path, mutation.offset, 1);
    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    const auto allocations = pool_->stats().numAllocs;
    EXPECT_THROW(reader.nextBatch(), BoltException);
    EXPECT_EQ(pool_->stats().numAllocs, allocations);
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
    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    const auto allocations = pool_->stats().numAllocs;
    EXPECT_THROW(reader.nextBatch(), BoltException);
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

    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    const auto allocations = pool_->stats().numAllocs;
    EXPECT_THROW(reader.nextBatch(), BoltException);
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

  RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
  const auto allocations = pool_->stats().numAllocs;
  EXPECT_THROW(reader.nextBatch(), BoltException);
  EXPECT_EQ(pool_->stats().numAllocs, allocations);
}

TEST_F(RadixSortSpillSectionsTest, rejectsInvalidScalarHeaderFields) {
  struct Mutation {
    const char* name;
    uint64_t offset;
    int32_t value;
  };
  for (const auto mutation : std::array<Mutation, 5>{{
           {"zero uncompressed size",
            offsetof(TestRadixSortSpillBlockHeader, uncompressedSize),
            0},
           {"zero stored size",
            offsetof(TestRadixSortSpillBlockHeader, storedSize),
            0},
           {"mismatched stored size",
            offsetof(TestRadixSortSpillBlockHeader, storedSize),
            1},
           {"zero row count",
            offsetof(TestRadixSortSpillBlockHeader, rowCount),
            0},
           {"mismatched row count",
            offsetof(TestRadixSortSpillBlockHeader, rowCount),
            2},
       }}) {
    SCOPED_TRACE(mutation.name);
    auto spill = writeInlineKeySpill(RadixSortKeyLayoutKind::kKeyOnlyFixed8, 1);
    overwriteSpillValue<int32_t>(
        spill.file.path, mutation.offset, mutation.value);
    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    EXPECT_THROW(reader.nextBatch(), BoltException);
  }

  auto spill = writeInlineKeySpill(RadixSortKeyLayoutKind::kKeyOnlyFixed8, 1);
  const auto oversized =
      maxUncompressedSpillBlockSize(common::CompressionKind_ZSTD) + 1;
  overwriteSpillValue<int32_t>(
      spill.file.path,
      offsetof(TestRadixSortSpillBlockHeader, uncompressedSize),
      oversized);
  spill.file.compressionKind = common::CompressionKind_ZSTD;
  RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
  EXPECT_THROW(reader.nextBatch(), BoltException);
}

TEST_F(RadixSortSpillSectionsTest, rejectsTruncatedHeaderAndBody) {
  {
    auto spill = writeInlineKeySpill(RadixSortKeyLayoutKind::kKeyOnlyFixed8, 1);
    std::filesystem::resize_file(spill.file.path, kBlockHeaderSize - 1);
    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    EXPECT_THROW(reader.nextBatch(), BoltException);
  }
  {
    auto spill = writeInlineKeySpill(RadixSortKeyLayoutKind::kKeyOnlyFixed8, 1);
    std::filesystem::resize_file(spill.file.path, spill.file.size - 1);
    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    EXPECT_THROW(reader.nextBatch(), BoltException);
  }
}

TEST_F(RadixSortSpillSectionsTest, propagatesCorruptCompressedBody) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage storage(pool_.get(), keyLayout, 4, 64);
  for (uint8_t value = 1; value <= 32; ++value) {
    storage.append(fixed8Key(value));
  }
  auto spill = spillSingleRun(storage, nullptr, common::CompressionKind_ZSTD);
  overwriteSpillBytes(
      spill.file.path, kBlockHeaderSize, std::vector<char>(8, '\0'));
  RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
  EXPECT_THROW(reader.nextBatch(), BoltException);
}

TEST_F(RadixSortSpillSectionsTest, rejectsCorruptSectionMetadata) {
  {
    auto spill =
        writeInlineKeySpill(RadixSortKeyLayoutKind::kKeyOnlyVariable32, 1);
    const auto keySizeOffset = spill.meta.keySizeOffset;
    const uint64_t invalidSize = spill.meta.keyHeapOffset - 1;
    overwriteSpillValue(
        spill.file.path, kBlockHeaderSize + keySizeOffset, invalidSize);
    EXPECT_THROW(
        makeRadixSortSpillMergeStream(
            RadixSortSpillRun{{spill.file}}, spill.meta, pool_.get(), false),
        BoltException);
  }

  auto checkPayloadSize = [&](bool complex) {
    RowVectorPtr payload;
    if (complex) {
      auto array = std::make_shared<ArrayVector>(
          pool_.get(),
          ARRAY(INTEGER()),
          nullptr,
          1,
          makeBuffer<vector_size_t>({0}),
          makeBuffer<vector_size_t>({2}),
          makeVector<int32_t>(INTEGER(), {1, 2}));
      payload = makeRows({"value"}, {array});
    } else {
      payload = makeRows({"value"}, {makeStringVector({std::string(64, 's')})});
    }
    auto payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
    auto keyLayout = RadixSortKeyLayout::fromKind(
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
    RadixSortRunStorage storage(
        pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 1024);
    PayloadRowBatch payloadBatch;
    PayloadRowWriter{}.append(*payload, storage, payloadBatch);
    storage.append(fixed8Key(1), payloadBatch.rowAt(0));
    auto spill = spillSingleRun(storage, payloadLayout.get());
    const auto header =
        readSpillValue<TestRadixSortSpillBlockHeader>(spill.file.path, 0);
    const auto slotOffset = kBlockHeaderSize + header.keyRecordBytes +
        header.keyHeapBytes + payloadLayout->columns()[0].offset;
    if (complex) {
      overwriteSpillValue<uint64_t>(
          spill.file.path, slotOffset, header.payloadHeapBytes + 1);
    } else {
      overwriteSpillValue<uint32_t>(
          spill.file.path,
          slotOffset,
          static_cast<uint32_t>(header.payloadHeapBytes + 1));
    }
    RadixSortSpillReader reader(spill.file, spill.meta, pool_.get(), false);
    EXPECT_THROW(reader.nextBatch(), BoltException);
  };
  checkPayloadSize(false);
  checkPayloadSize(true);

  auto spill =
      writeInlineKeySpill(RadixSortKeyLayoutKind::kKeyOnlyVariable32, 1);
  auto header =
      readSpillValue<TestRadixSortSpillBlockHeader>(spill.file.path, 0);
  ++header.uncompressedSize;
  ++header.storedSize;
  ++header.keyHeapBytes;
  overwriteSpillValue(spill.file.path, 0, header);
  ++spill.file.size;
  std::filesystem::resize_file(spill.file.path, spill.file.size);
  auto stream = makeRadixSortSpillMergeStream(
      RadixSortSpillRun{{spill.file}}, spill.meta, pool_.get(), false);
  ASSERT_TRUE(stream->hasData());
  EXPECT_FALSE(stream->tryAdvance());
  EXPECT_THROW(stream->advanceAfterFlush(), BoltException);
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

TEST_F(RadixSortSpillSectionsTest, oversizedRowGetsDedicatedBlock) {
  auto payloadLayout =
      PayloadRowLayout::create(ROW({"payload_string"}, {VARCHAR()}));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 1024, 64, payloadLayout, 1024, 2 << 20);
  auto payload = makeRows(
      {"payload_string"},
      {makeStringVector(
          {std::string(16, 'a'),
           std::string(kFixedWriteBufferSize, 'b'),
           std::string(16, 'c')})});
  PayloadRowBatch payloadBatch;
  PayloadRowWriter{}.append(*payload, storage, payloadBatch);
  for (vector_size_t row = 0; row < payload->size(); ++row) {
    storage.append(fixed8Key(row + 1), payloadBatch.rowAt(row));
  }

  auto spill = spillSingleRun(
      storage,
      payloadLayout.get(),
      common::CompressionKind_NONE,
      /*writeBufferSize=*/kBlockHeaderSize + 128);
  const auto blocks = readUncompressedBlocks(spill.file);
  ASSERT_EQ(blocks.size(), 3);
  EXPECT_EQ(blocks[0].header.rowCount, 1);
  EXPECT_EQ(blocks[1].header.rowCount, 1);
  EXPECT_EQ(blocks[2].header.rowCount, 1);
  EXPECT_GT(
      blocks[1].header.uncompressedSize,
      kFixedWriteBufferSize - kBlockHeaderSize);
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
    variableLayoutsRoundTripEncodedBoundariesAndConsumeHeapsExactly) {
  const CompareFlags flags{
      .nullsFirst = true,
      .ascending = true,
      .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
  for (const auto kind :
       {RadixSortKeyLayoutKind::kKeyOnlyVariable32,
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32}) {
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    const auto requestedLayout = RadixSortKeyLayout::fromKind(kind);
    const auto capacity = requestedLayout.inlineCapacity();
    const std::vector<std::optional<std::string>> keyValues{
        std::nullopt,
        std::string{},
        std::string(capacity - 3, 'a'),
        std::string(capacity - 2, 'b'),
        std::string(capacity - 1, 'c'),
        std::string(capacity + 32, 'd')};
    auto logicalKeys = makeRows({"key"}, {makeStringVector(keyValues)});
    std::unique_ptr<RadixSortKeyCodec> codec;
    RadixSortKeyCodec::bind({VARCHAR()}, {flags}, codec);
    EncodedKeyBatch encodedKeys;
    codec->encode(*logicalKeys, pool_.get(), encodedKeys);
    ASSERT_EQ(encodedKeys.format(), EncodedKeyFormat::kVariableBinary);
    ASSERT_EQ(encodedKeys.size(), keyValues.size());
    EXPECT_EQ(encodedKeys.variableKeyAt(0).size(), 1);
    EXPECT_EQ(encodedKeys.variableKeyAt(1).size(), 2);
    EXPECT_EQ(encodedKeys.variableKeyAt(2).size(), capacity - 1);
    EXPECT_EQ(encodedKeys.variableKeyAt(3).size(), capacity);
    EXPECT_EQ(encodedKeys.variableKeyAt(4).size(), capacity + 1);
    EXPECT_GT(encodedKeys.variableKeyAt(5).size(), capacity + 1);

    auto keyLayout = RadixSortKeyLayout::select(
        codec->maximumEncodedSize(),
        requestedLayout.hasPayload(),
        codec->heapKeyOffsetForVariableLayout(capacity));
    ASSERT_EQ(keyLayout.kind(), kind);
    ASSERT_EQ(keyLayout.heapKeyOffset(), 0);

    const std::vector<std::optional<std::string>> payloadValues{
        std::nullopt,
        std::string{},
        std::string(StringView::kInlineSize - 1, 'u'),
        std::string(StringView::kInlineSize, 'v'),
        std::string(StringView::kInlineSize + 1, 'w'),
        std::string(StringView::kInlineSize + 32, 'x')};
    RowVectorPtr payload;
    std::shared_ptr<const PayloadRowLayout> payloadLayout;
    PayloadRowBatch payloadBatch;
    std::vector<char*> payloadPointers;
    if (keyLayout.hasPayload()) {
      payload = makeRows({"payload"}, {makeStringVector(payloadValues)});
      payloadLayout = PayloadRowLayout::create(asRowType(payload->type()));
    }
    RadixSortRunStorage storage(
        pool_.get(), keyLayout, 8, 1024, payloadLayout, 8, 1024);
    if (payloadLayout) {
      PayloadRowWriter{}.append(*payload, storage, payloadBatch);
      payloadPointers.reserve(payloadBatch.size());
      for (vector_size_t row = 0; row < payloadBatch.size(); ++row) {
        payloadPointers.push_back(payloadBatch.rowAt(row));
      }
      storage.appendBatch(encodedKeys, payloadPointers);
    } else {
      storage.appendBatch(encodedKeys);
    }

    uint64_t expectedKeyHeapBytes = 0;
    for (vector_size_t row = 0; row < encodedKeys.size(); ++row) {
      expectedKeyHeapBytes +=
          encodedKeys.variableKeyAt(row).size() - keyLayout.heapKeyOffset();
    }
    uint64_t expectedPayloadHeapBytes = 0;
    if (payloadLayout) {
      for (const auto& value : payloadValues) {
        if (value.has_value() && value->size() > StringView::kInlineSize) {
          expectedPayloadHeapBytes += value->size();
        }
      }
    }

    auto directSpill = spillSingleRun(storage, payloadLayout.get());
    const auto blocks = readUncompressedBlocks(directSpill.file);
    ASSERT_EQ(blocks.size(), 1);
    const auto& header = blocks.front().header;
    EXPECT_EQ(header.rowCount, encodedKeys.size());
    EXPECT_EQ(header.keyHeapBytes, expectedKeyHeapBytes);
    EXPECT_EQ(header.payloadHeapBytes, expectedPayloadHeapBytes);
    EXPECT_EQ(
        static_cast<uint64_t>(header.uncompressedSize),
        header.keyRecordBytes + header.keyHeapBytes + header.payloadFixedBytes +
            header.payloadHeapBytes);

    auto directStream = makeRadixSortSpillMergeStream(
        RadixSortSpillRun{{directSpill.file}},
        directSpill.meta,
        pool_.get(),
        false);
    for (vector_size_t row = 0; row < encodedKeys.size(); ++row) {
      ASSERT_TRUE(directStream->hasData());
      const auto* variable =
          dynamic_cast<const RadixSortVariableMergeStream*>(directStream.get());
      ASSERT_NE(variable, nullptr);
      EXPECT_EQ(
          variable->encodedSuffix().bytes,
          encodedKeys.variableKeyAt(row).substr(keyLayout.heapKeyOffset()));
      if (payloadLayout) {
        RowVectorPtr output;
        std::array<char*, 1> rows{directStream->payload()};
        PayloadRowReader::gather(*payloadLayout, rows, pool_.get(), output);
        expectEquivalent(*payload, row, *output);
      }
      if (!directStream->tryAdvance()) {
        directStream->advanceAfterFlush();
      }
    }
    EXPECT_FALSE(directStream->hasData());

    RadixSortRunStorage left(
        pool_.get(), keyLayout, 8, 1024, payloadLayout, 8, 1024);
    RadixSortRunStorage right(
        pool_.get(), keyLayout, 8, 1024, payloadLayout, 8, 1024);
    for (vector_size_t row = 0; row < encodedKeys.size(); ++row) {
      auto& run = row % 2 == 0 ? left : right;
      run.append(
          encodedKeys.variableKeyAt(row),
          payloadLayout ? payloadBatch.rowAt(row) : nullptr);
    }
    auto leftSpill = spillSingleRun(left, payloadLayout.get());
    auto rightSpill = spillSingleRun(right, payloadLayout.get());
    const std::array<std::string, 2> mergePaths{
        leftSpill.file.path, rightSpill.file.path};
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    streams.push_back(makeRadixSortSpillMergeStream(
        RadixSortSpillRun{{leftSpill.file}},
        leftSpill.meta,
        pool_.get(),
        false));
    streams.push_back(makeRadixSortSpillMergeStream(
        RadixSortSpillRun{{rightSpill.file}},
        rightSpill.meta,
        pool_.get(),
        false));
    RadixSortMerger merger(keyLayout, std::move(streams));
    std::vector<const char*> records(encodedKeys.size());
    std::vector<char*> payloadRows(encodedKeys.size());
    std::vector<EncodedKeyView> views(encodedKeys.size());
    BufferPtr decodeScratch;
    vector_size_t outputOffset = 0;
    EXPECT_EQ(
        merger.collectRows(
            encodedKeys.size(),
            records.data(),
            keyLayout.hasPayload() ? payloadRows.data() : nullptr,
            views,
            [&](vector_size_t size) {
              RowVectorPtr decoded;
              codec->decode(
                  std::span<const EncodedKeyView>(views.data(), size),
                  {},
                  {},
                  pool_.get(),
                  decodeScratch,
                  decoded);
              expectEquivalent(*logicalKeys, outputOffset, *decoded);
              if (payloadLayout) {
                RowVectorPtr output;
                PayloadRowReader::gather(
                    *payloadLayout,
                    std::span<char* const>(payloadRows.data(), size),
                    pool_.get(),
                    output);
                expectEquivalent(*payload, outputOffset, *output);
              }
              outputOffset += size;
            }),
        encodedKeys.size());
    EXPECT_EQ(outputOffset, encodedKeys.size());
    for (const auto& path : mergePaths) {
      EXPECT_FALSE(std::filesystem::exists(path));
    }
  }
}

TEST_F(RadixSortSpillSectionsTest, readerReusesBuffersAcrossBlocks) {
  auto payloadLayout = PayloadRowLayout::create(ROW({"payload"}, {VARCHAR()}));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 4, 64, payloadLayout, 4, 2 << 20);
  const std::vector<std::optional<std::string>> values{
      std::string(700 << 10, 'a'),
      std::string(400 << 10, 'b'),
      std::string(800 << 10, 'c'),
      std::string((1 << 20) + 257, 'd')};
  auto payload = makeRows({"payload"}, {makeStringVector(values)});
  PayloadRowBatch payloadBatch;
  PayloadRowWriter{}.append(*payload, storage, payloadBatch);
  for (vector_size_t row = 0; row < payload->size(); ++row) {
    storage.append(fixed8Key(row + 1), payloadBatch.rowAt(row));
  }
  auto files = spillRunFiles(
      storage,
      payloadLayout.get(),
      std::numeric_limits<uint64_t>::max(),
      1 << 20);
  ASSERT_EQ(files.size(), 1);
  ASSERT_EQ(readUncompressedBlocks(files.front()).size(), values.size());

  RadixSortSpillReadBufferCache bufferCache;
  const auto meta =
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
  RadixSortSpillReader reader(
      files.front(), meta, pool_.get(), false, &bufferCache);
  auto block = reader.nextBatch();
  ASSERT_TRUE(block.has_value());
  EXPECT_EQ(
      readStringPayloads(
          *payloadLayout, std::vector<char*>{block->payloadFixedBegin}),
      std::vector<std::string>{*values[0]});

  const auto statsBeforeReuse = pool_->stats();
  block = reader.nextBatch();
  ASSERT_TRUE(block.has_value());
  const auto statsAfterReuse = pool_->stats();
  EXPECT_EQ(statsAfterReuse.numAllocs, statsBeforeReuse.numAllocs);
  EXPECT_EQ(statsAfterReuse.numFrees, statsBeforeReuse.numFrees);
  EXPECT_EQ(
      readStringPayloads(
          *payloadLayout, std::vector<char*>{block->payloadFixedBegin}),
      std::vector<std::string>{*values[1]});

  const auto statsBeforeGrowth = pool_->stats();
  block = reader.nextBatch();
  ASSERT_TRUE(block.has_value());
  const auto statsAfterGrowth = pool_->stats();
  EXPECT_EQ(statsAfterGrowth.numAllocs, statsBeforeGrowth.numAllocs + 1);
  EXPECT_EQ(statsAfterGrowth.numFrees, statsBeforeGrowth.numFrees + 1);
  EXPECT_EQ(
      readStringPayloads(
          *payloadLayout, std::vector<char*>{block->payloadFixedBegin}),
      std::vector<std::string>{*values[2]});

  const auto statsBeforeOversized = pool_->stats();
  block = reader.nextBatch();
  ASSERT_TRUE(block.has_value());
  const auto statsAfterOversized = pool_->stats();
  EXPECT_EQ(statsAfterOversized.numAllocs, statsBeforeOversized.numAllocs + 1);
  EXPECT_EQ(statsAfterOversized.numFrees, statsBeforeOversized.numFrees + 1);
  EXPECT_EQ(
      readStringPayloads(
          *payloadLayout, std::vector<char*>{block->payloadFixedBegin}),
      std::vector<std::string>{*values[3]});
  EXPECT_FALSE(reader.nextBatch().has_value());
  EXPECT_EQ(bufferCache.serializedBuffer, nullptr);
}

TEST_F(RadixSortSpillSectionsTest, readerCacheReusesAcrossFiles) {
  auto payloadLayout = PayloadRowLayout::create(ROW({"payload"}, {VARCHAR()}));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortSpillReadBufferCache bufferCache;
  const auto runReader = [&](const std::string& value,
                             uint8_t key,
                             Buffer* expectedReusableBuffer = nullptr) {
    RadixSortRunStorage storage(
        pool_.get(), keyLayout, 1, 64, payloadLayout, 1, value.size());
    auto payload = makeRows({"payload"}, {makeStringVector({value})});
    PayloadRowBatch payloadBatch;
    PayloadRowWriter{}.append(*payload, storage, payloadBatch);
    storage.append(fixed8Key(key), payloadBatch.rowAt(0));
    auto files = spillRunFiles(
        storage,
        payloadLayout.get(),
        std::numeric_limits<uint64_t>::max(),
        512 << 10);
    ASSERT_EQ(files.size(), 1);
    if (expectedReusableBuffer != nullptr) {
      ASSERT_EQ(bufferCache.serializedBuffer.get(), expectedReusableBuffer);
    }
    const auto meta =
        RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
    RadixSortSpillReader reader(
        files.front(), meta, pool_.get(), false, &bufferCache);
    const auto block = reader.nextBatch();
    ASSERT_TRUE(block.has_value());
    EXPECT_EQ(bufferCache.serializedBuffer, nullptr);
    EXPECT_EQ(
        readStringPayloads(
            *payloadLayout, std::vector<char*>{block->payloadFixedBegin}),
        std::vector<std::string>{value});
    EXPECT_FALSE(reader.nextBatch().has_value());
    if (expectedReusableBuffer != nullptr) {
      ASSERT_NE(bufferCache.serializedBuffer, nullptr);
      EXPECT_EQ(bufferCache.serializedBuffer.get(), expectedReusableBuffer);
    }
  };

  runReader(std::string(64 << 10, 'a'), 1);
  ASSERT_NE(bufferCache.serializedBuffer, nullptr);
  auto* const firstBuffer = bufferCache.serializedBuffer.get();
  const auto firstCapacity = bufferCache.serializedBuffer->capacity();
  runReader(std::string(32 << 10, 'b'), 2, firstBuffer);
  ASSERT_NE(bufferCache.serializedBuffer, nullptr);
  EXPECT_EQ(bufferCache.serializedBuffer.get(), firstBuffer);
  EXPECT_EQ(bufferCache.serializedBuffer->capacity(), firstCapacity);

  runReader(std::string(192 << 10, 'c'), 3);
  ASSERT_NE(bufferCache.serializedBuffer, nullptr);
  EXPECT_GT(bufferCache.serializedBuffer->capacity(), firstCapacity);

  const auto statsBeforeOversized = pool_->stats();
  runReader(std::string((1 << 20) + 257, 'd'), 4);
  EXPECT_GT(pool_->stats().numFrees, statsBeforeOversized.numFrees);
  EXPECT_EQ(bufferCache.serializedBuffer, nullptr);
}

TEST_F(RadixSortSpillSectionsTest, writerWritesOnlyRequestedStorageSuffix) {
  auto payloadLayout = PayloadRowLayout::create(ROW({"payload"}, {VARCHAR()}));
  auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  RadixSortRunStorage storage(
      pool_.get(), keyLayout, 2, 64, payloadLayout, 3, 1024);
  auto payload = makeRows(
      {"payload"},
      {makeStringVector(
          {std::string(80, 'a'),
           std::string(81, 'b'),
           std::string(82, 'c'),
           std::string(83, 'd'),
           std::string(84, 'e')})});
  PayloadRowBatch payloadBatch;
  PayloadRowWriter{}.append(*payload, storage, payloadBatch);
  for (uint8_t row = 0; row < 5; ++row) {
    storage.append(
        std::string(48 + row, static_cast<char>('k' + row)),
        payloadBatch.rowAt(row));
  }

  auto directory = exec::test::TempDirectoryPath::create();
  folly::Synchronized<common::SpillStats> stats;
  RadixSortSpillWriter writer(
      directory->path + "/spill",
      spillConfig(
          directory->path, common::CompressionKind_NONE, kFixedWriteBufferSize),
      pool_.get(),
      &stats);
  auto files = writer.writeRun(storage, payloadLayout.get(), 1);
  ASSERT_EQ(files.size(), 1);
  EXPECT_EQ(files.front().rowCount, 4);

  auto stream = makeRadixSortSpillMergeStream(
      RadixSortSpillRun{files},
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get()),
      pool_.get(),
      false);
  std::vector<std::string> restoredPayloads;
  for (vector_size_t row = 0; row < 4; ++row) {
    ASSERT_TRUE(stream->hasData());
    expectSpillStreamKey(keyLayout, *stream, storage.keyDataAt(row + 1));
    auto restored = readStringPayloads(*payloadLayout, {stream->payload()});
    ASSERT_EQ(restored.size(), 1);
    restoredPayloads.push_back(std::move(restored.front()));
    if (!stream->tryAdvance()) {
      stream->advanceAfterFlush();
    }
  }
  EXPECT_FALSE(stream->hasData());
  EXPECT_EQ(
      restoredPayloads,
      (std::vector<std::string>{
          std::string(81, 'b'),
          std::string(82, 'c'),
          std::string(83, 'd'),
          std::string(84, 'e')}));

  const auto statsBeforeEmptySuffix = stats.copy();
  RadixSortSpillWriter emptyWriter(
      directory->path + "/empty",
      spillConfig(
          directory->path, common::CompressionKind_NONE, kFixedWriteBufferSize),
      pool_.get(),
      &stats);
  EXPECT_TRUE(emptyWriter.writeRun(storage, payloadLayout.get(), storage.size())
                  .empty());
  EXPECT_EQ(stats.copy(), statsBeforeEmptySuffix);

  RadixSortSpillWriter invalidWriter(
      directory->path + "/invalid",
      spillConfig(
          directory->path, common::CompressionKind_NONE, kFixedWriteBufferSize),
      pool_.get(),
      &stats);
  EXPECT_THROW(
      invalidWriter.writeRun(storage, payloadLayout.get(), storage.size() + 1),
      BoltException);
}

TEST_F(RadixSortSpillSectionsTest, mergerFlushesBoundarySegmentsForAllShapes) {
  const auto runCase = [&](const RadixSortKeyLayout& layout,
                           const std::vector<std::vector<uint8_t>>& streamKeys,
                           const std::vector<std::vector<size_t>>& boundaries,
                           const std::vector<vector_size_t>& expectedSegments) {
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    std::vector<BoundaryTestMergeStream*> streamPointers;
    for (size_t i = 0; i < streamKeys.size(); ++i) {
      auto stream = std::make_unique<BoundaryTestMergeStream>(
          layout, streamKeys[i], boundaries[i], i * 100);
      streamPointers.push_back(stream.get());
      streams.push_back(std::move(stream));
    }
    RadixSortMerger merger(layout, std::move(streams));
    size_t rowCount = 0;
    for (const auto& keys : streamKeys) {
      rowCount += keys.size();
    }
    std::vector<const char*> keys(rowCount);
    std::vector<char*> payloads(rowCount);
    std::vector<vector_size_t> segments;
    std::vector<uint8_t> outputKeys;
    std::vector<int64_t> outputPayloads;
    const auto count = merger.collectRows(
        rowCount,
        keys.data(),
        payloads.data(),
        std::span<EncodedKeyView>{},
        [&](vector_size_t size) {
          segments.push_back(size);
          for (vector_size_t row = 0; row < size; ++row) {
            outputKeys.push_back(
                static_cast<uint8_t>(keys[row][sizeof(uint64_t) - 1]));
            if (layout.hasPayload()) {
              outputPayloads.push_back(
                  *reinterpret_cast<int64_t*>(payloads[row]));
            }
          }
        });
    EXPECT_EQ(count, rowCount);
    EXPECT_EQ(segments, expectedSegments);
    std::vector<uint8_t> expectedKeys;
    for (const auto& inputKeys : streamKeys) {
      expectedKeys.insert(
          expectedKeys.end(), inputKeys.begin(), inputKeys.end());
    }
    std::sort(expectedKeys.begin(), expectedKeys.end());
    EXPECT_EQ(outputKeys, expectedKeys);
    for (size_t i = 0; i < streamPointers.size(); ++i) {
      EXPECT_EQ(streamPointers[i]->safeAdvances(), boundaries[i].size());
      EXPECT_EQ(
          streamPointers[i]->ordinaryAdvances(),
          streamKeys[i].size() - boundaries[i].size());
    }
    if (layout.hasPayload()) {
      ASSERT_EQ(outputPayloads.size(), outputKeys.size());
      for (size_t row = 0; row < outputKeys.size(); ++row) {
        EXPECT_EQ(outputPayloads[row] % 100, outputKeys[row]);
      }
    }
  };

  runCase(
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8),
      {{1, 2, 3, 4}},
      {{0, 2}},
      {1, 2, 1});
  runCase(
      RadixSortKeyLayout::fromKind(
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed16),
      {{1, 2, 3, 4}},
      {{0, 2}},
      {1, 2, 1});
  runCase(
      RadixSortKeyLayout::fromKind(
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed16),
      {{1, 3, 5}, {2, 4, 6}},
      {{0, 2}, {1}},
      {1, 3, 1, 1});
  runCase(
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8),
      {{1, 3, 5}, {2, 4, 6}},
      {{0, 2}, {1}},
      {1, 3, 1, 1});
  runCase(
      RadixSortKeyLayout::fromKind(
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed16),
      {{1, 4}, {2, 5}, {3, 6}},
      {{1}, {0}, {1}},
      {2, 2, 2});
  runCase(
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8),
      {{1, 4}, {2, 5}, {3, 6}},
      {{1}, {0}, {1}},
      {2, 2, 2});
  runCase(
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8),
      {{1, 2, 3}},
      {{2}},
      {3});
}

TEST_F(RadixSortSpillSectionsTest, mergerRejectsStreamIndexSentinelCollision) {
  const auto layout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  constexpr size_t kMaxStreams = std::numeric_limits<uint16_t>::max();
  {
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams(kMaxStreams);
    EXPECT_NO_THROW(RadixSortMerger(layout, std::move(streams)));
  }
  {
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams(kMaxStreams + 1);
    BOLT_ASSERT_THROW(
        RadixSortMerger(layout, std::move(streams)),
        "Radix sort merger supports at most 65535 streams");
  }
}

TEST_F(RadixSortSpillSectionsTest, mergerRejectsInvalidMemoryOperations) {
  const auto layout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  const auto makeMergerWithoutMemory = [&]() {
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    streams.push_back(std::make_unique<BoundaryTestMergeStream>(
        layout, std::vector<uint8_t>{1}, std::vector<size_t>{}));
    return std::make_unique<RadixSortMerger>(layout, std::move(streams));
  };
  {
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    streams.push_back(std::make_unique<BoundaryTestMergeStream>(
        layout, std::vector<uint8_t>{1}, std::vector<size_t>{}));
    EXPECT_THROW(RadixSortMerger(layout, std::move(streams), 1), BoltException);
  }
  {
    auto merger = makeMergerWithoutMemory();
    EXPECT_THROW(merger->removeMemory(), BoltException);
  }
  {
    auto merger = makeMergerWithoutMemory();
    EXPECT_THROW(
        merger->replaceMemory(
            {},
            RadixSortSpillSectionMeta::create(layout, nullptr),
            pool_.get(),
            false),
        BoltException);
  }
  {
    RadixSortRunStorage storage(pool_.get(), layout, 1, 64);
    storage.append(fixed8Key(1));
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    streams.push_back(std::make_unique<RadixSortMemoryRunMergeStream>(storage));
    RadixSortMerger merger(layout, std::move(streams), 0);
    EXPECT_THROW(merger.removeMemory(), BoltException);
  }
  {
    RadixSortRunStorage storage(pool_.get(), layout, 1, 64);
    storage.append(fixed8Key(1));
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    streams.push_back(std::make_unique<RadixSortMemoryRunMergeStream>(storage));
    RadixSortMerger merger(layout, std::move(streams), 0);
    EXPECT_THROW(
        merger.replaceMemory(
            {},
            RadixSortSpillSectionMeta::create(layout, nullptr),
            pool_.get(),
            false),
        BoltException);
  }
}

TEST_F(RadixSortSpillSectionsTest, mergerReplacesMemoryAndResetsSelection) {
  const auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage memoryStorage(pool_.get(), keyLayout, 1, 64);
  for (const auto key : {uint64_t{2}, uint64_t{5}, uint64_t{8}}) {
    memoryStorage.append(fixed8Key(key));
  }

  RadixSortRunStorage leftStorage(pool_.get(), keyLayout, 1, 64);
  RadixSortRunStorage rightStorage(pool_.get(), keyLayout, 1, 64);
  for (const auto key : {uint64_t{1}, uint64_t{4}, uint64_t{7}}) {
    leftStorage.append(fixed8Key(key));
  }
  for (const auto key : {uint64_t{3}, uint64_t{6}, uint64_t{9}}) {
    rightStorage.append(fixed8Key(key));
  }

  std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
  auto left = std::make_unique<TrackingStorageMergeStream>(leftStorage);
  auto* leftPtr = left.get();
  streams.push_back(std::move(left));
  streams.push_back(
      std::make_unique<RadixSortMemoryRunMergeStream>(memoryStorage));
  auto right = std::make_unique<TrackingStorageMergeStream>(rightStorage);
  auto* rightPtr = right.get();
  streams.push_back(std::move(right));
  RadixSortMerger merger(keyLayout, std::move(streams), 1);

  std::array<const char*, 9> keyPointers{};
  vector_size_t outputRows = 0;
  auto collect = [&](vector_size_t count) {
    return merger.collectRows(
        count,
        keyPointers.data(),
        nullptr,
        std::span<EncodedKeyView>{},
        [&](vector_size_t size) {
          for (vector_size_t row = 0; row < size; ++row) {
            ASSERT_LT(outputRows, 9);
            EXPECT_EQ(
                loadUnaligned<uint64_t>(keyPointers[row]), outputRows + 1);
            ++outputRows;
          }
        });
  };

  EXPECT_EQ(collect(4), 4);
  EXPECT_EQ(outputRows, 4);
  ASSERT_TRUE(merger.memoryPosition());
  EXPECT_EQ(*merger.memoryPosition(), 1);
  const auto leftPosition = leftPtr->position();
  const auto rightPosition = rightPtr->position();

  RadixSortRunStorage suffixStorage(pool_.get(), keyLayout, 1, 64);
  for (const auto key : {uint64_t{5}, uint64_t{8}}) {
    suffixStorage.append(fixed8Key(key));
  }
  auto suffixFiles = spillRunFiles(suffixStorage, nullptr, /*maxFileSize=*/1);
  ASSERT_EQ(suffixFiles.size(), 1);
  merger.replaceMemory(
      RadixSortSpillRun{std::move(suffixFiles)},
      RadixSortSpillSectionMeta::create(keyLayout, nullptr),
      pool_.get(),
      false);

  EXPECT_FALSE(merger.memoryPosition());
  EXPECT_EQ(merger.testingNumStreams(), 3);
  EXPECT_EQ(leftPtr->position(), leftPosition);
  EXPECT_EQ(rightPtr->position(), rightPosition);
  EXPECT_EQ(collect(5), 5);
  EXPECT_EQ(outputRows, 9);
}

TEST_F(RadixSortSpillSectionsTest, mergerRemovesExhaustedMemoryStream) {
  const auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage leftStorage(pool_.get(), keyLayout, 1, 64);
  RadixSortRunStorage rightStorage(pool_.get(), keyLayout, 1, 64);
  RadixSortRunStorage memoryStorage(pool_.get(), keyLayout, 1, 64);
  for (const auto key : {uint8_t{3}, uint8_t{5}}) {
    leftStorage.append(fixed8Key(key));
  }
  for (const auto key : {uint8_t{4}, uint8_t{6}}) {
    rightStorage.append(fixed8Key(key));
  }
  for (const auto key : {uint8_t{1}, uint8_t{2}}) {
    memoryStorage.append(fixed8Key(key));
  }
  auto leftFiles = spillRunFiles(leftStorage, nullptr, 1 << 20);
  auto rightFiles = spillRunFiles(rightStorage, nullptr, 1 << 20);
  ASSERT_EQ(leftFiles.size(), 1);
  ASSERT_EQ(rightFiles.size(), 1);
  std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
  streams.push_back(makeRadixSortSpillMergeStream(
      RadixSortSpillRun{leftFiles},
      RadixSortSpillSectionMeta::create(keyLayout, nullptr),
      pool_.get(),
      false));
  streams.push_back(makeRadixSortMemoryRunMergeStream(memoryStorage));
  streams.push_back(makeRadixSortSpillMergeStream(
      RadixSortSpillRun{rightFiles},
      RadixSortSpillSectionMeta::create(keyLayout, nullptr),
      pool_.get(),
      false));
  RadixSortMerger merger(keyLayout, std::move(streams), 1);

  std::array<const char*, 6> keyPointers{};
  vector_size_t outputRows = 0;
  const auto collect = [&](vector_size_t count) {
    return merger.collectRows(
        count,
        keyPointers.data(),
        nullptr,
        std::span<EncodedKeyView>{},
        [&](vector_size_t size) {
          for (vector_size_t row = 0; row < size; ++row) {
            ASSERT_LT(outputRows, 6);
            EXPECT_EQ(
                loadUnaligned<uint64_t>(keyPointers[row]), outputRows + 1);
            ++outputRows;
          }
        });
  };

  EXPECT_EQ(collect(2), 2);
  ASSERT_TRUE(merger.memoryPosition());
  EXPECT_EQ(*merger.memoryPosition(), memoryStorage.size());

  merger.removeMemory();
  EXPECT_FALSE(merger.memoryPosition());
  EXPECT_EQ(merger.testingNumStreams(), 2);
  EXPECT_EQ(collect(4), 4);
  EXPECT_EQ(outputRows, 6);
}

TEST_F(
    RadixSortSpillSectionsTest,
    mergerCallbackFailurePropagatesForAllShapes) {
  const auto layout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  const auto runCase = [&](size_t numStreams) {
    SCOPED_TRACE(fmt::format("{} stream(s)", numStreams));
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    auto boundaryStream = std::make_unique<BoundaryTestMergeStream>(
        layout, std::vector<uint8_t>{1, 2}, std::vector<size_t>{0, 1});
    auto* const boundaryStreamPtr = boundaryStream.get();
    streams.push_back(std::move(boundaryStream));
    for (size_t stream = 1; stream < numStreams; ++stream) {
      streams.push_back(std::make_unique<BoundaryTestMergeStream>(
          layout,
          std::vector<uint8_t>{static_cast<uint8_t>(10 + stream)},
          std::vector<size_t>{}));
    }

    RadixSortMerger merger(layout, std::move(streams));
    std::array<const char*, 2> keys{};
    size_t callbacks = 0;
    EXPECT_THROW(
        merger.collectRows(
            2,
            keys.data(),
            nullptr,
            std::span<EncodedKeyView>{},
            [&](vector_size_t size) {
              EXPECT_EQ(size, 1);
              if (++callbacks == 2) {
                BOLT_FAIL("injected flush failure");
              }
            }),
        BoltException);
    EXPECT_EQ(callbacks, 2);
    EXPECT_EQ(boundaryStreamPtr->safeAdvances(), 1);
    EXPECT_EQ(boundaryStreamPtr->ordinaryAdvances(), 0);
    EXPECT_EQ(boundaryStreamPtr->index(), 1);
  };

  // Exercise all three collectRows dispatches: single-stream, two-way and
  // loser-tree merge.
  for (const auto numStreams : {1, 2, 3}) {
    runCase(numStreams);
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    variableCaptureViewsCallbackFailureCleansBorrowedSpillStorage) {
  for (const size_t numStreams : {1, 2, 3}) {
    SCOPED_TRACE(fmt::format("{} stream(s)", numStreams));
    const std::vector<std::string> expected{
        std::string(32, 'a'), std::string(64, 'b')};
    std::vector<VariableSpillFixture> fixtures;
    fixtures.reserve(numStreams);
    fixtures.push_back(writeVariableKeySpill(
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32, expected));
    for (size_t stream = 1; stream < numStreams; ++stream) {
      fixtures.push_back(writeVariableKeySpill(
          RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
          {std::string(32 + stream, 'z')}));
    }

    std::vector<std::string> spillPaths;
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    for (const auto& fixture : fixtures) {
      spillPaths.push_back(fixture.spill.file.path);
      ASSERT_TRUE(std::filesystem::exists(spillPaths.back()));
      streams.push_back(makeRadixSortSpillMergeStream(
          RadixSortSpillRun{{fixture.spill.file}},
          fixture.spill.meta,
          pool_.get(),
          false));
    }

    {
      RadixSortMerger merger(
          fixtures.front().spill.meta.keyLayout, std::move(streams));
      std::array<const char*, 2> records{};
      std::array<char*, 2> payloads{};
      std::array<EncodedKeyView, 2> views{};
      size_t callbacks = 0;
      BOLT_ASSERT_THROW(
          merger.collectRows(
              records.size(),
              records.data(),
              payloads.data(),
              views,
              [&](vector_size_t size) {
                ++callbacks;
                ASSERT_EQ(size, expected.size());
                for (vector_size_t row = 0; row < size; ++row) {
                  EXPECT_EQ(views[row].bytes, expected[row]);
                  EXPECT_EQ(
                      loadUnaligned<int64_t>(
                          payloads[row] +
                          fixtures.front().payloadLayout->columns()[0].offset),
                      row + 1);
                }
                for (const auto& path : spillPaths) {
                  EXPECT_TRUE(std::filesystem::exists(path));
                }
                BOLT_FAIL("injected capture-view callback failure");
              }),
          "injected capture-view callback failure");
      EXPECT_EQ(callbacks, 1);
      for (const auto& path : spillPaths) {
        EXPECT_TRUE(std::filesystem::exists(path));
      }
    }
    for (const auto& path : spillPaths) {
      EXPECT_FALSE(std::filesystem::exists(path));
    }
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    mergerTailCallbackPreservesExceptionTypeForAllShapes) {
  const auto layout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  for (const size_t numStreams : {1, 2, 3}) {
    SCOPED_TRACE(fmt::format("{} stream(s)", numStreams));
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    auto selectedStream = std::make_unique<BoundaryTestMergeStream>(
        layout, std::vector<uint8_t>{1, 2}, std::vector<size_t>{});
    auto* const selectedStreamPtr = selectedStream.get();
    streams.push_back(std::move(selectedStream));
    for (size_t stream = 0; stream < numStreams; ++stream) {
      if (stream != 0) {
        streams.push_back(std::make_unique<BoundaryTestMergeStream>(
            layout,
            std::vector<uint8_t>{static_cast<uint8_t>(10 + stream)},
            std::vector<size_t>{}));
      }
    }

    RadixSortMerger merger(layout, std::move(streams));
    std::array<const char*, 1> records{};
    size_t callbacks = 0;
    try {
      merger.collectRows(
          1,
          records.data(),
          nullptr,
          std::span<EncodedKeyView>{},
          [&](vector_size_t size) {
            ++callbacks;
            EXPECT_EQ(size, 1);
            throw std::runtime_error("injected tail callback failure");
          });
      FAIL() << "Expected std::runtime_error";
    } catch (const std::runtime_error& error) {
      EXPECT_STREQ(error.what(), "injected tail callback failure");
    } catch (...) {
      FAIL() << "Expected std::runtime_error";
    }
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(selectedStreamPtr->ordinaryAdvances(), 1);
    EXPECT_EQ(selectedStreamPtr->safeAdvances(), 0);
    EXPECT_EQ(selectedStreamPtr->index(), 1);
  }
}

TEST_F(RadixSortSpillSectionsTest, mergerRejectsTooSmallSelectedViews) {
  const auto layout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  RadixSortRunStorage storage(pool_.get(), layout, 2, 64);
  storage.append(std::string(32, 'a'));
  storage.append(std::string(32, 'b'));
  std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
  streams.push_back(makeRadixSortMemoryRunMergeStream(storage));
  RadixSortMerger merger(layout, std::move(streams));

  std::array<const char*, 2> keys{};
  std::array<EncodedKeyView, 1> selectedViews{};
  bool flushed = false;
  BOLT_ASSERT_THROW(
      merger.collectRows(
          keys.size(),
          keys.data(),
          nullptr,
          selectedViews,
          [&](vector_size_t) { flushed = true; }),
      "Radix sort selected view span is too small");
  EXPECT_FALSE(flushed);
}

TEST_F(RadixSortSpillSectionsTest, fixedMergerRejectsExternalSelectedViews) {
  const auto layout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage storage(pool_.get(), layout, 1, 64);
  storage.append(fixed8Key(1));
  std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
  streams.push_back(makeRadixSortMemoryRunMergeStream(storage));
  RadixSortMerger merger(layout, std::move(streams));

  std::array<const char*, 1> keys{};
  std::array<EncodedKeyView, 1> selectedViews{};
  bool flushed = false;
  BOLT_ASSERT_THROW(
      merger.collectRows(
          keys.size(),
          keys.data(),
          nullptr,
          selectedViews,
          [&](vector_size_t) { flushed = true; }),
      "Fixed radix sort merge cannot capture external key views");
  EXPECT_FALSE(flushed);
}

TEST_F(RadixSortSpillSectionsTest, emptyMergerRejectsNonEmptyCollection) {
  const auto layout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortMerger merger(layout, {}, std::nullopt);
  std::array<const char*, 1> keys{};
  bool flushed = false;
  EXPECT_EQ(
      merger.collectRows(
          0,
          keys.data(),
          nullptr,
          std::span<EncodedKeyView>{},
          [&](vector_size_t) { flushed = true; }),
      0);
  EXPECT_FALSE(flushed);
  EXPECT_THROW(
      merger.collectRows(
          1,
          keys.data(),
          nullptr,
          std::span<EncodedKeyView>{},
          [&](vector_size_t) { flushed = true; }),
      BoltException);
  EXPECT_FALSE(flushed);
}

TEST_F(
    RadixSortSpillSectionsTest,
    mergerOrdersUnevenStreamsWithUniquePayloads) {
  auto layout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  verifyMerge(layout, {{1, 4, 9}});
  verifyMerge(layout, {{1, 4, 8}, {2, 3, 10, 11}});
  verifyMerge(layout, {{1, 7}, {}, {2, 5, 8}, {3, 4, 6, 9, 12}});
}

TEST_F(
    RadixSortSpillSectionsTest,
    variableMemoryStreamsMergeAndCaptureExternalViews) {
  for (const auto kind :
       {RadixSortKeyLayoutKind::kKeyOnlyVariable32,
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32}) {
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    const auto layout = RadixSortKeyLayout::fromKind(kind);
    RadixSortRunStorage left(pool_.get(), layout, 4, 1024);
    RadixSortRunStorage right(pool_.get(), layout, 4, 1024);
    std::array<int64_t, 4> payloadValues{1, 2, 3, 4};
    const std::string prefix(layout.inlineCapacity() + 16, 'p');
    const std::array<std::string, 4> expected{
        prefix + "1", prefix + "2", prefix + "3", prefix + "4"};
    left.append(
        expected[0],
        layout.hasPayload() ? reinterpret_cast<char*>(&payloadValues[0])
                            : nullptr);
    left.append(
        expected[2],
        layout.hasPayload() ? reinterpret_cast<char*>(&payloadValues[2])
                            : nullptr);
    right.append(
        expected[1],
        layout.hasPayload() ? reinterpret_cast<char*>(&payloadValues[1])
                            : nullptr);
    right.append(
        expected[3],
        layout.hasPayload() ? reinterpret_cast<char*>(&payloadValues[3])
                            : nullptr);

    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    streams.push_back(makeRadixSortMemoryRunMergeStream(left));
    streams.push_back(makeRadixSortMemoryRunMergeStream(right));
    ASSERT_NE(
        dynamic_cast<RadixSortVariableMergeStream*>(streams[0].get()), nullptr);
    ASSERT_NE(
        dynamic_cast<RadixSortVariableMergeStream*>(streams[1].get()), nullptr);
    RadixSortMerger merger(layout, std::move(streams));

    std::array<const char*, 4> records{};
    std::array<char*, 4> payloads{};
    std::array<EncodedKeyView, 4> views{};
    vector_size_t output = 0;
    EXPECT_EQ(
        merger.collectRows(
            records.size(),
            records.data(),
            layout.hasPayload() ? payloads.data() : nullptr,
            views,
            [&](vector_size_t size) {
              for (vector_size_t row = 0; row < size; ++row) {
                ASSERT_LT(output, expected.size());
                EXPECT_EQ(
                    std::string(records[row], layout.heapKeyOffset()) +
                        std::string(views[row].bytes),
                    expected[output]);
                if (layout.hasPayload()) {
                  EXPECT_EQ(
                      *reinterpret_cast<int64_t*>(payloads[row]), output + 1);
                }
                ++output;
              }
            }),
        expected.size());
    EXPECT_EQ(output, expected.size());
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    variableComparatorMaterializesSuffixOnlyAfterInlinePrefixTie) {
  const auto layout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  const auto runCase =
      [&](std::string leftKey, std::string rightKey, bool expectRightSuffix) {
        RadixSortRunStorage left(pool_.get(), layout, 1, 1024);
        RadixSortRunStorage right(pool_.get(), layout, 1, 1024);
        left.append(leftKey);
        right.append(rightKey);

        std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
        auto leftStream =
            std::make_unique<InspectableVariableMemoryMergeStream>(left);
        auto rightStream =
            std::make_unique<InspectableVariableMemoryMergeStream>(right);
        auto* const leftStreamPtr = leftStream.get();
        auto* const rightStreamPtr = rightStream.get();
        streams.push_back(std::move(leftStream));
        streams.push_back(std::move(rightStream));
        RadixSortMerger merger(layout, std::move(streams));

        std::array<const char*, 1> records{};
        vector_size_t flushedRows = 0;
        EXPECT_EQ(
            merger.collectRows(
                1,
                records.data(),
                nullptr,
                std::span<EncodedKeyView>{},
                [&](vector_size_t size) { flushedRows += size; }),
            1);
        EXPECT_EQ(flushedRows, 1);
        EXPECT_FALSE(leftStreamPtr->suffixMaterialized());
        EXPECT_EQ(rightStreamPtr->suffixMaterialized(), expectRightSuffix);
      };

  runCase(std::string(32, 'a'), std::string(32, 'b'), false);
  const std::string commonPrefix(layout.inlineCapacity(), 'p');
  runCase(commonPrefix + 'a', commonPrefix + 'b', true);
}

TEST_F(
    RadixSortSpillSectionsTest,
    variableMergerRequiresVariableStreamDynamicType) {
  const auto layout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  RadixSortRunStorage storage(pool_.get(), layout, 4, 1024);
  storage.append(std::string(32, 'a'));

  {
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    streams.push_back(std::make_unique<TrackingStorageMergeStream>(storage));
    BOLT_ASSERT_THROW(
        RadixSortMerger(layout, std::move(streams)),
        "Variable radix sort merger requires variable merge streams");
  }
  {
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    streams.push_back(makeRadixSortMemoryRunMergeStream(storage));
    EXPECT_NO_THROW(RadixSortMerger(layout, std::move(streams), 0));
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    variableSpillStreamRejectsOneByteKeyHeapTruncationAndTrailingByte) {
  enum class Mutation { kTruncated, kTrailing };
  const std::vector<std::string> keys{
      "00000000|" + std::string(55, 'a'),
      "00000001|" + std::string(55, 'b'),
      "00000002|" + std::string(55, 'c'),
      "00000003|" + std::string(55, 'd')};
  for (const auto kind :
       {RadixSortKeyLayoutKind::kKeyOnlyVariable32,
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32}) {
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    for (const auto& [name, mutation] : std::array{
             std::pair{"truncated", Mutation::kTruncated},
             std::pair{"trailing", Mutation::kTrailing}}) {
      SCOPED_TRACE(name);
      auto fixture = writeVariableKeySpill(kind, keys);
      const auto spillPath = fixture.spill.file.path;
      auto header = readSpillValue<TestRadixSortSpillBlockHeader>(spillPath, 0);
      ASSERT_GT(header.keyHeapBytes, 1);
      auto bytes = readSpillBytes(spillPath);
      ASSERT_EQ(bytes.size(), fixture.spill.file.size);
      const auto keyHeapEnd =
          kBlockHeaderSize + header.keyRecordBytes + header.keyHeapBytes;
      ASSERT_LE(keyHeapEnd, bytes.size());
      if (mutation == Mutation::kTruncated) {
        bytes.erase(bytes.begin() + keyHeapEnd - 1);
        --header.uncompressedSize;
        --header.storedSize;
        --header.keyHeapBytes;
        --fixture.spill.file.size;
      } else {
        bytes.insert(bytes.begin() + keyHeapEnd, 'x');
        ++header.uncompressedSize;
        ++header.storedSize;
        ++header.keyHeapBytes;
        ++fixture.spill.file.size;
      }
      std::memcpy(bytes.data(), &header, sizeof(header));
      std::filesystem::resize_file(spillPath, fixture.spill.file.size);
      overwriteSpillBytes(spillPath, 0, bytes);

      {
        auto stream = makeRadixSortSpillMergeStream(
            RadixSortSpillRun{{fixture.spill.file}},
            fixture.spill.meta,
            pool_.get(),
            false);
        ASSERT_TRUE(stream->hasData());
        auto expectKey = [&](size_t row) {
          auto* const variableStream =
              dynamic_cast<RadixSortVariableMergeStream*>(stream.get());
          ASSERT_NE(variableStream, nullptr);
          EXPECT_EQ(
              variableStream->encodedSuffix().bytes,
              std::string_view(keys[row]).substr(
                  fixture.spill.meta.keyHeapOffset));
          if (fixture.spill.meta.hasPayload) {
            ASSERT_NE(stream->payload(), nullptr);
            EXPECT_EQ(
                loadUnaligned<int64_t>(
                    stream->payload() +
                    fixture.payloadLayout->columns()[0].offset),
                row + 1);
          }
        };
        if (mutation == Mutation::kTruncated) {
          for (size_t row = 0; row + 2 < keys.size(); ++row) {
            expectKey(row);
            ASSERT_TRUE(stream->tryAdvance()) << "row=" << row;
          }
          expectKey(keys.size() - 2);
          BOLT_ASSERT_THROW(
              stream->tryAdvance(),
              "Invalid radix sort spill variable key heap size");
        } else {
          for (size_t row = 0; row < keys.size(); ++row) {
            expectKey(row);
            if (row + 1 < keys.size()) {
              ASSERT_TRUE(stream->tryAdvance()) << "row=" << row;
            }
          }
          EXPECT_FALSE(stream->tryAdvance());
          BOLT_ASSERT_THROW(
              stream->advanceAfterFlush(),
              "Radix sort spill key heap is not consumed exactly");
        }
        EXPECT_TRUE(std::filesystem::exists(spillPath));
      }
      EXPECT_FALSE(std::filesystem::exists(spillPath));
    }
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    variableSpillValidatesCurrentLookaheadAndUnpreparedTail) {
  const std::vector<std::string> keys{
      std::string(32, 'a'),
      std::string(32, 'b'),
      std::string(32, 'c'),
      std::string(32, 'd')};
  for (const auto kind :
       {RadixSortKeyLayoutKind::kKeyOnlyVariable32,
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32}) {
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    {
      auto fixture = writeVariableKeySpill(kind, keys);
      overwriteSpillValue<uint64_t>(
          fixture.spill.file.path,
          variableKeySizeDiskOffset(0, 0, fixture.spill.meta),
          fixture.spill.meta.keyHeapOffset);
      EXPECT_THROW(
          makeRadixSortSpillMergeStream(
              RadixSortSpillRun{{fixture.spill.file}},
              fixture.spill.meta,
              pool_.get(),
              false),
          BoltException);
    }
    {
      auto fixture = writeVariableKeySpill(kind, keys);
      overwriteSpillValue<uint64_t>(
          fixture.spill.file.path,
          variableKeySizeDiskOffset(0, 1, fixture.spill.meta),
          std::numeric_limits<uint64_t>::max());
      std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
      streams.push_back(makeRadixSortSpillMergeStream(
          RadixSortSpillRun{{fixture.spill.file}},
          fixture.spill.meta,
          pool_.get(),
          false));
      RadixSortMerger merger(fixture.spill.meta.keyLayout, std::move(streams));
      std::array<const char*, 1> records{};
      std::array<char*, 1> payloads{};
      bool flushed = false;
      EXPECT_THROW(
          merger.collectRows(
              1,
              records.data(),
              fixture.spill.meta.hasPayload ? payloads.data() : nullptr,
              std::span<EncodedKeyView>{},
              [&](vector_size_t) { flushed = true; }),
          BoltException);
      EXPECT_FALSE(flushed);
    }
    {
      auto fixture = writeVariableKeySpill(kind, keys);
      overwriteSpillValue<uint64_t>(
          fixture.spill.file.path,
          variableKeySizeDiskOffset(0, 2, fixture.spill.meta),
          fixture.spill.meta.keyHeapOffset);
      std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
      streams.push_back(makeRadixSortSpillMergeStream(
          RadixSortSpillRun{{fixture.spill.file}},
          fixture.spill.meta,
          pool_.get(),
          false));
      RadixSortMerger merger(fixture.spill.meta.keyLayout, std::move(streams));
      std::array<const char*, 1> records{};
      std::array<char*, 1> payloads{};
      vector_size_t flushedRows = 0;
      EXPECT_EQ(
          merger.collectRows(
              1,
              records.data(),
              fixture.spill.meta.hasPayload ? payloads.data() : nullptr,
              std::span<EncodedKeyView>{},
              [&](vector_size_t size) { flushedRows += size; }),
          1);
      EXPECT_EQ(flushedRows, 1);
    }
    {
      auto fixture = writeVariableKeySpill(kind, keys);
      overwriteSpillValue<uint64_t>(
          fixture.spill.file.path,
          variableKeySizeDiskOffset(0, 2, fixture.spill.meta),
          fixture.spill.meta.keyHeapOffset);
      std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
      streams.push_back(makeRadixSortSpillMergeStream(
          RadixSortSpillRun{{fixture.spill.file}},
          fixture.spill.meta,
          pool_.get(),
          false));
      RadixSortMerger merger(fixture.spill.meta.keyLayout, std::move(streams));
      std::array<const char*, 4> records{};
      std::array<char*, 4> payloads{};
      EXPECT_THROW(
          merger.collectRows(
              records.size(),
              records.data(),
              fixture.spill.meta.hasPayload ? payloads.data() : nullptr,
              std::span<EncodedKeyView>{},
              [&](vector_size_t) {}),
          BoltException);
    }
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    variableSpillValidatesNextBlockAfterSafeFlush) {
  for (const auto kind :
       {RadixSortKeyLayoutKind::kKeyOnlyVariable32,
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32}) {
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    const auto layout = RadixSortKeyLayout::fromKind(kind);
    const auto payloadLayout = layout.hasPayload()
        ? PayloadRowLayout::create(ROW({"payload"}, {BIGINT()}))
        : nullptr;
    const auto meta =
        RadixSortSpillSectionMeta::create(layout, payloadLayout.get());
    const auto bodyCapacity = kFixedWriteBufferSize - kBlockHeaderSize;
    const auto heapBytes = bodyCapacity / 2;
    const auto encodedSize = layout.heapKeyOffset() + heapBytes;
    ASSERT_GT(2 * (meta.fixedWireBytesPerRow() + heapBytes), bodyCapacity);
    const std::string firstKey(encodedSize, 'a');
    const std::string secondKey(encodedSize, 'b');
    auto fixture = writeVariableKeySpill(kind, {firstKey, secondKey});
    const auto blocks = readUncompressedBlocks(fixture.spill.file);
    ASSERT_EQ(blocks.size(), 2);
    ASSERT_EQ(blocks[0].header.rowCount, 1);
    ASSERT_EQ(blocks[1].header.rowCount, 1);
    const auto secondBlockOffset =
        kBlockHeaderSize + static_cast<uint64_t>(blocks[0].header.storedSize);
    overwriteSpillValue<uint64_t>(
        fixture.spill.file.path,
        variableKeySizeDiskOffset(secondBlockOffset, 0, fixture.spill.meta),
        fixture.spill.meta.keyHeapOffset);

    auto stream = makeRadixSortSpillMergeStream(
        RadixSortSpillRun{{fixture.spill.file}},
        fixture.spill.meta,
        pool_.get(),
        false);
    auto* const streamPtr = stream.get();
    auto* const variableStream =
        dynamic_cast<RadixSortVariableMergeStream*>(streamPtr);
    ASSERT_NE(variableStream, nullptr);
    ASSERT_TRUE(streamPtr->hasData());
    EXPECT_EQ(
        variableStream->encodedSuffix().bytes,
        std::string_view(firstKey).substr(layout.heapKeyOffset()));
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
    streams.push_back(std::move(stream));
    RadixSortMerger merger(fixture.spill.meta.keyLayout, std::move(streams));
    std::array<const char*, 1> records{};
    std::array<char*, 1> payloads{};
    bool flushed = false;
    EXPECT_THROW(
        merger.collectRows(
            1,
            records.data(),
            fixture.spill.meta.hasPayload ? payloads.data() : nullptr,
            std::span<EncodedKeyView>{},
            [&](vector_size_t size) {
              EXPECT_EQ(size, 1);
              flushed = true;
            }),
        BoltException);
    EXPECT_TRUE(flushed);
    EXPECT_FALSE(streamPtr->hasData());
    EXPECT_EQ(streamPtr->key(), nullptr);
    EXPECT_EQ(streamPtr->payload(), nullptr);
    EXPECT_TRUE(variableStream->encodedSuffix().bytes.empty());
    EXPECT_EQ(variableStream->encodedSuffix().bytes.data(), nullptr);
  }
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
        auto stream = makeRadixSortSpillMergeStream(
            RadixSortSpillRun{{spill.file}}, spill.meta, pool_.get(), false);
        ASSERT_TRUE(stream->hasData());
        EXPECT_TRUE(std::filesystem::exists(spillPath));
      }
    } else {
      EXPECT_THROW(
          makeRadixSortSpillMergeStream(
              RadixSortSpillRun{{spill.file}}, spill.meta, pool_.get(), false),
          BoltException);
    }
    EXPECT_FALSE(std::filesystem::exists(spillPath));
    if (failure == Failure::kMissing) {
      std::filesystem::rename(missingPath, spillPath);
    }
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    fileMergeStreamRemovesFileWhenLaterBlockIsCorrupt) {
  const std::vector<std::string> values{
      "FIRST|" + std::string((1 << 20) + 101, 'a') + "|END-FIRST",
      "SECOND|" + std::string((1 << 20) + 211, 'b') + "|END-SECOND"};
  std::shared_ptr<const PayloadRowLayout> payloadLayout;
  auto spill = writeLargePayloadSpill(values, payloadLayout);
  const auto spillPath = spill.file.path;
  const auto firstHeader =
      readSpillValue<TestRadixSortSpillBlockHeader>(spillPath, 0);
  const auto secondHeaderOffset =
      kBlockHeaderSize + static_cast<uint64_t>(firstHeader.storedSize);
  overwriteSpillValue<uint32_t>(
      spillPath,
      secondHeaderOffset + offsetof(TestRadixSortSpillBlockHeader, rowCount),
      0);

  auto stream = makeRadixSortSpillMergeStream(
      RadixSortSpillRun{{spill.file}}, spill.meta, pool_.get(), false);
  ASSERT_TRUE(stream->hasData());
  auto* const variableStream =
      dynamic_cast<RadixSortVariableMergeStream*>(stream.get());
  ASSERT_NE(variableStream, nullptr);
  EXPECT_EQ(
      variableStream->encodedSuffix().bytes,
      std::string_view("key_0").substr(spill.meta.keyHeapOffset));
  EXPECT_TRUE(std::filesystem::exists(spillPath));
  EXPECT_FALSE(stream->tryAdvance());
  EXPECT_THROW(stream->advanceAfterFlush(), BoltException);
  EXPECT_FALSE(stream->hasData());
  EXPECT_EQ(stream->key(), nullptr);
  EXPECT_EQ(stream->payload(), nullptr);
  EXPECT_TRUE(variableStream->encodedSuffix().bytes.empty());
  EXPECT_EQ(variableStream->encodedSuffix().bytes.data(), nullptr);
  EXPECT_FALSE(std::filesystem::exists(spillPath));
}

TEST_F(
    RadixSortSpillSectionsTest,
    concatFileMergeStreamCleansFilesWhenLaterFileIsCorrupt) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  const auto rowCount = fixed8RowsPerBlock() + 17;
  auto files = spillLargeFixedKeyRunFiles(rowCount);
  ASSERT_GE(files.size(), 2);
  const auto firstFileRows = files.front().rowCount;
  std::vector<std::string> paths;
  for (const auto& file : files) {
    paths.push_back(file.path);
  }
  overwriteSpillValue<uint32_t>(
      files[1].path, offsetof(TestRadixSortSpillBlockHeader, rowCount), 0);

  {
    auto stream = makeRadixSortSpillMergeStream(
        RadixSortSpillRun{std::move(files)},
        RadixSortSpillSectionMeta::create(keyLayout, nullptr),
        pool_.get(),
        false);
    ASSERT_TRUE(stream->hasData());
    for (uint64_t row = 1; row < firstFileRows; ++row) {
      EXPECT_TRUE(stream->tryAdvance()) << "row=" << row;
    }
    EXPECT_FALSE(stream->tryAdvance());
    EXPECT_THROW(stream->advanceAfterFlush(), BoltException);
    EXPECT_FALSE(stream->hasData());
    EXPECT_EQ(stream->key(), nullptr);
    EXPECT_EQ(stream->payload(), nullptr);
    for (const auto& path : paths) {
      EXPECT_FALSE(std::filesystem::exists(path));
    }
  }
  for (const auto& path : paths) {
    EXPECT_FALSE(std::filesystem::exists(path));
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    concatFileVariableKeyAndPayloadStreamPreservesRowsBoundariesAndRetiresFiles) {
  constexpr vector_size_t kRows = 72;
  constexpr uint64_t kVariableBytesPerRow = 20 << 10;
  const auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  const auto payloadType = ROW({"payload"}, {VARCHAR()});
  auto payloadLayout = PayloadRowLayout::create(payloadType);
  RadixSortRunStorage storage(
      pool_.get(),
      keyLayout,
      1'024,
      kVariableBytesPerRow,
      payloadLayout,
      1'024,
      kVariableBytesPerRow);

  std::vector<std::string> keys;
  std::vector<std::optional<std::string>> payloadValues;
  keys.reserve(kRows);
  payloadValues.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    keys.push_back(
        fmt::format("{:08d}|", row) +
        std::string(kVariableBytesPerRow - 9, 'a' + (row % 23)));
    const auto payloadPrefix = fmt::format("payload-{:08d}|", row);
    payloadValues.push_back(
        payloadPrefix +
        std::string(
            kVariableBytesPerRow - payloadPrefix.size(), 'A' + (row % 23)));
  }
  auto payload = makeRows({"payload"}, {makeStringVector(payloadValues)});
  PayloadRowBatch payloadBatch;
  PayloadRowWriter{}.append(*payload, storage, payloadBatch);
  for (vector_size_t row = 0; row < kRows; ++row) {
    storage.append(keys[row], payloadBatch.rowAt(row));
  }

  auto files =
      spillRunFiles(storage, payloadLayout.get(), /*maxFileSize=*/1 << 20);
  ASSERT_GT(files.size(), 1);
  std::vector<std::string> paths;
  std::vector<uint64_t> blockEndRows;
  std::vector<uint64_t> fileEndRows;
  uint64_t rowsSeen = 0;
  bool sawMultipleBlocksInFile = false;
  for (const auto& file : files) {
    paths.push_back(file.path);
    const auto blocks = readUncompressedBlocks(file);
    ASSERT_FALSE(blocks.empty());
    sawMultipleBlocksInFile |= blocks.size() > 1;
    for (const auto& block : blocks) {
      rowsSeen += block.header.rowCount;
      blockEndRows.push_back(rowsSeen);
    }
    fileEndRows.push_back(rowsSeen);
  }
  ASSERT_TRUE(sawMultipleBlocksInFile);
  ASSERT_EQ(rowsSeen, kRows);

  auto stream = makeRadixSortSpillMergeStream(
      RadixSortSpillRun{std::move(files)},
      RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get()),
      pool_.get(),
      false);
  size_t nextBlock = 0;
  size_t nextFile = 0;
  for (vector_size_t row = 0; row < kRows; ++row) {
    ASSERT_TRUE(stream->hasData()) << "row=" << row;
    expectSpillStreamKey(keyLayout, *stream, storage.keyDataAt(row));
    EXPECT_EQ(
        readStringPayloads(*payloadLayout, {stream->payload()}),
        std::vector<std::string>{*payloadValues[row]});

    const auto atBlockEnd = row + 1 == blockEndRows[nextBlock];
    const auto advancedInBlock = stream->tryAdvance();
    EXPECT_EQ(advancedInBlock, !atBlockEnd) << "row=" << row;
    if (!atBlockEnd) {
      continue;
    }

    const auto atFileEnd =
        nextFile < fileEndRows.size() && row + 1 == fileEndRows[nextFile];
    if (!atFileEnd) {
      EXPECT_TRUE(std::filesystem::exists(paths[nextFile]));
    }
    stream->advanceAfterFlush();
    ++nextBlock;
    if (atFileEnd) {
      EXPECT_FALSE(std::filesystem::exists(paths[nextFile]));
      ++nextFile;
    } else {
      EXPECT_TRUE(std::filesystem::exists(paths[nextFile]));
    }
  }
  EXPECT_EQ(nextBlock, blockEndRows.size());
  EXPECT_EQ(nextFile, fileEndRows.size());
  EXPECT_FALSE(stream->hasData());
  for (const auto& path : paths) {
    EXPECT_FALSE(std::filesystem::exists(path));
  }
}

TEST_F(
    RadixSortSpillSectionsTest,
    variablePayloadStreamWorksAfterPayloadLayoutRelease) {
  constexpr vector_size_t kRows = 3;
  const auto keyLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  const auto payloadType = ROW({"payload"}, {VARCHAR()});
  std::unique_ptr<RadixSortMergeStream> stream;
  std::optional<RadixSortSpillSectionMeta> streamMeta;
  std::weak_ptr<const PayloadRowLayout> originalLayout;
  {
    auto payloadLayout = PayloadRowLayout::create(payloadType);
    originalLayout = payloadLayout;
    constexpr uint64_t kPayloadBytesPerRow = 16 << 10;
    RadixSortRunStorage storage(
        pool_.get(),
        keyLayout,
        1'024,
        64,
        payloadLayout,
        1'024,
        kPayloadBytesPerRow);
    std::vector<std::optional<std::string>> values;
    values.reserve(kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      values.push_back(
          fmt::format("{:08d}|", row) +
          std::string(kPayloadBytesPerRow - 9, 'a' + (row % 23)));
    }
    auto payload = makeRows({"payload"}, {makeStringVector(values)});
    PayloadRowBatch payloadBatch;
    PayloadRowWriter{}.append(*payload, storage, payloadBatch);
    for (vector_size_t row = 0; row < kRows; ++row) {
      storage.append(fixed8Key(row), payloadBatch.rowAt(row));
    }
    auto files = spillRunFiles(
        storage, payloadLayout.get(), std::numeric_limits<uint64_t>::max());
    ASSERT_EQ(files.size(), 1);
    streamMeta =
        RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get());
    stream = makeRadixSortSpillMergeStream(
        RadixSortSpillRun{std::move(files)}, *streamMeta, pool_.get(), false);
  }
  ASSERT_TRUE(originalLayout.expired());

  const auto gatherLayout = PayloadRowLayout::create(payloadType);
  for (vector_size_t row = 0; row < kRows; ++row) {
    ASSERT_TRUE(stream->hasData()) << "row=" << row;
    const auto restored =
        readStringPayloads(*gatherLayout, {stream->payload()});
    ASSERT_EQ(restored.size(), 1);
    EXPECT_EQ(
        restored.front(),
        fmt::format("{:08d}|", row) +
            std::string(16 * 1024 - 9, 'a' + (row % 23)));
    if (!stream->tryAdvance()) {
      stream->advanceAfterFlush();
    }
  }
  EXPECT_FALSE(stream->hasData());
}

TEST_F(
    RadixSortSpillSectionsTest,
    concatFileMergeStreamSafeAdvanceRetiresExhaustedFiles) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  const auto kRows = fixed8RowsPerBlock() + 17;
  auto files = spillLargeFixedKeyRunFiles(kRows);
  std::vector<std::string> paths;
  std::vector<uint64_t> fileRowCounts;
  for (const auto& file : files) {
    paths.push_back(file.path);
    fileRowCounts.push_back(file.rowCount);
  }
  RadixSortSpillReadBufferCache bufferCache;
  auto stream = makeRadixSortSpillMergeStream(
      RadixSortSpillRun{std::move(files)},
      RadixSortSpillSectionMeta::create(keyLayout, nullptr),
      pool_.get(),
      false,
      &bufferCache);

  uint64_t previousReadTime = 0;
  uint64_t row = 1;
  for (size_t fileIndex = 0; fileIndex < fileRowCounts.size(); ++fileIndex) {
    for (uint64_t index = 0; index < fileRowCounts[fileIndex]; ++index) {
      ASSERT_TRUE(stream->hasData());
      const auto expectedKey = orderedFixed8Key(row);
      expectSpillStreamKey(keyLayout, *stream, expectedKey.data());
      const auto advancedInFile = stream->tryAdvance();
      EXPECT_EQ(advancedInFile, index + 1 < fileRowCounts[fileIndex]);
      if (!advancedInFile) {
        stream->advanceAfterFlush();
        EXPECT_GE(stream->getSpillReadTime(), previousReadTime);
        previousReadTime = stream->getSpillReadTime();
        EXPECT_FALSE(std::filesystem::exists(paths[fileIndex]));
      }
      ++row;
    }
  }
  EXPECT_EQ(row, kRows + 1);
  EXPECT_FALSE(stream->hasData());
  EXPECT_NE(bufferCache.serializedBuffer, nullptr);
}

TEST_F(RadixSortSpillSectionsTest, concatFileMergeStreamsMergeByLogicalRun) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  const auto kRowsPerRun = fixed8RowsPerBlock() + 17;
  RadixSortRunStorage oddStorage(pool_.get(), keyLayout, 1'024, 64);
  RadixSortRunStorage evenStorage(pool_.get(), keyLayout, 1'024, 64);
  for (uint64_t row = 0; row < kRowsPerRun; ++row) {
    oddStorage.append(orderedFixed8EncodedKey(2 * row + 1));
    evenStorage.append(orderedFixed8EncodedKey(2 * row + 2));
  }

  auto oddFiles = spillRunFiles(oddStorage, nullptr, /*maxFileSize=*/1);
  auto evenFiles = spillRunFiles(evenStorage, nullptr, /*maxFileSize=*/1);
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
  for (uint64_t value = 1; value <= 2 * kRowsPerRun; ++value) {
    expectedStorage.append(orderedFixed8EncodedKey(value));
  }

  std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
  streams.push_back(makeRadixSortSpillMergeStream(
      RadixSortSpillRun{std::move(oddFiles)},
      RadixSortSpillSectionMeta::create(keyLayout, nullptr),
      pool_.get(),
      false));
  streams.push_back(makeRadixSortSpillMergeStream(
      RadixSortSpillRun{std::move(evenFiles)},
      RadixSortSpillSectionMeta::create(keyLayout, nullptr),
      pool_.get(),
      false));
  RadixSortMerger merger(keyLayout, std::move(streams));

  std::array<const char*, 256> keys;
  size_t outputOffset = 0;
  while (outputOffset < expectedStorage.size()) {
    const auto count = merger.collectRows(
        std::min<vector_size_t>(
            keys.size(), expectedStorage.size() - outputOffset),
        keys.data(),
        nullptr,
        std::span<EncodedKeyView>{},
        [&](vector_size_t size) {
          for (vector_size_t row = 0; row < size; ++row) {
            EXPECT_EQ(
                RadixSortKey(keyLayout, keys[row])
                    .compare(RadixSortKey(
                        keyLayout, expectedStorage.keyDataAt(outputOffset))),
                0)
                << "row=" << outputOffset;
            ++outputOffset;
          }
        });
    ASSERT_GT(count, 0);
  }
  EXPECT_EQ(outputOffset, expectedStorage.size());
  for (const auto& path : filePaths) {
    EXPECT_FALSE(std::filesystem::exists(path));
  }
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
