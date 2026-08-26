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

#include "bolt/exec/radixsort/RadixSortSpill.h"

#include <lz4.h>
#include <zstd.h>
#include <cstring>
#include <utility>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/time/Timer.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

struct RadixSortSpillBlockHeader {
  int32_t uncompressedSize;
  int32_t storedSize;
  uint32_t rowCount;
  uint32_t reserved;
  uint64_t keyHeapBytes;
  uint64_t payloadHeapBytes;
};

constexpr uint64_t kBlockHeaderSize = sizeof(RadixSortSpillBlockHeader);
constexpr uint64_t kDefaultWriteBufferSize = 1 << 20;
constexpr uint64_t kMaxReadBufferSize = (1 << 20) - AlignedBuffer::kPaddedSize;
static_assert(sizeof(RadixSortSpillBlockHeader) == 32);

template <RadixSortKeyLayoutKind KIND>
int32_t comparePhysicalKeys(
    const char* left,
    const char* right,
    uint32_t heapKeyOffset) {
  return RadixSortKeyOps<KIND>::compare(left, right, heapKeyOffset);
}

RadixSortMerger::CompareKeys compareKeysForLayout(RadixSortKeyLayoutKind kind) {
  switch (kind) {
    case RadixSortKeyLayoutKind::kKeyOnlyFixed8:
      return comparePhysicalKeys<RadixSortKeyLayoutKind::kKeyOnlyFixed8>;
    case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
      return comparePhysicalKeys<RadixSortKeyLayoutKind::kKeyOnlyFixed16>;
    case RadixSortKeyLayoutKind::kKeyOnlyFixed24:
      return comparePhysicalKeys<RadixSortKeyLayoutKind::kKeyOnlyFixed24>;
    case RadixSortKeyLayoutKind::kKeyOnlyFixed32:
      return comparePhysicalKeys<RadixSortKeyLayoutKind::kKeyOnlyFixed32>;
    case RadixSortKeyLayoutKind::kKeyOnlyVariable32:
      return comparePhysicalKeys<RadixSortKeyLayoutKind::kKeyOnlyVariable32>;
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
      return comparePhysicalKeys<
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>;
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
      return comparePhysicalKeys<
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>;
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
      return comparePhysicalKeys<
          RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>;
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32:
      return comparePhysicalKeys<
          RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>;
    case RadixSortKeyLayoutKind::kInvalid:
      break;
  }
  BOLT_FAIL("Invalid radix sort merge key layout");
}

bool compressionEnabled(common::CompressionKind kind) {
  return kind == common::CompressionKind_LZ4 ||
      kind == common::CompressionKind_ZSTD;
}

int32_t maxUncompressedBlockSize(common::CompressionKind kind) {
  constexpr auto kMaxBlockSize = std::numeric_limits<int32_t>::max();
  if (kind == common::CompressionKind_LZ4) {
    return LZ4_MAX_INPUT_SIZE;
  }
  if (kind == common::CompressionKind_ZSTD) {
    // For inputs above 128 KiB, ZSTD_compressBound(size) is size + size / 256.
    // Keep both block sizes representable by ByteInputStream's int32_t API.
    return kMaxBlockSize - (kMaxBlockSize >> 8) - 1;
  }
  return kMaxBlockSize;
}

uint32_t checkedFixedRowSize(uint64_t keySize, uint64_t payloadFixedSize) {
  auto rowSize = checkedAdd<uint64_t>(keySize, payloadFixedSize);
  BOLT_CHECK(rowSize.has_value(), "Radix sort spill row size overflows");
  BOLT_CHECK_LE(
      *rowSize,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
      "Radix sort spill row size exceeds int32 range");
  return static_cast<uint32_t>(*rowSize);
}

uint64_t checkedSectionSize(uint64_t rows, uint64_t width) {
  const auto bytes = checkedMultiply<uint64_t>(rows, width);
  BOLT_CHECK(bytes.has_value(), "Radix sort spill block section overflows");
  return *bytes;
}

uint64_t checkedBlockBodySize(
    uint64_t keyRecordBytes,
    uint64_t keyHeapBytes,
    uint64_t payloadFixedBytes,
    uint64_t payloadHeapBytes) {
  auto total = checkedAdd<uint64_t>(keyRecordBytes, keyHeapBytes);
  BOLT_CHECK(total.has_value(), "Radix sort spill block size overflows");
  total = checkedAdd<uint64_t>(*total, payloadFixedBytes);
  BOLT_CHECK(total.has_value(), "Radix sort spill block size overflows");
  total = checkedAdd<uint64_t>(*total, payloadHeapBytes);
  BOLT_CHECK(total.has_value(), "Radix sort spill block size overflows");
  return *total;
}

int32_t compressionBound(common::CompressionKind kind, int32_t size) {
  BOLT_CHECK_GT(size, 0, "Invalid radix sort spill block size");
  if (kind == common::CompressionKind_ZSTD) {
    const auto bound = ZSTD_compressBound(size);
    BOLT_CHECK(!ZSTD_isError(bound), "Invalid ZSTD spill block size");
    BOLT_CHECK_LE(
        bound,
        static_cast<size_t>(std::numeric_limits<int32_t>::max()),
        "ZSTD spill block exceeds int32 range");
    return static_cast<int32_t>(bound);
  }
  BOLT_CHECK_LE(
      size,
      static_cast<uint64_t>(LZ4_MAX_INPUT_SIZE),
      "Radix sort LZ4 spill block exceeds codec input limit");
  const auto bound = LZ4_compressBound(size);
  BOLT_CHECK_GT(bound, 0, "Invalid LZ4 spill block size");
  return bound;
}

int32_t compressBlock(
    common::CompressionKind kind,
    const char* input,
    int32_t inputSize,
    char* output,
    int32_t outputCapacity) {
  if (kind == common::CompressionKind_ZSTD) {
    const auto result =
        ZSTD_compress(output, outputCapacity, input, inputSize, 3);
    BOLT_CHECK(!ZSTD_isError(result));
    BOLT_CHECK_LE(
        result, static_cast<size_t>(std::numeric_limits<int32_t>::max()));
    return static_cast<int32_t>(result);
  }
  const auto result = LZ4_compress_default(
      input, output, inputSize, static_cast<int>(outputCapacity));
  BOLT_CHECK_GT(result, 0);
  return result;
}

void decompressBlock(
    common::CompressionKind kind,
    const char* input,
    int32_t inputSize,
    char* output,
    int32_t outputSize) {
  if (kind == common::CompressionKind_ZSTD) {
    const auto result = ZSTD_decompress(output, outputSize, input, inputSize);
    BOLT_CHECK(!ZSTD_isError(result));
    BOLT_CHECK_EQ(result, outputSize);
    return;
  }
  const auto result = LZ4_decompress_safe(input, output, inputSize, outputSize);
  BOLT_CHECK_EQ(result, outputSize);
}

std::unique_ptr<SpillInputStream> makeInputStream(
    const RadixSortSpillFile& fileInfo,
    memory::MemoryPool* pool,
    bool spillUringEnabled) {
  auto fs = filesystems::getFileSystem(fileInfo.path, nullptr);
  std::unique_ptr<ReadFile> file;
#ifdef IO_URING_SUPPORTED
  file = spillUringEnabled ? fs->openAsyncFileForRead(fileInfo.path)
                           : fs->openFileForRead(fileInfo.path);
#else
  file = fs->openFileForRead(fileInfo.path);
#endif
  const auto bufferCount = spillUringEnabled && file->uringEnabled() ? 2 : 1;
  std::vector<BufferPtr> buffers(bufferCount);
  for (auto& buffer : buffers) {
    buffer = AlignedBuffer::allocate<char>(kMaxReadBufferSize, pool);
  }
  return std::make_unique<SpillInputStream>(
      std::move(file), std::move(buffers), spillUringEnabled);
}

} // namespace

RadixSortSpillWriter::RadixSortSpillWriter(
    std::string pathPrefix,
    const common::SpillConfig& ioConfig,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats)
    : pathPrefix_(std::move(pathPrefix)),
      ioConfig_(ioConfig),
      pool_(pool),
      stats_(stats) {}

RadixSortSpillWriter::~RadixSortSpillWriter() {
  cleanupFilesNoThrow();
}

void RadixSortSpillWriter::cleanupFilesNoThrow() noexcept {
  const auto removeFileNoThrow = [](const std::string& path) noexcept {
    try {
      auto fs = filesystems::getFileSystem(path, nullptr);
      if (fs->exists(path)) {
        fs->remove(path);
      }
    } catch (const std::exception& error) {
      LOG(WARNING) << "Failed to remove radix sort spill file '" << path
                   << "': " << error.what();
    } catch (...) {
      LOG(WARNING) << "Failed to remove radix sort spill file '" << path << "'";
    }
  };

  for (const auto& file : files_) {
    removeFileNoThrow(file.path);
  }
  files_.clear();
  if (currentFile_ != nullptr) {
    try {
      currentFile_->finish();
    } catch (const std::exception& error) {
      LOG(WARNING) << "Failed to finish radix sort spill file '"
                   << currentFile_->path() << "': " << error.what();
    } catch (...) {
      LOG(WARNING) << "Failed to finish radix sort spill file '"
                   << currentFile_->path() << "'";
    }
    removeFileNoThrow(currentFile_->path());
    currentFile_.reset();
  }
}

void RadixSortSpillWriter::resetWriteState() {
  cleanupFilesNoThrow();
  currentFileRows_ = 0;
  inputBytes_ = 0;
  nextFileId_ = 0;
  pendingRows_.clear();
  pendingBodyCapacity_ = 0;
  pendingBodyBytes_ = 0;
  pendingKeyHeapBytes_ = 0;
  pendingPayloadHeapBytes_ = 0;
}

void RadixSortSpillWriter::prepareWriteBuffer() {
  const auto requested =
      std::max<uint64_t>(ioConfig_.writeBufferSize, kDefaultWriteBufferSize);
  const auto codecLimit =
      maxUncompressedBlockSize(ioConfig_.compressionKind) + kBlockHeaderSize;
  ensureBuffer(std::min(requested, codecLimit));
  normalBufferSize_ = std::min<uint64_t>(buffer_->capacity(), codecLimit);
  resetBuffer(normalBufferSize_);
}

void RadixSortSpillWriter::resetBuffer(uint64_t bytes) {
  pendingRows_.clear();
  pendingBodyCapacity_ = bytes - kBlockHeaderSize;
  pendingBodyBytes_ = 0;
  pendingKeyHeapBytes_ = 0;
  pendingPayloadHeapBytes_ = 0;
}

std::vector<RadixSortSpillFile> RadixSortSpillWriter::writeRun(
    const RadixSortRunStorage& storage,
    const PayloadRowLayout* payloadLayout) {
  resetWriteState();
  prepareWriteBuffer();

  const auto& keyLayout = storage.layout();
  meta_ = RadixRow2RowSerdeMeta::create(keyLayout, payloadLayout);
  const auto payloadFixedOnly =
      payloadLayout == nullptr || !payloadLayout->hasVariableFields();
  const auto dispatchFixedRows = [&]<RadixSortKeyLayoutKind KIND>() {
    for (const auto& block : storage.keyBlocks()) {
      appendFixedRows<KIND>(block.base, block.count);
    }
  };
  if (payloadFixedOnly) {
    switch (keyLayout.kind()) {
      case RadixSortKeyLayoutKind::kKeyOnlyFixed8:
        dispatchFixedRows
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed8>();
        flush();
        closeFile();
        return std::exchange(files_, {});
      case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
        dispatchFixedRows
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed16>();
        flush();
        closeFile();
        return std::exchange(files_, {});
      case RadixSortKeyLayoutKind::kKeyOnlyFixed24:
        dispatchFixedRows
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed24>();
        flush();
        closeFile();
        return std::exchange(files_, {});
      case RadixSortKeyLayoutKind::kKeyOnlyFixed32:
        dispatchFixedRows
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed32>();
        flush();
        closeFile();
        return std::exchange(files_, {});
      case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
        dispatchFixedRows.template
        operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>();
        flush();
        closeFile();
        return std::exchange(files_, {});
      case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
        dispatchFixedRows.template
        operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>();
        flush();
        closeFile();
        return std::exchange(files_, {});
      case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
        dispatchFixedRows.template
        operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>();
        flush();
        closeFile();
        return std::exchange(files_, {});
      default:
        break;
    }
  }

  for (const auto& block : storage.keyBlocks()) {
    for (uint32_t row = 0; row < block.count; ++row) {
      appendRow(block.base + static_cast<uint64_t>(row) * keyLayout.width());
    }
  }
  flush();
  closeFile();
  return std::exchange(files_, {});
}

void RadixSortSpillWriter::writeRows(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* const* keys,
    char* const* payloads,
    vector_size_t count) {
  if (count == 0) {
    return;
  }
  if (buffer_ == nullptr) {
    resetWriteState();
    prepareWriteBuffer();
  }
  meta_ = RadixRow2RowSerdeMeta::create(keyLayout, payloadLayout);
  if (payloadLayout == nullptr) {
    for (vector_size_t row = 0; row < count; ++row) {
      appendRow(keys[row], nullptr);
    }
  } else {
    for (vector_size_t row = 0; row < count; ++row) {
      appendRow(keys[row], payloads[row]);
    }
  }
  flush();
}

std::vector<RadixSortSpillFile> RadixSortSpillWriter::finishRows() {
  flush();
  closeFile();
  return std::exchange(files_, {});
}

void RadixSortSpillWriter::ensureBuffer(uint64_t bytes) {
  const auto required = std::max<uint64_t>(bytes, kBlockHeaderSize + 1);
  if (buffer_ == nullptr || buffer_->capacity() < required) {
    buffer_ = AlignedBuffer::allocate<char>(required, pool_, 0);
  }
}

void RadixSortSpillWriter::ensureRowFits(uint64_t rowSize) {
  BOLT_CHECK_LE(
      rowSize,
      maxUncompressedBlockSize(ioConfig_.compressionKind),
      "Radix sort spill row exceeds block or codec limit");
  ensureBuffer(kBlockHeaderSize + rowSize);
  resetBuffer(kBlockHeaderSize + rowSize);
}

void RadixSortSpillWriter::appendRow(const char* key) {
  appendRow(
      key,
      meta_.keyLayout.hasPayload()
          ? RadixSortKey(meta_.keyLayout, key).payload()
          : nullptr);
}

void RadixSortSpillWriter::appendRow(const char* key, char* payload) {
  const auto rowSize = RadixSortSpillRow::sizeForSerialize(meta_, key, payload);
  if (FOLLY_UNLIKELY(
          rowSize.totalSize > pendingBodyCapacity_ - pendingBodyBytes_)) {
    if (!pendingRows_.empty()) {
      flush();
    }
    if (rowSize.totalSize > pendingBodyCapacity_ - pendingBodyBytes_) {
      ensureRowFits(rowSize.totalSize);
    }
  }
  pendingRows_.push_back(PendingRow{key, rowSize});
  pendingBodyBytes_ += rowSize.totalSize;
  pendingKeyHeapBytes_ += rowSize.keyHeapSize;
  pendingPayloadHeapBytes_ += rowSize.payloadHeapSize;
  ++currentFileRows_;
}

template <RadixSortKeyLayoutKind KIND>
void RadixSortSpillWriter::appendFixedRows(
    const char* keys,
    vector_size_t count) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(!Traits::kVariable);
  const auto rowSize =
      checkedFixedRowSize(meta_.spilledKeyRecordSize, meta_.payloadFixedSize);
  const auto* key = keys;
  RadixSortSpillRowSize size;
  size.keySize = meta_.spilledKeyRecordSize;
  size.payloadFixedSize = meta_.payloadFixedSize;
  size.totalSize = rowSize;
  size.runtimeSize = RadixSortSpillRow::fixedRuntimeRowSize(meta_);
  for (vector_size_t row = 0; row < count; ++row, key += Traits::kWidth) {
    if (FOLLY_UNLIKELY(rowSize > pendingBodyCapacity_ - pendingBodyBytes_)) {
      if (!pendingRows_.empty()) {
        flush();
      }
      if (rowSize > pendingBodyCapacity_ - pendingBodyBytes_) {
        ensureRowFits(rowSize);
      }
    }
    if constexpr (Traits::kHasPayload) {
      size.payload = loadCompactPointer(key + Traits::kPayloadOffset);
    }
    pendingRows_.push_back(PendingRow{key, size});
    pendingBodyBytes_ += rowSize;
    ++currentFileRows_;
  }
}

SpillWriteFile* RadixSortSpillWriter::ensureFile() {
  if (currentFile_ == nullptr) {
    currentFile_ = SpillWriteFile::create(
        nextFileId_++,
        pathPrefix_,
        ioConfig_.fileCreateConfig,
        ioConfig_.spillUringEnabled);
  }
  return currentFile_.get();
}

void RadixSortSpillWriter::flush() {
  if (pendingRows_.empty()) {
    return;
  }
  auto* const start = buffer_->asMutable<char>();
  const auto rowCount = static_cast<uint64_t>(pendingRows_.size());
  BOLT_CHECK_LE(
      rowCount,
      static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()),
      "Radix sort spill block row count exceeds uint32 range");
  const auto keyRecordBytes =
      checkedSectionSize(rowCount, meta_.spilledKeyRecordSize);
  const auto payloadFixedBytes =
      checkedSectionSize(rowCount, meta_.payloadFixedSize);
  const auto uncompressedBytes = checkedBlockBodySize(
      keyRecordBytes,
      pendingKeyHeapBytes_,
      payloadFixedBytes,
      pendingPayloadHeapBytes_);
  BOLT_CHECK_EQ(uncompressedBytes, pendingBodyBytes_);
  BOLT_CHECK_LE(
      uncompressedBytes,
      maxUncompressedBlockSize(ioConfig_.compressionKind),
      "Radix sort spill block exceeds format or codec limit");
  const auto uncompressedSize = static_cast<int32_t>(uncompressedBytes);

  auto* keyRecords = start + kBlockHeaderSize;
  auto* keyHeap = keyRecords + keyRecordBytes;
  auto* payloadFixed = keyHeap + pendingKeyHeapBytes_;
  auto* payloadHeap = payloadFixed + payloadFixedBytes;
  auto* keyHeapCursor = keyHeap;
  auto* payloadHeapCursor = payloadHeap;
  for (uint64_t row = 0; row < rowCount; ++row) {
    auto* keyRecord =
        keyRecords + row * static_cast<uint64_t>(meta_.spilledKeyRecordSize);
    auto* payloadFixedRow = meta_.hasPayload
        ? payloadFixed + row * static_cast<uint64_t>(meta_.payloadFixedSize)
        : nullptr;
    RadixSortSpillRow::serializeRowToSections(
        meta_,
        pendingRows_[row].key,
        pendingRows_[row].size,
        keyRecord,
        keyHeapCursor,
        payloadFixedRow,
        payloadHeapCursor);
  }
  BOLT_DCHECK_EQ(keyHeapCursor, keyHeap + pendingKeyHeapBytes_);
  BOLT_DCHECK_EQ(payloadHeapCursor, payloadHeap + pendingPayloadHeapBytes_);

  const auto nextInputBytes = checkedAdd(inputBytes_, uncompressedBytes);
  BOLT_CHECK(
      nextInputBytes.has_value(), "Radix sort spill input size overflows");
  inputBytes_ = *nextInputBytes;

  auto* file = ensureFile();
  const char* writeBuffer = start;
  uint64_t writeSize = kBlockHeaderSize + uncompressedBytes;
  uint64_t compressTimeUs = 0;
  if (compressionEnabled(ioConfig_.compressionKind)) {
    const auto compressedCapacity =
        compressionBound(ioConfig_.compressionKind, uncompressedSize);
    const auto bound = kBlockHeaderSize + compressedCapacity;
    if (compressedBuffer_ == nullptr || compressedBuffer_->capacity() < bound) {
      compressedBuffer_ = AlignedBuffer::allocate<char>(bound, pool_);
    }
    int32_t compressedSize;
    {
      MicrosecondTimer timer(&compressTimeUs);
      compressedSize = compressBlock(
          ioConfig_.compressionKind,
          start + kBlockHeaderSize,
          uncompressedSize,
          compressedBuffer_->asMutable<char>() + kBlockHeaderSize,
          compressedCapacity);
    }
    auto* header = compressedBuffer_->asMutable<RadixSortSpillBlockHeader>();
    *header = RadixSortSpillBlockHeader{
        uncompressedSize,
        compressedSize,
        static_cast<uint32_t>(rowCount),
        0,
        pendingKeyHeapBytes_,
        pendingPayloadHeapBytes_};
    writeBuffer = compressedBuffer_->as<char>();
    writeSize = kBlockHeaderSize + compressedSize;
  } else {
    auto* header = buffer_->asMutable<RadixSortSpillBlockHeader>();
    *header = RadixSortSpillBlockHeader{
        uncompressedSize,
        uncompressedSize,
        static_cast<uint32_t>(rowCount),
        0,
        pendingKeyHeapBytes_,
        pendingPayloadHeapBytes_};
  }

  uint64_t writeTimeUs = 0;
  uint64_t writtenBytes;
  {
    MicrosecondTimer timer(&writeTimeUs);
    writtenBytes = file->write(std::string_view(writeBuffer, writeSize));
  }
  {
    auto locked = stats_->wlock();
    locked->spilledBytes += writtenBytes;
    locked->spillFlushTimeUs += compressTimeUs;
    locked->spillWriteTimeUs += writeTimeUs;
    ++locked->spillWrites;
  }
  common::updateGlobalSpillWriteStats(
      writtenBytes, compressTimeUs, writeTimeUs);
  if (ioConfig_.updateAndCheckSpillLimitCb != nullptr) {
    ioConfig_.updateAndCheckSpillLimitCb(writtenBytes);
  }
  if (ioConfig_.maxFileSize != 0 && file->size() > ioConfig_.maxFileSize) {
    closeFile();
  }
  resetBuffer(normalBufferSize_);
}

void RadixSortSpillWriter::closeFile() {
  if (currentFile_ == nullptr) {
    return;
  }
  uint64_t finishTimeUs = 0;
  {
    MicrosecondTimer timer(&finishTimeUs);
    currentFile_->finish();
  }
  const auto fileSize = currentFile_->size();
  files_.push_back(RadixSortSpillFile{
      currentFile_->id(),
      currentFile_->path(),
      fileSize,
      currentFileRows_,
      ioConfig_.compressionKind});
  {
    auto locked = stats_->wlock();
    locked->spillWriteTimeUs += finishTimeUs;
  }
  currentFile_.reset();
  currentFileRows_ = 0;
}

RadixSortSpillReader::RadixSortSpillReader(
    RadixSortSpillFile file,
    RadixSortSpillRunMeta meta,
    const PayloadRowLayout* payloadLayout,
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    RadixSortSpillReadBufferCache* bufferCache)
    : file_(std::move(file)),
      meta_(std::move(meta)),
      payloadLayout_(payloadLayout),
      pool_(pool),
      maxReusableRowBufferSize_(
          pool->preferredSize(
              kDefaultWriteBufferSize + AlignedBuffer::kPaddedSize) -
          AlignedBuffer::kPaddedSize),
      input_(makeInputStream(file_, pool, spillUringEnabled)),
      bufferCache_(std::move(bufferCache)) {
  meta_.initialize(payloadLayout_);
}

void RadixSortSpillReader::acquireSerializedBuffer(uint64_t size) {
  if (serializedBuffer_ != nullptr && serializedBuffer_->capacity() >= size) {
    serializedBuffer_->setSize(size);
    checkCompactPointerRange(
        serializedBuffer_->as<char>(), serializedBuffer_->capacity());
    return;
  }
  if (bufferCache_ != nullptr) {
    auto& reusable = bufferCache_->serializedBuffer;
    if (reusable != nullptr && reusable->isMutable() &&
        reusable->capacity() >= size) {
      serializedBuffer_ = std::move(reusable);
      serializedBuffer_->setSize(size);
      checkCompactPointerRange(
          serializedBuffer_->as<char>(), serializedBuffer_->capacity());
      return;
    }
  }
  serializedBuffer_ = AlignedBuffer::allocate<char>(size, pool_);
  checkCompactPointerRange(
      serializedBuffer_->as<char>(), serializedBuffer_->capacity());
}

void RadixSortSpillReader::recycleSerializedBuffer() {
  if (serializedBuffer_ == nullptr) {
    return;
  }
  if (bufferCache_ == nullptr) {
    return;
  }
  if (!serializedBuffer_->isMutable() ||
      serializedBuffer_->size() > maxReusableRowBufferSize_) {
    serializedBuffer_.reset();
    return;
  }
  if (bufferCache_->serializedBuffer == nullptr ||
      serializedBuffer_->capacity() >
          bufferCache_->serializedBuffer->capacity()) {
    bufferCache_->serializedBuffer = std::move(serializedBuffer_);
    return;
  }
  serializedBuffer_.reset();
}

void RadixSortSpillReader::recycleRetainedSerializedBuffer() {
  BufferPtr candidate;
  for (auto& buffer : retainedSerializedBuffers_) {
    if (buffer == nullptr || !buffer->isMutable() ||
        buffer->size() > maxReusableRowBufferSize_) {
      continue;
    }
    if (candidate == nullptr || buffer->capacity() > candidate->capacity()) {
      candidate = std::move(buffer);
    }
  }
  retainedSerializedBuffers_.clear();
  if (bufferCache_ != nullptr && candidate != nullptr &&
      (bufferCache_->serializedBuffer == nullptr ||
       candidate->capacity() > bufferCache_->serializedBuffer->capacity())) {
    bufferCache_->serializedBuffer = std::move(candidate);
  }
}

void RadixSortSpillReader::acquireRowBuffer(uint64_t size) {
  if (bufferCache_ != nullptr) {
    auto& reusable = bufferCache_->rowBuffer;
    if (reusable != nullptr && reusable->isMutable() &&
        reusable->capacity() >= size) {
      rowBuffer_ = std::move(reusable);
      rowBuffer_->setSize(size);
      checkCompactPointerRange(rowBuffer_->as<char>(), rowBuffer_->capacity());
      return;
    }
  }
  rowBuffer_ = AlignedBuffer::allocate<char>(size, pool_);
  checkCompactPointerRange(rowBuffer_->as<char>(), rowBuffer_->capacity());
}

void RadixSortSpillReader::recycleRetainedRowBuffer() {
  // Output pointers may span blocks. Recycle only after the caller has
  // materialized them and explicitly released the retained buffers.
  BufferPtr candidate;
  for (auto& buffer : retainedRowBuffers_) {
    if (buffer == nullptr || !buffer->isMutable() ||
        buffer->size() > maxReusableRowBufferSize_) {
      continue;
    }
    if (candidate == nullptr || buffer->capacity() > candidate->capacity()) {
      candidate = std::move(buffer);
    }
  }
  retainedRowBuffers_.clear();
  if (bufferCache_ != nullptr && candidate != nullptr &&
      (bufferCache_->rowBuffer == nullptr ||
       candidate->capacity() > bufferCache_->rowBuffer->capacity())) {
    bufferCache_->rowBuffer = std::move(candidate);
  }
}

bool RadixSortSpillReader::nextBatch(
    std::vector<char*>& keys,
    std::vector<char*>& payloads) {
  keys.clear();
  payloads.clear();
  if (input_ == nullptr) {
    return false;
  }
  if (input_->atEnd()) {
    return false;
  }

  MicrosecondTimer readTimer(&spillReadTimeUs_);
  RadixSortSpillBlockHeader header;
  input_->readBytes(reinterpret_cast<char*>(&header), sizeof(header));
  const auto uncompressedSize = header.uncompressedSize;
  const auto storedSize = header.storedSize;
  BOLT_CHECK_GT(uncompressedSize, 0);
  BOLT_CHECK_GT(storedSize, 0);
  BOLT_CHECK_GT(header.rowCount, 0);
  BOLT_CHECK_LE(
      uncompressedSize,
      maxUncompressedBlockSize(file_.compressionKind),
      "Radix sort spill block exceeds codec limit");
  if (!compressionEnabled(file_.compressionKind)) {
    BOLT_CHECK_EQ(
        uncompressedSize,
        storedSize,
        "Invalid uncompressed radix sort spill block size");
  } else {
    BOLT_CHECK_LE(
        storedSize,
        compressionBound(file_.compressionKind, uncompressedSize),
        "Invalid compressed radix sort spill block size");
  }
  if (rowBuffer_ != nullptr) {
    retainedRowBuffers_.push_back(std::move(rowBuffer_));
  }
  if (serializedBuffer_ != nullptr) {
    retainedSerializedBuffers_.push_back(std::move(serializedBuffer_));
  }
  const auto canUseDiskRowsAsRuntimeRows = meta_.payloadFixedSize == 0 &&
      !meta_.keyLayout.hasPayload() && !meta_.hasKeyHeap();
  const auto keyRecordBytes =
      checkedSectionSize(header.rowCount, meta_.spilledKeyRecordSize);
  const auto payloadFixedBytes =
      checkedSectionSize(header.rowCount, meta_.payloadFixedSize);
  const auto expectedUncompressedSize = checkedBlockBodySize(
      keyRecordBytes,
      header.keyHeapBytes,
      payloadFixedBytes,
      header.payloadHeapBytes);
  BOLT_CHECK_EQ(
      static_cast<uint64_t>(uncompressedSize), expectedUncompressedSize);
  const auto runtimeBufferBytes = canUseDiskRowsAsRuntimeRows
      ? 0
      : checkedSectionSize(header.rowCount, meta_.runtimeKeyRecordSize);
  if (canUseDiskRowsAsRuntimeRows) {
    acquireRowBuffer(uncompressedSize);
  } else {
    acquireSerializedBuffer(uncompressedSize);
    acquireRowBuffer(runtimeBufferBytes);
  }
  auto* block = canUseDiskRowsAsRuntimeRows
      ? rowBuffer_->asMutable<char>()
      : serializedBuffer_->asMutable<char>();
  if (compressionEnabled(file_.compressionKind)) {
    if (file_.compressionKind == common::CompressionKind_LZ4) {
      BOLT_CHECK_LE(
          uncompressedSize,
          LZ4_MAX_INPUT_SIZE,
          "Invalid radix sort LZ4 spill block size");
    }
    if (compressedBuffer_ == nullptr ||
        compressedBuffer_->capacity() < storedSize) {
      compressedBuffer_ = AlignedBuffer::allocate<char>(storedSize, pool_);
    }
    input_->readBytes(compressedBuffer_->asMutable<char>(), storedSize);
    {
      MicrosecondTimer timer(&spillDecompressTimeUs_);
      decompressBlock(
          file_.compressionKind,
          compressedBuffer_->as<char>(),
          storedSize,
          block,
          uncompressedSize);
    }
  } else {
    input_->readBytes(block, uncompressedSize);
  }

  if (nextFixedBatch(
          block,
          uncompressedSize,
          rowBuffer_->asMutable<char>(),
          keys,
          payloads)) {
    return true;
  }

  keys.reserve(header.rowCount);
  payloads.reserve(header.rowCount);
  char* keyRecord = block;
  char* keyHeap = keyRecord + keyRecordBytes;
  char* const keyHeapEnd = keyHeap + header.keyHeapBytes;
  char* payloadFixed = keyHeapEnd;
  char* payloadHeap = payloadFixed + payloadFixedBytes;
  char* const payloadHeapEnd = payloadHeap + header.payloadHeapBytes;
  auto* runtime = rowBuffer_->asMutable<char>();
  bool validRows = true;
  for (uint32_t rowIndex = 0; rowIndex < header.rowCount; ++rowIndex) {
    auto* const payloadFixedRow = meta_.hasPayload ? payloadFixed +
            static_cast<uint64_t>(rowIndex) * meta_.payloadFixedSize
                                                   : nullptr;
    const auto row = RadixSortSpillRow::deserializeRowFromSections(
        meta_,
        keyRecord,
        keyHeap,
        keyHeapEnd,
        payloadFixedRow,
        payloadHeap,
        payloadHeapEnd,
        runtime);
    validRows &= row.has_value();
    if (!row.has_value()) {
      break;
    }
    keys.push_back(row->key);
    payloads.push_back(row->payload);
    keyRecord += meta_.spilledKeyRecordSize;
    runtime = row->nextOutput;
  }
  BOLT_CHECK(validRows, "Invalid radix sort spill row in block");
  BOLT_CHECK_EQ(keyRecord, block + keyRecordBytes);
  BOLT_CHECK_EQ(keyHeap, keyHeapEnd);
  BOLT_CHECK_EQ(payloadHeap, payloadHeapEnd);
  return true;
}

bool RadixSortSpillReader::nextFixedBatch(
    char* block,
    int32_t uncompressedSize,
    char* output,
    std::vector<char*>& keys,
    std::vector<char*>& payloads) {
  const auto rowSize = RadixSortSpillRow::fixedSerializedRowSize(meta_);
  if (rowSize == 0 || uncompressedSize % rowSize != 0) {
    return false;
  }
  if (meta_.hasKeyHeap() || meta_.hasVariablePayload()) {
    return false;
  }
  const auto count = uncompressedSize / rowSize;
  keys.reserve(count);
  payloads.reserve(count);
  const auto keyRecordBytes =
      checkedSectionSize(count, meta_.spilledKeyRecordSize);
  auto* keyRecord = block;
  if (meta_.payloadFixedSize == 0 && !meta_.keyLayout.hasPayload()) {
    for (uint32_t row = 0; row < count; ++row) {
      keys.push_back(keyRecord);
      payloads.push_back(nullptr);
      keyRecord += meta_.spilledKeyRecordSize;
    }
    return true;
  }

  auto* runtime = output;
  char* payloadFixed = block + keyRecordBytes;
  for (uint32_t row = 0; row < count; ++row) {
    auto* const runtimeKey = runtime;
    std::memcpy(runtimeKey, keyRecord, meta_.spilledKeyRecordSize);
    runtime += meta_.runtimeKeyRecordSize;
    char* runtimePayload = nullptr;
    if (meta_.hasPayload) {
      runtimePayload = payloadFixed;
      payloadFixed += meta_.payloadFixedSize;
    }
    if (meta_.keyLayout.hasPayload()) {
      storeCompactPointer(runtimeKey + meta_.keyPayloadOffset, runtimePayload);
    }
    keys.push_back(runtimeKey);
    payloads.push_back(runtimePayload);
    keyRecord += meta_.spilledKeyRecordSize;
  }
  return true;
}

uint64_t RadixSortSpillReader::spillReadIOTimeUs() const {
  return spillReadIOTimeUs_ +
      (input_ == nullptr ? 0 : input_->getSpillReadIOTime());
}

void RadixSortSpillReader::close() {
  if (input_ == nullptr) {
    return;
  }
  spillReadIOTimeUs_ += input_->getSpillReadIOTime();
  input_.reset();
}

int32_t RadixSortMergeStream::compare(const MergeStream& other) const {
  const auto& typed = static_cast<const RadixSortMergeStream&>(other);
  return RadixSortKey(keyLayout_, key_)
      .compare(RadixSortKey(keyLayout_, typed.key_));
}

RadixSortMemoryRunMergeStream::RadixSortMemoryRunMergeStream(
    const RadixSortRunStorage& storage)
    : RadixSortMergeStream(storage.layout()), storage_(storage) {
  loadCurrent();
}

bool RadixSortMemoryRunMergeStream::hasData() const {
  return key_ != nullptr;
}

void RadixSortMemoryRunMergeStream::pop() {
  ++index_;
  loadCurrent();
}

void RadixSortMemoryRunMergeStream::loadCurrent() {
  if (index_ >= storage_.size()) {
    key_ = nullptr;
    payload_ = nullptr;
    return;
  }
  if (range_.data == nullptr || rangeIndex_ == range_.count) {
    range_ = storage_.keyRangeAt(index_, storage_.keysPerBlock());
    rangeIndex_ = 0;
  }
  key_ =
      range_.data + static_cast<uint64_t>(rangeIndex_++) * keyLayout_.width();
  payload_ = RadixSortKey(keyLayout_, key_).payload();
}

RadixSortSpillFileMergeStream::RadixSortSpillFileMergeStream(
    RadixSortSpillFile file,
    RadixSortSpillRunMeta meta,
    const PayloadRowLayout* payloadLayout,
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    RadixSortSpillReadBufferCache* bufferCache)
    : RadixSortMergeStream(meta.keyLayout),
      fileGuard_(file.path),
      reader_(
          std::move(file),
          std::move(meta),
          payloadLayout,
          pool,
          spillUringEnabled,
          bufferCache) {
  try {
    loadBatch();
  } catch (...) {
    closeNoThrow();
    throw;
  }
}

RadixSortSpillFileMergeStream::SpillFileGuard::~SpillFileGuard() {
  removeNoThrow();
}

void RadixSortSpillFileMergeStream::SpillFileGuard::remove() {
  if (path_.empty()) {
    return;
  }
  auto fs = filesystems::getFileSystem(path_, nullptr);
  fs->remove(path_);
  path_.clear();
}

void RadixSortSpillFileMergeStream::SpillFileGuard::removeNoThrow() noexcept {
  try {
    remove();
  } catch (const std::exception& error) {
    LOG(WARNING) << "Failed to remove radix sort spill file '" << path_
                 << "': " << error.what();
  } catch (...) {
    LOG(WARNING) << "Failed to remove radix sort spill file '" << path_ << "'";
  }
}

RadixSortSpillFileMergeStream::~RadixSortSpillFileMergeStream() {
  closeNoThrow();
}

bool RadixSortSpillFileMergeStream::hasData() const {
  return key_ != nullptr;
}

void RadixSortSpillFileMergeStream::pop() {
  ++index_;
  if (index_ >= keys_.size()) {
    loadBatch();
    return;
  }
  key_ = keys_[index_];
  payload_ = payloads_[index_];
}

void RadixSortSpillFileMergeStream::loadBatch() {
  try {
    if (!reader_.nextBatch(keys_, payloads_)) {
      finishReading();
      return;
    }
  } catch (...) {
    closeNoThrow();
    throw;
  }
  index_ = 0;
  key_ = keys_[0];
  payload_ = payloads_[0];
}

void RadixSortSpillFileMergeStream::finishReading() {
  key_ = nullptr;
  payload_ = nullptr;
  std::vector<char*>{}.swap(keys_);
  std::vector<char*>{}.swap(payloads_);
  reader_.close();
  fileGuard_.removeNoThrow();
}

void RadixSortSpillFileMergeStream::closeNoThrow() noexcept {
  try {
    finishReading();
  } catch (const std::exception& error) {
    LOG(WARNING) << "Failed to close radix sort spill input: " << error.what();
  } catch (...) {
    LOG(WARNING) << "Failed to close radix sort spill input";
  }
}

RadixSortMerger::RadixSortMerger(
    RadixSortKeyLayout keyLayout,
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams,
    std::unique_ptr<RadixSortSpillReadBufferCache> bufferCache)
    : keyLayout_(std::move(keyLayout)),
      compareKeys_(compareKeysForLayout(keyLayout_.kind())),
      bufferCache_(std::move(bufferCache)),
      streams_(std::move(streams)) {
  if (streams_.size() <= 2) {
    return;
  }

  int32_t size = 0;
  int32_t levelSize = 1;
  const auto numStreams = static_cast<int32_t>(streams_.size());
  while (numStreams > levelSize) {
    size += levelSize;
    levelSize *= 2;
  }
  if (numStreams == bits::nextPowerOfTwo(numStreams)) {
    firstStream_ = size;
  } else {
    const auto secondLastSize = levelSize / 2;
    const auto overflow = numStreams - secondLastSize;
    firstStream_ = (size - secondLastSize) + overflow;
  }
  losers_.resize(firstStream_, kEmpty);
}

vector_size_t RadixSortMerger::collectRows(
    vector_size_t count,
    const char** keys,
    char** payloads) {
  if (streams_.size() == 1) {
    return collectSingleStreamRows(count, keys, payloads);
  }
  if (streams_.size() == 2) {
    return collectTwoWayRows(count, keys, payloads);
  }
  return collectLoserTreeRows(count, keys, payloads);
}

vector_size_t RadixSortMerger::collectSingleStreamRows(
    vector_size_t count,
    const char** keys,
    char** payloads) {
  auto* stream = streams_[0].get();
  vector_size_t row = 0;
  for (; row < count; ++row) {
    keys[row] = stream->key();
    payloads[row] = stream->payload();
    stream->pop();
  }
  return row;
}

vector_size_t RadixSortMerger::collectTwoWayRows(
    vector_size_t count,
    const char** keys,
    char** payloads) {
  auto* left = streams_[0].get();
  auto* right = streams_[1].get();
  auto* leftKey = left->key();
  auto* rightKey = right->key();
  vector_size_t row = 0;
  for (; row < count; ++row) {
    if (rightKey == nullptr ||
        (leftKey != nullptr && compareKeys(leftKey, rightKey) < 0)) {
      keys[row] = leftKey;
      payloads[row] = left->payload();
      left->pop();
      leftKey = left->key();
    } else {
      keys[row] = rightKey;
      payloads[row] = right->payload();
      right->pop();
      rightKey = right->key();
    }
  }
  return row;
}

vector_size_t RadixSortMerger::collectLoserTreeRows(
    vector_size_t count,
    const char** keys,
    char** payloads) {
  vector_size_t row = 0;
  for (; row < count; ++row) {
    const auto index = nextLoserTreeStream();
    auto* stream = streams_[index].get();
    keys[row] = stream->key();
    payloads[row] = stream->payload();
    stream->pop();
  }
  return row;
}

bool RadixSortMerger::less(StreamIndex left, StreamIndex right) const {
  return compareKeys(streams_[left]->key(), streams_[right]->key()) < 0;
}

RadixSortMerger::StreamIndex RadixSortMerger::nextLoserTreeStream() {
  if (lastIndex_ == kEmpty) {
    lastIndex_ = first(0);
  } else {
    lastIndex_ = propagate(
        parent(firstStream_ + lastIndex_),
        streams_[lastIndex_]->key() == nullptr ? kEmpty : lastIndex_);
  }
  return lastIndex_;
}

RadixSortMerger::StreamIndex RadixSortMerger::first(int32_t node) {
  if (node >= firstStream_) {
    const auto index = static_cast<StreamIndex>(node - firstStream_);
    return streams_[index]->key() == nullptr ? kEmpty : index;
  }
  const auto left = first(leftChild(node));
  const auto right = first(rightChild(node));
  if (left == kEmpty) {
    return right;
  }
  if (right == kEmpty) {
    return left;
  }
  if (less(left, right)) {
    losers_[node] = right;
    return left;
  }
  losers_[node] = left;
  return right;
}

RadixSortMerger::StreamIndex RadixSortMerger::propagate(
    int32_t node,
    StreamIndex value) {
  while (losers_[node] == kEmpty) {
    if (node == 0) {
      return value;
    }
    node = parent(node);
  }
  for (;;) {
    if (losers_[node] == kEmpty) {
    } else if (value == kEmpty) {
      value = losers_[node];
      losers_[node] = kEmpty;
    } else if (less(losers_[node], value)) {
      std::swap(value, losers_[node]);
    }
    if (node == 0) {
      return value;
    }
    node = parent(node);
  }
}

uint64_t RadixSortMerger::getSpillReadTime() const {
  uint64_t time = 0;
  for (const auto& stream : streams_) {
    time += stream->getSpillReadTime();
  }
  return time;
}

uint64_t RadixSortMerger::getSpillDecompressTime() const {
  uint64_t time = 0;
  for (const auto& stream : streams_) {
    time += stream->getSpillDecompressTime();
  }
  return time;
}

uint64_t RadixSortMerger::getSpillReadIOTime() const {
  uint64_t time = 0;
  for (const auto& stream : streams_) {
    time += stream->getSpillReadIOTime();
  }
  return time;
}

void RadixSortMerger::releaseRetainedBuffers() {
  for (auto& stream : streams_) {
    stream->releaseRetainedBuffers();
  }
}

} // namespace bytedance::bolt::exec::radixsort
