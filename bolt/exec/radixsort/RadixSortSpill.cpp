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
  uint64_t keyRecordBytes;
  uint64_t keyHeapBytes;
  uint64_t payloadFixedBytes;
  uint64_t payloadHeapBytes;
};

constexpr uint64_t kBlockHeaderSize = sizeof(RadixSortSpillBlockHeader);
constexpr uint64_t kDefaultWriteBufferSize = 1 << 20;
constexpr uint64_t kMaxReadBufferSize = (1 << 20) - AlignedBuffer::kPaddedSize;
static_assert(sizeof(RadixSortSpillBlockHeader) == 48);

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
  clearPendingRange();
  pendingBodyCapacity_ = 0;
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
  clearPendingRange();
  pendingBodyCapacity_ = bytes - kBlockHeaderSize;
}

std::vector<RadixSortSpillFile> RadixSortSpillWriter::writeRun(
    const RadixSortRunStorage& storage,
    const PayloadRowLayout* payloadLayout) {
  resetWriteState();
  prepareWriteBuffer();

  const auto& keyLayout = storage.layout();
  meta_ = RadixSortSpillSectionMeta::create(keyLayout, payloadLayout);
  for (const auto& block : storage.keyBlocks()) {
    appendKeyRange(block.base, block.count);
    flush();
  }
  closeFile();
  return std::exchange(files_, {});
}

void RadixSortSpillWriter::writeKeyRange(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* keyBase,
    vector_size_t count) {
  if (count == 0) {
    return;
  }
  if (buffer_ == nullptr) {
    resetWriteState();
    prepareWriteBuffer();
  }
  meta_ = RadixSortSpillSectionMeta::create(keyLayout, payloadLayout);
  appendKeyRange(keyBase, count);
  flush();
}

std::vector<RadixSortSpillFile> RadixSortSpillWriter::finish() {
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

void RadixSortSpillWriter::ensureRecordFits(uint64_t recordSize) {
  BOLT_CHECK_LE(
      recordSize,
      maxUncompressedBlockSize(ioConfig_.compressionKind),
      "Radix sort spill record exceeds block or codec limit");
  ensureBuffer(kBlockHeaderSize + recordSize);
  resetBuffer(kBlockHeaderSize + recordSize);
}

void RadixSortSpillWriter::clearPendingRange() {
  pendingRange_ = PendingRange{};
  pendingSectionSizes_.clear();
}

void RadixSortSpillWriter::appendKeyRange(
    const char* keyBase,
    vector_size_t count) {
  const auto keyWidth = meta_.runtimeKeyRecordSize;
  for (vector_size_t row = 0; row < count; ++row) {
    const auto* key = keyBase + static_cast<uint64_t>(row) * keyWidth;
    const auto sectionSize =
        RadixSortSpillSections::sizeForSerialize(meta_, key);
    if (FOLLY_UNLIKELY(
            sectionSize.totalSize >
            pendingBodyCapacity_ - pendingRange_.totalBytes())) {
      if (pendingRange_.rowCount > 0) {
        flush();
      }
      if (sectionSize.totalSize >
          pendingBodyCapacity_ - pendingRange_.totalBytes()) {
        ensureRecordFits(sectionSize.totalSize);
      }
    }
    if (pendingRange_.rowCount == 0) {
      pendingRange_.keyBase = key;
    }
    BOLT_DCHECK_EQ(
        key,
        pendingRange_.keyBase +
            static_cast<uint64_t>(pendingRange_.rowCount) * keyWidth);
    pendingSectionSizes_.push_back(sectionSize);
    ++pendingRange_.rowCount;
    pendingRange_.keyRecordBytes += keyWidth;
    pendingRange_.keyHeapBytes += sectionSize.keyHeapSize;
    pendingRange_.payloadFixedBytes += meta_.payloadFixedSize;
    pendingRange_.payloadHeapBytes += sectionSize.payloadHeapSize;
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
  if (pendingRange_.rowCount == 0) {
    return;
  }
  auto* const start = buffer_->asMutable<char>();
  const auto rowCount = static_cast<uint64_t>(pendingRange_.rowCount);
  BOLT_CHECK_LE(
      rowCount,
      static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()),
      "Radix sort spill block row count exceeds uint32 range");
  const auto keyRecordBytes =
      checkedSectionSize(rowCount, meta_.runtimeKeyRecordSize);
  const auto payloadFixedBytes =
      checkedSectionSize(rowCount, meta_.payloadFixedSize);
  BOLT_DCHECK_EQ(keyRecordBytes, pendingRange_.keyRecordBytes);
  BOLT_DCHECK_EQ(payloadFixedBytes, pendingRange_.payloadFixedBytes);
  const auto uncompressedBytes = checkedBlockBodySize(
      keyRecordBytes,
      pendingRange_.keyHeapBytes,
      payloadFixedBytes,
      pendingRange_.payloadHeapBytes);
  BOLT_DCHECK_EQ(uncompressedBytes, pendingRange_.totalBytes());
  BOLT_CHECK_LE(
      uncompressedBytes,
      maxUncompressedBlockSize(ioConfig_.compressionKind),
      "Radix sort spill block exceeds format or codec limit");
  const auto uncompressedSize = static_cast<int32_t>(uncompressedBytes);

  auto* keyRecords = start + kBlockHeaderSize;
  auto* keyHeap = keyRecords + keyRecordBytes;
  auto* payloadFixed = keyHeap + pendingRange_.keyHeapBytes;
  auto* payloadHeap = payloadFixed + payloadFixedBytes;
  std::memcpy(keyRecords, pendingRange_.keyBase, keyRecordBytes);

  auto* keyHeapCursor = keyHeap;
  for (uint64_t row = 0; row < rowCount; ++row) {
    const auto* sourceKey = pendingRange_.keyBase +
        row * static_cast<uint64_t>(meta_.runtimeKeyRecordSize);
    RadixSortSpillSections::copyKeyHeapToSection(
        meta_, sourceKey, pendingSectionSizes_[row].keyHeapSize, keyHeapCursor);
  }
  BOLT_DCHECK_EQ(keyHeapCursor, keyHeap + pendingRange_.keyHeapBytes);

  if (meta_.hasPayload) {
    for (uint64_t row = 0; row < rowCount; ++row) {
      const auto* sourceKey = pendingRange_.keyBase +
          row * static_cast<uint64_t>(meta_.runtimeKeyRecordSize);
      auto* payloadFixedRow =
          payloadFixed + row * static_cast<uint64_t>(meta_.payloadFixedSize);
      RadixSortSpillSections::copyPayloadFixedToSection(
          meta_, sourceKey, payloadFixedRow);
    }
  }

  auto* payloadHeapCursor = payloadHeap;
  if (meta_.hasPayload) {
    for (uint64_t row = 0; row < rowCount; ++row) {
      auto* payloadFixedRow =
          payloadFixed + row * static_cast<uint64_t>(meta_.payloadFixedSize);
      RadixSortSpillSections::copyPayloadHeapFromFixedToSection(
          meta_,
          payloadFixedRow,
          pendingSectionSizes_[row].payloadHeapSize,
          payloadHeapCursor);
    }
  }
  BOLT_DCHECK_EQ(
      payloadHeapCursor, payloadHeap + pendingRange_.payloadHeapBytes);

  RadixSortSpillSections::clearKeyPointers(meta_, keyRecords, rowCount);
  if (meta_.hasPayload) {
    RadixSortSpillSections::clearPayloadPointers(meta_, payloadFixed, rowCount);
  }

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
        keyRecordBytes,
        pendingRange_.keyHeapBytes,
        payloadFixedBytes,
        pendingRange_.payloadHeapBytes};
    writeBuffer = compressedBuffer_->as<char>();
    writeSize = kBlockHeaderSize + compressedSize;
  } else {
    auto* header = buffer_->asMutable<RadixSortSpillBlockHeader>();
    *header = RadixSortSpillBlockHeader{
        uncompressedSize,
        uncompressedSize,
        static_cast<uint32_t>(rowCount),
        0,
        keyRecordBytes,
        pendingRange_.keyHeapBytes,
        payloadFixedBytes,
        pendingRange_.payloadHeapBytes};
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
    RadixSortSpillSectionMeta meta,
    const PayloadRowLayout* payloadLayout,
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    RadixSortSpillReadBufferCache* bufferCache)
    : file_(std::move(file)),
      meta_(std::move(meta)),
      payloadLayout_(payloadLayout),
      pool_(pool),
      maxReusableSerializedBufferSize_(
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
      serializedBuffer_->size() > maxReusableSerializedBufferSize_) {
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
        buffer->size() > maxReusableSerializedBufferSize_) {
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

bool RadixSortSpillReader::nextBatch(std::vector<const char*>& keys) {
  keys.clear();
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
  if (serializedBuffer_ != nullptr) {
    retainedSerializedBuffers_.push_back(std::move(serializedBuffer_));
  }
  const auto keyRecordBytes =
      checkedSectionSize(header.rowCount, meta_.runtimeKeyRecordSize);
  const auto payloadFixedBytes =
      checkedSectionSize(header.rowCount, meta_.payloadFixedSize);
  BOLT_CHECK_EQ(header.keyRecordBytes, keyRecordBytes);
  BOLT_CHECK_EQ(header.payloadFixedBytes, payloadFixedBytes);
  const auto expectedUncompressedSize = checkedBlockBodySize(
      header.keyRecordBytes,
      header.keyHeapBytes,
      header.payloadFixedBytes,
      header.payloadHeapBytes);
  BOLT_CHECK_EQ(
      static_cast<uint64_t>(uncompressedSize), expectedUncompressedSize);
  acquireSerializedBuffer(uncompressedSize);
  auto* block = serializedBuffer_->asMutable<char>();
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

  keys.reserve(header.rowCount);
  char* keyRecords = block;
  char* keyHeap = keyRecords + header.keyRecordBytes;
  char* const keyHeapEnd = keyHeap + header.keyHeapBytes;
  char* payloadFixed = keyHeapEnd;
  char* payloadHeap = payloadFixed + header.payloadFixedBytes;
  char* const payloadHeapEnd = payloadHeap + header.payloadHeapBytes;
  bool validRows = true;
  for (uint32_t rowIndex = 0; rowIndex < header.rowCount; ++rowIndex) {
    auto* key = keyRecords +
        static_cast<uint64_t>(rowIndex) * meta_.runtimeKeyRecordSize;
    auto* const payloadFixedRow = meta_.hasPayload ? payloadFixed +
            static_cast<uint64_t>(rowIndex) * meta_.payloadFixedSize
                                                   : nullptr;
    const auto restored = RadixSortSpillSections::restorePointersInSections(
        meta_,
        key,
        keyHeap,
        keyHeapEnd,
        payloadFixedRow,
        payloadHeap,
        payloadHeapEnd);
    validRows &= restored.has_value();
    if (!restored.has_value()) {
      break;
    }
    keys.push_back(key);
  }
  BOLT_CHECK(validRows, "Invalid radix sort spill section block");
  BOLT_CHECK_EQ(keyHeap, keyHeapEnd);
  BOLT_CHECK_EQ(payloadHeap, payloadHeapEnd);
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
    RadixSortSpillSectionMeta meta,
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
  payload_ = keyLayout_.hasPayload() ? RadixSortKey(keyLayout_, key_).payload()
                                     : nullptr;
}

void RadixSortSpillFileMergeStream::loadBatch() {
  try {
    if (!reader_.nextBatch(keys_)) {
      finishReading();
      return;
    }
  } catch (...) {
    closeNoThrow();
    throw;
  }
  index_ = 0;
  key_ = keys_[0];
  payload_ = keyLayout_.hasPayload() ? RadixSortKey(keyLayout_, key_).payload()
                                     : nullptr;
}

void RadixSortSpillFileMergeStream::finishReading() {
  key_ = nullptr;
  payload_ = nullptr;
  std::vector<const char*>{}.swap(keys_);
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
