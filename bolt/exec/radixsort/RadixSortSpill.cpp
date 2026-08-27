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

#include <cstring>
#include <limits>
#include <span>
#include <utility>

#include <folly/ScopeGuard.h>

#include "bolt/common/base/BitUtil.h"
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

RadixSortSpillSectionBatchSize fixedSizeRows(
    const RadixSortSpillSectionMeta& meta,
    uint64_t rowCount) {
  return RadixSortSpillSectionBatchSize{
      rowCount,
      rowCount * meta.runtimeKeyRecordSize,
      0,
      rowCount * meta.payloadFixedSize,
      0};
}

RadixSortSpillSectionBatchSize prefixFromPresizedRows(
    const RadixSortSpillSectionMeta& meta,
    uint64_t maxRowCount,
    uint64_t maxBytes,
    std::span<const RadixSortSpillSectionSize> rowSizes) {
  if (rowSizes.empty()) {
    const auto fixedRowBytes =
        meta.runtimeKeyRecordSize + meta.payloadFixedSize;
    const auto rowCount = std::min(maxRowCount, maxBytes / fixedRowBytes);
    return fixedSizeRows(meta, rowCount);
  }

  uint64_t rowCount = 0;
  uint64_t totalBytes = 0;
  uint64_t keyHeapBytes = 0;
  uint64_t payloadHeapBytes = 0;
  for (; rowCount < maxRowCount; ++rowCount) {
    const auto& rowSize = rowSizes[rowCount];
    if (rowSize.totalSize > maxBytes - totalBytes) {
      break;
    }
    totalBytes += rowSize.totalSize;
    keyHeapBytes += rowSize.keyHeapSize;
    payloadHeapBytes += rowSize.payloadHeapSize;
  }
  return RadixSortSpillSectionBatchSize{
      rowCount,
      rowCount * meta.runtimeKeyRecordSize,
      keyHeapBytes,
      rowCount * meta.payloadFixedSize,
      payloadHeapBytes};
}

void removeSpillFileNoThrow(const std::string& path) noexcept {
  if (path.empty()) {
    return;
  }
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
}

void cleanupSpillFilesNoThrow(const SpillFiles& files) noexcept {
  for (const auto& file : files) {
    removeSpillFileNoThrow(file.path);
  }
}

} // namespace

RadixSortSpillWriter::RadixSortSpillWriter(
    std::string pathPrefix,
    const common::SpillConfig& ioConfig,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats)
    : compressionKind_(ioConfig.compressionKind),
      writeBufferSize_(ioConfig.writeBufferSize),
      pool_(pool),
      spillWriter_(std::make_unique<SpillWriter>(
          std::move(pathPrefix),
          ioConfig.maxFileSize == 0 ? std::numeric_limits<uint64_t>::max()
                                    : ioConfig.maxFileSize,
          ioConfig.spillIOConfig(1),
          pool,
          stats)) {}

RadixSortSpillWriter::~RadixSortSpillWriter() {
  if (!finished_) {
    spillWriter_->cleanupFilesNoThrow();
  }
}

void RadixSortSpillWriter::resetWriteState() {
  spillWriter_->cleanupFilesNoThrow();
  finished_ = false;
  inputBytes_ = 0;
  clearPendingBlock();
  pendingBodyCapacity_ = 0;
}

void RadixSortSpillWriter::prepareWriteBuffer() {
  const auto requested =
      writeBufferSize_ == 0 ? kDefaultWriteBufferSize : writeBufferSize_;
  BOLT_CHECK_GT(
      requested,
      kBlockHeaderSize,
      "Radix sort spill write buffer must fit block header");
  const auto codecLimit =
      maxUncompressedSpillBlockSize(compressionKind_) + kBlockHeaderSize;
  ensureBuffer(std::min(requested, codecLimit));
  normalBufferSize_ = std::min<uint64_t>(requested, codecLimit);
  resetBuffer(normalBufferSize_);
}

void RadixSortSpillWriter::resetBuffer(uint64_t bytes) {
  clearPendingBlock();
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
  }
  return finish();
}

void RadixSortSpillWriter::writeKeyRange(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* keyBase,
    vector_size_t count) {
  if (count == 0) {
    return;
  }
  if (buffer_ == nullptr || finished_) {
    resetWriteState();
    prepareWriteBuffer();
  }
  meta_ = RadixSortSpillSectionMeta::create(keyLayout, payloadLayout);
  appendKeyRange(keyBase, count);
  flush();
}

vector_size_t RadixSortSpillWriter::writePresizedKeyRange(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* keyBase,
    vector_size_t count,
    std::span<const RadixSortSpillSectionSize> rowSizes) {
  if (count == 0) {
    return 0;
  }
  BOLT_DCHECK(rowSizes.empty() || rowSizes.size() == count);
  if (buffer_ == nullptr || finished_) {
    resetWriteState();
    prepareWriteBuffer();
  }
  meta_ = RadixSortSpillSectionMeta::create(keyLayout, payloadLayout);
  BOLT_DCHECK_LE(pendingBlock_.totalBytes, pendingBodyCapacity_);
  auto availableBytes = pendingBodyCapacity_ - pendingBlock_.totalBytes;
  auto batchSize =
      prefixFromPresizedRows(meta_, count, availableBytes, rowSizes);
  if (batchSize.rowCount == 0 && !pendingBlock_.ranges.empty()) {
    flush();
    availableBytes = pendingBodyCapacity_;
    batchSize = prefixFromPresizedRows(meta_, count, availableBytes, rowSizes);
  }
  if (batchSize.rowCount == 0) {
    batchSize = prefixFromPresizedRows(
        meta_, 1, std::numeric_limits<uint64_t>::max(), rowSizes);
    ensureRecordFits(batchSize.totalBytes());
  }
  auto writtenRowSizes = rowSizes.empty()
      ? rowSizes
      : std::span<const RadixSortSpillSectionSize>(
            rowSizes.data(), batchSize.rowCount);
  appendSizedKeyRange(keyBase, batchSize, writtenRowSizes);
  flush();
  return static_cast<vector_size_t>(batchSize.rowCount);
}

std::vector<RadixSortSpillFile> RadixSortSpillWriter::finish() {
  if (finished_) {
    return {};
  }
  flush();
  auto spillFiles = spillWriter_->finish();
  auto cleanupFilesOnError = folly::makeGuard(
      [&spillFiles]() { cleanupSpillFilesNoThrow(spillFiles); });
  std::vector<RadixSortSpillFile> files;
  files.reserve(spillFiles.size());
  for (auto& file : spillFiles) {
    files.push_back(RadixSortSpillFile{
        file.id, file.path, file.size, file.rowCount, file.compressionKind});
  }
  cleanupFilesOnError.dismiss();
  finished_ = true;
  return files;
}

void RadixSortSpillWriter::ensureBuffer(uint64_t bytes) {
  const auto required = std::max<uint64_t>(bytes, kBlockHeaderSize + 1);
  if (buffer_ == nullptr || buffer_->capacity() < required) {
    buffer_ = AlignedBuffer::allocate<char>(required, pool_, 0);
  }
}

uint64_t RadixSortSpillWriter::bodyCapacity() const {
  const auto requested =
      writeBufferSize_ == 0 ? kDefaultWriteBufferSize : writeBufferSize_;
  BOLT_CHECK_GT(
      requested,
      kBlockHeaderSize,
      "Radix sort spill write buffer must fit block header");
  const auto codecLimit =
      maxUncompressedSpillBlockSize(compressionKind_) + kBlockHeaderSize;
  return std::min<uint64_t>(requested, codecLimit) - kBlockHeaderSize;
}

vector_size_t RadixSortSpillWriter::estimatedRowsPerBlock(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout) const {
  const auto payloadFixedWidth =
      payloadLayout == nullptr ? 0 : payloadLayout->rowWidth();
  const auto fixedRowBytes =
      static_cast<uint64_t>(keyLayout.width()) + payloadFixedWidth;
  BOLT_CHECK_GT(fixedRowBytes, 0);
  const auto rowCount =
      std::max<uint64_t>(1, bits::divRoundUp(bodyCapacity(), fixedRowBytes));
  return static_cast<vector_size_t>(
      std::min<uint64_t>(rowCount, std::numeric_limits<vector_size_t>::max()));
}

void RadixSortSpillWriter::ensureRecordFits(uint64_t recordSize) {
  BOLT_CHECK_LE(
      recordSize,
      maxUncompressedSpillBlockSize(compressionKind_),
      "Radix sort spill record exceeds block or codec limit");
  ensureBuffer(kBlockHeaderSize + recordSize);
  resetBuffer(kBlockHeaderSize + recordSize);
}

void RadixSortSpillWriter::clearPendingBlock() {
  pendingBlock_.ranges.clear();
  pendingBlock_.rowSizes.clear();
  pendingBlock_.totalBytes = 0;
}

void RadixSortSpillWriter::appendKeyRange(
    const char* keyBase,
    vector_size_t count) {
  const auto keyWidth = meta_.runtimeKeyRecordSize;
  vector_size_t processed = 0;
  while (processed < count) {
    const auto* key = keyBase + static_cast<uint64_t>(processed) * keyWidth;
    const auto remaining = count - processed;
    const auto sizedRows = RadixSortSpillSections::sizeForSerializeRows(
        meta_,
        key,
        remaining,
        std::numeric_limits<uint64_t>::max(),
        sizingSectionSizes_);
    BOLT_DCHECK_EQ(sizedRows.rowCount, remaining);
    vector_size_t consumed = 0;
    while (consumed < remaining) {
      const auto* currentKey = key + static_cast<uint64_t>(consumed) * keyWidth;
      BOLT_DCHECK_LE(pendingBlock_.totalBytes, pendingBodyCapacity_);
      const auto availableBytes =
          pendingBodyCapacity_ - pendingBlock_.totalBytes;
      const auto remainingInRange = remaining - consumed;
      auto rowSizes = sizingSectionSizes_.empty()
          ? std::span<const RadixSortSpillSectionSize>{}
          : std::span<const RadixSortSpillSectionSize>(
                sizingSectionSizes_.data() + consumed, remainingInRange);
      auto batchSize = prefixFromPresizedRows(
          meta_, remainingInRange, availableBytes, rowSizes);
      if (FOLLY_UNLIKELY(batchSize.rowCount == 0)) {
        if (!pendingBlock_.ranges.empty()) {
          flush();
          continue;
        }
        auto singleRowSize = prefixFromPresizedRows(
            meta_, 1, std::numeric_limits<uint64_t>::max(), rowSizes);
        ensureRecordFits(singleRowSize.totalBytes());
        batchSize = singleRowSize;
      }
      rowSizes = rowSizes.empty() ? rowSizes
                                  : std::span<const RadixSortSpillSectionSize>(
                                        rowSizes.data(), batchSize.rowCount);
      BOLT_DCHECK_GT(batchSize.rowCount, 0);
      appendSizedKeyRange(currentKey, batchSize, rowSizes);
      consumed += static_cast<vector_size_t>(batchSize.rowCount);
    }
    processed += consumed;
  }
}

void RadixSortSpillWriter::appendSizedKeyRange(
    const char* keyBase,
    const RadixSortSpillSectionBatchSize& batchSize,
    std::span<const RadixSortSpillSectionSize> rowSizes) {
  BOLT_DCHECK_GT(batchSize.rowCount, 0);
  BOLT_DCHECK(rowSizes.empty() || rowSizes.size() == batchSize.rowCount);
  BOLT_DCHECK_LE(batchSize.rowCount, std::numeric_limits<uint32_t>::max());
  BOLT_DCHECK_LE(
      pendingBlock_.totalBytes + batchSize.totalBytes(), pendingBodyCapacity_);
  pendingBlock_.ranges.push_back(PendingRange{
      keyBase,
      static_cast<uint32_t>(batchSize.rowCount),
      batchSize.keyRecordBytes,
      batchSize.keyHeapBytes,
      batchSize.payloadFixedBytes,
      batchSize.payloadHeapBytes,
      pendingBlock_.rowSizes.size()});
  pendingBlock_.rowSizes.insert(
      pendingBlock_.rowSizes.end(), rowSizes.begin(), rowSizes.end());
  pendingBlock_.totalBytes += batchSize.totalBytes();
}

void RadixSortSpillWriter::flush() {
  if (pendingBlock_.ranges.empty()) {
    return;
  }
  auto* const start = buffer_->asMutable<char>();
  uint64_t rowCount = 0;
  uint64_t keyRecordBytes = 0;
  uint64_t keyHeapBytes = 0;
  uint64_t payloadFixedBytes = 0;
  uint64_t payloadHeapBytes = 0;
  for (const auto& range : pendingBlock_.ranges) {
    rowCount += range.rowCount;
    keyRecordBytes += range.keyRecordBytes;
    keyHeapBytes += range.keyHeapBytes;
    payloadFixedBytes += range.payloadFixedBytes;
    payloadHeapBytes += range.payloadHeapBytes;
  }
  BOLT_DCHECK_EQ(keyRecordBytes, rowCount * meta_.runtimeKeyRecordSize);
  BOLT_DCHECK_EQ(payloadFixedBytes, rowCount * meta_.payloadFixedSize);
  BOLT_DCHECK_EQ(
      pendingBlock_.totalBytes,
      keyRecordBytes + keyHeapBytes + payloadFixedBytes + payloadHeapBytes);
  const auto uncompressedBytes = pendingBlock_.totalBytes;
  BOLT_DCHECK_LE(
      uncompressedBytes, maxUncompressedSpillBlockSize(compressionKind_));
  const auto uncompressedSize = static_cast<int32_t>(uncompressedBytes);

  auto* keyRecords = start + kBlockHeaderSize;
  auto* keyHeap = keyRecords + keyRecordBytes;
  auto* payloadFixed = keyHeap + keyHeapBytes;
  auto* payloadHeap = payloadFixed + payloadFixedBytes;
  auto* keyRecordsCursor = keyRecords;
  auto* keyHeapCursor = keyHeap;
  auto* payloadFixedCursor = payloadFixed;
  auto* payloadHeapCursor = payloadHeap;
  for (const auto& range : pendingBlock_.ranges) {
    const auto* rowSizes = pendingBlock_.rowSizes.empty()
        ? nullptr
        : pendingBlock_.rowSizes.data() + range.rowSizeOffset;
    RadixSortSpillSections::copyRowsToSections(
        meta_,
        range.keyBase,
        rowSizes,
        range.rowCount,
        range.keyHeapBytes,
        range.payloadHeapBytes,
        keyRecordsCursor,
        keyHeapCursor,
        payloadFixedCursor,
        payloadHeapCursor);
    keyRecordsCursor += range.keyRecordBytes;
    payloadFixedCursor += range.payloadFixedBytes;
  }
  BOLT_DCHECK_EQ(keyRecordsCursor, keyRecords + keyRecordBytes);
  BOLT_DCHECK_EQ(keyHeapCursor, keyHeap + keyHeapBytes);
  BOLT_DCHECK_EQ(payloadFixedCursor, payloadFixed + payloadFixedBytes);
  BOLT_DCHECK_EQ(payloadHeapCursor, payloadHeap + payloadHeapBytes);

  inputBytes_ += uncompressedBytes;

  auto* header = buffer_->asMutable<RadixSortSpillBlockHeader>();
  *header = RadixSortSpillBlockHeader{
      uncompressedSize,
      uncompressedSize,
      static_cast<uint32_t>(rowCount),
      0,
      keyRecordBytes,
      keyHeapBytes,
      payloadFixedBytes,
      payloadHeapBytes};
  spillWriter_->writeEncodedBlock(
      start, kBlockHeaderSize, uncompressedSize, rowCount);
  resetBuffer(normalBufferSize_);
}

RadixSortSpillReader::RadixSortSpillReader(
    RadixSortSpillFile file,
    RadixSortSpillSectionMeta meta,
    const PayloadRowLayout* payloadLayout,
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    RadixSortSpillReadBufferCache* bufferCache)
    : SpillReadFileInput(file.path, pool, spillUringEnabled),
      file_(std::move(file)),
      meta_(std::move(meta)),
      payloadLayout_(payloadLayout),
      maxReusableSerializedBufferSize_(
          pool->preferredSize(
              kDefaultWriteBufferSize + AlignedBuffer::kPaddedSize) -
          AlignedBuffer::kPaddedSize),
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
      maxUncompressedSpillBlockSize(file_.compressionKind),
      "Radix sort spill block exceeds codec limit");
  if (!isSpillCompressionEnabled(file_.compressionKind)) {
    BOLT_CHECK_EQ(
        uncompressedSize,
        storedSize,
        "Invalid uncompressed radix sort spill block size");
  } else {
    BOLT_CHECK_LE(
        storedSize,
        spillCompressionBound(file_.compressionKind, uncompressedSize),
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
  readSpillBlockBody(
      *input_,
      file_.compressionKind,
      uncompressedSize,
      storedSize,
      block,
      compressedBuffer_,
      pool_,
      spillDecompressTimeUs_);

  keys.reserve(header.rowCount);
  char* keyRecords = block;
  char* keyHeap = keyRecords + header.keyRecordBytes;
  char* const keyHeapEnd = keyHeap + header.keyHeapBytes;
  char* payloadFixed = keyHeapEnd;
  char* payloadHeap = payloadFixed + header.payloadFixedBytes;
  char* const payloadHeapEnd = payloadHeap + header.payloadHeapBytes;
  const auto validRows = RadixSortSpillSections::restorePointersInSectionRows(
      meta_,
      keyRecords,
      header.rowCount,
      keyHeap,
      keyHeapEnd,
      payloadFixed,
      payloadHeap,
      payloadHeapEnd,
      keys);
  BOLT_CHECK(validRows, "Invalid radix sort spill section block");
  BOLT_CHECK_EQ(keyHeap, keyHeapEnd);
  BOLT_CHECK_EQ(payloadHeap, payloadHeapEnd);
  return true;
}

uint64_t RadixSortSpillReader::spillReadIOTimeUs() const {
  return inputSpillReadIOTimeUs();
}

void RadixSortSpillReader::close() {
  closeInput();
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

RadixSortConcatFilesSpillMergeStream::RadixSortConcatFilesSpillMergeStream(
    std::vector<RadixSortSpillFile> files,
    RadixSortSpillSectionMeta meta,
    const PayloadRowLayout* payloadLayout,
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    RadixSortSpillReadBufferCache* bufferCache)
    : RadixSortMergeStream(meta.keyLayout),
      meta_(std::move(meta)),
      payloadLayout_(payloadLayout),
      pool_(pool),
      spillUringEnabled_(spillUringEnabled),
      bufferCache_(bufferCache),
      files_(std::move(files)) {
  BOLT_CHECK(
      !files_.empty(), "Radix sort spill run must have at least one file");
  try {
    loadNextFile();
  } catch (...) {
    closeNoThrow();
    throw;
  }
}

RadixSortConcatFilesSpillMergeStream::~RadixSortConcatFilesSpillMergeStream() {
  closeNoThrow();
}

bool RadixSortConcatFilesSpillMergeStream::hasData() const {
  return key_ != nullptr;
}

void RadixSortConcatFilesSpillMergeStream::pop() {
  BOLT_DCHECK_NOT_NULL(current_);
  current_->pop();
  if (current_->hasData()) {
    updateCurrent();
    return;
  }
  retainCurrentFileStream();
  loadNextFile();
}

uint64_t RadixSortConcatFilesSpillMergeStream::getSpillReadTime() const {
  uint64_t time = completedSpillReadTimeUs_;
  for (const auto& stream : retainedFileStreams_) {
    time += stream->getSpillReadTime();
  }
  if (current_ != nullptr) {
    time += current_->getSpillReadTime();
  }
  return time;
}

uint64_t RadixSortConcatFilesSpillMergeStream::getSpillDecompressTime() const {
  uint64_t time = completedSpillDecompressTimeUs_;
  for (const auto& stream : retainedFileStreams_) {
    time += stream->getSpillDecompressTime();
  }
  if (current_ != nullptr) {
    time += current_->getSpillDecompressTime();
  }
  return time;
}

uint64_t RadixSortConcatFilesSpillMergeStream::getSpillReadIOTime() const {
  uint64_t time = completedSpillReadIOTimeUs_;
  for (const auto& stream : retainedFileStreams_) {
    time += stream->getSpillReadIOTime();
  }
  if (current_ != nullptr) {
    time += current_->getSpillReadIOTime();
  }
  return time;
}

void RadixSortConcatFilesSpillMergeStream::releaseRetainedBuffers() {
  for (auto& stream : retainedFileStreams_) {
    completedSpillReadTimeUs_ += stream->getSpillReadTime();
    completedSpillDecompressTimeUs_ += stream->getSpillDecompressTime();
    completedSpillReadIOTimeUs_ += stream->getSpillReadIOTime();
    stream->releaseRetainedBuffers();
  }
  retainedFileStreams_.clear();
  if (current_ != nullptr) {
    current_->releaseRetainedBuffers();
  }
}

void RadixSortConcatFilesSpillMergeStream::loadNextFile() {
  while (nextFileIndex_ < files_.size()) {
    auto current = std::make_unique<RadixSortSpillFileMergeStream>(
        files_[nextFileIndex_],
        meta_,
        payloadLayout_,
        pool_,
        spillUringEnabled_,
        bufferCache_);
    files_[nextFileIndex_].path.clear();
    ++nextFileIndex_;
    current_ = std::move(current);
    if (current_->hasData()) {
      updateCurrent();
      return;
    }
    retainCurrentFileStream();
  }
  key_ = nullptr;
  payload_ = nullptr;
}

void RadixSortConcatFilesSpillMergeStream::updateCurrent() {
  key_ = current_->key();
  payload_ = current_->payload();
}

void RadixSortConcatFilesSpillMergeStream::retainCurrentFileStream() {
  if (current_ != nullptr) {
    retainedFileStreams_.push_back(std::move(current_));
  }
}

void RadixSortConcatFilesSpillMergeStream::
    cleanupUnreadFilesNoThrow() noexcept {
  for (auto i = nextFileIndex_; i < files_.size(); ++i) {
    removeSpillFileNoThrow(files_[i].path);
    files_[i].path.clear();
  }
}

void RadixSortConcatFilesSpillMergeStream::closeNoThrow() noexcept {
  try {
    releaseRetainedBuffers();
    if (current_ != nullptr) {
      completedSpillReadTimeUs_ += current_->getSpillReadTime();
      completedSpillDecompressTimeUs_ += current_->getSpillDecompressTime();
      completedSpillReadIOTimeUs_ += current_->getSpillReadIOTime();
      current_.reset();
    }
    cleanupUnreadFilesNoThrow();
    key_ = nullptr;
    payload_ = nullptr;
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
  if (keyLayout_.hasPayload()) {
    if (streams_.size() == 1) {
      return collectSingleStreamRows<true>(count, keys, payloads);
    }
    if (streams_.size() == 2) {
      return collectTwoWayRows<true>(count, keys, payloads);
    }
    return collectLoserTreeRows<true>(count, keys, payloads);
  }
  if (streams_.size() == 1) {
    return collectSingleStreamRows<false>(count, keys, payloads);
  }
  if (streams_.size() == 2) {
    return collectTwoWayRows<false>(count, keys, payloads);
  }
  return collectLoserTreeRows<false>(count, keys, payloads);
}

template <bool HasPayload>
vector_size_t RadixSortMerger::collectSingleStreamRows(
    vector_size_t count,
    const char** keys,
    char** payloads) {
  auto* stream = streams_[0].get();
  vector_size_t row = 0;
  for (; row < count; ++row) {
    keys[row] = stream->key();
    if constexpr (HasPayload) {
      payloads[row] = stream->payload();
    }
    stream->pop();
  }
  return row;
}

template <bool HasPayload>
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
      if constexpr (HasPayload) {
        payloads[row] = left->payload();
      }
      left->pop();
      leftKey = left->key();
    } else {
      keys[row] = rightKey;
      if constexpr (HasPayload) {
        payloads[row] = right->payload();
      }
      right->pop();
      rightKey = right->key();
    }
  }
  return row;
}

template <bool HasPayload>
vector_size_t RadixSortMerger::collectLoserTreeRows(
    vector_size_t count,
    const char** keys,
    char** payloads) {
  vector_size_t row = 0;
  for (; row < count; ++row) {
    const auto index = nextLoserTreeStream();
    auto* stream = streams_[index].get();
    keys[row] = stream->key();
    if constexpr (HasPayload) {
      payloads[row] = stream->payload();
    }
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
