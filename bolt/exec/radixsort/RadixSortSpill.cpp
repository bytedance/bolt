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

#include <algorithm>
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
static_assert(sizeof(RadixSortSpillBlockHeader) == 48);

template <RadixSortKeyLayoutKind KIND>
int32_t comparePhysicalKeys(
    const char* left,
    const char* right,
    uint32_t heapKeyOffset) {
  return RadixSortKeyOps<KIND>::compare(left, right, heapKeyOffset);
}

template <RadixSortKeyLayoutKind KIND>
int32_t compareVariablePhysicalKeys(
    const char* left,
    const char* right,
    const RadixSortVariableMergeStream& leftStream,
    const RadixSortVariableMergeStream& rightStream,
    uint32_t variableSuffixSkip) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(Traits::kVariable);
  const auto prefixResult = compareInlineKeyPrefix<Traits, true>(left, right);
  if (prefixResult != 0) {
    return prefixResult;
  }

  const auto& leftBytes = leftStream.encodedSuffixInline().bytes;
  const auto& rightBytes = rightStream.encodedSuffixInline().bytes;
  BOLT_DCHECK(!leftBytes.empty());
  BOLT_DCHECK(!rightBytes.empty());
  const auto leftHeapBytes = leftBytes.size();
  const auto rightHeapBytes = rightBytes.size();
  if (leftHeapBytes <= variableSuffixSkip ||
      rightHeapBytes <= variableSuffixSkip) {
    return (leftHeapBytes > rightHeapBytes) - (leftHeapBytes < rightHeapBytes);
  }

  const auto result = std::memcmp(
      leftBytes.data() + variableSuffixSkip,
      rightBytes.data() + variableSuffixSkip,
      std::min(leftHeapBytes, rightHeapBytes) - variableSuffixSkip);
  if (result != 0) {
    return (result > 0) - (result < 0);
  }
  return (leftHeapBytes > rightHeapBytes) - (leftHeapBytes < rightHeapBytes);
}

RadixSortMerger::CompareFixedKeys compareFixedKeysForLayout(
    RadixSortKeyLayoutKind kind) {
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
      break;
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
      break;
    case RadixSortKeyLayoutKind::kInvalid:
      break;
  }
  BOLT_FAIL("Invalid radix sort merge key layout");
}

RadixSortMerger::CompareVariableKeys compareVariableKeysForLayout(
    RadixSortKeyLayoutKind kind) {
  switch (kind) {
    case RadixSortKeyLayoutKind::kKeyOnlyVariable32:
      return compareVariablePhysicalKeys<
          RadixSortKeyLayoutKind::kKeyOnlyVariable32>;
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32:
      return compareVariablePhysicalKeys<
          RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>;
    default:
      BOLT_FAIL("Invalid variable radix sort merge key layout");
  }
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

template <typename Files>
void cleanupSpillFilesNoThrow(const Files& files) noexcept {
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
      writeBufferSize_(kRadixSortSpillBufferSize),
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

void RadixSortSpillWriter::prepareWriteBuffer() {
  const auto requested = writeBufferSize_;
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
    const PayloadRowLayout* payloadLayout,
    uint64_t beginRow) {
  BOLT_CHECK_LE(beginRow, storage.size());
  prepareWriteBuffer();

  const auto& keyLayout = storage.layout();
  meta_ = RadixSortSpillSectionMeta::create(keyLayout, payloadLayout);
  if (beginRow == 0) {
    for (const auto& block : storage.keyBlocks()) {
      appendKeyRange(block.base, block.count);
    }
    return finish();
  }

  uint64_t row = beginRow;
  while (row < storage.size()) {
    const auto range =
        storage.keyRangeAt(row, std::numeric_limits<vector_size_t>::max());
    BOLT_DCHECK_GT(range.count, 0);
    appendKeyRange(range.data, range.count);
    row += range.count;
  }
  return finish();
}

std::vector<RadixSortSpillFile> RadixSortSpillWriter::finish() {
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

void RadixSortSpillWriter::ensureRecordFits(uint64_t recordSize) {
  BOLT_CHECK_LE(
      recordSize,
      maxUncompressedSpillBlockSize(compressionKind_),
      "Radix sort spill record exceeds block or codec limit");
  ensureBuffer(kBlockHeaderSize + recordSize);
  BOLT_DCHECK(pendingBlock_.ranges.empty());
  BOLT_DCHECK_EQ(pendingBlock_.totalBytes(meta_.fixedWireBytesPerRow()), 0);
  pendingBodyCapacity_ = recordSize;
}

void RadixSortSpillWriter::clearPendingBlock() {
  pendingBlock_.ranges.clear();
  pendingBlock_.rowCount = 0;
  pendingBlock_.keyHeapBytes = 0;
  pendingBlock_.payloadHeapBytes = 0;
}

void RadixSortSpillWriter::appendKeyRange(
    const char* keyBase,
    vector_size_t count) {
  const auto keyWidth = meta_.runtimeKeyRecordSize;
  vector_size_t processed = 0;
  while (processed < count) {
    const auto* key = keyBase + static_cast<uint64_t>(processed) * keyWidth;
    const auto pendingBytes =
        pendingBlock_.totalBytes(meta_.fixedWireBytesPerRow());
    BOLT_DCHECK_LE(pendingBytes, pendingBodyCapacity_);
    auto batchSize = RadixSortSpillSections::sizeForSerializeRows(
        meta_, key, count - processed, pendingBodyCapacity_ - pendingBytes);
    if (FOLLY_UNLIKELY(batchSize.rowCount == 0)) {
      if (!pendingBlock_.ranges.empty()) {
        flush();
        continue;
      }
      batchSize = RadixSortSpillSections::sizeForSerializeRows(
          meta_, key, 1, std::numeric_limits<uint64_t>::max());
      BOLT_CHECK_EQ(
          batchSize.rowCount,
          1,
          "Radix sort spill failed to size a single row");
      ensureRecordFits(batchSize.totalBytes(meta_.fixedWireBytesPerRow()));
    }
    BOLT_DCHECK_GT(batchSize.rowCount, 0);
    appendSizedKeyRange(key, batchSize);
    processed += static_cast<vector_size_t>(batchSize.rowCount);
  }
}

void RadixSortSpillWriter::appendSizedKeyRange(
    const char* keyBase,
    const RadixSortSpillSectionBatchSize& batchSize) {
  BOLT_DCHECK_GT(batchSize.rowCount, 0);
  BOLT_DCHECK_LE(batchSize.rowCount, std::numeric_limits<uint32_t>::max());
  BOLT_DCHECK_LE(
      pendingBlock_.totalBytes(meta_.fixedWireBytesPerRow()) +
          batchSize.totalBytes(meta_.fixedWireBytesPerRow()),
      pendingBodyCapacity_);
  pendingBlock_.ranges.push_back(PendingRange{
      keyBase,
      static_cast<uint32_t>(batchSize.rowCount),
      batchSize.keyHeapBytes,
      batchSize.payloadHeapBytes});
  pendingBlock_.rowCount += batchSize.rowCount;
  pendingBlock_.keyHeapBytes += batchSize.keyHeapBytes;
  pendingBlock_.payloadHeapBytes += batchSize.payloadHeapBytes;
}

void RadixSortSpillWriter::flush() {
  if (pendingBlock_.ranges.empty()) {
    return;
  }
  auto* const start = buffer_->asMutable<char>();
  const auto rowCount = pendingBlock_.rowCount;
  const auto keyRecordBytes = rowCount * meta_.wireKeyRecordSize;
  const auto keyHeapBytes = pendingBlock_.keyHeapBytes;
  const auto payloadFixedBytes = rowCount * meta_.payloadFixedSize;
  const auto payloadHeapBytes = pendingBlock_.payloadHeapBytes;
  const auto uncompressedBytes =
      pendingBlock_.totalBytes(meta_.fixedWireBytesPerRow());
  BOLT_DCHECK_EQ(
      uncompressedBytes,
      keyRecordBytes + keyHeapBytes + payloadFixedBytes + payloadHeapBytes);
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
    RadixSortSpillSections::copyRowsToSections(
        meta_,
        range.keyBase,
        range.rowCount,
        range.keyHeapBytes,
        range.payloadHeapBytes,
        keyRecordsCursor,
        keyHeapCursor,
        payloadFixedCursor,
        payloadHeapCursor);
    keyRecordsCursor +=
        static_cast<uint64_t>(range.rowCount) * meta_.wireKeyRecordSize;
    payloadFixedCursor +=
        static_cast<uint64_t>(range.rowCount) * meta_.payloadFixedSize;
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
      kCurrentRadixSortSpillFormat,
      keyRecordBytes,
      keyHeapBytes,
      payloadFixedBytes,
      payloadHeapBytes};
  spillWriter_->writeEncodedBlock(
      start, kBlockHeaderSize, uncompressedSize, rowCount);
  resetBuffer(normalBufferSize_);
}

RadixSortSpillReader::RadixSortSpillReader(
    const RadixSortSpillFile& file,
    const RadixSortSpillSectionMeta& meta,
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    RadixSortSpillReadBufferCache* bufferCache)
    : SpillReadFileInput(file.path, pool, spillUringEnabled),
      compressionKind_(file.compressionKind),
      meta_(meta),
      bufferCache_(bufferCache) {}

void RadixSortSpillReader::acquireSerializedBuffer(uint64_t size) {
  if (serializedBuffer_ != nullptr && serializedBuffer_->capacity() >= size) {
    serializedBuffer_->setSize(size);
    return;
  }
  if (bufferCache_ != nullptr) {
    auto& reusable = bufferCache_->serializedBuffer;
    if (reusable != nullptr && reusable->isMutable() &&
        reusable->capacity() >= size) {
      serializedBuffer_ = std::move(reusable);
      serializedBuffer_->setSize(size);
      return;
    }
    reusable.reset();
  }
  serializedBuffer_ = AlignedBuffer::allocate<char>(size, pool_);
}

void RadixSortSpillReader::recycleSerializedBuffer() {
  if (serializedBuffer_ == nullptr) {
    return;
  }
  if (bufferCache_ == nullptr) {
    serializedBuffer_.reset();
    return;
  }
  if (!serializedBuffer_->isMutable() ||
      serializedBuffer_->size() > kRadixSortSpillBufferSize) {
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

std::optional<RadixSortSpillBlockView> RadixSortSpillReader::nextBatch() {
  if (inputAtEnd()) {
    finishReading();
    return std::nullopt;
  }

  MicrosecondTimer readTimer(&spillReadTimeUs_);
  RadixSortSpillBlockHeader header;
  input_->readBytes(reinterpret_cast<char*>(&header), sizeof(header));
  BOLT_CHECK_EQ(
      header.reserved,
      kCurrentRadixSortSpillFormat,
      "Unsupported radix sort spill format");
  const auto uncompressedSize = header.uncompressedSize;
  const auto storedSize = header.storedSize;
  BOLT_CHECK_GT(uncompressedSize, 0);
  BOLT_CHECK_GT(storedSize, 0);
  BOLT_CHECK_GT(header.rowCount, 0);
  BOLT_CHECK_LE(
      uncompressedSize,
      maxUncompressedSpillBlockSize(compressionKind_),
      "Radix sort spill block exceeds codec limit");
  if (!isSpillCompressionEnabled(compressionKind_)) {
    BOLT_CHECK_EQ(
        uncompressedSize,
        storedSize,
        "Invalid uncompressed radix sort spill block size");
  } else {
    BOLT_CHECK_LE(
        storedSize,
        spillCompressionBound(compressionKind_, uncompressedSize),
        "Invalid compressed radix sort spill block size");
  }
  const auto keyRecordBytes =
      checkedSectionSize(header.rowCount, meta_.wireKeyRecordSize);
  const auto payloadFixedBytes =
      checkedSectionSize(header.rowCount, meta_.payloadFixedSize);
  BOLT_CHECK_EQ(header.keyRecordBytes, keyRecordBytes);
  BOLT_CHECK_EQ(header.payloadFixedBytes, payloadFixedBytes);
  if (meta_.hasKeyHeap) {
    BOLT_CHECK_GT(
        header.keyHeapBytes, 0, "Variable radix sort key heap is empty");
  } else {
    BOLT_CHECK_EQ(
        header.keyHeapBytes, 0, "Fixed radix sort key has a key heap");
  }
  if (!meta_.hasPayload) {
    BOLT_CHECK_EQ(
        header.payloadFixedBytes,
        0,
        "Key-only radix sort spill has a payload fixed section");
    BOLT_CHECK_EQ(
        header.payloadHeapBytes,
        0,
        "Key-only radix sort spill has a payload heap section");
  } else if (!meta_.hasVariablePayload()) {
    BOLT_CHECK_EQ(
        header.payloadHeapBytes,
        0,
        "Fixed radix sort payload has a payload heap section");
  }
  const auto expectedUncompressedSize = checkedBlockBodySize(
      header.keyRecordBytes,
      header.keyHeapBytes,
      header.payloadFixedBytes,
      header.payloadHeapBytes);
  BOLT_CHECK_EQ(
      static_cast<uint64_t>(uncompressedSize), expectedUncompressedSize);
  if (serializedBuffer_ != nullptr &&
      (serializedBuffer_->size() > kRadixSortSpillBufferSize ||
       serializedBuffer_->capacity() < uncompressedSize)) {
    // The current block has already been materialized. Drop it before growing
    // to avoid reserving the full new allocation while the old one is live.
    serializedBuffer_.reset();
  }
  acquireSerializedBuffer(uncompressedSize);
  auto* block = serializedBuffer_->asMutable<char>();
  readSpillBlockBody(
      *input_,
      compressionKind_,
      uncompressedSize,
      storedSize,
      block,
      compressedBuffer_,
      pool_,
      spillDecompressTimeUs_);

  char* keyRecords = block;
  char* keyHeap = keyRecords + header.keyRecordBytes;
  char* payloadFixed = keyHeap + header.keyHeapBytes;
  char* payloadHeap = payloadFixed + header.payloadFixedBytes;
  char* const payloadHeapEnd = payloadHeap + header.payloadHeapBytes;
  if (meta_.hasVariablePayload()) {
    const auto validRows =
        RadixSortSpillSections::restorePayloadPointersInSectionRows(
            meta_, header.rowCount, payloadFixed, payloadHeap, payloadHeapEnd);
    BOLT_CHECK(validRows, "Invalid radix sort spill payload heap");
  }
  return RadixSortSpillBlockView{keyRecords, keyHeap, payloadFixed};
}

uint64_t RadixSortSpillReader::spillReadIOTimeUs() const {
  return inputSpillReadIOTimeUs();
}

void RadixSortSpillReader::close() {
  finishReading();
}

void RadixSortSpillReader::finishReading() {
  recycleSerializedBuffer();
  compressedBuffer_.reset();
  closeInput();
}

RadixSortMemoryRunMergeStream::RadixSortMemoryRunMergeStream(
    const RadixSortRunStorage& storage)
    : RadixSortMergeStream(storage.layout()), storage_(storage) {
  BOLT_CHECK(
      !storage.layout().isVariable(),
      "Variable radix sort memory runs require a variable merge stream");
  loadCurrent();
}

bool RadixSortMemoryRunMergeStream::hasData() const {
  return key_ != nullptr;
}

bool RadixSortMemoryRunMergeStream::tryAdvance() {
  ++index_;
  loadCurrent();
  return true;
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
  key_ = range_.data + static_cast<uint64_t>(rangeIndex_++) * recordStride_;
  payload_ = hasPayload_ ? loadCompactPointer(key_ + payloadOffset_) : nullptr;
}

const EncodedKeyView& RadixSortVariableMergeStream::encodedSuffix() const {
  return encodedSuffixInline();
}

RadixSortVariableMemoryRunMergeStream::RadixSortVariableMemoryRunMergeStream(
    const RadixSortRunStorage& storage)
    : RadixSortVariableMergeStream(storage.layout(), storage.layout().width()),
      storage_(storage) {
  BOLT_CHECK(
      storage.layout().isVariable(),
      "Fixed radix sort memory runs require a fixed merge stream");
  loadCurrent();
}

bool RadixSortVariableMemoryRunMergeStream::hasData() const {
  return key_ != nullptr;
}

bool RadixSortVariableMemoryRunMergeStream::tryAdvance() {
  ++index_;
  loadCurrent();
  return true;
}

void RadixSortVariableMemoryRunMergeStream::loadCurrent() {
  if (index_ >= storage_.size()) {
    key_ = nullptr;
    payload_ = nullptr;
    encodedSuffix_ = {};
    return;
  }
  if (range_.data == nullptr || rangeIndex_ == range_.count) {
    range_ = storage_.keyRangeAt(index_, storage_.keysPerBlock());
    rangeIndex_ = 0;
  }
  key_ = range_.data + static_cast<uint64_t>(rangeIndex_++) * recordStride_;
  payload_ = hasPayload_ ? loadCompactPointer(key_ + payloadOffset_) : nullptr;
  encodedSuffix_ = {};
}

namespace {

class RadixSortSpillLogicalRunCore {
 public:
  RadixSortSpillLogicalRunCore(
      RadixSortSpillRun run,
      RadixSortSpillSectionMeta meta,
      memory::MemoryPool* pool,
      bool spillUringEnabled,
      RadixSortSpillReadBufferCache* bufferCache)
      : files_(std::move(run.files)),
        meta_(std::move(meta)),
        pool_(pool),
        spillUringEnabled_(spillUringEnabled),
        bufferCache_(bufferCache) {
    BOLT_CHECK(
        !files_.empty(), "Radix sort spill run must have at least one file");
  }

  ~RadixSortSpillLogicalRunCore() {
    closeNoThrow();
  }

  std::optional<RadixSortSpillBlockView> nextBatch() {
    try {
      for (;;) {
        if (reader_ != nullptr) {
          auto block = reader_->nextBatch();
          if (block.has_value()) {
            return block;
          }
          retireCurrentFile();
        }
        if (nextFileIndex_ == files_.size()) {
          return std::nullopt;
        }
        openNextFile();
      }
    } catch (...) {
      closeNoThrow();
      throw;
    }
  }

  uint64_t spillReadTimeUs() const {
    return completedSpillReadTimeUs_ +
        (reader_ == nullptr ? 0 : reader_->spillReadTimeUs());
  }

  uint64_t spillDecompressTimeUs() const {
    return completedSpillDecompressTimeUs_ +
        (reader_ == nullptr ? 0 : reader_->spillDecompressTimeUs());
  }

  uint64_t spillReadIOTimeUs() const {
    return completedSpillReadIOTimeUs_ +
        (reader_ == nullptr ? 0 : reader_->spillReadIOTimeUs());
  }

  const RadixSortSpillSectionMeta& meta() const {
    return meta_;
  }

 private:
  void openNextFile() {
    const auto fileIndex = nextFileIndex_++;
    currentFileIndex_ = fileIndex;
    reader_ = std::make_unique<RadixSortSpillReader>(
        files_[fileIndex], meta_, pool_, spillUringEnabled_, bufferCache_);
  }

  void retireCurrentFile() {
    if (reader_ != nullptr) {
      completedSpillReadTimeUs_ += reader_->spillReadTimeUs();
      completedSpillDecompressTimeUs_ += reader_->spillDecompressTimeUs();
      completedSpillReadIOTimeUs_ += reader_->spillReadIOTimeUs();
      reader_->close();
      reader_.reset();
    }
    if (currentFileIndex_.has_value()) {
      auto& path = files_[*currentFileIndex_].path;
      removeSpillFileNoThrow(path);
      path.clear();
      currentFileIndex_.reset();
    }
  }

  void closeNoThrow() noexcept {
    if (reader_ != nullptr) {
      completedSpillReadTimeUs_ += reader_->spillReadTimeUs();
      completedSpillDecompressTimeUs_ += reader_->spillDecompressTimeUs();
      completedSpillReadIOTimeUs_ += reader_->spillReadIOTimeUs();
      try {
        reader_->close();
      } catch (const std::exception& error) {
        LOG(WARNING) << "Failed to close radix sort spill input: "
                     << error.what();
      } catch (...) {
        LOG(WARNING) << "Failed to close radix sort spill input";
      }
      reader_.reset();
    }
    if (currentFileIndex_.has_value()) {
      auto& path = files_[*currentFileIndex_].path;
      removeSpillFileNoThrow(path);
      path.clear();
      currentFileIndex_.reset();
    }
    for (auto i = nextFileIndex_; i < files_.size(); ++i) {
      removeSpillFileNoThrow(files_[i].path);
      files_[i].path.clear();
    }
    nextFileIndex_ = files_.size();
  }

  std::vector<RadixSortSpillFile> files_;
  RadixSortSpillSectionMeta meta_;
  memory::MemoryPool* const pool_;
  const bool spillUringEnabled_;
  RadixSortSpillReadBufferCache* const bufferCache_;
  size_t nextFileIndex_{0};
  std::optional<size_t> currentFileIndex_;
  std::unique_ptr<RadixSortSpillReader> reader_;
  uint64_t completedSpillReadTimeUs_{0};
  uint64_t completedSpillDecompressTimeUs_{0};
  uint64_t completedSpillReadIOTimeUs_{0};
};

template <bool HasPayload>
struct SpillPayloadCursorState {
  explicit SpillPayloadCursorState(uint64_t) {}
};

template <>
struct SpillPayloadCursorState<true> {
  explicit SpillPayloadCursorState(uint64_t stride)
      : payloadFixedStride(stride) {}

  const uint64_t payloadFixedStride;
};

template <bool HasPayload>
class RadixSortFixedSpillMergeStream final
    : public RadixSortMergeStream,
      private SpillPayloadCursorState<HasPayload> {
 public:
  RadixSortFixedSpillMergeStream(
      RadixSortSpillRun run,
      RadixSortSpillSectionMeta meta,
      memory::MemoryPool* pool,
      bool spillUringEnabled,
      RadixSortSpillReadBufferCache* bufferCache)
      : RadixSortMergeStream(meta.keyLayout, meta.wireKeyRecordSize),
        SpillPayloadCursorState<HasPayload>(meta.payloadFixedSize),
        core_(
            std::move(run),
            std::move(meta),
            pool,
            spillUringEnabled,
            bufferCache) {
    BOLT_CHECK(!core_.meta().keyLayout.isVariable());
    BOLT_CHECK_EQ(core_.meta().hasPayload, HasPayload);
    loadBatch();
  }

  bool hasData() const override {
    return key_ != nullptr;
  }

  bool tryAdvance() override {
    BOLT_DCHECK_NOT_NULL(key_);
    const auto* nextKey = key_ + recordStride_;
    BOLT_DCHECK_LE(nextKey, keyRecordsEnd_);
    if (nextKey == keyRecordsEnd_) {
      return false;
    }
    if constexpr (HasPayload) {
      payload_ += this->payloadFixedStride;
    }
    key_ = nextKey;
    return true;
  }

  void advanceAfterFlush() override {
    BOLT_DCHECK_NOT_NULL(key_);
    BOLT_DCHECK_EQ(key_ + recordStride_, keyRecordsEnd_);
    loadBatch();
  }

  uint64_t getSpillReadTime() const override {
    return core_.spillReadTimeUs();
  }

  uint64_t getSpillDecompressTime() const override {
    return core_.spillDecompressTimeUs();
  }

  uint64_t getSpillReadIOTime() const override {
    return core_.spillReadIOTimeUs();
  }

 private:
  void clearCursor() noexcept {
    key_ = nullptr;
    payload_ = nullptr;
    keyRecordsEnd_ = nullptr;
  }

  void loadBatch() {
    try {
      auto block = core_.nextBatch();
      if (!block.has_value()) {
        clearCursor();
        return;
      }
      BOLT_DCHECK_LT(block->keyRecordsBegin, block->keyHeapBegin);
      keyRecordsEnd_ = block->keyHeapBegin;
      if constexpr (HasPayload) {
        payload_ = block->payloadFixedBegin;
      }
      key_ = block->keyRecordsBegin;
    } catch (...) {
      clearCursor();
      throw;
    }
  }

  RadixSortSpillLogicalRunCore core_;
  const char* keyRecordsEnd_{nullptr};
};

template <bool HasPayload>
class RadixSortVariableSpillMergeStream final
    : public RadixSortVariableMergeStream,
      private SpillPayloadCursorState<HasPayload> {
 private:
  using Kind = std::integral_constant<
      RadixSortKeyLayoutKind,
      HasPayload ? RadixSortKeyLayoutKind::kKeyWithPayloadVariable32
                 : RadixSortKeyLayoutKind::kKeyOnlyVariable32>;
  using Traits = RadixSortKeyTraits<Kind::value>;

 public:
  RadixSortVariableSpillMergeStream(
      RadixSortSpillRun run,
      RadixSortSpillSectionMeta meta,
      memory::MemoryPool* pool,
      bool spillUringEnabled,
      RadixSortSpillReadBufferCache* bufferCache)
      : RadixSortVariableMergeStream(meta.keyLayout, meta.wireKeyRecordSize),
        SpillPayloadCursorState<HasPayload>(meta.payloadFixedSize),
        core_(
            std::move(run),
            std::move(meta),
            pool,
            spillUringEnabled,
            bufferCache) {
    BOLT_CHECK(
        core_.meta().keyLayout.kind() == Kind::value,
        "Radix sort variable spill stream layout mismatch");
    BOLT_CHECK_EQ(core_.meta().hasPayload, HasPayload);
    loadBatch();
  }

  bool hasData() const override {
    return key_ != nullptr;
  }

  bool tryAdvance() override {
    BOLT_DCHECK_NOT_NULL(key_);
    const auto* nextKey = key_ + recordStride_;
    BOLT_DCHECK_LE(nextKey, keyRecordsEnd_);
    if (nextKey == keyRecordsEnd_) {
      return false;
    }
    const auto* nextHeap =
        encodedSuffix_.bytes.data() + encodedSuffix_.bytes.size();
    char* nextPayload = nullptr;
    if constexpr (HasPayload) {
      nextPayload = payload_ + this->payloadFixedStride;
    }
    prepareCurrent(nextKey, nextHeap, nextPayload);
    return true;
  }

  void advanceAfterFlush() override {
    BOLT_DCHECK_NOT_NULL(key_);
    BOLT_DCHECK_EQ(key_ + recordStride_, keyRecordsEnd_);
    BOLT_CHECK_EQ(
        encodedSuffix_.bytes.data() + encodedSuffix_.bytes.size(),
        keyHeapEnd_,
        "Radix sort spill key heap is not consumed exactly");
    loadBatch();
  }

  uint64_t getSpillReadTime() const override {
    return core_.spillReadTimeUs();
  }

  uint64_t getSpillDecompressTime() const override {
    return core_.spillDecompressTimeUs();
  }

  uint64_t getSpillReadIOTime() const override {
    return core_.spillReadIOTimeUs();
  }

 private:
  void prepareCurrent(const char* record, const char* keyHeap, char* payload) {
    BOLT_DCHECK_LE(keyHeap, keyHeapEnd_);
    const auto encodedSize =
        loadUnaligned<uint64_t>(record + Traits::kSizeOffset);
    BOLT_CHECK_GT(
        encodedSize,
        keyHeapOffset_,
        "Invalid radix sort spill variable key size");
    const auto heapBytes = encodedSize - keyHeapOffset_;
    BOLT_CHECK_LE(
        heapBytes,
        static_cast<uint64_t>(keyHeapEnd_ - keyHeap),
        "Invalid radix sort spill variable key heap size");
    encodedSuffix_ = EncodedKeyView{std::string_view(keyHeap, heapBytes)};
    if constexpr (HasPayload) {
      payload_ = payload;
    }
    key_ = record;
  }

  void clearCursor() noexcept {
    key_ = nullptr;
    payload_ = nullptr;
    encodedSuffix_ = {};
    keyRecordsEnd_ = nullptr;
    keyHeapEnd_ = nullptr;
  }

  void loadBatch() {
    try {
      // The current suffix view borrows the reader's block. Retire it before
      // the core can recycle the buffer or switch physical files.
      encodedSuffix_ = {};
      auto block = core_.nextBatch();
      if (!block.has_value()) {
        clearCursor();
        return;
      }
      keyRecordsEnd_ = block->keyHeapBegin;
      keyHeapEnd_ = block->payloadFixedBegin;
      prepareCurrent(
          block->keyRecordsBegin,
          block->keyHeapBegin,
          HasPayload ? block->payloadFixedBegin : nullptr);
    } catch (...) {
      clearCursor();
      throw;
    }
  }

  RadixSortSpillLogicalRunCore core_;
  const char* keyRecordsEnd_{nullptr};
  const char* keyHeapEnd_{nullptr};
};

} // namespace

std::unique_ptr<RadixSortMergeStream> makeRadixSortMemoryRunMergeStream(
    const RadixSortRunStorage& storage) {
  if (storage.layout().isVariable()) {
    return std::make_unique<RadixSortVariableMemoryRunMergeStream>(storage);
  }
  return std::make_unique<RadixSortMemoryRunMergeStream>(storage);
}

std::unique_ptr<RadixSortMergeStream> makeRadixSortSpillMergeStream(
    RadixSortSpillRun run,
    RadixSortSpillSectionMeta meta,
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    RadixSortSpillReadBufferCache* bufferCache) {
  auto cleanupFilesOnError =
      folly::makeGuard([&run]() { cleanupSpillFilesNoThrow(run.files); });
  std::unique_ptr<RadixSortMergeStream> stream;
  if (meta.keyLayout.isVariable()) {
    if (meta.hasPayload) {
      stream = std::make_unique<RadixSortVariableSpillMergeStream<true>>(
          std::move(run),
          std::move(meta),
          pool,
          spillUringEnabled,
          bufferCache);
    } else {
      stream = std::make_unique<RadixSortVariableSpillMergeStream<false>>(
          std::move(run),
          std::move(meta),
          pool,
          spillUringEnabled,
          bufferCache);
    }
  } else if (meta.hasPayload) {
    stream = std::make_unique<RadixSortFixedSpillMergeStream<true>>(
        std::move(run), std::move(meta), pool, spillUringEnabled, bufferCache);
  } else {
    stream = std::make_unique<RadixSortFixedSpillMergeStream<false>>(
        std::move(run), std::move(meta), pool, spillUringEnabled, bufferCache);
  }
  cleanupFilesOnError.dismiss();
  return stream;
}

RadixSortMerger::RadixSortMerger(
    RadixSortKeyLayout keyLayout,
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams,
    std::optional<size_t> memoryIndex,
    std::unique_ptr<RadixSortSpillReadBufferCache> bufferCache)
    : keyLayout_(std::move(keyLayout)),
      bufferCache_(std::move(bufferCache)),
      streams_(std::move(streams)),
      memoryIndex_(memoryIndex) {
  if (keyLayout_.isVariable()) {
    compare_.variable = compareVariableKeysForLayout(keyLayout_.kind());
    variableSuffixSkip_ =
        keyLayout_.inlineCapacity() - keyLayout_.heapKeyOffset();
    validateVariableStreams();
  } else {
    compare_.fixed = compareFixedKeysForLayout(keyLayout_.kind());
  }
  if (memoryIndex_.has_value()) {
    BOLT_CHECK_LT(*memoryIndex_, streams_.size());
  }
  resetSelection();
}

void RadixSortMerger::resetSelection() {
  BOLT_CHECK_LE(
      streams_.size(),
      static_cast<size_t>(kEmpty),
      "Radix sort merger supports at most {} streams",
      kEmpty);
  firstStream_ = 0;
  if (streams_.size() <= 2) {
    losers_.clear();
    lastIndex_ = kEmpty;
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
  losers_.assign(firstStream_, kEmpty);
  lastIndex_ = kEmpty;
}

std::optional<uint64_t> RadixSortMerger::memoryPosition() const {
  if (!memoryIndex_.has_value()) {
    return std::nullopt;
  }
  BOLT_DCHECK_LT(*memoryIndex_, streams_.size());
  if (keyLayout_.isVariable()) {
    return static_cast<const RadixSortVariableMemoryRunMergeStream&>(
               *streams_[*memoryIndex_])
        .position();
  }
  return static_cast<const RadixSortMemoryRunMergeStream&>(
             *streams_[*memoryIndex_])
      .position();
}

void RadixSortMerger::replaceMemory(
    RadixSortSpillRun run,
    RadixSortSpillSectionMeta meta,
    memory::MemoryPool* pool,
    bool spillUringEnabled) {
  BOLT_CHECK(memoryIndex_.has_value(), "Missing radix memory merge stream");
  BOLT_CHECK(!run.files.empty(), "Radix sort spill run has no files");
  BOLT_CHECK(
      meta.keyLayout.kind() == keyLayout_.kind(),
      "Radix sort replacement stream layout mismatch");

  auto replacement = makeRadixSortSpillMergeStream(
      std::move(run),
      std::move(meta),
      pool,
      spillUringEnabled,
      bufferCache_.get());
  streams_[*memoryIndex_] = std::move(replacement);
  memoryIndex_.reset();
  resetSelection();
}

void RadixSortMerger::removeMemory() {
  BOLT_CHECK(memoryIndex_.has_value(), "Missing radix memory merge stream");
  const auto index = *memoryIndex_;
  BOLT_CHECK(!streams_[index]->hasData(), "Radix memory stream is not empty");
  streams_.erase(streams_.begin() + index);
  memoryIndex_.reset();
  resetSelection();
}

vector_size_t RadixSortMerger::collectRows(
    vector_size_t count,
    const char** keys,
    char** payloads,
    std::span<EncodedKeyView> selectedViews,
    FlushRows flushRows) {
  if (count == 0) {
    return 0;
  }
  BOLT_CHECK(
      !streams_.empty(), "Radix sort merger exhausted before requested rows");
  BOLT_CHECK(
      selectedViews.empty() || selectedViews.size() >= count,
      "Radix sort selected view span is too small");
  BOLT_CHECK(
      selectedViews.empty() || keyLayout_.isVariable(),
      "Fixed radix sort merge cannot capture external key views");
  const auto streamCount = streams_.size();
  if (keyLayout_.hasPayload()) {
    if (streamCount == 1) {
      return selectedViews.empty()
          ? collectSingleStreamRows<true, false>(
                count, keys, payloads, selectedViews, flushRows)
          : collectSingleStreamRows<true, true>(
                count, keys, payloads, selectedViews, flushRows);
    }
    if (!keyLayout_.isVariable()) {
      return streamCount == 2
          ? collectTwoWayRows<true, false, false>(
                count, keys, payloads, selectedViews, flushRows)
          : collectLoserTreeRows<true, false, false>(
                count, keys, payloads, selectedViews, flushRows);
    }
    if (selectedViews.empty()) {
      return streamCount == 2
          ? collectTwoWayRows<true, true, false>(
                count, keys, payloads, selectedViews, flushRows)
          : collectLoserTreeRows<true, true, false>(
                count, keys, payloads, selectedViews, flushRows);
    }
    return streamCount == 2
        ? collectTwoWayRows<true, true, true>(
              count, keys, payloads, selectedViews, flushRows)
        : collectLoserTreeRows<true, true, true>(
              count, keys, payloads, selectedViews, flushRows);
  }
  if (streamCount == 1) {
    return selectedViews.empty()
        ? collectSingleStreamRows<false, false>(
              count, keys, payloads, selectedViews, flushRows)
        : collectSingleStreamRows<false, true>(
              count, keys, payloads, selectedViews, flushRows);
  }
  if (!keyLayout_.isVariable()) {
    return streamCount == 2
        ? collectTwoWayRows<false, false, false>(
              count, keys, payloads, selectedViews, flushRows)
        : collectLoserTreeRows<false, false, false>(
              count, keys, payloads, selectedViews, flushRows);
  }
  if (selectedViews.empty()) {
    return streamCount == 2
        ? collectTwoWayRows<false, true, false>(
              count, keys, payloads, selectedViews, flushRows)
        : collectLoserTreeRows<false, true, false>(
              count, keys, payloads, selectedViews, flushRows);
  }
  return streamCount == 2
      ? collectTwoWayRows<false, true, true>(
            count, keys, payloads, selectedViews, flushRows)
      : collectLoserTreeRows<false, true, true>(
            count, keys, payloads, selectedViews, flushRows);
}

template <bool HasPayload, bool CaptureViews>
vector_size_t RadixSortMerger::collectSingleStreamRows(
    vector_size_t count,
    const char** keys,
    char** payloads,
    std::span<EncodedKeyView> selectedViews,
    FlushRows flushRows) {
  auto* stream = streams_[0].get();
  vector_size_t totalCollected = 0;
  vector_size_t segmentSize = 0;
  while (totalCollected < count) {
    BOLT_CHECK_NOT_NULL(
        stream->key(), "Radix sort merger exhausted before requested rows");
    const auto* record = stream->key();
    keys[segmentSize] = record;
    if constexpr (HasPayload) {
      payloads[segmentSize] = stream->payload();
    }
    if constexpr (CaptureViews) {
      captureSelectedView(*stream, selectedViews[segmentSize]);
    }
    ++segmentSize;
    ++totalCollected;
    if (!stream->tryAdvance()) {
      flushRows(segmentSize);
      segmentSize = 0;
      stream->advanceAfterFlush();
    }
  }
  if (segmentSize != 0) {
    flushRows(segmentSize);
  }
  return totalCollected;
}

template <bool HasPayload, bool Variable, bool CaptureViews>
vector_size_t RadixSortMerger::collectTwoWayRows(
    vector_size_t count,
    const char** keys,
    char** payloads,
    std::span<EncodedKeyView> selectedViews,
    FlushRows flushRows) {
  auto* left = streams_[0].get();
  auto* right = streams_[1].get();
  auto* leftKey = left->key();
  auto* rightKey = right->key();
  vector_size_t totalCollected = 0;
  vector_size_t segmentSize = 0;
  while (totalCollected < count) {
    BOLT_CHECK(
        leftKey != nullptr || rightKey != nullptr,
        "Radix sort merger exhausted before requested rows");
    const bool selectLeft = rightKey == nullptr ||
        (leftKey != nullptr &&
         compareStreams<Variable>(leftKey, rightKey, *left, *right) < 0);
    auto* stream = selectLeft ? left : right;
    const auto* record = selectLeft ? leftKey : rightKey;
    keys[segmentSize] = record;
    if constexpr (HasPayload) {
      payloads[segmentSize] = stream->payload();
    }
    if constexpr (CaptureViews) {
      captureSelectedView(*stream, selectedViews[segmentSize]);
    }
    ++segmentSize;
    ++totalCollected;
    if (!stream->tryAdvance()) {
      flushRows(segmentSize);
      segmentSize = 0;
      stream->advanceAfterFlush();
    }
    if (selectLeft) {
      leftKey = left->key();
    } else {
      rightKey = right->key();
    }
  }
  if (segmentSize != 0) {
    flushRows(segmentSize);
  }
  return totalCollected;
}

template <bool HasPayload, bool Variable, bool CaptureViews>
vector_size_t RadixSortMerger::collectLoserTreeRows(
    vector_size_t count,
    const char** keys,
    char** payloads,
    std::span<EncodedKeyView> selectedViews,
    FlushRows flushRows) {
  vector_size_t totalCollected = 0;
  vector_size_t segmentSize = 0;
  while (totalCollected < count) {
    const auto index = nextLoserTreeStream<Variable>();
    BOLT_CHECK_NE(
        index, kEmpty, "Radix sort merger exhausted before requested rows");
    auto* stream = streams_[index].get();
    const auto* record = stream->key();
    keys[segmentSize] = record;
    if constexpr (HasPayload) {
      payloads[segmentSize] = stream->payload();
    }
    if constexpr (CaptureViews) {
      captureSelectedView(*stream, selectedViews[segmentSize]);
    }
    ++segmentSize;
    ++totalCollected;
    if (!stream->tryAdvance()) {
      flushRows(segmentSize);
      segmentSize = 0;
      stream->advanceAfterFlush();
    }
  }
  if (segmentSize != 0) {
    flushRows(segmentSize);
  }
  return totalCollected;
}

template <bool Variable>
bool RadixSortMerger::less(StreamIndex left, StreamIndex right) const {
  const auto* leftStream = streams_[left].get();
  const auto* rightStream = streams_[right].get();
  return compareStreams<Variable>(
             leftStream->key(), rightStream->key(), *leftStream, *rightStream) <
      0;
}

template <bool Variable>
RadixSortMerger::StreamIndex RadixSortMerger::nextLoserTreeStream() {
  if (lastIndex_ == kEmpty) {
    lastIndex_ = first<Variable>(0);
  } else {
    lastIndex_ = propagate<Variable>(
        parent(firstStream_ + lastIndex_),
        streams_[lastIndex_]->key() == nullptr ? kEmpty : lastIndex_);
  }
  return lastIndex_;
}

template <bool Variable>
RadixSortMerger::StreamIndex RadixSortMerger::first(int32_t node) {
  if (node >= firstStream_) {
    const auto index = static_cast<StreamIndex>(node - firstStream_);
    return streams_[index]->key() == nullptr ? kEmpty : index;
  }
  const auto left = first<Variable>(leftChild(node));
  const auto right = first<Variable>(rightChild(node));
  if (left == kEmpty) {
    return right;
  }
  if (right == kEmpty) {
    return left;
  }
  if (less<Variable>(left, right)) {
    losers_[node] = right;
    return left;
  }
  losers_[node] = left;
  return right;
}

template <bool Variable>
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
    } else if (less<Variable>(losers_[node], value)) {
      std::swap(value, losers_[node]);
    }
    if (node == 0) {
      return value;
    }
    node = parent(node);
  }
}

template <bool Variable>
int32_t RadixSortMerger::compareStreams(
    const char* left,
    const char* right,
    const RadixSortMergeStream& leftStream,
    const RadixSortMergeStream& rightStream) const {
  if constexpr (Variable) {
    return compare_.variable(
        left,
        right,
        static_cast<const RadixSortVariableMergeStream&>(leftStream),
        static_cast<const RadixSortVariableMergeStream&>(rightStream),
        variableSuffixSkip_);
  }
  return compare_.fixed(left, right, keyLayout_.heapKeyOffset());
}

void RadixSortMerger::captureSelectedView(
    const RadixSortMergeStream& stream,
    EncodedKeyView& view) const {
  view =
      static_cast<const RadixSortVariableMergeStream&>(stream).encodedSuffix();
}

void RadixSortMerger::validateVariableStreams() const {
  for (const auto& stream : streams_) {
    if (stream == nullptr) {
      continue;
    }
    BOLT_CHECK_NOT_NULL(
        dynamic_cast<const RadixSortVariableMergeStream*>(stream.get()),
        "Variable radix sort merger requires variable merge streams");
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

} // namespace bytedance::bolt::exec::radixsort
