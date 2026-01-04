/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/exec/SpillFile.h"
#include <lz4.h>
#include <zstd.h>
#include <cstdint>
#include <vector>
#include "bolt/common/base/RuntimeMetrics.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/ContainerRow2RowSerde.h"
namespace bytedance::bolt::exec {
namespace {
// Spilling currently uses the default PrestoSerializer which by default
// serializes timestamp with millisecond precision to maintain compatibility
// with presto. Since bolt's native timestamp implementation supports
// nanosecond precision, we use this serde option to ensure the serializer
// preserves precision.
static const bool kDefaultUseLosslessTimestamp = true;
} // namespace

void SpillInputStream::next(bool /*throwIfPastEnd*/) {
  MicrosecondTimer timer(&spillReadIOTimeUs_);
  if (spillUringEnabled_ && file_->uringEnabled()) {
    int curIdx = idx_ % bufferNum_;
    auto ret = file_->waitForComplete();
    BOLT_CHECK(ret, "Error occured when waiting for io_uring read to complete");
    size_t compeletedSize = bufferSizes_[curIdx];
    setRange(
        {readBuffers_[curIdx]->asMutable<uint8_t>(),
         (int32_t)compeletedSize,
         0});
    completed_ += compeletedSize;
    idx_++;
    prefetch(true);
  } else { // blocking pread
    const size_t readBytes =
        std::min(size_ - offset_, readBuffers_[0]->capacity());
    BOLT_CHECK_LT(0, readBytes, "Reading past end of spill file");
    setRange({readBuffers_[0]->asMutable<uint8_t>(), (int32_t)readBytes, 0});
    file_->pread(offset_, readBytes, readBuffers_[0]->asMutable<char>());
    offset_ += readBytes;
    completed_ += readBytes;
  }
}

void SpillInputStream::init(bool /*throwIfPastEnd*/) {
  MicrosecondTimer timer(&spillReadIOTimeUs_);
  BOLT_CHECK(
      !readBuffers_.empty(),
      "SpillInputStream::init read buffer cannot be empty");
  const size_t readBytes =
      std::min(size_ - offset_, readBuffers_[0]->capacity());
  BOLT_CHECK_LT(0, readBytes, "Reading past end of spill file");
  setRange({readBuffers_[0]->asMutable<uint8_t>(), (int32_t)readBytes, 0});
  file_->pread(offset_, readBytes, readBuffers_[0]->asMutable<char>());
  offset_ += readBytes;
  completed_ += readBytes;
  bufferSizes_[0] = readBytes;
  idx_++;
  // io_uring append with prefetch during init.
  prefetch(true);
}

void SpillInputStream::prefetch(bool /*throwIfPastEnd*/) {
  // uring failed to init, can't do prefetch
  if (!file_->uringEnabled())
    return;
  if (offset_ < size_) {
    int curIdx = idx_ % bufferNum_;
    const size_t preFetchBytes =
        std::min(size_ - offset_, readBuffers_[curIdx]->capacity());
    file_->submitRead(
        readBuffers_[curIdx]->asMutable<char>(), offset_, preFetchBytes);
    bufferSizes_[curIdx] = preFetchBytes;
    offset_ += preFetchBytes;
  }
}

std::unique_ptr<SpillWriteFile> SpillWriteFile::create(
    uint32_t id,
    const std::string& pathPrefix,
    const std::string& fileCreateConfig,
    const bool spillUringEnabled) {
  return std::unique_ptr<SpillWriteFile>(
      new SpillWriteFile(id, pathPrefix, fileCreateConfig, spillUringEnabled));
}

SpillWriteFile::SpillWriteFile(
    uint32_t id,
    const std::string& pathPrefix,
    const std::string& fileCreateConfig,
    const bool spillUringEnabled)
    : id_(id),
      path_(fmt::format("{}-{}", pathPrefix, ordinalCounter_++)),
      spillUringEnabled_(spillUringEnabled) {
  if (spillUringEnabled_) {
    // TODO: it is better to let memory pool to set the ringSize @Zhongjun
    // A smaller number should be set (e.g. 8) when row based spill is enabled
    const size_t ringSize =
        64; // should be same with ringDepth in LocalWriteFile
    writeBuffers_ = std::make_shared<WriteBuffers>(ringSize);
  }
  auto fs = filesystems::getFileSystem(path_, nullptr);
#ifdef IO_URING_SUPPORTED
  if (spillUringEnabled_) {
    file_ = fs->openAsyncFileForWrite(
        path_,
        filesystems::FileOptions{
            {{filesystems::FileOptions::kFileCreateConfig.toString(),
              fileCreateConfig}},
            0,
            nullptr});
    return;
  }
#endif

  file_ = fs->openFileForWrite(
      path_,
      filesystems::FileOptions{
          {{filesystems::FileOptions::kFileCreateConfig.toString(),
            fileCreateConfig}},
          0,
          nullptr});
}

void SpillWriteFile::finish() {
  BOLT_CHECK_NOT_NULL(file_);
  if (spillUringEnabled_)
    writeBuffers_->clear(file_);
  size_ = file_->size();
  file_->close();
  file_ = nullptr;
}

uint64_t SpillWriteFile::size() const {
  if (file_ != nullptr) {
    return file_->size();
  }
  return size_;
}

uint64_t SpillWriteFile::write(std::unique_ptr<folly::IOBuf> iobuf) {
  auto writtenBytes = iobuf->computeChainDataLength();
  if (spillUringEnabled_ && file_->uringEnabled()) {
    while (iobuf) {
      auto current = std::move(iobuf);
      iobuf = current->pop();
      auto taskId = taskId_++;
      auto writeBuff = writeBuffers_->add(current, taskId, file_);
      file_->submitWrite(writeBuff, taskId);
    }
  } else {
    file_->append(std::move(iobuf));
  }
  return writtenBytes;
}

uint64_t SpillWriteFile::write(std::string_view buf) {
  auto writtenBytes = buf.size();
  if (spillUringEnabled_ && file_->uringEnabled()) {
    auto ioBuf = folly::IOBuf::copyBuffer(buf);
    auto taskId = taskId_++;
    auto writeBuff = writeBuffers_->add(ioBuf, taskId, file_);
    file_->submitWrite(writeBuff, taskId);
  } else {
    file_->append(buf);
  }
  return writtenBytes;
}

SpillWriter::SpillWriter(
    const RowTypePtr& type,
    const uint32_t numSortKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    const std::string& pathPrefix,
    uint64_t targetFileSize,
    const common::SpillConfig::SpillIOConfig& ioConfig,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    uint32_t maxBatchRows,
    std::optional<RowFormatInfo> rowInfo)
    : type_(type),
      numSortKeys_(numSortKeys),
      sortCompareFlags_(sortCompareFlags),
      compressionKind_(ioConfig.compressionKind),
      pathPrefix_(pathPrefix),
      targetFileSize_(targetFileSize),
      spillUringEnabled_(ioConfig.spillUringEnabled),
      writeBufferSize_(ioConfig.writeBufferSize),
      fileCreateConfig_(ioConfig.fileCreateConfig),
      updateAndCheckSpillLimitCb_(ioConfig.updateAndCheckSpillLimitCb),
      pool_(pool),
      stats_(stats),
      maxBatchRows_(maxBatchRows),
      rowInfo_(rowInfo),
      spillSerdeKind_(ioConfig.spillSerdeKind) {
  if (ioConfig.spillSerdeKind) {
    serde_ = getNamedVectorSerde(*ioConfig.spillSerdeKind);
  }
  // NOTE: if the associated spilling operator has specified the sort
  // comparison flags, then it must match the number of sorting keys.
  BOLT_CHECK(
      sortCompareFlags_.empty() || sortCompareFlags_.size() == numSortKeys_);
}

SpillWriteFile* SpillWriter::ensureFile() {
  if ((currentFile_ != nullptr) && (currentFile_->size() > targetFileSize_)) {
    closeFile();
  }
  if (currentFile_ == nullptr) {
    currentFile_ = SpillWriteFile::create(
        nextFileId_++,
        fmt::format("{}-{}", pathPrefix_, finishedFiles_.size()),
        fileCreateConfig_,
        spillUringEnabled_);
  }
  return currentFile_.get();
}

void SpillWriter::closeFile() {
  if (currentFile_ == nullptr) {
    return;
  }
  currentFile_->finish();
  updateSpilledFileStats(currentFile_->size());
  finishedFiles_.push_back(SpillFileInfo{
      .id = currentFile_->id(),
      .type = type_,
      .path = currentFile_->path(),
      .size = currentFile_->size(),
      .rowCount = rowsInCurrentFile_,
      .numSortKeys = numSortKeys_,
      .sortFlags = sortCompareFlags_,
      .compressionKind = compressionKind_,
      .serdeKind = spillSerdeKind_,
      .rowInfo = rowInfo_});
  rowsInCurrentFile_ = 0;
  currentFile_.reset();
}

size_t SpillWriter::numFinishedFiles() const {
  return finishedFiles_.size();
}

uint64_t SpillWriter::flush() {
  if (batch_ == nullptr) {
    return 0;
  }

  auto* file = ensureFile();
  BOLT_CHECK_NOT_NULL(file);

  IOBufOutputStream out(
      *pool_, nullptr, std::max<int64_t>(64 * 1024, batch_->size()));
  uint64_t flushTimeUs{0};
  {
    MicrosecondTimer timer(&flushTimeUs);
    batch_->flush(&out);
  }
  batch_.reset();

  uint64_t writeTimeUs{0};
  uint64_t writtenBytes{0};
  auto iobuf = out.getIOBuf();
  {
    MicrosecondTimer timer(&writeTimeUs);
    writtenBytes = file->write(std::move(iobuf));
  }
  updateWriteStats(writtenBytes, flushTimeUs, writeTimeUs);
  updateAndCheckSpillLimitCb_(writtenBytes);
  return writtenBytes;
}

uint64_t SpillWriter::write(
    const RowVectorPtr& rows,
    const folly::Range<IndexRange*>& indices) {
  checkNotFinished();

  bool rowSizeExceed = false;
  uint64_t timeUs{0};
  {
    MicrosecondTimer timer(&timeUs);
    if (batch_ == nullptr) {
      bytedance::bolt::VectorSerde::Options options = {
          kDefaultUseLosslessTimestamp, compressionKind_};
      batch_ = std::make_unique<VectorStreamGroup>(pool_, serde_);
      batch_->createStreamTree(
          std::static_pointer_cast<const RowType>(rows->type()),
          1'000,
          &options);
      unflushedRows_ = 0;
      unflushedSizeInRowVector_ = 0;
    }
    int32_t writeRowSize = 0;
    for (const auto& range : indices) {
      writeRowSize += range.size;
    }
    unflushedRows_ += writeRowSize;
    batch_->append(rows, indices);
    if ((numSortKeys_ > 0) && (writeRowSize > 0)) {
      // only sort spill should limit batch memory size to avoid merge OOM
      unflushedSizeInRowVector_ +=
          rows->estimateFlatSize() / writeRowSize * rows->size();
      rowSizeExceed = unflushedSizeInRowVector_ > writeBufferSize_;
    }
  }
  updateAppendStats(rows->size(), timeUs);
  rowsInCurrentFile_ += rows->size();
  if (batch_->size() < writeBufferSize_ && !rowSizeExceed &&
      !(maxBatchRows_ > 0 && unflushedRows_ >= maxBatchRows_)) {
    return 0;
  }
  return flush();
}

template <typename T>
T alignUp(T value, int alignment) {
  if (value % alignment == 0) {
    return value;
  } else {
    return value + (alignment - value % alignment);
  }
}

char* alignUp(char* addr, int alignment) {
  const auto intptr = reinterpret_cast<std::uintptr_t>(addr);
  if (intptr % alignment == 0) {
    return addr;
  } else {
    return addr + (alignment - intptr % alignment);
  }
}

uint64_t SpillWriter::writeAndFlush(
    const RowVectorPtr& rows,
    const folly::Range<IndexRange*>& indices) {
  checkNotFinished();

  uint64_t timeUs{0};
  {
    MicrosecondTimer timer(&timeUs);
    if (batch_ == nullptr) {
      bytedance::bolt::VectorSerde::Options options = {
          kDefaultUseLosslessTimestamp, compressionKind_};
      batch_ = std::make_unique<VectorStreamGroup>(pool_, serde_);
      batch_->createStreamTree(
          std::static_pointer_cast<const RowType>(rows->type()),
          rows->size(),
          &options);
    }
    batch_->append(rows, indices);
  }
  rowsInCurrentFile_ += rows->size();
  updateAppendStats(rows->size(), timeUs);
  return flush();
}

uint64_t SpillWriter::write(
    const std::vector<char*, memory::StlAllocator<char*>>& rows,
    const RowFormatInfo& info) {
  checkNotFinished();
  static constexpr size_t kBufferSize =
      (1ULL << 20) - AlignedBuffer::kPaddedSize; // 1MB
  BufferPtr buffer = AlignedBuffer::allocate<char>(kBufferSize, pool_, 0);
  size_t writeBufferLimit = std::min<size_t>(kBufferSize, buffer->capacity());
  BufferPtr compressBuffer = nullptr;
  char* current = nullptr;
  const char *bufferStart = nullptr, *bufferEnd = nullptr;
  uint32_t* headerPtr = nullptr;
  uint64_t totalSize = 0;
  uint64_t timeUs{0}, compressWriteTimsUs{0};

  // header store two uint32_t to save data size and compressed data size
  constexpr size_t headerSize = sizeof(uint32_t) * 2;
  auto resetNewBufferState = [&]() {
    bufferStart = buffer->as<char>();
    headerPtr = (uint32_t*)bufferStart;
    current = buffer->asMutable<char>() + headerSize;
    bufferEnd = buffer->as<char>() + buffer->capacity();
  };

  resetNewBufferState();

  auto writeBuffer = [&]() {
    size_t bufferSize = current - bufferStart;
    size_t dataSize = bufferSize - headerSize;
    totalSize += bufferSize;
    BOLT_CHECK_LE(bufferSize, bufferEnd - bufferStart);
    auto* file = ensureFile();
    BOLT_CHECK_NOT_NULL(file);

    const char* writeBuffer = nullptr;
    size_t writeBufferSize = 0;
    uint64_t writtenBytes{0}, writeTimeUs{0}, compressTimeUs{0};
    if (info.enableCompression) {
      size_t needSize = headerSize +
          ((compressionKind_ == common::CompressionKind::CompressionKind_ZSTD)
               ? ZSTD_compressBound(dataSize)
               : LZ4_compressBound(dataSize));
      if (!compressBuffer || compressBuffer->capacity() < needSize) {
        compressBuffer = AlignedBuffer::allocate<char>(needSize, pool_);
      }

      {
        MicrosecondTimer timer(&compressTimeUs);
        uint32_t* compressedHeaderPtr =
            (uint32_t*)compressBuffer->asMutable<char>();
        char* compressedStart = compressBuffer->asMutable<char>() + headerSize;
        int ret =
            (compressionKind_ == common::CompressionKind::CompressionKind_ZSTD)
            ? ZSTD_compress(
                  compressedStart,
                  needSize - headerSize,
                  bufferStart + headerSize,
                  dataSize,
                  3)
            : LZ4_compress_default(
                  bufferStart + headerSize,
                  compressedStart,
                  dataSize,
                  needSize - headerSize);
        BOLT_CHECK(ret > 0);
        compressedHeaderPtr[0] = dataSize;
        compressedHeaderPtr[1] = ret;
        writeBufferSize = headerSize + ret;
        writeBuffer = compressBuffer->as<char>();
      }
    } else {
      headerPtr[0] = dataSize;
      headerPtr[1] = dataSize;
      writeBufferSize = bufferSize;
      writeBuffer = bufferStart;
    }

    {
      MicrosecondTimer timer(&writeTimeUs);
      writtenBytes =
          file->write(std::string_view(writeBuffer, writeBufferSize));
    }
    // keep writeBufferLimit to kBufferSize to avoid OOM in sort merge
    writeBufferLimit = std::min<size_t>(kBufferSize, buffer->capacity());
    // there is no flush here, so add compress time as flush time
    updateWriteStats(writtenBytes, compressTimeUs, writeTimeUs);
    updateAndCheckSpillLimitCb_(writtenBytes);
    compressWriteTimsUs += compressTimeUs + writeTimeUs;
  };

  {
    MicrosecondTimer timer(&timeUs);
    vector_size_t rowIndex = 0;
    vector_size_t needWriteRowCount = 0;
    while (rowIndex < rows.size()) {
      char* row = rows[rowIndex];
      int32_t rowSize = 0;
      if (info.serialized) {
        rowSize = ContainerRow2RowSerde::rowSize(row, info);
      } else {
        rowSize = info.getRowSize(row);
      }
      if (current + rowSize - bufferStart > writeBufferLimit) {
        if (needWriteRowCount > 0) {
          writeBuffer();
        }
        auto sizeWithHeader = rowSize + headerSize;
        if (sizeWithHeader > (bufferEnd - bufferStart)) {
          // rowSize large than buffer capacity
          LOG(INFO) << "large row in row based spill, header + row size: "
                    << sizeWithHeader
                    << ", buffer capacity: " << buffer->capacity();
          buffer = AlignedBuffer::allocate<char>(sizeWithHeader, pool_, 0);
          // increse writeBufferLimit to hold large row
          writeBufferLimit = sizeWithHeader;
        }
        needWriteRowCount = 0;
        resetNewBufferState();
      }

      if (info.serialized) {
        int currentRowSize = ContainerRow2RowSerde::rowSize(row, info);
        BOLT_CHECK(current + currentRowSize <= bufferEnd);
        simd::memcpy(current, row, currentRowSize);
        current += currentRowSize;
      } else {
        ContainerRow2RowSerde::serialize(row, current, bufferEnd, info);
      }

      rowIndex++;
      needWriteRowCount++;
    }
    if (needWriteRowCount > 0) {
      writeBuffer();
    }
  }
  rowsInCurrentFile_ += rows.size();
  updateAppendStats(rows.size(), timeUs - compressWriteTimsUs);
  return totalSize;
}

void SpillWriter::updateAppendStats(
    uint64_t numRows,
    uint64_t serializationTimeUs) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledRows += numRows;
  statsLocked->spillSerializationTimeUs += serializationTimeUs;
  common::updateGlobalSpillAppendStats(numRows, serializationTimeUs);
}

void SpillWriter::updateWriteStats(
    uint64_t spilledBytes,
    uint64_t flushTimeUs,
    uint64_t fileWriteTimeUs) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledBytes += spilledBytes;
  statsLocked->spillFlushTimeUs += flushTimeUs;
  statsLocked->spillWriteTimeUs += fileWriteTimeUs;
  ++statsLocked->spillWrites;
  common::updateGlobalSpillWriteStats(
      spilledBytes, flushTimeUs, fileWriteTimeUs);
}

void SpillWriter::updateSpilledFileStats(uint64_t fileSize) {
  ++stats_->wlock()->spilledFiles;
  addThreadLocalRuntimeStat(
      "spillFileSize", RuntimeCounter(fileSize, RuntimeCounter::Unit::kBytes));
  common::incrementGlobalSpilledFiles();
}

void SpillWriter::finishFile() {
  checkNotFinished();
  flush();
  closeFile();
  BOLT_CHECK_NULL(currentFile_);
}

std::vector<SpillFileInfo> SpillWriter::finish() {
  checkNotFinished();
  auto finishGuard = folly::makeGuard([this]() { finished_ = true; });

  finishFile();
  return std::move(finishedFiles_);
}

std::vector<std::string> SpillWriter::testingSpilledFilePaths() const {
  checkNotFinished();

  std::vector<std::string> spilledFilePaths;
  for (auto& file : finishedFiles_) {
    spilledFilePaths.push_back(file.path);
  }
  if (currentFile_ != nullptr) {
    spilledFilePaths.push_back(currentFile_->path());
  }
  return spilledFilePaths;
}

std::vector<uint32_t> SpillWriter::testingSpilledFileIds() const {
  checkNotFinished();

  std::vector<uint32_t> fileIds;
  for (auto& file : finishedFiles_) {
    fileIds.push_back(file.id);
  }
  if (currentFile_ != nullptr) {
    fileIds.push_back(currentFile_->id());
  }
  return fileIds;
}

SpillReadFileBase::SpillReadFileBase(
    const SpillFileInfo& fileInfo,
    memory::MemoryPool* pool,
    const bool spillUringEnabled)
    : id_(fileInfo.id),
      path_(fileInfo.path),
      size_(fileInfo.size),
      type_(fileInfo.type),
      numSortKeys_(fileInfo.numSortKeys),
      sortCompareFlags_(fileInfo.sortFlags),
      compressionKind_(fileInfo.compressionKind),
      readOptions_{kDefaultUseLosslessTimestamp, compressionKind_},
      serdeKind_(fileInfo.serdeKind),
      serde_(
          serdeKind_.has_value() ? getNamedVectorSerde(*serdeKind_) : nullptr),
      spillUringEnabled_(spillUringEnabled),
      pool_(pool) {
  constexpr uint64_t kMaxReadBufferSize =
      (1 << 20) - AlignedBuffer::kPaddedSize; // 1MB - padding.
  auto fs = filesystems::getFileSystem(path_, nullptr);
  std::unique_ptr<ReadFile> file;
#ifdef IO_URING_SUPPORTED
  file = spillUringEnabled_ ? fs->openAsyncFileForRead(path_)
                            : fs->openFileForRead(path_);
#else
  file = fs->openFileForRead(path_);
#endif

  // For io_uring enabled spill read, maintain two read buffers, one for storing
  // current batch, one for prefetching. If io_uring is disabled or fails to
  // init, just use one buffer for blocking pread.
  int bufferLength = (spillUringEnabled && file->uringEnabled()) ? 2 : 1;
  std::vector<BufferPtr> readBuffers;
  readBuffers.resize(bufferLength);
  for (int i = 0; i < bufferLength; i++) {
    readBuffers[i] = AlignedBuffer::allocate<char>(kMaxReadBufferSize, pool_);
  }

  input_ = std::make_unique<SpillInputStream>(
      std::move(file), readBuffers, spillUringEnabled);
}

bool SpillReadFile::nextBatch(RowVectorPtr& rowVector) {
  if (input_->atEnd()) {
    spillReadIOTimeUs_ = input_->getSpillReadIOTime();
    return false;
  }
  if (serde_ != nullptr) {
    serde_->deserialize(input_.get(), pool_, type_, &rowVector, &readOptions_);
  } else {
    VectorStreamGroup::read(
        input_.get(), pool_, type_, &rowVector, &readOptions_);
  }
  return true;
}

void SpillReadFile::reuse() {
  input_->reuse();
}

uint32_t RowBasedSpillReadFile::nextBatch(std::vector<char*>& rows) {
  rows.clear();
  if (input_->atEnd()) {
    spillReadIOTimeUs_ = input_->getSpillReadIOTime();
    return 0;
  }
  uint32_t bufferSize = input_->read<uint32_t>();
  uint32_t compressedSize = input_->read<uint32_t>();
  uint32_t alignedBufferSize = bufferSize + info_.alignment;
  if (rowBuffer_->capacity() < alignedBufferSize) {
    rowBuffer_ = AlignedBuffer::allocate<char>(alignedBufferSize, pool_);
  }
  char* current = alignUp(rowBuffer_->asMutable<char>(), info_.alignment);
  if (info_.enableCompression) {
    if (!compressedBuffer_ || compressedBuffer_->capacity() < compressedSize) {
      compressedBuffer_ = AlignedBuffer::allocate<char>(compressedSize, pool_);
    }
    input_->readBytes(compressedBuffer_->asMutable<char>(), compressedSize);
    MicrosecondTimer timer(&spillDecompressTimeUs_);
    if (compressionKind_ == common::CompressionKind::CompressionKind_ZSTD) {
      auto ret = ZSTD_decompress(
          current,
          rowBuffer_->capacity(),
          compressedBuffer_->as<char>(),
          compressedSize);
      BOLT_CHECK_EQ(ret, bufferSize);
    } else {
      int ret = LZ4_decompress_safe(
          compressedBuffer_->as<char>(),
          current,
          compressedSize,
          rowBuffer_->capacity());
      BOLT_CHECK_EQ(ret, bufferSize);
    }
  } else {
    BOLT_CHECK_EQ(bufferSize, compressedSize);
    input_->readBytes(current, bufferSize);
  }
  char* bufferEnd = current + bufferSize;
  while (current < bufferEnd) {
    rows.push_back(current);
    BOLT_DCHECK_EQ(
        reinterpret_cast<std::uintptr_t>(current) % info_.alignment, 0);
    ContainerRow2RowSerde::deserialize(current, info_);
  }
  BOLT_CHECK_EQ(current - bufferEnd, 0);
  /// when bypassh hash table in hash agg, the last row in the sorted stream
  /// shouldn't set nextEqual flag
  if (input_->atEnd() && info_.nextEqualOffset > 0 &&
      bits::isBitSet(rows.back(), info_.nextEqualOffset)) {
    bits::setBit(rows.back(), info_.nextEqualOffset, false);
  }
  return bufferSize;
}

bool RowBasedSpillReadFile::nextBatch(
    std::vector<char*>& rows,
    std::vector<size_t>& rowLengths) {
  rows.clear();
  rowLengths.clear();
  if (input_->atEnd()) {
    return false;
  }
  uint32_t bufferSize = input_->read<uint32_t>();
  uint32_t compressedSize = input_->read<uint32_t>();
  if (rowBuffer_->capacity() < bufferSize) {
    rowBuffer_ = AlignedBuffer::allocate<char>(bufferSize, pool_);
  }
  char* current = rowBuffer_->asMutable<char>();
  if (info_.enableCompression) {
    if (!compressedBuffer_ || compressedBuffer_->capacity() < compressedSize) {
      compressedBuffer_ = AlignedBuffer::allocate<char>(compressedSize, pool_);
    }
    input_->readBytes(compressedBuffer_->asMutable<char>(), compressedSize);
    MicrosecondTimer timer(&spillDecompressTimeUs_);
    if (compressionKind_ == common::CompressionKind::CompressionKind_ZSTD) {
      auto ret = ZSTD_decompress(
          current,
          rowBuffer_->capacity(),
          compressedBuffer_->as<char>(),
          compressedSize);
      BOLT_CHECK_EQ(ret, bufferSize);
    } else {
      int ret = LZ4_decompress_safe(
          compressedBuffer_->as<char>(),
          current,
          compressedSize,
          rowBuffer_->capacity());
      BOLT_CHECK_EQ(ret, bufferSize);
    }
  } else {
    BOLT_CHECK_EQ(bufferSize, compressedSize);
    input_->readBytes(current, bufferSize);
  }
  char* bufferEnd = current + bufferSize;
  while (current < bufferEnd) {
    rows.push_back(current);
    rowLengths.push_back(ContainerRow2RowSerde::deserialize(current, info_));
  }
  BOLT_CHECK_EQ(current - bufferEnd, 0);
  return true;
}
} // namespace bytedance::bolt::exec
