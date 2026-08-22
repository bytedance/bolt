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

constexpr uint64_t kBlockHeaderSize = sizeof(int32_t) * 2;
constexpr uint64_t kDefaultWriteBufferSize = 1 << 20;
constexpr uint64_t kMaxReadBufferSize = (1 << 20) - AlignedBuffer::kPaddedSize;

template <RadixSortKeyLayoutKind KIND>
int32_t comparePhysicalKeys(const char* left, const char* right) {
  return RadixSortKeyOps<KIND>::compare(left, right);
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
  auto rowSize = checkedAdd<uint64_t>(RadixSortSpillRow::kHeaderSize, keySize);
  BOLT_CHECK(rowSize.has_value(), "Radix sort spill row size overflows");
  rowSize = checkedAdd<uint64_t>(*rowSize, payloadFixedSize);
  BOLT_CHECK(rowSize.has_value(), "Radix sort spill row size overflows");
  BOLT_CHECK_LE(
      *rowSize,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
      "Radix sort spill row size exceeds int32 range");
  return static_cast<uint32_t>(*rowSize);
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

bool nextInlineVariableFixedBatch(
    const RadixSortSpillRunMeta& meta,
    char* block,
    int32_t uncompressedSize,
    std::vector<char*>& keys,
    std::vector<char*>& payloads) {
  if (uncompressedSize < RadixSortSpillRow::kHeaderSize) {
    return false;
  }
  const auto rowSize = loadUnaligned<uint32_t>(block);
  const auto keySize = RadixSortSpillRow::keyFixedSize(meta.keyLayout);
  const auto expectedRowSize =
      RadixSortSpillRow::kHeaderSize + keySize + meta.payloadFixedSize;
  if (rowSize != expectedRowSize || rowSize == 0 ||
      uncompressedSize % rowSize != 0) {
    return false;
  }

  const auto count = uncompressedSize / rowSize;
  const auto initialKeyCount = keys.size();
  const auto initialPayloadCount = payloads.size();
  const auto keySizeOffset = *meta.keyLayout.sizeOffset();
  const auto keyInlineCapacity = meta.keyLayout.inlineCapacity();
  keys.reserve(count);
  payloads.reserve(count);
  auto* current = block;
  for (uint32_t row = 0; row < count; ++row, current += rowSize) {
    if (loadUnaligned<uint32_t>(current) != expectedRowSize) {
      keys.resize(initialKeyCount);
      payloads.resize(initialPayloadCount);
      return false;
    }
    auto* key = current + RadixSortSpillRow::kHeaderSize;
    const auto storedSize = loadUnaligned<uint64_t>(key + keySizeOffset);
    if (storedSize > keyInlineCapacity) {
      keys.resize(initialKeyCount);
      payloads.resize(initialPayloadCount);
      return false;
    }
    keys.push_back(key);
    payloads.push_back(
        meta.payloadFixedSize == 0
            ? nullptr
            : current + RadixSortSpillRow::kHeaderSize + keySize);
  }
  return true;
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
  current_ = nullptr;
  end_ = nullptr;
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
  current_ = buffer_->asMutable<char>() + kBlockHeaderSize;
  end_ = buffer_->asMutable<char>() + bytes;
}

std::vector<RadixSortSpillFile> RadixSortSpillWriter::writeRun(
    const RadixSortRunStorage& storage,
    const PayloadRowLayout* payloadLayout) {
  resetWriteState();
  prepareWriteBuffer();

  const auto& keyLayout = storage.layout();
  const auto payloadFixedOnly =
      payloadLayout == nullptr || !payloadLayout->hasVariableFields();
  const auto dispatchFixedRows = [&]<RadixSortKeyLayoutKind KIND>() {
    for (const auto& block : storage.keyBlocks()) {
      appendFixedRows<KIND>(payloadLayout, block.base, block.count);
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
      case RadixSortKeyLayoutKind::kKeyOnlyVariable32: {
        bool allInline = true;
        for (const auto& block : storage.keyBlocks()) {
          allInline &=
              allVariableRowsInline<RadixSortKeyLayoutKind::kKeyOnlyVariable32>(
                  block.base, block.count);
        }
        if (allInline) {
          for (const auto& block : storage.keyBlocks()) {
            appendInlineVariableRows<
                RadixSortKeyLayoutKind::kKeyOnlyVariable32>(
                payloadLayout, block.base, block.count);
          }
          flush();
          closeFile();
          return std::exchange(files_, {});
        }
        break;
      }
      case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32: {
        bool allInline = true;
        for (const auto& block : storage.keyBlocks()) {
          allInline &= allVariableRowsInline<
              RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>(
              block.base, block.count);
        }
        if (allInline) {
          for (const auto& block : storage.keyBlocks()) {
            appendInlineVariableRows<
                RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>(
                payloadLayout, block.base, block.count);
          }
          flush();
          closeFile();
          return std::exchange(files_, {});
        }
        break;
      }
      default:
        break;
    }
  }

  for (const auto& block : storage.keyBlocks()) {
    for (uint32_t row = 0; row < block.count; ++row) {
      appendRow(
          keyLayout,
          payloadLayout,
          block.base + static_cast<uint64_t>(row) * keyLayout.width());
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
  if (current_ == nullptr) {
    resetWriteState();
    prepareWriteBuffer();
  }
  if (payloadLayout == nullptr) {
    for (vector_size_t row = 0; row < count; ++row) {
      appendRow(keyLayout, nullptr, keys[row], nullptr);
    }
  } else {
    for (vector_size_t row = 0; row < count; ++row) {
      appendRow(keyLayout, payloadLayout, keys[row], payloads[row]);
    }
  }
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

void RadixSortSpillWriter::appendRow(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* key) {
  appendRow(
      keyLayout, payloadLayout, key, RadixSortKey(keyLayout, key).payload());
}

void RadixSortSpillWriter::appendRow(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* key,
    char* payload) {
  const auto rowSize = RadixSortSpillRow::sizeForSerialize(
      keyLayout, payloadLayout, key, payload);
  if (FOLLY_UNLIKELY(
          rowSize.totalSize > static_cast<uint64_t>(end_ - current_))) {
    if (current_ > buffer_->as<char>() + kBlockHeaderSize) {
      flush();
    }
    if (rowSize.totalSize > static_cast<uint64_t>(end_ - current_)) {
      ensureRowFits(rowSize.totalSize);
    }
  }
  RadixSortSpillRow::serialize(
      keyLayout, payloadLayout, key, rowSize, current_);
  current_ += rowSize.totalSize;
  ++currentFileRows_;
}

template <RadixSortKeyLayoutKind KIND>
void RadixSortSpillWriter::appendFixedRows(
    const PayloadRowLayout* payloadLayout,
    const char* keys,
    vector_size_t count) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(!Traits::kVariable);
  const auto keySize =
      RadixSortSpillRow::keyFixedSize(RadixSortKeyLayout::fromKind(KIND));
  const auto payloadFixedSize =
      payloadLayout == nullptr ? 0 : payloadLayout->rowWidth();
  const auto rowSize = checkedFixedRowSize(keySize, payloadFixedSize);
  RadixSortSpillRowHeader header{rowSize};
  const auto* key = keys;
  for (vector_size_t row = 0; row < count; ++row, key += Traits::kWidth) {
    if (FOLLY_UNLIKELY(rowSize > static_cast<uint64_t>(end_ - current_))) {
      if (current_ > buffer_->as<char>() + kBlockHeaderSize) {
        flush();
      }
      if (rowSize > static_cast<uint64_t>(end_ - current_)) {
        ensureRowFits(rowSize);
      }
    }
    storeUnaligned<RadixSortSpillRowHeader>(current_, header);
    auto* cursor = current_ + RadixSortSpillRow::kHeaderSize;
    std::memcpy(cursor, key, keySize);
    cursor += keySize;
    if constexpr (Traits::kHasPayload) {
      std::memcpy(
          cursor,
          loadUnaligned<char*>(key + Traits::kPayloadOffset),
          payloadFixedSize);
    }
    current_ += rowSize;
    ++currentFileRows_;
  }
}

template <RadixSortKeyLayoutKind KIND>
bool RadixSortSpillWriter::allVariableRowsInline(
    const char* keys,
    vector_size_t count) const {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(Traits::kVariable);
  const auto* key = keys;
  for (vector_size_t row = 0; row < count; ++row, key += Traits::kWidth) {
    if (loadUnaligned<uint64_t>(key + Traits::kSizeOffset) >
        Traits::kInlineCapacity) {
      return false;
    }
  }
  return true;
}

template <RadixSortKeyLayoutKind KIND>
void RadixSortSpillWriter::appendInlineVariableRows(
    const PayloadRowLayout* payloadLayout,
    const char* keys,
    vector_size_t count) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(Traits::kVariable);
  const auto payloadFixedSize =
      payloadLayout == nullptr ? 0 : payloadLayout->rowWidth();
  const auto keySize = Traits::kDataOffset;
  const auto rowSize = checkedFixedRowSize(keySize, payloadFixedSize);
  RadixSortSpillRowHeader header{rowSize};
  const auto* key = keys;
  for (vector_size_t row = 0; row < count; ++row, key += Traits::kWidth) {
    if (FOLLY_UNLIKELY(rowSize > static_cast<uint64_t>(end_ - current_))) {
      if (current_ > buffer_->as<char>() + kBlockHeaderSize) {
        flush();
      }
      if (rowSize > static_cast<uint64_t>(end_ - current_)) {
        ensureRowFits(rowSize);
      }
    }
    storeUnaligned<RadixSortSpillRowHeader>(current_, header);
    auto* cursor = current_ + RadixSortSpillRow::kHeaderSize;
    std::memcpy(cursor, key, keySize);
    cursor += keySize;
    if constexpr (Traits::kHasPayload) {
      std::memcpy(
          cursor,
          loadUnaligned<char*>(key + Traits::kPayloadOffset),
          payloadFixedSize);
    }
    current_ += rowSize;
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
  const auto* start = buffer_->as<char>();
  const auto uncompressedBytes =
      static_cast<uint64_t>(current_ - start - kBlockHeaderSize);
  BOLT_CHECK_LE(
      uncompressedBytes,
      maxUncompressedBlockSize(ioConfig_.compressionKind),
      "Radix sort spill block exceeds format or codec limit");
  const auto uncompressedSize = static_cast<int32_t>(uncompressedBytes);
  if (uncompressedSize == 0) {
    return;
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
    auto* header = compressedBuffer_->asMutable<int32_t>();
    header[0] = uncompressedSize;
    header[1] = compressedSize;
    writeBuffer = compressedBuffer_->as<char>();
    writeSize = kBlockHeaderSize + compressedSize;
  } else {
    auto* header = buffer_->asMutable<int32_t>();
    header[0] = uncompressedSize;
    header[1] = uncompressedSize;
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
  if (payloadLayout_ != nullptr) {
    BOLT_CHECK_EQ(meta_.payloadFixedSize, payloadLayout_->rowWidth());
  } else {
    BOLT_CHECK_EQ(meta_.payloadFixedSize, 0);
  }
}

void RadixSortSpillReader::acquireRowBuffer(int32_t size) {
  if (bufferCache_ != nullptr) {
    auto& reusable = bufferCache_->rowBuffer;
    if (reusable != nullptr && reusable->isMutable() &&
        reusable->capacity() >= size) {
      rowBuffer_ = std::move(reusable);
      rowBuffer_->setSize(size);
      return;
    }
  }
  rowBuffer_ = AlignedBuffer::allocate<char>(size, pool_);
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
  const auto uncompressedSize = input_->read<int32_t>();
  const auto storedSize = input_->read<int32_t>();
  BOLT_CHECK_GT(uncompressedSize, 0);
  BOLT_CHECK_GT(storedSize, 0);
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
    acquireRowBuffer(uncompressedSize);
    auto* block = rowBuffer_->asMutable<char>();
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
    acquireRowBuffer(uncompressedSize);
    auto* block = rowBuffer_->asMutable<char>();
    input_->readBytes(block, uncompressedSize);
  }

  auto* block = rowBuffer_->asMutable<char>();
  auto* current = block;
  auto* const end = block + uncompressedSize;
  if (payloadLayout_ == nullptr || !payloadLayout_->hasVariableFields()) {
    if (!meta_.keyLayout.isVariable()) {
      if (nextFixedBatch(block, uncompressedSize, keys, payloads)) {
        return true;
      }
    } else if (nextInlineVariableFixedBatch(
                   meta_, block, uncompressedSize, keys, payloads)) {
      return true;
    }
  }
  bool validRows = true;
  if (payloadLayout_ != nullptr) {
    while (current < end) {
      const auto remaining = static_cast<uint64_t>(end - current);
      if (remaining < RadixSortSpillRow::kHeaderSize) {
        validRows = false;
        break;
      }
      auto row = RadixSortSpillRow(current);
      const auto rowSize = row.totalSize();
      const auto rowValid =
          rowSize >= RadixSortSpillRow::kHeaderSize && rowSize <= remaining;
      validRows &= rowValid;
      if (!rowValid) {
        break;
      }
      row.trustedRestoreKeyDataPointer(meta_);
      row.trustedRestorePayloadPointers(meta_, *payloadLayout_);
      keys.push_back(const_cast<char*>(row.trustedKeyBytes(meta_).data()));
      payloads.push_back(row.trustedPayloadFixed(meta_));
      current += rowSize;
    }
  } else {
    while (current < end) {
      const auto remaining = static_cast<uint64_t>(end - current);
      if (remaining < RadixSortSpillRow::kHeaderSize) {
        validRows = false;
        break;
      }
      auto row = RadixSortSpillRow(current);
      const auto rowSize = row.totalSize();
      const auto rowValid =
          rowSize >= RadixSortSpillRow::kHeaderSize && rowSize <= remaining;
      validRows &= rowValid;
      if (!rowValid) {
        break;
      }
      row.trustedRestoreKeyDataPointer(meta_);
      keys.push_back(const_cast<char*>(row.trustedKeyBytes(meta_).data()));
      payloads.push_back(nullptr);
      current += rowSize;
    }
  }
  BOLT_CHECK(validRows, "Invalid radix sort spill row in block");
  BOLT_CHECK_EQ(current, end);
  return true;
}

bool RadixSortSpillReader::nextFixedBatch(
    char* block,
    int32_t uncompressedSize,
    std::vector<char*>& keys,
    std::vector<char*>& payloads) {
  if (uncompressedSize < RadixSortSpillRow::kHeaderSize) {
    return false;
  }
  const auto rowSize = loadUnaligned<uint32_t>(block);
  const auto keySize = RadixSortSpillRow::keyFixedSize(meta_.keyLayout);
  const auto expectedRowSize =
      RadixSortSpillRow::kHeaderSize + keySize + meta_.payloadFixedSize;
  if (rowSize != expectedRowSize || rowSize == 0 ||
      uncompressedSize % rowSize != 0) {
    return false;
  }
  const auto count = uncompressedSize / rowSize;
  keys.reserve(count);
  payloads.reserve(count);
  auto* current = block;
  for (uint32_t row = 0; row < count; ++row, current += rowSize) {
    keys.push_back(current + RadixSortSpillRow::kHeaderSize);
    payloads.push_back(
        meta_.payloadFixedSize == 0
            ? nullptr
            : current + RadixSortSpillRow::kHeaderSize + keySize);
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
        (leftKey != nullptr && compareKeys_(leftKey, rightKey) < 0)) {
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
  return compareKeys_(streams_[left]->key(), streams_[right]->key()) < 0;
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
