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

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/time/Timer.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

constexpr uint32_t kBlockHeaderSize = sizeof(uint32_t) * 2;
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
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable56:
      return comparePhysicalKeys<
          RadixSortKeyLayoutKind::kKeyWithPayloadVariable56>;
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable64:
      return comparePhysicalKeys<
          RadixSortKeyLayoutKind::kKeyWithPayloadVariable64>;
    case RadixSortKeyLayoutKind::kInvalid:
      break;
  }
  BOLT_FAIL("Invalid radix sort merge key layout");
}

bool compressionEnabled(common::CompressionKind kind) {
  return kind != common::CompressionKind_NONE;
}

uint64_t compressionBound(common::CompressionKind kind, uint64_t size) {
  BOLT_CHECK_LE(size, std::numeric_limits<int32_t>::max());
  if (kind == common::CompressionKind_ZSTD) {
    return ZSTD_compressBound(size);
  }
  BOLT_CHECK_EQ(kind, common::CompressionKind_LZ4);
  return LZ4_compressBound(static_cast<int>(size));
}

uint32_t compressBlock(
    common::CompressionKind kind,
    const char* input,
    uint32_t inputSize,
    char* output,
    uint64_t outputCapacity) {
  if (kind == common::CompressionKind_ZSTD) {
    const auto result =
        ZSTD_compress(output, outputCapacity, input, inputSize, 3);
    BOLT_CHECK(!ZSTD_isError(result));
    BOLT_CHECK_LE(result, std::numeric_limits<uint32_t>::max());
    return static_cast<uint32_t>(result);
  }
  BOLT_CHECK_EQ(kind, common::CompressionKind_LZ4);
  BOLT_CHECK_LE(outputCapacity, std::numeric_limits<int32_t>::max());
  const auto result = LZ4_compress_default(
      input, output, inputSize, static_cast<int>(outputCapacity));
  BOLT_CHECK_GT(result, 0);
  return static_cast<uint32_t>(result);
}

void decompressBlock(
    common::CompressionKind kind,
    const char* input,
    uint32_t inputSize,
    char* output,
    uint32_t outputSize) {
  if (kind == common::CompressionKind_ZSTD) {
    const auto result = ZSTD_decompress(output, outputSize, input, inputSize);
    BOLT_CHECK(!ZSTD_isError(result));
    BOLT_CHECK_EQ(result, outputSize);
    return;
  }
  BOLT_CHECK_EQ(kind, common::CompressionKind_LZ4);
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
    uint32_t uncompressedSize,
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
    const common::SpillConfig::SpillIOConfig& ioConfig,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats)
    : pathPrefix_(std::move(pathPrefix)),
      ioConfig_(ioConfig),
      pool_(pool),
      stats_(stats) {
  BOLT_CHECK_NOT_NULL(pool_);
  BOLT_CHECK_NOT_NULL(stats_);
}

void RadixSortSpillWriter::resetWriteState() {
  files_.clear();
  currentFile_.reset();
  currentFileRows_ = 0;
  nextFileId_ = 0;
  current_ = nullptr;
  end_ = nullptr;
}

void RadixSortSpillWriter::prepareWriteBuffer() {
  ensureBuffer(
      std::max<uint64_t>(ioConfig_.writeBufferSize, kDefaultWriteBufferSize));
  current_ = buffer_->asMutable<char>() + kBlockHeaderSize;
  end_ = buffer_->asMutable<char>() + buffer_->capacity();
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
        return std::move(files_);
      case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
        dispatchFixedRows
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed16>();
        flush();
        closeFile();
        return std::move(files_);
      case RadixSortKeyLayoutKind::kKeyOnlyFixed24:
        dispatchFixedRows
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed24>();
        flush();
        closeFile();
        return std::move(files_);
      case RadixSortKeyLayoutKind::kKeyOnlyFixed32:
        dispatchFixedRows
            .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed32>();
        flush();
        closeFile();
        return std::move(files_);
      case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
        dispatchFixedRows.template
        operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>();
        flush();
        closeFile();
        return std::move(files_);
      case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
        dispatchFixedRows.template
        operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>();
        flush();
        closeFile();
        return std::move(files_);
      case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
        dispatchFixedRows.template
        operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>();
        flush();
        closeFile();
        return std::move(files_);
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
          return std::move(files_);
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
          return std::move(files_);
        }
        break;
      }
      case RadixSortKeyLayoutKind::kKeyWithPayloadVariable56: {
        bool allInline = true;
        for (const auto& block : storage.keyBlocks()) {
          allInline &= allVariableRowsInline<
              RadixSortKeyLayoutKind::kKeyWithPayloadVariable56>(
              block.base, block.count);
        }
        if (allInline) {
          for (const auto& block : storage.keyBlocks()) {
            appendInlineVariableRows<
                RadixSortKeyLayoutKind::kKeyWithPayloadVariable56>(
                payloadLayout, block.base, block.count);
          }
          flush();
          closeFile();
          return std::move(files_);
        }
        break;
      }
      case RadixSortKeyLayoutKind::kKeyWithPayloadVariable64: {
        bool allInline = true;
        for (const auto& block : storage.keyBlocks()) {
          allInline &= allVariableRowsInline<
              RadixSortKeyLayoutKind::kKeyWithPayloadVariable64>(
              block.base, block.count);
        }
        if (allInline) {
          for (const auto& block : storage.keyBlocks()) {
            appendInlineVariableRows<
                RadixSortKeyLayoutKind::kKeyWithPayloadVariable64>(
                payloadLayout, block.base, block.count);
          }
          flush();
          closeFile();
          return std::move(files_);
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
  return std::move(files_);
}

void RadixSortSpillWriter::writeRows(
    const RadixSortKeyLayout& keyLayout,
    const PayloadRowLayout* payloadLayout,
    const char* const* keys,
    char* const* payloads,
    vector_size_t count) {
  BOLT_CHECK_GE(count, 0);
  BOLT_CHECK(keys != nullptr || count == 0);
  BOLT_CHECK(
      payloadLayout == nullptr || payloads != nullptr || count == 0,
      "Radix sort spill row payloads must not be null when payload layout exists");
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
  return std::move(files_);
}

void RadixSortSpillWriter::ensureBuffer(uint64_t bytes) {
  const auto required = std::max<uint64_t>(bytes, kBlockHeaderSize + 1);
  if (buffer_ == nullptr || buffer_->capacity() < required) {
    buffer_ = AlignedBuffer::allocate<char>(required, pool_, 0);
  }
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
  const auto bufferedBytes =
      static_cast<uint64_t>(current_ - buffer_->as<char>());
  if (bufferedBytes > kBlockHeaderSize &&
      current_ + rowSize.totalSize >
          buffer_->as<char>() + buffer_->capacity()) {
    flush();
  }
  if (current_ + rowSize.totalSize >
      buffer_->as<char>() + buffer_->capacity()) {
    ensureBuffer(kBlockHeaderSize + rowSize.totalSize);
    current_ = buffer_->asMutable<char>() + kBlockHeaderSize;
    end_ = buffer_->asMutable<char>() + buffer_->capacity();
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
  const auto rowSize = static_cast<uint32_t>(
      RadixSortSpillRow::kHeaderSize + keySize + payloadFixedSize);
  RadixSortSpillRowHeader header{rowSize};
  const auto* key = keys;
  for (vector_size_t row = 0; row < count; ++row, key += Traits::kWidth) {
    const auto bufferedBytes =
        static_cast<uint64_t>(current_ - buffer_->as<char>());
    if (bufferedBytes > kBlockHeaderSize &&
        current_ + rowSize > buffer_->as<char>() + buffer_->capacity()) {
      flush();
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
  const auto rowSize = static_cast<uint32_t>(
      RadixSortSpillRow::kHeaderSize + keySize + payloadFixedSize);
  RadixSortSpillRowHeader header{rowSize};
  const auto* key = keys;
  for (vector_size_t row = 0; row < count; ++row, key += Traits::kWidth) {
    const auto bufferedBytes =
        static_cast<uint64_t>(current_ - buffer_->as<char>());
    if (bufferedBytes > kBlockHeaderSize &&
        current_ + rowSize > buffer_->as<char>() + buffer_->capacity()) {
      flush();
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
  const auto uncompressedSize =
      static_cast<uint32_t>(current_ - start - kBlockHeaderSize);
  if (uncompressedSize == 0) {
    return;
  }

  auto* file = ensureFile();
  const char* writeBuffer = start;
  uint32_t writeSize = kBlockHeaderSize + uncompressedSize;
  uint64_t compressTimeUs = 0;
  if (compressionEnabled(ioConfig_.compressionKind)) {
    const auto bound = kBlockHeaderSize +
        compressionBound(ioConfig_.compressionKind, uncompressedSize);
    if (compressedBuffer_ == nullptr || compressedBuffer_->capacity() < bound) {
      compressedBuffer_ = AlignedBuffer::allocate<char>(bound, pool_);
    }
    uint32_t compressedSize;
    {
      MicrosecondTimer timer(&compressTimeUs);
      compressedSize = compressBlock(
          ioConfig_.compressionKind,
          start + kBlockHeaderSize,
          uncompressedSize,
          compressedBuffer_->asMutable<char>() + kBlockHeaderSize,
          compressedBuffer_->capacity() - kBlockHeaderSize);
    }
    auto* header = compressedBuffer_->asMutable<uint32_t>();
    header[0] = uncompressedSize;
    header[1] = compressedSize;
    writeBuffer = compressedBuffer_->as<char>();
    writeSize = kBlockHeaderSize + compressedSize;
  } else {
    auto* header = buffer_->asMutable<uint32_t>();
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
  current_ = buffer_->asMutable<char>() + kBlockHeaderSize;
  end_ = buffer_->asMutable<char>() + buffer_->capacity();
}

void RadixSortSpillWriter::closeFile() {
  if (currentFile_ == nullptr) {
    return;
  }
  currentFile_->finish();
  const auto fileSize = currentFile_->size();
  files_.push_back(RadixSortSpillFile{
      currentFile_->id(),
      currentFile_->path(),
      fileSize,
      currentFileRows_,
      ioConfig_.compressionKind});
  {
    auto locked = stats_->wlock();
    ++locked->spilledFiles;
  }
  common::incrementGlobalSpilledFiles();
  currentFile_.reset();
  currentFileRows_ = 0;
}

RadixSortSpillReader::RadixSortSpillReader(
    RadixSortSpillFile file,
    RadixSortSpillRunMeta meta,
    const PayloadRowLayout* payloadLayout,
    memory::MemoryPool* pool,
    bool spillUringEnabled)
    : file_(std::move(file)),
      meta_(std::move(meta)),
      payloadLayout_(payloadLayout),
      pool_(pool),
      input_(makeInputStream(file_, pool, spillUringEnabled)) {
  BOLT_CHECK_NOT_NULL(pool_);
  if (payloadLayout_ != nullptr) {
    BOLT_CHECK_EQ(meta_.payloadFixedSize, payloadLayout_->rowWidth());
  } else {
    BOLT_CHECK_EQ(meta_.payloadFixedSize, 0);
  }
}

bool RadixSortSpillReader::nextBatch(
    std::vector<char*>& keys,
    std::vector<char*>& payloads) {
  keys.clear();
  payloads.clear();
  if (input_->atEnd()) {
    return false;
  }

  MicrosecondTimer readTimer(&spillReadTimeUs_);
  const auto uncompressedSize = input_->read<uint32_t>();
  const auto storedSize = input_->read<uint32_t>();
  BOLT_CHECK_GT(uncompressedSize, 0);
  BOLT_CHECK_GT(storedSize, 0);
  if (rowBuffer_ != nullptr) {
    retainedRowBuffers_.push_back(rowBuffer_);
  }
  rowBuffer_ = AlignedBuffer::allocate<char>(uncompressedSize, pool_);
  auto* block = rowBuffer_->asMutable<char>();
  if (compressionEnabled(file_.compressionKind)) {
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
    BOLT_CHECK_EQ(uncompressedSize, storedSize);
    input_->readBytes(block, uncompressedSize);
  }

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
      if (current + RadixSortSpillRow::kHeaderSize > end) {
        validRows = false;
        break;
      }
      auto row = RadixSortSpillRow(current);
      const auto rowSize = row.totalSize();
      const auto rowValid =
          rowSize >= RadixSortSpillRow::kHeaderSize && current + rowSize <= end;
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
      if (current + RadixSortSpillRow::kHeaderSize > end) {
        validRows = false;
        break;
      }
      auto row = RadixSortSpillRow(current);
      const auto rowSize = row.totalSize();
      const auto rowValid =
          rowSize >= RadixSortSpillRow::kHeaderSize && current + rowSize <= end;
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
    uint32_t uncompressedSize,
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
  return input_ == nullptr ? 0 : input_->getSpillReadIOTime();
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
    bool spillUringEnabled)
    : RadixSortMergeStream(meta.keyLayout),
      reader_(
          std::move(file),
          std::move(meta),
          payloadLayout,
          pool,
          spillUringEnabled) {
  loadBatch();
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
  if (!reader_.nextBatch(keys_, payloads_)) {
    key_ = nullptr;
    payload_ = nullptr;
    return;
  }
  BOLT_CHECK_EQ(keys_.size(), payloads_.size());
  BOLT_CHECK_GT(keys_.size(), 0);
  index_ = 0;
  key_ = keys_[0];
  payload_ = payloads_[0];
}

RadixSortMerger::RadixSortMerger(
    RadixSortKeyLayout keyLayout,
    std::vector<std::unique_ptr<RadixSortMergeStream>> streams)
    : keyLayout_(std::move(keyLayout)),
      compareKeys_(compareKeysForLayout(keyLayout_.kind())),
      streams_(std::move(streams)) {
  BOLT_CHECK(!streams_.empty(), "Radix sort merger requires input streams");
  BOLT_CHECK_LT(streams_.size(), std::numeric_limits<StreamIndex>::max());
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
