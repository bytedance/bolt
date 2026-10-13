/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/dwio/lance/NativeLanceLegacyPageReader.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {

NativeLanceLegacyPageReader::NativeLanceLegacyPageReader(
    std::string scheme,
    uint64_t compressedOffset,
    uint64_t compressedLength,
    memory::MemoryPool& pool,
    Read read,
    uint64_t streamChunkBytes)
    : scheme_(std::move(scheme)),
      compressedOffset_(compressedOffset),
      compressedLength_(compressedLength),
      pool_(pool),
      read_(std::move(read)),
      accessMode_(NativeLanceDecompressor::accessMode(scheme_)) {
  BOLT_CHECK_GT(compressedLength_, 0);
  if (accessMode_ == NativeLanceCodecAccess::kSequentialFrame) {
    zstdStream_ = std::make_unique<NativeLanceZstdStream>(
        compressedOffset_, compressedLength_, pool_, read_, streamChunkBytes);
  }
}

BufferPtr NativeLanceLegacyPageReader::decodeRange(
    uint64_t decodedOffset,
    uint64_t decodedLength) {
  BOLT_CHECK(!cancelled_.load(std::memory_order_acquire));
  if (zstdStream_ != nullptr) {
    return zstdStream_->decodeRange(decodedOffset, decodedLength);
  }

  BOLT_CHECK(accessMode_ == NativeLanceCodecAccess::kWholeBuffer);
  const auto compressed = read_(compressedOffset_, compressedLength_);
  const auto decompressed =
      NativeLanceDecompressor::decompress(scheme_, compressed, pool_);
  BOLT_CHECK_LE(decodedOffset, decompressed->size());
  BOLT_CHECK_LE(decodedLength, decompressed->size() - decodedOffset);
  auto result =
      Buffer::slice<char>(decompressed, decodedOffset, decodedLength, &pool_);
  wholeBufferOperationFinished_.store(true, std::memory_order_release);
  return result;
}

void NativeLanceLegacyPageReader::cancel() {
  cancelled_.store(true, std::memory_order_release);
}

bool NativeLanceLegacyPageReader::finished() const {
  return zstdStream_ == nullptr
      ? wholeBufferOperationFinished_.load(std::memory_order_acquire)
      : zstdStream_->finished();
}

uint64_t NativeLanceLegacyPageReader::restartCount() const {
  return zstdStream_ == nullptr ? 0 : zstdStream_->restartCount();
}

uint64_t NativeLanceLegacyPageReader::retainedBytes() const {
  return zstdStream_ == nullptr ? 0 : zstdStream_->retainedBytes();
}

} // namespace bytedance::bolt::lance::reader
