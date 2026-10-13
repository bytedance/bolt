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

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "bolt/dwio/lance/NativeLanceDecompressor.h"

namespace bytedance::bolt::lance::reader {

/// Page-local codec owner for legacy v2.0 Flat compressed buffers. Zstd uses
/// a monotonic stream cursor; whole-buffer codecs are materialized only for
/// the requested operation and never retained across batches.
class NativeLanceLegacyPageReader {
 public:
  using Read = NativeLanceZstdStream::Read;

  NativeLanceLegacyPageReader(
      std::string scheme,
      uint64_t compressedOffset,
      uint64_t compressedLength,
      memory::MemoryPool& pool,
      Read read,
      uint64_t streamChunkBytes);

  BufferPtr decodeRange(uint64_t decodedOffset, uint64_t decodedLength);
  void cancel();

  NativeLanceCodecAccess accessMode() const {
    return accessMode_;
  }

  bool finished() const;
  uint64_t restartCount() const;
  uint64_t retainedBytes() const;

 private:
  const std::string scheme_;
  const uint64_t compressedOffset_;
  const uint64_t compressedLength_;
  memory::MemoryPool& pool_;
  const Read read_;
  const NativeLanceCodecAccess accessMode_;
  std::unique_ptr<NativeLanceZstdStream> zstdStream_;
  std::atomic<bool> cancelled_{false};
  std::atomic<bool> wholeBufferOperationFinished_{false};
};

} // namespace bytedance::bolt::lance::reader
