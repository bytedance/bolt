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

#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

enum class NativeLanceCodecAccess : uint8_t {
  kSequentialFrame,
  kWholeBuffer,
};

/// Codec boundary shared by legacy and structural page readers.
class NativeLanceDecompressor {
 public:
  static NativeLanceCodecAccess accessMode(std::string_view scheme);

  /// Decompresses one complete legacy compressed buffer.
  ///
  /// This compatibility API is intentionally whole-buffer. Sequential page
  /// readers will use a separate cursor API and write directly into output.
  static BufferPtr decompress(
      std::string_view scheme,
      const BufferPtr& compressed,
      memory::MemoryPool& pool);
};

/// Monotonic, bounded-memory reader for one zstd-compressed Lance buffer.
/// It retains only the codec context, one compressed chunk, and a small tail
/// used by unaligned bitmap ranges. Requests outside that tail restart the
/// frame explicitly instead of retaining a decompressed page.
class NativeLanceZstdStream {
 public:
  using Read = std::function<BufferPtr(uint64_t offset, uint64_t length)>;

  NativeLanceZstdStream(
      uint64_t compressedOffset,
      uint64_t compressedLength,
      memory::MemoryPool& pool,
      Read read,
      uint64_t chunkBytes = 64 * 1024,
      uint64_t historyBytes = 64);
  ~NativeLanceZstdStream();

  NativeLanceZstdStream(const NativeLanceZstdStream&) = delete;
  NativeLanceZstdStream& operator=(const NativeLanceZstdStream&) = delete;

  BufferPtr decodeRange(uint64_t decodedOffset, uint64_t decodedLength);

  uint64_t decodedOffset() const;
  uint64_t restartCount() const;
  uint64_t retainedBytes() const;
  bool finished() const;
  std::optional<std::pair<uint64_t, uint64_t>> nextCompressedRange() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace bytedance::bolt::lance::reader
