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

#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"

#include "bolt/common/base/Exceptions.h"

#include <snappy.h>

#include <algorithm>

namespace bytedance::bolt::memory::bm::compress {

size_t SnappyMaxCompressedLength(size_t rawSize) {
  return snappy::MaxCompressedLength(rawSize);
}

uint64_t SnappyCompress(
    const SnappyOptions& options,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity) {
  size_t written = 0;
  switch (options.strategy) {
    case SnappyStrategy::kRaw:
      snappy::RawCompress(source, sourceSize, target, &written);
      return static_cast<uint64_t>(written);
    case SnappyStrategy::kWithOptions: {
      const auto level = std::clamp(
          options.compressionLevel,
          snappy::CompressionOptions::MinCompressionLevel(),
          snappy::CompressionOptions::MaxCompressionLevel());
      snappy::RawCompress(
          source,
          sourceSize,
          target,
          &written,
          snappy::CompressionOptions(level));
      return static_cast<uint64_t>(written);
    }
    default:
      BOLT_FAIL(
          "BM unsupported Snappy strategy={}",
          static_cast<int>(options.strategy));
  }
}

void SnappyDecompress(
    SnappyDecompressionContext* /*context*/,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize) {
  if (!snappy::RawUncompress(source, sourceSize, target)) {
    BOLT_FAIL("BM Snappy decompression failed, source_size={}", sourceSize);
  }
}

} // namespace bytedance::bolt::memory::bm::compress
