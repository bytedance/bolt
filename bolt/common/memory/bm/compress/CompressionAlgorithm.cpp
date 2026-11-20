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

#include <folly/ScopeGuard.h>
#include <openzl/openzl.h>
#include <zstd.h>

namespace bytedance::bolt::memory::bm::compress {

bool SupportedCompressionKind(CompressionKind kind) {
  return kind == CompressionKind::kNone || kind == CompressionKind::kLz4Block ||
      kind == CompressionKind::kZstdFrame ||
      kind == CompressionKind::kSnappyRaw ||
      kind == CompressionKind::kOpenZlFrame;
}

void SetCompressionLevel(CompressionConfig& config, int32_t level) {
  switch (config.kind) {
    case CompressionKind::kNone:
      return;
    case CompressionKind::kZstdFrame:
      BOLT_USER_CHECK(
          level >= ZSTD_minCLevel() && level <= ZSTD_maxCLevel(),
          "Invalid BM Zstd compression level {}; expected between {} and {}",
          level,
          ZSTD_minCLevel(),
          ZSTD_maxCLevel());
      config.zstd.compressionLevel = level;
      return;
    case CompressionKind::kOpenZlFrame: {
      auto* compressor = ZL_Compressor_create();
      BOLT_CHECK_NOT_NULL(compressor);
      auto cleanup = folly::makeGuard([&] { ZL_Compressor_free(compressor); });
      const auto status = ZL_Compressor_setParameter(
          compressor, ZL_CParam_compressionLevel, level);
      BOLT_USER_CHECK(
          !ZL_isError(status), "Invalid BM OpenZL compression level {}", level);
      config.openZl.compressionLevel = level;
      return;
    }
    default:
      BOLT_USER_FAIL(
          "BM compression level is not supported for codec {}",
          static_cast<uint32_t>(config.kind));
  }
}

size_t MaxCompressedLength(CompressionKind kind, size_t rawSize) {
  switch (kind) {
    case CompressionKind::kLz4Block:
      return Lz4MaxCompressedLength(rawSize);
    case CompressionKind::kZstdFrame:
      return ZstdMaxCompressedLength(rawSize);
    case CompressionKind::kSnappyRaw:
      return SnappyMaxCompressedLength(rawSize);
    default:
      BOLT_FAIL("BM unsupported compression kind={}", static_cast<int>(kind));
  }
}

uint64_t CompressWithAlgorithm(
    const CompressionContextSet& contexts,
    CompressionKind kind,
    const CompressionConfig& config,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity) {
  switch (kind) {
    case CompressionKind::kLz4Block:
      return Lz4Compress(
          contexts.lz4, config.lz4, source, sourceSize, target, targetCapacity);
    case CompressionKind::kZstdFrame:
      return ZstdCompress(
          contexts.zstd,
          config.zstd,
          source,
          sourceSize,
          target,
          targetCapacity);
    case CompressionKind::kSnappyRaw:
      return SnappyCompress(
          config.snappy, source, sourceSize, target, targetCapacity);
    default:
      BOLT_FAIL("BM unsupported compression kind={}", static_cast<int>(kind));
  }
}

void DecompressWithAlgorithm(
    const DecompressionContextSet& contexts,
    CompressionKind kind,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize) {
  switch (kind) {
    case CompressionKind::kLz4Block:
      Lz4Decompress(source, sourceSize, target, targetSize);
      return;
    case CompressionKind::kZstdFrame:
      ZstdDecompress(contexts.zstd, source, sourceSize, target, targetSize);
      return;
    case CompressionKind::kSnappyRaw:
      SnappyDecompress(source, sourceSize, target, targetSize);
      return;
    default:
      BOLT_FAIL("BM unsupported compression kind={}", static_cast<int>(kind));
  }
}

} // namespace bytedance::bolt::memory::bm::compress
