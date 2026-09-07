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

#include "bolt/common/memory/bm/compress/CompressionManager.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/common/caching/SimpleLRUCache.h"
#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"
#include "bolt/common/memory/bm/compress/CompressionContextPool.h"
#include "bolt/common/memory/bm/compress/CompressionRecord.h"
#include "bolt/common/memory/bm/compress/OpenZlCompression.h"
#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"
#include "bolt/common/time/Timer.h"

#include <folly/Synchronized.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace bytedance::bolt::memory::bm::compress {
namespace {

int32_t checkedSimdCopyBytes(uint64_t bytes, const char* context) {
  BOLT_CHECK_LE(
      bytes,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
      "BM raw spill {} payload is too large for simd::memcpy, bytes={}, max={}",
      context,
      bytes,
      std::numeric_limits<int32_t>::max());
  return static_cast<int32_t>(bytes);
}

CompressionRecordResult makeUncompressedRecord(std::span<const char> payload) {
  const auto rawSize = static_cast<uint64_t>(payload.size());
  const auto copyBytes = checkedSimdCopyBytes(rawSize, "write");
  auto record = AllocateSpillRecord(payload.size());
  FinalizeSpillRecord(record, CompressionKind::kNone, rawSize, rawSize);
  if (copyBytes > 0) {
    simd::memcpy(SpillRecordBody(record), payload.data(), copyBytes);
  }

  CompressionRecordResult result;
  result.record = std::move(record);
  result.rawSize = rawSize;
  result.physicalSize = SpillRecordHeaderSize() + rawSize;
  result.storedKind = CompressionKind::kNone;
  return result;
}

} // namespace

struct CompressionManager::Impl {
  using OpenZlGraphCache = bytedance::bolt::
      SimpleLRUCache<std::string, std::shared_ptr<const OpenZlCompressor>>;

  explicit Impl(CompressionConfig inputConfig)
      : config(std::move(inputConfig)),
        openZlGraphCache(
            folly::in_place,
            std::max<size_t>(1, config.openZl.graphCacheCapacity)) {}

  CompressionConfig config;
  folly::Synchronized<OpenZlGraphCache> openZlGraphCache;
  CompressionContextPool<Lz4CompressionContext> lz4Contexts;
  CompressionContextPool<ZstdCompressionContext> zstdContexts;
  CompressionContextPool<OpenZlCompressionContext> openZlContexts;
  CompressionContextPool<Lz4DecompressionContext> lz4DecodeContexts;
  CompressionContextPool<ZstdDecompressionContext> zstdDecodeContexts;
  CompressionContextPool<SnappyDecompressionContext> snappyDecodeContexts;
  CompressionContextPool<OpenZlDecompressionContext> openZlDecodeContexts;
};

CompressionManager::CompressionManager(CompressionConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {
  BOLT_CHECK(SupportedCompressionKind(impl_->config.kind));
}

CompressionManager::~CompressionManager() = default;

CompressionRecordResult CompressionManager::BuildSpillRecord(
    std::span<const char> payload,
    const BlockDescriptor* descriptor) {
  if (impl_->config.kind == CompressionKind::kNone ||
      payload.size() < impl_->config.minCompressBytes || payload.empty()) {
    return makeUncompressedRecord(payload);
  }

  const auto rawSize = static_cast<uint64_t>(payload.size());
  auto selectedKind = impl_->config.kind;
  if (selectedKind == CompressionKind::kOpenZlFrame &&
      (descriptor == nullptr ||
       !OpenZlSupportsDescriptor(*descriptor, payload.size()))) {
    selectedKind = CompressionKind::kZstdFrame;
  }

  if (selectedKind == CompressionKind::kOpenZlFrame) {
    uint64_t compressionTimeUs = 0;
    uint64_t storedSize = 0;
    IoBuffer record;
    {
      MicrosecondTimer timer(&compressionTimeUs);
      auto fingerprint =
          OpenZlDescriptorFingerprint(*descriptor, payload.size());
      std::shared_ptr<const OpenZlCompressor> compressor;
      if (impl_->config.openZl.graphCacheCapacity == 0) {
        compressor = BuildOpenZlCompressor(
            *descriptor, payload.size(), std::move(fingerprint));
      } else {
        auto cache = impl_->openZlGraphCache.wlock();
        auto cached = cache->get(fingerprint);
        if (cached.has_value()) {
          compressor = std::move(*cached);
        } else {
          compressor =
              BuildOpenZlCompressor(*descriptor, payload.size(), fingerprint);
          cache->add(fingerprint, compressor);
        }
      }

      const auto maxRepresentableOutput =
          std::numeric_limits<size_t>::max() - SpillRecordHeaderSize();
      const auto configuredMaxOutput = impl_->config.openZl.maxOutputBytes == 0
          ? maxRepresentableOutput
          : std::min(
                impl_->config.openZl.maxOutputBytes, maxRepresentableOutput);
      auto capacity = std::min(
          OpenZlInitialCompressedCapacity(payload.size()), configuredMaxOutput);
      record = AllocateSpillRecord(capacity);
      auto context = impl_->openZlContexts.Acquire();
      const auto result = OpenZlCompress(
          *context.get(),
          *compressor,
          payload.data(),
          payload.size(),
          SpillRecordBody(record),
          capacity);
      BOLT_CHECK(
          !result.capacityTooSmall,
          "BM OpenZL initial output capacity is too small, openzl_error=dstCapacity_tooSmall, schema={}, raw_size={}, capacity={}, max_output_bytes={}",
          compressor->fingerprint,
          payload.size(),
          capacity,
          impl_->config.openZl.maxOutputBytes);
      storedSize = result.size;
    }

    FinalizeSpillRecord(
        record, CompressionKind::kOpenZlFrame, rawSize, storedSize);
    CompressionRecordResult result;
    result.record = std::move(record);
    result.rawSize = rawSize;
    result.physicalSize = SpillRecordHeaderSize() + storedSize;
    result.storedKind = CompressionKind::kOpenZlFrame;
    result.compressionTimeUs = compressionTimeUs;
    result.compressed = true;
    return result;
  }

  const auto capacity = MaxCompressedLength(selectedKind, payload.size());
  // TODO: This allocates a fresh malloc-backed spill record for every write.
  // Consider a malloc-backed reusable buffer pool here, but do not allocate
  // from MemoryPool because spill itself can be triggered under MemoryPool
  // pressure and pool allocation may recurse back into spill.
  auto record = AllocateSpillRecord(capacity);

  CompressionContextSet contexts;
  CompressionContextPool<Lz4CompressionContext>::Ref lz4Context;
  CompressionContextPool<ZstdCompressionContext>::Ref zstdContext;
  if (selectedKind == CompressionKind::kLz4Block) {
    lz4Context = impl_->lz4Contexts.Acquire();
    contexts.lz4 = lz4Context.get();
  } else if (selectedKind == CompressionKind::kZstdFrame) {
    zstdContext = impl_->zstdContexts.Acquire();
    contexts.zstd = zstdContext.get();
  }

  uint64_t compressionTimeUs = 0;
  uint64_t storedSize = 0;
  {
    MicrosecondTimer timer(&compressionTimeUs);
    storedSize = CompressWithAlgorithm(
        contexts,
        selectedKind,
        impl_->config,
        payload.data(),
        payload.size(),
        SpillRecordBody(record),
        capacity);
  }

  FinalizeSpillRecord(record, selectedKind, rawSize, storedSize);

  CompressionRecordResult result;
  result.record = std::move(record);
  result.rawSize = rawSize;
  result.physicalSize = SpillRecordHeaderSize() + storedSize;
  result.storedKind = selectedKind;
  result.compressionTimeUs = compressionTimeUs;
  result.compressed = true;
  return result;
}

IoBuffer CompressionManager::DecodeSpillRecord(
    std::span<const char> record,
    uint64_t expectedRawSize,
    MemoryPool* outputPool,
    uint64_t* decompressionTimeUs,
    uint64_t blockId,
    CompressionKind* storedKindOut) {
  const auto header =
      DecodeSpillRecordHeader(record.data(), record.size(), expectedRawSize);
  const auto storedKind = static_cast<CompressionKind>(header.compressionKind);
  if (storedKindOut != nullptr) {
    *storedKindOut = storedKind;
  }
  const auto storedPayload =
      StoredPayloadSpan(record, header.headerSize, header.storedSize);

  std::optional<int32_t> rawCopyBytes;
  if (storedKind == CompressionKind::kNone) {
    if (header.storedSize != header.rawSize) {
      BOLT_FAIL(
          "BM uncompressed spill payload size mismatch, stored_size={}, raw_size={}",
          header.storedSize,
          header.rawSize);
    }
    rawCopyBytes = checkedSimdCopyBytes(header.rawSize, "read");
  }

  auto rawPayload = AllocateDecodedPayload(outputPool, header.rawSize);
  if (storedKind == CompressionKind::kNone) {
    if (*rawCopyBytes > 0) {
      simd::memcpy(rawPayload.data(), storedPayload.data(), *rawCopyBytes);
    }
    return rawPayload;
  }

  {
    MicrosecondTimer timer(decompressionTimeUs);
    DecompressionContextSet contexts;
    CompressionContextPool<Lz4DecompressionContext>::Ref lz4Context;
    CompressionContextPool<ZstdDecompressionContext>::Ref zstdContext;
    CompressionContextPool<SnappyDecompressionContext>::Ref snappyContext;
    CompressionContextPool<OpenZlDecompressionContext>::Ref openZlContext;
    if (storedKind == CompressionKind::kLz4Block) {
      lz4Context = impl_->lz4DecodeContexts.Acquire();
      contexts.lz4 = lz4Context.get();
    } else if (storedKind == CompressionKind::kZstdFrame) {
      zstdContext = impl_->zstdDecodeContexts.Acquire();
      contexts.zstd = zstdContext.get();
    } else if (storedKind == CompressionKind::kSnappyRaw) {
      snappyContext = impl_->snappyDecodeContexts.Acquire();
      contexts.snappy = snappyContext.get();
    } else if (storedKind == CompressionKind::kOpenZlFrame) {
      openZlContext = impl_->openZlDecodeContexts.Acquire();
      OpenZlDecompress(
          *openZlContext.get(),
          storedPayload.data(),
          storedPayload.size(),
          rawPayload.data(),
          rawPayload.length(),
          blockId);
      return rawPayload;
    }
    DecompressWithAlgorithm(
        contexts,
        storedKind,
        storedPayload.data(),
        storedPayload.size(),
        rawPayload.data(),
        rawPayload.length());
  }
  return rawPayload;
}

} // namespace bytedance::bolt::memory::bm::compress
