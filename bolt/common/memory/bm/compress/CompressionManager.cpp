#include "bolt/common/memory/bm/compress/CompressionManager.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"
#include "bolt/common/memory/bm/compress/CompressionContextPool.h"
#include "bolt/common/memory/bm/compress/CompressionRecord.h"
#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"
#include "bolt/common/time/Timer.h"

#include <limits>
#include <memory>
#include <optional>
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
  explicit Impl(CompressionConfig config) : config(std::move(config)) {}

  CompressionContextPool<Lz4CompressionContext> lz4Contexts;
  CompressionContextPool<ZstdCompressionContext> zstdContexts;
  CompressionContextPool<Lz4DecompressionContext> lz4DecodeContexts;
  CompressionContextPool<ZstdDecompressionContext> zstdDecodeContexts;
  CompressionContextPool<SnappyDecompressionContext> snappyDecodeContexts;
  CompressionConfig config;
};

CompressionManager::CompressionManager(CompressionConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {
  BOLT_CHECK(SupportedCompressionKind(impl_->config.kind));
}

CompressionManager::~CompressionManager() = default;

CompressionRecordResult CompressionManager::BuildSpillRecord(
    std::span<const char> payload) {
  if (impl_->config.kind == CompressionKind::kNone ||
      payload.size() < impl_->config.minCompressBytes || payload.empty()) {
    return makeUncompressedRecord(payload);
  }

  const auto rawSize = static_cast<uint64_t>(payload.size());
  const auto capacity = MaxCompressedLength(impl_->config.kind, payload.size());
  // TODO: This allocates a fresh malloc-backed spill record for every write.
  // Consider a malloc-backed reusable buffer pool here, but do not allocate
  // from MemoryPool because spill itself can be triggered under MemoryPool
  // pressure and pool allocation may recurse back into spill.
  auto record = AllocateSpillRecord(capacity);

  CompressionContextSet contexts;
  CompressionContextPool<Lz4CompressionContext>::Ref lz4Context;
  CompressionContextPool<ZstdCompressionContext>::Ref zstdContext;
  if (impl_->config.kind == CompressionKind::kLz4Block) {
    lz4Context = impl_->lz4Contexts.Acquire();
    contexts.lz4 = lz4Context.get();
  } else if (impl_->config.kind == CompressionKind::kZstdFrame) {
    zstdContext = impl_->zstdContexts.Acquire();
    contexts.zstd = zstdContext.get();
  }

  uint64_t compressionTimeUs = 0;
  uint64_t storedSize = 0;
  {
    MicrosecondTimer timer(&compressionTimeUs);
    storedSize = CompressWithAlgorithm(
        contexts,
        impl_->config.kind,
        impl_->config,
        payload.data(),
        payload.size(),
        SpillRecordBody(record),
        capacity);
  }

  FinalizeSpillRecord(record, impl_->config.kind, rawSize, storedSize);

  CompressionRecordResult result;
  result.record = std::move(record);
  result.rawSize = rawSize;
  result.physicalSize = SpillRecordHeaderSize() + storedSize;
  result.storedKind = impl_->config.kind;
  result.compressionTimeUs = compressionTimeUs;
  result.compressed = true;
  return result;
}

IoBuffer CompressionManager::DecodeSpillRecord(
    std::span<const char> record,
    uint64_t expectedRawSize,
    MemoryPool* outputPool,
    uint64_t* decompressionTimeUs) {
  const auto header =
      DecodeSpillRecordHeader(record.data(), record.size(), expectedRawSize);
  const auto storedKind = static_cast<CompressionKind>(header.compressionKind);
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
    if (storedKind == CompressionKind::kLz4Block) {
      lz4Context = impl_->lz4DecodeContexts.Acquire();
      contexts.lz4 = lz4Context.get();
    } else if (storedKind == CompressionKind::kZstdFrame) {
      zstdContext = impl_->zstdDecodeContexts.Acquire();
      contexts.zstd = zstdContext.get();
    } else if (storedKind == CompressionKind::kSnappyRaw) {
      snappyContext = impl_->snappyDecodeContexts.Acquire();
      contexts.snappy = snappyContext.get();
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
