#include "bolt/common/memory/bm/compress/CompressionManager.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"
#include "bolt/common/memory/bm/compress/CompressionContextPool.h"
#include "bolt/common/memory/bm/compress/CompressionRecord.h"
#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"
#include "bolt/common/time/Timer.h"

#include <cstring>
#include <memory>
#include <utility>

namespace bytedance::bolt::memory::bm::compress {
namespace {

CompressionRecordResult makeUncompressedRecord(std::span<const char> payload) {
  const auto rawSize = static_cast<uint64_t>(payload.size());
  auto record = AllocateSpillRecord(payload.size());
  FinalizeSpillRecord(record, CompressionKind::kNone, rawSize, rawSize);
  if (!payload.empty()) {
    std::memcpy(SpillRecordBody(record), payload.data(), payload.size());
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
  const auto storedKind =
      static_cast<CompressionKind>(header.compressionKind);
  const auto storedPayload =
      StoredPayloadSpan(record, header.headerSize, header.storedSize);

  auto rawPayload = AllocateDecodedPayload(outputPool, header.rawSize);
  if (storedKind == CompressionKind::kNone) {
    if (header.storedSize != header.rawSize) {
      BOLT_FAIL(
          "BM uncompressed spill payload size mismatch, stored_size={}, raw_size={}",
          header.storedSize,
          header.rawSize);
    }
    if (header.rawSize > 0) {
      std::memcpy(rawPayload.data(), storedPayload.data(), header.rawSize);
    }
    return rawPayload;
  }

  {
    MicrosecondTimer timer(decompressionTimeUs);
    DecompressWithAlgorithm(
        storedKind,
        storedPayload.data(),
        storedPayload.size(),
        rawPayload.data(),
        rawPayload.length());
  }
  return rawPayload;
}

} // namespace bytedance::bolt::memory::bm::compress
