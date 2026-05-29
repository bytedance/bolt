#include "bolt/common/memory/bm/compress/CompressionCodec.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"
#include "bolt/common/time/Timer.h"

#include <cstring>
#include <memory>
#include <mutex>

namespace bytedance::bolt::memory::bm::compress {
namespace {

IoBuffer allocateBuffer(MemoryPool* pool, size_t size) {
  if (pool != nullptr) {
    return IoBuffer::allocateFromPool(pool, size);
  }
  return IoBuffer{std::make_unique<char[]>(size), size, 0, size};
}

void decompressInto(
    CompressionKind kind,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize) {
  switch (kind) {
    case CompressionKind::kNone:
      if (sourceSize != targetSize) {
        BOLT_FAIL(
            "BM uncompressed spill payload size mismatch, source_size={}, target_size={}",
            sourceSize,
            targetSize);
      }
      std::memcpy(target, source, targetSize);
      return;
    case CompressionKind::kLz4:
    case CompressionKind::kLz4Default:
    case CompressionKind::kLz4Fast:
    case CompressionKind::kLz4Context:
    case CompressionKind::kZstd:
    case CompressionKind::kZstdOneShot:
    case CompressionKind::kZstdContext:
    case CompressionKind::kSnappy:
    case CompressionKind::kSnappyRaw:
    case CompressionKind::kSnappyLevel:
      DecompressWithAlgorithm(kind, source, sourceSize, target, targetSize);
      return;
    default:
      BOLT_FAIL("BM unsupported compression kind={}", static_cast<int>(kind));
  }
}

} // namespace

struct CompressionCodec::Impl {
  ~Impl() {
    DestroyCompressionAlgorithmContext(context);
  }

  CompressionAlgorithmContext context;
  std::mutex mutex;
};

CompressionCodec::CompressionCodec() : impl_(std::make_unique<Impl>()) {}

CompressionCodec::~CompressionCodec() = default;

CompressResult CompressionCodec::TryCompress(
    IoBuffer payload,
    const CompressionConfig& config,
    MemoryPool* pool) {
  BOLT_CHECK(payload.valid());
  BOLT_CHECK_GE(config.minCompressionRatio, 0.0);
  BOLT_CHECK_LE(config.minCompressionRatio, 1.0);
  BOLT_CHECK(SupportedCompressionKind(config.kind));

  CompressResult result;
  result.rawSize = payload.length();
  result.storedSize = payload.length();

  if (config.kind == CompressionKind::kNone ||
      payload.length() < config.minCompressBytes || payload.length() == 0) {
    result.buffer = std::move(payload);
    return result;
  }

  const auto capacity = MaxCompressedLength(config.kind, payload.length());
  auto compressed = allocateBuffer(pool, capacity);

  uint64_t compressionTimeUs = 0;
  uint64_t storedSize = 0;
  {
    MicrosecondTimer timer(&compressionTimeUs);
    std::lock_guard<std::mutex> lock(impl_->mutex);
    storedSize = CompressWithAlgorithm(
        &impl_->context,
        config.kind,
        config.compressionLevel,
        payload.data(),
        payload.length(),
        compressed.data(),
        compressed.length());
  }

  result.compressionTimeUs = compressionTimeUs;
  if (static_cast<double>(storedSize) >=
      static_cast<double>(payload.length()) * config.minCompressionRatio) {
    result.buffer = std::move(payload);
    result.storedSize = result.rawSize;
    return result;
  }

  result.buffer = std::move(compressed);
  result.storedSize = storedSize;
  result.storedKind = config.kind;
  result.compressed = true;
  return result;
}

CompressResult TryCompress(
    IoBuffer payload,
    const CompressionConfig& config,
    MemoryPool* pool) {
  CompressionCodec codec;
  return codec.TryCompress(std::move(payload), config, pool);
}

IoBuffer Decompress(
    IoBuffer storedPayload,
    uint64_t rawSize,
    uint64_t storedSize,
    CompressionKind storedKind,
    MemoryPool* pool,
    uint64_t* decompressionTimeUs) {
  BOLT_CHECK(storedPayload.valid());
  BOLT_CHECK_LE(storedSize, storedPayload.length());
  BOLT_CHECK(SupportedCompressionKind(storedKind));

  if (storedKind == CompressionKind::kNone) {
    if (storedSize != rawSize) {
      BOLT_FAIL(
          "BM uncompressed spill payload size mismatch, stored_size={}, raw_size={}",
          storedSize,
          rawSize);
    }
    return std::move(storedPayload);
  }

  auto rawPayload = allocateBuffer(pool, rawSize);
  {
    MicrosecondTimer timer(decompressionTimeUs);
    decompressInto(
        storedKind,
        storedPayload.data(),
        storedSize,
        rawPayload.data(),
        rawPayload.length());
  }
  return rawPayload;
}

} // namespace bytedance::bolt::memory::bm::compress
