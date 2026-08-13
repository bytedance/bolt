#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"

#include "bolt/common/base/Exceptions.h"

#include <zstd.h>

namespace bytedance::bolt::memory::bm::compress {
namespace {

uint64_t zstdCompressOneShot(
    int compressionLevel,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity) {
  const auto written = ZSTD_compress(
      target, targetCapacity, source, sourceSize, compressionLevel);
  if (ZSTD_isError(written)) {
    BOLT_FAIL(
        "BM ZSTD compression failed, source_size={}, error={}",
        sourceSize,
        ZSTD_getErrorName(written));
  }
  return static_cast<uint64_t>(written);
}

uint64_t zstdCompressWithContext(
    ZSTD_CCtx* context,
    int compressionLevel,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity) {
  if (context == nullptr) {
    BOLT_FAIL("BM ZSTD compression context is null");
  }
  const auto written = ZSTD_compressCCtx(
      context, target, targetCapacity, source, sourceSize, compressionLevel);
  if (ZSTD_isError(written)) {
    BOLT_FAIL(
        "BM ZSTD context compression failed, source_size={}, error={}",
        sourceSize,
        ZSTD_getErrorName(written));
  }
  return static_cast<uint64_t>(written);
}

ZSTD_CCtx* ensureZstdContext(ZstdCompressionContext* context) {
  if (context == nullptr) {
    return ZSTD_createCCtx();
  }
  if (context->native == nullptr) {
    context->native = ZSTD_createCCtx();
  }
  return static_cast<ZSTD_CCtx*>(context->native);
}

ZSTD_DCtx* ensureZstdDecompressionContext(ZstdDecompressionContext* context) {
  if (context == nullptr) {
    return ZSTD_createDCtx();
  }
  if (context->native == nullptr) {
    context->native = ZSTD_createDCtx();
  }
  return static_cast<ZSTD_DCtx*>(context->native);
}

} // namespace

size_t ZstdMaxCompressedLength(size_t rawSize) {
  return ZSTD_compressBound(rawSize);
}

uint64_t ZstdCompress(
    ZstdCompressionContext* context,
    const ZstdOptions& options,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity) {
  switch (options.strategy) {
    case ZstdStrategy::kOneShot:
      return zstdCompressOneShot(
          options.compressionLevel, source, sourceSize, target, targetCapacity);
    case ZstdStrategy::kPooledContext: {
      auto* zstdContext = ensureZstdContext(context);
      if (zstdContext == nullptr) {
        BOLT_FAIL("BM ZSTD context allocation failed");
      }
      const auto written = zstdCompressWithContext(
          zstdContext,
          options.compressionLevel,
          source,
          sourceSize,
          target,
          targetCapacity);
      if (context == nullptr) {
        ZSTD_freeCCtx(zstdContext);
      }
      return written;
    }
    default:
      BOLT_FAIL(
          "BM unsupported ZSTD strategy={}",
          static_cast<int>(options.strategy));
  }
  BOLT_FAIL(
      "BM unreachable ZSTD strategy={}", static_cast<int>(options.strategy));
}

void ZstdDecompress(
    ZstdDecompressionContext* context,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize) {
  auto* zstdContext = ensureZstdDecompressionContext(context);
  if (zstdContext == nullptr) {
    BOLT_FAIL("BM ZSTD decompression context allocation failed");
  }
  const auto read =
      ZSTD_decompressDCtx(zstdContext, target, targetSize, source, sourceSize);
  if (context == nullptr) {
    ZSTD_freeDCtx(zstdContext);
  }
  if (ZSTD_isError(read) || read != targetSize) {
    BOLT_FAIL(
        "BM ZSTD decompression failed, decoded_size={}, expected={}, error={}",
        ZSTD_isError(read) ? 0 : read,
        targetSize,
        ZSTD_isError(read) ? ZSTD_getErrorName(read) : "");
  }
}

ZstdDecompressionContext::~ZstdDecompressionContext() {
  if (native != nullptr) {
    ZSTD_freeDCtx(static_cast<ZSTD_DCtx*>(native));
  }
}

ZstdCompressionContext::~ZstdCompressionContext() {
  if (native != nullptr) {
    ZSTD_freeCCtx(static_cast<ZSTD_CCtx*>(native));
  }
}

} // namespace bytedance::bolt::memory::bm::compress
