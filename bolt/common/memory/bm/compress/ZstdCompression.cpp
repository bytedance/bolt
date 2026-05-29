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
  const auto written =
      ZSTD_compress(target, targetCapacity, source, sourceSize, compressionLevel);
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

ZSTD_CCtx* ensureZstdContext(CompressionAlgorithmContext* context) {
  if (context == nullptr) {
    return ZSTD_createCCtx();
  }
  if (context->zstdContext == nullptr) {
    context->zstdContext = ZSTD_createCCtx();
  }
  return static_cast<ZSTD_CCtx*>(context->zstdContext);
}

} // namespace

size_t ZstdMaxCompressedLength(size_t rawSize) {
  return ZSTD_compressBound(rawSize);
}

uint64_t ZstdCompress(
    CompressionAlgorithmContext* context,
    CompressionKind kind,
    int compressionLevel,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity) {
  switch (kind) {
    case CompressionKind::kZstd:
    case CompressionKind::kZstdOneShot:
      return zstdCompressOneShot(
          compressionLevel,
          source,
          sourceSize,
          target,
          targetCapacity);
    case CompressionKind::kZstdContext: {
      auto* zstdContext = ensureZstdContext(context);
      if (zstdContext == nullptr) {
        BOLT_FAIL("BM ZSTD context allocation failed");
      }
      const auto written = zstdCompressWithContext(
          zstdContext,
          compressionLevel,
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
          "BM unsupported ZSTD compression kind={}", static_cast<int>(kind));
  }
  BOLT_FAIL("BM unreachable ZSTD compression kind={}", static_cast<int>(kind));
}

void ZstdDecompress(
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize) {
  const auto read = ZSTD_decompress(target, targetSize, source, sourceSize);
  if (ZSTD_isError(read) || read != targetSize) {
    BOLT_FAIL(
        "BM ZSTD decompression failed, decoded_size={}, expected={}, error={}",
        ZSTD_isError(read) ? 0 : read,
        targetSize,
        ZSTD_isError(read) ? ZSTD_getErrorName(read) : "");
  }
}

void DestroyZstdContext(void* context) {
  if (context != nullptr) {
    ZSTD_freeCCtx(static_cast<ZSTD_CCtx*>(context));
  }
}

} // namespace bytedance::bolt::memory::bm::compress
