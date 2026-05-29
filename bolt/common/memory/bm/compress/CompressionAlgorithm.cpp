#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::memory::bm::compress {

void DestroyCompressionAlgorithmContext(CompressionAlgorithmContext& context) {
  DestroyLz4Context(context.lz4Context);
  context.lz4Context = nullptr;
  DestroyZstdContext(context.zstdContext);
  context.zstdContext = nullptr;
}

bool SupportedCompressionKind(CompressionKind kind) {
  return kind == CompressionKind::kNone || kind == CompressionKind::kLz4 ||
      kind == CompressionKind::kLz4Default ||
      kind == CompressionKind::kLz4Fast ||
      kind == CompressionKind::kLz4Context ||
      kind == CompressionKind::kZstd ||
      kind == CompressionKind::kZstdOneShot ||
      kind == CompressionKind::kZstdContext ||
      kind == CompressionKind::kSnappy ||
      kind == CompressionKind::kSnappyRaw ||
      kind == CompressionKind::kSnappyLevel;
}

size_t MaxCompressedLength(CompressionKind kind, size_t rawSize) {
  switch (kind) {
    case CompressionKind::kLz4:
    case CompressionKind::kLz4Default:
    case CompressionKind::kLz4Fast:
    case CompressionKind::kLz4Context:
      return Lz4MaxCompressedLength(rawSize);
    case CompressionKind::kZstd:
    case CompressionKind::kZstdOneShot:
    case CompressionKind::kZstdContext:
      return ZstdMaxCompressedLength(rawSize);
    case CompressionKind::kSnappy:
    case CompressionKind::kSnappyRaw:
    case CompressionKind::kSnappyLevel:
      return SnappyMaxCompressedLength(rawSize);
    default:
      BOLT_FAIL("BM unsupported compression kind={}", static_cast<int>(kind));
  }
}

uint64_t CompressWithAlgorithm(
    CompressionAlgorithmContext* context,
    CompressionKind kind,
    int compressionLevel,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity) {
  switch (kind) {
    case CompressionKind::kLz4:
    case CompressionKind::kLz4Default:
    case CompressionKind::kLz4Fast:
    case CompressionKind::kLz4Context:
      return Lz4Compress(
          context,
          kind,
          compressionLevel,
          source,
          sourceSize,
          target,
          targetCapacity);
    case CompressionKind::kZstd:
    case CompressionKind::kZstdOneShot:
    case CompressionKind::kZstdContext:
      return ZstdCompress(
          context,
          kind,
          compressionLevel,
          source,
          sourceSize,
          target,
          targetCapacity);
    case CompressionKind::kSnappy:
    case CompressionKind::kSnappyRaw:
    case CompressionKind::kSnappyLevel:
      return SnappyCompress(
          kind,
          compressionLevel,
          source,
          sourceSize,
          target,
          targetCapacity);
    default:
      BOLT_FAIL("BM unsupported compression kind={}", static_cast<int>(kind));
  }
}

void DecompressWithAlgorithm(
    CompressionKind kind,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize) {
  switch (kind) {
    case CompressionKind::kLz4:
    case CompressionKind::kLz4Default:
    case CompressionKind::kLz4Fast:
    case CompressionKind::kLz4Context:
      Lz4Decompress(source, sourceSize, target, targetSize);
      return;
    case CompressionKind::kZstd:
    case CompressionKind::kZstdOneShot:
    case CompressionKind::kZstdContext:
      ZstdDecompress(source, sourceSize, target, targetSize);
      return;
    case CompressionKind::kSnappy:
    case CompressionKind::kSnappyRaw:
    case CompressionKind::kSnappyLevel:
      SnappyDecompress(source, sourceSize, target, targetSize);
      return;
    default:
      BOLT_FAIL("BM unsupported compression kind={}", static_cast<int>(kind));
  }
}

} // namespace bytedance::bolt::memory::bm::compress
