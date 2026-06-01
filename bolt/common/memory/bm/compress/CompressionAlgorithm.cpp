#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::memory::bm::compress {

bool SupportedCompressionKind(CompressionKind kind) {
  return kind == CompressionKind::kNone ||
      kind == CompressionKind::kLz4Block ||
      kind == CompressionKind::kZstdFrame ||
      kind == CompressionKind::kSnappyRaw;
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
      Lz4Decompress(contexts.lz4, source, sourceSize, target, targetSize);
      return;
    case CompressionKind::kZstdFrame:
      ZstdDecompress(contexts.zstd, source, sourceSize, target, targetSize);
      return;
    case CompressionKind::kSnappyRaw:
      SnappyDecompress(contexts.snappy, source, sourceSize, target, targetSize);
      return;
    default:
      BOLT_FAIL("BM unsupported compression kind={}", static_cast<int>(kind));
  }
}

} // namespace bytedance::bolt::memory::bm::compress
