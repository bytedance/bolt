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
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize) {
  if (!snappy::RawUncompress(source, sourceSize, target)) {
    BOLT_FAIL("BM Snappy decompression failed, source_size={}", sourceSize);
  }
}

} // namespace bytedance::bolt::memory::bm::compress
