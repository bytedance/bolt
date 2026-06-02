#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"

#include "bolt/common/base/Exceptions.h"

#include <lz4.h>

#include <limits>

namespace bytedance::bolt::memory::bm::compress {
namespace {

LZ4_stream_t* ensureLz4Context(Lz4CompressionContext* context) {
  if (context == nullptr) {
    return LZ4_createStream();
  }
  if (context->native == nullptr) {
    context->native = LZ4_createStream();
  }
  return static_cast<LZ4_stream_t*>(context->native);
}

} // namespace

size_t Lz4MaxCompressedLength(size_t rawSize) {
  return static_cast<size_t>(LZ4_compressBound(static_cast<int>(rawSize)));
}

uint64_t Lz4Compress(
    Lz4CompressionContext* context,
    const Lz4Options& options,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity) {
  if (sourceSize > static_cast<size_t>(std::numeric_limits<int>::max()) ||
      targetCapacity > static_cast<size_t>(std::numeric_limits<int>::max())) {
    BOLT_FAIL("BM LZ4 input too large, source_size={}", sourceSize);
  }

  int written = 0;
  switch (options.strategy) {
    case Lz4Strategy::kDefault:
      written = LZ4_compress_default(
          source,
          target,
          static_cast<int>(sourceSize),
          static_cast<int>(targetCapacity));
      break;
    case Lz4Strategy::kFast:
      written = LZ4_compress_fast(
          source,
          target,
          static_cast<int>(sourceSize),
          static_cast<int>(targetCapacity),
          options.acceleration > 0 ? options.acceleration : 1);
      break;
    case Lz4Strategy::kPooledContext: {
      auto* lz4Context = ensureLz4Context(context);
      if (lz4Context == nullptr) {
        BOLT_FAIL("BM LZ4 context allocation failed");
      }
      written = LZ4_compress_fast_extState(
          lz4Context,
          source,
          target,
          static_cast<int>(sourceSize),
          static_cast<int>(targetCapacity),
          options.acceleration > 0 ? options.acceleration : 1);
      if (context == nullptr) {
        LZ4_freeStream(lz4Context);
      }
      break;
    }
    default:
      BOLT_FAIL(
          "BM unsupported LZ4 strategy={}", static_cast<int>(options.strategy));
  }
  if (written <= 0) {
    BOLT_FAIL("BM LZ4 compression failed, source_size={}", sourceSize);
  }
  return static_cast<uint64_t>(written);
}

void Lz4Decompress(
    Lz4DecompressionContext* /*context*/,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize) {
  if (sourceSize > static_cast<size_t>(std::numeric_limits<int>::max()) ||
      targetSize > static_cast<size_t>(std::numeric_limits<int>::max())) {
    BOLT_FAIL("BM LZ4 compressed input too large, source_size={}", sourceSize);
  }
  const auto read = LZ4_decompress_safe(
      source,
      target,
      static_cast<int>(sourceSize),
      static_cast<int>(targetSize));
  if (read != static_cast<int>(targetSize)) {
    BOLT_FAIL(
        "BM LZ4 decompression failed, decoded_size={}, expected={}",
        read,
        targetSize);
  }
}

Lz4DecompressionContext::~Lz4DecompressionContext() = default;

Lz4CompressionContext::~Lz4CompressionContext() {
  if (native != nullptr) {
    LZ4_freeStream(static_cast<LZ4_stream_t*>(native));
  }
}

} // namespace bytedance::bolt::memory::bm::compress
