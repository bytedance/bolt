#pragma once

#include <cstddef>
#include <cstdint>

namespace bytedance::bolt::memory::bm::compress {

enum class CompressionKind : uint32_t {
  kNone = 0,
  kLz4 = 1,
  kLz4Default = 2,
  kLz4Fast = 3,
  kLz4Context = 4,
  kZstd = 10,
  kZstdOneShot = 11,
  kZstdContext = 12,
  kSnappy = 20,
  kSnappyRaw = 21,
  kSnappyLevel = 22,
};

struct CompressionConfig {
  CompressionKind kind{CompressionKind::kLz4};
  size_t minCompressBytes{256 * 1024};
  double minCompressionRatio{0.95};
  int compressionLevel{3};
};

} // namespace bytedance::bolt::memory::bm::compress
