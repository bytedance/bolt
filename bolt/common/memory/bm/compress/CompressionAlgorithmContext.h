#pragma once

namespace bytedance::bolt::memory::bm::compress {

struct Lz4CompressionContext {
  Lz4CompressionContext() = default;
  ~Lz4CompressionContext();

  Lz4CompressionContext(const Lz4CompressionContext&) = delete;
  Lz4CompressionContext& operator=(const Lz4CompressionContext&) = delete;

  void* native{nullptr};
};

struct ZstdCompressionContext {
  ZstdCompressionContext() = default;
  ~ZstdCompressionContext();

  ZstdCompressionContext(const ZstdCompressionContext&) = delete;
  ZstdCompressionContext& operator=(const ZstdCompressionContext&) = delete;

  void* native{nullptr};
};

struct CompressionContextSet {
  Lz4CompressionContext* lz4{nullptr};
  ZstdCompressionContext* zstd{nullptr};
};

} // namespace bytedance::bolt::memory::bm::compress
