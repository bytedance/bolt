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

struct Lz4DecompressionContext {
  Lz4DecompressionContext() = default;
  ~Lz4DecompressionContext();

  Lz4DecompressionContext(const Lz4DecompressionContext&) = delete;
  Lz4DecompressionContext& operator=(const Lz4DecompressionContext&) = delete;

  void* native{nullptr};
};

struct ZstdDecompressionContext {
  ZstdDecompressionContext() = default;
  ~ZstdDecompressionContext();

  ZstdDecompressionContext(const ZstdDecompressionContext&) = delete;
  ZstdDecompressionContext& operator=(const ZstdDecompressionContext&) = delete;

  void* native{nullptr};
};

struct SnappyDecompressionContext {
  SnappyDecompressionContext() = default;

  SnappyDecompressionContext(const SnappyDecompressionContext&) = delete;
  SnappyDecompressionContext& operator=(const SnappyDecompressionContext&) =
      delete;
};

struct CompressionContextSet {
  Lz4CompressionContext* lz4{nullptr};
  ZstdCompressionContext* zstd{nullptr};
};

struct DecompressionContextSet {
  Lz4DecompressionContext* lz4{nullptr};
  ZstdDecompressionContext* zstd{nullptr};
  SnappyDecompressionContext* snappy{nullptr};
};

} // namespace bytedance::bolt::memory::bm::compress
