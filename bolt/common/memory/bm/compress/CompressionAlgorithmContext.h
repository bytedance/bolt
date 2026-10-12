/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

typedef union LZ4_stream_u LZ4_stream_t;
typedef struct ZSTD_CCtx_s ZSTD_CCtx;
typedef struct ZSTD_DCtx_s ZSTD_DCtx;

namespace bytedance::bolt::memory::bm::compress {

struct Lz4CompressionContext {
  Lz4CompressionContext() = default;
  ~Lz4CompressionContext();

  Lz4CompressionContext(const Lz4CompressionContext&) = delete;
  Lz4CompressionContext& operator=(const Lz4CompressionContext&) = delete;

  LZ4_stream_t* native{nullptr};
};

struct ZstdCompressionContext {
  ZstdCompressionContext() = default;
  ~ZstdCompressionContext();

  ZstdCompressionContext(const ZstdCompressionContext&) = delete;
  ZstdCompressionContext& operator=(const ZstdCompressionContext&) = delete;

  ZSTD_CCtx* native{nullptr};
};

struct ZstdDecompressionContext {
  ZstdDecompressionContext() = default;
  ~ZstdDecompressionContext();

  ZstdDecompressionContext(const ZstdDecompressionContext&) = delete;
  ZstdDecompressionContext& operator=(const ZstdDecompressionContext&) = delete;

  ZSTD_DCtx* native{nullptr};
};

struct CompressionContextSet {
  Lz4CompressionContext* lz4{nullptr};
  ZstdCompressionContext* zstd{nullptr};
};

struct DecompressionContextSet {
  ZstdDecompressionContext* zstd{nullptr};
};

} // namespace bytedance::bolt::memory::bm::compress
