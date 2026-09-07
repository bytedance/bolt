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

#include <cstddef>
#include <cstdint>

namespace bytedance::bolt::memory::bm::compress {

enum class CompressionKind : uint32_t {
  kNone = 0,
  kLz4Block = 1,
  kZstdFrame = 2,
  kSnappyRaw = 3,
};

enum class Lz4Strategy : uint8_t {
  kDefault,
  kFast,
  kPooledContext,
};

struct Lz4Options {
  Lz4Strategy strategy{Lz4Strategy::kDefault};
  int acceleration{1};
};

enum class ZstdStrategy : uint8_t {
  kOneShot,
  kPooledContext,
};

struct ZstdOptions {
  ZstdStrategy strategy{ZstdStrategy::kOneShot};
  int compressionLevel{3};
};

enum class SnappyStrategy : uint8_t {
  kRaw,
  kWithOptions,
};

struct SnappyOptions {
  SnappyStrategy strategy{SnappyStrategy::kRaw};
  int compressionLevel{0};
};

struct CompressionConfig {
  CompressionKind kind{CompressionKind::kLz4Block};
  size_t minCompressBytes{256 * 1024};
  Lz4Options lz4;
  ZstdOptions zstd;
  SnappyOptions snappy;
};

} // namespace bytedance::bolt::memory::bm::compress
