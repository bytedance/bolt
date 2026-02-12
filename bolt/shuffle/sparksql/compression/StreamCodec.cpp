/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/shuffle/sparksql/compression/StreamCodec.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/compression/GzipStreamCodec.h"
#include "bolt/shuffle/sparksql/compression/Lz4FrameStreamCodec.h"
#include "bolt/shuffle/sparksql/compression/ZstdStreamCodec.h"

#include <fmt/format.h>

namespace bytedance::bolt::shuffle::sparksql {

std::string StreamCompressor::toString() const {
  return fmt::format("{}: {}", name(), options_.toString());
}

std::unique_ptr<StreamCompressor> StreamCompressor::create(
    CodecType type,
    const CodecOptions& options) {
  switch (type) {
    case CodecType::ZSTD:
      return std::make_unique<ZstdStreamCompressor>(options);
    case CodecType::GZIP:
      return std::make_unique<GzipStreamCompressor>(options);
    case CodecType::LZ4_FRAME:
      return std::make_unique<Lz4FrameStreamCompressor>(options);
    default:
      BOLT_CHECK(
          false,
          "Bolt shuffle codec: Streaming compression not supported for this codec type.");
  }
}

std::unique_ptr<StreamDecompressor> StreamDecompressor::create(
    CodecType type,
    const CodecOptions& options) {
  switch (type) {
    case CodecType::ZSTD:
      return std::make_unique<ZstdStreamDecompressor>(options);
    case CodecType::GZIP:
      return std::make_unique<GzipStreamDecompressor>(options);
    case CodecType::LZ4_FRAME:
      return std::make_unique<Lz4FrameStreamDecompressor>(options);
    default:
      BOLT_CHECK(
          false,
          "Bolt shuffle codec: Streaming decompression not supported for this codec type.");
  }
}

StreamCompressor::StreamCompressor(
    CodecType codecType,
    const CodecOptions& options)
    : codecType_(codecType), options_(options) {}

} // namespace bytedance::bolt::shuffle::sparksql
