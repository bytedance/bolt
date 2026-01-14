/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <fmt/format.h>
#include <folly/compression/Compression.h>
#include <string>

namespace bytedance::bolt::common {

enum CompressionKind {
  CompressionKind_NONE = 0,
  CompressionKind_ZLIB = 1,
  CompressionKind_SNAPPY = 2,
  CompressionKind_LZO = 3,
  CompressionKind_ZSTD = 4,
  CompressionKind_LZ4 = 5,
  CompressionKind_GZIP = 6,
  CompressionKind_MAX = INT64_MAX
};

std::unique_ptr<folly::io::Codec> compressionKindToCodec(CompressionKind kind);

CompressionKind codecTypeToCompressionKind(folly::io::CodecType type);

/**
 * Get the name of the CompressionKind.
 */
std::string compressionKindToString(CompressionKind kind);

CompressionKind stringToCompressionKind(const std::string& kind);

constexpr uint64_t DEFAULT_COMPRESSION_BLOCK_SIZE = 256 * 1024;

} // namespace bytedance::bolt::common

template <>
struct fmt::formatter<bytedance::bolt::common::CompressionKind>
    : fmt::formatter<std::string> {
  auto format(
      const bytedance::bolt::common::CompressionKind& s,
      format_context& ctx) const {
    return formatter<std::string>::format(
        bytedance::bolt::common::compressionKindToString(s), ctx);
  }
};
