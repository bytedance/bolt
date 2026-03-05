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

// Partially inspired and adapted from Apache Arrow.

#pragma once

#include <string_view>

#include <fmt/format.h>
#include <fmt/ostream.h>

namespace bytedance::bolt::parquet {

struct ParquetCipher {
  enum type { AES_GCM_V1 = 0, AES_GCM_CTR_V1 = 1 };
};

} // namespace bytedance::bolt::parquet

template <>
struct fmt::formatter<bytedance::bolt::parquet::ParquetCipher::type>
    : fmt::formatter<std::string_view> {
  auto format(
      bytedance::bolt::parquet::ParquetCipher::type cipherType,
      format_context& ctx) const {
    switch (cipherType) {
      case bytedance::bolt::parquet::ParquetCipher::type::AES_GCM_V1:
        return fmt::formatter<std::string_view>::format("AES_GCM_V1", ctx);
      case bytedance::bolt::parquet::ParquetCipher::type::AES_GCM_CTR_V1:
        return fmt::formatter<std::string_view>::format("AES_GCM_CTR_V1", ctx);
      default:
        return fmt::format_to(
            ctx.out(), "unknown[{}]", static_cast<int>(cipherType));
    }
  }
};
