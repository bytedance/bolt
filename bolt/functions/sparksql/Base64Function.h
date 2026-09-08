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

#include "bolt/common/encode/Base64.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/functions/Macros.h"

namespace bytedance::bolt::functions::sparksql {

/// Encodes the input binary data into a Base64-encoded string. By default the
/// output is a single unchunked string, matching Spark <= 3.2 and Spark >=
/// 3.5.2 with spark.sql.chunkBase64String.enabled=false. When
/// spark.chunk_base64_string_enabled is true, the output is MIME-chunked into
/// 76-character lines separated by CRLF, matching Spark 3.3.0-3.5.1 and the
/// Spark >= 3.5.2 default.
template <typename T>
struct Base64Function {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varbinary>* /*input*/) {
    chunkOutput_ = config.sparkChunkBase64StringEnabled();
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    if (chunkOutput_) {
      result.resize(encoding::Base64::calculateMimeEncodedSize(input.size()));
      encoding::Base64::encodeMime(input.data(), input.size(), result.data());
    } else {
      result.resize(encoding::Base64::calculateEncodedSize(
          input.size(), /*withPadding=*/true));
      encoding::Base64::encode(input.data(), input.size(), result.data());
    }
  }

 private:
  bool chunkOutput_{false};
};
} // namespace bytedance::bolt::functions::sparksql
