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
#pragma once

#include <cstdint>

#include "bolt/functions/Macros.h"
#include "bolt/functions/sparksql/BitmapUtil.h"

namespace bytedance::bolt::functions::sparksql {

/// Spark-compatible bitmap_count(BINARY) -> BIGINT.
/// Matches Spark 3.5 BitmapExpressionUtils.bitmapCount.
template <typename T>
struct BitmapCountFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varbinary>& input) {
    result = bitmapPopcount(input.data(), input.size());
  }
};

} // namespace bytedance::bolt::functions::sparksql
