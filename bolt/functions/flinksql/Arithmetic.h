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

#include "bolt/functions/Udf.h"
#include "bolt/functions/prestosql/ArithmeticImpl.h"

namespace bytedance::bolt::functions::flinksql {

template <typename T>
struct FlinkTruncateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const TInput& input) {
    call(result, input, 0);
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& input, const int32_t& scale) {
    result = functions::round<TInput, int32_t, true, true>(input, scale);
  }
};

} // namespace bytedance::bolt::functions::flinksql
