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

#ifdef ENABLE_BOLT_JIT

#include <cstdint>
#include <type_traits>

#include "bolt/functions/lib/aggregates/DecimalAccumulatorLayout.h"

namespace bytedance::bolt::jit {

// JIT-internal decimal accumulator layouts. These alias the shared POD layout
// bases that the non-JIT accumulators (DecimalSum / LongDecimalWithOverflowState)
// also derive from, so the JIT and non-JIT in-memory layouts stay in sync by
// construction (no mirrored copy to drift). The codegen / extract runtime read
// fields via offsetof on these aliases.
using JitDecimalSumState = functions::aggregate::DecimalSumAccumulatorLayout;
using JitDecimalAvgState = functions::aggregate::LongDecimalWithOverflowLayout;

static_assert(std::is_standard_layout_v<JitDecimalSumState>);
static_assert(std::is_standard_layout_v<JitDecimalAvgState>);

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
