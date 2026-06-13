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
using JitDecimalSumState =
    bytedance::bolt::functions::aggregate::DecimalSumAccumulatorLayout;
using JitDecimalAvgState =
    bytedance::bolt::functions::aggregate::LongDecimalWithOverflowLayout;

static_assert(std::is_standard_layout_v<JitDecimalSumState>);
static_assert(std::is_standard_layout_v<JitDecimalAvgState>);

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
