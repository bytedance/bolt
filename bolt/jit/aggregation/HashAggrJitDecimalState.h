#pragma once

#ifdef ENABLE_BOLT_JIT

#include <cstdint>
#include <type_traits>

#include "bolt/type/Type.h"

namespace bytedance::bolt::jit {

// JIT-internal decimal accumulator layouts shared only by decimal aggregate
// codegen and decimal extract runtime helpers. Keep them out of the framework
// planning/types header so non-decimal ops don't depend on aggregate-private
// row state details.
struct JitDecimalSumState {
  int128_t sum{0};
  int64_t overflow{0};
  bool isEmpty{true};
};

struct JitDecimalAvgState {
  int128_t sum{0};
  int64_t count{0};
  int64_t overflow{0};
};

static_assert(std::is_standard_layout_v<JitDecimalSumState>);
static_assert(std::is_standard_layout_v<JitDecimalAvgState>);

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
