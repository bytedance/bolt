/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

// Decimal-specific JIT codegen helper shared across decimal ops translation
// units. It lives with the decimal ops (rather than on the framework
// HashAggrJitCodegen) so decimal knowledge stays out of the generic framework,
// and is declared here because it is defined in DecimalSumOps.cpp but also used
// by DecimalAvgOps.cpp. Extract helpers that are used within a single TU stay
// file-local in their respective ops files.
namespace bytedance::bolt::jit {

// Inline i128 accumulate-with-overflow used by decimal sum/avg add+merge.
// Loads the i128 sum at 'group + sumOffset' and the i64 overflow counter at
// 'group + overflowOffset', computes sum += addend, updates the overflow
// counter by the carry direction, and stores both back.
void emitDecimalAddWithOverflow(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    int32_t sumOffset,
    int32_t overflowOffset,
    llvm::Value* addend);

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
