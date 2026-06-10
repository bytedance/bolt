/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

// Decimal sum/avg extract runtime helpers called from JIT-generated HashAggr
// extract IR. They read the JIT decimal accumulator (JitDecimalSumState /
// JitDecimalAvgState) from 'group + offset', apply overflow / precision
// adjustment and write the result into the output vector. Resolved by the ORC
// JIT through the process global symbol table, so they only need default
// visibility and to be linked into the host process. These mirror the
// per-aggregate computeFinalValue logic but depend only on the type-layer
// DecimalUtil, so they live next to the other HashAggr runtime helpers.

#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJitTypes.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace {

// Mirrors DecimalSumAggregate::computeFinalValue: applies overflow adjustment
// and reports whether the value overflows the result precision range.
bytedance::bolt::int128_t jitDecimalSumComputeFinal(
    const bytedance::bolt::jit::JitDecimalSumState* state,
    int32_t precision,
    bool& overflow) {
  using bytedance::bolt::DecimalUtil;
  bytedance::bolt::int128_t sum = state->sum;
  if ((state->overflow == 1 && state->sum < 0) ||
      (state->overflow == -1 && state->sum > 0)) {
    sum = static_cast<bytedance::bolt::int128_t>(
        DecimalUtil::kOverflowMultiplier * state->overflow + state->sum);
  } else if (state->overflow != 0) {
    overflow = true;
    return 0;
  }
  overflow = !DecimalUtil::valueInPrecisionRange(sum, precision);
  return sum;
}

} // namespace

extern "C" {

// Final decimal sum extract: write FlatVector<int128_t>. Null when the group is
// empty (all inputs null) or the sum overflows the result precision.
__attribute__((__visibility__("default"))) void
jit_HashAggrExtractFinalDecimalSum(
    char* vector,
    int32_t row,
    char* group,
    int32_t offset,
    int32_t precision,
    int32_t /*scale*/,
    int8_t /*longDecimal*/) {
  auto* state =
      reinterpret_cast<bytedance::bolt::jit::JitDecimalSumState*>(group + offset);
  auto* flat = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                   ->as<bytedance::bolt::FlatVector<bytedance::bolt::int128_t>>();
  if (state->isEmpty) {
    flat->setNull(row, true);
    return;
  }
  bool overflow = false;
  auto result = jitDecimalSumComputeFinal(state, precision, overflow);
  if (overflow) {
    flat->setNull(row, true);
  } else {
    flat->set(row, result);
  }
}

// Partial decimal sum extract: write row(sum:decimal, isEmpty:bool).
__attribute__((__visibility__("default"))) void
jit_HashAggrExtractPartialDecimalSum(
    char* vector,
    int32_t row,
    char* group,
    int32_t offset,
    int32_t precision,
    int32_t /*scale*/,
    int8_t /*longDecimal*/) {
  auto* state =
      reinterpret_cast<bytedance::bolt::jit::JitDecimalSumState*>(group + offset);
  auto* rowVector = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                        ->as<bytedance::bolt::RowVector>();
  auto* sumVector =
      rowVector->childAt(0)->asFlatVector<bytedance::bolt::int128_t>();
  auto* isEmptyVector = rowVector->childAt(1)->asFlatVector<bool>();
  rowVector->setNull(row, false);
  if (state->isEmpty) {
    sumVector->set(row, 0);
    isEmptyVector->set(row, true);
    return;
  }
  bool overflow = false;
  auto result = jitDecimalSumComputeFinal(state, precision, overflow);
  if (overflow) {
    sumVector->setNull(row, true);
    isEmptyVector->set(row, false);
  } else {
    sumVector->set(row, result);
    isEmptyVector->set(row, state->isEmpty);
  }
}

// Partial decimal avg extract: write row(sum:decimal, count:bigint).
// Overflow during sum adjustment -> sum child set to null, count kept.
// (Final decimal avg extract stays on the non-JIT path; the rescale logic is
// too coupled to per-aggregate precision metadata.)
__attribute__((__visibility__("default"))) void
jit_HashAggrExtractPartialDecimalAvg(
    char* vector,
    int32_t row,
    char* group,
    int32_t offset,
    int32_t /*precision*/,
    int32_t /*scale*/,
    int8_t /*longDecimal*/) {
  auto* state =
      reinterpret_cast<bytedance::bolt::jit::JitDecimalAvgState*>(group + offset);
  auto* rowVector = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                        ->as<bytedance::bolt::RowVector>();
  auto* sumVector =
      rowVector->childAt(0)->asFlatVector<bytedance::bolt::int128_t>();
  auto* countVector = rowVector->childAt(1)->asFlatVector<int64_t>();
  rowVector->setNull(row, false);
  countVector->set(row, state->count);
  std::optional<bytedance::bolt::int128_t> adjustedSum =
      bytedance::bolt::DecimalUtil::adjustSumForOverflow(
          state->sum, state->overflow);
  if (adjustedSum.has_value()) {
    sumVector->set(row, adjustedSum.value());
  } else {
    sumVector->setNull(row, true);
  }
}

// Final decimal avg extract is intentionally not implemented in JIT; the
// declaration exists so the JIT module link succeeds, but it is never called
// because canExtract returns false for the final (non-partial) output.
__attribute__((__visibility__("default"))) void
jit_HashAggrExtractFinalDecimalAvg(
    char* /*vector*/,
    int32_t /*row*/,
    char* /*group*/,
    int32_t /*offset*/,
    int32_t /*precision*/,
    int32_t /*scale*/,
    int8_t /*longDecimal*/) {}

} // extern "C"

#endif // ENABLE_BOLT_JIT
