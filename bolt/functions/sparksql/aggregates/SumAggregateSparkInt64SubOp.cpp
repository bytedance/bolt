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

#include "bolt/functions/sparksql/aggregates/SumAggregateSparkInt64SubOp.h"

#include "bolt/exec/AggregationHook.h"
#include "bolt/functions/lib/CheckedArithmeticImpl.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/LazyVector.h"
#include "bolt/vector/VectorEncoding.h"

namespace bytedance::bolt::functions::aggregate::sparksql {

namespace {

static void sparkSumInt64UpdateSingle(int64_t& result, int64_t value) {
  if (::bytedance::bolt::functions::aggregate::Overflow) {
    result += value;
  } else {
    CHECK_ADD(result, value);
  }
}

} // namespace

#if !defined(__aarch64__) || !defined(__linux__)
bool sumInt64SubOpCanUseSveKernel() {
  return false;
}
#endif

SumAggregateSparkInt64SubOp::SumAggregateSparkInt64SubOp(TypePtr resultType)
    : Base(std::move(resultType)) {}

void SumAggregateSparkInt64SubOp::updateBatch(
    char** groups,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    bool mayPushdown,
    bool intermediate) {
  const auto& arg = args[0];

  auto delegateToBase = [&]() {
    if (intermediate) {
      Base::addIntermediateResults(groups, rows, args, mayPushdown);
    } else {
      Base::addRawInput(groups, rows, args, mayPushdown);
    }
  };

  if (mayPushdown && arg->isLazy()) {
    delegateToBase();
    return;
  }

  using ::bytedance::bolt::functions::aggregate::Overflow;
  if (!Overflow) {
    delegateToBase();
    return;
  }

  if (!sumInt64SubOpCanUseSveKernel()) {
    delegateToBase();
    return;
  }

  DecodedVector decoded(*arg, rows, !mayPushdown);
  const auto encoding = decoded.base()->encoding();
  // Match SimpleNumericAggregate::updateGroups: indirect lazy (e.g.
  // Dictionary(Lazy)) uses SimpleCallableHook regardless of numNulls_.
  if (UNLIKELY(encoding == VectorEncoding::Simple::LAZY)) {
    bytedance::bolt::aggregate::SimpleCallableHook<
        int64_t,
        int64_t,
        void (*)(int64_t&, int64_t)>
        hook(
            offset_,
            nullByte_,
            nullMask_,
            groups,
            &numNulls_,
            sparkSumInt64UpdateSingle);
    auto indices = decoded.indices();
    decoded.base()->as<const LazyVector>()->load(
        ::bytedance::bolt::RowSet(indices, arg->size()), &hook);
    return;
  }

  if (updateGroupsFromDecoded(groups, rows, decoded)) {
    return;
  }

  // Stub/defense when SVE TU is not linked.
  delegateToBase();
}

void SumAggregateSparkInt64SubOp::addRawInput(
    char** groups,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    bool mayPushdown) {
  updateBatch(groups, rows, args, mayPushdown, false);
}

void SumAggregateSparkInt64SubOp::addIntermediateResults(
    char** groups,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    bool mayPushdown) {
  updateBatch(groups, rows, args, mayPushdown, true);
}

#if !defined(__aarch64__)
bool SumAggregateSparkInt64SubOp::updateGroupsFromDecoded(
    char** /*groups*/,
    const SelectivityVector& /*rows*/,
    ::bytedance::bolt::DecodedVector& /*decoded*/) {
  return false;
}
#endif

} // namespace bytedance::bolt::functions::aggregate::sparksql
