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

#include "bolt/functions/lib/aggregates/SumAggregateBase.h"

namespace bytedance::bolt {
class DecodedVector;
}

namespace bytedance::bolt::functions::aggregate::sparksql {

/// True when Linux aarch64 runtime has SVE and the current vector length matches
/// the compiled 256-bit kernel (`svcntb() == 32`). Result is cached per process.
bool sumInt64SubOpCanUseSveKernel();

/// Spark sum(bigint)->bigint SubOp (default unless `BOLT_SPARK_SUM_INT64_USE_SUBOP`
/// is off; see `SumAggregate.cpp`). For Spark int64 sum with overflow checking,
/// runs the AArch64 SVE batch kernel when runtime SVE is available; handles
/// with/without accumulator nulls and with/without input nulls via `numNulls_`
/// and `BatchReadView` dispatch modes. Falls back to `SumAggregateBase` when SVE
/// is unavailable or the batch shape is unsupported (e.g. non-256-bit SVE VL).
class SumAggregateSparkInt64SubOp
    : public ::bytedance::bolt::functions::aggregate::SumAggregateBase<
          int64_t,
          int64_t,
          int64_t> {
  using Base = ::bytedance::bolt::functions::aggregate::SumAggregateBase<
      int64_t,
      int64_t,
      int64_t>;

 public:
  explicit SumAggregateSparkInt64SubOp(TypePtr resultType);

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override;

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override;

 private:
  /// Adapter for one decoded batch: `HashAggGroupSink` + `SelectedBatchReadView`
  /// → `sveHashAggBatchUpdateGroupSums`. Returns true when the SVE path was
  /// taken (caller must not invoke Base). Returns false only on non-aarch64
  /// stub builds.
  bool updateGroupsFromDecoded(
      char** groups,
      const SelectivityVector& rows,
      ::bytedance::bolt::DecodedVector& decoded);

  void updateBatch(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown,
      bool intermediate);
};

} // namespace bytedance::bolt::functions::aggregate::sparksql
