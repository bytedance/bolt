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

#include "bolt/exec/MemoryPressureUtils.h"

#include "bolt/common/base/NumberParsing.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"

namespace bytedance::bolt::exec::memorypressure {

std::optional<int64_t> extractSparkTaskAttemptId(std::string_view taskId) {
  constexpr std::string_view kTaskAttemptIdPrefix = "_TID_";
  constexpr std::string_view kTaskAttemptIdSuffix = "_ATTEMPT_";
  return extractNumberBetween(
      taskId, kTaskAttemptIdPrefix, kTaskAttemptIdSuffix);
}

MemoryPressureSnapshot snapshot(
    uint64_t reclaimWatermarkBytes,
    const std::optional<int64_t>& sparkTaskAttemptId) {
  uint64_t borrowFromRssWatermarkBytes = 0;
  uint64_t configuredTaskMemoryQuotaBytes = 0;
  if (sparkTaskAttemptId.has_value()) {
    borrowFromRssWatermarkBytes =
        memory::sparksql::ExecutionMemoryPool::borrowFromRssWatermarkBytes(
            *sparkTaskAttemptId);
    configuredTaskMemoryQuotaBytes =
        memory::sparksql::ExecutionMemoryPool::getConfiguredMemoryPerTask();
  }
  return MemoryPressureSnapshot{
      reclaimWatermarkBytes,
      borrowFromRssWatermarkBytes,
      configuredTaskMemoryQuotaBytes};
}

std::optional<ScopedMemoryExpansionGuard> maybeScopedDisableMemoryExpansion(
    const std::optional<int64_t>& sparkTaskAttemptId) {
  auto guard = memory::sparksql::ExecutionMemoryPool::
      maybeScopedDisableDynamicMemoryQuotaManagerForTask(sparkTaskAttemptId);
  if (!guard.has_value()) {
    return std::nullopt;
  }
  return ScopedMemoryExpansionGuard(std::move(*guard));
}

std::string details() {
  return memory::sparksql::ExecutionMemoryPool::debugString();
}

} // namespace bytedance::bolt::exec::memorypressure
