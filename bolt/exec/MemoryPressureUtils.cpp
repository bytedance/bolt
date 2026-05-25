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

#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"

#include <exception>

namespace bytedance::bolt::exec::memorypressure {

std::optional<int64_t> extractSparkTaskAttemptId(std::string_view taskId) {
  constexpr std::string_view kTaskAttemptIdPrefix = "_TID_";
  constexpr std::string_view kTaskAttemptIdSuffix = "_ATTEMPT_";

  const auto begin = taskId.find(kTaskAttemptIdPrefix);
  if (begin == std::string_view::npos) {
    return std::nullopt;
  }

  const auto idBegin = begin + kTaskAttemptIdPrefix.size();
  const auto idEnd = taskId.find(kTaskAttemptIdSuffix, idBegin);
  if (idEnd == std::string_view::npos || idEnd == idBegin) {
    return std::nullopt;
  }

  try {
    return std::stoll(std::string(taskId.substr(idBegin, idEnd - idBegin)));
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

MemoryPressureSnapshot snapshot(
    uint64_t reclaimWatermarkBytes,
    const std::optional<int64_t>& sparkTaskAttemptId) {
  uint64_t borrowFromRssWatermarkBytes = 0;
  if (sparkTaskAttemptId.has_value()) {
    borrowFromRssWatermarkBytes =
        memory::sparksql::ExecutionMemoryPool::borrowFromRssWatermarkBytes(
            *sparkTaskAttemptId);
  }
  return MemoryPressureSnapshot{
      reclaimWatermarkBytes, borrowFromRssWatermarkBytes};
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
