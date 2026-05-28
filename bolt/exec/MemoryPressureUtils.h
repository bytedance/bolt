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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace bytedance::bolt::exec::memorypressure {

class ScopedMemoryExpansionGuard {
 public:
  template <typename Guard>
  explicit ScopedMemoryExpansionGuard(Guard&& guard)
      : guard_(std::make_unique<GuardModel<std::decay_t<Guard>>>(
            std::forward<Guard>(guard))) {}

  ScopedMemoryExpansionGuard(ScopedMemoryExpansionGuard&&) noexcept = default;

  ScopedMemoryExpansionGuard& operator=(ScopedMemoryExpansionGuard&&) noexcept =
      default;

  ScopedMemoryExpansionGuard(const ScopedMemoryExpansionGuard&) = delete;
  ScopedMemoryExpansionGuard& operator=(const ScopedMemoryExpansionGuard&) =
      delete;

  ~ScopedMemoryExpansionGuard() = default;

 private:
  struct GuardConcept {
    virtual ~GuardConcept() = default;
  };

  template <typename Guard>
  struct GuardModel final : GuardConcept {
    explicit GuardModel(Guard&& guard) : guard_(std::move(guard)) {}

    Guard guard_;
  };

  std::unique_ptr<GuardConcept> guard_;
};

struct MemoryPressureSnapshot {
  uint64_t reclaimWatermarkBytes{0};
  uint64_t borrowFromRssWatermarkBytes{0};
  uint64_t configuredTaskMemoryQuotaBytes{0};

  uint64_t admissionWatermarkBytes() const {
    if (reclaimWatermarkBytes == 0) {
      return borrowFromRssWatermarkBytes == 0 ? configuredTaskMemoryQuotaBytes
                                              : borrowFromRssWatermarkBytes;
    }
    if (borrowFromRssWatermarkBytes == 0) {
      return reclaimWatermarkBytes;
    }
    return std::min(reclaimWatermarkBytes, borrowFromRssWatermarkBytes);
  }
};

std::optional<int64_t> extractSparkTaskAttemptId(std::string_view taskId);

MemoryPressureSnapshot snapshot(
    uint64_t reclaimWatermarkBytes,
    const std::optional<int64_t>& sparkTaskAttemptId);

std::optional<ScopedMemoryExpansionGuard> maybeScopedDisableMemoryExpansion(
    const std::optional<int64_t>& sparkTaskAttemptId);

std::string details();

} // namespace bytedance::bolt::exec::memorypressure
