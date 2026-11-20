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

#include <array>
#include <cstdint>
#include <optional>

#include "bolt/common/memory/bm/io/IoPriority.h"

namespace bytedance::bolt::memory::bm {

class DrrDispatcher {
 public:
  explicit DrrDispatcher(std::array<uint32_t, kIoPriorityCount> weights);

  std::optional<size_t> pick(
      const std::array<size_t, kIoPriorityCount>& queueSizes);
  void restore(size_t priority);
  void reset(size_t priority);

 private:
  bool hasDispatchableDeficit(
      const std::array<size_t, kIoPriorityCount>& queueSizes) const;
  void refillDeficits(const std::array<size_t, kIoPriorityCount>& queueSizes);

  std::array<uint32_t, kIoPriorityCount> weights_;
  std::array<int64_t, kIoPriorityCount> deficits_{};
  size_t nextPriorityCursor_{0};
};

} // namespace bytedance::bolt::memory::bm
