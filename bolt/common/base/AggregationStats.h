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
#include <stdint.h>
#include <cstdint>
namespace bytedance::bolt::common {
/// Provides the fine-grained grouping execution stats.
struct AggregationStats {
  uint64_t aggProbeTimeNs{0};
  uint64_t aggFunctionTimeNs{0};
  uint64_t aggOutputUpdateTimeNs{0};
  uint64_t aggExtractGroupsTimeNs{0};
  uint64_t aggOutputUniqueRows{0};
  uint64_t aggOutputTimeNs{0};
  uint64_t aggProbeBypassTimeNs{0};
  uint64_t aggProbeBypassCount{0};
  // Hash aggregation JIT fine-grained timing.
  // One-time codegen (LLVM compile) time for the JIT plan.
  uint64_t aggJitCodegenTimeNs{0};
  // JIT-executed part of the agg function update time.
  uint64_t aggFunctionJitTimeNs{0};
  // JIT-executed part of the extracting groups time.
  uint64_t aggExtractGroupsJitTimeNs{0};
};
} // namespace bytedance::bolt::common
