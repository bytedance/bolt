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

#include "bolt/exec/tests/utils/MemoryHogOperator.h"

namespace bytedance::bolt::exec::test {
namespace {
const bool kMemoryHogOperatorRegistered = [] {
  Operator::registerOperator(std::make_unique<MemoryHogTranslator>());
  return true;
}();
} // namespace

MemoryHogNode::MemoryHogNode(
    const core::PlanNodeId& id,
    RowTypePtr outputType,
    std::vector<RowVectorPtr> batches,
    int32_t triggerAt,
    int64_t allocBytes)
    : PlanNode(id),
      outputType_(std::move(outputType)),
      batches_(std::move(batches)),
      triggerAt_(triggerAt),
      allocBytes_(allocBytes) {}

} // namespace bytedance::bolt::exec::test
