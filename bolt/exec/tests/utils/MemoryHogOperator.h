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

#include "bolt/core/PlanNode.h"
#include "bolt/exec/Operator.h"

namespace bytedance::bolt::exec::test {

/// A leaf source operator that emits a fixed list of batches and, right before
/// emitting the batch at 'triggerAt', allocates 'allocBytes' from its own
/// operator memory pool and frees it again. The allocation creates real memory
/// pressure on the task, which can be used to drive a spill or a cross-operator
/// memory reclaim (e.g. to reclaim a downstream operator that is idle between
/// batches).
///
/// The translator is registered automatically at static-init time (see
/// MemoryHogOperator.cpp), so callers only need to add a MemoryHogNode to their
/// plan.
class MemoryHogNode : public core::PlanNode {
 public:
  // Defined out-of-line in MemoryHogOperator.cpp so that constructing a
  // MemoryHogNode pulls that translation unit in, which in turn runs the
  // static registration of the operator translator.
  MemoryHogNode(
      const core::PlanNodeId& id,
      RowTypePtr outputType,
      std::vector<RowVectorPtr> batches,
      int32_t triggerAt,
      int64_t allocBytes);

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    static const std::vector<core::PlanNodePtr> kEmpty;
    return kEmpty;
  }

  std::string_view name() const override {
    return "MemoryHog";
  }

  const std::vector<RowVectorPtr>& batches() const {
    return batches_;
  }

  int32_t triggerAt() const {
    return triggerAt_;
  }

  int64_t allocBytes() const {
    return allocBytes_;
  }

  folly::dynamic serialize() const override {
    return PlanNode::serialize();
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "MemoryHog";
  }

  const RowTypePtr outputType_;
  const std::vector<RowVectorPtr> batches_;
  const int32_t triggerAt_;
  const int64_t allocBytes_;
};

class MemoryHogOperator : public SourceOperator {
 public:
  MemoryHogOperator(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const MemoryHogNode> node)
      : SourceOperator(
            driverCtx,
            node->outputType(),
            operatorId,
            node->id(),
            "MemoryHog"),
        batches_(node->batches()),
        triggerAt_(node->triggerAt()),
        allocBytes_(node->allocBytes()) {}

  RowVectorPtr getOutput() override {
    if (current_ >= static_cast<int32_t>(batches_.size())) {
      return nullptr;
    }
    if (current_ == triggerAt_) {
      // Allocate from this operator's pool to create memory pressure on the
      // task, then free it. This drives the task's spill / reclaim path.
      void* buffer = pool()->allocate(allocBytes_);
      pool()->free(buffer, allocBytes_);
    }
    return batches_[current_++];
  }

  BlockingReason isBlocked(ContinueFuture* /*unused*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return current_ >= static_cast<int32_t>(batches_.size());
  }

 private:
  const std::vector<RowVectorPtr> batches_;
  const int32_t triggerAt_;
  const int64_t allocBytes_;
  int32_t current_ = 0;
};

class MemoryHogTranslator : public Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto hogNode = std::dynamic_pointer_cast<const MemoryHogNode>(node)) {
      return std::make_unique<MemoryHogOperator>(id, ctx, hogNode);
    }
    return nullptr;
  }
};

} // namespace bytedance::bolt::exec::test
