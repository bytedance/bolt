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

#include "bolt/dwio/lance/NativeLanceBatchBuilder.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {

NativeLanceBatchBuilder::NativeLanceBatchBuilder(
    RowTypePtr outputType,
    vector_size_t rowCount,
    memory::MemoryPool& pool)
    : outputType_(std::move(outputType)),
      rowCount_(rowCount),
      pool_(pool),
      children_(outputType_ == nullptr ? 0 : outputType_->size()) {
  BOLT_CHECK_NOT_NULL(outputType_);
  BOLT_CHECK_GE(rowCount_, 0);
  updateReadyState();
}

void NativeLanceBatchBuilder::setChild(
    column_index_t channel,
    VectorPtr child) {
  BOLT_CHECK(state_ == NativeLanceBatchBuilderState::kBuilding);
  BOLT_CHECK_GE(channel, 0);
  BOLT_CHECK_LT(channel, children_.size());
  BOLT_CHECK_NULL(children_[channel]);
  BOLT_CHECK_NOT_NULL(child);
  BOLT_CHECK_EQ(child->size(), rowCount_);
  BOLT_CHECK(
      child->type()->equivalent(*outputType_->childAt(channel)),
      "Native Lance output child type does not match the scan plan");
  children_[channel] = std::move(child);
  ++completedChildren_;
  updateReadyState();
}

VectorPtr NativeLanceBatchBuilder::finish() {
  BOLT_CHECK(
      state_ == NativeLanceBatchBuilderState::kReady,
      "Cannot publish an incomplete native Lance output batch");
  state_ = NativeLanceBatchBuilderState::kFinished;
  return std::make_shared<RowVector>(
      &pool_, outputType_, nullptr, rowCount_, std::move(children_));
}

void NativeLanceBatchBuilder::abort() {
  if (state_ == NativeLanceBatchBuilderState::kFinished ||
      state_ == NativeLanceBatchBuilderState::kAborted) {
    return;
  }
  children_.clear();
  completedChildren_ = 0;
  state_ = NativeLanceBatchBuilderState::kAborted;
}

void NativeLanceBatchBuilder::updateReadyState() {
  if (completedChildren_ == children_.size()) {
    state_ = NativeLanceBatchBuilderState::kReady;
  }
}

} // namespace bytedance::bolt::lance::reader
