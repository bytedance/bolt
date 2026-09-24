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

#include <cstdint>
#include <vector>

#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::lance::reader {

enum class NativeLanceBatchBuilderState : uint8_t {
  kBuilding,
  kReady,
  kFinished,
  kAborted,
};

/// Owns unpublished output children until one complete RowVector is finished.
class NativeLanceBatchBuilder {
 public:
  NativeLanceBatchBuilder(
      RowTypePtr outputType,
      vector_size_t rowCount,
      memory::MemoryPool& pool);

  void setChild(column_index_t channel, VectorPtr child);
  VectorPtr finish();
  void abort();

  NativeLanceBatchBuilderState state() const {
    return state_;
  }

 private:
  void updateReadyState();

  RowTypePtr outputType_;
  vector_size_t rowCount_;
  memory::MemoryPool& pool_;
  std::vector<VectorPtr> children_;
  size_t completedChildren_{0};
  NativeLanceBatchBuilderState state_{NativeLanceBatchBuilderState::kBuilding};
};

} // namespace bytedance::bolt::lance::reader
