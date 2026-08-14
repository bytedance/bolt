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

#include <cstddef>
#include <cstdint>
#include <optional>

#include "bolt/common/base/SortStat.h"
#include "bolt/common/base/SpillStats.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::exec {

class OperatorCtx;

class SortBufferBase {
 public:
  virtual ~SortBufferBase() = default;

  virtual void addInput(const VectorPtr& input) = 0;

  virtual void noMoreInput() = 0;

  virtual RowVectorPtr getOutput(vector_size_t maxOutputRows) = 0;

  virtual bool canSpill() const = 0;

  virtual bool canReclaim() const = 0;

  virtual void spill() = 0;

  virtual std::optional<common::SpillStats> spilledStats() const = 0;

  virtual std::optional<common::SpillReadStats> spillReadStats() const = 0;

  virtual std::optional<common::SortStats> sortStats() const = 0;

  virtual size_t numInputRows() const = 0;

  virtual size_t numOutputRows() const = 0;

  virtual std::optional<uint64_t> estimateOutputRowSize() const = 0;
};

} // namespace bytedance::bolt::exec
