/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include "bolt/core/Expressions.h"
#include "bolt/exec/Operator.h"
namespace bytedance::bolt::exec {

class Expand : public Operator {
 public:
  Expand(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::ExpandNode>& expandNode);

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }

  void setRollupEnabled(bool rollupEnabled) {
    rollupEnabled_ = rollupEnabled;
  }

 private:
  std::vector<std::vector<column_index_t>> fieldProjections_;

  std::vector<std::vector<std::shared_ptr<const core::ConstantTypedExpr>>>
      constantProjections_;

  // Used to indicate the index of fieldProjections_.
  int32_t rowIndex_{0};

  bool rollupEnabled_{false};
};
} // namespace bytedance::bolt::exec
