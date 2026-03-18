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

#include "bolt/exec/Operator.h"
#include "bolt/exec/OperatorUtils.h"
namespace bytedance::bolt::exec {

class CallbackSink : public Operator {
 public:
  CallbackSink(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::function<BlockingReason(RowVectorPtr, ContinueFuture*, uint32_t)>
          consumeCb,
      std::function<BlockingReason(ContinueFuture*)> startedCb = nullptr)
      : Operator(driverCtx, nullptr, operatorId, "N/A", "CallbackSink"),
        startedCb_{std::move(startedCb)},
        consumeCb_{std::move(consumeCb)} {}

  void addInput(RowVectorPtr input) override {
    loadColumns(input, *operatorCtx_->execCtx());
    blockingReason_ =
        consumeCb_(input, &future_, operatorCtx_->driverCtx()->partitionId);
  }

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  bool needsInput() const override {
    return consumeCb_ != nullptr;
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    close();
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    if (startedCb_ != nullptr) {
      blockingReason_ = startedCb_(&future_);
      startedCb_ = nullptr;
    }
    if (blockingReason_ != BlockingReason::kNotBlocked) {
      *future = std::move(future_);
      blockingReason_ = BlockingReason::kNotBlocked;
      return BlockingReason::kWaitForConsumer;
    }
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  void close() override {
    if (consumeCb_) {
      consumeCb_(nullptr, nullptr, 0);
      consumeCb_ = nullptr;
    }
  }

  ContinueFuture future_;
  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
  std::function<BlockingReason(ContinueFuture*)> startedCb_;
  std::function<BlockingReason(RowVectorPtr, ContinueFuture*, uint32_t)>
      consumeCb_;
};

} // namespace bytedance::bolt::exec
