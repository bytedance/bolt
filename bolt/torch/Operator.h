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

#include <atomic>
#include <condition_variable>
#include <deque>

#include <ATen/core/TensorBody.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <torch/csrc/jit/api/compilation_unit.h>

#include "bolt/exec/Operator.h"
#include "bolt/torch/Tensor.h"

namespace bytedance::bolt::torch {
/// The base bolt operator running a libtorch module under the hood.
///
/// When constructed, the operator will compile the input script and
/// store it in a KV cache. If the module already exists in the cache,
/// this step is skipped.
///
/// The operator will translate its bolt input into torch `Tensor`,
/// forward the input to its torch module to compute an output tensor.
/// Finally, the output tensors are converted into bolt Operator output.
///
/// See "bolt/torch/Compatibility.h" for supported types, inputs and outputs.
///
/// `module_script` is a plain text torch script parsable by
/// `torch::jit::compile`. The script must contain at least one function.
///  If multiple functions exist, the last one is used. That function
///  must take exactly one tensor as input and output one tensor that
///  can be converted to `outputType`.
/// Since the output type of the script cannot be known before the
/// script is evaluated on its input, we cannot infer the validity of
/// `outputType` in the constructor.
///
/// Torch does not officially support naming tensor dimensions.
/// Furthermore, there is no API to name individual columns.
/// Consequently, operators following TorchOperator should not be
/// operating based on column names unless the columns of the output
/// tensor have been renamed. For instance, you can't run a project or
/// filter operator directly after a TorchOperator.
class TorchOperator : public bytedance::bolt::exec::Operator {
 public:
  ~TorchOperator() override;
  TorchOperator(
      std::string module_script,
      bytedance::bolt::RowTypePtr outputType,
      int32_t operatorId,
      bytedance::bolt::exec::DriverCtx* driverCtx,
      std::string planNodeId,
      std::optional<bytedance::bolt::common::SpillConfig> spillConfig =
          std::nullopt);

  // Torch module requires an input tensor.
  bool needsInput() const override {
    return !noMoreInput_;
  }

  // Converts input to tensor input.
  void addInput(bytedance::bolt::RowVectorPtr input) override;

  void ProcessInput();

  // Executes the module with the provided inputs.
  void noMoreInput() override;

  // Converts the tensor output into operator output.
  bytedance::bolt::RowVectorPtr getOutput() override;

  bytedance::bolt::exec::BlockingReason isBlocked(
      bytedance::bolt::ContinueFuture* future) override;

  bool isFinished() override;

 private:
  // The queue of inputs to process.
  // A thread will take one from the queue and start processing.
  std::deque<::bytedance::bolt::RowVectorPtr> tensorInputs_;

  // The number inputs that are being processed.
  // The queue of output done processing.
  using ResultOutput =
      std::variant<std::exception_ptr, ::bytedance::bolt::RowVectorPtr>;
  std::deque<ResultOutput> tensorOutputs_;
  std::size_t numProcessingInputs_{0};

  std::mutex mtx_;
  std::condition_variable cv_;
  ::folly::CPUThreadPoolExecutor executor_{4};

  ::bytedance::bolt::TypePtr boltTensorType_;
  ::caffe2::TypeMeta torchTensorType_;

  // The compiled torch script.
  std::shared_ptr<::torch::jit::CompilationUnit> module_;
  ::torch::jit::Function& getTorchFunction() const;
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool_;

  // The default type to assign to operator's input/output when the inputs
  // are empty.
  static inline const bytedance::bolt::TypePtr kDefaultType_ =
      bytedance::bolt::RealType::create();
};
} // namespace bytedance::bolt::torch
