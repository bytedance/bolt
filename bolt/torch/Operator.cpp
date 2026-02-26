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

#include <functional>
#include <mutex>

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/caching/SimpleLRUCache.h"
#include "bolt/exec/Task.h"
#include "bolt/torch/Compatibility.h"
#include "bolt/torch/Operator.h"
#include "bolt/torch/Utils.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"

using ::bytedance::bolt::BoltUserError;
using ::bytedance::bolt::RowTypePtr;
using ::bytedance::bolt::RowVectorPtr;
using ::bytedance::bolt::common::SpillConfig;
using ::bytedance::bolt::exec::BlockingReason;
using ::bytedance::bolt::exec::DriverCtx;
using ::torch::jit::Function;

namespace bytedance::bolt::torch {
TorchOperator::TorchOperator(
    std::string module_script,
    RowTypePtr outputType,
    int32_t operatorId,
    DriverCtx* driverCtx,
    std::string planNodeId,
    std::optional<SpillConfig> spillConfig)
    : Operator(
          driverCtx,
          outputType,
          operatorId,
          std::move(planNodeId),
          std::string("TorchOperator::") +
              get_function(*load(module_script))->name(),
          std::move(spillConfig)),
      module_(load(module_script)) {
  // Set tensors input/output type.
  if (outputType->children().empty()) {
    boltTensorType_ = kDefaultType_;
  } else {
    boltTensorType_ = outputType->children().at(0);
  }

  // We don't support tensors with multiple types.
  for (auto t = ++outputType->children().cbegin();
       t < outputType->children().cend();
       ++t) {
    if (!t->get()->equivalent(*boltTensorType_)) {
      BOLT_FAIL(fmt::format(
          "TorchOperator does not support output with multiple types."
          "children[0]->type(): {}, other children type: {}.",
          boltTensorType_->name(),
          t->get()->name()));
    }
  }
}

TorchOperator::~TorchOperator() {
  // Wait for all tasks to complete before deleting anything.
  std::unique_lock lock(mtx_);
  cv_.wait(lock, [this]() {
    return tensorInputs_.empty() && tensorOutputs_.empty() &&
        numProcessingInputs_ == 0;
  });
}

Function& TorchOperator::getTorchFunction() const {
  return *get_function(*module_);
}

void TorchOperator::addInput(RowVectorPtr input) {
  // Push the tensor to the queue of tensors to process.
  {
    std::unique_lock lock(mtx_);
    tensorInputs_.emplace_back(std::move(input));
    // Schedule a task to process this input.
    executor_.add([this]() { ProcessInput(); });
  }
}

void TorchOperator::ProcessInput() {
  RowVectorPtr input_table;
  ResultOutput result{RowVectorPtr{nullptr}};

  // Pull input table from the input queue.
  {
    std::unique_lock lock(mtx_);
    // There is no more input to process.
    if (tensorInputs_.empty()) {
      return;
    }

    input_table = std::move(tensorInputs_.front());
    tensorInputs_.pop_front();
    // Increment the number of input processed.
    numProcessingInputs_++;
  }

  cv_.notify_all();

  // 1. Convert the table to a tensor.
  // 2. Process the input tensor through the compiled torch script.
  // 3. Convert the output tensor back to a bolt type.
  try {
    // Inner try block converts exceptions to BoltException;
    // Outer try block queues the exception in the output queue and returns.
    try {
      auto input_tensor =
          BoltRowVectorToTorchTensor(pool(), boltTensorType_)(*input_table);
      Function& fn = getTorchFunction();
      auto output_tensor = fn({::c10::IValue(input_tensor.get())});
      result = TorchTensorToBoltRowVector(
          pool(), outputType_)(output_tensor.toTensor());
    }
    // Errors from Tensor <-> RowVectorPtr conversions will throw a bolt
    // exception. Those are forwarded.
    catch (const bytedance::bolt::BoltException& e) {
      throw;
    }
    // The torch module execution throws ::c10::Error which extends
    // std::exception and implements what() method.
    catch (const std::exception& e) {
      throw BoltUserError(std::current_exception(), e.what(), false);
    } catch (...) {
      throw BoltUserError(
          std::current_exception(),
          "Unknown error while executing user torch script",
          false);
    }
  } catch (...) {
    result = std::current_exception();
  }

  // Append result to the output queue.
  {
    std::unique_lock lock(mtx_);
    tensorOutputs_.emplace_back(std::move(result));
    numProcessingInputs_--;
  }

  cv_.notify_all();
}

void TorchOperator::noMoreInput() {
  std::unique_lock lock(mtx_);
  Operator::noMoreInput();
  cv_.notify_all();
}

bool TorchOperator::isFinished() {
  std::unique_lock lock(mtx_);
  return noMoreInput_ && tensorInputs_.empty() && numProcessingInputs_ == 0;
}

BlockingReason TorchOperator::isBlocked(
    bytedance::bolt::ContinueFuture* future) {
  return BlockingReason::kNotBlocked;
}

RowVectorPtr TorchOperator::getOutput() {
  std::unique_lock lock(mtx_);

  cv_.wait(lock, [this]() {
    return !tensorOutputs_.empty() ||
        (tensorInputs_.empty() && numProcessingInputs_ == 0);
  });

  // There is no output at the moment.
  if (tensorOutputs_.empty()) {
    return nullptr;
  }

  auto result = std::move(tensorOutputs_.front());
  tensorOutputs_.pop_front();

  lock.unlock();
  cv_.notify_all();

  if (std::holds_alternative<std::exception_ptr>(result)) {
    throw std::get<std::exception_ptr>(result);
  }

  return std::get<RowVectorPtr>(std::move(result));
}
} // namespace bytedance::bolt::torch
