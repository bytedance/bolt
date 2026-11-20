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

#include <pybind11/eval.h>
#include <memory>

#include "bolt/expression/TypeSignature.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/python/Function.h"
#include "bolt/python/Utils.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/SelectivityVector.h"

using namespace ::bytedance::bolt;

namespace {
class PythonFunction : public exec::VectorFunction {
  pybind11::function function_;
  const size_t nArgs_;
  const std::vector<pybind11::object> defaultArgs_;
  bool mapBatch_;

 public:
  PythonFunction() = delete;
  PythonFunction(
      pybind11::function fn,
      const size_t nArgs,
      std::vector<pybind11::object> defaultArgs,
      bool mapBatch)
      : function_(std::move(fn)),
        nArgs_(nArgs),
        defaultArgs_(std::move(defaultArgs)),
        mapBatch_(mapBatch) {}

  // There is no guarantee that the user function is deterministic.
  bool isDeterministic() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    pybind11::list pyArgs;
    if (mapBatch_) {
      BOLT_CHECK_EQ(defaultArgs_.size(), 0)
      std::shared_ptr<RowVector> rowVector =
          bytedance::bolt::python::combineInputColumns(args, context);
      pyArgs.append(pybind11::cast(rowVector));
    } else {
      pyArgs = bytedance::bolt::python::asPyArgs(nArgs_, args, defaultArgs_);
    }

    bytedance::bolt::python::pyTry<void>(
        [&]() { result = function_(*pyArgs).cast<VectorPtr>(); },
        [&]() {
          return fmt::format(
              "Failed to evaluate function '{}' with "
              "arguments: '{}'",
              bytedance::bolt::python::pyTypeStr(function_),
              bytedance::bolt::python::toString(pyArgs));
        });
    // `result` will be allocated from the pool of the `pybolt` python
    // module. Its lifetime is the same as the one of that module.
    BOLT_CHECK(result->type()->equivalent(*outputType))
  }
};
} // namespace

namespace bytedance::bolt::python {
void registerPythonFunction(
    pybind11::function callable,
    const std::string& functionName,
    const TypePtr& returnType,
    const size_t nArgs,
    std::vector<pybind11::object> defaultArgs,
    bool mapBatch) {
  const size_t nOptionalArgs = defaultArgs.size();
  registerVectorFunction(
      functionName,
      mapBatch ? getSignatures(returnType)
               : getSignatures(returnType, nArgs, nOptionalArgs),
      std::make_unique<PythonFunction>(
          std::move(callable), nArgs, std::move(defaultArgs), mapBatch));
}
} // namespace bytedance::bolt::python
