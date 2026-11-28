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

#include "bolt/functions/sparksql/UnscaledValueFunction.h"

#include "bolt/expression/DecodedArgs.h"
namespace bytedance::bolt::functions::sparksql {
namespace {

// Return the unscaled bigint value of a decimal, assuming it
// fits in a bigint. Only short decimal input is accepted.
class UnscaledValueFunction final : public exec::VectorFunction {
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const final {
    BOLT_USER_CHECK(
        args[0]->type()->isShortDecimal(),
        "Expect short decimal type, but got: {}",
        args[0]->type());
    VectorPtr localResult;
    const auto& arg = args[0];
    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<int64_t>>()->valueAt(0);
      localResult = std::make_shared<ConstantVector<int64_t>>(
          context.pool(), rows.end(), false, BIGINT(), std::move(value));
    } else {
      auto flatInput = arg->asFlatVector<int64_t>();
      localResult = std::make_shared<FlatVector<int64_t>>(
          context.pool(),
          BIGINT(),
          nullptr,
          rows.end(),
          flatInput->values(),
          std::vector<BufferPtr>());
    }
    context.moveOrCopyResult(localResult, rows, result);
  }
};
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>>
unscaledValueSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("precision")
              .integerVariable("scale")
              .returnType("bigint")
              .argumentType("DECIMAL(precision, scale)")
              .build()};
}

std::unique_ptr<exec::VectorFunction> makeUnscaledValue() {
  return std::make_unique<UnscaledValueFunction>();
}

} // namespace bytedance::bolt::functions::sparksql
