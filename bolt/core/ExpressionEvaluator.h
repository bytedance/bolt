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

#include "bolt/vector/ComplexVector.h"
namespace bytedance::bolt::exec {
class ExprSet;
}
namespace bytedance::bolt::core {

class ITypedExpr;

// Exposes expression evaluation functionality of the engine to other parts of
// the code base.  Connector may use it, for example, to evaluate pushed down
// filters.  This is not thread safe and serializing operations is the
// responsibility of the caller.  This is self-contained and does not reference
// objects from the thread which constructs this.  Passing this between threads
// is allowed as long as uses are sequential.  May reference query-level
// structures like QueryCtx.
class ExpressionEvaluator {
 public:
  virtual ~ExpressionEvaluator() = default;

  // Compiles an expression. Returns an instance of exec::ExprSet that can be
  // used to evaluate that expression on multiple vectors using evaluate method.
  virtual std::unique_ptr<exec::ExprSet> compile(
      const std::shared_ptr<const ITypedExpr>& expression) = 0;

  // Evaluates previously compiled expression on the specified rows.
  // Reuses result vector if it is not null.
  virtual void evaluate(
      exec::ExprSet* exprSet,
      const SelectivityVector& rows,
      const RowVector& input,
      VectorPtr& result) = 0;

  virtual void evaluate(
      exec::ExprSet* exprSet,
      const SelectivityVector& rows,
      const RowVector& input,
      VectorPtr& result,
      const std::string& currentSplit) = 0;

  // Memory pool used to construct input or output vectors.
  virtual memory::MemoryPool* pool() = 0;
};

} // namespace bytedance::bolt::core
