/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
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

#include "bolt/functions/sparksql/Elt.h"

#include "bolt/expression/EvalCtx.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::functions::sparksql {
namespace {

/// Implements Spark's elt(n, input1, input2, ...).
///
/// The index argument is args[0]; the candidate inputs are args[1..n-1].
/// Rows are grouped by the index value so that each candidate input is copied
/// at most once, which keeps the number of copies bounded by the number of
/// inputs rather than by the number of rows.
class EltFunction final : public exec::VectorFunction {
 public:
  // elt() must return NULL, rather than propagating a NULL input, when the
  // index is NULL or out of range. It therefore cannot use the default null
  // behavior.
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // args[0] is the index; the remaining arguments are the candidate inputs.
    const auto numInputs = static_cast<int32_t>(args.size()) - 1;

    context.ensureWritable(rows, outputType, result);

    // Rows whose index is NULL or out of range stay NULL, so start from all
    // NULL and only overwrite the rows that select a valid input.
    rows.applyToSelected(
        [&](vector_size_t row) { result->setNull(row, true); });

    exec::LocalDecodedVector indexHolder(context, *args[0], rows);
    const auto* decodedIndex = indexHolder.get();

    // Group the rows by the input they select, so that each input vector is
    // copied at most once for the whole batch.
    exec::LocalSelectivityVector inputRowsHolder(context, rows.end());
    auto* inputRows = inputRowsHolder.get();

    for (int32_t input = 1; input <= numInputs; ++input) {
      inputRows->clearAll();
      bool hasRows = false;

      rows.applyToSelected([&](vector_size_t row) {
        if (decodedIndex->isNullAt(row)) {
          return;
        }
        if (decodedIndex->valueAt<int32_t>(row) == input) {
          inputRows->setValid(row, true);
          hasRows = true;
        }
      });

      if (!hasRows) {
        continue;
      }
      inputRows->updateBounds();

      // copy() handles nulls in the source, as well as dictionary- and
      // constant-encoded inputs, so the selected input does not need to be
      // decoded here.
      result->copy(args[input].get(), *inputRows, nullptr, false);
    }
  }
};

} // namespace

std::shared_ptr<exec::VectorFunction> makeElt(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  // One index argument plus at least one candidate input.
  BOLT_USER_CHECK_GE(
      inputArgs.size(),
      2,
      "{} requires an index argument and at least one input.",
      name);

  BOLT_USER_CHECK_EQ(
      inputArgs[0].type->kind(),
      TypeKind::INTEGER,
      "The first argument of {} must be an integer.",
      name);

  // Spark requires all candidate inputs to share the same type.
  for (size_t i = 2; i < inputArgs.size(); ++i) {
    BOLT_USER_CHECK(
        *inputArgs[i].type == *inputArgs[1].type,
        "All inputs of {} must have the same type. Got {} and {}.",
        name,
        inputArgs[1].type->toString(),
        inputArgs[i].type->toString());
  }

  const auto kind = inputArgs[1].type->kind();
  BOLT_USER_CHECK(
      kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY,
      "{} does not support inputs of type {}.",
      name,
      inputArgs[1].type->toString());

  return std::make_shared<EltFunction>();
}

std::vector<std::shared_ptr<exec::FunctionSignature>> eltSignatures() {
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  for (const auto& type : {"varchar", "varbinary"}) {
    // integer, T... -> T
    signatures.emplace_back(exec::FunctionSignatureBuilder()
                                .returnType(type)
                                .argumentType("integer")
                                .argumentType(type)
                                .variableArity()
                                .build());
  }
  return signatures;
}

} // namespace bytedance::bolt::functions::sparksql
