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

#include "bolt/functions/sparksql/Elt.h"

#include "bolt/expression/EvalCtx.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::functions::sparksql {
namespace {

/// Implements Spark's elt(n, input1, input2, ...).
///
/// The index argument is args[0]; the candidate inputs are args[1..n-1].
/// Each row is visited once and reads only the input its own index selects,
/// so the cost is O(rows) rather than O(rows x inputs).
class EltFunction final : public exec::VectorFunction {
 public:
  // The default null behavior sets a row to NULL when *any* argument is NULL
  // and never calls apply() for it. That is wrong here: a NULL in an input
  // that this row does not select must not make the result NULL --
  // elt(1, 'a', NULL) is 'a', not NULL. So the rows have to be visited
  // regardless of which inputs happen to contain nulls.
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
    auto* flatResult = result->asFlatVector<StringView>();

    exec::LocalDecodedVector indexHolder(context, *args[0], rows);
    const auto* decodedIndex = indexHolder.get();

    // Decode each input once up front. Decoding inside the row loop would
    // redo the work per row, and DecodedVector also hides whether the input
    // arrived flat, dictionary- or constant-encoded.
    std::vector<exec::LocalDecodedVector> inputHolders;
    std::vector<const DecodedVector*> decodedInputs;
    inputHolders.reserve(numInputs);
    decodedInputs.reserve(numInputs);
    for (int32_t input = 1; input <= numInputs; ++input) {
      inputHolders.emplace_back(context, *args[input], rows);
      decodedInputs.push_back(inputHolders.back().get());
    }

    // The result borrows the inputs' string buffers instead of copying their
    // bytes, so a selected value is referenced rather than duplicated. Only an
    // input that some selected row actually indexes into can contribute bytes,
    // so scan the indices first and acquire buffers for just those inputs; an
    // input no row selects would otherwise pin its buffers for nothing.
    std::vector<bool> inputReferenced(numInputs, false);
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedIndex->isNullAt(row)) {
        return;
      }
      const auto index = decodedIndex->valueAt<int32_t>(row);
      if (index >= 1 && index <= numInputs) {
        inputReferenced[index - 1] = true;
      }
    });
    for (int32_t input = 1; input <= numInputs; ++input) {
      if (inputReferenced[input - 1]) {
        flatResult->acquireSharedStringBuffers(args[input].get());
      }
    }

    rows.applyToSelected([&](vector_size_t row) {
      // A NULL or out-of-range index yields NULL rather than an error, and a
      // NULL in the selected input is propagated as NULL.
      if (decodedIndex->isNullAt(row)) {
        result->setNull(row, true);
        return;
      }
      const auto index = decodedIndex->valueAt<int32_t>(row);
      if (index < 1 || index > numInputs) {
        result->setNull(row, true);
        return;
      }
      const auto* selected = decodedInputs[index - 1];
      if (selected->isNullAt(row)) {
        result->setNull(row, true);
        return;
      }
      flatResult->setNoCopy(row, selected->valueAt<StringView>(row));
    });
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
