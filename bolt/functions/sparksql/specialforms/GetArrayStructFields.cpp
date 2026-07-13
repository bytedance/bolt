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

#include "bolt/functions/sparksql/specialforms/GetArrayStructFields.h"
#include "bolt/expression/ConstantExpr.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::functions::sparksql {
namespace {

class GetArrayStructFieldsFunction : public exec::VectorFunction {
 public:
  explicit GetArrayStructFieldsFunction(int32_t ordinal) : ordinal_(ordinal) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*resultType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Decode input array vector.
    exec::LocalDecodedVector decoded(context, *args[0], rows);
    auto arrayVec = decoded->base()->as<ArrayVector>();

    DecodedVector decodedElement(*arrayVec->elements());
    auto elements = decodedElement.base()->as<RowVector>();
    auto fieldVector = elements->childAt(ordinal_);

    VectorPtr fieldResult = fieldVector;
    if (elements->mayHaveNulls()) {
      auto size = elements->size();
      auto indices = AlignedBuffer::allocate<
          vector_size_t,
          AlignedBuffer::AllocationStrategy::kConservative>(
          size, context.pool());
      std::iota(
          indices->asMutable<vector_size_t>(),
          indices->asMutable<vector_size_t>() + size,
          0);

      // Wrap field with nulls from elements.
      fieldResult = BaseVector::wrapInDictionary(
          elements->nulls(), indices, size, fieldResult);
    }

    // Apply element decoding if needed.
    if (!decodedElement.isIdentityMapping()) {
      fieldResult = decodedElement.wrap(
          fieldResult, *arrayVec->elements(), decodedElement.size());
    }

    auto arrayResult = std::make_shared<ArrayVector>(
        context.pool(),
        ARRAY(fieldVector->type()),
        arrayVec->nulls(),
        arrayVec->size(),
        arrayVec->offsets(),
        arrayVec->sizes(),
        fieldResult);

    if (decoded->isIdentityMapping()) {
      result = arrayResult;
    } else {
      result = decoded->wrap(arrayResult, *args[0], decoded->size());
    }
  }

 private:
  // The position to select subfield from the struct.
  const int32_t ordinal_;
};

} // namespace

TypePtr GetArrayStructFieldsCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /*argTypes*/) {
  BOLT_FAIL("GetArrayStructFields function does not support type resolution.");
}

exec::ExprPtr GetArrayStructFieldsCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  BOLT_USER_CHECK_EQ(
      args.size(), 2, "get_array_struct_fields expects two arguments.");

  BOLT_USER_CHECK(
      args[0]->type()->kind() == TypeKind::ARRAY &&
          args[0]->type()->asArray().elementType()->kind() == TypeKind::ROW,
      "The first argument of get_array_struct_fields should be of array(row) type.");

  BOLT_USER_CHECK_EQ(
      args[1]->type()->kind(),
      TypeKind::INTEGER,
      "The second argument of get_array_struct_fields should be of integer type.");

  auto constantExpr = std::dynamic_pointer_cast<exec::ConstantExpr>(args[1]);
  BOLT_USER_CHECK_NOT_NULL(
      constantExpr,
      "The second argument of get_array_struct_fields should be constant expression.");
  BOLT_USER_CHECK(
      constantExpr->value()->isConstantEncoding(),
      "The second argument of get_array_struct_fields should be wrapped in constant vector.");
  auto constantVector =
      constantExpr->value()->asUnchecked<ConstantVector<int32_t>>();
  BOLT_USER_CHECK(
      !constantVector->isNullAt(0),
      "The second argument of get_array_struct_fields should be non-nullable.");

  auto ordinal = constantVector->valueAt(0);

  BOLT_USER_CHECK_GE(
      ordinal, 0, "Invalid ordinal. Should be greater than or equal to 0.");
  auto numFields = args[0]->type()->asArray().elementType()->asRow().size();
  BOLT_USER_CHECK_LT(
      ordinal,
      numFields,
      "Invalid ordinal {} for struct with {} fields.",
      ordinal,
      numFields);
  auto getArrayStructFieldsFunction =
      std::make_shared<GetArrayStructFieldsFunction>(ordinal);

  return std::make_shared<exec::Expr>(
      type,
      std::move(args),
      std::move(getArrayStructFieldsFunction),
      kGetArrayStructFields,
      trackCpuUsage);
}
} // namespace bytedance::bolt::functions::sparksql
