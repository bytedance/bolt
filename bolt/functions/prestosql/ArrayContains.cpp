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

#include "bolt/functions/prestosql/ArrayContains.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/vector/DecodedVector.h"
namespace bytedance::bolt::functions {
namespace {

template <TypeKind kind>
void applyTyped(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    exec::EvalCtx& /*context*/,
    FlatVector<bool>& flatResult,
    bool /*throwOnNestedNull*/) {
  using T = typename TypeTraits<kind>::NativeType;

  auto baseArray = arrayDecoded.base()->as<ArrayVector>();
  auto rawSizes = baseArray->rawSizes();
  auto rawOffsets = baseArray->rawOffsets();
  auto indices = arrayDecoded.indices();

  constexpr bool isBoolType = std::is_same_v<bool, T>;

  if (!isBoolType && elementsDecoded.isIdentityMapping() &&
      !elementsDecoded.mayHaveNulls() && searchDecoded.isConstantMapping()) {
    auto rawElements = elementsDecoded.data<T>();
    auto search = searchDecoded.valueAt<T>(0);

    rows.applyToSelected([&](auto row) {
      auto size = rawSizes[indices[row]];
      auto offset = rawOffsets[indices[row]];

      for (auto i = 0; i < size; i++) {
        if (rawElements[offset + i] == search) {
          flatResult.set(row, true);
          return;
        }
      }

      flatResult.set(row, false);
    });
  } else {
    rows.applyToSelected([&](auto row) {
      auto size = rawSizes[indices[row]];
      auto offset = rawOffsets[indices[row]];

      auto search = searchDecoded.valueAt<T>(row);

      bool foundNull = false;

      for (auto i = 0; i < size; i++) {
        if (elementsDecoded.isNullAt(offset + i)) {
          foundNull = true;
        } else if (elementsDecoded.valueAt<T>(offset + i) == search) {
          flatResult.set(row, true);
          return;
        }
      }

      if (foundNull) {
        flatResult.setNull(row, true);
      } else {
        flatResult.set(row, false);
      }
    });
  }
}

void applyComplexType(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    exec::EvalCtx& context,
    FlatVector<bool>& flatResult,
    bool throwOnNestedNull) {
  auto baseArray = arrayDecoded.base()->as<ArrayVector>();
  auto rawSizes = baseArray->rawSizes();
  auto rawOffsets = baseArray->rawOffsets();
  auto indices = arrayDecoded.indices();

  auto elementsBase = elementsDecoded.base();
  auto elementIndices = elementsDecoded.indices();
  auto searchBase = searchDecoded.base();
  auto searchIndices = searchDecoded.indices();

  const auto nullHandlingMode = throwOnNestedNull
      ? CompareFlags::NullHandlingMode::kNullAsIndeterminate
      : CompareFlags::NullHandlingMode::kNullAsValue;

  context.applyToSelectedNoThrow(rows, [&](auto row) {
    auto size = rawSizes[indices[row]];
    auto offset = rawOffsets[indices[row]];
    bool foundNull = false;
    auto searchIndex = searchIndices[row];

    for (auto i = 0; i < size; i++) {
      if (elementsDecoded.isNullAt(offset + i)) {
        foundNull = true;
      } else {
        std::optional<bool> result = elementsBase->equalValueAt(
            searchBase,
            elementIndices[offset + i],
            searchIndex,
            nullHandlingMode);
        BOLT_USER_CHECK(
            result.has_value(),
            "contains does not support arrays with elements that contain null");
        if (result.value()) {
          flatResult.set(row, true);
          return;
        }
      }
    }

    if (foundNull) {
      flatResult.setNull(row, true);
    } else {
      flatResult.set(row, false);
    }
  });
}

template <>
void applyTyped<TypeKind::ARRAY>(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    exec::EvalCtx& context,
    FlatVector<bool>& flatResult,
    bool throwOnNestedNull) {
  applyComplexType(
      rows,
      arrayDecoded,
      elementsDecoded,
      searchDecoded,
      context,
      flatResult,
      throwOnNestedNull);
}

template <>
void applyTyped<TypeKind::MAP>(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    exec::EvalCtx& context,
    FlatVector<bool>& flatResult,
    bool throwOnNestedNull) {
  applyComplexType(
      rows,
      arrayDecoded,
      elementsDecoded,
      searchDecoded,
      context,
      flatResult,
      throwOnNestedNull);
}

template <>
void applyTyped<TypeKind::ROW>(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    exec::EvalCtx& context,
    FlatVector<bool>& flatResult,
    bool throwOnNestedNull) {
  applyComplexType(
      rows,
      arrayDecoded,
      elementsDecoded,
      searchDecoded,
      context,
      flatResult,
      throwOnNestedNull);
}

class ArrayContainsFunction : public exec::VectorFunction {
 public:
  explicit ArrayContainsFunction(bool throwOnNestedNull)
      : throwOnNestedNull_(throwOnNestedNull) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_EQ(args.size(), 2);
    const auto& arrayVector = args[0];
    const auto& searchVector = args[1];

    BOLT_CHECK(arrayVector->type()->isArray());
    BOLT_CHECK(arrayVector->type()->asArray().elementType()->kindEquals(
        searchVector->type()));

    context.ensureWritable(rows, BOOLEAN(), result);
    auto flatResult = result->asFlatVector<bool>();

    exec::LocalDecodedVector arrayHolder(context, *arrayVector, rows);
    auto elements = arrayHolder.get()->base()->as<ArrayVector>()->elements();

    exec::LocalSelectivityVector nestedRows(context, elements->size());
    nestedRows.get()->setAll();

    exec::LocalDecodedVector elementsHolder(
        context, *elements, *nestedRows.get());

    exec::LocalDecodedVector searchHolder(context, *searchVector, rows);

    BOLT_DYNAMIC_TYPE_DISPATCH(
        applyTyped,
        searchVector->typeKind(),
        rows,
        *arrayHolder.get(),
        *elementsHolder.get(),
        *searchHolder.get(),
        context,
        *flatResult,
        throwOnNestedNull_);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(T), T -> boolean
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("boolean")
                .argumentType("array(T)")
                .argumentType("T")
                .build()};
  }

 private:
  bool throwOnNestedNull_;
};

} // namespace

core::CallTypedExprPtr asArrayContainsCall(
    const std::string& prefix,
    const core::TypedExprPtr& expr) {
  if (auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    if (call->name() == prefix + "array_contains" ||
        call->name() == prefix + "contains") {
      return call;
    }
  }
  return nullptr;
}

// rewrite array_contains(array, element) -> element in array if array is a
// constant
core::TypedExprPtr rewriteArrayContains2In(
    const std::string& prefix,
    const core::TypedExprPtr& expr) {
  const auto call = asArrayContainsCall(prefix, expr);
  if (call == nullptr || call->inputs().size() != 2) {
    return nullptr;
  }
  auto array = call->inputs()[0];
  if (!array->type()->isArray() ||
      !array->type()->asArray().elementType()->kindEquals(
          call->inputs()[1]->type())) {
    return nullptr;
  }
  // if array is constant, rewrite to element in array
  if (std::dynamic_pointer_cast<const core::ConstantTypedExpr>(array)) {
    return std::make_shared<core::CallTypedExpr>(
        call->type(),
        std::vector<core::TypedExprPtr>{call->inputs()[1], array},
        prefix + "in");
  }

  return nullptr;
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
arrayContainsSignatures() {
  return ArrayContainsFunction::signatures();
}

std::unique_ptr<exec::VectorFunction> createArrayContainsFunction(
    bool throwOnNestedNull) {
  return std::make_unique<ArrayContainsFunction>(throwOnNestedNull);
}

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_array_contains,
    ArrayContainsFunction::signatures(),
    std::make_unique<ArrayContainsFunction>(true));

// Internal function only used for testing. This function allows the array to
// have null elements and considers null as a value, i.e., null == null.
BOLT_DECLARE_VECTOR_FUNCTION(
    udf_$internal$contains,
    ArrayContainsFunction::signatures(),
    std::make_unique<ArrayContainsFunction>(false));

} // namespace bytedance::bolt::functions
