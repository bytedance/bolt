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

#include <folly/container/F14Set.h>
#include <cmath>
#include <cstdint>
#include "bolt/functions/Udf.h"

namespace bytedance::bolt::functions {
namespace {
///
/// Implements the array_slice_sum function.
///

template <typename TInput, typename TOutput>
class ArraySliceSumFunction : public exec::VectorFunction {
 public:
  template <bool mayHaveNulls, typename DataAtFunc, typename IsNullFunc>
  TOutput applyCore(
      vector_size_t row,
      const ArrayVector* arrayVector,
      DataAtFunc&& dataAtFunc,
      IsNullFunc&& isNullFunc,
      int32_t start,
      int32_t length) const {
    TOutput sum = 0;

    if (arrayVector->isNullAt(row)) {
      return sum;
    }

    auto offset = arrayVector->offsetAt(row);
    auto size = arrayVector->sizeAt(row);

    if (size < 1) {
      return sum;
    }

    if (std::abs(start) >= size) {
      return sum;
    }

    if (start < 0) {
      start += size;
    }

    int end = size;
    if (length >= 0 && start + length < end) {
      end = start + length;
    }

    auto addElement = [](TOutput& sum, TInput value) { sum += value; };

    for (auto i = start + offset; i < end + offset; i++) {
      if constexpr (mayHaveNulls) {
        BOLT_USER_CHECK(!isNullFunc(i), "can't calculate null value in array");
      }
      addElement(sum, dataAtFunc(i));
    }
    return sum;
  }

  template <
      bool mayHaveNulls,
      typename DataAtFunc,
      typename IsNullFunc,
      typename GetArgFunc>
  void applyCore(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      const ArrayVector* arrayVector,
      FlatVector<TOutput>* resultValues,
      DataAtFunc&& dataAtFunc,
      IsNullFunc&& isNullFunc,
      GetArgFunc&& getStartFunc,
      GetArgFunc&& getLengthFunc) const {
    rows.applyToSelected([&](auto row) {
      auto start = getStartFunc(row);
      auto length = getLengthFunc(row);
      resultValues->set(
          row,
          applyCore<mayHaveNulls>(
              row, arrayVector, dataAtFunc, isNullFunc, start, length));
    });
  }

  template <
      bool mayHaveNulls,
      typename DataAtFunc,
      typename IsNullFunc,
      typename GetArgFunc>
  void applyCore(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      VectorPtr args0,
      const ArrayVector* arrayVector,
      FlatVector<TOutput>* resultValues,
      DataAtFunc&& dataAtFunc,
      IsNullFunc&& isNullFunc,
      GetArgFunc&& getStartFunc,
      GetArgFunc&& getLengthFunc) const {
    rows.applyToSelected([&](auto row) {
      auto start = getStartFunc(row);
      auto length = getLengthFunc(row);
      resultValues->set(
          row,
          applyCore<mayHaveNulls>(
              args0->wrappedIndex(row),
              arrayVector,
              dataAtFunc,
              isNullFunc,
              start,
              length));
    });
  }

  template <bool mayHaveNulls, typename GetArgFunc>
  void applyFlat(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      ArrayVector* arrayVector,
      const uint64_t* rawNulls,
      const TInput* rawElements,
      FlatVector<TOutput>* resultValues,
      GetArgFunc&& getStartFunc,
      GetArgFunc&& getLengthFunc) const {
    applyCore<mayHaveNulls>(
        rows,
        context,
        arrayVector,
        resultValues,
        [&](vector_size_t index) { return rawElements[index]; },
        [&](vector_size_t index) { return bits::isBitNull(rawNulls, index); },
        getStartFunc,
        getLengthFunc);
  }

  template <bool mayHaveNulls, typename GetArgFunc>
  void applyNonFlat(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      ArrayVector* arrayVector,
      exec::LocalDecodedVector& elements,
      FlatVector<TOutput>* resultValues,
      GetArgFunc&& getStartFunc,
      GetArgFunc&& getLengthFunc) const {
    applyCore<mayHaveNulls>(
        rows,
        context,
        arrayVector,
        resultValues,
        [&](vector_size_t index) {
          return elements->template valueAt<TInput>(index);
        },
        [&](vector_size_t index) { return elements->isNullAt(index); },
        getStartFunc,
        getLengthFunc);
  }

  template <bool mayHaveNulls, typename GetArgFunc>
  void applyConstant(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      VectorPtr args0,
      ArrayVector* arrayVector,
      exec::LocalDecodedVector& elements,
      FlatVector<TOutput>* resultValues,
      GetArgFunc&& getStartFunc,
      GetArgFunc&& getLengthFunc) const {
    applyCore<mayHaveNulls>(
        rows,
        context,
        args0,
        arrayVector,
        resultValues,
        [&](vector_size_t index) { return elements->valueAt<TInput>(index); },
        [&](vector_size_t index) { return elements->isNullAt(index); },
        getStartFunc,
        getLengthFunc);
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Input is either flat or constant.

    BOLT_USER_CHECK(args.size() == 3, "Expected 3 arguments");
    BOLT_CHECK_EQ(args[1]->typeKind(), TypeKind::INTEGER);
    BOLT_CHECK_EQ(args[2]->typeKind(), TypeKind::INTEGER);

    exec::LocalDecodedVector starts(context, *args[1], rows);
    exec::LocalDecodedVector lengths(context, *args[2], rows);
    int32_t start;
    int32_t length;
    std::function<int32_t(vector_size_t)> getStartFunc = nullptr;
    std::function<int32_t(vector_size_t)> getLengthFunc = nullptr;
    if (args[1]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[1]->isNullAt(0) == false) {
      start = (args[1])->as<ConstantVector<int32_t>>()->valueAt(0);
      getStartFunc = [&](vector_size_t index) -> int32_t { return start; };
    } else {
      getStartFunc = [&](vector_size_t index) -> int32_t {
        return starts->valueAt<int32_t>(index);
      };
    }

    if (args[2]->encoding() == VectorEncoding::Simple::CONSTANT &&
        args[2]->isNullAt(0) == false) {
      length = (args[2])->as<ConstantVector<int32_t>>()->valueAt(0);
      getLengthFunc = [&](vector_size_t index) -> int32_t { return length; };
    } else {
      getLengthFunc = [&](vector_size_t index) -> int32_t {
        return lengths->valueAt<int32_t>(index);
      };
    }

    if (args[0]->isConstantEncoding() && args[1]->isConstantEncoding() &&
        args[2]->isConstantEncoding()) {
      auto arrayVector = args[0]->wrappedVector()->as<ArrayVector>();
      auto arrayRow = args[0]->wrappedIndex(rows.begin());
      BOLT_CHECK(arrayVector);
      auto elementsVector = arrayVector->elements();

      SelectivityVector elementsRows(elementsVector->size());
      exec::LocalDecodedVector elements(context, *elementsVector, elementsRows);

      TOutput sum;
      try {
        if (elementsVector->mayHaveNulls()) {
          sum = applyCore<true>(
              arrayRow,
              arrayVector,
              [&](auto index) { return elements->valueAt<TInput>(index); },
              [&](auto index) { return elements->isNullAt(index); },
              start,
              length);
        } else {
          sum = applyCore<false>(
              arrayRow,
              arrayVector,
              [&](auto index) { return elements->valueAt<TInput>(index); },
              [&](auto index) { return elements->isNullAt(index); },
              start,
              length);
        }
      } catch (...) {
        context.setErrors(rows, std::current_exception());
      }

      context.moveOrCopyResult(
          std::make_shared<ConstantVector<TOutput>>(
              context.pool(),
              rows.end(),
              false /*isNull*/,
              outputType,
              std::move(sum)),
          rows,
          result);
      return;
    }

    BOLT_CHECK_EQ(args[0]->typeKind(), TypeKind::ARRAY);

    // Prepare result vector for writing
    BaseVector::ensureWritable(rows, outputType, context.pool(), result);
    auto resultValues = result->template asFlatVector<TOutput>();
    BOLT_CHECK(resultValues);

    if (args[0]->isConstantEncoding()) {
      auto arrayVector =
          const_cast<ArrayVector*>(args[0]->wrappedVector()->as<ArrayVector>());
      auto arrayRow = args[0]->wrappedIndex(rows.begin());
      BOLT_CHECK(arrayVector);
      auto elementsVector = arrayVector->elements();

      SelectivityVector elementsRows(elementsVector->size());
      exec::LocalDecodedVector elements(context, *elementsVector, elementsRows);

      if (elementsVector->mayHaveNulls()) {
        applyConstant<true>(
            rows,
            context,
            args[0],
            arrayVector,
            elements,
            resultValues,
            getStartFunc,
            getLengthFunc);
      } else {
        applyConstant<false>(
            rows,
            context,
            args[0],
            arrayVector,
            elements,
            resultValues,
            getStartFunc,
            getLengthFunc);
      }
      return;
    }

    if (args[0]->encoding() == VectorEncoding::Simple::DICTIONARY) {
      BaseVector::flattenVector(args[0]);
    }
    auto arrayVector = args[0]->as<ArrayVector>();

    BOLT_CHECK(arrayVector);
    auto elementsVector = arrayVector->elements();

    if (elementsVector->isFlatEncoding()) {
      const TInput* __restrict rawElements =
          elementsVector->as<FlatVector<TInput>>()->rawValues();
      const uint64_t* __restrict rawNulls = elementsVector->rawNulls();

      if (elementsVector->mayHaveNulls()) {
        applyFlat<true>(
            rows,
            context,
            arrayVector,
            rawNulls,
            rawElements,
            resultValues,
            getStartFunc,
            getLengthFunc);
      } else {
        applyFlat<false>(
            rows,
            context,
            arrayVector,
            rawNulls,
            rawElements,
            resultValues,
            getStartFunc,
            getLengthFunc);
      }
    } else {
      SelectivityVector elementsRows(elementsVector->size());
      exec::LocalDecodedVector elements(context, *elementsVector, elementsRows);

      if (elementsVector->mayHaveNulls()) {
        applyNonFlat<true>(
            rows,
            context,
            arrayVector,
            elements,
            resultValues,
            getStartFunc,
            getLengthFunc);
      } else {
        applyNonFlat<false>(
            rows,
            context,
            arrayVector,
            elements,
            resultValues,
            getStartFunc,
            getLengthFunc);
      }
    }
  }

  bool isDefaultNullBehavior() const override {
    return false;
  }
};

// Create function.
std::shared_ptr<exec::VectorFunction> create(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto elementType = inputArgs.front().type->childAt(0);

  switch (elementType->kind()) {
    case TypeKind::TINYINT: {
      return std::make_shared<ArraySliceSumFunction<
          TypeTraits<TypeKind::TINYINT>::NativeType,
          int64_t>>();
    }
    case TypeKind::SMALLINT: {
      return std::make_shared<ArraySliceSumFunction<
          TypeTraits<TypeKind::SMALLINT>::NativeType,
          int64_t>>();
    }
    case TypeKind::INTEGER: {
      return std::make_shared<ArraySliceSumFunction<
          TypeTraits<TypeKind::INTEGER>::NativeType,
          int64_t>>();
    }
    case TypeKind::BIGINT: {
      return std::make_shared<ArraySliceSumFunction<
          TypeTraits<TypeKind::BIGINT>::NativeType,
          int64_t>>();
    }
    case TypeKind::DOUBLE: {
      return std::make_shared<ArraySliceSumFunction<
          TypeTraits<TypeKind::DOUBLE>::NativeType,
          double>>();
    }
    case TypeKind::REAL: {
      return std::make_shared<ArraySliceSumFunction<
          TypeTraits<TypeKind::REAL>::NativeType,
          double>>();
    }
    default: {
      BOLT_FAIL("Unsupported Type")
    }
  }
}

// Define function signature.
// array(T1) -> T2 where T1 must be coercible to bigint or double, and
// T2 is bigint or double
std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  static const std::map<std::string, std::string> typePairs = {
      {"tinyint", "bigint"},
      {"smallint", "bigint"},
      {"integer", "bigint"},
      {"bigint", "bigint"},
      {"double", "double"},
      {"real", "double"}};
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  signatures.reserve(typePairs.size());
  for (const auto& [argType, returnType] : typePairs) {
    signatures.emplace_back(exec::FunctionSignatureBuilder()
                                .returnType(returnType)
                                .argumentType(fmt::format("array({})", argType))
                                .argumentType("integer")
                                .argumentType("integer")
                                .build());
  }
  return signatures;
}
} // namespace

// Register function.
BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_array_slice_sum,
    signatures(),
    create);

} // namespace bytedance::bolt::functions
