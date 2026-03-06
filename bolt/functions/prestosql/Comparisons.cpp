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

#include "bolt/functions/prestosql/Comparisons.h"
#include <bolt/common/base/Exceptions.h>
#include "bolt/functions/Udf.h"
#include "bolt/functions/lib/SIMDComparisonUtil.h"
#include "bolt/vector/BaseVector.h"
namespace bytedance::bolt::functions {

namespace {

/// This class implements comparison for vectors of primitive types using SIMD.
/// Currently this only supports fixed length primitive types (except Boolean).
/// It also requires the vectors to have a flat encoding.
/// If the vector encoding is not flat, we revert to non simd approach.
template <typename ComparisonOp, typename Arch = xsimd::default_arch>
struct SimdComparator {
  template <
      TypeKind kind,
      typename std::enable_if_t<
          (xsimd::has_simd_register<
               typename TypeTraits<kind>::NativeType>::value &&
           kind != TypeKind::BOOLEAN) ||
              kind == TypeKind::HUGEINT,
          int> = 0>
  void applyComparison(
      const SelectivityVector& rows,
      BaseVector& lhs,
      BaseVector& rhs,
      exec::EvalCtx& context,
      VectorPtr& result) {
    using T = typename TypeTraits<kind>::NativeType;

    auto resultVector = result->asUnchecked<FlatVector<bool>>();
    auto rawResult = resultVector->mutableRawValues<uint8_t>();

    auto isSimdizable = (lhs.isConstantEncoding() || lhs.isFlatEncoding()) &&
        (rhs.isConstantEncoding() || rhs.isFlatEncoding()) &&
        rows.isAllSelected();

    if (!isSimdizable || std::is_same_v<T, int128_t>) {
      exec::LocalDecodedVector lhsDecoded(context, lhs, rows);
      exec::LocalDecodedVector rhsDecoded(context, rhs, rows);

      context.applyToSelectedNoThrow(rows, [&](auto row) {
        auto l = lhsDecoded->template valueAt<T>(row);
        auto r = rhsDecoded->template valueAt<T>(row);
        auto filtered = ComparisonOp()(l, r);
        resultVector->set(row, filtered);
      });
      return;
    }

    if constexpr (!std::is_same_v<T, int128_t>) {
      if (lhs.isConstantEncoding() && rhs.isConstantEncoding()) {
        auto l = lhs.asUnchecked<ConstantVector<T>>()->valueAt(0);
        auto r = rhs.asUnchecked<ConstantVector<T>>()->valueAt(0);
        applySimdComparison<T, true, true, ComparisonOp>(
            rows.begin(), rows.end(), &l, &r, rawResult);
      } else if (lhs.isConstantEncoding()) {
        auto l = lhs.asUnchecked<ConstantVector<T>>()->valueAt(0);
        auto rawRhs = rhs.asUnchecked<FlatVector<T>>()->rawValues();
        applySimdComparison<T, true, false, ComparisonOp>(
            rows.begin(), rows.end(), &l, rawRhs, rawResult);
      } else if (rhs.isConstantEncoding()) {
        auto rawLhs = lhs.asUnchecked<FlatVector<T>>()->rawValues();
        auto r = rhs.asUnchecked<ConstantVector<T>>()->valueAt(0);
        applySimdComparison<T, false, true, ComparisonOp>(
            rows.begin(), rows.end(), rawLhs, &r, rawResult);
      } else {
        auto rawLhs = lhs.asUnchecked<FlatVector<T>>()->rawValues();
        auto rawRhs = rhs.asUnchecked<FlatVector<T>>()->rawValues();
        applySimdComparison<T, false, false, ComparisonOp>(
            rows.begin(), rows.end(), rawLhs, rawRhs, rawResult);
      }

      resultVector->clearNulls(rows);
    }
  }

  template <
      TypeKind kind,
      typename std::enable_if_t<
          (!xsimd::has_simd_register<
               typename TypeTraits<kind>::NativeType>::value ||
           kind == TypeKind::BOOLEAN) &&
              kind != TypeKind::HUGEINT,
          int> = 0>
  void applyComparison(
      const SelectivityVector& /* rows */,
      BaseVector& /* lhs */,
      BaseVector& /* rhs */,
      exec::EvalCtx& /* context */,
      VectorPtr& /* result */) {
    BOLT_UNSUPPORTED("Unsupported type for SIMD comparison");
  }
};

template <typename ComparisonOp, typename Arch = xsimd::default_arch>
class ComparisonSimdFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_EQ(args.size(), 2, "Comparison requires two arguments");
    BOLT_CHECK_EQ(args[0]->typeKind(), args[1]->typeKind());
    BOLT_USER_CHECK_EQ(outputType->kind(), TypeKind::BOOLEAN);

    context.ensureWritable(rows, outputType, result);
    auto comparator = SimdComparator<ComparisonOp>{};

    if (args[0]->type()->isLongDecimal()) {
      comparator.template applyComparison<TypeKind::HUGEINT>(
          rows, *args[0], *args[1], context, result);
      return;
    }

    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        comparator.template applyComparison,
        args[0]->typeKind(),
        rows,
        *args[0],
        *args[1],
        context,
        result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;

    for (const auto& inputType :
         {"tinyint", "smallint", "integer", "bigint", "real", "double"}) {
      signatures.push_back(exec::FunctionSignatureBuilder()
                               .returnType("boolean")
                               .argumentType(inputType)
                               .argumentType(inputType)
                               .build());
    }
    signatures.push_back(exec::FunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("boolean")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .build());
    return signatures;
  }

  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }
  exec::FunctionCanonicalName getCanonicalName() const override {
    return std::is_same_v<ComparisonOp, std::less<>>
        ? exec::FunctionCanonicalName::kLt
        : exec::FunctionCanonicalName::kUnknown;
  }
};

} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_eq,
    (ComparisonSimdFunction<std::equal_to<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::equal_to<>>>()));

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_neq,
    (ComparisonSimdFunction<std::not_equal_to<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::not_equal_to<>>>()));

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_lt,
    (ComparisonSimdFunction<std::less<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::less<>>>()));

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_gt,
    (ComparisonSimdFunction<std::greater<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::greater<>>>()));

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_lte,
    (ComparisonSimdFunction<std::less_equal<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::less_equal<>>>()));

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_gte,
    (ComparisonSimdFunction<std::greater_equal<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::greater_equal<>>>()));

} // namespace bytedance::bolt::functions
