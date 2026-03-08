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

#include "bolt/functions/sparksql/LeastGreatest.h"

#include "bolt/expression/EvalCtx.h"
#include "bolt/expression/Expr.h"
#include "bolt/functions/lib/SIMDComparisonUtil.h"
#include "bolt/functions/sparksql/Comparisons.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::functions::sparksql {
namespace {

template <typename Cmp, TypeKind kind>
class ComparisonFunction final : public exec::VectorFunction {
  using T = typename TypeTraits<kind>::NativeType;

  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const Cmp cmp;
    context.ensureWritable(rows, BOOLEAN(), result);
    result->clearNulls(rows);
    if constexpr (
        kind == TypeKind::TINYINT || kind == TypeKind::SMALLINT ||
        kind == TypeKind::INTEGER || kind == TypeKind::BIGINT) {
      if ((args[0]->isFlatEncoding() || args[0]->isConstantEncoding()) &&
          (args[1]->isFlatEncoding() || args[1]->isConstantEncoding()) &&
          rows.isAllSelected()) {
        applySimdComparison<T, Cmp>(rows, args, result);
        return;
      }
    }
    if (shouldApplyAutoSimdComparison<T, T>(rows, args)) {
      applyAutoSimdComparison<T, T, Cmp>(rows, args, context, result);
      return;
    }
    auto* flatResult = result->asUnchecked<FlatVector<bool>>();

    if (args[0]->isFlatEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (flat, flat).
      const auto* rawA = args[0]->asUnchecked<FlatVector<T>>()->rawValues();
      const auto* rawB = args[1]->asUnchecked<FlatVector<T>>()->rawValues();
      rows.applyToSelected(
          [&](vector_size_t i) { flatResult->set(i, cmp(rawA[i], rawB[i])); });
    } else if (args[0]->isConstantEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (const, flat).
      auto constant = args[0]->asUnchecked<ConstantVector<T>>()->valueAt(0);
      const auto* rawValues =
          args[1]->asUnchecked<FlatVector<T>>()->rawValues();
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(i, cmp(constant, rawValues[i]));
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isConstantEncoding()) {
      // Fast path for (flat, const).
      const auto* rawValues =
          args[0]->asUnchecked<FlatVector<T>>()->rawValues();
      auto constant = args[1]->asUnchecked<ConstantVector<T>>()->valueAt(0);
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(i, cmp(rawValues[i], constant));
      });
    } else {
      // Path if one or more arguments are encoded.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto decodedA = decodedArgs.at(0);
      auto decodedB = decodedArgs.at(1);
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(
            i, cmp(decodedA->valueAt<T>(i), decodedB->valueAt<T>(i)));
      });
    }
  }
};

// ComparisonFunction instance for Array
template <typename Cmp>
class ArrayComparisonFunction final : public exec::VectorFunction {
  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    Cmp cmp;
    const CompareFlags flags{.equalsOnly = cmp.equalOnly()};

    context.ensureWritable(rows, BOOLEAN(), result);
    auto* flatResult = result->asUnchecked<FlatVector<bool>>();
    BOLT_CHECK_EQ(args[0]->typeKind(), TypeKind::ARRAY);
    BOLT_CHECK_EQ(args[1]->typeKind(), TypeKind::ARRAY);

    auto arrayCompareForOneConstant =
        [&](VectorPtr& args0, VectorPtr& args1, const int neg) {
          auto arrayVectorA = args0->wrappedVector();
          auto arrayVectorB = args1->wrappedVector();
          BOLT_CHECK(arrayVectorA);
          BOLT_CHECK(arrayVectorB);
          auto arrayRowB = args1->wrappedIndex(rows.begin());
          rows.applyToSelected([&](vector_size_t i) {
            auto cmpRes = arrayVectorA->compare(
                arrayVectorB, args0->wrappedIndex(i), arrayRowB, flags);
            if (!cmpRes.has_value()) {
              BOLT_USER_FAIL("Encounter null in array compare");
            } else {
              flatResult->set(i, (cmp(cmpRes.value() * neg, 0)));
            }
          });
        };

    if (args[0]->isConstantEncoding() && !args[1]->isConstantEncoding()) {
      arrayCompareForOneConstant(args[1], args[0], -1);
      return;
    }

    if (!args[0]->isConstantEncoding() && args[1]->isConstantEncoding()) {
      arrayCompareForOneConstant(args[0], args[1], 1);
      return;
    }

    auto arrayVectorA = args[0]->wrappedVector();
    auto arrayVectorB = args[1]->wrappedVector();
    BOLT_CHECK(arrayVectorA);
    BOLT_CHECK(arrayVectorB);

    if (args[0]->isConstantEncoding() && args[1]->isConstantEncoding()) {
      auto arrayRowA = args[0]->wrappedIndex(rows.begin());
      auto arrayRowB = args[1]->wrappedIndex(rows.begin());
      rows.applyToSelected([&](vector_size_t i) {
        auto cmpRes =
            arrayVectorA->compare(arrayVectorB, arrayRowA, arrayRowB, flags);
        if (!cmpRes.has_value()) {
          BOLT_USER_FAIL("Encounter null in array compare");
        } else {
          flatResult->set(i, (cmp(cmpRes.value(), 0)));
        }
      });
      return;
    }

    rows.applyToSelected([&](vector_size_t i) {
      auto cmpRes = arrayVectorA->compare(
          arrayVectorB,
          args[0]->wrappedIndex(i),
          args[1]->wrappedIndex(i),
          flags);
      if (!cmpRes.has_value()) {
        BOLT_USER_FAIL("Encounter null in array compare");
      } else {
        flatResult->set(i, (cmp(cmpRes.value(), 0)));
      }
    });
  }
};

// ComparisonFunction instance for bool as it uses compact representation
template <typename Cmp>
class BoolComparisonFunction final : public exec::VectorFunction {
  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, BOOLEAN(), result);
    auto* flatResult = result->asUnchecked<FlatVector<bool>>();
    const Cmp cmp;

    if (args[0]->isFlatEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (flat, flat).
      auto rawA = args[0]
                      ->asUnchecked<FlatVector<bool>>()
                      ->template mutableRawValues<uint64_t>();
      auto rawB = args[1]
                      ->asUnchecked<FlatVector<bool>>()
                      ->template mutableRawValues<uint64_t>();
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(
            i, cmp(bits::isBitSet(rawA, i), bits::isBitSet(rawB, i)));
      });
    } else if (args[0]->isConstantEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (const, flat).
      auto constant = args[0]->asUnchecked<ConstantVector<bool>>()->valueAt(0);
      auto rawValues = args[1]
                           ->asUnchecked<FlatVector<bool>>()
                           ->template mutableRawValues<uint64_t>();
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(i, cmp(constant, bits::isBitSet(rawValues, i)));
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isConstantEncoding()) {
      // Fast path for (flat, const).
      auto rawValues = args[0]
                           ->asUnchecked<FlatVector<bool>>()
                           ->template mutableRawValues<uint64_t>();
      auto constant = args[1]->asUnchecked<ConstantVector<bool>>()->valueAt(0);
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(i, cmp(bits::isBitSet(rawValues, i), constant));
      });
    } else {
      // Fast path if one or more arguments are encoded.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto decodedA = decodedArgs.at(0);
      auto decodedB = decodedArgs.at(1);
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(
            i, cmp(decodedA->valueAt<bool>(i), decodedB->valueAt<bool>(i)));
      });
    }
  }
};

template <template <typename> class Cmp, typename StdCmp>
std::shared_ptr<exec::VectorFunction> makeImpl(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args) {
  BOLT_CHECK_EQ(args.size(), 2);
  for (size_t i = 1; i < args.size(); i++) {
    BOLT_CHECK(*args[i].type == *args[0].type);
  }
  switch (args[0].type->kind()) {
#define SCALAR_CASE(kind)                            \
  case TypeKind::kind:                               \
    return std::make_shared<ComparisonFunction<      \
        Cmp<TypeTraits<TypeKind::kind>::NativeType>, \
        TypeKind::kind>>();
    SCALAR_CASE(REAL)
    SCALAR_CASE(DOUBLE)
    SCALAR_CASE(HUGEINT)
    SCALAR_CASE(VARCHAR)
    SCALAR_CASE(VARBINARY)
    SCALAR_CASE(TIMESTAMP)
#undef SCALAR_CASE
#define STD_SCALAR_CASE(kind) \
  case TypeKind::kind:        \
    return std::make_shared<ComparisonFunction<StdCmp, TypeKind::kind>>();
    STD_SCALAR_CASE(TINYINT)
    STD_SCALAR_CASE(SMALLINT)
    STD_SCALAR_CASE(INTEGER)
    STD_SCALAR_CASE(BIGINT)
#undef STDSCALAR_CASE
    case TypeKind::BOOLEAN:
      return std::make_shared<BoolComparisonFunction<
          Cmp<TypeTraits<TypeKind::BOOLEAN>::NativeType>>>();
    case TypeKind::ARRAY:
      return std::make_shared<ArrayComparisonFunction<Cmp<int32_t>>>();
    default:
      BOLT_NYI(
          "{} does not support arguments of type {}",
          functionName,
          args[0].type->kind());
  }
}

template <TypeKind kind>
void applyTyped(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    DecodedVector* decodedLhs,
    DecodedVector* decodedRhs,
    exec::EvalCtx& context,
    FlatVector<bool>* flatResult) {
  using T = typename TypeTraits<kind>::NativeType;

  Equal<T> equal;
  if (!args[0]->mayHaveNulls() && !args[1]->mayHaveNulls()) {
    // When there is no nulls, it reduces to normal equality function
    rows.applyToSelected([&](vector_size_t i) {
      flatResult->set(
          i, equal(decodedLhs->valueAt<T>(i), decodedRhs->valueAt<T>(i)));
    });
  } else {
    // (isnull(a) AND isnull(b)) || (a == b)
    // When DecodedVector::nulls() is null it means there are no nulls.
    auto* rawNulls0 = decodedLhs->nulls(&rows);
    auto* rawNulls1 = decodedRhs->nulls(&rows);
    rows.applyToSelected([&](vector_size_t i) {
      auto isNull0 = rawNulls0 && bits::isBitNull(rawNulls0, i);
      auto isNull1 = rawNulls1 && bits::isBitNull(rawNulls1, i);
      flatResult->set(
          i,
          (isNull0 || isNull1)
              ? isNull0 && isNull1
              : equal(decodedLhs->valueAt<T>(i), decodedRhs->valueAt<T>(i)));
    });
  }
}

class EqualtoNullSafe final : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decodedArgs(rows, args, context);

    DecodedVector* decodedLhs = decodedArgs.at(0);
    DecodedVector* decodedRhs = decodedArgs.at(1);
    context.ensureWritable(rows, BOOLEAN(), result);
    auto* flatResult = result->asUnchecked<FlatVector<bool>>();
    flatResult->mutableRawValues<int64_t>();

    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        applyTyped,
        args[0]->typeKind(),
        rows,
        args,
        decodedLhs,
        decodedRhs,
        context,
        flatResult);
  }
};

} // namespace

std::shared_ptr<exec::VectorFunction> makeEqualTo(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig& /*config*/) {
  return makeImpl<Equal, std::equal_to<>>(functionName, args);
}

std::shared_ptr<exec::VectorFunction> makeLessThan(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig& /*config*/) {
  return makeImpl<Less, std::less<>>(functionName, args);
}

std::shared_ptr<exec::VectorFunction> makeGreaterThan(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig& /*config*/) {
  return makeImpl<Greater, std::greater<>>(functionName, args);
}

std::shared_ptr<exec::VectorFunction> makeLessThanOrEqual(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig& /*config*/) {
  return makeImpl<LessOrEqual, std::less_equal<>>(functionName, args);
}

std::shared_ptr<exec::VectorFunction> makeGreaterThanOrEqual(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig& /*config*/) {
  return makeImpl<GreaterOrEqual, std::greater_equal<>>(functionName, args);
}

std::shared_ptr<exec::VectorFunction> makeEqualToNullSafe(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  static const auto kEqualtoNullSafeFunction =
      std::make_shared<EqualtoNullSafe>();
  return kEqualtoNullSafeFunction;
}
} // namespace bytedance::bolt::functions::sparksql
