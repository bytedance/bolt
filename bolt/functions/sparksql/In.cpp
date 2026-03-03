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

#include "folly/container/F14Set.h"
#include "folly/hash/Hash.h"

#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/expression/VectorReaders.h"
#include "bolt/functions/Macros.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/sparksql/Arena.h"
#include "bolt/functions/sparksql/Comparisons.h"
#include "bolt/type/Filter.h"
#include "bolt/vector/DecodedVector.h"
namespace bytedance::bolt::functions::sparksql {
namespace {

template <typename T>
class Set : public folly::F14FastSet<T, folly::hasher<T>, Equal<T>> {};

template <>
class Set<StringView> {
 public:
  using value_type = std::string_view;

  void emplace(const StringView& s) {
    std::string_view sv(s.data(), s.size());
    if (!set_.contains(sv)) {
      set_.emplace(arena_.writeString(sv));
    }
  }

  bool contains(const StringView& s) const {
    return set_.contains(std::string_view(s.data(), s.size()));
  }

  void reserve(size_t size) {
    set_.reserve(size);
  }

  size_t size() const {
    return set_.size();
  }

  auto begin() const {
    return set_.begin();
  }

 private:
  Arena arena_;
  folly::F14FastSet<std::string_view> set_;
};

template <typename TInput>
struct InFunctionOuter {
  template <typename TExecCtx>
  struct InFunctionInner {
    BOLT_DEFINE_FUNCTION_TYPES(TExecCtx);

    FOLLY_ALWAYS_INLINE void initialize(
        const std::vector<TypePtr>& /*inputTypes*/,
        const core::QueryConfig& /*config*/,
        const arg_type<TInput>* /*searchTerm*/,
        const arg_type<bolt::Array<TInput>>* searchElements) {
      if (searchElements == nullptr) {
        return;
      }

      elements_.reserve(searchElements->size());

      for (const auto& entry : *searchElements) {
        if (!entry.has_value()) {
          hasNull_ = true;
          continue;
        }
        elements_.emplace(entry.value());
      }
    }

    FOLLY_ALWAYS_INLINE bool callNullable(
        bool& result,
        const arg_type<TInput>* searchTerm,
        const arg_type<bolt::Array<TInput>>* /*array*/) {
      if (searchTerm == nullptr) {
        return false;
      }

      result = elements_.contains(*searchTerm);
      if (hasNull_ && !result) {
        return false;
      }
      return true;
    }

   private:
    Set<arg_type<TInput>> elements_;
    bool hasNull_{false};
  };

  template <typename T>
  using Inner = typename InFunctionOuter<TInput>::template InFunctionInner<T>;
};

template <typename T>
void registerInFn(const std::string& prefix) {
  registerFunction<InFunctionOuter<T>::template Inner, bool, T, Array<T>>(
      {prefix + "in"});
}

std::vector<std::shared_ptr<exec::FunctionSignature>> inDecimalSignatures() {
  // Accept any DECIMAL(p, s). The physical representation depends on
  // precision: short decimal (<= 18) uses BIGINT, long decimal uses HUGEINT.
  return {exec::FunctionSignatureBuilder()
              .integerVariable("p")
              .integerVariable("s")
              .returnType("boolean")
              .argumentType("DECIMAL(p, s)")
              .argumentType("array(DECIMAL(p, s))")
              .build()};
}

template <typename T>
class DecimalInVectorFunction final : public exec::VectorFunction {
 public:
  explicit DecimalInVectorFunction(const VectorPtr& constantArrayVector) {
    buildConstantSet(constantArrayVector);
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, outputType, result);
    result->clearNulls(rows);
    auto* flatResult = result->asUnchecked<FlatVector<bool>>();

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto* decodedLhs = decodedArgs.at(0);
    auto* decodedRhs = decodedArgs.at(1);

    if (hasConstantRhs_) {
      if (constantRhsIsNull_) {
        rows.applyToSelected([&](vector_size_t row) {
          if (decodedLhs->isNullAt(row)) {
            result->setNull(row, true);
            return;
          }
          // Non-null lhs IN NULL -> NULL.
          result->setNull(row, true);
        });
        return;
      }

      rows.applyToSelected([&](vector_size_t row) {
        if (decodedLhs->isNullAt(row)) {
          result->setNull(row, true);
          return;
        }

        const auto lhs = decodedLhs->valueAt<T>(row);
        const bool found = constantElements_.contains(lhs);
        if (found) {
          flatResult->set(row, true);
          return;
        }
        if (constantHasNull_) {
          result->setNull(row, true);
          return;
        }
        flatResult->set(row, false);
      });
      return;
    }

    // Fallback for non-constant rhs: scan per row.
    exec::VectorReader<Array<T>> rhsReader(decodedRhs);
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedLhs->isNullAt(row)) {
        result->setNull(row, true);
        return;
      }
      if (decodedRhs->isNullAt(row)) {
        result->setNull(row, true);
        return;
      }

      const auto lhs = decodedLhs->valueAt<T>(row);
      const auto arrayView = rhsReader[row];
      bool hasNull = false;
      bool found = false;
      for (const auto& entry : arrayView) {
        if (!entry.has_value()) {
          hasNull = true;
          continue;
        }
        if (entry.value() == lhs) {
          found = true;
          break;
        }
      }

      if (found) {
        flatResult->set(row, true);
        return;
      }
      if (hasNull) {
        result->setNull(row, true);
        return;
      }
      flatResult->set(row, false);
    });
  }

 private:
  void buildConstantSet(const VectorPtr& constantArrayVector) {
    if (constantArrayVector == nullptr) {
      // Not a constant argument.
      hasConstantRhs_ = false;
      return;
    }

    hasConstantRhs_ = true;

    SelectivityVector oneRow(1);
    DecodedVector decoded(*constantArrayVector, oneRow);
    exec::VectorReader<Array<T>> reader(&decoded);
    if (decoded.isNullAt(0)) {
      constantRhsIsNull_ = true;
      return;
    }

    const auto arrayView = reader[0];
    constantElements_.reserve(arrayView.size());
    for (const auto& entry : arrayView) {
      if (!entry.has_value()) {
        constantHasNull_ = true;
        continue;
      }
      constantElements_.emplace(entry.value());
    }
  }

  bool hasConstantRhs_{false};
  bool constantHasNull_{false};
  bool constantRhsIsNull_{false};
  Set<T> constantElements_;
};

std::shared_ptr<exec::VectorFunction> makeDecimalIn(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  const auto& lhsType = inputArgs.at(0).type;
  const auto& rhsConst = inputArgs.at(1).constantValue;

  if (lhsType->isShortDecimal()) {
    return std::make_shared<DecimalInVectorFunction<int64_t>>(rhsConst);
  }
  if (lhsType->isLongDecimal()) {
    return std::make_shared<DecimalInVectorFunction<int128_t>>(rhsConst);
  }

  BOLT_FAIL(
      "Decimal IN expects DECIMAL input type, but got {}", lhsType->toString());
}

} // namespace

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_in_decimal,
    inDecimalSignatures(),
    makeDecimalIn);

void registerIn(const std::string& prefix) {
  registerInFn<int8_t>(prefix);
  registerInFn<int16_t>(prefix);
  registerInFn<int32_t>(prefix);
  registerInFn<int64_t>(prefix);
  registerInFn<float>(prefix);
  registerInFn<double>(prefix);
  registerInFn<bool>(prefix);
  registerInFn<Varchar>(prefix);
  registerInFn<Timestamp>(prefix);
  registerInFn<Date>(prefix);
}

} // namespace bytedance::bolt::functions::sparksql
