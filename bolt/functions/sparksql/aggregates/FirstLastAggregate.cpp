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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include <string>

#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/lib/aggregates/AggregateToIntermediate.h"
#include "bolt/functions/lib/aggregates/SimpleNumericAggregate.h"
#include "bolt/functions/lib/aggregates/SingleValueAccumulator.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::functions::aggregate;
namespace bytedance::bolt::functions::aggregate::sparksql {

namespace {

/// FirstLastAggregate returns the first or last value of |expr| for a group of
/// rows. If |ignoreNull| is true, returns only non-null values.
///
/// The function is non-deterministic because its results depends on the order
/// of the rows which may be non-deterministic after a shuffle.
template <bool ignoreNull, typename TData, bool numeric>
class FirstLastAggregateBase
    : public SimpleNumericAggregate<TData, TData, TData> {
  using BaseAggregate = SimpleNumericAggregate<TData, TData, TData>;

 protected:
  using TAccumulator = std::conditional_t<
      numeric,
      std::optional<TData>,
      std::optional<SingleValueAccumulator>>;

 public:
  explicit FirstLastAggregateBase(TypePtr resultType)
      : BaseAggregate(std::move(resultType)) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TAccumulator);
  }

  int32_t accumulatorAlignmentSize() const override {
    return 1;
  }

  bool isFixedSize() const override {
    return numeric;
  }

  bool supportsToIntermediate() const override {
    return true;
  }

  FLATTEN void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    const auto& input = args[0];
    result->resize(rows.size());
    copyToIntermediate(rows, args, result->as<RowVector>()->childAt(0));
    auto rawNulls = result->mutableRawNulls();
    // set all rows to null
    bits::setAllNull(rawNulls, input->size());
    rows.applyToSelected([&](vector_size_t i) {
      if (!input->isNullAt(i)) {
        bits::clearNull(rawNulls, i);
      }
    });
  }

  FLATTEN void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    Aggregate::setAllNulls(groups, indices);

    for (auto i : indices) {
      new (groups[i] + Aggregate::offset_) TAccumulator();
    }
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    if constexpr (numeric) {
      BaseAggregate::doExtractValues(
          groups, numGroups, result, [&](char* group) {
            auto accumulator = Aggregate::value<TAccumulator>(group);
            return accumulator->value();
          });
    } else {
      BOLT_CHECK(result);
      (*result)->resize(numGroups);

      auto* rawNulls = Aggregate::getRawNulls(result->get());

      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        if (Aggregate::isNull(group)) {
          (*result)->setNull(i, true);
        } else {
          Aggregate::clearNull(rawNulls, i);
          auto accumulator = Aggregate::value<TAccumulator>(group);
          accumulator->value().read(*result, i);
        }
      }
    }
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    auto rowVector = (*result)->as<RowVector>();
    BOLT_CHECK_EQ(
        rowVector->childrenSize(),
        2,
        "intermediate results must have 2 children");

    auto ignoreNullVector = rowVector->childAt(1)->asFlatVector<bool>();
    rowVector->resize(numGroups);
    ignoreNullVector->resize(numGroups);

    extractValues(groups, numGroups, &(rowVector->childAt(0)));
    for (auto i = 0; i < numGroups; i++) {
      if constexpr (ignoreNull) {
        ignoreNullVector->set(i, false);
        continue;
      }
      if (Aggregate::value<TAccumulator>(groups[i])->has_value()) {
        rowVector->setNull(i, false);
        ignoreNullVector->set(i, true);
      } else {
        ignoreNullVector->set(i, false);
      }
    }
  }

  FLATTEN void destroy(folly::Range<char**> groups) override {
    if constexpr (!numeric) {
      for (auto group : groups) {
        auto accumulator = Aggregate::value<TAccumulator>(group);
        // If ignoreNull is true and groups are all null, accumulator will not
        // set.
        if (accumulator->has_value()) {
          accumulator->value().destroy(Aggregate::allocator_);
        }
      }
    }
  }

 protected:
  template <bool decodeIgnoreNullVector>
  void decodeIntermediateRows(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    this->decodedIntermediates_.decode(*args[0], rows);
    auto rowVector =
        this->decodedIntermediates_.base()->template as<RowVector>();
    BOLT_CHECK_NOT_NULL(rowVector);
    BOLT_CHECK_EQ(
        rowVector->childrenSize(),
        2,
        "intermediate results must have 2 children");
    this->decodedValue_.decode(*rowVector->childAt(0), rows);
    if constexpr (decodeIgnoreNullVector) {
      this->decodedIgnoreNull_.decode(*rowVector->childAt(1), rows);
    }
  }

  DecodedVector decodedValue_;
  DecodedVector decodedIntermediates_;
  DecodedVector decodedIgnoreNull_;
};

template <>
inline int32_t
FirstLastAggregateBase<false, int128_t, true>::accumulatorAlignmentSize()
    const {
  return static_cast<int32_t>(sizeof(int128_t));
}

template <>
inline int32_t
FirstLastAggregateBase<true, int128_t, true>::accumulatorAlignmentSize() const {
  return static_cast<int32_t>(sizeof(int128_t));
}

template <bool ignoreNull, typename TData, bool numeric>
class FirstAggregate
    : public FirstLastAggregateBase<ignoreNull, TData, numeric> {
 public:
  explicit FirstAggregate(TypePtr resultType)
      : FirstLastAggregateBase<ignoreNull, TData, numeric>(
            std::move(resultType)) {}

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodedValue_.decode(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      updateValue<false>(
          i, groups[i], this->decodedValue_, this->decodedIgnoreNull_);
    });
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->template decodeIntermediateRows<true>(rows, args);

    rows.applyToSelected([&](vector_size_t i) {
      if (!this->decodedIntermediates_.isNullAt(i)) {
        updateValue<true>(
            this->decodedIntermediates_.index(i),
            groups[i],
            this->decodedValue_,
            this->decodedIgnoreNull_);
      } else {
        updateNull(groups[i]);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodedValue_.decode(*args[0], rows);

    rows.testSelected([&](vector_size_t i) {
      return updateValue<false>(
          i, group, this->decodedValue_, this->decodedIgnoreNull_);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->template decodeIntermediateRows<true>(rows, args);

    rows.testSelected([&](vector_size_t i) {
      if (!this->decodedIntermediates_.isNullAt(i)) {
        return updateValue<true>(
            this->decodedIntermediates_.index(i),
            group,
            this->decodedValue_,
            this->decodedIgnoreNull_);
      } else {
        return updateNull(group);
      }
    });
  }

 private:
  using TAccumulator =
      typename FirstLastAggregateBase<ignoreNull, TData, numeric>::TAccumulator;

  bool updateNull(char* group) {
    auto accumulator = Aggregate::value<TAccumulator>(group);
    if (accumulator->has_value()) {
      return false;
    }

    if constexpr (ignoreNull) {
      return true;
    } else {
      if constexpr (numeric) {
        *accumulator = TData();
      } else {
        *accumulator = SingleValueAccumulator();
      }
      return false;
    }
  }

  // If we found a valid value, set to accumulator, then skip remaining rows in
  // group.
  template <bool checkIgnoreNullVector>
  bool updateValue(
      vector_size_t index,
      char* group,
      const DecodedVector& decodedVector,
      const DecodedVector& ignoreNullVector) {
    auto accumulator = Aggregate::value<TAccumulator>(group);
    if (accumulator->has_value()) {
      return false;
    }

    if constexpr (!numeric) {
      return updateNonNumeric<checkIgnoreNullVector>(
          index, group, decodedVector, ignoreNullVector);
    } else {
      if (!decodedVector.isNullAt(index)) {
        Aggregate::clearNull(group);
        auto value = decodedVector.valueAt<TData>(index);
        *accumulator = value;
        return false;
      }

      if constexpr (ignoreNull) {
        return true;
      }

      if constexpr (checkIgnoreNullVector) {
        if (!(ignoreNullVector.valueAt<bool>(index))) {
          return true;
        }
      }

      *accumulator = TData();
      return false;
    }
  }

  template <bool checkIgnoreNullVector>
  bool updateNonNumeric(
      vector_size_t index,
      char* group,
      const DecodedVector& decodedVector,
      const DecodedVector& ignoreNullVector) {
    auto accumulator = Aggregate::value<TAccumulator>(group);

    if (!decodedVector.isNullAt(index)) {
      Aggregate::clearNull(group);
      *accumulator = SingleValueAccumulator();
      accumulator->value().write(
          decodedVector.base(),
          decodedVector.index(index),
          Aggregate::allocator_);
      return false;
    }

    if constexpr (ignoreNull) {
      return true;
    }

    if constexpr (checkIgnoreNullVector) {
      if (!(ignoreNullVector.valueAt<bool>(index))) {
        return true;
      }
    }
    *accumulator = SingleValueAccumulator();
    return false;
  }
};

template <bool ignoreNull, typename TData, bool numeric>
class LastAggregate
    : public FirstLastAggregateBase<ignoreNull, TData, numeric> {
 public:
  explicit LastAggregate(TypePtr resultType)
      : FirstLastAggregateBase<ignoreNull, TData, numeric>(
            std::move(resultType)) {}

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodedValue_.decode(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      updateValue<false>(
          i, groups[i], this->decodedValue_, this->decodedIgnoreNull_);
    });
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->template decodeIntermediateRows<true>(rows, args);

    rows.applyToSelected([&](vector_size_t i) {
      if (!this->decodedIntermediates_.isNullAt(i)) {
        updateValue<true>(
            this->decodedIntermediates_.index(i),
            groups[i],
            this->decodedValue_,
            this->decodedIgnoreNull_);
      } else {
        updateNull(groups[i]);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodedValue_.decode(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      updateValue<false>(
          i, group, this->decodedValue_, this->decodedIgnoreNull_);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->template decodeIntermediateRows<true>(rows, args);

    rows.applyToSelected([&](vector_size_t i) {
      if (!this->decodedIntermediates_.isNullAt(i)) {
        updateValue<true>(
            this->decodedIntermediates_.index(i),
            group,
            this->decodedValue_,
            this->decodedIgnoreNull_);
      } else {
        updateNull(group);
      }
    });
  }

 private:
  using TAccumulator =
      typename FirstLastAggregateBase<ignoreNull, TData, numeric>::TAccumulator;

  void updateNull(char* group) {
    auto accumulator = Aggregate::value<TAccumulator>(group);

    if constexpr (!ignoreNull) {
      Aggregate::setNull(group);
      if constexpr (numeric) {
        *accumulator = TData();
      } else {
        *accumulator = SingleValueAccumulator();
      }
    }
  }

  template <bool checkIgnoreNullVector>
  void updateValue(
      vector_size_t index,
      char* group,
      const DecodedVector& decodedVector,
      const DecodedVector& ignoreNullVector) {
    if constexpr (!numeric) {
      return updateNonNumeric<checkIgnoreNullVector>(
          index, group, decodedVector, ignoreNullVector);
    } else {
      auto accumulator = Aggregate::value<TAccumulator>(group);

      if (!decodedVector.isNullAt(index)) {
        Aggregate::clearNull(group);
        *accumulator = decodedVector.valueAt<TData>(index);
        return;
      }

      if constexpr (!ignoreNull) {
        if constexpr (checkIgnoreNullVector) {
          if (!(ignoreNullVector.valueAt<bool>(index))) {
            return;
          }
        }
        Aggregate::setNull(group);
        *accumulator = TData();
      }
    }
  }

  template <bool checkIgnoreNullVector>
  void updateNonNumeric(
      vector_size_t index,
      char* group,
      const DecodedVector& decodedVector,
      const DecodedVector& ignoreNullVector) {
    auto accumulator = Aggregate::value<TAccumulator>(group);

    if (!decodedVector.isNullAt(index)) {
      Aggregate::clearNull(group);
      if (accumulator->has_value()) {
        accumulator->value().destroy(Aggregate::allocator_);
      }
      *accumulator = SingleValueAccumulator();
      accumulator->value().write(
          decodedVector.base(),
          decodedVector.index(index),
          Aggregate::allocator_);
      return;
    }

    if constexpr (!ignoreNull) {
      if constexpr (checkIgnoreNullVector) {
        if (!(ignoreNullVector.valueAt<bool>(index))) {
          return;
        }
      }
      if (accumulator->has_value()) {
        accumulator->value().destroy(Aggregate::allocator_);
      }
      Aggregate::setNull(group);
      *accumulator = SingleValueAccumulator();
    }
  }
};

} // namespace

template <template <bool B1, typename T, bool B2> class TClass, bool ignoreNull>
AggregateRegistrationResult registerFirstLast(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<AggregateFunctionSignature>> signatures = {
      AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("T")
          // Second column is a placeholder.
          .intermediateType("row(T, boolean)")
          .returnType("T")
          .build()};

  signatures.push_back(AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .intermediateType("DECIMAL(a_precision, a_scale)")
                           .returnType("DECIMAL(a_precision, a_scale)")
                           .build());

  return registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<Aggregate> {
        BOLT_CHECK_EQ(argTypes.size(), 1, "{} takes only 1 arguments", name);
        const auto& inputType = argTypes[0];
        TypeKind dataKind = isRawInput(step) ? inputType->kind()
                                             : inputType->childAt(0)->kind();
        switch (dataKind) {
          case TypeKind::BOOLEAN:
            return std::make_unique<TClass<ignoreNull, bool, true>>(resultType);
          case TypeKind::TINYINT:
            return std::make_unique<TClass<ignoreNull, int8_t, true>>(
                resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<TClass<ignoreNull, int16_t, true>>(
                resultType);
          case TypeKind::INTEGER:
            return std::make_unique<TClass<ignoreNull, int32_t, true>>(
                resultType);
          case TypeKind::BIGINT:
            return std::make_unique<TClass<ignoreNull, int64_t, true>>(
                resultType);
          case TypeKind::REAL:
            return std::make_unique<TClass<ignoreNull, float, true>>(
                resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<TClass<ignoreNull, double, true>>(
                resultType);
          case TypeKind::TIMESTAMP:
            return std::make_unique<TClass<ignoreNull, Timestamp, true>>(
                resultType);
          case TypeKind::HUGEINT:
            return std::make_unique<TClass<ignoreNull, int128_t, true>>(
                resultType);
          case TypeKind::VARBINARY:
          case TypeKind::VARCHAR:
          case TypeKind::ARRAY:
          case TypeKind::MAP:
          case TypeKind::ROW:
          case TypeKind::UNKNOWN:
            return std::make_unique<TClass<ignoreNull, ComplexType, false>>(
                resultType);
          default:
            BOLT_FAIL(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->toString());
        }
      },
      withCompanionFunctions,
      overwrite);
}

void registerFirstLastAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerFirstLast<FirstAggregate, false>(
      prefix + "first", withCompanionFunctions, overwrite);
  registerFirstLast<FirstAggregate, true>(
      prefix + "first_ignore_null", withCompanionFunctions, overwrite);
  registerFirstLast<LastAggregate, false>(
      prefix + "last", withCompanionFunctions, overwrite);
  registerFirstLast<LastAggregate, true>(
      prefix + "last_ignore_null", withCompanionFunctions, overwrite);
}

} // namespace bytedance::bolt::functions::aggregate::sparksql
