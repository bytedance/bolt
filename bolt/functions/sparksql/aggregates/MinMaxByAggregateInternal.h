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

#pragma once

#include "bolt/functions/lib/aggregates/MinMaxByAggregatesBase.h"

namespace bytedance::bolt::functions::aggregate::sparksql {

/// Returns compare result align with Spark's specific behavior,
/// which returns true if the value in 'index' row of 'newComparisons' is
/// greater than/equal or less than/equal the value in the 'accumulator'.
template <bool sparkGreaterThan, typename T, typename TAccumulator>
struct SparkComparator {
  static bool compare(
      TAccumulator* accumulator,
      const DecodedVector& newComparisons,
      vector_size_t index,
      bool isFirstValue) {
    if constexpr (isNumeric<T>()) {
      if (isFirstValue) {
        return true;
      }
      if constexpr (sparkGreaterThan) {
        return SimpleVector<T>::comparePrimitiveAsc(
                   newComparisons.valueAt<T>(index), *accumulator) >= 0;
      } else {
        return SimpleVector<T>::comparePrimitiveAsc(
                   newComparisons.valueAt<T>(index), *accumulator) <= 0;
      }
    } else {
      if constexpr (sparkGreaterThan) {
        return !accumulator->hasValue() ||
            compare(accumulator, newComparisons, index) <= 0;
      } else {
        return !accumulator->hasValue() ||
            compare(accumulator, newComparisons, index) >= 0;
      }
    }
  }

  FOLLY_ALWAYS_INLINE static int32_t compare(
      const SingleValueAccumulator* accumulator,
      const DecodedVector& decoded,
      vector_size_t index) {
    static const CompareFlags kCompareFlags{
        true, // nullsFirst
        true, // ascending
        false, // equalsOnly
        CompareFlags::NullHandlingMode::kNullAsValue};
    auto result = accumulator->compare(decoded, index, kCompareFlags);
    return result.value();
  }
};

template <typename ValueType>
struct MinMaxByAggregateFactory {
  static std::unique_ptr<exec::Aggregate> createMax(
      TypePtr resultType,
      TypePtr compareType,
      const std::string& errorMessage);

  static std::unique_ptr<exec::Aggregate> createMin(
      TypePtr resultType,
      TypePtr compareType,
      const std::string& errorMessage);
};

extern template struct MinMaxByAggregateFactory<bool>;
extern template struct MinMaxByAggregateFactory<int8_t>;
extern template struct MinMaxByAggregateFactory<int16_t>;
extern template struct MinMaxByAggregateFactory<int32_t>;
extern template struct MinMaxByAggregateFactory<int64_t>;
extern template struct MinMaxByAggregateFactory<int128_t>;
extern template struct MinMaxByAggregateFactory<float>;
extern template struct MinMaxByAggregateFactory<double>;
extern template struct MinMaxByAggregateFactory<StringView>;
extern template struct MinMaxByAggregateFactory<Timestamp>;
extern template struct MinMaxByAggregateFactory<ComplexType>;

template <typename ValueType, bool isMaxFunc>
std::unique_ptr<exec::Aggregate> createMinMaxByAggregateForValueType(
    TypePtr resultType,
    TypePtr compareType,
    const std::string& errorMessage) {
  if constexpr (isMaxFunc) {
    return MinMaxByAggregateFactory<ValueType>::createMax(
        resultType, compareType, errorMessage);
  } else {
    return MinMaxByAggregateFactory<ValueType>::createMin(
        resultType, compareType, errorMessage);
  }
}

template <bool isMaxFunc>
std::unique_ptr<exec::Aggregate> createMinMaxByAggregate(
    TypePtr resultType,
    TypePtr valueType,
    TypePtr compareType,
    const std::string& errorMessage) {
  switch (valueType->kind()) {
    case TypeKind::BOOLEAN:
      return createMinMaxByAggregateForValueType<bool, isMaxFunc>(
          resultType, compareType, errorMessage);
    case TypeKind::TINYINT:
      return createMinMaxByAggregateForValueType<int8_t, isMaxFunc>(
          resultType, compareType, errorMessage);
    case TypeKind::SMALLINT:
      return createMinMaxByAggregateForValueType<int16_t, isMaxFunc>(
          resultType, compareType, errorMessage);
    case TypeKind::INTEGER:
      return createMinMaxByAggregateForValueType<int32_t, isMaxFunc>(
          resultType, compareType, errorMessage);
    case TypeKind::BIGINT:
      return createMinMaxByAggregateForValueType<int64_t, isMaxFunc>(
          resultType, compareType, errorMessage);
    case TypeKind::HUGEINT:
      return createMinMaxByAggregateForValueType<int128_t, isMaxFunc>(
          resultType, compareType, errorMessage);
    case TypeKind::REAL:
      return createMinMaxByAggregateForValueType<float, isMaxFunc>(
          resultType, compareType, errorMessage);
    case TypeKind::DOUBLE:
      return createMinMaxByAggregateForValueType<double, isMaxFunc>(
          resultType, compareType, errorMessage);
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY:
      return createMinMaxByAggregateForValueType<StringView, isMaxFunc>(
          resultType, compareType, errorMessage);
    case TypeKind::TIMESTAMP:
      return createMinMaxByAggregateForValueType<Timestamp, isMaxFunc>(
          resultType, compareType, errorMessage);
    case TypeKind::ARRAY:
      [[fallthrough]];
    case TypeKind::MAP:
      [[fallthrough]];
    case TypeKind::ROW:
      return createMinMaxByAggregateForValueType<ComplexType, isMaxFunc>(
          resultType, compareType, errorMessage);
    default:
      BOLT_FAIL(errorMessage);
  }
}

} // namespace bytedance::bolt::functions::aggregate::sparksql
