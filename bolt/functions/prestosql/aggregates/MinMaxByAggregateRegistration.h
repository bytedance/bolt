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

#include <sstream>

#include "bolt/functions/prestosql/aggregates/MinMaxByAggregates.h"

namespace bytedance::bolt::aggregate::prestosql::detail {

inline std::string toString(const std::vector<TypePtr>& types) {
  std::ostringstream out;
  for (auto i = 0; i < types.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << types[i]->toString();
  }
  return out.str();
}

template <bool IsMaxFunc, typename V>
struct MinMaxByAggregateFactory {
  static std::unique_ptr<exec::Aggregate> create(
      TypePtr resultType,
      TypePtr compareType,
      const std::string& errorMessage);
};

extern template struct MinMaxByAggregateFactory<true, bool>;
extern template struct MinMaxByAggregateFactory<true, int8_t>;
extern template struct MinMaxByAggregateFactory<true, int16_t>;
extern template struct MinMaxByAggregateFactory<true, int32_t>;
extern template struct MinMaxByAggregateFactory<true, int64_t>;
extern template struct MinMaxByAggregateFactory<true, int128_t>;
extern template struct MinMaxByAggregateFactory<true, float>;
extern template struct MinMaxByAggregateFactory<true, double>;
extern template struct MinMaxByAggregateFactory<true, StringView>;
extern template struct MinMaxByAggregateFactory<true, Timestamp>;
extern template struct MinMaxByAggregateFactory<true, ComplexType>;
extern template struct MinMaxByAggregateFactory<false, bool>;
extern template struct MinMaxByAggregateFactory<false, int8_t>;
extern template struct MinMaxByAggregateFactory<false, int16_t>;
extern template struct MinMaxByAggregateFactory<false, int32_t>;
extern template struct MinMaxByAggregateFactory<false, int64_t>;
extern template struct MinMaxByAggregateFactory<false, int128_t>;
extern template struct MinMaxByAggregateFactory<false, float>;
extern template struct MinMaxByAggregateFactory<false, double>;
extern template struct MinMaxByAggregateFactory<false, StringView>;
extern template struct MinMaxByAggregateFactory<false, Timestamp>;
extern template struct MinMaxByAggregateFactory<false, ComplexType>;

template <bool IsMaxFunc, typename V>
std::unique_ptr<exec::Aggregate> createMinMaxByAggregateForValueType(
    TypePtr resultType,
    TypePtr compareType,
    const std::string& errorMessage) {
  return MinMaxByAggregateFactory<IsMaxFunc, V>::create(
      resultType, compareType, errorMessage);
}

template <bool IsMaxFunc>
std::unique_ptr<exec::Aggregate> createMinMaxByAggregate(
    TypePtr resultType,
    TypePtr valueType,
    TypePtr compareType,
    const std::string& errorMessage) {
  switch (valueType->kind()) {
    case TypeKind::BOOLEAN:
      return createMinMaxByAggregateForValueType<IsMaxFunc, bool>(
          resultType, compareType, errorMessage);
    case TypeKind::TINYINT:
      return createMinMaxByAggregateForValueType<IsMaxFunc, int8_t>(
          resultType, compareType, errorMessage);
    case TypeKind::SMALLINT:
      return createMinMaxByAggregateForValueType<IsMaxFunc, int16_t>(
          resultType, compareType, errorMessage);
    case TypeKind::INTEGER:
      return createMinMaxByAggregateForValueType<IsMaxFunc, int32_t>(
          resultType, compareType, errorMessage);
    case TypeKind::BIGINT:
      return createMinMaxByAggregateForValueType<IsMaxFunc, int64_t>(
          resultType, compareType, errorMessage);
    case TypeKind::HUGEINT:
      return createMinMaxByAggregateForValueType<IsMaxFunc, int128_t>(
          resultType, compareType, errorMessage);
    case TypeKind::REAL:
      return createMinMaxByAggregateForValueType<IsMaxFunc, float>(
          resultType, compareType, errorMessage);
    case TypeKind::DOUBLE:
      return createMinMaxByAggregateForValueType<IsMaxFunc, double>(
          resultType, compareType, errorMessage);
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY:
      return createMinMaxByAggregateForValueType<IsMaxFunc, StringView>(
          resultType, compareType, errorMessage);
    case TypeKind::TIMESTAMP:
      return createMinMaxByAggregateForValueType<IsMaxFunc, Timestamp>(
          resultType, compareType, errorMessage);
    case TypeKind::ARRAY:
      [[fallthrough]];
    case TypeKind::MAP:
      [[fallthrough]];
    case TypeKind::ROW:
      return createMinMaxByAggregateForValueType<IsMaxFunc, ComplexType>(
          resultType, compareType, errorMessage);
    default:
      BOLT_FAIL(errorMessage);
  }
}

template <
    template <
        typename U,
        typename V,
        bool B1,
        template <bool B2, typename C1, typename C2>
        class C>
    class Aggregate,
    bool IsMaxFunc,
    template <typename U, typename V>
    class NAggregate>
exec::AggregateRegistrationResult registerMinMaxBy(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .typeVariable("V")
                           .orderableTypeVariable("C")
                           .returnType("V")
                           .intermediateType("row(V,C)")
                           .argumentType("V")
                           .argumentType("C")
                           .build());
  static constexpr std::array<const char*, 10> kSupportedCompareTypes = {
      "boolean",
      "tinyint",
      "smallint",
      "integer",
      "bigint",
      "real",
      "double",
      "varchar",
      "date",
      "timestamp"};

  for (const auto& compareType : kSupportedCompareTypes) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .typeVariable("V")
                             .returnType("array(V)")
                             .intermediateType(fmt::format(
                                 "row(bigint,array({}),array(V))", compareType))
                             .argumentType("V")
                             .argumentType(compareType)
                             .argumentType("bigint")
                             .build());
  }

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        const std::string errorMessage = fmt::format(
            "Unknown input types for {} ({}) aggregation: {}",
            name,
            mapAggregationStepToName(step),
            toString(argTypes));

        const bool nAgg = (argTypes.size() == 3) ||
            (argTypes.size() == 1 && argTypes[0]->size() == 3);

        if (nAgg) {
          return createNArg<NAggregate>(
              resultType, argTypes[0], argTypes[1], errorMessage);
        }
        return createMinMaxByAggregate<IsMaxFunc>(
            resultType, argTypes[0], argTypes[1], errorMessage);
      });
}

} // namespace bytedance::bolt::aggregate::prestosql::detail
