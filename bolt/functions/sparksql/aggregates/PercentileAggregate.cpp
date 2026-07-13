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

#include "bolt/functions/sparksql/aggregates/PercentileAggregate.h"

namespace bytedance::bolt::functions::aggregate::sparksql {

namespace {

bool validPercentileType(const Type& type) {
  if (type.kind() == TypeKind::DOUBLE) {
    return true;
  }
  if (type.kind() != TypeKind::ARRAY) {
    return false;
  }
  return type.as<TypeKind::ARRAY>().elementType()->kind() == TypeKind::DOUBLE;
}

void addDecimalSignatures(
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  auto intermediateType =
      "row(array(double), boolean, array(DECIMAL(a_precision, a_scale)), array(bigint))";
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("double")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("double")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("array(double)")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("array(double)")
                           .build());
  for (const auto& weightType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("double")
                             .intermediateType(intermediateType)
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType("double")
                             .argumentType(weightType)
                             .build());
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("array(double)")
                             .intermediateType(intermediateType)
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType("array(double)")
                             .argumentType(weightType)
                             .build());
  }
}

void addSignatures(
    const std::string& inputType,
    const std::string& percentileType,
    const std::string& returnType,
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  auto intermediateType = fmt::format(
      "row(array(double), boolean, array({0}), array(bigint))", inputType);
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .build());
  for (const auto& weightType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(returnType)
                             .intermediateType(intermediateType)
                             .argumentType(inputType)
                             .argumentType(percentileType)
                             .argumentType(weightType)
                             .build());
  }
}

exec::AggregateRegistrationResult registerPercentile(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& inputType :
       {"tinyint",
        "smallint",
        "integer",
        "bigint",
        "hugeint",
        "real",
        "double",
        "varchar"}) {
    addSignatures(inputType, "double", "double", signatures);
    addSignatures(inputType, "array(double)", "array(double)", signatures);
  }
  addDecimalSignatures(signatures);
  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        auto isRawInput = exec::isRawInput(step);
        TypeKind weightType = TypeKind::BIGINT;
        auto hasWeight = argTypes.size() >= 3;

        if (isRawInput) {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              2 + hasWeight,
              "Wrong number of arguments passed to {}",
              name);
          if (hasWeight) {
            if (argTypes[2]->kind() != TypeKind::BIGINT &&
                argTypes[2]->kind() != TypeKind::INTEGER &&
                argTypes[2]->kind() != TypeKind::SMALLINT &&
                argTypes[2]->kind() != TypeKind::TINYINT) {
              BOLT_USER_FAIL(
                  "The type of the weight argument of {} must be integer type",
                  name);
            }
            weightType = argTypes[2]->kind();
          }
          BOLT_USER_CHECK(
              validPercentileType(*argTypes[1]),
              "The type of the percentile argument of {} must be DOUBLE or ARRAY(DOUBLE)",
              name);
        } else {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              1,
              "The type of partial result for {} must be ROW",
              name);
        }

        TypePtr type;
        if (isRawInput) {
          type = argTypes[0];
        } else {
          type = argTypes[0]->asRow().childAt(detail::kItems);
          type = type->as<TypeKind::ARRAY>().elementType();
        }

        int scale = 0;
        if (type->isLongDecimal() || type->isShortDecimal()) {
          scale = getDecimalPrecisionScale(*type).second;
        }
        auto powerOfScale = DecimalUtil::kPowersOfTen[scale];

        switch (type->kind()) {
          case TypeKind::TINYINT:
            return detail::PercentileAggregateFactory<TypeKind::TINYINT>::
                create(weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::SMALLINT:
            return detail::PercentileAggregateFactory<TypeKind::SMALLINT>::
                create(weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::INTEGER:
            return detail::PercentileAggregateFactory<TypeKind::INTEGER>::
                create(weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::BIGINT:
            return detail::PercentileAggregateFactory<TypeKind::BIGINT>::create(
                weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::HUGEINT:
            return detail::PercentileAggregateFactory<TypeKind::HUGEINT>::
                create(weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::REAL:
            return detail::PercentileAggregateFactory<TypeKind::REAL>::create(
                weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::DOUBLE:
            return detail::PercentileAggregateFactory<TypeKind::DOUBLE>::create(
                weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::VARCHAR:
            return detail::PercentileAggregateFactory<TypeKind::VARCHAR>::
                create(weightType, hasWeight, resultType, powerOfScale);
          default:
            BOLT_USER_FAIL(
                "Unsupported input type for percentile aggregation {}",
                type->toString());
        }
      },
      /*registerCompanionFunctions*/ true);
}

} // namespace

void registerPercentileAggregate(const std::string& prefix) {
  registerPercentile(prefix + "percentile");
}

} // namespace bytedance::bolt::functions::aggregate::sparksql
