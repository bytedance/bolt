/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include "bolt/functions/sparksql/aggregates/PercentileApproxAggregate.h"
namespace bytedance::bolt::functions::aggregate::sparksql {

bool validPercentileType(const Type& type) {
  if (type.kind() == TypeKind::DOUBLE) {
    return true;
  }
  if (type.kind() != TypeKind::ARRAY) {
    return false;
  }
  return type.as<TypeKind::ARRAY>().elementType()->kind() == TypeKind::DOUBLE;
}

void addSignatures(
    const std::string& inputType,
    const std::string& percentileType,
    const std::string& returnType,
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  auto intermediateType = fmt::format(
      "row(array(double), boolean, integer, {0}, varbinary)", inputType);
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .argumentType("integer")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .argumentType("bigint")
                           .build());
}

void addDecimalSignatures(
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  auto intermediateType =
      "row(array(double), boolean, integer, DECIMAL(a_precision, a_scale), varbinary)";
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("DECIMAL(a_precision, a_scale)")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("double")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("DECIMAL(a_precision, a_scale)")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("double")
                           .argumentType("integer")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("DECIMAL(a_precision, a_scale)")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("double")
                           .argumentType("bigint")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("array(DECIMAL(a_precision, a_scale))")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("array(double)")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("array(DECIMAL(a_precision, a_scale))")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("array(double)")
                           .argumentType("integer")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("array(DECIMAL(a_precision, a_scale))")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("array(double)")
                           .argumentType("bigint")
                           .build());
}

void registerPercentileApproxAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& inputType :
       {"tinyint",
        "smallint",
        "integer",
        "bigint",
        "hugeint",
        "real",
        "double",
        "timestamp"}) {
    addSignatures(inputType, "double", inputType, signatures);
    addSignatures(
        inputType,
        "array(double)",
        fmt::format("array({})", inputType),
        signatures);
  }
  addDecimalSignatures(signatures);
  auto name = prefix + bolt::aggregate::kPercentileApprox;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        auto isRawInput = exec::isRawInput(step);
        bool hasAccuracy = argTypes.size() == 3 &&
            (argTypes[2]->kind() == TypeKind::BIGINT ||
             argTypes[2]->kind() == TypeKind::INTEGER);
        TypeKind accuracyType = TypeKind::INTEGER;
        if (isRawInput) {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              2 + hasAccuracy,
              "Wrong number of arguments passed to {}",
              name);
          if (hasAccuracy) {
            accuracyType = argTypes.back()->kind();
            BOLT_USER_CHECK(
                accuracyType == TypeKind::BIGINT ||
                    accuracyType == TypeKind::INTEGER,
                "The type of the accuracy argument of {} must be BIGINT ot INTEGER",
                name);
          }
          BOLT_USER_CHECK(
              validPercentileType(*argTypes[argTypes.size() - 1 - hasAccuracy]),
              "The type of the percentile argument of {} must be DOUBLE or ARRAY(DOUBLE)",
              name);
        } else {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              1,
              "The type of partial result for {} must be ROW",
              name);
          BOLT_USER_CHECK_EQ(
              argTypes[0]->kind(),
              TypeKind::ROW,
              "The type of partial result for {} must be ROW",
              name);
        }

        TypePtr type;
        if (!isRawInput && exec::isPartialOutput(step)) {
          type = argTypes[0]->asRow().childAt(kTypePalceHolder);
        } else if (isRawInput) {
          type = argTypes[0];
        } else if (resultType->isArray()) {
          type = resultType->as<TypeKind::ARRAY>().elementType();
        } else {
          type = resultType;
        }
        switch (type->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<PercentileApproxAggregate<
                TypeTraits<TypeKind::TINYINT>::NativeType>>(
                hasAccuracy, resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<PercentileApproxAggregate<
                TypeTraits<TypeKind::SMALLINT>::NativeType>>(
                hasAccuracy, resultType);
          case TypeKind::INTEGER:
            return std::make_unique<PercentileApproxAggregate<
                TypeTraits<TypeKind::INTEGER>::NativeType>>(
                hasAccuracy, resultType);
          case TypeKind::BIGINT:
            return std::make_unique<PercentileApproxAggregate<
                TypeTraits<TypeKind::BIGINT>::NativeType>>(
                hasAccuracy, resultType);
          case TypeKind::HUGEINT:
            return std::make_unique<PercentileApproxAggregate<
                TypeTraits<TypeKind::HUGEINT>::NativeType>>(
                hasAccuracy, resultType);
          case TypeKind::REAL:
            return std::make_unique<PercentileApproxAggregate<
                TypeTraits<TypeKind::REAL>::NativeType>>(
                hasAccuracy, resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<PercentileApproxAggregate<
                TypeTraits<TypeKind::DOUBLE>::NativeType>>(
                hasAccuracy, resultType);
          case TypeKind::TIMESTAMP:
            return std::make_unique<PercentileApproxAggregate<
                TypeTraits<TypeKind::TIMESTAMP>::NativeType>>(
                hasAccuracy, resultType);
          default:
            BOLT_USER_FAIL(
                "Unsupported input type for {} aggregation {}, isRawInput: {}, setp: {}, reslut type: {}",
                name,
                type->toString(),
                isRawInput,
                core::AggregationNode::stepName(step),
                resultType->toString());
        }
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace bytedance::bolt::functions::aggregate::sparksql