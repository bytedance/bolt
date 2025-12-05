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

#include "bolt/functions/prestosql/aggregates/ApproxPercentileAggregate.h"
namespace bytedance::bolt::aggregate::prestosql {

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
      "row(array(double), boolean, double, integer, bigint, {0}, {0}, array({0}), array(integer))",
      inputType);
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
                           .argumentType("bigint")
                           .argumentType(percentileType)
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .argumentType("double")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType("bigint")
                           .argumentType(percentileType)
                           .argumentType("double")
                           .build());
}

void registerApproxPercentileAggregate(
    const std::string& name,
    bool withCompanionFunctions,
    const std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  exec::registerAggregateFunction(
      name,
      signatures,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        auto isRawInput = exec::isRawInput(step);
        auto hasWeight =
            argTypes.size() >= 2 && argTypes[1]->kind() == TypeKind::BIGINT;
        bool hasAccuracy = argTypes.size() == (hasWeight ? 4 : 3);

        if (isRawInput) {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              2 + hasWeight + hasAccuracy,
              "Wrong number of arguments passed to {}",
              name);
          if (hasWeight) {
            BOLT_USER_CHECK_EQ(
                argTypes[1]->kind(),
                TypeKind::BIGINT,
                "The type of the weight argument of {} must be BIGINT",
                name);
          }
          if (hasAccuracy) {
            BOLT_USER_CHECK_EQ(
                argTypes.back()->kind(),
                TypeKind::DOUBLE,
                "The type of the accuracy argument of {} must be DOUBLE",
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
          type = argTypes[0]->asRow().childAt(kMinValue);
        } else if (isRawInput) {
          type = argTypes[0];
        } else if (resultType->isArray()) {
          type = resultType->as<TypeKind::ARRAY>().elementType();
        } else {
          type = resultType;
        }

        switch (type->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<ApproxPercentileAggregate<int8_t>>(
                hasWeight, hasAccuracy, resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<ApproxPercentileAggregate<int16_t>>(
                hasWeight, hasAccuracy, resultType);
          case TypeKind::INTEGER:
            return std::make_unique<ApproxPercentileAggregate<int32_t>>(
                hasWeight, hasAccuracy, resultType);
          case TypeKind::BIGINT:
            return std::make_unique<ApproxPercentileAggregate<int64_t>>(
                hasWeight, hasAccuracy, resultType);
          case TypeKind::REAL:
            return std::make_unique<ApproxPercentileAggregate<float>>(
                hasWeight, hasAccuracy, resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<ApproxPercentileAggregate<double>>(
                hasWeight, hasAccuracy, resultType);
          default:
            BOLT_USER_FAIL(
                "Unsupported input type for {} aggregation {}",
                name,
                type->toString());
        }
      },
      withCompanionFunctions);
}

void registerApproxPercentileAggregate(
    const std::string& prefix,
    bool withCompanionFunctions) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& inputType :
       {"tinyint", "smallint", "integer", "bigint", "real", "double"}) {
    addSignatures(inputType, "double", inputType, signatures);
    addSignatures(
        inputType,
        "array(double)",
        fmt::format("array({})", inputType),
        signatures);
  }
  // register function named 'approx_percentile'
  registerApproxPercentileAggregate(
      prefix + kApproxPercentile, withCompanionFunctions, signatures);
}
} // namespace bytedance::bolt::aggregate::prestosql
