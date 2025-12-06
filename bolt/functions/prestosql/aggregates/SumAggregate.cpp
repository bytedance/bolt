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

#include "bolt/functions/lib/aggregates/SumAggregateBase.h"
#include "bolt/functions/prestosql/aggregates/AggregateNames.h"
using namespace bytedance::bolt::functions::aggregate;
namespace bytedance::bolt::aggregate::prestosql {

namespace {
template <typename TInput, typename TAccumulator, typename ResultType>
using SumAggregate = SumAggregateBase<TInput, TAccumulator, ResultType>;

template <template <typename U, typename V, typename W> class T>
exec::AggregateRegistrationResult registerSum(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("double")
          .argumentType("real")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("double")
          .argumentType("double")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .intermediateType("VARBINARY")
          .returnType("DECIMAL(38, a_scale)")
          .build(),
  };

  for (const auto& inputType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("bigint")
                             .intermediateType("bigint")
                             .argumentType(inputType)
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
        BOLT_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<T<int8_t, int64_t, int64_t>>(BIGINT());
          case TypeKind::SMALLINT:
            return std::make_unique<T<int16_t, int64_t, int64_t>>(BIGINT());
          case TypeKind::INTEGER:
            return std::make_unique<T<int32_t, int64_t, int64_t>>(BIGINT());
          case TypeKind::BIGINT: {
            if (inputType->isShortDecimal()) {
              return std::make_unique<DecimalSumAggregate<int64_t>>(resultType);
            }
            return std::make_unique<T<int64_t, int64_t, int64_t>>(BIGINT());
          }
          case TypeKind::HUGEINT: {
            if (inputType->isLongDecimal()) {
              return std::make_unique<DecimalSumAggregate<int128_t>>(
                  resultType);
            }
            BOLT_NYI();
          }
          case TypeKind::REAL:
            if (resultType->kind() == TypeKind::REAL) {
              return std::make_unique<T<float, double, float>>(resultType);
            }
            return std::make_unique<T<float, double, double>>(DOUBLE());
          case TypeKind::DOUBLE:
            if (resultType->kind() == TypeKind::REAL) {
              return std::make_unique<T<double, double, float>>(resultType);
            }
            return std::make_unique<T<double, double, double>>(DOUBLE());
          case TypeKind::VARBINARY:
            // Always use int128_t template for Varbinary as the result
            // type is either int128_t or
            // UnscaledLongDecimalWithOverflowState.
            return std::make_unique<DecimalSumAggregate<int128_t>>(resultType);

          default:
            BOLT_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      });
}
} // namespace

void registerSumAggregate(const std::string& prefix) {
  registerSum<SumAggregate>(prefix + kSum);
}
} // namespace bytedance::bolt::aggregate::prestosql
