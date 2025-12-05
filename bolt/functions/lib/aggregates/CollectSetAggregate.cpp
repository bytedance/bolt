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

#include "bolt/functions/lib/aggregates/CollectSetAggregate.h"
#include "bolt/functions/lib/aggregates/SetBaseAggregate.h"
namespace bytedance::bolt::functions::aggregate {
namespace {

// Null inputs are excluded by setting 'ignoreNulls' as true.
// Empty arrays are returned for empty groups by setting 'nullForEmpty'
// as false.
template <typename T>
using SparkSetAggAggregate = SetAggAggregate<T, true, false>;

} // namespace

void registerCollectSetAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("array(T)")
          .intermediateType("array(T)")
          .argumentType("T")
          .build()};
  auto name = prefix + "collect_set";
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_EQ(argTypes.size(), 1);

        const bool isRawInput = exec::isRawInput(step);
        const TypePtr& inputType =
            isRawInput ? argTypes[0] : argTypes[0]->childAt(0);
        const TypeKind typeKind = inputType->kind();

        switch (typeKind) {
          case TypeKind::BOOLEAN:
            return std::make_unique<SparkSetAggAggregate<bool>>(resultType);
          case TypeKind::TINYINT:
            return std::make_unique<SparkSetAggAggregate<int8_t>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<SparkSetAggAggregate<int16_t>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<SparkSetAggAggregate<int32_t>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<SparkSetAggAggregate<int64_t>>(resultType);
          case TypeKind::HUGEINT:
            BOLT_CHECK(
                inputType->isLongDecimal(),
                "Non-decimal use of HUGEINT is not supported");
            return std::make_unique<SparkSetAggAggregate<int128_t>>(resultType);
          case TypeKind::REAL:
            return std::make_unique<SparkSetAggAggregate<float>>(resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<SparkSetAggAggregate<double>>(resultType);
          case TypeKind::TIMESTAMP:
            return std::make_unique<SparkSetAggAggregate<Timestamp>>(
                resultType);
          case TypeKind::VARBINARY:
            [[fallthrough]];
          case TypeKind::VARCHAR:
            return std::make_unique<SparkSetAggAggregate<StringView>>(
                resultType);
          case TypeKind::ARRAY:
            [[fallthrough]];
          case TypeKind::ROW:
            // Nested nulls are allowed by setting 'throwOnNestedNulls' as
            // false.
            return std::make_unique<SparkSetAggAggregate<ComplexType>>(
                resultType, false);
          default:
            BOLT_UNSUPPORTED(
                "Unsupported type {}", mapTypeKindToName(typeKind));
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace bytedance::bolt::functions::aggregate
