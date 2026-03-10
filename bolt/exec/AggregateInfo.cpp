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

#include "bolt/exec/AggregateInfo.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/Operator.h"
#include "bolt/expression/Expr.h"
namespace bytedance::bolt::exec {

namespace {
std::vector<core::LambdaTypedExprPtr> extractLambdaInputs(
    const core::AggregationNode::Aggregate& aggregate) {
  std::vector<core::LambdaTypedExprPtr> lambdas;
  for (const auto& arg : aggregate.call->inputs()) {
    if (auto lambda =
            std::dynamic_pointer_cast<const core::LambdaTypedExpr>(arg)) {
      lambdas.push_back(lambda);
    }
  }

  return lambdas;
}
} // namespace

std::vector<AggregateInfo> toAggregateInfo(
    const core::AggregationNode& aggregationNode,
    const OperatorCtx& operatorCtx,
    uint32_t numKeys,
    std::shared_ptr<core::ExpressionEvaluator>& expressionEvaluator,
    bool isStreaming) {
  const auto numAggregates = aggregationNode.aggregates().size();
  std::vector<AggregateInfo> aggregates;
  aggregates.reserve(numAggregates);

  const auto& inputType = aggregationNode.sources()[0]->outputType();
  const auto& outputType = aggregationNode.outputType();
  const auto step = aggregationNode.step();

  auto esAggIntermediateTypeAsInputEnabled =
      operatorCtx.driverCtx()
          ->queryConfig()
          .esAggIntermediateTypeAsInputEnabled();

  for (auto i = 0; i < numAggregates; i++) {
    const auto& aggregate = aggregationNode.aggregates()[i];
    AggregateInfo info;
    // Populate input.
    auto& channels = info.inputs;
    auto& constants = info.constantInputs;
    for (const auto& arg : aggregate.call->inputs()) {
      if (auto field =
              dynamic_cast<const core::FieldAccessTypedExpr*>(arg.get())) {
        channels.push_back(inputType->getChildIdx(field->name()));
        constants.push_back(nullptr);
      } else if (
          auto constant =
              dynamic_cast<const core::ConstantTypedExpr*>(arg.get())) {
        channels.push_back(kConstantChannel);
        constants.push_back(constant->toConstantVector(operatorCtx.pool()));
      } else if (
          auto lambda = dynamic_cast<const core::LambdaTypedExpr*>(arg.get())) {
        for (const auto& name : lambda->signature()->names()) {
          if (auto captureIndex = inputType->getChildIdxIfExists(name)) {
            channels.push_back(captureIndex.value());
            constants.push_back(nullptr);
          }
        }
      } else {
        BOLT_FAIL(
            "Expression must be field access, constant, or lambda: {}",
            arg->toString());
      }
    }

    info.distinct = aggregate.distinct;
    // ES may use Intermediate type as rawInputType in the step kFinal.
    // So directly assign rawInputType to Intermediate Type.
    if (aggregationNode.isFinal() && esAggIntermediateTypeAsInputEnabled) {
      BOLT_CHECK_EQ(
          aggregate.rawInputTypes.size(),
          1,
          "Intermediate aggregates must have a single argument");
      info.intermediateType = aggregate.rawInputTypes[0];
    } else {
      info.intermediateType = Aggregate::intermediateType(
          aggregate.call->name(), aggregate.rawInputTypes);
    }

    // Setup aggregation mask: convert the Variable Reference name to the
    // channel (projection) index, if there is a mask.
    if (const auto& mask = aggregate.mask) {
      info.mask = inputType->asRow().getChildIdx(mask->name());
    } else {
      info.mask = std::nullopt;
    }

    auto index = numKeys + i;
    const auto& aggResultType = outputType->childAt(index);

    auto resultTypeStep = isPartialOutput(step)
        ? core::AggregationNode::Step::kPartial
        : core::AggregationNode::Step::kSingle;

    info.function = Aggregate::create(
        aggregate.call->name(),
        esAggIntermediateTypeAsInputEnabled ? step : resultTypeStep,
        aggregate.rawInputTypes,
        aggResultType,
        operatorCtx.driverCtx()->queryConfig());

    auto lambdas = extractLambdaInputs(aggregate);
    if (!lambdas.empty()) {
      if (expressionEvaluator == nullptr) {
        expressionEvaluator = std::make_shared<SimpleExpressionEvaluator>(
            operatorCtx.execCtx()->queryCtx(), operatorCtx.execCtx()->pool());
      }
      info.function->setLambdaExpressions(lambdas, expressionEvaluator);
    }

    // Sorting keys and orders.
    const auto numSortingKeys = aggregate.sortingKeys.size();
    BOLT_CHECK_EQ(numSortingKeys, aggregate.sortingOrders.size());
    info.sortingOrders = aggregate.sortingOrders;
    info.sortingKeys.reserve(numSortingKeys);
    for (const auto& key : aggregate.sortingKeys) {
      info.sortingKeys.push_back(exprToChannel(key.get(), inputType));
    }

    info.output = index;
    aggregates.emplace_back(std::move(info));
  }
  return aggregates;
}

std::vector<std::optional<column_index_t>> extractMaskChannels(
    const std::vector<AggregateInfo>& aggregates) {
  std::vector<std::optional<column_index_t>> masks;
  masks.reserve(aggregates.size());
  for (const auto& aggregate : aggregates) {
    masks.push_back(aggregate.mask);
  }
  return masks;
}

} // namespace bytedance::bolt::exec
