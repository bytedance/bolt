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

#pragma once

#include "bolt/core/PlanNode.h"
#include "bolt/vector/ComplexVector.h"
namespace bytedance::bolt::exec {

class Aggregate;

/// Information needed to evaluate an aggregate function.
struct AggregateInfo {
  /// Instance of the Aggregate class.
  std::unique_ptr<Aggregate> function;

  /// Indices of the input columns in the input RowVector.
  std::vector<column_index_t> inputs;

  /// Optional constant inputs. The size of this vector matches the size of
  /// 'inputs'. Non-constant inputs have null entries.
  std::vector<VectorPtr> constantInputs;

  /// Optional index of an input boolean column that should be used as a mask.
  std::optional<column_index_t> mask;

  /// Optional list of input columns that should be used to sort input rows
  /// before aggregating. These column may or may not overlap with 'inputs'.
  std::vector<column_index_t> sortingKeys;

  /// Optional list of sorting orders that goes with 'sortingKeys'.
  std::vector<core::SortOrder> sortingOrders;

  /// Boolean indicating whether inputs must be de-duplicated before
  /// aggregating.
  bool distinct{false};

  /// Index of the result column in the output RowVector.
  column_index_t output;

  /// Type of intermediate results. Used for spilling.
  TypePtr intermediateType;
};

class OperatorCtx;

/// Translate an AggregationNode to a list of AggregationInfo, which could be
/// a hash aggregation plan node or a streaming aggregation plan node.
///
/// @param aggregationNode Plan node of this aggregation.
/// @param operatorCtx Operator context.
/// @param numKeys Number of grouping keys.
/// @param expressionEvaluator An Expression evaluator. It is used by an
/// aggregate operator to compile and eval lambda expression. It should be
/// initiated/assigned for at most one time.
/// @param isStreaming Indicate whether this aggregation is streaming or not.
/// Pass true if the aggregate operator is a StreamingAggregation and false if
/// the aggregate operator is a HashAggregation. This parameter will be
/// removed after sorted, distinct aggregation, and lambda functions support
/// are added to StreamingAggregation.
/// @return List of AggregationInfo.
std::vector<AggregateInfo> toAggregateInfo(
    const core::AggregationNode& aggregationNode,
    const OperatorCtx& operatorCtx,
    uint32_t numKeys,
    std::shared_ptr<core::ExpressionEvaluator>& expressionEvaluator,
    bool isStreaming = false);

/// Extract index of the 'mask' column for each aggregation from aggregations.
/// Aggregations without masks use std::nullopt.
std::vector<std::optional<column_index_t>> extractMaskChannels(
    const std::vector<AggregateInfo>& aggregates);

} // namespace bytedance::bolt::exec
