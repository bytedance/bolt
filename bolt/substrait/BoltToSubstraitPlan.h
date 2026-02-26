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

#include <google/protobuf/arena.h>
#include <string>
#include <typeinfo>

#include "bolt/core/PlanNode.h"
#include "bolt/type/Type.h"

#include "bolt/substrait/BoltToSubstraitExpr.h"
#include "bolt/substrait/SubstraitExtensionCollector.h"
#include "bolt/substrait/proto/substrait/algebra.pb.h"
#include "bolt/substrait/proto/substrait/bolt/torch.pb.h"
#include "bolt/substrait/proto/substrait/plan.pb.h"
#if defined BOLT_HAS_TORCH && BOLT_HAS_TORCH == 1
#include "bolt/torch/PlanNode.h"
#endif
namespace bytedance::bolt::substrait {

/// Convert the Bolt plan into Substrait plan.
class BoltToSubstraitPlanConvertor {
 public:
  /// Convert Bolt PlanNode into Substrait Plan.
  /// @param vPlan Bolt query plan to convert.
  /// @param arena Arena to use for allocating Substrait plan objects.
  /// @return A pointer to Substrait plan object allocated on the arena and
  /// representing the input Bolt plan.
  ::substrait::Plan& toSubstrait(
      google::protobuf::Arena& arena,
      const core::PlanNodePtr& planNode);

 private:
  /// Convert Bolt PlanNode into Substrait Rel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const core::PlanNodePtr& planNode,
      ::substrait::Rel* rel);

  /// Convert Bolt FilterNode into Substrait FilterRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::FilterNode>& filterNode,
      ::substrait::FilterRel* filterRel);

  /// Convert Bolt ValuesNode into Substrait ReadRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::ValuesNode>& valuesNode,
      ::substrait::ReadRel* readRel);

  /// Convert Bolt ProjectNode into Substrait ProjectRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::ProjectNode>& projectNode,
      ::substrait::ProjectRel* projectRel);

  /// Convert Bolt Aggregation Node into Substrait AggregateRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::AggregationNode>& aggregateNode,
      ::substrait::AggregateRel* aggregateRel);

  /// Convert Bolt OrderBy Node into Substrait SortRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::OrderByNode>& orderByNode,
      ::substrait::SortRel* sortRel);

  /// Convert Bolt TopN Node into Substrait SortRel->FetchRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::TopNNode>& topNNode,
      ::substrait::FetchRel* fetchRel);

#if defined BOLT_HAS_TORCH && BOLT_HAS_TORCH == 1
  /// Convert Bolt TorchNode into Substrait Rel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const ::bytedance::bolt::torch::TorchNode>&
          torchNode,
      ::substrait::ExtensionSingleRel* extensionRel);
#endif

  /// Helper function to process sortingKeys and sortingOrders in Bolt to
  /// convert them to the sortField of SortRel in Substrait.
  const ::substrait::SortRel& processSortFields(
      google::protobuf::Arena& arena,
      const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
      const std::vector<core::SortOrder>& sortingOrders,
      const RowTypePtr& inputType);

  /// Convert Bolt Limit Node into Substrait FetchRel.
  void toSubstrait(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::LimitNode>& limitNode,
      ::substrait::FetchRel* fetchRel);

  /// Check there only have one source for the bolt node and return it.
  const core::PlanNodePtr& getSingleSource(const core::PlanNodePtr& node);

  /// The Expression converter used to convert Bolt representations into
  /// Substrait expressions.
  BoltToSubstraitExprConvertorPtr exprConvertor_;

  /// The Type converter used to convert bolt representation into Substrait
  /// type.
  std::shared_ptr<BoltToSubstraitTypeConvertor> typeConvertor_;

  /// The Extension collector storing the relations between the function
  /// signature and the function reference number.
  SubstraitExtensionCollectorPtr extensionCollector_;
};

} // namespace bytedance::bolt::substrait
