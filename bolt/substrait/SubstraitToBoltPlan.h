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

#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/core/PlanNode.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/exec/Split.h"
#include "bolt/substrait/SubstraitToBoltExpr.h"
namespace bytedance::bolt::substrait {

/// This class is used to convert the Substrait plan into Bolt plan.
class SubstraitBoltPlanConverter {
 public:
  /// Set appendProject to false for BoltML replacement projections and its
  /// legacy join layout hints. Standard Substrait keeps the default.
  explicit SubstraitBoltPlanConverter(
      memory::MemoryPool* pool,
      bool appendProject = true)
      : pool_(pool), appendProject_(appendProject) {}

  /// Convert Substrait AggregateRel into Bolt PlanNode.
  core::PlanNodePtr toBoltPlan(const ::substrait::AggregateRel& aggRel);

  /// Convert Substrait ProjectRel into Bolt PlanNode.
  core::PlanNodePtr toBoltPlan(const ::substrait::ProjectRel& projectRel);

  /// Convert Substrait FilterRel into Bolt PlanNode.
  core::PlanNodePtr toBoltPlan(const ::substrait::FilterRel& filterRel);

  /// Convert Substrait ReadRel into Bolt PlanNode and collect exec::Splits
  /// for TableScan if any. The produced splits preserve grouping semantics
  /// (e.g., PaimonConnectorSplit wrapping multiple Hive splits).
  core::PlanNodePtr toBoltPlan(
      const ::substrait::ReadRel& readRel,
      std::vector<exec::Split>& outSplits);

  /// Convert Substrait FetchRel into Bolt LimitNode or TopNNode according the
  /// different input of fetchRel.
  core::PlanNodePtr toBoltPlan(const ::substrait::FetchRel& fetchRel);

  /// Convert Substrait ReadRel into Bolt Values Node.
  core::PlanNodePtr toBoltPlan(
      const ::substrait::ReadRel& readRel,
      const RowTypePtr& type);

  /// Convert Substrait Rel into Bolt PlanNode.
  core::PlanNodePtr toBoltPlan(const ::substrait::Rel& rel);

  /// Convert Substrait RelRoot into Bolt PlanNode.
  core::PlanNodePtr toBoltPlan(const ::substrait::RelRoot& root);

  /// Convert Substrait SortRel into Bolt OrderByNode.
  core::PlanNodePtr toBoltPlan(const ::substrait::SortRel& sortRel);

  /// Convert Substrait Plan into Bolt PlanNode.
  core::PlanNodePtr toBoltPlan(const ::substrait::Plan& substraitPlan);

  /// Convert Substrait ExtensionSingleRel into Bolt matching PlanNode.
  /// The choice of the matching plan node depends on the protobuf definition
  /// in the detail field.
  core::PlanNodePtr toBoltPlan(const ::substrait::ExtensionSingleRel& rel);

  /// Convert Substrait HashJoinRel into Bolt HashJoinNode.
  core::PlanNodePtr toBoltPlan(const ::substrait::HashJoinRel& joinRel);

  /// Check the Substrait type extension only has one unknown extension.
  static bool checkTypeExtension(const ::substrait::Plan& substraitPlan);

  /// Construct the function map between the index and the Substrait function
  /// name.
  void constructFunctionMap(const ::substrait::Plan& substraitPlan);

  /// Return the function map used by this plan converter.
  const std::unordered_map<uint64_t, std::string>& getFunctionMap() const {
    return functionMap_;
  }

  /// Return the splits map used by this plan converter.
  const std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>&
  splitInfos() const {
    return splitInfoMap_;
  }

  /// Looks up a function by ID and returns function name if found. Throws if
  /// function with specified ID doesn't exist. Returns a compound
  /// function specification consisting of the function name and the input
  /// types. The format is as follows: <function
  /// name>:<arg_type0>_<arg_type1>_..._<arg_typeN>
  const std::string& findFunction(uint64_t id) const;

  /// Integrate Substrait emit feature. Here a given 'substrait::RelCommon'
  /// is passed and check if emit is defined for this relation. Basically a
  /// ProjectNode is added on top of 'noEmitNode' to represent output order
  /// specified in 'relCommon::emit'. Return 'noEmitNode' as is
  /// if output order is 'kDriect'.
  core::PlanNodePtr processEmit(
      const ::substrait::RelCommon& relCommon,
      const core::PlanNodePtr& noEmitNode);

 private:
  /// Returns unique ID to use for plan node. Produces sequential numbers
  /// starting from zero.
  std::string nextPlanNodeId();

  /// Used to convert Substrait Filter into Bolt SubfieldFilters which will
  /// be used in TableScan.
  connector::hive::SubfieldFilters toBoltFilter(
      const std::vector<std::string>& inputNameList,
      const std::vector<TypePtr>& inputTypeList,
      const ::substrait::Expression& substraitFilter);

  /// Multiple conditions are connected to a binary tree structure with
  /// the relation key words, including AND, OR, and etc. Currently, only
  /// AND is supported. This function is used to extract all the Substrait
  /// conditions in the binary tree structure into a vector.
  void flattenConditions(
      const ::substrait::Expression& substraitFilter,
      std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions);

  /// The Substrait parser used to convert Substrait representations into
  /// recognizable representations.
  std::shared_ptr<SubstraitParser> substraitParser_{
      std::make_shared<SubstraitParser>()};

  /// Helper Function to convert Substrait sortField to Bolt sortingKeys and
  /// sortingOrders.
  std::pair<
      std::vector<core::FieldAccessTypedExprPtr>,
      std::vector<core::SortOrder>>
  processSortField(
      const ::google::protobuf::RepeatedPtrField<::substrait::SortField>&
          sortField,
      const RowTypePtr& inputType);

  /// The Expression converter used to convert Substrait representations into
  /// Bolt expressions.
  std::shared_ptr<SubstraitBoltExprConverter> exprConverter_;

  /// The unique identification for each PlanNode.
  int planNodeId_ = 0;

  /// The map storing the relations between the function id and the function
  /// name. Will be constructed based on the Substrait representation.
  std::unordered_map<uint64_t, std::string> functionMap_;

  /// Mapping from leaf plan node ID to exec::Splits.
  std::unordered_map<core::PlanNodeId, std::vector<exec::Split>> splitInfoMap_;

  /// Memory pool.
  memory::MemoryPool* pool_;

  // Whether project nodes should append new columns to existing ones or just
  // replace all columns.
  bool appendProject_;

  /// Helper function to convert the input of Substrait Rel to Bolt Node.
  template <typename T>
  core::PlanNodePtr convertSingleInput(T rel) {
    BOLT_CHECK(
        rel.has_input(),
        "Child Rel is expected here. rel is " + rel.DebugString());
    return toBoltPlan(rel.input());
  }
};

} // namespace bytedance::bolt::substrait
