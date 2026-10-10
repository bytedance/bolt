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

#include <functional>
#include <limits>
#include <unordered_map>
#include <unordered_set>

#include "bolt/connectors/arrow/ArrowMemoryConnector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/HiveDataSink.h"
#include "bolt/connectors/hive/PaimonConnectorSplit.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/connectors/tpch/TpchConnector.h"
#include "bolt/connectors/tpch/TpchConnectorSplit.h"
#include "bolt/dwio/catalog/fbhive/FileUtils.h"
#include "bolt/exec/TableWriter.h"
#include "bolt/substrait/BoltExtensions.h"
#include "bolt/substrait/BoltToSubstraitType.h"
#include "bolt/substrait/SubstraitToBoltPlan.h"
#include "bolt/substrait/TypeUtils.h"
#include "bolt/substrait/VariantToVectorConverter.h"
#include "bolt/substrait/proto/substrait/algebra.pb.h"
#include "bolt/substrait/proto/substrait/bolt/extensions.pb.h"
#include "bolt/substrait/proto/substrait/bolt/torch.pb.h"
#if defined BOLT_HAS_TORCH && BOLT_HAS_TORCH == 1
#include "bolt/torch/PlanNode.h"
#endif
#include "bolt/substrait/proto/substrait/extensions/extensions.pb.h"
#include "bolt/type/Type.h"

#if defined BOLT_HAS_PYTHON && BOLT_HAS_PYTHON == 1
#include "bolt/substrait/PythonRel.h"
#endif

namespace bytedance::bolt::substrait {
namespace {
std::unordered_map<std::string, std::optional<std::string>>
extractPartitionKeysFromPath(const std::string& path) {
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  std::vector<std::string> pathSegments;
  folly::split('/', path, pathSegments);
  for (const auto& segment : pathSegments) {
    std::vector<std::string> kv;
    folly::split('=', segment, kv);
    if (kv.size() == 2 && !kv[0].empty() && !kv[1].empty()) {
      partitionKeys[dwio::catalog::fbhive::FileUtils::unescapePathName(kv[0])] =
          dwio::catalog::fbhive::FileUtils::unescapePathName(kv[1]);
    }
  }
  return partitionKeys;
}

core::AggregationNode::Step toAggregationStep(
    const ::substrait::AggregateRel& sAgg) {
  if (sAgg.measures().size() == 0) {
    // When only groupings exist, set the phase to be Single.
    return core::AggregationNode::Step::kSingle;
  }

  // Use the first measure to set aggregation phase.
  const auto& firstMeasure = sAgg.measures()[0];
  const auto& aggFunction = firstMeasure.measure();
  switch (aggFunction.phase()) {
    case ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE:
      return core::AggregationNode::Step::kPartial;
    case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE:
      return core::AggregationNode::Step::kIntermediate;
    case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT:
      return core::AggregationNode::Step::kFinal;
    case ::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT:
      return core::AggregationNode::Step::kSingle;
    default:
      BOLT_FAIL("Aggregate phase is not supported.");
  }
}

core::SortOrder toSortOrder(const ::substrait::SortField& sortField) {
  switch (sortField.direction()) {
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
      return core::kAscNullsFirst;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
      return core::kAscNullsLast;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
      return core::kDescNullsFirst;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
      return core::kDescNullsLast;
    default:
      BOLT_FAIL("Sort direction is not supported.");
  }
}

/// Holds the information required to create
/// a project node to simulate the emit
/// behavior in Substrait.
struct EmitInfo {
  std::vector<core::TypedExprPtr> expressions;
  std::vector<std::string> projectNames;
};

/// Helper function to extract the attributes required to create a ProjectNode
/// used for interpreting Substrait Emit.
EmitInfo getEmitInfo(
    const ::substrait::RelCommon& relCommon,
    const core::PlanNodePtr& node) {
  const auto& emit = relCommon.emit();
  int emitSize = emit.output_mapping_size();
  EmitInfo emitInfo;
  emitInfo.projectNames.resize(emitSize);
  emitInfo.expressions.resize(emitSize);
  const auto& outputType = node->outputType();
  for (int i = 0; i < emitSize; i++) {
    int32_t mapId = emit.output_mapping(i);
    BOLT_CHECK_GE(mapId, 0);
    BOLT_CHECK_LT(mapId, outputType->size());
    emitInfo.projectNames[i] = outputType->nameOf(mapId);
    emitInfo.expressions[i] = std::make_shared<core::FieldAccessTypedExpr>(
        outputType->childAt(mapId), outputType->nameOf(mapId));
  }
  return emitInfo;
}

} // namespace

core::PlanNodePtr SubstraitBoltPlanConverter::processEmit(
    const ::substrait::RelCommon& relCommon,
    const core::PlanNodePtr& noEmitNode) {
  core::PlanNodePtr node = noEmitNode;
  if (relCommon.has_emit()) {
    auto emitInfo = getEmitInfo(relCommon, node);
    node = std::make_shared<core::ProjectNode>(
        nextPlanNodeId(),
        std::move(emitInfo.projectNames),
        std::move(emitInfo.expressions),
        node);
  }
  const auto& names = relCommon.hint().output_names();
  if (names.empty()) {
    return node;
  }
  const auto& outputType = node->outputType();
  BOLT_CHECK_EQ(
      names.size(),
      outputType->size(),
      "Output names must match the relation schema");
  std::vector<std::string> aliases(names.begin(), names.end());
  if (aliases == outputType->names()) {
    return node;
  }
  std::vector<core::TypedExprPtr> fields;
  fields.reserve(outputType->size());
  for (size_t i = 0; i < outputType->size(); ++i) {
    fields.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(
        outputType->childAt(i), outputType->nameOf(i)));
  }
  return std::make_shared<core::ProjectNode>(
      nextPlanNodeId(), std::move(aliases), std::move(fields), node);
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::AggregateRel& aggRel) {
  auto childNode = convertSingleInput<::substrait::AggregateRel>(aggRel);
  core::AggregationNode::Step aggStep = toAggregationStep(aggRel);
  const auto& inputType = childNode->outputType();
  std::vector<core::FieldAccessTypedExprPtr> boltGroupingExprs;

  // Get the grouping expressions.
  auto toFieldRefExpr = [&, this](const ::substrait::Expression& expr) {
    if (!expr.has_selection()) {
      BOLT_FAIL(
          "Substrait grouping expressions only support field selection expressions.");
    }
    return exprConverter_->toBoltExpr(expr.selection(), inputType);
  };

  if (aggRel.grouping_expressions().empty()) {
    for (const auto& grouping : aggRel.groupings()) {
      for (const auto& groupingExpr : grouping.grouping_expressions()) {
        // Bolt's groupings are limited to be Field.
        boltGroupingExprs.emplace_back(toFieldRefExpr(groupingExpr));
      }
    }
  } else {
    const auto& groupingExpr = aggRel.grouping_expressions();
    for (const auto& grouping : aggRel.groupings()) {
      for (auto ref : grouping.expression_references()) {
        boltGroupingExprs.emplace_back(toFieldRefExpr(groupingExpr.Get(ref)));
      }
    }
  }

  // Parse measures and get the aggregate expressions.
  // Each measure represents one aggregate expression.
  std::vector<core::AggregationNode::Aggregate> aggregates;
  aggregates.reserve(aggRel.measures().size());

  for (const auto& measure : aggRel.measures()) {
    core::FieldAccessTypedExprPtr mask;
    ::substrait::Expression substraitAggMask = measure.filter();
    // Get Aggregation Masks.
    if (measure.has_filter()) {
      if (substraitAggMask.ByteSizeLong() > 0) {
        mask = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
            exprConverter_->toBoltExpr(substraitAggMask, inputType));
      }
    }

    const auto& aggFunction = measure.measure();
    auto funcName =
        substraitParser_->findBoltFunction(aggFunction.function_reference());

    std::vector<core::TypedExprPtr> aggParams;
    aggParams.reserve(aggFunction.arguments().size());
    for (const auto& arg : aggFunction.arguments()) {
      aggParams.emplace_back(
          exprConverter_->toBoltExpr(arg.value(), inputType));
    }
    auto aggBoltType = substraitParser_->parseType(aggFunction.output_type());
    auto aggExpr = std::make_shared<const core::CallTypedExpr>(
        aggBoltType, std::move(aggParams), funcName);
    std::vector<TypePtr> rawInputTypes = SubstraitParser::getInputTypes(
        findFunction(aggFunction.function_reference()));
    aggregates.emplace_back(
        core::AggregationNode::Aggregate{aggExpr, rawInputTypes, mask, {}, {}});
  }

  bool ignoreNullKeys = false;
  std::vector<core::FieldAccessTypedExprPtr> preGroupingExprs;

  // Get the output names of Aggregation.
  std::vector<std::string> aggOutNames;
  aggOutNames.reserve(aggRel.measures().size());
  for (int idx = boltGroupingExprs.size();
       idx < boltGroupingExprs.size() + aggRel.measures().size();
       ++idx) {
    aggOutNames.emplace_back(substraitParser_->makeNodeName(planNodeId_, idx));
  }

  auto aggregationNode = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      aggStep,
      boltGroupingExprs,
      preGroupingExprs,
      aggOutNames,
      aggregates,
      ignoreNullKeys,
      childNode);

  if (aggRel.has_common()) {
    return processEmit(aggRel.common(), std::move(aggregationNode));
  } else {
    return aggregationNode;
  }
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::ProjectRel& projectRel) {
  auto childNode = convertSingleInput<::substrait::ProjectRel>(projectRel);

  // Construct Bolt Expressions.
  auto projectExprs = projectRel.expressions();
  std::vector<std::string> projectNames;
  std::vector<core::TypedExprPtr> expressions;
  projectNames.reserve(projectExprs.size());
  expressions.reserve(projectExprs.size());

  const auto& inputType = childNode->outputType();
  // Note that Substrait projection adds the project expressions on top of the
  // input to the projection node. Thus we need to add the input columns first
  // and then add the projection expressions.

  // First, adding the project names and expressions from the input to
  // the project node.
  size_t colIdx = 0;
  if (appendProject_) {
    for (uint32_t idx = 0; idx < inputType->size(); idx++) {
      const auto& fieldName = inputType->nameOf(idx);
      projectNames.emplace_back(fieldName);
      expressions.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(
          inputType->childAt(idx), fieldName));
      colIdx += 1;
    }
  }
  // Then, adding project expression related project names and expressions.
  for (const auto& expr : projectExprs) {
    expressions.emplace_back(exprConverter_->toBoltExpr(expr, inputType));
    projectNames.emplace_back(
        substraitParser_->makeNodeName(planNodeId_, colIdx++));
  }

  auto projectNode = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(),
      std::move(projectNames),
      std::move(expressions),
      std::move(childNode));
  return projectRel.has_common() ? processEmit(projectRel.common(), projectNode)
                                 : projectNode;
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::SortRel& sortRel) {
  auto childNode = convertSingleInput<::substrait::SortRel>(sortRel);

  auto [sortingKeys, sortingOrders] =
      processSortField(sortRel.sorts(), childNode->outputType());

  return std::make_shared<core::OrderByNode>(
      nextPlanNodeId(),
      sortingKeys,
      sortingOrders,
      false /*isPartial*/,
      childNode);
}

std::pair<
    std::vector<core::FieldAccessTypedExprPtr>,
    std::vector<core::SortOrder>>
SubstraitBoltPlanConverter::processSortField(
    const ::google::protobuf::RepeatedPtrField<::substrait::SortField>&
        sortFields,
    const RowTypePtr& inputType) {
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  sortingKeys.reserve(sortFields.size());
  sortingOrders.reserve(sortFields.size());

  for (const auto& sort : sortFields) {
    sortingOrders.emplace_back(toSortOrder(sort));

    if (sort.has_expr()) {
      auto expression = exprConverter_->toBoltExpr(sort.expr(), inputType);
      auto fieldExpr =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              expression);
      BOLT_CHECK_NOT_NULL(
          fieldExpr, " the sorting key in Sort Operator only support field");
      sortingKeys.emplace_back(fieldExpr);
    }
  }
  return {sortingKeys, sortingOrders};
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::FilterRel& filterRel) {
  auto childNode = convertSingleInput<::substrait::FilterRel>(filterRel);

  auto filterNode = std::make_shared<core::FilterNode>(
      nextPlanNodeId(),
      exprConverter_->toBoltExpr(
          filterRel.condition(), childNode->outputType()),
      childNode);

  if (filterRel.has_common()) {
    return processEmit(filterRel.common(), std::move(filterNode));
  } else {
    return filterNode;
  }
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::FetchRel& fetchRel) {
  if (!fetchRel.has_input()) {
    BOLT_FAIL("Child Rel is expected in FetchRel.");
  }

  // ``FetchRel(SortRel, count > 0, offset == 0)`` lowers to a single
  // TopNNode which streams the top-K rows directly without
  // materialising the full sort. Same shape with ``offset > 0``
  // can't use TopN — TopN doesn't carry an offset — so we fall
  // back to the regular SortNode + LimitNode lowering (build the
  // sort over the input, then a Limit on top with the offset).
  // The writer side
  // (``BoltToSubstraitPlan.cpp::makeFetchFromLimit``) does emit
  // ``FetchRel(sort, offset, count)`` for ``OrderBy().limit(offset,
  // count)`` plans, so this fallback is required for round-trip
  // correctness — without it, plans round-tripping through Substrait
  // failed with ``BOLT_CHECK_EQ(offset, 0)``.
  int64_t earlyOffsetRaw = 0;
  if (fetchRel.has_offset()) {
    earlyOffsetRaw = fetchRel.offset();
  }
  // ``offset_expr`` requires a parsed literal — defer that
  // evaluation to the main path below. For the early-decision we
  // can only tell offset_expr-based plans need the Limit path if
  // the literal happens to be non-zero, so conservatively treat
  // any offset_expr-present case as "non-zero offset" → Limit
  // path. The main path then handles arbitrary literal values.
  const bool nonZeroOffset =
      (earlyOffsetRaw != 0) || fetchRel.has_offset_expr();

  core::PlanNodePtr childNode;
  ::substrait::SortRel sortRel;
  const bool topNFlag =
      fetchRel.input().has_sort() && fetchRel.count() > 0 && !nonZeroOffset;
  if (topNFlag) {
    sortRel = fetchRel.input().sort();
    childNode = toBoltPlan(sortRel.input());
  } else {
    childNode = toBoltPlan(fetchRel.input());
  }

  // Validate i64 → i32 narrowing for FetchRel count/offset before the
  // cast. The proto fields are int64 but ``TopNNode`` / ``LimitNode``
  // take int32; an unchecked ``static_cast`` would wrap any value
  // > INT32_MAX (≈ 2.1B) into a negative number and silently emit
  // wrong LIMIT/OFFSET behaviour. Reject the overflow range loudly.
  // Negative offsets are intentionally allowed — BoltML's high-level
  // ``df.select(offset=-N, count=M)`` API uses negative offset for
  // from-end semantics (fuzzer
  // ``test_random_select_negative_offset_and_sort_order`` exercises
  // this) and Bolt's ``LimitNode`` accepts negative values directly.
  // We only need to reject values that would *wrap* in the cast,
  // i.e. > INT32_MAX (positive) or < INT32_MIN (deeply negative).
  auto checkedI32 = [](int64_t value, const char* field) -> int32_t {
    BOLT_CHECK_GE(
        value,
        std::numeric_limits<int32_t>::min(),
        "Substrait Fetch Rel {} is outside int32 range.",
        field)
    BOLT_CHECK_LE(
        value,
        std::numeric_limits<int32_t>::max(),
        "Substrait Fetch Rel {} is outside int32 range.",
        field)
    return static_cast<int32_t>(value);
  };
  auto readCountFromExpr = [&](const ::substrait::Expression& expr,
                               const char* path) -> int64_t {
    if (!expr.has_literal()) {
      BOLT_FAIL("Substrait Fetch Rel {} only supports literals.", path);
    }
    const auto& lit = expr.literal();
    if (lit.has_i32()) {
      return static_cast<int64_t>(lit.i32());
    }
    if (lit.has_i64()) {
      return lit.i64();
    }
    BOLT_FAIL(
        "Substrait Fetch Rel {} only supports i32 and i64 literals.", path);
  };

  if (topNFlag) {
    auto [sortingKeys, sortingOrders] =
        processSortField(sortRel.sorts(), childNode->outputType());

    int64_t countRaw = 0;
    if (fetchRel.has_count()) {
      countRaw = fetchRel.count();
    } else if (fetchRel.has_count_expr()) {
      countRaw = readCountFromExpr(fetchRel.count_expr(), "count_expr");
    } else {
      BOLT_FAIL("Substrait Fetch Rel count is not set.");
    }
    int32_t count = checkedI32(countRaw, "count");

    int64_t offsetRaw = 0;
    if (fetchRel.has_offset()) {
      offsetRaw = fetchRel.offset();
    } else if (fetchRel.has_offset_expr()) {
      offsetRaw = readCountFromExpr(fetchRel.offset_expr(), "offset_expr");
    }
    int32_t offset = checkedI32(offsetRaw, "offset");

    BOLT_CHECK_EQ(offset, 0);

    return std::make_shared<core::TopNNode>(
        nextPlanNodeId(),
        sortingKeys,
        sortingOrders,
        count,
        false /*isPartial*/,
        childNode);
  }

  int64_t countRaw = 0;
  if (fetchRel.has_count()) {
    countRaw = fetchRel.count();
  } else if (fetchRel.has_count_expr()) {
    countRaw = readCountFromExpr(fetchRel.count_expr(), "count_expr");
  } else {
    BOLT_FAIL("Substrait Fetch Rel count is not set.");
  }
  int32_t count = checkedI32(countRaw, "count");

  int64_t offsetRaw = 0;
  if (fetchRel.has_offset()) {
    offsetRaw = fetchRel.offset();
  } else if (fetchRel.has_offset_expr()) {
    offsetRaw = readCountFromExpr(fetchRel.offset_expr(), "offset_expr");
  }
  int32_t offset = checkedI32(offsetRaw, "offset");

  return std::make_shared<core::LimitNode>(
      nextPlanNodeId(), offset, count, false /*isPartial*/, childNode);
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::ReadRel& readRel,
    std::vector<exec::Split>& outSplits) {
  // emit is not allowed in TableScanNode and ValuesNode related
  // outputs
  if (readRel.has_common()) {
    BOLT_USER_CHECK(
        !readRel.common().has_emit(),
        "Emit not supported for ValuesNode and TableScanNode related Substrait plans.");
  }
  // Get output names and types.
  std::vector<std::string> colNameList;
  std::vector<TypePtr> boltTypeList;
  if (readRel.has_base_schema()) {
    const auto& baseSchema = readRel.base_schema();
    colNameList.reserve(baseSchema.names().size());
    for (const auto& name : baseSchema.names()) {
      colNameList.emplace_back(name);
    }
    boltTypeList = substraitParser_->parseNamedStruct(baseSchema);
  }

  static const std::string kHiveConnectorId = "test-hive";

  // Parse local files and produce exec::Splits. Also collect table-level
  // parameters to pass into HiveTableHandle (e.g., paimon settings).
  std::unordered_map<std::string, std::string> tableParameters;
  std::unordered_set<std::string> localFilePartitionColumnNames;
  if (readRel.has_local_files()) {
    using SubstraitFileFormatCase =
        ::substrait::ReadRel_LocalFiles::FileOrFiles::FileFormatCase;
    const auto& fileList = readRel.local_files().items();
    for (const auto& file : fileList) {
      auto format = toBoltFileFormat(file);
      auto path = file.uri_file();
      auto start = file.start();
      auto length = file.length();
      // In Substrait, a length of 0 means read the entire file.
      if (length == 0) {
        length = std::numeric_limits<uint64_t>::max();
      }
      auto partitionKeys = extractPartitionKeysFromPath(path);
      for (const auto& partitionKey : partitionKeys) {
        localFilePartitionColumnNames.insert(partitionKey.first);
      }
      auto hiveSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
          kHiveConnectorId, path, format, start, length, partitionKeys);
      outSplits.emplace_back(
          std::dynamic_pointer_cast<connector::ConnectorSplit>(hiveSplit));
    }
  } else if (
      readRel.has_extension_table() &&
      readRel.extension_table()
          .detail()
          .Is<::substrait::bolt::PaimonExtensionTable>()) {
    // Handle PaimonExtensionTable: extract file format and file URIs.
    ::substrait::bolt::PaimonExtensionTable ext;
    readRel.extension_table().detail().UnpackTo(&ext);

    // Map file format via a stub FileOrFiles then use toBoltFileFormat.
    ::substrait::ReadRel::LocalFiles::FileOrFiles fmtStub;
    if (ext.has_file_format()) {
      fillSubstraitFileOrFiles(ext.file_format(), &fmtStub);
    }
    auto format = toBoltFileFormat(fmtStub);

    // Build PaimonConnectorSplits: each Paimon split wraps multiple hive
    // splits.
    for (const auto& s : ext.splits()) {
      std::vector<std::shared_ptr<connector::hive::HiveConnectorSplit>>
          hiveSplits;
      hiveSplits.reserve(s.file_uris_size());
      for (const auto& uri : s.file_uris()) {
        auto hiveSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            uri,
            format,
            /*start*/ 0,
            /*length*/ std::numeric_limits<uint64_t>::max());
        hiveSplits.emplace_back(std::move(hiveSplit));
      }
      auto paimonSplit =
          std::make_shared<connector::hive::PaimonConnectorSplit>(
              kHiveConnectorId, std::move(hiveSplits));
      outSplits.emplace_back(
          std::dynamic_pointer_cast<connector::ConnectorSplit>(paimonSplit));
    }

    // Propagate parameters map onto HiveTableHandle.
    if (ext.has_parameters()) {
      const auto& params = ext.parameters();
      for (const auto& kv : params.key_values()) {
        if (kv.has_key() && kv.key().has_string() && kv.has_value() &&
            kv.value().has_string()) {
          tableParameters.emplace(kv.key().string(), kv.value().string());
        }
      }
    }
  } else if (
      readRel.has_extension_table() &&
      readRel.extension_table()
          .detail()
          .Is<::substrait::bolt::TpchExtensionTable>()) {
    // Handle TpchExtensionTable: construct splits and set up TPCH handle.
    ::substrait::bolt::TpchExtensionTable ext;
    readRel.extension_table().detail().UnpackTo(&ext);

    // Build TPCH splits based on num_splits.
    BOLT_CHECK_GT(ext.num_splits(), 0);
    uint64_t parts = ext.num_splits();
    for (uint64_t i = 0; i < parts; ++i) {
      auto split = std::make_shared<connector::tpch::TpchConnectorSplit>(
          ext.connector_id(),
          static_cast<size_t>(parts),
          static_cast<size_t>(i));
      outSplits.emplace_back(
          std::dynamic_pointer_cast<connector::ConnectorSplit>(split));
    }

    // Prepare a TPCH table handle and assignments.
    auto t = tpch::fromTableName(ext.table_name());
    tableParameters.clear();

#ifdef BOLT_USE_ARROW_CONNECTOR
    auto tableHandle =
        std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
            "es-connector");
#else
    // Prefer the precise ``scale_factor_double`` field; fall back to
    // the deprecated uint64 field for plans serialised before the
    // double migration (the deprecated cast loses sub-1 scales but
    // works for whole-number SF=1/10/100/etc., which is the only
    // case the old uint64 ever supported anyway). See
    // ``TpchExtensionTable`` proto doc for the wire-compat
    // rationale.
    const double scaleFactorValue = ext.has_scale_factor_double()
        ? ext.scale_factor_double()
        : static_cast<double>(ext.scale_factor_deprecated());
    std::shared_ptr<connector::tpch::TpchTableHandle> tableHandle =
        std::make_shared<connector::tpch::TpchTableHandle>(
            ext.connector_id(), t, scaleFactorValue);
#endif

    // Build assignments for each requested column.
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        assignments;
    assignments.reserve(ext.column_names_size());
    for (const auto& name : ext.column_names()) {
      assignments.emplace(
          name, std::make_shared<connector::tpch::TpchColumnHandle>(name));
    }

    auto outputType = ROW(std::move(colNameList), std::move(boltTypeList));
    return std::make_shared<core::TableScanNode>(
        nextPlanNodeId(),
        std::move(outputType),
        std::move(tableHandle),
        std::move(assignments));
  }

#ifdef BOLT_USE_ARROW_CONNECTOR
  auto tableHandle = std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
      "es-connector");
#else
  // Bolt requires Filter Pushdown must being enabled.
  bool filterPushdownEnabled = true;
  std::shared_ptr<connector::hive::HiveTableHandle> tableHandle;
  if (!readRel.has_filter()) {
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        kHiveConnectorId,
        "hive_table",
        filterPushdownEnabled,
        connector::hive::SubfieldFilters{},
        nullptr,
        nullptr,
        tableParameters);
  } else {
    connector::hive::SubfieldFilters filters =
        toBoltFilter(colNameList, boltTypeList, readRel.filter());
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        kHiveConnectorId,
        "hive_table",
        filterPushdownEnabled,
        std::move(filters),
        nullptr,
        nullptr,
        tableParameters);
  }
#endif

  // Get assignments and out names.
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (int idx = 0; idx < colNameList.size(); idx++) {
    auto outName = colNameList[idx];
#ifdef BOLT_USE_ARROW_CONNECTOR
    assignments.emplace(
        outName,
        std::make_shared<connector::arrow::ArrowMemoryColumnHandle>(
            outName, boltTypeList[idx]));
#else
    bool isPartitionColumn = localFilePartitionColumnNames.count(outName) > 0;
    assignments[outName] = std::make_shared<connector::hive::HiveColumnHandle>(
        colNameList[idx],
        isPartitionColumn
            ? connector::hive::HiveColumnHandle::ColumnType::kPartitionKey
            : connector::hive::HiveColumnHandle::ColumnType::kRegular,
        boltTypeList[idx],
        boltTypeList[idx]);
#endif
  }
  auto outputType = ROW(std::move(colNameList), std::move(boltTypeList));

  if (readRel.has_virtual_table()) {
    return toBoltPlan(readRel, outputType);
  } else {
    return std::make_shared<core::TableScanNode>(
        nextPlanNodeId(),
        std::move(outputType),
        std::move(tableHandle),
        std::move(assignments));
  }
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::ReadRel& readRel,
    const RowTypePtr& type) {
  ::substrait::ReadRel_VirtualTable readVirtualTable;
  const int64_t numColumns = type->size();
  for (const auto& values : readRel.virtual_table().values()) {
    if (values.fields_size() == numColumns) {
      *readVirtualTable.add_values() = values;
      continue;
    }
    // Older Bolt writers packed each batch column by column into one
    // struct. Standard Substrait uses one struct per row.
    BOLT_CHECK_GT(numColumns, 0);
    BOLT_CHECK_GT(values.fields_size(), numColumns);
    BOLT_CHECK_EQ(values.fields_size() % numColumns, 0);
    const auto batchSize = values.fields_size() / numColumns;
    for (int64_t row = 0; row < batchSize; ++row) {
      auto* rowValues = readVirtualTable.add_values();
      for (int64_t col = 0; col < numColumns; ++col) {
        *rowValues->add_fields() = values.fields(col * batchSize + row);
      }
    }
  }
  const int64_t numRows = readVirtualTable.values_size();
  // An empty virtual table still has a declared schema (the
  // ``base_schema`` named-struct); ``ValuesNode`` constructed with an
  // empty vector list derives ``ROW({})`` and downstream
  // schema-sensitive operators then see a zero-column plan. Build a
  // zero-row ``RowVector`` with the declared ``type`` so the output
  // type matches the schema.
  if (numRows == 0) {
    std::vector<VectorPtr> emptyChildren;
    emptyChildren.reserve(numColumns);
    for (int64_t col = 0; col < numColumns; ++col) {
      emptyChildren.emplace_back(
          BaseVector::create(type->childAt(col), /*size=*/0, pool_));
    }
    auto emptyRow = std::make_shared<RowVector>(
        pool_,
        type,
        /*nulls=*/nullptr,
        /*length=*/0,
        std::move(emptyChildren));
    return std::make_shared<core::ValuesNode>(
        nextPlanNodeId(), std::vector<RowVectorPtr>{std::move(emptyRow)});
  }

  std::vector<VectorPtr> children;
  children.reserve(numColumns);
  for (int64_t col = 0; col < numColumns; ++col) {
    const TypePtr& outputChildType = type->childAt(col);
    std::vector<variant> batchChild;
    batchChild.reserve(numRows);
    VectorPtr complexVec{nullptr};
    auto nulls = allocateNulls(numRows, pool_);

    for (int64_t row = 0; row < numRows; ++row) {
      const auto& rowStruct = readVirtualTable.values(row);
      BOLT_CHECK_EQ(rowStruct.fields_size(), numColumns);
      auto expr = exprConverter_->toBoltExpr(rowStruct.fields(col));
      auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr);
      if (constantExpr == nullptr) {
        BOLT_FAIL("Expected constant expression");
      }
      if (!constantExpr->hasValueVector()) {
        if (!constantExpr->value().isNull() && complexVec != nullptr) {
          BOLT_FAIL(
              "Inconsistent batch values type when converting substrait literals of expected complex type.");
        }
        if (constantExpr->value().isNull()) {
          bits::setNull(
              nulls->asMutable<uint64_t>(), static_cast<int32_t>(row));
        }
        if (complexVec == nullptr) {
          batchChild.emplace_back(constantExpr->value());
        }
      } else {
        const auto& v = constantExpr->valueVector();
        if (complexVec == nullptr) {
          complexVec = BaseVector::create(outputChildType, numRows, pool_);
        }
        complexVec->copy(v.get(), row, /*sourceIndex*/ 0, /*count*/ 1);
      }
    }
    if (complexVec) {
      complexVec->resize(numRows);
      complexVec->setNulls(nulls);
      children.emplace_back(std::move(complexVec));
    } else {
      children.emplace_back(
          setVectorFromVariants(outputChildType, batchChild, pool_));
    }
  }

  auto rowVector = std::make_shared<RowVector>(
      pool_, type, /*nulls=*/nullptr, numRows, std::move(children));
  return std::make_shared<core::ValuesNode>(
      nextPlanNodeId(), std::vector<RowVectorPtr>{std::move(rowVector)});
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::Rel& rel) {
  if (rel.has_aggregate()) {
    return toBoltPlan(rel.aggregate());
  }
  if (rel.has_project()) {
    return toBoltPlan(rel.project());
  }
  if (rel.has_filter()) {
    return toBoltPlan(rel.filter());
  }
  if (rel.has_read()) {
    std::vector<exec::Split> splits;
    auto planNode = toBoltPlan(rel.read(), splits);
    /// Not all source nodes have splits. ValuesNode does not.
    if (planNode->requiresSplits() && !splits.empty()) {
      splitInfoMap_[planNode->id()] = std::move(splits);
    }
    return planNode;
  }
  if (rel.has_fetch()) {
    return toBoltPlan(rel.fetch());
  }
  if (rel.has_sort()) {
    return toBoltPlan(rel.sort());
  }
  if (rel.has_write()) {
    // Convert the input first and then build the write node via helper.
    auto input = toBoltPlan(rel.write().input());
    auto node = bytedance::bolt::substrait::makeWritePlanNode(
        rel.write(), input, nextPlanNodeId());
    if (rel.write().has_common()) {
      return processEmit(rel.write().common(), std::move(node));
    }
    return node;
  }
  if (rel.has_hash_join()) {
    return toBoltPlan(rel.hash_join());
  }
  if (rel.has_extension_single()) {
    if (auto node = toBoltPlan(rel.extension_single())) {
      return node;
    }
  }
  BOLT_NYI("Substrait conversion not supported for Rel.");
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::ExtensionSingleRel& rel) {
  core::PlanNodePtr node;
  core::PlanNodePtr input;

  if (rel.has_input()) {
    input = toBoltPlan(rel.input());
  }

  if (rel.detail().Is<::substrait::bolt::TorchRelDetail>()) {
    if (input == nullptr) {
      BOLT_FAIL(
          "ExtensionSingleRel with TorchRelDetail detail also requires a valid input Rel.");
    }
    ::substrait::bolt::TorchRelDetail torch_detail;
    rel.detail().UnpackTo(&torch_detail);
#if defined BOLT_HAS_TORCH && BOLT_HAS_TORCH == 1
    auto signature = exec::parseTypeSignature(torch_detail.type_signature());
    auto rowType = asRowType(signature.asType());
    if (rowType == nullptr) {
      BOLT_FAIL(
          "Torch node return type must be a valid ROW type: {}",
          torch_detail.type_signature());
    }
    node = std::make_shared<::bytedance::bolt::torch::TorchNode>(
        nextPlanNodeId(), input, torch_detail.script(), std::move(rowType));
#else
    BOLT_FAIL("Bolt is not compiled with torch support enabled.");
#endif
  }

  // Handle extensions: try Shuffle (Struct detail) first, then Python
  // (EmbeddedFunction). The converter owns PlanNode id allocation; pass a
  // freshly-allocated id so multiple shuffles / Python UDFs in the same
  // plan get distinct ids (engine invariant: PlanNode ids are unique).
  if (node == nullptr) {
    if (auto shuffleNode = bytedance::bolt::substrait::makeShufflePlanNode(
            nextPlanNodeId(), rel, input)) {
      node = std::move(shuffleNode);
    } else if (rel.detail().Is<::substrait::Expression_EmbeddedFunction>()) {
#if defined BOLT_HAS_PYTHON && BOLT_HAS_PYTHON == 1
      node = bytedance::bolt::substrait::makePythonPlanNode(
          nextPlanNodeId(), rel, input);
#else
      BOLT_UNSUPPORTED(
          "Python Substrait functions require BOLT_BUILD_PYTHON_PACKAGE.");
#endif
    }
  }

  if (node != nullptr && rel.has_common()) {
    node = processEmit(rel.common(), std::move(node));
  }
  return node;
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::RelRoot& root) {
  // TODO: Use the names as the output names for the whole computing.
  if (root.has_input()) {
    const auto& rel = root.input();
    return toBoltPlan(rel);
  }
  BOLT_FAIL("Input is expected in RelRoot.");
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::Plan& substraitPlan) {
  BOLT_CHECK(
      checkTypeExtension(substraitPlan),
      "Unsupported Substrait type extension.")
  // Construct the function map based on the Substrait representation.
  constructFunctionMap(substraitPlan);

  // Construct the expression converter.
  // Share the same SubstraitParser with the expression converter and seed it
  // with the function map for lookups.
  exprConverter_ =
      std::make_shared<SubstraitBoltExprConverter>(pool_, substraitParser_);

  // In fact, only one RelRoot or Rel is expected here.
  BOLT_CHECK_EQ(substraitPlan.relations_size(), 1);
  const auto& rel = substraitPlan.relations(0);
  if (rel.has_root()) {
    return toBoltPlan(rel.root());
  }
  if (rel.has_rel()) {
    return toBoltPlan(rel.rel());
  }

  BOLT_FAIL("RelRoot or Rel is expected in Plan.");
}

std::string SubstraitBoltPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

// This class contains the needed infos for Filter Pushdown.
// TODO: Support different types here.
class FilterInfo {
 public:
  // Used to set the left bound.
  void setLeft(double left, bool isExclusive) {
    left_ = left;
    leftExclusive_ = isExclusive;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Used to set the right bound.
  void setRight(double right, bool isExclusive) {
    right_ = right;
    rightExclusive_ = isExclusive;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Will fordis Null value if called once.
  void forbidsNull() {
    nullAllowed_ = false;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Return the initialization status.
  bool isInitialized() {
    return isInitialized_ ? true : false;
  }

  // The left bound.
  std::optional<double> left_ = std::nullopt;
  // The right bound.
  std::optional<double> right_ = std::nullopt;
  // The Null allowing.
  bool nullAllowed_ = true;
  // If true, left bound will be exclusive.
  bool leftExclusive_ = false;
  // If true, right bound will be exclusive.
  bool rightExclusive_ = false;

 private:
  bool isInitialized_ = false;
};

connector::hive::SubfieldFilters SubstraitBoltPlanConverter::toBoltFilter(
    const std::vector<std::string>& inputNameList,
    const std::vector<TypePtr>& inputTypeList,
    const ::substrait::Expression& substraitFilter) {
  connector::hive::SubfieldFilters filters;
  // A map between the column index and the FilterInfo for that column.
  std::unordered_map<int, std::shared_ptr<FilterInfo>> colInfoMap;
  for (int idx = 0; idx < inputNameList.size(); idx++) {
    colInfoMap[idx] = std::make_shared<FilterInfo>();
  }

  std::vector<::substrait::Expression_ScalarFunction> scalarFunctions;
  flattenConditions(substraitFilter, scalarFunctions);
  // Construct the FilterInfo for the related column.
  for (const auto& scalarFunction : scalarFunctions) {
    auto filterNameSpec =
        substraitParser_->findFunctionSpec(scalarFunction.function_reference());
    auto filterName = getNameBeforeDelimiter(filterNameSpec, ":");
    int32_t colIdx;
    // TODO: Add different types' support here.
    double val;
    for (auto& arg : scalarFunction.arguments()) {
      auto argExpr = arg.value();
      auto typeCase = argExpr.rex_type_case();
      switch (typeCase) {
        case ::substrait::Expression::RexTypeCase::kSelection: {
          auto sel = argExpr.selection();
          // TODO: Only direct reference is considered here.
          auto dRef = sel.direct_reference();
          colIdx = substraitParser_->parseReferenceSegment(dRef);
          break;
        }
        case ::substrait::Expression::RexTypeCase::kLiteral: {
          auto sLit = argExpr.literal();
          // TODO: Only double is considered here.
          val = sLit.fp64();
          break;
        }
        default:
          BOLT_NYI(
              "Substrait conversion not supported for arg type '{}'", typeCase);
      }
    }
    if (filterName == "is_not_null") {
      colInfoMap[colIdx]->forbidsNull();
    } else if (filterName == "gte") {
      colInfoMap[colIdx]->setLeft(val, false);
    } else if (filterName == "gt") {
      colInfoMap[colIdx]->setLeft(val, true);
    } else if (filterName == "lte") {
      colInfoMap[colIdx]->setRight(val, false);
    } else if (filterName == "lt") {
      colInfoMap[colIdx]->setRight(val, true);
    } else {
      BOLT_NYI(
          "Substrait conversion not supported for filter name '{}'",
          filterName);
    }
  }

  // Construct the Filters.
  for (int idx = 0; idx < inputNameList.size(); idx++) {
    auto filterInfo = colInfoMap[idx];
    double leftBound;
    double rightBound;
    bool leftUnbounded = true;
    bool rightUnbounded = true;
    bool leftExclusive = false;
    bool rightExclusive = false;
    if (filterInfo->isInitialized()) {
      if (filterInfo->left_) {
        leftUnbounded = false;
        leftBound = filterInfo->left_.value();
        leftExclusive = filterInfo->leftExclusive_;
      }
      if (filterInfo->right_) {
        rightUnbounded = false;
        rightBound = filterInfo->right_.value();
        rightExclusive = filterInfo->rightExclusive_;
      }
      bool nullAllowed = filterInfo->nullAllowed_;
      filters[common::Subfield(inputNameList[idx])] =
          std::make_unique<common::DoubleRange>(
              leftBound,
              leftUnbounded,
              leftExclusive,
              rightBound,
              rightUnbounded,
              rightExclusive,
              nullAllowed);
    }
  }
  return filters;
}

void SubstraitBoltPlanConverter::flattenConditions(
    const ::substrait::Expression& substraitFilter,
    std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions) {
  auto typeCase = substraitFilter.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kScalarFunction: {
      auto sFunc = substraitFilter.scalar_function();
      auto filterNameSpec =
          substraitParser_->findFunctionSpec(sFunc.function_reference());
      // TODO: Only and relation is supported here.
      if (getNameBeforeDelimiter(filterNameSpec, ":") == "and") {
        for (const auto& sCondition : sFunc.arguments()) {
          flattenConditions(sCondition.value(), scalarFunctions);
        }
      } else {
        scalarFunctions.emplace_back(sFunc);
      }
      break;
    }
    default:
      BOLT_NYI("GetFlatConditions not supported for type '{}'", typeCase);
  }
}

void SubstraitBoltPlanConverter::constructFunctionMap(
    const ::substrait::Plan& substraitPlan) {
  // Each plan owns its anchors; do not retain declarations from an earlier
  // plan.
  functionMap_.clear();
  // Also collect type extension mappings (anchor -> name) and pass to parser.
  std::unordered_map<uint32_t, std::string> typeExtMap{{0, "UNKNOWN"}};
  for (const auto& sExtension : substraitPlan.extensions()) {
    if (sExtension.has_extension_function()) {
      const auto& sFmap = sExtension.extension_function();
      auto id = sFmap.function_anchor();
      auto name = sFmap.name();
      functionMap_[id] = name;
    }
    if (sExtension.has_extension_type()) {
      const auto& tDecl = sExtension.extension_type();
      // Map type_anchor to its declared name.
      typeExtMap[tDecl.type_anchor()] = tDecl.name();
    }
  }
  substraitParser_->setFunctionMap(functionMap_);
  substraitParser_->setTypeExtensionMap(typeExtMap);
}

bool SubstraitBoltPlanConverter::checkTypeExtension(
    const ::substrait::Plan& substraitPlan) {
  for (const auto& sExtension : substraitPlan.extensions()) {
    if (!sExtension.has_extension_type()) {
      continue;
    }

    const auto& name = sExtension.extension_type().name();
    if (!isSupportedTypeExtensionName(name)) {
      return false;
    }
  }
  return true;
}

const std::string& SubstraitBoltPlanConverter::findFunction(uint64_t id) const {
  return substraitParser_->findFunctionSpec(id);
}

core::PlanNodePtr SubstraitBoltPlanConverter::toBoltPlan(
    const ::substrait::HashJoinRel& joinRel) {
  // Convert left and right inputs first.
  auto leftNode = toBoltPlan(joinRel.left());
  auto rightNode = toBoltPlan(joinRel.right());

  const auto& leftType = leftNode->outputType();
  const auto& rightType = rightNode->outputType();

  // Parse join type.
  auto boltJoinType = toBoltJoinType(joinRel.type());

  // Prepare join keys.
  std::vector<core::FieldAccessTypedExprPtr> leftKeys;
  std::vector<core::FieldAccessTypedExprPtr> rightKeys;
  leftKeys.reserve(
      joinRel.keys_size() > 0 ? joinRel.keys_size() : joinRel.left_keys_size());
  rightKeys.reserve(
      joinRel.keys_size() > 0 ? joinRel.keys_size()
                              : joinRel.right_keys_size());

  if (joinRel.keys_size() > 0) {
    for (const auto& key : joinRel.keys()) {
      // Only simple comparisons are supported.
      ::substrait::ComparisonJoinKey_ComparisonType comparison =
          key.comparison();
      if (!comparison.has_simple()) {
        BOLT_UNSUPPORTED(
            "Custom comparison functions in HashJoinRel keys are not supported");
      }
      switch (comparison.simple()) {
        case ::substrait::
            ComparisonJoinKey_SimpleComparisonType_SIMPLE_COMPARISON_TYPE_EQ:
          break; // regular join key
        case ::substrait::
            ComparisonJoinKey_SimpleComparisonType_SIMPLE_COMPARISON_TYPE_IS_NOT_DISTINCT_FROM:
          BOLT_UNSUPPORTED(
              "HashJoinRel IS_NOT_DISTINCT_FROM keys are not supported");
        default:
          BOLT_UNSUPPORTED(
              "Unsupported HashJoinRel key comparison type: {}",
              static_cast<int>(comparison.simple()));
      }

      auto lExpr = exprConverter_->toBoltExpr(key.left(), leftType);
      auto rExpr = exprConverter_->toBoltExpr(key.right(), rightType);
      auto lField =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(lExpr);
      auto rField =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(rExpr);
      BOLT_CHECK_NOT_NULL(lField, "Join left key must be a field");
      BOLT_CHECK_NOT_NULL(rField, "Join right key must be a field");
      leftKeys.emplace_back(lField);
      rightKeys.emplace_back(rField);
    }
  } else {
    // Fallback to deprecated left_keys/right_keys.
    BOLT_CHECK_EQ(joinRel.left_keys_size(), joinRel.right_keys_size());
    for (int i = 0; i < joinRel.left_keys_size(); ++i) {
      auto lExpr = exprConverter_->toBoltExpr(joinRel.left_keys(i), leftType);
      auto rExpr = exprConverter_->toBoltExpr(joinRel.right_keys(i), rightType);
      auto lField =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(lExpr);
      auto rField =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(rExpr);
      BOLT_CHECK_NOT_NULL(lField, "Join left key must be a field");
      BOLT_CHECK_NOT_NULL(rField, "Join right key must be a field");
      leftKeys.emplace_back(lField);
      rightKeys.emplace_back(rField);
    }
  }

  // Compute output schema based on join type.
  std::vector<std::string> outNames;
  std::vector<TypePtr> outTypes;

  auto appendRowType = [&](const RowTypePtr& rowType) {
    const auto& names = rowType->names();
    const auto& types = rowType->children();
    outNames.insert(outNames.end(), names.begin(), names.end());
    outTypes.insert(outTypes.end(), types.begin(), types.end());
  };

  const bool rightAnti =
      joinRel.type() == ::substrait::HashJoinRel::JOIN_TYPE_RIGHT_ANTI;
  if (rightAnti) {
    appendRowType(rightType);
  } else if (
      core::isLeftSemiFilterJoin(boltJoinType) ||
      boltJoinType == core::JoinType::kAnti) {
    appendRowType(leftType);
  } else if (core::isRightSemiFilterJoin(boltJoinType)) {
    appendRowType(rightType);
  } else if (core::isLeftSemiProjectJoin(boltJoinType)) {
    appendRowType(leftType);
    outNames.emplace_back("match");
    outTypes.emplace_back(BOOLEAN());
  } else if (core::isRightSemiProjectJoin(boltJoinType)) {
    appendRowType(rightType);
    outNames.emplace_back("match");
    outTypes.emplace_back(BOOLEAN());
  } else {
    // Inner/Left/Right/Full: concatenate left then right.
    appendRowType(leftType);
    appendRowType(rightType);
  }

  // Older BoltML plans use output_names as a layout selection in their
  // explicit replacement-project mode. Keep that compatibility isolated
  // from standard Substrait, where output_names only supplies aliases.
  if (!appendProject_ && joinRel.has_common() &&
      joinRel.common().hint().output_names_size() > 0) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    names.reserve(joinRel.common().hint().output_names_size());
    types.reserve(joinRel.common().hint().output_names_size());
    for (const auto& name : joinRel.common().hint().output_names()) {
      names.push_back(name);
      for (size_t i = 0; i < outNames.size(); i++) {
        if (outNames[i] == name) {
          types.push_back(outTypes[i]);
          break;
        }
      }
      if (names.size() != types.size()) {
        BOLT_FAIL(
            "Invalid hint output name {} for hash join. Possible names are: {}",
            name,
            fmt::join(outNames, ", "));
      }
    }
    outNames = std::move(names);
    outTypes = std::move(types);
  }

  auto outputType = ROW(std::move(outNames), std::move(outTypes));

  if (rightAnti) {
    std::swap(leftNode, rightNode);
    std::swap(leftKeys, rightKeys);
  }
  core::PlanNodePtr joinNode = std::make_shared<core::HashJoinNode>(
      nextPlanNodeId(),
      boltJoinType,
      false /*nullAware*/,
      leftKeys,
      rightKeys,
      nullptr /*filter*/,
      leftNode,
      rightNode,
      outputType);
  if (joinRel.has_post_join_filter()) {
    joinNode = std::make_shared<core::FilterNode>(
        nextPlanNodeId(),
        exprConverter_->toBoltExpr(joinRel.post_join_filter(), outputType),
        joinNode);
  }

  if (joinRel.has_common()) {
    return processEmit(joinRel.common(), std::move(joinNode));
  }
  return joinNode;
}
} // namespace bytedance::bolt::substrait
