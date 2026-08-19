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

#include "bolt/common/base/SparkCompatibility.h"

#include "bolt/core/Expressions.h"
#include "bolt/exec/FilterProject.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/FieldReference.h"
#include "bolt/vector/LazyVector.h"
#include "bolt/vector/VectorEncoding.h"
namespace bytedance::bolt::exec {
namespace {
bool checkAddIdentityProjection(
    const core::TypedExprPtr& projection,
    const RowTypePtr& inputType,
    column_index_t outputChannel,
    std::vector<IdentityProjection>& identityProjections) {
  if (auto field = core::TypedExprs::asFieldAccess(projection)) {
    const auto& inputs = field->inputs();
    if (inputs.empty() ||
        (inputs.size() == 1 &&
         dynamic_cast<const core::InputTypedExpr*>(inputs[0].get()))) {
      const auto inputChannel = inputType->getChildIdx(field->name());
      identityProjections.emplace_back(inputChannel, outputChannel);
      return true;
    }
  }

  return false;
}

void loadReusedLazyVectors(
    const RowVectorPtr& input,
    const std::vector<column_index_t>& reusedInputChannels,
    const SelectivityVector& rows) {
  if (!input || reusedInputChannels.empty()) {
    return;
  }

  auto& children = input->children();
  for (auto inputChannel : reusedInputChannels) {
    if (isLazyNotLoaded(*children[inputChannel])) {
      LazyVector::ensureLoadedRows(children[inputChannel], rows);
    }
  }
}

// Split stats to attrbitute cardinality reduction to the Filter node.
std::vector<OperatorStats> splitStats(
    const OperatorStats& combinedStats,
    const core::PlanNodeId& filterNodeId) {
  OperatorStats filterStats;

  filterStats.operatorId = combinedStats.operatorId;
  filterStats.pipelineId = combinedStats.pipelineId;
  filterStats.planNodeId = filterNodeId;
  filterStats.operatorType = combinedStats.operatorType;
  filterStats.numDrivers = combinedStats.numDrivers;

  filterStats.inputBytes = combinedStats.inputBytes;
  filterStats.inputPositions = combinedStats.inputPositions;
  filterStats.inputVectors = combinedStats.inputVectors;

  // Estimate Filter's output bytes based on cardinality change.
  const double filterRate = combinedStats.inputPositions > 0
      ? (combinedStats.outputPositions * 1.0 / combinedStats.inputPositions)
      : 1.0;

  filterStats.outputBytes = (uint64_t)(filterStats.inputBytes * filterRate);
  filterStats.outputPositions = combinedStats.outputPositions;
  filterStats.outputVectors = combinedStats.outputVectors;

  auto projectStats = combinedStats;
  projectStats.inputBytes = filterStats.outputBytes;
  projectStats.inputPositions = filterStats.outputPositions;
  projectStats.inputVectors = filterStats.outputVectors;

  return {std::move(projectStats), std::move(filterStats)};
}
} // namespace

FilterProject::FilterProject(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::FilterNode>& filter,
    const std::shared_ptr<const core::ProjectNode>& project,
    ProjectType type)
    : Operator(
          driverCtx,
          project ? project->outputType() : filter->outputType(),
          operatorId,
          project ? project->id() : filter->id(),
          "FilterProject"),
      hasFilter_(filter != nullptr),
      driverCtx_(driverCtx),
      project_(project),
      filter_(filter),
      acceptCompositeInput_(
          driverCtx->queryConfig().isHashAggregationCompositeOutputEnabled()),
      skipForCompositeInput_(type == ProjectType::kSkipCompositeRowVector) {
  if (filter_ != nullptr && project_ != nullptr) {
    folly::Synchronized<OperatorStats>& opStats = Operator::stats();
    opStats.withWLock([&](auto& stats) {
      stats.setStatSplitter(
          [filterId = filter_->id()](const auto& combinedStats) {
            return splitStats(combinedStats, filterId);
          });
    });
  }
}

void FilterProject::initialize() {
  Operator::initialize();
  std::vector<core::TypedExprPtr> allExprs;
  if (hasFilter_) {
    BOLT_CHECK_NOT_NULL(filter_);
    allExprs.push_back(filter_->filter());
  }
  if (project_) {
    const auto& inputType = project_->sources()[0]->outputType();
    for (column_index_t i = 0; i < project_->projections().size(); i++) {
      auto& projection = project_->projections()[i];
      bool identityProjection = checkAddIdentityProjection(
          projection, inputType, i, identityProjections_);
      if (!identityProjection) {
        allExprs.push_back(projection);
        resultProjections_.emplace_back(allExprs.size() - 1, i);
      }
    }
  } else {
    for (column_index_t i = 0; i < outputType_->size(); ++i) {
      identityProjections_.emplace_back(i, i);
    }
    isIdentityProjection_ = true;
  }
  {
    std::unordered_map<column_index_t, size_t> inputChannelCounts;
    for (const auto& projection : identityProjections_) {
      ++inputChannelCounts[projection.inputChannel];
    }
    for (const auto& [inputChannel, count] : inputChannelCounts) {
      if (count > 1) {
        reusedInputChannels_.push_back(inputChannel);
      }
    }
  }
  numExprs_ = allExprs.size();
  exprs_ = makeExprSetFromFlag(std::move(allExprs), operatorCtx_->execCtx());

  if (numExprs_ > 0 && !identityProjections_.empty()) {
    const auto inputType = project_ ? project_->sources()[0]->outputType()
                                    : filter_->sources()[0]->outputType();
    std::unordered_set<uint32_t> distinctFieldIndices;
    for (auto field : exprs_->distinctFields()) {
      auto fieldIndex = inputType->getChildIdx(field->name());
      distinctFieldIndices.insert(fieldIndex);
    }
    for (auto identityField : identityProjections_) {
      if (distinctFieldIndices.find(identityField.inputChannel) !=
          distinctFieldIndices.end()) {
        multiplyReferencedFieldIndices_.push_back(identityField.inputChannel);
      }
    }
  }
  filter_.reset();
  project_.reset();
}

void FilterProject::addInput(RowVectorPtr input) {
  input_ = std::move(input);
  if (!skipForCompositeInput_ || !RowVector::isComposite(input_)) {
    for (auto& childVec : input_->children()) {
      if ((acceptCompositeInput_ && childVec == nullptr) ||
          childVec->isLazy() || isLazyNotLoaded(*childVec) ||
          childVec->encoding() != VectorEncoding::Simple::DICTIONARY) {
        continue;
      }
      // only flat dictionary vectors if needed
      auto size = childVec->size();
      auto baseSize = childVec->baseSize();
      double ratio = baseSize > 0 ? size * 1.0 / baseSize : 1;
      if (ratio <= 0.01) {
        BaseVector::flattenVector(childVec);
      }
    }
  }
  numProcessedInputRows_ = 0;
}

bool FilterProject::allInputProcessed() {
  if (!input_) {
    return true;
  }
  if (numProcessedInputRows_ == input_->size()) {
    input_ = nullptr;
    return true;
  }
  return false;
}

bool FilterProject::isFinished() {
  return noMoreInput_ && allInputProcessed();
}

RowVectorPtr FilterProject::getOutput() {
  if (allInputProcessed()) {
    return nullptr;
  }

  vector_size_t size = input_->size();
  bool isCompositeInput = RowVector::isComposite(input_);

  if constexpr (::bytedance::bolt::kSparkCompatible) {
    if (skipForCompositeInput_ && isCompositeInput) {
      BOLT_CHECK(!hasFilter_ && !isIdentityProjection_);
      numProcessedInputRows_ = size;
      return fillCompositeOutput(size, input_->children());
    }
  }

  LocalSelectivityVector localRows(*operatorCtx_->execCtx(), size);
  auto* rows = localRows.get();
  BOLT_DCHECK_NOT_NULL(rows)
  rows->setAll();
  EvalCtx evalCtx(
      operatorCtx_->execCtx(),
      exprs_.get(),
      input_.get(),
      driverCtx_->currentSplitStr,
      acceptCompositeInput_);

  // Pre-load lazy vectors which are referenced by both expressions and identity
  // projections.
  for (auto fieldIdx : multiplyReferencedFieldIndices_) {
    evalCtx.ensureFieldLoaded(fieldIdx, *rows);
  }

  if (!hasFilter_) {
    numProcessedInputRows_ = size;
    BOLT_CHECK(!isIdentityProjection_);
    auto results = project(*rows, evalCtx);
    if (isCompositeInput) {
      return fillCompositeOutput(size, results);
    } else {
      loadReusedLazyVectors(input_, reusedInputChannels_, *rows);
      return fillOutput(size, nullptr, results);
    }
  }
  BOLT_CHECK(!isCompositeInput);

  // evaluate filter
  auto numOut = filter(evalCtx, *rows);
  numProcessedInputRows_ = size;
  if (numOut == 0) { // no rows passed the filer
    input_ = nullptr;
    return nullptr;
  }

  bool allRowsSelected = (numOut == size);

  // evaluate projections (if present)
  std::vector<VectorPtr> results;
  if (!isIdentityProjection_) {
    if (!allRowsSelected) {
      rows->setFromBits(filterEvalCtx_.selectedBits->as<uint64_t>(), size);
    }
    results = project(*rows, evalCtx);
  } else if (!allRowsSelected) {
    rows->setFromBits(filterEvalCtx_.selectedBits->as<uint64_t>(), size);
  }

  loadReusedLazyVectors(input_, reusedInputChannels_, *rows);
  return fillOutput(
      numOut,
      allRowsSelected ? nullptr : filterEvalCtx_.selectedIndices,
      results);
}

std::vector<VectorPtr> FilterProject::project(
    const SelectivityVector& rows,
    EvalCtx& evalCtx) {
  std::vector<VectorPtr> results(exprs_->size(), nullptr);
  exprs_->eval(
      hasFilter_ ? 1 : 0, numExprs_, !hasFilter_, rows, evalCtx, results);
  return results;
}

vector_size_t FilterProject::filter(
    EvalCtx& evalCtx,
    const SelectivityVector& allRows) {
  std::vector<VectorPtr> results;
  exprs_->eval(0, 1, true, allRows, evalCtx, results);
  return processFilterResults(results[0], allRows, filterEvalCtx_, pool());
}

RowVectorPtr FilterProject::fillCompositeOutput(
    vector_size_t size,
    std::vector<VectorPtr>& results) {
  CompositeRowVectorPtr output;
  if (!skipForCompositeInput_) {
    std::vector<VectorPtr> projectedChildren(outputType_->size());
    projectChildren(
        projectedChildren, input_, identityProjections_, size, nullptr);
    projectChildren(
        projectedChildren, results, resultProjections_, size, nullptr);
    output = std::make_shared<CompositeRowVector>(
        outputType_,
        size,
        operatorCtx_->pool(),
        nullptr,
        std::move(projectedChildren));
  } else {
    results.resize(outputType_->size(), nullptr);
    output = std::make_shared<CompositeRowVector>(
        outputType_, size, operatorCtx_->pool(), nullptr, std::move(results));
  }
  std::dynamic_pointer_cast<CompositeRowVector>(input_)->moveto(output);
  return output;
}

OperatorStats FilterProject::stats(bool clear) {
  auto stats = Operator::stats(clear);
  if (operatorCtx_->driverCtx()->queryConfig().operatorTrackExpressionStats() &&
      exprs_ != nullptr) {
    stats.expressionStats = exprs_->stats();
  }
  return stats;
}

} // namespace bytedance::bolt::exec
