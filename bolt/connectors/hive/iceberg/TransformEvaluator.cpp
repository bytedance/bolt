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
 * 2026-03-30.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/connectors/hive/iceberg/TransformEvaluator.h"

#include "bolt/expression/Expr.h"

namespace bytedance::bolt::connector::hive::iceberg {

TransformEvaluator::TransformEvaluator(
    const std::vector<core::TypedExprPtr>& expressions,
    const ConnectorQueryCtx* connectorQueryCtx)
    : connectorQueryCtx_(connectorQueryCtx) {
  BOLT_CHECK_NOT_NULL(connectorQueryCtx_);
  BOLT_CHECK_NOT_NULL(connectorQueryCtx_->expressionEvaluator());
  exprSets_.reserve(expressions.size());
  for (const auto& expression : expressions) {
    exprSets_.push_back(
        connectorQueryCtx_->expressionEvaluator()->compile(expression));
  }
}

std::vector<VectorPtr> TransformEvaluator::evaluate(
    const RowVectorPtr& input) const {
  std::vector<VectorPtr> results(exprSets_.size());
  SelectivityVector rows(input->size());

  for (auto i = 0; i < exprSets_.size(); ++i) {
    connectorQueryCtx_->expressionEvaluator()->evaluate(
        exprSets_[i].get(), rows, *input, results[i]);
  }

  return results;
}

} // namespace bytedance::bolt::connector::hive::iceberg
