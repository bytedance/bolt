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

#include "bolt/exec/MarkDistinct.h"
#include "bolt/common/base/Range.h"
#include "bolt/vector/FlatVector.h"

#include <algorithm>
#include <utility>
namespace bytedance::bolt::exec {

MarkDistinct::MarkDistinct(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::MarkDistinctNode>& planNode)
    : Operator(
          driverCtx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "MarkDistinct") {
  const auto& inputType = planNode->sources()[0]->outputType();

  // Set all input columns as identity projection.
  for (auto i = 0; i < inputType->size(); ++i) {
    identityProjections_.emplace_back(i, i);
  }

  // We will use result[0] for distinct mask output.
  resultProjections_.emplace_back(0, inputType->size());

  groupingSet_ = GroupingSet::createForMarkDistinct(
      inputType,
      createVectorHashers(inputType, planNode->distinctKeys()),
      operatorCtx_.get(),
      &nonReclaimableSection_);

  results_.resize(1);
}

void MarkDistinct::addInput(RowVectorPtr input) {
  groupingSet_->addInput(input, false /*mayPushdown*/);

  input_ = std::move(input);
}

RowVectorPtr MarkDistinct::getOutput() {
  if (isFinished() || !input_) {
    return nullptr;
  }

  auto outputSize = input_->size();
  // Reuse memory for the ID vector if possible.
  VectorPtr& result = results_[0];
  if (result && result.use_count() == 1) {
    BaseVector::prepareForReuse(result, outputSize);
  } else {
    result = BaseVector::create(BOOLEAN(), outputSize, operatorCtx_->pool());
  }

  // newGroups contains the indices of distinct rows.
  // For each index in newGroups, we mark the index'th bit true in the result
  // vector.
  auto resultBits =
      results_[0]->as<FlatVector<bool>>()->mutableRawValues<uint64_t>();

  bits::fillBits(resultBits, 0, outputSize, false);
  // TODO: May be able to utilize batch processing with savedNewGroups from
  // GroupingSet
  for (const auto i : groupingSet_->hashLookup().newGroups) {
    bits::setBit(resultBits, i, true);
  }
  auto output = fillOutput(outputSize, nullptr);

  // Drop reference to input_ to make it singly-referenced at the producer and
  // allow for memory reuse.
  input_ = nullptr;

  return output;
}

bool MarkDistinct::isFinished() {
  return noMoreInput_ && !input_;
}
} // namespace bytedance::bolt::exec
