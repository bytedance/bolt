/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/shuffle/sparksql/EffectiveSizeEstimator.h"

#include "bolt/type/Type.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql {

bool EffectiveSizeEstimator::typeContainsStringView(
    const bytedance::bolt::TypePtr& type) {
  auto kind = type->kind();
  if (kind == bytedance::bolt::TypeKind::VARCHAR ||
      kind == bytedance::bolt::TypeKind::VARBINARY) {
    return true;
  }
  for (auto i = 0; i < type->size(); ++i) {
    if (typeContainsStringView(type->childAt(i))) {
      return true;
    }
  }
  return false;
}

void EffectiveSizeEstimator::detectBinaryColumns(
    const bytedance::bolt::RowVectorPtr& rv) {
  binaryColumnIndices_.clear();
  for (uint32_t i = 0; i < rv->childrenSize(); ++i) {
    if (typeContainsStringView(rv->childAt(i)->type())) {
      binaryColumnIndices_.push_back(i);
    }
  }
  initialized_ = true;
}

uint64_t EffectiveSizeEstimator::estimate(
    const bytedance::bolt::RowVectorPtr& rv) {
  auto flatSize = rv->estimateFlatSize();

  if (!initialized_) {
    detectBinaryColumns(rv);
  }
  if (binaryColumnIndices_.empty()) {
    return flatSize;
  }

  // Fast path: if no batch has triggered full scan mode, use flatSize.
  // Once any batch has ratio >= kFullScanRatioThreshold AND
  // flatSize > kFullScanSizeThreshold, switch to full scan permanently.
  if (!alwaysFullScan_) {
    // First batch: do a probe scan to determine if full scan is needed.
    if (flatSize <= kFullScanSizeThreshold) {
      LOG(INFO) << "flatSize: " << flatSize;
      return flatSize;
    }
  }

  // Full scan mode: scan every batch.
  uint64_t estimatedBinaryFlatSize = 0;
  auto actualBinarySize = computeActualBinarySize(rv, estimatedBinaryFlatSize);
  auto effectiveSize = (flatSize > estimatedBinaryFlatSize)
      ? flatSize - estimatedBinaryFlatSize + actualBinarySize
      : actualBinarySize;

  if (estimatedBinaryFlatSize > 0) {
    cachedRatio_ =
        static_cast<double>(actualBinarySize) / estimatedBinaryFlatSize;
    if (cachedRatio_ > kFullScanRatioThreshold &&
        flatSize > kFullScanSizeThreshold) {
      alwaysFullScan_ = true;
    }
  }

  LOG(INFO) << "effectiveSize: " << effectiveSize << ", flatSize: " << flatSize;
  return effectiveSize;
}

uint64_t EffectiveSizeEstimator::computeActualBinarySize(
    const bytedance::bolt::RowVectorPtr& rv,
    uint64_t& estimatedBinaryFlatSize) const {
  uint64_t totalSize = 0;
  estimatedBinaryFlatSize = 0;
  for (auto colIdx : binaryColumnIndices_) {
    auto* child = rv->childAt(colIdx).get();
    totalSize += scanVector(child, estimatedBinaryFlatSize);
  }
  return totalSize;
}

uint64_t EffectiveSizeEstimator::scanVector(
    const bytedance::bolt::BaseVector* vec,
    uint64_t& estimatedFlatSize) const {
  auto kind = vec->typeKind();

  if (kind == bytedance::bolt::TypeKind::VARCHAR ||
      kind == bytedance::bolt::TypeKind::VARBINARY) {
    auto* column = vec->asFlatVector<bytedance::bolt::StringView>();
    if (!column) {
      return 0;
    }
    estimatedFlatSize += vec->estimateFlatSize();
    uint64_t size = vec->size() * sizeof(bytedance::bolt::StringView);
    for (auto i = 0; i < vec->size(); ++i) {
      if (!column->isNullAt(i)) {
        size += column->valueAt(i).size();
      }
    }
    return size;
  }

  if (kind == bytedance::bolt::TypeKind::ARRAY) {
    auto* array = vec->asUnchecked<bytedance::bolt::ArrayVector>();
    // Account for array's own overhead: nulls, offsets, sizes.
    auto overhead = vec->retainedSize() + array->offsets()->capacity() +
        array->sizes()->capacity();
    estimatedFlatSize += overhead;
    return overhead + scanVector(array->elements().get(), estimatedFlatSize);
  }

  if (kind == bytedance::bolt::TypeKind::MAP) {
    auto* map = vec->asUnchecked<bytedance::bolt::MapVector>();
    // Account for map's own overhead: nulls, offsets, sizes.
    auto overhead = vec->retainedSize() + map->offsets()->capacity() +
        map->sizes()->capacity();
    estimatedFlatSize += overhead;
    uint64_t size = overhead;
    size += scanVector(map->mapKeys().get(), estimatedFlatSize);
    size += scanVector(map->mapValues().get(), estimatedFlatSize);
    return size;
  }

  if (kind == bytedance::bolt::TypeKind::ROW) {
    auto* row = vec->asUnchecked<bytedance::bolt::RowVector>();
    // Account for row's own overhead: nulls.
    auto overhead = vec->retainedSize();
    estimatedFlatSize += overhead;
    uint64_t size = overhead;
    for (auto i = 0; i < row->childrenSize(); ++i) {
      if (typeContainsStringView(row->childAt(i)->type())) {
        size += scanVector(row->childAt(i).get(), estimatedFlatSize);
      }
    }
    return size;
  }

  return 0;
}

} // namespace bytedance::bolt::shuffle::sparksql
