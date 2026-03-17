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

#pragma once

#include <cstdint>
#include <vector>

#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::shuffle::sparksql {

/// Corrects estimateFlatSize() underestimation for binary columns by
/// scanning actual StringView sizes. Shared string buffers (e.g. Parquet
/// reader's decoded pages) cause estimateFlatSize() to report sizes far
/// below reality. Uses adaptive sampling with a fast path when the
/// correction ratio is small.
class EffectiveSizeEstimator {
 public:
  static constexpr double kFullScanRatioThreshold = 2.0;
  static constexpr uint64_t kFullScanSizeThreshold = 32 << 20; // 32MB

  EffectiveSizeEstimator() = default;

  uint64_t estimate(const bytedance::bolt::RowVectorPtr& rv);

  double cachedRatio() const {
    return cachedRatio_;
  }

 private:
  static bool typeContainsStringView(const bytedance::bolt::TypePtr& type);

  void detectBinaryColumns(const bytedance::bolt::RowVectorPtr& rv);

  uint64_t computeActualBinarySize(
      const bytedance::bolt::RowVectorPtr& rv,
      uint64_t& estimatedBinaryFlatSize) const;

  /// Recursively scan a vector tree, summing actual sizes of all leaf
  /// FlatVector<StringView> nodes. Adds each scanned leaf's
  /// estimateFlatSize() to @p estimatedFlatSize.
  uint64_t scanVector(
      const bytedance::bolt::BaseVector* vec,
      uint64_t& estimatedFlatSize) const;

  double cachedRatio_{1.0};
  std::vector<uint32_t> binaryColumnIndices_;
  bool initialized_{false};
  bool alwaysFullScan_{false};
};

} // namespace bytedance::bolt::shuffle::sparksql
