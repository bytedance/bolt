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

#include "bolt/dwio/lance/NativeLanceColumnReader.h"
#include "bolt/dwio/lance/NativeLanceFileContext.h"

namespace bytedance::bolt::lance::reader {

/// Immutable projection and physical-column plan for one row reader.
class NativeLanceScanPlan {
 public:
  static std::unique_ptr<NativeLanceScanPlan> build(
      const NativeLanceFileContext& context,
      const dwio::common::RowReaderOptions& options);

  const NativeLanceRootColumnReader& rootColumnReader() const {
    return *rootColumnReader_;
  }

  const RowTypePtr& outputType() const {
    return rootColumnReader_->outputType();
  }

  const std::vector<uint32_t>& requiredColumns() const {
    return requiredColumns_;
  }

  const std::vector<std::pair<uint64_t, uint64_t>>& rowRanges() const {
    return rowRanges_;
  }

  uint64_t estimatedBytesPerRow() const {
    return estimatedBytesPerRow_;
  }

 private:
  NativeLanceScanPlan(
      std::unique_ptr<NativeLanceRootColumnReader> rootColumnReader,
      std::vector<uint32_t> requiredColumns,
      std::vector<std::pair<uint64_t, uint64_t>> rowRanges,
      uint64_t estimatedBytesPerRow);

  std::unique_ptr<NativeLanceRootColumnReader> rootColumnReader_;
  std::vector<uint32_t> requiredColumns_;
  std::vector<std::pair<uint64_t, uint64_t>> rowRanges_;
  uint64_t estimatedBytesPerRow_;
};

} // namespace bytedance::bolt::lance::reader
