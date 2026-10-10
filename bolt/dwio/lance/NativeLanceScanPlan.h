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

#include <unordered_map>

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

  const NativeLanceColumnReader& filterColumnReader(
      uint32_t fileColumnIndex) const {
    return *filterColumnReaders_.at(fileColumnIndex);
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
      std::unordered_map<uint32_t, std::unique_ptr<NativeLanceColumnReader>>
          filterColumnReaders,
      std::vector<std::pair<uint64_t, uint64_t>> rowRanges,
      uint64_t estimatedBytesPerRow);

  std::unique_ptr<NativeLanceRootColumnReader> rootColumnReader_;
  std::unordered_map<uint32_t, std::unique_ptr<NativeLanceColumnReader>>
      filterColumnReaders_;
  std::vector<std::pair<uint64_t, uint64_t>> rowRanges_;
  uint64_t estimatedBytesPerRow_;
};

} // namespace bytedance::bolt::lance::reader
