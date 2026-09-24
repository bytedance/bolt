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

#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::shuffle::sparksql {

// Integration boundary for logical Spark rows. Cell itself only sees primitive
// columns and, when needed, one final binary containing all complex fields.
class CellShuffleTypeAdapter {
 public:
  explicit CellShuffleTypeAdapter(RowTypePtr logicalType);

  static bool isSupported(const TypePtr& type);

  const RowTypePtr& physicalType() const {
    return physicalType_;
  }

  bool hasComplexColumns() const {
    return !complexColumns_.empty();
  }

  // Converts a bounded slice and advances offset. Simple columns share their
  // input storage. Without complex columns returns the original batch.
  RowVectorPtr encodeNext(
      const RowVectorPtr& input,
      vector_size_t& offset,
      memory::MemoryPool* pool) const;

  RowVectorPtr decode(const RowVectorPtr& input, memory::MemoryPool* pool)
      const;

  // Restores a bounded part of a physical payload, advancing offset only after
  // success. The caller retains input until every row has been consumed.
  RowVectorPtr decodeNext(
      const RowVectorPtr& input,
      vector_size_t& offset,
      memory::MemoryPool* pool) const;

  // Implementation bounds, independent of Cell's wire representation.
  // Actual allocations use the caller's task pool and may fail sooner.
  static constexpr vector_size_t kMaxBatchRows = 1024;
  static constexpr uint64_t kTargetBatchBytes = 8ULL << 20;
  static constexpr uint64_t kMaxRowBytes = 64ULL << 20;
  static constexpr uint64_t kMaxDecodedBytes = 1ULL << 30;

 private:
  RowVectorPtr decodeBatch(
      const RowVectorPtr& input,
      vector_size_t& offset,
      vector_size_t maxRows,
      uint64_t maxBytes,
      memory::MemoryPool* pool) const;

  RowTypePtr logicalType_;
  RowTypePtr physicalType_;
  RowTypePtr complexType_;
  std::vector<uint32_t> retainedColumns_;
  std::vector<uint32_t> complexColumns_;
};

} // namespace bytedance::bolt::shuffle::sparksql
