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
#include <memory>
#include <string_view>
#include <vector>

#include <folly/Range.h>

#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"

// Dense row serializer — sibling to CompactRow / UnsafeRowFast, but
// column-batched (processes all rows at once) rather than row-at-a-time, so it
// exposes only batch operations (no single-row rowSize(i) / serialize(i)).
//
// The wire is the "dense", no-waste layout
// 1. variable-length (varint) values
// 2. nulls fused into the structure bytes (no null bitmap)
// 3. no alignment padding,
// 4. level-hoisted nesting.
// The grammar is documented at the top of DenseRow.cpp.
//
// Usage (mirrors CompactRow):
//   DenseRow rows(rowVector);                 // builds plan + sizes (once)
//   auto offsets = to_offsets(rows.rowSizes());
//   rows.serialize(base, offsets);            // write all rows at the offsets
//
//   auto rv = DenseRow::deserialize(ranges, rowType, pool);
namespace bytedance::bolt::row {

class DenseRow {
 public:
  explicit DenseRow(const RowVectorPtr& vector);
  DenseRow(DenseRow&&) noexcept;
  DenseRow& operator=(DenseRow&&) noexcept;
  ~DenseRow();

  vector_size_t numRows() const;

  size_t rowSizeAt(vector_size_t index) const;

  // Per-row encoded byte counts (all rows). DenseRow precomputes these in its
  // size pass, so this bulk accessor is free; rowSizeAt() indexes into it.
  const std::vector<size_t>& rowSizes() const;

  // Sum of rowSizes()
  size_t totalSize() const;

  // Serialize every row into `base + offsets[r]`. `offsets.size()` must equal
  // numRows(); row r writes exactly rowSizes()[r] bytes.
  void serialize(uint8_t* base, folly::Range<const size_t*> offsets) const;

  // Serialize rows in [offset, offset + size) into `base + offsets[r -
  // offset]`. This reuses the size/write plan built by the constructor.
  void serialize(
      vector_size_t offset,
      vector_size_t size,
      uint8_t* base,
      folly::Range<const size_t*> offsets) const;

  // Reconstruct a flat RowVector of `rowType` from pre-split per-row byte
  // ranges (one entry per row). Inverse of serialize().
  static RowVectorPtr deserialize(
      const std::vector<std::string_view>& data,
      const RowTypePtr& rowType,
      memory::MemoryPool* pool);

 private:
  struct State;
  std::unique_ptr<State> state_;
};

} // namespace bytedance::bolt::row
