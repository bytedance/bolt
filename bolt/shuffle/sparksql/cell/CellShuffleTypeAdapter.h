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

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Adapts a logical row type to a Cell physical row type: strips UNKNOWN
/// columns (restored as null constants on unpack) and packs ARRAY, MAP and
/// ROW columns into one trailing VARBINARY column of CompactRows.
class CellShuffleTypeAdapter {
 public:
  /// Whether every leaf of `type`, at any depth of ARRAY, MAP and ROW, is a
  /// supported Cell scalar type or UNKNOWN.
  static bool isSupported(const TypePtr& type);

  explicit CellShuffleTypeAdapter(RowTypePtr logicalType);

  const RowTypePtr& physicalType() const {
    return physicalType_;
  }

  bool hasComplexColumns() const {
    return !complexColumns_.empty();
  }

  bool hasUnknownColumns() const {
    return !unknownColumns_.empty();
  }

  bool needsAdaptation() const {
    return hasComplexColumns() || hasUnknownColumns();
  }

  /// The physical batch of `input`, row for row: the simple columns,
  /// sharing their storage, with UNKNOWN columns omitted and (when
  /// hasComplexColumns()) one trailing binary column holding each row's
  /// complex fields as a CompactRow. When !needsAdaptation(), returns `input`.
  RowVectorPtr pack(const RowVectorPtr& input, memory::MemoryPool* pool) const;

  /// The logical batch of a physical batch pack() made, row for row: the
  /// simple columns shared, complex columns deserialized, and UNKNOWN columns
  /// restored as null constants.
  RowVectorPtr unpack(const RowVectorPtr& input, memory::MemoryPool* pool)
      const;

 private:
  RowTypePtr logicalType_;
  RowTypePtr physicalType_;
  RowTypePtr packedType_;
  std::vector<uint32_t> simpleColumns_;
  std::vector<uint32_t> complexColumns_;
  std::vector<uint32_t> unknownColumns_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
