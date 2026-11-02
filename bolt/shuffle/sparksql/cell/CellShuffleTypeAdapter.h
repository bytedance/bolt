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

/// Packs a row type's ARRAY, MAP and ROW columns into one trailing VARBINARY
/// column of CompactRows, and unpacks them. Used when nested types are not
/// shredded (CellShuffleOptions::nativeNestedTypes off, or a schema past
/// the nested layout's column limit).
class CellShuffleTypeAdapter {
 public:
  /// Whether every leaf of `type`, at any depth of ARRAY, MAP and ROW, is a
  /// type a Cell column carries: the schemas both Cell layouts take.
  static bool isSupported(const TypePtr& type);

  explicit CellShuffleTypeAdapter(RowTypePtr logicalType);

  const RowTypePtr& physicalType() const {
    return physicalType_;
  }

  bool hasComplexColumns() const {
    return !complexColumns_.empty();
  }

  /// The physical batch of `input`, row for row: the simple columns,
  /// sharing their storage, then one binary column holding each row's
  /// complex fields as a CompactRow. Without complex columns, `input`
  /// itself.
  RowVectorPtr encode(const RowVectorPtr& input, memory::MemoryPool* pool)
      const;

  /// The logical batch of a physical batch encode() made, row for row: the
  /// simple columns shared, the complex ones deserialized.
  RowVectorPtr decode(const RowVectorPtr& input, memory::MemoryPool* pool)
      const;

 private:
  RowTypePtr logicalType_;
  RowTypePtr physicalType_;
  RowTypePtr complexType_;
  std::vector<uint32_t> retainedColumns_;
  std::vector<uint32_t> complexColumns_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
