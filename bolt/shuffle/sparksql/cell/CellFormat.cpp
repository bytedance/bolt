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

#include "bolt/shuffle/sparksql/cell/CellFormat.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

// static
CellLayout CellLayout::create(const RowTypePtr& rowType) {
  // No column at all is a pid-only shuffle: a layout without wire columns,
  // whose payloads are headers only.
  for (const auto& child : rowType->children()) {
    BOLT_CHECK(
        isSupportedType(child),
        "CellLayout: unsupported row type {}",
        rowType->toString());
  }
  BOLT_CHECK_LE(rowType->size(), UINT16_MAX, "too many Cell columns");
  CellLayout layout;
  layout.rowType_ = rowType;
  layout.numColumns_ = static_cast<uint32_t>(rowType->size());
  layout.firstStream_.reserve(layout.numColumns_);
  for (uint32_t col = 0; col < layout.numColumns_; ++col) {
    const auto kind = rowType->childAt(col)->kind();
    layout.firstStream_.push_back(
        static_cast<uint32_t>(layout.streams_.size()));
    switch (kind) {
      case TypeKind::BOOLEAN:
      case TypeKind::TINYINT:
      case TypeKind::HUGEINT:
      case TypeKind::REAL:
      case TypeKind::DOUBLE:
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kRawFixed});
        break;
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kEncoded});
        break;
      case TypeKind::TIMESTAMP:
        // The full 16-byte value: Seconds stream first, Nanos stream second,
        // both encoded as Bigint.
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kEncoded});
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kEncoded});
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        // Length/Index stream first, Data stream second.
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kEncoded});
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kStringData});
        break;
      default:
        BOLT_UNREACHABLE();
    }
  }
  return layout;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
