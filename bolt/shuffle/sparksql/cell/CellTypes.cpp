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

#include "bolt/shuffle/sparksql/cell/CellTypes.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

// static
CellLayout CellLayout::create(const RowTypePtr& rowType) {
  BOLT_CHECK(
      isSupportedRowType(rowType),
      "CellLayout: unsupported row type {}",
      rowType->toString());
  CellLayout layout;
  layout.rowType_ = rowType;
  layout.numColumns_ = static_cast<uint32_t>(rowType->size());
  layout.columnStream_.reserve(layout.numColumns_);
  layout.isString_.reserve(layout.numColumns_);
  for (uint32_t col = 0; col < layout.numColumns_; ++col) {
    const auto kind = rowType->childAt(col)->kind();
    layout.columnStream_.push_back(
        static_cast<uint32_t>(layout.streams_.size()));
    switch (kind) {
      case TypeKind::TINYINT:
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kRawFixed, 1, true});
        layout.isString_.push_back(false);
        break;
      case TypeKind::SMALLINT:
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kEncoded, 2, true});
        layout.isString_.push_back(false);
        break;
      case TypeKind::INTEGER:
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kEncoded, 4, true});
        layout.isString_.push_back(false);
        break;
      case TypeKind::BIGINT:
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kEncoded, 8, true});
        layout.isString_.push_back(false);
        break;
      case TypeKind::REAL:
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kRawFixed, 4, false});
        layout.isString_.push_back(false);
        break;
      case TypeKind::DOUBLE:
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kRawFixed, 8, false});
        layout.isString_.push_back(false);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        // Length/Index stream first, Data stream second (spec section 1.4).
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kEncoded, 8, true});
        layout.streams_.push_back(
            {static_cast<uint16_t>(col), StreamKind::kStringData, 0, false});
        layout.isString_.push_back(true);
        break;
      default:
        BOLT_UNREACHABLE();
    }
  }
  return layout;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
