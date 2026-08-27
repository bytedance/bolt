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

#include "bolt/type/Type.h"

namespace bytedance::bolt::parquet {

inline bool isReaderCastFilterMismatch(
    const TypePtr& fileType,
    const TypePtr& requestedType) {
  const auto requestedKind = requestedType->kind();
  switch (fileType->kind()) {
    case TypeKind::REAL:
      // VARCHAR filters cannot be evaluated against REAL statistics.
      return requestedType->isVarchar();
    case TypeKind::DOUBLE:
      // VARCHAR and BIGINT filters cannot be evaluated against DOUBLE
      // statistics.
      return requestedType->isVarchar() || requestedKind == TypeKind::BIGINT;
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
      // DOUBLE filters cannot be evaluated against integer statistics.
      return requestedKind == TypeKind::DOUBLE;
    case TypeKind::INTEGER:
      // Reader casts change the interpretation of DATE-to-VARCHAR and
      // integer-to-DOUBLE filters.
      return requestedKind == TypeKind::DOUBLE ||
          (fileType->isDate() && requestedType->isVarchar());
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
      // Decimal type changes can alter the scale or native storage width.
      return fileType->isDecimal() && requestedType->isDecimal() &&
          !fileType->equivalent(*requestedType);
    default:
      return false;
  }
}

} // namespace bytedance::bolt::parquet
