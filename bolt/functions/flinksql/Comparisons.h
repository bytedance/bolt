/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <string_view>

#include "bolt/core/QueryConfig.h"
#include "bolt/functions/prestosql/Comparisons.h"

namespace bytedance::bolt::functions::flinksql {
namespace detail {

inline bool supportsRowEquality(const Type& type) {
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
      return type.isDecimal() || type.isDate() || type.isIntervalDayTime() ||
          type.isIntervalYearMonth() ||
          std::string_view(type.name()) == type.kindName();
    case TypeKind::ROW:
      for (const auto& child : type.asRow().children()) {
        if (!supportsRowEquality(*child)) {
          return false;
        }
      }
      return true;
    default:
      return false;
  }
}

} // namespace detail

/// Flink compares null ROW fields as values. Floating-point and collection
/// fields need separate policies and are deliberately excluded here.
template <typename T>
struct RowEqualFunction
    : EqFunction<T, CompareFlags::NullHandlingMode::kNullAsValue> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig&,
      const arg_type<Generic<T1>>*,
      const arg_type<Generic<T1>>*) {
    BOLT_USER_CHECK(
        inputTypes[0]->isRow() && detail::supportsRowEquality(*inputTypes[0]),
        "Unsupported Flink ROW equality type: {}",
        inputTypes[0]->toString());
  }
};

} // namespace bytedance::bolt::functions::flinksql
