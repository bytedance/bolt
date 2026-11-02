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

inline bool supportsMapKeyEquality(const Type& type) {
  if (type.kind() == TypeKind::REAL || type.kind() == TypeKind::DOUBLE) {
    return std::string_view(type.name()) == type.kindName();
  }
  // Java byte arrays and composite keys have representation-dependent equality.
  return type.kind() != TypeKind::ROW && type.kind() != TypeKind::VARBINARY &&
      supportsRowEquality(type);
}

inline bool supportsMapEquality(const Type& type);

inline bool supportsMapValueEquality(const Type& type) {
  switch (type.kind()) {
    case TypeKind::ARRAY:
      return supportsMapValueEquality(*type.childAt(0));
    case TypeKind::ROW:
      for (const auto& child : type.asRow().children()) {
        if (!supportsMapValueEquality(*child)) {
          return false;
        }
      }
      return true;
    case TypeKind::MAP:
      return supportsMapEquality(type);
    case TypeKind::VARBINARY:
      return std::string_view(type.name()) == type.kindName();
    default:
      return supportsMapKeyEquality(type);
  }
}

inline bool supportsMapEquality(const Type& type) {
  return type.isMap() && supportsMapKeyEquality(*type.childAt(0)) &&
      supportsMapValueEquality(*type.childAt(1));
}

} // namespace detail

/// Flink compares null ROW fields as values. Floating-point and collection
/// fields need separate policies and are deliberately excluded here.
template <typename T, bool negate = false>
struct RowEqualFunction
    : EqFunction<T, CompareFlags::NullHandlingMode::kNullAsValue> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  bool call(
      bool& result,
      const arg_type<Generic<T1>>& left,
      const arg_type<Generic<T1>>& right) {
    const auto valid =
        EqFunction<T, CompareFlags::NullHandlingMode::kNullAsValue>::call(
            result, left, right);
    if constexpr (negate) {
      if (valid) {
        result = !result;
      }
    }
    return valid;
  }

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

template <typename T>
using RowNotEqualFunction = RowEqualFunction<T, true>;

} // namespace bytedance::bolt::functions::flinksql
