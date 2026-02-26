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

#include <limits>
#include "bolt/type/Type.h"
namespace bytedance::bolt::type {

class NumericTypeUtils {
 public:
  /// Returns true if the type is an integral type (TINYINT, SMALLINT, INTEGER,
  /// BIGINT)
  static constexpr bool isIntegralType(TypeKind kind) {
    return kind == TypeKind::TINYINT || kind == TypeKind::SMALLINT ||
        kind == TypeKind::INTEGER || kind == TypeKind::BIGINT;
  }

  /// Returns true if the type is a floating point type (REAL, DOUBLE)
  static constexpr bool isFloatingPointType(TypeKind kind) {
    return kind == TypeKind::REAL || kind == TypeKind::DOUBLE;
  }

  /// Returns true if the kind is either integral or float.
  static constexpr bool isNumericType(TypeKind kind) {
    return isFloatingPointType(kind) || isIntegralType(kind);
  }
};

} // namespace bytedance::bolt::type
