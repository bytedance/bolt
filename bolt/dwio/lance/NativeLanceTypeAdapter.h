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

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "bolt/type/Type.h"

namespace bytedance::bolt::lance::reader {

/// Resolves Lance logical and Arrow extension types to layout-compatible Bolt
/// types. Returned types must have the same TypeKind as storageType so the
/// native decoder can populate them without an Arrow conversion round trip.
class NativeLanceTypeAdapter {
 public:
  struct Request {
    std::string_view fieldName;
    std::string_view logicalType;
    std::string_view extensionName;
    std::string_view extensionMetadata;
    TypePtr storageType;
  };

  virtual ~NativeLanceTypeAdapter() = default;

  /// Returns nullopt when the adapter does not recognize the semantic type.
  virtual std::optional<TypePtr> resolve(const Request& request) const = 0;
};

/// Built-in adapter that preserves the unit/timezone/extension identity of
/// Lance TIME, zoned TIMESTAMP, JSONB, and bfloat16 fields.
std::shared_ptr<const NativeLanceTypeAdapter> defaultNativeLanceTypeAdapter();

/// Returns a stable semantic descriptor for types produced by the built-in
/// adapter, or nullopt for ordinary Bolt types and application adapters.
std::optional<std::string_view> nativeLanceSemanticType(const TypePtr& type);

/// Extracts a dictionary's value logical type from
/// dict:<value-type>:<index-type>:<ordered>. Parsing is anchored from the
/// right because value types such as timestamps and decimals contain colons.
std::string_view nativeLanceDictionaryValueLogicalType(
    std::string_view logicalType);

} // namespace bytedance::bolt::lance::reader
