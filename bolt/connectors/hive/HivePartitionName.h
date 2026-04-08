/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include <string>
#include <utility>
#include <vector>

#include <fmt/core.h>

#include <folly/CPortability.h>

#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/SimpleVector.h"

namespace bytedance::bolt::connector::hive {

class HivePartitionName {
 public:
  template <typename T>
  FOLLY_ALWAYS_INLINE static std::string toName(T value, const TypePtr& type) {
    return fmt::to_string(value);
  }

  static std::string toName(int32_t value, const TypePtr& type);
  static std::string toName(int64_t value, const TypePtr& type);
  static std::string toName(int128_t value, const TypePtr& type);
  static std::string toName(Timestamp value, const TypePtr& type);

  template <typename F>
  static std::vector<std::pair<std::string, std::string>> partitionKeyValues(
      uint32_t partitionId,
      const RowVectorPtr& partitionValues,
      const std::string& nullValueString,
      const F& toPartitionName);

  static std::string partitionName(
      uint32_t partitionId,
      const RowVectorPtr& partitionValues,
      bool partitionKeyAsLowerCase);
};

namespace detail {

template <TypeKind Kind, typename F>
std::string makePartitionKeyValueString(
    const BaseVector& partitionVector,
    vector_size_t row,
    const TypePtr& type,
    int columnIndex,
    const F& toPartitionName) {
  using T = typename TypeTraits<Kind>::NativeType;
  return toPartitionName(
      partitionVector.as<SimpleVector<T>>()->valueAt(row), type, columnIndex);
}

} // namespace detail

template <typename F>
std::vector<std::pair<std::string, std::string>>
HivePartitionName::partitionKeyValues(
    uint32_t partitionId,
    const RowVectorPtr& partitionValues,
    const std::string& nullValueString,
    const F& toPartitionName) {
  std::vector<std::pair<std::string, std::string>> partitionKeyValuePairs;
  for (auto i = 0; i < partitionValues->childrenSize(); i++) {
    const auto& child = partitionValues->childAt(i);
    const auto& name = partitionValues->type()->asRow().nameOf(i);
    if (child->isNullAt(partitionId)) {
      partitionKeyValuePairs.emplace_back(
          std::make_pair(name, nullValueString));
      continue;
    }

    partitionKeyValuePairs.emplace_back(std::make_pair(name, [&]() {
      const auto* partitionVector = child->loadedVector();
      switch (child->typeKind()) {
        case TypeKind::BOOLEAN:
          return detail::makePartitionKeyValueString<TypeKind::BOOLEAN>(
              *partitionVector, partitionId, child->type(), i, toPartitionName);
        case TypeKind::TINYINT:
          return detail::makePartitionKeyValueString<TypeKind::TINYINT>(
              *partitionVector, partitionId, child->type(), i, toPartitionName);
        case TypeKind::SMALLINT:
          return detail::makePartitionKeyValueString<TypeKind::SMALLINT>(
              *partitionVector, partitionId, child->type(), i, toPartitionName);
        case TypeKind::INTEGER:
          return detail::makePartitionKeyValueString<TypeKind::INTEGER>(
              *partitionVector, partitionId, child->type(), i, toPartitionName);
        case TypeKind::BIGINT:
          return detail::makePartitionKeyValueString<TypeKind::BIGINT>(
              *partitionVector, partitionId, child->type(), i, toPartitionName);
        case TypeKind::HUGEINT:
          return detail::makePartitionKeyValueString<TypeKind::HUGEINT>(
              *partitionVector, partitionId, child->type(), i, toPartitionName);
        case TypeKind::VARCHAR:
          return detail::makePartitionKeyValueString<TypeKind::VARCHAR>(
              *partitionVector, partitionId, child->type(), i, toPartitionName);
        case TypeKind::VARBINARY:
          return detail::makePartitionKeyValueString<TypeKind::VARBINARY>(
              *partitionVector, partitionId, child->type(), i, toPartitionName);
        case TypeKind::TIMESTAMP:
          return detail::makePartitionKeyValueString<TypeKind::TIMESTAMP>(
              *partitionVector, partitionId, child->type(), i, toPartitionName);
        default:
          BOLT_UNSUPPORTED(
              "Unsupported partition type: {}",
              mapTypeKindToName(child->typeKind()));
      }
    }()));
  }
  return partitionKeyValuePairs;
}

} // namespace bytedance::bolt::connector::hive
