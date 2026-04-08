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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/connectors/hive/HivePartitionUtil.h"
#include "bolt/type/DecimalUtil.h"
namespace bytedance::bolt::connector::hive {

#define PARTITION_TYPE_DISPATCH(TEMPLATE_FUNC, typeKind, ...)               \
  [&]() {                                                                   \
    switch (typeKind) {                                                     \
      case TypeKind::BOOLEAN:                                               \
      case TypeKind::TINYINT:                                               \
      case TypeKind::SMALLINT:                                              \
      case TypeKind::INTEGER:                                               \
      case TypeKind::BIGINT:                                                \
      case TypeKind::HUGEINT:                                               \
      case TypeKind::TIMESTAMP:                                             \
      case TypeKind::VARCHAR:                                               \
      case TypeKind::VARBINARY:                                             \
        return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(                           \
            TEMPLATE_FUNC, typeKind, __VA_ARGS__);                          \
      default:                                                              \
        BOLT_UNSUPPORTED(                                                   \
            "Unsupported partition type: {}", mapTypeKindToName(typeKind)); \
    }                                                                       \
  }()

namespace {
template <typename T>
inline std::string makePartitionValueString(T value) {
  return folly::to<std::string>(value);
}

template <>
inline std::string makePartitionValueString(bool value) {
  return value ? "true" : "false";
}

template <>
inline std::string makePartitionValueString(Timestamp value) {
  return value.toString();
}

template <>
inline std::string makePartitionValueString(int64_t value) {
  return folly::to<std::string>(value);
}

template <>
inline std::string makePartitionValueString(int128_t value) {
  return std::to_string(value);
}

template <TypeKind Kind>
std::pair<std::string, std::string> makePartitionKeyValueString(
    const BaseVector* partitionVector,
    vector_size_t row,
    const std::string& name,
    const TypePtr& type,
    bool isDate) {
  using T = typename TypeTraits<Kind>::NativeType;
  if (partitionVector->as<SimpleVector<T>>()->isNullAt(row)) {
    return std::make_pair(name, "");
  }
  if (isDate) {
    return std::make_pair(
        name,
        DATE()->toString(
            partitionVector->as<SimpleVector<int32_t>>()->valueAt(row)));
  }
  if constexpr (Kind == TypeKind::BIGINT || Kind == TypeKind::HUGEINT) {
    if (type->isDecimal()) {
      return std::make_pair(
          name,
          DecimalUtil::toString(
              partitionVector->as<SimpleVector<T>>()->valueAt(row), type));
    }
  }
  return std::make_pair(
      name,
      makePartitionValueString(
          partitionVector->as<SimpleVector<T>>()->valueAt(row)));
}

} // namespace

std::vector<std::pair<std::string, std::string>> extractPartitionKeyValues(
    const RowVectorPtr& partitionsVector,
    vector_size_t row) {
  std::vector<std::pair<std::string, std::string>> partitionKeyValues;
  for (auto i = 0; i < partitionsVector->childrenSize(); i++) {
    partitionKeyValues.push_back(PARTITION_TYPE_DISPATCH(
        makePartitionKeyValueString,
        partitionsVector->childAt(i)->typeKind(),
        partitionsVector->childAt(i)->loadedVector(),
        row,
        asRowType(partitionsVector->type())->nameOf(i),
        partitionsVector->childAt(i)->type(),
        partitionsVector->childAt(i)->type()->isDate()));
  }
  return partitionKeyValues;
}

} // namespace bytedance::bolt::connector::hive
