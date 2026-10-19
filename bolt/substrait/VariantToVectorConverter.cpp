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

#include "bolt/substrait/VariantToVectorConverter.h"
#include "Variant.h"
#include "bolt/buffer/Buffer.h"
#include "bolt/common/base/BitUtil.h"
#include "bolt/common/memory/AllocationPool.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorUtil.h"
namespace bytedance::bolt::substrait {

namespace {
template <TypeKind KIND>
VectorPtr setVectorFromVariantsByKind(
    const std::vector<bolt::variant>& values,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<KIND>::NativeType;

  auto flatVector =
      BaseVector::create<FlatVector<T>>(type, values.size(), pool);

  for (vector_size_t i = 0; i < values.size(); i++) {
    if (values[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, values[i].value<T>());
    }
  }
  return flatVector;
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::VARBINARY>(
    const std::vector<bolt::variant>& values,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  auto flatVector =
      BaseVector::create<FlatVector<StringView>>(type, values.size(), pool);

  for (vector_size_t i = 0; i < values.size(); i++) {
    if (values[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      if (values[i].kind() == TypeKind::VARBINARY) {
        flatVector->set(i, StringView(values[i].value<Varbinary>()));
      } else if (values[i].kind() == TypeKind::VARCHAR) {
        // Permit string literal to populate binary columns.
        flatVector->set(i, StringView(values[i].value<Varchar>()));
      } else {
        BOLT_FAIL(
            "Unsupported variant kind {} for VARBINARY vector",
            static_cast<int>(values[i].kind()));
      }
    }
  }
  return flatVector;
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::VARCHAR>(
    const std::vector<bolt::variant>& values,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  auto flatVector =
      BaseVector::create<FlatVector<StringView>>(type, values.size(), pool);

  for (vector_size_t i = 0; i < values.size(); i++) {
    if (values[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, StringView(values[i].value<Varchar>()));
    }
  }
  return flatVector;
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::MAP>(
    const std::vector<bolt::variant>& items,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  // Create a key vector and a values vector
  std::vector<bolt::variant> keyItems;
  std::vector<bolt::variant> valueItems;

  vector_size_t totalPairs = 0;

  // Create offsets and sizes buffers
  auto offsetsBuffer =
      AlignedBuffer::allocate<vector_size_t>(items.size() + 1, pool);
  auto sizesBuffer = AlignedBuffer::allocate<vector_size_t>(items.size(), pool);
  auto nulls = allocateNulls(items.size(), pool);

  auto offsets = offsetsBuffer->asMutable<vector_size_t>();
  auto sizes = sizesBuffer->asMutable<vector_size_t>();

  // Each item is a map, so we need to extract keys and values from each map
  for (vector_size_t i = 0; i < items.size(); i++) {
    const auto& item = items[i];
    offsets[i] = totalPairs;
    if (item.isNull()) {
      sizes[i] = 0;
      bits::setNull(
          reinterpret_cast<uint64_t*>(nulls->asMutable<uint8_t>()),
          static_cast<int32_t>(i));
    } else {
      // Extract map from variant using the map() method
      const auto& map = item.map();
      sizes[i] = map.size();
      for (const auto& kv : map) {
        keyItems.push_back(kv.first);
        valueItems.push_back(kv.second);
      }
      totalPairs += map.size();
    }
  }
  offsets[items.size()] = totalPairs;

  // Set keyType and valueType.
  const auto& mapType = type->asMap();
  auto keys = setVectorFromVariants(mapType.keyType(), keyItems, pool);
  auto values = setVectorFromVariants(mapType.valueType(), valueItems, pool);

  // Return a new MapVector containing keys and values.
  return std::make_shared<MapVector>(
      pool,
      type,
      nulls,
      items.size(),
      offsetsBuffer,
      sizesBuffer,
      keys,
      values);
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::ARRAY>(
    const std::vector<bolt::variant>& items,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  // Set elementType.
  const auto& arrayType = type->asArray();
  TypePtr elementType = arrayType.elementType();

  // Extract elements from each array
  std::vector<bolt::variant> elementItems;

  vector_size_t totalElements = 0;

  // Create offsets and lengths buffers
  auto offsetsBuffer =
      AlignedBuffer::allocate<vector_size_t>(items.size() + 1, pool);
  auto lengthsBuffer =
      AlignedBuffer::allocate<vector_size_t>(items.size(), pool);
  auto nulls = allocateNulls(items.size(), pool);

  auto offsets = offsetsBuffer->asMutable<vector_size_t>();
  auto lengths = lengthsBuffer->asMutable<vector_size_t>();

  for (vector_size_t i = 0; i < items.size(); i++) {
    const auto& item = items[i];
    offsets[i] = totalElements;
    if (item.isNull()) {
      lengths[i] = 0;
      bits::setNull(
          reinterpret_cast<uint64_t*>(nulls->asMutable<uint8_t>()),
          static_cast<int32_t>(i));
    } else {
      // Extract array from variant using the array() method
      const auto& array = item.array();
      lengths[i] = array.size();
      for (const auto& element : array) {
        elementItems.push_back(element);
      }
      totalElements += array.size();
    }
  }
  offsets[items.size()] = totalElements;

  auto elements = setVectorFromVariants(elementType, elementItems, pool);

  // Return a new ArrayVector containing elements.
  return std::make_shared<ArrayVector>(
      pool, type, nulls, items.size(), offsetsBuffer, lengthsBuffer, elements);
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::ROW>(
    const std::vector<bolt::variant>& items,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> children;
  auto nulls = allocateNulls(items.size(), pool);
  children.reserve(type->size());
  for (size_t rowIdx = 0; rowIdx < items.size(); ++rowIdx) {
    if (items[rowIdx].isNull()) {
      bits::setNull(nulls->asMutable<uint64_t>(), rowIdx);
    } else {
      BOLT_CHECK_EQ(items[rowIdx].row().size(), type->size());
    }
  }
  for (size_t fieldIdx = 0; fieldIdx < type->size(); ++fieldIdx) {
    TypePtr fieldType = type->childAt(fieldIdx);
    std::vector<bolt::variant> fieldItems;
    fieldItems.reserve(items.size());
    for (size_t rowIdx = 0; rowIdx < items.size(); ++rowIdx) {
      fieldItems.push_back(
          items[rowIdx].isNull() ? variant::null(fieldType->kind())
                                 : items[rowIdx].row()[fieldIdx]);
    }

    // Create vector for this field
    children.push_back(setVectorFromVariants(fieldType, fieldItems, pool));
  }

  // Return a new RowVector containing children.
  return std::make_shared<RowVector>(pool, type, nulls, items.size(), children);
}
} // namespace

VectorPtr setVectorFromVariants(
    const TypePtr& type,
    const std::vector<bolt::variant>& values,
    memory::MemoryPool* pool) {
  switch (type->kind()) {
    case TypeKind::MAP:
      return setVectorFromVariantsByKind<TypeKind::MAP>(values, type, pool);
    case TypeKind::ARRAY:
      return setVectorFromVariantsByKind<TypeKind::ARRAY>(values, type, pool);
    case TypeKind::ROW:
      return setVectorFromVariantsByKind<TypeKind::ROW>(values, type, pool);
    default:
      return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          setVectorFromVariantsByKind, type->kind(), values, type, pool);
  }
}
} // namespace bytedance::bolt::substrait
