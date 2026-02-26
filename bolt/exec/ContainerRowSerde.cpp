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

#include "bolt/exec/ContainerRowSerde.h"

#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::exec {
namespace {

// Copy from vector to stream.
void serializeSwitch(
    const BaseVector& source,
    vector_size_t index,
    ByteOutputStream& out);

void serializeManySwitch(
    const BaseVector& vector,
    vector_size_t offset,
    vector_size_t size,
    ByteOutputStream& stream);

void serializeManySwitch(
    const BaseVector& vector,
    folly::Range<const vector_size_t*> indices,
    ByteOutputStream& stream);

template <TypeKind Kind>
void serializeOne(
    const BaseVector& vector,
    vector_size_t index,
    ByteOutputStream& stream) {
  using T = typename TypeTraits<Kind>::NativeType;
  stream.appendOne<T>(vector.asUnchecked<SimpleVector<T>>()->valueAt(index));
}

template <TypeKind Kind>
void serializeMany(
    const BaseVector& vector,
    vector_size_t offset,
    vector_size_t size,
    ByteOutputStream& stream) {
  if constexpr (
      Kind == TypeKind::TINYINT || Kind == TypeKind::SMALLINT ||
      Kind == TypeKind::INTEGER || Kind == TypeKind::BIGINT ||
      Kind == TypeKind::HUGEINT || Kind == TypeKind::REAL ||
      Kind == TypeKind::DOUBLE) {
    using T = typename TypeTraits<Kind>::NativeType;
    auto* simpleVector = vector.asUnchecked<SimpleVector<T>>();
    if (simpleVector->encoding() == VectorEncoding::Simple::FLAT) {
      auto flatVector = vector.asUnchecked<FlatVector<T>>();
      std::optional<uint64_t> nullCount =
          flatVector->getNullCount(offset, offset + size);
      if (nullCount.has_value()) {
        if (nullCount.value() == 0) {
          // all values is not null, just copy the values.
          // to avoid coredump caused by memory align for 16 bytes type such as
          // hugeint and decimal, we use int8_t to copy the values.
          const T* values = flatVector->rawValues() + offset;
          if constexpr (std::is_same_v<T, __int128>) {
            stream.append<int8_t>(folly::Range<const int8_t*>(
                reinterpret_cast<const int8_t*>(values),
                size * sizeof(T) / sizeof(int8_t)));
          } else {
            stream.append<T>(
                folly::Range<const T*>(flatVector->rawValues() + offset, size));
          }
        } else if (nullCount.value() != size) {
          // some values are null, need to copy the values.
          vector_size_t start = offset;
          vector_size_t count = 0;
          for (vector_size_t i = 0; i < size; ++i) {
            auto index = offset + i;
            if (flatVector->isNullAt(index)) {
              if (count > 0) {
                const T* values = flatVector->rawValues() + start;
                if constexpr (std::is_same_v<T, __int128>) {
                  stream.append<int8_t>(folly::Range<const int8_t*>(
                      reinterpret_cast<const int8_t*>(values),
                      count * sizeof(T) / sizeof(int8_t)));
                } else {
                  stream.append<T>(folly::Range<const T*>(
                      flatVector->rawValues() + start, count));
                }
                count = 0;
              }
            } else {
              if (count == 0) {
                start = index;
              }
              ++count;
            }
          }
          if (count > 0) {
            const T* values = flatVector->rawValues() + start;
            if constexpr (std::is_same_v<T, __int128>) {
              stream.append<int8_t>(folly::Range<const int8_t*>(
                  reinterpret_cast<const int8_t*>(values),
                  count * sizeof(T) / sizeof(int8_t)));
            } else {
              stream.append<T>(folly::Range<const T*>(
                  flatVector->rawValues() + start, count));
            }
          }
        }
        return;
      }
    } else if (simpleVector->encoding() == VectorEncoding::Simple::CONSTANT) {
      auto* constantVector = vector.asUnchecked<ConstantVector<T>>();
      if (!constantVector->isNullAt(0)) {
        if constexpr (std::is_same_v<T, __int128>) {
          HashStringAllocator hashStringAllocator(vector.pool());
          AlignedStlAllocator<T, sizeof(T)> allocator(&hashStringAllocator);
          std::vector<T, AlignedStlAllocator<T, sizeof(T)>> values(
              size, constantVector->valueAt(0), allocator);
          stream.append<T>(values);
        } else {
          std::vector<T> values(size, constantVector->valueAt(0));
          stream.append<T>(values);
        }
      }
      return;
    }
    for (auto i = 0; i < size; ++i) {
      auto index = offset + i;
      if (!vector.isNullAt(index)) {
        stream.appendOne<T>(simpleVector->valueAt(index));
      }
    }
  } else {
    for (auto i = 0; i < size; ++i) {
      auto index = offset + i;
      if (!vector.isNullAt(index)) {
        serializeOne<Kind>(vector, index, stream);
      }
    }
  }
}

template <TypeKind Kind>
void serializeMany(
    const BaseVector& vector,
    folly::Range<const vector_size_t*> indices,
    ByteOutputStream& stream) {
  if constexpr (
      Kind == TypeKind::TINYINT || Kind == TypeKind::SMALLINT ||
      Kind == TypeKind::INTEGER || Kind == TypeKind::BIGINT ||
      Kind == TypeKind::HUGEINT || Kind == TypeKind::REAL ||
      Kind == TypeKind::DOUBLE) {
    using T = typename TypeTraits<Kind>::NativeType;
    auto* simpleVector = vector.asUnchecked<SimpleVector<T>>();
    for (auto index : indices) {
      if (!vector.isNullAt(index)) {
        stream.appendOne<T>(simpleVector->valueAt(index));
      }
    }
  } else {
    for (auto index : indices) {
      if (!vector.isNullAt(index)) {
        serializeOne<Kind>(vector, index, stream);
      }
    }
  }
}

template <>
void serializeOne<TypeKind::VARCHAR>(
    const BaseVector& vector,
    vector_size_t index,
    ByteOutputStream& stream) {
  auto string = vector.asUnchecked<SimpleVector<StringView>>()->valueAt(index);
  stream.appendOne<int32_t>(string.size());
  stream.appendStringView(string);
}

template <>
void serializeOne<TypeKind::VARBINARY>(
    const BaseVector& vector,
    vector_size_t index,
    ByteOutputStream& stream) {
  auto string = vector.asUnchecked<SimpleVector<StringView>>()->valueAt(index);
  stream.appendOne<int32_t>(string.size());
  stream.appendStringView(string);
}

template <>
void serializeOne<TypeKind::ROW>(
    const BaseVector& vector,
    vector_size_t index,
    ByteOutputStream& out) {
  auto row = vector.wrappedVector()->asUnchecked<RowVector>();
  auto wrappedIndex = vector.wrappedIndex(index);
  const auto& type = row->type()->as<TypeKind::ROW>();
  // The layout is given by the type, not the instance. This will work
  // in the case of missing elements which will come out as null in
  // deserialization.
  auto childrenSize = type.size();
  auto children = row->children();
  std::vector<uint64_t> nulls(bits::nwords(childrenSize));
  for (auto i = 0; i < childrenSize; ++i) {
    if (i >= children.size() || !children[i] ||
        children[i]->isNullAt(wrappedIndex)) {
      bits::setBit(nulls.data(), i);
    }
  }
  out.append<uint64_t>(nulls);
  for (auto i = 0; i < children.size(); ++i) {
    if (!bits ::isBitSet(nulls.data(), i)) {
      serializeSwitch(*children[i], wrappedIndex, out);
    }
  }
}

void writeNulls(
    const BaseVector& values,
    vector_size_t offset,
    vector_size_t size,
    ByteOutputStream& out) {
  for (auto i = 0; i < size; i += 64) {
    uint64_t flags = 0;
    auto end = i + 64 < size ? 64 : size - i;
    for (auto bit = 0; bit < end; ++bit) {
      if (values.isNullAt(offset + i + bit)) {
        bits::setBit(&flags, bit, true);
      }
    }
    out.appendOne<uint64_t>(flags);
  }
}

void writeNulls(
    const BaseVector& values,
    folly::Range<const vector_size_t*> indices,
    ByteOutputStream& out) {
  auto size = indices.size();
  for (auto i = 0; i < size; i += 64) {
    uint64_t flags = 0;
    auto end = i + 64 < size ? 64 : size - i;
    for (auto bit = 0; bit < end; ++bit) {
      if (values.isNullAt(indices[i + bit])) {
        bits::setBit(&flags, bit, true);
      }
    }
    out.appendOne<uint64_t>(flags);
  }
}

void serializeArray(
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t size,
    ByteOutputStream& out) {
  BOLT_CHECK_GE(size, 0);
  out.appendOne<int32_t>(size);
  writeNulls(elements, offset, size, out);
  if (size > 0) {
    serializeManySwitch(elements, offset, size, out);
  }
}

void serializeArray(
    const BaseVector& elements,
    folly::Range<const vector_size_t*> indices,
    ByteOutputStream& out) {
  BOLT_CHECK_GE(indices.size(), 0);
  out.appendOne<int32_t>(indices.size());
  writeNulls(elements, indices, out);
  serializeManySwitch(elements, indices, out);
}

template <>
void serializeOne<TypeKind::ARRAY>(
    const BaseVector& source,
    vector_size_t index,
    ByteOutputStream& out) {
  auto array = source.wrappedVector()->asUnchecked<ArrayVector>();
  auto wrappedIndex = source.wrappedIndex(index);
  serializeArray(
      *array->elements(),
      array->offsetAt(wrappedIndex),
      array->sizeAt(wrappedIndex),
      out);
}

template <>
void serializeOne<TypeKind::MAP>(
    const BaseVector& vector,
    vector_size_t index,
    ByteOutputStream& out) {
  auto map = vector.wrappedVector()->asUnchecked<MapVector>();
  auto wrappedIndex = vector.wrappedIndex(index);
  auto size = map->sizeAt(wrappedIndex);
  auto offset = map->offsetAt(wrappedIndex);
  auto indices = map->sortedKeyIndices(wrappedIndex);
  serializeArray(*map->mapKeys(), indices, out);
  serializeArray(*map->mapValues(), indices, out);
}

void serializeSwitch(
    const BaseVector& source,
    vector_size_t index,
    ByteOutputStream& stream) {
  BOLT_DYNAMIC_TYPE_DISPATCH(
      serializeOne, source.typeKind(), source, index, stream);
}

void serializeManySwitch(
    const BaseVector& vector,
    vector_size_t offset,
    vector_size_t size,
    ByteOutputStream& stream) {
  BOLT_DYNAMIC_TYPE_DISPATCH(
      serializeMany, vector.typeKind(), vector, offset, size, stream);
}

void serializeManySwitch(
    const BaseVector& vector,
    folly::Range<const vector_size_t*> indices,
    ByteOutputStream& stream) {
  BOLT_DYNAMIC_TYPE_DISPATCH(
      serializeMany, vector.typeKind(), vector, indices, stream);
}

// Copy from serialization to vector.
void deserializeSwitch(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result,
    bool exactSize = false);

void deserializeManySwitch(
    ByteInputStream& in,
    BaseVector& elements,
    vector_size_t offset,
    int32_t size,
    const uint64_t* nulls);

template <TypeKind Kind>
void deserializeOne(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result,
    bool /*exactSize*/) {
  using T = typename TypeTraits<Kind>::NativeType;
  // Check that the vector is writable. This is faster than dynamic_cast.
  BOLT_CHECK_EQ(result.encoding(), VectorEncoding::Simple::FLAT);
  auto values = result.asUnchecked<FlatVector<T>>();
  values->set(index, in.read<T>());
}

template <TypeKind Kind>
void deserializeMany(
    ByteInputStream& in,
    BaseVector& elements,
    vector_size_t offset,
    int32_t size,
    const uint64_t* nulls) {
  // elements.resize(offset + size, true);

  if constexpr (
      Kind == TypeKind::TINYINT || Kind == TypeKind::SMALLINT ||
      Kind == TypeKind::INTEGER || Kind == TypeKind::BIGINT ||
      Kind == TypeKind::HUGEINT || Kind == TypeKind::REAL ||
      Kind == TypeKind::DOUBLE) {
    using T = typename TypeTraits<Kind>::NativeType;
    // Check that the vector is writable. This is faster than dynamic_cast.
    BOLT_CHECK_EQ(elements.encoding(), VectorEncoding::Simple::FLAT);
    auto values = elements.asUnchecked<FlatVector<T>>();
    auto nonNullCount = size - bits::countBits(nulls, 0, size);
    if (nonNullCount == 0) {
      for (auto i = 0; i < size; ++i) {
        elements.setNull(i + offset, true);
      }
    } else {
      T* data = new T[nonNullCount];
      in.readBytes(reinterpret_cast<uint8_t*>(data), nonNullCount * sizeof(T));
      int dataIndex = 0;

      if (nonNullCount == size) {
        for (auto i = 0; i < size; ++i) {
          elements.setNull(i + offset, false);
        }
        memcpy(
            values->mutableRawValues() + offset,
            data,
            nonNullCount * sizeof(T));
      } else {
        for (auto i = 0; i < size; ++i) {
          if (bits::isBitSet(nulls, i)) {
            elements.setNull(i + offset, true);
          } else {
            values->set(i + offset, data[dataIndex++]);
          }
        }
      }
      delete[] data;
    }
  } else {
    for (auto i = 0; i < size; ++i) {
      if (bits::isBitSet(nulls, i)) {
        elements.setNull(i + offset, true);
      } else {
        deserializeOne<Kind>(in, i + offset, elements, false);
      }
    }
  }
}

void deserializeString(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result,
    bool exactSize) {
  BOLT_CHECK_EQ(result.encoding(), VectorEncoding::Simple::FLAT);
  auto values = result.asUnchecked<FlatVector<StringView>>();
  auto size = in.read<int32_t>();
  if (StringView::isInline(size)) {
    char start[StringView::kInlineSize];
    in.readBytes(start, size);
    values->setNoCopy(index, StringView(start, size));
  } else {
    auto buffer = values->getBufferWithSpace(size, exactSize);
    auto start = buffer->asMutable<char>() + buffer->size();
    in.readBytes(start, size);
    buffer->setSize(buffer->size() + size);
    values->setNoCopy(index, StringView(start, size));
  }
}

template <>
void deserializeOne<TypeKind::VARCHAR>(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result,
    bool exactSize) {
  deserializeString(in, index, result, exactSize);
}

template <>
void deserializeOne<TypeKind::VARBINARY>(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result,
    bool exactSize) {
  deserializeString(in, index, result, exactSize);
}

std::vector<uint64_t> readNulls(ByteInputStream& in, int32_t size) {
  auto n = bits::nwords(size);
  std::vector<uint64_t> nulls(n);
  in.readBytes(reinterpret_cast<uint8_t*>(nulls.data()), n * sizeof(uint64_t));
  return nulls;
}

template <>
void deserializeOne<TypeKind::ROW>(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result,
    bool /*exactSize*/) {
  const auto& type = result.type()->as<TypeKind::ROW>();
  BOLT_CHECK_EQ(result.encoding(), VectorEncoding::Simple::ROW);
  auto row = result.asUnchecked<RowVector>();
  auto childrenSize = type.size();
  BOLT_CHECK_EQ(childrenSize, row->childrenSize());
  auto nulls = readNulls(in, childrenSize);
  for (auto i = 0; i < childrenSize; ++i) {
    auto child = row->childAt(i);
    if (child->size() <= index) {
      child->resize(index + 1);
    }
    if (bits::isBitSet(nulls.data(), i)) {
      child->setNull(index, true);
    } else {
      deserializeSwitch(in, index, *child);
    }
  }
  result.setNull(index, false);
}

// Reads the size, null flags and deserializes from 'in', appending to
// the end of 'elements'. Returns the number of added elements and
// sets 'offset' to the index of the first added element.
vector_size_t deserializeArray(
    ByteInputStream& in,
    BaseVector& elements,
    vector_size_t& offset) {
  auto size = in.read<int32_t>();
  BOLT_CHECK_GE(size, 0);
  auto nulls = readNulls(in, size);
  offset = elements.size();
  elements.resize(offset + size);
  deserializeManySwitch(in, elements, offset, size, nulls.data());
  return size;
}

template <>
void deserializeOne<TypeKind::ARRAY>(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result,
    bool /*exactSize*/) {
  BOLT_CHECK_EQ(result.encoding(), VectorEncoding::Simple::ARRAY);
  auto array = result.asUnchecked<ArrayVector>();
  if (array->size() <= index) {
    array->resize(index + 1);
  }
  vector_size_t offset;
  auto size = deserializeArray(in, *array->elements(), offset);
  array->setOffsetAndSize(index, offset, size);
  result.setNull(index, false);
}

template <>
void deserializeOne<TypeKind::MAP>(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result,
    bool /*exactSize*/) {
  BOLT_CHECK_EQ(result.encoding(), VectorEncoding::Simple::MAP);
  auto map = result.asUnchecked<MapVector>();
  if (map->size() <= index) {
    map->resize(index + 1);
  }
  vector_size_t keyOffset;
  auto keySize = deserializeArray(in, *map->mapKeys(), keyOffset);
  vector_size_t valueOffset;
  auto valueSize = deserializeArray(in, *map->mapValues(), valueOffset);
  BOLT_CHECK_EQ(keySize, valueSize);
  BOLT_CHECK_EQ(keyOffset, valueOffset);
  map->setOffsetAndSize(index, keyOffset, keySize);
  result.setNull(index, false);
}

void deserializeSwitch(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result,
    bool exactSize) {
  BOLT_DYNAMIC_TYPE_DISPATCH(
      deserializeOne, result.typeKind(), in, index, result, exactSize);
}

void deserializeManySwitch(
    ByteInputStream& in,
    BaseVector& elements,
    vector_size_t offset,
    int32_t size,
    const uint64_t* nulls) {
  BOLT_DYNAMIC_TYPE_DISPATCH(
      deserializeMany, elements.typeKind(), in, elements, offset, size, nulls);
}

// Comparison of serialization and vector.
std::optional<int32_t> compareSwitch(
    ByteInputStream& stream,
    const BaseVector& vector,
    vector_size_t index,
    CompareFlags flags);

template <TypeKind Kind>
std::optional<int32_t> compare(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  using T = typename TypeTraits<Kind>::NativeType;
  auto rightValue = right.asUnchecked<SimpleVector<T>>()->valueAt(index);
  auto leftValue = left.read<T>();
  auto result = SimpleVector<T>::comparePrimitiveAsc(leftValue, rightValue);
  return flags.ascending ? result : result * -1;
}

int compareStringAsc(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    bool equalsOnly) {
  int32_t leftSize = left.read<int32_t>();
  auto rightView =
      right.asUnchecked<SimpleVector<StringView>>()->valueAt(index);
  if (rightView.size() != leftSize && equalsOnly) {
    return 1;
  }
  auto compareSize = std::min<int32_t>(leftSize, rightView.size());
  int32_t rightOffset = 0;
  while (compareSize > 0) {
    auto leftView = left.nextView(compareSize);
    auto result = memcmp(
        leftView.data(), rightView.data() + rightOffset, leftView.size());
    if (result != 0) {
      return result;
    }
    rightOffset += leftView.size();
    compareSize -= leftView.size();
  }
  return leftSize - rightView.size();
}

template <>
std::optional<int32_t> compare<TypeKind::VARCHAR>(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  auto result = compareStringAsc(left, right, index, flags.equalsOnly);
  return flags.ascending ? result : result * -1;
}

template <>
std::optional<int32_t> compare<TypeKind::VARBINARY>(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  auto result = compareStringAsc(left, right, index, flags.equalsOnly);
  return flags.ascending ? result : result * -1;
}

template <>
std::optional<int32_t> compare<TypeKind::ROW>(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  auto row = right.wrappedVector()->asUnchecked<RowVector>();
  auto wrappedIndex = right.wrappedIndex(index);
  BOLT_CHECK_EQ(row->encoding(), VectorEncoding::Simple::ROW);
  const auto& type = row->type()->as<TypeKind::ROW>();
  auto childrenSize = type.size();
  BOLT_CHECK_EQ(childrenSize, row->childrenSize());
  auto nulls = readNulls(left, childrenSize);
  for (auto i = 0; i < childrenSize; ++i) {
    auto child = row->childAt(i);
    auto leftNull = bits::isBitSet(nulls.data(), i);
    auto rightNull = child->isNullAt(wrappedIndex);

    if (leftNull || rightNull) {
      auto result = BaseVector::compareNulls(leftNull, rightNull, flags);
      if (result.has_value() && result.value() == 0) {
        continue;
      }
      return result;
    }

    auto result = compareSwitch(left, *child, wrappedIndex, flags);
    if (result.has_value() && result.value() == 0) {
      continue;
    }
    return result;
  }
  return 0;
}

template <TypeKind Kind>
std::optional<int32_t> compareArraysInternal(
    ByteInputStream& left,
    const BaseVector& elements,
    const BaseVector* wrappedElements,
    vector_size_t offset,
    vector_size_t rightSize,
    CompareFlags flags) {
  int leftSize = left.read<int32_t>();
  if (leftSize != rightSize && flags.equalsOnly) {
    return flags.ascending ? 1 : -1;
  }
  auto compareSize = std::min(leftSize, rightSize);
  auto leftNulls = readNulls(left, leftSize);
  for (auto i = 0; i < compareSize; ++i) {
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = elements.isNullAt(offset + i);

    if (leftNull || rightNull) {
      auto result = BaseVector::compareNulls(leftNull, rightNull, flags);
      if (result.has_value() && result.value() == 0) {
        continue;
      }
      return result;
    }

    auto elementIndex = elements.wrappedIndex(offset + i);
    auto result = compare<Kind>(left, *wrappedElements, elementIndex, flags);
    if (result.has_value() && result.value() == 0) {
      continue;
    }
    return result;
  }
  return flags.ascending ? (leftSize - rightSize) : (rightSize - leftSize);
}

std::optional<int32_t> compareArrays(
    ByteInputStream& left,
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t rightSize,
    CompareFlags flags) {
  auto wrappedElements = elements.wrappedVector();
  return BOLT_DYNAMIC_TYPE_DISPATCH(
      compareArraysInternal,
      wrappedElements->typeKind(),
      left,
      elements,
      wrappedElements,
      offset,
      rightSize,
      flags);
}

template <TypeKind Kind>
std::optional<int32_t> compareArrayIndicesInternal(
    ByteInputStream& left,
    const BaseVector& elements,
    const BaseVector* wrappedElements,
    folly::Range<const vector_size_t*> rightIndices,
    CompareFlags flags) {
  int32_t leftSize = left.read<int32_t>();
  int32_t rightSize = rightIndices.size();
  if (leftSize != rightSize && flags.equalsOnly) {
    return flags.ascending ? 1 : -1;
  }
  auto compareSize = std::min(leftSize, rightSize);
  auto leftNulls = readNulls(left, leftSize);
  for (auto i = 0; i < compareSize; ++i) {
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = elements.isNullAt(rightIndices[i]);

    if (leftNull || rightNull) {
      auto result = BaseVector::compareNulls(leftNull, rightNull, flags);
      if (result.has_value() && result.value() == 0) {
        continue;
      }
      return result;
    }

    auto elementIndex = elements.wrappedIndex(rightIndices[i]);
    auto result = compare<Kind>(left, *wrappedElements, elementIndex, flags);
    if (result.has_value() && result.value() == 0) {
      continue;
    }
    return result;
  }
  return flags.ascending ? (leftSize - rightSize) : (rightSize - leftSize);
}

std::optional<int32_t> compareArrayIndices(
    ByteInputStream& left,
    const BaseVector& elements,
    folly::Range<const vector_size_t*> rightIndices,
    CompareFlags flags) {
  auto wrappedElements = elements.wrappedVector();
  return BOLT_DYNAMIC_TYPE_DISPATCH(
      compareArrayIndicesInternal,
      wrappedElements->typeKind(),
      left,
      elements,
      wrappedElements,
      rightIndices,
      flags);
}

template <>
std::optional<int32_t> compare<TypeKind::ARRAY>(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  auto array = right.wrappedVector()->asUnchecked<ArrayVector>();
  BOLT_CHECK_EQ(array->encoding(), VectorEncoding::Simple::ARRAY);
  auto wrappedIndex = right.wrappedIndex(index);
  return compareArrays(
      left,
      *array->elements(),
      array->offsetAt(wrappedIndex),
      array->sizeAt(wrappedIndex),
      flags);
}

template <>
std::optional<int32_t> compare<TypeKind::MAP>(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  auto map = right.wrappedVector()->asUnchecked<MapVector>();
  BOLT_CHECK_EQ(map->encoding(), VectorEncoding::Simple::MAP);
  auto wrappedIndex = right.wrappedIndex(index);
  auto size = map->sizeAt(wrappedIndex);

  auto leftSize = left.peek<int32_t>();
  if (flags.compareSizeFirst && leftSize != size) {
    return flags.ascending ? (leftSize - size) : (size - leftSize);
  }

  std::vector<vector_size_t> indices(size);
  auto rightIndices = map->sortedKeyIndices(wrappedIndex);
  auto result = compareArrayIndices(left, *map->mapKeys(), rightIndices, flags);
  if (result.has_value() && result.value() == 0) {
    return compareArrayIndices(left, *map->mapValues(), rightIndices, flags);
  }
  return result;
}

std::optional<int32_t> compareSwitch(
    ByteInputStream& stream,
    const BaseVector& vector,
    vector_size_t index,
    CompareFlags flags) {
  return BOLT_DYNAMIC_TYPE_DISPATCH(
      compare, vector.typeKind(), stream, vector, index, flags);
}

// Returns a view over a serialized string with the string as a
// contiguous array of bytes. This may use 'storage' for a temporary
// copy.
StringView readStringView(ByteInputStream& stream, std::string& storage) {
  int32_t size = stream.read<int32_t>();
  auto view = stream.nextView(size);
  if (view.size() == size) {
    // The string is all in one piece, no copy.
    return StringView(view.data(), view.size());
  }
  storage.resize(size);
  memcpy(storage.data(), view.data(), view.size());
  stream.readBytes(storage.data() + view.size(), size - view.size());
  return StringView(storage);
}

// Comparison of two serializations.
std::optional<int32_t> compareSwitch(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags);

template <TypeKind Kind>
std::optional<int32_t> compare(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* /*type*/,
    CompareFlags flags) {
  using T = typename TypeTraits<Kind>::NativeType;
  T leftValue = left.read<T>();
  T rightValue = right.read<T>();
  auto result = SimpleVector<T>::comparePrimitiveAsc(leftValue, rightValue);
  return flags.ascending ? result : result * -1;
}

template <>
std::optional<int32_t> compare<TypeKind::VARCHAR>(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* /*type*/,
    CompareFlags flags) {
  std::string leftStorage;
  std::string rightStorage;
  StringView leftValue = readStringView(left, leftStorage);
  StringView rightValue = readStringView(right, rightStorage);
  return flags.ascending ? leftValue.compare(rightValue)
                         : rightValue.compare(leftValue);
}

template <>
std::optional<int32_t> compare<TypeKind::VARBINARY>(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* /*type*/,
    CompareFlags flags) {
  std::string leftStorage;
  std::string rightStorage;
  StringView leftValue = readStringView(left, leftStorage);
  StringView rightValue = readStringView(right, rightStorage);
  return flags.ascending ? leftValue.compare(rightValue)
                         : rightValue.compare(leftValue);
}

template <TypeKind Kind>
std::optional<int32_t> compareArraysInternal(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* elementType,
    CompareFlags flags) {
  auto leftSize = left.read<int32_t>();
  auto rightSize = right.read<int32_t>();
  if (flags.equalsOnly && leftSize != rightSize) {
    return flags.ascending ? 1 : -1;
  }
  auto compareSize = std::min(leftSize, rightSize);
  auto leftNulls = readNulls(left, leftSize);
  auto rightNulls = readNulls(right, rightSize);
  for (auto i = 0; i < compareSize; ++i) {
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = bits::isBitSet(rightNulls.data(), i);
    if (leftNull || rightNull) {
      auto result = BaseVector::compareNulls(leftNull, rightNull, flags);
      if (result.has_value() && result.value() == 0) {
        continue;
      }
      return result;
    }

    auto result = compare<Kind>(left, right, elementType, flags);
    if (result.has_value() && result.value() == 0) {
      continue;
    }
    return result;
  }
  return flags.ascending ? (leftSize - rightSize) : (rightSize - leftSize);
}

std::optional<int32_t> compareArrays(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* elementType,
    CompareFlags flags) {
  return BOLT_DYNAMIC_TYPE_DISPATCH(
      compareArraysInternal,
      elementType->kind(),
      left,
      right,
      elementType,
      flags);
}

template <>
std::optional<int32_t> compare<TypeKind::ROW>(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  const auto& rowType = type->as<TypeKind::ROW>();
  int size = rowType.size();
  auto leftNulls = readNulls(left, size);
  auto rightNulls = readNulls(right, size);
  for (auto i = 0; i < size; ++i) {
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = bits::isBitSet(rightNulls.data(), i);
    if (leftNull || rightNull) {
      auto result = BaseVector::compareNulls(leftNull, rightNull, flags);
      if (result.has_value() && result.value() == 0) {
        continue;
      }
      return result;
    }

    auto result = compareSwitch(left, right, rowType.childAt(i).get(), flags);
    if (result.has_value() && result.value() == 0) {
      continue;
    }
    return result;
  }
  return 0;
}

template <>
std::optional<int32_t> compare<TypeKind::ARRAY>(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  return compareArrays(left, right, type->childAt(0).get(), flags);
}

template <>
std::optional<int32_t> compare<TypeKind::MAP>(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  auto result = compareArrays(left, right, type->childAt(0).get(), flags);
  if (result.has_value() && result.value() == 0) {
    return compareArrays(left, right, type->childAt(1).get(), flags);
  }
  return result;
}

std::optional<int32_t> compareSwitch(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  return BOLT_DYNAMIC_TYPE_DISPATCH(
      compare, type->kind(), left, right, type, flags);
}

// Hash functions.
uint64_t hashSwitch(ByteInputStream& stream, const Type* type);

template <TypeKind Kind>
uint64_t hashOne(ByteInputStream& stream, const Type* /*type*/) {
  using T = typename TypeTraits<Kind>::NativeType;
  return folly::hasher<T>()(stream.read<T>());
}

template <>
uint64_t hashOne<TypeKind::VARCHAR>(
    ByteInputStream& stream,
    const Type* /*type*/) {
  std::string storage;
  return folly::hasher<StringView>()(readStringView(stream, storage));
}

template <>
uint64_t hashOne<TypeKind::VARBINARY>(
    ByteInputStream& stream,
    const Type* /*type*/) {
  std::string storage;
  return folly::hasher<StringView>()(readStringView(stream, storage));
}

uint64_t
hashArray(ByteInputStream& in, uint64_t hash, const Type* elementType) {
  auto size = in.read<int32_t>();
  auto nulls = readNulls(in, size);
  for (auto i = 0; i < size; ++i) {
    uint64_t value;
    if (bits::isBitSet(nulls.data(), i)) {
      value = BaseVector::kNullHash;
    } else {
      value = hashSwitch(in, elementType);
    }
    hash = bits::commutativeHashMix(hash, value);
  }
  return hash;
}

template <>
uint64_t hashOne<TypeKind::ROW>(ByteInputStream& in, const Type* type) {
  auto size = type->size();
  auto nulls = readNulls(in, size);
  uint64_t hash = BaseVector::kNullHash;
  for (auto i = 0; i < size; ++i) {
    uint64_t value;
    if (bits::isBitSet(nulls.data(), i)) {
      value = BaseVector::kNullHash;
    } else {
      value = hashSwitch(in, type->childAt(i).get());
    }
    hash = i == 0 ? value : bits::hashMix(hash, value);
  }
  return hash;
}

template <>
uint64_t hashOne<TypeKind::ARRAY>(ByteInputStream& in, const Type* type) {
  return hashArray(in, BaseVector::kNullHash, type->childAt(0).get());
}

template <>
uint64_t hashOne<TypeKind::MAP>(ByteInputStream& in, const Type* type) {
  return hashArray(
      in,
      hashArray(in, BaseVector::kNullHash, type->childAt(0).get()),
      type->childAt(1).get());
}

uint64_t hashSwitch(ByteInputStream& in, const Type* type) {
  return BOLT_DYNAMIC_TYPE_DISPATCH(hashOne, type->kind(), in, type);
}

} // namespace

// static
void ContainerRowSerde::serialize(
    const BaseVector& source,
    vector_size_t index,
    ByteOutputStream& out) {
  BOLT_DCHECK(
      !source.isNullAt(index), "Null top-level values are not supported");
  serializeSwitch(source, index, out);
}

// static
void ContainerRowSerde::deserialize(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector* result,
    bool exactSize) {
  deserializeSwitch(in, index, *result, exactSize);
}

// static
int32_t ContainerRowSerde::compare(
    ByteInputStream& left,
    const DecodedVector& right,
    vector_size_t index,
    CompareFlags flags) {
  BOLT_DCHECK(
      !right.isNullAt(index), "Null top-level values are not supported");
  BOLT_DCHECK(flags.nullAsValue(), "not supported null handling mode");
  return compareSwitch(left, *right.base(), right.index(index), flags).value();
}

// static
int32_t ContainerRowSerde::compare(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  BOLT_DCHECK(flags.nullAsValue(), "not supported null handling mode");

  return compareSwitch(left, right, type, flags).value();
}

std::optional<int32_t> ContainerRowSerde::compareWithNulls(
    ByteInputStream& left,
    const DecodedVector& right,
    vector_size_t index,
    CompareFlags flags) {
  BOLT_DCHECK(
      !right.isNullAt(index), "Null top-level values are not supported");
  return compareSwitch(left, *right.base(), right.index(index), flags);
}

std::optional<int32_t> ContainerRowSerde::compareWithNulls(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  return compareSwitch(left, right, type, flags);
}

// static
uint64_t ContainerRowSerde::hash(ByteInputStream& in, const Type* type) {
  return hashSwitch(in, type);
}

} // namespace bytedance::bolt::exec
