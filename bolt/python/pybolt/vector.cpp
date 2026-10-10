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

// NOLINTBEGIN(misc-no-recursion)
#include <pybind11/functional.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

#include "bolt/buffer/StringViewBufferHolder.h"
#include "bolt/common/serialization/Serializable.h"
#include "bolt/expression/Expr.h"
#include "bolt/parse/Expressions.h"
#include "bolt/parse/IExpr.h"
#include "bolt/python/numpy.h"
#include "bolt/python/pybolt/context.h"
#include "bolt/type/Type.h"
#include "bolt/type/Variant.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DictionaryVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/TypeAliases.h"

using namespace ::bytedance::bolt;
using namespace ::bytedance::bolt::python;

namespace {

// Structure used to keep check on the number
// of constituent elements. Attributes totalElements
// and insertedElements keeps the length of the vector, and
// the number of elements inserted during the operation
// respectively.
struct ElementCounter {
  vector_size_t insertedElements =
      0; // to track the elements already in the vector
  vector_size_t totalElements = 0;
  std::vector<ElementCounter> children;
  TypePtr inferredMapKeyType;
  TypePtr inferredMapValueType;
};

void checkOrAssignType(TypePtr& type, const TypePtr& expectedType) {
  if (type->kind() == TypeKind::UNKNOWN) {
    type = expectedType;
  } else if (!(type->kindEquals(expectedType))) {
    throw pybind11::type_error(
        "Cannot construct type tree, invalid variant for complex type");
  }
}

template <TypeKind Kind>
void setElementInFlatVector(
    vector_size_t idx,
    const variant& v,
    VectorPtr& vector) {
  using NativeType = typename TypeTraits<Kind>::NativeType;
  auto asFlat = vector->asFlatVector<NativeType>();
  asFlat->set(idx, NativeType{v.value<NativeType>()});
}

void countElementsForType(
    const variant& v,
    const TypePtr& type,
    ElementCounter& counter) {
  ++counter.totalElements;

  if (v.isNull()) {
    return;
  }

  if (v.kind() != type->kind()) {
    throw pybind11::type_error(fmt::format(
        "Invalid value kind {} for expected type {}",
        static_cast<int>(v.kind()),
        type->toString()));
  }

  switch (type->kind()) {
    case TypeKind::ARRAY: {
      counter.children.resize(1);
      const auto& elements = v.array();
      const auto& elementType = type->childAt(0);
      for (const auto& elt : elements) {
        countElementsForType(elt, elementType, counter.children[0]);
      }
      break;
    }
    case TypeKind::MAP: {
      counter.children.resize(2);
      const auto& map = v.map();
      const auto& keyType = type->childAt(0);
      const auto& valueType = type->childAt(1);
      for (const auto& [key, value] : map) {
        countElementsForType(key, keyType, counter.children[0]);
        countElementsForType(value, valueType, counter.children[1]);
      }
      break;
    }
    default:
      break;
  }
}

// This function determines the type and the number of elements for a variant.
// Takes reference to Type and ElementCounter which will be set after the run.
// It is supposed to run a recursive call with a pre-instantiated TypePtr,
// the target variant and the counter. The passed variant is checked for its
// data type, and for any complex type involved, the function is called again.
// The counter here is used to keep in track of the number of elements inserted
// and the number of types of elements allowed if a complex vector is involved
// in the variant.
void constructType(const variant& v, TypePtr& type, ElementCounter& counter) {
  ++counter.totalElements;

  if (v.isNull()) {
    // since the variant is NULL, we can't infer the data type
    // thus it maybe UNKNOWN or INVALID at this stage
    // which implies further investigation is required
    if (v.kind() != TypeKind::UNKNOWN && v.kind() != TypeKind::INVALID &&
        v.kind() != type->kind()) {
      throw std::invalid_argument("Variant was of an unexpected kind");
    }
    return;
  }

  // if a Non-Null variant's type is unknown or not one of the valid
  // types which are supported then the Type tree cannot be constructed
  if (v.kind() == TypeKind::UNKNOWN || v.kind() == TypeKind::INVALID) {
    throw std::invalid_argument("Non-null variant has unknown or invalid kind");
  }

  switch (v.kind()) {
    case TypeKind::ARRAY: {
      counter.children.resize(1);
      auto asArray = v.array();
      TypePtr childType = createType(TypeKind::UNKNOWN, {});
      for (const auto& element : asArray) {
        constructType(element, childType, counter.children[0]);
      }

      // if child's type still remains Unknown, implies all the
      // elements in the array are actually NULL
      if (childType->kind() == TypeKind::UNKNOWN) {
        throw pybind11::value_error(
            "Cannot construct array with all None values");
      }
      checkOrAssignType(type, createType<TypeKind::ARRAY>({childType}));
      break;
    }
    case TypeKind::MAP: {
      counter.children.resize(2);
      const auto& asMap = v.map();

      if (type->kind() == TypeKind::MAP) {
        const auto& keyType = type->childAt(0);
        const auto& valueType = type->childAt(1);
        for (const auto& [key, value] : asMap) {
          if (key.isNull()) {
            throw pybind11::type_error("Map key cannot be None");
          }
          countElementsForType(key, keyType, counter.children[0]);
          countElementsForType(value, valueType, counter.children[1]);
        }
        break;
      }

      if (type->kind() != TypeKind::UNKNOWN) {
        throw pybind11::type_error(
            "Cannot construct type tree, invalid variant for complex type");
      }

      TypePtr keyType = counter.inferredMapKeyType
          ? counter.inferredMapKeyType
          : createType(TypeKind::UNKNOWN, {});
      TypePtr valueType = counter.inferredMapValueType
          ? counter.inferredMapValueType
          : createType(TypeKind::UNKNOWN, {});
      for (const auto& [key, value] : asMap) {
        if (key.isNull()) {
          throw pybind11::type_error("Map key cannot be None");
        }
        if (keyType->kind() == TypeKind::UNKNOWN) {
          constructType(key, keyType, counter.children[0]);
        } else {
          countElementsForType(key, keyType, counter.children[0]);
        }

        if (valueType->kind() == TypeKind::UNKNOWN) {
          if (value.isNull()) {
            countElementsForType(value, valueType, counter.children[1]);
          } else {
            constructType(value, valueType, counter.children[1]);
          }
        } else {
          countElementsForType(value, valueType, counter.children[1]);
        }
      }

      if (keyType->kind() != TypeKind::UNKNOWN) {
        counter.inferredMapKeyType = keyType;
      }
      if (valueType->kind() != TypeKind::UNKNOWN) {
        counter.inferredMapValueType = valueType;
      }
      if (counter.inferredMapKeyType && counter.inferredMapValueType) {
        checkOrAssignType(
            type,
            createType<TypeKind::MAP>(
                {counter.inferredMapKeyType, counter.inferredMapValueType}));
      }
      break;
    }

    default: {
      checkOrAssignType(type, createScalarType(v.kind()));
      break;
    }
  }
}

// Function is called with the variant to be added,
// the target vector and the element counter. The element counter
// is used to track the number of elements already inserted, so as
// to get the index for the next element to insert. For an array
// vector, the required offset and size is first set into the vector
// then the function is called recursively for the contained elements.
// In the default case where the variant is a scalar type, the
// setElementInFlatVector is called without any further recursion.
void insertVariantIntoVector(
    const variant& v,
    VectorPtr& vector,
    ElementCounter& counter,
    vector_size_t previousSize,
    vector_size_t previousOffset) {
  if (v.isNull()) {
    vector->setNull(counter.insertedElements, true);
  } else {
    switch (v.kind()) {
      case TypeKind::ARRAY: {
        auto asArray = vector->as<ArrayVector>();
        const std::vector<variant>& elements = v.array();
        if (asArray->elements()->size() != counter.children[0].totalElements) {
          asArray->elements()->resize(counter.children[0].totalElements);
        }
        vector_size_t offset = counter.children[0].insertedElements;
        vector_size_t size = elements.size();
        asArray->setOffsetAndSize(counter.insertedElements, offset, size);
        for (const variant& elt : elements) {
          insertVariantIntoVector(
              elt, asArray->elements(), counter.children[0], offset, size);
        }

        break;
      }
      case TypeKind::MAP: {
        auto asMap = vector->as<MapVector>();
        const auto& entries = v.map();

        if (asMap->mapKeys()->size() != counter.children[0].totalElements) {
          asMap->mapKeys()->resize(counter.children[0].totalElements);
        }
        if (asMap->mapValues()->size() != counter.children[1].totalElements) {
          asMap->mapValues()->resize(counter.children[1].totalElements);
        }

        vector_size_t offset = counter.children[0].insertedElements;
        vector_size_t size = entries.size();
        asMap->setOffsetAndSize(counter.insertedElements, offset, size);

        for (const auto& [key, value] : entries) {
          insertVariantIntoVector(
              key, asMap->mapKeys(), counter.children[0], offset, size);
          insertVariantIntoVector(
              value, asMap->mapValues(), counter.children[1], offset, size);
        }
        break;
      }
      default: {
        BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
            setElementInFlatVector,
            v.kind(),
            counter.insertedElements,
            v,
            vector);
        break;
      }
    }
  }
  counter.insertedElements += 1;
}

VectorPtr variantsToVector(
    const std::vector<variant>& variants,
    memory::MemoryPool* pool) {
  ElementCounter counter;
  TypePtr type = createType(TypeKind::UNKNOWN, {});
  for (const auto& variant : variants) {
    constructType(variant, type, counter);
  }
  if (type->kind() == TypeKind::UNKNOWN) {
    if (!counter.inferredMapKeyType) {
      throw pybind11::value_error("Cannot construct map with empty keys");
    }
    if (!counter.inferredMapValueType) {
      throw pybind11::value_error("Cannot construct map with all None values");
    }
  }
  VectorPtr resultVector =
      BaseVector::create(std::move(type), variants.size(), pool);
  for (const variant& v : variants) {
    insertVariantIntoVector(
        v,
        resultVector,
        counter,
        /*previous_size*/ 0,
        /*previous_offset*/ 0);
  }
  return resultVector;
}

std::string serializeType(const std::shared_ptr<const Type>& type) {
  const auto& obj = type->serialize();
  return folly::json::serialize(obj, getSerializationOptions());
}

template <typename VecPtr>
inline void checkBounds(VecPtr& v, vector_size_t idx) {
  if (idx < 0 || idx >= v->size()) {
    throw std::out_of_range("Index out of range");
  }
}

template <TypeKind T>
inline auto pyToVariant(const pybind11::handle& obj) {
  using NativeType = typename TypeTraits<T>::DeepCopiedType;
  return variant::create<T>(pybind11::cast<NativeType>(obj));
}

inline variant pyToVariant(const pybind11::handle& obj) {
  if (obj.is_none()) {
    return variant();
  }
  if (pybind11::isinstance<pybind11::bool_>(obj)) {
    return pyToVariant<TypeKind::BOOLEAN>(obj);
  }
  if (pybind11::isinstance<pybind11::int_>(obj)) {
    return pyToVariant<TypeKind::BIGINT>(obj);
  }
  if (pybind11::isinstance<pybind11::float_>(obj)) {
    return pyToVariant<TypeKind::DOUBLE>(obj);
  }
  if (pybind11::isinstance<pybind11::str>(obj)) {
    return pyToVariant<TypeKind::VARCHAR>(obj);
  }
  if (pybind11::isinstance<pybind11::bytes>(obj)) {
    return pyToVariant<TypeKind::VARBINARY>(obj);
  }
  if (pybind11::isinstance<pybind11::bytearray>(obj)) {
    return pyToVariant<TypeKind::VARBINARY>(obj);
  }
  if (pybind11::isinstance<pybind11::dict>(obj)) {
    pybind11::dict objAsDict = pybind11::cast<pybind11::dict>(obj);
    std::map<variant, variant> result;
    for (const auto& item : objAsDict) {
      if (item.first.is_none()) {
        throw pybind11::type_error("Map key cannot be None");
      }
      result.emplace(pyToVariant(item.first), pyToVariant(item.second));
    }
    return variant::map(std::move(result));
  }
  if (pybind11::isinstance<pybind11::list>(obj)) {
    pybind11::list objAsList = pybind11::cast<pybind11::list>(obj);
    std::vector<variant> result;
    for (const auto& item : objAsList) {
      result.push_back(pyToVariant(item));
    }
    return variant::array(std::move(result));
  }
  throw pybind11::type_error("Invalid type of object");
}

inline variant pyToVariant(const pybind11::handle& obj, const Type& dtype) {
  if (obj.is_none()) {
    return variant(dtype.kind());
  }
  switch (dtype.kind()) {
    case TypeKind::BOOLEAN: {
      return pyToVariant<TypeKind::BOOLEAN>(obj);
    }
    case TypeKind::TINYINT: {
      return pyToVariant<TypeKind::TINYINT>(obj);
    }
    case TypeKind::SMALLINT: {
      return pyToVariant<TypeKind::SMALLINT>(obj);
    }
    case TypeKind::INTEGER: {
      return pyToVariant<TypeKind::INTEGER>(obj);
    }
    case TypeKind::BIGINT: {
      return pyToVariant<TypeKind::BIGINT>(obj);
    }
    case TypeKind::REAL: {
      return pyToVariant<TypeKind::REAL>(obj);
    }
    case TypeKind::DOUBLE: {
      return pyToVariant<TypeKind::DOUBLE>(obj);
    }
    case TypeKind::VARCHAR: {
      return pyToVariant<TypeKind::VARCHAR>(obj);
    }
    case TypeKind::VARBINARY: {
      return pyToVariant<TypeKind::VARBINARY>(obj);
    }
    case TypeKind::TIMESTAMP: {
      return pyToVariant<TypeKind::TIMESTAMP>(obj);
    }
    case TypeKind::ARRAY: {
      if (!pybind11::isinstance<pybind11::list>(obj)) {
        throw pybind11::type_error("Expected list for ARRAY type");
      }
      pybind11::list objAsList = pybind11::cast<pybind11::list>(obj);
      std::vector<variant> result;
      result.reserve(objAsList.size());
      const auto& elementType = dtype.childAt(0);
      for (const auto& item : objAsList) {
        result.push_back(pyToVariant(item, *elementType));
      }
      return variant::array(std::move(result));
    }
    case TypeKind::MAP: {
      if (!pybind11::isinstance<pybind11::dict>(obj)) {
        throw pybind11::type_error("Expected dict for MAP type");
      }
      pybind11::dict objAsDict = pybind11::cast<pybind11::dict>(obj);
      std::map<variant, variant> result;
      const auto& keyType = dtype.childAt(0);
      const auto& valueType = dtype.childAt(1);
      for (const auto& item : objAsDict) {
        if (item.first.is_none()) {
          throw pybind11::type_error("Map key cannot be None");
        }
        auto keyVar = pyToVariant(item.first, *keyType);
        auto valueVar = pyToVariant(item.second, *valueType);
        result.emplace(std::move(keyVar), std::move(valueVar));
      }
      return variant::map(std::move(result));
    }
    default:
      throw pybind11::type_error(
          fmt::format("Unsupported type {} supplied", dtype.toString()));
  }
}

inline void checkRowVectorBounds(const RowVectorPtr& v, vector_size_t idx) {
  if (idx < 0 || static_cast<size_t>(idx) >= v->childrenSize()) {
    throw std::out_of_range("Index out of range");
  }
}

bool compareRowVector(const RowVectorPtr& u, const RowVectorPtr& v) {
  CompareFlags compFlags =
      CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

  if (u->size() != v->size()) {
    return false;
  }
  for (size_t i = 0; i < u->size(); i++) {
    if (u->compare(v.get(), i, i, compFlags) != 0) {
      return false;
    }
  }

  return true;
}

template <typename NativeType>
bool compareFlatVector(
    const FlatVectorPtr<NativeType>& u,
    const FlatVectorPtr<NativeType>& v) {
  constexpr CompareFlags kCompFlags =
      CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

  if (u->size() != v->size()) {
    return false;
  }
  for (size_t i = 0; i < u->size(); i++) {
    if (u->compare(v.get(), i, i, kCompFlags) != 0) {
      return false;
    }
  }

  return true;
}

bool compareComplexVector(const VectorPtr& u, const VectorPtr& v) {
  CompareFlags compFlags =
      CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

  if (u->size() != v->size()) {
    return false;
  }
  for (size_t i = 0; i < u->size(); i++) {
    if (u->compare(v.get(), i, i, compFlags) != 0) {
      return false;
    }
  }

  return true;
}

inline std::string rowVectorToString(const RowVectorPtr& vector) {
  return vector->toString(0, vector->size());
}

template <TypeKind T>
VectorPtr variantsToFlatVector(
    const std::vector<variant>& variants,
    memory::MemoryPool* pool,
    const TypePtr& typeOverride = nullptr);

inline VectorPtr pyListToVector(
    const pybind11::list& list,
    memory::MemoryPool* pool);

inline VectorPtr pyListToVector(
    const pybind11::list& list,
    const TypePtr& dtype,
    memory::MemoryPool* pool);

template <TypeKind T>
VectorPtr createDictionaryVector(
    BufferPtr baseVector,
    VectorPtr values,
    memory::MemoryPool* pool) {
  using NativeType = typename TypeTraits<T>::NativeType;
  size_t length = baseVector->size() / sizeof(vector_size_t);
  return std::make_shared<DictionaryVector<NativeType>>(
      pool,
      /*nulls=*/nullptr,
      length,
      std::move(values),
      std::move(baseVector));
}

template <typename NativeType>
inline pybind11::object getItemFromSimpleVector(
    SimpleVectorPtr<NativeType>& v,
    vector_size_t idx);

inline pybind11::object getItemFromVector(VectorPtr& v, vector_size_t idx);

template <typename NativeType>
inline pybind11::object getItemFromSimpleVector(
    SimpleVector<NativeType>* v,
    vector_size_t idx) {
  checkBounds(v, idx);
  if (v->isNullAt(idx)) {
    return pybind11::none();
  }
  if constexpr (std::is_same_v<NativeType, StringView>) {
    const StringView value = v->valueAt(idx);
    if (v->typeKind() == TypeKind::VARBINARY) {
      return pybind11::bytes(value.data(), value.size());
    }
    return pybind11::str(std::string_view(value));
  } else {
    return pybind11::cast(v->valueAt(idx));
  }
}

template <TypeKind Kind>
inline pybind11::object getItemFromVectorAsSimple(
    VectorPtr& v,
    vector_size_t idx) {
  using NativeType = typename TypeTraits<Kind>::NativeType;
  auto* asSimple = v->as<SimpleVector<NativeType>>();
  if (asSimple == nullptr) {
    throw pybind11::type_error("Expected SimpleVector for scalar __getitem__");
  }
  return getItemFromSimpleVector(asSimple, idx);
}

inline pybind11::object getItemFromArrayVector(
    ArrayVector* v,
    vector_size_t idx) {
  checkBounds(v, idx);
  if (v->isNullAt(idx)) {
    return pybind11::none();
  }
  const auto offset = v->offsetAt(idx);
  const auto size = v->sizeAt(idx);
  auto elements = v->elements();
  pybind11::list out;
  for (vector_size_t i = 0; i < size; i++) {
    out.append(getItemFromVector(elements, offset + i));
  }
  return out;
}

inline pybind11::object getItemFromArrayVector(
    const ArrayVectorPtr& v,
    vector_size_t idx) {
  return getItemFromArrayVector(v.get(), idx);
}

inline pybind11::object getItemFromMapVector(MapVector* v, vector_size_t idx) {
  checkBounds(v, idx);
  if (v->isNullAt(idx)) {
    return pybind11::none();
  }
  const auto offset = v->offsetAt(idx);
  const auto size = v->sizeAt(idx);
  auto keys = v->mapKeys();
  auto values = v->mapValues();
  pybind11::dict out;
  for (vector_size_t i = 0; i < size; i++) {
    auto keyObj = getItemFromVector(keys, offset + i);
    if (keyObj.is_none()) {
      throw pybind11::value_error("Map key cannot be None");
    }
    out[keyObj] = getItemFromVector(values, offset + i);
  }
  return out;
}

inline pybind11::object getItemFromMapVector(
    const MapVectorPtr& v,
    vector_size_t idx) {
  return getItemFromMapVector(v.get(), idx);
}

inline pybind11::object getItemFromVector(VectorPtr& v, vector_size_t idx) {
  checkBounds(v, idx);
  switch (v->typeKind()) {
    case TypeKind::ARRAY: {
      return getItemFromArrayVector(v->as<ArrayVector>(), idx);
    }
    case TypeKind::MAP: {
      return getItemFromMapVector(v->as<MapVector>(), idx);
    }
    default:
      return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          getItemFromVectorAsSimple, v->typeKind(), v, idx);
  }
}

template <typename NativeType>
inline void setItemInFlatVector(
    FlatVectorPtr<NativeType>& v,
    vector_size_t idx,
    pybind11::handle& obj);

inline void appendVectors(VectorPtr& u, VectorPtr& v) {
  if (u->typeKind() != v->typeKind()) {
    throw pybind11::type_error(
        "Tried to append vectors of two different types");
  }
  u->append(v.get());
}

VectorPtr evaluateExpression(
    std::shared_ptr<const core::IExpr>& expr,
    std::vector<std::string> names,
    std::vector<VectorPtr>& inputs);

struct DictionaryIndices {
  const BufferPtr indices;
};

template <>
inline void checkBounds(DictionaryIndices& indices, vector_size_t idx) {
  if (idx < 0 || idx >= (indices.indices->size() / sizeof(vector_size_t))) {
    throw std::out_of_range("Index out of range");
  }
}

// Currently PyBolt will only register vectors for primitive types.
template <TypeKind T>
void registerTypedVectors(pybind11::module& m, bool asModuleLocalDefinitions) {
  using NativeType = typename TypeTraits<T>::NativeType;
  const std::string typeName = TypeTraits<T>::name;
  pybind11::
      class_<SimpleVector<NativeType>, SimpleVectorPtr<NativeType>, BaseVector>(
          m,
          ("SimpleVector_" + typeName).c_str(),
          pybind11::module_local(asModuleLocalDefinitions))
          .def(
              "__getitem__",
              [](SimpleVectorPtr<NativeType> v, vector_size_t idx) {
                return getItemFromSimpleVector(v, idx);
              })
          .def(
              "__getitem__",
              [](std::shared_ptr<SimpleVector<NativeType>> v,
                 pybind11::slice slice) {
                size_t start, stop, step, length;
                if (!slice.compute(v->size(), &start, &stop, &step, &length)) {
                  throw pybind11::error_already_set();
                }
                if (step != 1) {
                  PyErr_SetString(
                      PyExc_NotImplementedError,
                      "Slicing with step other than 1 is not supported");
                  throw pybind11::error_already_set();
                }
                return v->slice(start, length);
              });

  pybind11::class_<
      FlatVector<NativeType>,
      FlatVectorPtr<NativeType>,
      SimpleVector<NativeType>>(
      m,
      ("FlatVector_" + typeName).c_str(),
      pybind11::module_local(asModuleLocalDefinitions))
      .def(
          "__setitem__",
          [](FlatVectorPtr<NativeType> v,
             vector_size_t idx,
             pybind11::handle& obj) { setItemInFlatVector(v, idx, obj); })
      .def(
          "__eq__",
          [](FlatVectorPtr<NativeType> u, FlatVectorPtr<NativeType> v) {
            return compareFlatVector(u, v);
          })
      .def("__str__", [](FlatVectorPtr<NativeType> v) {
        return v->toString(0, v->size());
      });

  pybind11::class_<
      ConstantVector<NativeType>,
      ConstantVectorPtr<NativeType>,
      SimpleVector<NativeType>>(
      m,
      ("ConstantVector_" + typeName).c_str(),
      pybind11::module_local(asModuleLocalDefinitions));

  pybind11::class_<
      DictionaryVector<NativeType>,
      DictionaryVectorPtr<NativeType>,
      SimpleVector<NativeType>>(
      m,
      ("DictionaryVector_" + typeName).c_str(),
      pybind11::module_local(asModuleLocalDefinitions))
      .def(
          "indices",
          [](DictionaryVectorPtr<NativeType> vec) {
            return DictionaryIndices{vec->indices()};
          })
      .def("values", [](DictionaryVectorPtr<NativeType> vec) {
        return vec->valueVector();
      });
}

template <TypeKind T>
VectorPtr variantToConstantVector(
    const variant& variant,
    vector_size_t length,
    memory::MemoryPool* pool,
    const TypePtr& typeOverride) {
  using NativeType = typename TypeTraits<T>::NativeType;

  TypePtr typePtr = typeOverride ? typeOverride : createScalarType(T);
  if (!variant.hasValue()) {
    return std::make_shared<ConstantVector<NativeType>>(
        pool,
        length,
        /*isNull=*/true,
        typePtr,
        NativeType{});
  }

  NativeType value;
  if constexpr (std::is_same_v<NativeType, StringView>) {
    const std::string& str = variant.value<std::string>();
    value = StringView(str);
  } else {
    value = variant.value<NativeType>();
  }
  auto result = std::make_shared<ConstantVector<NativeType>>(
      pool,
      length,
      /*isNull=*/false,
      typePtr,
      std::move(value));
  return result;
}

VectorPtr pyToConstantVector(
    const pybind11::handle& obj,
    vector_size_t length,
    memory::MemoryPool* pool,
    const TypePtr& type) {
  if (obj.is_none() && !type) {
    throw pybind11::type_error("Cannot infer type of constant None vector");
  }
  variant variant = pyToVariant(obj);
  TypeKind kind = variant.kind();
  if (type) {
    kind = type->kind();
    if (!obj.is_none()) {
      variant = VariantConverter::convert(variant, type->kind());
    }
  }
  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      variantToConstantVector, kind, variant, length, pool, type);
}

template <TypeKind T>
VectorPtr variantsToFlatVector(
    const std::vector<variant>& variants,
    memory::MemoryPool* pool,
    const TypePtr& typeOverride) {
  using NativeType = typename TypeTraits<T>::NativeType;
  constexpr bool kNeedsHolder =
      (T == TypeKind::VARCHAR || T == TypeKind::VARBINARY);

  TypePtr type = typeOverride ? typeOverride : createScalarType(T);
  auto result =
      BaseVector::create<FlatVector<NativeType>>(type, variants.size(), pool);

  std::conditional_t<kNeedsHolder, StringViewBufferHolder, memory::MemoryPool*>
      holder{pool};
  for (int i = 0; i < variants.size(); i++) {
    if (variants[i].isNull()) {
      result->setNull(i, true);
    } else {
      if constexpr (kNeedsHolder) {
        StringView view;
        if constexpr (T == TypeKind::VARBINARY) {
          view = holder.getOwnedValue(variants[i].value<Varbinary>());
        } else {
          view = holder.getOwnedValue(variants[i].value<std::string>());
        }
        result->set(i, view);
      } else {
        result->set(i, variants[i].value<NativeType>());
      }
    }
  }
  return result;
}

VectorPtr pyListToVector(const pybind11::list& list, memory::MemoryPool* pool) {
  std::vector<variant> variants;
  variants.reserve(list.size());
  for (auto item : list) {
    variants.push_back(pyToVariant(item));
  }

  if (variants.empty()) {
    throw pybind11::value_error(
        "Can't create a Bolt vector from an empty list");
  }

  TypeKind firstKind = TypeKind::INVALID;
  for (variant& var : variants) {
    if (var.hasValue()) {
      if (firstKind == TypeKind::INVALID) {
        firstKind = var.kind();
      } else if (var.kind() != firstKind) {
        throw pybind11::type_error(
            "Bolt Vector must consist of items of the same type");
      }
    }
  }

  if (firstKind == TypeKind::INVALID) {
    throw pybind11::value_error(
        "Can't create a Bolt vector consisting of only None");
  }
  if (firstKind == TypeKind::ARRAY || firstKind == TypeKind::MAP) {
    return variantsToVector(variants, pool);
  }

  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      variantsToFlatVector, firstKind, variants, pool);
}

VectorPtr pyListToVector(
    const pybind11::list& list,
    const TypePtr& dtype,
    memory::MemoryPool* pool) {
  std::vector<variant> variants;
  variants.reserve(list.size());
  for (auto item : list) {
    variants.push_back(pyToVariant(item, *dtype));
  }

  if (dtype->kind() == TypeKind::ARRAY || dtype->kind() == TypeKind::MAP) {
    ElementCounter counter;
    for (const auto& v : variants) {
      countElementsForType(v, dtype, counter);
    }
    VectorPtr resultVector = BaseVector::create(dtype, variants.size(), pool);
    for (const auto& v : variants) {
      insertVariantIntoVector(v, resultVector, counter, 0, 0);
    }
    return resultVector;
  }

  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      variantsToFlatVector, dtype->kind(), variants, pool, dtype);
}

template <typename NativeType>
inline pybind11::object getItemFromSimpleVector(
    SimpleVectorPtr<NativeType>& vector,
    vector_size_t idx) {
  checkBounds(vector, idx);
  if (vector->isNullAt(idx)) {
    return pybind11::none();
  }
  if constexpr (std::is_same_v<NativeType, StringView>) {
    const StringView value = vector->valueAt(idx);
    if (vector->typeKind() == TypeKind::VARBINARY) {
      return pybind11::bytes(value.data(), value.size());
    }
    return pybind11::str(std::string_view(value));
    pybind11::str result = std::string_view(value);
    return result;
  } else {
    pybind11::object result = pybind11::cast(vector->valueAt(idx));
    return result;
  }
}

template <typename NativeType>
inline void setItemInFlatVector(
    FlatVectorPtr<NativeType>& vector,
    vector_size_t idx,
    pybind11::handle& obj) {
  checkBounds(vector, idx);

  variant var = pyToVariant(obj);
  if (var.kind() == TypeKind::INVALID) {
    return vector->setNull(idx, true);
  }

  if (var.kind() != vector->typeKind()) {
    throw pybind11::type_error("Attempted to insert value of mismatched types");
  }

  vector->set(idx, NativeType{var.value<NativeType>()});
}

inline void setItemInComplexVector(
    VectorPtr& vector,
    vector_size_t idx,
    pybind11::handle& obj) {
  checkBounds(vector, idx);
  if (obj.is_none()) {
    vector->setNull(idx, true);
    return;
  }
  pybind11::list items;
  items.append(obj);
  auto temp = pyListToVector(items, vector->type(), vector->pool());
  vector->copy(temp.get(), idx, 0, 1);
}

VectorPtr evaluateExpression(
    std::shared_ptr<const core::IExpr>& expr,
    std::vector<std::string> names,
    std::vector<VectorPtr>& inputs) {
  using namespace ::bytedance::bolt;
  if (names.size() != inputs.size()) {
    throw pybind11::value_error(
        "Must specify the same number of names as inputs");
  }
  PyBoltContext& ctx = PyBoltContext::getSingletonInstance();
  vector_size_t numRows = inputs.empty() ? 0 : inputs[0]->size();
  std::vector<std::shared_ptr<const Type>> types;
  types.reserve(inputs.size());
  for (const auto& vector : inputs) {
    types.push_back(vector->type());
    if (vector->size() != numRows) {
      throw pybind11::value_error("Inputs must have matching number of rows");
    }
  }
  auto rowType = ROW(std::move(names), std::move(types));
  memory::MemoryPool* pool = ctx.pool();
  RowVectorPtr rowVector = std::make_shared<RowVector>(
      pool, rowType, BufferPtr{nullptr}, numRows, inputs);
  core::TypedExprPtr typed = core::Expressions::inferTypes(expr, rowType, pool);
  exec::ExprSet set({typed}, ctx.execCtx());
  exec::EvalCtx evalCtx(ctx.execCtx(), &set, rowVector.get());
  SelectivityVector rows(numRows);
  std::vector<VectorPtr> result;
  set.eval(rows, evalCtx, result);
  return result[0];
}
} // namespace

namespace bytedance::bolt::python {
void addVectorBindings( // NOLINT
    pybind11::module& m,
    bool asModuleLocalDefinitions) {
  pybind11::class_<BaseVector, VectorPtr>(
      m, "BaseVector", pybind11::module_local(asModuleLocalDefinitions))
      .def("__str__", [](VectorPtr& v) { return v->toString(); })
      .def("__len__", &BaseVector::size)
      .def("size", &BaseVector::size)
      .def("dtype", &BaseVector::type)
      .def("typeKind", &BaseVector::typeKind)
      .def("encoding", &BaseVector::encoding)
      .def("isLazy", &BaseVector::isLazy)
      .def("mayHaveNulls", &BaseVector::mayHaveNulls)
      .def(
          "setNull",
          [](VectorPtr& v, vector_size_t idx) {
            checkBounds(v, idx);
            return v->setNull(idx, true);
          })
      .def(
          "isNullAt",
          [](VectorPtr& v, vector_size_t idx) {
            checkBounds(v, idx);
            return v->isNullAt(idx);
          })
      .def(
          "hashValueAt",
          [](VectorPtr& v, vector_size_t idx) {
            checkBounds(v, idx);
            return v->hashValueAt(idx);
          })
      .def("append", [](VectorPtr& u, VectorPtr& v) { appendVectors(u, v); })
      .def(
          "slice",
          [](VectorPtr& u,
             vector_size_t start,
             vector_size_t stop,
             vector_size_t step) {
            if (step != 1) {
              PyErr_SetString(
                  PyExc_NotImplementedError,
                  "Slicing with step other than 1 is not supported");
              throw pybind11::error_already_set();
            }
            return u->slice(start, stop - start);
          },
          pybind11::arg("start"),
          pybind11::arg("stop"),
          pybind11::arg("step") = 1)
      .def("copy", [](VectorPtr& u) -> VectorPtr {
        auto copy = BaseVector::create(
            u->type(), u->size(), PyBoltContext::getSingletonInstance().pool());
        copy->copy(u.get(), 0, 0, u->size());
        return copy;
      });

  pybind11::class_<ArrayVector, ArrayVectorPtr, BaseVector>(
      m, "ArrayVector", pybind11::module_local(asModuleLocalDefinitions))
      .def(
          "elements",
          [](const ArrayVectorPtr& vec) -> VectorPtr {
            return vec->elements();
          })
      .def("__len__", &BaseVector::size)
      .def(
          "__getitem__",
          [](ArrayVectorPtr& v, vector_size_t idx) {
            return getItemFromArrayVector(v, idx);
          })
      .def(
          "__eq__",
          [](ArrayVectorPtr u, ArrayVectorPtr v) {
            return compareComplexVector(u, v);
          })
      .def(
          "__setitem__",
          [](ArrayVectorPtr& v, vector_size_t idx, pybind11::handle& obj) {
            VectorPtr base = v;
            setItemInComplexVector(base, idx, obj);
          })
      .def(
          "to_numpy",
          [](const ArrayVectorPtr& v, std::vector<int64_t> shape) {
            if (shape.empty()) {
              throw pybind11::value_error("to_numpy: shape must be non-empty");
            }
            for (auto d : shape) {
              if (d <= 0) {
                throw pybind11::value_error(
                    "to_numpy: shape dimensions must be positive");
              }
            }
            std::vector<int64_t> resolvedShape = std::move(shape);

            if (v->mayHaveNulls()) {
              throw pybind11::value_error(
                  "to_numpy on an ArrayVector with nulls is not supported");
            }

            const vector_size_t numRows = v->size();
            int64_t listSize = 1;
            for (auto d : resolvedShape) {
              listSize *= d;
            }

            // Verify dense uniform layout. Cheap check, important: any
            // intermediate operator that re-laid out the rows (e.g.
            // dictionary-encoded selection) would break the zero-copy
            // assumption.
            const auto* offsets = v->offsets()->template as<vector_size_t>();
            const auto* sizes = v->sizes()->template as<vector_size_t>();
            for (vector_size_t i = 0; i < numRows; ++i) {
              if (offsets[i] != static_cast<vector_size_t>(i * listSize) ||
                  sizes[i] != static_cast<vector_size_t>(listSize)) {
                throw pybind11::value_error(
                    "to_numpy: ArrayVector layout is not the dense "
                    "uniform layout required for zero-copy export "
                    "(or shape product disagrees with per-row list "
                    "size). Re-pack via fromNumpy(arrayVec, type) first.");
              }
            }

            // The elements vector must be a FlatVector of the matching
            // scalar kind, with a single contiguous values buffer.
            auto elements = v->elements();
            const auto elementKind = elements->typeKind();
            if (elements->encoding() != VectorEncoding::Simple::FLAT) {
              throw pybind11::value_error(
                  "to_numpy: elements vector is not FLAT-encoded; "
                  "zero-copy export not possible");
            }

            // Get the contiguous values buffer. Explicit switch instead of
            // BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH because that macro returns
            // a value of TEMPLATE_FUNC's return type, and we want a
            // BufferPtr while only some kinds are tensor-bridge-eligible.
            BufferPtr valuesBuf =
                flatValuesBufferForElementKind(elements, elementKind);

            // Build numpy shape (N, *resolvedShape).
            std::vector<pybind11::ssize_t> npShape;
            npShape.reserve(resolvedShape.size() + 1);
            npShape.push_back(static_cast<pybind11::ssize_t>(numRows));
            for (auto d : resolvedShape) {
              npShape.push_back(static_cast<pybind11::ssize_t>(d));
            }

            // Capsule keepalive: heap-allocated copy of the BufferPtr
            // (extra strong ref). Capsule destructor drops the ref.
            auto* heapPtr = new BufferPtr(valuesBuf);
            pybind11::capsule keepalive(heapPtr, [](void* p) {
              delete reinterpret_cast<BufferPtr*>(p);
            });

            // strides default to C-contiguous for the given dtype+shape.
            auto dtype = numpyDtypeForElementKind(elementKind);
            return pybind11::array(
                dtype,
                npShape,
                /*strides=*/{},
                valuesBuf->as<void>(),
                keepalive);
          },
          pybind11::arg("shape"),
          "Zero-copy export as a numpy ndarray of shape (N, *shape) "
          "and dtype matching the element kind. The column must be a "
          "plain ArrayType<element_type> with the dense uniform layout "
          "that `pybolt.fromNumpy` produces (offsets[i] == i*listSize, "
          "sizes[i] == listSize, where listSize == product(shape)). "
          "`shape` is the producer/consumer contract and is required.");

  pybind11::class_<MapVector, MapVectorPtr, BaseVector>(
      m, "MapVector", pybind11::module_local(asModuleLocalDefinitions))
      .def(
          "mapKeys",
          [](const MapVectorPtr& vec) -> VectorPtr { return vec->mapKeys(); })
      .def(
          "mapValues",
          [](const MapVectorPtr& vec) -> VectorPtr { return vec->mapValues(); })
      .def("__len__", &BaseVector::size)
      .def(
          "__getitem__",
          [](MapVectorPtr& v, vector_size_t idx) {
            return getItemFromMapVector(v, idx);
          })
      .def(
          "__eq__",
          [](MapVectorPtr u, MapVectorPtr v) {
            return compareComplexVector(u, v);
          })
      .def(
          "__setitem__",
          [](MapVectorPtr& v, vector_size_t idx, pybind11::handle& obj) {
            VectorPtr base = v;
            setItemInComplexVector(base, idx, obj);
          });

  constexpr TypeKind kSupportedTypes[] = {
      TypeKind::BOOLEAN,
      TypeKind::TINYINT,
      TypeKind::SMALLINT,
      TypeKind::INTEGER,
      TypeKind::BIGINT,
      TypeKind::REAL,
      TypeKind::DOUBLE,
      TypeKind::VARBINARY,
      TypeKind::TIMESTAMP};

  for (int i = 0; i < sizeof(kSupportedTypes) / sizeof(kSupportedTypes[0]);
       i++) {
    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        registerTypedVectors, kSupportedTypes[i], m, asModuleLocalDefinitions);
  }

  pybind11::class_<DictionaryIndices>(
      m, "DictionaryIndices", pybind11::module_local(asModuleLocalDefinitions))
      .def(
          "__len__",
          [](const DictionaryIndices& indices) {
            return (indices.indices->size()) / sizeof(vector_size_t);
          })
      .def("__getitem__", [](DictionaryIndices indices, vector_size_t idx) {
        checkBounds(indices, idx);
        return indices.indices->as<vector_size_t>()[idx];
      });
  m.def(
      "fromList",
      [](const pybind11::list& list, const TypePtr dtype = nullptr) mutable {
        if (dtype == nullptr ||
            pybind11::isinstance<pybind11::none>(pybind11::cast(dtype))) {
          return pyListToVector(
              list, PyBoltContext::getSingletonInstance().pool());
        }
        return pyListToVector(
            list, dtype, PyBoltContext::getSingletonInstance().pool());
      },
      pybind11::arg("list"),
      pybind11::arg("dtype") = nullptr);

  m.def(
      "fromNumpy",
      [](pybind11::array arr,
         std::shared_ptr<const Type> elementType,
         std::vector<int64_t> shape) -> VectorPtr {
        if (elementType == nullptr) {
          throw pybind11::type_error("fromNumpy: element_type is null");
        }
        if (shape.empty()) {
          throw pybind11::value_error("fromNumpy: shape must be non-empty");
        }
        for (auto d : shape) {
          if (d <= 0) {
            throw pybind11::value_error(
                "fromNumpy: shape dimensions must be positive");
          }
        }

        // Shape check: ndim must be 1 + len(shape).
        if (static_cast<size_t>(arr.ndim()) != shape.size() + 1) {
          throw pybind11::value_error(
              "fromNumpy: array ndim " + std::to_string(arr.ndim()) +
              " incompatible with declared shape ndim " +
              std::to_string(shape.size() + 1));
        }
        for (size_t i = 0; i < shape.size(); ++i) {
          if (arr.shape(i + 1) != shape[i]) {
            throw pybind11::value_error(
                "fromNumpy: array shape mismatch at axis " +
                std::to_string(i + 1) + ": got " +
                std::to_string(arr.shape(i + 1)) + ", want " +
                std::to_string(shape[i]));
          }
        }

        // Dtype check against the declared element_type.
        auto wantDtype = numpyDtypeForElementKind(elementType->kind());
        if (!arr.dtype().is(wantDtype)) {
          throw pybind11::type_error(
              std::string("fromNumpy: array dtype mismatch; want ") +
              pybind11::str(wantDtype).cast<std::string>() + ", got " +
              pybind11::str(arr.dtype()).cast<std::string>());
        }

        // Contiguity check. We rely on np.frombuffer-style C-contiguous
        // layout to interpret the elements buffer as a flat run of
        // `N * listSize` elements.
        if (!(arr.flags() & pybind11::array::c_style)) {
          throw pybind11::value_error(
              "fromNumpy: array must be C-contiguous; call "
              "np.ascontiguousarray() first if needed");
        }

        const auto numRows = static_cast<vector_size_t>(arr.shape(0));
        int64_t listSize = 1;
        for (auto d : shape) {
          listSize *= d;
        }

        NumpyReleaser releaser{arr.ptr()};
        const auto sizeBytes = static_cast<size_t>(arr.nbytes());
        BufferPtr numpyBuf = BufferView<NumpyReleaser>::create(
            static_cast<const uint8_t*>(arr.data()), sizeBytes, releaser);

        return makeTensorArrayVector(
            std::move(numpyBuf),
            numRows,
            elementType,
            listSize,
            PyBoltContext::getSingletonInstance().pool());
      },
      pybind11::arg("array"),
      pybind11::arg("element_type"),
      pybind11::arg("shape"),
      "Zero-copy import a numpy ndarray of shape (N, *shape) and dtype "
      "matching `element_type` as an ARRAY<element_type>-typed "
      "ArrayVector with dense uniform rows. The returned vector keeps "
      "the input array alive. See @tensorFunction in boltml.function for "
      "the sugar decorator that pairs this with vector UDFs.");
  m.def(
      "constantVector",
      [](const pybind11::handle& obj,
         vector_size_t length,
         const TypePtr& type) {
        return pyToConstantVector(
            obj, length, PyBoltContext::getSingletonInstance().pool(), type);
      },
      pybind11::arg("value"),
      pybind11::arg("length"),
      pybind11::arg("type") = nullptr);

  m.def(
      "dictionaryVector",
      [](VectorPtr baseVector, const pybind11::list& indicesList) {
        BufferPtr indicesBuffer = AlignedBuffer::allocate<vector_size_t>(
            indicesList.size(), PyBoltContext::getSingletonInstance().pool());
        auto* indicesPtr = indicesBuffer->asMutable<vector_size_t>();
        for (size_t i = 0; i < indicesList.size(); i++) {
          if (!pybind11::isinstance<pybind11::int_>(indicesList[i]))
            throw pybind11::type_error("Found an index that's not an integer");
          vector_size_t idx = pybind11::cast<vector_size_t>(indicesList[i]);
          checkBounds(baseVector, idx);
          indicesPtr[i] = pybind11::cast<vector_size_t>(indicesList[i]);
        }
        return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
            createDictionaryVector,
            baseVector->typeKind(),
            std::move(indicesBuffer),
            std::move(baseVector),
            PyBoltContext::getSingletonInstance().pool());
      });
  m.def(
      "rowVector",
      [](const RowTypePtr& rowType) {
        auto* pool = PyBoltContext::getSingletonInstance().pool();
        std::vector<VectorPtr> children;
        for (const auto& t : rowType->children()) {
          children.emplace_back(BaseVector::create(t, 0, pool));
        }
        return std::make_shared<RowVector>(pool, rowType, nullptr, 0, children);
      },
      pybind11::arg("dtype"),
      "Create an empty RowVector");
  m.def(
      "rowVector",
      [](std::vector<std::string>& names,
         std::vector<VectorPtr>& children,
         const std::optional<pybind11::dict>& nullabilityDict) {
        if (children.empty() || names.size() == 0) {
          throw pybind11::value_error("RowVector must have children.");
        }
        std::vector<std::shared_ptr<const Type>> childTypes;
        childTypes.reserve(children.size());

        size_t vectorSize = children[0]->size();
        for (int i = 0; i < children.size(); i++) {
          if (i > 0 && children[i]->size() != vectorSize) {
            PyErr_SetString(PyExc_ValueError, "Each child must have same size");
            throw pybind11::error_already_set();
          }
          childTypes.push_back(children[i]->type());
        }
        auto rowType = ROW(std::move(names), std::move(childTypes));

        BufferPtr nullabilityBuffer = nullptr;
        if (nullabilityDict.has_value()) {
          auto nullabilityValues = nullabilityDict.value();
          nullabilityBuffer = AlignedBuffer::allocate<bool>(
              vectorSize, PyBoltContext::getSingletonInstance().pool(), true);
          for (const auto&& item : nullabilityValues) {
            auto row = item.first;
            auto nullability = item.second;
            if (!pybind11::isinstance<pybind11::int_>(row) ||
                !pybind11::isinstance<pybind11::bool_>(nullability)) {
              throw pybind11::type_error(
                  "Nullability must be a dictionary, rowId in int and nullability in boolean.");
            }
            int rowId = pybind11::cast<int>(row);
            if (rowId < 0 || rowId >= vectorSize) {
              throw pybind11::type_error("Nullability index out of bounds.");
            }
            bool nullabilityVal = pybind11::cast<bool>(nullability);
            bits::setBit(
                nullabilityBuffer->asMutable<uint64_t>(),
                rowId,
                bits::kNull ? nullabilityVal : !nullabilityVal);
          }
        }

        return std::make_shared<RowVector>(
            PyBoltContext::getSingletonInstance().pool(),
            rowType,
            nullabilityBuffer,
            vectorSize,
            children);
      },
      pybind11::arg("names"),
      pybind11::arg("children"),
      pybind11::arg("nullability") = std::nullopt);

  pybind11::class_<RowVector, BaseVector, RowVectorPtr>(
      m, "RowVector", pybind11::module_local(asModuleLocalDefinitions))
      .def("__len__", [](RowVectorPtr& v) { return v->size(); })
      .def("__str__", [](RowVectorPtr& v) { return rowVectorToString(v); })
      .def(
          "__eq__",
          [](RowVectorPtr& u, RowVectorPtr& v) {
            return compareRowVector(u, v);
          })
      .def("childAt", [](RowVectorPtr& u, const std::string& s) {
        return u->childAt(s);
      });
}

} // namespace bytedance::bolt::python
// NOLINTEND(misc-no-recursion)
