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

#include <cstdint>

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>

#include "bolt/buffer/Buffer.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::python {

// `NumpyReleaser` is the lifetime hook used by Velox `BufferView<Releaser>`
// to keep a numpy ndarray alive for as long as a Velox vector aliases its
// memory.
//
// Semantics:
//   - Hold a raw `PyObject*` to the numpy array (no implicit refcounting
//     from pybind11::object — addRef()/release() drive the refcount
//     explicitly so the net count is exactly +1 over the BufferView's
//     lifetime).
//   - `addRef()` is called once by the `BufferView` constructor; the
//     caller (`fromNumpy` on the Python side) holds the GIL at that point.
//   - `release()` is called once by the `BufferView` destructor. That
//     destructor may run on any thread without the GIL (Velox memory
//     management is not Python-aware), so this method acquires the GIL
//     before touching the refcount.
//   - Both methods are `const` because Velox stores the releaser as
//     `Releaser const releaser_` (see `bolt/buffer/Buffer.h`).
struct NumpyReleaser {
  PyObject* npArray{nullptr};

  void addRef() const {
    // Caller (fromNumpy) holds the GIL when this runs.
    if (npArray != nullptr) {
      Py_INCREF(npArray);
    }
  }

  void release() const {
    // BufferView dtor may run on any thread without the GIL.
    if (npArray != nullptr) {
      pybind11::gil_scoped_acquire gil;
      Py_DECREF(npArray);
    }
  }
};

// Map a scalar element TypeKind (used as the ARRAY element type of a
// tensor column) to the matching numpy dtype.
inline pybind11::dtype numpyDtypeForElementKind(TypeKind kind) {
  switch (kind) {
    case TypeKind::TINYINT:
      return pybind11::dtype::of<int8_t>();
    case TypeKind::SMALLINT:
      return pybind11::dtype::of<int16_t>();
    case TypeKind::INTEGER:
      return pybind11::dtype::of<int32_t>();
    case TypeKind::BIGINT:
      return pybind11::dtype::of<int64_t>();
    case TypeKind::REAL:
      return pybind11::dtype::of<float>();
    case TypeKind::DOUBLE:
      return pybind11::dtype::of<double>();
    case TypeKind::BOOLEAN:
      return pybind11::dtype::of<bool>();
    default:
      throw pybind11::type_error(
          "tensor element kind not supported by numpy bridge: " +
          std::string(mapTypeKindToName(kind)));
  }
}

// Return the contiguous values buffer of a FLAT-encoded `elements`
// vector for the given scalar element `kind`. Caller must have already
// checked that `elements->encoding() == VectorEncoding::Simple::FLAT`
// and that `kind == elements->typeKind()`. Same allowlist as
// `numpyDtypeForElementKind` so the two stay in lockstep.
inline BufferPtr flatValuesBufferForElementKind(
    const VectorPtr& elements,
    TypeKind kind) {
  switch (kind) {
    case TypeKind::TINYINT:
      return elements->asFlatVector<int8_t>()->values();
    case TypeKind::SMALLINT:
      return elements->asFlatVector<int16_t>()->values();
    case TypeKind::INTEGER:
      return elements->asFlatVector<int32_t>()->values();
    case TypeKind::BIGINT:
      return elements->asFlatVector<int64_t>()->values();
    case TypeKind::REAL:
      return elements->asFlatVector<float>()->values();
    case TypeKind::DOUBLE:
      return elements->asFlatVector<double>()->values();
    case TypeKind::BOOLEAN:
      return elements->asFlatVector<bool>()->values();
    default:
      throw pybind11::type_error(
          "tensor element kind not supported by numpy bridge: " +
          std::string(mapTypeKindToName(kind)));
  }
}

// Build a FlatVector<NativeType> whose values buffer is `valuesBuffer`.
// Template — must live in a header. The TypeKind tag picks the native
// scalar storage type via `TypeTraits<Kind>::NativeType`.
template <TypeKind Kind>
VectorPtr makeFlatVectorFromValuesBuffer(
    BufferPtr valuesBuffer,
    vector_size_t length,
    TypePtr elementType,
    memory::MemoryPool* pool) {
  using NativeType = typename TypeTraits<Kind>::NativeType;
  return std::make_shared<FlatVector<NativeType>>(
      pool,
      std::move(elementType),
      /*nulls=*/BufferPtr{nullptr},
      length,
      std::move(valuesBuffer),
      /*stringBuffers=*/std::vector<BufferPtr>{});
}

// Construct an ARRAY<element_type>-typed ArrayVector of length N whose
// elements buffer aliases `numpyBuf`. Each row holds exactly `listSize`
// elements (offsets[i] == i*listSize, sizes[i] == listSize). The shape
// itself is not part of the column type — it's the producer/consumer
// contract carried in code.
//
// Marked `inline` so it can live in this header without ODR conflict
// across the multiple translation units that include `numpy.h`.
inline ArrayVectorPtr makeTensorArrayVector(
    BufferPtr numpyBuf,
    vector_size_t numRows,
    TypePtr elementType,
    int64_t listSize,
    memory::MemoryPool* pool) {
  const auto totalElements = static_cast<vector_size_t>(numRows * listSize);

  VectorPtr elements = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      makeFlatVectorFromValuesBuffer,
      elementType->kind(),
      std::move(numpyBuf),
      totalElements,
      elementType,
      pool);

  BufferPtr offsetsBuf = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  BufferPtr sizesBuf = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto* offsets = offsetsBuf->asMutable<vector_size_t>();
  auto* sizes = sizesBuf->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    offsets[i] = static_cast<vector_size_t>(i * listSize);
    sizes[i] = static_cast<vector_size_t>(listSize);
  }

  auto arrayType = std::make_shared<const ArrayType>(elementType);
  return std::make_shared<ArrayVector>(
      pool,
      arrayType,
      /*nulls=*/BufferPtr{nullptr},
      numRows,
      std::move(offsetsBuf),
      std::move(sizesBuf),
      std::move(elements));
}

} // namespace bytedance::bolt::python
