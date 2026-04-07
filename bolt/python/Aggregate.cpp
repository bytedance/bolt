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
#include <pybind11/embed.h>

#include "bolt/expression/TypeSignature.h"
#include "bolt/python/Aggregate.h"
#include "bolt/python/Utils.h"
#include "bolt/vector/FlatVector.h"

using namespace bytedance::bolt;

namespace {
// pybind11::object is the intermediateType of aggregations.
// We store it in vectors as a pointer with the type below:
using IntermediateFlatType = TypeTraits<TypeKind::BIGINT>::NativeType;
// Intermediate type is stored in a pybind11::object;
// Since we can't store arbitrary types in vectors of intermediate types,
// we store a pointer to `pybind11::object` encoded on the type large enough
// to store that pointer, i.e uint64_t;
constexpr auto kSignatureIntermediateType = "bigint";

pybind11::object** asIntermediatePtrTypes(BaseVector& vec) {
  auto* flatVector = vec.as<FlatVector<IntermediateFlatType>>();
  BOLT_CHECK(flatVector != nullptr);
  auto* rawValues = flatVector->mutableRawValues();
  return reinterpret_cast<pybind11::object**>(rawValues);
}

void accumulate(pybind11::object& lhs, pybind11::object rhs) {
  bolt::python::pyTry<void>(
      [&]() { lhs.attr("accumulate")(rhs); },
      [&]() {
        return fmt::format(
            "Failure during call to 'accumulate()' method between: '{}' and: '{}'.",
            bolt::python::pyInstanceTypeStr(lhs),
            bolt::python::pyInstanceTypeStr(rhs));
      });
}

pybind11::object reduce(pybind11::object& accumulator) {
  return bolt::python::pyTry<pybind11::object>(
      [&]() { return accumulator.attr("reduce")(); },
      [&]() {
        return fmt::format(
            "Failure during call to 'reduce()' method from python object of type: '{}'.",
            bolt::python::pyInstanceTypeStr(accumulator));
      });
}

pybind11::object initAggregator(pybind11::object& cls) {
  return bolt::python::pyTry<pybind11::object>(
      [&]() { return cls(); },
      [&]() {
        return fmt::format(
            "Failure during call to '{}' python class constructor.",
            bolt::python::pyTypeStr(cls));
      });
}

pybind11::object aggregate(pybind11::object& cls, pybind11::list columns) {
  return bolt::python::pyTry<pybind11::object>(
      [&]() -> pybind11::object { return cls.attr("aggregate")(*columns); },
      [&]() {
        return fmt::format(
            "Failure during call to 'aggregate()' method of python class: '{}'.",
            bolt::python::pyTypeStr(cls));
      });
}

template <TypeKind TYPE>
void setValueAt(
    BaseVector& v,
    const vector_size_t i,
    const pybind11::object& pyVal) {
  bolt::python::pyTry<void>(
      [&]() {
        if constexpr (TYPE == TypeKind::ROW) {
          auto rhs = pyVal.cast<RowVectorPtr>();
          v.slice(i, 1)->copy(*rhs);
        }
        if constexpr (TYPE == TypeKind::MAP) {
          auto rhs = pyVal.cast<std::shared_ptr<MapVector>>();
          v.slice(i, 1)->copy(*rhs);
        }
        if constexpr (TYPE == TypeKind::ARRAY) {
          auto rhs = pyVal.cast<std::shared_ptr<ArrayVector>>();
          v.slice(i, 1)->copy(*rhs);
        }
        if constexpr (
            TYPE != TypeKind::ROW && TYPE != TypeKind::MAP &&
            TYPE != TypeKind::ARRAY) {
          auto cppVal = pyVal.cast<typename TypeTraits<TYPE>::DeepCopiedType>();
          auto* vec = v.as<FlatVector<decltype(cppVal)>>();
          BOLT_CHECK(vec != nullptr);
          vec->set(i, std::move(cppVal));
        }
      },
      [&]() {
        return fmt::format(
            "Invalid or unsupported cast of python aggregation function"
            "result type '{}' to the cpp type: '{}'.",
            bolt::python::pyInstanceTypeStr(pyVal),
            mapTypeKindToName(TYPE));
      });
}
} // namespace

namespace bolt::python {
PythonAggregate::PythonAggregate(
    pybind11::object aggregatorClass,
    const bytedance::bolt::TypePtr& returnType)
    : exec::Aggregate(returnType),
      aggregatorClass_(std::move(aggregatorClass)) {}

void PythonAggregate::initializeNewGroups(
    char** groups,
    folly::Range<const vector_size_t*> indices) {
  for (auto i : indices) {
    char* group = groups[i];

    std::unique_ptr<pybind11::object>* pyAggregator =
        value<std::unique_ptr<pybind11::object>>(groups[i]);

    // pyAggregator has not been initialized at this point.
    // If we try to set its value directly, it will call the
    // unique_ptr destructor on the non-initiliazed memory.
    // Consequently we need to initialize the bytes to a valid
    // temporary value before assigning it.
    std::unique_ptr<pybind11::object> nullPtr = nullptr;
    std::memcpy(pyAggregator, &nullPtr, sizeof(*pyAggregator));

    *pyAggregator =
        std::make_unique<pybind11::object>(initAggregator(aggregatorClass_));
    clearNull(group);
  }
}

void PythonAggregate::destroy(folly::Range<char**> groups) {
  for (char* group : groups) {
    auto& pyAggregator = *value<std::unique_ptr<pybind11::object>>(group);
    pyAggregator.reset();
  }
}

void PythonAggregate::addSingleGroupRawInput(
    char* group,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    [[maybe_unused]] bool mayPushdown) {
  if (!rows.hasSelections()) {
    return;
  }

  pybind11::object rhs = [&]() {
    pybind11::list columns;
    for (auto& vec : args) {
      vec->loadedVector();
      columns.append(pybind11::cast(vec));
    }
    return aggregate(aggregatorClass_, std::move(columns));
  }();

  auto& lhs = *value<std::unique_ptr<pybind11::object>>(group);
  accumulate(*lhs, rhs);
}

void PythonAggregate::addRawInput(
    char** groups,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    bool mayPushdown) {
  rows.applyToSelected([&](vector_size_t i) {
    auto& lhs = *value<std::unique_ptr<pybind11::object>>(groups[i]);
    pybind11::object rhs = [&]() {
      pybind11::list columns;
      for (auto& vec : args) {
        columns.append(vec->loadedVector()->slice(i, 1));
      }
      return aggregate(aggregatorClass_, std::move(columns));
    }();

    accumulate(*lhs, rhs);
  });
}

void PythonAggregate::extractAccumulators(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  if (numGroups == 0) {
    return;
  }

  pybind11::object** accumulators = asIntermediatePtrTypes(**result);
  uint64_t* rawNulls = getRawNulls(result->get());

  for (int32_t i = 0; i < numGroups; ++i) {
    char* group = groups[i];
    if (isNull(group)) {
      bits::setBit(rawNulls, i);
    } else {
      bits::clearBit(rawNulls, i);
      accumulators[i] = value<std::unique_ptr<pybind11::object>>(group)->get();
    }
  }
}

void PythonAggregate::addSingleGroupIntermediateResults(
    char* group,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    [[maybe_unused]] bool mayPushdown) {
  auto& lhs = *value<std::unique_ptr<pybind11::object>>(group);

  for (const auto& intermediate : args) {
    pybind11::object** accumulators = asIntermediatePtrTypes(*intermediate);
    rows.applyToSelected(
        [&](vector_size_t i) { accumulate(*lhs, *accumulators[i]); });
  }
}

void PythonAggregate::addIntermediateResults(
    char** groups,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    bool mayPushdown) {
  for (const auto& intermediate : args) {
    pybind11::object** accumulators = asIntermediatePtrTypes(*intermediate);
    rows.applyToSelected([&](vector_size_t i) {
      auto& lhs = *value<std::unique_ptr<pybind11::object>>(groups[i]);
      accumulate(*lhs, *accumulators[i]);
    });
  }
}

void PythonAggregate::extractValues(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  if (numGroups == 0) {
    return;
  }

  for (int32_t i = 0; i < numGroups; ++i) {
    char* group = groups[i];
    if (isNull(group)) {
      (*result)->setNull(i, true);
    } else {
      auto& accumulator = *value<std::unique_ptr<pybind11::object>>(group);
      pybind11::object pyVal = reduce(*accumulator);
      BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          ::setValueAt, resultType()->kind(), **result, i, pyVal);
    }
  }
}

void PythonAggregate::registerAggregationFunction(
    pybind11::object pyAggregatorClass,
    const std::string& fnName,
    const TypePtr& returnType,
    const size_t nArgs) {
  // Build aggregate function signature.
  auto signature = exec::AggregateFunctionSignatureBuilder()
                       .intermediateType(kSignatureIntermediateType)
                       .returnType(exec::TypeSignature(*returnType).toString());
  for (size_t i = 0; i < nArgs; i++) {
    auto varName = fmt::format("T{}", i);
    signature.typeVariable(varName).argumentType(varName);
  }

  // Create aggregate function factory.
  auto factory = [returnType, pyAggregatorClass](
                     [[maybe_unused]] core::AggregationNode::Step step,
                     [[maybe_unused]] const std::vector<TypePtr>& argTypes,
                     const TypePtr& resultType,
                     [[maybe_unused]] const core::QueryConfig& config) {
    return std::unique_ptr<Aggregate>(
        new PythonAggregate(pyAggregatorClass, returnType));
  };

  // register function.
  exec::registerAggregateFunction({fnName}, {signature.build()}, factory);
}
} // namespace bolt::python
