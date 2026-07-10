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

#include "bolt/functions/sparksql/aggregates/CollectListAggregate.h"

#include <numeric>

#include "bolt/exec/Aggregate.h"
#include "bolt/functions/lib/aggregates/ValueList.h"
using namespace bytedance::bolt::aggregate;
using namespace bytedance::bolt::exec;
namespace bytedance::bolt::functions::aggregate::sparksql {
namespace {
struct CollectListAccumulator {
  ValueList elements;
};

class CollectListAggregate : public exec::Aggregate {
 public:
  explicit CollectListAggregate(TypePtr resultType)
      : exec::Aggregate(std::move(resultType)) {}

  int32_t accumulatorFixedWidthSize() const final {
    return sizeof(CollectListAccumulator);
  }

  bool isFixedSize() const final {
    return false;
  }

  bool supportsToIntermediate() const final {
    return true;
  }

  FLATTEN void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const final {
    const auto& elements = args[0];
    const auto numRows = rows.size();
    auto* pool = allocator_->pool();

    BufferPtr nulls = allocateNulls(numRows, pool);
    auto* rawNulls = nulls->asMutable<uint64_t>();
    memcpy(rawNulls, rows.asRange().bits(), bits::nbytes(numRows));

    auto loadedElements = BaseVector::loadedVectorShared(elements);
    if (loadedElements->mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t row) {
        if (loadedElements->isNullAt(row)) {
          bits::setNull(rawNulls, row);
        }
      });
    }

    BufferPtr offsets = allocateOffsets(numRows, pool);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    std::iota(rawOffsets, rawOffsets + numRows, 0);

    BufferPtr sizes = allocateSizes(numRows, pool);
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    std::fill(rawSizes, rawSizes + numRows, 1);

    result = std::make_shared<ArrayVector>(
        pool,
        ARRAY(elements->type()),
        nulls,
        numRows,
        offsets,
        sizes,
        loadedElements);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) final {
    for (auto index : indices) {
      new (groups[index] + offset_) CollectListAccumulator();
    }
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) final {
    auto vector = (*result)->as<ArrayVector>();
    BOLT_CHECK(vector);
    vector->resize(numGroups);

    auto elements = vector->elements();
    elements->resize(countElements(groups, numGroups));

    uint64_t* rawNulls = getRawNulls(vector);
    vector_size_t offset = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      auto& values = value(groups[i])->elements;
      const auto arraySize = values.size();
      clearNull(rawNulls, i);
      vector->setOffsetAndSize(i, offset, arraySize);

      if (arraySize) {
        ValueListReader reader(values);
        for (auto index = 0; index < arraySize; ++index) {
          reader.next(*elements, offset + index);
        }
        offset += arraySize;
      }
    }
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) final {
    extractValues(groups, numGroups, result);
  }

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) final {
    decodedElements_.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedElements_.isNullAt(row)) {
        return;
      }

      auto tracker = trackRowSize(groups[row]);
      value(groups[row])->elements.appendValue(
          decodedElements_, row, allocator_);
    });
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) final {
    decodedIntermediate_.decode(*args[0], rows);

    auto arrayVector = decodedIntermediate_.base()->as<ArrayVector>();
    auto elements = arrayVector->elements();
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedIntermediate_.isNullAt(row)) {
        return;
      }

      const auto decodedRow = decodedIntermediate_.index(row);
      auto tracker = trackRowSize(groups[row]);
      value(groups[row])->elements.appendRange(
          elements,
          arrayVector->offsetAt(decodedRow),
          arrayVector->sizeAt(decodedRow),
          allocator_);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) final {
    auto& values = value(group)->elements;

    decodedElements_.decode(*args[0], rows);
    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedElements_.isNullAt(row)) {
        return;
      }
      values.appendValue(decodedElements_, row, allocator_);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) final {
    decodedIntermediate_.decode(*args[0], rows);

    auto arrayVector = decodedIntermediate_.base()->as<ArrayVector>();
    auto elements = arrayVector->elements();
    auto& values = value(group)->elements;
    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedIntermediate_.isNullAt(row)) {
        return;
      }

      const auto decodedRow = decodedIntermediate_.index(row);
      values.appendRange(
          elements,
          arrayVector->offsetAt(decodedRow),
          arrayVector->sizeAt(decodedRow),
          allocator_);
    });
  }

  void destroy(folly::Range<char**> groups) final {
    for (auto group : groups) {
      value(group)->elements.free(allocator_);
    }
  }

 private:
  CollectListAccumulator* value(char* group) const {
    return reinterpret_cast<CollectListAccumulator*>(group + offset_);
  }

  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value(groups[i])->elements.size();
    }
    return size;
  }

  DecodedVector decodedElements_;
  DecodedVector decodedIntermediate_;
};

AggregateRegistrationResult registerCollectList(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("E")
          .returnType("array(E)")
          .intermediateType("array(E)")
          .argumentType("E")
          .build()};
  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_EQ(
            argTypes.size(), 1, "{} takes at most one argument", name);
        return std::make_unique<CollectListAggregate>(resultType);
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace

void registerCollectListAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerCollectList(
      prefix + "collect_list", withCompanionFunctions, overwrite);
}
} // namespace bytedance::bolt::functions::aggregate::sparksql
