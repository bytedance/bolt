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

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/lib/aggregates/ValueList.h"
#include "bolt/functions/prestosql/aggregates/AggregateNames.h"
#include "bolt/vector/ComplexVector.h"
namespace bytedance::bolt::aggregate::prestosql {
namespace {

constexpr size_t kAggregatedArrayVectorSize = 1;

struct ArrayAccumulator {
  ValueList elements;
};

/// ArrayCountAggregate is a UDAF function and is used to calculate the
/// Count between two arrays. This UDAF adds given arrays element-wise. The
/// shapes of two arrays must be the same.
template <typename TInput, typename ResultType>
class ArrayCountAggregate : public exec::Aggregate {
 public:
  explicit ArrayCountAggregate(TypePtr resultType) : Aggregate(resultType) {}

  ~ArrayCountAggregate() {
    auto size = aggregatedVectors_.size();
    for (int i = 0; i < size; ++i) {
      aggregatedVectors_[i] = nullptr;
    }
    aggregatedVectors_.clear();
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ArrayAccumulator);
  }

  bool isFixedSize() const override {
    return false;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto index : indices) {
      new (groups[index] + offset_) ArrayAccumulator();
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->as<ArrayVector>();
    BOLT_CHECK(vector);
    vector->resize(numGroups);

    auto elements = vector->elements();
    elements->resize(countElements(groups, numGroups));

    uint64_t* rawNulls = getRawNulls(vector);
    vector_size_t offset = 0;

    // Copy aggregated array vector to the result vector.
    for (int32_t i = 0; i < numGroups; ++i) {
      auto group = groups[i];
      auto& values = value<ArrayAccumulator>(groups[i])->elements;
      auto aggregatedArrayIndex = values.aggregatedArrayIndex();
      auto arraySize = values.aggregatedArraySize();

      if (arraySize && aggregatedArrayIndex > -1) {
        clearNull(rawNulls, i);

        auto aggregatedArrayVec =
            aggregatedVectors_[aggregatedArrayIndex]->elements();
        next<ResultType>(
            group, aggregatedArrayVec, arraySize, elements, offset);

        vector->setOffsetAndSize(i, offset, arraySize);
        offset += arraySize;
        aggregatedVectors_[aggregatedArrayIndex] = nullptr;
        values.setAggregatedArrayIndex(-1);
      } else {
        vector->setNull(i, true);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    updateGroupAggregatedArray<ResultType>(
        decodedElements_, groups, rows, args, false);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    updateGroupAggregatedArray<ResultType>(
        decodedIntermediate_, groups, rows, args, false);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    updateSingleGroupAggregatedArray<ResultType>(
        decodedElements_, group, rows, args, false);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    updateSingleGroupAggregatedArray<ResultType>(
        decodedIntermediate_, group, rows, args, false);
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<ArrayAccumulator>(group)->elements.free(allocator_);
    }
  }

 private:
  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size +=
          value<ArrayAccumulator>(groups[i])->elements.aggregatedArraySize();
    }
    return size;
  }

  template <typename TData, typename TValue = TInput>
  void updateGroupAggregatedArray(
      DecodedVector& decoded,
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) {
    decoded.decode(*args[0], rows);
    auto arrayVector = decoded.base()->as<ArrayVector>();
    auto elements = arrayVector->elements();

    auto begin = rows.begin();
    auto end = rows.end();

    for (vector_size_t row = begin; row < end; ++row) {
      if (!decoded.isNullAt(row)) {
        auto group = groups[row];
        auto aggregatedArraySize =
            value<ArrayAccumulator>(group)->elements.aggregatedArraySize();
        auto decodedRow = decoded.index(row);
        auto offset = arrayVector->offsetAt(decodedRow);
        auto size = arrayVector->sizeAt(decodedRow);
        // Check the shapes of all input arrays are the same.
        if (aggregatedArraySize > 0) {
          BOLT_CHECK_EQ(aggregatedArraySize, size);
        }
        addToAggregatedArray<TData, TValue>(group, offset, size, elements);
      }
    }
  }

  template <typename TData, typename TValue = TInput>
  void updateSingleGroupAggregatedArray(
      DecodedVector& decoded,
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) {
    decoded.decode(*args[0], rows);
    auto arrayVector = decoded.base()->as<ArrayVector>();
    auto elements = arrayVector->elements();

    auto begin = rows.begin();
    auto end = rows.end();

    for (vector_size_t row = begin; row < end; ++row) {
      if (!decodedElements_.isNullAt(row)) {
        auto decodedRow = decodedElements_.index(row);
        auto aggregatedArraySize =
            value<ArrayAccumulator>(group)->elements.aggregatedArraySize();
        auto offset = arrayVector->offsetAt(decodedRow);
        auto size = arrayVector->sizeAt(decodedRow);
        // Check the shapes of all input arrays are the same.
        if (aggregatedArraySize > 0) {
          BOLT_CHECK_EQ(aggregatedArraySize, size);
        }
        addToAggregatedArray<TData, TValue>(group, offset, size, elements);
      }
    }
  }

  template <typename TData, typename TValue = TInput>
  void addToAggregatedArray(
      char* group,
      vector_size_t offset,
      vector_size_t size,
      const VectorPtr& vector) {
    auto accumulator = value<ArrayAccumulator>(group);
    accumulator->elements.setAggregatedArraySize(size);
    auto values = vector->asUnchecked<FlatVector<TValue>>();

    auto aggregatedArrayIndex = accumulator->elements.aggregatedArrayIndex();
    if (aggregatedArrayIndex > -1) {
      auto aggregatedVec = aggregatedVectors_[aggregatedArrayIndex]
                               ->elements()
                               ->template asUnchecked<FlatVector<TData>>();
      auto groupIndex = 0;
      for (auto index = offset; index < offset + size; ++index) {
        auto value = 1;
        if (!aggregatedVec->isNullAt(groupIndex)) {
          value = value + aggregatedVec->valueAt(groupIndex);
        }
        aggregatedVec->set(groupIndex, value);
        ++groupIndex;
      }
    } else {
      createArrayVector<TData>(group, offset, size, vector);
    }
  }

  // Create a new Array Vector as an aggregated array.
  template <typename TData, typename TValue = TInput>
  void createArrayVector(
      char* group,
      vector_size_t offset,
      vector_size_t numElements,
      const VectorPtr& vector) {
    auto accumulator = value<ArrayAccumulator>(group);
    auto values = vector->asUnchecked<FlatVector<TValue>>();

    BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(
        kAggregatedArrayVectorSize, allocator_->pool());
    BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(
        kAggregatedArrayVectorSize, allocator_->pool());

    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto rawSizes = sizes->asMutable<vector_size_t>();

    // Create a flat vector and copy group elements to the new flat vector.
    auto flatVector =
        std::dynamic_pointer_cast<FlatVector<TData>>(BaseVector::create(
            CppToType<TData>::create(), numElements, allocator_->pool()));

    auto currentIdx = 0;
    *rawSizes++ = numElements;
    *rawOffsets++ = currentIdx;

    for (auto index = offset; index < offset + numElements; ++index) {
      flatVector->set(currentIdx++, TData(1));
    }

    aggregatedVectors_.emplace_back(std::move(std::make_shared<ArrayVector>(
        allocator_->pool(),
        ARRAY(CppToType<TData>::create()),
        nullptr,
        kAggregatedArrayVectorSize,
        offsets,
        sizes,
        flatVector,
        0)));

    auto groupOffset = aggregatedVectors_.size() - 1;
    accumulator->elements.setAggregatedArrayIndex(groupOffset);
  }

  template <typename TData, typename TValue = ResultType>
  void next(
      char* group,
      VectorPtr& srcVector,
      vector_size_t srcVectorSize,
      VectorPtr& result,
      vector_size_t destOffset) {
    auto accumulator = value<ArrayAccumulator>(group);
    auto aggregatedArray = srcVector->asUnchecked<FlatVector<TValue>>();
    auto elements = result->asUnchecked<FlatVector<TData>>();

    auto fixedArraySize = accumulator->elements.aggregatedArraySize();

    for (int index = 0; index < fixedArraySize; ++index) {
      if (aggregatedArray->isNullAt(index)) {
        elements->setNull(destOffset + index, true);
      } else {
        auto value = aggregatedArray->valueAt(index);
        elements->set(destOffset + index, TData(value));
      }
    }
  }

  // DecodedVector for decoding raw and intermediate inputs.
  DecodedVector decodedElements_;
  DecodedVector decodedIntermediate_;

  // Aggregated Array Vectors for all groups.
  std::vector<std::shared_ptr<ArrayVector>> aggregatedVectors_;
};

bool registerArrayCount(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("array(double)")
          .intermediateType("array(double)")
          .argumentType("array(real)")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .returnType("array(double)")
          .intermediateType("array(double)")
          .argumentType("array(double)")
          .build()};

  for (const auto& inputType :
       {"array(tinyint)",
        "array(smallint)",
        "array(integer)",
        "array(bigint)"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("array(bigint)")
                             .intermediateType("array(bigint)")
                             .argumentType(inputType)
                             .build());
  }

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_EQ(
            argTypes.size(), 1, "{} takes at most one argument", name);
        auto inputType = argTypes[0];
        auto kind = inputType->kind();
        BOLT_CHECK_EQ(kind, TypeKind::ARRAY, "The input type must be ARRAY")
        auto child = inputType->childAt(0);
        switch (child->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<ArrayCountAggregate<int8_t, int64_t>>(
                ARRAY(BIGINT()));
          case TypeKind::SMALLINT:
            return std::make_unique<ArrayCountAggregate<int16_t, int64_t>>(
                ARRAY(BIGINT()));
          case TypeKind::INTEGER:
            return std::make_unique<ArrayCountAggregate<int32_t, int64_t>>(
                ARRAY(BIGINT()));
          case TypeKind::BIGINT:
            return std::make_unique<ArrayCountAggregate<int64_t, int64_t>>(
                ARRAY(BIGINT()));
          case TypeKind::REAL:
            return std::make_unique<ArrayCountAggregate<float, double>>(
                ARRAY(DOUBLE()));
          case TypeKind::DOUBLE:
            return std::make_unique<ArrayCountAggregate<double, double>>(
                ARRAY(DOUBLE()));
          default:
            BOLT_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
            return nullptr;
        }
      });
  return true;
}

} // namespace

void registerArrayCountAggregate(const std::string& prefix) {
  registerArrayCount(prefix + kArrayCountAgg);
}

} // namespace bytedance::bolt::aggregate::prestosql
