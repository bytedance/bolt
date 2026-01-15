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
  vector_size_t aggregatedArraySize_ = 0;
  vector_size_t aggregatedArrayIndex_ = -1;
  bool initialized = false;
};

/// ArrayAdditionAggregate is a UDAF function and is used to calculate the
/// addition between two arrays. This UDAF adds given arrays element-wise. The
/// shapes of two arrays must be the same.
template <typename TInput, typename ResultType>
class ArrayAdditionAggregate : public exec::Aggregate {
 public:
  explicit ArrayAdditionAggregate(TypePtr resultType) : Aggregate(resultType) {}

  ~ArrayAdditionAggregate() {
    auto size = resultVectors_.size();
    for (int i = 0; i < size; ++i) {
      resultVectors_[i] = nullptr;
    }
    resultVectors_.clear();
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
      auto accumulator = value<ArrayAccumulator>(group);
      auto aggregatedArrayIndex = accumulator->aggregatedArrayIndex_;
      auto arraySize = accumulator->aggregatedArraySize_;

      if (arraySize > 0 && aggregatedArrayIndex > -1) {
        clearNull(rawNulls, i);

        auto aggregatedArrayVec = resultVectors_[aggregatedArrayIndex];
        elements->copy(aggregatedArrayVec.get(), offset, 0, arraySize);

        vector->setOffsetAndSize(i, offset, arraySize);
        offset += arraySize;
        resultVectors_[aggregatedArrayIndex] = nullptr;
        accumulator->aggregatedArrayIndex_ = -1;
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
    auto size = resultVectors_.size();
    for (int i = 0; i < size; ++i) {
      resultVectors_[i] = nullptr;
    }
    resultVectors_.clear();
  }

 private:
  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<ArrayAccumulator>(groups[i])->aggregatedArraySize_;
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
    auto values = arrayVector->elements()->asUnchecked<FlatVector<TValue>>();

    rows.applyToSelected([&](vector_size_t row) {
      if (!decoded.isNullAt(row)) {
        auto group = groups[row];
        auto decodedRow = decoded.index(row);
        auto offset = arrayVector->offsetAt(decodedRow);
        auto size = arrayVector->sizeAt(decodedRow);
        auto accumulator = value<ArrayAccumulator>(group);
        bool initialized = accumulator->initialized;

        if (!initialized) {
          resultVectors_.emplace_back(BaseVector::create(
              CppToType<ResultType>::create(), size, allocator_->pool()));
          accumulator->aggregatedArrayIndex_ = resultVectors_.size() - 1;
          accumulator->aggregatedArraySize_ = size;
        }

        auto aggregatedArraySize = accumulator->aggregatedArraySize_;
        // Check the shapes of all input arrays are the same.
        BOLT_CHECK_EQ(aggregatedArraySize, size);
        auto arrayIndex = accumulator->aggregatedArrayIndex_;
        auto aggregatedVec =
            resultVectors_[arrayIndex]->template as<FlatVector<ResultType>>();

        int resultIndex = 0;
        for (int index = offset; index < offset + size; ++index) {
          bool isInputNull = values->isNullAt(index);
          bool isResNull = aggregatedVec->isNullAt(resultIndex);

          if (!isInputNull) {
            ResultType value = values->valueAt(index);
            value += initialized && !isResNull
                ? aggregatedVec->valueAt(resultIndex)
                : 0;
            aggregatedVec->set(resultIndex, value);
          } else if (isInputNull && !initialized) {
            aggregatedVec->setNull(resultIndex, true);
          }

          ++resultIndex;
        }
        accumulator->initialized = true;
      }
    });
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
    auto elements = arrayVector->elements()->asUnchecked<FlatVector<TValue>>();

    rows.applyToSelected([&](vector_size_t row) {
      if (!decoded.isNullAt(row)) {
        auto decodedRow = decoded.index(row);
        auto offset = arrayVector->offsetAt(decodedRow);
        auto size = arrayVector->sizeAt(decodedRow);
        auto accumulator = value<ArrayAccumulator>(group);
        bool initialized = accumulator->initialized;

        if (!initialized) {
          resultVectors_.emplace_back(BaseVector::create(
              CppToType<ResultType>::create(), size, allocator_->pool()));
          accumulator->aggregatedArrayIndex_ = resultVectors_.size() - 1;
          accumulator->aggregatedArraySize_ = size;
        }

        auto aggregatedArraySize = accumulator->aggregatedArraySize_;
        // Check the shapes of all input arrays are the same.
        BOLT_CHECK_EQ(aggregatedArraySize, size);
        auto arrayIndex = value<ArrayAccumulator>(group)->aggregatedArrayIndex_;
        auto aggregatedVec =
            resultVectors_[arrayIndex]->template as<FlatVector<ResultType>>();

        int resultIndex = 0;
        for (int index = offset; index < offset + size; ++index) {
          bool isInputNull = elements->isNullAt(index);
          bool isResNull = aggregatedVec->isNullAt(resultIndex);

          if (!isInputNull) {
            ResultType value = elements->valueAt(index);
            value += initialized && !isResNull
                ? aggregatedVec->valueAt(resultIndex)
                : 0;
            aggregatedVec->set(resultIndex, value);
          } else if (isInputNull && !initialized) {
            aggregatedVec->setNull(resultIndex, true);
          }

          ++resultIndex;
        }
        value<ArrayAccumulator>(group)->initialized = true;
      }
    });
  }

  // DecodedVector for decoding raw and intermediate inputs.
  DecodedVector decodedElements_;
  DecodedVector decodedIntermediate_;

  // Aggregated Array Vectors for all groups.
  std::vector<VectorPtr> resultVectors_;
};

bool registerArrayAddition(const std::string& name) {
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
            return std::make_unique<ArrayAdditionAggregate<int8_t, int64_t>>(
                ARRAY(BIGINT()));
          case TypeKind::SMALLINT:
            return std::make_unique<ArrayAdditionAggregate<int16_t, int64_t>>(
                ARRAY(BIGINT()));
          case TypeKind::INTEGER:
            return std::make_unique<ArrayAdditionAggregate<int32_t, int64_t>>(
                ARRAY(BIGINT()));
          case TypeKind::BIGINT:
            return std::make_unique<ArrayAdditionAggregate<int64_t, int64_t>>(
                ARRAY(BIGINT()));
          case TypeKind::REAL:
            return std::make_unique<ArrayAdditionAggregate<float, double>>(
                ARRAY(DOUBLE()));
          case TypeKind::DOUBLE:
            return std::make_unique<ArrayAdditionAggregate<double, double>>(
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

void registerArrayAdditionAggregate(const std::string& prefix) {
  registerArrayAddition(prefix + kArrayAdditionAgg);
}

} // namespace bytedance::bolt::aggregate::prestosql
