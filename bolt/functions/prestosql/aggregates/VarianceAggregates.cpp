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

#include "bolt/exec/Aggregate.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/prestosql/aggregates/AggregateNames.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::aggregate::prestosql {

namespace {

// Indices into Row Vector, in which we store necessary accumulator data.
constexpr int32_t kCountIdx{0};
constexpr int32_t kMeanIdx{1};
constexpr int32_t kM2Idx{2};

// Structure storing necessary data to calculate variance-based aggregations.
struct VarianceAccumulator {
  // Default (empty) ctor
  VarianceAccumulator() = default;

  // Fast construct from repetitive values.
  VarianceAccumulator(int64_t count, double value)
      : count_(count), mean_(value), m2_(0.0) {}

  int64_t count() const {
    return count_;
  }

  double mean() const {
    return mean_;
  }

  double m2() const {
    return m2_;
  }

  void update(double value) {
    count_ += 1;
    double delta = value - mean();
    mean_ += delta / count();
    m2_ += delta * (value - mean());
  }

  inline void merge(const VarianceAccumulator& other) {
    merge(other.count(), other.mean(), other.m2());
  }

  void merge(int64_t countOther, double meanOther, double m2Other) {
    if (countOther == 0) {
      return;
    }
    if (count_ == 0) {
      count_ = countOther;
      mean_ = meanOther;
      m2_ = m2Other;
      return;
    }
    int64_t newCount = countOther + count();
    double delta = meanOther - mean();
    double newMean = mean() + delta / newCount * countOther;
    m2_ += m2Other + delta * delta * countOther * count() / (double)newCount;
    count_ = newCount;
    mean_ = newMean;
  }

 private:
  int64_t count_{0};
  double mean_{0};
  double m2_{0};
};

// 'Population standard deviation' result accessor for the Variance Accumulator.
template <bool nullOnDivideByZero>
struct StdDevPopResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    return accData.count() > 0;
  }

  static double result(const VarianceAccumulator& accData) {
    return std::sqrt(accData.m2() / accData.count());
  }
};

// 'Sample standard deviation' result accessor for the Variance Accumulator.
template <bool nullOnDivideByZero>
struct StdDevSampResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    if (nullOnDivideByZero) {
      return accData.count() >= 2;
    } else {
      return accData.count() >= 1;
    }
  }

  static double result(const VarianceAccumulator& accData) {
    if (accData.count() == 1) {
      BOLT_CHECK(
          !nullOnDivideByZero,
          "NaN is returned only when m2 is 0 and nullOnDivideByZero is false.");
      return std::numeric_limits<double>::quiet_NaN();
    }
    return std::sqrt(accData.m2() / (accData.count() - 1));
  }
};

// 'Population variance' result accessor for the Variance Accumulator.
template <bool nullOnDivideByZero>
struct VarPopResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    return accData.count() > 0;
  }

  static double result(const VarianceAccumulator& accData) {
    return accData.m2() / accData.count();
  }
};

// 'Sample variance' result accessor for the Variance Accumulator.
template <bool nullOnDivideByZero>
struct VarSampResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    if (nullOnDivideByZero) {
      return accData.count() >= 2;
    } else {
      return accData.count() >= 1;
    }
  }

  static double result(const VarianceAccumulator& accData) {
    if (accData.count() == 1) {
      BOLT_CHECK(
          !nullOnDivideByZero,
          "NaN is returned only when m2 is 0 and nullOnDivideByZero is false.");
      return std::numeric_limits<double>::quiet_NaN();
    }
    return accData.m2() / (accData.count() - 1);
  }
};

// Base class for a set of Variance-based aggregations. Not used on its own,
// the classes derived from it are used instead. Partial aggregation produces
// variance struct. Final aggregation takes the variance struct and returns a
// double. T is the input type for partial aggregation. Not used for final
// aggregation. TResultAccessor is the type of the static struct that will
// access the result in a certain way from the Variance Accumulator.
template <typename T, typename TResultAccessor>
class VarianceAggregate : public exec::Aggregate {
 public:
  explicit VarianceAggregate(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(VarianceAccumulator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) VarianceAccumulator();
    }
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    auto rowVector = (*result)->as<RowVector>();
    auto countVector = rowVector->childAt(kCountIdx)->asFlatVector<int64_t>();
    auto meanVector = rowVector->childAt(kMeanIdx)->asFlatVector<double>();
    auto m2Vector = rowVector->childAt(kM2Idx)->asFlatVector<double>();

    rowVector->resize(numGroups);
    for (auto& child : rowVector->children()) {
      child->resize(numGroups);
    }
    uint64_t* rawNulls = getRawNulls(rowVector);

    int64_t* rawCounts = countVector->mutableRawValues();
    double* rawMeans = meanVector->mutableRawValues();
    double* rawM2s = m2Vector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rowVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        VarianceAccumulator* accData = accumulator(group);
        rawCounts[i] = accData->count();
        rawMeans[i] = accData->mean();
        rawM2s[i] = accData->m2();
      }
    }
  }

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedRaw_.decode(*args[0], rows);
    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        auto value = decodedRaw_.valueAt<T>(0);
        rows.applyToSelected(
            [&](vector_size_t i) { updateNonNullValue(groups[i], value); });
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedRaw_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedRaw_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedRaw_.size(),
                rows.end()));
        nulls = decodedRaw_.nulls();
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            updateNonNullValue(groups[i], decodedRaw_.valueAt<T>(i));
          },
          nulls);
    } else if (exec::Aggregate::hasNoNulls() && decodedRaw_.isIdentityMapping()) {
      auto data = decodedRaw_.data<T>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<false>(groups[i], data[i]);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue(groups[i], decodedRaw_.valueAt<T>(i));
      });
    }
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedRaw_.decode(*args[0], rows);

    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        const T value = decodedRaw_.valueAt<T>(0);
        const auto numRows = rows.countSelected();
        VarianceAccumulator accData(numRows, (double)value);
        updateNonNullValue(group, accData);
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedRaw_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedRaw_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedRaw_.size(),
                rows.end()));
        nulls = decodedRaw_.nulls();
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            updateNonNullValue(group, decodedRaw_.valueAt<T>(i));
          },
          nulls);
    } else if (exec::Aggregate::hasNoNulls() && decodedRaw_.isIdentityMapping()) {
      const T* data = decodedRaw_.data<T>();
      VarianceAccumulator accData;
      rows.applyToSelected([&](vector_size_t i) { accData.update(data[i]); });
      updateNonNullValue<false>(group, accData);
    } else {
      VarianceAccumulator accData;
      rows.applyToSelected(
          [&](vector_size_t i) { accData.update(decodedRaw_.valueAt<T>(i)); });
      updateNonNullValue(group, accData);
    }
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPartial_.base());
    auto baseCountVector =
        baseRowVector->childAt(kCountIdx)->as<SimpleVector<int64_t>>();
    auto baseMeanVector =
        baseRowVector->childAt(kMeanIdx)->as<SimpleVector<double>>();
    auto baseM2Vector =
        baseRowVector->childAt(kM2Idx)->as<SimpleVector<double>>();

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        auto count = baseCountVector->valueAt(decodedIndex);
        auto mean = baseMeanVector->valueAt(decodedIndex);
        auto m2 = baseM2Vector->valueAt(decodedIndex);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(groups[i], count, mean, m2);
        });
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedPartial_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedPartial_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedPartial_.size(),
                rows.end()));
        nulls = decodedPartial_.nulls();
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            auto decodedIndex = decodedPartial_.index(i);
            updateNonNullValue(
                groups[i],
                baseCountVector->valueAt(decodedIndex),
                baseMeanVector->valueAt(decodedIndex),
                baseM2Vector->valueAt(decodedIndex));
          },
          nulls);
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        updateNonNullValue(
            groups[i],
            baseCountVector->valueAt(decodedIndex),
            baseMeanVector->valueAt(decodedIndex),
            baseM2Vector->valueAt(decodedIndex));
      });
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPartial_.base());
    auto baseCountVector =
        baseRowVector->childAt(kCountIdx)->as<SimpleVector<int64_t>>();
    auto baseMeanVector =
        baseRowVector->childAt(kMeanIdx)->as<SimpleVector<double>>();
    auto baseM2Vector =
        baseRowVector->childAt(kM2Idx)->as<SimpleVector<double>>();

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        VarianceAccumulator accData;
        for (auto i = 0; i < rows.countSelected(); ++i) {
          accData.merge(
              baseCountVector->valueAt(decodedIndex),
              baseMeanVector->valueAt(decodedIndex),
              baseM2Vector->valueAt(decodedIndex));
        }
        updateNonNullValue(group, accData);
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedPartial_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedPartial_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedPartial_.size(),
                rows.end()));
        nulls = decodedPartial_.nulls();
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            auto decodedIndex = decodedPartial_.index(i);
            updateNonNullValue(
                group,
                baseCountVector->valueAt(decodedIndex),
                baseMeanVector->valueAt(decodedIndex),
                baseM2Vector->valueAt(decodedIndex));
          },
          nulls);
    } else {
      VarianceAccumulator accData;
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        accData.merge(
            baseCountVector->valueAt(decodedIndex),
            baseMeanVector->valueAt(decodedIndex),
            baseM2Vector->valueAt(decodedIndex));
      });
      updateNonNullValue(group, accData);
    }
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    auto vector = (*result)->as<FlatVector<double>>();
    BOLT_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);

    double* rawValues = vector->mutableRawValues();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        VarianceAccumulator* accData = accumulator(group);
        if (TResultAccessor::hasResult(*accData)) {
          clearNull(rawNulls, i);
          rawValues[i] = TResultAccessor::result(*accData);
        } else {
          vector->setNull(i, true);
        }
      }
    }
  }

 protected:
  inline VarianceAccumulator* accumulator(char* group) {
    return exec::Aggregate::value<VarianceAccumulator>(group);
  }

 private:
  // partial
  template <bool tableHasNulls = true>
  inline void updateNonNullValue(char* group, T value) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    VarianceAccumulator* thisAccData = accumulator(group);
    thisAccData->update((double)value);
  }

  template <bool tableHasNulls = true>
  inline void updateNonNullValue(
      char* group,
      const VarianceAccumulator& accData) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    VarianceAccumulator* thisAccData = accumulator(group);
    thisAccData->merge(accData);
  }

  template <bool tableHasNulls = true>
  inline void
  updateNonNullValue(char* group, int64_t count, double mean, double m2) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    VarianceAccumulator* thisAccData = accumulator(group);
    thisAccData->merge(count, mean, m2);
  }

  DecodedVector decodedRaw_;
  DecodedVector decodedPartial_;
};

// Implements 'Population Standard Deviation' aggregate.
// T is the input type for partial aggregation. Not used for final
// aggregation.
template <typename T, bool nullOnDivideByZero>
class StdDevPopAggregate
    : public VarianceAggregate<T, StdDevPopResultAccessor<nullOnDivideByZero>> {
 public:
  explicit StdDevPopAggregate(TypePtr resultType)
      : VarianceAggregate<T, StdDevPopResultAccessor<nullOnDivideByZero>>(
            resultType) {}
};

// Implements 'Sample Standard Deviation' aggregate.
// T is the input type for partial aggregation. Not used for final
// aggregation.
template <typename T, bool nullOnDivideByZero>
class StdDevSampAggregate : public VarianceAggregate<
                                T,
                                StdDevSampResultAccessor<nullOnDivideByZero>> {
 public:
  explicit StdDevSampAggregate(TypePtr resultType)
      : VarianceAggregate<T, StdDevSampResultAccessor<nullOnDivideByZero>>(
            resultType) {}
};

// Implements 'Population Variance' aggregate.
// T is the input type for partial aggregation. Not used for final
// aggregation.
template <typename T, bool nullOnDivideByZero>
class VarPopAggregate
    : public VarianceAggregate<T, VarPopResultAccessor<nullOnDivideByZero>> {
 public:
  explicit VarPopAggregate(TypePtr resultType)
      : VarianceAggregate<T, VarPopResultAccessor<nullOnDivideByZero>>(
            resultType) {}
};

// Implements 'Sample Variance' aggregate.
// T is the input type for partial aggregation. Not used for final
// aggregation.
template <typename T, bool nullOnDivideByZero>
class VarSampAggregate
    : public VarianceAggregate<T, VarSampResultAccessor<nullOnDivideByZero>> {
 public:
  explicit VarSampAggregate(TypePtr resultType)
      : VarianceAggregate<T, VarSampResultAccessor<nullOnDivideByZero>>(
            resultType) {}
};

// Registration code

void checkSumCountRowType(
    const TypePtr& type,
    const std::string& errorMessage) {
  BOLT_CHECK_EQ(type->kind(), TypeKind::ROW, "{}", errorMessage);
  BOLT_CHECK_EQ(
      type->childAt(kCountIdx)->kind(), TypeKind::BIGINT, "{}", errorMessage);
  BOLT_CHECK_EQ(
      type->childAt(kMeanIdx)->kind(), TypeKind::DOUBLE, "{}", errorMessage);
  BOLT_CHECK_EQ(
      type->childAt(kM2Idx)->kind(), TypeKind::DOUBLE, "{}", errorMessage);
}

template <template <typename TInput, bool> class TClass>
exec::AggregateRegistrationResult registerVariance(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  std::vector<std::string> inputTypes = {
      "smallint", "integer", "bigint", "real", "double"};
  for (const auto& inputType : inputTypes) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("row(bigint,double,double)")
                             .argumentType(inputType)
                             .build());
  }

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        auto inputType = argTypes[0];
        bool nullOnDivideByZero = !config.sparkLegacyStatisticalAggregate();
        if (exec::isRawInput(step)) {
          switch (inputType->kind()) {
            case TypeKind::SMALLINT:
              if (nullOnDivideByZero)
                return std::make_unique<TClass<int16_t, true>>(resultType);
              else
                return std::make_unique<TClass<int16_t, false>>(resultType);
            case TypeKind::INTEGER:
              if (nullOnDivideByZero)
                return std::make_unique<TClass<int32_t, true>>(resultType);
              else
                return std::make_unique<TClass<int32_t, false>>(resultType);
            case TypeKind::BIGINT:
              if (nullOnDivideByZero)
                return std::make_unique<TClass<int64_t, true>>(resultType);
              else
                return std::make_unique<TClass<int64_t, false>>(resultType);
            case TypeKind::REAL:
              if (nullOnDivideByZero)
                return std::make_unique<TClass<float, true>>(resultType);
              else
                return std::make_unique<TClass<float, false>>(resultType);
            case TypeKind::DOUBLE:
              if (nullOnDivideByZero)
                return std::make_unique<TClass<double, true>>(resultType);
              else
                return std::make_unique<TClass<double, false>>(resultType);
            default:
              BOLT_FAIL(
                  "Unknown input type for {} aggregation: {}",
                  name,
                  inputType->toString());
          }
        } else {
          checkSumCountRowType(
              inputType,
              "Input type for final aggregation must be "
              "(count:bigint, mean:double, m2:double) struct");
          if (nullOnDivideByZero)
            return std::make_unique<TClass<int64_t, true>>(resultType);
          else
            return std::make_unique<TClass<int64_t, false>>(resultType);
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerVarianceAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerVariance<StdDevSampAggregate>(
      prefix + kStd, withCompanionFunctions, overwrite);
  registerVariance<StdDevSampAggregate>(
      prefix + kStdDev, withCompanionFunctions, overwrite);
  registerVariance<StdDevPopAggregate>(
      prefix + kStdDevPop, withCompanionFunctions, overwrite);
  registerVariance<StdDevSampAggregate>(
      prefix + kStdDevSamp, withCompanionFunctions, overwrite);
  registerVariance<VarSampAggregate>(
      prefix + kVariance, withCompanionFunctions, overwrite);
  registerVariance<VarPopAggregate>(
      prefix + kVarPop, withCompanionFunctions, overwrite);
  registerVariance<VarSampAggregate>(
      prefix + kVarSamp, withCompanionFunctions, overwrite);
}

} // namespace bytedance::bolt::aggregate::prestosql
