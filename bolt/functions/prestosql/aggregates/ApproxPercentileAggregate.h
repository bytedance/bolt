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

#pragma once

#include "bolt/common/base/IOUtils.h"
#include "bolt/common/base/Macros.h"
#include "bolt/common/base/RandomUtil.h"
#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/lib/KllSketch.h"
#include "bolt/functions/prestosql/aggregates/AggregateNames.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::aggregate::prestosql {

template <typename T>
using KllSketch = functions::kll::KllSketch<T, StlAllocator<T>>;

// Accumulator to buffer large count values in addition to the KLL
// sketch itself.
template <typename T>
struct KllSketchAccumulator {
  explicit KllSketchAccumulator(HashStringAllocator* allocator)
      : allocator_(allocator),
        sketch_(
            functions::kll::kDefaultK,
            StlAllocator<T>(allocator),
            random::getSeed()),
        largeCountValues_(StlAllocator<std::pair<T, int64_t>>(allocator)) {}

  void setAccuracy(double value) {
    k_ = functions::kll::kFromEpsilon(value);
    sketch_.setK(k_);
  }

  void append(T value) {
    sketch_.insert(value);
  }

  void append(T value, int64_t count) {
    constexpr size_t kMaxBufferSize = 4096;
    constexpr int64_t kMinCountToBuffer = 512;
    if (count < kMinCountToBuffer) {
      for (int i = 0; i < count; ++i) {
        sketch_.insert(value);
      }
    } else {
      largeCountValues_.emplace_back(value, count);
      if (largeCountValues_.size() >= kMaxBufferSize) {
        flush();
      }
    }
  }

  void append(const typename KllSketch<T>::View& view) {
    sketch_.mergeViews(folly::Range(&view, 1));
  }

  void append(const std::vector<typename KllSketch<T>::View>& views) {
    sketch_.mergeViews(views);
  }

  void finalize() {
    if (!largeCountValues_.empty()) {
      flush();
    }
    sketch_.compact();
  }

  const KllSketch<T>& getSketch() const {
    return sketch_;
  }

 private:
  uint16_t k_;
  HashStringAllocator* allocator_;
  KllSketch<T> sketch_;
  std::vector<std::pair<T, int64_t>, StlAllocator<std::pair<T, int64_t>>>
      largeCountValues_;

  void flush() {
    std::vector<KllSketch<T>> sketches;
    sketches.reserve(largeCountValues_.size());
    for (auto [x, n] : largeCountValues_) {
      sketches.push_back(KllSketch<T>::fromRepeatedValue(
          x, n, k_, StlAllocator<T>(allocator_), random::getSeed()));
    }
    sketch_.merge(folly::Range(sketches.begin(), sketches.end()));
    largeCountValues_.clear();
  }
};

enum IntermediateTypeChildIndex {
  kPercentiles = 0,
  kPercentilesIsArray = 1,
  kAccuracy = 2,
  kK = 3,
  kN = 4,
  kMinValue = 5,
  kMaxValue = 6,
  kItems = 7,
  kLevels = 8,
};

inline void checkWeight(int64_t weight) {
  constexpr int64_t kMaxWeight = (1ll << 60) - 1;
  BOLT_USER_CHECK(
      1 <= weight && weight <= kMaxWeight,
      "{}: weight must be in range [1, {}], got {}",
      kApproxPercentile,
      kMaxWeight,
      weight);
}

template <typename T>
class ApproxPercentileAggregate : public exec::Aggregate {
 public:
  ApproxPercentileAggregate(
      bool hasWeight,
      bool hasAccuracy,
      const TypePtr& resultType)
      : exec::Aggregate(resultType),
        hasWeight_{hasWeight},
        hasAccuracy_(hasAccuracy) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(KllSketchAccumulator<T>);
  }

  bool isFixedSize() const override {
    return false;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) KllSketchAccumulator<T>(allocator_);
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<KllSketchAccumulator<T>>(group)->~KllSketchAccumulator<T>();
    }
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    finalize(groups, numGroups);

    BOLT_CHECK(result);
    // When all inputs are nulls or masked out, percentiles_ can be
    // uninitialized. The result should be nulls in this case.
    if (!percentiles_.has_value()) {
      *result = BaseVector::createNullConstant(
          (*result)->type(), numGroups, (*result)->pool());
      return;
    }

    if (percentiles_ && percentiles_->isArray) {
      folly::Range percentiles(
          percentiles_->values.begin(), percentiles_->values.end());
      auto arrayResult = (*result)->asUnchecked<ArrayVector>();
      vector_size_t elementsCount = 0;
      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        auto accumulator = value<KllSketchAccumulator<T>>(group);
        if (accumulator->getSketch().totalCount() > 0) {
          elementsCount += percentiles.size();
        }
      }
      arrayResult->elements()->resize(elementsCount);
      elementsCount = 0;
      auto rawValues =
          arrayResult->elements()->asFlatVector<T>()->mutableRawValues();
      extract(
          groups,
          numGroups,
          arrayResult,
          [&](const KllSketch<T>& digest,
              ArrayVector* result,
              vector_size_t index) {
            digest.estimateQuantiles(percentiles, rawValues + elementsCount);
            result->setOffsetAndSize(index, elementsCount, percentiles.size());
            elementsCount += percentiles.size();
          });
    } else {
      extract(
          groups,
          numGroups,
          (*result)->asFlatVector<T>(),
          [&](const KllSketch<T>& digest,
              FlatVector<T>* result,
              vector_size_t index) {
            BOLT_DCHECK_EQ(percentiles_->values.size(), 1);
            result->set(
                index, digest.estimateQuantile(percentiles_->values.back()));
          });
    }
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    finalize(groups, numGroups);

    BOLT_CHECK(result);
    auto rowResult = (*result)->as<RowVector>();
    BOLT_CHECK(rowResult);
    auto pool = rowResult->pool();

    // percentiles_ can be uninitialized during an intermediate aggregation step
    // when all input intermediate states are nulls. Result should be nulls in
    // this case.
    if (!percentiles_) {
      rowResult->ensureWritable(SelectivityVector{numGroups});
      // rowResult->childAt(i) for i = kPercentiles, kPercentilesIsArray, and
      // kAccuracy are expected to be constant in addIntermediateResults.
      rowResult->childAt(kPercentiles) =
          BaseVector::createNullConstant(ARRAY(DOUBLE()), numGroups, pool);
      rowResult->childAt(kPercentilesIsArray) =
          BaseVector::createNullConstant(BOOLEAN(), numGroups, pool);
      rowResult->childAt(kAccuracy) =
          BaseVector::createNullConstant(DOUBLE(), numGroups, pool);

      // Set nulls for all rows.
      auto rawNulls = rowResult->mutableRawNulls();
      bits::fillBits(rawNulls, 0, rowResult->size(), bits::kNull);
      return;
    }
    auto& values = percentiles_->values;
    auto size = values.size();
    auto elements =
        BaseVector::create<FlatVector<double>>(DOUBLE(), size, pool);
    std::copy(values.begin(), values.end(), elements->mutableRawValues());
    auto array = std::make_shared<ArrayVector>(
        pool,
        ARRAY(DOUBLE()),
        nullptr,
        1,
        AlignedBuffer::allocate<vector_size_t>(1, pool, 0),
        AlignedBuffer::allocate<vector_size_t>(1, pool, size),
        std::move(elements));
    rowResult->childAt(kPercentiles) =
        BaseVector::wrapInConstant(numGroups, 0, std::move(array));
    rowResult->childAt(kPercentilesIsArray) =
        std::make_shared<ConstantVector<bool>>(
            pool, numGroups, false, BOOLEAN(), bool(percentiles_->isArray));
    rowResult->childAt(kAccuracy) = std::make_shared<ConstantVector<double>>(
        pool,
        numGroups,
        accuracy_ == kMissingNormalizedValue,
        DOUBLE(),
        double(accuracy_));
    auto k = rowResult->childAt(kK)->asFlatVector<int32_t>();
    auto n = rowResult->childAt(kN)->asFlatVector<int64_t>();
    auto minValue = rowResult->childAt(kMinValue)->asFlatVector<T>();
    auto maxValue = rowResult->childAt(kMaxValue)->asFlatVector<T>();
    auto items = rowResult->childAt(kItems)->as<ArrayVector>();
    auto levels = rowResult->childAt(kLevels)->as<ArrayVector>();

    rowResult->resize(numGroups);
    k->resize(numGroups);
    n->resize(numGroups);
    minValue->resize(numGroups);
    maxValue->resize(numGroups);
    items->resize(numGroups);
    levels->resize(numGroups);

    auto itemsElements = items->elements()->asFlatVector<T>();
    auto levelsElements = levels->elements()->asFlatVector<int32_t>();
    size_t itemsCount = 0;
    vector_size_t levelsCount = 0;
    for (int i = 0; i < numGroups; ++i) {
      auto accumulator = value<const KllSketchAccumulator<T>>(groups[i]);
      auto v = accumulator->getSketch().toView();
      itemsCount += v.items.size();
      levelsCount += v.levels.size();
    }
    BOLT_CHECK_LE(itemsCount, std::numeric_limits<vector_size_t>::max());
    itemsElements->resetNulls();
    itemsElements->resize(itemsCount);
    levelsElements->resetNulls();
    levelsElements->resize(levelsCount);

    auto rawItems = itemsElements->mutableRawValues();
    auto rawLevels = levelsElements->mutableRawValues();
    itemsCount = 0;
    levelsCount = 0;
    for (int i = 0; i < numGroups; ++i) {
      auto accumulator = value<const KllSketchAccumulator<T>>(groups[i]);
      auto v = accumulator->getSketch().toView();
      if (v.n == 0) {
        rowResult->setNull(i, true);
      } else {
        rowResult->setNull(i, false);
        k->set(i, v.k);
        n->set(i, v.n);
        minValue->set(i, v.minValue);
        maxValue->set(i, v.maxValue);
        std::copy(v.items.begin(), v.items.end(), rawItems + itemsCount);
        items->setOffsetAndSize(i, itemsCount, v.items.size());
        itemsCount += v.items.size();
        std::copy(v.levels.begin(), v.levels.end(), rawLevels + levelsCount);
        levels->setOffsetAndSize(i, levelsCount, v.levels.size());
        levelsCount += v.levels.size();
      }
    }
  }

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);

    if (hasWeight_) {
      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row) || decodedWeight_.isNullAt(row)) {
          return;
        }

        auto tracker = trackRowSize(groups[row]);
        auto accumulator = initRawAccumulator(groups[row]);
        auto value = decodedValue_.valueAt<T>(row);
        auto weight = decodedWeight_.valueAt<int64_t>(row);
        checkWeight(weight);
        accumulator->append(value, weight);
      });
    } else {
      if (decodedValue_.mayHaveNulls()) {
        const uint64_t* nulls = nullptr;
        if (decodedValue_.nulls() != nullptr) {
          BOLT_CHECK(
              decodedValue_.size() == rows.end(),
              fmt::format(
                  "decoded.size() {}!= rows.end() {}",
                  decodedValue_.size(),
                  rows.end()));
          nulls = decodedValue_.nulls();
        }
        rows.applyToSelected(
            [&](auto row) {
              auto accumulator = initRawAccumulator(groups[row]);
              accumulator->append(decodedValue_.valueAt<T>(row));
            },
            nulls);
      } else {
        rows.applyToSelected([&](auto row) {
          auto accumulator = initRawAccumulator(groups[row]);
          accumulator->append(decodedValue_.valueAt<T>(row));
        });
      }
    }
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addIntermediate<false>(groups, rows, args);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);

    auto tracker = trackRowSize(group);
    auto accumulator = initRawAccumulator(group);

    if (hasWeight_) {
      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row) || decodedWeight_.isNullAt(row)) {
          return;
        }

        auto value = decodedValue_.valueAt<T>(row);
        auto weight = decodedWeight_.valueAt<int64_t>(row);
        checkWeight(weight);
        accumulator->append(value, weight);
      });
    } else {
      if (decodedValue_.mayHaveNulls()) {
        const uint64_t* nulls = nullptr;
        if (decodedValue_.nulls() != nullptr) {
          BOLT_CHECK(
              decodedValue_.size() == rows.end(),
              fmt::format(
                  "decoded.size() {}!= rows.end() {}",
                  decodedValue_.size(),
                  rows.end()));
          nulls = decodedValue_.nulls();
        }
        rows.applyToSelected(
            [&](auto row) {
              accumulator->append(decodedValue_.valueAt<T>(row));
            },
            nulls);
      } else {
        rows.applyToSelected([&](auto row) {
          accumulator->append(decodedValue_.valueAt<T>(row));
        });
      }
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addIntermediate<true>(group, rows, args);
  }

 private:
  void finalize(char** groups, int32_t numGroups) {
    for (auto i = 0; i < numGroups; ++i) {
      value<KllSketchAccumulator<T>>(groups[i])->finalize();
    }
  }

  template <typename VectorType, typename ExtractFunc>
  void extract(
      char** groups,
      int32_t numGroups,
      VectorType* result,
      ExtractFunc extractFunction) {
    BOLT_CHECK(result);
    result->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if (result->mayHaveNulls()) {
      BufferPtr& nulls = result->mutableNulls(result->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto accumulator = value<KllSketchAccumulator<T>>(group);
      if (accumulator->getSketch().totalCount() == 0) {
        result->setNull(i, true);
      } else {
        if (rawNulls) {
          bits::setNull(rawNulls, i, false);
        }
        extractFunction(accumulator->getSketch(), result, i);
      }
    }
  }

  void decodeArguments(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    size_t argIndex = 0;
    decodedValue_.decode(*args[argIndex++], rows, true);
    if (hasWeight_) {
      decodedWeight_.decode(*args[argIndex++], rows, true);
    }
    checkSetPercentile(rows, *args[argIndex++]);
    if (hasAccuracy_) {
      decodedAccuracy_.decode(*args[argIndex++], rows, true);
      checkSetAccuracy(rows);
    }
    BOLT_CHECK_EQ(argIndex, args.size());
  }

  void checkSetPercentile(
      const SelectivityVector& rows,
      const BaseVector& vec) {
    DecodedVector decoded(vec, rows);

    auto* base = decoded.base();
    auto baseFirstRow = decoded.index(rows.begin());
    if (!decoded.isConstantMapping()) {
      rows.applyToSelected([&](vector_size_t row) {
        BOLT_USER_CHECK(!decoded.isNullAt(row), "Percentile cannot be null")
        auto baseRow = decoded.index(row);
        BOLT_USER_CHECK(
            base->equalValueAt(base, baseRow, baseFirstRow),
            "Percentile argument must be constant for all input rows: {} vs. {}",
            base->toString(baseRow),
            base->toString(baseFirstRow));
      });
    }

    bool isArray;
    vector_size_t offset;
    vector_size_t len;
    if (base->typeKind() == TypeKind::DOUBLE) {
      isArray = false;
      offset = rows.begin();
      len = 1;
    } else if (base->typeKind() == TypeKind::ARRAY) {
      isArray = true;
      auto arrays = base->asUnchecked<ArrayVector>();
      decoded.decode(*arrays->elements());
      offset = arrays->offsetAt(baseFirstRow);
      len = arrays->sizeAt(baseFirstRow);
    } else {
      BOLT_USER_FAIL(
          "Incorrect type for percentile: {}", base->type()->toString());
    }
    checkSetPercentile(isArray, decoded, offset, len);
  }

  void checkSetPercentile(
      bool isArray,
      const DecodedVector& percentiles,
      vector_size_t offset,
      vector_size_t len) {
    if (!percentiles_) {
      BOLT_USER_CHECK_GT(len, 0, "Percentile cannot be empty");
      percentiles_.emplace();
      percentiles_->values.resize(len);
      percentiles_->isArray = isArray;
      for (vector_size_t i = 0; i < len; ++i) {
        BOLT_USER_CHECK(!percentiles.isNullAt(i), "Percentile cannot be null");
        auto value = percentiles.valueAt<double>(offset + i);
        BOLT_USER_CHECK_GE(value, 0, "Percentile must be between 0 and 1");
        BOLT_USER_CHECK_LE(value, 1, "Percentile must be between 0 and 1");
        percentiles_->values[i] = value;
      }
    } else {
      BOLT_USER_CHECK_EQ(
          isArray,
          percentiles_->isArray,
          "Percentile argument must be constant for all input rows");
      BOLT_USER_CHECK_EQ(
          len,
          percentiles_->values.size(),
          "Percentile argument must be constant for all input rows");
      for (vector_size_t i = 0; i < len; ++i) {
        BOLT_USER_CHECK_EQ(
            percentiles.valueAt<double>(offset + i),
            percentiles_->values[i],
            "Percentile argument must be constant for all input rows");
      }
    }
  }

  void checkSetAccuracy(const SelectivityVector& rows) {
    if (!hasAccuracy_) {
      return;
    }

    if (decodedAccuracy_.isConstantMapping()) {
      BOLT_USER_CHECK(!decodedAccuracy_.isNullAt(0), "Accuracy cannot be null");
      checkSetAccuracy(decodedAccuracy_.valueAt<double>(0));
    } else {
      rows.applyToSelected([&](auto row) {
        BOLT_USER_CHECK(
            !decodedAccuracy_.isNullAt(row), "Accuracy cannot be null");
        const auto accuracy = decodedAccuracy_.valueAt<double>(row);
        if (accuracy_ == kMissingNormalizedValue) {
          checkSetAccuracy(accuracy);
        }
        BOLT_USER_CHECK_EQ(
            accuracy,
            accuracy_,
            "Accuracy argument must be constant for all input rows");
      });
    }
  }

  void checkSetAccuracy(double accuracy) {
    BOLT_USER_CHECK(
        0 < accuracy && accuracy <= 1, "Accuracy must be between 0 and 1");
    if (accuracy_ == kMissingNormalizedValue) {
      accuracy_ = accuracy;
    } else {
      BOLT_USER_CHECK_EQ(
          accuracy,
          accuracy_,
          "Accuracy argument must be constant for all input rows");
    }
  }

  KllSketchAccumulator<T>* initRawAccumulator(char* group) {
    auto accumulator = value<KllSketchAccumulator<T>>(group);
    if (accuracy_ != kMissingNormalizedValue) {
      accumulator->setAccuracy(accuracy_);
    }
    return accumulator;
  }

  template <bool kSingleGroup>
  void addIntermediate(
      std::conditional_t<kSingleGroup, char*, char**> group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    if (validateIntermediateInputs_) {
      addIntermediateImpl<kSingleGroup, true>(group, rows, args);
    } else {
      addIntermediateImpl<kSingleGroup, false>(group, rows, args);
    }
  }

  struct Percentiles {
    std::vector<double> values;
    bool isArray;
  };

  static constexpr double kMissingNormalizedValue = -1;
  const bool hasWeight_;
  const bool hasAccuracy_;
  std::optional<Percentiles> percentiles_;
  double accuracy_{kMissingNormalizedValue};
  DecodedVector decodedValue_;
  DecodedVector decodedWeight_;
  DecodedVector decodedAccuracy_;
  DecodedVector decodedDigest_;

 private:
  template <bool kSingleGroup, bool checkIntermediateInputs>
  void addIntermediateImpl(
      std::conditional_t<kSingleGroup, char*, char**> group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    BOLT_CHECK_EQ(args.size(), 1);
    DecodedVector decoded(*args[0], rows);
    auto rowVec = decoded.base()->as<RowVector>();
    if constexpr (checkIntermediateInputs) {
      BOLT_USER_CHECK(rowVec);
      for (int i = kPercentiles; i <= kAccuracy; ++i) {
        BOLT_USER_CHECK(rowVec->childAt(i)->isConstantEncoding());
      }
      for (int i = kK; i <= kMaxValue; ++i) {
        BOLT_USER_CHECK(rowVec->childAt(i)->isFlatEncoding());
      }
      for (int i = kItems; i <= kLevels; ++i) {
        BOLT_USER_CHECK(
            rowVec->childAt(i)->encoding() == VectorEncoding::Simple::ARRAY);
      }
    } else {
      BOLT_CHECK(rowVec);
    }

    const SelectivityVector* baseRows = &rows;
    SelectivityVector innerRows{rowVec->size(), false};
    if (!decoded.isIdentityMapping()) {
      if (decoded.isConstantMapping()) {
        innerRows.setValid(decoded.index(0), true);
        innerRows.updateBounds();
      } else {
        bolt::translateToInnerRows(
            rows, decoded.indices(), decoded.nulls(&rows), innerRows);
      }
      baseRows = &innerRows;
    }

    DecodedVector percentiles(*rowVec->childAt(kPercentiles), *baseRows);
    auto percentileIsArray =
        rowVec->childAt(kPercentilesIsArray)->asUnchecked<SimpleVector<bool>>();
    auto accuracy =
        rowVec->childAt(kAccuracy)->asUnchecked<SimpleVector<double>>();
    auto k = rowVec->childAt(kK)->asUnchecked<SimpleVector<int32_t>>();
    auto n = rowVec->childAt(kN)->asUnchecked<SimpleVector<int64_t>>();
    auto minValue = rowVec->childAt(kMinValue)->asUnchecked<SimpleVector<T>>();
    auto maxValue = rowVec->childAt(kMaxValue)->asUnchecked<SimpleVector<T>>();
    auto items = rowVec->childAt(kItems)->asUnchecked<ArrayVector>();
    auto levels = rowVec->childAt(kLevels)->asUnchecked<ArrayVector>();

    auto itemsElements = items->elements()->asFlatVector<T>();
    auto levelElements = levels->elements()->asFlatVector<int32_t>();
    if constexpr (checkIntermediateInputs) {
      BOLT_USER_CHECK(itemsElements);
      BOLT_USER_CHECK(levelElements);
    } else {
      BOLT_CHECK(itemsElements);
      BOLT_CHECK(levelElements);
    }
    auto rawItems = itemsElements->rawValues();
    auto rawLevels = levelElements->rawValues<uint32_t>();

    KllSketchAccumulator<T>* accumulator = nullptr;
    std::vector<typename KllSketch<T>::View> views;
    if constexpr (kSingleGroup) {
      views.reserve(rows.end());
    }
    rows.applyToSelected([&](auto row) {
      if (decoded.isNullAt(row)) {
        return;
      }
      int i = decoded.index(row);
      if (percentileIsArray->isNullAt(i)) {
        return;
      }
      if (!accumulator) {
        int indexInBaseVector = percentiles.index(i);
        auto percentilesBase = percentiles.base()->asUnchecked<ArrayVector>();
        auto percentileBaseElements =
            percentilesBase->elements()->asFlatVector<double>();
        if constexpr (checkIntermediateInputs) {
          BOLT_USER_CHECK(percentileBaseElements);
          BOLT_USER_CHECK(!percentilesBase->isNullAt(indexInBaseVector));
        }

        bool isArray = percentileIsArray->valueAt(i);
        DecodedVector decodedElements(*percentilesBase->elements());
        checkSetPercentile(
            isArray,
            decodedElements,
            percentilesBase->offsetAt(indexInBaseVector),
            percentilesBase->sizeAt(indexInBaseVector));

        if (!accuracy->isNullAt(i)) {
          checkSetAccuracy(accuracy->valueAt(i));
        }
      }
      if constexpr (kSingleGroup) {
        if (!accumulator) {
          accumulator = initRawAccumulator(group);
        }
      } else {
        accumulator = initRawAccumulator(group[row]);
      }

      if constexpr (checkIntermediateInputs) {
        BOLT_USER_CHECK(
            !(k->isNullAt(i) || n->isNullAt(i) || minValue->isNullAt(i) ||
              maxValue->isNullAt(i) || items->isNullAt(i) ||
              levels->isNullAt(i)));
      }
      typename KllSketch<T>::View v{
          .k = static_cast<uint32_t>(k->valueAt(i)),
          .n = static_cast<size_t>(n->valueAt(i)),
          .minValue = minValue->valueAt(i),
          .maxValue = maxValue->valueAt(i),
          .items =
              {rawItems + items->offsetAt(i),
               static_cast<size_t>(items->sizeAt(i))},
          .levels =
              {rawLevels + levels->offsetAt(i),
               static_cast<size_t>(levels->sizeAt(i))},
      };
      if constexpr (kSingleGroup) {
        views.push_back(v);
      } else {
        auto tracker = trackRowSize(group[row]);
        accumulator->append(v);
      }
    });
    if constexpr (kSingleGroup) {
      if (!views.empty()) {
        auto tracker = trackRowSize(group);
        accumulator->append(views);
      }
    }
  }
};

extern template class KllSketchAccumulator<int8_t>;
extern template class KllSketchAccumulator<int16_t>;
extern template class KllSketchAccumulator<int32_t>;
extern template class KllSketchAccumulator<int64_t>;
extern template class KllSketchAccumulator<float>;
extern template class KllSketchAccumulator<double>;

} // namespace bytedance::bolt::aggregate::prestosql
