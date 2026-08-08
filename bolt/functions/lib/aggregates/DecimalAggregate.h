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
#include "bolt/exec/Aggregate.h"
#include "bolt/functions/lib/aggregates/DecimalAccumulatorLayout.h"
#include "bolt/type/HugeInt.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::functions::aggregate {

/**
 *  LongDecimalWithOverflowState has the following fields:
 *    SUM: Total sum so far.
 *    COUNT: Total number of rows so far.
 *    OVERFLOW: Total count of net overflow or underflow so far.
 */
struct LongDecimalWithOverflowState : LongDecimalWithOverflowLayout {
 public:
  void mergeWith(const StringView& serializedData) {
    BOLT_CHECK_EQ(serializedData.size(), serializedSize());
    auto serialized = serializedData.data();
    common::InputByteStream stream(serialized);
    count += stream.read<int64_t>();
    overflow += stream.read<int64_t>();
    uint64_t lowerSum = stream.read<uint64_t>();
    int64_t upperSum = stream.read<int64_t>();
    overflow += DecimalUtil::addWithOverflow(
        this->sum, HugeInt::build(upperSum, lowerSum), this->sum);
  }

  void serialize(StringView& serialized) {
    BOLT_CHECK_EQ(serialized.size(), serializedSize());
    char* outputBuffer = const_cast<char*>(serialized.data());
    common::OutputByteStream outStream(outputBuffer);
    outStream.append((char*)&count, sizeof(int64_t));
    outStream.append((char*)&overflow, sizeof(int64_t));
    uint64_t lower = HugeInt::lower(sum);
    int64_t upper = HugeInt::upper(sum);
    outStream.append((char*)&lower, sizeof(int64_t));
    outStream.append((char*)&upper, sizeof(int64_t));
  }

  /*
   * Total size = sizeOf(count) + sizeOf(overflow) + sizeOf(sum)
   *            = 8 + 8 + 16 = 32.
   */
  static constexpr size_t serializedSize() {
    return sizeof(int64_t) * 4;
  }
};

static_assert(std::is_standard_layout_v<LongDecimalWithOverflowState>);

template <typename TResultType, typename TInputType = TResultType>
class DecimalAggregate : public exec::Aggregate {
 public:
  explicit DecimalAggregate(TypePtr resultType) : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(LongDecimalWithOverflowState);
  }

  int32_t accumulatorAlignmentSize() const override {
    return alignof(LongDecimalWithOverflowState);
  }

  FLATTEN void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) LongDecimalWithOverflowState();
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
        auto value = decodedRaw_.valueAt<TInputType>(0);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(groups[i], TResultType(value));
        });
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
            updateNonNullValue(
                groups[i], TResultType(decodedRaw_.valueAt<TInputType>(i)));
          },
          nulls);
    } else if (exec::Aggregate::hasNoNulls() && decodedRaw_.isIdentityMapping()) {
      auto data = decodedRaw_.data<TInputType>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<false>(groups[i], TResultType(data[i]));
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue(
            groups[i], TResultType(decodedRaw_.valueAt<TInputType>(i)));
      });
    }
  }

  FLATTEN void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedRaw_.decode(*args[0], rows);
    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        const auto numRows = rows.countSelected();
        int64_t overflow = 0;
        int128_t totalSum{0};
        auto value = decodedRaw_.valueAt<TInputType>(0);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(group, TResultType(value));
        });
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
            updateNonNullValue(
                group, TResultType(decodedRaw_.valueAt<TInputType>(i)));
          },
          nulls);
    } else if (exec::Aggregate::hasNoNulls() && decodedRaw_.isIdentityMapping()) {
      const TInputType* data = decodedRaw_.data<TInputType>();
      LongDecimalWithOverflowState accumulator;
      rows.applyToSelected([&](vector_size_t i) {
        accumulator.overflow += DecimalUtil::addWithOverflow(
            accumulator.sum, data[i], accumulator.sum);
      });
      accumulator.count = rows.countSelected();
      char rawData[LongDecimalWithOverflowState::serializedSize()];
      StringView serialized(
          rawData, LongDecimalWithOverflowState::serializedSize());
      accumulator.serialize(serialized);
      mergeAccumulators<false>(group, serialized);
    } else {
      LongDecimalWithOverflowState accumulator;
      rows.applyToSelected([&](vector_size_t i) {
        accumulator.overflow += DecimalUtil::addWithOverflow(
            accumulator.sum,
            decodedRaw_.valueAt<TInputType>(i),
            accumulator.sum);
      });
      accumulator.count = rows.countSelected();
      char rawData[LongDecimalWithOverflowState::serializedSize()];
      StringView serialized(
          rawData, LongDecimalWithOverflowState::serializedSize());
      accumulator.serialize(serialized);
      mergeAccumulators(group, serialized);
    }
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    auto intermediateFlatVector =
        dynamic_cast<const FlatVector<StringView>*>(decodedPartial_.base());
    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        auto serializedAccumulator =
            intermediateFlatVector->valueAt(decodedIndex);
        rows.applyToSelected([&](vector_size_t i) {
          clearNull(groups[i]);
          auto* accumulator = decimalAccumulator(groups[i]);
          accumulator->mergeWith(serializedAccumulator);
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
            clearNull(groups[i]);
            auto decodedIndex = decodedPartial_.index(i);
            auto serializedAccumulator =
                intermediateFlatVector->valueAt(decodedIndex);
            auto* accumulator = decimalAccumulator(groups[i]);
            accumulator->mergeWith(serializedAccumulator);
          },
          nulls);
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        clearNull(groups[i]);
        auto decodedIndex = decodedPartial_.index(i);
        auto serializedAccumulator =
            intermediateFlatVector->valueAt(decodedIndex);
        auto* accumulator = decimalAccumulator(groups[i]);
        accumulator->mergeWith(serializedAccumulator);
      });
    }
  }

  FLATTEN void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    auto intermediateFlatVector =
        dynamic_cast<const FlatVector<StringView>*>(decodedPartial_.base());

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        auto serializedAccumulator =
            intermediateFlatVector->valueAt(decodedIndex);
        if (rows.hasSelections()) {
          clearNull(group);
        }
        rows.applyToSelected([&](vector_size_t i) {
          mergeAccumulators(group, serializedAccumulator);
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
            clearNull(group);
            auto decodedIndex = decodedPartial_.index(i);
            auto serializedAccumulator =
                intermediateFlatVector->valueAt(decodedIndex);
            mergeAccumulators(group, serializedAccumulator);
          },
          nulls);
    } else {
      if (rows.hasSelections()) {
        clearNull(group);
      }
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        auto serializedAccumulator =
            intermediateFlatVector->valueAt(decodedIndex);
        mergeAccumulators(group, serializedAccumulator);
      });
    }
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    auto stringViewVector = (*result)->as<FlatVector<StringView>>();
    stringViewVector->resize(numGroups);
    uint64_t* rawNulls = nullptr;
    rawNulls = getRawNulls(stringViewVector);
    for (auto i = 0; i < numGroups; ++i) {
      auto accumulator = decimalAccumulator(groups[i]);
      if (isNull(groups[i])) {
        stringViewVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        auto size = accumulator->serializedSize();
        char* rawBuffer = stringViewVector->getRawStringBufferWithSpace(size);
        StringView serialized(rawBuffer, size);
        accumulator->serialize(serialized);
        stringViewVector->setNoCopy(i, serialized);
      }
    }
  }

  virtual TResultType computeFinalValue(
      LongDecimalWithOverflowState* accumulator) {
    return 0;
  };

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    auto vector = (*result)->as<FlatVector<TResultType>>();
    BOLT_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);

    TResultType* rawValues = vector->mutableRawValues();
    BOLT_CHECK(rawValues);
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
        if (!rawNulls) {
          rawNulls = getRawNulls(vector);
        }
      } else {
        if (rawNulls) {
          clearNull(rawNulls, i);
        }
        auto* accumulator = decimalAccumulator(group);
        rawValues[i] = computeFinalValue(accumulator);
      }
    }
  }

  template <bool tableHasNulls = true>
  void mergeAccumulators(char* group, const StringView& serialized) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    auto* accumulator = decimalAccumulator(group);
    accumulator->mergeWith(serialized);
  }

  template <bool tableHasNulls = true>
  void updateNonNullValue(char* group, TResultType value) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    auto* accumulator = decimalAccumulator(group);
    accumulator->overflow +=
        DecimalUtil::addWithOverflow(accumulator->sum, value, accumulator->sum);
    accumulator->count += 1;
  }

 protected:
  inline LongDecimalWithOverflowState* decimalAccumulator(char* group) {
    return exec::Aggregate::value<LongDecimalWithOverflowState>(group);
  }

 private:
  DecodedVector decodedRaw_;
  DecodedVector decodedPartial_;
};

} // namespace bytedance::bolt::functions::aggregate
