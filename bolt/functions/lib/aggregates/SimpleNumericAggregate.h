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

#include "bolt/exec/Aggregate.h"
#include "bolt/exec/AggregationHook.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/LazyVector.h"
namespace bytedance::bolt::functions::aggregate {

template <typename TInput, typename TAccumulator, typename TResult>
class SimpleNumericAggregate : public exec::Aggregate {
 protected:
  explicit SimpleNumericAggregate(TypePtr resultType) : Aggregate(resultType) {}

 public:
  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

 protected:
  // TData is either TAccumulator or TResult, which in most cases are the same,
  // but for sum(real) can differ.
  template <typename TData = TResult, typename ExtractOneValue>
  void doExtractValues(
      char** groups,
      int32_t numGroups,
      VectorPtr* result,
      ExtractOneValue extractOneValue) {
    BOLT_CHECK_EQ((*result)->encoding(), VectorEncoding::Simple::FLAT);
    auto vector = (*result)->as<FlatVector<TData>>();
    BOLT_CHECK(
        vector,
        "Unexpected type of the result vector: {}",
        (*result)->type()->toString());
    BOLT_CHECK_EQ(vector->elementSize(), sizeof(TData));
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);
    if constexpr (std::is_same_v<TData, bool>) {
      uint64_t* rawValues = vector->template mutableRawValues<uint64_t>();
      for (int32_t i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        if (isNull(group)) {
          vector->setNull(i, true);
        } else {
          clearNull(rawNulls, i);
          bits::setBit(rawValues, i, extractOneValue(group));
        }
      }
    } else {
      TData* rawValues = vector->mutableRawValues();
      for (int32_t i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        if (isNull(group)) {
          vector->setNull(i, true);
        } else {
          clearNull(rawNulls, i);
          rawValues[i] = extractOneValue(group);
        }
      }
    }
  }

  // TData is used to store the updated group states. It can be either
  // TAccumulator or TResult, which in most cases are the same, but for
  // sum(real) can differ. TValue is used to decode the update input 'args'.
  // It can be either TAccumulator or TInput, which is most cases are the same
  // but for sum(real) can differ.
  template <
      bool tableHasNulls,
      typename TData = TResult,
      typename TValue = TInput,
      typename UpdateSingleValue>
  FLATTEN void updateGroups(
      char** groups,
      const SelectivityVector& rows,
      const VectorPtr& arg,
      UpdateSingleValue updateSingleValue,
      bool mayPushdown) {
    DecodedVector decoded(*arg, rows, !mayPushdown);
    auto encoding = decoded.base()->encoding();
    if (UNLIKELY(encoding == VectorEncoding::Simple::LAZY)) {
      bolt::aggregate::SimpleCallableHook<TValue, TData, UpdateSingleValue>
          hook(
              exec::Aggregate::offset_,
              exec::Aggregate::nullByte_,
              exec::Aggregate::nullMask_,
              groups,
              this->exec::Aggregate::numNulls_,
              updateSingleValue);

      auto indices = decoded.indices();
      decoded.base()->as<const LazyVector>()->load(
          RowSet(indices, arg->size()), &hook);
      return;
    }

    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        auto value = decoded.valueAt<TValue>(0);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue<tableHasNulls, TData>(
              groups[i], TData(value), updateSingleValue);
        });
      }
    } else if (decoded.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decoded.nulls(&rows) != nullptr) {
        BOLT_CHECK(
            decoded.size() == rows.end(),
            fmt::format(
                "decoded.size() {} != rows.end() {}",
                decoded.size(),
                rows.end()));
        nulls = decoded.nulls(&rows);
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            updateNonNullValue<tableHasNulls, TData>(
                groups[i],
                TData(decoded.valueAt<TValue>(i)),
                updateSingleValue);
          },
          nulls);
    } else if (decoded.isIdentityMapping() && !std::is_same_v<TValue, bool>) {
      auto data = decoded.data<TValue>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<tableHasNulls, TData>(
            groups[i], TData(data[i]), updateSingleValue);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<tableHasNulls, TData>(
            groups[i], TData(decoded.valueAt<TValue>(i)), updateSingleValue);
      });
    }
  }

  // TData is used to store the updated group state. It can be either
  // TAccumulator or TResult, which in most cases are the same, but for
  // sum(real) can differ. TValue is used to decode the update input 'args'.
  // It can be either TAccumulator or TInput, which is most cases are the same
  // but for sum(real) can differ.
  template <
      typename TData = TResult,
      typename TValue = TInput,
      typename UpdateSingle,
      typename UpdateDuplicate>
  void updateOneGroup(
      char* group,
      const SelectivityVector& rows,
      const VectorPtr& arg,
      UpdateSingle updateSingleValue,
      UpdateDuplicate updateDuplicateValues,
      bool /*mayPushdown*/,
      TData initialValue) {
    DecodedVector decoded(*arg, rows);

    // Do row by row if not all rows are selected.
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        updateDuplicateValues(
            initialValue,
            TData(decoded.valueAt<TValue>(0)),
            rows.countSelected());
        updateNonNullValue<true, TData>(group, initialValue, updateSingleValue);
      }
    } else if (decoded.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decoded.nulls(&rows) != nullptr) {
        BOLT_CHECK(
            decoded.size() == rows.end(),
            fmt::format(
                "decoded.size() {} != rows.end() {}",
                decoded.size(),
                rows.end()));
        nulls = decoded.nulls(&rows);
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            updateNonNullValue<true, TData>(
                group, TData(decoded.valueAt<TValue>(i)), updateSingleValue);
          },
          nulls);
    } else if (decoded.isIdentityMapping() && !std::is_same_v<TValue, bool>) {
      auto data = decoded.data<TValue>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<true, TData>(
            group, TData(data[i]), updateSingleValue);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<true, TData>(
            group, TData(decoded.valueAt<TValue>(i)), updateSingleValue);
      });
    }
  }

  template <typename THook>
  void
  pushdown(char** groups, const SelectivityVector& rows, const VectorPtr& arg) {
    DecodedVector decoded(*arg, rows, false);
    const vector_size_t* indices = decoded.indices();
    THook hook(
        exec::Aggregate::offset_,
        exec::Aggregate::nullByte_,
        exec::Aggregate::nullMask_,
        groups,
        this->exec::Aggregate::numNulls_);
    // The decoded vector does not really keep the info from the 'rows', except
    // for the 'upper bound' of it. In case not all rows are selected we need to
    // generate proper indices, which we 'indirect' through the ones we got from
    // the decoded vector.
    vector_size_t numIndices{arg->size()};
    if (not rows.isAllSelected()) {
      const auto numSelected = rows.countSelected();
      if (numSelected != arg->size()) {
        pushdownCustomIndices_.resize(numSelected);
        vector_size_t tgtIndex{0};
        rows.applyToSelected([&](vector_size_t i) {
          pushdownCustomIndices_[tgtIndex++] = indices[i];
        });
        indices = pushdownCustomIndices_.data();
        numIndices = numSelected;
      }
    }

    decoded.base()->as<const LazyVector>()->load(
        RowSet(indices, numIndices), &hook);
  }

 private:
  // TData is either TAccumulator or TResult, which in most cases are the same,
  // but for sum(real) can differ.
  template <
      bool tableHasNulls,
      typename TDataType = TAccumulator,
      typename Update>
  inline void
  updateNonNullValue(char* group, TDataType value, Update updateValue) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    updateValue(*exec::Aggregate::value<TDataType>(group), value);
  }
};

} // namespace bytedance::bolt::functions::aggregate
