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

#include "bolt/dwio/common/SelectiveColumnReaderInternal.h"
namespace bytedance::bolt::dwio::common {

template <typename TData, typename TRequested>
class SelectiveFloatingPointColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = TRequested;
  SelectiveFloatingPointColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      FormatParams& params,
      bolt::common::ScanSpec& scanSpec)
      : SelectiveColumnReader(
            requestedType,
            std::move(fileType),
            params,
            scanSpec) {}

  // Offers a fast path only if data and result widths match.
  static constexpr bool kHasBulkPath = std::is_same_v<TData, TRequested>;

  bool hasBulkPath() const override {
    return kHasBulkPath;
  }

  template <typename Reader>
  void
  readCommon(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls);

  void getValues(const RowSet& rows, VectorPtr* result) override {
    getFlatValues<TData, TRequested>(rows, result, requestedType_);
  }

 protected:
  template <
      typename Reader,
      typename TFilter,
      bool isDense,
      typename ExtractValues>
  void
  readHelper(bolt::common::Filter* filter, RowSet rows, ExtractValues values);

  template <typename Reader, bool isDense, typename ExtractValues>
  void processFilter(
      bolt::common::Filter* filter,
      RowSet rows,
      ExtractValues extractValues);

  template <typename Reader, bool isDense>
  void processValueHook(const RowSet& rows, ValueHook* hook);
};

template <typename TData, typename TRequested>
template <
    typename Reader,
    typename TFilter,
    bool isDense,
    typename ExtractValues>
void SelectiveFloatingPointColumnReader<TData, TRequested>::readHelper(
    bolt::common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  reinterpret_cast<Reader*>(this)->readWithVisitor(
      rows,
      ColumnVisitor<
          TData,
          TFilter,
          ExtractValues,
          isDense,
          Reader::kHasBulkPath>(
          *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
}

template <typename TData, typename TRequested>
template <typename Reader, bool isDense, typename ExtractValues>
void SelectiveFloatingPointColumnReader<TData, TRequested>::processFilter(
    bolt::common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (filter ? filter->kind() : bolt::common::FilterKind::kAlwaysTrue) {
    case bolt::common::FilterKind::kAlwaysTrue:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          filter, rows, extractValues);
      break;
    case bolt::common::FilterKind::kIsNull:
      filterNulls<TRequested>(
          rows, true, !std::is_same_v<decltype(extractValues), DropValues>);
      break;
    case bolt::common::FilterKind::kIsNotNull:
      if (std::is_same_v<decltype(extractValues), DropValues>) {
        filterNulls<TRequested>(rows, false, false);
      } else {
        readHelper<Reader, bolt::common::IsNotNull, isDense>(
            filter, rows, extractValues);
      }
      break;
    case bolt::common::FilterKind::kDoubleRange:
    case bolt::common::FilterKind::kFloatRange:
      readHelper<Reader, bolt::common::FloatingPointRange<TRequested>, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<Reader, bolt::common::Filter, isDense>(
          filter, rows, extractValues);
      break;
  }
}

template <typename TData, typename TRequested>
template <typename Reader, bool isDense>
void SelectiveFloatingPointColumnReader<TData, TRequested>::processValueHook(
    const RowSet& rows,
    ValueHook* hook) {
  switch (hook->kind()) {
    case aggregate::AggregationHook::kSumFloatToDouble:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<float, double>>(hook));
      break;
    case aggregate::AggregationHook::kSumDoubleToDouble:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<double, double>>(hook));
      break;
    case aggregate::AggregationHook::kFloatMax:
    case aggregate::AggregationHook::kDoubleMax:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::MinMaxHook<TRequested, false>>(hook));
      break;
    case aggregate::AggregationHook::kFloatMin:
    case aggregate::AggregationHook::kDoubleMin:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::MinMaxHook<TRequested, true>>(hook));
      break;
    default:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &alwaysTrue(), rows, ExtractToGenericHook(hook));
  }
}

template <typename TData, typename TRequested>
template <typename Reader>
void SelectiveFloatingPointColumnReader<TData, TRequested>::readCommon(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  prepareRead<TData>(offset, rows, incomingNulls);
  const bool isDense = rows.back() == rows.size() - 1;
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<Reader, true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<Reader, false>(rows, scanSpec_->valueHook());
      }
    } else {
      if (isDense) {
        processFilter<Reader, true>(
            scanSpec_->filter(), rows, ExtractToReader(this));
      } else {
        processFilter<Reader, false>(
            scanSpec_->filter(), rows, ExtractToReader(this));
      }
    }
  } else {
    if (isDense) {
      processFilter<Reader, true>(scanSpec_->filter(), rows, DropValues());
    } else {
      processFilter<Reader, false>(scanSpec_->filter(), rows, DropValues());
    }
  }
}

} // namespace bytedance::bolt::dwio::common
