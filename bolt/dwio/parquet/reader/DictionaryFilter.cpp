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

#include "bolt/dwio/parquet/reader/DictionaryFilter.h"
#include "bolt/dwio/parquet/reader/DictionaryEncodingInfo.h"
#include "bolt/dwio/parquet/reader/DictionaryFilterDecider.h"
#include "bolt/dwio/parquet/reader/DictionaryPageReader.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/filter/FilterBase.h"
namespace bytedance::bolt::parquet {

DictionaryFilter::MatchResult DictionaryFilter::tryMatch() {
  // 1) Check if fully dictionary-encoded
  DictionaryEncodingInfo encodingInfo(columnChunk_.meta_data, schema_);
  if (!encodingInfo.isFullyDictionaryEncoded() || !isDictionaryFriendly()) {
    // We cannot do dictionary pruning, so assume it "may match"
    return DictionaryFilter::MatchResult::NoConclusion;
  }

  // 2) Depending on the effective type, decode dictionary and test
  switch (encodingInfo.effectiveType()) {
    case thrift::Type::BOOLEAN:
      return checkDictionaryValues<bool>();

    case thrift::Type::INT32:
      return checkDictionaryValues<int32_t>();

    case thrift::Type::INT64:
      return checkDictionaryValues<int64_t>();

    case thrift::Type::INT96: // Used for timestamps in some Parquet files
      return checkDictionaryValues<Timestamp>();

    case thrift::Type::FLOAT:
      return checkDictionaryValues<float>();

    case thrift::Type::DOUBLE:
      return checkDictionaryValues<double>();

    case thrift::Type::BYTE_ARRAY:
    case thrift::Type::FIXED_LEN_BYTE_ARRAY:
      // Handle both variable-length and fixed-length byte arrays as StringView
      if (schema_.__isset.logicalType) {
        const auto& lt = schema_.logicalType;
        if (lt.__isset.DECIMAL) {
          if (schema_.precision <= 18) {
            return checkDictionaryValues<int64_t>();
          } else {
            // For high-precision decimals (> 18 digits), use INT128
            return checkDictionaryValues<int128_t>();
          }
        }
      }
      // Default to StringView for other byte array types
      return checkDictionaryValues<bolt::StringView>();

    default:
      // Types we don't handle yet, assume it may match
      VLOG(1) << "Unhandled Parquet type in dictionary filter: "
              << encodingInfo.effectiveType();
      return DictionaryFilter::MatchResult::Keep;
  }
}

bool DictionaryFilter::isDictionaryFriendly() const {
  return !filter_->isValueIndependent();
}

template <typename T, typename U>
bool hasOverlap(
    const FlatVector<U>& vecA,
    const folly::F14FastSet<T>& setB,
    bool isNegate) {
  if (!isNegate) {
    for (auto i = 0; i < vecA.size(); ++i) {
      const auto& val = vecA.valueAt(i);
      if (setB.find(val) != setB.end()) {
        return true;
      }
    }
    return false;
  } else {
    for (auto i = 0; i < vecA.size(); ++i) {
      const auto& val = vecA.valueAt(i);
      if (setB.find(val) == setB.end()) {
        return true;
      }
    }
    return false;
  }
}

template <typename T>
DictionaryFilter::MatchResult DictionaryFilter::checkDictionaryValues() {
  using FilterType = std::conditional_t<is_filter_int_v<T>, int64_t, T>;
  auto value_filter =
      dynamic_cast<const common::IFilterWithValues<FilterType>*>(filter_);
  if (value_filter == nullptr) {
    return DictionaryFilter::MatchResult::NoConclusion;
  }

  DictionaryPageReader reader(
      columnChunk_, type_, input_, pool_, rowGroupIndex_);

  if (!reader.prepareDictionary()) {
    return DictionaryFilter::MatchResult::NoConclusion;
  }

  const auto& filterKeys = value_filter->set();

  // Check if we really need filtering by dictionary page.
  auto dictElementsCount =
      reader.pageHeader().dictionary_page_header.num_values;
  auto filterKeysCount = filterKeys.size();
  auto numOfRow = numOfRow_;

  DictionaryFilterContext context(dictElementsCount, filterKeysCount, numOfRow);
  auto decider = DeciderFactory::create(context);
  if (!decider->shouldApplyFiltering()) {
    return DictionaryFilter::MatchResult::NoConclusion;
  }

  // Read the dictionary values.
  VectorPtr dictValues;
  if (!reader.readDictionaryValues<T>(dictValues)) {
    return DictionaryFilter::MatchResult::NoConclusion;
  }

  // Convert the VectorPtr to FlatVector<T>.
  auto dictVector = dictValues->template asFlatVector<T>();
  if (!dictVector) {
    return DictionaryFilter::MatchResult::NoConclusion;
  }

  // Try to check if there is overlap between the dictionary values and the
  // filter keys.
  if (hasOverlap(*dictVector, filterKeys, value_filter->isNegateFilter())) {
    return DictionaryFilter::MatchResult::Keep;
  } else {
    return DictionaryFilter::MatchResult::Drop;
  }
}

// Explicitly instantiate the templates we support:
template DictionaryFilter::MatchResult
DictionaryFilter::checkDictionaryValues<bool>();
template DictionaryFilter::MatchResult
DictionaryFilter::checkDictionaryValues<int8_t>();
template DictionaryFilter::MatchResult
DictionaryFilter::checkDictionaryValues<int16_t>();
template DictionaryFilter::MatchResult
DictionaryFilter::checkDictionaryValues<int32_t>();
template DictionaryFilter::MatchResult
DictionaryFilter::checkDictionaryValues<int64_t>();
template DictionaryFilter::MatchResult
DictionaryFilter::checkDictionaryValues<float>();
template DictionaryFilter::MatchResult
DictionaryFilter::checkDictionaryValues<double>();
template DictionaryFilter::MatchResult
DictionaryFilter::checkDictionaryValues<Timestamp>();
template DictionaryFilter::MatchResult
DictionaryFilter::checkDictionaryValues<bolt::StringView>();

} // namespace bytedance::bolt::parquet
