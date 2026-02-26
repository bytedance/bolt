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

#pragma once

#include <cstdint>
#include <string>
#include <type_traits>
#include "bolt/common/base/Exceptions.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/Type.h"

namespace {

template <typename T>
inline constexpr bool is_filter_int_v =
    (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
     std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>);
}
namespace bytedance::bolt::common {

// Forward declarations
class Filter;
class HugeintRange;
class TimestampRange;

template <typename T>
class FloatingPointRange;

using bytedance::bolt::Timestamp;
//------------------------------------------------------------------------------
// Filter Application Templates
//------------------------------------------------------------------------------

template <typename T>
constexpr bool always_false_v = false;

/**
 * Generic filter application helper for different types.
 * Dispatches to appropriate test method based on type.
 *
 * @param filter The filter to apply
 * @param value The value to test
 * @return true if the value passes the filter
 */
template <typename TFilter, typename T>
static inline bool applyFilter(TFilter& filter, T value) {
  if constexpr (std::is_same_v<T, int128_t>) {
    return filter.testInt128(value);
  } else if constexpr (is_filter_int_v<T>) {
    return filter.testInt64(value);
  } else if constexpr (std::is_same_v<T, float>) {
    return filter.testFloat(value);
  } else if constexpr (std::is_same_v<T, double>) {
    return filter.testDouble(value);
  } else if constexpr (std::is_same_v<T, bool>) {
    return filter.testBool(value);
  } else if constexpr (std::is_same_v<T, Timestamp>) {
    return filter.testTimestamp(value);
  } else if constexpr (std::is_same_v<T, std::string>) {
    return filter.testBytes(value.c_str(), value.length());
  } else if constexpr (std::is_same_v<T, folly::StringPiece>) {
    return filter.testBytes(value.data(), value.size());
  } else if constexpr (std::is_same_v<T, StringView>) {
    return filter.testBytes(value.data(), value.size());
  } else if constexpr (std::is_same_v<T, bytedance::bolt::UnknownValue>) {
    BOLT_UNSUPPORTED("Bad argument type to filter with 'UnknownValue'");
  } else {
    static_assert(always_false_v<T>, "Bad argument type to filter");
  }
}

/**
 * String-specific filter application overloads for different string types.
 */
template <typename TFilter>
static inline bool applyFilter(TFilter& filter, const std::string& value) {
  return filter.testBytes(value.data(), value.size());
}

//------------------------------------------------------------------------------
// Filter Creation Functions
//------------------------------------------------------------------------------

/**
 * Creates filters for different numeric types and operations.
 * Implementation provided elsewhere.
 */
extern std::unique_ptr<Filter> createBigintValues(
    const std::vector<int64_t>& values,
    bool nullAllowed);

extern std::unique_ptr<Filter> createHugeintValues(
    const std::vector<int128_t>& values,
    bool nullAllowed);

extern std::unique_ptr<Filter> createNegatedBigintValues(
    const std::vector<int64_t>& values,
    bool nullAllowed);

extern std::unique_ptr<Filter> createBigintValuesFilter(
    const std::vector<int64_t>& values,
    bool nullAllowed,
    bool negated);

//------------------------------------------------------------------------------
// Range Combination Functions
//------------------------------------------------------------------------------

/**
 * Combines multiple HugeintRange filters into a single filter.
 * Sorts and merges overlapping or adjacent ranges.
 *
 * @param ranges Input ranges to combine. Takes ownership of the ranges.
 * @param nullAllowed Whether null values should pass the filter.
 * @return A new filter representing the combined ranges. Returns nullptr if
 * ranges is empty.
 */
extern std::unique_ptr<Filter> combineHugeintRanges(
    std::vector<std::unique_ptr<HugeintRange>> ranges,
    bool nullAllowed);

/**
 * Combines a negated timestamp range with multiple regular timestamp ranges.
 * Creates new ranges that represent the intersection.
 *
 * @param lower Lower bound of the negated range (inclusive)
 * @param upper Upper bound of the negated range (inclusive)
 * @param ranges Regular ranges to combine with the negated range
 * @param nullAllowed Whether null values should pass the filter
 * @return A new filter representing the combined result
 */
extern std::unique_ptr<Filter> combineNegatedRangeOnTimestampRanges(
    const Timestamp& lower,
    const Timestamp& upper,
    const std::vector<std::unique_ptr<TimestampRange>>& ranges,
    bool nullAllowed);

/**
 * Combines a negated hugeint range with multiple regular hugeint ranges.
 * Creates new ranges that represent the intersection.
 *
 * @param lower Lower bound of the negated range (inclusive)
 * @param upper Upper bound of the negated range (inclusive)
 * @param ranges Regular ranges to combine with the negated range
 * @param nullAllowed Whether null values should pass the filter
 * @return A new filter representing the combined result
 */
extern std::unique_ptr<Filter> combineNegatedRangeOnHugeintRanges(
    const int128_t& lower,
    const int128_t& upper,
    const std::vector<std::unique_ptr<HugeintRange>>& ranges,
    bool nullAllowed);

/**
 * Combines multiple timestamp ranges into a single filter.
 */
extern std::unique_ptr<Filter> combineTimestampRanges(
    std::vector<std::unique_ptr<TimestampRange>> ranges,
    bool nullAllowed);

extern std::unique_ptr<Filter> createBigintRange(
    int64_t min,
    int64_t max,
    bool nullAllowed,
    bool rangeAllowed);

extern std::unique_ptr<Filter> createBytesValues(
    const std::vector<std::string>& values,
    bool nullAllowed);

extern std::unique_ptr<Filter> createBytesValues(
    const std::vector<StringView>& values,
    bool nullAllowed);

extern std::unique_ptr<Filter> createBytesRange(
    std::optional<std::string> lower,
    bool lowerInclusive,
    std::optional<std::string> upper,
    bool upperInclusive,
    bool nullAllowed);

//------------------------------------------------------------------------------
// Utility Functions
//------------------------------------------------------------------------------

/**
 * Deserializes the nullAllowed property from a dynamic object.
 */
inline bool deserializeNullAllowed(const folly::dynamic& obj) {
  return obj["nullAllowed"].asBool();
}

/**
 * Creates a filter that returns true only for null values or always returns
 * false.
 *
 * @param nullAllowed If true, returns an IsNull filter, otherwise returns
 * AlwaysFalse
 * @return A new filter instance
 */
extern std::unique_ptr<Filter> nullOrFalse(bool nullAllowed);

/**
 * Creates a filter that returns true for non-null values or always returns
 * true.
 *
 * @param nullAllowed If true, returns an AlwaysTrue filter, otherwise returns
 * IsNotNull
 * @return A new filter instance
 */
extern std::unique_ptr<Filter> notNullOrTrue(bool nullAllowed);

template <typename T>
std::unique_ptr<Filter> combineFloatRanges(
    std::vector<std::unique_ptr<FloatingPointRange<T>>> ranges,
    bool nullAllowed);

extern template std::unique_ptr<Filter> combineFloatRanges<float>(
    std::vector<std::unique_ptr<FloatingPointRange<float>>> ranges,
    bool nullAllowed);

extern template std::unique_ptr<Filter> combineFloatRanges<double>(
    std::vector<std::unique_ptr<FloatingPointRange<double>>> ranges,
    bool nullAllowed);

} // namespace bytedance::bolt::common
