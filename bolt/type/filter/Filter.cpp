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

#include <boost/sort/pdqsort/pdqsort.hpp>
#include <folly/Conv.h>
#include <folly/String.h>

#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>

#include "bolt/common/base/Exceptions.h"
#include "bolt/type/filter/Cast.h"
#include "bolt/type/filter/FilterBase.h"
#include "bolt/type/filter/FilterUtil.h"
#include "bolt/type/filter/FloatingPointMultiRange.h"
#include "bolt/type/filter/FloatingPointRange.h"
#include "bolt/type/filter/FloatingPointValues.h"
#include "bolt/type/filter/NegatedFloatingPointRange.h"
#include "bolt/type/filter/NegatedFloatingPointValues.h"
namespace bytedance::bolt::common {

std::string Filter::toString() const {
  std::string strKind;
  switch (kind_) {
    case FilterKind::kAlwaysFalse:
      strKind = "AlwaysFalse";
      break;
    case FilterKind::kAlwaysTrue:
      strKind = "AlwaysTrue";
      break;
    case FilterKind::kIsNull:
      strKind = "IsNull";
      break;
    case FilterKind::kIsNotNull:
      strKind = "IsNotNull";
      break;
    case FilterKind::kBoolValue:
      strKind = "BoolValue";
      break;
    case FilterKind::kBigintRange:
      strKind = "BigintRange";
      break;
    case FilterKind::kNegatedBigintRange:
      strKind = "NegatedBigintRange";
      break;
    case FilterKind::kBigintValuesUsingHashTable:
      strKind = "BigintValuesUsingHashTable";
      break;
    case FilterKind::kBigintValuesUsingBitmask:
      strKind = "BigintValuesUsingBitmask";
      break;
    case FilterKind::kNegatedBigintValuesUsingHashTable:
      strKind = "NegatedBigintValuesUsingHashTable";
      break;
    case FilterKind::kNegatedBigintValuesUsingBitmask:
      strKind = "NegatedBigintValuesUsingBitmask";
      break;
    case FilterKind::kDoubleRange:
      strKind = "DoubleRange";
      break;
    case FilterKind::kDoubleValues:
      strKind = "DoubleValues";
      break;
    case FilterKind::kFloatRange:
      strKind = "FloatRange";
      break;
    case FilterKind::kFloatValues:
      strKind = "FloatValues";
      break;
    case FilterKind::kBytesRange:
      strKind = "BytesRange";
      break;
    case FilterKind::kNegatedBytesRange:
      strKind = "NegatedBytesRange";
      break;
    case FilterKind::kBytesValues:
      strKind = "BytesValues";
      break;
    case FilterKind::kNegatedBytesValues:
      strKind = "NegatedBytesValues";
      break;
    case FilterKind::kBigintMultiRange:
      strKind = "BigintMultiRange";
      break;
    case FilterKind::kMultiRange:
      strKind = "MultiRange";
      break;
    case FilterKind::kHugeintRange:
      strKind = "HugeintRange";
      break;
    case FilterKind::kTimestampRange:
      strKind = "TimestampRange";
      break;
    case FilterKind::kHugeintValuesUsingHashTable:
      strKind = "HugeintValuesUsingHashTable";
      break;
    case FilterKind::kBytesLike:
      strKind = "BytesLike";
      break;
    case FilterKind::kNegatedDoubleValues:
      strKind = "kNegatedDoubleValues";
      break;
    case FilterKind::kNegatedFloatValues:
      strKind = "kNegatedFloatValues";
      break;
    case FilterKind::kFloatMultiRange:
      strKind = "kFloatMultiRange";
      break;
    case FilterKind::kDoubleMultiRange:
      strKind = "kDoubleMultiRange";
      break;
    case FilterKind::kNegatedHugeintRange:
      return "NegatedHugeintRange";
    case FilterKind::kNegatedTimestampRange:
      return "NegatedTimestampRange";
    case FilterKind::kNegatedDoubleRange:
      return "NegatedDoubleRange";
    case FilterKind::kNegatedFloatRange:
      return "NegatedFloatRange";
    case FilterKind::kIsTrue:
      return "IsTrue";
    case FilterKind::kCast:
      return "Cast";
    case FilterKind::kMapSubscript:
      return "MapSubscript";
  };

  return fmt::format(
      "Filter({}, {}, {})",
      strKind,
      deterministic_ ? "deterministic" : "nondeterministic",
      nullAllowed_ ? "null allowed" : "null not allowed");
}

namespace {

int32_t binarySearch(const std::vector<int64_t>& values, int64_t value) {
  auto it = std::lower_bound(values.begin(), values.end(), value);
  if (it == values.end() || *it != value) {
    return -std::distance(values.begin(), it) - 1;
  } else {
    return std::distance(values.begin(), it);
  }
}

std::unordered_map<FilterKind, std::string> filterKindNames() {
  return {
      {FilterKind::kAlwaysFalse, "kAlwaysFalse"},
      {FilterKind::kAlwaysTrue, "kAlwaysTrue"},
      {FilterKind::kIsNull, "kIsNull"},
      {FilterKind::kIsNotNull, "kIsNotNull"},
      {FilterKind::kBoolValue, "kBoolValue"},
      {FilterKind::kBigintRange, "kBigintRange"},
      {FilterKind::kBigintValuesUsingHashTable, "kBigintValuesUsingHashTable"},
      {FilterKind::kBigintValuesUsingBitmask, "kBigintValuesUsingBitmask"},
      {FilterKind::kNegatedBigintRange, "kNegatedBigintRange"},
      {FilterKind::kNegatedBigintValuesUsingHashTable,
       "kNegatedBigintValuesUsingHashTable"},
      {FilterKind::kNegatedBigintValuesUsingBitmask,
       "kNegatedBigintValuesUsingBitmask"},
      {FilterKind::kDoubleRange, "kDoubleRange"},
      {FilterKind::kFloatRange, "kFloatRange"},
      {FilterKind::kBytesRange, "kBytesRange"},
      {FilterKind::kNegatedBytesRange, "kNegatedBytesRange"},
      {FilterKind::kBytesValues, "kBytesValues"},
      {FilterKind::kNegatedBytesValues, "kNegatedBytesValues"},
      {FilterKind::kBigintMultiRange, "kBigintMultiRange"},
      {FilterKind::kMultiRange, "kMultiRange"},
      {FilterKind::kHugeintRange, "kHugeintRange"},
      {FilterKind::kTimestampRange, "kTimestampRange"},
      {FilterKind::kHugeintValuesUsingHashTable,
       "kHugeintValuesUsingHashTable"},
      {FilterKind::kNegatedFloatRange, "kNegatedFloatRange"},
      {FilterKind::kIsTrue, "kIsTrue"},
      {FilterKind::kCast, "kCast"},
      {FilterKind::kNegatedDoubleRange, "kNegatedDoubleRange"},
      {FilterKind::kNegatedHugeintRange, "kNegatedHugeintRange"},
      {FilterKind::kNegatedTimestampRange, "kNegatedTimestampRange"},
      {FilterKind::kDoubleValues, "kDoubleValues"},
      {FilterKind::kFloatValues, "kFloatValues"},
      {FilterKind::kBytesLike, "kBytesLike"},
      {FilterKind::kNegatedDoubleValues, "kNegatedDoubleValues"},
      {FilterKind::kNegatedFloatValues, "kNegatedFloatValues"},
      {FilterKind::kFloatMultiRange, "kFloatMultiRange"},
      {FilterKind::kMapSubscript, "kMapSubscript"},
      {FilterKind::kDoubleMultiRange, "kDoubleMultiRange"}};
}

/**
 * Compares two ranges of characters represented by std::string_view.
 *
 * This function performs a lexicographical comparison between two string views:
 * 1. It first determines the minimum length of the two ranges.
 * 2. It uses memcmp to compare the first 'min(lengths)' characters of both
 * ranges.
 *    - If memcmp returns a non-zero value, that value is returned immediately,
 * indicating a difference in the common prefix.
 * 3. If the common prefix is identical, the function then compares the lengths:
 *    - If lhs is shorter than rhs, it returns -1.
 *    - If lhs is longer than rhs, it returns 1.
 *    - If both are of equal length, it returns 0.
 *
 * This explicit return of -1, 0, or 1 for length differences avoids potential
 * pitfalls of subtracting unsigned sizes.
 *
 * @param lhs The first string view to compare.
 * @param rhs The second string view to compare.
 * @return An integer less than, equal to, or greater than zero if lhs is found,
 * respectively, to be less than, equal to, or greater than rhs.
 */
int compareRanges(std::string_view lhs, std::string_view rhs) {
  size_t size = std::min(lhs.size(), rhs.size());
  int compare = memcmp(lhs.data(), rhs.data(), size);
  if (compare) {
    return compare;
  }

  if (lhs.size() < rhs.size()) {
    return -1;
  }
  if (lhs.size() > rhs.size()) {
    return 1;
  }
  return 0;
}

const char* filterKindName(FilterKind kind) {
  static const auto kNames = filterKindNames();
  return kNames.at(kind).c_str();
}

std::vector<int64_t> deserializeValues(const folly::dynamic& obj) {
  std::vector<int64_t> values;
  auto valuesArray = obj["values"];
  values.reserve(valuesArray.size());
  for (const auto& v : valuesArray) {
    values.push_back(v.asInt());
  }
  return values;
}

std::vector<int128_t> deserializeHugeintValues(const folly::dynamic& obj) {
  auto lowerValuesArray = obj["lower_values"];
  auto upperValuesArray = obj["upper_values"];
  auto len = lowerValuesArray.size();
  std::vector<int128_t> values(len);

  for (auto i = 0; i < len; i++) {
    values[i] = HugeInt::build(
        upperValuesArray[i].asInt(), lowerValuesArray[i].asInt());
  }
  return values;
}

// Helper to safely check if max - min + 1 would overflow
bool isRangeValid(int64_t min, int64_t max) {
  if (min >= max) {
    return false;
  }
  if (min <= 0 && max >= 0) {
    // When range crosses 0, just check if the absolute values would sum to
    // overflow
    return max <= std::numeric_limits<int64_t>::max() + min;
  }
  // When range is all positive or all negative, check if difference would
  // overflow
  return max <= std::numeric_limits<int64_t>::max() - 1 + min;
}
} // namespace

void Filter::registerSerDe() {
  auto& registry = deserializationRegistryForUniquePtr();

  registry.Register("AlwaysFalse", AlwaysFalse::create);
  registry.Register("AlwaysTrue", AlwaysTrue::create);
  registry.Register("IsNull", IsNull::create);
  registry.Register("IsNotNull", IsNotNull::create);
  registry.Register("BoolValue", BoolValue::create);
  registry.Register("BigintRange", BigintRange::create);
  registry.Register("NegatedBigintRange", NegatedBigintRange::create);
  registry.Register("HugeintRange", HugeintRange::create);
  registry.Register(
      "BigintValuesUsingHashTable", BigintValuesUsingHashTable::create);
  registry.Register(
      "BigintValuesUsingBitmask", BigintValuesUsingBitmask::create);
  registry.Register(
      "NegatedBigintValuesUsingHashTable",
      NegatedBigintValuesUsingHashTable::create);
  registry.Register(
      "NegatedBigintValuesUsingBitmask",
      NegatedBigintValuesUsingBitmask::create);
  registry.Register(
      "HugeintValuesUsingHashTable", HugeintValuesUsingHashTable::create);
  registry.Register("FloatRange", FloatRange::create);
  registry.Register("DoubleRange", DoubleRange::create);
  registry.Register("BytesRange", BytesRange::create);
  registry.Register("NegatedBytesRange", NegatedBytesRange::create);
  registry.Register("BytesValues", BytesValues::create);
  registry.Register("BigintMultiRange", BigintMultiRange::create);
  registry.Register("NegatedBytesValues", NegatedBytesValues::create);
  registry.Register("MultiRange", MultiRange::create);
  registry.Register("TimestampRange", TimestampRange::create);
  registry.Register("DoubleValues", DoubleValues::create);
  registry.Register("FloatValues", FloatValues::create);
  registry.Register("NegatedDoubleValues", NegatedDoubleValues::create);
  registry.Register("NegatedFloatValues", NegatedFloatValues::create);
  registry.Register("FloatMultiRange", FloatMultiRange::create);
  registry.Register("DoubleMultiRange", DoubleMultiRange::create);
  // registry.Register("BytesLike", Like::create);
  registry.Register("NegatedHugeintRange", NegatedHugeintRange::create);
  registry.Register("NegatedTimestampRange", NegatedTimestampRange::create);
  registry.Register("NegatedDoubleRange", NegatedDoubleRange::create);
  registry.Register("NegatedFloatRange", NegatedFloatRange::create);
  registry.Register("Cast", deserializeCastFilter);
}

folly::dynamic Filter::serializeBase(std::string_view name) const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = name;
  obj["nullAllowed"] = nullAllowed_;
  obj["kind"] = filterKindName(kind_);
  return obj;
}

folly::dynamic AlwaysFalse::serialize() const {
  return Filter::serializeBase("AlwaysFalse");
}

FilterPtr AlwaysFalse::create(const folly::dynamic& /*obj*/) {
  return std::make_unique<AlwaysFalse>();
}

folly::dynamic AlwaysTrue::serialize() const {
  return Filter::serializeBase("AlwaysTrue");
}

FilterPtr AlwaysTrue::create(const folly::dynamic& /*obj*/) {
  return std::make_unique<AlwaysTrue>();
}

folly::dynamic IsNull::serialize() const {
  return Filter::serializeBase("IsNull");
}

FilterPtr IsNull::create(const folly::dynamic& /*obj*/) {
  return std::make_unique<IsNull>();
}

folly::dynamic IsNotNull::serialize() const {
  return Filter::serializeBase("IsNotNull");
}

FilterPtr IsNotNull::create(const folly::dynamic& /*obj*/) {
  return std::make_unique<IsNotNull>();
}

FilterPtr IsTrue::create(const folly::dynamic& /*obj*/) {
  return std::make_unique<IsNotNull>();
}

folly::dynamic IsTrue::serialize() const {
  return Filter::serializeBase("IsTrue");
}

folly::dynamic BoolValue::serialize() const {
  auto obj = Filter::serializeBase("BoolValue");
  obj["value"] = value_;
  return obj;
}

FilterPtr BoolValue::create(const folly::dynamic& obj) {
  auto value = obj["value"].asBool();
  auto nullAllowed = deserializeNullAllowed(obj);
  return std::make_unique<BoolValue>(value, nullAllowed);
}

bool BoolValue::testingEquals(const Filter& other) const {
  auto otherBoolValue = dynamic_cast<const BoolValue*>(&other);
  return otherBoolValue != nullptr && Filter::testingBaseEquals(other) &&
      (value_ == otherBoolValue->value_);
}

folly::dynamic BigintRange::serialize() const {
  auto obj = Filter::serializeBase("BigintRange");
  obj["lower"] = lower_;
  obj["upper"] = upper_;
  return obj;
}

FilterPtr BigintRange::create(const folly::dynamic& obj) {
  auto lower = obj["lower"].asInt();
  auto upper = obj["upper"].asInt();
  auto nullAllowed = deserializeNullAllowed(obj);
  return std::make_unique<BigintRange>(lower, upper, nullAllowed);
}

bool BigintRange::testingEquals(const Filter& other) const {
  auto otherBigintRange = dynamic_cast<const BigintRange*>(&other);
  return otherBigintRange != nullptr && Filter::testingBaseEquals(other) &&
      (lower_ == otherBigintRange->lower_) &&
      (upper_ == otherBigintRange->upper_);
}

folly::dynamic NegatedBigintRange::serialize() const {
  auto obj = Filter::serializeBase("NegatedBigintRange");
  obj["lower"] = nonNegated_->lower();
  obj["upper"] = nonNegated_->upper();
  return obj;
}

FilterPtr NegatedBigintRange::create(const folly::dynamic& obj) {
  auto lower = obj["lower"].asInt();
  auto upper = obj["upper"].asInt();
  auto nullAllowed = deserializeNullAllowed(obj);
  return std::make_unique<NegatedBigintRange>(lower, upper, nullAllowed);
}

bool NegatedBigintRange::testingEquals(const Filter& other) const {
  auto otherNegatedBigintRange =
      dynamic_cast<const NegatedBigintRange*>(&other);
  return otherNegatedBigintRange != nullptr &&
      Filter::testingBaseEquals(other) &&
      nonNegated_->testingEquals(*(otherNegatedBigintRange->nonNegated_));
}

folly::dynamic HugeintRange::serialize() const {
  auto obj = Filter::serializeBase("HugeintRange");
  obj["lower"] = std::to_string(lower_);
  obj["upper"] = std::to_string(upper_);
  return obj;
}

FilterPtr HugeintRange::create(const folly::dynamic& obj) {
  auto lower = HugeInt::parse(obj["lower"].asString());
  auto upper = HugeInt::parse(obj["upper"].asString());
  auto nullAllowed = deserializeNullAllowed(obj);
  return std::make_unique<HugeintRange>(lower, upper, nullAllowed);
}

bool HugeintRange::testingEquals(const Filter& other) const {
  auto otherHugeintRange = dynamic_cast<const HugeintRange*>(&other);
  return otherHugeintRange != nullptr && Filter::testingBaseEquals(other) &&
      lower_ == otherHugeintRange->lower_ &&
      upper_ == otherHugeintRange->upper_;
}

folly::dynamic NegatedHugeintRange::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["lower"] = folly::to<std::string>(lower());
  obj["upper"] = folly::to<std::string>(upper());
  obj["nullAllowed"] = nullAllowed_;
  return obj;
}

FilterPtr NegatedHugeintRange::create(const folly::dynamic& obj) {
  auto lower = folly::to<int128_t>(obj["lower"].getString());
  auto upper = folly::to<int128_t>(obj["upper"].getString());
  auto nullAllowed = obj["nullAllowed"].getBool();
  return std::make_unique<NegatedHugeintRange>(lower, upper, nullAllowed);
}

bool NegatedHugeintRange::testingEquals(const Filter& other) const {
  if (dynamic_cast<const NegatedHugeintRange*>(&other)) {
    auto otherRange = static_cast<const NegatedHugeintRange*>(&other);
    return nullAllowed_ == otherRange->nullAllowed_ &&
        lower() == otherRange->lower() && upper() == otherRange->upper();
  }
  return false;
}

std::unique_ptr<Filter> NegatedHugeintRange::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kHugeintRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherRange = static_cast<const HugeintRange*>(other);
      std::vector<std::unique_ptr<HugeintRange>> rangeList;
      rangeList.emplace_back(std::make_unique<HugeintRange>(
          otherRange->lower(), otherRange->upper(), false));
      return combineNegatedRangeOnHugeintRanges(
          this->lower(), this->upper(), rangeList, bothNullAllowed);
    }
    case FilterKind::kNegatedHugeintRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherNegatedRange = static_cast<const NegatedHugeintRange*>(other);

      if (this->lower() > otherNegatedRange->lower()) {
        return other->mergeWith(this);
      }

      assert(this->lower() <= otherNegatedRange->lower());

      if (this->upper() < otherNegatedRange->lower()) {
        std::vector<std::unique_ptr<HugeintRange>> outRanges;
        int128_t smallLower = this->lower();
        int128_t smallUpper = this->upper();
        int128_t bigLower = otherNegatedRange->lower();
        int128_t bigUpper = otherNegatedRange->upper();

        // Add ranges below the first negated range
        if (smallLower > std::numeric_limits<int128_t>::min()) {
          outRanges.emplace_back(std::make_unique<HugeintRange>(
              std::numeric_limits<int128_t>::min(), smallLower - 1, false));
        }

        // Add range between the two negated ranges if there is a gap
        if (smallUpper < std::numeric_limits<int128_t>::max() &&
            bigLower > std::numeric_limits<int128_t>::min()) {
          outRanges.emplace_back(std::make_unique<HugeintRange>(
              smallUpper + 1, bigLower - 1, false));
        }

        // Add range above the second negated range
        if (bigUpper < std::numeric_limits<int128_t>::max()) {
          outRanges.emplace_back(std::make_unique<HugeintRange>(
              bigUpper + 1, std::numeric_limits<int128_t>::max(), false));
        }

        return combineHugeintRanges(std::move(outRanges), bothNullAllowed);
      }

      return std::make_unique<NegatedHugeintRange>(
          this->lower(),
          std::max(this->upper(), otherNegatedRange->upper()),
          bothNullAllowed);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

folly::dynamic TimestampRange::serialize() const {
  auto obj = Filter::serializeBase("TimestampRange");
  obj["lower"] = lower_.serialize();
  obj["upper"] = upper_.serialize();
  return obj;
}

FilterPtr TimestampRange::create(const folly::dynamic& obj) {
  auto lower = ISerializable::deserialize<Timestamp>(obj["lower"]);
  auto upper = ISerializable::deserialize<Timestamp>(obj["upper"]);
  auto nullAllowed = deserializeNullAllowed(obj);
  return std::make_unique<TimestampRange>(lower, upper, nullAllowed);
}

bool TimestampRange::testingEquals(const Filter& other) const {
  auto otherTimestampRange = dynamic_cast<const TimestampRange*>(&other);
  return otherTimestampRange != nullptr && Filter::testingBaseEquals(other) &&
      (lower_ == otherTimestampRange->lower_) &&
      (upper_ == otherTimestampRange->upper_);
}

NegatedTimestampRange::NegatedTimestampRange(
    const Timestamp& lower,
    const Timestamp& upper,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kNegatedTimestampRange),
      nonNegated_(
          std::make_unique<TimestampRange>(lower, upper, !nullAllowed)) {}

folly::dynamic NegatedTimestampRange::serialize() const {
  auto obj = Filter::serializeBase("NegatedTimestampRange");
  obj["lower"] = lower().serialize();
  obj["upper"] = upper().serialize();
  return obj;
}

FilterPtr NegatedTimestampRange::create(const folly::dynamic& obj) {
  auto lower = ISerializable::deserialize<Timestamp>(obj["lower"]);
  auto upper = ISerializable::deserialize<Timestamp>(obj["upper"]);
  auto nullAllowed = deserializeNullAllowed(obj);
  return std::make_unique<NegatedTimestampRange>(lower, upper, nullAllowed);
}

bool NegatedTimestampRange::testingEquals(const Filter& other) const {
  if (auto otherRange = dynamic_cast<const NegatedTimestampRange*>(&other)) {
    return Filter::testingBaseEquals(other) &&
        (lower() == otherRange->lower()) && (upper() == otherRange->upper());
  }
  return false;
}

std::unique_ptr<Filter> NegatedTimestampRange::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kTimestampRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherRange = static_cast<const TimestampRange*>(other);

      // If the other range is completely outside our negated range,
      // the result is a negated range from min to max of both ranges
      if (otherRange->upper() < lower() || otherRange->lower() > upper()) {
        return std::make_unique<TimestampRange>(
            std::min(lower(), otherRange->lower()),
            std::max(upper(), otherRange->upper()),
            bothNullAllowed);
      }

      // If the other range is completely inside our negated range,
      // the result is our original negated range
      if (otherRange->lower() >= lower() && otherRange->upper() <= upper()) {
        return this->clone(bothNullAllowed);
      }

      // If ranges overlap, split into multiple ranges as needed
      std::vector<std::unique_ptr<TimestampRange>> outRanges;

      // Add range before the negated range if it exists
      if (otherRange->lower() < lower()) {
        outRanges.emplace_back(std::make_unique<TimestampRange>(
            otherRange->lower(), lower(), false));
      }

      // Add range after the negated range if it exists
      if (otherRange->upper() > upper()) {
        outRanges.emplace_back(std::make_unique<TimestampRange>(
            upper(), otherRange->upper(), false));
      }

      return combineTimestampRanges(std::move(outRanges), bothNullAllowed);
    }
    case FilterKind::kNegatedTimestampRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherNegatedRange = static_cast<const NegatedTimestampRange*>(other);

      // If this range starts later, swap the order
      if (this->lower() > otherNegatedRange->lower()) {
        return other->mergeWith(this);
      }

      assert(this->lower() <= otherNegatedRange->lower());

      // If ranges are disjoint with a gap
      if (this->upper() < otherNegatedRange->lower()) {
        std::vector<std::unique_ptr<TimestampRange>> outRanges;

        // Add range below first negated range if it exists
        if (this->lower() > Timestamp::min()) {
          outRanges.emplace_back(std::make_unique<TimestampRange>(
              Timestamp::min(), this->lower(), false));
        }

        // Add range between negated ranges if there is a gap
        outRanges.emplace_back(std::make_unique<TimestampRange>(
            this->upper(), otherNegatedRange->lower(), false));

        // Add range above second negated range if it exists
        if (otherNegatedRange->upper() < Timestamp::max()) {
          outRanges.emplace_back(std::make_unique<TimestampRange>(
              otherNegatedRange->upper(), Timestamp::max(), false));
        }

        return combineTimestampRanges(std::move(outRanges), bothNullAllowed);
      }

      // Ranges overlap or are adjacent - merge them
      return std::make_unique<NegatedTimestampRange>(
          this->lower(),
          std::max(this->upper(), otherNegatedRange->upper()),
          bothNullAllowed);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> NegatedTimestampRange::clone(
    std::optional<bool> nullAllowed) const {
  return std::make_unique<NegatedTimestampRange>(
      nonNegated_->lower(),
      nonNegated_->upper(),
      nullAllowed.value_or(nullAllowed_));
}

bool NegatedTimestampRange::testInt128(const int128_t& value) const {
  return !nonNegated_->testInt128(value);
}

bool NegatedTimestampRange::testTimestamp(const Timestamp& value) const {
  return !nonNegated_->testTimestamp(value);
}

bool NegatedTimestampRange::testTimestampRange(
    Timestamp min,
    Timestamp max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  return !(nonNegated_->lower() <= min && max <= nonNegated_->upper());
}

const Timestamp NegatedTimestampRange::lower() const {
  return nonNegated_->lower();
}

const Timestamp NegatedTimestampRange::upper() const {
  return nonNegated_->upper();
}

bool NegatedTimestampRange::isSingleValue() const {
  return nonNegated_->isSingleValue();
}

std::string NegatedTimestampRange::toString() const {
  return fmt::format(
      "NegatedTimestampRange: NOT [{}, {}] {}",
      lower().toString(),
      upper().toString(),
      nullAllowed_ ? "with nulls" : "no nulls");
}

folly::dynamic BigintValuesUsingHashTable::serialize() const {
  auto obj = Filter::serializeBase("BigintValuesUsingHashTable");
  obj["min"] = min_;
  obj["max"] = max_;

  folly::dynamic values = folly::dynamic::array;
  for (auto v : values_) {
    values.push_back(v);
  }
  obj["values"] = values;

  return obj;
}

FilterPtr BigintValuesUsingHashTable::create(const folly::dynamic& obj) {
  auto nullAllowed = deserializeNullAllowed(obj);
  auto min = obj["min"].asInt();
  auto max = obj["max"].asInt();
  auto values = deserializeValues(obj);

  return std::make_unique<BigintValuesUsingHashTable>(
      min, max, values, nullAllowed);
}

bool BigintValuesUsingHashTable::testingEquals(const Filter& other) const {
  auto otherBigintValues =
      dynamic_cast<const BigintValuesUsingHashTable*>(&other);
  bool res = otherBigintValues != nullptr && Filter::testingBaseEquals(other) &&
      min_ == otherBigintValues->min_ && max_ == otherBigintValues->max_ &&
      values_.size() == otherBigintValues->values_.size();

  if (!res) {
    return false;
  }

  // values_ can be compared pair-wise since they are sorted.
  for (size_t i = 0; i < values_.size(); ++i) {
    if (values_.at(i) != otherBigintValues->values_.at(i)) {
      return false;
    }
  }

  return true;
}

folly::dynamic BigintValuesUsingBitmask::serialize() const {
  auto obj = Filter::serializeBase("BigintValuesUsingBitmask");
  obj["min"] = min_;
  obj["max"] = max_;

  folly::dynamic values = folly::dynamic::array;
  for (size_t i = 0; i < bitmask_.size(); ++i) {
    if (bitmask_[i]) {
      values.push_back(i + min_);
    }
  }
  obj["values"] = values;

  return obj;
}

FilterPtr BigintValuesUsingBitmask::create(const folly::dynamic& obj) {
  auto min = obj["min"].asInt();
  auto max = obj["max"].asInt();
  auto nullAllowed = deserializeNullAllowed(obj);
  auto values = deserializeValues(obj);

  return std::make_unique<BigintValuesUsingBitmask>(
      min, max, values, nullAllowed);
}

bool BigintValuesUsingBitmask::testingEquals(const Filter& other) const {
  auto otherBigintValues =
      dynamic_cast<const BigintValuesUsingBitmask*>(&other);
  bool res = otherBigintValues != nullptr && Filter::testingBaseEquals(other) &&
      min_ == otherBigintValues->min_ && max_ == otherBigintValues->max_ &&
      bitmask_.size() == otherBigintValues->bitmask_.size();

  if (!res) {
    return false;
  }

  for (size_t i = 0; i < bitmask_.size(); ++i) {
    if (bitmask_.at(i) != otherBigintValues->bitmask_.at(i)) {
      return false;
    }
  }

  return true;
}

folly::dynamic NegatedBigintValuesUsingHashTable::serialize() const {
  auto obj = Filter::serializeBase("NegatedBigintValuesUsingHashTable");
  obj["nonNegated"] = nonNegated_->serialize();
  return obj;
}

FilterPtr NegatedBigintValuesUsingHashTable::create(const folly::dynamic& obj) {
  auto nullAllowed = deserializeNullAllowed(obj);
  auto nonNegated =
      ISerializable::deserialize<BigintValuesUsingHashTable>(obj["nonNegated"]);
  auto min = nonNegated->min();
  auto max = nonNegated->max();
  auto values = nonNegated->values();
  auto res = std::make_unique<NegatedBigintValuesUsingHashTable>(
      min, max, values, nullAllowed);
  return res;
}

bool NegatedBigintValuesUsingHashTable::testingEquals(
    const Filter& other) const {
  auto otherNegatedBigintValues =
      dynamic_cast<const NegatedBigintValuesUsingHashTable*>(&other);
  return otherNegatedBigintValues != nullptr &&
      Filter::testingBaseEquals(other) &&
      nonNegated_->testingEquals(*(otherNegatedBigintValues->nonNegated_));
}

folly::dynamic NegatedBigintValuesUsingBitmask::serialize() const {
  auto obj = Filter::serializeBase("NegatedBigintValuesUsingBitmask");
  obj["min"] = min_;
  obj["max"] = max_;
  obj["nonNegated"] = nonNegated_->serialize();
  return obj;
}

FilterPtr NegatedBigintValuesUsingBitmask::create(const folly::dynamic& obj) {
  auto min = obj["min"].asInt();
  auto max = obj["max"].asInt();
  auto nullAllowed = deserializeNullAllowed(obj);
  auto nonNegated =
      ISerializable::deserialize<BigintValuesUsingBitmask>(obj["nonNegated"]);
  auto values = nonNegated->values();
  return std::make_unique<NegatedBigintValuesUsingBitmask>(
      min, max, values, nullAllowed);
}

bool NegatedBigintValuesUsingBitmask::testingEquals(const Filter& other) const {
  auto otherNegatedBigintValues =
      dynamic_cast<const NegatedBigintValuesUsingBitmask*>(&other);
  return otherNegatedBigintValues != nullptr &&
      Filter::testingBaseEquals(other) &&
      nonNegated_->testingEquals(*(otherNegatedBigintValues->nonNegated_));
}

FilterPtr AbstractRange::create(const folly::dynamic& obj) {
  auto lowerUnbounded = obj["lowerUnbounded"].asBool();
  auto lowerExclusive = obj["lowerExclusive"].asBool();
  auto upperUnbounded = obj["upperUnbounded"].asBool();
  auto upperExclusive = obj["upperExclusive"].asBool();
  auto lower = obj["lower"].asDouble();
  auto upper = obj["upper"].asDouble();
  auto nullAllowed = deserializeNullAllowed(obj);
  auto name = obj["name"].asString();

  if (name == "DoubleRange") {
    return std::make_unique<FloatingPointRange<double>>(
        lower,
        lowerUnbounded,
        lowerExclusive,
        upper,
        upperUnbounded,
        upperExclusive,
        nullAllowed);
  } else {
    return std::make_unique<FloatingPointRange<float>>(
        static_cast<float>(lower),
        lowerUnbounded,
        lowerExclusive,
        static_cast<float>(upper),
        upperUnbounded,
        upperExclusive,
        nullAllowed);
  }
}

folly::dynamic BytesRange::serialize() const {
  auto obj = AbstractRange::serializeBase("BytesRange");
  obj["lower"] = lower_;
  obj["upper"] = upper_;
  obj["singleValue"] = singleValue_;
  return obj;
}

FilterPtr BytesRange::create(const folly::dynamic& obj) {
  auto lowerUnbounded = obj["lowerUnbounded"].asBool();
  auto lowerExclusive = obj["lowerExclusive"].asBool();
  auto upperUnbounded = obj["upperUnbounded"].asBool();
  auto upperExclusive = obj["upperExclusive"].asBool();
  auto lower = obj["lower"].asString();
  auto upper = obj["upper"].asString();
  auto nullAllowed = deserializeNullAllowed(obj);
  return std::make_unique<BytesRange>(
      lower,
      lowerUnbounded,
      lowerExclusive,
      upper,
      upperUnbounded,
      upperExclusive,
      nullAllowed);
}

bool BytesRange::testingEquals(const Filter& other) const {
  auto otherBytesRange = dynamic_cast<const BytesRange*>(&other);
  return otherBytesRange != nullptr && Filter::testingBaseEquals(other) &&
      lower_ == otherBytesRange->lower_ && upper_ == otherBytesRange->upper_ &&
      singleValue_ == otherBytesRange->singleValue_;
}

folly::dynamic NegatedBytesRange::serialize() const {
  auto obj = Filter::serializeBase("NegatedBytesRange");
  obj["nonNegated"] = nonNegated_->serialize();
  return obj;
}

FilterPtr NegatedBytesRange::create(const folly::dynamic& obj) {
  auto nonNegated = ISerializable::deserialize<BytesRange>(obj["nonNegated"]);

  return std::make_unique<NegatedBytesRange>(
      nonNegated->lower(),
      nonNegated->lowerUnbounded(),
      nonNegated->lowerExclusive(),
      nonNegated->upper(),
      nonNegated->upperUnbounded(),
      nonNegated->upperExclusive(),
      nonNegated->testNull());
}

bool NegatedBytesRange::testingEquals(const Filter& other) const {
  auto otherNegatedBytesRange = dynamic_cast<const NegatedBytesRange*>(&other);
  return otherNegatedBytesRange != nullptr &&
      Filter::testingBaseEquals(other) &&
      nonNegated_->testingEquals(*(otherNegatedBytesRange->nonNegated_));
}

folly::dynamic BytesValues::serialize() const {
  auto obj = Filter::serializeBase("BytesValues");
  folly::dynamic values = folly::dynamic::array;
  for (auto v : value_) {
    values.push_back(v);
  }
  obj["values"] = values;
  return obj;
}

FilterPtr BytesValues::create(const folly::dynamic& obj) {
  auto nullAllowed = deserializeNullAllowed(obj);
  auto arr = obj["values"];
  std::vector<std::string> values;
  values.reserve(arr.size());

  for (const auto& v : arr) {
    values.emplace_back(v.asString());
  }

  return std::make_unique<BytesValues>(values, nullAllowed);
}

bool BytesValues::testingEquals(const Filter& other) const {
  auto otherBytesValues = dynamic_cast<const BytesValues*>(&other);
  auto res = otherBytesValues != nullptr && Filter::testingBaseEquals(other) &&
      lower_ == otherBytesValues->lower_ &&
      upper_ == otherBytesValues->upper_ &&
      set_.size() == otherBytesValues->set_.size() &&
      value_.size() == otherBytesValues->value_.size() &&
      lengths_.size() == otherBytesValues->lengths_.size();

  if (!res) {
    return false;
  }

  for (const auto& v : set_) {
    if (!otherBytesValues->set_.contains(v)) {
      return false;
    }
  }

  for (auto l : lengths_) {
    if (!otherBytesValues->lengths_.contains(l)) {
      return false;
    }
  }

  return true;
}

BytesValues::BytesValues(
    const std::vector<std::string>& values,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kBytesValues),
      set_(values.begin(), values.end()),
      value_(set_.begin(), set_.end()),
      isSingleValue_(values.size() == 1) {
  BOLT_CHECK(!values.empty(), "values must not be empty");

  for (const auto& value : values) {
    lengths_.insert(value.size());
  }

  lower_ = *std::min_element(value_.begin(), value_.end());
  upper_ = *std::max_element(value_.begin(), value_.end());
}

BytesValues::BytesValues(
    const std::vector<StringView>& values,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kBytesValues),
      set_(values.begin(), values.end()),
      value_(set_.begin(), set_.end()),
      isSingleValue_(values.size() == 1) {
  BOLT_CHECK(!values.empty(), "values must not be empty");

  // Convert StringView to std::string and add to sets
  for (const auto& value : values) {
    lengths_.insert(value.size());
  }

  // Find min/max values
  std::vector<std::string> strings;
  strings.reserve(values.size());
  for (const auto& value : values) {
    strings.emplace_back(value.data(), value.size());
  }
  lower_ = *std::min_element(strings.begin(), strings.end());
  upper_ = *std::max_element(strings.begin(), strings.end());
}

BytesValues::BytesValues(const BytesValues& other, bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kBytesValues),
      lower_(other.lower_),
      upper_(other.upper_),
      set_(other.set_),
      lengths_(other.lengths_),
      value_(set_.begin(), set_.end()),
      isSingleValue_((other.isSingleValue_)) {}

std::unique_ptr<Filter> BytesValues::clone(
    std::optional<bool> nullAllowed) const {
  if (nullAllowed) {
    return std::make_unique<BytesValues>(*this, nullAllowed.value());
  } else {
    return std::make_unique<BytesValues>(*this, nullAllowed_);
  }
}

bool BytesValues::hasTestLength() const {
  return true;
}

bool BytesValues::testLength(int32_t length) const {
  return lengths_.contains(length);
}

bool BytesValues::isSingleValue() const {
  return isSingleValue_;
}

bool BytesValues::testBytes(const char* value, int32_t length) const {
  const auto& valueSet_ = this->set();
  return lengths_.contains(length) &&
      valueSet_.contains(StringView(value, length));
}

const std::vector<StringView>& BytesValues::values() const {
  return value_;
}

folly::dynamic BigintMultiRange::serialize() const {
  auto obj = Filter::serializeBase("BigintMultiRange");
  folly::dynamic arr = folly::dynamic::array;
  for (const auto& r : ranges_) {
    arr.push_back(r->serialize());
  }
  obj["ranges"] = arr;
  return obj;
}

FilterPtr BigintMultiRange::create(const folly::dynamic& obj) {
  auto nullAllowed = deserializeNullAllowed(obj);
  auto arr = obj["ranges"];
  std::vector<std::unique_ptr<BigintRange>> ranges;
  ranges.reserve(arr.size());
  for (const auto& r : arr) {
    ranges.push_back(std::make_unique<BigintRange>(
        *ISerializable::deserialize<BigintRange>(r)));
  }
  return std::make_unique<BigintMultiRange>(std::move(ranges), nullAllowed);
}

bool BigintMultiRange::testingEquals(const Filter& other) const {
  auto otherBigintMultiRange = dynamic_cast<const BigintMultiRange*>(&other);
  auto res = otherBigintMultiRange != nullptr &&
      Filter::testingBaseEquals(other) &&
      ranges_.size() == otherBigintMultiRange->ranges_.size();

  if (!res) {
    return false;
  }

  for (size_t i = 0; i < ranges_.size(); ++i) {
    if (!(ranges_.at(i)->testingEquals(
            *(otherBigintMultiRange->ranges_.at(i))))) {
      return false;
    }
  }

  return true;
}

NegatedBytesValues::NegatedBytesValues(
    const std::vector<std::string>& values,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kNegatedBytesValues) {
  BOLT_CHECK(!values.empty(), "values must not be empty");
  nonNegated_ = std::make_unique<BytesValues>(values, !nullAllowed);
}

NegatedBytesValues::NegatedBytesValues(
    const NegatedBytesValues& other,
    bool nullAllowed)
    : Filter(true, nullAllowed, other.kind()),
      nonNegated_(std::make_unique<BytesValues>(
          other.nonNegated_->values(),
          other.nonNegated_->testNull())) {}

std::unique_ptr<Filter> NegatedBytesValues::clone(
    std::optional<bool> nullAllowed) const {
  return std::make_unique<NegatedBytesValues>(
      *this, nullAllowed.value_or(nullAllowed_));
}

bool NegatedBytesValues::hasTestLength() const {
  return true;
}

bool NegatedBytesValues::testLength(int32_t length) const {
  return true;
}

bool NegatedBytesValues::testBytes(const char* value, int32_t length) const {
  return !nonNegated_->testBytes(value, length);
}

const std::vector<StringView>& NegatedBytesValues::values() const {
  return nonNegated_->values();
}

bool NegatedBytesValues::isNegateFilter() const {
  return !nonNegated_->isNegateFilter();
}

folly::dynamic NegatedBytesValues::serialize() const {
  auto obj = Filter::serializeBase("NegatedBytesValues");
  obj["nonNegated"] = nonNegated_->serialize();
  return obj;
}

FilterPtr NegatedBytesValues::create(const folly::dynamic& obj) {
  auto nullAllowed = deserializeNullAllowed(obj);
  auto nonNegated = ISerializable::deserialize<BytesValues>(obj["nonNegated"]);
  return std::make_unique<NegatedBytesValues>(
      std::vector<std::string>(
          nonNegated->values().begin(), nonNegated->values().end()),
      nullAllowed);
}

bool NegatedBytesValues::testingEquals(const Filter& other) const {
  auto otherNegatedBytesValues =
      dynamic_cast<const NegatedBytesValues*>(&other);
  return otherNegatedBytesValues != nullptr &&
      Filter::testingBaseEquals(other) &&
      nonNegated_->testingEquals((*(otherNegatedBytesValues->nonNegated_)));
}

folly::dynamic MultiRange::serialize() const {
  auto obj = Filter::serializeBase("MultiRange");
  folly::dynamic arr = folly::dynamic::array;
  for (const auto& f : filters_) {
    arr.push_back(f->serialize());
  }
  obj["filters"] = arr;
  return obj;
}

bool MultiRange::hasTestLength() const {
  return true;
}

std::unique_ptr<Filter> MultiRange::create(const folly::dynamic& obj) {
  auto nullAllowed = deserializeNullAllowed(obj);
  folly::dynamic arr = obj["filters"];
  auto tmpFilters = ISerializable::deserialize<std::vector<Filter>>(arr);

  std::vector<std::unique_ptr<Filter>> filters;
  filters.reserve(tmpFilters.size());
  for (const auto& f : tmpFilters) {
    filters.emplace_back(f->clone());
  }

  return std::make_unique<MultiRange>(std::move(filters), nullAllowed);
}

bool MultiRange::testingEquals(const Filter& other) const {
  auto otherMultiRange = dynamic_cast<const MultiRange*>(&other);
  auto res = otherMultiRange != nullptr && Filter::testingBaseEquals(other) &&

      filters_.size() == otherMultiRange->filters_.size();

  if (!res) {
    return false;
  }

  for (size_t i = 0; i < filters_.size(); ++i) {
    if (!filters_.at(i)->testingEquals(
            *(otherMultiRange->filters_.at(i)->clone()))) {
      return false;
    }
  }

  return true;
}

BigintValuesUsingBitmask::BigintValuesUsingBitmask(
    int64_t min,
    int64_t max,
    const std::vector<int64_t>& values,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kBigintValuesUsingBitmask),
      min_(min),
      max_(max) {
  BOLT_CHECK(min < max, "min must be less than max");
  BOLT_CHECK(values.size() > 1, "values must contain at least 2 entries");

  const auto range = static_cast<size_t>(max - min + 1);
  bitmask_.resize(range);

  // Validate and set values
  for (int64_t value : values) {
    BOLT_CHECK_GE(
        value, min_, "Value {} is less than specified minimum {}", value, min_);
    BOLT_CHECK_LE(
        value,
        max_,
        "Value {} is greater than specified maximum {}",
        value,
        max_);
    bitmask_[value - min] = true;
  }

  for (int i = 0; i < bitmask_.size(); i++) {
    if (bitmask_[i]) {
      values_.push_back(min_ + i);
    }
  }
}

BigintValuesUsingBitmask::BigintValuesUsingBitmask(
    const BigintValuesUsingBitmask& other,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kBigintValuesUsingBitmask),
      bitmask_(other.bitmask_),
      values_(other.values_),
      min_(other.min_),
      max_(other.max_) {}

bool BigintValuesUsingBitmask::testInt64(int64_t value) const {
  if (value < min_ || value > max_) {
    return false;
  }
  return bitmask_[value - min_];
}

const std::vector<int64_t>& BigintValuesUsingBitmask::values() const {
  return values_;
}

bool BigintValuesUsingBitmask::testInt64Range(
    int64_t min,
    int64_t max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min == max) {
    return testInt64(min);
  }

  return !(min > max_ || max < min_);
}

std::unique_ptr<Filter> BigintValuesUsingBitmask::clone(
    std::optional<bool> nullAllowed) const {
  if (nullAllowed) {
    return std::make_unique<BigintValuesUsingBitmask>(
        *this, nullAllowed.value());
  } else {
    return std::make_unique<BigintValuesUsingBitmask>(*this, nullAllowed_);
  }
}

BigintValuesUsingHashTable::BigintValuesUsingHashTable(
    int64_t min,
    int64_t max,
    const std::vector<int64_t>& values,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kBigintValuesUsingHashTable),
      min_(min),
      max_(max),
      values_(values) {
  BOLT_CHECK(min < max, "min must be less than max");
  BOLT_CHECK(values.size() > 1, "values must contain at least 2 entries");

  for (auto value : values) {
    hashTable_.insert(value);
  }
  boost::sort::pdqsort(values_.begin(), values_.end());
}

bool BigintValuesUsingHashTable::testInt64(int64_t value) const {
  if (value < min_ || value > max_) {
    return false;
  }
  return hashTable_.contains(value);
}

xsimd::batch_bool<int64_t> BigintValuesUsingHashTable::testValues(
    xsimd::batch<int64_t> x) const {
  auto outOfRange = (x < xsimd::broadcast<int64_t>(min_)) |
      (x > xsimd::broadcast<int64_t>(max_));
  if (simd::toBitMask(outOfRange) == simd::allSetBitMask<int64_t>()) {
    return xsimd::batch_bool<int64_t>(false);
  }

  constexpr int kArraySize = xsimd::batch<int64_t>::size;

  // Use aligned storage for temporary array
  alignas(32) std::array<int64_t, kArraySize> values;

  // Verify alignment of temporary buffer
  BOLT_CHECK_EQ(
      reinterpret_cast<uintptr_t>(values.data()) & 31,
      0,
      "temporary buffer must be 32-byte aligned for SIMD operations");

  x.store_aligned(values.data());

  return std::apply(
      [this](auto... args) {
        return xsimd::batch_bool<int64_t>{
            (args >= min_ && args <= max_ && hashTable_.contains(args))...};
      },
      values);
}

xsimd::batch_bool<int32_t> BigintValuesUsingHashTable::testValues(
    xsimd::batch<int32_t> x) const {
  // Calls 4x64 twice since the hash table is 64 bits wide in any
  // case. A 32-bit hash table would be possible but all the use
  // cases seen are in the 64 bit range.
  auto first = simd::toBitMask(testValues(simd::getHalf<int64_t, 0>(x)));
  auto second = simd::toBitMask(testValues(simd::getHalf<int64_t, 1>(x)));
  return simd::fromBitMask<int32_t>(
      first | (second << xsimd::batch<int64_t>::size));
}

bool BigintValuesUsingHashTable::testInt64Range(
    int64_t min,
    int64_t max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min == max) {
    return testInt64(min);
  }

  if (min > max_ || max < min_) {
    return false;
  }
  auto it = std::lower_bound(values_.begin(), values_.end(), min);
  assert(it != values_.end()); // min is already tested to be <= max_.
  if (min == *it) {
    return true;
  }
  return max >= *it;
}

folly::dynamic HugeintValuesUsingHashTable::serialize() const {
  auto obj = Filter::serializeBase("HugeintValuesUsingHashTable");
  obj["min_lower"] = HugeInt::lower(min_);
  obj["min_upper"] = HugeInt::upper(min_);
  obj["max_lower"] = HugeInt::lower(max_);
  obj["max_upper"] = HugeInt::upper(max_);

  folly::dynamic lowerValues = folly::dynamic::array;
  folly::dynamic upperValues = folly::dynamic::array;
  for (auto v : values_) {
    lowerValues.push_back(HugeInt::lower(v));
    upperValues.push_back(HugeInt::upper(v));
  }
  obj["lower_values"] = lowerValues;
  obj["upper_values"] = upperValues;

  return obj;
}

FilterPtr HugeintValuesUsingHashTable::create(const folly::dynamic& obj) {
  auto nullAllowed = deserializeNullAllowed(obj);
  auto min = HugeInt::build(obj["min_upper"].asInt(), obj["min_lower"].asInt());
  auto max = HugeInt::build(obj["max_upper"].asInt(), obj["max_lower"].asInt());
  auto values = deserializeHugeintValues(obj);

  return std::make_unique<HugeintValuesUsingHashTable>(
      min, max, values, nullAllowed);
}

HugeintValuesUsingHashTable::HugeintValuesUsingHashTable(
    const int128_t min,
    const int128_t max,
    const std::vector<int128_t>& values,
    const bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kHugeintValuesUsingHashTable),
      min_(min),
      max_(max),
      set_(values.begin(), values.end()),
      values_(values) {
  BOLT_CHECK(!values.empty(), "values must not be empty");
  BOLT_CHECK_LE(min_, max_, "min must not be greater than max");
}

bool HugeintValuesUsingHashTable::testInt128(const int128_t& value) const {
  return set_.contains(value);
}

bool HugeintValuesUsingHashTable::testingEquals(const Filter& other) const {
  auto otherHugeintValues =
      dynamic_cast<const HugeintValuesUsingHashTable*>(&other);
  bool res = otherHugeintValues != nullptr &&
      Filter::testingBaseEquals(other) && min_ == otherHugeintValues->min_ &&
      max_ == otherHugeintValues->max_ &&
      values_.size() == otherHugeintValues->values_.size();
  if (!res) {
    return false;
  }

  for (auto value : values_) {
    if (!otherHugeintValues->set_.contains(value)) {
      return false;
    }
  }
  return true;
}

const std::vector<int128_t>& HugeintValuesUsingHashTable::values() const {
  return values_;
}

NegatedBigintValuesUsingBitmask::NegatedBigintValuesUsingBitmask(
    int64_t min,
    int64_t max,
    const std::vector<int64_t>& values,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kNegatedBigintValuesUsingBitmask),
      min_(min),
      max_(max) {
  BOLT_CHECK(min <= max, "min must be no greater than max");

  nonNegated_ = std::make_unique<BigintValuesUsingBitmask>(
      min, max, values, !nullAllowed);
}

bool NegatedBigintValuesUsingBitmask::testInt64Range(
    int64_t min,
    int64_t max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min == max) {
    return testInt64(min);
  }

  return true;
}

NegatedBigintValuesUsingHashTable::NegatedBigintValuesUsingHashTable(
    int64_t min,
    int64_t max,
    const std::vector<int64_t>& values,
    bool nullAllowed)
    : Filter(
          true,
          nullAllowed,
          FilterKind::kNegatedBigintValuesUsingHashTable) {
  nonNegated_ = std::make_unique<BigintValuesUsingHashTable>(
      min, max, values, !nullAllowed);
}

bool NegatedBigintValuesUsingHashTable::testInt64Range(
    int64_t min,
    int64_t max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min == max) {
    return testInt64(min);
  }

  if (max > nonNegated_->max() || min < nonNegated_->min()) {
    return true;
  }

  auto lo = std::lower_bound(
      nonNegated_->values().begin(), nonNegated_->values().end(), min);
  auto hi = std::lower_bound(
      nonNegated_->values().begin(), nonNegated_->values().end(), max);
  assert(
      lo !=
      nonNegated_->values().end()); // min is already tested to be <= max_.
  if (min != *lo || max != *hi) {
    // at least one of the endpoints of the range succeeds
    return true;
  }
  // Check if all values in this range are in values_ by counting the number
  // of things between min and max
  // if distance is any less, then we are missing an element => something
  // in the range is accepted
  return (std::distance(lo, hi) != max - min);
}

BigintMultiRange::BigintMultiRange(
    std::vector<std::unique_ptr<BigintRange>> ranges,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kBigintMultiRange),
      ranges_(std::move(ranges)) {
  BOLT_CHECK(!ranges_.empty(), "ranges is empty");
  BOLT_CHECK(ranges_.size() > 1, "should contain at least 2 ranges");
  for (const auto& range : ranges_) {
    lowerBounds_.push_back(range->lower());
  }
  for (int i = 1; i < lowerBounds_.size(); i++) {
    BOLT_CHECK(
        lowerBounds_[i] >= ranges_[i - 1]->upper(),
        "bigint ranges must not overlap");
  }
}

namespace {
int compareRanges(const char* lhs, size_t length, const std::string& rhs) {
  int size = std::min(length, rhs.length());
  int compare = memcmp(lhs, rhs.data(), size);
  if (compare) {
    return compare;
  }
  return length - rhs.size();
}
} // namespace

bool BytesRange::testBytes(const char* value, int32_t length) const {
  if (length == 0) {
    // Empty string. value is null. This is the smallest possible string.
    // It passes the following filters: < non-empty, <= empty | non-empty, >=
    // empty.
    if (lowerUnbounded_) {
      return !upper_.empty() || !upperExclusive_;
    }

    return lower_.empty() && !lowerExclusive_;
  }

  if (singleValue_) {
    if (length != lower_.size()) {
      return false;
    }
    return memcmp(value, lower_.data(), length) == 0;
  }
  if (!lowerUnbounded_) {
    int compare = compareRanges(value, length, lower_);
    if (compare < 0 || (lowerExclusive_ && compare == 0)) {
      return false;
    }
  }
  if (!upperUnbounded_) {
    int compare = compareRanges(value, length, upper_);
    return compare < 0 || (!upperExclusive_ && compare == 0);
  }
  return true;
}

bool BytesRange::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }
  // clang-format off
  return
    (lowerUnbounded_ || !max.has_value() || compareRanges(max->data(), max->length(), lower_) >=  !!lowerExclusive_) &&
    (upperUnbounded_ || !min.has_value() || compareRanges(min->data(), min->length(), upper_) <= -!!upperExclusive_);
  // clang-format on
}

bool BytesValues::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min.has_value() && max.has_value() && min.value() == max.value()) {
    return testBytes(min->data(), min->length());
  }

  // min > upper_
  if (min.has_value() &&
      compareRanges(min.value(), std::string_view(upper_)) > 0) {
    return false;
  }

  // max < lower_
  if (max.has_value() &&
      compareRanges(max.value(), std::string_view(lower_)) < 0) {
    return false;
  }

  return true;
}

bool NegatedBytesRange::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if ((!min.has_value() && !isLowerUnbounded()) ||
      (!max.has_value() && !isUpperUnbounded())) {
    return true;
  }

  if (min.has_value() && max.has_value() && min.value() == max.value()) {
    return testBytes(min->data(), min->length());
  }

  // if both min and max are within the negated range then reject
  if (!testBytes(min->data(), min->length()) &&
      !testBytes(max->data(), max->length())) {
    return false;
  }

  return true;
}

std::unique_ptr<Filter> NegatedBytesRange::toMultiRange() const {
  std::vector<std::unique_ptr<Filter>> accepted;
  if (!isLowerUnbounded()) {
    accepted.push_back(std::make_unique<BytesRange>(
        "",
        true,
        false,
        lower(),
        false,
        !testBytes(lower().data(), lower().length()),
        false));
  }
  if (!isUpperUnbounded()) {
    accepted.push_back(std::make_unique<BytesRange>(
        upper(),
        false,
        !testBytes(upper().data(), upper().length()),
        "",
        true,
        false,
        false));
  }

  if (accepted.size() == 0) {
    return nullOrFalse(nullAllowed_);
  }
  if (accepted.size() == 1) {
    return accepted[0]->clone(nullAllowed_);
  }
  return std::make_unique<MultiRange>(std::move(accepted), nullAllowed_, false);
}

bool NegatedBytesValues::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }
  if (min.has_value() && max.has_value() && min.value() == max.value()) {
    return testBytes(min->data(), min->length());
  }
  // a range of strings will always contain at least one string not in a set
  return true;
}

std::unique_ptr<Filter> BigintMultiRange::clone(
    std::optional<bool> nullAllowed) const {
  std::vector<std::unique_ptr<BigintRange>> ranges;
  ranges.reserve(ranges_.size());
  for (auto& range : ranges_) {
    ranges.emplace_back(std::make_unique<BigintRange>(*range));
  }
  if (nullAllowed) {
    return std::make_unique<BigintMultiRange>(
        std::move(ranges), nullAllowed.value());
  } else {
    return std::make_unique<BigintMultiRange>(std::move(ranges), nullAllowed_);
  }
}

bool BigintMultiRange::testInt64(int64_t value) const {
  int32_t i = binarySearch(lowerBounds_, value);
  if (i >= 0) {
    return true;
  }
  int place = (-i) - 1;
  if (place == 0) {
    // Below first
    return false;
  }
  // When value did not hit a lower bound of a filter, test with the filter
  // before the place where value would be inserted.
  return ranges_[place - 1]->testInt64(value);
}

bool BigintMultiRange::testInt64Range(int64_t min, int64_t max, bool hasNull)
    const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  for (const auto& range : ranges_) {
    if (range->testInt64Range(min, max, hasNull)) {
      return true;
    }
  }

  return false;
}

std::unique_ptr<Filter> MultiRange::clone(
    std::optional<bool> nullAllowed) const {
  std::vector<std::unique_ptr<Filter>> filters;
  for (auto& filter : filters_) {
    filters.push_back(filter->clone());
  }

  if (nullAllowed) {
    return std::make_unique<MultiRange>(
        std::move(filters), nullAllowed.value());
  } else {
    return std::make_unique<MultiRange>(std::move(filters), nullAllowed_);
  }
}

bool MultiRange::testDouble(double value) const {
  for (const auto& filter : filters_) {
    if (filter->testDouble(value)) {
      return true;
    }
  }
  return false;
}

bool MultiRange::testFloat(float value) const {
  for (const auto& filter : filters_) {
    if (filter->testFloat(value)) {
      return true;
    }
  }
  return false;
}

bool MultiRange::testInt128(const int128_t& value) const {
  for (const auto& filter : filters_) {
    if (filter->testInt128(value)) {
      return true;
    }
  }
  return false;
}

bool MultiRange::testBytes(const char* value, int32_t length) const {
  for (const auto& filter : filters_) {
    if (filter->testBytes(value, length)) {
      return true;
    }
  }
  return false;
}

bool MultiRange::testTimestamp(const Timestamp& timestamp) const {
  for (const auto& filter : filters_) {
    if (filter->testTimestamp(timestamp)) {
      return true;
    }
  }
  return false;
}

bool MultiRange::testLength(int32_t length) const {
  for (const auto& filter : filters_) {
    if (filter->testLength(length)) {
      return true;
    }
  }
  return false;
}

bool MultiRange::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  for (const auto& filter : filters_) {
    if (filter->testBytesRange(min, max, hasNull)) {
      return true;
    }
  }

  return false;
}

bool MultiRange::testDoubleRange(double min, double max, bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  for (const auto& filter : filters_) {
    if (filter->testDoubleRange(min, max, hasNull)) {
      return true;
    }
  }

  return false;
}

std::unique_ptr<Filter> MultiRange::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    // Rules of MultiRange with IsNull/IsNotNull
    // 1. MultiRange(nullAllowed=true) AND IS NULL => IS NULL
    // 2. MultiRange(nullAllowed=true) AND IS NOT NULL =>
    // MultiRange(nullAllowed=false)
    // 3. MultiRange(nullAllowed=false) AND IS NULL
    // => ALWAYS FALSE
    // 4. MultiRange(nullAllowed=false) AND IS NOT NULL
    // =>MultiRange(nullAllowed=false)
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kNegatedBytesRange:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(/*nullAllowed=*/false);
    case FilterKind::kDoubleRange:
    case FilterKind::kFloatRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      std::vector<std::unique_ptr<Filter>> merged;

      for (auto const& filter : this->filters()) {
        auto innerMerged = filter->mergeWith(other);
        switch (innerMerged->kind()) {
          case FilterKind::kAlwaysFalse:
          case FilterKind::kIsNull:
            continue;
          case FilterKind::kMultiRange: {
            auto innerMergedMulti =
                static_cast<const MultiRange*>(innerMerged.get());
            merged.reserve(merged.size() + innerMergedMulti->filters().size());
            for (int i = 0; i < innerMergedMulti->filters().size(); ++i) {
              merged.emplace_back(innerMergedMulti->filters()[i]->clone());
            }
            break;
          }
          default:
            merged.emplace_back(innerMerged.release());
        }
      }

      if (merged.empty()) {
        return nullOrFalse(bothNullAllowed);
      } else if (merged.size() == 1) {
        return merged.front()->clone(bothNullAllowed);
      } else {
        return std::make_unique<MultiRange>(std::move(merged), bothNullAllowed);
      }
    }
    case FilterKind::kBytesValues:
    case FilterKind::kNegatedBytesValues:
    case FilterKind::kBytesRange:
    case FilterKind::kMultiRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      std::vector<const Filter*> otherFilters;

      if (other->kind() == FilterKind::kMultiRange) {
        auto multiRangeOther = static_cast<const MultiRange*>(other);
        for (auto const& filterOther : multiRangeOther->filters()) {
          otherFilters.emplace_back(filterOther.get());
        }
      } else {
        otherFilters.emplace_back(other);
      }

      std::vector<std::string> byteValues;
      std::vector<std::unique_ptr<Filter>> merged;
      merged.reserve(this->filters().size() + otherFilters.size());

      for (auto const& filter : this->filters()) {
        for (auto const& filterOther : otherFilters) {
          auto innerMerged = filter->mergeWith(filterOther);
          switch (innerMerged->kind()) {
            case FilterKind::kAlwaysFalse:
            case FilterKind::kIsNull:
              continue;
            case FilterKind::kBytesValues: {
              auto mergedBytesValues =
                  static_cast<const BytesValues*>(innerMerged.get());
              byteValues.reserve(
                  byteValues.size() + mergedBytesValues->values().size());
              for (const auto& value : mergedBytesValues->values()) {
                byteValues.emplace_back(value);
              }
              break;
            }
            case FilterKind::kMultiRange: {
              auto innerMergedMulti =
                  static_cast<const MultiRange*>(innerMerged.get());
              merged.reserve(
                  merged.size() + innerMergedMulti->filters().size());
              for (int i = 0; i < innerMergedMulti->filters().size(); ++i) {
                merged.emplace_back(innerMergedMulti->filters()[i]->clone());
              }
              break;
            }
            default:
              merged.emplace_back(innerMerged.release());
          }
        }
      }

      if (!byteValues.empty()) {
        merged.emplace_back(std::make_unique<BytesValues>(
            std::move(byteValues), bothNullAllowed));
      }

      if (merged.empty()) {
        return nullOrFalse(bothNullAllowed);
      } else if (merged.size() == 1) {
        return merged.front()->clone(bothNullAllowed);
      } else {
        return std::make_unique<MultiRange>(std::move(merged), bothNullAllowed);
      }
    }
    default:
      BOLT_UNREACHABLE();
  }
}

bool Like::testBloomFilter(
    const std::vector<std::pair<
        std::unique_ptr<struct NgramTokenExtractor>,
        std::unique_ptr<NGramBloomFilter>>>& token_bloom_filters) const {
  if (token_bloom_filters.size() == 0) {
    return true;
  }

  for (auto split : value_) {
    for (auto& token_bloom_filter : token_bloom_filters) {
      BOLT_CHECK_NOT_NULL(token_bloom_filter.first);
      BOLT_CHECK_NOT_NULL(token_bloom_filter.second);
      uint32_t cur = 0;
      std::string token;
      auto tokenExtractor = token_bloom_filter.first.get();
      auto bloomFilter = token_bloom_filter.second.get();
      while (cur < split.size() &&
             tokenExtractor->nextInStringLike(
                 split.c_str(), split.size(), &cur, token)) {
        if (!bloomFilter->mayContain(token)) {
          return false;
        }
      }
    }
  }

  return true;
}

std::unique_ptr<Filter> IsNull::mergeWith(const Filter* other) const {
  BOLT_CHECK(other->isDeterministic());

  if (other->testNull()) {
    return this->clone();
  }

  return std::make_unique<AlwaysFalse>();
}

std::unique_ptr<Filter> IsNotNull::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kIsNotNull:
    case FilterKind::kCast:
      return this->clone();
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return std::make_unique<AlwaysFalse>();
    default:
      return other->mergeWith(this);
  }
}

std::unique_ptr<Filter> BoolValue::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<BoolValue>(value_, false);
    case FilterKind::kBoolValue: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      if (other->testBool(value_)) {
        return std::make_unique<BoolValue>(value_, bothNullAllowed);
      }

      return nullOrFalse(bothNullAllowed);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

namespace {
std::unique_ptr<Filter> combineBigintRanges(
    std::vector<std::unique_ptr<BigintRange>> ranges,
    bool nullAllowed) {
  if (ranges.empty()) {
    return nullOrFalse(nullAllowed);
  }

  if (ranges.size() == 1) {
    return std::make_unique<BigintRange>(
        ranges.front()->lower(), ranges.front()->upper(), nullAllowed);
  }

  return std::make_unique<BigintMultiRange>(std::move(ranges), nullAllowed);
}

std::unique_ptr<BigintRange> toBigintRange(std::unique_ptr<Filter> filter) {
  return std::unique_ptr<BigintRange>(
      dynamic_cast<BigintRange*>(filter.release()));
}

// takes a sorted vector of ranges and a sorted vector of rejected values, and
// returns a range filter of values accepted by both filters
std::unique_ptr<Filter> combineRangesAndNegatedValues(
    const std::vector<std::unique_ptr<BigintRange>>& ranges,
    std::vector<int64_t>& rejects,
    bool nullAllowed) {
  std::vector<std::unique_ptr<BigintRange>> outRanges;
  for (int i = 0; i < ranges.size(); ++i) {
    auto it =
        std::lower_bound(rejects.begin(), rejects.end(), ranges[i]->lower());
    int64_t start = ranges[i]->lower();
    int64_t end;

    while (it != rejects.end()) {
      end = *it - 1;
      if (start >= ranges[i]->lower() && end < ranges[i]->upper()) {
        if (start <= end) {
          outRanges.emplace_back(
              std::make_unique<common::BigintRange>(start, end, false));
        }
        start = *it + 1;
        ++it;
      } else {
        break;
      }
    }
    end = ranges[i]->upper();
    if (start <= end && start >= ranges[i]->lower() &&
        end <= ranges[i]->upper()) {
      outRanges.emplace_back(
          std::make_unique<common::BigintRange>(start, end, false));
    }
  }

  return combineBigintRanges(std::move(outRanges), nullAllowed);
}

std::unique_ptr<Filter> combineNegatedBigintLists(
    const std::vector<int64_t>& first,
    const std::vector<int64_t>& second,
    bool nullAllowed) {
  std::vector<int64_t> allRejected;
  allRejected.reserve(first.size() + second.size());

  auto it1 = first.begin();
  auto it2 = second.begin();

  // merge first and second lists
  while (it1 != first.end() && it2 != second.end()) {
    int64_t lo = std::min(*it1, *it2);
    allRejected.emplace_back(lo);
    // remove duplicates
    if (lo == *it1) {
      ++it1;
    }
    if (lo == *it2) {
      ++it2;
    }
  }
  // fill in remaining values from each list
  while (it1 != first.end()) {
    allRejected.emplace_back(*it1);
    ++it1;
  }
  while (it2 != second.end()) {
    allRejected.emplace_back(*it2);
    ++it2;
  }
  return createNegatedBigintValues(allRejected, nullAllowed);
}

std::unique_ptr<Filter> combineNegatedRangeOnIntRanges(
    int64_t negatedLower,
    int64_t negatedUpper,
    const std::vector<std::unique_ptr<BigintRange>>& ranges,
    bool nullAllowed) {
  std::vector<std::unique_ptr<BigintRange>> outRanges;
  // for a sensible set of ranges, at most one creates 2 output ranges
  outRanges.reserve(ranges.size() + 1);
  for (int i = 0; i < ranges.size(); ++i) {
    if (negatedUpper < ranges[i]->lower() ||
        ranges[i]->upper() < negatedLower) {
      outRanges.emplace_back(std::make_unique<BigintRange>(
          ranges[i]->lower(), ranges[i]->upper(), false));
    } else {
      if (ranges[i]->lower() < negatedLower) {
        outRanges.emplace_back(std::make_unique<BigintRange>(
            ranges[i]->lower(), negatedLower - 1, false));
      }
      if (negatedUpper < ranges[i]->upper()) {
        outRanges.emplace_back(std::make_unique<BigintRange>(
            negatedUpper + 1, ranges[i]->upper(), false));
      }
    }
  }

  return combineBigintRanges(std::move(outRanges), nullAllowed);
}

std::vector<std::unique_ptr<BigintRange>> negatedValuesToRanges(
    std::vector<int64_t>& values) {
  BOLT_DCHECK(std::is_sorted(values.begin(), values.end()));
  auto front = ++(values.begin());
  auto back = values.begin();
  std::vector<std::unique_ptr<BigintRange>> res;
  res.reserve(values.size() + 1);
  if (*back > std::numeric_limits<int64_t>::min()) {
    res.emplace_back(std::make_unique<BigintRange>(
        std::numeric_limits<int64_t>::min(), *back - 1, false));
  }
  while (front != values.end()) {
    if (*back + 1 <= *front - 1) {
      res.emplace_back(
          std::make_unique<BigintRange>(*back + 1, *front - 1, false));
    }
    ++front;
    ++back;
  }
  if (*back < std::numeric_limits<int64_t>::max()) {
    res.emplace_back(std::make_unique<BigintRange>(
        *back + 1, std::numeric_limits<int64_t>::max(), false));
  }
  return res;
}
} // namespace

std::unique_ptr<Filter> BigintRange::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<BigintRange>(lower_, upper_, false);
    case FilterKind::kBigintRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();

      auto otherRange = static_cast<const BigintRange*>(other);

      auto lower = std::max(lower_, otherRange->lower_);
      auto upper = std::min(upper_, otherRange->upper_);

      if (lower <= upper) {
        return std::make_unique<BigintRange>(lower, upper, bothNullAllowed);
      }

      return nullOrFalse(bothNullAllowed);
    }
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kBigintValuesUsingBitmask:
    case FilterKind::kBigintValuesUsingHashTable:
      return other->mergeWith(this);
    case FilterKind::kBigintMultiRange: {
      auto otherMultiRange = dynamic_cast<const BigintMultiRange*>(other);
      std::vector<std::unique_ptr<BigintRange>> newRanges;
      for (const auto& range : otherMultiRange->ranges()) {
        auto merged = this->mergeWith(range.get());
        if (merged->kind() == FilterKind::kBigintRange) {
          newRanges.push_back(toBigintRange(std::move(merged)));
        } else {
          BOLT_CHECK(merged->kind() == FilterKind::kAlwaysFalse);
        }
      }

      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return combineBigintRanges(std::move(newRanges), bothNullAllowed);
    }
    case FilterKind::kNegatedBigintValuesUsingBitmask:
    case FilterKind::kNegatedBigintValuesUsingHashTable: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      if (!other->testInt64Range(lower_, upper_, false)) {
        return nullOrFalse(bothNullAllowed);
      }
      std::vector<int64_t> vals;
      if (other->kind() == FilterKind::kNegatedBigintValuesUsingBitmask) {
        auto otherNegated =
            dynamic_cast<const NegatedBigintValuesUsingBitmask*>(other);
        vals = otherNegated->values();
      } else {
        auto otherNegated =
            dynamic_cast<const NegatedBigintValuesUsingHashTable*>(other);
        vals = otherNegated->values();
      }
      std::vector<std::unique_ptr<common::BigintRange>> rangeList;
      rangeList.emplace_back(
          std::make_unique<common::BigintRange>(lower_, upper_, false));
      return combineRangesAndNegatedValues(rangeList, vals, bothNullAllowed);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> TimestampRange::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kTimestampRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherRange = static_cast<const TimestampRange*>(other);

      auto lower = std::max(lower_, otherRange->lower_);
      auto upper = std::min(upper_, otherRange->upper_);

      if (lower <= upper) {
        return std::make_unique<TimestampRange>(lower, upper, bothNullAllowed);
      }

      return nullOrFalse(bothNullAllowed);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> NegatedBigintRange::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kBigintRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherRange = static_cast<const BigintRange*>(other);
      std::vector<std::unique_ptr<common::BigintRange>> rangeList;
      rangeList.emplace_back(std::make_unique<BigintRange>(
          otherRange->lower(), otherRange->upper(), false));
      return combineNegatedRangeOnIntRanges(
          this->lower(), this->upper(), rangeList, bothNullAllowed);
    }
    case FilterKind::kNegatedBigintRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherNegatedRange = static_cast<const NegatedBigintRange*>(other);
      if (this->lower() > otherNegatedRange->lower()) {
        return other->mergeWith(this);
      }
      assert(this->lower() <= otherNegatedRange->lower());
      if (this->upper() + 1 < otherNegatedRange->lower()) {
        std::vector<std::unique_ptr<common::BigintRange>> outRanges;
        int64_t smallLower = this->lower();
        int64_t smallUpper = this->upper();
        int64_t bigLower = otherNegatedRange->lower();
        int64_t bigUpper = otherNegatedRange->upper();
        if (smallLower > std::numeric_limits<int64_t>::min()) {
          outRanges.emplace_back(std::make_unique<common::BigintRange>(
              std::numeric_limits<int64_t>::min(), smallLower - 1, false));
        }
        if (smallUpper < std::numeric_limits<int64_t>::max() &&
            bigLower > std::numeric_limits<int64_t>::min()) {
          outRanges.emplace_back(std::make_unique<common::BigintRange>(
              smallUpper + 1, bigLower - 1, false));
        }
        if (bigUpper < std::numeric_limits<int64_t>::max()) {
          outRanges.emplace_back(std::make_unique<common::BigintRange>(
              bigUpper + 1, std::numeric_limits<int64_t>::max(), false));
        }
        return combineBigintRanges(std::move(outRanges), bothNullAllowed);
      }
      return std::make_unique<common::NegatedBigintRange>(
          this->lower(),
          std::max<int64_t>(this->upper(), otherNegatedRange->upper()),
          bothNullAllowed);
    }
    case FilterKind::kBigintMultiRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherMultiRanges = static_cast<const BigintMultiRange*>(other);
      return combineNegatedRangeOnIntRanges(
          this->lower(),
          this->upper(),
          otherMultiRanges->ranges(),
          bothNullAllowed);
    }
    case FilterKind::kBigintValuesUsingHashTable:
    case FilterKind::kBigintValuesUsingBitmask:
      return other->mergeWith(this);
    case FilterKind::kNegatedBigintValuesUsingHashTable:
    case FilterKind::kNegatedBigintValuesUsingBitmask: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      std::vector<int64_t> rejectedValues;
      if (other->kind() == FilterKind::kNegatedBigintValuesUsingHashTable) {
        auto otherHashTable =
            static_cast<const NegatedBigintValuesUsingHashTable*>(other);
        rejectedValues = otherHashTable->values();
      } else {
        auto otherBitmask =
            static_cast<const NegatedBigintValuesUsingBitmask*>(other);
        rejectedValues = otherBitmask->values();
      }
      if (nonNegated_->isSingleValue()) {
        if (other->testInt64(this->lower())) {
          rejectedValues.push_back(this->lower());
        }
        return createNegatedBigintValues(rejectedValues, bothNullAllowed);
      }
      return combineNegatedRangeOnIntRanges(
          this->lower(),
          this->upper(),
          negatedValuesToRanges(rejectedValues),
          bothNullAllowed);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> BigintValuesUsingHashTable::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<BigintValuesUsingHashTable>(*this, false);
    case FilterKind::kBigintRange: {
      auto otherRange = dynamic_cast<const BigintRange*>(other);
      auto min = std::max(min_, otherRange->lower());
      auto max = std::min(max_, otherRange->upper());

      return mergeWith(min, max, other);
    }
    case FilterKind::kBigintValuesUsingHashTable: {
      auto otherValues = dynamic_cast<const BigintValuesUsingHashTable*>(other);
      auto min = std::max(min_, otherValues->min());
      auto max = std::min(max_, otherValues->max());

      return mergeWith(min, max, other);
    }
    case FilterKind::kBigintValuesUsingBitmask:
      return other->mergeWith(this);
    case FilterKind::kBigintMultiRange: {
      auto otherMultiRange = dynamic_cast<const BigintMultiRange*>(other);

      std::vector<int64_t> valuesToKeep;
      for (const auto& range : otherMultiRange->ranges()) {
        auto min = std::max(min_, range->lower());
        auto max = std::min(max_, range->upper());

        if (min <= max) {
          for (int64_t v : values_) {
            if (range->testInt64(v)) {
              valuesToKeep.emplace_back(v);
            }
          }
        }
      }

      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return createBigintValues(valuesToKeep, bothNullAllowed);
    }
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kNegatedBigintValuesUsingBitmask:
    case FilterKind::kNegatedBigintValuesUsingHashTable: {
      return mergeWith(min_, max_, other);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> BigintValuesUsingHashTable::mergeWith(
    int64_t min,
    int64_t max,
    const Filter* other) const {
  bool bothNullAllowed = nullAllowed_ && other->testNull();

  if (max < min) {
    return nullOrFalse(bothNullAllowed);
  }

  if (max == min) {
    if (testInt64(min) && other->testInt64(min)) {
      return std::make_unique<BigintRange>(min, min, bothNullAllowed);
    }

    return nullOrFalse(bothNullAllowed);
  }

  std::vector<int64_t> valuesToKeep;
  valuesToKeep.reserve(values_.size());

  for (int64_t v : hashTable_) {
    if (other->testInt64(v)) {
      valuesToKeep.emplace_back(v);
    }
  }

  return createBigintValues(valuesToKeep, bothNullAllowed);
}

std::unique_ptr<Filter> BigintValuesUsingBitmask::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<BigintValuesUsingBitmask>(*this, false);
    case FilterKind::kBigintRange: {
      auto otherRange = dynamic_cast<const BigintRange*>(other);

      auto min = std::max(min_, otherRange->lower());
      auto max = std::min(max_, otherRange->upper());

      return mergeWith(min, max, other);
    }
    case FilterKind::kBigintValuesUsingHashTable: {
      auto otherValues = dynamic_cast<const BigintValuesUsingHashTable*>(other);

      auto min = std::max(min_, otherValues->min());
      auto max = std::min(max_, otherValues->max());

      return mergeWith(min, max, other);
    }
    case FilterKind::kBigintValuesUsingBitmask: {
      auto otherValues = dynamic_cast<const BigintValuesUsingBitmask*>(other);

      auto min = std::max(min_, otherValues->min_);
      auto max = std::min(max_, otherValues->max_);

      return mergeWith(min, max, other);
    }
    case FilterKind::kBigintMultiRange: {
      auto otherMultiRange = dynamic_cast<const BigintMultiRange*>(other);

      std::vector<int64_t> valuesToKeep;
      for (const auto& range : otherMultiRange->ranges()) {
        auto min = std::max(min_, range->lower());
        auto max = std::min(max_, range->upper());
        for (auto i = min; i <= max; ++i) {
          if (bitmask_[i - min_] && range->testInt64(i)) {
            valuesToKeep.push_back(i);
          }
        }
      }

      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return createBigintValues(valuesToKeep, bothNullAllowed);
    }
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kNegatedBigintValuesUsingBitmask:
    case FilterKind::kNegatedBigintValuesUsingHashTable: {
      return mergeWith(min_, max_, other);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> BigintValuesUsingBitmask::mergeWith(
    int64_t min,
    int64_t max,
    const Filter* other) const {
  bool bothNullAllowed = nullAllowed_ && other->testNull();

  std::vector<int64_t> valuesToKeep;
  for (auto i = min; i <= max; ++i) {
    if (bitmask_[i - min_] && other->testInt64(i)) {
      valuesToKeep.push_back(i);
    }
  }
  return createBigintValues(valuesToKeep, bothNullAllowed);
}

std::unique_ptr<Filter> NegatedBigintValuesUsingHashTable::mergeWith(
    const Filter* other) const {
  // Rules of NegatedBigintValuesUsingHashTable with IsNull/IsNotNull
  // 1. Negated...(nullAllowed=true) AND IS NULL => IS NULL
  // 2. Negated...(nullAllowed=true) AND IS NOT NULL =>
  // Negated...(nullAllowed=false)
  // 3. Negated...(nullAllowed=false) AND IS NULL
  // => ALWAYS FALSE
  // 4. Negated...(nullAllowed=false) AND IS NOT NULL
  // =>Negated...(nullAllowed=false)
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<NegatedBigintValuesUsingHashTable>(*this, false);
    case FilterKind::kBigintValuesUsingHashTable:
    case FilterKind::kBigintValuesUsingBitmask:
    case FilterKind::kBigintRange:
    case FilterKind::kBigintMultiRange: {
      return other->mergeWith(this);
    }
    case FilterKind::kNegatedBigintValuesUsingHashTable: {
      auto otherNegated =
          static_cast<const NegatedBigintValuesUsingHashTable*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return combineNegatedBigintLists(
          values(), otherNegated->values(), bothNullAllowed);
    }
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kNegatedBigintValuesUsingBitmask: {
      return other->mergeWith(this);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> NegatedBigintValuesUsingBitmask::mergeWith(
    const Filter* other) const {
  // Rules of NegatedBigintValuesUsingBitmask with IsNull/IsNotNull
  // 1. Negated...(nullAllowed=true) AND IS NULL => IS NULL
  // 2. Negated...(nullAllowed=true) AND IS NOT NULL =>
  // Negated...(nullAllowed=false)
  // 3. Negated...(nullAllowed=false) AND IS NULL
  // => ALWAYS FALSE
  // 4. Negated...(nullAllowed=false) AND IS NOT NULL
  // =>Negated...(nullAllowed=false)
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<NegatedBigintValuesUsingBitmask>(*this, false);
    case FilterKind::kBigintValuesUsingHashTable:
    case FilterKind::kBigintValuesUsingBitmask:
    case FilterKind::kBigintRange:
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kBigintMultiRange: {
      return other->mergeWith(this);
    }
    case FilterKind::kNegatedBigintValuesUsingHashTable: {
      auto otherHashTable =
          dynamic_cast<const NegatedBigintValuesUsingHashTable*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      // kEmptyMarker is already in values for a bitmask
      return combineNegatedBigintLists(
          values(), otherHashTable->values(), bothNullAllowed);
    }
    case FilterKind::kNegatedBigintValuesUsingBitmask: {
      auto otherBitmask =
          dynamic_cast<const NegatedBigintValuesUsingBitmask*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return combineNegatedBigintLists(
          values(), otherBitmask->values(), bothNullAllowed);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> BigintMultiRange::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull: {
      std::vector<std::unique_ptr<BigintRange>> ranges;
      ranges.reserve(ranges_.size());
      for (auto& range : ranges_) {
        ranges.push_back(std::make_unique<BigintRange>(*range));
      }
      return std::make_unique<BigintMultiRange>(std::move(ranges), false);
    }
    case FilterKind::kBigintRange:
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kBigintValuesUsingBitmask:
    case FilterKind::kBigintValuesUsingHashTable: {
      return other->mergeWith(this);
    }
    case FilterKind::kBigintMultiRange: {
      std::vector<std::unique_ptr<BigintRange>> newRanges;
      for (const auto& range : ranges_) {
        auto merged = range->mergeWith(other);
        if (merged->kind() == FilterKind::kBigintRange) {
          newRanges.push_back(toBigintRange(std::move(merged)));
        } else if (merged->kind() == FilterKind::kBigintMultiRange) {
          auto mergedMultiRange = dynamic_cast<BigintMultiRange*>(merged.get());
          for (const auto& newRange : mergedMultiRange->ranges_) {
            newRanges.push_back(toBigintRange(newRange->clone()));
          }
        } else {
          BOLT_CHECK(merged->kind() == FilterKind::kAlwaysFalse);
        }
      }

      bool bothNullAllowed = nullAllowed_ && other->testNull();
      if (newRanges.empty()) {
        return nullOrFalse(bothNullAllowed);
      }

      if (newRanges.size() == 1) {
        return std::make_unique<BigintRange>(
            newRanges.front()->lower(),
            newRanges.front()->upper(),
            bothNullAllowed);
      }

      return std::make_unique<BigintMultiRange>(
          std::move(newRanges), bothNullAllowed);
    }
    case FilterKind::kNegatedBigintValuesUsingHashTable:
    case FilterKind::kNegatedBigintValuesUsingBitmask: {
      std::vector<std::unique_ptr<BigintRange>> newRanges;
      std::vector<int64_t> rejects;
      if (other->kind() == FilterKind::kNegatedBigintValuesUsingBitmask) {
        auto otherNegated =
            dynamic_cast<const NegatedBigintValuesUsingBitmask*>(other);
        rejects = otherNegated->values();
      } else {
        auto otherNegated =
            dynamic_cast<const NegatedBigintValuesUsingHashTable*>(other);
        rejects = otherNegated->values();
      }

      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return combineRangesAndNegatedValues(ranges_, rejects, bothNullAllowed);
    }
    default:
      BOLT_UNREACHABLE();
  }
}

namespace {
// compareResult = left < right for upper, right < left for lower
bool mergeExclusive(int compareResult, bool left, bool right) {
  return compareResult == 0 ? (left || right)
                            : (compareResult < 0 ? left : right);
}
} // namespace

std::unique_ptr<Filter> BytesRange::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kBytesValues:
    case FilterKind::kNegatedBytesValues:
    case FilterKind::kNegatedBytesRange:
    case FilterKind::kMultiRange:
      return other->mergeWith(this);
    case FilterKind::kBytesRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();

      auto otherRange = static_cast<const BytesRange*>(other);

      bool upperUnbounded = false;
      bool lowerUnbounded = false;
      bool upperExclusive = false;
      bool lowerExclusive = false;
      std::string upper = "";
      std::string lower = "";

      if (lowerUnbounded_) {
        lowerUnbounded = otherRange->lowerUnbounded_;
        lowerExclusive = otherRange->lowerExclusive_;
        lower = otherRange->lower_;
      } else if (otherRange->lowerUnbounded_) {
        lowerUnbounded = lowerUnbounded_;
        lowerExclusive = lowerExclusive_;
        lower = lower_;
      } else {
        lowerUnbounded = false;
        auto compare = lower_.compare(otherRange->lower_);
        lower = compare < 0 ? otherRange->lower_ : lower_;
        lowerExclusive = mergeExclusive(
            -compare, lowerExclusive_, otherRange->lowerExclusive_);
      }

      if (upperUnbounded_) {
        upperUnbounded = otherRange->upperUnbounded_;
        upperExclusive = otherRange->upperExclusive_;
        upper = otherRange->upper_;
      } else if (otherRange->upperUnbounded_) {
        upperUnbounded = upperUnbounded_;
        upperExclusive = upperExclusive_;
        upper = upper_;
      } else {
        upperUnbounded = false;
        auto compare = upper_.compare(otherRange->upper_);
        upper = compare < 0 ? upper_ : otherRange->upper_;
        upperExclusive = mergeExclusive(
            compare, upperExclusive_, otherRange->upperExclusive_);
      }

      if (!lowerUnbounded && !upperUnbounded &&
          (lower > upper ||
           (lower == upper && (lowerExclusive || upperExclusive)))) {
        return nullOrFalse(bothNullAllowed);
      }

      return std::make_unique<BytesRange>(
          lower,
          lowerUnbounded,
          lowerExclusive,
          upper,
          upperUnbounded,
          upperExclusive,
          bothNullAllowed);
    }

    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> NegatedBytesRange::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kBytesValues:
    case FilterKind::kCast:
      return other->mergeWith(this);
    case FilterKind::kNegatedBytesValues:
    case FilterKind::kBytesRange:
    case FilterKind::kNegatedBytesRange:
    case FilterKind::kMultiRange: {
      // these cases are likely to end up as a MultiRange anyway
      return other->mergeWith(toMultiRange().get());
    }
    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> BytesValues::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kMultiRange:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kBytesValues: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherBytesValues = static_cast<const BytesValues*>(other);

      if (this->upper_.compare(otherBytesValues->lower_) < 0 ||
          otherBytesValues->upper_.compare(this->lower_) < 0) {
        return nullOrFalse(bothNullAllowed);
      }
      const BytesValues* smallerFilter = this;
      const BytesValues* largerFilter = otherBytesValues;
      if (this->values().size() > otherBytesValues->values().size()) {
        smallerFilter = otherBytesValues;
        largerFilter = this;
      }

      std::vector<std::string> newValues;
      newValues.reserve(smallerFilter->values().size());

      for (const auto& value : smallerFilter->values()) {
        if (largerFilter->set_.contains(value)) {
          newValues.emplace_back(value);
        }
      }

      if (newValues.empty()) {
        return nullOrFalse(bothNullAllowed);
      }

      return std::make_unique<BytesValues>(
          std::move(newValues), bothNullAllowed);
    }
    case FilterKind::kNegatedBytesValues: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      std::vector<std::string> newValues;
      newValues.reserve(values().size());
      for (const auto& value : values()) {
        if (other->testBytes(value.data(), value.size())) {
          newValues.emplace_back(value);
        }
      }

      if (newValues.empty()) {
        return nullOrFalse(bothNullAllowed);
      }

      return std::make_unique<BytesValues>(
          std::move(newValues), bothNullAllowed);
    }
    case FilterKind::kBytesRange: {
      auto otherBytesRange = static_cast<const BytesRange*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();

      if (!testBytesRange(
              otherBytesRange->isLowerUnbounded()
                  ? std::nullopt
                  : std::optional(otherBytesRange->lower()),
              otherBytesRange->isUpperUnbounded()
                  ? std::nullopt
                  : std::optional(otherBytesRange->upper()),
              bothNullAllowed)) {
        return nullOrFalse(bothNullAllowed);
      }

      std::vector<std::string> newValues;
      newValues.reserve(this->values().size());
      for (const auto& value : this->values()) {
        if (otherBytesRange->testBytes(value.data(), value.size())) {
          newValues.emplace_back(value);
        }
      }

      if (newValues.empty()) {
        return nullOrFalse(bothNullAllowed);
      }

      return std::make_unique<BytesValues>(
          std::move(newValues), bothNullAllowed);
    }
    case FilterKind::kNegatedBytesRange: {
      auto otherBytesRange = static_cast<const NegatedBytesRange*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();

      std::vector<std::string> newValues;
      newValues.reserve(this->values().size());
      for (const auto& value : this->values()) {
        if (otherBytesRange->testBytes(value.data(), value.size())) {
          newValues.emplace_back(value);
        }
      }

      if (newValues.empty()) {
        return nullOrFalse(bothNullAllowed);
      }

      return std::make_unique<BytesValues>(
          std::move(newValues), bothNullAllowed);
    }
    case FilterKind::kCast:
      return other->mergeWith(this);
    default:
      BOLT_UNREACHABLE();
  }
}

std::unique_ptr<Filter> NegatedBytesValues::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kBytesValues:
    case FilterKind::kNegatedBytesRange:
    case FilterKind::kMultiRange:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kNegatedBytesValues: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto negatedBytesOther = static_cast<const NegatedBytesValues*>(other);
      if (values().size() < negatedBytesOther->values().size()) {
        return other->mergeWith(this);
      }
      std::vector<std::string> rejectedValues;
      rejectedValues.reserve(
          values().size() + negatedBytesOther->values().size());
      rejectedValues.insert(
          rejectedValues.begin(), values().begin(), values().end());
      for (auto value : negatedBytesOther->values()) {
        // put in all values rejected by this filter that pass the other one
        if (testBytes(value.data(), value.size())) {
          rejectedValues.emplace_back(value);
        }
      }
      return std::make_unique<NegatedBytesValues>(
          std::move(rejectedValues), bothNullAllowed);
    }
    case FilterKind::kBytesRange: {
      auto bytesRangeOther = static_cast<const BytesRange*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      // ordered set of values in the range that are rejected
      std::set<std::string> rejectedValues;
      for (const auto& value : values()) {
        if (other->testBytes(value.data(), value.size())) {
          rejectedValues.insert(value);
        }
      }
      // edge checks - if an inclusive endpoint is negated, just make exclusive
      // std::set contains is C++ 20, so we use count instead :(
      bool loExclusive = !bytesRangeOther->lowerUnbounded() &&
          (bytesRangeOther->lowerExclusive() ||
           rejectedValues.count(bytesRangeOther->lower()) > 0);
      if (!bytesRangeOther->lowerUnbounded()) {
        rejectedValues.erase(bytesRangeOther->lower());
      }
      bool hiExclusive = !bytesRangeOther->upperUnbounded() &&
          (bytesRangeOther->upperExclusive() ||
           rejectedValues.count(bytesRangeOther->upper()) > 0);
      if (!bytesRangeOther->upperUnbounded()) {
        rejectedValues.erase(bytesRangeOther->upper());
      }
      if (rejectedValues.empty()) {
        return std::make_unique<BytesRange>(
            bytesRangeOther->lower(),
            bytesRangeOther->lowerUnbounded(),
            loExclusive,
            bytesRangeOther->upper(),
            bytesRangeOther->upperUnbounded(),
            hiExclusive,
            bothNullAllowed);
      }

      // accumulate filters in a vector here
      std::vector<std::unique_ptr<Filter>> ranges;
      ranges.reserve(rejectedValues.size() + 1);
      auto back = rejectedValues.begin();
      auto front = ++(rejectedValues.begin());
      ranges.emplace_back(std::make_unique<BytesRange>(
          bytesRangeOther->lower(),
          bytesRangeOther->lowerUnbounded(),
          loExclusive,
          *back,
          false, // not unbounded
          true, // exclusive
          false));
      while (front != rejectedValues.end()) {
        ranges.emplace_back(std::make_unique<BytesRange>(
            *back, false, true, *front, false, true, false));
        ++front;
        ++back;
      }
      ranges.emplace_back(std::make_unique<BytesRange>(
          *back,
          false,
          true,
          bytesRangeOther->upper(),
          bytesRangeOther->upperUnbounded(),
          hiExclusive,
          false));
      return std::make_unique<MultiRange>(
          std::move(ranges), bothNullAllowed, false);
    }
    case FilterKind::kCast:
      return other->mergeWith(this);
    default:
      BOLT_UNREACHABLE();
  }
}

} // namespace bytedance::bolt::common
