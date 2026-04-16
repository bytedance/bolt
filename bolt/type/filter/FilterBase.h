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

#include <algorithm>
#include <cmath>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <folly/Range.h>
#include <folly/container/F14Set.h>

#include "bolt/common/base/BloomFilter.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/common/serialization/Serializable.h"
#include "bolt/common/token/ITokenExtractor.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Subfield.h"
#include "bolt/type/Type.h"
#include "bolt/type/filter/FilterUtil.h"

namespace bytedance::bolt::common {

enum class FilterKind {
  kAlwaysFalse,
  kAlwaysTrue,
  kIsNull,
  kIsNotNull,
  kBoolValue,
  kBigintRange,
  kBigintValuesUsingHashTable,
  kBigintValuesUsingBitmask,
  kNegatedBigintRange,
  kNegatedBigintValuesUsingHashTable,
  kNegatedBigintValuesUsingBitmask,
  kDoubleRange,
  kNegatedDoubleValues,
  kDoubleValues,
  kFloatRange,
  kNegatedFloatValues,
  kFloatValues,
  kBytesRange,
  kNegatedBytesRange,
  kBytesValues,
  kNegatedBytesValues,
  kBigintMultiRange,
  kFloatMultiRange,
  kDoubleMultiRange,
  kMultiRange,
  kHugeintRange,
  kTimestampRange,
  kHugeintValuesUsingHashTable,
  kBytesLike,
  kNegatedDoubleRange,
  kNegatedFloatRange,
  kNegatedHugeintRange,
  kNegatedTimestampRange,
  kIsTrue,
  kCast,
  kMapSubscript
};

class Filter;
using FilterPtr = std::unique_ptr<Filter>;
using SubfieldFilters = std::unordered_map<Subfield, FilterPtr>;

class IFilterWithInnerFilter {
 public:
  virtual ~IFilterWithInnerFilter() = default;
  // Pure virtual methods that derived classes must implement
  virtual const Filter* innerFilter() const = 0;
  virtual const TypePtr targetType() const = 0;
  virtual const TypePtr sourceType() const = 0;
};

template <typename T>
class IFilterWithValues {
 public:
  virtual ~IFilterWithValues() = default;
  virtual const std::vector<T>& values() const = 0;

  const folly::F14FastSet<T>& set() const {
    std::call_once(initFlag, [this]() {
      for (const auto& value : values()) {
        valueSet_.insert(value);
      }
    });
    return valueSet_;
  }

  // Returns true if the filter is a negated filter another filter.
  virtual bool isNegateFilter() const = 0;

 protected:
  mutable std::once_flag initFlag;
  mutable folly::F14FastSet<T> valueSet_;
};

template <typename T>
class FloatingPointRange;

/**
 * A simple filter (e.g. comparison with literal) that can be applied
 * efficiently while extracting values from an ORC stream.
 */
class Filter : public bolt::ISerializable {
 protected:
  Filter(bool is_deterministic, bool nullAllowed, FilterKind kind)
      : nullAllowed_(nullAllowed),
        deterministic_(is_deterministic),
        kind_(kind) {}

 public:
  virtual ~Filter() = default;

  static void registerSerDe();

  // Templates parametrized on filter need to know determinism at compile
  // time. If this is false, deterministic() will be consulted at
  // runtime.
  static constexpr bool deterministic = true;

  FilterKind kind() const {
    return kind_;
  }

  bool nullAllowed() const {
    return nullAllowed_;
  }

  /// Return a copy of this filter. If nullAllowed is set, modified the
  /// nullAllowed flag in the copy to match.
  virtual std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const = 0;

  /**
   * A filter becomes non-deterministic when applies to nested column,
   * e.g. a[1] > 10 is non-deterministic because > 10 filter applies only to
   * some positions, e.g. first entry in a set of entries that correspond to a
   * single top-level position.
   */
  virtual bool isDeterministic() const {
    return deterministic_;
  }

  /**
   * When a filter applied to a nested column fails, the whole top-level
   * position should fail. To enable this functionality, the filter keeps track
   * of the boundaries of top-level positions and allows the caller to find out
   * where the current top-level position started and how far it continues.
   * @return number of positions from the start of the current top-level
   * position up to the current position (excluding current position)
   */
  virtual int getPrecedingPositionsToFail() const {
    return 0;
  }

  /**
   * Only used in test code.
   * @return Whether an object is the same as itself.
   */
  virtual bool testingEquals(const Filter& other) const = 0;

  bool testingBaseEquals(const Filter& other) const {
    return deterministic_ == other.isDeterministic() &&
        nullAllowed_ == other.nullAllowed_ && kind_ == other.kind();
  }

  /**
   * @return number of positions remaining until the end of the current
   * top-level position
   */
  virtual int getSucceedingPositionsToFail() const {
    return 0;
  }

  virtual bool testNull() const {
    return nullAllowed_;
  }

  /**
   * Used to apply is [not] null filters to complex types, e.g.
   * a[1] is null AND a[3] is not null, where a is an array(array(T)).
   *
   * In these case, the exact values are not known, but it is known whether they
   * are null or not. Furthermore, for some positions only nulls are allowed
   * (a[1] is null), for others only non-nulls (a[3] is not null), and for the
   * rest both are allowed (a[2] and a[N], where N > 3).
   */
  virtual bool testNonNull() const {
    BOLT_UNSUPPORTED("{}: testNonNull() is not supported.", toString());
  }

  virtual bool testInt64(int64_t /* unused */) const {
    BOLT_UNSUPPORTED("{}: testInt64() is not supported.", toString());
  }

  virtual bool testInt128(const int128_t& /* unused */) const {
    BOLT_UNSUPPORTED("{}: testInt128() is not supported.", toString());
  }

  virtual bool testDouble(double /* unused */) const {
    BOLT_UNSUPPORTED("{}: testDouble() is not supported.", toString());
  }

  virtual bool testFloat(float /* unused */) const {
    BOLT_UNSUPPORTED("{}: testFloat() is not supported.", toString());
  }

  virtual xsimd::batch_bool<int64_t> testValues(xsimd::batch<int64_t> x) const {
    return genericTestValues(x, [this](int64_t x) { return testInt64(x); });
  }

  virtual xsimd::batch_bool<int32_t> testValues(xsimd::batch<int32_t> x) const {
    return genericTestValues(x, [this](int32_t x) { return testInt64(x); });
  }

  virtual xsimd::batch_bool<int16_t> testValues(xsimd::batch<int16_t> x) const {
    return genericTestValues(x, [this](int16_t x) { return testInt64(x); });
  }

  virtual xsimd::batch_bool<double> testValues(xsimd::batch<double> x) const {
    return genericTestValues(x, [this](double x) { return testDouble(x); });
  }

  virtual xsimd::batch_bool<float> testValues(xsimd::batch<float> x) const {
    return genericTestValues(x, [this](float x) { return testFloat(x); });
  }

  virtual bool testBool(bool /* unused */) const {
    BOLT_UNSUPPORTED("{}: testBool() is not supported.", toString());
  }

  virtual bool testBytes(const char* /* unused */, int32_t /* unused */) const {
    BOLT_UNSUPPORTED("{}: testBytes() is not supported.", toString());
  }

  virtual bool testStringView(const StringView& view) const {
    return testBytes(view.data(), view.size());
  }

  virtual bool testTimestamp(const Timestamp& /* unused */) const {
    BOLT_UNSUPPORTED("{}: testTimestamp() is not supported.", toString());
  }

  // Returns true if it is useful to call testLength before other
  // tests. This should be true for string IN and equals because it is
  // possible to fail these based on the length alone. This would
  // typically be false of string ranges because these cannot be
  // generally decided without looking at the string itself.
  virtual bool hasTestLength() const {
    return false;
  }

  /**
   * Filters like string equality and IN, as well as conditions on cardinality
   * of lists and maps can be at least partly decided by looking at lengths
   * alone. If this is false, then no further checks are needed. If true,
   * eventual filters on the data itself need to be evaluated.
   */
  virtual bool testLength(int32_t /* unused */) const {
    BOLT_UNSUPPORTED("{}: testLength() is not supported.", toString());
  }

  // Tests multiple lengths at a time.
  virtual xsimd::batch_bool<int32_t> testLengths(
      xsimd::batch<int32_t> lengths) const {
    return genericTestValues(
        lengths, [this](int32_t x) { return testLength(x); });
  }

  // Returns true if at least one value in the specified range can pass the
  // filter. The range is defined as all values between min and max inclusive
  // plus null if hasNull is true.
  virtual bool
  testInt64Range(int64_t /*min*/, int64_t /*max*/, bool /*hasNull*/) const {
    BOLT_UNSUPPORTED("{}: testInt64Range() is not supported.", toString());
  }

  virtual bool testInt128Range(
      const int128_t& /*min*/,
      const int128_t& /*max*/,
      bool /*hasNull*/) const {
    BOLT_UNSUPPORTED("{}: testInt128Range() is not supported.", toString());
  }

  // Returns true if at least one value in the specified range can pass the
  // filter. The range is defined as all values between min and max inclusive
  // plus null if hasNull is true.
  virtual bool testDoubleRange(double /*min*/, double /*max*/, bool /*hasNull*/)
      const {
    BOLT_UNSUPPORTED("{}: testDoubleRange() is not supported.", toString());
  }

  virtual bool testBytesRange(
      std::optional<std::string_view> /*min*/,
      std::optional<std::string_view> /*max*/,
      bool /*hasNull*/) const {
    BOLT_UNSUPPORTED("{}: testBytesRange() is not supported.", toString());
  }

  virtual bool testTimestampRange(
      Timestamp /*min*/,
      Timestamp /*max*/,
      bool /*hasNull*/) const {
    BOLT_UNSUPPORTED("{}: testTimestampRange() is not supported.", toString());
  }

  // Combines this filter with another filter using 'AND' logic.
  virtual std::unique_ptr<Filter> mergeWith(const Filter* /*other*/) const {
    BOLT_UNSUPPORTED("{}: mergeWith() is not supported.", toString());
  }

  virtual bool testBloomFilter(
      const std::vector<std::pair<
          std::unique_ptr<struct NgramTokenExtractor>,
          std::unique_ptr<NGramBloomFilter>>>& token_bloom_filters) const {
    BOLT_UNSUPPORTED("{}: testBloomFilter() is not supported.", toString());
  }

  virtual std::string toString() const;

 protected:
  folly::dynamic serializeBase(std::string_view name) const;

 protected:
  const bool nullAllowed_;

 private:
  const bool deterministic_;
  const FilterKind kind_;

  template <typename T, typename F>
  xsimd::batch_bool<T> genericTestValues(xsimd::batch<T> batch, F&& testValue)
      const {
    constexpr int N = decltype(batch)::size;
    constexpr int kAlign = decltype(batch)::arch_type::alignment();
    alignas(kAlign) T data[N];
    alignas(kAlign) T res[N];
    batch.store_aligned(data);
    for (int i = 0; i < N; ++i) {
      res[i] = testValue(data[i]);
    }
    return xsimd::broadcast<T>(0) != xsimd::load_aligned(res);
  }
};

/// TODO Check if this filter is needed. This should not be passed down.
class AlwaysFalse final : public Filter {
 public:
  AlwaysFalse() : Filter(true, false, FilterKind::kAlwaysFalse) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& /*obj*/);

  bool testingEquals(const Filter& other) const final {
    return Filter::testingBaseEquals(other);
  }

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<AlwaysFalse>();
  }

  bool testNonNull() const final {
    return false;
  }

  bool testNull() const final {
    return false;
  }

  bool testInt64(int64_t /* unused */) const final {
    return false;
  }

  bool testInt64Range(int64_t /*min*/, int64_t /*max*/, bool /*hasNull*/)
      const final {
    return false;
  }

  bool testInt128Range(
      const int128_t& /*min*/,
      const int128_t& /*max*/,
      bool /*hasNull*/) const final {
    return false;
  }

  bool testInt128(const int128_t& /* unused */) const final {
    return false;
  }

  bool testDouble(double /* unused */) const final {
    return false;
  }

  bool testDoubleRange(double /*min*/, double /*max*/, bool /*hasNull*/)
      const final {
    return false;
  }

  bool testFloat(float /* unused */) const final {
    return false;
  }

  bool testBool(bool /* unused */) const final {
    return false;
  }

  bool testBytes(const char* /* unused */, int32_t /* unused */) const final {
    return false;
  }

  bool testBytesRange(
      std::optional<std::string_view> /*min*/,
      std::optional<std::string_view> /*max*/,
      bool /*hasNull*/) const final {
    return false;
  }

  bool testTimestampRange(
      Timestamp /*min*/,
      Timestamp /*max*/,
      bool /*hasNull*/) const final {
    return false;
  }

  bool hasTestLength() const override {
    return true;
  }

  bool testLength(int32_t /* unused */) const final {
    return false;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* /*other*/) const final {
    // false AND <any> is false.
    return this->clone();
  }
};

/// TODO Check if this filter is needed. This should not be passed down.
class AlwaysTrue final : public Filter {
 public:
  AlwaysTrue() : Filter(true, true, FilterKind::kAlwaysTrue) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<AlwaysTrue>();
  }

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& /*obj*/);

  bool testingEquals(const Filter& other) const final {
    return Filter::testingBaseEquals(other);
  }

  bool testNull() const final {
    return true;
  }

  bool testNonNull() const final {
    return true;
  }

  bool testInt64(int64_t /* unused */) const final {
    return true;
  }

  bool testInt128(const int128_t& /* unused */) const final {
    return true;
  }

  bool testInt64Range(int64_t /*min*/, int64_t /*max*/, bool /*hasNull*/)
      const final {
    return true;
  }

  bool testInt128Range(
      const int128_t& /*min*/,
      const int128_t& /*max*/,
      bool /*hasNull*/) const final {
    return true;
  }

  bool testDoubleRange(double /*min*/, double /*max*/, bool /*hasNull*/)
      const final {
    return true;
  }

  bool testDouble(double /* unused */) const final {
    return true;
  }

  bool testFloat(float /* unused */) const final {
    return true;
  }

  bool testBool(bool /* unused */) const final {
    return true;
  }

  bool testBytes(const char* /* unused */, int32_t /* unused */) const final {
    return true;
  }

  bool testBytesRange(
      std::optional<std::string_view> /*min*/,
      std::optional<std::string_view> /*max*/,
      bool /*hasNull*/) const final {
    return true;
  }

  bool testTimestampRange(
      Timestamp /*min*/,
      Timestamp /*max*/,
      bool /*hasNull*/) const final {
    return true;
  }

  bool hasTestLength() const override {
    return true;
  }

  bool testLength(int32_t /* unused */) const final {
    return true;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final {
    // true AND <any> is <any>.
    return other->clone();
  }
};

class IsTrue final : public Filter {
 public:
  IsTrue() : Filter(true, true, FilterKind::kIsTrue) {}
  virtual ~IsTrue() = default;

  folly::dynamic serialize() const override;
  static FilterPtr create(const folly::dynamic& /*obj*/);

  bool testingEquals(const Filter& other) const final {
    return Filter::testingBaseEquals(other);
  }

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<IsTrue>();
  }

  bool testBool(bool value) const final {
    return value;
  }
};

/// Returns true if the value is null. Supports all data types.
class IsNull final : public Filter {
 public:
  IsNull() : Filter(true, true, FilterKind::kIsNull) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& /*obj*/);

  bool testingEquals(const Filter& other) const final {
    return Filter::testingBaseEquals(other);
  }

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<IsNull>();
  }

  bool testNonNull() const final {
    return false;
  }

  bool testInt64(int64_t /* unused */) const final {
    return false;
  }

  bool testInt128(const int128_t& /* unused */) const final {
    return false;
  }

  bool testInt64Range(int64_t /*min*/, int64_t /*max*/, bool hasNull)
      const final {
    return hasNull;
  }

  bool testInt128Range(
      const int128_t& /*min*/,
      const int128_t& /*max*/,
      bool hasNull) const final {
    return hasNull;
  }

  bool testDoubleRange(double /*min*/, double /*max*/, bool hasNull)
      const final {
    return hasNull;
  }

  bool testDouble(double /* unused */) const final {
    return false;
  }

  bool testFloat(float /* unused */) const final {
    return false;
  }

  bool testBool(bool /* unused */) const final {
    return false;
  }

  bool testBytes(const char* /* unused */, int32_t /* unused */) const final {
    return false;
  }

  bool testTimestamp(const Timestamp& /* unused */) const final {
    return false;
  }

  bool testBytesRange(
      std::optional<std::string_view> /*min*/,
      std::optional<std::string_view> /*max*/,
      bool hasNull) const final {
    return hasNull;
  }

  bool testTimestampRange(Timestamp /*min*/, Timestamp /*max*/, bool hasNull)
      const final {
    return hasNull;
  }

  bool hasTestLength() const override {
    return true;
  }

  bool testLength(int32_t /* unused */) const final {
    return false;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;
};

/// Returns true if the value is not null. Supports all data types.
class IsNotNull final : public Filter {
 public:
  IsNotNull() : Filter(true, false, FilterKind::kIsNotNull) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& /*obj*/);

  bool testingEquals(const Filter& other) const final {
    return Filter::testingBaseEquals(other);
  }

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<IsNotNull>();
  }

  bool testNonNull() const final {
    return true;
  }

  bool testInt64(int64_t /* unused */) const final {
    return true;
  }

  bool testInt128(const int128_t& /* unused */) const final {
    return true;
  }

  bool testInt64Range(int64_t /*min*/, int64_t /*max*/, bool /*hasNull*/)
      const final {
    return true;
  }

  bool testInt128Range(
      const int128_t& /*min*/,
      const int128_t& /*max*/,
      bool /*hasNull*/) const final {
    return true;
  }

  bool testDoubleRange(double /*min*/, double /*max*/, bool /*hasNull*/)
      const final {
    return true;
  }

  bool testDouble(double /* unused */) const final {
    return true;
  }

  bool testFloat(float /* unused */) const final {
    return true;
  }

  bool testBool(bool /* unused */) const final {
    return true;
  }

  bool testBytes(const char* /* unused */, int32_t /* unused */) const final {
    return true;
  }

  bool testTimestamp(const Timestamp& /* unused */) const final {
    return true;
  }

  bool testBytesRange(
      std::optional<std::string_view> /*min*/,
      std::optional<std::string_view> /*max*/,
      bool /*hasNull*/) const final {
    return true;
  }

  bool testTimestampRange(
      Timestamp /*min*/,
      Timestamp /*max*/,
      bool /*hasNull*/) const final {
    return true;
  }

  bool hasTestLength() const override {
    return true;
  }

  bool testLength(int /* unused */) const final {
    return true;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;
};

/// Tests whether boolean value is true or false or integral value is zero or
/// not. Support boolean and integral data types.
class BoolValue final : public Filter {
 public:
  /// @param value The boolean value that passes the filter. If true, integral
  /// values that are not zero are passing as well.
  /// @param nullAllowed Null values are passing the filter if true.
  BoolValue(bool value, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kBoolValue), value_(value) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  bool testingEquals(const Filter& other) const final;

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<BoolValue>(this->value_, nullAllowed.value());
    } else {
      return std::make_unique<BoolValue>(*this);
    }
  }

  bool testBool(bool value) const final {
    return value_ == value;
  }

  bool testInt64(int64_t value) const final {
    return value_ == (value != 0);
  }

  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final {
    if (hasNull && nullAllowed_) {
      return true;
    }

    if (value_) {
      return min != 0 || max != 0;
    } else {
      return min <= 0 && max >= 0;
    }
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

 private:
  const bool value_;
};

/// Range filter for integral data types. Supports open, closed and unbounded
/// ranges, e.g. c >= 10, c <= 34, c BETWEEN 10 and 34. Open ranges can be
/// implemented by using the value to the left or right of the end of the range,
/// e.g. a < 10 is equivalent to a <= 9.
class BigintRange final : public Filter {
 public:
  /// @param lower Lower end of the range, inclusive.
  /// @param upper Upper end of the range, inclusive.
  /// @param nullAllowed Null values are passing the filter if true.
  BigintRange(int64_t lower, int64_t upper, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kBigintRange),
        lower_(lower),
        upper_(upper),
        lower32_(std::max<int64_t>(lower, std::numeric_limits<int32_t>::min())),
        upper32_(std::min<int64_t>(upper, std::numeric_limits<int32_t>::max())),
        lower16_(std::max<int64_t>(lower, std::numeric_limits<int16_t>::min())),
        upper16_(std::min<int64_t>(upper, std::numeric_limits<int16_t>::max())),
        isSingleValue_(upper_ == lower_),
        inInt32Range_(
            lower <= std::numeric_limits<int32_t>::max() &&
            upper >= std::numeric_limits<int32_t>::min()),
        inInt16Range_(
            lower <= std::numeric_limits<int16_t>::max() &&
            upper >= std::numeric_limits<int16_t>::min()) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<BigintRange>(
          this->lower_, this->upper_, nullAllowed.value());
    } else {
      return std::make_unique<BigintRange>(*this);
    }
  }

  bool testInt64(int64_t value) const final {
    return value >= lower_ && value <= upper_;
  }

  xsimd::batch_bool<int64_t> testValues(
      xsimd::batch<int64_t> values) const final {
    if (isSingleValue_) {
      return values == xsimd::broadcast<int64_t>(lower_);
    }
    return (xsimd::broadcast<int64_t>(lower_) <= values) &
        (values <= xsimd::broadcast<int64_t>(upper_));
  }

  xsimd::batch_bool<int32_t> testValues(
      xsimd::batch<int32_t> values) const final {
    if (!inInt32Range_) {
      return xsimd::batch_bool<int32_t>(false);
    }
    if (isSingleValue_) {
      if (UNLIKELY(lower32_ != lower_)) {
        return xsimd::batch_bool<int32_t>(false);
      }
      return values == xsimd::broadcast<int32_t>(lower_);
    }
    return (xsimd::broadcast<int32_t>(lower32_) <= values) &
        (values <= xsimd::broadcast<int32_t>(upper32_));
  }

  xsimd::batch_bool<int16_t> testValues(
      xsimd::batch<int16_t> values) const final {
    if (!inInt16Range_) {
      return xsimd::batch_bool<int16_t>(false);
    }
    if (isSingleValue_) {
      if (UNLIKELY(lower16_ != lower_)) {
        return xsimd::batch_bool<int16_t>(false);
      }
      return values == xsimd::broadcast<int16_t>(lower_);
    }
    return (xsimd::broadcast<int16_t>(lower16_) <= values) &
        (values <= xsimd::broadcast<int16_t>(upper16_));
  }

  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final {
    if (hasNull && nullAllowed_) {
      return true;
    }

    return !(min > upper_ || max < lower_);
  }

  int64_t lower() const {
    return lower_;
  }

  int64_t upper() const {
    return upper_;
  }

  bool isSingleValue() const {
    return isSingleValue_;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  std::string toString() const final {
    return fmt::format(
        "BigintRange: [{}, {}] {}",
        lower_,
        upper_,
        nullAllowed_ ? "with nulls" : "no nulls");
  }

  bool testingEquals(const Filter& other) const final;

 private:
  const int64_t lower_;
  const int64_t upper_;
  const int32_t lower32_;
  const int32_t upper32_;
  const int16_t lower16_;
  const int16_t upper16_;
  const bool isSingleValue_;
  const bool inInt32Range_;
  const bool inInt16Range_;
};

class NegatedBigintRange final : public Filter {
 public:
  /// @param lower Lowest value in the rejected range, inclusive.
  /// @param upper Highest value in the range, inclusive.
  /// @param nullAllowed Null values are passing the filter if true.
  NegatedBigintRange(int64_t lower, int64_t upper, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kNegatedBigintRange),
        nonNegated_(std::make_unique<BigintRange>(lower, upper, !nullAllowed)) {
  }

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<NegatedBigintRange>(
        nonNegated_->lower(),
        nonNegated_->upper(),
        nullAllowed.value_or(nullAllowed_));
  }

  bool testInt64(int64_t value) const final {
    return !nonNegated_->testInt64(value);
  }

  xsimd::batch_bool<int64_t> testValues(
      xsimd::batch<int64_t> values) const final {
    return ~nonNegated_->testValues(values);
  }

  xsimd::batch_bool<int32_t> testValues(
      xsimd::batch<int32_t> values) const final {
    return ~nonNegated_->testValues(values);
  }

  xsimd::batch_bool<int16_t> testValues(
      xsimd::batch<int16_t> values) const final {
    return ~nonNegated_->testValues(values);
  }

  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final {
    if (hasNull && nullAllowed_) {
      return true;
    }

    return !(nonNegated_->lower() <= min && max <= nonNegated_->upper());
  }

  int64_t lower() const {
    return nonNegated_->lower();
  }

  int64_t upper() const {
    return nonNegated_->upper();
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  std::string toString() const final {
    return "Negated" + nonNegated_->toString();
  }

  bool testingEquals(const Filter& other) const final;

 private:
  std::unique_ptr<BigintRange> nonNegated_;
};

class HugeintRange final : public Filter {
 public:
  /// @param lower Lowest value in the rejected range, inclusive.
  /// @param upper Highest value in the range, inclusive.
  /// @param nullAllowed Null values are passing the filter if true.
  HugeintRange(int128_t lower, int128_t upper, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kHugeintRange),
        lower_(lower),
        upper_(upper) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<HugeintRange>(
          this->lower_, this->upper_, nullAllowed.value());
    } else {
      return std::make_unique<HugeintRange>(*this);
    }
  }

  bool testInt128(const int128_t& value) const final {
    return value >= lower_ && value <= upper_;
  }

  bool testInt128Range(const int128_t& min, const int128_t& max, bool hasNull)
      const final {
    if (hasNull && nullAllowed_) {
      return true;
    }

    return !(min > upper_ || max < lower_);
  }

  int128_t lower() const {
    return lower_;
  }

  int128_t upper() const {
    return upper_;
  }

  std::string toString() const final {
    return fmt::format(
        "HugeintRange: [{}, {}] {}",
        lower_,
        upper_,
        nullAllowed_ ? "with nulls" : "no nulls");
  }

  bool testingEquals(const Filter& other) const final;

 private:
  const int128_t lower_;
  const int128_t upper_;
};

/// Range filter for int128 data type that matches values outside the specified
/// range. For example, if range is [10, 20], this filter will match values < 10
/// OR > 20.
class NegatedHugeintRange final : public Filter {
 public:
  /// @param lower Lower bound of the rejected range (inclusive).
  /// @param upper Upper bound of the rejected range (inclusive).
  /// @param nullAllowed Null values pass the filter if true.
  NegatedHugeintRange(int128_t lower, int128_t upper, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kNegatedHugeintRange),
        nonNegated_(
            std::make_unique<HugeintRange>(lower, upper, !nullAllowed)) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<NegatedHugeintRange>(
        nonNegated_->lower(),
        nonNegated_->upper(),
        nullAllowed.value_or(nullAllowed_));
  }

  bool testInt128(const int128_t& value) const final {
    return !nonNegated_->testInt128(value);
  }

  bool testInt128Range(const int128_t& min, const int128_t& max, bool hasNull)
      const final {
    if (hasNull && nullAllowed_) {
      return true;
    }

    return !(nonNegated_->lower() <= min && max <= nonNegated_->upper());
  }

  int128_t lower() const {
    return nonNegated_->lower();
  }

  int128_t upper() const {
    return nonNegated_->upper();
  }

  std::string toString() const final {
    return "Negated" + nonNegated_->toString();
  }

  bool testingEquals(const Filter& other) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

 private:
  std::unique_ptr<HugeintRange> nonNegated_;
};

/// IN-list filter for integral data types. Implemented as a hash table. Good
/// for large number of values that do not fit within a small range.
class BigintValuesUsingHashTable final : public Filter,
                                         public IFilterWithValues<int64_t> {
 public:
  /// @param min Minimum value.
  /// @param max Maximum value.
  /// @param values A list of unique values that pass the filter. Must contain
  /// at least two entries.
  /// @param nullAllowed Null values are passing the filter if true.
  BigintValuesUsingHashTable(
      int64_t min,
      int64_t max,
      const std::vector<int64_t>& values,
      bool nullAllowed);

  BigintValuesUsingHashTable(
      const BigintValuesUsingHashTable& other,
      bool nullAllowed)
      : Filter(true, nullAllowed, other.kind()),
        min_(other.min_),
        max_(other.max_),
        hashTable_(other.hashTable_),
        values_(other.values_) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<BigintValuesUsingHashTable>(
          *this, nullAllowed.value());
    } else {
      return std::make_unique<BigintValuesUsingHashTable>(*this, nullAllowed_);
    }
  }

  bool testInt64(int64_t value) const final;
  xsimd::batch_bool<int64_t> testValues(xsimd::batch<int64_t>) const final;
  xsimd::batch_bool<int32_t> testValues(xsimd::batch<int32_t>) const final;
  xsimd::batch_bool<int16_t> testValues(xsimd::batch<int16_t> x) const final {
    return Filter::testValues(x);
  }
  bool testInt64Range(int64_t min, int64_t max, bool hashNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  int64_t min() const {
    return min_;
  }

  int64_t max() const {
    return max_;
  }

  const std::vector<int64_t>& values() const final {
    return values_;
  }

  bool isNegateFilter() const final {
    return false;
  }

  std::string toString() const final {
    return fmt::format(
        "BigintValuesUsingHashTable: [{}, {}] {}",
        min_,
        max_,
        nullAllowed_ ? "with nulls" : "no nulls");
  }

  bool testingEquals(const Filter& other) const final;

 private:
  std::unique_ptr<Filter>
  mergeWith(int64_t min, int64_t max, const Filter* other) const;

  const int64_t min_;
  const int64_t max_;
  folly::F14FastSet<int64_t> hashTable_;
  std::vector<int64_t> values_;
};

/// IN-list filter for int128_t data type, implemented as a hash table.
class HugeintValuesUsingHashTable final : public Filter,
                                          public IFilterWithValues<int128_t> {
 public:
  HugeintValuesUsingHashTable(
      const int128_t min,
      const int128_t max,
      const std::vector<int128_t>& values,
      const bool nullAllowed);

  HugeintValuesUsingHashTable(
      const HugeintValuesUsingHashTable& other,
      bool nullAllowed)
      : Filter(true, nullAllowed, other.kind()),
        min_(other.min_),
        max_(other.max_),
        values_(other.values_) {}

  folly::dynamic serialize() const override;
  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<HugeintValuesUsingHashTable>(
          *this, nullAllowed.value());
    } else {
      return std::make_unique<HugeintValuesUsingHashTable>(*this, nullAllowed_);
    }
  }

  bool testInt128(const int128_t& value) const final;
  bool testingEquals(const Filter& other) const override;
  const std::vector<int128_t>& values() const final;
  bool isNegateFilter() const final {
    return false;
  }

  int128_t min() const {
    return min_;
  }

  int128_t max() const {
    return max_;
  }

 private:
  const int128_t min_;
  const int128_t max_;
  folly::F14FastSet<int128_t> set_;
  std::vector<int128_t> values_;
};

/// IN-list filter for integral data types. Implemented as a bitmask.
/// Offers better performance than the hash table when the range of values is
/// small.
class BigintValuesUsingBitmask final : public Filter,
                                       public IFilterWithValues<int64_t> {
 public:
  /// @param min Minimum value in the range.
  /// @param max Maximum value in the range.
  /// @param values A list of unique values that pass the filter. Must contain
  /// at least two entries and all values must be within [min, max] range.
  /// @param nullAllowed Null values are passing the filter if true.
  /// @throw BoltException if preconditions are not met.
  BigintValuesUsingBitmask(
      int64_t min,
      int64_t max,
      const std::vector<int64_t>& values,
      bool nullAllowed);

  /// Copy constructor with optional null handling override
  BigintValuesUsingBitmask(
      const BigintValuesUsingBitmask& other,
      bool nullAllowed);

  // Serialization support
  folly::dynamic serialize() const override;
  static std::unique_ptr<Filter> create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const override;

  // IFilterWithValues interface
  const std::vector<int64_t>& values() const final;

  // Filter interface
  bool testInt64(int64_t value) const override;
  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const override;
  std::unique_ptr<Filter> mergeWith(const Filter* other) const override;
  bool testingEquals(const Filter& other) const override;
  bool isNegateFilter() const final {
    return false;
  }

  int64_t min() const {
    return min_;
  }

  int64_t max() const {
    return max_;
  }

 private:
  std::unique_ptr<Filter>
  mergeWith(int64_t min, int64_t max, const Filter* other) const;

  // We keep both bitmask and values_ to avoid recomputing values
  std::vector<bool> bitmask_;
  mutable std::vector<int64_t> values_; // Lazy initialized
  const int64_t min_;
  const int64_t max_;
};

// NOT IN-list filter for integral data types. Implemented as a hash table. Good
// for large number of rejected values that do not fit within a small range.
class NegatedBigintValuesUsingHashTable final
    : public Filter,
      public IFilterWithValues<int64_t> {
 public:
  /// @param min Minimum rejected value.
  /// @param max Maximum rejected value.
  /// @param values A list of unique values that fail the filter. Must contain
  /// at least two entries.
  /// @param nullAllowed Null values are passing the filter if true.
  NegatedBigintValuesUsingHashTable(
      int64_t min,
      int64_t max,
      const std::vector<int64_t>& values,
      bool nullAllowed);

  NegatedBigintValuesUsingHashTable(
      const NegatedBigintValuesUsingHashTable& other,
      bool nullAllowed)
      : Filter(true, nullAllowed, other.kind()),
        nonNegated_(std::make_unique<BigintValuesUsingHashTable>(
            *other.nonNegated_,
            other.nonNegated_->testNull())) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<NegatedBigintValuesUsingHashTable>(
        *this, nullAllowed.value_or(nullAllowed_));
  }

  bool testInt64(int64_t value) const final {
    return !nonNegated_->testInt64(value);
  }

  xsimd::batch_bool<int64_t> testValues(xsimd::batch<int64_t> x) const final {
    return ~nonNegated_->testValues(x);
  }

  xsimd::batch_bool<int32_t> testValues(xsimd::batch<int32_t> x) const final {
    return ~nonNegated_->testValues(x);
  }

  xsimd::batch_bool<int16_t> testValues(xsimd::batch<int16_t> x) const final {
    return ~nonNegated_->testValues(x);
  }

  bool testInt64Range(int64_t min, int64_t max, bool hashNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  int64_t min() const {
    return nonNegated_->min();
  }

  int64_t max() const {
    return nonNegated_->max();
  }

  const std::vector<int64_t>& values() const final {
    return nonNegated_->values();
  }

  std::string toString() const final {
    return fmt::format(
        "NegatedBigintValuesUsingHashTable: [{}, {}] {}",
        nonNegated_->min(),
        nonNegated_->max(),
        nullAllowed_ ? "with nulls" : "no nulls");
  }

  bool isNegateFilter() const final {
    return !nonNegated_->isNegateFilter();
  }

  bool testingEquals(const Filter& other) const final;

 private:
  std::unique_ptr<Filter>
  mergeWith(int64_t min, int64_t max, const Filter* other) const;

  std::unique_ptr<BigintValuesUsingHashTable> nonNegated_;
};

/// NOT IN-list filter for integral data types. Implemented as a bitmask. Offers
/// better performance than the hash table when the range of values is small.
class NegatedBigintValuesUsingBitmask final
    : public Filter,
      public IFilterWithValues<int64_t> {
 public:
  /// @param min Minimum REJECTED value.
  /// @param max Maximum REJECTED value.
  /// @param values A list of unique values that pass the filter. Must contain
  /// at least two entries.
  /// @param nullAllowed Null values are passing the filter if true.
  NegatedBigintValuesUsingBitmask(
      int64_t min,
      int64_t max,
      const std::vector<int64_t>& values,
      bool nullAllowed);
  NegatedBigintValuesUsingBitmask(
      const NegatedBigintValuesUsingBitmask& other,
      bool nullAllowed)
      : Filter(true, nullAllowed, other.kind()),
        min_(other.min_),
        max_(other.max_),
        nonNegated_(std::make_unique<BigintValuesUsingBitmask>(
            *other.nonNegated_,
            other.nonNegated_->testNull())) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<NegatedBigintValuesUsingBitmask>(
        *this, nullAllowed.value_or(nullAllowed_));
  }

  const std::vector<int64_t>& values() const final {
    return nonNegated_->values();
  }

  bool testInt64(int64_t value) const final {
    return !nonNegated_->testInt64(value);
  }
  bool isNegateFilter() const final {
    return !nonNegated_->isNegateFilter();
  }
  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  bool testingEquals(const Filter& other) const final;

  int64_t min() const {
    return min_;
  }

  int64_t max() const {
    return max_;
  }

 private:
  std::unique_ptr<Filter>
  mergeWith(int64_t min, int64_t max, const Filter* other) const;

  int min_;
  int max_;
  std::unique_ptr<BigintValuesUsingBitmask> nonNegated_;
};

/// Base class for range filters on floating point and string data types.
class AbstractRange : public Filter {
 public:
  bool lowerUnbounded() const {
    return lowerUnbounded_;
  }

  bool lowerExclusive() const {
    return lowerExclusive_;
  }

  bool upperUnbounded() const {
    return upperUnbounded_;
  }

  bool upperExclusive() const {
    return upperExclusive_;
  }

  static FilterPtr create(const folly::dynamic& obj);

 protected:
  AbstractRange(
      bool lowerUnbounded,
      bool lowerExclusive,
      bool upperUnbounded,
      bool upperExclusive,
      bool nullAllowed,
      FilterKind kind)
      : Filter(true, nullAllowed, kind),
        lowerUnbounded_(lowerUnbounded),
        lowerExclusive_(lowerExclusive),
        upperUnbounded_(upperUnbounded),
        upperExclusive_(upperExclusive) {
    BOLT_CHECK(
        !lowerUnbounded_ || !upperUnbounded_,
        "A range filter must have  a lower or upper  bound");
  }

  folly::dynamic serializeBase(std::string_view name) const {
    auto obj = Filter::serializeBase(name);
    obj["lowerUnbounded"] = lowerUnbounded_;
    obj["lowerExclusive"] = lowerExclusive_;
    obj["upperUnbounded"] = upperUnbounded_;
    obj["upperExclusive"] = upperExclusive_;
    return obj;
  }

 protected:
  const bool lowerUnbounded_;
  const bool lowerExclusive_;
  const bool upperUnbounded_;
  const bool upperExclusive_;
};

// Helper functions for float comparison
inline bool isLessThan(float a, float b) {
  constexpr float epsilon = std::numeric_limits<float>::epsilon();
  return (b - a) > (std::abs(a) + std::abs(b)) * epsilon;
}

inline bool isGreaterThan(float a, float b) {
  constexpr float epsilon = std::numeric_limits<float>::epsilon();
  return (a - b) > (std::abs(a) + std::abs(b)) * epsilon;
}

inline bool isEqual(float a, float b) {
  constexpr float epsilon = std::numeric_limits<float>::epsilon();
  return std::abs(a - b) <= (std::abs(a) + std::abs(b)) * epsilon;
}

/// Range filter for string data type. Supports open, closed and
/// unbounded ranges.
class BytesRange final : public AbstractRange {
 public:
  /// @param lower Lower end of the range.
  /// @param lowerUnbounded True if lower end is "negative infinity" in which
  /// case the value of lower is ignored.
  /// @param lowerExclusive True if open range, e.g. lower value doesn't pass
  /// the filter.
  /// @param upper Upper end of the range.
  /// @param upperUnbounded True if upper end is "positive infinity" in which
  /// case the value of upper is ignored.
  /// @param upperExclusive True if open range, e.g. upper value doesn't pass
  /// the filter.
  /// @param nullAllowed Null values are passing the filter if true.
  BytesRange(
      std::string lower,
      bool lowerUnbounded,
      bool lowerExclusive,
      std::string upper,
      bool upperUnbounded,
      bool upperExclusive,
      bool nullAllowed)
      : AbstractRange(
            lowerUnbounded,
            lowerExclusive,
            upperUnbounded,
            upperExclusive,
            nullAllowed,
            FilterKind::kBytesRange),
        lower_(std::move(lower)),
        upper_(std::move(upper)),
        lowerView_(lower_),
        upperView_(upper_),
        singleValue_(
            !lowerExclusive_ && !upperExclusive_ && !lowerUnbounded_ &&
            !upperUnbounded_ && lower_ == upper_) {
    // Always-true filters should be specified using AlwaysTrue.
    BOLT_CHECK(!lowerUnbounded_ || !upperUnbounded_);
  }

  BytesRange(const BytesRange& other, bool nullAllowed)
      : AbstractRange(
            other.lowerUnbounded_,
            other.lowerExclusive_,
            other.upperUnbounded_,
            other.upperExclusive_,
            nullAllowed,
            FilterKind::kBytesRange),
        lower_(other.lower_),
        upper_(other.upper_),
        lowerView_(lower_),
        upperView_(upper_),
        singleValue_(other.singleValue_) {}

  folly::dynamic serialize() const override;

  static std::unique_ptr<Filter> create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<BytesRange>(*this, nullAllowed.value());
    } else {
      return std::make_unique<BytesRange>(*this);
    }
  }

  std::string toString() const override {
    return fmt::format(
        "BytesRange: {}{}, {}{} {}",
        (lowerUnbounded_ || lowerExclusive_) ? "(" : "[",
        lowerUnbounded_ ? "..." : lower_,
        upperUnbounded_ ? "..." : upper_,
        (upperUnbounded_ || upperExclusive_) ? ")" : "]",
        nullAllowed_ ? "with nulls" : "no nulls");
  }

  bool testBytes(const char* value, int32_t length) const final;

  bool testStringView(const StringView& view) const final {
    if (singleValue_) {
      return view == lowerView_;
    }
    if (!lowerUnbounded_) {
      if (lowerExclusive_) {
        if (view <= lowerView_) {
          return false;
        }
      } else {
        if (view < lowerView_) {
          return false;
        }
      }
    }
    if (!upperUnbounded_) {
      if (upperExclusive_) {
        if (view >= upperView_) {
          return false;
        }
      } else {
        if (view > upperView_) {
          return false;
        }
      }
    }
    return true;
  }

  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  bool hasTestLength() const final {
    return true;
  }

  bool testLength(int length) const final {
    return !singleValue_ || static_cast<int64_t>(lower_.size()) == length;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  xsimd::batch_bool<int32_t> testLengths(
      xsimd::batch<int32_t> lengths) const final {
    BOLT_DCHECK(singleValue_);
    return lengths == xsimd::broadcast<int32_t>(lower_.size());
  }

  bool isSingleValue() const {
    return singleValue_;
  }

  bool isUpperUnbounded() const {
    return upperUnbounded_;
  }

  bool isLowerUnbounded() const {
    return lowerUnbounded_;
  }

  bool isUpperExclusive() const {
    return upperExclusive_;
  }

  bool isLowerExclusive() const {
    return lowerExclusive_;
  }

  const std::string& lower() const {
    return lower_;
  }

  const std::string& upper() const {
    return upper_;
  }

  bool testingEquals(const Filter& other) const final;

 private:
  const std::string lower_;
  const std::string upper_;
  const StringView lowerView_;
  const StringView upperView_;
  const bool singleValue_;
};

// Negated range filter for strings
class NegatedBytesRange final : public Filter {
 public:
  /// @param lower Lower end of the rejected range.
  /// @param lowerUnbounded True if lower end is "negative infinity" in which
  /// case the value of lower is ignored.
  /// @param lowerExclusive True if open range, e.g. lower value doesn't pass
  /// the filter.
  /// @param upper Upper end of the range.
  /// @param upperUnbounded True if upper end is "positive infinity" in which
  /// case the value of upper is ignored.
  /// @param upperExclusive True if open range, e.g. upper value doesn't pass
  /// the filter.
  /// @param nullAllowed Null values are passing the filter if true.
  NegatedBytesRange(
      const std::string& lower,
      bool lowerUnbounded,
      bool lowerExclusive,
      const std::string& upper,
      bool upperUnbounded,
      bool upperExclusive,
      bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kNegatedBytesRange) {
    nonNegated_ = std::make_unique<BytesRange>(
        lower,
        lowerUnbounded,
        lowerExclusive,
        upper,
        upperUnbounded,
        upperExclusive,
        nullAllowed);
  }

  NegatedBytesRange(const NegatedBytesRange& other, bool nullAllowed)
      : Filter(true, nullAllowed, other.kind()),
        nonNegated_(std::make_unique<BytesRange>(*other.nonNegated_)) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<NegatedBytesRange>(
        *this, nullAllowed.value_or(nullAllowed_));
  }

  std::string toString() const final {
    return "Negated" + nonNegated_->toString();
  }

  bool testBytes(const char* value, int32_t length) const final {
    return !nonNegated_->testBytes(value, length);
  }

  bool testStringView(const StringView& view) const final {
    return !nonNegated_->testStringView(view);
  }

  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  bool hasTestLength() const final {
    return true;
  }

  bool testLength(int length) const final {
    return nonNegated_->testLength(length);
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  bool isSingleValue() const {
    return nonNegated_->isSingleValue();
  }

  bool isUpperUnbounded() const {
    return nonNegated_->isUpperUnbounded();
  }

  bool isLowerUnbounded() const {
    return nonNegated_->isLowerUnbounded();
  }

  const std::string& lower() const {
    return nonNegated_->lower();
  }

  const std::string& upper() const {
    return nonNegated_->upper();
  }

  bool testingEquals(const Filter& other) const final;

 private:
  std::unique_ptr<Filter> toMultiRange() const;

  std::unique_ptr<BytesRange> nonNegated_;
};

/// Range filter for Timestamp. Supports open, closed and unbounded
/// ranges.
/// Examples:
/// c > timestamp '2023-07-19 17:00:00.000'
/// c <= timestamp '1970-02-01 08:00:00.000'
/// c BETWEEN timestamp '2002-12-19 23:00:00.000' and timestamp '2018-02-13
/// 08:00:00.000'
///
/// Open ranges can be implemented by using the value to the left
/// or right of the end of the range, e.g. a < timestamp '2023-07-19
/// 17:00:00.777' is equivalent to a <= timestamp '2023-07-19 17:00:00.776'.
class TimestampRange : public Filter {
 public:
  /// @param lower Lower end of the range, inclusive.
  /// @param upper Upper end of the range, inclusive.
  /// @param nullAllowed Null values are passing the filter if true.
  TimestampRange(
      const Timestamp& lower,
      const Timestamp& upper,
      bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kTimestampRange),
        lower_(lower),
        upper_(upper),
        singleValue_(lower_ == upper) {}

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<TimestampRange>(
          this->lower_, this->upper_, nullAllowed.value());
    } else {
      return std::make_unique<TimestampRange>(*this);
    }
  }

  std::string toString() const final {
    return fmt::format(
        "TimestampRange: [{}, {}] {}",
        lower_.toString(),
        upper_.toString(),
        nullAllowed_ ? "with nulls" : "no nulls");
  }

  bool testTimestamp(const Timestamp& value) const override {
    return value >= lower_ && value <= upper_;
  }

  bool testTimestampRange(Timestamp min, Timestamp max, bool hasNull)
      const final {
    if (hasNull && nullAllowed_) {
      return true;
    }

    return !(min > upper_ || max < lower_);
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  bool isSingleValue() const {
    return singleValue_;
  }

  const Timestamp lower() const {
    return lower_;
  }

  const Timestamp upper() const {
    return upper_;
  }

  bool testingEquals(const Filter& other) const final;

  const bool nullAllowed() const {
    return nullAllowed_;
  }

 private:
  const Timestamp lower_;
  const Timestamp upper_;
  const bool singleValue_;
};

/// Range filter for Timestamp that matches values outside the specified range.
/// For example, if range is [timestamp '2020-01-01', timestamp '2021-01-01'],
/// this filter will match values < '2020-01-01' OR > '2021-01-01'.
class NegatedTimestampRange final : public Filter {
 public:
  /// @param lower Lower end of the rejected range, inclusive.
  /// @param upper Upper end of the rejected range, inclusive.
  /// @param nullAllowed Null values pass the filter if true.
  NegatedTimestampRange(
      const Timestamp& lower,
      const Timestamp& upper,
      bool nullAllowed);

  folly::dynamic serialize() const override;
  static FilterPtr create(const folly::dynamic& obj);
  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;

  bool testInt128(const int128_t& value) const final;
  bool testTimestamp(const Timestamp& value) const final;
  bool testTimestampRange(Timestamp min, Timestamp max, bool hasNull)
      const final;

  const Timestamp lower() const;
  const Timestamp upper() const;
  bool isSingleValue() const;
  std::string toString() const final;
  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;
  bool testingEquals(const Filter& other) const final;

 private:
  const std::unique_ptr<class TimestampRange> nonNegated_;
};

/// IN-list filter for string data type.
class BytesValues final : public Filter, public IFilterWithValues<StringView> {
 public:
  /// @param values List of values that pass the filter. Must contain at least
  /// one entry.
  /// @param nullAllowed Null values are passing the filter if true.
  BytesValues(const std::vector<std::string>& values, bool nullAllowed);
  BytesValues(const std::vector<StringView>& values, bool nullAllowed);
  BytesValues(const BytesValues& other, bool nullAllowed);

  folly::dynamic serialize() const override;
  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;

  bool hasTestLength() const final;
  bool testLength(int32_t length) const final;
  bool isSingleValue() const;
  bool testBytes(const char* value, int32_t length) const final;
  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;
  const std::vector<StringView>& values() const final;
  bool testingEquals(const Filter& other) const final;
  bool isNegateFilter() const final {
    return false;
  }

  std::string toString() const final {
    std::string str = fmt::format(
        "BytesValues isSingleValue {} lower {}, upper {}",
        isSingleValue_,
        lower_,
        upper_);
    str += ", set is:<";
    // folly::F14FastSet is unordered, so let's sort it at the beginning
    std::vector<std::string> sortedVector(set_.begin(), set_.end());
    std::sort(sortedVector.begin(), sortedVector.end());
    for (const auto& s : sortedVector) {
      str += s + ",";
    }
    str += ">, value is:<";
    sortedVector.clear();
    for (const auto& v : value_) {
      sortedVector.emplace_back(std::string(v));
    }
    std::sort(sortedVector.begin(), sortedVector.end());
    for (const auto& v : sortedVector) {
      str += v + ",";
    }
    str += ">";
    return str;
  }

 private:
  std::string lower_;
  std::string upper_;
  folly::F14FastSet<std::string> set_;
  folly::F14FastSet<uint32_t> lengths_;
  std::vector<StringView> value_;
  const bool isSingleValue_;
};

/// Represents a combination of two of more range filters on integral types with
/// OR semantics. The filter passes if at least one of the contained filters
/// passes.
class BigintMultiRange final : public Filter {
 public:
  /// @param ranges List of range filters. Must contain at least two entries.
  /// @param nullAllowed Null values are passing the filter if true. nullAllowed
  /// flags in the 'ranges' filters are ignored.
  BigintMultiRange(
      std::vector<std::unique_ptr<BigintRange>> ranges,
      bool nullAllowed);

  BigintMultiRange(const BigintMultiRange& other, bool nullAllowed);

  folly::dynamic serialize() const override;

  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;

  bool testInt64(int64_t value) const final;

  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  const std::vector<std::unique_ptr<BigintRange>>& ranges() const {
    return ranges_;
  }

  std::string toString() const override {
    std::ostringstream out;
    out << "BigintMultiRange: [";
    for (const auto& range : ranges_) {
      out << " " << range->toString();
    }
    out << " ]" << (nullAllowed_ ? "with nulls" : "no nulls");
    return out.str();
  }

  bool testingEquals(const Filter& other) const final;

 private:
  const std::vector<std::unique_ptr<BigintRange>> ranges_;
  std::vector<int64_t> lowerBounds_;
};

/// NOT IN-list filter for string data type.
class NegatedBytesValues final : public Filter,
                                 public IFilterWithValues<StringView> {
 public:
  /// @param values List of values that fail the filter. Must contain at least
  /// one entry.
  /// @param nullAllowed Null values are passing the filter if true.
  NegatedBytesValues(const std::vector<std::string>& values, bool nullAllowed);
  NegatedBytesValues(const NegatedBytesValues& other, bool nullAllowed);

  folly::dynamic serialize() const override;
  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;

  bool hasTestLength() const final;
  bool testLength(int32_t length) const final;
  bool testBytes(const char* value, int32_t length) const final;
  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;
  const std::vector<StringView>& values() const final;
  bool testingEquals(const Filter& other) const final;
  bool isNegateFilter() const final;

 private:
  std::unique_ptr<BytesValues> nonNegated_;
};

/// Represents a combination of two of more filters with
/// OR semantics. The filter passes if at least one of the contained filters
/// passes.
class MultiRange final : public Filter {
 public:
  /// @param ranges List of range filters. Must contain at least two entries.
  /// All entries must support the same data types.
  /// @param nullAllowed Null values are passing the filter if true. nullAllowed
  /// flags in the 'ranges' filters are ignored.
  /// TODO: remove redundant param `nanAllowed` after presto removes the use of
  /// this param. For now, we set a default value to avoid breaking presto.
  MultiRange(
      std::vector<std::unique_ptr<Filter>> filters,
      bool nullAllowed,
      bool nanAllowed = false)
      : Filter(true, nullAllowed, FilterKind::kMultiRange),
        filters_(std::move(filters)) {}

  folly::dynamic serialize() const override;

  static std::unique_ptr<Filter> create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;

  bool testDouble(double value) const final;

  bool testFloat(float value) const final;

  bool testInt128(const int128_t& value) const final;

  bool testBytes(const char* value, int32_t length) const final;

  bool testTimestamp(const Timestamp& value) const final;

  bool hasTestLength() const final;

  bool testLength(int32_t length) const final;

  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  bool testDoubleRange(double min, double max, bool hasNull) const final;

  const std::vector<std::unique_ptr<Filter>>& filters() const {
    return filters_;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  bool testingEquals(const Filter& other) const final;

 private:
  const std::vector<std::unique_ptr<Filter>> filters_;
};

class Like final : public Filter {
 public:
  /// @param value value that pass the filter.
  /// @param nullAllowed Null value are passing the filter if true.
  Like(const std::string& value, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kBytesLike) {
    value_ = ITokenExtractor::splitToStringVector(value.c_str(), value.size());
    BOLT_CHECK(value_.size() > 0, "value must not be empty");
  }

  Like(const Like& other, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kBytesLike),
        value_(other.value_) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<Like>(*this, nullAllowed.value());
    } else {
      return std::make_unique<Like>(*this);
    }
  }

  bool testBloomFilter(
      const std::vector<std::pair<
          std::unique_ptr<struct NgramTokenExtractor>,
          std::unique_ptr<NGramBloomFilter>>>& token_bloom_filters) const final;

  const std::vector<std::string>& value() const {
    return value_;
  }

  folly::dynamic serialize() const {
    BOLT_CHECK(false, "Not implemented Like's serialize");
  }

  bool testingEquals(const Filter& other) const {
    BOLT_CHECK(false, "Not implemented Like's testingEquals");
  }

 private:
  std::vector<std::string> value_;
};

} // namespace bytedance::bolt::common
