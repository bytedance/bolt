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

#include <folly/dynamic.h>
#include <type_traits>
#include "bolt/common/base/SimdUtil.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/filter/FilterBase.h"
namespace bytedance::bolt::common {
/// Range filter for floating point data types. Supports open, closed and
/// unbounded ranges, e.g. c >= 10.3, c > 10.3, c <= 34.8, c < 34.8, c >= 10.3
/// AND c < 34.8, c BETWEEN 10.3 and 34.8.
/// @tparam T Floating point type: float or double.
template <typename T>
class FloatingPointRange final : public AbstractRange {
 public:
  /// @param lower Lower end of the range.
  /// @param lowerUnbounded True if lower end is negative infinity in which case
  /// the value of lower is ignored.
  /// @param lowerExclusive True if open range, e.g. lower value doesn't pass
  /// the filter.
  /// @param upper Upper end of the range.
  /// @param upperUnbounded True if upper end is positive infinity in which case
  /// the value of upper is ignored.
  /// @param upperExclusive True if open range, e.g. upper value doesn't pass
  /// the filter.
  /// @param nullAllowed Null values are passing the filter if true.
  FloatingPointRange(
      T lower,
      bool lowerUnbounded,
      bool lowerExclusive,
      T upper,
      bool upperUnbounded,
      bool upperExclusive,
      bool nullAllowed)
      : AbstractRange(
            lowerUnbounded,
            lowerExclusive,
            upperUnbounded,
            upperExclusive,
            nullAllowed,
            (std::is_same_v<T, double>) ? FilterKind::kDoubleRange
                                        : FilterKind::kFloatRange),
        lower_(lower),
        upper_(upper) {
    BOLT_CHECK(lowerUnbounded || !std::isnan(lower_));
    BOLT_CHECK(upperUnbounded || !std::isnan(upper_));
  }

  FloatingPointRange(const FloatingPointRange& other, bool nullAllowed)
      : AbstractRange(
            other.lowerUnbounded_,
            other.lowerExclusive_,
            other.upperUnbounded_,
            other.upperExclusive_,
            nullAllowed,
            (std::is_same_v<T, double>) ? FilterKind::kDoubleRange
                                        : FilterKind::kFloatRange),
        lower_(other.lower_),
        upper_(other.upper_) {
    BOLT_CHECK(lowerUnbounded_ || !std::isnan(lower_));
    BOLT_CHECK(upperUnbounded_ || !std::isnan(upper_));
  }

  folly::dynamic serialize() const override;

  double lower() const {
    return lower_;
  }

  double upper() const {
    return upper_;
  }

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<FloatingPointRange<T>>(
          *this, nullAllowed.value());
    } else {
      return std::make_unique<FloatingPointRange<T>>(*this);
    }
  }

  bool testDouble(double value) const final {
    return testFloatingPoint(value);
  }

  bool testFloat(float value) const final {
    return testFloatingPoint(value);
  }

  xsimd::batch_bool<double> testValues(xsimd::batch<double>) const final;
  xsimd::batch_bool<float> testValues(xsimd::batch<float>) const final;

  bool testDoubleRange(double min, double max, bool hasNull) const final {
    if (hasNull && nullAllowed_) {
      return true;
    }

    return !(
        (!upperUnbounded_ && min > upper_) ||
        (!lowerUnbounded_ && max < lower_));
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final {
    switch (other->kind()) {
      case FilterKind::kAlwaysTrue:
      case FilterKind::kAlwaysFalse:
      case FilterKind::kIsNull:
        return other->mergeWith(this);
      case FilterKind::kIsNotNull:
        return std::make_unique<FloatingPointRange<T>>(
            lower_,
            lowerUnbounded_,
            lowerExclusive_,
            upper_,
            upperUnbounded_,
            upperExclusive_,
            false);
      case FilterKind::kDoubleRange:
      case FilterKind::kFloatRange: {
        bool bothNullAllowed = nullAllowed_ && other->testNull();

        auto otherRange = static_cast<const FloatingPointRange<T>*>(other);

        auto lower = std::max(lower_, otherRange->lower_);
        auto upper = std::min(upper_, otherRange->upper_);

        auto bothLowerUnbounded =
            lowerUnbounded_ && otherRange->lowerUnbounded_;
        auto bothUpperUnbounded =
            upperUnbounded_ && otherRange->upperUnbounded_;

        auto lowerExclusive = !bothLowerUnbounded &&
            (!testDouble(lower) || !other->testDouble(lower));
        auto upperExclusive = !bothUpperUnbounded &&
            (!testDouble(upper) || !other->testDouble(upper));

        if (lower > upper || (lower == upper && lowerExclusive_)) {
          if (bothNullAllowed) {
            return std::make_unique<IsNull>();
          }
          return std::make_unique<AlwaysFalse>();
        }

        return std::make_unique<FloatingPointRange<T>>(
            lower,
            bothLowerUnbounded,
            lowerExclusive,
            upper,
            bothUpperUnbounded,
            upperExclusive,
            bothNullAllowed);
      }
      default:
        BOLT_UNREACHABLE();
    }
  }

  std::string toString() const override;

  bool testingEquals(const Filter& other) const final;

 private:
  std::string toString(const std::string& name) const {
    return fmt::format(
        "{}: {}{}, {}{} {}",
        name,
        (lowerExclusive_ || lowerUnbounded_) ? "(" : "[",
        lowerUnbounded_ ? "-inf" : std::to_string(lower_),
        upperUnbounded_ ? "nan" : std::to_string(upper_),
        (upperExclusive_ && !upperUnbounded_) ? ")" : "]",
        nullAllowed_ ? "with nulls" : "no nulls");
  }

  bool testFloatingPoint(T value) const {
    if (std::isnan(value)) {
      return upperUnbounded_;
    }
    if (!lowerUnbounded_) {
      if (value < lower_) {
        return false;
      }
      if (lowerExclusive_ && lower_ == value) {
        return false;
      }
    }
    if (!upperUnbounded_) {
      if (value > upper_) {
        return false;
      }
      if (upperExclusive_ && value == upper_) {
        return false;
      }
    }
    return true;
  }

  xsimd::batch_bool<T> testFloatingPoints(xsimd::batch<T> values) const {
    xsimd::batch_bool<T> result;
    if (!lowerUnbounded_) {
      auto allLower = xsimd::broadcast<T>(lower_);
      if (lowerExclusive_) {
        result = allLower < values;
      } else {
        result = allLower <= values;
      }
      if (!upperUnbounded_) {
        auto allUpper = xsimd::broadcast<T>(upper_);
        if (upperExclusive_) {
          result = result & (values < allUpper);
        } else {
          result = result & (values <= allUpper);
        }
      }
    } else {
      auto allUpper = xsimd::broadcast<T>(upper_);
      if (upperExclusive_) {
        result = values < allUpper;
      } else {
        result = values <= allUpper;
      }
    }
    if (upperUnbounded_) {
      auto nanResult = xsimd::isnan(values);
      result = xsimd::bitwise_or(nanResult, result);
    }
    return result;
  }

  const T lower_;
  const T upper_;
};

template <>
inline std::string FloatingPointRange<double>::toString() const {
  return toString("DoubleRange");
}

template <>
inline std::string FloatingPointRange<float>::toString() const {
  return toString("FloatRange");
}

template <>
inline bool FloatingPointRange<float>::testingEquals(
    const Filter& other) const {
  return toString() == other.toString();
}

template <>
inline bool FloatingPointRange<double>::testingEquals(
    const Filter& other) const {
  return toString() == other.toString();
}

template <>
inline xsimd::batch_bool<double> FloatingPointRange<double>::testValues(
    xsimd::batch<double> x) const {
  return testFloatingPoints(x);
}

template <>
inline xsimd::batch_bool<float> FloatingPointRange<double>::testValues(
    xsimd::batch<float> x) const {
  // Slow path for schema evolution:  handles the case where double filter is
  // applied to float data (e.g., reading float column with double type
  // request). Falls back to element-by-element testing as batch processing
  // requires type matching.
  return Filter::testValues(x);
}

template <>
inline xsimd::batch_bool<double> FloatingPointRange<float>::testValues(
    xsimd::batch<double>) const {
  BOLT_FAIL("Not defined for float filter");
}

template <>
inline xsimd::batch_bool<float> FloatingPointRange<float>::testValues(
    xsimd::batch<float> x) const {
  return testFloatingPoints(x);
}

using DoubleRange = FloatingPointRange<double>;
using FloatRange = FloatingPointRange<float>;

} // namespace bytedance::bolt::common
