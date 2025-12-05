/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include "bolt/type/filter/Cast.h"
#include <folly/Range.h>
#include <folly/String.h>
#include <ryu/ryu.h>
#include "Type.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/serialization/Serializable.h"
#include "bolt/type/NumericTypeUtils.h"

namespace {

// Helper macro to create cast filters for a given target type
#define HANDLE_TARGET_TYPES(SOURCE_KIND)                                   \
  switch (targetKind) {                                                    \
    case TypeKind::TINYINT:                                                \
      return std::make_unique<Cast<SOURCE_KIND, TypeKind::TINYINT>>(       \
          sourceType,                                                      \
          targetType,                                                      \
          std::move(innerFilter),                                          \
          std::move(mergedNonCastFilter));                                 \
    case TypeKind::SMALLINT:                                               \
      return std::make_unique<Cast<SOURCE_KIND, TypeKind::SMALLINT>>(      \
          sourceType,                                                      \
          targetType,                                                      \
          std::move(innerFilter),                                          \
          std::move(mergedNonCastFilter));                                 \
    case TypeKind::INTEGER:                                                \
      return std::make_unique<Cast<SOURCE_KIND, TypeKind::INTEGER>>(       \
          sourceType,                                                      \
          targetType,                                                      \
          std::move(innerFilter),                                          \
          std::move(mergedNonCastFilter));                                 \
    case TypeKind::BIGINT:                                                 \
      return std::make_unique<Cast<SOURCE_KIND, TypeKind::BIGINT>>(        \
          sourceType,                                                      \
          targetType,                                                      \
          std::move(innerFilter),                                          \
          std::move(mergedNonCastFilter));                                 \
    case TypeKind::REAL:                                                   \
      return std::make_unique<Cast<SOURCE_KIND, TypeKind::REAL>>(          \
          sourceType,                                                      \
          targetType,                                                      \
          std::move(innerFilter),                                          \
          std::move(mergedNonCastFilter));                                 \
    case TypeKind::DOUBLE:                                                 \
      return std::make_unique<Cast<SOURCE_KIND, TypeKind::DOUBLE>>(        \
          sourceType,                                                      \
          targetType,                                                      \
          std::move(innerFilter),                                          \
          std::move(mergedNonCastFilter));                                 \
    case TypeKind::VARCHAR:                                                \
      return std::make_unique<Cast<SOURCE_KIND, TypeKind::VARCHAR>>(       \
          sourceType,                                                      \
          targetType,                                                      \
          std::move(innerFilter),                                          \
          std::move(mergedNonCastFilter));                                 \
    case TypeKind::VARBINARY:                                              \
      return std::make_unique<Cast<SOURCE_KIND, TypeKind::VARBINARY>>(     \
          sourceType,                                                      \
          targetType,                                                      \
          std::move(innerFilter),                                          \
          std::move(mergedNonCastFilter));                                 \
    case TypeKind::BOOLEAN:                                                \
      return std::make_unique<Cast<SOURCE_KIND, TypeKind::BOOLEAN>>(       \
          sourceType,                                                      \
          targetType,                                                      \
          std::move(innerFilter),                                          \
          std::move(mergedNonCastFilter));                                 \
    case TypeKind::TIMESTAMP:                                              \
      return std::make_unique<Cast<SOURCE_KIND, TypeKind::TIMESTAMP>>(     \
          sourceType,                                                      \
          targetType,                                                      \
          std::move(innerFilter),                                          \
          std::move(mergedNonCastFilter));                                 \
    default:                                                               \
      BOLT_UNSUPPORTED(                                                    \
          "Unsupported target type for cast: {}", targetType->toString()); \
  }

} // namespace
namespace bytedance::bolt::common {
using bytedance::bolt::type::NumericTypeUtils;

std::unique_ptr<ISerializable> deserializeCastFilter(
    const folly::dynamic& obj) {
  TypePtr sourceType = Type::create(obj["sourceType"].asString());
  TypePtr targetType = Type::create(obj["targetType"].asString());
  std::shared_ptr<const Filter> inputFilter = nullptr;
  if (obj.count("inputFilter")) {
    inputFilter = ISerializable::deserialize<Filter>(obj["inputFilter"]);
  }
  std::shared_ptr<const Filter> mergedNonCastFilter = nullptr;
  if (obj.count("mergedNonCastFilter")) {
    mergedNonCastFilter =
        ISerializable::deserialize<Filter>(obj["mergedNonCastFilter"]);
  }
  return createCastFilter(
      sourceType,
      targetType,
      std::move(inputFilter),
      std::move(mergedNonCastFilter));
}

std::unique_ptr<Filter> createCastFilter(
    const TypePtr& sourceType,
    const TypePtr& targetType,
    std::shared_ptr<const Filter> innerFilter) {
  return createCastFilter(sourceType, targetType, innerFilter, nullptr);
}

std::unique_ptr<Filter> createCastFilter(
    const TypePtr& sourceType,
    const TypePtr& targetType,
    std::shared_ptr<const Filter> innerFilter,
    std::shared_ptr<const Filter> mergedNonCastFilter) {
  auto sourceKind = sourceType->kind();
  auto targetKind = targetType->kind();

  switch (sourceKind) {
    case TypeKind::TINYINT:
      HANDLE_TARGET_TYPES(TypeKind::TINYINT)
    case TypeKind::SMALLINT:
      HANDLE_TARGET_TYPES(TypeKind::SMALLINT)
    case TypeKind::INTEGER:
      HANDLE_TARGET_TYPES(TypeKind::INTEGER)
    case TypeKind::BIGINT:
      HANDLE_TARGET_TYPES(TypeKind::BIGINT)
    case TypeKind::REAL:
      HANDLE_TARGET_TYPES(TypeKind::REAL)
    case TypeKind::DOUBLE:
      HANDLE_TARGET_TYPES(TypeKind::DOUBLE)
    case TypeKind::VARCHAR:
      HANDLE_TARGET_TYPES(TypeKind::VARCHAR)
    case TypeKind::VARBINARY:
      HANDLE_TARGET_TYPES(TypeKind::VARBINARY)
    case TypeKind::BOOLEAN:
      HANDLE_TARGET_TYPES(TypeKind::BOOLEAN)
    case TypeKind::TIMESTAMP:
      HANDLE_TARGET_TYPES(TypeKind::TIMESTAMP)
    default:
      BOLT_UNSUPPORTED(
          "Unsupported source type for cast: {}", sourceType->toString());
  }
}

template <TypeKind SOURCE, TypeKind TARGET>
Cast<SOURCE, TARGET>::Cast(
    const TypePtr& sourceType,
    const TypePtr& targetType,
    std::shared_ptr<const Filter> innerFilter)
    : Filter(true, innerFilter->testNull(), FilterKind::kCast),
      sourceType_(sourceType),
      targetType_(targetType),
      innerFilter_(std::move(innerFilter)),
      mergedNonCastFilter_(nullptr) {
  BOLT_CHECK(sourceType->kind() == SOURCE);
  BOLT_CHECK(TARGET == TARGET)
}

template <TypeKind SOURCE, TypeKind TARGET>
Cast<SOURCE, TARGET>::Cast(
    const TypePtr& sourceType,
    const TypePtr& targetType,
    std::shared_ptr<const Filter> innerFilter,
    std::shared_ptr<const Filter> mergedNonCastFilter)
    : Filter(true, innerFilter->testNull(), FilterKind::kCast),
      sourceType_(sourceType),
      targetType_(targetType),
      innerFilter_(std::move(innerFilter)),
      mergedNonCastFilter_(std::move(mergedNonCastFilter)) {
  BOLT_CHECK(sourceType->kind() == SOURCE);
  BOLT_CHECK(targetType->kind() == TARGET)
}

template <TypeKind SOURCE, TypeKind TARGET>
std::unique_ptr<Filter> Cast<SOURCE, TARGET>::clone(
    std::optional<bool> nullAllowed) const {
  auto newInnerFilter = nullAllowed.has_value()
      ? innerFilter_->clone(nullAllowed.value())
      : innerFilter_->clone();

  std::shared_ptr<const Filter> newMergedFilter = nullptr;
  if (this->mergedNonCastFilter_ != nullptr) {
    newMergedFilter = nullAllowed.has_value()
        ? mergedNonCastFilter_->clone(nullAllowed.value())
        : mergedNonCastFilter_->clone();
  }

  return std::make_unique<Cast<SOURCE, TARGET>>(
      sourceType_,
      targetType_,
      std::move(newInnerFilter),
      std::move(newMergedFilter));
}

template <TypeKind SOURCE, TypeKind TARGET>
folly::dynamic Cast<SOURCE, TARGET>::serialize() const {
  auto base = serializeBase("Cast");
  base["sourceType"] = sourceType_->toString();
  base["targetType"] = targetType_->toString();
  base["innerFilter"] = innerFilter_->serialize();
  if (mergedNonCastFilter_ != nullptr) {
    base["mergedNonCastFilter"] = mergedNonCastFilter_->serialize();
  }
  return base;
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testingEquals(const Filter& other) const {
  // First check if the other filter is a Cast filter and if basic properties
  // match
  bool isTestingEquals =
      other.kind() == FilterKind::kCast && Filter::testingBaseEquals(other);

  const Cast* otherCast = nullptr;
  if (isTestingEquals) {
    // If basic equality check passed, cast the other filter to Cast type
    otherCast = static_cast<const Cast*>(&other);

    // Check if source and target types have the same kind and if inner filters
    // are equal
    isTestingEquals = sourceType_->kindEquals(otherCast->sourceType_) &&
        targetType_->kindEquals(otherCast->targetType_) &&
        innerFilter_->testingEquals(*otherCast->innerFilter_);
  }

  // Return false if any equality check failed
  if (!isTestingEquals) {
    return false;
  }

  // If we reached here, basic equality checks passed
  // Now check for merged non-cast filters
  if (mergedNonCastFilter_ != nullptr &&
      otherCast->mergedNonCastFilter_ != nullptr) {
    // If both have non-null merged filters, compare them
    return mergedNonCastFilter_->testingEquals(
        *otherCast->mergedNonCastFilter_);
  }

  // Return true only if both merged filters are null
  return mergedNonCastFilter_ == nullptr &&
      otherCast->mergedNonCastFilter_ == nullptr;
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testNull() const {
  // First check the non-cast filter if we have one
  if (mergedNonCastFilter_ && !mergedNonCastFilter_->testNull()) {
    return false;
  }
  return innerFilter_->testNull();
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testTimestampRange(
    Timestamp min,
    Timestamp max,
    bool hasNull) const {
  // First check the non-cast filter if we have one
  if (mergedNonCastFilter_ &&
      !mergedNonCastFilter_->testTimestampRange(min, max, hasNull)) {
    return false;
  }

  try {
    if constexpr (NumericTypeUtils::isIntegralType(TARGET)) {
      if constexpr (TARGET == TypeKind::BIGINT) {
        const int64_t minValue = min.toMillis();
        const int64_t maxValue = max.toMillis();
        return innerFilter_->testInt64Range(minValue, maxValue, hasNull);
      } else {
        const int64_t minValue = min.getSeconds();
        const int64_t maxValue = max.getSeconds();
        return innerFilter_->testInt64Range(minValue, maxValue, hasNull);
      }
    }

    else if constexpr (TARGET == TypeKind::REAL || TARGET == TypeKind::DOUBLE) {
      const double minSeconds = min.toMillis() / 1000.0;
      const double maxSeconds = max.toMillis() / 1000.0;
      return innerFilter_->testDoubleRange(minSeconds, maxSeconds, hasNull);
    }

    else {
      return true;
    }

  } catch (const BoltUserError& e) {
    VLOG(1) << "Error in timestamp range cast test: " << e.what();
    return true;
  }
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testBool(bool value) const {
  // First check the non-cast filter if we have one
  if (mergedNonCastFilter_ && !mergedNonCastFilter_->testBool(value)) {
    return false;
  }

  if constexpr (NumericTypeUtils::isIntegralType(TARGET)) {
    // Cast bool to 1/0 then test with inner filter
    return innerFilter_->testInt64(value ? 1 : 0);
  }

  if constexpr (TARGET == TypeKind::REAL) {
    // Cast to float 1.0/0.0
    return innerFilter_->testFloat(value ? 1.0f : 0.0f);
  }

  else if constexpr (TARGET == TypeKind::DOUBLE) {
    // Cast to double 1.0/0.0
    return innerFilter_->testDouble(value ? 1.0 : 0.0);
  } else {
    // Conservative for other target types
    return true; // Conservative for other target types
  }
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testBytes(const char* bytes, int32_t len) const {
  if (mergedNonCastFilter_ && !mergedNonCastFilter_->testBytes(bytes, len)) {
    return false;
  }

  if (!bytes || len <= 0) {
    return true;
  }
  if constexpr (
      !NumericTypeUtils::isIntegralType(TARGET) &&
      !NumericTypeUtils::isFloatingPointType(TARGET)) {
    return true;
  }

  try {
    folly::StringPiece piece(bytes, len);
    folly::trimWhitespace(piece);

    if (piece.empty()) {
      return true;
    }

    if constexpr (TARGET == TypeKind::BOOLEAN) {
      folly::AsciiCaseInsensitive cmp;
      if (piece.equals("true", cmp) || piece == "1") {
        return innerFilter_->testBool(true);
      }
      if (piece.equals("false", cmp) || piece == "0") {
        return innerFilter_->testBool(false);
      }
      return true;
    }

    else if constexpr (TARGET == TypeKind::TIMESTAMP) {
      try {
        Timestamp ts;
        parseTo(piece, ts);
        return innerFilter_->testTimestamp(ts);
      } catch (...) {
        return true; // Conservative on parse error
      }
    } else if constexpr (NumericTypeUtils::isIntegralType(TARGET)) {
      try {
        // 尝试解析为int64_t
        auto parsedValue = folly::tryTo<int64_t>(piece);
        if (!parsedValue.hasValue())
          return true;

        const int64_t value = parsedValue.value();
        if constexpr (TARGET != TypeKind::BIGINT) {
          using TargetType = typename TypeTraits<TARGET>::NativeType;
          if (value < std::numeric_limits<TargetType>::min() ||
              value > std::numeric_limits<TargetType>::max()) {
            return true;
          }
        }

        return innerFilter_->testInt64(value);

      } catch (...) {
        return true;
      }
    } else if constexpr (NumericTypeUtils::isFloatingPointType(TARGET)) {
      try {
        auto parsedValue = folly::tryTo<double>(piece);
        if (!parsedValue.hasValue())
          return false;

        const double value = parsedValue.value();
        if (std::isnan(value)) {
          return false;
        }

        if constexpr (TARGET == TypeKind::REAL) {
          const float floatValue = static_cast<float>(value);
          return innerFilter_->testFloat(floatValue);
        } else {
          return innerFilter_->testDouble(value);
        }

      } catch (...) {
        // string -> double conversion failed, treat as UNKNOW.
        return false;
      }
    } else {
      static_assert(
          TARGET == TypeKind::VARCHAR || TARGET == TypeKind::VARBINARY,
          "Only VARCHAR and VARBINARY are expected for byte conversion");
      return true;
    }

  } catch (...) {
    VLOG(1) << "Unexpected error in byte conversion test";
    return false;
  }
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  // First check the non-cast filter if we have one
  if (mergedNonCastFilter_ &&
      !mergedNonCastFilter_->testBytesRange(min, max, hasNull)) {
    return false;
  }
  // A string column's range cannot be directly mapped to an integer/float range
  // test. For example:
  //   Consider Table A with columns: { col1: string, col2: bigint }.
  //   Query: CAST(col1 AS bigint) = 123.
  //   Suppose the statistics for col1 show a range of ["11", "99"].
  //   A numeric range test like testInt64Range(11, 99) would fail,
  //   even though the string "123" falls between "11" and "99" in
  //   lexicographical order.
  if constexpr (TARGET == TypeKind::VARCHAR) {
    return innerFilter_->testBytesRange(min, max, hasNull);
  } else {
    return true;
  }
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testTimestamp(Timestamp timestamp) const {
  // First check the non-cast filter if we have one
  if (mergedNonCastFilter_ && !mergedNonCastFilter_->testTimestamp(timestamp)) {
    return false;
  }
  // Handle different numeric target types
  try {
    if constexpr (NumericTypeUtils::isIntegralType(TARGET)) {
      // Convert to seconds or millis based on the target type precision
      bool useMillis = TARGET == TypeKind::BIGINT;
      int64_t value = useMillis ? timestamp.toMillis() : timestamp.getSeconds();
      return innerFilter_->testInt64(value);
    }

    else if constexpr (TARGET == TypeKind::REAL) {
      // For float, convert to seconds with decimal precision
      double seconds = timestamp.toMillis() / 1000.0;
      return innerFilter_->testFloat(static_cast<float>(seconds));
    }

    else if constexpr (TARGET == TypeKind::DOUBLE) {
      // For double, can preserve more timestamp precision
      double seconds = timestamp.toMillis() / 1000.0;
      return innerFilter_->testDouble(seconds);
    }

    else if constexpr (TARGET == TypeKind::TIMESTAMP) {
      return innerFilter_->testTimestamp(timestamp);
    }
  } catch (const BoltUserError& e) {
    // Handle potential timestamp conversion errors conservatively
    VLOG(1) << "Error in timestamp cast test: " << e.what();
    return true;
  }

  // Conservative for other target types
  return true;
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testInt64(int64_t value) const {
  if (mergedNonCastFilter_ && !mergedNonCastFilter_->testInt64(value)) {
    return false;
  }

  if constexpr (!NumericTypeUtils::isIntegralType(SOURCE)) {
    return true;
  }

  try {
    // Handle floating point types
    if constexpr (TARGET == TypeKind::DOUBLE) {
      return innerFilter_->testDouble(static_cast<double>(value));
    }

    else if constexpr (TARGET == TypeKind::REAL) {
      return innerFilter_->testFloat(static_cast<float>(value));
    }

    // Handle integral types - no conversion needed
    else if constexpr (
        TARGET == TypeKind::BIGINT || TARGET == TypeKind::INTEGER ||
        TARGET == TypeKind::SMALLINT || TARGET == TypeKind::TINYINT) {
      return innerFilter_->testInt64(value);
    }

    // Handle string types
    else if constexpr (
        TARGET == TypeKind::VARCHAR || TARGET == TypeKind::VARBINARY) {
      std::string strValue;
      try {
        strValue = sourceType_->isDate() ? DATE()->toString(value)
                                         : std::to_string(value);
      } catch (const BoltException& ue) {
        if (!ue.isUserError()) {
          throw;
        }

        // Keep the record and let CastExpr to handle the filtering.
        return true;
      }
      return innerFilter_->testBytes(strValue.data(), strValue.size());
    }

    // Handle boolean type
    else if constexpr (TARGET == TypeKind::BOOLEAN) {
      return innerFilter_->testBool(value != 0);
    }

    // Handle timestamp type
    else if constexpr (TARGET == TypeKind::TIMESTAMP) {
      Timestamp ts(value, 0 /* nanos */);
      return innerFilter_->testTimestamp(ts);
    }

    // Handle unsupported types at compile time
    else {
      return true;
    }

  } catch (...) {
    VLOG(1) << "Error in int64 cast test";
    return true;
  }
}

template <TypeKind SOURCE, TypeKind TARGET>
template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, bool>
Cast<SOURCE, TARGET>::testFloatingPoint(T value) const {
  // Compile-time check for source type compatibility
  if constexpr (
      !NumericTypeUtils::isFloatingPointType(SOURCE) &&
      !NumericTypeUtils::isIntegralType(SOURCE)) {
    return true;
  }

  // Handle special values
  if (std::isnan(value)) {
    return false;
  }

  try {
    // Handle integral target types
    if constexpr (NumericTypeUtils::isIntegralType(TARGET)) {
      int64_t intVal = static_cast<int64_t>(std::round(value));
      return innerFilter_->testInt64(intVal);
    }

    // Handle REAL target type
    else if constexpr (TARGET == TypeKind::REAL) {
      return innerFilter_->testFloat(static_cast<float>(value));
    }

    // Handle DOUBLE target type
    else if constexpr (TARGET == TypeKind::DOUBLE) {
      if constexpr (std::is_same_v<T, float>) {
        return innerFilter_->testFloat(value);
      } else {
        return innerFilter_->testDouble(value);
      }
    }

    // Handle string target types
    else if constexpr (
        TARGET == TypeKind::VARCHAR || TARGET == TypeKind::VARBINARY) {
      static auto deleter = [](char* ptr) { free(ptr); };

      if constexpr (std::is_same_v<T, double>) {
        std::unique_ptr<char, decltype(deleter)> d2sBuffer(d2s(value), deleter);

        BOLT_CHECK(
            d2sBuffer.get() != nullptr,
            "Failed to use ryu convert double value {} to string.",
            value);

        std::string strValue = d2sBuffer ? std::string(d2sBuffer.get()) : "";
        return innerFilter_->testBytes(strValue.data(), strValue.size());
      }

      else if constexpr (std::is_same_v<T, float>) {
        std::unique_ptr<char, decltype(deleter)> f2sBuffer(f2s(value), deleter);

        BOLT_CHECK(
            f2sBuffer.get() != nullptr,
            "Failed to use ryu convert float value {} to string.",
            value);

        std::string strValue = f2sBuffer ? std::string(f2sBuffer.get()) : "";
        return innerFilter_->testBytes(strValue.data(), strValue.size());
      }

      else {
        static_assert(
            std::is_same_v<T, double> || std::is_same_v<T, float>,
            "Unsupported type for conversion. Only double and float are allowed.");
      }
    }

    // Handle unsupported types
    else {
      return true;
    }

  } catch (...) {
    VLOG(1) << "Error in floating point cast test";
    return true;
  }
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testInt64Range(
    int64_t min,
    int64_t max,
    bool hasNull) const {
  if (mergedNonCastFilter_ &&
      !mergedNonCastFilter_->testInt64Range(min, max, hasNull)) {
    return false;
  }

  if constexpr (!NumericTypeUtils::isIntegralType(SOURCE)) {
    return true;
  }

  try {
    // Handle floating point types (DOUBLE and REAL)
    if constexpr (TARGET == TypeKind::DOUBLE) {
      // Compile-time constants for safe integer boundaries in floating point
      constexpr double kMaxSafeInteger = (1LL << 53) - 1;
      constexpr double kMinSafeInteger = -(1LL << 53) + 1;

      // Check if range exceeds safe integers
      if (max > kMaxSafeInteger || min < kMinSafeInteger) {
        return true;
      }

      // Check for potential overflow in conversion
      if (std::abs(static_cast<double>(max)) ==
              std::numeric_limits<double>::infinity() ||
          std::abs(static_cast<double>(min)) ==
              std::numeric_limits<double>::infinity()) {
        return true;
      }

      return innerFilter_->testDoubleRange(
          static_cast<double>(min),
          static_cast<double>(max),
          hasNull || !nullAllowed_);
    } else if constexpr (TARGET == TypeKind::REAL) {
      constexpr float kMaxSafeIntegerFloat = (1LL << 24) - 1; // 16,777,215
      constexpr float kMinSafeIntegerFloat = -((1LL << 24) - 1); // -16,777,215

      // Check if range exceeds safe integers for float
      if (max > kMaxSafeIntegerFloat || min < kMinSafeIntegerFloat) {
        return true;
      }

      // Check for potential overflow in conversion for float
      if (std::abs(static_cast<float>(max)) ==
              std::numeric_limits<float>::infinity() ||
          std::abs(static_cast<float>(min)) ==
              std::numeric_limits<float>::infinity()) {
        return true;
      }

      return innerFilter_->testDoubleRange(
          static_cast<float>(min),
          static_cast<float>(max),
          hasNull || !nullAllowed_);
    }

    // Handle BIGINT - no range check needed
    else if constexpr (TARGET == TypeKind::BIGINT) {
      return innerFilter_->testInt64Range(min, max, hasNull || !nullAllowed_);
    }

    // Handle other integral types with range checks
    else if constexpr (TARGET == TypeKind::INTEGER) {
      if (!isRangeInBounds<int32_t>(
              static_cast<double>(min), static_cast<double>(max))) {
        return true;
      }
      return innerFilter_->testInt64Range(min, max, hasNull || !nullAllowed_);
    }

    else if constexpr (TARGET == TypeKind::SMALLINT) {
      if (!isRangeInBounds<int16_t>(
              static_cast<double>(min), static_cast<double>(max))) {
        return true;
      }
      return innerFilter_->testInt64Range(min, max, hasNull || !nullAllowed_);
    }

    else if constexpr (TARGET == TypeKind::TINYINT) {
      if (!isRangeInBounds<int8_t>(
              static_cast<double>(min), static_cast<double>(max))) {
        return true;
      }
      return innerFilter_->testInt64Range(min, max, hasNull || !nullAllowed_);
    }

    else if constexpr (TARGET == TypeKind::VARCHAR) {
      if (sourceType_->isDate()) {
        auto minString = DATE()->toString(min);
        auto maxString = DATE()->toString(max);
        return innerFilter_->testBytesRange(minString, maxString, hasNull);
      } else if (min == max) {
        // We can handle single variable case.
        auto value = std::to_string(min);
        return innerFilter_->testBytesRange(value, value, hasNull);
      } else {
        VLOG(1) << "Cannot convert integer range [" << min << ", " << max
                << "] to string range for VARCHAR cast. String ranges are "
                << "not directly comparable to numeric ranges. Target filter: "
                << targetType_->toString();
        return true;
      }
    }

    // Handle unsupported types
    else {
      VLOG(1) << "Unsupported target type for cast: "
              << targetType_->toString();
      return true;
    }
  } catch (const std::exception& e) {
    VLOG(1) << "Error in cast range test: " << e.what()
            << " source=" << sourceType_->toString()
            << " target=" << targetType_->toString();
    return true;
  }
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testDoubleRange(double min, double max, bool hasNull)
    const {
  if (mergedNonCastFilter_ &&
      !mergedNonCastFilter_->testDoubleRange(min, max, hasNull)) {
    return false;
  }

  // Check if source type supports double values at compile time
  if constexpr (
      !type::NumericTypeUtils::isFloatingPointType(SOURCE) &&
      !type::NumericTypeUtils::isIntegralType(SOURCE)) {
    return true;
  }

  // Handle special floating point values
  if (std::isnan(min) || std::isnan(max)) {
    return false;
  }

  try {
    const double ceilMin = std::ceil(min);
    const double floorMax = std::floor(max);

    // Handle integral targets using type traits
    if constexpr (type::NumericTypeUtils::isIntegralType(TARGET)) {
      using TargetNative = typename TypeTraits<TARGET>::NativeType;
      if (!isRangeInBounds<TargetNative>(ceilMin, floorMax)) {
        return true;
      }
      return innerFilter_->testInt64Range(
          static_cast<int64_t>(std::round(ceilMin)),
          static_cast<int64_t>(std::round(floorMax)),
          hasNull || !nullAllowed_);
    }

    // Handle floating point targets
    else if constexpr (TARGET == TypeKind::REAL) {
      if (min < std::numeric_limits<float>::lowest() ||
          max > std::numeric_limits<float>::max()) {
        return true;
      }
      return innerFilter_->testDoubleRange(min, max, hasNull || !nullAllowed_);
    } else if constexpr (TARGET == TypeKind::DOUBLE) {
      return innerFilter_->testDoubleRange(min, max, hasNull || !nullAllowed_);
    }

    // Unsupported target types
    else {
      VLOG(1) << "Unsupported target type for cast from double: "
              << TypeTraits<TARGET>::name;
      return true;
    }
  } catch (const std::exception& e) {
    VLOG(1) << "Error in cast double range test: " << e.what()
            << " source=" << TypeTraits<SOURCE>::name
            << " target=" << TypeTraits<TARGET>::name << " range=[" << min
            << "," << max << "]";
    return true;
  }
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testDouble(double value) const {
  if (mergedNonCastFilter_ && !mergedNonCastFilter_->testDouble(value)) {
    return false;
  }
  return testFloatingPoint(value);
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::testFloat(float value) const {
  if (mergedNonCastFilter_ && !mergedNonCastFilter_->testFloat(value)) {
    return false;
  }

  return testFloatingPoint(value);
}

template <TypeKind SOURCE, TypeKind TARGET>
std::unique_ptr<Filter> Cast<SOURCE, TARGET>::mergeWith(
    const Filter* other) const {
  if (!other) {
    return nullptr;
  }

  if (other->kind() != FilterKind::kCast) {
    // For non-Cast filters, create a Cast with the existing innerFilter_
    // and the other filter as mergedNonCastFilter_
    std::shared_ptr<const Filter> mergedNonCast;

    if (mergedNonCastFilter_) {
      // If we already have a non-cast filter, merge them
      auto combined = mergedNonCastFilter_->mergeWith(other);
      if (!combined) {
        return nullptr;
      }
      mergedNonCast = std::move(combined);
    } else {
      // Otherwise, use the other filter directly
      mergedNonCast = other->clone();
    }

    return std::make_unique<Cast<SOURCE, TARGET>>(
        sourceType_, targetType_, innerFilter_, std::move(mergedNonCast));
  }

  auto otherCast = static_cast<const Cast*>(other);
  BOLT_CHECK(
      sourceType_ && targetType_ && otherCast->sourceType_ &&
          otherCast->targetType_,
      "Cast filter has null type");

  if (!sourceType_->kindEquals(otherCast->sourceType_) ||
      !targetType_->kindEquals(otherCast->targetType_)) {
    return nullptr;
  }

  BOLT_CHECK(
      innerFilter_ && otherCast->innerFilter_,
      "Cast filter has null inner filter");

  auto mergedInner = innerFilter_->mergeWith(otherCast->innerFilter_.get());
  if (!mergedInner) {
    return nullptr;
  }

  std::shared_ptr<const Filter> mergedNonCast = nullptr;

  // Handle merging of non-cast filters if either side has one
  if (mergedNonCastFilter_ && otherCast->mergedNonCastFilter_) {
    auto combined =
        mergedNonCastFilter_->mergeWith(otherCast->mergedNonCastFilter_.get());
    if (!combined) {
      return nullptr;
    }
    mergedNonCast = std::move(combined);
  } else if (mergedNonCastFilter_) {
    mergedNonCast = mergedNonCastFilter_;
  } else if (otherCast->mergedNonCastFilter_) {
    mergedNonCast = otherCast->mergedNonCastFilter_;
  }

  return std::make_unique<Cast<SOURCE, TARGET>>(
      sourceType_,
      targetType_,
      std::move(mergedInner),
      std::move(mergedNonCast));
}

template <TypeKind SOURCE, TypeKind TARGET>
std::string Cast<SOURCE, TARGET>::toString() const {
  return fmt::format(
      "CAST({} AS {}) {}",
      sourceType_->toString(),
      targetType_->toString(),
      innerFilter_->toString());
}

template <TypeKind SOURCE, TypeKind TARGET>
bool Cast<SOURCE, TARGET>::hasTestLength() const {
  return innerFilter_ != nullptr && innerFilter_->hasTestLength();
}

// Template instantiations
#define INSTANTIATE_CAST_TEMPLATES(SOURCE_KIND)          \
  template class Cast<SOURCE_KIND, TypeKind::TINYINT>;   \
  template class Cast<SOURCE_KIND, TypeKind::SMALLINT>;  \
  template class Cast<SOURCE_KIND, TypeKind::INTEGER>;   \
  template class Cast<SOURCE_KIND, TypeKind::BIGINT>;    \
  template class Cast<SOURCE_KIND, TypeKind::REAL>;      \
  template class Cast<SOURCE_KIND, TypeKind::DOUBLE>;    \
  template class Cast<SOURCE_KIND, TypeKind::VARCHAR>;   \
  template class Cast<SOURCE_KIND, TypeKind::VARBINARY>; \
  template class Cast<SOURCE_KIND, TypeKind::BOOLEAN>;   \
  template class Cast<SOURCE_KIND, TypeKind::TIMESTAMP>;

INSTANTIATE_CAST_TEMPLATES(TypeKind::TINYINT)
INSTANTIATE_CAST_TEMPLATES(TypeKind::SMALLINT)
INSTANTIATE_CAST_TEMPLATES(TypeKind::INTEGER)
INSTANTIATE_CAST_TEMPLATES(TypeKind::BIGINT)
INSTANTIATE_CAST_TEMPLATES(TypeKind::REAL)
INSTANTIATE_CAST_TEMPLATES(TypeKind::DOUBLE)
INSTANTIATE_CAST_TEMPLATES(TypeKind::VARCHAR)
INSTANTIATE_CAST_TEMPLATES(TypeKind::VARBINARY)
INSTANTIATE_CAST_TEMPLATES(TypeKind::BOOLEAN)
INSTANTIATE_CAST_TEMPLATES(TypeKind::TIMESTAMP)

#undef INSTANTIATE_CAST_TEMPLATES
} // namespace bytedance::bolt::common