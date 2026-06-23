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

#include "bolt/expression/CastExpr-tpl.h"

#if defined(__linux__)
#include <byteswap.h>
#elif defined(__APPLE__)
#include <machine/endian.h>
#define bswap_16(x) __builtin_bswap16(x)
#define bswap_32(x) __builtin_bswap32(x)
#define bswap_64(x) __builtin_bswap64(x)
#endif

#include <algorithm>
#include "bolt/functions/InlineFlatten.h"
#include "bolt/functions/lib/RowsTranslationUtil.h"
#include "bolt/functions/lib/StringUtil.h"
#include "bolt/functions/lib/string/StringImpl.h"
#include "bolt/type/Conversions.h"
#include "bolt/type/FloatingDecimal.h"
#include "bolt/type/tz/TimeZoneMap.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::CastUtils;
namespace bytedance::bolt::exec::CastUtils {

constexpr size_t kStackBufSize = 64;

#ifdef SPARK_COMPATIBLE
constexpr bool isInSpark = true;
#else
constexpr bool isInSpark = false;
#endif

// convert status to indicate whether conversion behaviors.
// Note: for INTEGER_OVERFLOW, the output value should save a wrapped value.
enum class ConvertStatus : int8_t {
  SUCCESS,
  INTEGER_OVERFLOW,
  INTEGER_HAS_POINT,
  OTHER_FAILURE
};

// Decoration conveys extra type information for types that share the same
// physical storage (TypeKind) but require different casting behavior.
enum class Decoration : int8_t { None, ShortDecimal, LongDecimal, Date };

template <TypeKind Storage, Decoration Deco = Decoration::None>
struct CastKindType {
  static constexpr TypeKind storage = Storage;
  static constexpr Decoration deco = Deco;
};

using BooleanKind = CastKindType<TypeKind::BOOLEAN>;
using TinyintKind = CastKindType<TypeKind::TINYINT>;
using SmallintKind = CastKindType<TypeKind::SMALLINT>;
using IntegerKind = CastKindType<TypeKind::INTEGER>;
using BigintKind = CastKindType<TypeKind::BIGINT>;
using RealKind = CastKindType<TypeKind::REAL>;
using DoubleKind = CastKindType<TypeKind::DOUBLE>;
using StringKind = CastKindType<TypeKind::VARCHAR>;
using BinaryKind = CastKindType<TypeKind::VARBINARY>;
using TimestampKind = CastKindType<TypeKind::TIMESTAMP>;
using ShortDecimalKind =
    CastKindType<TypeKind::BIGINT, Decoration::ShortDecimal>;
using LongDecimalKind =
    CastKindType<TypeKind::HUGEINT, Decoration::LongDecimal>;
using DateKind = CastKindType<TypeKind::INTEGER, Decoration::Date>;

template <typename CK>
using PrimitiveNativeType = typename TypeTraits<CK::storage>::NativeType;

template <typename CK>
constexpr bool isStringLikeKind =
    std::is_same_v<CK, StringKind> || std::is_same_v<CK, BinaryKind>;

template <typename CK>
constexpr bool isBooleanKind = std::is_same_v<CK, BooleanKind>;

template <typename CK>
constexpr bool isOtherKind = !isStringLikeKind<CK> && !isBooleanKind<CK>;

template <typename CK, typename Enable = void>
class OutType;

template <typename CK>
class OutType<CK, std::enable_if_t<isOtherKind<CK>>> {
 public:
  using ParamType = PrimitiveNativeType<CK>;
  using ArrayType = ParamType*;

  static ArrayType getOutArray(FlatVector<ParamType>* result) {
    return result->mutableRawValues();
  }
};

template <typename CK>
class OutType<CK, std::enable_if_t<isStringLikeKind<CK>>> {
 public:
  using ParamType = OutType<CK>;
  using ArrayType = ParamType;
  static ArrayType getOutArray(FlatVector<StringView>* result) {
    return OutType(result);
  }

  OutType(FlatVector<StringView>* result)
      : values(result->mutableRawValues()),
        index(0),
        buffer(result->getBufferWithSpace(0)),
        result(result) {}

  FOLLY_ALWAYS_INLINE char* acquire(size_t size) {
    if (buffer->size() + size > buffer->capacity()) {
      buffer = result->getBufferWithSpace(size);
    }
    char* result = buffer->asMutable<char>() + buffer->size();
    buffer->setSize(buffer->size() + size);
    return result;
  }

  FOLLY_ALWAYS_INLINE ParamType& operator[](size_t index) {
    this->index = index;
    return *this;
  }

  FOLLY_ALWAYS_INLINE void set(const std::string_view& view) {
    if (StringView::isInline(view.size())) {
      values[index].set(view.data(), view.size());
    } else {
      char* ptr = acquire(view.size());
      memcpy(ptr, view.data(), view.size());
      values[index].set(ptr, view.size());
    }
  }

  FOLLY_ALWAYS_INLINE void setNoCopy(const std::string_view& view) {
    values[index].set(view.data(), view.size());
  }

 private:
  StringView* values;
  size_t index;
  Buffer* buffer;
  FlatVector<StringView>* result;
};

template <typename CK>
class OutType<CK, std::enable_if_t<isBooleanKind<CK>>> {
 public:
  using ParamType = OutType<CK>;
  using ArrayType = ParamType;

  static ArrayType getOutArray(FlatVector<bool>* result) {
    return OutType(result);
  }

  OutType(FlatVector<bool>* result)
      : index(0), rawValues_(result->mutableRawValues<uint64_t>()) {}

  FOLLY_ALWAYS_INLINE ParamType& operator[](size_t index) {
    this->index = index;
    return *this;
  }

  FOLLY_ALWAYS_INLINE void set(bool value) {
    bits::setBit(reinterpret_cast<uint64_t*>(rawValues_), index, value);
  }

 private:
  size_t index;
  uint64_t* rawValues_;
  FlatVector<bool>* result;
};

template <typename From, typename To>
FOLLY_ALWAYS_INLINE ConvertStatus
tryIntegerToInteger(const From& from, To& to) {
  if (FOLLY_UNLIKELY(
          (from < std::numeric_limits<To>::min()) ||
          (from > std::numeric_limits<To>::max()))) {
    // save wrapped value and return overflow status, will handle it in caller
    // function
    to = static_cast<To>(from);
    return ConvertStatus::INTEGER_OVERFLOW;
  }
  to = static_cast<To>(from);
  return ConvertStatus::SUCCESS;
}

template <typename From, typename To>
FOLLY_ALWAYS_INLINE ConvertStatus tryToWithFolly(const From& from, To& to) {
  auto expected = folly::tryTo<To>(from);
  if (expected.hasValue()) {
    to = expected.value();
    return ConvertStatus::SUCCESS;
  }
  return ConvertStatus::OTHER_FAILURE;
}

std::optional<bool> sparkStringToBoolean(const folly::StringPiece& str) {
  std::string s = folly::trimWhitespace(str).str();
  folly::toLowerAscii(s);
  if (s == "true" || s == "t" || s == "y" || s == "yes" || s == "1") {
    return true;
  } else if (s == "false" || s == "f" || s == "n" || s == "no" || s == "0") {
    return false;
  }
  return std::nullopt;
}

/**
 * @brief A generic Converter class template for type casting between primitive
 * types.
 *
 * This class provides a mechanism to convert values from one primitive type to
 * another while supporting various casting policies such as legacy casting and
 * truncation. It is designed to handle conversions between numeric, string,
 * boolean, decimal, timestamp, and date types.
 *
 * @tparam FromKind The source CastKindType (TypeKind + Decoration).
 * @tparam ToKind The target CastKindType (TypeKind + Decoration).
 * @tparam legacy A boolean indicating whether to use legacy casting behavior.
 * @tparam truncate A boolean indicating whether to truncate values during
 * conversion.
 */
template <typename FromKind, typename ToKind, bool legacy, bool truncate>
class Converter {
  using FromType = PrimitiveNativeType<FromKind>;
  using ToType = typename OutType<ToKind>::ParamType;

  template <typename K>
  static constexpr bool isIntegral =
      std::is_same_v<K, TinyintKind> || std::is_same_v<K, SmallintKind> ||
      std::is_same_v<K, IntegerKind> || std::is_same_v<K, BigintKind>;

  template <typename K>
  static constexpr bool isFloat =
      std::is_same_v<K, RealKind> || std::is_same_v<K, DoubleKind>;

  template <typename K>
  static constexpr bool isDecimal =
      std::is_same_v<K, ShortDecimalKind> || std::is_same_v<K, LongDecimalKind>;

  static constexpr bool fromBool = std::is_same_v<FromKind, BooleanKind>;
  static constexpr bool fromString = std::is_same_v<FromKind, StringKind>;
  static constexpr bool fromFloat = isFloat<FromKind>;
  static constexpr bool fromDecimal = isDecimal<FromKind>;

  static constexpr bool fromIntegral = isIntegral<FromKind>;

#define TO_KIND(Kind)             \
  template <typename CK = ToKind> \
  FOLLY_ALWAYS_INLINE             \
      std::enable_if_t<std::is_same_v<CK, Kind>, ConvertStatus>

#define TO_IF(cond)               \
  template <typename CK = ToKind> \
  FOLLY_ALWAYS_INLINE std::enable_if_t<cond<CK>, ConvertStatus>

 public:
  Converter(exec::EvalCtx& context, TypePtr fromType, TypePtr toType) {
    if constexpr (std::is_same_v<FromKind, ShortDecimalKind>) {
      fromPrecision_ = fromType->asShortDecimal().precision();
      fromScale_ = fromType->asShortDecimal().scale();
      fromDecimalMaxSize_ = DecimalUtil::stringSize(fromPrecision_, fromScale_);
      canAsInlinedStr_ = StringView::isInline(fromDecimalMaxSize_);
    } else if constexpr (std::is_same_v<FromKind, LongDecimalKind>) {
      fromPrecision_ = fromType->asLongDecimal().precision();
      fromScale_ = fromType->asLongDecimal().scale();
      fromDecimalMaxSize_ = DecimalUtil::stringSize(fromPrecision_, fromScale_);
      canAsInlinedStr_ = StringView::isInline(fromDecimalMaxSize_);
    }
    if constexpr (std::is_same_v<ToKind, ShortDecimalKind>) {
      toPrecision_ = toType->asShortDecimal().precision();
      toScale_ = toType->asShortDecimal().scale();
    } else if constexpr (std::is_same_v<ToKind, LongDecimalKind>) {
      toPrecision_ = toType->asLongDecimal().precision();
      toScale_ = toType->asLongDecimal().scale();
    }

    const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();
    auto sessionTzName = queryConfig.sessionTimezone();
    if (queryConfig.adjustTimestampToTimezone() && !sessionTzName.empty()) {
      timeZone_ = tz::locateZone(sessionTzName);
    }
    isFlinkCompatible_ = queryConfig.enableFlinkCompatible();
  }

  TO_KIND(BooleanKind) convert(const FromType& from, ToType& to) {
    if constexpr (fromFloat) {
      bool val = false;
      auto status = tryToWithFolly(from, val);
      if (status == ConvertStatus::SUCCESS) {
        to.set(val);
      }
      return status;
    } else if constexpr (fromIntegral) {
      if constexpr (truncate) {
        to.set(bool(from));
      } else {
        bool val = false;
        auto status = tryToWithFolly(from, val);
        if (status == ConvertStatus::SUCCESS) {
          to.set(val);
        }
        return status;
      }
    } else if constexpr (fromString && isInSpark) {
      // spark support trim and does not accept on/off instead of folly::to
      auto boolOpt = sparkStringToBoolean(folly::StringPiece(from));
      if (boolOpt.has_value()) {
        to.set(*boolOpt);
      } else {
        return ConvertStatus::OTHER_FAILURE;
      }
    } else {
      bool val = false;
      auto status = tryToWithFolly(from, val);
      if (status == ConvertStatus::SUCCESS) {
        to.set(val);
      }
      return status;
    }
    return ConvertStatus::SUCCESS;
  }

  TO_IF(isIntegral) convert(const FromType& from, ToType& to) {
    if constexpr (fromString) {
      if constexpr (truncate) {
        bool nullOutput = false;
        bool hasPoint = false;
        constexpr TypeKind originKind = ToKind::storage;
        to = bytedance::bolt::util::
            Converter<originKind, void, util::TruncateCastPolicy>::
                convertStringToInt(
                    folly::StringPiece(from), &nullOutput, &hasPoint);
        if (nullOutput) {
          return ConvertStatus::OTHER_FAILURE;
        } else if (hasPoint) {
          return ConvertStatus::INTEGER_HAS_POINT;
        } else {
          return ConvertStatus::SUCCESS;
        }
      } else {
        return tryToWithFolly(folly::StringPiece(from.data(), from.size()), to);
      }
    } else if constexpr (fromFloat) {
      if constexpr (truncate) {
        if (std::isnan(from)) {
          to = 0;
          return ConvertStatus::SUCCESS;
        }
        constexpr TypeKind originKind = ToKind::storage;
        using LimitType = typename util::
            Converter<originKind, void, util::TruncateCastPolicy>::LimitType;
        if (from > LimitType::maxLimit()) {
          to = LimitType::max();
          return ConvertStatus::INTEGER_OVERFLOW;
        }
        if (from < LimitType::minLimit()) {
          to = LimitType::min();
          return ConvertStatus::INTEGER_OVERFLOW;
        }
        if (FOLLY_UNLIKELY(
                (from > std::numeric_limits<ToType>::max()) ||
                (from < std::numeric_limits<ToType>::min()))) {
          to = LimitType::cast(from);
          return ConvertStatus::INTEGER_OVERFLOW;
        }
        to = LimitType::cast(from);
        return ConvertStatus::SUCCESS;
      } else {
        if (std::isnan(from)) {
          return ConvertStatus::OTHER_FAILURE;
        }
        return tryIntegerToInteger<FromType, ToType>(std::round(from), to);
      }
    } else if constexpr (fromDecimal) {
      const auto scaleFactor = DecimalUtil::getPowersOfTen(fromScale_);
      if (truncate) {
        return tryIntegerToInteger<FromType, ToType>(from / scaleFactor, to);
      } else {
        auto integralPart = from / scaleFactor;
        auto fractionPart = from % scaleFactor;
        auto sign = from >= 0 ? 1 : -1;
        bool needsRoundUp =
            (scaleFactor != 1) && (sign * fractionPart >= (scaleFactor >> 1));
        integralPart += needsRoundUp ? sign : 0;
        return tryIntegerToInteger(integralPart, to);
      }
    } else if constexpr (fromBool) {
      return tryToWithFolly(from, to);
    } else if constexpr (std::is_same_v<FromKind, TimestampKind>) {
      return tryIntegerToInteger<int64_t, ToType>(from.getSeconds(), to);
    } else {
      // INTEGER
      return tryIntegerToInteger<FromType, ToType>(from, to);
    }
    return ConvertStatus::SUCCESS;
  }

  TO_IF(isFloat) convert(const FromType& from, ToType& to) {
    if constexpr (fromIntegral) {
      // Convert integer to double or float directly, not using folly, as it
      // might throw 'loss of precision' error.
      to = static_cast<ToType>(from);
    } else if constexpr (fromString) {
      folly::StringPiece newV = folly::trimWhitespace(from);
      if (newV.empty()) {
        return ConvertStatus::OTHER_FAILURE;
      }

      auto pos = 0;
      if (newV.front() == '+' || newV.front() == '-') {
        pos++;
      }

      bool noPop = pos < newV.size() && (newV[pos] == 'n' || newV[pos] == 'i');

      if (!noPop && !newV.empty() &&
          (newV.back() == 'f' || newV.back() == 'F' || newV.back() == 'd' ||
           newV.back() == 'D')) {
        newV.pop_back();
      }
      if (newV.empty()) {
        return ConvertStatus::OTHER_FAILURE;
      }
      return tryToWithFolly(newV, to);
    } else if constexpr (fromDecimal) {
      if constexpr (isInSpark) {
        std::optional<ToType> fValue;
        if constexpr (std::is_same_v<ToKind, RealKind>) {
          fValue = FloatingDecimal::toFloatFromValue(from, fromScale_);
        } else {
          fValue = FloatingDecimal::toDoubleFromValue(from, fromScale_);
        }
        if (fValue.has_value()) {
          to = *fValue;
        } else {
          return ConvertStatus::OTHER_FAILURE;
        }
      } else {
        const auto scaleFactor = DecimalUtil::getPowersOfTen(fromScale_);
        to = static_cast<ToType>(from) / scaleFactor;
      }

    } else if constexpr (fromFloat && truncate) {
      to = ToType(from);
    } else {
      return tryToWithFolly(from, to);
    }
    return ConvertStatus::SUCCESS;
  }

  TO_KIND(StringKind) convert(const FromType& from, ToType& to) {
    if constexpr (std::is_same_v<FromKind, BooleanKind>) {
      to.setNoCopy(from ? "true" : "false");
    } else if constexpr (std::is_same_v<FromKind, TimestampKind>) {
      try {
        auto ts = from;
        if (timeZone_) {
          if (isFlinkCompatible_) {
            TimestampToStringOptions options;
            options.precision =
                TimestampToStringOptions::Precision::kNanoseconds;
            options.skipTrailingZeros = true;
            options.zeroPaddingYear = true;
            options.dateTimeSeparator = ' ';
            ts.toTimezone(*timeZone_);
            auto output = ts.toString(options);
            auto dot = output.find('.');
            if (dot == std::string::npos) {
              output.append(".000");
            } else {
              const auto fractionalSize = output.size() - dot - 1;
              if (fractionalSize < 3) {
                output.append(3 - fractionalSize, '0');
              }
            }
            to.set(output);
          } else if (isInSpark) {
            constexpr TimestampToStringOptions options = {
                .precision = TimestampToStringOptions::Precision::kMicroseconds,
                .leadingPositiveSign = true,
                .skipTrailingZeros = true,
                .zeroPaddingYear = true,
                .dateTimeSeparator = ' '};
            ts.toTimezone(*timeZone_);
            to.set(ts.toString(options));
          } else {
            to.set(from.toString(
                TimestampToStringOptions::Precision::kMilliseconds, timeZone_));
          }
        } else {
          if (isFlinkCompatible_) {
            TimestampToStringOptions options;
            options.precision =
                TimestampToStringOptions::Precision::kNanoseconds;
            options.skipTrailingZeros = true;
            options.zeroPaddingYear = true;
            options.dateTimeSeparator = ' ';
            auto output = ts.toString(options);
            auto dot = output.find('.');
            if (dot == std::string::npos) {
              output.append(".000");
            } else {
              const auto fractionalSize = output.size() - dot - 1;
              if (fractionalSize < 3) {
                output.append(3 - fractionalSize, '0');
              }
            }
            to.set(output);
          } else if (isInSpark) {
            constexpr TimestampToStringOptions options = {
                .precision = TimestampToStringOptions::Precision::kMicroseconds,
                .leadingPositiveSign = true,
                .skipTrailingZeros = true,
                .zeroPaddingYear = true,
                .dateTimeSeparator = ' '};
            to.set(ts.toString(options));
          } else {
            TimestampToStringOptions options;
            options.precision =
                TimestampToStringOptions::Precision::kMilliseconds;
            if constexpr (!legacy) {
              options.zeroPaddingYear = true;
              options.dateTimeSeparator = ' ';
            }
            to.set(ts.toString(options));
          }
        }
      } catch (...) {
        return ConvertStatus::OTHER_FAILURE;
      }
    } else if constexpr (fromFloat) {
      if constexpr (legacy) {
        cached_.resize(0);
        folly::toAppend<std::string>(from, &cached_);
        bytedance::bolt::util::Converter<
            TypeKind::VARCHAR,
            void,
            util::LegacyCastPolicy>::normalizeStandardNotation(cached_);
        to.set(cached_);
      } else {
        if constexpr (std::is_same_v<FromKind, DoubleKind>) {
          // d2s/f2s reserve 25/16 bytes buffer, so 32 bytes is enough
          char buffer[32];
          int size = d2s_buffered_n(from, buffer);
          to.set(std::string_view(buffer, size));
        } else { // float use f2s
          char buffer[32];
          int size = f2s_buffered_n(from, buffer);
          to.set(std::string_view(buffer, size));
        }
      }
    } else if constexpr (fromDecimal) {
      if (canAsInlinedStr_) {
        char inlined[StringView::kInlineSize];
        auto strSize = DecimalUtil::convertToString(
            from, fromScale_, StringView::kInlineSize, inlined);
        to.setNoCopy(std::string_view(inlined, strSize));
      } else {
        BOLT_DCHECK_LE(
            fromDecimalMaxSize_,
            kStackBufSize,
            "DecimalSize must be less than 64");
        char cached[kStackBufSize];
        auto strSize = DecimalUtil::convertToString(
            from, fromScale_, fromDecimalMaxSize_, cached);
        to.set(std::string_view(cached, strSize));
      }
    } else if constexpr (std::is_same_v<FromKind, DateKind>) {
      try {
        auto output = DATE()->toString(from);
        to.set(output);
      } catch (const std::exception& e) {
        return ConvertStatus::OTHER_FAILURE;
      }
    } else if constexpr (fromIntegral) {
      char cached[32];
      auto [position, errorCode] = std::to_chars(cached, cached + 32, from);
      to.set(std::string_view(cached, position - cached));
    } else {
      cached_.resize(0);
      folly::toAppend<std::string>(from, &cached_);
      to.set(cached_);
    }
    return ConvertStatus::SUCCESS;
  }

  TO_IF(isDecimal) convert(const FromType& from, ToType& to) {
    if constexpr (fromIntegral || fromBool) {
      auto rescaledValue = DecimalUtil::rescaleInt<FromType, ToType>(
          from, toPrecision_, toScale_);
      if (rescaledValue.has_value()) {
        to = rescaledValue.value();
      } else {
        return ConvertStatus::OTHER_FAILURE;
      }
    } else if constexpr (fromFloat) {
      const auto status =
          DecimalUtil::rescaleFullFloatingPoint<FromType, ToType>(
              from, toPrecision_, toScale_, to);
      if (!status.ok()) {
        return ConvertStatus::OTHER_FAILURE;
      }
    } else if constexpr (fromDecimal) {
      const auto status = DecimalUtil::rescaleWithRoundUp<FromType, ToType>(
          from, fromPrecision_, fromScale_, toPrecision_, toScale_, to);
      if (!status.ok()) {
        return ConvertStatus::OTHER_FAILURE;
      }
    } else if constexpr (fromString) {
      StringView view(from);
      if (isInSpark) {
        bytedance::bolt::functions::stringImpl::
            trimUnicodeWhiteSpace<true, true, StringView, StringView>(
                view, from);
      }
      const auto status =
          DecimalUtil::toDecimalValue<ToType>(view, toPrecision_, toScale_, to);
      if (!status.ok()) {
        return ConvertStatus::OTHER_FAILURE;
      }
    }
    return ConvertStatus::SUCCESS;
  }

  TO_KIND(TimestampKind) convert(const FromType& from, ToType& to) {
    if constexpr (std::is_same_v<FromKind, StringKind>) {
      if (isInSpark) {
        auto resultOpt =
            util::fromTimestampWithTimezoneString(from.data(), from.size());
        if (!resultOpt.has_value()) {
          return ConvertStatus::OTHER_FAILURE;
        }

        auto result = resultOpt.value();

        bool hasError = false;
        // If the parsed string has timezone information, convert the
        // timestamp at GMT at that time. For example, "1970-01-01 00:00:00
        // -00:01" is 60 seconds at GMT.
        if (result.second != -1) {
          result.first.toGMT(result.second, &hasError);
        } else if (timeZone_ != nullptr) {
          // If no timezone information is available in the input string, check
          // if we should understand it as being at the session timezone, and if
          // so, convert to GMT.
          result.first.toGMT(*timeZone_, &hasError);
        }
        to = result.first;
        return hasError ? ConvertStatus::OTHER_FAILURE : ConvertStatus::SUCCESS;
      } else {
        bool nullOutput = false;
        to = bytedance::bolt::util::fromTimestampString(from, &nullOutput);
        if (timeZone_) {
          bool hasError = false;
          to.toGMT(*timeZone_, &hasError);
          nullOutput |= hasError;
        }
        return nullOutput ? ConvertStatus::OTHER_FAILURE
                          : ConvertStatus::SUCCESS;
      }
    } else if constexpr (std::is_same_v<FromKind, DateKind>) {
      static const int64_t kMillisPerDay{86'400'000};
      to = Timestamp::fromMillis(from * kMillisPerDay);
      bool hasError = false;
      if (timeZone_) {
        to.toGMT(*timeZone_, &hasError);
      }
      return hasError ? ConvertStatus::OTHER_FAILURE : ConvertStatus::SUCCESS;
    } else if constexpr (std::is_same_v<FromKind, BooleanKind>) {
      if constexpr (isInSpark) {
        // Spark treats boolean as microseconds since epoch when casting to
        // timestamp: false -> 0us, true -> 1us.
        to = Timestamp::fromMicrosNoError(from ? 1 : 0);
        return ConvertStatus::SUCCESS;
      }
      return ConvertStatus::OTHER_FAILURE;
    } else if constexpr (fromIntegral || fromFloat) {
      if constexpr (fromFloat) {
        if (FOLLY_UNLIKELY(std::isnan(from) || std::isinf(from))) {
          return ConvertStatus::OTHER_FAILURE;
        }
      }
      // Spark internally use microsecond precision for timestamp.
      // To avoid overflow, we need to check the range of seconds.
      static constexpr int64_t maxSeconds =
          std::numeric_limits<int64_t>::max() /
          Timestamp::kMicrosecondsInSecond;
      if (from > maxSeconds) {
        to = Timestamp::fromMicrosNoError(std::numeric_limits<int64_t>::max());
      } else if (from < -maxSeconds) {
        to = Timestamp::fromMicrosNoError(std::numeric_limits<int64_t>::min());
      } else {
        if constexpr (fromFloat) {
          // NOTE: `from` can be `float`. Make sure we do the multiplication in
          // at least double precision, otherwise we lose too much precision at
          // ~1e15 scale and produce unexpected microseconds.
          const auto micros = static_cast<long double>(from) *
              static_cast<long double>(Timestamp::kMicrosecondsInSecond);
          to = Timestamp::fromMicrosNoError(static_cast<int64_t>(micros));
        } else {
          to = Timestamp(from, 0);
        }
      }
    }
    return ConvertStatus::SUCCESS;
  }

  TO_KIND(DateKind) convert(const FromType& from, ToType& to) {
    if constexpr (std::is_same_v<FromKind, StringKind>) {
      bool isIso8601 = !isInSpark;
      StringView view(from);
      if (isInSpark) {
        bytedance::bolt::functions::stringImpl::
            trimUnicodeWhiteSpace<true, true, StringView, StringView>(
                view, from);
      }
      auto result = util::castFromDateString(view, isIso8601);
      if (result.has_value()) {
        to = result.value();
      } else {
        return ConvertStatus::OTHER_FAILURE;
      }
    } else if constexpr (std::is_same_v<FromKind, TimestampKind>) {
      static const int32_t kSecsPerDay{86'400};
      auto ts = from;
      if (timeZone_) {
        ts.toTimezone(*timeZone_);
      }
      auto seconds = ts.getSeconds();
      if (seconds >= 0 || seconds % kSecsPerDay == 0) {
        to = seconds / kSecsPerDay;
      } else {
        // For division with negatives, minus 1 to compensate the
        // discarded fractional part. e.g. -1/86'400 yields 0, yet it
        // should be considered as -1 day.
        to = seconds / kSecsPerDay - 1;
      }
    }
    return ConvertStatus::SUCCESS;
  }

  TO_KIND(BinaryKind) convert(const FromType& from, ToType& to) {
    if constexpr (std::is_same_v<FromKind, StringKind>) {
      to.set(from);
    } else if constexpr (fromIntegral) {
      // Convert integer to binary string with big-endian representation using
      // bswap
      FromType value = from;
      if constexpr (sizeof(FromType) == 2) {
        value = bswap_16(from);
      } else if constexpr (sizeof(FromType) == 4) {
        value = bswap_32(from);
      } else if constexpr (sizeof(FromType) == 8) {
        value = bswap_64(from);
      }
      to.setNoCopy(std::string_view(
          reinterpret_cast<const char*>(&value), sizeof(FromType)));
    } else {
      return ConvertStatus::OTHER_FAILURE;
    }
    return ConvertStatus::SUCCESS;
  }

 private:
  std::string cached_;
  int fromPrecision_ = 0;
  int fromScale_ = 0;
  int fromDecimalMaxSize_ = 0;
  int toPrecision_ = 0;
  int toScale_ = 0;
  const tz::TimeZone* timeZone_ = nullptr;
  bool isFlinkCompatible_ = false;

  bool canAsInlinedStr_ = false;
};

class ConverterBase {
 public:
  virtual void convert(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      VectorPtr& result,
      CastUtils::CastErrorPolicy) = 0;
};

FOLLY_ALWAYS_INLINE std::string makeErrorMessage(
    const BaseVector& input,
    vector_size_t row,
    const TypePtr& toType) {
  return fmt::format(
      "Cannot cast {} '{}' to {}.",
      input.type()->toString(),
      input.toString(row),
      toType->toString());
}

FOLLY_ALWAYS_INLINE std::exception_ptr makeBadCastException(
    const TypePtr& resultType,
    const BaseVector& input,
    vector_size_t row) {
  return std::make_exception_ptr(BoltUserError(
      std::current_exception(),
      makeErrorMessage(input, row, resultType),
      false));
}

template <typename FromKind, typename ToKind>
class VectorConverter : public ConverterBase {
 public:
  virtual ~VectorConverter() = default;

 private:
  template <typename K>
  static constexpr bool isIntegral =
      std::is_same_v<K, TinyintKind> || std::is_same_v<K, SmallintKind> ||
      std::is_same_v<K, IntegerKind> || std::is_same_v<K, BigintKind>;

  template <typename K>
  static constexpr bool isFloatKind =
      std::is_same_v<K, RealKind> || std::is_same_v<K, DoubleKind>;

  template <typename K>
  static constexpr bool isDecimalKind =
      std::is_same_v<K, ShortDecimalKind> || std::is_same_v<K, LongDecimalKind>;

  static constexpr bool kLegacySensitive = std::is_same_v<ToKind, StringKind> &&
      (std::is_same_v<FromKind, TimestampKind> || isFloatKind<FromKind>);

  static constexpr bool kTruncateSensitive =
      // integer -> boolean
      (std::is_same_v<ToKind, BooleanKind> && isIntegral<FromKind>) ||
      // string/float/decimal -> integer
      (isIntegral<ToKind> &&
       (std::is_same_v<FromKind, StringKind> || isFloatKind<FromKind> ||
        isDecimalKind<FromKind>)) ||
      // float -> float
      (isFloatKind<ToKind> && isFloatKind<FromKind>);

 public:
  template <bool legacy, bool truncate>
  FLATTEN void convertWithPolicy(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      VectorPtr& result,
      CastErrorPolicy errorPolicy) {
    using FromType = PrimitiveNativeType<FromKind>;
    using ToType = PrimitiveNativeType<ToKind>;

    auto sourceVector = input.as<SimpleVector<FromType>>();
    auto resultFlatVector = result->asUnchecked<FlatVector<ToType>>();
    Converter<FromKind, ToKind, legacy, truncate> converter(
        context, input.type(), result->type());
    auto outArray = OutType<ToKind>::getOutArray(resultFlatVector);
    if (FOLLY_LIKELY(errorPolicy == CastErrorPolicy::NullOnFailure)) {
      rows.applyToSelected([&](auto row) INLINE_LAMBDA {
        if (converter.convert(sourceVector->valueAt(row), outArray[row]) !=
            ConvertStatus::SUCCESS) {
          result->setNull(row, true);
        }
      });
    } else if (errorPolicy == CastErrorPolicy::ThrowOnFailure) {
      rows.applyToSelected([&](auto row) INLINE_LAMBDA {
        if (converter.convert(sourceVector->valueAt(row), outArray[row]) !=
            ConvertStatus::SUCCESS) {
          context.setBoltExceptionError(
              row, makeBadCastException(result->type(), input, row));
        }
      });
    } else if (errorPolicy == CastErrorPolicy::SparkCastPolicy) {
      rows.applyToSelected([&](auto row) INLINE_LAMBDA {
        ConvertStatus status =
            converter.convert(sourceVector->valueAt(row), outArray[row]);
        if (status != ConvertStatus::SUCCESS &&
            status != ConvertStatus::INTEGER_OVERFLOW &&
            status != ConvertStatus::INTEGER_HAS_POINT) {
          // integer overflow will wrap around instead return null
          // string to integer with point will truncate the point part
          result->setNull(row, true);
        }
      });
    }
  }

  void convert(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      VectorPtr& result,
      CastErrorPolicy errorPolicy) override {
    if constexpr (isInSpark) {
      constexpr bool legacy = false;
      constexpr bool truncate = true;
      convertWithPolicy<legacy, truncate>(
          rows, input, context, result, errorPolicy);
    } else {
      const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();

      if constexpr (kLegacySensitive && kTruncateSensitive) {
        bool legacy = queryConfig.isLegacyCast();
        bool truncate = queryConfig.isCastToIntByTruncate();
        if (legacy && truncate) {
          convertWithPolicy<true, true>(
              rows, input, context, result, errorPolicy);
        } else if (legacy && !truncate) {
          convertWithPolicy<true, false>(
              rows, input, context, result, errorPolicy);
        } else if (!legacy && truncate) {
          convertWithPolicy<false, true>(
              rows, input, context, result, errorPolicy);
        } else {
          // !legacy && !truncate
          convertWithPolicy<false, false>(
              rows, input, context, result, errorPolicy);
        }
      } else if constexpr (kLegacySensitive) {
        bool legacy = queryConfig.isLegacyCast();
        if (legacy) {
          convertWithPolicy<true, false>(
              rows, input, context, result, errorPolicy);
        } else {
          convertWithPolicy<false, false>(
              rows, input, context, result, errorPolicy);
        }
      } else if constexpr (kTruncateSensitive) {
        bool truncate = queryConfig.isCastToIntByTruncate();
        if (truncate) {
          convertWithPolicy<false, true>(
              rows, input, context, result, errorPolicy);
        } else {
          convertWithPolicy<false, false>(
              rows, input, context, result, errorPolicy);
        }
      } else {
        convertWithPolicy<false, false>(
            rows, input, context, result, errorPolicy);
      }
    }
  }
};

// Runtime map key. Lookup happens once per doCast() call (outside the
// per-row loop).
struct CastKindKey {
  TypeKind storage;
  Decoration deco;
  bool operator==(const CastKindKey& o) const {
    return storage == o.storage && deco == o.deco;
  }

  bool operator<(const CastKindKey& o) const {
    return std::tie(storage, deco) < std::tie(o.storage, o.deco);
  }
};

CastKindKey getCastKind(const TypePtr& type) {
  if (type->isShortDecimal()) {
    return {TypeKind::BIGINT, Decoration::ShortDecimal};
  }
  if (type->isLongDecimal()) {
    return {TypeKind::HUGEINT, Decoration::LongDecimal};
  }
  if (type->isDate()) {
    return {TypeKind::INTEGER, Decoration::Date};
  }
  auto kind = type->kind();

  switch (kind) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
    case TypeKind::UNKNOWN:
      return {kind, Decoration::None};

    case TypeKind::HUGEINT:
      return {kind, Decoration::LongDecimal};

    default:
      BOLT_FAIL("Unsupported type kind: {}", type->toString());
  }
}

std::map<std::pair<CastKindKey, CastKindKey>, std::shared_ptr<ConverterBase>>
    converters;

template <typename... Kinds>
struct KindList {};

template <typename FromKind, typename ToKind>
void registerOne() {
  // Skip same plain types (identity handled by copy fast-path in doCast).
  // Allow same-type-with-decoration (e.g., ShortDecimal→ShortDecimal).
  if constexpr (!(std::is_same_v<FromKind, ToKind> &&
                  FromKind::deco == Decoration::None)) {
    converters[{
        CastKindKey{FromKind::storage, FromKind::deco},
        CastKindKey{ToKind::storage, ToKind::deco}}] =
        std::make_shared<VectorConverter<FromKind, ToKind>>();
  }
}

template <typename FromKind, typename... ToKinds>
void registerForOneFrom() {
  (registerOne<FromKind, ToKinds>(), ...);
}

template <typename FromList, typename ToList>
struct RegisterAllPairs;

template <typename... FromKinds, typename... ToKinds>
struct RegisterAllPairs<KindList<FromKinds...>, KindList<ToKinds...>> {
  static void apply() {
    (registerForOneFrom<FromKinds, ToKinds...>(), ...);
  }
};

void registerConverter() {
  using NumStrType = KindList<
      BooleanKind,
      TinyintKind,
      SmallintKind,
      IntegerKind,
      BigintKind,
      RealKind,
      DoubleKind,
      ShortDecimalKind,
      LongDecimalKind,
      StringKind>;
  using DateStrType = KindList<DateKind, TimestampKind, StringKind>;

  RegisterAllPairs<NumStrType, NumStrType>::apply();
  RegisterAllPairs<DateStrType, DateStrType>::apply();

#ifdef SPARK_COMPATIBLE
  using IntegerKinds =
      KindList<TinyintKind, SmallintKind, IntegerKind, BigintKind>;
  RegisterAllPairs<IntegerKinds, KindList<TimestampKind>>::apply();
  RegisterAllPairs<KindList<RealKind, DoubleKind>, KindList<TimestampKind>>::
      apply();
  RegisterAllPairs<KindList<BooleanKind>, KindList<TimestampKind>>::apply();
  RegisterAllPairs<IntegerKinds, KindList<BinaryKind>>::apply();
  RegisterAllPairs<KindList<TimestampKind>, IntegerKinds>::apply();
#endif
}

void doCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& result,
    CastErrorPolicy errorPolicy);

void doCastArrayToVarchar(
    const SelectivityVector& rows,
    const ArrayVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    VectorPtr& result,
    CastErrorPolicy errorPolicy) {
  context.ensureWritable(rows, VARCHAR(), result);
  result->clearNulls(rows);
  auto flatResult = result->as<FlatVector<StringView>>();

  VectorPtr resultElements;
  auto arrayElements = input.elements();

  auto nestedRows =
      functions::toElementRows(arrayElements->size(), rows, &input);

  LocalSelectivityVector remainingRows(context, nestedRows);

  LocalDecodedVector decoded(context, *arrayElements, *remainingRows);
  auto* rawNulls = decoded->nulls(remainingRows.get());

  if (rawNulls) {
    remainingRows->deselectNulls(
        rawNulls, remainingRows->begin(), remainingRows->end());
  }

  context.ensureWritable(nestedRows, VARCHAR(), resultElements);
  doCast(
      *remainingRows,
      *arrayElements,
      context,
      arrayElements->type(),
      VARCHAR(),
      resultElements,
      errorPolicy);

  resultElements->addNulls(remainingRows->asRange().bits(), nestedRows);

  const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();
  const bool legacyComplex = isInSpark &&
      (queryConfig.isSparkLegacyCastComplexTypesToStringEnabled() != "false");
  if (queryConfig.enableFlinkCompatible() &&
      arrayElements->type()->isTimestamp()) {
    auto flatElements = resultElements->as<FlatVector<StringView>>();
    nestedRows.applyToSelected([&](auto row) INLINE_LAMBDA {
      if (flatElements->isNullAt(row)) {
        return;
      }
      auto value = flatElements->valueAt(row);
      int32_t dot = -1;
      for (int32_t i = 0; i < value.size(); ++i) {
        if (value.data()[i] == '.') {
          dot = i;
          break;
        }
      }
      if (dot == -1) {
        return;
      }
      bool allZeros = true;
      for (int32_t i = dot + 1; i < value.size(); ++i) {
        if (value.data()[i] != '0') {
          allZeros = false;
          break;
        }
      }
      if (allZeros) {
        flatElements->set(row, StringView(value.data(), dot));
      }
    });
  }

  auto rawElements = resultElements->as<FlatVector<StringView>>()->rawValues();
  rows.applyToSelected([&](auto row) INLINE_LAMBDA {
    if (input.isNullAt(row)) {
      result->setNull(row, true);
    } else {
      functions::InPlaceString str{flatResult};
      auto offset = input.offsetAt(row);
      auto size = input.sizeAt(row);
      str.append(std::string_view("["), flatResult);
      for (auto i = offset; i < offset + size; ++i) {
        if (i > offset) {
          str.append(std::string_view(","), flatResult);
        }
        if (resultElements->isNullAt(i)) {
          if (!legacyComplex) {
            if (i > offset) {
              str.append(std::string_view(" "), flatResult);
            }
            str.append(std::string_view("null"), flatResult);
          }
        } else {
          if (i > offset) {
            str.append(std::string_view(" "), flatResult);
          }
          str.append(rawElements[i], flatResult);
        }
      }
      str.append(std::string_view("]"), flatResult);
      str.set(row, flatResult);
    }
  });
}

void doCastMapToVarchar(
    const SelectivityVector& rows,
    const MapVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    VectorPtr& result,
    CastErrorPolicy errorPolicy) {
  context.ensureWritable(rows, VARCHAR(), result);
  result->clearNulls(rows);
  auto flatResult = result->as<FlatVector<StringView>>();

  VectorPtr resultKeys, resultValues;
  auto keys = input.mapKeys();
  auto values = input.mapValues();

  auto nestedRows = functions::toElementRows(keys->size(), rows, &input);

  // keys
  {
    LocalSelectivityVector remainingRows(context, nestedRows);
    LocalDecodedVector decoded(context, *keys, *remainingRows);
    auto* rawNulls = decoded->nulls(remainingRows.get());
    if (rawNulls) {
      remainingRows->deselectNulls(
          rawNulls, remainingRows->begin(), remainingRows->end());
    }

    context.ensureWritable(nestedRows, VARCHAR(), resultKeys);
    doCast(
        *remainingRows,
        *keys,
        context,
        keys->type(),
        VARCHAR(),
        resultKeys,
        errorPolicy);

    resultKeys->addNulls(remainingRows->asRange().bits(), nestedRows);
  }

  // values
  {
    LocalSelectivityVector remainingRows(context, nestedRows);
    LocalDecodedVector decoded(context, *values, *remainingRows);
    auto* rawNulls = decoded->nulls(remainingRows.get());
    if (rawNulls) {
      remainingRows->deselectNulls(
          rawNulls, remainingRows->begin(), remainingRows->end());
    }

    context.ensureWritable(nestedRows, VARCHAR(), resultValues);
    doCast(
        *remainingRows,
        *values,
        context,
        values->type(),
        VARCHAR(),
        resultValues,
        errorPolicy);

    resultValues->addNulls(remainingRows->asRange().bits(), nestedRows);
  }

  auto rawKeys = resultKeys->as<FlatVector<StringView>>()->rawValues();
  auto rawValues = resultValues->as<FlatVector<StringView>>()->rawValues();

  const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();
  const bool legacyComplex = isInSpark &&
      (queryConfig.isSparkLegacyCastComplexTypesToStringEnabled() != "false");
  const bool isFlinkCompatible = queryConfig.enableFlinkCompatible();

  std::string_view leftBracket = legacyComplex ? "[" : "{";
  std::string_view rightBracket = legacyComplex ? "]" : "}";
  std::string_view kvConnector = isFlinkCompatible ? "=" : " ->";

  rows.applyToSelected([&](auto row) INLINE_LAMBDA {
    if (input.isNullAt(row)) {
      result->setNull(row, true);
    } else {
      functions::InPlaceString str{flatResult};
      auto offset = input.offsetAt(row);
      auto size = input.sizeAt(row);
      str.append(leftBracket, flatResult);
      for (auto i = offset; i < offset + size; ++i) {
        if (i > offset) {
          str.append(std::string_view(", "), flatResult);
        }
        BOLT_CHECK(!resultKeys->isNullAt(i));
        str.append(rawKeys[i], flatResult);
        str.append(kvConnector, flatResult);
        if (resultValues->isNullAt(i)) {
          if (!legacyComplex) {
            if (!isFlinkCompatible) {
              str.append(std::string_view(" "), flatResult);
            }
            str.append(std::string_view("null"), flatResult);
          }
        } else {
          if (!isFlinkCompatible) {
            str.append(std::string_view(" "), flatResult);
          }
          str.append(rawValues[i], flatResult);
        }
      }
      str.append(rightBracket, flatResult);
      str.set(row, flatResult);
    }
  });
}

void doCastRowToVarchar(
    const SelectivityVector& rows,
    const RowVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    VectorPtr& result,
    CastErrorPolicy errorPolicy) {
  context.ensureWritable(rows, VARCHAR(), result);
  result->clearNulls(rows);
  auto flatResult = result->as<FlatVector<StringView>>();

  size_t colSize = input.childrenSize();
  std::vector<VectorPtr> resultElements(colSize);
  std::vector<const StringView*> rawResultElements(colSize);

  for (auto i = 0; i < colSize; ++i) {
    LocalSelectivityVector remainingRows(context, rows);

    LocalDecodedVector decoded(context, *input.childAt(i), *remainingRows);
    auto* rawNulls = decoded->nulls(remainingRows.get());

    if (rawNulls) {
      remainingRows->deselectNulls(
          rawNulls, remainingRows->begin(), remainingRows->end());
    }
    context.ensureWritable(rows, VARCHAR(), resultElements[i]);
    doCast(
        *remainingRows,
        *input.childAt(i),
        context,
        input.childAt(i)->type(),
        VARCHAR(),
        resultElements[i],
        errorPolicy);

    resultElements[i]->addNulls(remainingRows->asRange().bits(), rows);
    rawResultElements[i] =
        resultElements[i]->as<FlatVector<StringView>>()->rawValues();
  }

  const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();
  const bool legacyComplex = isInSpark &&
      (queryConfig.isSparkLegacyCastComplexTypesToStringEnabled() != "false");

  std::string_view leftBracket = legacyComplex ? "[" : "{";
  std::string_view rightBracket = legacyComplex ? "]" : "}";

  rows.applyToSelected([&](auto row) INLINE_LAMBDA {
    if (input.isNullAt(row)) {
      result->setNull(row, true);
    } else {
      functions::InPlaceString str{flatResult};
      str.append(leftBracket, flatResult);
      for (size_t i = 0; i < colSize; i++) {
        if (i > 0) {
          str.append(std::string_view(","), flatResult);
        }
        if (resultElements[i]->isNullAt(row)) {
          if (!legacyComplex) {
            if (i > 0) {
              str.append(std::string_view(" "), flatResult);
            }
            str.append(std::string_view("null"), flatResult);
          }
        } else {
          if (i > 0) {
            str.append(std::string_view(" "), flatResult);
          }
          str.append(rawResultElements[i][row], flatResult);
        }
      }

      str.append(rightBracket, flatResult);
      str.set(row, flatResult);
    }
  });
}

folly::once_flag onceFlag;

/**
 * @brief Cast from one type to another type.
 *
 * we classify cast into three categories:
 * 1. primary to primary
 * 2. complex to string
 * 3. complex to complex
 *
 * for primary to primary, we divide into two parts:
 * 1. from numeric or string to numeric or string
 * 2. from date or timestamp or string to date or timestamp or string
 *
 * for complex to string, we convert with columnar execution, and deep into
 * the inner primitive type, and convert to string step by step.
 *
 * for complex to complex, it was handled in CastExpr.cpp, and will call here
 * for its inner primitive type conversion.
 */
void doCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& result,
    CastErrorPolicy errorPolicy) {
  folly::call_once(onceFlag, [&] { registerConverter(); });
  if (fromType->isPrimitiveType() && toType->isPrimitiveType()) {
    context.ensureWritable(rows, toType, result);
    if (toType->kind() == TypeKind::UNKNOWN) {
      result->addNulls(rows);
      return;
    }
    result->clearNulls(rows);
    if (fromType->equivalent(*toType) ||
        (fromType->isUseStringView() && toType->isUseStringView())) {
      // fromType is the same as toType, just copy the input vector
      result->copy(&input, rows, nullptr, context.isFinalSelection());
      return;
    }
    auto fromKey = getCastKind(fromType);
    auto toKey = getCastKind(toType);
    auto it = converters.find(std::make_pair(fromKey, toKey));
    if (it != converters.end()) {
      it->second->convert(rows, input, context, result, errorPolicy);
    } else {
      BOLT_FAIL(
          "unsupported type conversion from {} to {}",
          fromType->toString(),
          toType->toString());
    }
  } else if (
      !fromType->isPrimitiveType() && toType->kind() == TypeKind::VARCHAR) {
    if (fromType->isArray()) {
      doCastArrayToVarchar(
          rows,
          *input.as<ArrayVector>(),
          context,
          fromType,
          result,
          errorPolicy);
    } else if (fromType->isMap()) {
      doCastMapToVarchar(
          rows, *input.as<MapVector>(), context, fromType, result, errorPolicy);
    } else if (fromType->isRow()) {
      doCastRowToVarchar(
          rows, *input.as<RowVector>(), context, fromType, result, errorPolicy);
    }
  } else {
    BOLT_FAIL(
        "unsupported type conversion from {} to {}",
        fromType->toString(),
        toType->toString());
  }
}

} // namespace bytedance::bolt::exec::CastUtils
