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

#include "bolt/functions/lib/DateTimeFormatterBuilder.h"
#include "bolt/functions/lib/DateTimeFormatter.h"

namespace bytedance::bolt::functions {

DateTimeFormatterBuilder::DateTimeFormatterBuilder(size_t literalBufSize) {
  literalBuf_ = std::unique_ptr<char[]>(new char[literalBufSize]);
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendEra() {
  return appendEra(2);
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendEra(
    size_t minDigits) {
  tokens_.emplace_back(FormatPattern{DateTimeFormatSpecifier::ERA, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendCenturyOfEra(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::CENTURY_OF_ERA, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendYearOfEra(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::YEAR_OF_ERA, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendWeekYear(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::WEEK_YEAR, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendWeekOfWeekYear(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendWeekOfMonth(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::WEEK_OF_MONTH, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendDayOfWeekInMonth(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendDayOfWeek0Based(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_0_BASED, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendDayOfWeek1Based(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendDayOfWeekText(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendYear(
    size_t minDigits) {
  tokens_.emplace_back(FormatPattern{DateTimeFormatSpecifier::YEAR, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendDayOfYear(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::DAY_OF_YEAR, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendMonthOfYear(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendMonthOfYearText(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendDayOfMonth(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::DAY_OF_MONTH, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendHalfDayOfDay() {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::HALFDAY_OF_DAY, 2});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendHourOfHalfDay(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::HOUR_OF_HALFDAY, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendClockHourOfHalfDay(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendHourOfDay(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::HOUR_OF_DAY, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendClockHourOfDay(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendMinuteOfHour(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::MINUTE_OF_HOUR, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendSecondOfMinute(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::SECOND_OF_MINUTE, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendFractionOfSecond(
    size_t digits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::FRACTION_OF_SECOND, digits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendTimeZone(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::TIMEZONE, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendTimeZoneOffsetId(
    size_t minDigits) {
  tokens_.emplace_back(
      FormatPattern{DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendTimeZoneId() {
  tokens_.emplace_back(FormatPattern{DateTimeFormatSpecifier::TIMEZONE_ID, 3});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendISOTimeZoneOffsetId(
    size_t minDigits) {
  return appendISOTimeZoneOffsetId(minDigits, true);
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendISOTimeZoneOffsetId(
    size_t minDigits,
    bool zeroAsZ) {
  tokens_.emplace_back(FormatPattern{
      zeroAsZ ? DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601
              : DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601_NO_Z,
      minDigits});
  return *this;
}

DateTimeFormatterBuilder&
DateTimeFormatterBuilder::appendRFC822TimeZoneOffsetId(size_t minDigits) {
  tokens_.emplace_back(FormatPattern{
      DateTimeFormatSpecifier::TIMEZONE_OFFSET_RFC822, minDigits});
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendLiteral(
    const std::string_view& literal) {
  return appendLiteral(literal.data(), literal.size());
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::appendLiteral(
    const char* literalStart,
    size_t literalSize) {
  std::strncpy(literalBuf_.get() + bufEnd_, literalStart, literalSize);
  bufEnd_ += literalSize;
  if (!tokens_.empty() &&
      tokens_.back().type == DateTimeToken::Type::kLiteral) {
    // Extend the previous literal string view.
    auto& prev = tokens_.back().literal;
    tokens_.back().literal =
        std::string_view(prev.data(), prev.size() + literalSize);
  } else {
    tokens_.emplace_back(std::string_view(
        literalBuf_.get() + bufEnd_ - literalSize, literalSize));
  }
  return *this;
}

DateTimeFormatterBuilder& DateTimeFormatterBuilder::setType(
    DateTimeFormatterType type) {
  type_ = type;
  return *this;
}

std::shared_ptr<DateTimeFormatter> DateTimeFormatterBuilder::build() {
  BOLT_CHECK_NE(type_, DateTimeFormatterType::UNKNOWN);
  return std::make_shared<DateTimeFormatter>(
      std::move(literalBuf_), bufEnd_, std::move(tokens_), type_);
}

} // namespace bytedance::bolt::functions
