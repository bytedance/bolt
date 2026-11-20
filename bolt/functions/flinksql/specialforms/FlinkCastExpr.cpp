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

#include "bolt/functions/flinksql/specialforms/FlinkCastExpr.h"

#include <array>
#include <limits>
#include <string_view>

#include "bolt/expression/EvalCtx.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::functions::flinksql {
namespace {

// Matches Flink SqlDateTimeUtils.dateStringToUnixDate. The zero-date
// replacement precedes suffix removal and trimming.
std::optional<int32_t>
parseDate(std::string_view value, bool compatibleWithMysql, bool& overflow) {
  if (compatibleWithMysql && value == "0000-00-00") {
    value = "0001-01-01";
  }
  std::array<int32_t, 3> parts{0, 1, 1};
  auto* pos = value.data();
  const auto* end = pos + value.size();
  const bool startsWithSpace = value.starts_with(' ');
  const auto skipWhitespace = [&]() {
    // Java String.trim() removes characters up to U+0020.
    while (pos != end && static_cast<unsigned char>(*pos) <= 0x20) {
      // A space terminates the date unless the original string starts with one.
      if (*pos == ' ' && !startsWithSpace) {
        end = pos;
        break;
      }
      ++pos;
    }
  };
  for (size_t i = 0; i < parts.size(); ++i) {
    skipWhitespace();
    if (pos == end || *pos < '0' || *pos > '9') {
      return std::nullopt;
    }
    int64_t part = 0;
    do {
      part = part * 10 + (*pos - '0');
      ++pos;
      if (part > std::numeric_limits<int32_t>::max()) {
        while (pos != end && *pos >= '0' && *pos <= '9') {
          ++pos;
        }
        break;
      }
    } while (pos != end && *pos >= '0' && *pos <= '9');
    skipWhitespace();
    // Validate the whole component before reporting integer overflow.
    if (pos != end && (i == parts.size() - 1 || *pos != '-')) {
      return std::nullopt;
    }
    if (part > std::numeric_limits<int32_t>::max()) {
      overflow = true;
      return std::nullopt;
    }
    parts[i] = part;
    if (pos == end) {
      break;
    }
    ++pos;
  }

  if (parts[0] > 9999) {
    return std::nullopt;
  }
  bool valid;
  const auto days =
      util::daysSinceEpochFromDate(parts[0], parts[1], parts[2], &valid);
  return valid ? std::optional<int32_t>(days) : std::nullopt;
}

template <typename T>
void setPlainDecimalString(
    FlatVector<StringView>* result,
    vector_size_t row,
    T value,
    int32_t scale,
    int32_t maxSize) {
  std::array<char, 128> buffer;
  BOLT_CHECK_LE(
      maxSize,
      buffer.size(),
      "Unexpected decimal to varchar buffer size: {}",
      maxSize);
  const auto size =
      DecimalUtil::convertToString<DecimalUtil::DecimalStringFormat::kPlain>(
          value, scale, maxSize, buffer.data());
  result->set(row, StringView(buffer.data(), size));
}

} // namespace

void FlinkCastExpr::evalSpecialForm(
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    VectorPtr& result) {
  const auto fromType = inputs_[0]->type();
  const bool stringToDate = fromType->isVarchar() && type_->isDate();
  if (!stringToDate && !(fromType->isDecimal() && type_->isVarchar())) {
    exec::CastExpr::evalSpecialForm(rows, context, result);
    return;
  }

  VectorPtr input;
  inputs_[0]->eval(rows, context, input);

  exec::LocalSelectivityVector remainingRows(context, rows);
  context.deselectErrors(*remainingRows);

  exec::LocalDecodedVector decoded(context, *input, *remainingRows);
  auto* rawNulls = decoded->nulls(remainingRows.get());
  if (rawNulls) {
    remainingRows->deselectNulls(
        rawNulls, remainingRows->begin(), remainingRows->end());
  }

  VectorPtr localResult;
  if (!remainingRows->hasSelections()) {
    localResult =
        BaseVector::createNullConstant(type_, rows.end(), context.pool());
  } else {
    context.ensureWritable(*remainingRows, type_, localResult);
    if (stringToDate) {
      auto* flatResult = localResult->asFlatVector<int32_t>();
      flatResult->clearNulls(*remainingRows);
      auto* rawResult = flatResult->mutableRawValues();
      const bool compatibleWithMysql =
          context.execCtx()->queryCtx()->queryConfig().get<bool>(
              "table.exec.compatible-with-mysql", false);
      context.applyToSelectedNoThrow(*remainingRows, [&](vector_size_t row) {
        const auto value = decoded->valueAt<StringView>(row);
        bool overflow = false;
        const auto date =
            parseDate(std::string_view(value), compatibleWithMysql, overflow);
        BOLT_USER_CHECK(
            !overflow || nullOnFailure_,
            "Flink DATE component exceeds the 32-bit integer range");
        if (date) {
          rawResult[row] = *date;
        } else {
          flatResult->setNull(row, true);
        }
      });
    } else if (fromType->isShortDecimal()) {
      auto* flatResult = localResult->asFlatVector<StringView>();
      const auto precision = fromType->asShortDecimal().precision();
      const auto scale = fromType->asShortDecimal().scale();
      const auto maxSize = DecimalUtil::stringSize(precision, scale);
      remainingRows->applyToSelected([&](vector_size_t row) {
        setPlainDecimalString(
            flatResult, row, decoded->valueAt<int64_t>(row), scale, maxSize);
      });
    } else {
      auto* flatResult = localResult->asFlatVector<StringView>();
      const auto precision = fromType->asLongDecimal().precision();
      const auto scale = fromType->asLongDecimal().scale();
      const auto maxSize = DecimalUtil::stringSize(precision, scale);
      remainingRows->applyToSelected([&](vector_size_t row) {
        setPlainDecimalString(
            flatResult, row, decoded->valueAt<int128_t>(row), scale, maxSize);
      });
    }
  }

  context.deselectErrors(*remainingRows);
  context.moveOrCopyResult(localResult, *remainingRows, result);
  context.releaseVector(localResult);
  localResult.reset();

  if (rawNulls || context.errors()) {
    exec::EvalCtx::addNulls(
        rows, remainingRows->asRange().bits(), context, type_, result);
  }
  context.releaseVector(input);
}

exec::ExprPtr FlinkCastCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  BOLT_CHECK_EQ(
      compiledChildren.size(),
      1,
      "CAST statements expect exactly 1 argument, received {}.",
      compiledChildren.size());
  return std::make_shared<FlinkCastExpr>(
      type, std::move(compiledChildren[0]), trackCpuUsage, false);
}

exec::ExprPtr FlinkTryCastCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  BOLT_CHECK_EQ(
      compiledChildren.size(),
      1,
      "TRY CAST statements expect exactly 1 argument, received {}.",
      compiledChildren.size());
  return std::make_shared<FlinkCastExpr>(
      type, std::move(compiledChildren[0]), trackCpuUsage, true);
}

} // namespace bytedance::bolt::functions::flinksql
