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

#include "bolt/expression/EvalCtx.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::functions::flinksql {
namespace {

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
  if (!(fromType->isDecimal() && type_->isVarchar())) {
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
    auto* flatResult = localResult->asFlatVector<StringView>();
    BOLT_CHECK_NOT_NULL(flatResult);

    int32_t precision;
    int32_t scale;
    if (fromType->isShortDecimal()) {
      precision = fromType->asShortDecimal().precision();
      scale = fromType->asShortDecimal().scale();
      const auto maxSize = DecimalUtil::stringSize(precision, scale);
      remainingRows->applyToSelected([&](vector_size_t row) {
        setPlainDecimalString(
            flatResult, row, decoded->valueAt<int64_t>(row), scale, maxSize);
      });
    } else {
      precision = fromType->asLongDecimal().precision();
      scale = fromType->asLongDecimal().scale();
      const auto maxSize = DecimalUtil::stringSize(precision, scale);
      remainingRows->applyToSelected([&](vector_size_t row) {
        setPlainDecimalString(
            flatResult, row, decoded->valueAt<int128_t>(row), scale, maxSize);
      });
    }
  }

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
