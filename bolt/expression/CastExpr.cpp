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

#include "bolt/expression/CastExpr.h"

#include <fmt/format.h>
#include <stdexcept>

#include <date/tz.h>
#include "bolt/common/base/Exceptions.h"
#include "bolt/core/CoreTypeSystem.h"
#include "bolt/expression/CastExpr-tpl.h"
#include "bolt/expression/PeeledEncoding.h"
#include "bolt/expression/ScopedVarSetter.h"
#include "bolt/functions/lib/RowsTranslationUtil.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FunctionVector.h"
#include "bolt/vector/SelectivityVector.h"
namespace bytedance::bolt::exec {

namespace {
void propagateErrorsOrSetNulls(
    bool setNullInResultAtError,
    EvalCtx& context,
    const SelectivityVector& nestedRows,
    const BufferPtr& elementToTopLevelRows,
    VectorPtr& result,
    ErrorVectorPtr& oldErrors) {
  if (context.errors()) {
    if (setNullInResultAtError) {
      // Errors in context.errors() should be translated to nulls in the top
      // level rows.
      context.convertElementErrorsToTopLevelNulls(
          nestedRows, elementToTopLevelRows, result);
    } else {
      context.addElementErrorsToTopLevel(
          nestedRows, elementToTopLevelRows, oldErrors);
    }
  }
}
} // namespace

#define BOLT_DYNAMIC_DECIMAL_TYPE_DISPATCH(TEMPLATE_FUNC, decimalTypePtr, ...) \
  [&]() {                                                                      \
    if (decimalTypePtr->isLongDecimal()) {                                     \
      return TEMPLATE_FUNC<int128_t>(__VA_ARGS__);                             \
    } else {                                                                   \
      return TEMPLATE_FUNC<int64_t>(__VA_ARGS__);                              \
    }                                                                          \
  }()

VectorPtr CastExpr::applyMap(
    const SelectivityVector& rows,
    const MapVector* input,
    exec::EvalCtx& context,
    const MapType& fromType,
    const MapType& toType) {
  // Cast input keys/values vector to output keys/values vector using their
  // element selectivity vector

  // Initialize nested rows
  auto mapKeys = input->mapKeys();
  auto mapValues = input->mapValues();

  SelectivityVector nestedRows;
  BufferPtr elementToTopLevelRows;
  if (fromType.keyType() != toType.keyType() ||
      fromType.valueType() != toType.valueType()) {
    nestedRows = functions::toElementRows(mapKeys->size(), rows, input);
    elementToTopLevelRows = functions::getElementToTopLevelRows(
        mapKeys->size(), rows, input, context.pool());
  }

  ErrorVectorPtr oldErrors;
  context.swapErrors(oldErrors);

  // Cast keys
  VectorPtr newMapKeys;
  if (fromType.keyType() == toType.keyType()) {
    newMapKeys = input->mapKeys();
  } else {
    {
      ScopedVarSetter holder(&inTopLevel, false);
      apply(
          nestedRows,
          mapKeys,
          context,
          fromType.keyType(),
          toType.keyType(),
          newMapKeys);
    }
  }

  // Cast values
  VectorPtr newMapValues;
  if (fromType.valueType() == toType.valueType()) {
    newMapValues = mapValues;
  } else {
    {
      ScopedVarSetter holder(&inTopLevel, false);
      apply(
          nestedRows,
          mapValues,
          context,
          fromType.valueType(),
          toType.valueType(),
          newMapValues);
    }
  }

  // Returned map vector should be addressable for every element, even those
  // that are not selected.
  BufferPtr sizes = input->sizes();
  if (newMapKeys->isConstantEncoding() && newMapValues->isConstantEncoding()) {
    // We extends size since that is cheap.
    newMapKeys->resize(input->mapKeys()->size());
    newMapValues->resize(input->mapValues()->size());
  } else if (
      newMapKeys->size() < input->mapKeys()->size() ||
      newMapValues->size() < input->mapValues()->size()) {
    sizes =
        AlignedBuffer::allocate<vector_size_t>(rows.end(), context.pool(), 0);
    auto* inputSizes = input->rawSizes();
    auto* rawSizes = sizes->asMutable<vector_size_t>();

    rows.applyToSelected(
        [&](vector_size_t row) { rawSizes[row] = inputSizes[row]; });
  }

  // Assemble the output map
  VectorPtr result = std::make_shared<MapVector>(
      context.pool(),
      MAP(toType.keyType(), toType.valueType()),
      input->nulls(),
      rows.end(),
      input->offsets(),
      sizes,
      newMapKeys,
      newMapValues);

#ifndef SPARK_COMPATIBLE
  propagateErrorsOrSetNulls(
      setNullInResultAtError(),
      context,
      nestedRows,
      elementToTopLevelRows,
      result,
      oldErrors);
#endif
  // Restore original state.
  context.swapErrors(oldErrors);
  return result;
}

VectorPtr CastExpr::applyArray(
    const SelectivityVector& rows,
    const ArrayVector* input,
    exec::EvalCtx& context,
    const ArrayType& fromType,
    const ArrayType& toType) {
  // Cast input array elements to output array elements based on their types
  // using their linear selectivity vector
  auto arrayElements = input->elements();

  auto nestedRows =
      functions::toElementRows(arrayElements->size(), rows, input);
  auto elementToTopLevelRows = functions::getElementToTopLevelRows(
      arrayElements->size(), rows, input, context.pool());

  ErrorVectorPtr oldErrors;
  context.swapErrors(oldErrors);

  VectorPtr newElements;
  {
    ScopedVarSetter holder(&inTopLevel, false);
    apply(
        nestedRows,
        arrayElements,
        context,
        fromType.elementType(),
        toType.elementType(),
        newElements);
  }

  // Returned array vector should be addressable for every element, even those
  // that are not selected.
  BufferPtr sizes = input->sizes();
  if (newElements->isConstantEncoding()) {
    // If the newElements we extends its size since that is cheap.
    newElements->resize(input->elements()->size());
  } else if (newElements->size() < input->elements()->size()) {
    sizes =
        AlignedBuffer::allocate<vector_size_t>(rows.end(), context.pool(), 0);
    auto* inputSizes = input->rawSizes();
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    rows.applyToSelected(
        [&](vector_size_t row) { rawSizes[row] = inputSizes[row]; });
  }

  VectorPtr result = std::make_shared<ArrayVector>(
      context.pool(),
      ARRAY(toType.elementType()),
      input->nulls(),
      rows.end(),
      input->offsets(),
      sizes,
      newElements);

#ifndef SPARK_COMPATIBLE
  propagateErrorsOrSetNulls(
      setNullInResultAtError(),
      context,
      nestedRows,
      elementToTopLevelRows,
      result,
      oldErrors);
#endif

  // Restore original state.
  context.swapErrors(oldErrors);
  return result;
}

// Cast from unknown type to other types.
VectorPtr CastExpr::applyUnknown(
    const SelectivityVector& rows,
    const BaseVector& /*input*/,
    exec::EvalCtx& context,
    const TypePtr& /*fromType*/,
    const TypePtr& toType) {
  return BaseVector::createNullConstant(toType, rows.end(), context.pool());
}

VectorPtr CastExpr::applyRow(
    const SelectivityVector& rows,
    const RowVector* input,
    exec::EvalCtx& context,
    const RowType& fromType,
    const TypePtr& toType) {
  const RowType& toRowType = toType->asRow();
  int numInputChildren = input->children().size();
  int numOutputChildren = toRowType.size();

  // Extract the flag indicating matching of children must be done by name or
  // position
  auto matchByName =
      context.execCtx()->queryCtx()->queryConfig().isMatchStructByName();

  // Cast each row child to its corresponding output child
  std::vector<VectorPtr> newChildren;
  newChildren.reserve(numOutputChildren);

  ErrorVectorPtr oldErrors;
  if (setNullInResultAtError()) {
    // We need to isolate errors that happen during the cast from previous
    // errors since those translate to nulls, unlike existing errors.
    context.swapErrors(oldErrors);
  }

  for (auto toChildrenIndex = 0; toChildrenIndex < numOutputChildren;
       toChildrenIndex++) {
    // For each child, find the corresponding column index in the output
    const auto& toFieldName = toRowType.nameOf(toChildrenIndex);
    bool matchNotFound = false;

    // If match is by field name and the input field name is not found
    // in the output row type, do not consider it in the output
    int fromChildrenIndex = -1;
    if (matchByName) {
      if (!fromType.containsChild(toFieldName)) {
        matchNotFound = true;
      } else {
        fromChildrenIndex = fromType.getChildIdx(toFieldName);
        toChildrenIndex = toRowType.getChildIdx(toFieldName);
      }
    } else {
      fromChildrenIndex = toChildrenIndex;
      if (fromChildrenIndex >= numInputChildren) {
        matchNotFound = true;
      }
    }

    // Updating output types and names
    VectorPtr outputChild;
    const auto& toChildType = toRowType.childAt(toChildrenIndex);

    if (matchNotFound) {
      // Create a vector for null for this child
      context.ensureWritable(rows, toChildType, outputChild);
      outputChild->addNulls(rows);
    } else {
      const auto& inputChild = input->children()[fromChildrenIndex];
      if (toChildType == inputChild->type()) {
        outputChild = inputChild;
      } else {
        // Apply cast for the child.
        ScopedVarSetter holder(&inTopLevel, false);
        apply(
            rows,
            inputChild,
            context,
            inputChild->type(),
            toChildType,
            outputChild);
      }
    }
    newChildren.emplace_back(std::move(outputChild));
  }

  // Assemble the output row
  VectorPtr result = std::make_shared<RowVector>(
      context.pool(),
      toType,
      input->nulls(),
      rows.end(),
      std::move(newChildren));

#ifndef SPARK_COMPATIBLE
  if (setNullInResultAtError()) {
    // Set errors as nulls.
    if (auto errors = context.errors()) {
      rows.applyToSelected([&](auto row) {
        if (errors->isIndexInRange(row) && !errors->isNullAt(row)) {
          result->setNull(row, true);
        }
      });
    }
    // Restore original state.
    context.swapErrors(oldErrors);
  }
#endif

  return result;
}

void CastExpr::applyPeeled(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& result) {
  auto castFromOperator = getCastOperator(fromType);
  if (castFromOperator && !castFromOperator->isSupportedToType(toType)) {
    BOLT_USER_FAIL(
        "Cannot cast {} to {}.", fromType->toString(), toType->toString());
  }

  auto castToOperator = getCastOperator(toType);
  if (castToOperator && !castToOperator->isSupportedFromType(fromType)) {
    BOLT_USER_FAIL(
        "Cannot cast {} to {}.", fromType->toString(), toType->toString());
  }

  if (castFromOperator || castToOperator) {
    BOLT_USER_CHECK(
        *fromType != *toType,
        "Attempting to cast from {} to itself.",
        fromType->toString());

    auto applyCustomCast = [&]() {
      if (castToOperator) {
        castToOperator->castTo(input, context, rows, toType, result);
      } else {
        castFromOperator->castFrom(input, context, rows, toType, result);
      }
    };

    if (setNullInResultAtError()) {
      // This can be optimized by passing setNullInResultAtError() to castTo
      // and castFrom operations.

      ErrorVectorPtr oldErrors;
      context.swapErrors(oldErrors);

      applyCustomCast();

      if (context.errors()) {
        auto errors = context.errors();
        auto rawNulls = result->mutableRawNulls();

        rows.applyToSelected([&](auto row) {
          if (errors->isIndexInRange(row) && !errors->isNullAt(row)) {
            bits::setNull(rawNulls, row, true);
          }
        });
      };
      // Restore original state.
      context.swapErrors(oldErrors);

    } else {
      applyCustomCast();
    }
  } else if (fromType->isPrimitiveType() || toType->isPrimitiveType()) {
#ifdef SPARK_COMPATIBLE
    CastUtils::CastErrorPolicy policy = nullOnFailure()
        ? CastUtils::CastErrorPolicy::NullOnFailure
        : CastUtils::CastErrorPolicy::SparkCastPolicy;
#else
    CastUtils::CastErrorPolicy policy = setNullInResultAtError()
        ? CastUtils::CastErrorPolicy::NullOnFailure
        : CastUtils::CastErrorPolicy::ThrowOnFailure;
#endif
    CastUtils::doCast(rows, input, context, fromType, toType, result, policy);
  } else {
    switch (toType->kind()) {
      case TypeKind::MAP:
        result = applyMap(
            rows,
            input.asUnchecked<MapVector>(),
            context,
            fromType->asMap(),
            toType->asMap());
        break;
      case TypeKind::ARRAY:
        result = applyArray(
            rows,
            input.asUnchecked<ArrayVector>(),
            context,
            fromType->asArray(),
            toType->asArray());
        break;
      case TypeKind::ROW:
        result = applyRow(
            rows,
            input.asUnchecked<RowVector>(),
            context,
            fromType->asRow(),
            toType);
        break;
      case TypeKind::UNKNOWN:
        result = applyUnknown(rows, input, context, fromType, toType);
        break;
      default: {
        BOLT_FAIL(
            "Unsupported cast from {} to {}",
            fromType->toString(),
            toType->toString());
      }
    }
  }
}

void CastExpr::apply(
    const SelectivityVector& rows,
    const VectorPtr& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& result) {
  LocalSelectivityVector remainingRows(context, rows);

  context.deselectErrors(*remainingRows);

  LocalDecodedVector decoded(context, *input, *remainingRows);
  auto* rawNulls = decoded->nulls(remainingRows.get());

  if (rawNulls) {
    remainingRows->deselectNulls(
        rawNulls, remainingRows->begin(), remainingRows->end());
  }

  VectorPtr localResult;
  if (!remainingRows->hasSelections()) {
    localResult =
        BaseVector::createNullConstant(toType, rows.end(), context.pool());
  } else if (decoded->isIdentityMapping()) {
    applyPeeled(
        *remainingRows,
        *decoded->base(),
        context,
        fromType,
        toType,
        localResult);
  } else {
    withContextSaver([&](ContextSaver& saver) {
      LocalSelectivityVector newRowsHolder(*context.execCtx());

      LocalDecodedVector localDecoded(context);
      std::vector<VectorPtr> peeledVectors;
      auto peeledEncoding = PeeledEncoding::peel(
          {input}, *remainingRows, localDecoded, true, peeledVectors);
      BOLT_CHECK_EQ(peeledVectors.size(), 1);
      if (peeledVectors[0]->isLazy()) {
        peeledVectors[0] =
            peeledVectors[0]->as<LazyVector>()->loadedVectorShared();
      }
      auto newRows =
          peeledEncoding->translateToInnerRows(*remainingRows, newRowsHolder);
      double ratio =
          static_cast<double>(newRows->countSelected()) / newRows->size();
      if (ratio <= 0.01 && fromType->isPrimitiveType()) {
        auto newInputVec =
            BaseVector::create(fromType, remainingRows->end(), context.pool());
        newInputVec->copy(input.get(), *remainingRows, nullptr, false);
        applyPeeled(
            *remainingRows,
            *newInputVec,
            context,
            fromType,
            toType,
            localResult);
      } else {
        // Save context and set the peel.
        context.saveAndReset(saver, *remainingRows);
        context.setPeeledEncoding(peeledEncoding);
        applyPeeled(
            *newRows,
            *peeledVectors[0],
            context,
            fromType,
            toType,
            localResult);

        localResult = context.getPeeledEncoding()->wrap(
            toType, context.pool(), localResult, *remainingRows);
      }
    });
  }
  context.moveOrCopyResult(localResult, *remainingRows, result);
  context.releaseVector(localResult);
  localResult.reset();

  // If there are nulls or rows that encountered errors in the input, add
  // nulls to the result at the same rows.
  BOLT_CHECK_NOT_NULL(result);
  if (rawNulls || context.errors()) {
    EvalCtx::addNulls(
        rows, remainingRows->asRange().bits(), context, toType, result);
  }
}

void CastExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  VectorPtr input;
  inputs_[0]->eval(rows, context, input);
  auto fromType = inputs_[0]->type();
  auto toType = std::const_pointer_cast<const Type>(type_);

  inTopLevel = true;
  if (nullOnFailure()) {
    ScopedVarSetter holder{context.mutableThrowOnError(), false};
    apply(rows, input, context, fromType, toType, result);
  } else {
    apply(rows, input, context, fromType, toType, result);
  }
  // Return 'input' back to the vector pool in 'context' so it can be reused.
  context.releaseVector(input);
}

std::string CastExpr::toString(bool recursive) const {
  std::stringstream out;
  out << name() << "(";
  if (recursive) {
    appendInputs(out);
  } else {
    out << inputs_[0]->toString(false);
  }
  out << " as " << type_->toString() << ")";
  return out.str();
}

std::string CastExpr::toSql(std::vector<VectorPtr>* complexConstants) const {
  std::stringstream out;
  out << name() << "(";
  appendInputsSql(out, complexConstants);
  out << " as ";
  toTypeSql(type_, out);
  out << ")";
  return out.str();
}

CastOperatorPtr CastExpr::getCastOperator(const TypePtr& type) {
  const auto* key = type->name();

  auto it = castOperators_.find(key);
  if (it != castOperators_.end()) {
    return it->second;
  }

  auto castOperator = getCustomTypeCastOperator(key);
  if (castOperator == nullptr) {
    return nullptr;
  }

  castOperators_.emplace(key, castOperator);
  return castOperator;
}

TypePtr CastCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /* argTypes */) {
  BOLT_FAIL("CAST expressions do not support type resolution.");
}

ExprPtr CastCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  BOLT_CHECK_EQ(
      compiledChildren.size(),
      1,
      "CAST statements expect exactly 1 argument, received {}.",
      compiledChildren.size());
  return std::make_shared<CastExpr>(
      type, std::move(compiledChildren[0]), trackCpuUsage, false);
}

TypePtr TryCastCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /* argTypes */) {
  BOLT_FAIL("TRY CAST expressions do not support type resolution.");
}

ExprPtr TryCastCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  BOLT_CHECK_EQ(
      compiledChildren.size(),
      1,
      "TRY CAST statements expect exactly 1 argument, received {}.",
      compiledChildren.size());
  return std::make_shared<CastExpr>(
      type, std::move(compiledChildren[0]), trackCpuUsage, true);
}
} // namespace bytedance::bolt::exec
