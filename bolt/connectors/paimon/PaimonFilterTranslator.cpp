/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/connectors/paimon/PaimonFilterTranslator.h"
#include <paimon/data/decimal.h>
#include <paimon/data/timestamp.h>
#include <paimon/defs.h>
#include <paimon/predicate/compound_predicate.h>
#include <paimon/predicate/function.h>
#include <paimon/predicate/leaf_predicate.h>
#include <paimon/predicate/literal.h>
#include <paimon/predicate/predicate.h>
#include <paimon/predicate/predicate_builder.h>
#include <algorithm>
#include <limits>
#include <optional>
#include "bolt/core/Expressions.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/type/Subfield.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/filter/FilterBase.h"
#include "bolt/type/filter/FilterUtil.h"
#include "bolt/type/filter/FloatingPointRange.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
#include "common/base/Exceptions.h"

namespace bytedance::bolt::connector::paimon {

namespace {

// Helper: check if a string ends with a suffix.
bool endsWith(const std::string& str, const std::string& suffix) {
  if (suffix.size() > str.size()) {
    return false;
  }
  return std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}

/// Build a ConstantTypedExpr containing an ArrayVector from scalar variants.
/// Used to construct the array-literal argument of IN/NOT_IN expressions.
/// @param pool Memory pool for buffer allocation (must outlive the result).
core::TypedExprPtr makeArrayConstant(
    const std::vector<variant>& values,
    const TypePtr& elementType,
    memory::MemoryPool* pool) {
  if (!pool || values.empty()) {
    return nullptr;
  }
  auto kind = elementType->kind();
  // For integer types, always use BIGINT as the element type since paimon
  // literals are int64_t and buildFilterFromCall reads elements as int64_t.
  auto effectiveElementType =
      (kind == TypeKind::TINYINT || kind == TypeKind::SMALLINT ||
       kind == TypeKind::INTEGER || kind == TypeKind::BIGINT)
      ? BIGINT()
      : elementType;
  auto arrayType = ARRAY(effectiveElementType);
  auto numElements = static_cast<vector_size_t>(values.size());

  // Allocate offsets and sizes for a single-row array vector.
  auto offsets = AlignedBuffer::allocate<vector_size_t>(2, pool);
  auto sizes = AlignedBuffer::allocate<vector_size_t>(1, pool);
  offsets->asMutable<vector_size_t>()[0] = 0;
  offsets->asMutable<vector_size_t>()[1] = numElements;
  sizes->asMutable<vector_size_t>()[0] = numElements;

  VectorPtr elements;
  switch (kind) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT: {
      auto flat = BaseVector::create<FlatVector<int64_t>>(
          effectiveElementType, numElements, pool);
      for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].isNull()) {
          flat->setNull(i, true);
        } else {
          flat->set(i, values[i].value<int64_t>());
        }
      }
      elements = std::move(flat);
      break;
    }
    case TypeKind::REAL: {
      auto flat =
          BaseVector::create<FlatVector<float>>(elementType, numElements, pool);
      for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].isNull()) {
          flat->setNull(i, true);
        } else {
          flat->set(i, values[i].value<float>());
        }
      }
      elements = std::move(flat);
      break;
    }
    case TypeKind::DOUBLE: {
      auto flat = BaseVector::create<FlatVector<double>>(
          elementType, numElements, pool);
      for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].isNull()) {
          flat->setNull(i, true);
        } else {
          flat->set(i, values[i].value<double>());
        }
      }
      elements = std::move(flat);
      break;
    }
    case TypeKind::BOOLEAN: {
      auto flat =
          BaseVector::create<FlatVector<bool>>(elementType, numElements, pool);
      for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].isNull()) {
          flat->setNull(i, true);
        } else {
          flat->set(i, values[i].value<bool>());
        }
      }
      elements = std::move(flat);
      break;
    }
    case TypeKind::TIMESTAMP: {
      auto flat = BaseVector::create<FlatVector<bytedance::bolt::Timestamp>>(
          elementType, numElements, pool);
      for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].isNull()) {
          flat->setNull(i, true);
        } else {
          flat->set(i, values[i].value<bytedance::bolt::Timestamp>());
        }
      }
      elements = std::move(flat);
      break;
    }
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      auto flat = BaseVector::create<FlatVector<StringView>>(
          elementType, numElements, pool);
      // Keep temporary strings alive so StringViews don't dangle.
      std::vector<std::string> ownedStrings;
      ownedStrings.reserve(values.size());
      for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].isNull()) {
          flat->setNull(i, true);
        } else {
          ownedStrings.push_back(values[i].value<std::string>());
          flat->set(i, StringView(ownedStrings.back()));
        }
      }
      elements = std::move(flat);
      break;
    }
    default:
      BOLT_UNSUPPORTED(
          "Unsupported element type {} for IN/NOT_IN array constant",
          elementType->toString());
  }

  BOLT_CHECK_NOT_NULL(
      elements, "Failed to create element vector for IN/NOT_IN");

  auto arrVec = std::make_shared<ArrayVector>(
      pool,
      arrayType,
      nullptr /*nulls*/,
      1 /*length*/,
      std::move(offsets),
      std::move(sizes),
      std::move(elements));
  auto wrapped = BaseVector::wrapInConstant(1, 0, arrVec);
  return std::make_shared<core::ConstantTypedExpr>(wrapped);
}

VectorPtr evaluateConstantExpression(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator) {
  if (!evaluator) {
    return nullptr;
  }
  auto exprSet = evaluator->compile(expr);
  if (exprSet->size() != 1 || !exprSet->exprs()[0]->isConstant()) {
    return nullptr;
  }

  RowVector input(
      evaluator->pool(), ROW({}, {}), nullptr, 1, std::vector<VectorPtr>{});
  SelectivityVector rows(1);
  VectorPtr result;
  try {
    evaluator->evaluate(exprSet.get(), rows, input, result);
  } catch (const BoltUserError&) {
    return nullptr;
  }
  return result;
}

std::optional<int64_t> integerValueAt(const VectorPtr& vector) {
  switch (vector->typeKind()) {
    case TypeKind::TINYINT:
      return vector->as<SimpleVector<int8_t>>()->valueAt(0);
    case TypeKind::SMALLINT:
      return vector->as<SimpleVector<int16_t>>()->valueAt(0);
    case TypeKind::INTEGER:
      return vector->as<SimpleVector<int32_t>>()->valueAt(0);
    case TypeKind::BIGINT:
      return vector->as<SimpleVector<int64_t>>()->valueAt(0);
    default:
      return std::nullopt;
  }
}

std::optional<::paimon::Literal> literalFromVector(
    const VectorPtr& vector,
    ::paimon::FieldType fieldType) {
  if (!vector || vector->size() == 0) {
    return std::nullopt;
  }
  if (vector->isNullAt(0)) {
    return ::paimon::Literal(fieldType);
  }

  switch (fieldType) {
    case ::paimon::FieldType::BOOLEAN:
      if (vector->typeKind() != TypeKind::BOOLEAN) {
        return std::nullopt;
      }
      return ::paimon::Literal(vector->as<SimpleVector<bool>>()->valueAt(0));
    case ::paimon::FieldType::TINYINT:
    case ::paimon::FieldType::SMALLINT:
    case ::paimon::FieldType::INT:
    case ::paimon::FieldType::BIGINT: {
      auto v = integerValueAt(vector);
      if (!v) {
        return std::nullopt;
      }
      if (fieldType == ::paimon::FieldType::TINYINT) {
        return ::paimon::Literal(static_cast<int8_t>(*v));
      }
      if (fieldType == ::paimon::FieldType::SMALLINT) {
        return ::paimon::Literal(static_cast<int16_t>(*v));
      }
      if (fieldType == ::paimon::FieldType::INT) {
        return ::paimon::Literal(static_cast<int32_t>(*v));
      }
      return ::paimon::Literal(*v);
    }
    case ::paimon::FieldType::FLOAT: {
      if (vector->typeKind() == TypeKind::REAL) {
        return ::paimon::Literal(vector->as<SimpleVector<float>>()->valueAt(0));
      }
      if (vector->typeKind() == TypeKind::DOUBLE) {
        return ::paimon::Literal(
            static_cast<float>(vector->as<SimpleVector<double>>()->valueAt(0)));
      }
      if (auto v = integerValueAt(vector)) {
        return ::paimon::Literal(static_cast<float>(*v));
      }
      return std::nullopt;
    }
    case ::paimon::FieldType::DOUBLE: {
      if (vector->typeKind() == TypeKind::REAL) {
        return ::paimon::Literal(
            static_cast<double>(vector->as<SimpleVector<float>>()->valueAt(0)));
      }
      if (vector->typeKind() == TypeKind::DOUBLE) {
        return ::paimon::Literal(
            vector->as<SimpleVector<double>>()->valueAt(0));
      }
      if (auto v = integerValueAt(vector)) {
        return ::paimon::Literal(static_cast<double>(*v));
      }
      return std::nullopt;
    }
    case ::paimon::FieldType::STRING:
    case ::paimon::FieldType::BINARY: {
      if (vector->typeKind() != TypeKind::VARCHAR &&
          vector->typeKind() != TypeKind::VARBINARY) {
        return std::nullopt;
      }
      auto sv = vector->as<SimpleVector<StringView>>()->valueAt(0);
      return ::paimon::Literal(
          ::paimon::FieldType::STRING, sv.data(), sv.size());
    }
    case ::paimon::FieldType::TIMESTAMP: {
      if (vector->typeKind() != TypeKind::TIMESTAMP) {
        return std::nullopt;
      }
      auto ts = vector->as<SimpleVector<Timestamp>>()->valueAt(0);
      int64_t totalNanos = (ts.getSeconds() * 1'000'000'000LL) + ts.getNanos();
      int64_t millis = totalNanos / 1'000'000;
      auto nanoOfMillis = static_cast<int32_t>(totalNanos % 1'000'000);
      return ::paimon::Literal(::paimon::Timestamp(millis, nanoOfMillis));
    }
    default:
      return std::nullopt;
  }
}

} // namespace

// ===========================================================================
// Operator name normalization
// ===========================================================================

std::string PaimonFilterTranslator::normalizeOpName(const std::string& opName) {
  // SparkSQL long-form names -> canonical short forms.
  // These are the names registered in RegisterComparison.cpp for the
  // SparkSQL/gluten expression parser.
  if (opName == "equalto" || opName == "=") {
    return "eq";
  }
  if (opName == "lessthan" || opName == "<") {
    return "lt";
  }
  if (opName == "greaterthan" || opName == ">") {
    return "gt";
  }
  if (opName == "lessthanorequal" || opName == "<=") {
    return "lte";
  }
  if (opName == "greaterthanorequal" || opName == ">=") {
    return "gte";
  }
  if (opName == "isnull") {
    return "is_null";
  }
  if (opName == "isnotnull") {
    return "is_not_null";
  }
  return opName;
}

ToPaimonPredicateResult PaimonFilterTranslator::translate(
    const core::TypedExprPtr& expr,
    const RowTypePtr& rowType,
    core::ExpressionEvaluator* evaluator) {
  auto fail = [](std::string reason) -> ToPaimonPredicateResult {
    LOG(WARNING) << "translate (with rowType): " << reason;
    return {nullptr, std::move(reason)};
  };

  if (!expr) {
    return fail("expr is null");
  }

  const auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  if (!call) {
    return fail("expr is not a CallTypedExpr");
  }

  return translateCall(*call, rowType, evaluator);
}

ToPaimonPredicateResult PaimonFilterTranslator::translateCall(
    const core::CallTypedExpr& call,
    const RowTypePtr& rowType,
    core::ExpressionEvaluator* evaluator) {
  auto fail = [](std::string reason) -> ToPaimonPredicateResult {
    LOG(WARNING) << "translateCall (with rowType): " << reason;
    return {nullptr, std::move(reason)};
  };
  auto ok =
      [](std::shared_ptr<::paimon::Predicate> pred) -> ToPaimonPredicateResult {
    return {std::move(pred), ""};
  };

  auto translateLeafCall =
      [&](const core::CallTypedExpr& leafCall) -> ToPaimonPredicateResult {
    const auto opName = normalizeOpName(leafCall.name());
    const auto& inputs = leafCall.inputs();

    // Leaf predicates require at least one operand (the field).
    if (inputs.empty()) {
      return fail("leaf op has no inputs");
    }

    auto fieldInfo = extractFieldInfo(inputs[0], rowType);
    if (!fieldInfo) {
      return fail(fmt::format(
          "could not extract field info for op {}, input[0]={}",
          opName,
          inputs[0]->toString()));
    }

    const auto& fieldName = fieldInfo->name;
    const auto fieldIndex = fieldInfo->index;
    const auto& fieldType = fieldInfo->fieldType;

    // Equality: eq(field, constant)
    if (opName == "eq") {
      if (inputs.size() < 2) {
        return fail("eq requires 2 inputs");
      }
      auto lit = extractLiteral(inputs[1], fieldType, evaluator);
      if (!lit) {
        return fail("eq: could not extract literal");
      }
      return ok(::paimon::PredicateBuilder::Equal(
          fieldIndex, fieldName, fieldType, *lit));
    }

    // Not equal: neq(field, constant)
    if (opName == "neq") {
      if (inputs.size() < 2) {
        return fail("neq requires 2 inputs");
      }
      auto lit = extractLiteral(inputs[1], fieldType, evaluator);
      if (!lit) {
        return fail("neq: could not extract literal");
      }
      return ok(::paimon::PredicateBuilder::NotEqual(
          fieldIndex, fieldName, fieldType, *lit));
    }

    // Less than: lt(field, constant)
    if (opName == "lt") {
      if (inputs.size() < 2) {
        return fail("lt requires 2 inputs");
      }
      auto lit = extractLiteral(inputs[1], fieldType, evaluator);
      if (!lit) {
        return fail("lt: could not extract literal");
      }
      return ok(::paimon::PredicateBuilder::LessThan(
          fieldIndex, fieldName, fieldType, *lit));
    }

    // Less or equal: lte(field, constant)
    if (opName == "lte") {
      if (inputs.size() < 2) {
        return fail("lte requires 2 inputs");
      }
      auto lit = extractLiteral(inputs[1], fieldType, evaluator);
      if (!lit) {
        return fail("lte: could not extract literal");
      }
      return ok(::paimon::PredicateBuilder::LessOrEqual(
          fieldIndex, fieldName, fieldType, *lit));
    }

    // Greater than: gt(field, constant)
    if (opName == "gt") {
      if (inputs.size() < 2) {
        return fail("gt requires 2 inputs");
      }
      auto lit = extractLiteral(inputs[1], fieldType, evaluator);
      if (!lit) {
        return fail("gt: could not extract literal");
      }
      return ok(::paimon::PredicateBuilder::GreaterThan(
          fieldIndex, fieldName, fieldType, *lit));
    }

    // Greater or equal: gte(field, constant)
    if (opName == "gte") {
      if (inputs.size() < 2) {
        return fail("gte requires 2 inputs");
      }
      auto lit = extractLiteral(inputs[1], fieldType, evaluator);
      if (!lit) {
        return fail("gte: could not extract literal");
      }
      return ok(::paimon::PredicateBuilder::GreaterOrEqual(
          fieldIndex, fieldName, fieldType, *lit));
    }

    // Between: between(field, lower, upper)
    if (opName == "between") {
      if (inputs.size() < 3) {
        return fail("between requires 3 inputs");
      }
      auto low = extractLiteral(inputs[1], fieldType, evaluator);
      auto high = extractLiteral(inputs[2], fieldType, evaluator);
      if (!low || !high) {
        return fail("between: could not extract literals");
      }
      return ok(::paimon::PredicateBuilder::Between(
          fieldIndex, fieldName, fieldType, *low, *high));
    }

    // In: in(field, array_constant) / not_in(field, array_constant)
    if (opName == "in" || opName == "not_in") {
      if (inputs.size() < 2) {
        return fail("in/not_in requires 2 inputs");
      }
      bool negated = (opName == "not_in");
      auto result = extractInListLiterals(inputs[1], fieldType);
      if (!result.has_value()) {
        return fail("in/not_in: could not extract IN-list literals");
      }
      auto literals = std::move(result.value());
      if (negated) {
        return ok(::paimon::PredicateBuilder::NotIn(
            fieldIndex, fieldName, fieldType, literals));
      }
      return ok(::paimon::PredicateBuilder::In(
          fieldIndex, fieldName, fieldType, literals));
    }

    // Like: like(string_field, pattern). Three-arg LIKE with an explicit escape
    // character is kept as a remaining filter to avoid changing escape
    // semantics.
    if (opName == "like") {
      if (inputs.size() != 2) {
        return fail("like requires 2 inputs");
      }
      if (fieldType != ::paimon::FieldType::STRING &&
          fieldType != ::paimon::FieldType::BINARY) {
        return fail("like only supports string/binary fields");
      }
      auto lit = extractLiteral(inputs[1], fieldType, evaluator);
      if (!lit) {
        return fail("like: could not extract pattern literal");
      }
      auto result = ::paimon::PredicateBuilder::Like(
          fieldIndex, fieldName, fieldType, *lit);
      if (!result.ok()) {
        return fail("like: " + result.status().ToString());
      }
      return ok(std::move(result).value());
    }

    // Is null: is_null(field)
    if (opName == "is_null") {
      return ok(
          ::paimon::PredicateBuilder::IsNull(fieldIndex, fieldName, fieldType));
    }

    // Is not null: is_not_null(field)
    if (opName == "is_not_null") {
      return ok(::paimon::PredicateBuilder::IsNotNull(
          fieldIndex, fieldName, fieldType));
    }

    return fail(fmt::format("unsupported opName={}", opName));
  };

  auto translateNonCompoundCall =
      [&](const core::CallTypedExpr& node) -> ToPaimonPredicateResult {
    const auto opName = normalizeOpName(node.name());
    const auto& inputs = node.inputs();

    // Handle NOT: translate inner with negation applied per-operator.
    if (opName == "not") {
      if (inputs.empty()) {
        return fail("NOT has no inputs");
      }
      const auto* innerCall =
          dynamic_cast<const core::CallTypedExpr*>(inputs[0].get());
      if (!innerCall) {
        return fail("NOT inner is not a CallTypedExpr");
      }
      auto negatedOp = applyNegation(normalizeOpName(innerCall->name()));
      if (negatedOp.has_value()) {
        core::CallTypedExpr negatedCall(
            innerCall->type(), innerCall->inputs(), negatedOp.value());
        return translateLeafCall(negatedCall);
      }
      if (normalizeOpName(innerCall->name()) == "is_null") {
        if (innerCall->inputs().empty()) {
          return fail("NOT is_null has no inputs");
        }
        auto fieldInfo = extractFieldInfo(innerCall->inputs()[0], rowType);
        if (!fieldInfo) {
          return fail("NOT is_null: could not extract field info");
        }
        return ok(::paimon::PredicateBuilder::IsNotNull(
            fieldInfo->index, fieldInfo->name, fieldInfo->fieldType));
      }
      if (normalizeOpName(innerCall->name()) == "is_not_null") {
        if (innerCall->inputs().empty()) {
          return fail("NOT is_not_null has no inputs");
        }
        auto fieldInfo = extractFieldInfo(innerCall->inputs()[0], rowType);
        if (!fieldInfo) {
          return fail("NOT is_not_null: could not extract field info");
        }
        return ok(::paimon::PredicateBuilder::IsNull(
            fieldInfo->index, fieldInfo->name, fieldInfo->fieldType));
      }
      return fail("unsupported negation target");
    }

    return translateLeafCall(node);
  };

  struct Frame {
    const core::CallTypedExpr* node;
    bool combine;
  };

  std::vector<Frame> pending{{&call, false}};
  std::vector<std::shared_ptr<::paimon::Predicate>> results;

  while (!pending.empty()) {
    auto frame = pending.back();
    pending.pop_back();

    const auto opName = normalizeOpName(frame.node->name());
    const auto& inputs = frame.node->inputs();
    const bool isCompound = opName == "and" || opName == "or";

    if (!isCompound) {
      auto leaf = translateNonCompoundCall(*frame.node);
      if (!leaf.ok()) {
        return leaf;
      }
      results.push_back(std::move(leaf.value));
      continue;
    }

    if (!frame.combine) {
      pending.push_back({frame.node, true});
      for (size_t i = inputs.size(); i > 0; --i) {
        const auto& input = inputs[i - 1];
        const auto* childCall =
            dynamic_cast<const core::CallTypedExpr*>(input.get());
        if (!childCall) {
          return fail(fmt::format(
              "{} child failed: expr is not a CallTypedExpr",
              opName == "and" ? "AND" : "OR"));
        }
        pending.push_back({childCall, false});
      }
      continue;
    }

    if (results.size() < inputs.size()) {
      return fail("compound predicate result stack underflow");
    }
    const auto firstChild = results.size() - inputs.size();
    std::vector<std::shared_ptr<::paimon::Predicate>> children;
    children.reserve(inputs.size());
    for (size_t i = firstChild; i < results.size(); ++i) {
      children.push_back(std::move(results[i]));
    }
    results.resize(firstChild);

    auto result = opName == "and" ? ::paimon::PredicateBuilder::And(children)
                                  : ::paimon::PredicateBuilder::Or(children);
    if (!result.ok()) {
      return fail(
          opName == "and" ? "PredicateBuilder::And failed"
                          : "PredicateBuilder::Or failed");
    }
    results.push_back(std::move(result).value());
  }

  if (results.size() != 1) {
    return fail("predicate translation produced an invalid result stack");
  }
  return ok(std::move(results.front()));
}

std::optional<std::vector<::paimon::Literal>>
PaimonFilterTranslator::extractInListLiterals(
    const core::TypedExprPtr& expr,
    ::paimon::FieldType fieldType) {
  const auto* constant =
      dynamic_cast<const core::ConstantTypedExpr*>(expr.get());
  if (!constant || !constant->hasValueVector()) {
    return std::nullopt;
  }

  const auto& vec = constant->valueVector();
  if (!vec->type()->isArray()) {
    return std::nullopt;
  }

  // Follow Hive's makeInFilter pattern (ExprToSubfieldFilter.cpp):
  // ArrayVector stores flattened elements with offset/size per array.
  // The value vector may be wrapped in a ConstantVector (from
  // wrapInConstant), so unwrap first.
  const BaseVector* rawVec = vec.get();
  if (vec->isConstantEncoding() && vec->wrappedVector()) {
    rawVec = vec->wrappedVector();
  }
  const auto* arrayVec = rawVec->as<ArrayVector>();
  if (!arrayVec) {
    return std::nullopt;
  }
  // For a simple constant array expression, index is always 0.
  constexpr vector_size_t kIndex = 0;
  auto offset = arrayVec->offsetAt(kIndex);
  auto size = arrayVec->sizeAt(kIndex);
  auto elements = arrayVec->elements();
  if (!elements) {
    return std::nullopt;
  }

  std::vector<::paimon::Literal> literals;
  literals.reserve(size);

  switch (fieldType) {
    case ::paimon::FieldType::TINYINT:
    case ::paimon::FieldType::SMALLINT:
    case ::paimon::FieldType::INT:
    case ::paimon::FieldType::BIGINT: {
      const auto* intVec =
          dynamic_cast<const SimpleVector<int64_t>*>(elements.get());
      if (!intVec) {
        return std::nullopt;
      }
      for (vector_size_t i = 0; i < size; ++i) {
        if (intVec->isNullAt(offset + i)) {
          literals.emplace_back(fieldType);
        } else {
          literals.emplace_back(intVec->valueAt(offset + i));
        }
      }
      break;
    }
    case ::paimon::FieldType::STRING:
    case ::paimon::FieldType::BINARY: {
      const auto* strVec =
          dynamic_cast<const SimpleVector<StringView>*>(elements.get());
      if (!strVec) {
        return std::nullopt;
      }
      for (vector_size_t i = 0; i < size; ++i) {
        if (strVec->isNullAt(offset + i)) {
          literals.emplace_back(fieldType);
        } else {
          auto sv = strVec->valueAt(offset + i);
          literals.emplace_back(
              ::paimon::FieldType::STRING, sv.data(), sv.size());
        }
      }
      break;
    }
    case ::paimon::FieldType::FLOAT: {
      const auto* floatVec =
          dynamic_cast<const SimpleVector<float>*>(elements.get());
      if (!floatVec) {
        return std::nullopt;
      }
      for (vector_size_t i = 0; i < size; ++i) {
        if (floatVec->isNullAt(offset + i)) {
          literals.emplace_back(fieldType);
        } else {
          literals.emplace_back(floatVec->valueAt(offset + i));
        }
      }
      break;
    }
    case ::paimon::FieldType::DOUBLE: {
      const auto* doubleVec =
          dynamic_cast<const SimpleVector<double>*>(elements.get());
      if (!doubleVec) {
        return std::nullopt;
      }
      for (vector_size_t i = 0; i < size; ++i) {
        if (doubleVec->isNullAt(offset + i)) {
          literals.emplace_back(fieldType);
        } else {
          literals.emplace_back(doubleVec->valueAt(offset + i));
        }
      }
      break;
    }
    case ::paimon::FieldType::TIMESTAMP: {
      const auto* tsVec =
          dynamic_cast<const SimpleVector<bytedance::bolt::Timestamp>*>(
              elements.get());
      if (!tsVec) {
        return std::nullopt;
      }
      for (vector_size_t i = 0; i < size; ++i) {
        if (tsVec->isNullAt(offset + i)) {
          literals.emplace_back(fieldType);
        } else {
          auto boltTs = tsVec->valueAt(offset + i);
          int64_t totalNanos =
              (boltTs.getSeconds() * 1'000'000'000LL) + boltTs.getNanos();
          int64_t millis = totalNanos / 1'000'000;
          int32_t nanoOfMillis = static_cast<int32_t>(totalNanos % 1'000'000);
          literals.emplace_back(::paimon::Timestamp(millis, nanoOfMillis));
        }
      }
      break;
    }
    case ::paimon::FieldType::BOOLEAN: {
      const auto* boolVec =
          dynamic_cast<const SimpleVector<bool>*>(elements.get());
      if (!boolVec) {
        return std::nullopt;
      }
      for (vector_size_t i = 0; i < size; ++i) {
        if (boolVec->isNullAt(offset + i)) {
          literals.emplace_back(fieldType);
        } else {
          literals.emplace_back(boolVec->valueAt(offset + i));
        }
      }
      break;
    }
    default:
      // DECIMAL, DATE, etc. not yet supported for IN lists.
      return std::nullopt;
  }
  return literals;
}

std::optional<PaimonFilterTranslator::FieldInfo>
PaimonFilterTranslator::extractFieldInfo(const core::TypedExprPtr& expr) {
  // Unwrap CastTypedExpr — query planners often wrap field refs in casts.
  const core::ITypedExpr* unwrapped = expr.get();
  while (const auto* cast =
             dynamic_cast<const core::CastTypedExpr*>(unwrapped)) {
    unwrapped = cast->inputs()[0].get();
  }

  if (const auto* fieldAccess =
          dynamic_cast<const core::FieldAccessTypedExpr*>(unwrapped)) {
    FieldInfo info;
    info.name = fieldAccess->name();
    auto fieldType = toPaimonFieldType(expr->type());
    if (!fieldType) {
      return std::nullopt;
    }
    info.fieldType = fieldType.value();
    // Index will be filled in later by the caller if needed; use -1 as
    // sentinel.
    info.index = -1;
    return info;
  }
  if (const auto* deref =
          dynamic_cast<const core::DereferenceTypedExpr*>(unwrapped)) {
    FieldInfo info;
    info.name = deref->name();
    auto fieldType = toPaimonFieldType(expr->type());
    if (!fieldType) {
      return std::nullopt;
    }
    info.fieldType = fieldType.value();
    info.index = -1;
    return info;
  }
  return std::nullopt;
}

std::optional<PaimonFilterTranslator::FieldInfo>
PaimonFilterTranslator::extractFieldInfo(
    const core::TypedExprPtr& expr,
    const RowTypePtr& rowType) {
  auto info = extractFieldInfo(expr);
  if (!info) {
    return std::nullopt;
  }
  // Resolve the field index and type from the row type's column names.
  // The rowType carries the *actual* paimon schema types (e.g., BIGINT),
  // while the expression may carry a narrower planner type (e.g., INTEGER).
  if (rowType) {
    for (size_t i = 0; i < rowType->size(); ++i) {
      if (rowType->nameOf(i) == info->name) {
        info->index = static_cast<int32_t>(i);
        auto ft = toPaimonFieldType(rowType->childAt(i));
        if (!ft) {
          return std::nullopt;
        }
        info->fieldType = std::move(ft.value());
        break;
      }
    }
  }
  return info;
}

std::optional<::paimon::Literal> PaimonFilterTranslator::extractLiteral(
    const core::TypedExprPtr& expr,
    ::paimon::FieldType fieldType,
    core::ExpressionEvaluator* evaluator) {
  // Unwrap CastTypedExpr — query planners often wrap constants in casts.
  auto unwrapped = expr;
  while (const auto* cast =
             dynamic_cast<const core::CastTypedExpr*>(unwrapped.get())) {
    unwrapped = cast->inputs()[0];
  }

  const auto* constant =
      dynamic_cast<const core::ConstantTypedExpr*>(unwrapped.get());
  if (!constant || constant->hasValueVector()) {
    return literalFromVector(
        evaluateConstantExpression(unwrapped, evaluator), fieldType);
  }

  const auto& value = constant->value();
  if (value.isNull()) {
    return ::paimon::Literal(fieldType); // Null literal.
  }

  // Extract using the variant's *actual* kind (after CastTypedExpr unwrapping
  // the inner constant may have a narrower type than the target fieldType).
  // Coerce to the target paimon type after extraction.
  switch (fieldType) {
    case ::paimon::FieldType::BOOLEAN:
      return ::paimon::Literal(value.value<bool>());
    case ::paimon::FieldType::TINYINT:
    case ::paimon::FieldType::SMALLINT:
    case ::paimon::FieldType::INT:
    case ::paimon::FieldType::BIGINT: {
      // Extract using the variant's actual kind (safe widening),
      // then narrow to match the target field type.
      auto v = [&, &val = value]() -> int64_t {
        switch (val.kind()) {
          case TypeKind::TINYINT:
            return static_cast<int64_t>(val.value<int8_t>());
          case TypeKind::SMALLINT:
            return static_cast<int64_t>(val.value<int16_t>());
          case TypeKind::INTEGER:
            return static_cast<int64_t>(val.value<int32_t>());
          case TypeKind::BIGINT:
            return val.value<int64_t>();
          default:
            BOLT_FAIL("unexpected integer kind");
        }
      }();
      if (fieldType == ::paimon::FieldType::TINYINT)
        return ::paimon::Literal(static_cast<int8_t>(v));
      if (fieldType == ::paimon::FieldType::SMALLINT)
        return ::paimon::Literal(static_cast<int16_t>(v));
      if (fieldType == ::paimon::FieldType::INT)
        return ::paimon::Literal(static_cast<int32_t>(v));
      return ::paimon::Literal(v);
    }
    case ::paimon::FieldType::FLOAT:
    case ::paimon::FieldType::DOUBLE: {
      double doubleValue = 0.0;
      switch (value.kind()) {
        case TypeKind::REAL:
          doubleValue = static_cast<double>(value.value<float>());
          break;
        case TypeKind::DOUBLE:
          doubleValue = value.value<double>();
          break;
        case TypeKind::INTEGER:
          doubleValue = static_cast<double>(value.value<int32_t>());
          break;
        case TypeKind::BIGINT:
          doubleValue = static_cast<double>(value.value<int64_t>());
          break;
        default:
          return std::nullopt;
      }
      if (fieldType == ::paimon::FieldType::FLOAT) {
        return ::paimon::Literal(static_cast<float>(doubleValue));
      }
      return ::paimon::Literal(doubleValue);
    }
    case ::paimon::FieldType::STRING:
    case ::paimon::FieldType::BINARY: {
      auto sv = value.value<StringView>();
      return ::paimon::Literal(
          ::paimon::FieldType::STRING, sv.data(), sv.size());
    }
    case ::paimon::FieldType::TIMESTAMP: {
      // Bolt stores Timestamp as (seconds, nanoseconds).
      // Paimon stores as (millis_since_epoch, nano_of_millis).
      // Preserve full nanosecond precision via total nanoseconds.
      auto ts = value.value<bytedance::bolt::Timestamp>();
      int64_t totalNanos = (ts.getSeconds() * 1'000'000'000LL) + ts.getNanos();
      int64_t millis = totalNanos / 1'000'000;
      int32_t nanoOfMillis = static_cast<int32_t>(totalNanos % 1'000'000);
      auto paimonTs = ::paimon::Timestamp(millis, nanoOfMillis);
      return ::paimon::Literal(paimonTs);
    }
    case ::paimon::FieldType::DECIMAL: {
      // Bolt stores short decimals as int64, long decimals as int128.
      // paimon::Literal accepts paimon::Decimal which holds an int128.
      const auto* shortDec =
          dynamic_cast<const ShortDecimalType*>(unwrapped->type().get());
      const auto* longDec =
          dynamic_cast<const LongDecimalType*>(unwrapped->type().get());
      if (!shortDec && !longDec) {
        return std::nullopt;
      }
      int32_t precision =
          shortDec ? shortDec->precision() : longDec->precision();
      int32_t scale = shortDec ? shortDec->scale() : longDec->scale();
      ::paimon::Decimal::int128_t unscaled = 0;
      switch (value.kind()) {
        case TypeKind::BIGINT:
          unscaled = value.value<int64_t>();
          break;
        case TypeKind::HUGEINT:
          unscaled = value.value<__int128_t>();
          break;
        default:
          return std::nullopt;
      }
      auto paimonDec = ::paimon::Decimal(precision, scale, unscaled);
      return ::paimon::Literal(paimonDec);
    }
    default:
      return std::nullopt;
  }
}

std::optional<::paimon::FieldType> PaimonFilterTranslator::toPaimonFieldType(
    const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return ::paimon::FieldType::BOOLEAN;
    case TypeKind::TINYINT:
      return ::paimon::FieldType::TINYINT;
    case TypeKind::SMALLINT:
      return ::paimon::FieldType::SMALLINT;
    case TypeKind::INTEGER:
      return ::paimon::FieldType::INT;
    case TypeKind::BIGINT:
      // Short decimal (precision <= 18) is stored as int64.
      if (type->isShortDecimal()) {
        return ::paimon::FieldType::DECIMAL;
      }
      return ::paimon::FieldType::BIGINT;
    case TypeKind::HUGEINT:
      // Long decimal (precision > 18) is stored as int128.
      if (type->isLongDecimal()) {
        return ::paimon::FieldType::DECIMAL;
      }
      return std::nullopt;
    case TypeKind::REAL:
      return ::paimon::FieldType::FLOAT;
    case TypeKind::DOUBLE:
      return ::paimon::FieldType::DOUBLE;
    case TypeKind::VARCHAR:
      return ::paimon::FieldType::STRING;
    case TypeKind::VARBINARY:
      return ::paimon::FieldType::BINARY;
    case TypeKind::TIMESTAMP:
      return ::paimon::FieldType::TIMESTAMP;
    case TypeKind::ARRAY:
      return ::paimon::FieldType::ARRAY;
    case TypeKind::MAP:
      return ::paimon::FieldType::MAP;
    case TypeKind::ROW:
      return ::paimon::FieldType::STRUCT;
    case TypeKind::UNKNOWN:
      return ::paimon::FieldType::UNKNOWN;
    case TypeKind::FUNCTION:
    case TypeKind::OPAQUE:
    case TypeKind::VARIANT:
    case TypeKind::INVALID:
    default:
      return std::nullopt;
  }
}

std::optional<std::string> PaimonFilterTranslator::applyNegation(
    const std::string& opName) {
  // Map each operator to its logical negation counterpart.
  // These are symmetric swaps that paimon supports natively.
  if (opName == "eq") {
    return std::string("neq");
  }
  if (opName == "neq") {
    return std::string("eq");
  }
  if (opName == "lt") {
    return std::string("gte");
  }
  if (opName == "lte") {
    return std::string("gt");
  }
  if (opName == "gt") {
    return std::string("lte");
  }
  if (opName == "gte") {
    return std::string("lt");
  }
  // is_null / is_not_null are handled specially in translateCall since they
  // have different arity.
  return std::nullopt;
}

// ===========================================================================
// Direction 2a: paimon::Predicate → TypedExprPtr
// ===========================================================================

TypePtr PaimonFilterTranslator::fromPaimonFieldType(
    ::paimon::FieldType fieldType) {
  switch (fieldType) {
    case ::paimon::FieldType::BOOLEAN:
      return BOOLEAN();
    case ::paimon::FieldType::TINYINT:
      return TINYINT();
    case ::paimon::FieldType::SMALLINT:
      return SMALLINT();
    case ::paimon::FieldType::INT:
      return INTEGER();
    case ::paimon::FieldType::BIGINT:
      return BIGINT();
    case ::paimon::FieldType::FLOAT:
      return REAL();
    case ::paimon::FieldType::DOUBLE:
      return DOUBLE();
    case ::paimon::FieldType::STRING:
    case ::paimon::FieldType::BINARY:
      return VARCHAR();
    case ::paimon::FieldType::TIMESTAMP:
      return TIMESTAMP();
    case ::paimon::FieldType::DECIMAL:
      // Default: max precision/scale. The actual precision/scale comes from
      // the file schema during filter application.
      return DECIMAL(38, 18);
    default:
      return VARCHAR();
  }
}

std::optional<std::string> PaimonFilterTranslator::functionToOpName(
    int functionType) {
  switch (static_cast<::paimon::Function::Type>(functionType)) {
    case ::paimon::Function::Type::EQUAL:
      return std::string("eq");
    case ::paimon::Function::Type::NOT_EQUAL:
      return std::string("neq");
    case ::paimon::Function::Type::GREATER_THAN:
      return std::string("gt");
    case ::paimon::Function::Type::GREATER_OR_EQUAL:
      return std::string("gte");
    case ::paimon::Function::Type::LESS_THAN:
      return std::string("lt");
    case ::paimon::Function::Type::LESS_OR_EQUAL:
      return std::string("lte");
    case ::paimon::Function::Type::IS_NULL:
      return std::string("is_null");
    case ::paimon::Function::Type::IS_NOT_NULL:
      return std::string("is_not_null");
    case ::paimon::Function::Type::IN:
      return std::string("in");
    case ::paimon::Function::Type::NOT_IN:
      return std::string("not_in");
    case ::paimon::Function::Type::LIKE:
      return std::string("like");
    default:
      return std::nullopt;
  }
}

core::TypedExprPtr PaimonFilterTranslator::literalToConstantExpr(
    const ::paimon::Literal& literal,
    ::paimon::FieldType fieldType) {
  auto type = fromPaimonFieldType(fieldType);
  if (literal.IsNull()) {
    return std::make_shared<core::ConstantTypedExpr>(
        type, variant::null(type->kind()));
  }

  switch (fieldType) {
    case ::paimon::FieldType::BOOLEAN:
      return std::make_shared<core::ConstantTypedExpr>(
          type, variant(literal.GetValue<bool>()));
    case ::paimon::FieldType::TINYINT:
      return std::make_shared<core::ConstantTypedExpr>(
          type, variant(static_cast<int64_t>(literal.GetValue<int8_t>())));
    case ::paimon::FieldType::SMALLINT:
      return std::make_shared<core::ConstantTypedExpr>(
          type, variant(static_cast<int64_t>(literal.GetValue<int16_t>())));
    case ::paimon::FieldType::INT:
      return std::make_shared<core::ConstantTypedExpr>(
          type, variant(static_cast<int64_t>(literal.GetValue<int32_t>())));
    case ::paimon::FieldType::BIGINT:
      return std::make_shared<core::ConstantTypedExpr>(
          type, variant(literal.GetValue<int64_t>()));
    case ::paimon::FieldType::FLOAT:
      return std::make_shared<core::ConstantTypedExpr>(
          type, variant(literal.GetValue<float>()));
    case ::paimon::FieldType::DOUBLE:
      return std::make_shared<core::ConstantTypedExpr>(
          type, variant(literal.GetValue<double>()));
    case ::paimon::FieldType::STRING:
    case ::paimon::FieldType::BINARY: {
      auto str = literal.GetValue<std::string>();
      return std::make_shared<core::ConstantTypedExpr>(
          type, variant(std::move(str)));
    }
    case ::paimon::FieldType::TIMESTAMP: {
      auto ts = literal.GetValue<::paimon::Timestamp>();
      const int64_t millis = ts.GetMillisecond();
      const int32_t nanosOfMillisecond = ts.GetNanoOfMillisecond();
      auto boltTs = Timestamp::fromMillis(millis);
      uint64_t nanos =
          boltTs.getNanos() + static_cast<uint64_t>(nanosOfMillisecond);
      auto seconds = boltTs.getSeconds();
      if (nanos >= 1'000'000'000) {
        nanos -= 1'000'000'000;
        ++seconds;
      }
      return std::make_shared<core::ConstantTypedExpr>(
          type, variant(Timestamp(seconds, nanos)));
    }
    case ::paimon::FieldType::DECIMAL: {
      auto dec = literal.GetValue<::paimon::Decimal>();
      // Extract unscaled value; use int64 for short decimals,
      // int128 for long decimals.
      if (dec.IsCompact()) {
        return std::make_shared<core::ConstantTypedExpr>(
            bytedance::bolt::ShortDecimalType::create(),
            variant(dec.ToUnscaledLong()));
      }
      // Long decimal: store as HUGEINT (__int128_t).
      auto val = dec.Value();
      return std::make_shared<core::ConstantTypedExpr>(
          type, variant(*reinterpret_cast<__int128_t*>(&val)));
    }
    default:
      return nullptr;
  }
}

PaimonFilterTranslator::ToTypedExprResult PaimonFilterTranslator::toTypedExpr(
    const std::shared_ptr<::paimon::Predicate>& predicate,
    memory::MemoryPool* pool) {
  auto fail = [](std::string reason) -> ToTypedExprResult {
    LOG(WARNING) << "toTypedExpr: conversion failed — " << reason;
    return {nullptr, std::move(reason)};
  };

  if (!predicate) {
    return fail("predicate is null");
  }

  // Check if it's a leaf predicate.
  auto leaf = std::dynamic_pointer_cast<::paimon::LeafPredicate>(predicate);
  if (leaf) {
    const auto& function = leaf->GetFunction();
    const auto functionType = function.GetType();
    const auto fieldType = leaf->GetFieldType();
    const auto& fieldName = leaf->FieldName();
    const auto& literals = leaf->Literals();

    auto opName = functionToOpName(static_cast<int>(functionType));
    if (!opName.has_value()) {
      return fail(fmt::format(
          "unsupported function type {} for field '{}'",
          static_cast<int>(functionType),
          fieldName));
    }

    auto fieldTypePtr = fromPaimonFieldType(fieldType);
    auto fieldExpr =
        std::make_shared<core::FieldAccessTypedExpr>(fieldTypePtr, fieldName);

    // Null checks: is_null / is_not_null take only the field.
    if (functionType == ::paimon::Function::Type::IS_NULL ||
        functionType == ::paimon::Function::Type::IS_NOT_NULL) {
      return {
          std::make_shared<core::CallTypedExpr>(
              BOOLEAN(),
              std::vector<core::TypedExprPtr>{fieldExpr},
              opName.value()),
          ""};
    }

    // All other operators require at least one literal.
    if (literals.empty()) {
      return fail(
          fmt::format("{} on '{}' has no literals", opName.value(), fieldName));
    }

    // IN / NOT_IN: requires a memory pool for array constant allocation.
    if (functionType == ::paimon::Function::Type::IN ||
        functionType == ::paimon::Function::Type::NOT_IN) {
      if (!pool) {
        return fail(fmt::format(
            "{} on '{}' requires a memory pool for array constant "
            "construction",
            opName.value(),
            fieldName));
      }
      auto elementType = fromPaimonFieldType(fieldType);
      std::vector<variant> values;
      values.reserve(literals.size());
      for (const auto& lit : literals) {
        auto constExpr = literalToConstantExpr(lit, fieldType);
        if (!constExpr) {
          return fail(fmt::format(
              "{} on '{}': failed to convert literal to constant",
              opName.value(),
              fieldName));
        }
        const auto* concrete =
            dynamic_cast<const core::ConstantTypedExpr*>(constExpr.get());
        if (!concrete) {
          return fail(fmt::format(
              "{} on '{}': expected ConstantTypedExpr, got {}",
              opName.value(),
              fieldName,
              constExpr->type()->toString()));
        }
        values.push_back(concrete->value());
      }
      if (values.empty()) {
        return fail(fmt::format(
            "{} on '{}': all literals are null", opName.value(), fieldName));
      }
      auto constExpr = makeArrayConstant(values, elementType, pool);
      if (!constExpr) {
        return fail(fmt::format(
            "{} on '{}': failed to build array constant of type {}",
            opName.value(),
            fieldName,
            elementType->toString()));
      }
      std::string inOp =
          (functionType == ::paimon::Function::Type::IN) ? "in" : "not_in";
      return {
          std::make_shared<core::CallTypedExpr>(
              BOOLEAN(),
              std::vector<core::TypedExprPtr>{fieldExpr, constExpr},
              inOp),
          ""};
    }

    // Binary comparison: eq, neq, lt, lte, gt, gte — use first literal only.
    auto constExpr = literalToConstantExpr(literals.front(), fieldType);
    if (!constExpr) {
      return fail(fmt::format(
          "{} on '{}': failed to convert literal to constant",
          opName.value(),
          fieldName));
    }
    return {
        std::make_shared<core::CallTypedExpr>(
            BOOLEAN(),
            std::vector<core::TypedExprPtr>{fieldExpr, constExpr},
            opName.value()),
        ""};
  }

  // Compound predicate (AND/OR).
  auto compound =
      std::dynamic_pointer_cast<::paimon::CompoundPredicate>(predicate);
  if (!compound) {
    return fail("predicate is neither LeafPredicate nor CompoundPredicate");
  }

  const auto functionType = compound->GetFunction().GetType();
  std::string combineOp;
  if (functionType == ::paimon::Function::Type::AND) {
    combineOp = "and";
  } else if (functionType == ::paimon::Function::Type::OR) {
    combineOp = "or";
  } else {
    return fail(fmt::format(
        "unsupported compound function type {}",
        static_cast<int>(functionType)));
  }

  const auto& children = compound->Children();
  if (children.empty()) {
    return fail("compound predicate has no children");
  }

  std::vector<core::TypedExprPtr> exprChildren;
  exprChildren.reserve(children.size());
  for (size_t i = 0; i < children.size(); ++i) {
    auto childResult = toTypedExpr(children[i], pool);
    if (!childResult.ok()) {
      return fail(
          fmt::format("{} child {}: {}", combineOp, i, childResult.reason));
    }
    exprChildren.push_back(std::move(childResult.value));
  }

  if (exprChildren.size() == 1) {
    return {exprChildren[0], ""};
  }

  auto result = exprChildren[0];
  for (size_t i = 1; i < exprChildren.size(); ++i) {
    result = std::make_shared<core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{result, exprChildren[i]},
        combineOp);
  }
  return {result, ""};
}

// ===========================================================================
// Direction 2b: TypedExpr → SubfieldFilters (constant-only, no evaluator)
// ===========================================================================

namespace {

/// Try to extract a field name from a TypedExpr (FieldAccessTypedExpr).
std::optional<std::string> extractFieldName(const core::TypedExprPtr& expr) {
  if (const auto* field =
          dynamic_cast<const core::FieldAccessTypedExpr*>(expr.get())) {
    return field->name();
  }
  return std::nullopt;
}

/// Try to extract a constant value from a ConstantTypedExpr (or CastTypedExpr
/// wrapping one). Returns nullopt for non-constant expressions.
std::optional<variant> extractConstant(const core::TypedExprPtr& expr) {
  const auto* current = expr.get();
  // Unwrap CastTypedExpr layers.
  while (const auto* cast = dynamic_cast<const core::CastTypedExpr*>(current)) {
    current = cast->inputs()[0].get();
  }
  const auto* constant = dynamic_cast<const core::ConstantTypedExpr*>(current);
  if (!constant) {
    return std::nullopt;
  }
  return constant->value();
}

/// Macro to build an IN/NOT_IN BigintValues filter from narrow integer
/// elements. Uses DecodedVector with native-width reads for correct physical
/// type handling.
#define BUILD_INT_IN_FILTER(kindEnum, nativeType)                         \
  case TypeKind::kindEnum: {                                              \
    DecodedVector decoded(*elements);                                     \
    std::vector<int64_t> values;                                          \
    values.reserve(size);                                                 \
    for (vector_size_t i = 0; i < size; ++i)                              \
      values.push_back(                                                   \
          static_cast<int64_t>(decoded.valueAt<nativeType>(offset + i))); \
    return {                                                              \
        negated ? common::createNegatedBigintValues(values, false)        \
                : common::createBigintValues(values, false),              \
        ""};                                                              \
  }

/// Build a bolt Filter from a leaf CallTypedExpr (eq, neq, lt, lte, gt, gte,
/// between, in, not_in, is_null, is_not_null) with ConstantTypedExpr literals.
PaimonFilterTranslator::FilterBuildResult buildFilterFromCall(
    const core::CallTypedExpr& call) {
  auto fail =
      [](std::string reason) -> PaimonFilterTranslator::FilterBuildResult {
    return {nullptr, std::move(reason)};
  };

  const auto& op = call.name();
  const auto& inputs = call.inputs();
  if (inputs.empty()) {
    return fail("call has no inputs");
  }

  auto fieldName = extractFieldName(inputs[0]);
  if (!fieldName) {
    return fail("could not extract field name from inputs[0]");
  }

  // Null checks — no literal needed.
  if (op == "is_null") {
    return {common::nullOrFalse(true), ""};
  }
  if (op == "is_not_null") {
    return {common::notNullOrTrue(false), ""};
  }

  if (inputs.size() < 2) {
    return fail(fmt::format("operator '{}' requires at least 2 inputs", op));
  }

  auto value = extractConstant(inputs[1]);
  if (!value) {
    return fail("could not extract constant from inputs[1]");
  }

  const auto kind = inputs[1]->type()->kind();

  // IN / NOT_IN — handle directly from array constants.
  if ((op == "in" || op == "not_in")) {
    const auto* arrayConst =
        dynamic_cast<const core::ConstantTypedExpr*>(inputs[1].get());
    if (arrayConst && arrayConst->hasValueVector()) {
      const auto& vec = arrayConst->valueVector();
      if (vec->type()->isArray()) {
        const BaseVector* rawVec = vec.get();
        if (vec->isConstantEncoding() && vec->wrappedVector()) {
          rawVec = vec->wrappedVector();
        }
        const auto* arrVec = rawVec->as<ArrayVector>();
        if (arrVec) {
          constexpr vector_size_t kIndex = 0;
          auto offset = arrVec->offsetAt(kIndex);
          auto size = arrVec->sizeAt(kIndex);
          auto elements = arrVec->elements();
          if (elements) {
            const bool negated = (op == "not_in");
            const auto elemKind = elements->type()->kind();
            switch (elemKind) {
              BUILD_INT_IN_FILTER(TINYINT, int8_t)
              BUILD_INT_IN_FILTER(SMALLINT, int16_t)
              BUILD_INT_IN_FILTER(INTEGER, int32_t)
              BUILD_INT_IN_FILTER(BIGINT, int64_t)
              case TypeKind::HUGEINT: {
                DecodedVector decoded(*elements);
                std::vector<int128_t> values;
                values.reserve(size);
                for (vector_size_t i = 0; i < size; ++i) {
                  values.push_back(decoded.valueAt<int128_t>(offset + i));
                }
                auto [minIt, maxIt] =
                    std::minmax_element(values.begin(), values.end());
                if (negated) {
                  return fail("NOT_IN for HUGEINT/DECIMAL not supported");
                }
                return {
                    std::make_unique<common::HugeintValuesUsingHashTable>(
                        *minIt, *maxIt, values, false),
                    ""};
              }
              case TypeKind::VARCHAR:
              case TypeKind::VARBINARY: {
                DecodedVector decoded(*elements);
                std::vector<std::string> values;
                values.reserve(size);
                for (vector_size_t i = 0; i < size; ++i) {
                  values.emplace_back(decoded.valueAt<StringView>(offset + i));
                }
                return {
                    negated ? std::make_unique<common::NegatedBytesValues>(
                                  std::move(values), false)
                            : common::createBytesValues(values, false),
                    ""};
              }
              default:
                return fail(fmt::format(
                    "unsupported type kind {} for IN/NOT_IN",
                    static_cast<int>(elemKind)));
            }
          }
        }
      }
    }
  } // end IN / NOT_IN

  // Equality.
  if (op == "eq") {
    switch (kind) {
      case TypeKind::TINYINT:
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
        return {
            common::createBigintRange(
                value->value<int64_t>(), value->value<int64_t>(), false, true),
            ""};
      case TypeKind::HUGEINT:
        return {
            std::make_unique<common::HugeintRange>(
                value->value<int128_t>(), value->value<int128_t>(), false),
            ""};
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        return {
            common::createBytesRange(
                value->value<std::string>(),
                true,
                value->value<std::string>(),
                true,
                false),
            ""};
      case TypeKind::TIMESTAMP:
        return {
            std::make_unique<common::TimestampRange>(
                value->value<Timestamp>(), value->value<Timestamp>(), false),
            ""};
      default:
        return fail(fmt::format(
            "unsupported type kind {} for eq", static_cast<int>(kind)));
    }
  }

  // Inequality.
  if (op == "neq") {
    switch (kind) {
      case TypeKind::TINYINT:
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
        return {
            std::make_unique<common::NegatedBigintRange>(
                value->value<int64_t>(), value->value<int64_t>(), false),
            ""};
      case TypeKind::HUGEINT:
        return {
            std::make_unique<common::NegatedHugeintRange>(
                value->value<int128_t>(), value->value<int128_t>(), false),
            ""};
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        return {
            std::make_unique<common::NegatedBytesValues>(
                std::vector<std::string>{value->value<std::string>()}, false),
            ""};
      case TypeKind::TIMESTAMP:
        return {
            std::make_unique<common::NegatedTimestampRange>(
                value->value<Timestamp>(), value->value<Timestamp>(), false),
            ""};
      default:
        return fail(fmt::format(
            "unsupported type kind {} for neq", static_cast<int>(kind)));
    }
  }

  // Range comparisons.
  bool isGreater = (op == "gt" || op == "gte");
  bool isExclusive = (op == "gt" || op == "lt");

  if (isGreater || op == "lt" || op == "lte") {
    switch (kind) {
      case TypeKind::TINYINT:
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER:
      case TypeKind::BIGINT: {
        int64_t v = value->value<int64_t>();
        int64_t lower = std::numeric_limits<int64_t>::min();
        int64_t upper = std::numeric_limits<int64_t>::max();
        if (isGreater) {
          lower = isExclusive ? v + 1 : v;
        } else {
          upper = isExclusive ? v - 1 : v;
        }
        return {common::createBigintRange(lower, upper, false, true), ""};
      }
      case TypeKind::HUGEINT: {
        int128_t v = value->value<int128_t>();
        int128_t lower = std::numeric_limits<int128_t>::min();
        int128_t upper = std::numeric_limits<int128_t>::max();
        if (isGreater) {
          lower = isExclusive ? v + 1 : v;
        } else {
          upper = isExclusive ? v - 1 : v;
        }
        return {
            std::make_unique<common::HugeintRange>(lower, upper, false), ""};
      }
      case TypeKind::DOUBLE: {
        double v = value->value<double>();
        if (isGreater) {
          return {
              std::make_unique<common::FloatingPointRange<double>>(
                  v, false, isExclusive, 0.0, true, false, false),
              ""};
        }
        return {
            std::make_unique<common::FloatingPointRange<double>>(
                0.0, true, false, v, false, isExclusive, false),
            ""};
      }
      case TypeKind::REAL: {
        float v = value->value<float>();
        if (isGreater) {
          return {
              std::make_unique<common::FloatingPointRange<float>>(
                  v, false, isExclusive, 0.0F, true, false, false),
              ""};
        }
        return {
            std::make_unique<common::FloatingPointRange<float>>(
                0.0F, true, false, v, false, isExclusive, false),
            ""};
      }
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        auto s = value->value<std::string>();
        if (isGreater) {
          return {
              common::createBytesRange(
                  s, !isExclusive, std::nullopt, false, false),
              ""};
        }
        return {
            common::createBytesRange(
                std::nullopt, false, s, !isExclusive, false),
            ""};
      }
      case TypeKind::TIMESTAMP: {
        auto ts = value->value<Timestamp>();
        if (isGreater) {
          if (!isExclusive) {
            return {
                std::make_unique<common::TimestampRange>(
                    ts, Timestamp::max(), false),
                ""};
          }
          auto lower = ts;
          ++lower;
          return {
              std::make_unique<common::TimestampRange>(
                  lower, Timestamp::max(), false),
              ""};
        }
        if (!isExclusive) {
          return {
              std::make_unique<common::TimestampRange>(
                  Timestamp::min(), ts, false),
              ""};
        }
        auto upper = ts;
        --upper;
        return {
            std::make_unique<common::TimestampRange>(
                Timestamp::min(), upper, false),
            ""};
      }
      default:
        return fail(fmt::format(
            "unsupported type kind {} for range comparison '{}'",
            static_cast<int>(kind),
            op));
    }
  }

  // Between.
  if (op == "between" && inputs.size() >= 3) {
    auto low = extractConstant(inputs[1]);
    auto high = extractConstant(inputs[2]);
    if (!low || !high) {
      return fail("between: could not extract low/high constants");
    }
    switch (kind) {
      case TypeKind::TINYINT:
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
        return {
            common::createBigintRange(
                low->value<int64_t>(), high->value<int64_t>(), false, true),
            ""};
      case TypeKind::HUGEINT:
        return {
            std::make_unique<common::HugeintRange>(
                low->value<int128_t>(), high->value<int128_t>(), false),
            ""};
      case TypeKind::TIMESTAMP:
        return {
            std::make_unique<common::TimestampRange>(
                low->value<Timestamp>(), high->value<Timestamp>(), false),
            ""};
      default:
        return fail(fmt::format(
            "unsupported type kind {} for between", static_cast<int>(kind)));
    }
  }

  return fail(fmt::format("unsupported operator '{}'", op));
}

void addSubfieldFilter(
    common::SubfieldFilters& filters,
    common::Subfield subfield,
    std::unique_ptr<common::Filter> filter) {
  if (!filter) {
    return;
  }
  const auto fieldName = subfield.toString();
  auto [it, inserted] =
      filters.try_emplace(std::move(subfield), std::move(filter));
  if (!inserted && it->second && filter) {
    try {
      it->second = it->second->mergeWith(filter.get());
    } catch (const BoltException& e) {
      // Some filter types (e.g. HugeintRange) don't support mergeWith.
      // Skip pushdown for this subfield rather than failing.
      LOG(WARNING) << "toSubfieldFilters: cannot merge filters on '"
                   << fieldName << "': " << e.message();
      filters.erase(it);
    }
  }
}

bool extractSubfieldFilter(
    const core::TypedExprPtr& expr,
    common::SubfieldFilters& filters) {
  const auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  if (!call) {
    return false;
  }

  const auto& op = call->name();

  // OR: only push down when ALL direct children are EQUAL predicates
  // on the same column (produces a Values filter). Cross-column or mixed
  // ORs cannot be pushed into ScanSpec.
  if (op == "or") {
    std::optional<std::string> columnName;
    std::vector<int64_t> intValues;
    std::vector<int128_t> hugeintValues;
    std::vector<std::string> stringValues;
    bool allEqual = true;

    std::vector<const core::CallTypedExpr*> orStack;
    for (const auto& child : call->inputs()) {
      const auto* childCall =
          dynamic_cast<const core::CallTypedExpr*>(child.get());
      if (childCall) {
        orStack.push_back(childCall);
      } else {
        allEqual = false;
        break;
      }
    }
    while (!orStack.empty() && allEqual) {
      const auto* current = orStack.back();
      orStack.pop_back();
      if (!current || current->name() != "eq") {
        allEqual = false;
        break;
      }
      auto name = extractFieldName(current->inputs()[0]);
      if (!name) {
        allEqual = false;
        break;
      }
      if (!columnName.has_value()) {
        columnName = name;
      } else if (*columnName != *name) {
        allEqual = false;
        break;
      }
      auto val = extractConstant(current->inputs()[1]);
      if (!val) {
        allEqual = false;
        break;
      }
      auto kind = current->inputs()[1]->type()->kind();
      if (kind == TypeKind::TINYINT || kind == TypeKind::SMALLINT ||
          kind == TypeKind::INTEGER || kind == TypeKind::BIGINT) {
        // literalToConstantExpr promotes all integral types < BIGINT to
        // int64_t in the variant, so always read as int64_t.
        intValues.push_back(val->value<int64_t>());
      } else if (kind == TypeKind::HUGEINT) {
        hugeintValues.push_back(val->value<int128_t>());
      } else if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
        stringValues.push_back(val->value<std::string>());
      } else {
        allEqual = false;
        break;
      }
    }

    if (!allEqual) {
      LOG(INFO)
          << "[FilterPushdown] OR predicate cannot be pushed down: "
             "not all children are equality predicates on the same column";
      return false;
    }
    if (!columnName.has_value()) {
      return false;
    }

    std::unique_ptr<common::Filter> filter;
    if (!intValues.empty()) {
      filter = common::createBigintValues(intValues, false);
    } else if (!hugeintValues.empty()) {
      auto [minIt, maxIt] =
          std::minmax_element(hugeintValues.begin(), hugeintValues.end());
      filter = std::make_unique<common::HugeintValuesUsingHashTable>(
          *minIt, *maxIt, hugeintValues, false);
    } else if (!stringValues.empty()) {
      filter = common::createBytesValues(stringValues, false);
    }
    if (!filter) {
      return false;
    }
    addSubfieldFilter(
        filters, common::Subfield(columnName.value()), std::move(filter));
    return true;
  }

  // Leaf predicate — try to build a filter directly.
  auto result = buildFilterFromCall(*call);
  if (!result) {
    LOG(INFO) << "[FilterPushdown] skipping leaf predicate '" << call->name()
              << "': " << result.reason;
    return false;
  }

  auto fieldName = extractFieldName(call->inputs()[0]);
  if (!fieldName) {
    return false;
  }

  addSubfieldFilter(
      filters, common::Subfield(*fieldName), std::move(result.value));
  return true;
}

bool extractSubfieldFiltersWithEvaluator(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    common::SubfieldFilters& filters) {
  const auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  if (!call) {
    return false;
  }

  try {
    auto subfieldFilter = exec::toSubfieldFilter(expr, evaluator);
    addSubfieldFilter(
        filters,
        std::move(subfieldFilter.first),
        std::move(subfieldFilter.second));
    return true;
  } catch (const BoltException& e) {
    LOG(INFO) << "[FilterPushdown] ExprToSubfieldFilterParser skipped '"
              << call->name() << "': " << e.message();
  } catch (const std::exception& e) {
    LOG(INFO) << "[FilterPushdown] ExprToSubfieldFilterParser skipped '"
              << call->name() << "': " << e.what();
  }

  return false;
}

} // namespace

common::SubfieldFilters PaimonFilterTranslator::toSubfieldFilters(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator) {
  common::SubfieldFilters filters;
  for (const auto& conjunct : exec::flattenTopLevelConjuncts(expr)) {
    if (evaluator &&
        extractSubfieldFiltersWithEvaluator(conjunct, evaluator, filters)) {
      continue;
    }

    // Fallback to direct subfield filter extraction (no evaluator).
    extractSubfieldFilter(conjunct, filters);
  }
  return filters;
}

} // namespace bytedance::bolt::connector::paimon
