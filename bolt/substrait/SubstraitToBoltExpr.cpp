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

#include "bolt/substrait/SubstraitToBoltExpr.h"
#include <cstring>
#include "bolt/substrait/TypeUtils.h"
#include "bolt/vector/FlatVector.h"
using namespace bytedance::bolt;
namespace {
// Get values for the different supported types.
template <typename T>
T getLiteralValue(const ::substrait::Expression::Literal& /* literal */) {
  BOLT_NYI();
}

template <>
int8_t getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return static_cast<int8_t>(literal.i8());
}

template <>
int16_t getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return static_cast<int16_t>(literal.i16());
}

template <>
int32_t getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.i32();
}

template <>
int64_t getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.i64();
}

template <>
double getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.fp64();
}

template <>
float getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.fp32();
}

template <>
bool getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.boolean();
}

template <>
uint32_t getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.i32();
}

template <>
Timestamp getLiteralValue(const ::substrait::Expression::Literal& literal) {
  if (literal.has_timestamp()) {
    return Timestamp::fromMicros(literal.timestamp());
  }

  const auto& timestamp = literal.precision_timestamp();
  const auto precision = timestamp.precision();
  BOLT_USER_CHECK_GE(precision, 0, "Timestamp precision must be non-negative");
  BOLT_USER_CHECK_LE(precision, 9, "Timestamp precision exceeds nanoseconds");
  int64_t unitsPerSecond = 1;
  for (int32_t i = 0; i < precision; ++i) {
    unitsPerSecond *= 10;
  }
  auto seconds = timestamp.value() / unitsPerSecond;
  auto remainder = timestamp.value() % unitsPerSecond;
  // Timestamp stores negative fractional times with a non-negative nanos part.
  if (remainder < 0) {
    --seconds;
    remainder += unitsPerSecond;
  }
  BOLT_USER_CHECK_GE(
      seconds, Timestamp::kMinSeconds, "Timestamp seconds out of range");
  BOLT_USER_CHECK_LE(
      seconds, Timestamp::kMaxSeconds, "Timestamp seconds out of range");
  return Timestamp(
      seconds, remainder * (Timestamp::kNanosInSecond / unitsPerSecond));
}

int128_t decimalLiteralValue(
    const ::substrait::Expression_Literal_Decimal& decimal) {
  BOLT_CHECK_EQ(
      decimal.value().size(),
      sizeof(int128_t),
      "Expected decimal literal to be encoded as a 16-byte signed integer.");
  int128_t value;
  std::memcpy(&value, decimal.value().data(), sizeof(value));
  return value;
}

ArrayVectorPtr makeArrayVector(const VectorPtr& elements) {
  BufferPtr offsets = allocateOffsets(2, elements->pool());
  BufferPtr sizes = allocateOffsets(1, elements->pool());
  sizes->asMutable<vector_size_t>()[0] = elements->size();
  offsets->asMutable<vector_size_t>()[0] = 0;
  offsets->asMutable<vector_size_t>()[1] = elements->size();

  return std::make_shared<ArrayVector>(
      elements->pool(),
      ARRAY(elements->type()),
      nullptr,
      1,
      offsets,
      sizes,
      elements);
}

template <typename T>
void setLiteralValue(
    const ::substrait::Expression::Literal& literal,
    FlatVector<T>* vector,
    vector_size_t index) {
  if (literal.has_null()) {
    vector->setNull(index, true);
  } else if constexpr (std::is_same_v<T, StringView>) {
    if (literal.has_string()) {
      vector->set(index, StringView(literal.string()));
    } else if (literal.has_var_char()) {
      vector->set(index, StringView(literal.var_char().value()));
    } else if (literal.has_binary()) {
      vector->set(index, StringView(literal.binary()));
    } else {
      BOLT_FAIL("Unexpected string literal");
    }
  } else if (vector->type()->isDate()) {
    auto dateVector = vector->template asFlatVector<int32_t>();
    dateVector->set(index, int(literal.date()));
  } else {
    vector->set(index, getLiteralValue<T>(literal));
  }
}

template <TypeKind kind>
VectorPtr constructFlatVector(
    const ::substrait::Expression::Literal& listLiteral,
    const vector_size_t size,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  BOLT_CHECK(type->isPrimitiveType());
  auto vector = BaseVector::create(type, size, pool);
  if (size == 0) {
    return vector;
  }
  using T = typename TypeTraits<kind>::NativeType;
  auto flatVector = vector->as<FlatVector<T>>();

  vector_size_t index = 0;
  for (auto child : listLiteral.list().values()) {
    setLiteralValue(child, flatVector, index++);
  }
  return vector;
}

/// Whether null will be returned on cast failure.
bool isNullOnFailure(
    ::substrait::Expression::Cast::FailureBehavior failureBehavior) {
  switch (failureBehavior) {
    case ::substrait::
        Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_UNSPECIFIED:
    case ::substrait::
        Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION:
      return false;
    case ::substrait::
        Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_RETURN_NULL:
      return true;
    default:
      BOLT_NYI(
          "The given failure behavior is NOT supported: '{}'", failureBehavior);
  }
}

} // namespace
namespace bytedance::bolt::substrait {

std::shared_ptr<const core::FieldAccessTypedExpr>
SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression::FieldReference& substraitField,
    const RowTypePtr& inputType) {
  auto typeCase = substraitField.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::FieldReference::ReferenceTypeCase::
        kDirectReference: {
      const auto& directRef = substraitField.direct_reference();
      int32_t colIdx = substraitParser_->parseReferenceSegment(directRef);
      const auto& inputNames = inputType->names();
      const int64_t inputSize = inputNames.size();
      if (colIdx <= inputSize) {
        const auto& inputTypes = inputType->children();
        // Convert type to row.
        return std::make_shared<core::FieldAccessTypedExpr>(
            inputTypes[colIdx],
            std::make_shared<core::InputTypedExpr>(inputTypes[colIdx]),
            inputNames[colIdx]);
      } else {
        BOLT_FAIL("Missing the column with id '{}' .", colIdx);
      }
    }
    default:
      BOLT_NYI(
          "Substrait conversion not supported for Reference '{}'", typeCase);
  }
}

core::TypedExprPtr SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression::ScalarFunction& substraitFunc,
    const RowTypePtr& inputType) {
  std::vector<core::TypedExprPtr> params;
  params.reserve(substraitFunc.arguments().size());
  std::vector<TypePtr> inputTypes;
  for (const auto& sArg : substraitFunc.arguments()) {
    core::TypedExprPtr expr = toBoltExpr(sArg.value(), inputType);
    params.emplace_back(expr);
    inputTypes.emplace_back(expr->type());
  }
  const auto& boltFunction = functionMap_
      ? substraitParser_->findBoltFunction(
            *functionMap_, substraitFunc.function_reference())
      : substraitParser_->findBoltFunction(substraitFunc.function_reference());
  auto outputType = substraitParser_->parseType(substraitFunc.output_type());
  if (specialFunctions_.count(boltFunction) == 0) {
    auto returnType = resolveFunction(boltFunction, inputTypes);
    if (!returnType) {
      auto it = functionSignatureMap_.find(boltFunction);
      if (it == functionSignatureMap_.end()) {
        BOLT_FAIL(
            "Scalar function name not registered: {}, called with arguments: ({})",
            boltFunction,
            folly::join(", ", inputTypes));
      } else {
        std::vector<std::string> signatures;
        for (const auto& signature : it->second) {
          signatures.push_back(fmt::format("({})", signature->toString()));
        }
        BOLT_FAIL(
            "Scalar function {} not registered with arguments: ({}). "
            "Found function registered with the following signatures:\n{}",
            boltFunction,
            folly::join(", ", inputTypes),
            folly::join("\n", signatures));
      }
    } else if (!outputType->equivalent(*returnType.get())) {
      BOLT_FAIL(
          "Found incompatible return types for '{}' ({} vs. {}) "
          "for input types ({})",
          boltFunction,
          outputType->toString(),
          returnType->toString(),
          folly::join(", ", inputTypes));
    }
  }
  return std::make_shared<const core::CallTypedExpr>(
      outputType, std::move(params), boltFunction);
}

std::shared_ptr<const core::ConstantTypedExpr>
SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression::Literal& substraitLit) {
  auto typeCase = substraitLit.literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::LITERAL_TYPE_NOT_SET:
      // Treat unspecified literal as NULL of UNKNOWN type.
      // Some producers emit an unset literal to represent None.
      {
        auto t = UNKNOWN();
        return std::make_shared<core::ConstantTypedExpr>(
            t, variant::null(t->kind()));
      }
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
      return std::make_shared<core::ConstantTypedExpr>(
          BOOLEAN(), variant(substraitLit.boolean()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI8:
      // SubstraitLit.i8() will return int32, so we need this type conversion.
      return std::make_shared<core::ConstantTypedExpr>(
          TINYINT(), variant(static_cast<int8_t>(substraitLit.i8())));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI16:
      // SubstraitLit.i16() will return int32, so we need this type conversion.
      return std::make_shared<core::ConstantTypedExpr>(
          SMALLINT(), variant(static_cast<int16_t>(substraitLit.i16())));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
      return std::make_shared<core::ConstantTypedExpr>(
          INTEGER(), variant(substraitLit.i32()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp32:
      return std::make_shared<core::ConstantTypedExpr>(
          REAL(), variant(substraitLit.fp32()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
      return std::make_shared<core::ConstantTypedExpr>(
          BIGINT(), variant(substraitLit.i64()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
      return std::make_shared<core::ConstantTypedExpr>(
          DOUBLE(), variant(substraitLit.fp64()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kBinary:
      // Binary literal maps to Bolt VARBINARY
      return std::make_shared<core::ConstantTypedExpr>(
          VARBINARY(), variant::binary(substraitLit.binary()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kString:
      return std::make_shared<core::ConstantTypedExpr>(
          VARCHAR(), variant(substraitLit.string()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
      auto boltType = substraitParser_->parseType(substraitLit.null());
      return std::make_shared<core::ConstantTypedExpr>(
          boltType, variant::null(boltType->kind()));
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kVarChar:
      return std::make_shared<core::ConstantTypedExpr>(
          VARCHAR(), variant(substraitLit.var_char().value()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kFixedChar:
      return std::make_shared<core::ConstantTypedExpr>(
          VARCHAR(), variant(substraitLit.fixed_char()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kDate:
      return std::make_shared<core::ConstantTypedExpr>(
          DATE(), variant(int(substraitLit.date())));
    case ::substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
    case ::substrait::Expression_Literal::LiteralTypeCase::kPrecisionTimestamp:
      return std::make_shared<core::ConstantTypedExpr>(
          TIMESTAMP(), variant(getLiteralValue<Timestamp>(substraitLit)));
    case ::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
      const auto& decimal = substraitLit.decimal();
      auto boltType = DECIMAL(decimal.precision(), decimal.scale());
      auto value = decimalLiteralValue(decimal);
      if (boltType->isShortDecimal()) {
        return std::make_shared<core::ConstantTypedExpr>(
            boltType, variant(static_cast<int64_t>(value)));
      }
      return std::make_shared<core::ConstantTypedExpr>(
          boltType, variant(value));
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kUserDefined: {
      const auto& ud = substraitLit.user_defined();
      const uint32_t anchor = ud.type_reference();
      const std::string& name{substraitParser_->getTypeExtensionName(anchor)};
      if (name == "UNKNOWN") {
        auto t = UNKNOWN();
        return std::make_shared<core::ConstantTypedExpr>(
            t, variant::null(t->kind()));
      }
      BOLT_NYI("Unsupported user-defined literal type '{}'.", name);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kList:
    case ::substrait::Expression_Literal::LiteralTypeCase::kEmptyList: {
      auto constantVector =
          BaseVector::wrapInConstant(1, 0, literalsToArrayVector(substraitLit));
      return std::make_shared<const core::ConstantTypedExpr>(constantVector);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kMap:
    case ::substrait::Expression_Literal::LiteralTypeCase::kEmptyMap: {
      auto constantVector =
          BaseVector::wrapInConstant(1, 0, literalsToMapVector(substraitLit));
      return std::make_shared<const core::ConstantTypedExpr>(constantVector);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kStruct:
      // TODO: return a RowVector.
      // Use a similar method as for the kList case above.
    default:
      BOLT_NYI(
          "Substrait conversion not supported for type case '{}'", typeCase);
  }
}

MapVectorPtr SubstraitBoltExprConverter::literalsToMapVector(
    const ::substrait::Expression::Literal& mapLiteral) {
  if (mapLiteral.has_empty_map()) {
    // Create empty list literals for keys and values
    ::substrait::Expression::Literal emptyKeysList;
    *emptyKeysList.mutable_empty_list()->mutable_type() =
        mapLiteral.empty_map().key();
    ::substrait::Expression::Literal emptyValuesList;
    *emptyValuesList.mutable_empty_list()->mutable_type() =
        mapLiteral.empty_map().value();

    // Convert to array vectors
    auto emptyKeysArray = literalsToArrayVector(emptyKeysList);
    auto emptyValuesArray = literalsToArrayVector(emptyValuesList);
    auto mapType =
        MAP(emptyKeysArray->type()->childAt(0),
            emptyValuesArray->type()->childAt(0));

    // Create empty map vector
    BufferPtr offsets = allocateOffsets(1, pool_);
    BufferPtr sizes = allocateOffsets(1, pool_);
    offsets->asMutable<vector_size_t>()[0] = 0;
    sizes->asMutable<vector_size_t>()[0] = 0;
    return std::make_shared<MapVector>(
        pool_,
        mapType,
        nullptr,
        1,
        offsets,
        sizes,
        emptyKeysArray->elements(),
        emptyValuesArray->elements());
  } else if (mapLiteral.has_map()) {
    const auto& map = mapLiteral.map();
    size_t entryCount = map.key_values_size();

    // Create list literals for keys and values
    ::substrait::Expression::Literal keysList;
    auto* keysListValue = keysList.mutable_list();
    ::substrait::Expression::Literal valuesList;
    auto* valuesListValue = valuesList.mutable_list();

    // Populate keys and values lists
    for (int i = 0; i < map.key_values_size(); i++) {
      const auto& keyValue = map.key_values(i);
      keysListValue->add_values()->CopyFrom(keyValue.key());
      valuesListValue->add_values()->CopyFrom(keyValue.value());
    }

    // Convert to array vectors
    auto keysArray = literalsToArrayVector(keysList);
    auto valuesArray = literalsToArrayVector(valuesList);

    // Create map vector
    BufferPtr offsets = allocateOffsets(2, pool_);
    BufferPtr sizes = allocateOffsets(1, pool_);
    sizes->asMutable<vector_size_t>()[0] = entryCount;
    offsets->asMutable<vector_size_t>()[0] = 0;
    offsets->asMutable<vector_size_t>()[1] = entryCount;

    // Get child types from array vectors
    TypePtr keyType = keysArray->type()->childAt(0);
    TypePtr valueType = valuesArray->type()->childAt(0);

    return std::make_shared<MapVector>(
        pool_,
        MAP(keyType, valueType),
        nullptr,
        1,
        offsets,
        sizes,
        keysArray->elements(),
        valuesArray->elements());
  } else {
    BOLT_FAIL(
        "Cannot convert literal {} to map.", mapLiteral.SerializeAsString());
  }
}

ArrayVectorPtr SubstraitBoltExprConverter::literalsToArrayVector(
    const ::substrait::Expression::Literal& listLiteral) {
  vector_size_t childSize = 0;
  if (listLiteral.has_empty_list()) {
    auto elementType =
        substraitParser_->parseType(listLiteral.empty_list().type());
    return makeArrayVector(BaseVector::create(elementType, 0, pool_));
  } else if (listLiteral.has_list()) {
    childSize = listLiteral.list().values().size();
  } else {
    BOLT_FAIL(
        "Cannot convert literal {} to list.", listLiteral.SerializeAsString());
  }

  if (childSize == 0) {
    return makeArrayVector(BaseVector::create(UNKNOWN(), 0, pool_));
  }
  auto typeCase = listLiteral.list().values(0).literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
      return makeArrayVector(constructFlatVector<TypeKind::BOOLEAN>(
          listLiteral, childSize, BOOLEAN(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI8:
      return makeArrayVector(constructFlatVector<TypeKind::TINYINT>(
          listLiteral, childSize, TINYINT(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI16:
      return makeArrayVector(constructFlatVector<TypeKind::SMALLINT>(
          listLiteral, childSize, SMALLINT(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
      return makeArrayVector(constructFlatVector<TypeKind::INTEGER>(
          listLiteral, childSize, INTEGER(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp32:
      return makeArrayVector(constructFlatVector<TypeKind::REAL>(
          listLiteral, childSize, REAL(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
      return makeArrayVector(constructFlatVector<TypeKind::BIGINT>(
          listLiteral, childSize, BIGINT(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
      return makeArrayVector(constructFlatVector<TypeKind::DOUBLE>(
          listLiteral, childSize, DOUBLE(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kString:
    case ::substrait::Expression_Literal::LiteralTypeCase::kVarChar:
      return makeArrayVector(constructFlatVector<TypeKind::VARCHAR>(
          listLiteral, childSize, VARCHAR(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
      auto boltType =
          substraitParser_->parseType(listLiteral.list().values(0).null());
      auto kind = boltType->kind();
      return makeArrayVector(BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          constructFlatVector, kind, listLiteral, childSize, boltType, pool_));
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kDate:
      return makeArrayVector(constructFlatVector<TypeKind::INTEGER>(
          listLiteral, childSize, DATE(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
    case ::substrait::Expression_Literal::LiteralTypeCase::kPrecisionTimestamp:
      return makeArrayVector(constructFlatVector<TypeKind::TIMESTAMP>(
          listLiteral, childSize, TIMESTAMP(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kBinary:
      return makeArrayVector(constructFlatVector<TypeKind::VARBINARY>(
          listLiteral, childSize, VARBINARY(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kIntervalDayToSecond:
      return makeArrayVector(constructFlatVector<TypeKind::BIGINT>(
          listLiteral, childSize, INTERVAL_DAY_TIME(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kIntervalYearToMonth:
      return makeArrayVector(constructFlatVector<TypeKind::INTEGER>(
          listLiteral, childSize, INTERVAL_YEAR_MONTH(), pool_));
    case ::substrait::Expression_Literal::LiteralTypeCase::kList:
    case ::substrait::Expression_Literal::LiteralTypeCase::kEmptyList: {
      VectorPtr elements;
      for (auto it : listLiteral.list().values()) {
        auto v = literalsToArrayVector(it);
        if (!elements) {
          elements = v;
        } else {
          elements->append(v.get());
        }
      }
      return makeArrayVector(elements);
    }
    default:
      BOLT_NYI(
          "literalsToArrayVector not supported for type case '{}'", typeCase);
  }
}

core::TypedExprPtr SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression::Cast& castExpr,
    const RowTypePtr& inputType) {
  auto type = substraitParser_->parseType(castExpr.type());
  bool nullOnFailure = isNullOnFailure(castExpr.failure_behavior());

  std::vector<core::TypedExprPtr> inputs{
      toBoltExpr(castExpr.input(), inputType)};

  return std::make_shared<core::CastTypedExpr>(type, inputs, nullOnFailure);
}

core::TypedExprPtr SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression& substraitExpr,
    const RowTypePtr& inputType) {
  core::TypedExprPtr boltExpr;
  auto typeCase = substraitExpr.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kLiteral:
      return toBoltExpr(substraitExpr.literal());
    case ::substrait::Expression::RexTypeCase::kScalarFunction:
      return toBoltExpr(substraitExpr.scalar_function(), inputType);
    case ::substrait::Expression::RexTypeCase::kSelection:
      return toBoltExpr(substraitExpr.selection(), inputType);
    case ::substrait::Expression::RexTypeCase::kCast:
      return toBoltExpr(substraitExpr.cast(), inputType);
    case ::substrait::Expression::RexTypeCase::kIfThen:
      return toBoltExpr(substraitExpr.if_then(), inputType);
    default:
      BOLT_NYI(
          "Substrait conversion not supported for Expression '{}'", typeCase);
  }
}

core::TypedExprPtr SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression_IfThen& substraitIfThen,
    const RowTypePtr& inputType) {
  std::vector<core::TypedExprPtr> inputs;
  if (substraitIfThen.has_else_()) {
    inputs.reserve(substraitIfThen.ifs_size() * 2 + 1);
  } else {
    inputs.reserve(substraitIfThen.ifs_size() * 2);
  }

  TypePtr resultType;
  for (auto& ifExpr : substraitIfThen.ifs()) {
    auto ifClauseExpr = toBoltExpr(ifExpr.if_(), inputType);
    inputs.emplace_back(ifClauseExpr);
    auto thenClauseExpr = toBoltExpr(ifExpr.then(), inputType);
    inputs.emplace_back(thenClauseExpr);

    if (!thenClauseExpr->type()->containsUnknown()) {
      resultType = thenClauseExpr->type();
    }
  }

  if (substraitIfThen.has_else_()) {
    auto elseClauseExpr = toBoltExpr(substraitIfThen.else_(), inputType);
    inputs.emplace_back(elseClauseExpr);
    if (!resultType && !elseClauseExpr->type()->containsUnknown()) {
      resultType = elseClauseExpr->type();
    }
  }

  BOLT_CHECK_NOT_NULL(resultType, "Result type not found");

  return std::make_shared<const core::CallTypedExpr>(
      resultType, std::move(inputs), "if");
}
std::unordered_set<std::string> SubstraitBoltExprConverter::specialFunctions_ =
    {"and",
     "or",
     "cast",
     "try_cast",
     "coalesce",
     "if",
     "switch",
     "try",
     // Null checks are treated as built-ins with explicit output types
     "is_null",
     "NOT is_null"};
} // namespace bytedance::bolt::substrait
