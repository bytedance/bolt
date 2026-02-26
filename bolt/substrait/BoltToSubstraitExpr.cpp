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

#include "bolt/substrait/BoltToSubstraitExpr.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::substrait {

namespace {
const ::substrait::Expression_Literal& toSubstraitNullLiteral(
    google::protobuf::Arena& arena,
    const bolt::TypeKind& typeKind) {
  ::substrait::Expression_Literal* substraitField =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  switch (typeKind) {
    case bolt::TypeKind::BOOLEAN: {
      ::substrait::Type_Boolean* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_Boolean>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_bool_(nullValue);
      break;
    }
    case bolt::TypeKind::TINYINT: {
      ::substrait::Type_I8* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I8>(&arena);

      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_i8(nullValue);
      break;
    }
    case bolt::TypeKind::SMALLINT: {
      ::substrait::Type_I16* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I16>(&arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_i16(nullValue);
      break;
    }
    case bolt::TypeKind::INTEGER: {
      ::substrait::Type_I32* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I32>(&arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_i32(nullValue);
      break;
    }
    case bolt::TypeKind::BIGINT: {
      ::substrait::Type_I64* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I64>(&arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_i64(nullValue);
      break;
    }
    case bolt::TypeKind::VARCHAR: {
      ::substrait::Type_String* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_String>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_string(nullValue);
      break;
    }
    case bolt::TypeKind::REAL: {
      ::substrait::Type_FP32* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_FP32>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_fp32(nullValue);
      break;
    }
    case bolt::TypeKind::DOUBLE: {
      ::substrait::Type_FP64* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_FP64>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_fp64(nullValue);
      break;
    }
    case bolt::TypeKind::ARRAY: {
      ::substrait::Type_List* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_List>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_list(nullValue);
      break;
    }
    case bolt::TypeKind::UNKNOWN: {
      ::substrait::Type_UserDefined* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_UserDefined>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      nullValue->set_type_reference(0);
      substraitField->mutable_null()->set_allocated_user_defined(nullValue);

      break;
    }
    default: {
      BOLT_UNSUPPORTED("Unsupported type '{}'", mapTypeKindToName(typeKind));
    }
  }
  substraitField->set_nullable(true);
  return *substraitField;
}

const ::substrait::Expression_Literal& toSubstraitNotNullLiteral(
    google::protobuf::Arena& arena,
    const bolt::variant& variantValue) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  switch (variantValue.kind()) {
    case bolt::TypeKind::BOOLEAN: {
      literalExpr->set_boolean(variantValue.value<TypeKind::BOOLEAN>());
      break;
    }
    case bolt::TypeKind::TINYINT: {
      literalExpr->set_i8(variantValue.value<TypeKind::TINYINT>());
      break;
    }
    case bolt::TypeKind::SMALLINT: {
      literalExpr->set_i16(variantValue.value<TypeKind::SMALLINT>());
      break;
    }
    case bolt::TypeKind::INTEGER: {
      literalExpr->set_i32(variantValue.value<TypeKind::INTEGER>());
      break;
    }
    case bolt::TypeKind::BIGINT: {
      literalExpr->set_i64(variantValue.value<TypeKind::BIGINT>());
      break;
    }
    case bolt::TypeKind::REAL: {
      literalExpr->set_fp32(variantValue.value<TypeKind::REAL>());
      break;
    }
    case bolt::TypeKind::DOUBLE: {
      literalExpr->set_fp64(variantValue.value<TypeKind::DOUBLE>());
      break;
    }
    case bolt::TypeKind::TIMESTAMP: {
      auto vTimeStamp = variantValue.value<TypeKind::TIMESTAMP>();
      auto micros =
          vTimeStamp.getSeconds() * 1000000 + vTimeStamp.getNanos() / 1000;
      literalExpr->set_timestamp(micros);
      break;
    }
    case bolt::TypeKind::VARCHAR: {
      auto vCharValue = variantValue.value<StringView>();
      ::substrait::Expression_Literal::VarChar* sVarChar =
          new ::substrait::Expression_Literal::VarChar();
      sVarChar->set_value(vCharValue.data());
      sVarChar->set_length(vCharValue.size());
      literalExpr->set_allocated_var_char(sVarChar);
      break;
    }
    default:
      BOLT_NYI(
          "Unsupported constant Type '{}' ",
          mapTypeKindToName(variantValue.kind()));
  }
  literalExpr->set_nullable(false);
  return *literalExpr;
}

template <TypeKind kind>
const ::substrait::Expression_Literal& toSubstraitNotNullLiteral(
    google::protobuf::Arena& arena,
    const typename TypeTraits<kind>::NativeType& /* value */) {
  BOLT_UNSUPPORTED(
      "toSubstraitNotNullLiteral function doesn't support {} type",
      TypeTraits<kind>::name);

  // Make compiler happy.
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  return *literalExpr;
}

template <>
const ::substrait::Expression_Literal& toSubstraitNotNullLiteral<
    TypeKind::BOOLEAN>(google::protobuf::Arena& arena, const bool& value) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  literalExpr->set_boolean(value);
  literalExpr->set_nullable(false);
  return *literalExpr;
}

template <>
const ::substrait::Expression_Literal& toSubstraitNotNullLiteral<
    TypeKind::TINYINT>(google::protobuf::Arena& arena, const int8_t& value) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  literalExpr->set_i8(value);
  literalExpr->set_nullable(false);
  return *literalExpr;
}

template <>
const ::substrait::Expression_Literal& toSubstraitNotNullLiteral<
    TypeKind::SMALLINT>(google::protobuf::Arena& arena, const int16_t& value) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  literalExpr->set_i16(value);
  literalExpr->set_nullable(false);
  return *literalExpr;
}

template <>
const ::substrait::Expression_Literal& toSubstraitNotNullLiteral<
    TypeKind::INTEGER>(google::protobuf::Arena& arena, const int32_t& value) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  literalExpr->set_i32(value);
  literalExpr->set_nullable(false);
  return *literalExpr;
}

template <>
const ::substrait::Expression_Literal& toSubstraitNotNullLiteral<
    TypeKind::BIGINT>(google::protobuf::Arena& arena, const int64_t& value) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  literalExpr->set_i64(value);
  literalExpr->set_nullable(false);
  return *literalExpr;
}

template <>
const ::substrait::Expression_Literal& toSubstraitNotNullLiteral<
    TypeKind::REAL>(google::protobuf::Arena& arena, const float& value) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  literalExpr->set_fp32(value);
  literalExpr->set_nullable(false);
  return *literalExpr;
}

template <>
const ::substrait::Expression_Literal& toSubstraitNotNullLiteral<
    TypeKind::DOUBLE>(google::protobuf::Arena& arena, const double& value) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  literalExpr->set_fp64(value);
  literalExpr->set_nullable(false);
  return *literalExpr;
}

template <>
const ::substrait::Expression_Literal&
toSubstraitNotNullLiteral<TypeKind::TIMESTAMP>(
    google::protobuf::Arena& arena,
    const Timestamp& value) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  auto micros = value.getSeconds() * 1000000 + value.getNanos() / 1000;
  literalExpr->set_timestamp(micros);
  literalExpr->set_nullable(false);
  return *literalExpr;
}

template <>
const ::substrait::Expression_Literal&
toSubstraitNotNullLiteral<TypeKind::VARCHAR>(
    google::protobuf::Arena& arena,
    const bolt::StringView& value) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  ::substrait::Expression_Literal::VarChar* sVarChar =
      new ::substrait::Expression_Literal::VarChar();
  sVarChar->set_value(value.data());
  sVarChar->set_length(value.size());
  literalExpr->set_allocated_var_char(sVarChar);
  literalExpr->set_nullable(false);
  return *literalExpr;
}

template <TypeKind Kind>
void arrayVectorToLiteral(
    google::protobuf::Arena& arena,
    const ArrayVector* arrayVector,
    ::substrait::Expression_Literal_List* listLiteral,
    vector_size_t offset,
    vector_size_t size) {
  using T = typename TypeTraits<Kind>::NativeType;
  auto elements = arrayVector->elements()->as<SimpleVector<T>>();
  for (auto i = offset; i < offset + size; ++i) {
    ::substrait::Expression_Literal* childLiteral = listLiteral->add_values();
    if (elements->isNullAt(i)) {
      childLiteral->MergeFrom(toSubstraitNullLiteral(arena, Kind));
    } else {
      childLiteral->MergeFrom(
          toSubstraitNotNullLiteral<Kind>(arena, elements->valueAt(i)));
    }
  }
}

template <TypeKind kind>
void convertVectorValue(
    google::protobuf::Arena& arena,
    const bolt::VectorPtr& vectorValue,
    ::substrait::Expression_Literal_Struct* litValue,
    ::substrait::Expression_Literal* substraitField) {
  const TypePtr& childType = vectorValue->type();

  using T = typename TypeTraits<kind>::NativeType;

  auto childToFlatVec = vectorValue->as<SimpleVector<T>>();

  //  Get the batchSize and convert each value in it.
  vector_size_t flatVecSize = childToFlatVec->size();
  for (int64_t i = 0; i < flatVecSize; i++) {
    substraitField = litValue->add_fields();
    if (childToFlatVec->isNullAt(i)) {
      // Process the null value.
      substraitField->MergeFrom(
          toSubstraitNullLiteral(arena, childType->kind()));
    } else {
      substraitField->MergeFrom(
          toSubstraitNotNullLiteral<kind>(arena, childToFlatVec->valueAt(i)));
    }
  }
}
} // namespace

const ::substrait::Expression& BoltToSubstraitExprConvertor::toSubstraitExpr(
    google::protobuf::Arena& arena,
    const core::TypedExprPtr& expr,
    const RowTypePtr& inputType) {
  ::substrait::Expression* substraitExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression>(&arena);
  if (auto constExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
    substraitExpr->mutable_literal()->MergeFrom(
        toSubstraitExpr(arena, constExpr));
    return *substraitExpr;
  }
  if (auto callTypeExpr =
          std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    substraitExpr->MergeFrom(toSubstraitExpr(arena, callTypeExpr, inputType));
    return *substraitExpr;
  }
  if (auto fieldExpr =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
    substraitExpr->mutable_selection()->MergeFrom(
        toSubstraitExpr(arena, fieldExpr, inputType));

    return *substraitExpr;
  }
  if (auto castExpr =
          std::dynamic_pointer_cast<const core::CastTypedExpr>(expr)) {
    substraitExpr->mutable_cast()->MergeFrom(
        toSubstraitExpr(arena, castExpr, inputType));

    return *substraitExpr;
  }

  BOLT_UNSUPPORTED("Unsupported Expr '{}' in Substrait", expr->toString());
}

const ::substrait::Expression_Cast&
BoltToSubstraitExprConvertor::toSubstraitExpr(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::CastTypedExpr>& castExpr,
    const RowTypePtr& inputType) {
  ::substrait::Expression_Cast* substraitCastExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Cast>(
          &arena);
  std::vector<core::TypedExprPtr> castExprInputs = castExpr->inputs();

  substraitCastExpr->mutable_type()->MergeFrom(
      typeConvertor_->toSubstraitType(arena, castExpr->type()));

  for (auto& arg : castExprInputs) {
    substraitCastExpr->mutable_input()->MergeFrom(
        toSubstraitExpr(arena, arg, inputType));
  }
  if (castExpr->nullOnFailure()) {
    substraitCastExpr->set_failure_behavior(
        ::substrait::
            Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_RETURN_NULL);
  } else {
    substraitCastExpr->set_failure_behavior(
        ::substrait::
            Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION);
  }
  return *substraitCastExpr;
}

const ::substrait::Expression_FieldReference&
BoltToSubstraitExprConvertor::toSubstraitExpr(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::FieldAccessTypedExpr>& fieldExpr,
    const RowTypePtr& inputType) {
  ::substrait::Expression_FieldReference* substraitFieldExpr =
      google::protobuf::Arena::CreateMessage<
          ::substrait::Expression_FieldReference>(&arena);

  std::string exprName = fieldExpr->name();

  ::substrait::Expression_ReferenceSegment_StructField* directStruct =
      substraitFieldExpr->mutable_direct_reference()->mutable_struct_field();

  directStruct->set_field(inputType->getChildIdx(exprName));
  return *substraitFieldExpr;
}

const ::substrait::Expression& BoltToSubstraitExprConvertor::toSubstraitExpr(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::CallTypedExpr>& callTypeExpr,
    const RowTypePtr& inputType) {
  ::substrait::Expression* substraitExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression>(&arena);

  auto inputs = callTypeExpr->inputs();
  auto& functionName = callTypeExpr->name();

  if (functionName != "if" && functionName != "switch") {
    ::substrait::Expression_ScalarFunction* scalarExpr =
        substraitExpr->mutable_scalar_function();

    std::vector<TypePtr> types;
    types.reserve(callTypeExpr->inputs().size());
    for (auto& typedExpr : callTypeExpr->inputs()) {
      types.emplace_back(typedExpr->type());
    }

    scalarExpr->set_function_reference(
        extensionCollector_->getReferenceNumber(functionName, types));

    for (auto& arg : inputs) {
      scalarExpr->add_arguments()->mutable_value()->MergeFrom(
          toSubstraitExpr(arena, arg, inputType));
    }

    scalarExpr->mutable_output_type()->MergeFrom(
        typeConvertor_->toSubstraitType(arena, callTypeExpr->type()));

  } else {
    // For today's version of Substrait, you'd have to use IfThen if you need
    // the switch cases to be call typed expression.

    size_t inputsSize = callTypeExpr->inputs().size();
    bool hasElseInput = inputsSize % 2 == 1;

    auto ifThenExpr = substraitExpr->mutable_if_then();
    for (int i = 0; i < inputsSize / 2; i++) {
      auto ifClauseExpr = ifThenExpr->add_ifs();
      ifClauseExpr->mutable_if_()->MergeFrom(
          toSubstraitExpr(arena, callTypeExpr->inputs().at(i * 2), inputType));
      ifClauseExpr->mutable_then()->MergeFrom(toSubstraitExpr(
          arena, callTypeExpr->inputs().at(i * 2 + 1), inputType));
    }
    if (hasElseInput) {
      ifThenExpr->mutable_else_()->MergeFrom(toSubstraitExpr(
          arena, callTypeExpr->inputs().at(inputsSize - 1), inputType));
    }
  }

  return *substraitExpr;
}

const ::substrait::Expression_Literal&
BoltToSubstraitExprConvertor::toSubstraitExpr(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::ConstantTypedExpr>& constExpr,
    ::substrait::Expression_Literal_Struct* litValue) {
  if (constExpr->hasValueVector()) {
    return toSubstraitLiteral(arena, constExpr->valueVector(), litValue);
  } else {
    return toSubstraitLiteral(arena, constExpr->value());
  }
}

const ::substrait::Expression_Literal&
BoltToSubstraitExprConvertor::toSubstraitLiteral(
    google::protobuf::Arena& arena,
    const bolt::variant& variantValue) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);

  if (variantValue.isNull()) {
    literalExpr->MergeFrom(toSubstraitNullLiteral(arena, variantValue.kind()));
  } else {
    literalExpr->MergeFrom(toSubstraitNotNullLiteral(arena, variantValue));
  }
  return *literalExpr;
}

const ::substrait::Expression_Literal_List&
BoltToSubstraitExprConvertor::toSubstraitLiteralList(
    google::protobuf::Arena& arena,
    const ArrayVector* arrayVector,
    vector_size_t row) {
  ::substrait::Expression_Literal_List* listLiteral =
      google::protobuf::Arena::CreateMessage<
          ::substrait::Expression_Literal_List>(&arena);
  auto size = arrayVector->sizeAt(row);
  if (size == 0) {
    return *listLiteral;
  }
  auto offset = arrayVector->offsetAt(row);
  if (arrayVector->elements()->isScalar()) {
    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        arrayVectorToLiteral,
        arrayVector->elements()->type()->kind(),
        arena,
        arrayVector,
        listLiteral,
        offset,
        size);
    return *listLiteral;
  }

  if (arrayVector->elements()->typeKind() == TypeKind::ARRAY) {
    auto encoding = arrayVector->elements()->encoding();
    if (encoding == VectorEncoding::Simple::ARRAY) {
      auto nestedArrayVector = arrayVector->elements()->as<ArrayVector>();
      BOLT_CHECK_NOT_NULL(nestedArrayVector);
      for (auto i = offset; i < offset + size; ++i) {
        ::substrait::Expression_Literal* literal = listLiteral->add_values();
        literal->set_allocated_list(
            const_cast<::substrait::Expression_Literal_List*>(
                &toSubstraitLiteralList(arena, nestedArrayVector, i)));
      }
      return *listLiteral;
    }
  }
  BOLT_NYI(
      "Complex type literals are not supported: {}",
      arrayVector->elements()->type()->toString());
}

const ::substrait::Expression_Literal&
BoltToSubstraitExprConvertor::toSubstraitLiteralComplex(
    google::protobuf::Arena& arena,
    const std::shared_ptr<ConstantVector<ComplexType>>& constantVector) {
  ::substrait::Expression_Literal* substraitField =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  if (constantVector->typeKind() == TypeKind::ARRAY) {
    if (constantVector->isNullAt(0)) {
      // Process the null value.
      substraitField->MergeFrom(
          toSubstraitNullLiteral(arena, constantVector->typeKind()));
      return *substraitField;
    }
    auto encoding = constantVector->valueVector()->encoding();
    if (encoding == VectorEncoding::Simple::ARRAY) {
      auto arrayVector = constantVector->valueVector()->as<ArrayVector>();
      substraitField->mutable_list()->MergeFrom(
          toSubstraitLiteralList(arena, arrayVector, constantVector->index()));
      return *substraitField;
    }
  }
  BOLT_NYI(
      "Complex type literals are not supported: {}",
      constantVector->type()->toString());
}

const ::substrait::Expression_Literal&
BoltToSubstraitExprConvertor::toSubstraitLiteral(
    google::protobuf::Arena& arena,
    const bolt::VectorPtr& vectorValue,
    ::substrait::Expression_Literal_Struct* litValue) {
  ::substrait::Expression_Literal* substraitField =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  if (vectorValue->isScalar()) {
    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        convertVectorValue,
        vectorValue->type()->kind(),
        arena,
        vectorValue,
        litValue,
        substraitField);
    return *substraitField;
  }

  if (auto constantVector =
          std::dynamic_pointer_cast<ConstantVector<ComplexType>>(vectorValue)) {
    return toSubstraitLiteralComplex(arena, constantVector);
  }
  return *substraitField;
}

} // namespace bytedance::bolt::substrait
