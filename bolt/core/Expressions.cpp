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

#include "bolt/core/Expressions.h"
#include <bit>
#include "bolt/common/encode/Base64.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/VectorSaver.h"
namespace bytedance::bolt::core {

namespace {
TypePtr deserializeType(const folly::dynamic& obj, void* context) {
  return ISerializable::deserialize<Type>(obj["type"]);
}

std::vector<TypedExprPtr> deserializeInputs(
    const folly::dynamic& obj,
    void* context) {
  if (obj.count("inputs")) {
    return ISerializable::deserialize<std::vector<ITypedExpr>>(
        obj["inputs"], context);
  }

  return {};
}

template <typename T>
bool equalFloatingPointConstants(T left, T right) {
  using Bits =
      std::conditional_t<sizeof(T) == sizeof(uint32_t), uint32_t, uint64_t>;
  return std::bit_cast<Bits>(left) == std::bit_cast<Bits>(right);
}

// SQL equality can discard observable constant data, including zero signs and
// MAP entry order. Expression deduplication must preserve these values.
bool equalConstantValues(
    const BaseVector* left,
    vector_size_t leftIndex,
    const BaseVector* right,
    vector_size_t rightIndex) {
  if (left == right && leftIndex == rightIndex) {
    return true;
  }
  if (!left || !right) {
    return left == right;
  }
  const bool leftNull = left->isNullAt(leftIndex);
  const bool rightNull = right->isNullAt(rightIndex);
  if (leftNull || rightNull) {
    return leftNull == rightNull;
  }
  leftIndex = left->wrappedIndex(leftIndex);
  rightIndex = right->wrappedIndex(rightIndex);
  left = left->wrappedVector();
  right = right->wrappedVector();
  const auto equalElements = [&](const VectorPtr& leftElements,
                                 vector_size_t leftOffset,
                                 const VectorPtr& rightElements,
                                 vector_size_t rightOffset,
                                 vector_size_t size) {
    for (vector_size_t i = 0; i < size; ++i) {
      if (!equalConstantValues(
              leftElements.get(),
              leftOffset + i,
              rightElements.get(),
              rightOffset + i)) {
        return false;
      }
    }
    return true;
  };
  switch (left->typeKind()) {
    case TypeKind::REAL:
      return equalFloatingPointConstants(
          left->as<SimpleVector<float>>()->valueAt(leftIndex),
          right->as<SimpleVector<float>>()->valueAt(rightIndex));
    case TypeKind::DOUBLE:
      return equalFloatingPointConstants(
          left->as<SimpleVector<double>>()->valueAt(leftIndex),
          right->as<SimpleVector<double>>()->valueAt(rightIndex));
    case TypeKind::ROW: {
      const auto* leftRow = left->as<RowVector>();
      const auto* rightRow = right->as<RowVector>();
      if (leftRow->childrenSize() != rightRow->childrenSize()) {
        return false;
      }
      for (column_index_t i = 0; i < leftRow->childrenSize(); ++i) {
        if (!equalConstantValues(
                leftRow->childAt(i).get(),
                leftIndex,
                rightRow->childAt(i).get(),
                rightIndex)) {
          return false;
        }
      }
      return true;
    }
    case TypeKind::ARRAY:
    case TypeKind::MAP: {
      const auto* leftArray = left->as<ArrayVectorBase>();
      const auto* rightArray = right->as<ArrayVectorBase>();
      const auto size = leftArray->sizeAt(leftIndex);
      if (size != rightArray->sizeAt(rightIndex)) {
        return false;
      }
      const auto leftOffset = leftArray->offsetAt(leftIndex);
      const auto rightOffset = rightArray->offsetAt(rightIndex);
      if (left->typeKind() == TypeKind::ARRAY) {
        return equalElements(
            left->as<ArrayVector>()->elements(),
            leftOffset,
            right->as<ArrayVector>()->elements(),
            rightOffset,
            size);
      }
      const auto* leftMap = left->as<MapVector>();
      const auto* rightMap = right->as<MapVector>();
      return equalElements(
                 leftMap->mapKeys(),
                 leftOffset,
                 rightMap->mapKeys(),
                 rightOffset,
                 size) &&
          equalElements(
                 leftMap->mapValues(),
                 leftOffset,
                 rightMap->mapValues(),
                 rightOffset,
                 size);
    }
    default:
      return left->equalValueAt(right, leftIndex, rightIndex);
  }
}
} // namespace

folly::dynamic ITypedExpr::serializeBase(std::string_view name) const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = name;
  obj["type"] = type_->serialize();

  if (!inputs_.empty()) {
    folly::dynamic serializedInputs = folly::dynamic::array;
    for (const auto& input : inputs_) {
      serializedInputs.push_back(input->serialize());
    }

    obj["inputs"] = serializedInputs;
  }

  return obj;
}

// static
void ITypedExpr::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();

  registry.Register("CallTypedExpr", core::CallTypedExpr::create);
  registry.Register("CastTypedExpr", core::CastTypedExpr::create);
  registry.Register("ConcatTypedExpr", core::ConcatTypedExpr::create);
  registry.Register("ConstantTypedExpr", core::ConstantTypedExpr::create);
  registry.Register("DereferenceTypedExpr", core::DereferenceTypedExpr::create);
  registry.Register("FieldAccessTypedExpr", core::FieldAccessTypedExpr::create);
  registry.Register("InputTypedExpr", core::InputTypedExpr::create);
  registry.Register("LambdaTypedExpr", core::LambdaTypedExpr::create);
}

folly::dynamic InputTypedExpr::serialize() const {
  return ITypedExpr::serializeBase("InputTypedExpr");
}

// static
TypedExprPtr InputTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto type = core::deserializeType(obj, context);

  return std::make_shared<InputTypedExpr>(std::move(type));
}

bool ConstantTypedExpr::equals(const ITypedExpr& other) const {
  const auto* casted = dynamic_cast<const ConstantTypedExpr*>(&other);
  if (!casted || *type() != *casted->type() ||
      hasValueVector() != casted->hasValueVector()) {
    return false;
  }
  if (hasValueVector()) {
    return equalConstantValues(
        valueVector_.get(), 0, casted->valueVector_.get(), 0);
  }
  if (value_.kind() != casted->value_.kind()) {
    return false;
  }
  if (!value_.isNull() && !casted->value_.isNull()) {
    if (value_.kind() == TypeKind::REAL) {
      return equalFloatingPointConstants(
          value_.value<TypeKind::REAL>(),
          casted->value_.value<TypeKind::REAL>());
    }
    if (value_.kind() == TypeKind::DOUBLE) {
      return equalFloatingPointConstants(
          value_.value<TypeKind::DOUBLE>(),
          casted->value_.value<TypeKind::DOUBLE>());
    }
  }
  return value_ == casted->value_;
}

folly::dynamic ConstantTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("ConstantTypedExpr");
  if (valueVector_) {
    std::ostringstream out;
    saveVector(*valueVector_, out);
    auto serializedValue = out.str();
    obj["valueVector"] = encoding::Base64::encode(
        serializedValue.data(), serializedValue.size());
  } else {
    obj["value"] = value_.serialize();
  }

  return obj;
}

// static
TypedExprPtr ConstantTypedExpr::create(
    const folly::dynamic& obj,
    void* context) {
  auto type = core::deserializeType(obj, context);

  if (obj.count("value")) {
    auto value = variant::create(obj["value"]);
    return std::make_shared<ConstantTypedExpr>(std::move(type), value);
  }

  auto encodedData = obj["valueVector"].asString();
  auto serializedData = encoding::Base64::decode(encodedData);
  std::istringstream dataStream(serializedData);

  auto* pool = static_cast<memory::MemoryPool*>(context);

  return std::make_shared<ConstantTypedExpr>(restoreVector(dataStream, pool));
}

folly::dynamic CallTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("CallTypedExpr");
  obj["functionName"] = name_;
  return obj;
}

// static
TypedExprPtr CallTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);

  return std::make_shared<CallTypedExpr>(
      std::move(type), std::move(inputs), obj["functionName"].asString());
}

folly::dynamic FieldAccessTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("FieldAccessTypedExpr");
  obj["fieldName"] = name_;
  return obj;
}

// static
TypedExprPtr FieldAccessTypedExpr::create(
    const folly::dynamic& obj,
    void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);
  BOLT_CHECK_LE(inputs.size(), 1);

  auto name = obj["fieldName"].asString();

  if (inputs.empty()) {
    return std::make_shared<FieldAccessTypedExpr>(std::move(type), name);
  } else {
    return std::make_shared<FieldAccessTypedExpr>(
        std::move(type), std::move(inputs[0]), name);
  }
}

folly::dynamic DereferenceTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("DereferenceTypedExpr");
  obj["fieldIndex"] = index_;
  return obj;
}

// static
TypedExprPtr DereferenceTypedExpr::create(
    const folly::dynamic& obj,
    void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);
  BOLT_CHECK_EQ(inputs.size(), 1);

  uint32_t index = obj["fieldIndex"].asInt();

  return std::make_shared<DereferenceTypedExpr>(
      std::move(type), std::move(inputs[0]), index);
}

folly::dynamic ConcatTypedExpr::serialize() const {
  return ITypedExpr::serializeBase("ConcatTypedExpr");
}

// static
TypedExprPtr ConcatTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);

  return std::make_shared<ConcatTypedExpr>(
      type->asRow().names(), std::move(inputs));
}

folly::dynamic LambdaTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("LambdaTypedExpr");
  obj["signature"] = signature_->serialize();
  obj["body"] = body_->serialize();
  return obj;
}

// static
TypedExprPtr LambdaTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto signature = ISerializable::deserialize<Type>(obj["signature"]);
  auto body = ISerializable::deserialize<ITypedExpr>(obj["body"], context);

  return std::make_shared<LambdaTypedExpr>(
      asRowType(signature), std::move(body));
}

folly::dynamic CastTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("CastTypedExpr");
  obj["nullOnFailure"] = nullOnFailure_;
  return obj;
}

// static
TypedExprPtr CastTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);

  return std::make_shared<CastTypedExpr>(
      std::move(type),
      std::move(inputs),
      obj.count("nullOnFailure") && obj["nullOnFailure"].asBool());
}

} // namespace bytedance::bolt::core
