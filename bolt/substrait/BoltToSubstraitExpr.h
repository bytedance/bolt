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

#pragma once

#include "bolt/core/PlanNode.h"

#include "bolt/substrait/BoltToSubstraitType.h"
#include "bolt/substrait/SubstraitExtensionCollector.h"
#include "bolt/substrait/proto/substrait/algebra.pb.h"
#include "bolt/vector/ConstantVector.h"
namespace bytedance::bolt::substrait {

class BoltToSubstraitExprConvertor {
 public:
  explicit BoltToSubstraitExprConvertor(
      const SubstraitExtensionCollectorPtr& extensionCollector)
      : typeConvertor_(
            std::make_shared<BoltToSubstraitTypeConvertor>(extensionCollector)),
        extensionCollector_(extensionCollector) {}

  /// Convert Bolt Expression to Substrait Expression.
  /// @param arena Arena to use for allocating Substrait plan objects.
  /// @param expr Bolt expression needed to be converted.
  /// @param inputType The input row Type of the current processed node,
  /// which also equals the output row type of the previous node of the current.
  /// @return A pointer to Substrait expression object allocated on the arena
  /// and representing the input Bolt expression.
  const ::substrait::Expression& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const core::TypedExprPtr& expr,
      const RowTypePtr& inputType);

  /// Convert Bolt Constant Expression to Substrait
  /// Literal Expression.
  /// @param arena Arena to use for allocating Substrait plan objects.
  /// @param constExpr Bolt Constant expression needed to be converted.
  /// @param litValue The Struct that returned literal expression belong to.
  /// @return A pointer to Substrait Literal expression object allocated on
  /// the arena and representing the input Bolt Constant expression.
  const ::substrait::Expression_Literal& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::ConstantTypedExpr>& constExpr,
      ::substrait::Expression_Literal_Struct* litValue = nullptr);

  /// Convert Bolt FieldAccessTypedExpr to Substrait FieldReference Expression.
  const ::substrait::Expression_FieldReference& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::FieldAccessTypedExpr>& fieldExpr,
      const RowTypePtr& inputType);

  /// Convert Bolt vector to Substrait literal.
  const ::substrait::Expression_Literal& toSubstraitLiteral(
      google::protobuf::Arena& arena,
      const bolt::VectorPtr& vectorValue,
      ::substrait::Expression_Literal_Struct* litValue);

 private:
  /// Convert Bolt Cast Expression to Substrait Cast Expression.
  const ::substrait::Expression_Cast& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::CastTypedExpr>& castExpr,
      const RowTypePtr& inputType);

  /// Convert Bolt CallTypedExpr Expression to Substrait Expression.
  const ::substrait::Expression& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::CallTypedExpr>& callTypeExpr,
      const RowTypePtr& inputType);

  /// Convert Bolt variant to Substrait Literal Expression.
  const ::substrait::Expression_Literal& toSubstraitLiteral(
      google::protobuf::Arena& arena,
      const bolt::variant& variantValue);

  /// Convert values in Bolt array vector to Substrait Literal List.
  const ::substrait::Expression_Literal_List& toSubstraitLiteralList(
      google::protobuf::Arena& arena,
      const ArrayVector* arrayVector,
      vector_size_t row);

  /// Convert values in Bolt complex vector to Substrait Literal.
  const ::substrait::Expression_Literal& toSubstraitLiteralComplex(
      google::protobuf::Arena& arena,
      const std::shared_ptr<ConstantVector<ComplexType>>& constantVector);

  BoltToSubstraitTypeConvertorPtr typeConvertor_;

  SubstraitExtensionCollectorPtr extensionCollector_;
};

using BoltToSubstraitExprConvertorPtr =
    std::shared_ptr<BoltToSubstraitExprConvertor>;

} // namespace bytedance::bolt::substrait
