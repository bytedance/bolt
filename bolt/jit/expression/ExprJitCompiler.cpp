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

// TODO:
// Refactor LLVM IR generation for expression evaluation

#ifdef ENABLE_BOLT_EXPR_JIT

#include "bolt/jit/expression/ExprJitCompiler.h"
#include "bolt/expression/CastExpr.h"
#include "bolt/expression/ConjunctExpr.h"
#include "bolt/expression/ConstantExpr.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/FieldReference.h"
#include "bolt/expression/LambdaExpr.h"
#include "bolt/expression/SwitchExpr.h"
#include "bolt/expression/TryExpr.h"
#include "bolt/type/Type.h"

#include "bolt/vector/ConstantVector.h"

#include <thrust/jit/data_types.h>
#include <thrust/jit/expr.h>
#include <thrust/jit/function.h>
#include <thrust/jit/ir_compile_expr.h>

#include <glog/logging.h>
#include <type_traits>
namespace bytedance::bolt {

struct RawBufferHolder {
  std::vector<void*> buffers;
  std::vector<void*> children;
};

// TODO： remove this, use the std::function wrapper which will manage the
// resource holder's life cycle.
void JitBoltVectorFunction::operator()(
    const std::vector<VectorPtr>& args,
    VectorPtr* result) {
  std::vector<RawBufferHolder> rawArgs;
  std::vector<thrust::jit::VectorData> jitArgs;

  auto decode = [&rawArgs](const VectorPtr& arg) {
    auto raw = (void*)arg->valuesAsVoid();
    auto nulls = (void*)arg->rawNulls();
    RawBufferHolder holder;
    holder.buffers.emplace_back(nulls);
    holder.buffers.emplace_back(raw);

    if (arg->typeKind() == TypeKind::VARCHAR ||
        arg->typeKind() == TypeKind::VARBINARY) {
      // string view buffer
      //  auto strings = std::static_pointer_cast<FlatVector<StringView>>(

      for (auto& buffer : arg->asFlatVector<StringView>()->stringBuffers()) {
        if (buffer->refCount() > 1) {
          // return false;
        }
      }
    }
    rawArgs.emplace_back(std::move(holder));
  };
  for (auto&& arg : args) {
    decode(arg);
  }
  decode(*result);
  for (auto&& raw : rawArgs) {
    thrust::jit::VectorData data;
    data.buffers = (void**)(raw.buffers.data());
    if (raw.children.size() != 0) {
      data.children = (void**)(raw.children.data());
    }
    jitArgs.emplace_back(std::move(data));
  }
  auto size = args[0]->size();
  auto* jitRawArgs = jitArgs.data();

  // TODO: refine code
  ((thrust::jit::JITedFunction)jitFunc)(size, jitRawArgs);
}

} // namespace bytedance::bolt
namespace bytedance::bolt::jit {

using JitTypePtr = std::unique_ptr<thrust::jit::IDataType>;
using JitTypes = std::vector<JitTypePtr>;
using BoltTypePtr = TypePtr;
using BoltTypes = std::vector<std::shared_ptr<const Type>>;
using JitIFunctionUPtr = std::unique_ptr<thrust::jit::IFunction>;

struct FunctionResourceWrapper {
  std::shared_ptr<thrust::jit::JitFunctionHolder> jitHolder;
};

void bolt_call_function_wrapper(
    FunctionResourceWrapper wrapper,
    const std::vector<VectorPtr>& args,
    VectorPtr* result) {
  if (wrapper.jitHolder == nullptr) {
    return;
    // TODO
  }
}

class ExprConverter {
 public:
  thrust::jit::ExprPtr convertExprToJitExpr(const exec::Expr* expr) {
    auto&& inputs = expr->inputs();

    std::vector<thrust::jit::ExprPtr> children;
    if (inputs.size() > 0) {
      for (auto&& input : inputs) {
        auto&& child = convertExprToJitExpr(input.get());
        children.emplace_back(std::move(child));
      }
    }
    if (auto constValue = dynamic_cast<const exec::ConstantExpr*>(expr)) {
      return convertConstVariant(constValue);
    } else if (
        auto fieldRef = dynamic_cast<const exec::FieldReference*>(expr)) {
      auto fieldIdx = argIdx_;
      if (fieldsArgIdx_.contains(fieldRef->name())) {
        fieldIdx = fieldsArgIdx_[fieldRef->name()];
      } else {
        fieldIdx = argIdx_++;
        fieldsArgIdx_[fieldRef->name()] = fieldIdx;
      }

      auto r = std::make_shared<thrust::jit::FieldRefExpr>(
          convertType(fieldRef->type()), fieldIdx, fieldRef->name());
      return std::move(r);
    } else if (auto switchExpr = dynamic_cast<const exec::SwitchExpr*>(expr)) {
      BOLT_UNSUPPORTED("Unsupported");
    } else if (
        auto conjunctExpr = dynamic_cast<const exec::ConjunctExpr*>(expr)) {
      // Convert the flatten conjunct expr to JIT's binary tree
      if (expr->inputs().size() < 2) {
        throw std::logic_error("conjunct expr should has more than two args.");
      }

      thrust::jit::ExprPtr left = children[0];
      for (size_t i = 1; i < children.size(); i++) {
        auto jitTypedFunc =
            convertFunction(expr->name(), {BOOLEAN(), BOOLEAN()}, BOOLEAN());
        std::vector<thrust::jit::ExprPtr> newChildren{left, children[i]};
        left = std::make_shared<thrust::jit::FuncExpr>(
            std::move(jitTypedFunc), newChildren);
      }

      return left;
    } else if (auto lambdaExpr = dynamic_cast<const exec::LambdaExpr*>(expr)) {
      BOLT_UNSUPPORTED("Unsupported");
    } else if (auto tryExpr = dynamic_cast<const exec::TryExpr*>(expr)) {
      BOLT_UNSUPPORTED("Unsupported");
    } else {
      // Now, it a function call
      auto call = expr;

      // TODO: some names may need to be mapped.
      auto&& name = expr->name();
      auto jitTypedFunc =
          convertFunction(name, getInputsTypes(expr), call->type());

      return std::make_shared<thrust::jit::FuncExpr>(
          std::move(jitTypedFunc), children);
    }
    // TODO cast

    return nullptr;
  }

  JitTypePtr convertType(const std::shared_ptr<const Type>& t) {
    switch (t->kind()) {
      case TypeKind::BOOLEAN:
        return std::make_unique<thrust::jit::BoolDataType>();

      case TypeKind::TINYINT:
        return std::make_unique<thrust::jit::Int8DataType>();

      case TypeKind::SMALLINT:
        return std::make_unique<thrust::jit::Int16DataType>();

      case TypeKind::INTEGER:
        return std::make_unique<thrust::jit::Int32DataType>();

      case TypeKind::BIGINT:
        return std::make_unique<thrust::jit::Int64DataType>();

      case TypeKind::REAL:
        return std::make_unique<thrust::jit::FloatDataType>();

      case TypeKind::DOUBLE:
        return std::make_unique<thrust::jit::DoubleDataType>();

      case TypeKind::VARCHAR:
        return std::make_unique<thrust::jit::StringDataType>();

      case TypeKind::TIMESTAMP:
        return std::make_unique<thrust::jit::TimestampDataType>();

      case TypeKind::DATE:
        return std::make_unique<thrust::jit::DateDataType>();

      case TypeKind::SHORT_DECIMAL:
        // TODO: precision scale
        return std::make_unique<thrust::jit::Decimal64DataType>(6);

      case TypeKind::LONG_DECIMAL:
      case TypeKind::VARBINARY:
      default:
        BOLT_UNSUPPORTED("Unsupported");
        break;
    }
  }

  // If result is not explicitly specified, thrust-jit will reduce the result
  // type automatically.
  JitIFunctionUPtr convertFunction(
      const std::string& name,
      const BoltTypes& argsType,
      const TypePtr& resultType = nullptr) {
    JitTypes argsJitTypes;
    for (auto&& t : argsType) {
      argsJitTypes.emplace_back(convertType(t));
    }

    JitTypePtr resultJitType =
        (resultType == nullptr ? nullptr : convertType(resultType));

    return thrust::jit::MakeFunction(
        name, std::move(argsJitTypes), std::move(resultJitType));
  }

  thrust::jit::ExprPtr convertTypedExprToJitExpr(
      const core::TypedExprPtr& expr) {
    if (auto concat = dynamic_cast<const core::ConcatTypedExpr*>(expr.get())) {
      BOLT_UNSUPPORTED("Unsupported");
    } else if (
        auto cast = dynamic_cast<const core::CastTypedExpr*>(expr.get())) {
      BOLT_UNSUPPORTED("TODO: add explicit cast in Jit");
    } else if (
        auto call = dynamic_cast<const core::CallTypedExpr*>(expr.get())) {
      // TODO: some names need to be mapped.
      auto name = call->name();
      std::vector<TypePtr> inputTypes;
      for (auto&& input : expr->inputs()) {
        inputTypes.emplace_back(input->type());
      }
      auto jitTypedFunc = convertFunction(name, inputTypes, expr->type());

      std::vector<thrust::jit::ExprPtr> children;
      for (auto&& child : expr->inputs()) {
        children.emplace_back(convertTypedExprToJitExpr(child));
      }
      // type comes from func
      return std::make_shared<thrust::jit::FuncExpr>(
          std::move(jitTypedFunc), children);
    } else if (
        auto access =
            dynamic_cast<const core::FieldAccessTypedExpr*>(expr.get())) {
      return std::make_shared<thrust::jit::FieldRefExpr>(
          convertType(expr->type()), -1, access->name());
    } else if (
        auto row = dynamic_cast<const core::InputTypedExpr*>(expr.get())) {
      BOLT_UNSUPPORTED("Unsupported");
    } else if (
        auto constant =
            dynamic_cast<const core::ConstantTypedExpr*>(expr.get())) {
    } else if (
        auto lambda = dynamic_cast<const core::LambdaTypedExpr*>(expr.get())) {
      BOLT_UNSUPPORTED("Unsupported");
    } else {
      BOLT_UNSUPPORTED("Unknown typed expression");
    }

    return nullptr;
  }

  // utils:
  template <typename T>
  auto getConstantVariant(const exec::ConstantExpr* constExpr) {
    auto literal = constExpr->value();
    auto constVector = std::dynamic_pointer_cast<ConstantVector<T>>(literal);
    auto res = constVector->valueAtFast(0);
    return res;
  }

  std::vector<TypePtr> getInputsTypes(const exec::Expr* expr) {
    std::vector<TypePtr> inputTypes;
    for (auto&& input : expr->inputs()) {
      inputTypes.emplace_back(input->type());
    }
    return inputTypes;
  }

  thrust::jit::ExprPtr convertConstVariant(const exec::ConstantExpr* expr) {
    auto constValue = dynamic_cast<const exec::ConstantExpr*>(expr);
    if (constValue == nullptr)
      return nullptr;

    auto ty = expr->type();
    if (ty->isDate()) {
      auto d = getConstantVariant<Date>(constValue);
      auto constJitExpr = std::make_shared<thrust::jit::LiteralExpr>(
          std::make_unique<thrust::jit::DateDataType>(), d.days());
      return constJitExpr;
    } else if (ty->isTimestamp()) {
      BOLT_UNSUPPORTED("Unsupported Timestamp type... // TODO");
    } else if (ty->isShortDecimal()) {
      // TODO
      BOLT_UNSUPPORTED("Unsupported ShortDecimal type... // TODO");
    } else if (ty->isVarchar()) {
      thrust::jit::NativeLiteralType constVariant =
          getConstantVariant<StringView>(constValue).str();
      auto constJitExpr = std::make_shared<thrust::jit::LiteralExpr>(
          std::make_unique<thrust::jit::StringDataType>(), constVariant);
      return constJitExpr;
    } else if (ty->isVarbinary()) {
      // TODO: same as varchar ? std::array<std::byte>> ?
      BOLT_UNSUPPORTED("Unsupported Varbinary type... // TODO");
    }
    // primitive types start
    else if (ty->isBigint()) {
      thrust::jit::NativeLiteralType constVariant =
          getConstantVariant<int64_t>(constValue);
      auto constJitExpr = std::make_shared<thrust::jit::LiteralExpr>(
          std::make_unique<thrust::jit::UInt64DataType>(), constVariant);
      return constJitExpr;
    } else if (ty->isBoolean()) {
      // TODO: check it
      thrust::jit::NativeLiteralType constVariant =
          getConstantVariant<bool>(constValue);
      auto constJitExpr = std::make_shared<thrust::jit::LiteralExpr>(
          std::make_unique<thrust::jit::BoolDataType>(), constVariant);
      return constJitExpr;
    } else if (ty->isInteger()) {
      thrust::jit::NativeLiteralType constVariant =
          getConstantVariant<int32_t>(constValue);
      auto constJitExpr = std::make_shared<thrust::jit::LiteralExpr>(
          std::make_unique<thrust::jit::Int32DataType>(), constVariant);
      return constJitExpr;
    } else if (ty->isTinyint()) {
      thrust::jit::NativeLiteralType constVariant =
          getConstantVariant<int8_t>(constValue);
      auto constJitExpr = std::make_shared<thrust::jit::LiteralExpr>(
          std::make_unique<thrust::jit::Int32DataType>(), constVariant);
      return constJitExpr;
    } else if (ty->isSmallint()) {
      thrust::jit::NativeLiteralType constVariant =
          getConstantVariant<int16_t>(constValue);
      auto constJitExpr = std::make_shared<thrust::jit::LiteralExpr>(
          std::make_unique<thrust::jit::Int16DataType>(), constVariant);
      return constJitExpr;
    } else if (ty->isReal()) {
      thrust::jit::NativeLiteralType constVariant =
          getConstantVariant<float>(constValue);
      auto constJitExpr = std::make_shared<thrust::jit::LiteralExpr>(
          std::make_unique<thrust::jit::Int16DataType>(), constVariant);
      return constJitExpr;
    } else if (ty->isDouble()) {
      thrust::jit::NativeLiteralType constVariant =
          getConstantVariant<double>(constValue);
      auto constJitExpr = std::make_shared<thrust::jit::LiteralExpr>(
          std::make_unique<thrust::jit::Int16DataType>(), constVariant);
      return constJitExpr;
    } else if (ty->isLongDecimal()) {
      BOLT_UNSUPPORTED("Unsupported LongDeciaml type... // TODO");
    } else if (ty->isArray()) {
      BOLT_UNSUPPORTED("Unsupported Array type... // TODO");
    } else if (ty->isMap()) {
      BOLT_UNSUPPORTED("Unsupported Map type... // TODO");
    } else if (ty->isRow()) { // struct
      BOLT_UNSUPPORTED("Unsupported Row type... // TODO");
    } else if (ty->isOpaque()) {
      BOLT_UNSUPPORTED("Unsupported opapue type... // TODO");
    } else {
      // else if (ty->isArray()) {} MAP....
      BOLT_UNSUPPORTED("Unsupported constant type... // TODO");
    }
  }

 private:
  std::map<std::string, int32_t> fieldsArgIdx_;
  int32_t argIdx_{0};
};

JitBoltVectorFunction compileExprToJitVectorFunc(const exec::Expr* expr) {
  JitBoltVectorFunction result;
  ExprConverter converter;

  try {
    auto jitExpr = converter.convertExprToJitExpr(expr);
    bool compilable = thrust::jit::compilableExpr(jitExpr.get());
    if (!compilable) {
      return result;
    }

    auto fields = expr->distinctFields();
    std::vector<thrust::jit::ColumnDataMeta> jitMetas;

    for (auto&& f : fields) {
      auto jitType = converter.convertType(f->type());
      // TODO complex type
      // if (f->inputs().size() > 0) {
      // }

      thrust::jit::ColumnDataMeta jitMeta;
      jitMeta.type = jitType->kind;
      jitMeta.nullable = true; // presto
      jitMeta.name = f->name();
      jitMetas.emplace_back(std::move(jitMeta));
    }

    auto jitResourceHolder =
        thrust::jit::compileExprForVector(jitExpr.get(), jitMetas);

    result.jitFunc = (JitBoltVectorFunction::JitedFuncAddress)
                         jitResourceHolder->getFunction();

    // std::function<void(const std::vector<VectorPtr>& args, VectorPtr*
    // result)> func;
    FunctionResourceWrapper wrapper;
    wrapper.jitHolder = jitResourceHolder;
    result.func = std::bind(
        bolt_call_function_wrapper,
        wrapper,
        std::placeholders::_1,
        std::placeholders::_2);

  } catch (std::exception& e) {
    // log
  }
  return result;
}

} // namespace bytedance::bolt::jit
#endif
