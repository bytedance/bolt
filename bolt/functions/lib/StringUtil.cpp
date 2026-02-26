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

#include "bolt/functions/lib/StringUtil.h"
#include <iostream>
#include "bolt/expression/VectorFunction.h"
namespace bytedance::bolt::functions {

using namespace stringCore;
namespace {

struct NormalizedArg {
  bool isConstant = true;
  bool isNull = false;
  int originIndex = 0;
  std::string constantValue = "";
  mutable DecodedVector* decoded = nullptr;
  mutable DecodedVector* decodedElms = nullptr;
  NormalizedArg(
      bool isConstant = true,
      bool isNull = false,
      int originIndex = 0,
      std::string constantValue = "")
      : isConstant(isConstant),
        isNull(isNull),
        originIndex(originIndex),
        constantValue(constantValue) {}
  std::string toString() const {
    std::ostringstream oss;
    oss << "NormalizedArg("
        << "isConstant: " << (isConstant ? "true" : "false") << ", "
        << "isNull: " << (isNull ? "true" : "false") << ", "
        << "originIndex: " << originIndex << ", "
        << "constantValue: \"" << constantValue << "\")";
    return oss.str();
  }
};

/**
 * concat_ws(delimiter, string1/array<string>1...stringN/array<string>N) →
 * varchar Returns the concatenation of string/array<string> separated by
 * delimiter.
 * */
class ConcatWsFunction : public exec::VectorFunction {
 public:
  ConcatWsFunction(
      const std::string& /* name */,
      const std::vector<exec::VectorFunctionArg>& inputArgs) {
    auto numArgs = inputArgs.size();
    BOLT_CHECK_GE(numArgs, 1);

    if (inputArgs[0].constantValue) {
      normalizedArgs_.emplace_back(
          true,
          inputArgs[0].constantValue->isNullAt(0),
          0,
          inputArgs[0]
              .constantValue->as<ConstantVector<StringView>>()
              ->valueAt(0)
              .str());
    } else {
      BOLT_CHECK_NE(inputArgs[0].type->kind(), TypeKind::ARRAY);
      normalizedArgs_.emplace_back(false, false, 0, "");
    }
    for (auto i = 1; i < numArgs; ++i) {
      const auto& arg = inputArgs[i];

      // we can't concat array without delimiter
      if (arg.constantValue &&
          (delimiter().isConstant || !arg.type->isArray())) {
        if (inputArgs[i].constantValue->isNullAt(0)) {
          continue;
        }
        std::ostringstream value;
        auto appendDelim = false;
        constantToString(value, inputArgs[i].constantValue, appendDelim);
        if (delimiter().isConstant && !delimiter().isNull) {
          for (++i; i < inputArgs.size(); ++i) {
            if (!inputArgs[i].constantValue) {
              break;
            }
            if (inputArgs[i].constantValue->isNullAt(0)) {
              continue;
            }
            constantToString(value, inputArgs[i].constantValue, appendDelim);
          }

          normalizedArgs_.emplace_back(true, false, -1, value.str());
          --i;
        } else {
          normalizedArgs_.emplace_back(true, false, i, value.str());
        }
      } else {
        normalizedArgs_.emplace_back(false, false, i, "");
      }
    }
  }

  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto delimeterPtr = args[0].get();
    if (delimeterPtr->isConstantEncoding() && delimeterPtr->isNullAt(0)) {
      result = BaseVector::createNullConstant(
          VARCHAR(), rows.size(), context.pool());
      return;
    }
    if (args.size() == 1) {
      result = BaseVector::createConstant(
          VARCHAR(), StringView(), rows.size(), context.pool());
      return;
    }

    context.ensureWritable(rows, VARCHAR(), result);
    auto flatResult = result->asFlatVector<StringView>();

    auto numArgs = normalizedArgs_.size();

    std::vector<exec::LocalDecodedVector> decodedArgsHolder;
    decodedArgsHolder.reserve(numArgs);

    std::vector<VectorPtr> flatArray;

    for (auto& arg : normalizedArgs_) {
      if (!arg.isConstant) {
        decodedArgsHolder.emplace_back(context, *args[arg.originIndex], rows);
        auto decoded = decodedArgsHolder.back().get();
        if (decoded->base()->typeKind() == TypeKind::ARRAY) {
          auto decodedArray = decoded->base()->asUnchecked<ArrayVector>();
          auto innerRowsSize = decodedArray->elements()->size();
          auto innerRows = exec::LocalSelectivityVector(context, innerRowsSize);
          decodedArgsHolder.emplace_back(
              context, *decodedArray->elements(), *innerRows.get());
          arg.decodedElms = decodedArgsHolder.back().get();
        }
        arg.decoded = decoded;
      }
    }

    rows.applyToSelected([&](int row) {
      StringView delim;
      if (delimiter().isConstant) {
        delim = StringView(delimiter().constantValue);
      } else {
        if (delimiter().decoded->isNullAt(row)) {
          flatResult->setNull(row, true);
          return;
        }
        delim = delimiter().decoded->valueAt<StringView>(row);
      }

      auto result = InPlaceString(flatResult);
      bool appendDelim = false;
      auto appendValue = [&](StringView value) {
        if (appendDelim) {
          result.append(delim, flatResult);
        }
        result.append(value, flatResult);
        appendDelim = true;
      };
      for (int argIndex = 1; argIndex < numArgs; argIndex++) {
        if (normalizedArgs_[argIndex].isConstant) {
          if (normalizedArgs_[argIndex].isNull) {
            continue;
          }
          auto value = StringView(
              normalizedArgs_[argIndex].constantValue.data(),
              normalizedArgs_[argIndex].constantValue.size());
          appendValue(value);
        } else {
          auto* decodedArg = normalizedArgs_.at(argIndex).decoded;
          BOLT_CHECK_NOT_NULL(decodedArg);
          if (decodedArg->isNullAt(row)) {
            continue;
          }
          if (decodedArg->base()->typeKind() == TypeKind::ARRAY) {
            const auto* decodedArray =
                decodedArg->base()->asUnchecked<ArrayVector>();
            auto internalIndex = decodedArg->index(row);
            auto offset = decodedArray->offsetAt(internalIndex);
            auto size = decodedArray->sizeAt(internalIndex);
            auto* decodedElms = normalizedArgs_.at(argIndex).decodedElms;
            for (auto i = offset; i < offset + size; ++i) {
              if (decodedElms->isNullAt(i)) {
                continue;
              }
              auto value = decodedElms->valueAt<StringView>(i);
              appendValue(value);
            }
          } else {
            auto value = decodedArg->valueAt<StringView>(row);
            appendValue(value);
          }
        }
      }
      result.set(row, flatResult);
    });
  }

  void constantToString(
      std::ostringstream& value,
      const VectorPtr& arg,
      bool& appendDelim) {
    if (arg->typeKind() == TypeKind::VARCHAR) {
      if (appendDelim) {
        value << delimiter().constantValue;
      }
      value << arg->as<ConstantVector<StringView>>()->valueAt(0).str();
      appendDelim = true;
    } else if (arg->typeKind() == TypeKind::ARRAY) {
      flatConstantArray(value, arg, appendDelim);
    } else {
      BOLT_FAIL("For concat_ws, only varchar or array<varchar> supported");
    }
  }

  void flatConstantArray(
      std::ostringstream& result,
      const VectorPtr& arg,
      bool& appendDelim) {
    auto* constantArray = arg->as<ConstantVector<ComplexType>>();
    const auto& arrayVector = constantArray->wrappedVector()->as<ArrayVector>();
    BOLT_CHECK_EQ(arrayVector->elements()->typeKind(), TypeKind::VARCHAR);
    auto row = constantArray->index();
    auto rows = SelectivityVector(row + 1, false);
    rows.setValid(row, true);
    auto decodedElements = DecodedVector(*arrayVector->elements(), rows);
    if (!arrayVector->isNullAt(row)) {
      auto size = arrayVector->sizeAt(row);
      auto offset = arrayVector->offsetAt(row);
      for (auto i = offset; i < offset + size; ++i) {
        if (!decodedElements.isNullAt(i)) {
          if (appendDelim) {
            result << delimiter().constantValue.data();
          }
          result << decodedElements.valueAt<StringView>(i);
          appendDelim = true;
        }
      }
    }
  }

  static exec::VectorFunctionMetadata metadata() {
    return {true /* supportsFlattening */, {}};
  }

 private:
  // std::string constantDelim_;
  // StringView delimStringView_;
  std::vector<NormalizedArg> normalizedArgs_;
  inline const NormalizedArg& delimiter() const {
    return normalizedArgs_.at(0);
  }
};

} // namespace

std::shared_ptr<exec::VectorFunction> makeConcatWs(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return std::make_shared<ConcatWsFunction>(name, inputArgs);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> ConcatWsSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .variableArity()
          .build(),

      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("array(varchar)")
          .variableArity()
          .build(),
      // TODO : "any" is not correct
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("any")
          .variableArity()
          .build(),

      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("array(varchar)")
          .argumentType("any")
          .variableArity()
          .build(),

  };
}

} // namespace bytedance::bolt::functions
