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

#include <iostream>
#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/lib/StringEncodingUtils.h"
#include "bolt/functions/lib/StringUtil.h"
#include "bolt/functions/lib/string/StringCore.h"
namespace bytedance::bolt::functions::sparksql {

using namespace stringCore;
namespace {

template <bool isAscii>
int32_t instr(
    const folly::StringPiece haystack,
    const folly::StringPiece needle) {
  int32_t offset = haystack.find(needle);
  if constexpr (isAscii) {
    return offset + 1;
  } else {
    // If the string is unicode, convert the byte offset to a codepoints.
    return offset == -1 ? 0 : lengthUnicode(haystack.data(), offset) + 1;
  }
}

class Instr : public exec::VectorFunction {
  bool ensureStringEncodingSetAtAllInputs() const override {
    return true;
  }

  void apply(
      const SelectivityVector& selected,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_EQ(args.size(), 2);
    BOLT_CHECK_EQ(args[0]->typeKind(), TypeKind::VARCHAR);
    BOLT_CHECK_EQ(args[1]->typeKind(), TypeKind::VARCHAR);
    exec::LocalDecodedVector haystack(context, *args[0], selected);
    exec::LocalDecodedVector needle(context, *args[1], selected);
    context.ensureWritable(selected, INTEGER(), result);
    auto* output = result->as<FlatVector<int32_t>>();

    if (isAscii(args[0].get(), selected)) {
      selected.applyToSelected([&](vector_size_t row) {
        auto h = haystack->valueAt<StringView>(row);
        auto n = needle->valueAt<StringView>(row);
        output->set(row, instr<true>(h, n));
      });
    } else {
      selected.applyToSelected([&](vector_size_t row) {
        auto h = haystack->valueAt<StringView>(row);
        auto n = needle->valueAt<StringView>(row);
        output->set(row, instr<false>(h, n));
      });
    }
  }
};

class Length : public exec::VectorFunction {
  bool ensureStringEncodingSetAtAllInputs() const override {
    return true;
  }

  void apply(
      const SelectivityVector& selected,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_EQ(args.size(), 1);
    BOLT_CHECK(
        args[0]->typeKind() == TypeKind::VARCHAR ||
        args[0]->typeKind() == TypeKind::VARBINARY);
    exec::LocalDecodedVector input(context, *args[0], selected);
    context.ensureWritable(selected, INTEGER(), result);
    auto* output = result->as<FlatVector<int32_t>>();

    if (args[0]->typeKind() == TypeKind::VARCHAR &&
        !isAscii(args[0].get(), selected)) {
      selected.applyToSelected([&](vector_size_t row) {
        const StringView str = input->valueAt<StringView>(row);
        output->set(row, lengthUnicode(str.data(), str.size()));
      });
    } else {
      selected.applyToSelected([&](vector_size_t row) {
        output->set(row, input->valueAt<StringView>(row).size());
      });
    }
  }
};

/// repeat(input, n) -> varchar
///
///    Returns the string which repeats input n times.
///    Result size must be less than or equal to 1MB.
///    If n is less than or equal to 0, empty string is returned.
class Repeat : public exec::VectorFunction {
 public:
  explicit Repeat(bool allowNegativeCount)
      : allowNegativeCount_(allowNegativeCount) {}

  bool isDefaultNullBehavior() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, outputType, result);
    exec::LocalDecodedVector input(context, *args[0], rows);
    exec::LocalDecodedVector countVec(context, *args[1], rows);

    auto flatResult = result->asFlatVector<StringView>();

    rows.applyToSelected([&](vector_size_t row) {
      auto str = input->valueAt<StringView>(row);
      auto count = countVec->valueAt<int32_t>(row);
      auto result = InPlaceString(flatResult);

      count = checkCount(count, allowNegativeCount_);
      if (count == 0) {
        result.append(StringView{}, flatResult);
        result.set(row, flatResult);
        return;
      }
      int32_t totalLen = bolt::checkedMultiply<int32_t>(str.size(), count);
      result.setNewBuffer(totalLen, flatResult);
      for (int i = 0; i < count; ++i) {
        result.append(str, flatResult);
      }
      result.set(row, flatResult);
    });
  }

 private:
  static int32_t checkCount(int32_t count, bool allowNegativeCount) {
    if (count < 0) {
      BOLT_USER_CHECK(allowNegativeCount, "Count must be non-negative");
    }
    return std::max(0, count);
  }

  bool allowNegativeCount_;
};
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> instrSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .returnType("INTEGER")
          .argumentType("VARCHAR")
          .argumentType("VARCHAR")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeInstr(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  static const auto kInstrFunction = std::make_shared<Instr>();
  return kInstrFunction;
}

std::vector<std::shared_ptr<exec::FunctionSignature>> lengthSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .returnType("INTEGER")
          .argumentType("VARCHAR")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("INTEGER")
          .argumentType("VARBINARY")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeLength(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  static const auto kLengthFunction = std::make_shared<Length>();
  return kLengthFunction;
}

std::vector<std::shared_ptr<exec::FunctionSignature>> sparkRepeatSignatures() {
  // VARCHAR, INTEGER -> VARCHAR
  return {exec::FunctionSignatureBuilder()
              .typeVariable("VARCHAR")
              .returnType("VARCHAR")
              .argumentType("VARCHAR")
              .argumentType("INTEGER")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeRepeat(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& /*inputArgs*/,
    const core::QueryConfig& /*config*/) {
  return std::make_unique<Repeat>(true);
}

void encodeDigestToBase16(uint8_t* output, int digestSize) {
  static unsigned char const kHexCodes[] = "0123456789abcdef";
  for (int i = digestSize - 1; i >= 0; --i) {
    int digestChar = output[i];
    output[i * 2] = kHexCodes[(digestChar >> 4) & 0xf];
    output[i * 2 + 1] = kHexCodes[digestChar & 0xf];
  }
}

} // namespace bytedance::bolt::functions::sparksql
