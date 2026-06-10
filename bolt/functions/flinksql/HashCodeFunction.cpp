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

#include "bolt/functions/flinksql/HashCodeFunction.h"
#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"

namespace bytedance::bolt::functions::flinksql {
namespace {

class HashCodeDecimalFunction final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const final {
    BOLT_CHECK_EQ(args.size(), 1);
    context.ensureWritable(rows, outputType, result);
    result->clearNulls(rows);

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto input = decodedArgs.at(0);
    auto rawResult =
        result->asUnchecked<FlatVector<int32_t>>()->mutableRawValues();

    const auto& inputType = *args[0]->type();
    const auto scale = getDecimalPrecisionScale(inputType).second;
    if (inputType.isShortDecimal()) {
      rows.applyToSelected([&](auto row) {
        rawResult[row] = hash_code_detail::hashShortDecimal(
            input->valueAt<int64_t>(row), scale);
      });
    } else {
      BOLT_USER_CHECK(
          inputType.isLongDecimal(),
          "Expect decimal type, but got: {}",
          args[0]->type()->toString());
      rows.applyToSelected([&](auto row) {
        rawResult[row] = hash_code_detail::hashLongDecimal(
            input->valueAt<int128_t>(row), scale);
      });
    }
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>>
hashCodeDecimalSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("precision")
              .integerVariable("scale")
              .returnType("integer")
              .argumentType("DECIMAL(precision, scale)")
              .build()};
}

std::unique_ptr<exec::VectorFunction> makeHashCodeDecimal() {
  return std::make_unique<HashCodeDecimalFunction>();
}

} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    flink_hash_code_decimal,
    hashCodeDecimalSignatures(),
    makeHashCodeDecimal());

} // namespace bytedance::bolt::functions::flinksql
