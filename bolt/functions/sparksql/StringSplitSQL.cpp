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

#include "bolt/functions/sparksql/StringSplitSQL.h"

#include "bolt/expression/VectorFunction.h"
#include "bolt/expression/VectorWriters.h"

namespace bytedance::bolt::functions::sparksql {
namespace {

class StringSplitSQLFunction final : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    // We handle NULLs explicitly to implement NullIntolerant semantics:
    // if either input or delimiter is NULL, the result is NULL.
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::LocalDecodedVector input(context, *args[0], rows);
    exec::LocalDecodedVector delimiters(context, *args[1], rows);

    BaseVector::ensureWritable(rows, ARRAY(VARCHAR()), context.pool(), result);

    exec::VectorWriter<Array<Varchar>> resultWriter;
    resultWriter.init(*result->as<ArrayVector>());

    rows.applyToSelected([&](auto row) {
      resultWriter.setOffset(row);

      if (input->isNullAt(row) || delimiters->isNullAt(row)) {
        resultWriter.commitNull();
        return;
      }

      auto& arrayWriter = resultWriter.current();

      const auto inputSv = input->valueAt<StringView>(row);
      const auto delimSv = delimiters->valueAt<StringView>(row);

      const std::string_view inputView(inputSv.data(), inputSv.size());
      const std::string_view delimView(delimSv.data(), delimSv.size());

      stringSplitSQLImpl(inputView, delimView, [&](std::string_view token) {
        arrayWriter.add_item().setNoCopy(StringView(token));
      });

      resultWriter.commit();
    });

    resultWriter.finish();
    // Reuse the input string buffers for output values.
    result->as<ArrayVector>()
        ->elements()
        ->as<FlatVector<StringView>>()
        ->acquireSharedStringBuffers(args[0].get());
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // string_split_sql(varchar input, varchar delimiter) -> array(varchar)
  return {exec::FunctionSignatureBuilder()
              .returnType("array(varchar)")
              .argumentType("varchar")
              .argumentType("varchar")
              .build()};
}

std::shared_ptr<exec::VectorFunction> createStringSplitSQL(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& /*inputArgs*/,
    const core::QueryConfig& /*config*/) {
  return std::make_shared<StringSplitSQLFunction>();
}

} // namespace

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_string_split_sql,
    signatures(),
    createStringSplitSQL);

} // namespace bytedance::bolt::functions::sparksql
