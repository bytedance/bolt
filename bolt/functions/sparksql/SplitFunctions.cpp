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

#include <unicode/localpointer.h>
#include "bolt/expression/VectorFunction.h"
#include "bolt/expression/VectorWriters.h"
#include "bolt/functions/lib/string/JavaStyleSplit.h"
#include "bolt/functions/lib/string/RegexUtils.h"
#include "bolt/functions/lib/string/StringCore.h"
#include "bolt/version/version.h"

/// https://docs.databricks.com/sql/language-manual/functions/split.html
namespace bytedance::bolt::functions::sparksql {
namespace {
template <bool ConstPattern, bool ConstLimit, bool isDelimRegex>
class Split final : public exec::VectorFunction {
 public:
  explicit Split(std::string& pattern, int64_t limit = -1)
      : pattern_(pattern), limit_(limit) {
    // https://github.com/apache/spark/blob/master/common/unsafe/src/
    // main/java/org/apache/spark/unsafe/types/UTF8String.java#L1046
    if (limit_ == 0) {
      limit_ = -1;
    }
  }

  explicit Split(
      std::string& pattern,
      icu::LocalPointer<icu::RegexPattern>& patternRegex,
      int64_t limit = -1)
      : pattern_(pattern), regex_(std::move(patternRegex)), limit_(limit) {
    // https://github.com/apache/spark/blob/master/common/unsafe/src/
    // main/java/org/apache/spark/unsafe/types/UTF8String.java#L1046
    if (limit_ == 0) {
      limit_ = -1;
    }
  }

  int32_t getLimit(const exec::LocalDecodedVector& limits, int32_t row) const {
    auto limit = -1;
    if constexpr (ConstLimit) {
      limit = limit_;
    } else {
      limit = limits->valueAt<int32_t>(row);
    }
    return limit == 0 ? -1 : limit;
  }

  __attribute__((flatten)) void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::LocalDecodedVector input(context, *args[0], rows);
    exec::LocalDecodedVector patterns(context, *args[1], rows);
    exec::LocalDecodedVector limits(context, *args[2], rows);
    BaseVector::ensureWritable(rows, ARRAY(VARCHAR()), context.pool(), result);
    exec::VectorWriter<Array<Varchar>> resultWriter;
    resultWriter.init(*result->as<ArrayVector>());
    auto limit = limit_;
    if constexpr (ConstPattern) {
      if (pattern_.empty()) {
        rows.applyToSelected([&](vector_size_t row) {
          resultWriter.setOffset(row);
          auto& arrayWriter = resultWriter.current();

          const StringView& current = input->valueAt<StringView>(row);
          if constexpr (!ConstLimit) {
            limit = getLimit(limits, row);
          }
          constexpr bool isSpark34Plus =
              (kSparkVersion >= SparkVersion::SPARK_3_4);
          javaStyle::javaStyleSplitFastEmptyPatternWriter<isSpark34Plus>(
              std::string_view(current), limit, arrayWriter);
          resultWriter.commit();
        });
      } else if constexpr (!isDelimRegex) {
        rows.applyToSelected([&](vector_size_t row) {
          resultWriter.setOffset(row);
          auto& arrayWriter = resultWriter.current();
          const StringView& current = input->valueAt<StringView>(row);
          if constexpr (!ConstLimit) {
            limit = getLimit(limits, row);
          }
          javaStyle::javaStyleSplitFastWriter(
              std::string_view(current), pattern_, limit, arrayWriter);
          resultWriter.commit();
        });
      } else {
        rows.applyToSelected([&](vector_size_t row) {
          resultWriter.setOffset(row);
          auto& arrayWriter = resultWriter.current();

          const StringView& current = input->valueAt<StringView>(row);
          if constexpr (!ConstLimit) {
            limit = getLimit(limits, row);
          }
          auto res = javaStyle::javaStyleSplitRegex(
              std::string_view(current), regex_, limit);
          for (const auto& s : res) {
            arrayWriter.add_item().setNoCopy(StringView(s));
          }
          resultWriter.commit();
        });
      }
    } else {
      rows.applyToSelected([&](vector_size_t row) {
        resultWriter.setOffset(row);
        auto& arrayWriter = resultWriter.current();

        const StringView& current = input->valueAt<StringView>(row);
        const StringView& pattern = patterns->valueAt<StringView>(row);
        if constexpr (!ConstLimit) {
          limit = getLimit(limits, row);
        }
        constexpr bool isSpark34Plus =
            (kSparkVersion >= SparkVersion::SPARK_3_4);
        if (pattern.empty()) {
          javaStyle::javaStyleSplitFastEmptyPatternWriter<isSpark34Plus>(
              std::string_view(current), limit, arrayWriter);
        } else {
          auto res = javaStyle::javaStyleSplitNoEmpty<isSpark34Plus>(
              std::string_view(current), std::string_view(pattern), limit);
          for (const auto& s : res) {
            arrayWriter.add_item().setNoCopy(StringView(s));
          }
        }
        resultWriter.commit();
      });
    }
    // Reference the input StringBuffers since we did not deep copy above.
    resultWriter.finish();
    result->as<ArrayVector>()
        ->elements()
        ->as<FlatVector<StringView>>()
        ->acquireSharedStringBuffers(args[0].get());
  }

 private:
  std::string pattern_;
  icu::LocalPointer<icu::RegexPattern> regex_;
  size_t limit_;
};

/// The function returns specialized version of split based on the constant
/// inputs.
/// \param inputArgs the inputs types (VARCHAR, VARCHAR, int64) and constant
///     values (if provided).
std::shared_ptr<exec::VectorFunction> createSplit(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  BaseVector* constantPattern = inputArgs[1].constantValue.get();
  std::string pattern = "";
  int32_t limit = -1;
  if (constantPattern) {
    pattern = std::string(
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0));
    std::pair<bool, icu::LocalPointer<icu::RegexPattern>> compileResult;
    compileResult.first = true;
    if (!pattern.empty()) {
      compileResult.first = stringCore::checkRegexPattern(pattern);
      if (compileResult.first) {
        compileResult.second = regex::compileRegexPattern(pattern);
      }
    }

    if (inputArgs.size() == 3 && inputArgs[2].constantValue.get()) {
      limit = inputArgs[2]
                  .constantValue.get()
                  ->as<ConstantVector<int32_t>>()
                  ->valueAt(0);
      if (compileResult.first) { // isRegex
        return std::make_shared<Split<true, true, true>>(
            pattern, compileResult.second, limit);
      } else {
        return std::make_shared<Split<true, true, false>>(pattern, limit);
      }
    } else {
      if (compileResult.first) { // isRegex
        return std::make_shared<Split<true, false, true>>(
            pattern, compileResult.second, limit);
      } else {
        return std::make_shared<Split<true, false, false>>(pattern, limit);
      }
    }
  } else {
    if (inputArgs.size() == 3 && inputArgs[2].constantValue.get()) {
      limit = inputArgs[2]
                  .constantValue.get()
                  ->as<ConstantVector<int32_t>>()
                  ->valueAt(0);
      return std::make_shared<Split<false, true, true>>(pattern, limit);
    } else {
      return std::make_shared<Split<false, false, true>>(pattern, limit);
    }
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // varchar, varchar -> array(varchar)
  return {exec::FunctionSignatureBuilder()
              .returnType("array(varchar)")
              .argumentType("varchar")
              .constantArgumentType("varchar")
              .argumentType("integer")
              .build()};
}

} // namespace

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_regexp_split,
    signatures(),
    createSplit);
} // namespace bytedance::bolt::functions::sparksql
