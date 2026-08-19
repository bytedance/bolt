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

#include "bolt/common/base/SparkCompatibility.h"

#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/Re2Functions.h"
#include "bolt/functions/lib/StringUtil.h"
#include "bolt/functions/prestosql/FromUtf8.h"
#include "bolt/functions/prestosql/RegexFunctions.h"
#include "bolt/functions/prestosql/SplitPart.h"
#include "bolt/functions/prestosql/SplitToMap.h"
#include "bolt/functions/prestosql/StringFunctions.h"
#include "bolt/functions/sparksql/ICURegexFunctions.h"
#include "bolt/functions/sparksql/String.h"
namespace bytedance::bolt::functions {

namespace {
std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return makeRe2Extract(name, inputArgs, config, /*emptyNoMatch=*/false);
}

void registerSimpleFunctions(const std::string& prefix) {
  using namespace stringImpl;

  // Register string functions.
  registerFunction<ChrFunction, Varchar, int64_t>({prefix + "chr"});
  registerFunction<CodePointFunction, int32_t, Varchar>({prefix + "codepoint"});
  registerFunction<LevenshteinDistanceFunction, int64_t, Varchar, Varchar>(
      {prefix + "levenshtein_distance"});
  registerFunction<LengthFunction, int64_t, Varchar>({prefix + "length"});

  // Length for varbinary have different semantics.
  registerFunction<LengthVarbinaryFunction, int64_t, Varbinary>(
      {prefix + "length"});

  registerFunction<StartsWithFunction, bool, Varchar, Varchar>(
      {prefix + "starts_with"});
  registerFunction<EndsWithFunction, bool, Varchar, Varchar>(
      {prefix + "ends_with"});

  registerFunction<SubstrFunction, Varchar, Varchar, int64_t>(
      {prefix + "substr", prefix + "substring"});
  registerFunction<SubstrFunction, Varchar, Varchar, int64_t, int64_t>(
      {prefix + "substr", prefix + "substring"});
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t>(
      {prefix + "substr", prefix + "substring"});
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t, int32_t>(
      {prefix + "substr", prefix + "substring"});

  registerFunction<SplitPart, Varchar, Varchar, Varchar, int64_t>(
      {prefix + "split_part"});

  registerFunction<TrimFunction, Varchar, Varchar>({prefix + "trim"});
  registerFunction<TrimFunction, Varchar, Varchar, Varchar>({prefix + "trim"});
  registerFunction<LTrimFunction, Varchar, Varchar>({prefix + "ltrim"});
  registerFunction<LTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "ltrim"});
  registerFunction<RTrimFunction, Varchar, Varchar>({prefix + "rtrim"});
  registerFunction<RTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "rtrim"});

  registerFunction<LPadFunction, Varchar, Varchar, int64_t, Varchar>(
      {prefix + "lpad"});
  registerFunction<RPadFunction, Varchar, Varchar, int64_t, Varchar>(
      {prefix + "rpad"});

  exec::registerStatefulVectorFunction(
      prefix + "like", likeSignatures(), makeLike);

  exec::registerStatefulVectorFunction(
      prefix + "regexp_replace",
      sparksql::icuRegexReplaceSignatures(),
      sparksql::makeICURegexReplace);
  // regexp_replace('ab','a') -> 'b'
  registerFunction<Re2RegexpReplacePresto, Varchar, Varchar, Constant<Varchar>>(
      {prefix + "regexp_replace"});
  registerFunction<Re2RegexpSplitPresto, Array<Varchar>, Varchar, Varchar>(
      {prefix + "regexp_split"});
}
} // namespace

void registerStringFunctions(const std::string& prefix) {
  registerSimpleFunctions(prefix);

  BOLT_REGISTER_VECTOR_FUNCTION(udf_lower, prefix + "lower");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_upper, prefix + "upper");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_split, prefix + "split");
  registerFunction<
      SplitToMapFunction,
      Map<Varchar, Varchar>,
      Varchar,
      Varchar,
      Varchar>({prefix + "split_to_map"});
  BOLT_REGISTER_VECTOR_FUNCTION(udf_concat, prefix + "concat");
  if constexpr (!::bytedance::bolt::kSparkCompatible) {
    // spark has another implementation
    BOLT_REGISTER_VECTOR_FUNCTION(udf_replace, prefix + "replace");
  }
  BOLT_REGISTER_VECTOR_FUNCTION(udf_reverse, prefix + "reverse");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_to_utf8, prefix + "to_utf8");
  registerFromUtf8(prefix + "from_utf8");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_str_to_map, prefix + "str_to_map");

  // Regex functions
  exec::registerStatefulVectorFunction(
      prefix + "regexp_extract", re2ExtractSignatures(), makeRegexExtract);

  exec::registerStatefulVectorFunction(
      prefix + "regexp_extract_all",
      re2ExtractAllSignatures(),
      makeRe2ExtractAll);

  exec::registerStatefulVectorFunction(
      prefix + "regexp_like", re2SearchSignatures(), makeRe2Search);
  exec::registerStatefulVectorFunction(
      prefix + "rlike", re2SearchSignatures(), makeRe2Search);

  registerFunction<StrLPosFunction, int64_t, Varchar, Varchar>(
      {prefix + "strpos"});
  registerFunction<StrLPosFunction, int64_t, Varchar, Varchar, int64_t>(
      {prefix + "strpos"});
  registerFunction<StrRPosFunction, int64_t, Varchar, Varchar>(
      {prefix + "strrpos"});
  registerFunction<StrRPosFunction, int64_t, Varchar, Varchar, int64_t>(
      {prefix + "strrpos"});
  registerFunction<
      bytedance::bolt::functions::sparksql::StartsWithFunction,
      bool,
      Varchar,
      Varchar>({prefix + "startswith"});
  registerFunction<
      bytedance::bolt::functions::sparksql::SubstringIndexFunction,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "substring_index"});
  registerFunction<
      bytedance::bolt::functions::sparksql::SpaceFunction,
      Varchar,
      int32_t>({prefix + "space"});

  exec::registerStatefulVectorFunction(
      prefix + "concat_ws_spark", ConcatWsSignatures(), makeConcatWs);

  registerFunction<sparksql::LocateFunction, int32_t, Varchar, Varchar>(
      {prefix + "locate"});
  registerFunction<
      sparksql::LocateFunction,
      int32_t,
      Varchar,
      Varchar,
      int32_t>({prefix + "locate"});

  if constexpr (!::bytedance::bolt::kSparkCompatible) {
    registerFunction<ParseURLFunction, Varchar, Varchar, Varchar>(
        {prefix + "parse_url"});
    registerFunction<ParseURLFunction, Varchar, Varchar, Varchar, Varchar>(
        {prefix + "parse_url"});
  }

  BOLT_REGISTER_VECTOR_FUNCTION(udf_printf, {prefix + "printf"})
}
} // namespace bytedance::bolt::functions
