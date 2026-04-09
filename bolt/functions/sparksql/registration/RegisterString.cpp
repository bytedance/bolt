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

#include "bolt/functions/lib/Re2Functions.h"
#include "bolt/functions/prestosql/StringFunctions.h"
#include "bolt/functions/sparksql/Base64Function.h"
#include "bolt/functions/sparksql/LuhnCheckFunction.h"
#include "bolt/functions/sparksql/MaskFunction.h"
#include "bolt/functions/sparksql/String.h"
#include "bolt/functions/sparksql/UnBase64Function.h"

namespace bytedance::bolt::functions {
void registerSparkStringFunctions(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_concat, prefix + "concat");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_lower, prefix + "lower");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_upper, prefix + "upper");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_str_to_map, prefix + "str_to_map");
}

namespace sparksql {
void registerStringFunctions(const std::string& prefix) {
  registerSparkStringFunctions(prefix);

  registerFunction<StartsWithFunction, bool, Varchar, Varchar>(
      {prefix + "startswith"});
  registerFunction<EndsWithFunction, bool, Varchar, Varchar>(
      {prefix + "endswith"});
  registerFunction<ContainsFunction, bool, Varchar, Varchar>(
      {prefix + "contains"});
  registerFunction<LocateFunction, int32_t, Varchar, Varchar>(
      {prefix + "locate"});
  registerFunction<LocateFunction, int32_t, Varchar, Varchar, int32_t>(
      {prefix + "locate"});
  registerFunction<RepeatStringFunction, Varchar, Varchar, int32_t>(
      {prefix + "string_repeat"});
  exec::registerStatefulVectorFunction(
      prefix + "repeat", sparkRepeatSignatures(), makeRepeat);
  registerFunction<UUIDWithoutSeedFunction, Varchar>({prefix + "uuid"});
  registerFunction<UUIDWithSeedFunction, Varchar, Constant<int64_t>>(
      {prefix + "uuid"});

  registerFunction<ToTitleFunction, Varchar, Varchar>(
      {prefix + "to_title", prefix + "initcap"});

  registerFunction<TrimSpaceFunction, Varchar, Varchar>({prefix + "trim"});
  registerFunction<TrimFunction, Varchar, Varchar, Varchar>({prefix + "trim"});
  registerFunction<LTrimSpaceFunction, Varchar, Varchar>({prefix + "ltrim"});
  registerFunction<LTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "ltrim"});
  registerFunction<RTrimSpaceFunction, Varchar, Varchar>({prefix + "rtrim"});
  registerFunction<RTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "rtrim"});
  registerFunction<SpaceFunction, Varchar, int32_t>({prefix + "space"});

  registerFunction<TranslateFunction, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "translate"});

  registerFunction<ConvFunction, Varchar, Varchar, int32_t, int32_t>(
      {prefix + "conv"});

  registerFunction<ReplaceFunction, Varchar, Varchar, Varchar>(
      {prefix + "replace"});
  registerFunction<ReplaceFunction, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "replace"});

  registerFunction<FindInSetFunction, int32_t, Varchar, Varchar>(
      {prefix + "find_in_set"});

  exec::registerStatefulVectorFunction(
      prefix + "instr", instrSignatures(), makeInstr);
  exec::registerStatefulVectorFunction(
      prefix + "length", lengthSignatures(), makeLength);
  registerFunction<SubstringIndexFunction, Varchar, Varchar, Varchar, int32_t>(
      {prefix + "substring_index"});

  registerFunction<sparksql::LeftFunction, Varchar, Varchar, int32_t>(
      {prefix + "left"});
  registerFunction<sparksql::RightFunction, Varchar, Varchar, int32_t>(
      {prefix + "right"});

  registerFunction<sparksql::ChrFunction, Varchar, int64_t>({prefix + "chr"});
  registerFunction<AsciiFunction, int32_t, Varchar>({prefix + "ascii"});
  registerFunction<sparksql::LPadFunction, Varchar, Varchar, int32_t, Varchar>(
      {prefix + "lpad"});
  registerFunction<sparksql::RPadFunction, Varchar, Varchar, int32_t, Varchar>(
      {prefix + "rpad"});
  registerFunction<sparksql::LPadFunction, Varchar, Varchar, int32_t>(
      {prefix + "lpad"});
  registerFunction<sparksql::RPadFunction, Varchar, Varchar, int32_t>(
      {prefix + "rpad"});
  registerFunction<sparksql::SubstrFunction, Varchar, Varchar, int32_t>(
      {prefix + "substring"});
  registerFunction<
      sparksql::SubstrFunction,
      Varchar,
      Varchar,
      int32_t,
      int32_t>({prefix + "substring"});
  registerFunction<
      sparksql::OverlayVarcharFunction,
      Varchar,
      Varchar,
      Varchar,
      int32_t,
      int32_t>({prefix + "overlay"});
  registerFunction<
      sparksql::OverlayVarbinaryFunction,
      Varbinary,
      Varbinary,
      Varbinary,
      int32_t,
      int32_t>({prefix + "overlay"});

  registerFunction<
      LevenshteinDistanceFunction,
      int32_t,
      Varchar,
      Varchar,
      int32_t>({prefix + "levenshtein"});
  registerFunction<LevenshteinDistanceFunction, int32_t, Varchar, Varchar>(
      {prefix + "levenshtein"});

  registerFunction<Base64Function, Varchar, Varbinary>({prefix + "base64"});
  registerFunction<UnBase64Function, Varbinary, Varchar>({prefix + "unbase64"});
  registerFunction<MaskFunction, Varchar, Varchar>({prefix + "mask"});
  registerFunction<MaskFunction, Varchar, Varchar, Varchar>({prefix + "mask"});
  registerFunction<MaskFunction, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "mask"});
  registerFunction<MaskFunction, Varchar, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "mask"});
  registerFunction<
      MaskFunction,
      Varchar,
      Varchar,
      Varchar,
      Varchar,
      Varchar,
      Varchar>({prefix + "mask"});

  registerFunction<SoundexFunction, Varchar, Varchar>({prefix + "soundex"});

  registerFunction<sparksql::BitLengthFunction, int32_t, Varchar>(
      {prefix + "bit_length"});
  registerFunction<sparksql::BitLengthFunction, int32_t, Varbinary>(
      {prefix + "bit_length"});
  registerFunction<Empty2NullFunction, Varchar, Varchar>(
      {prefix + "empty2null"});

  registerFunction<LuhnCheckFunction, bool, Varchar>({prefix + "luhn_check"});
}
} // namespace sparksql
} // namespace bytedance::bolt::functions
