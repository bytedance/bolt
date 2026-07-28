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

#include "bolt/functions/flinksql/RegexFunctions.h"

#include "bolt/functions/lib/Re2Functions.h"

namespace bytedance::bolt::functions::flinksql {

std::shared_ptr<exec::VectorFunction> makeRLike(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return makeRe2SearchWithPolicy(
      name, inputArgs, config, InvalidRegexPolicy::kReturnFalse);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> rlikeSignatures() {
  return re2SearchSignatures();
}

std::shared_ptr<exec::VectorFunction> makeRegexpExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  Re2ExtractOptions options;
  options.invalidRegexPolicy = Re2ExtractErrorPolicy::kReturnNull;
  options.invalidGroupPolicy = Re2ExtractErrorPolicy::kReturnNull;
  options.unmatchedGroupPolicy = Re2ExtractResultPolicy::kReturnNull;

  auto result = makeRe2ExtractWithOptions(name, inputArgs, config, options);
  BOLT_USER_CHECK(
      inputArgs[1].constantValue != nullptr &&
          inputArgs[1].constantValue->isConstantEncoding(),
      "{} requires a constant pattern.",
      name);
  return result;
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
regexpExtractSignatures() {
  return re2ExtractSignatures();
}

} // namespace bytedance::bolt::functions::flinksql
