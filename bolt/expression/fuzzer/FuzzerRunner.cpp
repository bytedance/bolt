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

#include "bolt/expression/fuzzer/FuzzerRunner.h"
#include "bolt/exec/fuzzer/FuzzerFlags.h"
#include "bolt/expression/fuzzer/ExpressionFuzzer.h"
namespace bytedance::bolt::fuzzer {
namespace {
VectorFuzzer::Options getVectorFuzzerOptions() {
  VectorFuzzer::Options opts;
  opts.vectorSize = FLAGS_bolt_fuzzer_batch_size;
  opts.stringVariableLength = FLAGS_bolt_fuzzer_string_variable_length;
  opts.stringLength = FLAGS_bolt_fuzzer_string_length;
  opts.nullRatio = FLAGS_bolt_fuzzer_null_ratio;
  opts.enableStringIncrementalGeneration =
      FLAGS_bolt_fuzzer_enable_string_incremental_generation;
  opts.enableDuplicates = FLAGS_bolt_fuzzer_enable_duplicates;
  opts.enableDictionary = FLAGS_bolt_fuzzer_enable_dictionary;
  opts.charEncodings = std::vector<UTF8CharList>{
      UTF8CharList::ASCII,
      UTF8CharList::UNICODE_CASE_SENSITIVE,
      UTF8CharList::EXTENDED_UNICODE,
      UTF8CharList::MATHEMATICAL_SYMBOLS,
      UTF8CharList::ALL_OTHERS};
  return opts;
}

ExpressionFuzzer::Options getExpressionFuzzerOptions(
    const std::unordered_set<std::string>& skipFunctions) {
  ExpressionFuzzer::Options opts;
  opts.maxLevelOfNesting = FLAGS_bolt_fuzzer_max_level_of_nesting;
  opts.maxNumVarArgs = FLAGS_bolt_fuzzer_max_num_varargs;
  opts.enableVariadicSignatures = FLAGS_bolt_fuzzer_enable_variadic_signatures;
  opts.enableDereference = FLAGS_bolt_fuzzer_enable_dereference;
  opts.enableComplexTypes = FLAGS_bolt_fuzzer_enable_complex_types;
  opts.enableColumnReuse = FLAGS_bolt_fuzzer_enable_column_reuse;
  opts.enableExpressionReuse = FLAGS_bolt_fuzzer_enable_expression_reuse;
  opts.functionTickets = FLAGS_bolt_fuzzer_assign_function_tickets;
  opts.nullRatio = FLAGS_bolt_fuzzer_null_ratio;
  opts.specialForms = FLAGS_bolt_fuzzer_special_forms;
  opts.useOnlyFunctions = FLAGS_bolt_fuzzer_only;
  opts.skipFunctions = skipFunctions;
  return opts;
}

ExpressionFuzzerVerifier::Options getExpressionFuzzerVerifierOptions(
    const std::unordered_set<std::string>& skipFunctions,
    const std::unordered_map<std::string, std::string>& queryConfigs) {
  ExpressionFuzzerVerifier::Options opts;
  opts.steps = FLAGS_bolt_fuzzer_steps;
  opts.durationSeconds = FLAGS_bolt_fuzzer_duration_sec;
  opts.batchSize = FLAGS_bolt_fuzzer_batch_size;
  opts.retryWithTry = FLAGS_bolt_fuzzer_retry_with_try;
  opts.findMinimalSubexpression = FLAGS_bolt_fuzzer_find_minimal_subexpression;
  opts.disableConstantFolding = FLAGS_bolt_fuzzer_disable_constant_folding;
  opts.reproPersistPath = FLAGS_bolt_fuzzer_repro_persist_path;
  opts.persistAndRunOnce = FLAGS_bolt_fuzzer_persist_and_run_once;
  opts.lazyVectorGenerationRatio =
      FLAGS_bolt_fuzzer_lazy_vector_generation_ratio;
  opts.maxExpressionTreesPerStep =
      FLAGS_bolt_fuzzer_max_expression_trees_per_step;
  opts.vectorFuzzerOptions = getVectorFuzzerOptions();
  opts.expressionFuzzerOptions = getExpressionFuzzerOptions(skipFunctions);
  opts.queryConfigs = queryConfigs;
  return opts;
}

} // namespace

// static
int FuzzerRunner::run(
    size_t seed,
    const std::unordered_set<std::string>& skipFunctions,
    const std::unordered_map<std::string, std::string>& queryConfigs) {
  runFromGtest(seed, skipFunctions, queryConfigs);
  return RUN_ALL_TESTS();
}

// static
void FuzzerRunner::runFromGtest(
    size_t seed,
    const std::unordered_set<std::string>& skipFunctions,
    const std::unordered_map<std::string, std::string>& queryConfigs) {
  memory::MemoryManager::testingSetInstance({});
  auto signatures = bytedance::bolt::getFunctionSignatures();
  ExpressionFuzzerVerifier(
      signatures,
      seed,
      getExpressionFuzzerVerifierOptions(skipFunctions, queryConfigs))
      .go();
}
} // namespace bytedance::bolt::fuzzer
