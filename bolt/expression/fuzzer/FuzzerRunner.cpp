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
#include "bolt/expression/fuzzer/ExpressionFuzzer.h"

DEFINE_int32(steps, 10, "Number of expressions to generate and execute.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(string_length, 100, "The max length of generated strings.");

DEFINE_bool(
    string_variable_length,
    false,
    "Whether to generate variable lengths of strings,"
    "or all the generated strings should have the same length.");

DEFINE_bool(
    enable_string_incremental_generation,
    false,
    "Whether the generated strings could share common substrings.");

DEFINE_bool(
    enable_duplicates,
    false,
    "Whether to allow duplicated data (i.e., reuse already generated tests).");

DEFINE_bool(enable_dictionary, true, "Whether to allow dictionary encoding.");

DEFINE_bool(
    retry_with_try,
    false,
    "Retry failed expressions by wrapping it using a try() statement.");

DEFINE_bool(
    find_minimal_subexpression,
    false,
    "Automatically seeks minimum failed subexpression on result mismatch");

DEFINE_bool(
    disable_constant_folding,
    false,
    "Disable constant-folding in the common evaluation path.");

DEFINE_string(
    repro_persist_path,
    "",
    "Directory path for persistence of data and SQL when fuzzer fails for "
    "future reproduction. Empty string disables this feature.");

DEFINE_bool(
    persist_and_run_once,
    false,
    "Persist repro info before evaluation and only run one iteration. "
    "This is to rerun with the seed number and persist repro info upon a "
    "crash failure. Only effective if repro_persist_path is set.");

DEFINE_double(
    lazy_vector_generation_ratio,
    0.0,
    "Specifies the probability with which columns in the input row "
    "vector will be selected to be wrapped in lazy encoding "
    "(expressed as double from 0 to 1).");

DEFINE_int32(
    max_expression_trees_per_step,
    1,
    "This sets an upper limit on the number of expression trees to generate "
    "per step. These trees would be executed in the same ExprSet and can "
    "reuse already generated columns and subexpressions (if reuse is "
    "enabled).");

// The flags below are used to initialize ExpressionFuzzer::options.
DEFINE_string(
    only,
    "",
    "If specified, Fuzzer will only choose functions from "
    "this comma separated list of function names "
    "(e.g: --only \"split\" or --only \"substr,ltrim\").");

DEFINE_string(
    special_forms,
    "and,or,cast,coalesce,if,switch",
    "Comma-separated list of special forms to use in generated expression. "
    "Supported special forms: and, or, coalesce, if, switch, cast.");

DEFINE_int32(
    bolt_fuzzer_max_level_of_nesting,
    10,
    "Max levels of expression nesting. The default value is 10 and minimum is 1.");

DEFINE_int32(
    max_num_varargs,
    5,
    "The maximum number of variadic arguments fuzzer will generate for "
    "functions that accept variadic arguments. Fuzzer will generate up to "
    "max_num_varargs arguments for the variadic list in addition to the "
    "required arguments by the function.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null constant to the plan, or null value in a vector "
    "(expressed as double from 0 to 1).");

DEFINE_bool(
    enable_variadic_signatures,
    false,
    "Enable testing of function signatures with variadic arguments.");

DEFINE_bool(
    enable_dereference,
    false,
    "Allow fuzzer to generate random expressions with dereference and row_constructor functions.");

DEFINE_bool(
    bolt_fuzzer_enable_complex_types,
    false,
    "Enable testing of function signatures with complex argument or return types.");

DEFINE_bool(
    bolt_fuzzer_enable_column_reuse,
    false,
    "Enable generation of expressions where one input column can be "
    "used by multiple subexpressions");

DEFINE_bool(
    bolt_fuzzer_enable_expression_reuse,
    false,
    "Enable generation of expressions that reuses already generated "
    "subexpressions.");

DEFINE_string(
    assign_function_tickets,
    "",
    "Comma separated list of function names and their tickets in the format "
    "<function_name>=<tickets>. Every ticket represents an opportunity for "
    "a function to be chosen from a pool of candidates. By default, "
    "every function has one ticket, and the likelihood of a function "
    "being picked can be increased by allotting it more tickets. Note "
    "that in practice, increasing the number of tickets does not "
    "proportionally increase the likelihood of selection, as the selection "
    "process involves filtering the pool of candidates by a required "
    "return type so not all functions may compete against the same number "
    "of functions at every instance. Number of tickets must be a positive "
    "integer. Example: eq=3,floor=5");
namespace bytedance::bolt::fuzzer {
namespace {
VectorFuzzer::Options getVectorFuzzerOptions() {
  VectorFuzzer::Options opts;
  opts.vectorSize = FLAGS_batch_size;
  opts.stringVariableLength = FLAGS_string_variable_length;
  opts.stringLength = FLAGS_string_length;
  opts.nullRatio = FLAGS_null_ratio;
  opts.enableStringIncrementalGeneration =
      FLAGS_enable_string_incremental_generation;
  opts.enableDuplicates = FLAGS_enable_duplicates;
  opts.enableDictionary = FLAGS_enable_dictionary;
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
  opts.maxNumVarArgs = FLAGS_max_num_varargs;
  opts.enableVariadicSignatures = FLAGS_enable_variadic_signatures;
  opts.enableDereference = FLAGS_enable_dereference;
  opts.enableComplexTypes = FLAGS_bolt_fuzzer_enable_complex_types;
  opts.enableColumnReuse = FLAGS_bolt_fuzzer_enable_column_reuse;
  opts.enableExpressionReuse = FLAGS_bolt_fuzzer_enable_expression_reuse;
  opts.functionTickets = FLAGS_assign_function_tickets;
  opts.nullRatio = FLAGS_null_ratio;
  opts.specialForms = FLAGS_special_forms;
  opts.useOnlyFunctions = FLAGS_only;
  opts.skipFunctions = skipFunctions;
  return opts;
}

ExpressionFuzzerVerifier::Options getExpressionFuzzerVerifierOptions(
    const std::unordered_set<std::string>& skipFunctions,
    const std::unordered_map<std::string, std::string>& queryConfigs) {
  ExpressionFuzzerVerifier::Options opts;
  opts.steps = FLAGS_steps;
  opts.durationSeconds = FLAGS_duration_sec;
  opts.batchSize = FLAGS_batch_size;
  opts.retryWithTry = FLAGS_retry_with_try;
  opts.findMinimalSubexpression = FLAGS_find_minimal_subexpression;
  opts.disableConstantFolding = FLAGS_disable_constant_folding;
  opts.reproPersistPath = FLAGS_repro_persist_path;
  opts.persistAndRunOnce = FLAGS_persist_and_run_once;
  opts.lazyVectorGenerationRatio = FLAGS_lazy_vector_generation_ratio;
  opts.maxExpressionTreesPerStep = FLAGS_max_expression_trees_per_step;
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
