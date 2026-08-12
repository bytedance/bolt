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

#include "bolt/exec/fuzzer/FuzzerFlags.h"

#include <stdexcept>
#include <string>

DEFINE_bool(
    bolt_fuzzer_enable_column_reuse,
    false,
    "Enable generation of expressions where one input column can be "
    "used by multiple subexpressions");

DEFINE_bool(
    bolt_fuzzer_enable_complex_types,
    false,
    "Enable testing of function signatures with complex argument or return types.");

DEFINE_bool(
    bolt_fuzzer_enable_expression_reuse,
    false,
    "Enable generation of expressions that reuses already generated "
    "subexpressions.");

DEFINE_bool(
    bolt_fuzzer_disable_constant_folding,
    false,
    "Disable constant-folding in the common evaluation path.");

DEFINE_bool(
    bolt_fuzzer_enable_dereference,
    false,
    "Allow fuzzer to generate random expressions with dereference and row_constructor functions.");

DEFINE_bool(
    bolt_fuzzer_enable_dictionary,
    true,
    "Whether to allow dictionary encoding.");

DEFINE_bool(
    bolt_fuzzer_enable_duckdb_verification,
    true,
    "Whether to compare with DuckDB results.");

DEFINE_bool(
    bolt_fuzzer_enable_duplicates,
    false,
    "Whether to allow duplicated data (i.e., reuse already generated tests).");

DEFINE_bool(bolt_fuzzer_enable_hugeint, true, "Whether to generate HUGEINT.");

DEFINE_bool(
    bolt_fuzzer_enable_oom_injection,
    false,
    "When enabled OOMs will randomly be triggered while executing query "
    "plans. The goal of this mode is to ensure unexpected exceptions "
    "aren't thrown and the process isn't killed in the process of cleaning "
    "up after failures. Therefore, results are not compared when this is "
    "enabled. Note that this option only works in debug builds.");

DEFINE_bool(
    bolt_fuzzer_enable_sorted_aggregations,
    true,
    "When true, generates plans with aggregations over sorted inputs");

DEFINE_bool(
    bolt_fuzzer_enable_spill,
    true,
    "Whether to test plans with spilling enabled");

DEFINE_bool(
    bolt_fuzzer_enable_string_incremental_generation,
    false,
    "Whether the generated strings could share common substrings.");

DEFINE_bool(
    bolt_fuzzer_enable_variadic_signatures,
    false,
    "Enable testing of function signatures with variadic arguments.");

DEFINE_bool(
    bolt_fuzzer_enable_window_reference_verification,
    false,
    "When true, the results of the window aggregation are compared to reference DB results");

DEFINE_bool(
    bolt_fuzzer_find_minimal_subexpression,
    false,
    "Automatically seeks minimum failed subexpression on result mismatch");

DEFINE_bool(
    bolt_fuzzer_inject_failure,
    false,
    "Inject a failure for testing repro");

DEFINE_bool(
    bolt_fuzzer_log_signature_stats,
    false,
    "Log statistics about function signatures");

DEFINE_bool(
    bolt_fuzzer_persist_and_run_once,
    false,
    "Persist repro info before evaluation and only run one iteration. "
    "This is to rerun with the seed number and persist repro info upon a "
    "crash failure. Only effective if repro_persist_path is set.");

DEFINE_bool(
    bolt_fuzzer_retry_with_try,
    false,
    "Retry failed expressions by wrapping it using a try() statement.");

DEFINE_bool(
    bolt_fuzzer_string_variable_length,
    false,
    "Whether to generate variable lengths of strings,"
    "or all the generated strings should have the same length.");

DEFINE_double(
    bolt_fuzzer_lazy_vector_generation_ratio,
    0.0,
    "Specifies the probability with which columns in the input row "
    "vector will be selected to be wrapped in lazy encoding "
    "(expressed as double from 0 to 1).");

DEFINE_double(
    bolt_fuzzer_null_ratio,
    0.1,
    "Chance of adding a null constant to the plan, or null value in a vector "
    "(expressed as double from 0 to 1).");

DEFINE_int32(
    bolt_fuzzer_batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(
    bolt_fuzzer_max_level_of_nesting,
    10,
    "Max levels of expression nesting. The default value is 10 and minimum is 1.");

DEFINE_int32(
    bolt_fuzzer_drivers_per_task,
    4,
    "Number of threads in each task in shuffle");

DEFINE_int32(
    bolt_fuzzer_duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --bolt_fuzzer_steps iterations and exits.");

DEFINE_int32(
    bolt_fuzzer_max_buffer_mb,
    20,
    "Max buffer size for output/exchange per task");

DEFINE_int32(
    bolt_fuzzer_max_expression_trees_per_step,
    1,
    "This sets an upper limit on the number of expression trees to generate "
    "per step. These trees would be executed in the same ExprSet and can "
    "reuse already generated columns and subexpressions (if reuse is "
    "enabled).");

DEFINE_int32(
    bolt_fuzzer_max_num_varargs,
    5,
    "The maximum number of variadic arguments fuzzer will generate for "
    "functions that accept variadic arguments. Fuzzer will generate up to "
    "max_num_varargs arguments for the variadic list in addition to the "
    "required arguments by the function.");

DEFINE_int32(
    bolt_fuzzer_max_tasks_per_stage,
    16,
    "Max number of sources/destinations");
DEFINE_int32(bolt_fuzzer_num_batches, 10, "The number of generated vectors.");
DEFINE_int32(bolt_fuzzer_steps, 10, "Number of plans to generate and execute.");
DEFINE_int32(
    bolt_fuzzer_string_length,
    100,
    "The max length of generated strings.");

DEFINE_int64(
    bolt_fuzzer_shuffle_bytes,
    4UL << 30,
    "Shuffle data volume in each step");

DEFINE_uint64(
    bolt_fuzzer_seed,
    0,
    "Initial seed for random number generator used to reproduce previous "
    "results (0 means start with random seed).");

DEFINE_string(
    bolt_fuzzer_assign_function_tickets,
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

// The flags below are used to initialize ExpressionFuzzer::options.
DEFINE_string(
    bolt_fuzzer_only,
    "",
    "If specified, Fuzzer will only choose functions from "
    "this comma separated list of function names "
    "(e.g: --bolt_fuzzer_only \"split\" or --bolt_fuzzer_only \"substr,ltrim\").");

DEFINE_string(
    bolt_fuzzer_presto_url,
    "",
    "Presto coordinator URI along with port. If set, we use Presto "
    "source of truth. Otherwise, use DuckDB. Example: "
    "--bolt_fuzzer_presto_url=http://127.0.0.1:8080");

DEFINE_string(
    bolt_fuzzer_replay,
    "",
    "File to replay. Files are produced on failure in --bolt_fuzzer_repro_path");

DEFINE_string(
    bolt_fuzzer_repro_path,
    ".",
    "Path for writing repro files in case of failure");

DEFINE_string(
    bolt_fuzzer_repro_persist_path,
    "",
    "Directory path for persistence of data and SQL when fuzzer fails for "
    "future reproduction. Empty string disables this feature.");

DEFINE_string(
    bolt_fuzzer_special_forms,
    "and,or,cast,coalesce,if,switch",
    "Comma-separated list of special forms to use in generated expression. "
    "Supported special forms: and, or, coalesce, if, switch, cast.");

namespace bytedance::bolt::fuzzer {
namespace {

void setFlagDefault(const char* name, const char* value) {
  if (gflags::SetCommandLineOptionWithMode(
          name, value, gflags::SET_FLAGS_DEFAULT)
          .empty()) {
    throw std::invalid_argument(
        std::string("Failed to set default for fuzzer flag: ") + name);
  }
}

} // namespace

void setJoinFuzzerFlagDefaults() {
  setFlagDefault("bolt_fuzzer_num_batches", "5");
}

void setParquetFuzzerFlagDefaults() {
  setFlagDefault("bolt_fuzzer_steps", "0");
}

void setSparkExpressionFuzzerFlagDefaults() {
  setFlagDefault("bolt_fuzzer_seed", "123456");
}

} // namespace bytedance::bolt::fuzzer
