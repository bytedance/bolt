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

#pragma once

#include <gflags/gflags.h>

DECLARE_bool(bolt_fuzzer_enable_column_reuse);
DECLARE_bool(bolt_fuzzer_enable_complex_types);
DECLARE_bool(bolt_fuzzer_enable_expression_reuse);
DECLARE_bool(bolt_fuzzer_disable_constant_folding);
DECLARE_bool(bolt_fuzzer_enable_dereference);
DECLARE_bool(bolt_fuzzer_enable_dictionary);
DECLARE_bool(bolt_fuzzer_enable_duckdb_verification);
DECLARE_bool(bolt_fuzzer_enable_duplicates);
DECLARE_bool(bolt_fuzzer_enable_hugeint);
DECLARE_bool(bolt_fuzzer_enable_oom_injection);
DECLARE_bool(bolt_fuzzer_enable_sorted_aggregations);
DECLARE_bool(bolt_fuzzer_enable_spill);
DECLARE_bool(bolt_fuzzer_enable_string_incremental_generation);
DECLARE_bool(bolt_fuzzer_enable_variadic_signatures);
DECLARE_bool(bolt_fuzzer_enable_window_reference_verification);
DECLARE_bool(bolt_fuzzer_find_minimal_subexpression);
DECLARE_bool(bolt_fuzzer_inject_failure);
DECLARE_bool(bolt_fuzzer_log_signature_stats);
DECLARE_bool(bolt_fuzzer_persist_and_run_once);
DECLARE_bool(bolt_fuzzer_retry_with_try);
DECLARE_bool(bolt_fuzzer_string_variable_length);

DECLARE_double(bolt_fuzzer_lazy_vector_generation_ratio);
DECLARE_double(bolt_fuzzer_null_ratio);

DECLARE_int32(bolt_fuzzer_batch_size);
DECLARE_int32(bolt_fuzzer_max_level_of_nesting);
DECLARE_int32(bolt_fuzzer_drivers_per_task);
DECLARE_int32(bolt_fuzzer_duration_sec);
DECLARE_int32(bolt_fuzzer_max_buffer_mb);
DECLARE_int32(bolt_fuzzer_max_expression_trees_per_step);
DECLARE_int32(bolt_fuzzer_max_num_varargs);
DECLARE_int32(bolt_fuzzer_max_tasks_per_stage);
DECLARE_int32(bolt_fuzzer_num_batches);
DECLARE_int32(bolt_fuzzer_steps);
DECLARE_int32(bolt_fuzzer_string_length);

DECLARE_int64(bolt_fuzzer_shuffle_bytes);
DECLARE_uint64(bolt_fuzzer_seed);

DECLARE_string(bolt_fuzzer_assign_function_tickets);
DECLARE_string(bolt_fuzzer_only);
DECLARE_string(bolt_fuzzer_presto_url);
DECLARE_string(bolt_fuzzer_replay);
DECLARE_string(bolt_fuzzer_repro_path);
DECLARE_string(bolt_fuzzer_repro_persist_path);
DECLARE_string(bolt_fuzzer_special_forms);

namespace bytedance::bolt::fuzzer {

void setJoinFuzzerFlagDefaults();
void setParquetFuzzerFlagDefaults();
void setSparkExpressionFuzzerFlagDefaults();

} // namespace bytedance::bolt::fuzzer
