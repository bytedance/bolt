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

DECLARE_bool(bolt_enable_avx2);
DECLARE_bool(bolt_enable_bmi2);
DECLARE_bool(bolt_enable_memory_usage_track_in_default_memory_pool);
DECLARE_bool(bolt_exception_system_stacktrace_enabled);
DECLARE_bool(bolt_exception_user_stacktrace_enabled);
DECLARE_bool(bolt_experimental_save_input_on_fatal_signal);
DECLARE_bool(bolt_memory_leak_check_enabled);
DECLARE_bool(bolt_memory_pool_capacity_transfer_across_tasks);
DECLARE_bool(bolt_memory_pool_debug_enabled);
DECLARE_bool(bolt_memory_use_hugepages);
DECLARE_bool(bolt_suppress_memory_capacity_exceeding_error_message);
DECLARE_bool(bolt_time_allocations);
DECLARE_bool(bolt_collect_import_time);
DECLARE_bool(bolt_experimental_enable_legacy_cast);
DECLARE_bool(bolt_force_eval_simplified);
DECLARE_bool(bolt_ssd_odirect);
DECLARE_bool(bolt_ssd_verify_write);
DECLARE_bool(bolt_use_ws_vread);

DECLARE_int32(bolt_exception_system_stacktrace_rate_limit_ms);
DECLARE_int32(bolt_exception_user_stacktrace_rate_limit_ms);
DECLARE_int32(cache_prefetch_min_pct);
DECLARE_int32(bolt_shuffle_zstd_compression_level);

DECLARE_string(bolt_save_input_on_expression_any_failure_path);
DECLARE_string(bolt_save_input_on_expression_system_failure_path);
