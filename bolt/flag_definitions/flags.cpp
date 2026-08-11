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

#include <gflags/gflags.h>

DEFINE_int32(
    bolt_memory_num_shared_leaf_pools,
    32,
    "Deprecated compatibility flag; no longer used");

DEFINE_bool(
    bolt_time_allocations,
    true,
    "Record time and volume for large allocation/free");

// Used in common/base/BoltException.cpp
DEFINE_bool(
    bolt_exception_user_stacktrace_enabled,
    false,
    "Enable the stacktrace for user type of BoltException");

DEFINE_bool(
    bolt_exception_system_stacktrace_enabled,
    true,
    "Enable the stacktrace for system type of BoltException");

DEFINE_int32(
    bolt_exception_user_stacktrace_rate_limit_ms,
    0, // effectively turns off rate-limiting
    "Min time interval in milliseconds between stack traces captured in"
    " user type of BoltException; off when set to 0 (the default)");

DEFINE_int32(
    bolt_exception_system_stacktrace_rate_limit_ms,
    0, // effectively turns off rate-limiting
    "Min time interval in milliseconds between stack traces captured in"
    " system type of BoltException; off when set to 0 (the default)");

// Used in common/base/ProcessBase.cpp

DEFINE_bool(avx2, true, "Enables use of AVX2 when available");

DEFINE_bool(bmi2, true, "Enables use of BMI2 when available");

// Used in exec/Expr.cpp

DEFINE_string(
    bolt_save_input_on_expression_any_failure_path,
    "",
    "Enable saving input vector and expression SQL on any failure during "
    "expression evaluation. Specifies the directory to use for storing the "
    "vectors and expression SQL strings.");

DEFINE_string(
    bolt_save_input_on_expression_system_failure_path,
    "",
    "Enable saving input vector and expression SQL on system failure during "
    "expression evaluation. Specifies the directory to use for storing the "
    "vectors and expression SQL strings. This flag is ignored if "
    "bolt_save_input_on_expression_any_failure_path is set.");

// TODO: deprecate this once all the memory leak issues have been fixed in
// existing meta internal use cases.
DEFINE_bool(
    bolt_memory_leak_check_enabled,
    false,
    "If true, check fails on any memory leaks in memory pool and memory manager");

DEFINE_bool(
    bolt_memory_pool_debug_enabled,
    false,
    "If true, 'MemoryPool' will be running in debug mode to track the allocation and free call sites to detect the source of memory leak for testing purpose");

// TODO: deprecate this after solves all the use cases that can cause
// significant performance regression by memory usage tracking.
DEFINE_bool(
    bolt_enable_memory_usage_track_in_default_memory_pool,
    false,
    "If true, enable memory usage tracking in the default memory pool");

DEFINE_bool(
    bolt_suppress_memory_capacity_exceeding_error_message,
    false,
    "If true, suppress the verbose error message in memory capacity exceeded "
    "exception. This is only used by test to control the test error output size");

DEFINE_bool(bolt_memory_use_hugepages, true, "Use explicit huge pages");

DEFINE_int32(
    shuffle_zstd_compression_level,
    0,
    "shuffle_zstd_compression_level");

DEFINE_bool(bolt_ssd_odirect, true, "Use O_DIRECT for SSD cache IO");

DEFINE_string(
    testing_only_set_scan_exception_mesg_for_prepare,
    "",
    "Only used in testing env. Throwing std::exception when call SplitReader::prepareSplit");
DEFINE_string(
    testing_only_set_scan_exception_mesg_for_next,
    "",
    "Only used in testing env. Throwing std::exception when call SplitReader::next");
