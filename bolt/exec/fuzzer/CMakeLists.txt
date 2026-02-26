# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# This file has been modified by ByteDance Ltd. and/or its affiliates on
# 2025-11-11.
#
# Original file was released under the Apache License 2.0,
# with the full license text available at:
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This modified file is released under the same license.
# --------------------------------------------------------------------------

bolt_add_library(bolt_fuzzer_util DuckQueryRunner.cpp PrestoQueryRunner.cpp)

target_link_libraries(
  bolt_fuzzer_util bolt_core bolt_exec_test_lib cpr::cpr bolt_type_parser Folly::folly
)

bolt_add_library(bolt_aggregation_fuzzer_base AggregationFuzzerBase.cpp)

target_link_libraries(
  bolt_aggregation_fuzzer_base
  bolt_exec_test_lib
  bolt_temp_path
  bolt_common_base
  bolt_file
  bolt_hive_connector
  bolt_dwio_dwrf_reader
  bolt_dwio_dwrf_writer
  bolt_type
  bolt_vector_fuzzer
  bolt_fuzzer_util
  bolt_expression_test_utility
  bolt_vector
  bolt_core
)

bolt_add_library(bolt_aggregation_fuzzer AggregationFuzzer.cpp)

target_link_libraries(
  bolt_aggregation_fuzzer
  bolt_type
  bolt_vector_fuzzer
  bolt_exec_test_lib
  bolt_expression_test_utility
  bolt_aggregation_fuzzer_base
  bolt_fuzzer_util
)

bolt_add_library(bolt_window_fuzzer WindowFuzzer.cpp)

target_link_libraries(
  bolt_window_fuzzer
  bolt_fuzzer_util
  bolt_type
  bolt_vector_fuzzer
  bolt_exec_test_lib
  bolt_expression_test_utility
  bolt_aggregation_fuzzer_base
  bolt_temp_path
)
