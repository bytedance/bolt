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

add_executable(bolt_aggregation_fuzzer_test AggregationFuzzerTest.cpp)

target_link_libraries(
  bolt_aggregation_fuzzer_test
  bolt_aggregation_fuzzer
  bolt_aggregation_fuzzer_base
  bolt_aggregates
  bolt_window
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)

# Window Fuzzer
add_executable(bolt_window_fuzzer_test WindowFuzzerTest.cpp)

target_link_libraries(
  bolt_window_fuzzer_test
  bolt_window_fuzzer
  bolt_fuzzer_util
  bolt_aggregates
  bolt_window
  bolt_expression_test_utility
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)
