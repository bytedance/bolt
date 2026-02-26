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

bolt_add_library(bolt_expression_test_utility ArgumentTypeFuzzer.cpp FuzzerToolkit.cpp)

target_link_libraries(bolt_expression_test_utility bolt_type bolt_expression_functions GTest::gtest)

# exclude bolt_expression_fuzzer from bolt_testutils
# to avoid multiple definition error
add_library(
  bolt_expression_fuzzer OBJECT ArgumentTypeFuzzer.cpp ExpressionFuzzer.cpp ExpressionFuzzerVerifier.cpp
                         FuzzerRunner.cpp
)

target_link_libraries(
  bolt_expression_fuzzer
  bolt_expression_verifier
  bolt_type
  bolt_vector_fuzzer
  bolt_vector_test_lib
  bolt_function_registry
  bolt_expression_test_utility
)

add_executable(bolt_expression_fuzzer_test ExpressionFuzzerTest.cpp)

target_link_libraries(
  bolt_expression_fuzzer_test PRIVATE
  bolt_expression_fuzzer
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)

add_executable(spark_expression_fuzzer_test SparkExpressionFuzzerTest.cpp)

target_link_libraries(
  spark_expression_fuzzer_test PRIVATE
  bolt_expression_fuzzer
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)

find_package(Arrow CONFIG REQUIRED)
add_executable(spark_double_to_decimal_fuzzer_test DoubleToDecimalFuzzerTest.cpp)

target_link_libraries(
  spark_double_to_decimal_fuzzer_test
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
