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

add_executable(
  bolt_expression_test
  CastExprTest.cpp
  CoalesceTest.cpp
  ConjunctTest.cpp
  ConstantFlatVectorReaderTest.cpp
  CustomTypeTest.cpp
  EvalCtxTest.cpp
  EvalSimplifiedTest.cpp
  ExprCompilerTest.cpp
  ExprEncodingsTest.cpp
  ExprStatsTest.cpp
  ExprTest.cpp
  ExprToSubfieldFilterTest.cpp
  FromSignature.cpp
  FunctionCallToSpecialFormTest.cpp
  GenericViewTest.cpp
  GenericWriterTest.cpp
  Main.cpp
  MapViewTest.cpp
  MapWriterTest.cpp
  PeeledEncodingTest.cpp
  ReverseSignatureBinderTest.cpp
  RowViewTest.cpp
  RowWriterTest.cpp
  SignatureBinderTest.cpp
  SimpleFunctionCallNullFreeTest.cpp
  SimpleFunctionInitTest.cpp
  SimpleFunctionPresetNullsTest.cpp
  SimpleFunctionTest.cpp
  SingleSubfieldExtractorTest.cpp
  StringWriterTest.cpp
  StringWriterTest.cpp
  TryExprTest.cpp
  VariadicViewTest.cpp
  VectorReaderTest.cpp
)

add_test(
  NAME bolt_expression_test
  COMMAND bolt_expression_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  bolt_expression_test PRIVATE
    bolt_testutils
    GTest::gtest
    GTest::gtest_main
    GTest::gmock
)

bolt_add_library(bolt_expression_verifier ExpressionVerifier.cpp)

target_link_libraries(
  bolt_expression_verifier bolt_vector_test_lib bolt_vector_fuzzer bolt_type
  bolt_expression_test_utility
)

bolt_add_library(bolt_expression_runner ExpressionRunner.cpp)
target_link_libraries(
  bolt_expression_runner bolt_expression_verifier bolt_functions_prestosql bolt_parse_parser
  GTest::gtest
)

add_executable(bolt_expression_runner_test ExpressionRunnerTest.cpp)
target_link_libraries(
  bolt_expression_runner_test  PRIVATE
  bolt_expression_runner
  bolt_exec_test_lib
  bolt_function_registry
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)

add_executable(spark_expression_runner_test SparkExpressionRunnerTest.cpp)
target_link_libraries(
  spark_expression_runner_test bolt_expression_runner bolt_exec_test_lib bolt_function_registry
  bolt_testutils
  GTest::gtest GTest::gtest_main
)

add_executable(bolt_expression_verifier_unit_test ExpressionVerifierUnitTest.cpp)
target_link_libraries(
  bolt_expression_verifier_unit_test
  bolt_expression_verifier
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_expression_runner_unit_test ExpressionRunnerUnitTest.cpp)
target_link_libraries(
  bolt_expression_runner_unit_test
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)
