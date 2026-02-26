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
  bolt_functions_spark_test
  ArithmeticTest.cpp
  ArrayAppendTest.cpp
  ArrayFlattenTest.cpp
  ArrayGetTest.cpp
  ArrayInsertTest.cpp
  ArrayMaxTest.cpp
  ArrayMinTest.cpp
  ArrayPrependTest.cpp
  ArrayShuffleTest.cpp
  ArraySliceSumTest.cpp
  ArrayUnionTest.cpp
  AtLeastNNonNullsTest.cpp
  Base64Test.cpp
  BitwiseTest.cpp
  ComparisonsTest.cpp
  DecimalArithmeticFuncTest.cpp
  DecimalArithmeticTest.cpp
  DecimalCompareTest.cpp
  DecimalRoundTest.cpp
  DecimalUtilTest.cpp
  ElementAtTest.cpp
  FactorialTest.cpp
  FromJsonTest.cpp
  FromJsonTest.cpp
  GetArrayStructFieldsTest.cpp
  GetStructFieldTest.cpp
  HashTest.cpp
  HiveHashTest.cpp
  ICURegexFunctionsTest.cpp
  InTest.cpp
  JsonFunctionSonicOnDemandPerfTest.cpp
  JsonFunctionTest.cpp
  JsonObjectKeysTest.cpp
  JsonSplitTest.cpp
  JsonToMapTest.cpp
  JsonTupleTest.cpp
  LeastGreatestTest.cpp
  MakeDecimalTest.cpp
  MakeTimestampTest.cpp
  MapConcatTest.cpp
  MapTest.cpp
  MaskTest.cpp
  MightContainTest.cpp
  MightContainTest.cpp
  RaiseErrorTest.cpp
  RandTest.cpp
  RegexFunctionsTest.cpp
  SizeTest.cpp
  SortArrayTest.cpp
  SparkArraySortTest.cpp
  SparkCastExprTest.cpp
  SparkPartitionIdTest.cpp
  SparkSqlDateTimeFunctionsTest.cpp
  SplitFunctionsTest.cpp
  StringTest.cpp
  UdfMapArrayCombineTest.cpp
  UdfMapFilterKeysTest.cpp
  UdfMapUpdateTest.cpp
  UdfStringToLongArrayTest.cpp
  UnBase64Test.cpp
  UnscaledValueFunctionTest.cpp
  URLFunctionsTest.cpp
  XxHash64Test.cpp
)

add_test(
  NAME bolt_functions_spark_test
  COMMAND bolt_functions_spark_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  bolt_functions_spark_test
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
)
