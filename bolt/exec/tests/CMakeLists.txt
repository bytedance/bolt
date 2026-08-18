# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
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

add_subdirectory(utils)

add_executable(
  aggregate_companion_functions_test
  AggregateCompanionAdapterTest.cpp
  AggregateCompanionSignaturesTest.cpp
  DummyAggregateFunction.cpp
)

add_test(
  NAME aggregate_companion_functions_test
  COMMAND
    aggregate_companion_functions_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(aggregate_companion_functions_test PRIVATE bolt_gtest_main)

add_executable(
  bolt_exec_test
  AddressableNonNullValueListTest.cpp
  AggregateFunctionRegistryTest.cpp
  ArrowStreamTest.cpp
  AssignUniqueIdTest.cpp
  AsyncConnectorTest.cpp
  ConcatFilesSpillMergeStreamTest.cpp
  ContainerRowSerdeTest.cpp
  CustomJoinTest.cpp
  EnforceSingleRowTest.cpp
  ExchangeClientTest.cpp
  ExpandTest.cpp
  FilterProjectTest.cpp
  FunctionResolutionTest.cpp
  LimitTest.cpp
  LocalPartitionTest.cpp
  LocalShuffleTest.cpp
  MarkDistinctTest.cpp
  MemoryReclaimerTest.cpp
  MergeTest.cpp
  MergerTest.cpp
  MorselDrivenTest.cpp
  OneWayStatusFlagTest.cpp
  OperatorTraceTest.cpp
  OrderByTest.cpp
  OutputBufferManagerTest.cpp
  PlanNodeSerdeTest.cpp
  PlanNodeToStringTest.cpp
  PrintPlanWithStatsTest.cpp
  ProbeOperatorStateTest.cpp
  RoundRobinPartitionFunctionTest.cpp
  RowContainerTest.cpp
  RowNumberTest.cpp
  RowStreamingWindowTest.cpp
  SortAlgoTest.cpp
  SortAndWindowTest.cpp
  SortBufferTest.cpp
  SortWindowTest.cpp
  SpillOperatorGroupTest.cpp
  SpillTest.cpp
  SplitToStringTest.cpp
  SqlTest.cpp
  StreamingAggregationTest.cpp
  TaskListenerTest.cpp
  ThreadDebugInfoTest.cpp
  TopNRowNumberTest.cpp
  TopNTest.cpp
  TraceUtilTest.cpp
  UnorderedStreamReaderTest.cpp
  ValuesTest.cpp
  VectorHasherTest.cpp
  WindowFunctionRegistryTest.cpp
  WindowTest.cpp
)

add_executable(
  row_based_spill_test
  RowBasedCompareTest.cpp
  RowToColumnVectorTest.cpp
)
if(KERNEL_SUPPORTS_IO_URING)
  add_executable(iouring_spill_test AsyncSpillerTest.cpp)
endif()

add_executable(bolt_exec_aggregation_test AggregationTest.cpp)

add_executable(bolt_exec_spiller_test SpillerTest.cpp)

add_executable(
  bolt_exec_multifragment_window_test
  MultiFragmentTest.cpp
  SpillableWindowTest.cpp
)

add_executable(
  bolt_exec_hash_join_test
  HashBitRangeTest.cpp
  HashJoinBridgeTest.cpp
  HashJoinTest.cpp
  HashPartitionFunctionTest.cpp
  HashTableTest.cpp
)

add_executable(
  bolt_exec_merge_join_test
  MergeJoinTest.cpp
  NestedLoopJoinTest.cpp
)

add_executable(bolt_exec_scan_test TableScanTest.cpp)

add_executable(bolt_exec_unested_test UnnestTest.cpp)

add_executable(bolt_exec_table_writer_test TableWriteTest.cpp)

add_executable(
  bolt_exec_infra_test
  AssertQueryBuilderTest.cpp
  DriverTest.cpp
  FunctionSignatureBuilderTest.cpp
  GroupedExecutionTest.cpp
  OperatorUtilsTest.cpp
  PlanBuilderTest.cpp
  PrestoQueryRunnerTest.cpp
  QueryAssertionsTest.cpp
  TaskTest.cpp
  TreeOfLosersTest.cpp
)

add_test(NAME bolt_exec_test COMMAND bolt_exec_test WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
set_tests_properties(
  bolt_exec_test
  PROPERTIES
    COST
      1
    TIMEOUT
      3000
)

add_test(
  NAME row_based_spill_test
  COMMAND
    row_based_spill_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

if(KERNEL_SUPPORTS_IO_URING)
  add_test(
    NAME iouring_spill_test
    COMMAND
      iouring_spill_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )
  set_tests_properties(
    iouring_spill_test
    PROPERTIES
      COST
        1
  )
endif()

add_test(
  NAME bolt_exec_infra_test
  COMMAND
    bolt_exec_infra_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME bolt_exec_aggregation_test
  COMMAND
    bolt_exec_aggregation_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME bolt_exec_spiller_test
  COMMAND
    bolt_exec_spiller_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
set_tests_properties(
  bolt_exec_spiller_test
  PROPERTIES
    COST
      1
    TIMEOUT
      3000
)

add_test(
  NAME bolt_exec_multifragment_window_test
  COMMAND
    bolt_exec_multifragment_window_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
set_tests_properties(
  bolt_exec_multifragment_window_test
  PROPERTIES
    COST
      1
    TIMEOUT
      3000
)

bolt_add_sharded_gtest(bolt_exec_hash_join_test 5)

bolt_add_sharded_gtest(bolt_exec_scan_test 4)

add_test(
  NAME bolt_exec_merge_join_test
  COMMAND
    bolt_exec_merge_join_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME bolt_exec_unested_test
  COMMAND
    bolt_exec_unested_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME bolt_exec_table_writer_test
  COMMAND
    bolt_exec_table_writer_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

set(
  BOLT_EXEC_TEST_DEPENDENCIES
  bolt_gtest_main
  GTest::gmock
)

target_link_libraries(bolt_exec_test ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_infra_test ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(row_based_spill_test ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_aggregation_test ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_spiller_test ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_multifragment_window_test ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_hash_join_test ${BOLT_EXEC_TEST_DEPENDENCIES})

if(KERNEL_SUPPORTS_IO_URING)
  target_link_libraries(iouring_spill_test ${BOLT_EXEC_TEST_DEPENDENCIES})
endif()

target_link_libraries(bolt_exec_scan_test ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_merge_join_test ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_unested_test ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_table_writer_test ${BOLT_EXEC_TEST_DEPENDENCIES})

add_executable(bolt_in_10_min_demo BoltIn10MinDemo.cpp)

target_link_libraries(bolt_in_10_min_demo bolt_testutils)

# Join Fuzzer.
add_library(bolt_join_fuzzer STATIC JoinFuzzer.cpp)

target_link_libraries(
  bolt_join_fuzzer
  bolt_testutils
  bolt_fuzzer_flags
)

add_executable(bolt_join_fuzzer_test JoinFuzzerTest.cpp)

target_link_libraries(
  bolt_join_fuzzer_test
  PRIVATE
    bolt_join_fuzzer
    bolt_testutils
    GTest::gtest
)

add_executable(bolt_exchange_fuzzer_test ExchangeFuzzer.cpp)

target_link_libraries(
  bolt_exchange_fuzzer_test
  bolt_fuzzer_flags
  bolt_testutils
  GTest::gtest
)

add_executable(bolt_aggregation_runner_test AggregationRunnerTest.cpp)

target_link_libraries(
  bolt_aggregation_runner_test
  bolt_aggregation_fuzzer
  bolt_testutils
  GTest::gtest
)

add_executable(spark_aggregation_runner_test SparkAggregationRunnerTest.cpp)

target_link_libraries(
  spark_aggregation_runner_test
  bolt_aggregation_fuzzer
  bolt_testutils
  GTest::gtest
)

add_executable(spark_window_runner_test SparkWindowRunnerTest.cpp)

target_link_libraries(
  spark_window_runner_test
  bolt_window_fuzzer
  bolt_testutils
  GTest::gtest
)

add_library(
  bolt_simple_aggregate
  STATIC
  SimpleArrayAggAggregate.cpp
  SimpleAverageAggregate.cpp
)

target_link_libraries(bolt_simple_aggregate PUBLIC bolt_testutils)

add_executable(bolt_simple_aggregate_test SimpleAggregateAdapterTest.cpp)

target_link_libraries(
  bolt_simple_aggregate_test
  bolt_simple_aggregate
  bolt_gtest_main
)

add_library(
  bolt_spiller_join_benchmark_base
  STATIC
  JoinSpillInputBenchmarkBase.cpp
  SpillerBenchmarkBase.cpp
)
target_link_libraries(
  bolt_spiller_join_benchmark_base
  bolt_testutils
  pthread
)

add_executable(bolt_spiller_join_benchmark SpillerJoinInputBenchmarkTest.cpp)
target_link_libraries(
  bolt_spiller_join_benchmark
  PUBLIC
    bolt_spiller_join_benchmark_base
    bolt_testutils
)

add_library(
  bolt_spiller_aggregate_benchmark_base
  STATIC
  AggregateSpillBenchmarkBase.cpp
  SpillerBenchmarkBase.cpp
)
target_link_libraries(
  bolt_spiller_aggregate_benchmark_base
  bolt_testutils
  pthread
)

add_executable(bolt_spiller_aggregate_benchmark SpillerAggregateBenchmarkTest.cpp)
target_link_libraries(
  bolt_spiller_aggregate_benchmark
  bolt_testutils
  bolt_spiller_aggregate_benchmark_base
)

add_executable(cpr_http_client_test CprHttpClientTest.cpp)
add_test(
  NAME cpr_http_client_test
  COMMAND
    cpr_http_client_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(cpr_http_client_test PRIVATE bolt_gtest_main)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

if(${BOLT_ENABLE_CUDF})
  target_compile_definitions(bolt_exec_test PRIVATE BOLT_HAS_CUDF=1)
  target_compile_definitions(bolt_exec_aggregation_test PRIVATE BOLT_HAS_CUDF=1)
endif()
