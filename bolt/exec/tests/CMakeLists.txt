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
  AggregateCompanionAdapterTest.cpp AggregateCompanionSignaturesTest.cpp DummyAggregateFunction.cpp
)

add_test(
  NAME aggregate_companion_functions_test
  COMMAND aggregate_companion_functions_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  aggregate_companion_functions_test bolt_exec bolt_function_registry bolt_process GTest::gtest
  GTest::gtest_main
)

add_executable(
  bolt_exec_test
  AddressableNonNullValueListTest.cpp
  AggregateFunctionRegistryTest.cpp
  ArrowStreamTest.cpp
  AssignUniqueIdTest.cpp
  AsyncConnectorTest.cpp
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
  Main.cpp
  MarkDistinctTest.cpp
  MemoryReclaimerTest.cpp
  MergeTest.cpp
  MorselDrivenTest.cpp
  MultiFragmentTest.cpp
  OneWayStatusFlagTest.cpp
  OperatorTraceTest.cpp
  OperatorUtilsTest.cpp
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
  SpillableWindowTest.cpp
  SpillerTest.cpp
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

add_executable(row_based_spill_test RowBasedCompareTest.cpp RowToColumnVectorTest.cpp)
if(KERNEL_SUPPORTS_IO_URING)
  add_executable(iouring_spill_test AsyncSpillerTest.cpp)
endif()

add_executable(bolt_exec_aggregation_test AggregationTest.cpp Main.cpp)

add_executable(
  bolt_exec_hash_join_test HashBitRangeTest.cpp HashJoinBridgeTest.cpp HashJoinTest.cpp
                           HashPartitionFunctionTest.cpp HashTableTest.cpp Main.cpp
)

add_executable(bolt_exec_merge_join_test Main.cpp MergeJoinTest.cpp NestedLoopJoinTest.cpp)

add_executable(bolt_exec_scan_test Main.cpp TableScanTest.cpp)

add_executable(bolt_exec_unested_test Main.cpp UnnestTest.cpp)

add_executable(bolt_exec_table_writer_test Main.cpp TableWriteTest.cpp)

target_compile_features(bolt_exec_test PRIVATE cxx_std_20)

add_executable(
  bolt_exec_infra_test
  AssertQueryBuilderTest.cpp
  DriverTest.cpp
  FunctionSignatureBuilderTest.cpp
  GroupedExecutionTest.cpp
  Main.cpp
  OperatorUtilsTest.cpp
  PlanBuilderTest.cpp
  PrestoQueryRunnerTest.cpp
  QueryAssertionsTest.cpp
  TaskTest.cpp
  TreeOfLosersTest.cpp
)

add_test(
  NAME bolt_exec_test
  COMMAND bolt_exec_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME row_based_spill_test
  COMMAND row_based_spill_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

if(KERNEL_SUPPORTS_IO_URING)
  add_test(
    NAME iouring_spill_test
    COMMAND iouring_spill_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )
endif()

set_tests_properties(bolt_exec_test PROPERTIES TIMEOUT 3000)

add_test(
  NAME bolt_exec_infra_test
  COMMAND bolt_exec_infra_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME bolt_exec_aggregation_test
  COMMAND bolt_exec_aggregation_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME bolt_exec_hash_join_test
  COMMAND bolt_exec_hash_join_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME bolt_exec_scan_test
  COMMAND bolt_exec_scan_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME bolt_exec_merge_join_test
  COMMAND bolt_exec_merge_join_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME bolt_exec_unested_test
  COMMAND bolt_exec_unested_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(
  NAME bolt_exec_table_writer_test
  COMMAND bolt_exec_table_writer_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

set(BOLT_EXEC_TEST_DEPENDENCIES
    bolt_dwio_common
    bolt_dwio_common_exception
    bolt_dwio_common_test_utils
    bolt_dwio_parquet_reader
    bolt_dwio_parquet_writer
    bolt_dwio_orc_reader
    bolt_dwio_orc_writer
    bolt_exec
    bolt_exec_test_lib
    bolt_functions_json
    bolt_functions_lib
    bolt_functions_prestosql
    bolt_functions_test_lib
    bolt_hive_connector
    bolt_memory
    bolt_serialization
    bolt_test_util
    bolt_type
    bolt_vector
    bolt_vector_fuzzer
    bolt_window
    Boost::atomic
    Boost::context
    Boost::date_time
    Boost::filesystem
    Boost::program_options
    Boost::thread
    Boost::system
    GTest::gtest
    GTest::gtest_main
    GTest::gmock
    ${FOLLY_WITH_DEPENDENCIES}
    bolt_process
    gflags::gflags
    glog::glog
    fmt::fmt
    ${FILESYSTEM}
)

target_link_libraries(bolt_exec_test bolt_aggregates ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(
  bolt_exec_infra_test bolt_presto_serializer bolt_fuzzer_util ${BOLT_EXEC_TEST_DEPENDENCIES}
)

target_link_libraries(row_based_spill_test bolt_aggregates ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_aggregation_test bolt_aggregates ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_hash_join_test bolt_aggregates ${BOLT_EXEC_TEST_DEPENDENCIES})

if(KERNEL_SUPPORTS_IO_URING)
  target_link_libraries(iouring_spill_test bolt_aggregates ${BOLT_EXEC_TEST_DEPENDENCIES})
endif()

target_link_libraries(bolt_exec_scan_test bolt_aggregates ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_merge_join_test bolt_aggregates ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_unested_test bolt_aggregates ${BOLT_EXEC_TEST_DEPENDENCIES})

target_link_libraries(bolt_exec_table_writer_test bolt_aggregates ${BOLT_EXEC_TEST_DEPENDENCIES})

add_executable(bolt_in_10_min_demo BoltIn10MinDemo.cpp)

target_link_libraries(
  bolt_in_10_min_demo
  bolt_aggregates
  bolt_type
  bolt_vector
  bolt_vector_test_lib
  bolt_exec
  bolt_exec_test_lib
  bolt_tpch_connector
  bolt_memory
)

# Join Fuzzer.
add_library(bolt_join_fuzzer JoinFuzzer.cpp)

target_link_libraries(
  bolt_join_fuzzer bolt_type bolt_vector_fuzzer bolt_exec_test_lib bolt_expression_test_utility
)

add_executable(bolt_join_fuzzer_test JoinFuzzerTest.cpp)

target_link_libraries(bolt_join_fuzzer_test bolt_join_fuzzer GTest::gtest GTest::gtest_main)

add_executable(bolt_exchange_fuzzer_test ExchangeFuzzer.cpp)

target_link_libraries(
  bolt_exchange_fuzzer_test
  bolt_exec_test_lib
  bolt_aggregates
  bolt_vector_test_lib
  bolt_vector_fuzzer
  bolt_process
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_aggregation_runner_test AggregationRunnerTest.cpp)

target_link_libraries(
  bolt_aggregation_runner_test
  bolt_aggregation_fuzzer
  bolt_fuzzer_util
  bolt_aggregates
  bolt_vector_test_lib
  GTest::gtest
  GTest::gtest_main
)

add_executable(spark_aggregation_runner_test SparkAggregationRunnerTest.cpp)

target_link_libraries(
  spark_aggregation_runner_test
  bolt_aggregation_fuzzer
  bolt_fuzzer_util
  bolt_aggregates
  bolt_functions_spark_aggregates
  bolt_vector_test_lib
  GTest::gtest
  GTest::gtest_main
)

add_executable(spark_window_runner_test SparkWindowRunnerTest.cpp)

target_link_libraries(
  spark_window_runner_test
  bolt_window_fuzzer
  bolt_fuzzer_util
  bolt_aggregates
  bolt_functions_spark_aggregates
  bolt_functions_spark_window
  bolt_expression_test_utility
  GTest::gtest
  GTest::gtest_main
)

add_library(bolt_simple_aggregate SimpleArrayAggAggregate.cpp SimpleAverageAggregate.cpp)

target_link_libraries(
  bolt_simple_aggregate bolt_exec bolt_expression bolt_expression_functions bolt_aggregates
)

add_executable(bolt_simple_aggregate_test Main.cpp SimpleAggregateAdapterTest.cpp)

target_link_libraries(
  bolt_simple_aggregate_test bolt_simple_aggregate bolt_exec bolt_functions_aggregates_test_lib
  GTest::gtest GTest::gtest_main
)

add_library(
  bolt_spiller_join_benchmark_base JoinSpillInputBenchmarkBase.cpp SpillerBenchmarkBase.cpp
)
target_link_libraries(
  bolt_spiller_join_benchmark_base
  bolt_exec
  bolt_exec_test_lib
  bolt_memory
  bolt_vector_fuzzer
  glog::glog
  gflags::gflags
  Folly::folly
  pthread
)

add_executable(bolt_spiller_join_benchmark SpillerJoinInputBenchmarkTest.cpp)
target_link_libraries(
  bolt_spiller_join_benchmark bolt_exec bolt_exec_test_lib bolt_spiller_join_benchmark_base
)

add_library(
  bolt_spiller_aggregate_benchmark_base AggregateSpillBenchmarkBase.cpp SpillerBenchmarkBase.cpp
)
target_link_libraries(
  bolt_spiller_aggregate_benchmark_base
  bolt_exec
  bolt_exec_test_lib
  bolt_memory
  bolt_vector_fuzzer
  glog::glog
  gflags::gflags
  Folly::folly
  pthread
)

add_executable(bolt_spiller_aggregate_benchmark SpillerAggregateBenchmarkTest.cpp)
target_link_libraries(
  bolt_spiller_aggregate_benchmark bolt_exec bolt_exec_test_lib
  bolt_spiller_aggregate_benchmark_base
)

add_executable(cpr_http_client_test CprHttpClientTest.cpp)
add_test(
  NAME cpr_http_client_test
  COMMAND cpr_http_client_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  cpr_http_client_test bolt_process cpr::cpr GTest::gtest
  GTest::gtest_main stdc++
)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

if(${BOLT_ENABLE_CUDF})
  target_compile_definitions(bolt_exec_test PRIVATE BOLT_HAS_CUDF=1)
  target_compile_definitions(bolt_exec_aggregation_test PRIVATE BOLT_HAS_CUDF=1)
endif()
