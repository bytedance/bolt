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

set(AGGREGATES_TEST_SRC
    AggregationFunctionRegTest.cpp
    ApproxDistinctTest.cpp
    ApproxMostFrequentTest.cpp
    ApproxPercentileTest.cpp
    ArbitraryTest.cpp
    ArrayAggTest.cpp
    ArrayAdditionAggregateTest.cpp
    ArrayCountAggregateTest.cpp
    AverageAggregationTest.cpp
    BitwiseAggregationTest.cpp
    BoolAndOrTest.cpp
    CentralMomentsAggregationTest.cpp
    ChecksumAggregateTest.cpp
    CountAggregationTest.cpp
    CountDistinctTest.cpp
    CountIfAggregationTest.cpp
    CovarianceAggregationTest.cpp
    EntropyAggregationTest.cpp
    GeometricMeanTest.cpp
    HistogramTest.cpp
    Main.cpp
    MapAccumulatorTest.cpp
    MapSumAggregateTest.cpp
    MinMaxTest.cpp
    MultiMapAggTest.cpp
    PrestoHasherTest.cpp
    SetAggTest.cpp
    SetUnionTest.cpp
    SumTest.cpp
    MapAggTest.cpp
    MapUnionAggregationTest.cpp
    MapUnionSumTest.cpp
    ReduceAggTest.cpp
    VarianceAggregationTest.cpp
    MaxSizeForStatsTest.cpp
    SumDataSizeForStatsTest.cpp
    MapUnionAvgTest.cpp
    MapUnionCountTest.cpp
    MapUnionMinMaxTest.cpp
    AggregationLazyInputTest.cpp
    BitDaysOrAggregationTest.cpp
)

if(NOT BOLT_ENABLE_SPARK_COMPATIBLE)
  list(APPEND AGGREGATES_TEST_SRC HiveUDAFPercentileTest.cpp)
endif()

add_executable(bolt_aggregates_test ${AGGREGATES_TEST_SRC})

add_executable(bolt_minmaxby_aggregates_test Main.cpp MinMaxByAggregationTest.cpp)

add_test(
  NAME bolt_aggregates_test
  COMMAND bolt_aggregates_test
  WORKING_DIRECTORY .
)

add_test(
  NAME bolt_minmaxby_aggregates_test
  COMMAND bolt_minmaxby_aggregates_test
  WORKING_DIRECTORY .
)

set(BOLT_AGGREGATES_TEST_DEPENDENCIES
    bolt_aggregates
    bolt_core
    bolt_dwio_common_test_utils
    bolt_exec
    bolt_exec_test_lib
    bolt_file
    bolt_functions_aggregates
    bolt_functions_aggregates_test_lib
    bolt_functions_test_lib
    bolt_functions_prestosql
    bolt_functions_lib
    bolt_hive_connector
    bolt_simple_aggregate
    bolt_type
    bolt_vector_fuzzer
    bolt_testutils
    gflags::gflags
    GTest::gtest
    GTest::gtest_main
)

target_link_libraries(bolt_aggregates_test ${BOLT_AGGREGATES_TEST_DEPENDENCIES})

target_link_libraries(bolt_minmaxby_aggregates_test ${BOLT_AGGREGATES_TEST_DEPENDENCIES})
