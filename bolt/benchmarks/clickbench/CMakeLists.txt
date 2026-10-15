# Copyright (c) ByteDance Ltd. and/or its affiliates
# SPDX-License-Identifier: Apache-2.0

add_library(bolt_clickbench_query_builder OBJECT ClickBenchQueryBuilder.cpp)
target_link_libraries(
  bolt_clickbench_query_builder
  PRIVATE
    bolt_testutils
    fmt::fmt
)

add_library(bolt_clickbench_benchmark_lib OBJECT ClickBenchBenchmark.cpp)
target_link_libraries(
  bolt_clickbench_benchmark_lib
  PRIVATE
    bolt_clickbench_query_builder
    bolt_query_benchmark
    bolt_testutils
    ${FOLLY_BENCHMARK}
    fmt::fmt
)

add_executable(bolt_clickbench_benchmark ClickBenchBenchmarkMain.cpp)
target_link_libraries(
  bolt_clickbench_benchmark
  PRIVATE
    bolt_clickbench_benchmark_lib
    bolt_clickbench_query_builder
    bolt_query_benchmark
    bolt_testutils
    ${FOLLY_BENCHMARK}
)

add_executable(bolt_clickbench_query_builder_test ClickBenchQueryBuilderTest.cpp)
target_link_libraries(
  bolt_clickbench_query_builder_test
  PRIVATE
    bolt_clickbench_query_builder
    bolt_dwio_parquet_reader
    bolt_dwio_parquet_writer
    bolt_gtest_main
)
add_test(NAME bolt_clickbench_query_builder_test COMMAND bolt_clickbench_query_builder_test)
