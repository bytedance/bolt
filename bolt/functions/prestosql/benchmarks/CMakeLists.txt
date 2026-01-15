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

set(BENCHMARK_DEPENDENCIES_NO_FUNC
    bolt_functions_test_lib
    bolt_exec
    bolt_exec_test_lib
    gflags::gflags
    Folly::folly
    ${FOLLY_BENCHMARK}
    bolt_benchmark_builder
)

set(BENCHMARK_DEPENDENCIES
  bolt_functions_prestosql
  bolt_functions_lib bolt_vector_fuzzer
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  bolt_testutils
)

add_executable(bolt_functions_prestosql_benchmarks_array_contains ArrayContainsBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_array_contains ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_functions_prestosql_benchmarks_array_min_max ArrayMinMaxBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_array_min_max ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_functions_prestosql_benchmarks_array_position ArrayPositionBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_array_position ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_functions_prestosql_benchmarks_array_sum ArraySumBenchmark.cpp)

target_link_libraries(bolt_functions_prestosql_benchmarks_array_sum ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_functions_prestosql_benchmarks_width_bucket WidthBucketBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_width_bucket ${BENCHMARK_DEPENDENCIES})

add_executable(
  bolt_functions_prestosql_benchmarks_string_ascii_utf_functions
  StringAsciiUTFFunctionBenchmarks.cpp
)
target_link_libraries(
  bolt_functions_prestosql_benchmarks_string_ascii_utf_functions ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_prestosql_benchmarks_not NotBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_not ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_functions_prestosql_benchmarks_date_time DateTimeBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_date_time ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_functions_prestosql_benchmarks_bitwise BitwiseBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_bitwise ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_functions_prestosql_benchmarks_in InBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_in ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_functions_prestosql_benchmarks_map_input MapInputBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_map_input ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_functions_benchmarks_url URLBenchmark.cpp)
target_link_libraries(bolt_functions_benchmarks_url ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_functions_benchmarks_compare CompareBenchmark.cpp)
target_link_libraries(bolt_functions_benchmarks_compare ${BENCHMARK_DEPENDENCIES})

add_executable(bolt_benchmark_array_writer_with_nulls ArrayWriterBenchmark.cpp)
target_compile_definitions(bolt_benchmark_array_writer_with_nulls PUBLIC WITH_NULLS=true)
target_link_libraries(bolt_benchmark_array_writer_with_nulls PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_benchmark_array_writer_no_nulls ArrayWriterBenchmark.cpp)
target_link_libraries(bolt_benchmark_array_writer_no_nulls PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)
target_compile_definitions(bolt_benchmark_array_writer_no_nulls PUBLIC WITH_NULLS=false)

add_executable(bolt_benchmark_nested_array_writer_no_nulls NestedArrayWriterBenchmark.cpp)
target_link_libraries(bolt_benchmark_nested_array_writer_no_nulls PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)
target_compile_definitions(bolt_benchmark_nested_array_writer_no_nulls PUBLIC WITH_NULLS=false)

add_executable(bolt_benchmark_nested_array_writer_with_nulls NestedArrayWriterBenchmark.cpp)
target_compile_definitions(bolt_benchmark_nested_array_writer_with_nulls PUBLIC WITH_NULLS=true)
target_link_libraries(
  bolt_benchmark_nested_array_writer_with_nulls PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)


add_executable(bolt_benchmark_map_writer_with_nulls MapWriterBenchmarks.cpp)
target_compile_definitions(bolt_benchmark_map_writer_with_nulls PUBLIC WITH_NULLS=true)
target_link_libraries(bolt_benchmark_map_writer_with_nulls PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_benchmark_map_writer_no_nulls MapWriterBenchmarks.cpp)
target_link_libraries(bolt_benchmark_map_writer_no_nulls PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)
target_compile_definitions(bolt_benchmark_map_writer_no_nulls PUBLIC WITH_NULLS=false)

add_executable(bolt_functions_benchmarks_row_writer_no_nulls RowWriterBenchmark.cpp)
target_link_libraries(
  bolt_functions_benchmarks_row_writer_no_nulls PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_benchmarks_string_writer_no_nulls StringWriterBenchmark.cpp)
target_link_libraries(
  bolt_functions_benchmarks_string_writer_no_nulls PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_prestosql_benchmarks_zip ZipBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_zip PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_prestosql_benchmarks_row Row.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_row PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_prestosql_benchmarks_comparisons ComparisonsBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_comparisons PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_prestosql_benchmarks_concat ConcatBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_concat PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_prestosql_benchmarks_zip_with ZipWithBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_zip_with PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_prestosql_benchmarks_map_zip_with MapZipWithBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_map_zip_with PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_prestosql_benchmarks_cardinality CardinalityBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_cardinality PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_benchmarks_simdjson_function_with_expr JsonExprBenchmark.cpp)
target_link_libraries(
  bolt_functions_benchmarks_simdjson_function_with_expr PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_prestosql_benchmarks_map_subscript MapSubscriptCachingBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_map_subscript PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)

add_executable(bolt_functions_prestosql_benchmarks_generic GenericBenchmark.cpp)
target_link_libraries(bolt_functions_prestosql_benchmarks_generic PRIVATE
  ${BENCHMARK_DEPENDENCIES_NO_FUNC}
  ${BENCHMARK_DEPENDENCIES}
)
