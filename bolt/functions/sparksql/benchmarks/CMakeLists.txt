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

add_executable(bolt_sparksql_benchmarks_in In.cpp)

target_link_libraries(
  bolt_sparksql_benchmarks_in
  bolt_functions_prestosql
  bolt_functions_spark
  bolt_expression
  bolt_exec_test_lib
  bolt_vector_test_lib
  bolt_vector_fuzzer
  Folly::folly
  ${FOLLY_BENCHMARK}
)

add_executable(bolt_sparksql_benchmarks_compare CompareBenchmark.cpp)
target_link_libraries(
  bolt_sparksql_benchmarks_compare bolt_functions_spark bolt_benchmark_builder bolt_vector_test_lib
)

add_executable(bolt_sparksql_benchmarks_regex RegexBenchmark.cpp)
target_link_libraries(
  bolt_sparksql_benchmarks_regex Folly::folly ${FOLLY_BENCHMARK} bolt_process re2::re2
  ${ICU_LIBRARIES}
)
add_executable(bolt_sparksql_benchmarks_split Split.cpp)
target_link_libraries(
  bolt_sparksql_benchmarks_split bolt_functions_spark bolt_benchmark_builder bolt_vector_test_lib
)

add_executable(bolt_sparksql_benchmarks_hash HiveHashBenchmark.cpp)
target_link_libraries(
  bolt_sparksql_benchmarks_hash bolt_functions_spark bolt_benchmark_builder bolt_vector_test_lib
)

add_executable(bolt_sparksql_benchmarks_map MapBenchmark.cpp)
target_link_libraries(
  bolt_sparksql_benchmarks_map bolt_functions_spark bolt_benchmark_builder bolt_vector_test_lib
)
