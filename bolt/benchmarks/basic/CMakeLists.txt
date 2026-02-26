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

set(BOLT_BENCHMARK_DEPS
    bolt_vector_fuzzer
    bolt_benchmark_builder
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest
)

add_executable(bolt_benchmark_basic_simple_arithmetic SimpleArithmetic.cpp)
target_link_libraries(bolt_benchmark_basic_simple_arithmetic ${BOLT_BENCHMARK_DEPS})

add_executable(bolt_benchmark_basic_comparison_conjunct ComparisonConjunct.cpp)
target_link_libraries(bolt_benchmark_basic_comparison_conjunct ${BOLT_BENCHMARK_DEPS})

add_executable(bolt_benchmark_basic_simple_cast SimpleCastExpr.cpp)
target_link_libraries(bolt_benchmark_basic_simple_cast ${BOLT_BENCHMARK_DEPS})

add_executable(bolt_benchmark_basic_decoded_vector DecodedVector.cpp)
target_link_libraries(bolt_benchmark_basic_decoded_vector ${BOLT_BENCHMARK_DEPS})

add_executable(bolt_benchmark_basic_selectivity_vector SelectivityVector.cpp)
target_link_libraries(bolt_benchmark_basic_selectivity_vector ${BOLT_BENCHMARK_DEPS})

add_executable(bolt_benchmark_basic_vector_compare VectorCompare.cpp)
target_link_libraries(
  bolt_benchmark_basic_vector_compare ${BOLT_BENCHMARK_DEPS} bolt_vector_test_lib
)

add_executable(bolt_benchmark_basic_vector_slice VectorSlice.cpp)
target_link_libraries(bolt_benchmark_basic_vector_slice ${BOLT_BENCHMARK_DEPS} bolt_vector_test_lib)

add_executable(bolt_benchmark_feature_normalization FeatureNormalization.cpp)
target_link_libraries(
  bolt_benchmark_feature_normalization ${BOLT_BENCHMARK_DEPS} bolt_functions_prestosql
)

add_executable(bolt_benchmark_basic_preproc Preproc.cpp)
target_link_libraries(
  bolt_benchmark_basic_preproc ${BOLT_BENCHMARK_DEPS} bolt_functions_prestosql bolt_vector_test_lib
)

add_executable(bolt_like_tpch_benchmark LikeTpchBenchmark.cpp)
target_link_libraries(
  bolt_like_tpch_benchmark ${BOLT_BENCHMARK_DEPS} bolt_functions_lib bolt_tpch_gen
  bolt_vector_test_lib
)

add_executable(bolt_like_benchmark LikeBenchmark.cpp)
target_link_libraries(
  bolt_like_benchmark ${BOLT_BENCHMARK_DEPS} bolt_functions_lib bolt_functions_prestosql
  bolt_vector_test_lib
)

add_executable(bolt_benchmark_basic_vector_fuzzer VectorFuzzer.cpp)
target_link_libraries(
  bolt_benchmark_basic_vector_fuzzer ${BOLT_BENCHMARK_DEPS} bolt_vector_test_lib
)

add_executable(bolt_cast_benchmark CastBenchmark.cpp)
target_link_libraries(bolt_cast_benchmark ${BOLT_BENCHMARK_DEPS} bolt_vector_test_lib)

add_executable(bolt_format_datetime_benchmark FormatDateTimeBenchmark.cpp)
target_link_libraries(
  bolt_format_datetime_benchmark ${BOLT_BENCHMARK_DEPS} bolt_vector_test_lib bolt_functions_spark
  bolt_functions_prestosql
)
