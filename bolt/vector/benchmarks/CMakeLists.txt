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

add_executable(bolt_vector_hash_all_benchmark SimpleVectorHashAllBenchmark.cpp)

target_link_libraries(
  bolt_vector_hash_all_benchmark bolt_vector Folly::folly ${FOLLY_BENCHMARK} fmt::fmt
)
# This is a workaround for the use of VectorTestUtils.h which include gtest.h
target_link_libraries(bolt_vector_hash_all_benchmark GTest::gtest)

add_executable(bolt_vector_selectivity_vector_benchmark SelectivityVectorBenchmark.cpp)

target_link_libraries(bolt_vector_selectivity_vector_benchmark bolt_vector
                      Folly::folly ${FOLLY_BENCHMARK} bolt_process)

add_executable(copy_benchmark CopyBenchmark.cpp)

target_link_libraries(copy_benchmark bolt_vector_test_lib Folly::folly ${FOLLY_BENCHMARK})

add_executable(copy_vectors_opt_bench CopyVecOptBenchmark.cpp)
target_link_libraries(
  copy_vectors_opt_bench
  bolt_exec
  bolt_hive_connector
  bolt_functions_lib
  bolt_exec_test_lib
  bolt_functions_prestosql
  bolt_dwio_common_exception
  bolt_vector_fuzzer
  bolt_vector_test_lib
  Folly::folly
  ${FOLLY_BENCHMARK}
  gflags::gflags
)
