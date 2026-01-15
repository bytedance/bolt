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

add_executable(bolt_exec_vector_hasher_benchmark VectorHasherBenchmark.cpp)

target_link_libraries(
  bolt_exec_vector_hasher_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)

add_executable(bolt_filter_project_benchmark FilterProjectBenchmark.cpp)

target_link_libraries(
  bolt_filter_project_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)

add_executable(bolt_exchange_benchmark ExchangeBenchmark.cpp)

target_link_libraries(
  bolt_exchange_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)

add_executable(bolt_merge_benchmark MergeBenchmark.cpp)

target_link_libraries(
  bolt_merge_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)

add_executable(bolt_hash_benchmark HashTableBenchmark.cpp)

target_link_libraries(
  bolt_hash_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)

add_executable(bolt_sort_random_data_benchmark SortRandomDataBenchmark.cpp)

add_executable(bolt_sort_window_benchmark SortWindowBenchmark.cpp)
target_link_libraries(
  bolt_sort_random_data_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)

add_executable(bolt_hash_join_prepare_join_table_benchmark HashJoinPrepareJoinTableBenchmark.cpp)

target_link_libraries(
  bolt_hash_join_prepare_join_table_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)

target_link_libraries(
  bolt_sort_window_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)

if(${BOLT_ENABLE_PARQUET})
  add_executable(bolt_sort_benchmark RowContainerSortBenchmark.cpp)
  find_package(Thrift CONFIG REQUIRED)
  find_package(Arrow CONFIG REQUIRED)

  target_link_libraries(
    bolt_sort_benchmark PRIVATE
      bolt_testutils
      ${FOLLY_BENCHMARK}
      GTest::gtest_main
  )
endif()

add_executable(bolt_gather_copy_benchmark GatherCopyBenchmark.cpp)
target_link_libraries(
  bolt_gather_copy_benchmark bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)

add_executable(rowEqVectors_benchmark RowEqVectorsBenchmark.cpp)
target_link_libraries(
  rowEqVectors_benchmark PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)

add_executable(hash_vectors_bench HashVectorsBenchmark.cpp)
target_link_libraries(
  hash_vectors_bench PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)
