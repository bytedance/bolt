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

set(TEST_LINK_LIBS
    bolt_testutils
    bolt_temp_path
    GTest::gtest
    GTest::gtest_main
    GTest::gmock
    gflags::gflags
    ${GLOG}
    ${FILESYSTEM}
)

add_subdirectory(reader)
add_subdirectory(writer)
add_subdirectory(thrift)
add_subdirectory(encryption)

add_executable(bolt_dwio_parquet_tpch_test ParquetTpchTest.cpp)
add_test(
  NAME bolt_dwio_parquet_tpch_test
  COMMAND bolt_dwio_parquet_tpch_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  bolt_dwio_parquet_tpch_test
  bolt_dwio_parquet_reader
  bolt_exec_test_lib
  bolt_exec
  bolt_hive_connector
  bolt_tpch_connector
  bolt_aggregates
  bolt_tpch_gen
  ${TEST_LINK_LIBS}
)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/examples DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
