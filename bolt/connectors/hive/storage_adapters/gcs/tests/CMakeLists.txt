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

add_executable(bolt_gcs_file_test GcsFileSystemTest.cpp GcsUtilTest.cpp)
add_test(bolt_gcs_file_test bolt_gcsfile_test)
target_link_libraries(
  bolt_gcs_file_test
  bolt_testutils
  bolt_engine
  bolt_core
  bolt_exec_test_lib
  bolt_dwio_common_exception
  bolt_exec
  bolt_gcs
  bolt_exception
  bolt_temp_path
  google-cloud-cpp::storage
  GTest::gmock
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_gcs_insert_test GcsInsertTest.cpp)
add_test(bolt_gcs_insert_test bolt_gcs_insert_test)
target_link_libraries(
  bolt_gcs_insert_test
  bolt_testutils
  bolt_engine
  bolt_file
  bolt_gcs
  bolt_exception
  bolt_hive_config
  bolt_core
  bolt_dwio_parquet_reader
  bolt_dwio_parquet_writer
  bolt_exec_test_lib
  bolt_dwio_common_exception
  bolt_exec
  GTest::gmock
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_gcs_multiendpoints_test GcsMultipleEndpointsTest.cpp)
add_test(bolt_gcs_multiendpoints_test bolt_gcs_multiendpoints_test)
target_link_libraries(
  bolt_gcs_multiendpoints_test
  bolt_testutils
  bolt_engine
  bolt_file
  bolt_gcs
  bolt_exception
  bolt_hive_config
  bolt_core
  bolt_exec_test_lib
  bolt_dwio_parquet_reader
  bolt_dwio_parquet_writer
  bolt_dwio_common_exception
  bolt_exec
  GTest::gmock
  GTest::gtest
  GTest::gtest_main
)
