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
# Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

add_executable(bolt_s3config_test S3ConfigTest.cpp)
add_test(bolt_s3config_test bolt_s3config_test)
target_link_libraries(
  bolt_s3config_test
  bolt_common_config
  bolt_s3fs
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_s3file_test S3FileSystemTest.cpp S3UtilTest.cpp)
add_test(bolt_s3file_test bolt_s3file_test)
target_link_libraries(
  bolt_s3file_test
  bolt_file
  bolt_s3fs
  bolt_core
  bolt_exec_test_lib
  bolt_dwio_common_exception
  bolt_exec
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_s3registration_test S3FileSystemRegistrationTest.cpp)
add_test(bolt_s3registration_test bolt_s3registration_test)
target_link_libraries(
  bolt_s3registration_test
  bolt_file
  bolt_s3fs
  bolt_core
  bolt_exec_test_lib
  bolt_dwio_parquet_reader
  bolt_dwio_common_exception
  bolt_exec
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_s3finalize_test S3FileSystemFinalizeTest.cpp)
add_test(bolt_s3finalize_test bolt_s3finalize_test)
target_link_libraries(
  bolt_s3finalize_test
  bolt_s3fs
  bolt_file
  bolt_core
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_s3insert_test S3InsertTest.cpp)
add_test(bolt_s3insert_test bolt_s3insert_test)
target_link_libraries(
  bolt_s3insert_test
  bolt_file
  bolt_s3fs
  bolt_core
  bolt_exec_test_lib
  bolt_dwio_parquet_writer
  bolt_dwio_parquet_reader
  bolt_dwio_common_exception
  bolt_exec
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_s3read_test S3ReadTest.cpp)
add_test(
  NAME bolt_s3read_test
  COMMAND bolt_s3read_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  bolt_s3read_test
  bolt_file
  bolt_s3fs
  bolt_core
  bolt_exec_test_lib
  bolt_dwio_parquet_reader
  bolt_dwio_common_exception
  bolt_exec
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_s3metrics_test S3FileSystemMetricsTest.cpp)
add_test(bolt_s3metrics_test bolt_s3metrics_test)
target_link_libraries(
  bolt_s3metrics_test
  bolt_s3fs
  bolt_exec_test_lib
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_s3multiendpoints_test S3MultipleEndpointsTest.cpp)
add_test(bolt_s3multiendpoints_test bolt_s3multiendpoints_test)
target_link_libraries(
  bolt_s3multiendpoints_test
  bolt_file
  bolt_s3fs
  bolt_core
  bolt_exec_test_lib
  bolt_dwio_parquet_reader
  bolt_dwio_parquet_writer
  bolt_dwio_common_exception
  bolt_exec
  GTest::gtest
  GTest::gtest_main
)
