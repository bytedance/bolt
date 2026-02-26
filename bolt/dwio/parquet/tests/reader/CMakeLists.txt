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

# find_package(lzo CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)

add_executable(bolt_dwio_parquet_reader_test
  ParquetReaderTest.cpp
  ParquetPageReaderTest.cpp
  NestedStructureDecoderTest.cpp
)

add_test(
  NAME bolt_dwio_parquet_reader_test
  COMMAND bolt_dwio_parquet_reader_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  bolt_dwio_parquet_reader_test PRIVATE
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)

add_executable(bolt_parquet_filter_test
  E2EFilterTest.cpp
  ParquetDictionaryFilterTest.cpp
  ParquetRowGroupFilterTest.cpp
)

add_test(bolt_parquet_filter_test bolt_parquet_filter_test)
target_link_libraries(
  bolt_parquet_filter_test
  bolt_e2e_filter_test_base
  bolt_testutils
)

add_executable(bolt_dwio_parquet_reader_benchmark ParquetReaderBenchmark.cpp)
target_link_libraries(
  bolt_dwio_parquet_reader_benchmark
  PRIVATE bolt_testutils
  ${FOLLY_BENCHMARK}
)

add_executable(bolt_dwio_parquet_structure_decoder_benchmark NestedStructureDecoderBenchmark.cpp)
target_link_libraries(
  bolt_dwio_parquet_structure_decoder_benchmark PRIVATE
  bolt_testutils
  ${FOLLY_BENCHMARK}
)

add_executable(bolt_dwio_parquet_table_scan_test ParquetTableScanTest.cpp)
add_test(
  NAME bolt_dwio_parquet_table_scan_test
  COMMAND bolt_dwio_parquet_table_scan_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  bolt_dwio_parquet_table_scan_test PRIVATE
  bolt_testutils
  ${TEST_LINK_LIBS}
)

add_executable(bolt_dwio_parquet_cli_test ParquetReaderCliTest.cpp)

add_test(
  NAME bolt_dwio_parquet_cli_test
  COMMAND bolt_dwio_parquet_cli_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  bolt_dwio_parquet_cli_test PRIVATE
  bolt_dwio_native_parquet_reader_cli
  bolt_testutils
)

if(${BOLT_ENABLE_ARROW})

  add_executable(bolt_dwio_parquet_rlebp_decoder_test RleBpDecoderTest.cpp)
  add_test(
    NAME bolt_dwio_parquet_rlebp_decoder_test
    COMMAND bolt_dwio_parquet_rlebp_decoder_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )
  target_link_libraries(
    bolt_dwio_parquet_rlebp_decoder_test PRIVATE
    bolt_testutils
    ${TEST_LINK_LIBS}
  )

endif()
