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


add_executable(
  bolt_dwio_arrow_parquet_writer_test
  BloomFilterTest.cpp
  ColumnReaderTest.cpp
  ColumnWriterTest.cpp
  EncodingTest.cpp
  FileDeserializeTest.cpp
  FileSerializeTest.cpp
  LevelConversionTest.cpp
  MetadataTest.cpp
  PageIndexTest.cpp
  PropertiesTest.cpp
  SchemaTest.cpp
  StatisticsTest.cpp
  TypesTest.cpp
  LZ4HadoopCodecTest.cpp)

add_test(bolt_dwio_arrow_parquet_writer_test
         bolt_dwio_arrow_parquet_writer_test)
target_link_libraries(
  bolt_dwio_arrow_parquet_writer_test
  bolt_dwio_arrow_parquet_writer_lib
  bolt_dwio_arrow_parquet_writer_test_lib
  GTest::gmock
  arrow::arrow
  arrow::libarrow_testing
  glog::glog
  gflags::gflags
  GTest::gtest
  GTest::gtest_main)

add_library(
  bolt_dwio_arrow_parquet_writer_test_lib
  BloomFilter.cpp
  BloomFilterReader.cpp
  ColumnReader.cpp
  ColumnScanner.cpp
  FileReader.cpp
  TestUtil.cpp
  XxHasher.cpp)

target_link_libraries(bolt_dwio_arrow_parquet_writer_test_lib
                      bolt_dwio_arrow_parquet_writer_lib arrow::arrow GTest::gtest GTest::gtest_main)
