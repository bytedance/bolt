#
# Copyright (c) ByteDance Ltd. and/or its affiliates
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

add_executable(
  bolt_dwio_parquet_encryption_test
  AesDecryptorTest.cpp
  InternalFileDecryptorTest.cpp)

add_test(
  NAME bolt_dwio_parquet_encryption_test
  COMMAND bolt_dwio_parquet_encryption_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  bolt_dwio_parquet_encryption_test
  #bolt_dwio_parquet_encryption
  #bolt_dwio_parquet_arrow_lib
  bolt_testutils
  GTest::gtest
  GTest::gtest_main)
