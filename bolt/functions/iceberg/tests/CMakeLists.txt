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
add_executable(
  bolt_functions_iceberg_test
  BucketFunctionTest.cpp
  DateTimeFunctionsTest.cpp
  Murmur3Hash32Test.cpp
  TruncateTest.cpp
)

add_test(
  NAME bolt_functions_iceberg_test
  COMMAND bolt_functions_iceberg_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  bolt_functions_iceberg_test
  PRIVATE
  bolt_functions_iceberg
  bolt_functions_test_lib
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
)
