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

add_executable(bolt_dwio_paimon_test
               PaimonReaderDeduplicateTest.cpp
               PaimonReaderAggregateTest.cpp
               PaimonReaderPartialUpdateTest.cpp
               PaimonReaderMetadataFieldTest.cpp
               PaimonTestUtils.cpp)

add_test(bolt_dwio_paimon_test bolt_dwio_paimon_test)

target_link_libraries(
  bolt_dwio_paimon_test
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
  GTest::gmock_main
)
