# Copyright (c) ByteDance Ltd. and/or its affiliates.
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
  bolt_tos_file_system_test
  TosFileSystemRegistrationTest.cpp
  TosConfigTest.cpp
  TosCredentialsFileTest.cpp
  TosReadWriteTest.cpp
)
target_link_libraries(
  bolt_tos_file_system_test
  PRIVATE
    bolt_gtest_main
    tos_client::tos_client
)
add_test(bolt_tos_file_system_test bolt_tos_file_system_test)
add_executable(bolt_tos_extension_registry_test TosFileSystemExtensionRegistryTest.cpp)
target_link_libraries(
  bolt_tos_extension_registry_test
  PRIVATE
    bolt_gtest_main
    tos_client::tos_client
)
add_test(bolt_tos_extension_registry_test bolt_tos_extension_registry_test)
set_tests_properties(
  bolt_tos_file_system_test
  PROPERTIES
    ENVIRONMENT
      "NO_PROXY=127.0.0.1,localhost;no_proxy=127.0.0.1,localhost"
    TIMEOUT
      120
)
