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
  torch_test
  test_compatibility.cpp
  test_operator.cpp
  test_plan_node.cpp
  utils.cpp
)
target_link_libraries(
  torch_test
  PRIVATE
    bolt_torch
    bolt_exec
    bolt_core
    glog::glog
    bolt_gtest_main
)
target_include_directories(
  torch_test
  SYSTEM
  PRIVATE
    ${TORCH_INCLUDE_DIRS}
    ${GTest_INCLUDE_DIRS}
)
target_compile_definitions(torch_test PRIVATE BOLT_HAS_TORCH=1)

add_test(torch_test torch_test)
