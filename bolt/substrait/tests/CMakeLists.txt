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

add_executable(
  bolt_plan_conversion_test
  BoltSubstraitRoundTripTest.cpp
  BoltSubstraitSignatureTest.cpp
  BoltToSubstraitTypeTest.cpp
  FunctionTest.cpp
  JsonToProtoConverter.cpp
  Substrait2BoltPlanConversionTest.cpp
  Substrait2BoltValuesNodeConversionTest.cpp
  SubstraitExtensionCollectorTest.cpp
)

add_dependencies(bolt_plan_conversion_test bolt_substrait_plan_converter)

add_test(
  NAME bolt_plan_conversion_test
  COMMAND bolt_plan_conversion_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  bolt_plan_conversion_test
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
  ${FILESYSTEM}
)

if(${BOLT_ENABLE_TORCH})
  target_compile_definitions(bolt_plan_conversion_test PRIVATE BOLT_HAS_TORCH=1)
  target_include_directories(bolt_plan_conversion_test SYSTEM PRIVATE ${TORCH_INCLUDE_DIRS})
  target_link_libraries(bolt_plan_conversion_test bolt_torch bolt_vector_fuzzer)
endif()
