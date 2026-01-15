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

add_executable(bolt_example_simple_functions SimpleFunctions.cpp)

find_package(simdjson CONFIG REQUIRED)
find_package(sonic-cpp CONFIG REQUIRED)
target_link_libraries(
  bolt_example_simple_functions
  bolt_testutils
)

add_executable(bolt_example_expression_eval ExpressionEval.cpp)

target_link_libraries(
  bolt_example_expression_eval bolt_testutils
)

add_executable(bolt_example_opaque_type OpaqueType.cpp)

target_link_libraries(
  bolt_example_opaque_type bolt_testutils
)

# This is disabled temporarily until we figure out why g++ is crashing linking it on linux builds.

# add_executable(bolt_example_scan_and_sort ScanAndSort.cpp)

# target_link_libraries( bolt_example_scan_and_sort bolt_type bolt_vector bolt_exec
# bolt_exec_test_lib bolt_hive_connector bolt_memory bolt_vector_test_lib)

add_executable(bolt_example_scan_orc ScanOrc.cpp)

target_link_libraries(
  bolt_example_scan_orc
  bolt_testutils
)

add_executable(bolt_example_vector_reader_writer VectorReaderWriter.cpp)

target_link_libraries(bolt_example_vector_reader_writer bolt_testutils)

add_executable(bolt_example_operator_extensibility OperatorExtensibility.cpp)

target_link_libraries(
  bolt_example_operator_extensibility
  bolt_testutils
)
