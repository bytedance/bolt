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

# Add subdirectories
add_subdirectory(utils)
add_subdirectory(dsdgen)

bolt_add_library(bolt_tpcds_gen TpcdsGen.cpp DSDGenIterator.cpp)
target_include_directories(
  bolt_tpcds_gen
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/dsdgen/include>"
)
target_link_libraries(
  bolt_tpcds_gen
  PUBLIC
    bolt_memory
    bolt_type
    bolt_core
    bolt_tpcds_append_info
  PRIVATE
    bolt_vector
    bolt_tpcds_dsdgen
    fmt::fmt
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
