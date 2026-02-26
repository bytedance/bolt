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

bolt_add_library(bolt_function_registry FunctionRegistry.cpp)
bolt_add_library(bolt_coverage_util CoverageUtil.cpp)

target_link_libraries(bolt_function_registry bolt_expression bolt_type bolt_core bolt_exception)
target_link_libraries(bolt_coverage_util bolt_function_registry)
add_subdirectory(lib)
if(${BOLT_ENABLE_PRESTO_FUNCTIONS})
  add_subdirectory(prestosql)
endif()

if(${BOLT_ENABLE_SPARK_FUNCTIONS})
  add_subdirectory(sparksql)
endif()

if(${BOLT_ENABLE_FLINK_FUNCTIONS})
  add_subdirectory(flinksql)
endif()

if(${BOLT_ENABLE_REMOTE_FUNCTIONS})
  add_subdirectory(remote)
endif()

if(${BOLT_ENABLE_SKETCH_FUNCTIONS})
  add_subdirectory(sketches)
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
