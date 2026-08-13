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
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_library(bolt_debug_util OBJECT ScopedTestTime.cpp TestValue.cpp)
  target_link_libraries(bolt_debug_util PUBLIC bolt_exception)

  if(${BOLT_BUILD_TESTING})
    add_subdirectory(tests)
  endif()
endif()

if(${BOLT_ENABLE_PERF})
  bolt_add_library(bolt_test_util_gperf OBJECT GPerf.cpp)

  target_compile_definitions(bolt_test_util_gperf PRIVATE -DBOLT_ENABLE_PERF)
  find_package(gperftools REQUIRED CONFIG)
  target_link_libraries(bolt_test_util_gperf PRIVATE gperftools::profiler)
endif()
