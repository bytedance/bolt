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

bolt_add_library(
  bolt_process ExceptionTrace.cpp ProcessBase.cpp StackTrace.cpp ThreadDebugInfo.cpp
               TraceContext.cpp
)

target_link_libraries(
  bolt_process
  PUBLIC bolt_flag_definitions Folly::folly
  PRIVATE fmt::fmt gflags::gflags glog::glog
)

find_package(libbacktrace REQUIRED CONFIG)

bolt_add_library(bolt_process_crash_report CrashReport.cpp)

target_link_libraries(bolt_process_crash_report libbacktrace::libbacktrace)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  find_package(libunwind REQUIRED CONFIG)
  target_link_libraries(bolt_process_crash_report libunwind::libunwind)
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
