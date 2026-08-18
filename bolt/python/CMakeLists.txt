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
  bolt_python Aggregate.cpp Function.cpp Operator.cpp PlanNode.cpp PythonScalarFunction.cpp
              Utils.cpp
)

target_include_directories(
  bolt_python
  PRIVATE
    ${pybind11_INCLUDE_DIRS}
    ${fmt_INCLUDE_DIRS}
    ${glog_INCLUDE_DIRS} # transitive include
    ${gflags_INCLUDE_DIRS} # transitive include
    ${folly_INCLUDE_DIRS} # transitive include
    ${Boost_INCLUDE_DIRS} # transitive include
    ${double-conversion_INCLUDE_DIRS} # transitive include
    ${xsimd_INCLUDE_DIRS} # transitive include
    ${Libevent_INCLUDE_DIRS} # transitive include
    ${ryu_INCLUDE_DIRS} # transitive include
    ${gfx-timsort_INCLUDE_DIRS} # transitive include
    ${xxHash_INCLUDE_DIRS} # transitive include
    ${date_INCLUDE_DIRS} # transitive include
)

target_compile_options(bolt_python PRIVATE -fvisibility=hidden)

if(${ENABLE_BOLT_JIT})
  target_include_directories(bolt_python PRIVATE ${LLVM_INCLUDE_DIRS})
endif()

# icu is needed as a result of a transitive include calling a function from icu library.
target_link_libraries(
  bolt_python
  PRIVATE
    pybind11::pybind11
    icu::icu
    folly::folly
)
target_compile_definitions(bolt_python PRIVATE ${glog_DEFINITIONS})
