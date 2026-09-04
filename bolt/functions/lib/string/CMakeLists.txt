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

bolt_add_library(bolt_functions_string INTERFACE)

if(BOLT_ENABLE_SPARK_COMPATIBLE)
  bolt_add_library(bolt_functions_spark_lower SparkLower.cpp)
  target_link_libraries(
    bolt_functions_spark_lower
    PUBLIC
      Folly::folly
      ${ICU_LIBRARIES}
  )
  target_link_libraries(bolt_functions_string INTERFACE bolt_functions_spark_lower)
endif()

target_link_libraries(
  bolt_functions_string
  INTERFACE
    bolt_exception
    xxHash::xxhash
    utf8proc::utf8proc
    Folly::folly
    ${ICU_LIBRARIES}
    re2::re2
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
