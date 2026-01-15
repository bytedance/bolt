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
  bolt_functions_json JsonExtractor.cpp JsonPathTokenizer.cpp SIMDJsonExtractor.cpp
                      SIMDJsonUtil.cpp
)

find_package(simdjson CONFIG REQUIRED)
find_package(sonic-cpp CONFIG REQUIRED)
find_package(xxHash CONFIG REQUIRED)
find_package(ryu CONFIG REQUIRED)

target_link_libraries(
  bolt_functions_json
  bolt_exception
  simdjson::simdjson
  xxHash::xxhash
  ryu::ryu
  sonic-cpp::sonic-cpp
  Folly::folly
)
target_compile_features(bolt_functions_json PRIVATE cxx_std_20)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
