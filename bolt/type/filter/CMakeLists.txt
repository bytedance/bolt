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

bolt_add_library(
  bolt_filter
  Cast.cpp
  Filter.cpp
  FilterUtil.cpp
  FloatingPointMultiRange.cpp
  FloatingPointRange.cpp
  FloatingPointValues.cpp
  MapSubscriptFilter.cpp
  NegatedFloatingPointRange.cpp
  NegatedFloatingPointValue.cpp
)

target_include_directories(bolt_filter PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/filter>)

find_package(xxHash CONFIG REQUIRED)
find_package(ryu CONFIG REQUIRED)

target_link_libraries(
  bolt_filter
  PUBLIC bolt_type_headers
  PRIVATE bolt_common_base bolt_token_extractor bolt_exception re2::re2 xxHash::xxhash ryu::ryu
)
