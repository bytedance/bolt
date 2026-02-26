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

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

add_subdirectory(parser)
add_subdirectory(tz)
add_subdirectory(fbhive)
add_subdirectory(filter)

find_package(xxHash CONFIG REQUIRED)
find_package(ryu CONFIG REQUIRED)

# Header-only interface library for type definitions
bolt_add_library(bolt_type_headers INTERFACE)
target_include_directories(
  bolt_type_headers INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

# Implementation library for core type functionality
bolt_add_library(
  bolt_type_base
  BigDecimal.cpp
  Conversions.cpp
  DecimalUtil.cpp
  FloatingDecimal.cpp
  FloatingPointUtil.cpp
  HugeInt.cpp
  StringView.cpp
  StringView.h
  Subfield.cpp
  Timestamp.cpp
  TimestampConversion.cpp
  Tokenizer.cpp
  Type.cpp
  Variant.cpp
)

target_include_directories(
  bolt_type_base PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> ${xxHash_INCLUDE_DIR}
                        ${ryu_INCLUDE_DIR} ${re2_INCLUDE_DIR} ${date_INCLUDE_DIR}
)

target_link_libraries(
  bolt_type_base
  PUBLIC bolt_type_headers date::date
  PRIVATE bolt_type_tz
          bolt_common_base
          bolt_encode
          bolt_exception
          bolt_serialization
          bolt_status
          bolt_token_extractor
          Boost::headers
          re2::re2
          xxHash::xxhash
          ryu::ryu
          Folly::folly
)

# Main interface library that combines everything
bolt_add_library(bolt_type INTERFACE)

target_link_libraries(
  bolt_type
  INTERFACE bolt_type_headers
            bolt_type_base
            bolt_filter
            bolt_vector
            bolt_exec
            bolt_type_tz
            bolt_common_base
            bolt_encode
            bolt_exception
            bolt_serialization
            bolt_status
            bolt_token_extractor
            Boost::headers
            re2::re2
            xxHash::xxhash
            ryu::ryu
            Folly::folly
)
