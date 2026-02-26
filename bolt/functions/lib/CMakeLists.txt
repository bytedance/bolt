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

bolt_add_library(bolt_is_null_functions IsNull.cpp)

target_link_libraries(bolt_is_null_functions bolt_expression)

bolt_add_library(bolt_functions_util LambdaFunctionUtil.cpp RowsTranslationUtil.cpp)

target_link_libraries(bolt_functions_util bolt_vector bolt_common_base)

find_package(simdjson CONFIG REQUIRED)

set(GK_TYPES
    "int8_t"
    "int16_t"
    "int32_t"
    "int64_t"
    "int128_t"
    "float"
    "double"
    "Timestamp"
)
specialize_template(GK_GENERATED_FILES GreenwaldKhanna.cpp.in ${GK_TYPES})

set(KLL_TYPES "int8_t" "int16_t" "int32_t" "int64_t" "float" "double")
specialize_template(KLL_GENERATED_FILES KllSketch.cpp.in ${KLL_TYPES})

bolt_add_library(
  bolt_functions_lib
  ${GK_GENERATED_FILES}
  ${KLL_GENERATED_FILES}
  ArrayShuffle.cpp
  ArraySort.cpp
  CheckDuplicateKeys.cpp
  CheckNestedNulls.cpp
  DateTimeFormatter.cpp
  DateTimeFormatterBuilder.cpp
  KllSketch.cpp
  MapConcat.cpp
  MapFromEntries.cpp
  MapUpdate.cpp
  Re2Functions.cpp
  Repeat.cpp
  SimpleComparisonMatcher.cpp
  Size.cpp
  StringEncodingUtils.cpp
  StringUtil.cpp
  SubscriptUtil.cpp
  TimeUtils.cpp
)

target_link_libraries(
  bolt_functions_lib
  bolt_functions_util
  bolt_vector
  re2::re2
  Folly::folly
  simdjson::simdjson
  gfx::timsort
  utf8proc::utf8proc
  date::date
)

add_subdirectory(aggregates)
add_subdirectory(string)
add_subdirectory(window)
if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
