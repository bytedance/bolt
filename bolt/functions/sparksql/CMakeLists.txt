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

add_subdirectory(specialforms)
bolt_add_library(
  bolt_functions_spark_impl
  ArrayGetFunction.cpp
  ArraySliceSum.cpp
  ArraySort.cpp
  Comparisons.cpp
  DecimalArithmetic.cpp
  DecimalCompare.cpp
  DecimalVectorFunctions.cpp
  Hash.cpp
  ICURegexFunctions.cpp
  In.cpp
  InputFileName.cpp
  JsonToMap.cpp
  JsonTuple.cpp
  LeastGreatest.cpp
  MakeTimestamp.cpp
  Map.cpp
  RegexFunctions.cpp
  # Register.cpp
  # RegisterArithmetic.cpp
  # RegisterCompare.cpp
  SplitFunctions.cpp
  String.cpp
  UnscaledValueFunction.cpp
)

target_link_libraries(
  bolt_functions_spark_impl
  bolt_functions_lib
  bolt_functions_prestosql_impl
  bolt_functions_spark_specialforms
  bolt_functions_util
  bolt_process
  Folly::folly
  ${ICU_LIBRARIES}
  simdjson::simdjson
)

set_property(TARGET bolt_functions_spark_impl PROPERTY JOB_POOL_COMPILE high_memory_pool)

# directory for generated version.h
include_directories("${CMAKE_BINARY_DIR}/gen_cpp")

add_subdirectory(window)

if(${BOLT_ENABLE_AGGREGATES})
  add_subdirectory(aggregates)
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
  add_subdirectory(coverage)
  add_subdirectory(fuzzer)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()

add_subdirectory(registration)
