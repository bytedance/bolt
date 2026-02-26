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

add_subdirectory(json)
add_subdirectory(types)
add_subdirectory(window)

bolt_add_library(
  bolt_functions_prestosql_impl
  ArrayAndMapMatch.cpp
  ArrayConstructor.cpp
  ArrayContains.cpp
  ArrayDistinct.cpp
  ArrayDuplicates.cpp
  ArrayIntersectExcept.cpp
  ArrayPosition.cpp
  ArraySort.cpp
  ArraySum.cpp
  Comparisons.cpp
  DecimalVectorFunctions.cpp
  Element.cpp
  ElementAt.cpp
  FilterFunctions.cpp
  FindFirst.cpp
  FromUnixTime.cpp
  FromUtf8.cpp
  GreatestLeast.cpp
  InPredicate.cpp
  JsonFunctions.cpp
  Map.cpp
  MapEntries.cpp
  MapKeysAndValues.cpp
  MapZipWith.cpp
  Not.cpp
  Reduce.cpp
  Reverse.cpp
  RowFunction.cpp
  Sequence.cpp
  Slice.cpp
  Split.cpp
  StringFunctions.cpp
  StringToMap.cpp
  Subscript.cpp
  ToJson.cpp
  ToUtf8.cpp
  Transform.cpp
  TransformKeys.cpp
  TransformValues.cpp
  UdfMapArrayCombine.cpp
  UdfMapFilterKeys.cpp
  UdfStringToLongArray.cpp
  URLFunctions.cpp
  Utf8Utils.cpp
  VectorArithmetic.cpp
  WidthBucketArray.cpp
  Zip.cpp
  ZipWith.cpp
)

target_link_libraries(
  bolt_functions_prestosql_impl
  bolt_common_hyperloglog
  bolt_functions_json
  bolt_functions_lib
  bolt_expression
  bolt_type_tz
  bolt_presto_types
  bolt_functions_util
  ${ICU_LIBRARIES}
  utf8proc::utf8proc
  Boost::regex
  Folly::folly
)

set_property(TARGET bolt_functions_prestosql_impl PROPERTY JOB_POOL_COMPILE high_memory_pool)

if(${BOLT_ENABLE_AGGREGATES})
  add_subdirectory(aggregates)
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
  add_subdirectory(coverage)
  add_subdirectory(fuzzer)
elseif(${BOLT_BUILD_TEST_UTILS})
  add_subdirectory(tests/utils)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()

add_subdirectory(registration)
