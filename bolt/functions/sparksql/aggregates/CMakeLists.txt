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

set(PAAGG_TYPES
    "int8_t"
    "int16_t"
    "int32_t"
    "int64_t"
    "int128_t"
    "float"
    "double"
    "Timestamp"
)
specialize_template(PAAGG_GENERATED_FILES PercentileApproxAggregate.cpp.in ${PAAGG_TYPES})

set(BOLT_SPARK_SUM_INT64_SUBOP_SVE "")
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
   CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*|arm64.*)")
  list(APPEND BOLT_SPARK_SUM_INT64_SUBOP_SVE SumAggregateSparkInt64SubOpSve.cpp)
endif()

set(PERCENTILE_TYPES
    "TypeKind::TINYINT"
    "TypeKind::SMALLINT"
    "TypeKind::INTEGER"
    "TypeKind::BIGINT"
    "TypeKind::HUGEINT"
    "TypeKind::REAL"
    "TypeKind::DOUBLE"
    "TypeKind::VARCHAR"
)
specialize_template(
  PERCENTILE_GENERATED_FILES PercentileAggregate.cpp.in ${PERCENTILE_TYPES}
)

set(MIN_MAX_BY_TYPES
    "bool"
    "int8_t"
    "int16_t"
    "int32_t"
    "int64_t"
    "int128_t"
    "float"
    "double"
    "StringView"
    "Timestamp"
    "ComplexType"
)
specialize_template(
  MIN_MAX_BY_GENERATED_FILES MinMaxByAggregate.cpp.in ${MIN_MAX_BY_TYPES}
)

bolt_add_library(
  bolt_functions_spark_aggregates
  ${PAAGG_GENERATED_FILES}
  ${PERCENTILE_GENERATED_FILES}
  ${MIN_MAX_BY_GENERATED_FILES}
  AverageAggregate.cpp
  BitmapConstructAggAggregate.cpp
  BitmapOrAggAggregate.cpp
  BitwiseXorAggregate.cpp
  BloomFilterAggAggregate.cpp
  CentralMomentsAggregate.cpp
  CollectListAggregate.cpp
  CovarianceAggregate.cpp
  FirstLastAggregate.cpp
  MinMaxByAggregate.cpp
  ModeAggregate.cpp
  PercentileApproxAggregate.cpp
  PercentileAggregate.cpp
  Register.cpp
  RegrReplacementAggregate.cpp
  SumAggregateSparkInt64SubOp.cpp
  ${BOLT_SPARK_SUM_INT64_SUBOP_SVE}
  SumAggregate.cpp
)

target_link_libraries(
  bolt_functions_spark_aggregates bolt_exec bolt_expression_functions bolt_aggregates bolt_vector
  fmt::fmt
)

# SumAggregateSparkInt64SubOpSve.cpp uses <arm_sve.h>; default Bolt -march=armv8.3-a
# does not enable the SVE ISA; compile this Linux AArch64 TU with +sve only.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
   CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*|arm64.*)")
  set_source_files_properties(
    SumAggregateSparkInt64SubOpSve.cpp
    PROPERTIES COMPILE_OPTIONS "-march=armv8-a+sve")
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
