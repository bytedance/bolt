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

bolt_add_library(
  bolt_functions_spark_aggregates
  ${PAAGG_GENERATED_FILES}
  AverageAggregate.cpp
  BitwiseXorAggregate.cpp
  BloomFilterAggAggregate.cpp
  CentralMomentsAggregate.cpp
  CollectListAggregate.cpp
  FirstLastAggregate.cpp
  MinMaxByAggregate.cpp
  PercentileApproxAggregate.cpp
  Register.cpp
  RegrReplacementAggregate.cpp
  SumAggregate.cpp
  SumAggregate.cpp
)

target_link_libraries(
  bolt_functions_spark_aggregates bolt_exec bolt_expression_functions bolt_aggregates bolt_vector
  fmt::fmt
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
