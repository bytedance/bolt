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

set(APAGG_TYPES "int8_t" "int16_t" "int32_t" "int64_t" "float" "double")
specialize_template(APAGG_GENERATED_FILES ApproxPercentileAggregate.cpp.in ${APAGG_TYPES})

set(MIN_MAX_BY_TYPES
    "bool"
    "int8_t"
    "int16_t"
    "int32_t"
    "int64_t"
    "float"
    "double"
    "StringView"
    "Timestamp"
    "ComplexType"
)
specialize_template(MAX_BY_NARG_GENERATED_FILES MaxByAggregatesNArg.cpp.in ${MIN_MAX_BY_TYPES})
specialize_template(MIN_BY_NARG_GENERATED_FILES MinByAggregatesNArg.cpp.in ${MIN_MAX_BY_TYPES})

bolt_add_library(
  bolt_aggregates
  ${APAGG_GENERATED_FILES}
  ${MAX_BY_NARG_GENERATED_FILES}
  ${MIN_BY_NARG_GENERATED_FILES}
  ApproxDistinctAggregate.cpp
  ApproxMostFrequentAggregate.cpp
  ApproxPercentileAggregate.cpp
  ArbitraryAggregate.cpp
  ArrayAdditionAggregate.cpp
  ArrayAggAggregate.cpp
  ArrayCountAggregate.cpp
  AverageAggregate.cpp
  BitwiseAggregates.cpp
  BitwiseXorAggregate.cpp
  BoolAggregates.cpp
  CentralMomentsAggregates.cpp
  ChecksumAggregate.cpp
  Compare.cpp
  CountAggregate.cpp
  CountIfAggregate.cpp
  CovarianceAggregates.cpp
  EntropyAggregates.cpp
  FirstLastAggregate.cpp
  GeometricMeanAggregate.cpp
  HistogramAggregate.cpp
  HiveUDAFPercentileAggregate.cpp
  MapAggAggregate.cpp
  MapSumAggregate.cpp
  MapUnionAggregate.cpp
  MapUnionAvgAggregate.cpp
  MapUnionCountAggregate.cpp
  MapUnionMinMaxAggregates.cpp
  MapUnionSumAggregate.cpp
  MaxSizeForStatsAggregate.cpp
  MinMaxAggregates.cpp
  MinMaxByAggregates.cpp
  MultiMapAggAggregate.cpp
  PrestoHasher.cpp
  ReduceAgg.cpp
  RegisterAggregateFunctions.cpp
  SetAggregates.cpp
  SumAggregate.cpp
  SumDataSizeForStatsAggregate.cpp
  VarianceAggregates.cpp
)

target_link_libraries(
  bolt_aggregates
  bolt_common_hyperloglog
  bolt_exec
  bolt_expression
  bolt_presto_serializer
  bolt_functions_aggregates
  bolt_functions_lib
  bolt_functions_util
  Folly::folly
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
