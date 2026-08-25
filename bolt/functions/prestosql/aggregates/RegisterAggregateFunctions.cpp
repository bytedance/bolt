/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/base/SparkCompatibility.h"

#include "bolt/exec/Aggregate.h"
#include "bolt/functions/lib/aggregates/BitDaysOrAggregate.h"
#include "bolt/functions/lib/aggregates/CollectSetAggregate.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
namespace bytedance::bolt::aggregate::prestosql {

extern void registerApproxMostFrequentAggregate(const std::string& prefix);
extern void registerApproxPercentileAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerArbitraryAggregate(const std::string& prefix);
extern void registerCollectListAsArrayAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerArrayAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);

extern void registerArrayAdditionAggregate(const std::string& prefix);
extern void registerArrayCountAggregate(const std::string& prefix);
extern void registerAverageAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerBitwiseXorAggregate(const std::string& prefix);
extern void registerChecksumAggregate(const std::string& prefix);
extern void registerCountAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerCountIfAggregate(const std::string& prefix);
extern void registerEntropyAggregate(const std::string& prefix);
extern void registerGeometricMeanAggregate(const std::string& prefix);
extern void registerHistogramAggregate(const std::string& prefix);
extern void registerMapAggAggregate(const std::string& prefix);
extern void registerMapUnionAggregate(const std::string& prefix);
extern void registerMapUnionSumAggregate(const std::string& prefix);
extern void registerMaxDataSizeForStatsAggregate(const std::string& prefix);
extern void registerMultiMapAggAggregate(const std::string& prefix);
extern void registerSumDataSizeForStatsAggregate(const std::string& prefix);
extern void registerReduceAgg(const std::string& prefix);
extern void registerMapUnionAvgAggregate(const std::string& prefix);
extern void registerMapUnionCountAggregate(const std::string& prefix);
extern void registerMapUnionMinMaxAggregate(const std::string& prefix);
extern void registerMaxDataSizeForStatsAggregate(const std::string& prefix);
extern void registerMinMaxAggregates(const std::string& prefix);
extern void registerMinMaxByAggregates(const std::string& prefix);
extern void registerSetAggAggregate(const std::string& prefix);
extern void registerSetUnionAggregate(const std::string& prefix);
extern void registerApproxDistinctAggregates(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerBitwiseAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerBoolAggregates(const std::string& prefix);
extern void registerCentralMomentsAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerCovarianceAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMinMaxAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMinMaxByAggregates(const std::string& prefix);
extern void registerSumAggregate(const std::string& prefix);
extern void registerVarianceAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMapSumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerFirstLastAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerHiveUDAFPercentileAggregate(const std::string& prefix);

void registerAllAggregateFunctions(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerApproxDistinctAggregates(prefix, withCompanionFunctions);
  registerApproxMostFrequentAggregate(prefix);
  registerApproxPercentileAggregate(prefix, withCompanionFunctions);
  registerArbitraryAggregate(prefix);
  registerArrayAggregate(prefix, withCompanionFunctions);
  registerArrayAdditionAggregate(prefix);
  registerArrayCountAggregate(prefix);
  registerAverageAggregate(prefix, withCompanionFunctions);
  registerBitwiseAggregates(prefix, withCompanionFunctions, overwrite);
  registerBitwiseXorAggregate(prefix);
  registerBoolAggregates(prefix);
  registerCentralMomentsAggregates(prefix, withCompanionFunctions, overwrite);
  registerChecksumAggregate(prefix);
  registerCollectListAsArrayAggregate(prefix, withCompanionFunctions);
  registerCountAggregate(prefix, withCompanionFunctions, overwrite);
  registerCountIfAggregate(prefix);
  registerCovarianceAggregates(prefix, withCompanionFunctions, overwrite);
  registerEntropyAggregate(prefix);
  registerGeometricMeanAggregate(prefix);
  registerHistogramAggregate(prefix);
  registerMapAggAggregate(prefix);
  registerMapUnionAggregate(prefix);
  registerMapUnionSumAggregate(prefix);
  registerMaxDataSizeForStatsAggregate(prefix);
  registerMultiMapAggAggregate(prefix);
  registerSumDataSizeForStatsAggregate(prefix);
  registerMinMaxAggregates(prefix, withCompanionFunctions, overwrite);
  registerMapUnionAvgAggregate(prefix);
  registerMapUnionCountAggregate(prefix);
  registerMapUnionMinMaxAggregate(prefix);
  registerMaxDataSizeForStatsAggregate(prefix);
  registerMinMaxByAggregates(prefix);
  registerReduceAgg(prefix);
  registerSetAggAggregate(prefix);
  registerSetUnionAggregate(prefix);
  registerSumAggregate(prefix);
  registerVarianceAggregates(prefix, withCompanionFunctions, overwrite);
  registerMapSumAggregate(prefix, withCompanionFunctions, overwrite);
  registerFirstLastAggregates(prefix, withCompanionFunctions, overwrite);
  functions::aggregate::registerCollectSetAggregate(
      prefix, withCompanionFunctions, overwrite);
  functions::aggregate::registerBitDaysOrAggregate(
      prefix + "bit_days_or", withCompanionFunctions, overwrite);
  if (::bytedance::bolt::kSparkCompatible) {
    return;
  }
  // hive.udaf.percentile for presto.default.percentile
  registerHiveUDAFPercentileAggregate(prefix);
}

extern void registerCountDistinctAggregate(const std::string& prefix);

void registerInternalAggregateFunctions(const std::string& prefix) {
  registerCountDistinctAggregate(prefix);
}

} // namespace bytedance::bolt::aggregate::prestosql
