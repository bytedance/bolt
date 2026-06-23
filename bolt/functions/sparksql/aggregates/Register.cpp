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

#include "bolt/functions/sparksql/aggregates/Register.h"

#include "bolt/functions/lib/aggregates/BitDaysOrAggregate.h"
#include "bolt/functions/lib/aggregates/CollectSetAggregate.h"
#include "bolt/functions/sparksql/aggregates/AverageAggregate.h"
#include "bolt/functions/sparksql/aggregates/BitwiseXorAggregate.h"
#include "bolt/functions/sparksql/aggregates/BloomFilterAggAggregate.h"
#include "bolt/functions/sparksql/aggregates/CentralMomentsAggregate.h"
#include "bolt/functions/sparksql/aggregates/CollectListAggregate.h"
#include "bolt/functions/sparksql/aggregates/CovarianceAggregate.h"
#include "bolt/functions/sparksql/aggregates/DecimalSumAggregate.h"
#include "bolt/functions/sparksql/aggregates/PercentileAggregate.h"
#include "bolt/functions/sparksql/aggregates/RegrReplacementAggregate.h"
#include "bolt/functions/sparksql/aggregates/SumAggregate.h"
namespace bytedance::bolt::aggregate::prestosql {
extern void registerMapSumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
} // namespace bytedance::bolt::aggregate::prestosql
namespace bytedance::bolt::functions::aggregate::sparksql {

extern void registerFirstLastAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMinMaxByAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerPercentileApproxAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerModeAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);

void registerAggregateFunctions(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerFirstLastAggregates(prefix, withCompanionFunctions, overwrite);
  registerMinMaxByAggregates(prefix, withCompanionFunctions, overwrite);
  registerBitDaysOrAggregate(
      prefix + "bit_days_or", withCompanionFunctions, overwrite);
  registerBitwiseXorAggregate(prefix, withCompanionFunctions, overwrite);
  registerPercentileAggregate(prefix);
  registerPercentileApproxAggregate(prefix, withCompanionFunctions, overwrite);
  registerBloomFilterAggAggregate(
      prefix + "bloom_filter_agg", withCompanionFunctions, overwrite);
  registerAverage(prefix + "avg", withCompanionFunctions, overwrite);
  registerSumAggregate(prefix + "sum", withCompanionFunctions, overwrite);
  bytedance::bolt::aggregate::prestosql::registerMapSumAggregate(
      prefix, withCompanionFunctions, overwrite);
  registerCentralMomentsAggregate(prefix, withCompanionFunctions, overwrite);
  registerCollectSetAggregate(prefix, withCompanionFunctions, overwrite);
  registerCollectListAggregate(prefix, withCompanionFunctions, overwrite);
  registerRegrReplacementAggregate(prefix, withCompanionFunctions, overwrite);
  registerCovarianceAggregates(prefix, withCompanionFunctions, overwrite);
  registerModeAggregate(prefix, withCompanionFunctions, overwrite);
}
} // namespace bytedance::bolt::functions::aggregate::sparksql
