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

#pragma once
namespace bytedance::bolt::aggregate {

const char* const kApproxDistinct = "approx_distinct";
const char* const kApproxMostFrequent = "approx_most_frequent";
const char* const kApproxPercentile = "approx_percentile";
const char* const kPercentileApprox = "percentile_approx";
const char* const kApproxSet = "approx_set";
const char* const kArbitrary = "arbitrary";
const char* const kAnyValue = "any_value";
const char* const kArrayAgg = "array_agg";
const char* const kArrayAdditionAgg = "array_addition";
const char* const kArrayCountAgg = "array_count";
const char* const kAvg = "avg";
const char* const kBitwiseAnd = "bitwise_and_agg";
const char* const kBitwiseOr = "bitwise_or_agg";
const char* const kBitwiseXor = "bitwise_xor_agg";
const char* const kBoolAnd = "bool_and";
const char* const kBoolOr = "bool_or";
const char* const kChecksum = "checksum";
const char* const kCorr = "corr";
const char* const kCount = "count";
const char* const kCountIf = "count_if";
const char* const kCovarPop = "covar_pop";
const char* const kCovarSamp = "covar_samp";
const char* const kEntropy = "entropy";
const char* const kEvery = "every";
const char* const kGeometricMean = "geometric_mean";
const char* const kHistogram = "histogram";
const char* const kKurtosis = "kurtosis";
const char* const kMapAgg = "map_agg";
const char* const kMapSum = "aggregate_map_sum";
const char* const kMapUnion = "map_union";
const char* const kMapUnionSum = "map_union_sum";
const char* const kMapUnionAvg = "map_union_avg";
const char* const kMapUnionCount = "map_union_count";
const char* const kMapUnionMin = "map_union_min";
const char* const kMapUnionMax = "map_union_max";
const char* const kMax = "max";
const char* const kMaxBy = "max_by";
const char* const kMerge = "merge";
const char* const kMin = "min";
const char* const kMinBy = "min_by";
const char* const kMultiMapAgg = "multimap_agg";
const char* const kReduceAgg = "reduce_agg";
const char* const kRegrIntercept = "regr_intercept";
const char* const kRegrSlop = "regr_slope";
const char* const kRegrCount = "regr_count";
const char* const kRegrAvgy = "regr_avgy";
const char* const kRegrAvgx = "regr_avgx";
const char* const kRegrSxy = "regr_sxy";
const char* const kRegrSxx = "regr_sxx";
const char* const kRegrSyy = "regr_syy";
const char* const kRegrR2 = "regr_r2";
const char* const kSetAgg = "set_agg";
const char* const kCollectSet = "collect_set";
const char* const kCollectList = "collect_list";
const char* const kSetUnion = "set_union";
const char* const kSkewness = "skewness";
const char* const kStd = "std"; // Alias for stddev_pop.
const char* const kStdDev = "stddev"; // Alias for stddev_samp.
const char* const kStdDevPop = "stddev_pop";
const char* const kStdDevSamp = "stddev_samp";
const char* const kSum = "sum";
const char* const kVariance = "variance"; // Alias for var_samp.
const char* const kVarPop = "var_pop";
const char* const kVarSamp = "var_samp";
const char* const kMaxSizeForStats = "max_data_size_for_stats";
const char* const kSumDataSizeForStats = "sum_data_size_for_stats";
} // namespace bytedance::bolt::aggregate
