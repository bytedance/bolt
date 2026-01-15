/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

#include "FallbackRangePartitioner.h"
#include "bolt/shuffle/sparksql/partitioner/FallbackRangePartitioner.h"
namespace bytedance::bolt::shuffle::sparksql {

arrow::Status FallbackRangePartitioner::compute(
    const int32_t* pidArr,
    const int64_t numRows,
    std::vector<uint32_t>& row2Partition,
    std::vector<uint32_t>& partition2RowCount) {
  row2Partition.resize(numRows);
  std::fill(std::begin(partition2RowCount), std::end(partition2RowCount), 0);
  for (auto i = 0; i < numRows; ++i) {
    auto pid = pidArr[i];
    if (pid >= numPartitions_) {
      return arrow::Status::Invalid(
          "Partition id ",
          std::to_string(pid),
          " is equal or greater than ",
          std::to_string(numPartitions_));
    }
    row2Partition[i] = pid;
    partition2RowCount[pid]++;
  }
  return arrow::Status::OK();
}

arrow::Status FallbackRangePartitioner::precompute(
    int32_t* pidArr,
    const int64_t numRows,
    std::vector<uint32_t>& partition2RowCount,
    bool doInitialize) {
  if (doInitialize) {
    std::fill(std::begin(partition2RowCount), std::end(partition2RowCount), 0);
  }
  for (auto i = 0; i < numRows; ++i) {
    auto pid = pidArr[i];
    if (pid >= numPartitions_) {
      return arrow::Status::Invalid(
          "Partition id ",
          std::to_string(pid),
          " is equal or greater than ",
          std::to_string(numPartitions_));
    }
    partition2RowCount[pid]++;
  }
  return arrow::Status::OK();
}

arrow::Status FallbackRangePartitioner::fill(
    const int32_t* pidArr,
    const int64_t numRows,
    std::vector<uint32_t>& row2Partition,
    std::vector<uint32_t>& partition2RowCount) {
  return compute(pidArr, numRows, row2Partition, partition2RowCount);
}

} // namespace bytedance::bolt::shuffle::sparksql
