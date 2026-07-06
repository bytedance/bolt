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

#include "bolt/shuffle/sparksql/partitioner/HashPartitioner.h"
namespace bytedance::bolt::shuffle::sparksql {

arrow::Status HashPartitioner::compute(
    const int32_t* pidArr,
    const int64_t numRows,
    std::vector<uint32_t>& row2partition,
    std::vector<uint32_t>& partition2RowCount) {
  row2partition.resize(numRows);
  std::fill(std::begin(partition2RowCount), std::end(partition2RowCount), 0);

  for (auto i = 0; i < numRows; ++i) {
    auto pid = pidArr[i] % numPartitions_;
    if (pid < 0) {
      pid += numPartitions_;
    }
    row2partition[i] = pid;
  }

  for (auto& pid : row2partition) {
    partition2RowCount[pid]++;
  }

  return arrow::Status::OK();
}

arrow::Status HashPartitioner::precompute(
    int32_t* pidArr,
    const int64_t numRows,
    std::vector<uint32_t>& partition2RowCount,
    bool doInitialize) {
  if (doInitialize) {
    std::fill(std::begin(partition2RowCount), std::end(partition2RowCount), 0);
  }

  for (auto i = 0; i < numRows; ++i) {
    auto pid = pidArr[i] % numPartitions_;
    if (pid < 0) {
      pid += numPartitions_;
    }
    pidArr[i] = pid;
    partition2RowCount[pid]++;
  }

  return arrow::Status::OK();
}

arrow::Status HashPartitioner::fill(
    const int32_t* pidArr,
    const int64_t numRows,
    std::vector<uint32_t>& row2partition,
    std::vector<uint32_t>& partition2RowCount) {
  row2partition.resize(numRows);
  std::fill(std::begin(partition2RowCount), std::end(partition2RowCount), 0);

  for (auto i = 0; i < numRows; ++i) {
    row2partition[i] = pidArr[i];
  }

  for (auto& pid : row2partition) {
    partition2RowCount[pid]++;
  }
  return arrow::Status::OK();
}

} // namespace bytedance::bolt::shuffle::sparksql
