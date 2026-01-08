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

#include "bolt/shuffle/sparksql/partitioner/Partitioner.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/partitioner/FallbackRangePartitioner.h"
#include "bolt/shuffle/sparksql/partitioner/HashPartitioner.h"
#include "bolt/shuffle/sparksql/partitioner/RoundRobinPartitioner.h"
#include "bolt/shuffle/sparksql/partitioner/SinglePartitioner.h"
namespace bytedance::bolt::shuffle::sparksql {

arrow::Result<std::shared_ptr<Partitioner>> Partitioner::make(
    Partitioning partitioning,
    int32_t numPartitions,
    int32_t startPartitionId,
    bool sortBeforeRepartition) {
  switch (partitioning) {
    case Partitioning::kHash:
      return std::make_shared<HashPartitioner>(numPartitions);
    case Partitioning::kRoundRobin:
      return std::make_shared<RoundRobinPartitioner>(
          numPartitions, startPartitionId, sortBeforeRepartition);
    case Partitioning::kSingle:
      return std::make_shared<SinglePartitioner>();
    case Partitioning::kRange:
      return std::make_shared<FallbackRangePartitioner>(numPartitions);
    default:
      return arrow::Status::Invalid(
          "Unsupported partitioning type: " + std::to_string(partitioning));
  }
}

arrow::Status Partitioner::precompute(
    int32_t* pidArr,
    const int64_t numRows,
    std::vector<uint32_t>& partition2RowCount,
    bool doInitialize) {
  BOLT_NYI("precompute is not supported for this partitioner type");
}

arrow::Status Partitioner::fill(
    const int32_t* pidArr,
    const int64_t numRows,
    std::vector<uint32_t>& row2partition,
    std::vector<uint32_t>& partition2RowCount) {
  BOLT_NYI("fill is not supported for this partitioner type");
  return arrow::Status::OK();
}

} // namespace bytedance::bolt::shuffle::sparksql
