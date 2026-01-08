/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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
 */

#include "bolt/shuffle/sparksql/partition_writer/rss/NativeCelebornClient.h"

#include <celeborn/client/ShuffleClient.h>

#include <utility>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql {

NativeCelebornClient::NativeCelebornClient(
    std::shared_ptr<celeborn::client::ShuffleClient> shuffleClient,
    int32_t shuffleId,
    int32_t mapId,
    int32_t attemptId,
    int32_t numMappers,
    int32_t numPartitions)
    : shuffleClient_(std::move(shuffleClient)),
      shuffleId_(shuffleId),
      mapId_(mapId),
      attemptId_(attemptId),
      numMappers_(numMappers),
      numPartitions_(numPartitions) {
  BOLT_CHECK_NOT_NULL(
      shuffleClient_, "ShuffleClient is null for NativeCelebornClient");
}

int32_t NativeCelebornClient::pushPartitionData(
    int32_t partitionId,
    char* bytes,
    int64_t size) {
  BOLT_CHECK(
      !stopped_,
      "Cannot push data after NativeCelebornClient has been stopped");
  return shuffleClient_->pushData(
      shuffleId_,
      mapId_,
      attemptId_,
      partitionId,
      reinterpret_cast<const uint8_t*>(bytes),
      0,
      size,
      numMappers_,
      numPartitions_);
}

void NativeCelebornClient::stop() {
  BOLT_CHECK(
      !stopped_,
      "NativeCelebornClient stop() called multiple times for the same map attempt");
  shuffleClient_->mapperEnd(shuffleId_, mapId_, attemptId_, numMappers_);
  stopped_ = true;
}

} // namespace bytedance::bolt::shuffle::sparksql
