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

#pragma once

#include <cstdint>
#include <memory>

#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"

namespace celeborn::client {
class ShuffleClient;
} // namespace celeborn::client

namespace bytedance::bolt::shuffle::sparksql {
class NativeCelebornClient : public RssClient {
 public:
  NativeCelebornClient(
      std::shared_ptr<celeborn::client::ShuffleClient> shuffleClient,
      int32_t shuffleId,
      int32_t mapId,
      int32_t attemptId,
      int32_t numMappers,
      int32_t numPartitions);
  ~NativeCelebornClient() override = default;

  int32_t pushPartitionData(int32_t partitionId, char* bytes, int64_t size)
      override;

  void stop() override;

 private:
  std::shared_ptr<celeborn::client::ShuffleClient> shuffleClient_;
  int32_t shuffleId_;
  int32_t mapId_;
  int32_t attemptId_;
  int32_t numMappers_;
  int32_t numPartitions_;
  bool stopped_{false};
};
} // namespace bytedance::bolt::shuffle::sparksql
