/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <map>
#include <vector>
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"

namespace bytedance::bolt::shuffle::sparksql::test {

class MockRssClient : public RssClient {
 public:
  // Simple implementation to store data
  int32_t pushPartitionData(int32_t partitionId, char* bytes, int64_t size)
      override {
    if (data_.find(partitionId) == data_.end()) {
      data_[partitionId] = std::vector<char>();
    }
    data_[partitionId].insert(data_[partitionId].end(), bytes, bytes + size);
    return size;
  }

  void stop() override {}

  const auto& getData() const {
    return data_;
  }

 public:
  // Helper to store data for verification or reading
  std::map<int32_t, std::vector<char>> data_;
};

} // namespace bytedance::bolt::shuffle::sparksql::test
