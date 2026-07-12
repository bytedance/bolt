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
    pushCalls_++;
    pushBytes_ += size;
    appendData(partitionId, bytes, size);
    return size;
  }

  int32_t mergePartitionData(int32_t partitionId, char* bytes, int64_t size)
      override {
    mergeCalls_++;
    mergeBytes_ += size;
    appendData(partitionId, bytes, size);
    return size;
  }

  void stop() override {}

  const auto& getData() const {
    return data_;
  }

  int64_t pushCalls() const {
    return pushCalls_;
  }
  int64_t mergeCalls() const {
    return mergeCalls_;
  }
  int64_t pushBytes() const {
    return pushBytes_;
  }
  int64_t mergeBytes() const {
    return mergeBytes_;
  }

 public:
  // Helper to store data for verification or reading
  std::map<int32_t, std::vector<char>> data_;

 private:
  void appendData(int32_t partitionId, char* bytes, int64_t size) {
    if (data_.find(partitionId) == data_.end()) {
      data_[partitionId] = std::vector<char>();
    }
    data_[partitionId].insert(data_[partitionId].end(), bytes, bytes + size);
  }

  int64_t pushCalls_{0};
  int64_t mergeCalls_{0};
  int64_t pushBytes_{0};
  int64_t mergeBytes_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::test
