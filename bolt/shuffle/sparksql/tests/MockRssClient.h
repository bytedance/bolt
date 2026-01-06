/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <gmock/gmock.h>
#include <map>
#include <string>
#include <vector>
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"

namespace bytedance::bolt::shuffle::sparksql::test {

class MockRssClient : public RssClient {
 public:
  MOCK_METHOD(
      int32_t,
      pushPartitionData,
      (int32_t partitionId, char* bytes, int64_t size),
      (override));
  MOCK_METHOD(void, stop, (), (override));

  // Helper to store data for verification or reading
  std::map<int32_t, std::vector<char>> data_;

  // Simple implementation to store data
  int32_t
  pushPartitionDataImpl(int32_t partitionId, char* bytes, int64_t size) {
    if (data_.find(partitionId) == data_.end()) {
      data_[partitionId] = std::vector<char>();
    }
    data_[partitionId].insert(data_[partitionId].end(), bytes, bytes + size);
    return size;
  }

  void delegateToFake() {
    ON_CALL(*this, pushPartitionData)
        .WillByDefault([this](int32_t pid, char* b, int64_t s) {
          return this->pushPartitionDataImpl(pid, b, s);
        });
  }
};

} // namespace bytedance::bolt::shuffle::sparksql::test
