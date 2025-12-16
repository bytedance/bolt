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
#include <vector>

#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"

namespace celeborn::client {
class ShuffleClient;
class CelebornInputStream;
} // namespace celeborn::client

namespace bytedance::bolt::shuffle::sparksql {
class CelebornReaderStreamIterator : public ReaderStreamIterator {
 public:
  CelebornReaderStreamIterator(
      std::shared_ptr<celeborn::client::ShuffleClient> shuffleClient,
      int32_t shuffleId,
      std::vector<int32_t> partitionIds,
      int32_t attemptNumber,
      int32_t startMapIndex,
      int32_t endMapIndex,
      bool needCompression);
  ~CelebornReaderStreamIterator() override;

  std::shared_ptr<arrow::io::InputStream> nextStream(
      arrow::MemoryPool* pool) override;
  void close() override;
  void updateMetrics(
      int64_t numRows,
      int64_t numBatches,
      int64_t decompressTime,
      int64_t deserializeTime,
      int64_t totalReadTime) override;

 private:
  std::shared_ptr<celeborn::client::ShuffleClient> shuffleClient_;
  int32_t shuffleId_;
  std::vector<int32_t> partitionIds_;
  int32_t attemptNumber_;
  int32_t startMapIndex_;
  int32_t endMapIndex_;
  bool needCompression_;
  size_t nextPartitionIndex_{0};
  bool closed_{false};

  int64_t totalRows_{0};
  int64_t totalBatches_{0};
  int64_t totalDecompressTime_{0};
  int64_t totalDeserializeTime_{0};
  int64_t totalReadTime_{0};
};
} // namespace bytedance::bolt::shuffle::sparksql
