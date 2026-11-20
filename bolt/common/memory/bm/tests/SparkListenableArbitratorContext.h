/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/sparksql/NativeMemoryManagerFactory.h"
#include "bolt/common/memory/sparksql/Spiller.h"
#include "bolt/common/memory/sparksql/TaskMemoryManager.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace bytedance::bolt::memory::bm {

struct SparkListenableArbitratorContextOptions {
  std::string name{"bm-benchmark"};
  int64_t memoryLimitBytes{0};
  int64_t minMemoryMaxWaitMs{10'000};
  bool memoryIsolation{false};
  double overAcquiredRatio{0};
  std::unordered_map<std::string, std::string> sessionConf;
  int32_t maxTaskNumber{1};
  bool initializeExecutionMemoryPool{true};
};

struct SparkListenableArbitratorContextStats {
  uint64_t automaticSpillTriggers{0};
  uint64_t automaticSpillRequestedBytes{0};
  uint64_t automaticSpillShrunkenBytes{0};
  uint64_t automaticSpillReclaimedBytes{0};
  uint64_t automaticSpillReturnedBytes{0};
  uint64_t automaticSpillTimeUs{0};
};

class SparkListenableArbitratorContext {
 public:
  explicit SparkListenableArbitratorContext(
      SparkListenableArbitratorContextOptions options);
  ~SparkListenableArbitratorContext();

  SparkListenableArbitratorContext(const SparkListenableArbitratorContext&) =
      delete;
  SparkListenableArbitratorContext& operator=(
      const SparkListenableArbitratorContext&) = delete;

  std::shared_ptr<MemoryPool> rootPool() const;
  void installAutomaticReclaimSpill();
  SparkListenableArbitratorContextStats stats() const;

  static void initializeExecutionMemoryPool(
      int64_t memoryLimitBytes,
      int64_t minMemoryMaxWaitMs,
      int32_t maxTaskNumber);

 private:
  int64_t spillFixedSize(int64_t size);

  SparkListenableArbitratorContextOptions options_;
  mutable std::mutex statsMutex_;
  SparkListenableArbitratorContextStats stats_;
  std::shared_ptr<sparksql::TaskMemoryManager> taskMemoryManager_;
  sparksql::BoltMemoryManagerHolder* holder_{nullptr};
};

} // namespace bytedance::bolt::memory::bm
