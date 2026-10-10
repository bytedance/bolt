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

#include <chrono>
#include <cstdint>
#include <future>
#include <optional>
#include <unordered_map>

#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

struct InflightIoRequest {
  IoRequest request;
  std::promise<IoResult> promise;
  std::chrono::steady_clock::time_point enqueueTime;
  std::chrono::steady_clock::time_point submitTime;
};

class InflightRegistry {
 public:
  void add(uint64_t requestId, InflightIoRequest request);
  std::optional<InflightIoRequest> take(uint64_t requestId);

  bool empty() const;
  size_t size() const;

 private:
  std::unordered_map<uint64_t, InflightIoRequest> requests_;
};

} // namespace bytedance::bolt::memory::bm
