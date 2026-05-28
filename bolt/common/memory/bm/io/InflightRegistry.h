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
