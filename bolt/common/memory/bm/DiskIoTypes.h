#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>

namespace bytedance::bolt::memory::bm {

enum class IoOpcode : uint8_t {
  Read,
  Write,
};

enum class IoPriority : uint8_t {
  High = 0,
  Medium = 1,
  Low = 2,
  Count,
};

constexpr size_t kIoPriorityCount = static_cast<size_t>(IoPriority::Count);

inline size_t priorityIndex(IoPriority priority) {
  return static_cast<size_t>(priority);
}

struct IoBuffer {
  std::shared_ptr<void> data;
  size_t size{0};
  size_t offset{0};
  size_t length{0};
};

struct IoRequest {
  IoOpcode opcode{IoOpcode::Read};
  IoPriority priority{IoPriority::Medium};
  int fd{-1};
  uint64_t fileOffset{0};
  IoBuffer buffer;
};

enum class IoErrorCode : uint8_t {
  Ok,
  InvalidRequest,
  Shutdown,
  BackendSubmitFailed,
  BackendIoError,
};

struct IoResult {
  uint64_t bytes{0};
  IoErrorCode error{IoErrorCode::Ok};
  int nativeErrorCode{0};

  bool ok() const {
    return error == IoErrorCode::Ok;
  }
};

struct AdaptiveDepthConfig {
  bool enabled{true};
  uint32_t minDepth{1};
  uint32_t initialDepth{64};
  uint32_t maxDepth{256};
  std::chrono::milliseconds controlInterval{200};
  uint32_t increaseStep{4};
  double minThroughputGain{0.02};
  double throughputSmoothingFactor{0.5};
};

struct DiskIoSchedulerConfig {
  uint32_t ringDepth{256};
  std::array<uint32_t, kIoPriorityCount> priorityWeights{{8, 4, 1}};
  AdaptiveDepthConfig adaptiveDepth;
};

struct DiskIoSchedulerStats {
  std::array<uint64_t, kIoPriorityCount> queuedRequests{{0, 0, 0}};
  std::array<uint64_t, kIoPriorityCount> submittedRequests{{0, 0, 0}};
  std::array<uint64_t, kIoPriorityCount> completedRequestsByPriority{{0, 0, 0}};
  uint64_t inflightRequests{0};
  uint32_t currentDepth{0};
  uint64_t completedRequests{0};
  uint64_t completedBytes{0};
  uint64_t successfulRequests{0};
  uint64_t failedRequests{0};
  double recentThroughputBytesPerSecond{0};
  uint64_t cumulativeLatencyUs{0};
  double averageLatencyUs{0};
};

inline bool validOpcode(IoOpcode opcode) {
  return opcode == IoOpcode::Read || opcode == IoOpcode::Write;
}

inline bool validPriority(IoPriority priority) {
  return priorityIndex(priority) < kIoPriorityCount;
}

inline IoErrorCode validateIoRequest(const IoRequest& request) {
  if (!validOpcode(request.opcode) || !validPriority(request.priority)) {
    return IoErrorCode::InvalidRequest;
  }
  if (request.fd < 0 || !request.buffer.data || request.buffer.length == 0) {
    return IoErrorCode::InvalidRequest;
  }
  if (request.buffer.offset > request.buffer.size) {
    return IoErrorCode::InvalidRequest;
  }
  if (request.buffer.length > request.buffer.size - request.buffer.offset) {
    return IoErrorCode::InvalidRequest;
  }
  return IoErrorCode::Ok;
}

inline IoErrorCode validateDiskIoSchedulerConfig(
    const DiskIoSchedulerConfig& config) {
  if (config.ringDepth == 0) {
    return IoErrorCode::InvalidRequest;
  }
  for (const auto weight : config.priorityWeights) {
    if (weight == 0) {
      return IoErrorCode::InvalidRequest;
    }
  }
  const auto& adaptive = config.adaptiveDepth;
  if (adaptive.minDepth == 0 || adaptive.increaseStep == 0) {
    return IoErrorCode::InvalidRequest;
  }
  if (adaptive.minDepth > adaptive.initialDepth ||
      adaptive.initialDepth > adaptive.maxDepth ||
      adaptive.maxDepth > config.ringDepth) {
    return IoErrorCode::InvalidRequest;
  }
  if (adaptive.controlInterval.count() <= 0 ||
      adaptive.minThroughputGain < 0 ||
      adaptive.throughputSmoothingFactor <= 0 ||
      adaptive.throughputSmoothingFactor > 1) {
    return IoErrorCode::InvalidRequest;
  }
  return IoErrorCode::Ok;
}

} // namespace bytedance::bolt::memory::bm
