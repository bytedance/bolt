# Disk IO Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `bolt/common/memory/bm` 下实现一个独立的 Disk IO 调度基础库，提供 `std::future<IoResult>` API、三档优先级、可配置加权公平调度、吞吐导向 adaptive inflight depth、io_uring backend 和 mock backend。

**Architecture:** 公共 API 保持标准库接口，不暴露 Folly。`DiskIoScheduler` 负责队列、调度、线程、promise completion、stats 和 adaptive depth；`IoBackend` 只负责 submit/reap；`MockIoBackend` 用于确定性单测，`IoUringBackend` 用于真实 IO。第一版 single-shard，不接入现有 spill/file 路径。

**Tech Stack:** C++20, std::future/std::promise, std::thread/std::condition_variable, GTest/GMock, io_uring/liburing behind `IO_URING_SUPPORTED`, new isolated `bolt_memory_bm` CMake target.

---

## File Structure

- Create `bolt/common/memory/bm/DiskIoTypes.h`
  - Public enums, request/result structs, config structs, stats structs, validation helpers.
- Create `bolt/common/memory/bm/IoBackend.h`
  - Internal backend interface and backend request/completion structs.
- Create `bolt/common/memory/bm/MockIoBackend.h`
  - Test-only backend declaration with deterministic submit log and controlled completions.
- Create `bolt/common/memory/bm/MockIoBackend.cpp`
  - Test-only mock backend implementation, compiled into `bolt_memory_bm_test`, not `bolt_memory_bm`.
- Create `bolt/common/memory/bm/AdaptiveDepthController.h`
  - Throughput hill-climbing controller, independent from scheduler threading.
- Create `bolt/common/memory/bm/AdaptiveDepthController.cpp`
  - Controller implementation.
- Create `bolt/common/memory/bm/DiskIoScheduler.h`
  - Public scheduler facade.
- Create `bolt/common/memory/bm/DiskIoScheduler.cpp`
  - Queueing, DWRR dispatch, backend completion, stats, shutdown/drain.
- Create `bolt/common/memory/bm/IoUringBackend.h`
  - Real backend declaration behind `IO_URING_SUPPORTED`.
- Create `bolt/common/memory/bm/IoUringBackend.cpp`
  - Real io_uring backend implementation behind `IO_URING_SUPPORTED`.
- Create `bolt/common/memory/bm/CMakeLists.txt`
  - Defines isolated `bolt_memory_bm` library target and adds `tests` when testing is enabled.
- Create `bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp`
  - Mock backend tests for validation, FIFO, weighted fairness, depth, stats, shutdown.
- Create `bolt/common/memory/bm/tests/AdaptiveDepthControllerTest.cpp`
  - Isolated controller tests.
- Create `bolt/common/memory/bm/tests/IoUringBackendTest.cpp`
  - Conditional integration tests for real read/write and invalid fd.
- Create `bolt/common/memory/bm/tests/CMakeLists.txt`
  - Defines isolated `bolt_memory_bm_test` executable target.
- Modify `bolt/common/memory/CMakeLists.txt`
  - Add only `add_subdirectory(bm)` so BM code is isolated from `bolt_memory`.

## Verification Commands

Use these commands throughout the plan:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
```

Expected: build exits 0 and updates `_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test`.

```bash
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test --gtest_filter='*DiskIo*:*AdaptiveDepth*'
```

Expected: all matching tests pass.

```bash
PATH=/data00/home/wangxinshuo.db/tools/miniconda3/bin:$PATH make release_with_test
```

Expected: repository release build and tests complete successfully.

---

### Task 1: Public Types, Config, Stats, and Validation

**Files:**
- Create: `bolt/common/memory/bm/DiskIoTypes.h`
- Create: `bolt/common/memory/bm/CMakeLists.txt`
- Create: `bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp`
- Create: `bolt/common/memory/bm/tests/CMakeLists.txt`
- Modify: `bolt/common/memory/CMakeLists.txt`

- [ ] **Step 1: Write failing validation tests**

Add `bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp` with these initial tests:

```cpp
#include "bolt/common/memory/bm/DiskIoTypes.h"

#include <cerrno>
#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

namespace {

std::shared_ptr<void> makeBuffer(size_t size) {
  return std::shared_ptr<void>(
      new char[size], [](void* ptr) { delete[] static_cast<char*>(ptr); });
}

} // namespace

TEST(DiskIoTypesTest, validateRequestAcceptsValidRead) {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::High;
  request.fd = 3;
  request.fileOffset = 128;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 64, 1024};

  EXPECT_EQ(0, validateIoRequest(request));
}

TEST(DiskIoTypesTest, validateRequestRejectsBadBufferRange) {
  IoRequest request;
  request.opcode = IoOpcode::Write;
  request.priority = IoPriority::Low;
  request.fd = 3;
  request.fileOffset = 0;
  request.buffer = IoBuffer{makeBuffer(128), 128, 64, 128};

  EXPECT_EQ(EINVAL, validateIoRequest(request));
}

TEST(DiskIoTypesTest, validateConfigRejectsInvalidDepth) {
  DiskIoSchedulerConfig config;
  config.ringDepth = 16;
  config.adaptiveDepth.minDepth = 1;
  config.adaptiveDepth.initialDepth = 32;
  config.adaptiveDepth.maxDepth = 32;

  EXPECT_EQ(EINVAL, validateDiskIoSchedulerConfig(config));
}
```

- [ ] **Step 2: Add tests to CMake and verify they fail**

Create `bolt/common/memory/bm/CMakeLists.txt`:

```cmake
if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

add_library(bolt_memory_bm INTERFACE)

target_include_directories(bolt_memory_bm INTERFACE ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm
  INTERFACE bolt_common_base
            bolt_exception
            Folly::folly
            glog::glog
)
```

Create `bolt/common/memory/bm/tests/CMakeLists.txt`:

```cmake
include(GoogleTest)

add_executable(
  bolt_memory_bm_test
  DiskIoSchedulerTest.cpp
)

target_compile_features(bolt_memory_bm_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_memory_bm_test
  PRIVATE bolt_memory_bm
          GTest::gmock
          GTest::gtest
          GTest::gtest_main
          pthread
)

gtest_add_tests(bolt_memory_bm_test "" AUTO)
```

Modify `bolt/common/memory/CMakeLists.txt` near the top:

```cmake
add_subdirectory(bm)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
  add_subdirectory(sparksql/tests)
endif()
```

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
```

Expected: FAIL because `bolt/common/memory/bm/DiskIoTypes.h` does not exist.

- [ ] **Step 3: Implement public types and validation**

Create `bolt/common/memory/bm/DiskIoTypes.h`:

```cpp
#pragma once

#include <array>
#include <cerrno>
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
};

constexpr size_t kIoPriorityCount = 3;

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

struct IoResult {
  uint64_t bytes{0};
  int errorCode{0};

  bool ok() const {
    return errorCode == 0;
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
  double averageLatencyUs{0};
};

inline bool validOpcode(IoOpcode opcode) {
  return opcode == IoOpcode::Read || opcode == IoOpcode::Write;
}

inline bool validPriority(IoPriority priority) {
  return priorityIndex(priority) < kIoPriorityCount;
}

inline int validateIoRequest(const IoRequest& request) {
  if (!validOpcode(request.opcode) || !validPriority(request.priority)) {
    return EINVAL;
  }
  if (request.fd < 0 || !request.buffer.data || request.buffer.length == 0) {
    return EINVAL;
  }
  if (request.buffer.offset > request.buffer.size) {
    return EINVAL;
  }
  if (request.buffer.length > request.buffer.size - request.buffer.offset) {
    return EINVAL;
  }
  return 0;
}

inline int validateDiskIoSchedulerConfig(const DiskIoSchedulerConfig& config) {
  if (config.ringDepth == 0) {
    return EINVAL;
  }
  for (const auto weight : config.priorityWeights) {
    if (weight == 0) {
      return EINVAL;
    }
  }
  const auto& adaptive = config.adaptiveDepth;
  if (adaptive.minDepth == 0 || adaptive.increaseStep == 0) {
    return EINVAL;
  }
  if (adaptive.minDepth > adaptive.initialDepth ||
      adaptive.initialDepth > adaptive.maxDepth ||
      adaptive.maxDepth > config.ringDepth) {
    return EINVAL;
  }
  if (adaptive.controlInterval.count() <= 0 ||
      adaptive.minThroughputGain < 0) {
    return EINVAL;
  }
  return 0;
}

} // namespace bytedance::bolt::memory::bm
```

- [ ] **Step 4: Verify isolated target**

No source is added to `bolt_memory` in this task. `bolt_memory_bm` is an isolated interface target until `.cpp` files are introduced.

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test --gtest_filter='DiskIoTypesTest.*'
```

Expected: build exits 0; `3 tests from DiskIoTypesTest` pass.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/CMakeLists.txt bolt/common/memory/bm/CMakeLists.txt bolt/common/memory/bm/DiskIoTypes.h bolt/common/memory/bm/tests/CMakeLists.txt bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp
git commit -m "feat: add disk io public types"
```

---

### Task 2: Backend Interface and Deterministic Mock Backend

**Files:**
- Create: `bolt/common/memory/bm/IoBackend.h`
- Create: `bolt/common/memory/bm/MockIoBackend.h`
- Create: `bolt/common/memory/bm/MockIoBackend.cpp`
- Modify: `bolt/common/memory/bm/tests/CMakeLists.txt`
- Modify: `bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp`

- [ ] **Step 1: Write failing mock backend tests**

Append to `DiskIoSchedulerTest.cpp`:

```cpp
#include "bolt/common/memory/bm/MockIoBackend.h"

TEST(MockIoBackendTest, recordsSubmittedRequestsAndCompletesInChosenOrder) {
  MockIoBackend backend;
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::Medium;
  request.fd = 7;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 0, 4096};

  EXPECT_TRUE(backend.submit(11, request));
  EXPECT_TRUE(backend.submit(12, request));
  ASSERT_EQ(2, backend.submitted().size());
  EXPECT_EQ(11, backend.submitted()[0].requestId);
  EXPECT_EQ(12, backend.submitted()[1].requestId);

  backend.complete(12, IoResult{4096, 0});
  auto completions = backend.reap();
  ASSERT_EQ(1, completions.size());
  EXPECT_EQ(12, completions[0].requestId);
  EXPECT_EQ(4096, completions[0].result.bytes);
}
```

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
```

Expected: FAIL because `MockIoBackend.h` does not exist.

- [ ] **Step 2: Define backend interface**

Create `bolt/common/memory/bm/IoBackend.h`:

```cpp
#pragma once

#include <cstdint>
#include <vector>

#include "bolt/common/memory/bm/DiskIoTypes.h"

namespace bytedance::bolt::memory::bm {

struct BackendCompletion {
  uint64_t requestId{0};
  IoResult result;
};

class IoBackend {
 public:
  virtual ~IoBackend() = default;

  virtual bool submit(uint64_t requestId, const IoRequest& request) = 0;
  virtual std::vector<BackendCompletion> reap() = 0;
};

} // namespace bytedance::bolt::memory::bm
```

- [ ] **Step 3: Implement mock backend**

Create `bolt/common/memory/bm/MockIoBackend.h`:

```cpp
#pragma once

#include <mutex>
#include <unordered_set>
#include <vector>

#include "bolt/common/memory/bm/IoBackend.h"

namespace bytedance::bolt::memory::bm {

struct MockSubmittedRequest {
  uint64_t requestId{0};
  IoRequest request;
};

class MockIoBackend : public IoBackend {
 public:
  bool submit(uint64_t requestId, const IoRequest& request) override;
  std::vector<BackendCompletion> reap() override;

  void complete(uint64_t requestId, IoResult result);

  std::vector<MockSubmittedRequest> submitted() const;
  size_t inflight() const;

 private:
  mutable std::mutex mutex_;
  std::vector<MockSubmittedRequest> submitted_;
  std::vector<BackendCompletion> completions_;
  std::unordered_set<uint64_t> inflight_;
};

} // namespace bytedance::bolt::memory::bm
```

Create `bolt/common/memory/bm/MockIoBackend.cpp`:

```cpp
#include "bolt/common/memory/bm/MockIoBackend.h"

namespace bytedance::bolt::memory::bm {

bool MockIoBackend::submit(uint64_t requestId, const IoRequest& request) {
  std::lock_guard<std::mutex> lock(mutex_);
  submitted_.push_back(MockSubmittedRequest{requestId, request});
  inflight_.insert(requestId);
  return true;
}

std::vector<BackendCompletion> MockIoBackend::reap() {
  std::lock_guard<std::mutex> lock(mutex_);
  auto completions = std::move(completions_);
  completions_.clear();
  return completions;
}

void MockIoBackend::complete(uint64_t requestId, IoResult result) {
  std::lock_guard<std::mutex> lock(mutex_);
  inflight_.erase(requestId);
  completions_.push_back(BackendCompletion{requestId, result});
}

std::vector<MockSubmittedRequest> MockIoBackend::submitted() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return submitted_;
}

size_t MockIoBackend::inflight() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return inflight_.size();
}

} // namespace bytedance::bolt::memory::bm
```

- [ ] **Step 4: Compile mock backend only into the isolated test target and verify**

Modify `bolt/common/memory/bm/tests/CMakeLists.txt`:

```cmake
add_executable(
  bolt_memory_bm_test
  ../MockIoBackend.cpp
  DiskIoSchedulerTest.cpp
)
```

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test --gtest_filter='MockIoBackendTest.*'
```

Expected: build exits 0; mock backend test passes.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/IoBackend.h bolt/common/memory/bm/MockIoBackend.h bolt/common/memory/bm/MockIoBackend.cpp bolt/common/memory/bm/tests/CMakeLists.txt bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp
git commit -m "feat: add disk io backend abstraction"
```

---

### Task 3: Scheduler Submit, Fixed Depth Dispatch, Completion, and Drain

**Files:**
- Create: `bolt/common/memory/bm/DiskIoScheduler.h`
- Create: `bolt/common/memory/bm/DiskIoScheduler.cpp`
- Modify: `bolt/common/memory/bm/CMakeLists.txt`
- Modify: `bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp`

- [ ] **Step 1: Write failing scheduler lifecycle tests**

Append to `DiskIoSchedulerTest.cpp`:

```cpp
#include "bolt/common/memory/bm/DiskIoScheduler.h"

#include <thread>

TEST(DiskIoSchedulerTest, invalidRequestReturnsCompletedErrorFuture) {
  auto backend = std::make_unique<MockIoBackend>();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  IoRequest request;
  request.fd = -1;
  auto result = scheduler.submit(request).get();

  EXPECT_EQ(EINVAL, result.errorCode);
  scheduler.stopAndDrain();
}

TEST(DiskIoSchedulerTest, submitsAndCompletesSingleRequest) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoSchedulerConfig config;
  config.adaptiveDepth.enabled = false;
  config.adaptiveDepth.initialDepth = 1;
  DiskIoScheduler scheduler(config, std::move(backend));

  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::High;
  request.fd = 10;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 0, 4096};

  auto future = scheduler.submit(request);
  while (backendPtr->submitted().empty()) {
    std::this_thread::yield();
  }
  backendPtr->complete(1, IoResult{4096, 0});

  auto result = future.get();
  EXPECT_EQ(0, result.errorCode);
  EXPECT_EQ(4096, result.bytes);
  scheduler.stopAndDrain();
}
```

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
```

Expected: FAIL because `DiskIoScheduler.h` does not exist.

- [ ] **Step 2: Implement scheduler facade and constructor injection for tests**

Create `bolt/common/memory/bm/DiskIoScheduler.h`:

```cpp
#pragma once

#include <array>
#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "bolt/common/memory/bm/DiskIoTypes.h"
#include "bolt/common/memory/bm/IoBackend.h"

namespace bytedance::bolt::memory::bm {

class DiskIoScheduler {
 public:
  explicit DiskIoScheduler(DiskIoSchedulerConfig config);
  DiskIoScheduler(DiskIoSchedulerConfig config, std::unique_ptr<IoBackend> backend);
  ~DiskIoScheduler();

  std::future<IoResult> submit(IoRequest request);
  void stopAndDrain();
  DiskIoSchedulerStats stats() const;

 private:
  struct PendingRequest {
    uint64_t requestId{0};
    IoRequest request;
    std::promise<IoResult> promise;
  };

  static std::future<IoResult> completedFuture(IoResult result);
  void run();
  bool dispatchOneLocked();
  void reapCompletionsLocked();
  bool hasQueuedLocked() const;
  bool drainedLocked() const;

  DiskIoSchedulerConfig config_;
  std::unique_ptr<IoBackend> backend_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::array<std::deque<PendingRequest>, kIoPriorityCount> queues_;
  std::unordered_map<uint64_t, PendingRequest> inflight_;
  DiskIoSchedulerStats stats_;
  std::thread schedulerThread_;
  uint64_t nextRequestId_{1};
  uint32_t currentDepth_{1};
  bool accepting_{true};
  bool stopping_{false};
};

} // namespace bytedance::bolt::memory::bm
```

- [ ] **Step 3: Implement minimal fixed-depth scheduler**

Create `bolt/common/memory/bm/DiskIoScheduler.cpp` with a first implementation that scans priorities in enum order:

```cpp
#include "bolt/common/memory/bm/DiskIoScheduler.h"

#include <cerrno>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::memory::bm {

DiskIoScheduler::DiskIoScheduler(DiskIoSchedulerConfig config)
{
  (void)config;
  BOLT_FAIL("DiskIoScheduler default constructor requires IoUringBackend");
}

DiskIoScheduler::DiskIoScheduler(
    DiskIoSchedulerConfig config,
    std::unique_ptr<IoBackend> backend)
    : config_(std::move(config)),
      backend_(std::move(backend)),
      currentDepth_(config_.adaptiveDepth.initialDepth) {
  BOLT_CHECK_EQ(validateDiskIoSchedulerConfig(config_), 0);
  BOLT_CHECK_NOT_NULL(backend_);
  stats_.currentDepth = currentDepth_;
  schedulerThread_ = std::thread([this] { run(); });
}

DiskIoScheduler::~DiskIoScheduler() {
  stopAndDrain();
}

std::future<IoResult> DiskIoScheduler::completedFuture(IoResult result) {
  std::promise<IoResult> promise;
  auto future = promise.get_future();
  promise.set_value(result);
  return future;
}

std::future<IoResult> DiskIoScheduler::submit(IoRequest request) {
  const auto validationError = validateIoRequest(request);
  if (validationError != 0) {
    return completedFuture(IoResult{0, validationError});
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (!accepting_) {
    return completedFuture(IoResult{0, ESHUTDOWN});
  }

  PendingRequest pending;
  pending.requestId = nextRequestId_++;
  pending.request = std::move(request);
  auto future = pending.promise.get_future();
  const auto index = priorityIndex(pending.request.priority);
  queues_[index].push_back(std::move(pending));
  stats_.queuedRequests[index] = queues_[index].size();
  cv_.notify_one();
  return future;
}

void DiskIoScheduler::run() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    reapCompletionsLocked();
    while (dispatchOneLocked()) {
      reapCompletionsLocked();
    }
    if (stopping_ && drainedLocked()) {
      return;
    }
    cv_.wait_for(lock, std::chrono::milliseconds(1));
  }
}

bool DiskIoScheduler::dispatchOneLocked() {
  if (inflight_.size() >= currentDepth_) {
    return false;
  }
  for (size_t index = 0; index < kIoPriorityCount; ++index) {
    if (queues_[index].empty()) {
      continue;
    }
    auto pending = std::move(queues_[index].front());
    queues_[index].pop_front();
    stats_.queuedRequests[index] = queues_[index].size();
    const auto requestId = pending.requestId;
    const auto priority = pending.request.priority;
    const bool submitted = backend_->submit(requestId, pending.request);
    if (!submitted) {
      pending.promise.set_value(IoResult{0, EIO});
      return true;
    }
    stats_.submittedRequests[priorityIndex(priority)]++;
    inflight_.emplace(requestId, std::move(pending));
    stats_.inflightRequests = inflight_.size();
    return true;
  }
  return false;
}

void DiskIoScheduler::reapCompletionsLocked() {
  for (auto& completion : backend_->reap()) {
    auto it = inflight_.find(completion.requestId);
    if (it == inflight_.end()) {
      continue;
    }
    const auto priority = it->second.request.priority;
    it->second.promise.set_value(completion.result);
    stats_.completedRequests++;
    stats_.completedRequestsByPriority[priorityIndex(priority)]++;
    stats_.completedBytes += completion.result.bytes;
    if (completion.result.ok()) {
      stats_.successfulRequests++;
    } else {
      stats_.failedRequests++;
    }
    inflight_.erase(it);
    stats_.inflightRequests = inflight_.size();
  }
}

bool DiskIoScheduler::hasQueuedLocked() const {
  for (const auto& queue : queues_) {
    if (!queue.empty()) {
      return true;
    }
  }
  return false;
}

bool DiskIoScheduler::drainedLocked() const {
  return !hasQueuedLocked() && inflight_.empty();
}

void DiskIoScheduler::stopAndDrain() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    accepting_ = false;
    stopping_ = true;
    cv_.notify_all();
  }
  if (schedulerThread_.joinable()) {
    schedulerThread_.join();
  }
}

DiskIoSchedulerStats DiskIoScheduler::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto snapshot = stats_;
  snapshot.currentDepth = currentDepth_;
  snapshot.inflightRequests = inflight_.size();
  for (size_t i = 0; i < kIoPriorityCount; ++i) {
    snapshot.queuedRequests[i] = queues_[i].size();
  }
  return snapshot;
}

} // namespace bytedance::bolt::memory::bm
```

- [ ] **Step 4: Convert isolated library target to compile scheduler source and verify**

Replace the interface target in `bolt/common/memory/bm/CMakeLists.txt` with a real isolated library:

```cmake
if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

bolt_add_library(
  bolt_memory_bm
  DiskIoScheduler.cpp
)

target_include_directories(bolt_memory_bm PUBLIC ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm
  PUBLIC bolt_common_base
         bolt_exception
         Folly::folly
         glog::glog
)
```

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test --gtest_filter='DiskIoSchedulerTest.invalidRequestReturnsCompletedErrorFuture:DiskIoSchedulerTest.submitsAndCompletesSingleRequest'
```

Expected: selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/CMakeLists.txt bolt/common/memory/bm/DiskIoScheduler.h bolt/common/memory/bm/DiskIoScheduler.cpp bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp
git commit -m "feat: add basic disk io scheduler"
```

---

### Task 4: Weighted Fair Priority Scheduling

**Files:**
- Modify: `bolt/common/memory/bm/DiskIoScheduler.h`
- Modify: `bolt/common/memory/bm/DiskIoScheduler.cpp`
- Modify: `bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp`

- [ ] **Step 1: Write failing weighted fairness test**

Append:

```cpp
TEST(DiskIoSchedulerTest, dispatchesUsingConfiguredWeights) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoSchedulerConfig config;
  config.adaptiveDepth.enabled = false;
  config.adaptiveDepth.initialDepth = 13;
  config.priorityWeights = {{3, 2, 1}};
  DiskIoScheduler scheduler(config, std::move(backend));

  auto makeRequest = [](IoPriority priority) {
    IoRequest request;
    request.opcode = IoOpcode::Read;
    request.priority = priority;
    request.fd = 10;
    request.buffer = IoBuffer{makeBuffer(4096), 4096, 0, 4096};
    return request;
  };

  std::vector<std::future<IoResult>> futures;
  for (int i = 0; i < 6; ++i) {
    futures.push_back(scheduler.submit(makeRequest(IoPriority::High)));
    futures.push_back(scheduler.submit(makeRequest(IoPriority::Medium)));
    futures.push_back(scheduler.submit(makeRequest(IoPriority::Low)));
  }

  while (backendPtr->submitted().size() < 6) {
    std::this_thread::yield();
  }

  auto submitted = backendPtr->submitted();
  std::array<int, kIoPriorityCount> counts{{0, 0, 0}};
  for (size_t i = 0; i < 6; ++i) {
    counts[priorityIndex(submitted[i].request.priority)]++;
  }
  EXPECT_EQ(3, counts[priorityIndex(IoPriority::High)]);
  EXPECT_EQ(2, counts[priorityIndex(IoPriority::Medium)]);
  EXPECT_EQ(1, counts[priorityIndex(IoPriority::Low)]);

  for (const auto& item : submitted) {
    backendPtr->complete(item.requestId, IoResult{4096, 0});
  }
  for (auto& future : futures) {
    future.get();
  }
  scheduler.stopAndDrain();
}
```

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test --gtest_filter='DiskIoSchedulerTest.dispatchesUsingConfiguredWeights'
```

Expected: FAIL because fixed priority scan submits all `High` requests first.

- [ ] **Step 2: Add DWRR state to scheduler**

Add to private members in `DiskIoScheduler.h`:

```cpp
  std::array<int64_t, kIoPriorityCount> deficits_{{0, 0, 0}};
  size_t nextPriorityCursor_{0};
```

- [ ] **Step 3: Replace fixed scan with DWRR pick**

Add private method declaration:

```cpp
  std::optional<size_t> pickQueueLocked();
```

Include `<optional>` in the header.

Implement in `DiskIoScheduler.cpp`:

```cpp
std::optional<size_t> DiskIoScheduler::pickQueueLocked() {
  for (size_t visited = 0; visited < kIoPriorityCount; ++visited) {
    const auto index = (nextPriorityCursor_ + visited) % kIoPriorityCount;
    if (!queues_[index].empty()) {
      deficits_[index] += config_.priorityWeights[index];
    }
  }

  for (size_t visited = 0; visited < kIoPriorityCount; ++visited) {
    const auto index = (nextPriorityCursor_ + visited) % kIoPriorityCount;
    if (!queues_[index].empty() && deficits_[index] > 0) {
      deficits_[index] -= 1;
      nextPriorityCursor_ = (index + 1) % kIoPriorityCount;
      return index;
    }
  }
  return std::nullopt;
}
```

Change `dispatchOneLocked()` to use `pickQueueLocked()`:

```cpp
bool DiskIoScheduler::dispatchOneLocked() {
  if (inflight_.size() >= currentDepth_) {
    return false;
  }
  auto index = pickQueueLocked();
  if (!index.has_value()) {
    return false;
  }
  auto pending = std::move(queues_[*index].front());
  queues_[*index].pop_front();
  stats_.queuedRequests[*index] = queues_[*index].size();
  const auto requestId = pending.requestId;
  const auto priority = pending.request.priority;
  const bool submitted = backend_->submit(requestId, pending.request);
  if (!submitted) {
    pending.promise.set_value(IoResult{0, EIO});
    return true;
  }
  stats_.submittedRequests[priorityIndex(priority)]++;
  inflight_.emplace(requestId, std::move(pending));
  stats_.inflightRequests = inflight_.size();
  return true;
}
```

- [ ] **Step 4: Verify weighted fairness**

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test --gtest_filter='DiskIoSchedulerTest.dispatchesUsingConfiguredWeights'
```

Expected: selected test passes.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/DiskIoScheduler.h bolt/common/memory/bm/DiskIoScheduler.cpp bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp
git commit -m "feat: add weighted disk io scheduling"
```

---

### Task 5: Adaptive Depth Controller

**Files:**
- Create: `bolt/common/memory/bm/AdaptiveDepthController.h`
- Create: `bolt/common/memory/bm/AdaptiveDepthController.cpp`
- Create: `bolt/common/memory/bm/tests/AdaptiveDepthControllerTest.cpp`
- Modify: `bolt/common/memory/bm/CMakeLists.txt`
- Modify: `bolt/common/memory/bm/tests/CMakeLists.txt`

- [ ] **Step 1: Write failing controller tests**

Create `bolt/common/memory/bm/tests/AdaptiveDepthControllerTest.cpp`:

```cpp
#include "bolt/common/memory/bm/AdaptiveDepthController.h"

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

TEST(AdaptiveDepthControllerTest, increasesDepthWhenThroughputImproves) {
  AdaptiveDepthConfig config;
  config.minDepth = 1;
  config.initialDepth = 4;
  config.maxDepth = 16;
  config.increaseStep = 4;
  config.minThroughputGain = 0.02;
  AdaptiveDepthController controller(config);

  EXPECT_EQ(4, controller.currentDepth());
  controller.onWindow(1000, true);
  EXPECT_EQ(8, controller.currentDepth());
  controller.onWindow(1100, true);
  EXPECT_EQ(12, controller.currentDepth());
}

TEST(AdaptiveDepthControllerTest, rollsBackWhenThroughputDoesNotImprove) {
  AdaptiveDepthConfig config;
  config.initialDepth = 8;
  config.maxDepth = 16;
  config.increaseStep = 4;
  config.minThroughputGain = 0.10;
  AdaptiveDepthController controller(config);

  controller.onWindow(1000, true);
  EXPECT_EQ(12, controller.currentDepth());
  controller.onWindow(1020, true);
  EXPECT_EQ(8, controller.currentDepth());
}
```

Add `AdaptiveDepthControllerTest.cpp` to `bolt_memory_bm_test`, run build, and expect missing header failure.

- [ ] **Step 2: Implement controller header**

Create `bolt/common/memory/bm/AdaptiveDepthController.h`:

```cpp
#pragma once

#include <cstdint>

#include "bolt/common/memory/bm/DiskIoTypes.h"

namespace bytedance::bolt::memory::bm {

class AdaptiveDepthController {
 public:
  explicit AdaptiveDepthController(AdaptiveDepthConfig config);

  uint32_t currentDepth() const;
  double recentThroughputBytesPerSecond() const;

  void onWindow(double throughputBytesPerSecond, bool hasBacklog);

 private:
  AdaptiveDepthConfig config_;
  uint32_t currentDepth_{1};
  uint32_t bestDepth_{1};
  double bestThroughput_{0};
  double recentThroughput_{0};
};

} // namespace bytedance::bolt::memory::bm
```

- [ ] **Step 3: Implement throughput hill-climbing**

Create `bolt/common/memory/bm/AdaptiveDepthController.cpp`:

```cpp
#include "bolt/common/memory/bm/AdaptiveDepthController.h"

#include <algorithm>

namespace bytedance::bolt::memory::bm {

AdaptiveDepthController::AdaptiveDepthController(AdaptiveDepthConfig config)
    : config_(config),
      currentDepth_(config.initialDepth),
      bestDepth_(config.initialDepth) {}

uint32_t AdaptiveDepthController::currentDepth() const {
  return currentDepth_;
}

double AdaptiveDepthController::recentThroughputBytesPerSecond() const {
  return recentThroughput_;
}

void AdaptiveDepthController::onWindow(
    double throughputBytesPerSecond,
    bool hasBacklog) {
  recentThroughput_ = throughputBytesPerSecond;
  if (!config_.enabled || !hasBacklog) {
    return;
  }

  const auto required = bestThroughput_ * (1.0 + config_.minThroughputGain);
  if (bestThroughput_ == 0 || throughputBytesPerSecond >= required) {
    bestThroughput_ = throughputBytesPerSecond;
    bestDepth_ = currentDepth_;
    currentDepth_ =
        std::min<uint32_t>(config_.maxDepth, currentDepth_ + config_.increaseStep);
    return;
  }

  currentDepth_ = bestDepth_;
}

} // namespace bytedance::bolt::memory::bm
```

- [ ] **Step 4: Add source/test to CMake and verify**

Modify `bolt/common/memory/bm/CMakeLists.txt`:

```cmake
  AdaptiveDepthController.cpp
```

Modify `bolt/common/memory/bm/tests/CMakeLists.txt`:

```cmake
  AdaptiveDepthControllerTest.cpp
```

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test --gtest_filter='AdaptiveDepthControllerTest.*'
```

Expected: controller tests pass.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/CMakeLists.txt bolt/common/memory/bm/AdaptiveDepthController.h bolt/common/memory/bm/AdaptiveDepthController.cpp bolt/common/memory/bm/tests/AdaptiveDepthControllerTest.cpp bolt/common/memory/bm/tests/CMakeLists.txt
git commit -m "feat: add adaptive disk io depth controller"
```

---

### Task 6: Wire Adaptive Depth and Stats into Scheduler

**Files:**
- Modify: `bolt/common/memory/bm/DiskIoScheduler.h`
- Modify: `bolt/common/memory/bm/DiskIoScheduler.cpp`
- Modify: `bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp`

- [ ] **Step 1: Write failing scheduler stats and adaptive depth tests**

Append:

```cpp
TEST(DiskIoSchedulerTest, statsTrackCompletionAndDepth) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoSchedulerConfig config;
  config.adaptiveDepth.enabled = false;
  config.adaptiveDepth.initialDepth = 2;
  DiskIoScheduler scheduler(config, std::move(backend));

  IoRequest request;
  request.fd = 10;
  request.priority = IoPriority::Medium;
  request.buffer = IoBuffer{makeBuffer(1024), 1024, 0, 1024};

  auto future = scheduler.submit(request);
  while (backendPtr->submitted().empty()) {
    std::this_thread::yield();
  }
  backendPtr->complete(1, IoResult{1024, 0});
  EXPECT_EQ(0, future.get().errorCode);

  auto stats = scheduler.stats();
  EXPECT_EQ(2, stats.currentDepth);
  EXPECT_EQ(1, stats.completedRequests);
  EXPECT_EQ(1024, stats.completedBytes);
  EXPECT_EQ(1, stats.successfulRequests);
  scheduler.stopAndDrain();
}
```

- [ ] **Step 2: Add controller member and window accounting**

Add to `DiskIoScheduler.h`:

```cpp
#include "bolt/common/memory/bm/AdaptiveDepthController.h"

  AdaptiveDepthController adaptiveDepth_;
  std::chrono::steady_clock::time_point windowStart_;
  uint64_t windowCompletedBytes_{0};
```

Initialize in constructor:

```cpp
      adaptiveDepth_(config_.adaptiveDepth),
      windowStart_(std::chrono::steady_clock::now()) {
  currentDepth_ = adaptiveDepth_.currentDepth();
```

- [ ] **Step 3: Update stats and depth on completion windows**

Add private method:

```cpp
  void updateAdaptiveDepthLocked(std::chrono::steady_clock::time_point now);
```

Implement:

```cpp
void DiskIoScheduler::updateAdaptiveDepthLocked(
    std::chrono::steady_clock::time_point now) {
  const auto elapsed = now - windowStart_;
  if (elapsed < config_.adaptiveDepth.controlInterval) {
    return;
  }
  const auto seconds =
      std::chrono::duration<double>(elapsed).count();
  const auto throughput = seconds > 0 ? windowCompletedBytes_ / seconds : 0;
  adaptiveDepth_.onWindow(throughput, hasQueuedLocked());
  currentDepth_ = adaptiveDepth_.currentDepth();
  stats_.currentDepth = currentDepth_;
  stats_.recentThroughputBytesPerSecond =
      adaptiveDepth_.recentThroughputBytesPerSecond();
  windowCompletedBytes_ = 0;
  windowStart_ = now;
}
```

In `reapCompletionsLocked()`, after `stats_.completedBytes += completion.result.bytes;` add:

```cpp
    windowCompletedBytes_ += completion.result.bytes;
    updateAdaptiveDepthLocked(std::chrono::steady_clock::now());
```

- [ ] **Step 4: Verify scheduler stats**

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test --gtest_filter='DiskIoSchedulerTest.statsTrackCompletionAndDepth'
```

Expected: selected test passes.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/DiskIoScheduler.h bolt/common/memory/bm/DiskIoScheduler.cpp bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp
git commit -m "feat: track disk io scheduler stats"
```

---

### Task 7: io_uring Backend and Integration Tests

**Files:**
- Create: `bolt/common/memory/bm/IoUringBackend.h`
- Create: `bolt/common/memory/bm/IoUringBackend.cpp`
- Create: `bolt/common/memory/bm/tests/IoUringBackendTest.cpp`
- Modify: `bolt/common/memory/bm/CMakeLists.txt`
- Modify: `bolt/common/memory/bm/tests/CMakeLists.txt`

- [ ] **Step 1: Write conditional integration tests**

Create `bolt/common/memory/bm/tests/IoUringBackendTest.cpp`:

```cpp
#include "bolt/common/memory/bm/DiskIoScheduler.h"
#include "bolt/common/memory/bm/IoUringBackend.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <string>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

#ifdef IO_URING_SUPPORTED
namespace {

std::shared_ptr<void> makeBuffer(size_t size) {
  return std::shared_ptr<void>(
      new char[size], [](void* ptr) { delete[] static_cast<char*>(ptr); });
}

} // namespace

TEST(IoUringBackendTest, writeAndReadTemporaryFile) {
  char path[] = "/tmp/bolt_disk_io_test_XXXXXX";
  int fd = mkstemp(path);
  ASSERT_GE(fd, 0);

  DiskIoSchedulerConfig config;
  config.ringDepth = 32;
  config.adaptiveDepth.enabled = false;
  config.adaptiveDepth.initialDepth = 4;
  DiskIoScheduler scheduler(config, std::make_unique<IoUringBackend>(config.ringDepth));

  auto writeBuffer = makeBuffer(4096);
  std::memset(writeBuffer.get(), 'x', 4096);
  IoRequest write;
  write.opcode = IoOpcode::Write;
  write.priority = IoPriority::High;
  write.fd = fd;
  write.fileOffset = 0;
  write.buffer = IoBuffer{writeBuffer, 4096, 0, 4096};
  EXPECT_EQ(0, scheduler.submit(write).get().errorCode);

  auto readBuffer = makeBuffer(4096);
  IoRequest read;
  read.opcode = IoOpcode::Read;
  read.priority = IoPriority::High;
  read.fd = fd;
  read.fileOffset = 0;
  read.buffer = IoBuffer{readBuffer, 4096, 0, 4096};
  auto result = scheduler.submit(read).get();
  EXPECT_EQ(0, result.errorCode);
  EXPECT_EQ(4096, result.bytes);
  EXPECT_EQ(0, std::memcmp(writeBuffer.get(), readBuffer.get(), 4096));

  scheduler.stopAndDrain();
  close(fd);
  unlink(path);
}
#endif
```

- [ ] **Step 2: Implement io_uring backend declarations**

Create `bolt/common/memory/bm/IoUringBackend.h`:

```cpp
#pragma once

#include "bolt/common/memory/bm/IoBackend.h"

#ifdef IO_URING_SUPPORTED
struct io_uring;
#endif

namespace bytedance::bolt::memory::bm {

class IoUringBackend : public IoBackend {
 public:
  explicit IoUringBackend(uint32_t ringDepth);
  ~IoUringBackend() override;

  bool submit(uint64_t requestId, const IoRequest& request) override;
  std::vector<BackendCompletion> reap() override;

 private:
#ifdef IO_URING_SUPPORTED
  std::unique_ptr<io_uring> ring_;
#endif
};

} // namespace bytedance::bolt::memory::bm
```

- [ ] **Step 3: Implement io_uring backend**

Create `bolt/common/memory/bm/IoUringBackend.cpp`:

```cpp
#include "bolt/common/memory/bm/IoUringBackend.h"

#ifdef IO_URING_SUPPORTED
#include <liburing.h>
#include <liburing/io_uring.h>
#endif

#include <cerrno>
#include <cstring>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::memory::bm {

IoUringBackend::IoUringBackend(uint32_t ringDepth) {
#ifdef IO_URING_SUPPORTED
  ring_ = std::make_unique<io_uring>();
  const int ret = io_uring_queue_init(ringDepth, ring_.get(), 0);
  BOLT_CHECK_GE(ret, 0, "io_uring_queue_init failed: {}", std::strerror(-ret));
#else
  (void)ringDepth;
  BOLT_FAIL("IoUringBackend requires IO_URING_SUPPORTED");
#endif
}

IoUringBackend::~IoUringBackend() {
#ifdef IO_URING_SUPPORTED
  if (ring_) {
    io_uring_queue_exit(ring_.get());
  }
#endif
}

bool IoUringBackend::submit(uint64_t requestId, const IoRequest& request) {
#ifdef IO_URING_SUPPORTED
  auto* sqe = io_uring_get_sqe(ring_.get());
  if (sqe == nullptr) {
    return false;
  }
  auto* base = static_cast<char*>(request.buffer.data.get()) + request.buffer.offset;
  if (request.opcode == IoOpcode::Read) {
    io_uring_prep_read(
        sqe, request.fd, base, request.buffer.length, request.fileOffset);
  } else {
    io_uring_prep_write(
        sqe, request.fd, base, request.buffer.length, request.fileOffset);
  }
  sqe->user_data = requestId;
  const int ret = io_uring_submit(ring_.get());
  return ret >= 0;
#else
  (void)requestId;
  (void)request;
  return false;
#endif
}

std::vector<BackendCompletion> IoUringBackend::reap() {
  std::vector<BackendCompletion> completions;
#ifdef IO_URING_SUPPORTED
  io_uring_cqe* cqe = nullptr;
  while (io_uring_peek_cqe(ring_.get(), &cqe) == 0 && cqe != nullptr) {
    IoResult result;
    if (cqe->res >= 0) {
      result.bytes = static_cast<uint64_t>(cqe->res);
      result.errorCode = 0;
    } else {
      result.bytes = 0;
      result.errorCode = -cqe->res;
    }
    completions.push_back(BackendCompletion{cqe->user_data, result});
    io_uring_cqe_seen(ring_.get(), cqe);
    cqe = nullptr;
  }
#endif
  return completions;
}

} // namespace bytedance::bolt::memory::bm
```

- [ ] **Step 4: Add conditional CMake wiring**

Modify `bolt/common/memory/bm/CMakeLists.txt` so the isolated library owns io_uring dependencies:

```cmake
if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

bolt_add_library(
  bolt_memory_bm
  AdaptiveDepthController.cpp
  DiskIoScheduler.cpp
  IoUringBackend.cpp
)

target_include_directories(bolt_memory_bm PUBLIC ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm
  PUBLIC bolt_common_base
         bolt_exception
         Folly::folly
         glog::glog
)

if(KERNEL_SUPPORTS_IO_URING)
  find_package(liburing REQUIRED)
  target_link_libraries(bolt_memory_bm PUBLIC liburing::liburing)
endif()
```

Modify the default constructor in `bolt/common/memory/bm/DiskIoScheduler.cpp` so production construction uses `IoUringBackend` when available:

```cpp
#include "bolt/common/memory/bm/IoUringBackend.h"

DiskIoScheduler::DiskIoScheduler(DiskIoSchedulerConfig config)
#ifdef IO_URING_SUPPORTED
    : DiskIoScheduler(config, std::make_unique<IoUringBackend>(config.ringDepth)) {}
#else
{
  (void)config;
  BOLT_FAIL("DiskIoScheduler default constructor requires IO_URING_SUPPORTED");
}
#endif
```

Modify `bolt/common/memory/bm/tests/CMakeLists.txt`:

```cmake
  IoUringBackendTest.cpp
```

Run:

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test --gtest_filter='IoUringBackendTest.*'
```

Expected: if `IO_URING_SUPPORTED` is enabled, integration test passes; otherwise gtest reports 0 matching tests or compiles with the test body excluded.

- [ ] **Step 5: Commit**

```bash
git add bolt/common/memory/bm/CMakeLists.txt bolt/common/memory/bm/DiskIoScheduler.cpp bolt/common/memory/bm/IoUringBackend.h bolt/common/memory/bm/IoUringBackend.cpp bolt/common/memory/bm/tests/IoUringBackendTest.cpp bolt/common/memory/bm/tests/CMakeLists.txt
git commit -m "feat: add io_uring disk io backend"
```

---

### Task 8: Final Verification and Cleanup

**Files:**
- Modify only files that fail formatting, compile, or tests from prior tasks.

- [ ] **Step 1: Run focused tests**

```bash
cmake --build _build/Release --target bolt_memory_bm_test -j 8
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test --gtest_filter='*DiskIo*:*AdaptiveDepth*:*IoUringBackend*'
```

Expected: all matching tests pass.

- [ ] **Step 2: Run full memory test binary**

```bash
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test
```

Expected: existing memory tests and new Disk IO tests pass.

- [ ] **Step 3: Run release verification**

```bash
PATH=/data00/home/wangxinshuo.db/tools/miniconda3/bin:$PATH make release_with_test
```

Expected: command exits 0.

- [ ] **Step 4: Inspect final diff**

```bash
git status --short
git diff --stat HEAD
```

Expected: only intended Disk IO implementation, tests, and CMake files are modified.

- [ ] **Step 5: Commit final cleanup if any files changed**

```bash
git add bolt/common/memory/bm bolt/common/memory/CMakeLists.txt bolt/common/memory/bm/tests/CMakeLists.txt bolt/common/memory/bm/tests/DiskIoSchedulerTest.cpp bolt/common/memory/bm/tests/AdaptiveDepthControllerTest.cpp bolt/common/memory/bm/tests/IoUringBackendTest.cpp
git commit -m "chore: verify disk io scheduler"
```

If Step 4 shows no uncommitted changes, skip this commit.
