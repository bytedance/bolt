#include "bolt/common/memory/bm/io/DiskIoScheduler.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfigValidator.h"
#include "bolt/common/memory/bm/io/EventFd.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoRequestValidator.h"
#include "bolt/common/memory/bm/io/IoResult.h"
#include "bolt/common/memory/bm/io/tests/MockIoBackend.h"

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/tests/GTestUtils.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <vector>

#include <dirent.h>
#include <gtest/gtest.h>
#include <poll.h>

using namespace bytedance::bolt::memory::bm;

static_assert(!std::is_copy_constructible_v<IoRequest>);
static_assert(std::is_move_constructible_v<IoRequest>);

namespace {

std::unique_ptr<char[]> makeBuffer(size_t size) {
  return std::make_unique<char[]>(size);
}

constexpr auto kFutureTimeout = std::chrono::seconds(5);

IoRequest makeValidRequest(IoPriority priority = IoPriority::Medium) {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = priority;
  request.fd = 7;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 0, 4096};
  return request;
}

bool waitUntilSubmitted(MockIoBackend& backend, size_t expected) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    if (backend.submitted().size() >= expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return false;
}

bool waitUntilReady(std::future<IoResult>& future) {
  return future.wait_for(kFutureTimeout) == std::future_status::ready;
}

size_t countOpenFds() {
  DIR* dir = ::opendir("/proc/self/fd");
  BOLT_CHECK(dir != nullptr, "failed to open /proc/self/fd");

  size_t count = 0;
  while (::readdir(dir) != nullptr) {
    ++count;
  }
  ::closedir(dir);
  return count;
}

bool waitUntilCurrentDepth(DiskIoScheduler& scheduler, uint32_t expected) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    if (scheduler.stats().currentDepth >= expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return false;
}

void submitBalancedRequests(
    DiskIoScheduler& scheduler,
    std::vector<std::future<IoResult>>& futures,
    size_t requestsPerPriority) {
  for (size_t i = 0; i < requestsPerPriority; ++i) {
    futures.push_back(scheduler.submit(makeValidRequest(IoPriority::High)));
    futures.push_back(scheduler.submit(makeValidRequest(IoPriority::Medium)));
    futures.push_back(scheduler.submit(makeValidRequest(IoPriority::Low)));
  }
}

bool completeSubmission(MockIoBackend& backend, size_t submittedIndex) {
  const auto submitted = backend.submitted();
  if (submittedIndex >= submitted.size()) {
    return false;
  }
  backend.complete(submitted[submittedIndex].requestId, IoResult{4096});
  return true;
}

bool completeNextSubmittedWindow(
    MockIoBackend& backend,
    size_t& completedSubmissions,
    size_t windowSize,
    std::array<size_t, kIoPriorityCount>& submittedByPriority) {
  for (size_t i = 0; i < windowSize; ++i) {
    if (!waitUntilSubmitted(backend, completedSubmissions + 1)) {
      return false;
    }
    const auto submitted = backend.submitted();
    if (completedSubmissions >= submitted.size()) {
      return false;
    }
    const auto& request = submitted[completedSubmissions];
    ++submittedByPriority[priorityIndex(request.priority)];
    backend.complete(request.requestId, IoResult{4096});
    ++completedSubmissions;
  }
  return true;
}

void completeAllSubmittedRequests(
    MockIoBackend& backend,
    size_t& completedSubmissions,
    size_t expectedSubmissions) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (completedSubmissions < expectedSubmissions &&
         std::chrono::steady_clock::now() < deadline) {
    const auto submitted = backend.submitted();
    for (size_t i = completedSubmissions; i < submitted.size(); ++i) {
      backend.complete(submitted[i].requestId, IoResult{4096});
    }
    completedSubmissions = submitted.size();
    if (completedSubmissions < expectedSubmissions) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  EXPECT_EQ(expectedSubmissions, completedSubmissions);
}

void expectWeightedWindow(
    const std::array<size_t, kIoPriorityCount>& submittedByPriority) {
  EXPECT_EQ(3, submittedByPriority[priorityIndex(IoPriority::High)]);
  EXPECT_EQ(2, submittedByPriority[priorityIndex(IoPriority::Medium)]);
  EXPECT_EQ(1, submittedByPriority[priorityIndex(IoPriority::Low)]);
}

class FailingSubmitBackend : public IoBackend {
 public:
  int completionFd() const override {
    return completionEvent_.fd();
  }

  BackendSubmitStatus submit(
      uint64_t /*requestId*/,
      const IoRequest& /*request*/) override {
    ++submitAttempts_;
    return BackendSubmitStatus::Failed;
  }

  std::vector<BackendCompletion> reap() override {
    return {};
  }

  uint64_t submitAttempts() const {
    return submitAttempts_.load();
  }

 private:
  std::atomic<uint64_t> submitAttempts_{0};
  EventFd completionEvent_;
};

class BlockingReapBackend : public IoBackend {
 public:
  int completionFd() const override {
    return completionEvent_.fd();
  }

  BackendSubmitStatus submit(
      uint64_t /*requestId*/,
      const IoRequest& /*request*/) override {
    submitAttempts_.fetch_add(1);
    return BackendSubmitStatus::Failed;
  }

  std::vector<BackendCompletion> reap() override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!enteredReap_) {
      enteredReap_ = true;
      cv_.notify_all();
      cv_.wait(lock, [this] { return allowReapReturn_; });
    }
    return {};
  }

  bool waitUntilEnteredReap() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, kFutureTimeout, [this] { return enteredReap_; });
  }

  void unblockReap() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      allowReapReturn_ = true;
    }
    cv_.notify_all();
  }

  uint64_t submitAttempts() const {
    return submitAttempts_.load();
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool enteredReap_{false};
  bool allowReapReturn_{false};
  std::atomic<uint64_t> submitAttempts_{0};
  EventFd completionEvent_;
};

class BusyOnceBackend : public IoBackend {
 public:
  int completionFd() const override {
    return completionEvent_.fd();
  }

  BackendSubmitStatus submit(
      uint64_t requestId,
      const IoRequest& request) override {
    submittedRequestId_ = requestId;
    priority_ = request.priority;
    ++submitAttempts_;
    if (submitAttempts_.load() == 1) {
      return BackendSubmitStatus::RetryableBusy;
    }
    submitted_ = true;
    return BackendSubmitStatus::Submitted;
  }

  std::vector<BackendCompletion> reap() override {
    completionEvent_.drain();
    if (!submitted_ || completed_) {
      return {};
    }
    completed_ = true;
    std::vector<BackendCompletion> completions;
    completions.push_back(
        BackendCompletion{submittedRequestId_, IoResult{4096}});
    return completions;
  }

  uint64_t submitAttempts() const {
    return submitAttempts_.load();
  }

  IoPriority priority() const {
    return priority_;
  }

  void wake() {
    completionEvent_.notify();
  }

 private:
  std::atomic<uint64_t> submitAttempts_{0};
  uint64_t submittedRequestId_{0};
  IoPriority priority_{IoPriority::Medium};
  bool submitted_{false};
  bool completed_{false};
  EventFd completionEvent_;
};

class AlwaysBusyBackend : public IoBackend {
 public:
  int completionFd() const override {
    return completionEvent_.fd();
  }

  BackendSubmitStatus submit(
      uint64_t /*requestId*/,
      const IoRequest& /*request*/) override {
    ++submitAttempts_;
    return BackendSubmitStatus::RetryableBusy;
  }

  std::vector<BackendCompletion> reap() override {
    completionEvent_.drain();
    return {};
  }

  uint64_t submitAttempts() const {
    return submitAttempts_.load();
  }

 private:
  std::atomic<uint64_t> submitAttempts_{0};
  EventFd completionEvent_;
};

class NeverCompleteBackend : public IoBackend {
 public:
  int completionFd() const override {
    return completionEvent_.fd();
  }

  BackendSubmitStatus submit(
      uint64_t /*requestId*/,
      const IoRequest& /*request*/) override {
    ++submitAttempts_;
    return BackendSubmitStatus::Submitted;
  }

  std::vector<BackendCompletion> reap() override {
    return {};
  }

  uint64_t submitAttempts() const {
    return submitAttempts_.load();
  }

 private:
  std::atomic<uint64_t> submitAttempts_{0};
  EventFd completionEvent_;
};

} // namespace

TEST(IoRequestValidatorTest, validateRequestAcceptsValidRead) {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::High;
  request.fd = 3;
  request.fileOffset = 128;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 64, 1024};

  EXPECT_EQ(IoErrorCode::Ok, validateIoRequest(request));
}

TEST(IoPriorityTest, priorityCountTracksPriorityEnumSentinel) {
  EXPECT_EQ(
      static_cast<size_t>(IoPriority::Count),
      kIoPriorityCount);
}

TEST(IoRequestValidatorTest, validateRequestRejectsBadBufferRange) {
  IoRequest request;
  request.opcode = IoOpcode::Write;
  request.priority = IoPriority::Low;
  request.fd = 3;
  request.fileOffset = 0;
  request.buffer = IoBuffer{makeBuffer(128), 128, 64, 128};

  EXPECT_EQ(IoErrorCode::InvalidRequest, validateIoRequest(request));
}

TEST(IoRequestValidatorTest, validateRequestRejectsOffsetAtEndWithLength) {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::Medium;
  request.fd = 3;
  request.buffer = IoBuffer{makeBuffer(128), 128, 128, 1};

  EXPECT_EQ(IoErrorCode::InvalidRequest, validateIoRequest(request));
}

TEST(DiskIoSchedulerConfigValidatorTest, validateConfigRejectsInvalidDepth) {
  DiskIoSchedulerConfig config;
  config.ringDepth = 16;
  config.adaptiveDepth.minDepth = 1;
  config.adaptiveDepth.initialDepth = 32;
  config.adaptiveDepth.maxDepth = 32;

  EXPECT_EQ(
      IoErrorCode::InvalidRequest,
      validateDiskIoSchedulerConfig(config));
}

TEST(DiskIoSchedulerConfigTest, defaultConfigUsesFixedDepth) {
  DiskIoSchedulerConfig config;

  EXPECT_FALSE(config.adaptiveDepth.enabled);
  EXPECT_EQ(config.adaptiveDepth.initialDepth, config.adaptiveDepth.maxDepth);
}

TEST(DiskIoSchedulerTest, constructorClosesEpollFdWhenValidationThrows) {
  const auto openFdsBefore = countOpenFds();

  DiskIoSchedulerConfig config;
  config.ringDepth = 16;
  config.adaptiveDepth.initialDepth = 32;
  config.adaptiveDepth.maxDepth = 32;

  BOLT_ASSERT_THROW(
      [&] {
        auto backend = std::make_unique<MockIoBackend>();
        DiskIoScheduler scheduler(config, std::move(backend));
      }(),
      "invalid DiskIoSchedulerConfig");

  EXPECT_EQ(openFdsBefore, countOpenFds());
}

TEST(MockIoBackendTest, recordsSubmittedRequestsAndCompletesInChosenOrder) {
  MockIoBackend backend;
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::Medium;
  request.fd = 7;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 0, 4096};

  EXPECT_EQ(BackendSubmitStatus::Submitted, backend.submit(11, request));
  EXPECT_EQ(BackendSubmitStatus::Submitted, backend.submit(12, request));
  auto submitted = backend.submitted();
  ASSERT_EQ(2, submitted.size());
  EXPECT_EQ(11, submitted[0].requestId);
  EXPECT_EQ(12, submitted[1].requestId);

  backend.complete(12, IoResult{4096});
  auto completions = backend.reap();
  ASSERT_EQ(1, completions.size());
  EXPECT_EQ(12, completions[0].requestId);
  EXPECT_EQ(4096, completions[0].result.bytes);
}

TEST(MockIoBackendTest, duplicateSubmitReturnsFalseAndDoesNotRecord) {
  MockIoBackend backend;
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::Medium;
  request.fd = 7;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 0, 4096};

  EXPECT_EQ(BackendSubmitStatus::Submitted, backend.submit(11, request));
  EXPECT_EQ(BackendSubmitStatus::Failed, backend.submit(11, request));

  auto submitted = backend.submitted();
  ASSERT_EQ(1, submitted.size());
  EXPECT_EQ(11, submitted[0].requestId);
  EXPECT_EQ(1, backend.inflight());
}

TEST(MockIoBackendTest, completeUnknownRequestFailsFast) {
  MockIoBackend backend;

  BOLT_ASSERT_THROW(
      backend.complete(11, IoResult{4096}), "unknown requestId");
}

TEST(MockIoBackendTest, completionFdSignalsCompletedRequests) {
  MockIoBackend backend;
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::Medium;
  request.fd = 7;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 0, 4096};

  EXPECT_EQ(BackendSubmitStatus::Submitted, backend.submit(11, request));

  pollfd completionEvent{backend.completionFd(), POLLIN, 0};
  EXPECT_EQ(0, ::poll(&completionEvent, 1, 0));

  backend.complete(11, IoResult{4096});
  ASSERT_EQ(1, ::poll(&completionEvent, 1, 100));
  EXPECT_NE(0, completionEvent.revents & POLLIN);

  auto completions = backend.reap();
  ASSERT_EQ(1, completions.size());
  EXPECT_EQ(11, completions[0].requestId);
  completionEvent.revents = 0;
  EXPECT_EQ(0, ::poll(&completionEvent, 1, 0));
}

TEST(DiskIoSchedulerTest, facadeConstructorUsesDefaultBackendWhenSupported) {
  DiskIoSchedulerConfig config;

#ifdef IO_URING_SUPPORTED
  try {
    DiskIoScheduler scheduler(config);
    scheduler.stopAndDrain();
  } catch (const std::exception& ex) {
    EXPECT_NE(
        std::string(ex.what()).find("Operation not permitted"),
        std::string::npos)
        << ex.what();
  }
#else
  BOLT_ASSERT_THROW(
      [&] { DiskIoScheduler scheduler(config); }(),
      "IoUringBackend requires IO_URING_SUPPORTED");
#endif
}

TEST(DiskIoSchedulerTest, invalidRequestReturnsCompletedErrorFuture) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  IoRequest request = makeValidRequest();
  request.fd = -1;

  auto future = scheduler.submit(std::move(request));
  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();

  EXPECT_EQ(0, result.bytes);
  EXPECT_EQ(IoErrorCode::InvalidRequest, result.error);
  EXPECT_EQ(0, result.nativeErrorCode);
  EXPECT_TRUE(backendPtr->submitted().empty());

  const auto stats = scheduler.stats();
  EXPECT_EQ(1, stats.rejectedRequests);
  EXPECT_EQ(0, stats.acceptedRequests);
  EXPECT_EQ(0, stats.completedRequests);
  EXPECT_EQ(0, stats.failedRequests);
  EXPECT_EQ(0, stats.latencySamples);
  EXPECT_EQ(0, stats.failedRequestsByPriority[priorityIndex(IoPriority::Medium)]);
  EXPECT_NE(
      std::string::npos,
      stats.toString().find("rejected_requests=1"));
}

TEST(DiskIoSchedulerTest, submitsAndCompletesSingleRequest) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  auto future = scheduler.submit(makeValidRequest());
  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 1));

  const auto submitted = backendPtr->submitted();
  ASSERT_EQ(1, submitted.size());
  EXPECT_EQ(1, submitted[0].requestId);

  backendPtr->complete(1, IoResult{4096});
  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();

  EXPECT_EQ(4096, result.bytes);
  EXPECT_EQ(IoErrorCode::Ok, result.error);
}

TEST(DiskIoSchedulerTest, fixedDepthOneKeepsSecondRequestQueued) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoSchedulerConfig config;
  config.adaptiveDepth.initialDepth = 1;
  DiskIoScheduler scheduler(config, std::move(backend));

  auto first = scheduler.submit(makeValidRequest());
  auto second = scheduler.submit(makeValidRequest());

  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  EXPECT_EQ(1, backendPtr->submitted().size());
  auto stats = scheduler.stats();
  EXPECT_EQ(1, stats.inflightRequests);
  EXPECT_EQ(1, stats.queuedRequests[priorityIndex(IoPriority::Medium)]);

  backendPtr->complete(1, IoResult{4096});
  ASSERT_TRUE(waitUntilReady(first));
  EXPECT_EQ(IoErrorCode::Ok, first.get().error);

  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 2));
  backendPtr->complete(2, IoResult{4096});
  ASSERT_TRUE(waitUntilReady(second));
  EXPECT_EQ(IoErrorCode::Ok, second.get().error);
}

TEST(DiskIoSchedulerTest, submitDoesNotWaitForBackendReap) {
  auto backend = std::make_unique<BlockingReapBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));
  ASSERT_TRUE(backendPtr->waitUntilEnteredReap());

  auto submitFuture = std::async(std::launch::async, [&scheduler] {
    return scheduler.submit(makeValidRequest());
  });

  const auto submitStatus = submitFuture.wait_for(std::chrono::milliseconds(50));

  backendPtr->unblockReap();
  ASSERT_EQ(std::future_status::ready, submitStatus);
  auto ioFuture = submitFuture.get();

  ASSERT_TRUE(waitUntilReady(ioFuture));
  EXPECT_EQ(1, backendPtr->submitAttempts());
  EXPECT_EQ(IoErrorCode::BackendSubmitFailed, ioFuture.get().error);
}

TEST(DiskIoSchedulerTest, dispatchesUsingConfiguredWeights) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoSchedulerConfig config;
  config.adaptiveDepth.initialDepth = 1;
  config.priorityWeights = {{3, 2, 1}};
  DiskIoScheduler scheduler(config, std::move(backend));

  std::vector<std::future<IoResult>> futures;
  futures.reserve(19);
  futures.push_back(scheduler.submit(makeValidRequest(IoPriority::Low)));
  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 1));

  submitBalancedRequests(scheduler, futures, 6);

  size_t completedSubmissions = 0;
  ASSERT_TRUE(completeSubmission(*backendPtr, completedSubmissions++));
  std::array<size_t, kIoPriorityCount> firstWeightedWindow{{0, 0, 0}};
  ASSERT_TRUE(completeNextSubmittedWindow(
      *backendPtr, completedSubmissions, 6, firstWeightedWindow));
  expectWeightedWindow(firstWeightedWindow);
  completeAllSubmittedRequests(
      *backendPtr, completedSubmissions, futures.size());

  for (auto& future : futures) {
    ASSERT_TRUE(waitUntilReady(future));
    EXPECT_EQ(IoErrorCode::Ok, future.get().error);
  }
}

TEST(DiskIoSchedulerTest, resetsDeficitWhenPriorityQueueDrains) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoSchedulerConfig config;
  config.adaptiveDepth.initialDepth = 1;
  config.priorityWeights = {{3, 2, 1}};
  DiskIoScheduler scheduler(config, std::move(backend));

  std::vector<std::future<IoResult>> futures;
  futures.reserve(20);
  futures.push_back(scheduler.submit(makeValidRequest(IoPriority::Low)));
  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 1));

  futures.push_back(scheduler.submit(makeValidRequest(IoPriority::Medium)));
  size_t completedSubmissions = 0;
  ASSERT_TRUE(completeSubmission(*backendPtr, completedSubmissions++));
  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 2));

  submitBalancedRequests(scheduler, futures, 6);

  ASSERT_TRUE(completeSubmission(*backendPtr, completedSubmissions++));
  std::array<size_t, kIoPriorityCount> firstWeightedWindow{{0, 0, 0}};
  ASSERT_TRUE(completeNextSubmittedWindow(
      *backendPtr, completedSubmissions, 6, firstWeightedWindow));
  expectWeightedWindow(firstWeightedWindow);
  completeAllSubmittedRequests(
      *backendPtr, completedSubmissions, futures.size());

  for (auto& future : futures) {
    ASSERT_TRUE(waitUntilReady(future));
    EXPECT_EQ(IoErrorCode::Ok, future.get().error);
  }
}

TEST(DiskIoSchedulerTest, submitAfterStopAndDrainReturnsShutdownFuture) {
  auto backend = std::make_unique<MockIoBackend>();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  scheduler.stopAndDrain();

  auto future = scheduler.submit(makeValidRequest());
  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();

  EXPECT_EQ(0, result.bytes);
  EXPECT_EQ(IoErrorCode::Shutdown, result.error);
  EXPECT_EQ(0, result.nativeErrorCode);

  const auto stats = scheduler.stats();
  EXPECT_EQ(1, stats.shutdownRejectedRequests);
  EXPECT_EQ(0, stats.completedRequests);
  EXPECT_EQ(0, stats.failedRequests);
  EXPECT_EQ(0, stats.latencySamples);
}

TEST(DiskIoSchedulerTest, stopAndDrainWaitsForInflightCompletion) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  auto future = scheduler.submit(makeValidRequest(IoPriority::High));
  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 1));

  std::promise<void> firstStopFinished;
  std::promise<void> secondStopFinished;
  auto firstStopFuture = firstStopFinished.get_future();
  auto secondStopFuture = secondStopFinished.get_future();
  std::thread firstStopThread([&scheduler, &firstStopFinished] {
    scheduler.stopAndDrain();
    firstStopFinished.set_value();
  });
  std::thread secondStopThread([&scheduler, &secondStopFinished] {
    scheduler.stopAndDrain();
    secondStopFinished.set_value();
  });

  EXPECT_EQ(
      std::future_status::timeout,
      firstStopFuture.wait_for(std::chrono::milliseconds(10)));
  EXPECT_EQ(
      std::future_status::timeout,
      secondStopFuture.wait_for(std::chrono::milliseconds(10)));

  backendPtr->complete(1, IoResult{4096});
  ASSERT_EQ(
      std::future_status::ready, firstStopFuture.wait_for(kFutureTimeout));
  ASSERT_EQ(
      std::future_status::ready, secondStopFuture.wait_for(kFutureTimeout));
  firstStopThread.join();
  secondStopThread.join();
  scheduler.stopAndDrain();

  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();
  EXPECT_EQ(4096, result.bytes);
  EXPECT_EQ(IoErrorCode::Ok, result.error);

  const auto stats = scheduler.stats();
  EXPECT_EQ(0, stats.inflightRequests);
  EXPECT_EQ(0, stats.queuedRequests[priorityIndex(IoPriority::High)]);
  EXPECT_EQ(1, stats.completedRequests);
  EXPECT_EQ(4096, stats.completedBytes);
  EXPECT_EQ(1, stats.successfulRequests);
}

TEST(DiskIoSchedulerTest, statsReflectSuccessfulCompletion) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  auto future = scheduler.submit(makeValidRequest(IoPriority::High));
  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 1));

  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  backendPtr->complete(1, IoResult{4096});
  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();
  EXPECT_EQ(IoErrorCode::Ok, result.error);

  const auto stats = scheduler.stats();
  EXPECT_EQ(0, stats.inflightRequests);
  EXPECT_EQ(64, stats.currentDepth);
  EXPECT_EQ(1, stats.completedRequests);
  EXPECT_EQ(4096, stats.completedBytes);
  EXPECT_EQ(4096, stats.completedBytesByPriority[priorityIndex(IoPriority::High)]);
  EXPECT_EQ(1, stats.successfulRequests);
  EXPECT_EQ(
      1, stats.successfulRequestsByPriority[priorityIndex(IoPriority::High)]);
  EXPECT_EQ(0, stats.failedRequests);
  EXPECT_GT(stats.cumulativeLatencyUs, 0);
  EXPECT_EQ(1, stats.latencySamples);
  EXPECT_GT(stats.averageLatencyUs, 0);
  EXPECT_EQ(1, stats.queueWaitSamples);
  EXPECT_GE(stats.averageEndToEndLatencyUs, stats.averageLatencyUs);
  EXPECT_GE(stats.maxEndToEndLatencyUs, stats.maxLatencyUs);
  EXPECT_EQ(1, stats.submitBatches);
  EXPECT_EQ(1, stats.submittedRequestsInBatches);
  EXPECT_EQ(1, stats.maxSubmitBatchSize);
  EXPECT_EQ(1, stats.completionBatches);
  EXPECT_EQ(1, stats.completedRequestsInBatches);
  EXPECT_EQ(1, stats.maxCompletionBatchSize);
  EXPECT_GT(stats.maxLatencyUs, 0);
  EXPECT_LE(stats.minLatencyUs, stats.maxLatencyUs);
  EXPECT_GE(stats.maxObservedInflightRequests, 1);
  EXPECT_EQ(
      1, stats.completedRequestsByPriority[priorityIndex(IoPriority::High)]);
  EXPECT_NE(std::string::npos, stats.toString().find("completed_requests=1"));
  EXPECT_NE(
      std::string::npos,
      stats.toString().find("average_queue_wait_us="));
  EXPECT_NE(
      std::string::npos,
      stats.toString().find("average_device_latency_us="));
  EXPECT_NE(
      std::string::npos,
      stats.toString().find("average_end_to_end_latency_us="));
  EXPECT_NE(
      std::string::npos,
      stats.toString().find("average_submit_batch_size="));
  EXPECT_NE(
      std::string::npos,
      stats.toString().find("average_completion_batch_size="));
  EXPECT_NE(std::string::npos, stats.toString().find("adaptive_current_depth=64"));
}

TEST(DiskIoSchedulerTest, adaptiveDepthIncreasesWhenThroughputImprovesWithBacklog) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoSchedulerConfig config;
  config.ringDepth = 4;
  config.adaptiveDepth.enabled = true;
  config.adaptiveDepth.minDepth = 1;
  config.adaptiveDepth.initialDepth = 1;
  config.adaptiveDepth.maxDepth = 4;
  config.adaptiveDepth.controlInterval = std::chrono::milliseconds(1);
  config.adaptiveDepth.increaseStep = 1;
  config.adaptiveDepth.minThroughputGain = 0.0;
  DiskIoScheduler scheduler(config, std::move(backend));

  std::vector<std::future<IoResult>> futures;
  futures.reserve(4);
  for (int i = 0; i < 4; ++i) {
    futures.push_back(scheduler.submit(makeValidRequest(IoPriority::Medium)));
  }

  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  backendPtr->complete(1, IoResult{4096});

  ASSERT_TRUE(waitUntilReady(futures[0]));
  EXPECT_EQ(IoErrorCode::Ok, futures[0].get().error);
  EXPECT_EQ(1, scheduler.stats().currentDepth);

  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 2));
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  backendPtr->complete(2, IoResult{4096});

  ASSERT_TRUE(waitUntilReady(futures[1]));
  EXPECT_EQ(IoErrorCode::Ok, futures[1].get().error);
  ASSERT_TRUE(waitUntilCurrentDepth(scheduler, 2));

  const auto stats = scheduler.stats();
  EXPECT_EQ(2, stats.currentDepth);
  EXPECT_EQ(2, stats.adaptive.currentDepth);
  EXPECT_EQ(2, stats.adaptive.completedWindows);
  EXPECT_GT(stats.adaptive.lastWindowThroughputBytesPerSecond, 0);
  EXPECT_GT(stats.recentThroughputBytesPerSecond, 0);

  size_t completedSubmissions = 2;
  completeAllSubmittedRequests(
      *backendPtr, completedSubmissions, futures.size());
  for (size_t i = 2; i < futures.size(); ++i) {
    ASSERT_TRUE(waitUntilReady(futures[i]));
    EXPECT_EQ(IoErrorCode::Ok, futures[i].get().error);
  }
}

TEST(DiskIoSchedulerTest, backendSubmitFailureCompletesFutureAndStats) {
  auto backend = std::make_unique<FailingSubmitBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  auto future = scheduler.submit(makeValidRequest(IoPriority::Low));
  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();

  EXPECT_EQ(1, backendPtr->submitAttempts());
  EXPECT_EQ(0, result.bytes);
  EXPECT_EQ(IoErrorCode::BackendSubmitFailed, result.error);
  EXPECT_EQ(0, result.nativeErrorCode);

  const auto stats = scheduler.stats();
  EXPECT_EQ(0, stats.inflightRequests);
  EXPECT_EQ(1, stats.completedRequests);
  EXPECT_EQ(1, stats.failedRequests);
  EXPECT_EQ(1, stats.backendSubmitFailedRequests);
  EXPECT_EQ(0, stats.latencySamples);
  EXPECT_EQ(0, stats.averageLatencyUs);
  EXPECT_EQ(1, stats.failedRequestsByPriority[priorityIndex(IoPriority::Low)]);
  EXPECT_EQ(
      1, stats.completedRequestsByPriority[priorityIndex(IoPriority::Low)]);
}

TEST(DiskIoSchedulerTest, retryableBackendBusyDoesNotFailRequest) {
  auto backend = std::make_unique<BusyOnceBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  auto future = scheduler.submit(makeValidRequest(IoPriority::Low));
  const auto deadline = std::chrono::steady_clock::now() + kFutureTimeout;
  while (backendPtr->submitAttempts() < 1 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(1, backendPtr->submitAttempts());
  EXPECT_EQ(
      std::future_status::timeout,
      future.wait_for(std::chrono::milliseconds(10)));

  backendPtr->wake();
  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();

  EXPECT_EQ(2, backendPtr->submitAttempts());
  EXPECT_EQ(IoPriority::Low, backendPtr->priority());
  EXPECT_EQ(4096, result.bytes);
  EXPECT_EQ(IoErrorCode::Ok, result.error);

  const auto stats = scheduler.stats();
  EXPECT_EQ(1, stats.completedRequests);
  EXPECT_EQ(1, stats.successfulRequests);
  EXPECT_EQ(1, stats.latencySamples);
  EXPECT_EQ(0, stats.backendSubmitFailedRequests);
  EXPECT_EQ(0, stats.failedRequests);
}

TEST(DiskIoSchedulerTest, retryableBackendBusyWaitsForWakeupBeforeRetrying) {
  auto backend = std::make_unique<AlwaysBusyBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  auto future = scheduler.submit(makeValidRequest(IoPriority::Low));
  const auto deadline = std::chrono::steady_clock::now() + kFutureTimeout;
  while (backendPtr->submitAttempts() < 1 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(1, backendPtr->submitAttempts());

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_EQ(1, backendPtr->submitAttempts());
  EXPECT_EQ(
      std::future_status::timeout,
      future.wait_for(std::chrono::milliseconds(0)));

  scheduler.stopAndDrain();
  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();
  EXPECT_EQ(0, result.bytes);
  EXPECT_EQ(IoErrorCode::Shutdown, result.error);
}

TEST(DiskIoSchedulerTest, stopAndDrainFailsQueuedAndWaitsForInflightCompletion) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoSchedulerConfig config;
  config.adaptiveDepth.initialDepth = 1;
  DiskIoScheduler scheduler(config, std::move(backend));

  auto inflightFuture = scheduler.submit(makeValidRequest(IoPriority::High));
  auto queuedFuture = scheduler.submit(makeValidRequest(IoPriority::Low));
  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 1));
  ASSERT_EQ(1, backendPtr->submitted().size());

  std::promise<void> stopFinished;
  auto stopFuture = stopFinished.get_future();
  std::thread stopThread([&scheduler, &stopFinished] {
    scheduler.stopAndDrain();
    stopFinished.set_value();
  });

  ASSERT_TRUE(waitUntilReady(queuedFuture));
  auto queuedResult = queuedFuture.get();
  EXPECT_EQ(0, queuedResult.bytes);
  EXPECT_EQ(IoErrorCode::Shutdown, queuedResult.error);

  EXPECT_EQ(
      std::future_status::timeout,
      inflightFuture.wait_for(std::chrono::milliseconds(10)));
  EXPECT_EQ(
      std::future_status::timeout,
      stopFuture.wait_for(std::chrono::milliseconds(10)));

  backendPtr->complete(1, IoResult{4096});
  ASSERT_TRUE(waitUntilReady(inflightFuture));
  auto inflightResult = inflightFuture.get();
  EXPECT_EQ(4096, inflightResult.bytes);
  EXPECT_EQ(IoErrorCode::Ok, inflightResult.error);

  ASSERT_EQ(
      std::future_status::ready, stopFuture.wait_for(kFutureTimeout));
  stopThread.join();

  const auto stats = scheduler.stats();
  EXPECT_EQ(0, stats.inflightRequests);
  EXPECT_EQ(1, stats.failedRequests);
  EXPECT_EQ(1, stats.successfulRequests);
  EXPECT_EQ(1, stats.latencySamples);
}

TEST(DiskIoSchedulerTest, backendIoErrorStatsIncludePriorityAndNativeError) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  auto future = scheduler.submit(makeValidRequest(IoPriority::Low));
  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 1));
  backendPtr->complete(
      1, IoResult{0, IoErrorCode::BackendIoError, EIO});
  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();

  EXPECT_EQ(IoErrorCode::BackendIoError, result.error);
  EXPECT_EQ(EIO, result.nativeErrorCode);

  const auto stats = scheduler.stats();
  EXPECT_EQ(1, stats.completedRequests);
  EXPECT_EQ(1, stats.failedRequests);
  EXPECT_EQ(1, stats.latencySamples);
  EXPECT_EQ(1, stats.backendIoErrorRequests);
  EXPECT_EQ(1, stats.failedRequestsByPriority[priorityIndex(IoPriority::Low)]);
  EXPECT_NE(
      std::string::npos,
      stats.toString().find("backend_io_error_requests=1"));
}

TEST(DiskIoSchedulerTest, shortCompletionIsReportedAsFailedShortIo) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  auto future = scheduler.submit(makeValidRequest(IoPriority::Medium));
  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 1));
  backendPtr->complete(1, IoResult{2048});
  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();

  EXPECT_EQ(2048, result.bytes);
  EXPECT_EQ(IoErrorCode::ShortIo, result.error);

  const auto stats = scheduler.stats();
  EXPECT_EQ(1, stats.completedRequests);
  EXPECT_EQ(0, stats.successfulRequests);
  EXPECT_EQ(1, stats.failedRequests);
  EXPECT_EQ(1, stats.latencySamples);
  EXPECT_EQ(
      1, stats.failedRequestsByPriority[priorityIndex(IoPriority::Medium)]);
}

TEST(DiskIoSchedulerTest, statsLoggingConfigRejectsNonPositiveInterval) {
  DiskIoSchedulerConfig config;
  config.enableStatsLogging = true;
  config.statsLogInterval = std::chrono::milliseconds(0);

  EXPECT_EQ(
      IoErrorCode::InvalidRequest,
      validateDiskIoSchedulerConfig(config));
}
