#include "bolt/common/memory/bm/DiskIoScheduler.h"
#include "bolt/common/memory/bm/DiskIoTypes.h"
#include "bolt/common/memory/bm/MockIoBackend.h"

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/tests/GTestUtils.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

namespace {

std::shared_ptr<void> makeBuffer(size_t size) {
  return std::shared_ptr<void>(
      new char[size], [](void* ptr) { delete[] static_cast<char*>(ptr); });
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
  backend.complete(submitted[submittedIndex].requestId, IoResult{4096, 0});
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
    ++submittedByPriority[priorityIndex(request.request.priority)];
    backend.complete(request.requestId, IoResult{4096, 0});
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
      backend.complete(submitted[i].requestId, IoResult{4096, 0});
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
  bool submit(uint64_t /*requestId*/, const IoRequest& /*request*/) override {
    ++submitAttempts_;
    return false;
  }

  std::vector<BackendCompletion> reap() override {
    return {};
  }

  uint64_t submitAttempts() const {
    return submitAttempts_.load();
  }

 private:
  std::atomic<uint64_t> submitAttempts_{0};
};

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

TEST(MockIoBackendTest, recordsSubmittedRequestsAndCompletesInChosenOrder) {
  MockIoBackend backend;
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::Medium;
  request.fd = 7;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 0, 4096};

  EXPECT_TRUE(backend.submit(11, request));
  EXPECT_TRUE(backend.submit(12, request));
  auto submitted = backend.submitted();
  ASSERT_EQ(2, submitted.size());
  EXPECT_EQ(11, submitted[0].requestId);
  EXPECT_EQ(12, submitted[1].requestId);

  backend.complete(12, IoResult{4096, 0});
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

  EXPECT_TRUE(backend.submit(11, request));
  EXPECT_FALSE(backend.submit(11, request));

  auto submitted = backend.submitted();
  ASSERT_EQ(1, submitted.size());
  EXPECT_EQ(11, submitted[0].requestId);
  EXPECT_EQ(1, backend.inflight());
}

TEST(MockIoBackendTest, completeUnknownRequestFailsFast) {
  MockIoBackend backend;

  BOLT_ASSERT_THROW(
      backend.complete(11, IoResult{4096, 0}), "unknown requestId");
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
      "DiskIoScheduler default constructor requires IoUringBackend");
#endif
}

TEST(DiskIoSchedulerTest, invalidRequestReturnsCompletedErrorFuture) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  IoRequest request = makeValidRequest();
  request.fd = -1;

  auto future = scheduler.submit(request);
  ASSERT_TRUE(waitUntilReady(future));
  const auto result = future.get();

  EXPECT_EQ(0, result.bytes);
  EXPECT_EQ(EINVAL, result.errorCode);
  EXPECT_TRUE(backendPtr->submitted().empty());
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

  backendPtr->complete(1, IoResult{4096, 0});
  ASSERT_TRUE(waitUntilReady(future));
  const auto result = future.get();

  EXPECT_EQ(4096, result.bytes);
  EXPECT_EQ(0, result.errorCode);
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

  backendPtr->complete(1, IoResult{4096, 0});
  ASSERT_TRUE(waitUntilReady(first));
  EXPECT_EQ(0, first.get().errorCode);

  ASSERT_TRUE(waitUntilSubmitted(*backendPtr, 2));
  backendPtr->complete(2, IoResult{4096, 0});
  ASSERT_TRUE(waitUntilReady(second));
  EXPECT_EQ(0, second.get().errorCode);
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
    EXPECT_EQ(0, future.get().errorCode);
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
    EXPECT_EQ(0, future.get().errorCode);
  }
}

TEST(DiskIoSchedulerTest, submitAfterStopAndDrainReturnsShutdownFuture) {
  auto backend = std::make_unique<MockIoBackend>();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  scheduler.stopAndDrain();

  auto future = scheduler.submit(makeValidRequest());
  ASSERT_TRUE(waitUntilReady(future));
  const auto result = future.get();

  EXPECT_EQ(0, result.bytes);
  EXPECT_EQ(ESHUTDOWN, result.errorCode);
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

  backendPtr->complete(1, IoResult{4096, 0});
  ASSERT_EQ(
      std::future_status::ready, firstStopFuture.wait_for(kFutureTimeout));
  ASSERT_EQ(
      std::future_status::ready, secondStopFuture.wait_for(kFutureTimeout));
  firstStopThread.join();
  secondStopThread.join();
  scheduler.stopAndDrain();

  ASSERT_TRUE(waitUntilReady(future));
  const auto result = future.get();
  EXPECT_EQ(4096, result.bytes);
  EXPECT_EQ(0, result.errorCode);

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

  backendPtr->complete(1, IoResult{4096, 0});
  ASSERT_TRUE(waitUntilReady(future));
  const auto result = future.get();
  EXPECT_EQ(0, result.errorCode);

  const auto stats = scheduler.stats();
  EXPECT_EQ(0, stats.inflightRequests);
  EXPECT_EQ(64, stats.currentDepth);
  EXPECT_EQ(1, stats.completedRequests);
  EXPECT_EQ(4096, stats.completedBytes);
  EXPECT_EQ(1, stats.successfulRequests);
  EXPECT_EQ(0, stats.failedRequests);
  EXPECT_EQ(
      1, stats.completedRequestsByPriority[priorityIndex(IoPriority::High)]);
}

TEST(DiskIoSchedulerTest, adaptiveDepthIncreasesWhenThroughputImprovesWithBacklog) {
  auto backend = std::make_unique<MockIoBackend>();
  auto* backendPtr = backend.get();
  DiskIoSchedulerConfig config;
  config.ringDepth = 4;
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
  backendPtr->complete(1, IoResult{4096, 0});

  ASSERT_TRUE(waitUntilReady(futures[0]));
  EXPECT_EQ(0, futures[0].get().errorCode);
  ASSERT_TRUE(waitUntilCurrentDepth(scheduler, 2));

  const auto stats = scheduler.stats();
  EXPECT_EQ(2, stats.currentDepth);
  EXPECT_GT(stats.recentThroughputBytesPerSecond, 0);

  size_t completedSubmissions = 1;
  completeAllSubmittedRequests(
      *backendPtr, completedSubmissions, futures.size());
  for (size_t i = 1; i < futures.size(); ++i) {
    ASSERT_TRUE(waitUntilReady(futures[i]));
    EXPECT_EQ(0, futures[i].get().errorCode);
  }
}

TEST(DiskIoSchedulerTest, backendSubmitFailureCompletesFutureAndStats) {
  auto backend = std::make_unique<FailingSubmitBackend>();
  auto* backendPtr = backend.get();
  DiskIoScheduler scheduler(DiskIoSchedulerConfig{}, std::move(backend));

  auto future = scheduler.submit(makeValidRequest(IoPriority::Low));
  ASSERT_TRUE(waitUntilReady(future));
  const auto result = future.get();

  EXPECT_EQ(1, backendPtr->submitAttempts());
  EXPECT_EQ(0, result.bytes);
  EXPECT_EQ(EIO, result.errorCode);

  const auto stats = scheduler.stats();
  EXPECT_EQ(0, stats.inflightRequests);
  EXPECT_EQ(1, stats.completedRequests);
  EXPECT_EQ(1, stats.failedRequests);
  EXPECT_EQ(
      1, stats.completedRequestsByPriority[priorityIndex(IoPriority::Low)]);
}
