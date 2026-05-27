#include "bolt/common/memory/bm/io/DiskIoScheduler.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfigValidator.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoRequestValidator.h"
#include "bolt/common/memory/bm/io/IoResult.h"
#include "bolt/common/memory/bm/io/tests/MockIoBackend.h"

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/tests/GTestUtils.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

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
      backend.complete(11, IoResult{4096}), "unknown requestId");
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

  auto future = scheduler.submit(std::move(request));
  ASSERT_TRUE(waitUntilReady(future));
  auto result = future.get();

  EXPECT_EQ(0, result.bytes);
  EXPECT_EQ(IoErrorCode::InvalidRequest, result.error);
  EXPECT_EQ(0, result.nativeErrorCode);
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
  EXPECT_EQ(1, stats.successfulRequests);
  EXPECT_EQ(0, stats.failedRequests);
  EXPECT_GT(stats.cumulativeLatencyUs, 0);
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
  backendPtr->complete(1, IoResult{4096});

  ASSERT_TRUE(waitUntilReady(futures[0]));
  EXPECT_EQ(IoErrorCode::Ok, futures[0].get().error);
  ASSERT_TRUE(waitUntilCurrentDepth(scheduler, 2));

  const auto stats = scheduler.stats();
  EXPECT_EQ(2, stats.currentDepth);
  EXPECT_GT(stats.recentThroughputBytesPerSecond, 0);

  size_t completedSubmissions = 1;
  completeAllSubmittedRequests(
      *backendPtr, completedSubmissions, futures.size());
  for (size_t i = 1; i < futures.size(); ++i) {
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
  EXPECT_EQ(
      1, stats.completedRequestsByPriority[priorityIndex(IoPriority::Low)]);
}
