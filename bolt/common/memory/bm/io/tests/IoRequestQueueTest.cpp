#include "bolt/common/memory/bm/io/IoRequestQueue.h"

#include <chrono>
#include <future>
#include <memory>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

namespace {

std::unique_ptr<char[]> makeBuffer(size_t size) {
  return std::make_unique<char[]>(size);
}

QueuedIoRequest makeQueued(uint64_t requestId, IoPriority priority) {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = priority;
  request.fd = 7;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 0, 4096};
  return QueuedIoRequest{
      requestId,
      std::move(request),
      std::promise<IoResult>(),
      std::chrono::steady_clock::now()};
}

} // namespace

TEST(IoRequestQueueTest, CollectsUsingConfiguredWeights) {
  IoRequestQueue queue({2, 1, 1});
  queue.enqueue(makeQueued(1, IoPriority::High));
  queue.enqueue(makeQueued(2, IoPriority::High));
  queue.enqueue(makeQueued(3, IoPriority::Medium));

  auto batch = queue.collect(3);

  ASSERT_EQ(batch.size(), 3);
  EXPECT_EQ(batch[0].request.priority, IoPriority::High);
  EXPECT_EQ(batch[1].request.priority, IoPriority::Medium);
  EXPECT_EQ(batch[2].request.priority, IoPriority::High);
  EXPECT_FALSE(queue.hasRequests());
}

TEST(IoRequestQueueTest, ReturnToFrontRestoresDispatchCredit) {
  IoRequestQueue queue({1, 1, 1});
  queue.enqueue(makeQueued(1, IoPriority::High));
  queue.enqueue(makeQueued(2, IoPriority::Medium));

  auto firstBatch = queue.collect(1);
  ASSERT_EQ(firstBatch.size(), 1);
  ASSERT_EQ(firstBatch[0].request.priority, IoPriority::High);

  queue.returnToFront(std::move(firstBatch[0]));

  auto retryBatch = queue.collect(1);

  ASSERT_EQ(retryBatch.size(), 1);
  EXPECT_EQ(retryBatch[0].requestId, 1);
  EXPECT_EQ(retryBatch[0].request.priority, IoPriority::High);
  EXPECT_EQ(queue.totalQueued(), 1);
  const auto counts = queue.queuedCounts();
  EXPECT_EQ(counts[priorityIndex(IoPriority::High)], 0);
  EXPECT_EQ(counts[priorityIndex(IoPriority::Medium)], 1);
}
