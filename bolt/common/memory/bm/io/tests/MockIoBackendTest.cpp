#include "bolt/common/memory/bm/io/tests/MockIoBackend.h"

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <memory>

#include <gtest/gtest.h>
#include <poll.h>

using namespace bytedance::bolt::memory::bm;

namespace {

std::unique_ptr<char[]> makeBuffer(size_t size) {
  return std::make_unique<char[]>(size);
}

IoRequest makeRequest() {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::Medium;
  request.fd = 7;
  request.buffer = IoBuffer{makeBuffer(4096), 4096, 0, 4096};
  return request;
}

} // namespace

TEST(MockIoBackendTest, recordsSubmittedRequestsAndCompletesInChosenOrder) {
  MockIoBackend backend;
  auto request = makeRequest();

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
  auto request = makeRequest();

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
  auto request = makeRequest();

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
