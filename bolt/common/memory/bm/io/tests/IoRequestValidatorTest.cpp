#include "bolt/common/memory/bm/io/IoRequestValidator.h"

#include "bolt/common/memory/bm/io/IoBuffer.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <memory>
#include <type_traits>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

static_assert(!std::is_copy_constructible_v<IoRequest>);
static_assert(std::is_move_constructible_v<IoRequest>);
static_assert(!std::is_copy_constructible_v<IoBuffer>);
static_assert(std::is_move_constructible_v<IoBuffer>);

namespace {

struct CountingDeleter {
  int* freeCount{nullptr};

  void operator()(char* data) const noexcept {
    delete[] data;
    ++(*freeCount);
  }
};

IoBuffer makeBuffer(size_t size, int* freeCount) {
  auto* data = new char[size];
  return IoBuffer::fromOwned(data, size, 0, size, CountingDeleter{freeCount});
}

} // namespace

TEST(IoRequestValidatorTest, validateRequestAcceptsValidRead) {
  int freeCount = 0;
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::High;
  request.fd = 3;
  request.fileOffset = 128;
  request.buffer = IoBuffer::fromOwned(
      new char[4096], 4096, 64, 1024, CountingDeleter{&freeCount});

  EXPECT_EQ(IoErrorCode::Ok, validateIoRequest(request));
}

TEST(IoRequestValidatorTest, validateRequestRejectsBadBufferRange) {
  int freeCount = 0;
  IoRequest request;
  request.opcode = IoOpcode::Write;
  request.priority = IoPriority::Low;
  request.fd = 3;
  request.fileOffset = 0;
  request.buffer = IoBuffer::fromOwned(
      new char[128], 128, 64, 128, CountingDeleter{&freeCount});

  EXPECT_EQ(IoErrorCode::InvalidRequest, validateIoRequest(request));
}

TEST(IoRequestValidatorTest, validateRequestRejectsOffsetAtEndWithLength) {
  int freeCount = 0;
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::Medium;
  request.fd = 3;
  request.buffer = IoBuffer::fromOwned(
      new char[128], 128, 128, 1, CountingDeleter{&freeCount});

  EXPECT_EQ(IoErrorCode::InvalidRequest, validateIoRequest(request));
}

TEST(IoRequestValidatorTest, ioBufferRunsCustomDeleterOnceAfterMove) {
  int freeCount = 0;
  {
    auto buffer = makeBuffer(256, &freeCount);
    IoBuffer moved = std::move(buffer);

    EXPECT_EQ(0, freeCount);
    EXPECT_EQ(256, moved.size());
    EXPECT_EQ(256, moved.length());
  }

  EXPECT_EQ(1, freeCount);
}
