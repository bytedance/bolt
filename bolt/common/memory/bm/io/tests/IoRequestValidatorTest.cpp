#include "bolt/common/memory/bm/io/IoRequestValidator.h"

#include "bolt/common/memory/bm/io/IoBuffer.h"
#include "bolt/common/memory/bm/io/IoBufferOwner.h"
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

TEST(IoRequestValidatorTest, ioBufferValidReflectsOwnershipAndRange) {
  int freeCount = 0;

  IoBuffer empty;
  EXPECT_FALSE(empty.valid());

  auto valid = IoBuffer::fromOwned(
      new char[256], 256, 64, 128, CountingDeleter{&freeCount});
  EXPECT_TRUE(valid.valid());
  EXPECT_EQ(valid.data() + 64, valid.ioData());
  EXPECT_EQ(64, valid.offset());
  EXPECT_EQ(128, valid.length());
  valid.setLength(192);
  EXPECT_EQ(192, valid.length());
  EXPECT_THROW(valid.setLength(193), std::exception);

  auto offsetAtEnd = IoBuffer::fromOwned(
      new char[64], 64, 64, 0, CountingDeleter{&freeCount});
  EXPECT_TRUE(offsetAtEnd.valid());

  auto offsetPastEnd = IoBuffer::fromOwned(
      new char[64], 64, 65, 0, CountingDeleter{&freeCount});
  EXPECT_FALSE(offsetPastEnd.valid());

  auto lengthPastEnd = IoBuffer::fromOwned(
      new char[64], 64, 32, 33, CountingDeleter{&freeCount});
  EXPECT_FALSE(lengthPastEnd.valid());
}

TEST(IoRequestValidatorTest, uniqueBufferOwnerResetAndMoveAssignment) {
  int freeCount = 0;
  UniqueBufferOwner empty;
  EXPECT_FALSE(empty.owns());
  empty.reset();

  UniqueBufferOwner owner(new char[8], CountingDeleter{&freeCount});
  EXPECT_TRUE(owner.owns());
  owner.reset();
  EXPECT_FALSE(owner.owns());
  EXPECT_EQ(1, freeCount);

  UniqueBufferOwner first(new char[8], CountingDeleter{&freeCount});
  UniqueBufferOwner second(new char[8], CountingDeleter{&freeCount});
  first = std::move(second);
  EXPECT_TRUE(first.owns());
  EXPECT_FALSE(second.owns());
  EXPECT_EQ(2, freeCount);

  first = std::move(first);
  EXPECT_TRUE(first.owns());
}

TEST(IoRequestValidatorTest, ioResultOkReflectsErrorCode) {
  IoResult result;
  EXPECT_TRUE(result.ok());

  result.error = IoErrorCode::InvalidRequest;
  EXPECT_FALSE(result.ok());
  result.error = IoErrorCode::Shutdown;
  EXPECT_FALSE(result.ok());
  result.error = IoErrorCode::BackendSubmitFailed;
  EXPECT_FALSE(result.ok());
  result.error = IoErrorCode::BackendIoError;
  EXPECT_FALSE(result.ok());
  result.error = IoErrorCode::ShortIo;
  EXPECT_FALSE(result.ok());

  result = IoResult(128, IoErrorCode::Ok);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(128, result.bytes);

  result = IoResult(64, IoErrorCode::ShortIo, 5);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(64, result.bytes);
  EXPECT_EQ(5, result.nativeErrorCode);
}
