#include "bolt/common/memory/bm/io/IoRequestValidator.h"

#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <memory>
#include <type_traits>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

static_assert(!std::is_copy_constructible_v<IoRequest>);
static_assert(std::is_move_constructible_v<IoRequest>);

namespace {

std::unique_ptr<char[]> makeBuffer(size_t size) {
  return std::make_unique<char[]>(size);
}

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
