/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bolt/common/memory/bm/io/IoRequestValidator.h"

#include "bolt/common/memory/bm/io/IoBuffer.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <type_traits>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

static_assert(!std::is_copy_constructible_v<IoRequest>);
static_assert(std::is_move_constructible_v<IoRequest>);

namespace {

struct CountingDeleter {
  int* freeCount{nullptr};

  void operator()(char* data) const noexcept {
    delete[] data;
    ++(*freeCount);
  }
};

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
