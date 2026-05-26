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
