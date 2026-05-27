#include "bolt/common/memory/bm/io/DiskIoScheduler.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"
#include "bolt/common/memory/bm/io/IoUringBackend.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <exception>
#include <memory>
#include <string>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

#ifdef IO_URING_SUPPORTED
namespace {

std::unique_ptr<char[]> makeBuffer(size_t size) {
  return std::make_unique<char[]>(size);
}

class TempFd {
 public:
  TempFd() {
    char path[] = "/tmp/bolt_disk_io_test_XXXXXX";
    fd_ = mkstemp(path);
    path_ = path;
  }

  ~TempFd() {
    if (fd_ >= 0) {
      close(fd_);
    }
    if (!path_.empty()) {
      unlink(path_.c_str());
    }
  }

  int fd() const {
    return fd_;
  }

 private:
  int fd_{-1};
  std::string path_;
};

DiskIoSchedulerConfig makeConfig() {
  DiskIoSchedulerConfig config;
  config.ringDepth = 32;
  config.adaptiveDepth.enabled = false;
  config.adaptiveDepth.initialDepth = 4;
  return config;
}

IoRequest makeRequest(IoOpcode opcode, int fd, std::unique_ptr<char[]> buffer) {
  IoRequest request;
  request.opcode = opcode;
  request.priority = IoPriority::High;
  request.fd = fd;
  request.fileOffset = 0;
  request.buffer = IoBuffer{std::move(buffer), 4096, 0, 4096};
  return request;
}

std::unique_ptr<DiskIoScheduler> makeSchedulerOrSkip(
    const DiskIoSchedulerConfig& config,
    std::string& skipReason) {
  try {
    return std::make_unique<DiskIoScheduler>(
        config, std::make_unique<IoUringBackend>(config.ringDepth));
  } catch (const std::exception& ex) {
    if (std::string(ex.what()).find("Operation not permitted") !=
        std::string::npos) {
      skipReason = "io_uring is not permitted in this runtime";
      return nullptr;
    }
    throw;
  }
}

} // namespace

TEST(IoUringBackendTest, writeAndReadTemporaryFile) {
  TempFd file;
  ASSERT_GE(file.fd(), 0);

  auto config = makeConfig();
  std::string skipReason;
  auto scheduler = makeSchedulerOrSkip(config, skipReason);
  if (!scheduler) {
    GTEST_SKIP() << skipReason;
  }

  auto writeBuffer = makeBuffer(4096);
  std::memset(writeBuffer.get(), 'x', 4096);
  auto write = makeRequest(IoOpcode::Write, file.fd(), std::move(writeBuffer));
  auto writeResult = scheduler->submit(std::move(write)).get();
  EXPECT_EQ(IoErrorCode::Ok, writeResult.error);

  auto readBuffer = makeBuffer(4096);
  auto read = makeRequest(IoOpcode::Read, file.fd(), std::move(readBuffer));
  auto result = scheduler->submit(std::move(read)).get();
  EXPECT_EQ(IoErrorCode::Ok, result.error);
  EXPECT_EQ(4096, result.bytes);
  EXPECT_EQ(
      0,
      std::memcmp(
          writeResult.buffer.data.get(), result.buffer.data.get(), 4096));
}

TEST(IoUringBackendTest, invalidFdReturnsErrorResult) {
  auto config = makeConfig();
  std::string skipReason;
  auto scheduler = makeSchedulerOrSkip(config, skipReason);
  if (!scheduler) {
    GTEST_SKIP() << skipReason;
  }

  auto buffer = makeBuffer(4096);
  auto request = makeRequest(IoOpcode::Read, 999999, std::move(buffer));
  auto result = scheduler->submit(std::move(request)).get();

  EXPECT_EQ(0, result.bytes);
  EXPECT_EQ(IoErrorCode::BackendIoError, result.error);
  EXPECT_EQ(EBADF, result.nativeErrorCode);
}
#endif
