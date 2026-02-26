/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/file/File.h"
#include "bolt/common/base/Fs.h"

#include <fcntl.h>
#include <fmt/format.h>
#include <folly/portability/SysUio.h>
#include <glog/logging.h>
#ifdef IO_URING_SUPPORTED
#include <liburing.h>
#include <liburing/io_uring.h>
#endif
#include <memory>
#include <stdexcept>
namespace bytedance::bolt {

#define RETURN_IF_ERROR(func, result) \
  result = func;                      \
  if (result < 0) {                   \
    return result;                    \
  }

namespace {
FOLLY_ALWAYS_INLINE void checkNotClosed(bool closed) {
  BOLT_CHECK(!closed, "file is closed");
}
} // namespace
std::string ReadFile::pread(uint64_t offset, uint64_t length) const {
  std::string buf;
  buf.resize(length);
  auto res = pread(offset, length, buf.data());
  buf.resize(res.size());
  return buf;
}

uint64_t ReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  auto fileSize = size();
  uint64_t numRead = 0;
  if (offset >= fileSize) {
    return 0;
  }
  for (auto& range : buffers) {
    auto copySize = std::min<size_t>(range.size(), fileSize - offset);
    // NOTE: skip the gap in case of coalesce io.
    if (range.data() != nullptr) {
      pread(offset, copySize, range.data());
    }
    offset += copySize;
    numRead += copySize;
  }
  return numRead;
}

void ReadFile::preadv(
    folly::Range<const common::Region*> regions,
    folly::Range<folly::IOBuf*> iobufs) const {
  BOLT_CHECK_EQ(regions.size(), iobufs.size());
  for (size_t i = 0; i < regions.size(); ++i) {
    const auto& region = regions[i];
    auto& output = iobufs[i];
    output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
    pread(region.offset, region.length, output.writableData());
    output.append(region.length);
  }
}

std::string_view
InMemoryReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  bytesRead_ += length;
  memcpy(buf, file_.data() + offset, length);
  return {static_cast<char*>(buf), length};
}

std::string InMemoryReadFile::pread(uint64_t offset, uint64_t length) const {
  bytesRead_ += length;
  return std::string(file_.data() + offset, length);
}

void InMemoryWriteFile::append(std::string_view data) {
  file_->append(data);
}

void InMemoryWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  for (auto rangeIter = data->begin(); rangeIter != data->end(); ++rangeIter) {
    file_->append(
        reinterpret_cast<const char*>(rangeIter->data()), rangeIter->size());
  }
}

uint64_t InMemoryWriteFile::size() const {
  return file_->size();
}

LocalReadFile::LocalReadFile(std::string_view path) : path_(path) {
  fd_ = open(path_.c_str(), O_RDONLY);
  if (fd_ < 0) {
    if (errno == ENOENT) {
      BOLT_FILE_NOT_FOUND_ERROR("No such file or directory: {}", path);
    } else {
      BOLT_FAIL(
          "open failure in LocalReadFile constructor, {} {} {}.",
          fd_,
          path,
          folly::errnoStr(errno));
    }
  }
  const off_t rc = lseek(fd_, 0, SEEK_END);
  BOLT_CHECK_GE(
      rc,
      0,
      "fseek failure in LocalReadFile constructor, {} {} {}.",
      rc,
      path,
      folly::errnoStr(errno));
  size_ = rc;
}

LocalReadFile::LocalReadFile(int32_t fd) : fd_(fd) {}

LocalReadFile::~LocalReadFile() {
  const int ret = close(fd_);
  if (ret < 0) {
    LOG(WARNING) << "close failure in LocalReadFile destructor: " << ret << ", "
                 << folly::errnoStr(errno);
  }
}

void LocalReadFile::preadInternal(uint64_t offset, uint64_t length, char* pos)
    const {
  bytesRead_ += length;
  auto bytesRead = ::pread(fd_, pos, length, offset);
  BOLT_CHECK_EQ(
      bytesRead,
      length,
      "fread failure in LocalReadFile::PReadInternal, {} vs {}.",
      bytesRead,
      length);
}

std::string_view
LocalReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  preadInternal(offset, length, static_cast<char*>(buf));
  return {static_cast<char*>(buf), length};
}

uint64_t LocalReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  // Dropped bytes sized so that a typical dropped range of 50K is not
  // too many iovecs.
  static thread_local std::vector<char> droppedBytes(16 * 1024);
  uint64_t totalBytesRead = 0;
  std::vector<struct iovec> iovecs;
  iovecs.reserve(buffers.size());

  auto readvFunc = [&]() -> ssize_t {
    const auto bytesRead =
        folly::preadv(fd_, iovecs.data(), iovecs.size(), offset);
    if (bytesRead < 0) {
      LOG(ERROR) << "preadv failed with error: " << folly::errnoStr(errno);
    } else {
      totalBytesRead += bytesRead;
      offset += bytesRead;
    }
    iovecs.clear();
    return bytesRead;
  };

  for (auto& range : buffers) {
    if (!range.data()) {
      auto skipSize = range.size();
      while (skipSize) {
        auto bytes = std::min<size_t>(droppedBytes.size(), skipSize);

        if (iovecs.size() >= IOV_MAX) {
          ssize_t bytesRead{0};
          RETURN_IF_ERROR(readvFunc(), bytesRead);
        }

        iovecs.push_back({droppedBytes.data(), bytes});
        skipSize -= bytes;
      }
    } else {
      if (iovecs.size() >= IOV_MAX) {
        ssize_t bytesRead{0};
        RETURN_IF_ERROR(readvFunc(), bytesRead);
      }

      iovecs.push_back({range.data(), range.size()});
    }
  }

  // Perform any remaining preadv calls
  if (!iovecs.empty()) {
    ssize_t bytesRead{0};
    RETURN_IF_ERROR(readvFunc(), bytesRead);
  }

  return totalBytesRead;
}

uint64_t LocalReadFile::size() const {
  return size_;
}

uint64_t LocalReadFile::memoryUsage() const {
  // TODO: does FILE really not use any more memory? From the stdio.h
  // source code it looks like it has only a single integer? Probably
  // we need to go deeper and see how much system memory is being taken
  // by the file descriptor the integer refers to?
  return sizeof(FILE);
}

LocalWriteFile::LocalWriteFile(
    std::string_view path,
    bool shouldCreateParentDirectories,
    bool shouldThrowOnFileAlreadyExists) {
  auto dir = fs::path(path).parent_path();
  if (shouldCreateParentDirectories && !fs::exists(dir)) {
    BOLT_CHECK(
        common::generateFileDirectory(dir.c_str()),
        "Failed to generate file directory");
  }

  std::unique_ptr<char[]> buf(new char[path.size() + 1]);
  buf[path.size()] = 0;
  memcpy(buf.get(), path.data(), path.size());
  {
    if (shouldThrowOnFileAlreadyExists) {
      FILE* exists = fopen(buf.get(), "rb");
      if (exists) {
        fclose(exists);
        BOLT_FAIL("Failure in LocalWriteFile: path '{}' already exists.", path);
      }
    }
  }
  auto* file = fopen(buf.get(), "ab");
  BOLT_CHECK_NOT_NULL(
      file,
      "fopen failure in LocalWriteFile constructor, {} {}.",
      path,
      folly::errnoStr(errno));
  file_ = file;
}

LocalWriteFile::~LocalWriteFile() {
  try {
    close();
  } catch (const std::exception& ex) {
    // We cannot throw an exception from the destructor. Warn instead.
    LOG(WARNING) << "fclose failure in LocalWriteFile destructor: "
                 << ex.what();
  }
}

void LocalWriteFile::append(std::string_view data) {
  BOLT_CHECK(!closed_, "file is closed");
  const uint64_t bytesWritten = fwrite(data.data(), 1, data.size(), file_);
  BOLT_CHECK_EQ(
      bytesWritten,
      data.size(),
      "fwrite failure in LocalWriteFile::append, {} vs {}: {}",
      bytesWritten,
      data.size(),
      folly::errnoStr(errno));
}

void LocalWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  BOLT_CHECK(!closed_, "file is closed");
  uint64_t totalBytesWritten{0};
  for (auto rangeIter = data->begin(); rangeIter != data->end(); ++rangeIter) {
    const auto bytesToWrite = rangeIter->size();
    const auto bytesWritten =
        fwrite(rangeIter->data(), 1, rangeIter->size(), file_);
    totalBytesWritten += bytesWritten;
    if (bytesWritten != bytesToWrite) {
      BOLT_FAIL(
          "fwrite failure in LocalWriteFile::append, {} vs {}: {}",
          bytesWritten,
          bytesToWrite,
          folly::errnoStr(errno));
    }
  }
  const auto totalBytesToWrite = data->computeChainDataLength();
  BOLT_CHECK_EQ(
      totalBytesWritten,
      totalBytesToWrite,
      "Failure in LocalWriteFile::append, {} vs {}",
      totalBytesWritten,
      totalBytesToWrite);
}

void LocalWriteFile::truncate(int64_t newSize) {
  checkNotClosed(closed_);
  BOLT_CHECK_GE(newSize, 0, "New size cannot be negative.");
  auto fd_ = fileno(file_);
  const auto ret = ::ftruncate(fd_, newSize);
  BOLT_CHECK_EQ(
      ret,
      0,
      "ftruncate failed in LocalWriteFile::truncate: {}.",
      folly::errnoStr(errno));
  // Reposition the file offset to the end of the file for append().
  ::lseek(fd_, newSize, SEEK_SET);
  size_ = newSize;
}

void LocalWriteFile::flush() {
  BOLT_CHECK(!closed_, "file is closed");
  auto ret = fflush(file_);
  BOLT_CHECK_EQ(
      ret,
      0,
      "fflush failed in LocalWriteFile::flush: {}.",
      folly::errnoStr(errno));
}

void LocalWriteFile::close() {
  if (!closed_) {
    auto ret = fclose(file_);
    BOLT_CHECK_EQ(
        ret,
        0,
        "fwrite failure in LocalWriteFile::close: {}.",
        folly::errnoStr(errno));
    closed_ = true;
  }
}

uint64_t LocalWriteFile::size() const {
  return ftell(file_);
}

folly::IOBuf* WriteBuffers::add(
    std::unique_ptr<folly::IOBuf>& buf,
    int taskId,
    const std::unique_ptr<WriteFile>& file) {
  int idx = taskId % capacity_;
  std::lock_guard<std::mutex> lock(mutexes_[idx]);
  if (buffers_[idx] != nullptr) {
    BOLT_CHECK_GE(taskId, capacity_, "writeBuffers out of index");
    // we use round-robin, so the taskId to wait is taskId - capacity_
    auto ret = file->waitForComplete(taskId - capacity_, buffers_);
    BOLT_CHECK(
        ret, "Error occurred when waiting for io_uring write to complete");
  }
  buffers_[idx] = std::move(buf);
  return buffers_[idx].get();
}

void WriteBuffers::clear(std::unique_ptr<WriteFile>& file) {
  auto ret = file->waitForCompleteAll();
  BOLT_CHECK(ret, "Error occurred when waiting for io_uring write to complete");
  for (int i = 0; i < capacity_; i++) {
    std::lock_guard<std::mutex> lock(mutexes_[i]);
    if (buffers_[i] != nullptr)
      buffers_[i].reset();
  }
}

#ifdef IO_URING_SUPPORTED
AsyncLocalReadFile::AsyncLocalReadFile(std::string_view path) : path_(path) {
  fd_ = open(path_.c_str(), O_RDONLY);
  BOLT_CHECK_GE(
      fd_,
      0,
      "open failure in LocalReadFile constructor, {} {} {}.",
      fd_,
      path,
      folly::errnoStr(errno));
  const off_t rc = lseek(fd_, 0, SEEK_END);
  BOLT_CHECK_GE(
      rc,
      0,
      "fseek failure in LocalReadFile constructor, {} {} {}.",
      rc,
      path,
      folly::errnoStr(errno));
  size_ = rc;
  int ringDepth = 64;
  ring_ = std::make_unique<io_uring>();
  int ret = io_uring_queue_init(ringDepth, ring_.get(), 0);
  if (ret < 0) {
    LOG(WARNING) << "Failed to init ring:" << -ret << ". Use sync read";
    uringEnabled_ = false;
    ring_.reset();
  } else {
    LOG(INFO) << "io_uring sets up successfully for AsyncReadWriteFile";
  }
}

AsyncLocalReadFile::~AsyncLocalReadFile() {
  const int ret = close(fd_);
  if (ret < 0) {
    LOG(WARNING) << "close failure in LocalReadFile destructor: " << ret << ", "
                 << folly::errnoStr(errno);
  }
  if (uringEnabled_) {
    io_uring_queue_exit(ring_.get());
  }
}

void AsyncLocalReadFile::submitRead(
    char* buffer,
    uint64_t offset,
    size_t readBytes) {
  if (!uringEnabled_) {
    LOG(ERROR) << "io_uring is disabled, but submitRead() was called!";
    return;
  }
  const size_t readBlockSize = 128 << 10; // 128K
  //  int readBlockSize = 1 << 30; // 1M
  bytesRead_ += readBytes;
  size_t buffOffset = 0;
  io_uring_sqe* sqe;
  {
    auto pending = pending_.wlock();
    while (readBytes > 0) {
      size_t bytesToRead = std::min(readBlockSize, readBytes);
      readBytes -= bytesToRead;
      sqe = io_uring_get_sqe(ring_.get());
      BOLT_CHECK(sqe, "No available SQE");
      io_uring_prep_read(sqe, fd_, buffer + buffOffset, bytesToRead, offset);
      offset += bytesToRead;
      buffOffset += bytesToRead;
      (*pending)++;
    }
  }
  int ret = io_uring_submit(ring_.get());
  BOLT_CHECK_GE(
      ret,
      0,
      "Failed to submit io_uring request[{}]: {}",
      -ret,
      std::strerror(-ret));
}

bool AsyncLocalReadFile::waitForComplete() {
  auto pending = pending_.wlock();
  if (*pending == 0)
    return true;
  io_uring_cqe* cqe;
  while (*pending > 0) {
    int ret = io_uring_wait_cqe(ring_.get(), &cqe);
    if (ret < 0) {
      LOG(ERROR) << "Failed to wait for CQE with errorno " << -ret << " : "
                 << std::strerror(-ret);
      return false;
    }
    if (cqe->res < 0) {
      LOG(ERROR) << "Failed to get result with errorno " << -(cqe->res) << " : "
                 << std::strerror(-cqe->res);
      return false;
    } else {
      (*pending)--;
      io_uring_cqe_seen(ring_.get(), cqe);
    }
  }
  return true;
}

uint64_t AsyncLocalReadFile::size() const {
  return size_;
}

void AsyncLocalReadFile::preadInternal(
    uint64_t offset,
    uint64_t length,
    char* pos) const {
  bytesRead_ += length;
  auto bytesRead = ::pread(fd_, pos, length, offset);
  BOLT_CHECK_EQ(
      bytesRead,
      length,
      "fread failure in LocalReadFile::PReadInternal, {} vs {}.",
      bytesRead,
      length);
}

uint64_t AsyncLocalReadFile::memoryUsage() const {
  return sizeof(FILE);
}

std::string_view
AsyncLocalReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  preadInternal(offset, length, static_cast<char*>(buf));
  return {static_cast<char*>(buf), length};
}

AsyncLocalWriteFile::AsyncLocalWriteFile(
    std::string_view path,
    bool shouldCreateParentDirectories,
    bool shouldThrowOnFileAlreadyExists) {
  auto dir = fs::path(path).parent_path();
  if (shouldCreateParentDirectories && !fs::exists(dir)) {
    BOLT_CHECK(
        common::generateFileDirectory(dir.c_str()),
        "Failed to generate file directory");
  }
  std::unique_ptr<char[]> buf(new char[path.size() + 1]);
  buf[path.size()] = 0;
  memcpy(buf.get(), path.data(), path.size());
  {
    if (shouldThrowOnFileAlreadyExists) {
      FILE* exists = fopen(buf.get(), "rb");
      BOLT_CHECK(
          !exists,
          "Failure in LocalWriteFile: path '{}' already exists.",
          path);
    }
  }
  FILE* file;
  int ringDepth = 64;
  ring_ = std::make_unique<io_uring>();
  auto ret = io_uring_queue_init(ringDepth, ring_.get(), 0);
  if (ret < 0) {
    LOG(WARNING) << "Failed to init ring:" << -ret << ". Use sync write";
    uringEnabled_ = false;
    ring_.reset();
    file = fopen(buf.get(), "ab");
  } else {
    LOG(INFO) << "io_uring sets up successfully for AsyncLocalWriteFile";
    file = fopen(buf.get(), "wb");
  }
  BOLT_CHECK_NOT_NULL(
      file,
      "fopen failure in LocalWriteFile constructor, {} {}.",
      path,
      folly::errnoStr(errno));
  file_ = file;
}

AsyncLocalWriteFile::~AsyncLocalWriteFile() {
  try {
    close();
  } catch (const std::exception& ex) {
    // We cannot throw an exception from the destructor. Warn instead.
    LOG(WARNING) << "fclose failure in LocalWriteFile destructor: "
                 << ex.what();
  }
}

void AsyncLocalWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  BOLT_CHECK(!uringEnabled_, "uring is enabled");
  BOLT_CHECK(!closed_, "file is closed");
  uint64_t totalBytesWritten{0};
  for (auto rangeIter = data->begin(); rangeIter != data->end(); ++rangeIter) {
    const auto bytesToWrite = rangeIter->size();
    const auto bytesWritten =
        fwrite(rangeIter->data(), 1, rangeIter->size(), file_);
    totalBytesWritten += bytesWritten;
    if (bytesWritten != bytesToWrite) {
      BOLT_FAIL(
          "fwrite failure in LocalWriteFile::append, {} vs {}: {}",
          bytesWritten,
          bytesToWrite,
          folly::errnoStr(errno));
    }
  }
  const auto totalBytesToWrite = data->computeChainDataLength();
  BOLT_CHECK_EQ(
      totalBytesWritten,
      totalBytesToWrite,
      "Failure in LocalWriteFile::append, {} vs {}",
      totalBytesWritten,
      totalBytesToWrite);
}

void AsyncLocalWriteFile::append(std::string_view data) {
  BOLT_CHECK(!closed_, "file is closed");
  const uint64_t bytesWritten = fwrite(data.data(), 1, data.size(), file_);
  BOLT_CHECK_EQ(
      bytesWritten,
      data.size(),
      "fwrite failure in LocalWriteFile::append, {} vs {}: {}",
      bytesWritten,
      data.size(),
      folly::errnoStr(errno));
}

void AsyncLocalWriteFile::submitWrite(folly::IOBuf* data, int taskId) {
  auto pending = pending_.wlock();
  const auto bytesToWrite = data->length();
  io_uring_sqe* sqe;
  sqe = io_uring_get_sqe(ring_.get());
  BOLT_CHECK(sqe, "No available SQE");
  sqe->user_data = taskId;
  io_uring_prep_write(sqe, fileno(file_), data->data(), bytesToWrite, offset_);
  int ret = io_uring_submit(ring_.get());
  BOLT_CHECK_GE(
      ret,
      0,
      "Failed to submit io_uring request[{}]: {}",
      -ret,
      std::strerror(-ret));
  offset_ += bytesToWrite;
  (*pending)++;
}

bool AsyncLocalWriteFile::waitForComplete(
    int taskId,
    std::vector<std::unique_ptr<folly::IOBuf>>& buffers) {
  std::vector<uint64_t> res;
  int completed = -1;
  io_uring_cqe* cqe;
  auto pending = pending_.wlock();
  while (completed != taskId) {
    int ret = io_uring_wait_cqe(ring_.get(), &cqe);
    if (ret < 0) {
      LOG(ERROR) << "Failed to wait for CQE with errorno " << -ret << " : "
                 << std::strerror(-ret);
      return false;
    }
    if (cqe->res < 0) {
      LOG(ERROR) << "Failed to get result with errorno " << -(cqe->res) << " : "
                 << std::strerror(-cqe->res);
      return false;
    } else {
      (*pending)--;
      completed = cqe->user_data;
      int idx = completed % buffers.size();
      buffers[idx].reset();
      io_uring_cqe_seen(ring_.get(), cqe);
    }
  }
  return true;
}

bool AsyncLocalWriteFile::waitForCompleteAll() {
  io_uring_cqe* cqe;
  auto pending = pending_.wlock();
  while (*pending > 0) {
    int ret = io_uring_wait_cqe(ring_.get(), &cqe);
    if (ret < 0) {
      LOG(ERROR) << "Failed to wait for CQE with errorno " << -ret << " : "
                 << std::strerror(-ret);
      return false;
    }
    if (cqe->res < 0) {
      LOG(ERROR) << "Failed to get result with errorno " << -(cqe->res) << " : "
                 << std::strerror(-cqe->res);
      return false;
    } else {
      (*pending)--;
      io_uring_cqe_seen(ring_.get(), cqe);
    }
  }
  return true;
}

void AsyncLocalWriteFile::close() {
  if (!closed_) {
    if (uringEnabled_) {
      auto pending = pending_.rlock();
      BOLT_CHECK_EQ(*pending, 0, "io_uring has remaining write tasks!");
      io_uring_queue_exit(ring_.get());
    }
    auto ret = fclose(file_);
    BOLT_CHECK_EQ(
        ret,
        0,
        "fwrite failure in LocalWriteFile::close: {}.",
        folly::errnoStr(errno));
    closed_ = true;
  }
}

uint64_t AsyncLocalWriteFile::size() const {
  if (uringEnabled_)
    return offset_;
  return ftell(file_);
}
#endif
} // namespace bytedance::bolt
