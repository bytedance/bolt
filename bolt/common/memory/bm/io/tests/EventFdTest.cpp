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

// Model a downstream consumer that does not inherit CMake definitions.
#ifdef OS_MACOSX
#undef OS_MACOSX
#endif
#include "bolt/common/memory/bm/io/EventFd.h"

#include <poll.h>
#if defined(__APPLE__)
#include <sys/ioctl.h>
#endif
#include <unistd.h>
#include <cstdint>
#include <limits>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

// A macOS consumer must reserve both pipe descriptors; Linux keeps its ABI.
#if defined(__APPLE__)
static_assert(sizeof(EventFd) == 2 * sizeof(ScopedFd));
#else
static_assert(sizeof(EventFd) == sizeof(ScopedFd));
#endif

namespace {

bool isReadable(int fd) {
  pollfd pollFd{};
  pollFd.fd = fd;
  pollFd.events = POLLIN;
  return ::poll(&pollFd, 1, 0) == 1 && (pollFd.revents & POLLIN) != 0;
}

} // namespace

TEST(EventFdTest, moveTransfersReadableEvent) {
  EventFd event;
  const int originalFd = event.fd();

  event.notify();
  EventFd moved(std::move(event));

  EXPECT_EQ(originalFd, moved.fd());
  EXPECT_TRUE(isReadable(moved.fd()));
  moved.drainNonBlocking();
  EXPECT_FALSE(isReadable(moved.fd()));
}

TEST(EventFdTest, moveAssignmentClosesPreviousFdAndTransfersEvent) {
  EventFd source;
  source.notify();
  EventFd target;
  const int sourceFd = source.fd();

  target = std::move(source);

  EXPECT_EQ(sourceFd, target.fd());
  EXPECT_TRUE(isReadable(target.fd()));
}

#if !defined(__APPLE__)
TEST(EventFdTest, notifyReturnsWhenCounterIsAlreadySaturated) {
  EventFd event;
  const uint64_t saturated = std::numeric_limits<uint64_t>::max() - 1;

  ASSERT_EQ(
      static_cast<ssize_t>(sizeof(saturated)),
      ::write(event.fd(), &saturated, sizeof(saturated)));
  ASSERT_TRUE(isReadable(event.fd()));

  event.notify();

  EXPECT_TRUE(isReadable(event.fd()));
  event.drainNonBlocking();
  EXPECT_FALSE(isReadable(event.fd()));
}
#else
TEST(EventFdTest, notifyReturnsWhenPipeIsAlreadySaturated) {
  EventFd event;
  int pendingBytes = 0;
  bool saturated = false;
  // Bound the loop, but observe actual pipe occupancy rather than assuming a
  // platform-specific capacity. notify() writes one uint64_t or gets EAGAIN.
  for (size_t i = 0; i < 1024 * 1024; ++i) {
    event.notify();
    int bytes = 0;
    ASSERT_EQ(0, ::ioctl(event.fd(), FIONREAD, &bytes));
    if (bytes == pendingBytes) {
      saturated = true;
      break;
    }
    ASSERT_EQ(pendingBytes + sizeof(uint64_t), bytes);
    pendingBytes = bytes;
  }
  ASSERT_TRUE(saturated);
  ASSERT_GT(pendingBytes, 0);
  EXPECT_TRUE(isReadable(event.fd()));

  event.notify();
  int bytes = 0;
  ASSERT_EQ(0, ::ioctl(event.fd(), FIONREAD, &bytes));
  EXPECT_EQ(pendingBytes, bytes);
  event.drainNonBlocking();
  EXPECT_FALSE(isReadable(event.fd()));
  ASSERT_EQ(0, ::ioctl(event.fd(), FIONREAD, &bytes));
  EXPECT_EQ(0, bytes);

  event.notify();
  EXPECT_TRUE(isReadable(event.fd()));
  event.drainNonBlocking();
  EXPECT_FALSE(isReadable(event.fd()));
}
#endif
