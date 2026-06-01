#include "bolt/common/memory/bm/io/EventFd.h"

#include <cstdint>
#include <limits>
#include <poll.h>
#include <unistd.h>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

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
  moved.drain();
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

TEST(EventFdTest, notifyReturnsWhenCounterIsAlreadySaturated) {
  EventFd event;
  const uint64_t saturated = std::numeric_limits<uint64_t>::max() - 1;

  ASSERT_EQ(
      static_cast<ssize_t>(sizeof(saturated)),
      ::write(event.fd(), &saturated, sizeof(saturated)));
  ASSERT_TRUE(isReadable(event.fd()));

  event.notify();

  EXPECT_TRUE(isReadable(event.fd()));
  event.drain();
  EXPECT_FALSE(isReadable(event.fd()));
}
