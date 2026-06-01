#include "bolt/common/memory/bm/io/EventFd.h"

#include <poll.h>

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
